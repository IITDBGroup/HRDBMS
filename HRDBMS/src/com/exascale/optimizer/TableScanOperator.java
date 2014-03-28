package com.exascale.optimizer;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import com.exascale.filesystem.Block;
import com.exascale.filesystem.Page;
import com.exascale.filesystem.RID;
import com.exascale.gpu.Kernel;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.managers.ResourceManager;
import com.exascale.misc.BufferedLinkedBlockingQueue;
import com.exascale.misc.DataEndMarker;
import com.exascale.misc.DateParser;
import com.exascale.misc.FastStringTokenizer;
import com.exascale.misc.HParms;
import com.exascale.misc.MyDate;
import com.exascale.misc.Utils;
import com.exascale.optimizer.MetaData.PartitionMetaData;
import com.exascale.tables.DataType;
import com.exascale.tables.Plan;
import com.exascale.tables.Schema;
import com.exascale.tables.Schema.FieldValue;
import com.exascale.tables.Schema.Row;
import com.exascale.tables.Schema.RowIterator;
import com.exascale.tables.Transaction;
import com.exascale.threads.ThreadPoolThread;

public final class TableScanOperator implements Operator, Serializable
{
	private HashMap<String, String> cols2Types = new HashMap<String, String>();

	private HashMap<String, Integer> cols2Pos = new HashMap<String, Integer>();

	private TreeMap<Integer, String> pos2Col = new TreeMap<Integer, String>();
	private final String name;
	private final String schema;
	private final ArrayList<String> ins = new ArrayList<String>();
	private final ArrayList<Operator> parents = new ArrayList<Operator>();
	public volatile BufferedLinkedBlockingQueue readBuffer;
	private volatile HashMap<Operator, BufferedLinkedBlockingQueue> readBuffers = new HashMap<Operator, BufferedLinkedBlockingQueue>();
	private boolean startDone = false;
	private boolean optimize = false;
	private final HashMap<Operator, HashSet<HashMap<Filter, Filter>>> filters = new HashMap<Operator, HashSet<HashMap<Filter, Filter>>>();
	HashMap<Operator, CNFFilter> orderedFilters = new HashMap<Operator, CNFFilter>();
	private final MetaData meta;
	private final HashMap<Operator, Operator> opParents = new HashMap<Operator, Operator>();
	private ArrayList<Integer> neededPos;
	private ArrayList<Integer> fetchPos;
	private TreeMap<Integer, String> midPos2Col;
	private HashMap<String, String> midCols2Types;
	private boolean set = false;
	private PartitionMetaData partMeta;
	private final HashMap<Operator, ArrayList<Integer>> activeDevices = new HashMap<Operator, ArrayList<Integer>>();
	private final HashMap<Operator, ArrayList<Integer>> activeNodes = new HashMap<Operator, ArrayList<Integer>>();
	private ArrayList<Integer> devices = new ArrayList<Integer>();
	private int node;
	private boolean phase2Done = false;
	public HashMap<Integer, Operator> device2Child = new HashMap<Integer, Operator>();
	private final ArrayList<Operator> children = new ArrayList<Operator>();
	private final ArrayList<String> randomIns = new ArrayList<String>();
	private final HashMap<String, Integer> ins2Device = new HashMap<String, Integer>();
	private boolean indexOnly = false;
	private volatile boolean forceDone = false;
	private static int PREFETCH_REQUEST_SIZE;
	private static int PAGES_IN_ADVANCE;
	private Transaction tx;
	private Plan plan;
	
	public void setPlan(Plan plan)
	{
		this.plan = plan;
	}

	static
	{
		HParms hparms = HRDBMSWorker.getHParms();
		PREFETCH_REQUEST_SIZE = Integer.parseInt(hparms.getProperty("prefetch_request_size")); // 80
		PAGES_IN_ADVANCE = Integer.parseInt(hparms.getProperty("pages_in_advance")); // 40
	}

	public TableScanOperator(String schema, String name, MetaData meta) throws Exception
	{
		this.meta = meta;
		this.name = name;
		this.schema = schema;
		cols2Types = meta.getCols2TypesForTable(schema, name);
		cols2Pos = meta.getCols2PosForTable(schema, name);
		pos2Col = meta.getPos2ColForTable(schema, name);
	}

	@Override
	public void add(Operator op) throws Exception
	{
		children.add(op);
		op.registerParent(this);
	}

	public void addActiveDeviceForParent(int i, Operator op)
	{
		ArrayList<Integer> list = activeDevices.get(op);

		if (list == null)
		{
			list = new ArrayList<Integer>(1);
			list.add(i);
			activeDevices.put(op, list);
			return;
		}

		list.add(i);
	}

	public void addActiveDevices(ArrayList<Integer> devs)
	{
		devices.addAll(devs);
	}

	public void addActiveDevicesForParent(ArrayList<Integer> is, Operator op)
	{
		final ArrayList<Integer> list = activeDevices.get(op);

		if (list == null)
		{
			activeDevices.put(op, is);
			return;
		}

		list.addAll(is);
	}

	public void addActiveNodeForParent(int i, Operator op)
	{
		ArrayList<Integer> list = activeNodes.get(op);

		if (list == null)
		{
			list = new ArrayList<Integer>(1);
			list.add(i);
			activeNodes.put(op, list);
			return;
		}

		list.add(i);
	}

	public void addFilter(ArrayList<Filter> filters, Operator op, Operator opParent)
	{
		opParents.put(opParent, op);

		HashSet<HashMap<Filter, Filter>> f = this.filters.get(op);
		if (f == null)
		{
			final Operator op2 = opParents.get(op);
			if (op2 != null)
			{
				// parent has changed because of previous addFilter()
				opParents.remove(op);
				f = this.filters.get(op2);
				this.filters.remove(op2);
			}
			else
			{
				f = new HashSet<HashMap<Filter, Filter>>();
				final HashMap<Filter, Filter> map = new HashMap<Filter, Filter>();
				for (final Filter filter : filters)
				{
					map.put(filter, filter);
				}

				f.add(map);
				this.filters.put(op, f);
				orderedFilters.put(op, new CNFFilter(f, meta, cols2Pos));
				return;
			}
		}

		final HashMap<Filter, Filter> map = new HashMap<Filter, Filter>();
		for (final Filter filter : filters)
		{
			map.put(filter, filter);
		}

		f.add(map);
		this.filters.put(op, f);
		orderedFilters.put(op, new CNFFilter(f, meta, cols2Pos));
	}

	public boolean allDevices()
	{
		return partMeta.allDevices();
	}

	public boolean allNodes()
	{
		return partMeta.allNodes();
	}

	public boolean anyNode()
	{
		return partMeta.anyNode();
	}

	@Override
	public ArrayList<Operator> children()
	{
		return children;
	}

	public void cleanupOrderedFilters()
	{
		if (orderedFilters.size() <= 1)
		{
			return;
		}
		final CNFFilter theOne = orderedFilters.get(parents.get(0));
		orderedFilters.clear();
		if (theOne != null)
		{
			orderedFilters.put(parents.get(0), theOne);
		}
	}

	public void clearOpParents()
	{
		if (opParents.size() > 0)
		{
			opParents.clear();
		}
	}

	@Override
	public TableScanOperator clone()
	{
		TableScanOperator retval = null;
		try
		{
			retval = new TableScanOperator(schema, name, meta);
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.error("", e);
			System.exit(1);
		}
		retval.neededPos = (ArrayList<Integer>)neededPos.clone();
		retval.fetchPos = (ArrayList<Integer>)fetchPos.clone();
		retval.midPos2Col = (TreeMap<Integer, String>)midPos2Col.clone();
		retval.midCols2Types = (HashMap<String, String>)midCols2Types.clone();
		retval.cols2Pos = (HashMap<String, Integer>)cols2Pos.clone();
		retval.pos2Col = (TreeMap<Integer, String>)pos2Col.clone();
		retval.cols2Types = (HashMap<String, String>)cols2Types.clone();
		retval.set = set;
		retval.partMeta = partMeta;
		retval.phase2Done = phase2Done;
		retval.node = node;
		retval.devices = (ArrayList<Integer>)devices.clone();
		retval.indexOnly = indexOnly;
		return retval;
	}

	@Override
	public void close() throws IOException
	{
		for (final Operator o : children)
		{
			try
			{
				o.close();
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				System.exit(1);
			}
		}
	}

	public boolean deviceIsHash()
	{
		return partMeta.deviceIsHash();
	}

	public ArrayList<Integer> deviceSet()
	{
		return partMeta.deviceSet();
	}

	@Override
	public boolean equals(Object rhs)
	{
		if (rhs == null || (!(rhs instanceof TableScanOperator)))
		{
			return false;
		}
		else
		{
			return (schema.equals(((TableScanOperator)rhs).schema) && name.equals(((TableScanOperator)rhs).name) && node == ((TableScanOperator)rhs).node);
		}
	}

	public Operator firstParent()
	{
		return parents.get(0);
	}

	@Override
	public int getChildPos()
	{
		return 0;
	}

	public CNFFilter getCNFForParent(Operator op)
	{
		return orderedFilters.get(op);
	}

	@Override
	public HashMap<String, Integer> getCols2Pos()
	{
		return cols2Pos;
	}

	@Override
	public HashMap<String, String> getCols2Types()
	{
		return cols2Types;
	}

	public ArrayList<String> getDeviceHash()
	{
		return partMeta.getDeviceHash();
	}

	public ArrayList<Integer> getDeviceList()
	{
		return devices;
	}

	public ArrayList<Integer> getDeviceList(Operator op)
	{
		return activeDevices.get(op);
	}

	public String getDeviceRangeCol()
	{
		return partMeta.getDeviceRangeCol();
	}

	public ArrayList<Object> getDeviceRanges()
	{
		return partMeta.getDeviceRanges();
	}

	public ArrayList<Integer> getDevicesMatchingRangeFilters(ArrayList<Filter> rangeFilters)
	{
		final ArrayList<Integer> retval = new ArrayList<Integer>();
		ArrayList<Integer> deviceList = null;
		if (partMeta.allDevices())
		{
			deviceList = new ArrayList<Integer>(partMeta.getNumDevices());
			int i = 0;
			while (i < partMeta.getNumDevices())
			{
				deviceList.add(i);
				i++;
			}
		}
		else
		{
			deviceList = partMeta.deviceSet();
		}

		Object oldLE = null;
		int i = 0;
		for (final Object leVal : partMeta.getDeviceRanges())
		{
			if (canAnythingInRangeSatisfyFilters(rangeFilters, oldLE, leVal))
			{
				retval.add(deviceList.get(i));
			}

			i++;
			oldLE = leVal;
		}

		if (canAnythingInRangeSatisfyFilters(rangeFilters, oldLE, null))
		{
			retval.add(deviceList.get(i));
		}

		return retval;
	}

	public CNFFilter getFirstCNF()
	{
		for (final CNFFilter cnf : orderedFilters.values())
		{
			return cnf;
		}

		return null;
	}

	public HashSet<HashMap<Filter, Filter>> getHSHM()
	{
		final CNFFilter retval = orderedFilters.get(parents.get(0));
		if (retval == null)
		{
			return null;
		}

		return retval.getHSHM();
	}

	@Override
	public MetaData getMeta()
	{
		return meta;
	}

	@Override
	public int getNode()
	{
		return node;
	}

	public ArrayList<String> getNodeGroupHash()
	{
		return partMeta.getNodeGroupHash();
	}

	public HashMap<Integer, ArrayList<Integer>> getNodeGroupHashMap()
	{
		return partMeta.getNodeGroupHashMap();
	}

	public String getNodeGroupRangeCol()
	{
		return partMeta.getNodeGroupRangeCol();
	}

	public ArrayList<Object> getNodeGroupRanges()
	{
		return partMeta.getNodeGroupRanges();
	}

	public ArrayList<Integer> getNodeGroupsMatchingRangeFilters(ArrayList<Filter> rangeFilters)
	{
		final ArrayList<Integer> retval = new ArrayList<Integer>();
		final ArrayList<Integer> nodeGroupList = partMeta.nodeGroupSet();

		Object oldLE = null;
		int i = 0;
		for (final Object leVal : partMeta.getNodeGroupRanges())
		{
			if (canAnythingInRangeSatisfyFilters(rangeFilters, oldLE, leVal))
			{
				retval.add(nodeGroupList.get(i));
			}

			i++;
			oldLE = leVal;
		}

		if (canAnythingInRangeSatisfyFilters(rangeFilters, oldLE, null))
		{
			retval.add(nodeGroupList.get(i));
		}

		return retval;
	}

	public ArrayList<String> getNodeHash()
	{
		return partMeta.getNodeHash();
	}

	public ArrayList<Integer> getNodeList(Operator op)
	{
		return activeNodes.get(op);
	}

	public String getNodeRangeCol()
	{
		return partMeta.getNodeRangeCol();
	}

	public ArrayList<Object> getNodeRanges()
	{
		return partMeta.getNodeRanges();
	}

	public ArrayList<Integer> getNodesMatchingRangeFilters(ArrayList<Filter> rangeFilters)
	{
		final ArrayList<Integer> retval = new ArrayList<Integer>();
		ArrayList<Integer> nodeList = null;
		if (partMeta.allNodes())
		{
			nodeList = new ArrayList<Integer>(partMeta.getNumNodes());
			int i = 0;

			if (partMeta.noNodeGroupSet())
			{
				while (i < partMeta.getNumNodes())
				{
					nodeList.add(i);
					i++;
				}
			}
			else
			{
				final Iterator iter = partMeta.getNodeGroupHashMap().values().iterator();
				iter.hasNext();
				final int size = ((ArrayList<Integer>)(iter.next())).size();
				while (i < size)
				{
					nodeList.add(i);
					i++;
				}
			}
		}
		else
		{
			nodeList = partMeta.nodeSet();
		}

		Object oldLE = null;
		int i = 0;
		for (final Object leVal : partMeta.getNodeRanges())
		{
			if (canAnythingInRangeSatisfyFilters(rangeFilters, oldLE, leVal))
			{
				retval.add(nodeList.get(i));
			}

			i++;
			oldLE = leVal;
		}

		if (canAnythingInRangeSatisfyFilters(rangeFilters, oldLE, null))
		{
			retval.add(nodeList.get(i));
		}

		return retval;
	}

	public int getNumDevices()
	{
		return partMeta.getNumDevices();
	}

	public int getNumNodeGroups()
	{
		return partMeta.getNumNodeGroups();
	}

	public int getNumNodes()
	{
		return partMeta.getNumNodes();
	}

	@Override
	public TreeMap<Integer, String> getPos2Col()
	{
		return pos2Col;
	}

	@Override
	public ArrayList<String> getReferences()
	{
		final ArrayList<String> retval = new ArrayList<String>(0);
		return retval;
	}

	public String getSchema()
	{
		return schema;
	}

	public int getSingleDevice()
	{
		return partMeta.getSingleDevice();
	}

	public int getSingleNode()
	{
		return partMeta.getSingleNode();
	}

	public int getSingleNodeGroup()
	{
		return partMeta.getSingleNodeGroup();
	}

	public String getTable()
	{
		return name;
	}

	@Override
	public int hashCode()
	{
		return schema.hashCode() + name.hashCode();
	}

	public boolean isSingleDeviceSet()
	{
		return partMeta.isSingleDeviceSet();
	}

	public boolean isSingleNodeGroupSet()
	{
		return partMeta.isSingleNodeGroupSet();
	}

	public boolean isSingleNodeSet()
	{
		return partMeta.isSingleNodeSet();
	}

	public boolean metaDataSet()
	{
		return set;
	}

	@Override
	public Object next(Operator op) throws Exception
	{
		if (!optimize)
		{
			Object o;
			final BufferedLinkedBlockingQueue buffer = readBuffers.get(op);

			o = buffer.take();

			if (o instanceof DataEndMarker)
			{
				o = buffer.peek();
				if (o == null)
				{
					buffer.put(new DataEndMarker());
					return new DataEndMarker();
				}
				else
				{
					buffer.put(new DataEndMarker());
					return o;
				}
			}
			return o;
		}
		else
		{
			Object o = readBuffer.take();

			if (o instanceof DataEndMarker)
			{
				o = readBuffer.peek();
				if (o == null)
				{
					readBuffer.put(new DataEndMarker());
					return new DataEndMarker();
				}
				else
				{
					readBuffer.put(new DataEndMarker());
					return o;
				}
			}
			return o;
		}
	}

	@Override
	public void nextAll(Operator op)
	{
		forceDone = true;
	}

	public boolean nodeGroupIsHash()
	{
		return partMeta.nodeGroupIsHash();
	}

	public ArrayList<Integer> nodeGroupSet()
	{
		return partMeta.nodeGroupSet();
	}

	public boolean nodeIsHash()
	{
		return partMeta.nodeIsHash();
	}

	public ArrayList<Integer> nodeSet()
	{
		return partMeta.nodeSet();
	}

	public boolean noNodeGroupSet()
	{
		return partMeta.noNodeGroupSet();
	}

	@Override
	public Operator parent()
	{
		throw new UnsupportedOperationException("TableScanOperator does not support parent()");
	}

	public ArrayList<Operator> parents()
	{
		return parents;
	}

	public boolean phase2Done()
	{
		return phase2Done;
	}

	@Override
	public void registerParent(Operator op)
	{
		parents.add(op);
		if (opParents.containsKey(op))
		{
			orderedFilters.put(op, orderedFilters.get(opParents.get(op)));
			opParents.put(op.parent(), op);
		}
	}

	@Override
	public void removeChild(Operator op)
	{
		children.remove(op);
		op.removeParent(this);
	}

	@Override
	public void removeParent(Operator op)
	{
		parents.remove(op);
	}

	@Override
	public void reset()
	{
		if (!startDone)
		{
			try
			{
				start();
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				System.exit(1);
			}
		}
		else
		{
			for (final Operator o : children)
			{
				try
				{
					o.reset();
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.error("", e);
					System.exit(1);
				}
			}

			readBuffer.clear();
			forceDone = false;
			init();
		}
	}

	public void setChildForDevice(int device, Operator child)
	{
		device2Child.put(device, child);
	}

	@Override
	public void setChildPos(int pos)
	{
	}

	public void setCNFForParent(Operator op, CNFFilter filter)
	{
		orderedFilters.put(op, filter);
		if (op instanceof NetworkHashAndSendOperator || op instanceof NetworkSendMultipleOperator || op instanceof NetworkSendRROperator)
		{
			return;
		}
		opParents.put(op.parent(), op);
	}

	public void setIndexOnly()
	{
		indexOnly = true;
	}

	public void setMetaData()
	{
		set = true;
		partMeta = meta.getPartMeta(schema, name);
	}

	public void setNeededCols(ArrayList<String> needed)
	{
		fetchPos = new ArrayList<Integer>(needed.size());
		for (final String col : needed)
		{
			fetchPos.add(cols2Pos.get(col));
		}

		for (final CNFFilter filter : orderedFilters.values())
		{
			for (final String col2 : filter.getReferences())
			{
				final int pos = cols2Pos.get(col2);
				if (!fetchPos.contains(pos))
				{
					fetchPos.add(pos);
				}
			}
		}

		int i = 0;
		final HashMap<String, Integer> fetchCols2Pos = new HashMap<String, Integer>();
		midPos2Col = new TreeMap<Integer, String>();
		midCols2Types = new HashMap<String, String>();
		for (final int pos : fetchPos)
		{
			fetchCols2Pos.put(pos2Col.get(pos), i);
			midPos2Col.put(i, pos2Col.get(pos));
			midCols2Types.put(pos2Col.get(pos), cols2Types.get(pos2Col.get(pos)));
			i++;
		}

		for (final CNFFilter filter : orderedFilters.values())
		{
			filter.updateCols2Pos(fetchCols2Pos);
		}

		// calculate how to go from what was fetched to what needs to be output
		neededPos = new ArrayList<Integer>(needed.size());
		for (final String col : needed)
		{
			neededPos.add(fetchCols2Pos.get(col));
		}

		// update internal metadata x3
		cols2Pos = new HashMap<String, Integer>();
		pos2Col = new TreeMap<Integer, String>();
		final HashMap<String, String> tempCols2Types = new HashMap<String, String>();
		i = 0;
		for (final String col : needed)
		{
			cols2Pos.put(col, i);
			pos2Col.put(i, col);
			tempCols2Types.put(col, cols2Types.get(col));
			i++;
		}

		cols2Types = tempCols2Types;
	}

	@Override
	public void setNode(int node)
	{
		this.node = node;
	}

	public void setPhase2Done()
	{
		phase2Done = true;
	}

	public void setTransaction(Transaction tx)
	{
		this.tx = tx;
	}

	@Override
	public synchronized void start() throws IOException
	{
		if (!startDone)
		{
			startDone = true;

			for (final Operator o : children)
			{
				try
				{
					o.start();
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.error("", e);
					System.exit(1);
				}
			}

			if (devices.size() == 0)
			{
				final String in = name.toLowerCase() + ".tbl";
				ins.add(in);
			}
			else
			{
				for (final int device : devices)
				{
					if (children.size() == 0)
					{
						final String in = meta.getDevicePath(device) + name.toLowerCase() + ".tbl";
						ins.add(in);
					}
					else
					{
						final String in = meta.getDevicePath(device) + name.toLowerCase() + ".tbl";
						randomIns.add(in);
						ins2Device.put(in, device);
					}
				}
			}

			for (final Operator parent : parents)
			{
				if (!orderedFilters.containsKey(parent))
				{
					orderedFilters.put(parent, new NullCNFFilter());
				}
			}

			if (parents.size() == 1)
			{
				optimize = true;
			}

			if (!optimize)
			{
				for (final Operator parent : parents)
				{
					readBuffers.put(parent, new BufferedLinkedBlockingQueue(ResourceManager.QUEUE_SIZE));
				}
			}
			else
			{
				readBuffer = new BufferedLinkedBlockingQueue(ResourceManager.QUEUE_SIZE);
			}

			init();
		}
	}

	@Override
	public String toString()
	{
		String retval = "TableScanOperator(" + node + ":" + devices + "): " + schema + "." + name;
		for (final Map.Entry entry : orderedFilters.entrySet())
		{
			retval += (", (" + entry.getKey().toString() + ", " + entry.getValue().toString()) + ")";
		}

		return retval;
	}

	private boolean canAnythingInRangeSatisfyFilters(ArrayList<Filter> filters, Object lowLE, Object highLE)
	{
		if (lowLE == null)
		{
			if (highLE instanceof Double)
			{
				lowLE = Double.MIN_VALUE;
			}
			else if (highLE instanceof Long)
			{
				lowLE = Long.MIN_VALUE;
			}
			else if (highLE instanceof Integer)
			{
				lowLE = Integer.MIN_VALUE;
			}
			else if (highLE instanceof MyDate)
			{
				lowLE = new MyDate(Long.MIN_VALUE);
			}
			else if (highLE instanceof String)
			{
				lowLE = "";
			}
		}

		if (highLE == null)
		{
			if (lowLE instanceof Double)
			{
				highLE = Double.MAX_VALUE;
			}
			else if (lowLE instanceof Long)
			{
				highLE = Long.MAX_VALUE;
			}
			else if (lowLE instanceof Integer)
			{
				highLE = Integer.MAX_VALUE;
			}
			else if (lowLE instanceof MyDate)
			{
				highLE = new MyDate(Long.MAX_VALUE);
			}
			else if (lowLE instanceof String)
			{
				highLE = "\uFFFF";
			}
		}

		for (final Filter filter : filters)
		{
			if (filter.op().equals("E"))
			{
				Object literal = null;
				if (filter.leftIsColumn())
				{
					literal = filter.rightLiteral();
				}
				else
				{
					literal = filter.leftLiteral();
				}

				if (((Comparable)lowLE).compareTo(literal) < 1 && ((Comparable)highLE).compareTo(literal) > -1)
				{
					continue;
				}
				else
				{
					return false;
				}
			}

			final HashMap<String, Integer> cols2Pos = new HashMap<String, Integer>();
			String col = null;
			if (filter.leftIsColumn())
			{
				col = filter.leftColumn();
			}
			else
			{
				col = filter.rightColumn();
			}
			cols2Pos.put(col, 0);
			final ArrayList<Object> row1 = new ArrayList<Object>(1);
			row1.add(lowLE);
			final ArrayList<Object> row2 = new ArrayList<Object>(1);
			row2.add(highLE);

			try
			{
				if (filter.passes(row1, cols2Pos) || filter.passes(row2, cols2Pos))
				{
					continue;
				}
				else
				{
					return false;
				}
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				System.exit(1);
			}
		}

		return true;
	}

	private void init()
	{
		final InitThread t = new InitThread();
		t.start();
	}

	private final class InitThread extends ThreadPoolThread
	{
		private final ArrayList<ReaderThread> reads = new ArrayList<ReaderThread>(ins.size());

		@Override
		public void run()
		{
			try
			{
				for (final String in : ins)
				{
					final ReaderThread read = new ReaderThread(in);
					read.start();
					reads.add(read);
				}

				for (final String in : randomIns)
				{
					final ReaderThread read = new ReaderThread(in, true);
					read.start();
					reads.add(read);
				}

				for (final ReaderThread read : reads)
				{
					read.join();
				}

				if (optimize)
				{
					readBuffer.put(new DataEndMarker());
				}
				else
				{
					for (final BufferedLinkedBlockingQueue q : readBuffers.values())
					{
						q.put(new DataEndMarker());
					}
				}
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				System.exit(1);
			}
		}
	}

	private final class ReaderThread extends ThreadPoolThread
	{
		private String in;
		private String in2;

		public ReaderThread(String in2, boolean marker)
		{
			this.in2 = in2;
		}

		public ReaderThread(String in)
		{
			this.in = in;
		}

		@Override
		public final void run()
		{
			ArrayList<String> types = null;
			CNFFilter filter = orderedFilters.get(parents.get(0));
			if (types == null)
			{
				types = new ArrayList<String>(midPos2Col.size());
				for (final Map.Entry entry : midPos2Col.entrySet())
				{
					types.add(midCols2Types.get(entry.getValue()));
				}
			}

			try
			{
				if (in2 == null)
				{
					//TODO acquire table length lock in shared mode
					int numBlocks = (int)(new File(in).length() / Page.BLOCK_SIZE);
					HashMap<Integer, DataType> layout = new HashMap<Integer, DataType>();
					HashMap<String, String> cols2Types = meta.getCols2TypesForTable(schema, name);
					for (Map.Entry entry : meta.getPos2ColForTable(schema, name).entrySet())
					{
						String type = cols2Types.get(entry.getValue());
						DataType value = null;
						if (type.equals("INT"))
						{
							value = new DataType(DataType.INTEGER, 0, 0);
						}
						else if (type.equals("FLOAT"))
						{
							value = new DataType(DataType.DOUBLE, 0, 0);
						}
						else if (type.equals("CHAR"))
						{
							value = new DataType(DataType.VARCHAR, 0, 0);
						}
						else if (type.equals("LONG"))
						{
							value = new DataType(DataType.BIGINT, 0, 0);
						}
						else if (type.equals("DATE"))
						{
							value = new DataType(DataType.DATE, 0, 0);
						}

						layout.put((Integer)entry.getKey(), value);
					}

					Schema sch = new Schema(layout);
					int onPage = Schema.HEADER_SIZE;
					int lastRequested = Schema.HEADER_SIZE - 1;
					while (onPage < numBlocks)
					{
						if (lastRequested - onPage < PAGES_IN_ADVANCE)
						{
							Block[] toRequest = new Block[lastRequested + PREFETCH_REQUEST_SIZE < numBlocks ? PREFETCH_REQUEST_SIZE : numBlocks - lastRequested - 1];
							int i = 0;
							while (i < toRequest.length)
							{
								toRequest[i] = new Block(in, lastRequested + i + 1);
								i++;
							}
							tx.requestPages(toRequest);
							lastRequested += toRequest.length;
						}

						tx.read(new Block(in, onPage++), sch);
						RowIterator rit = sch.rowIterator();
						while (rit.hasNext())
						{
							Row r = rit.next();
							if (!r.getCol(0).exists())
							{
								continue;
							}
							final ArrayList<Object> row = new ArrayList<Object>(types.size());
							int j = 0;
							while (j < fetchPos.size())
							{
								types.get(j);
								FieldValue fv = r.getCol(fetchPos.get(j));
								row.add(fv.getValue());
								j++;
							}

							if (!optimize)
							{
								for (final Map.Entry entry : readBuffers.entrySet())
								{
									final BufferedLinkedBlockingQueue q = (BufferedLinkedBlockingQueue)entry.getValue();
									filter = orderedFilters.get(entry.getKey());

									if (filter != null)
									{
										if (filter.passes(row))
										{
											final ArrayList<Object> newRow = new ArrayList<Object>(neededPos.size());
											for (final int pos : neededPos)
											{
												newRow.add(row.get(pos));
											}
											q.put(newRow);
										}
									}
									else
									{
										final ArrayList<Object> newRow = new ArrayList<Object>(neededPos.size());
										for (final int pos : neededPos)
										{
											newRow.add(row.get(pos));
										}
										q.put(newRow);
									}
								}
							}
							else
							{
								if (filter != null)
								{
									if (filter.passes(row))
									{
										final ArrayList<Object> newRow = new ArrayList<Object>(neededPos.size());
										for (final int pos : neededPos)
										{
											newRow.add(row.get(pos));
										}
										if (!forceDone)
										{
											readBuffer.put(newRow);
										}
										else
										{
											readBuffer.put(new DataEndMarker());
											return;
										}
									}
								}
								else
								{
									final ArrayList<Object> newRow = new ArrayList<Object>(neededPos.size());
									for (final int pos : neededPos)
									{
										newRow.add(row.get(pos));
									}
									if (!forceDone)
									{
										readBuffer.put(newRow);
									}
									else
									{
										readBuffer.put(new DataEndMarker());
										return;
									}
								}
							}
						}
					}
				}
				else
				{
					final Operator child = device2Child.get(ins2Device.get(in2));
					Object o = null;
					try
					{
						o = child.next(TableScanOperator.this);
					}
					catch (final Exception e)
					{
						HRDBMSWorker.logger.error("ins2Device = " + ins2Device, e);
						HRDBMSWorker.logger.error("ins2Device.get(in2) = " + ins2Device.get(in2));
						HRDBMSWorker.logger.error("device2Child = " + device2Child);
						HRDBMSWorker.logger.error("device2Child.get(ins2Device.get(in2)) = " + device2Child.get(ins2Device.get(in2)));
						System.exit(1);
					}
					// @?Parallel
					int device = ins2Device.get(in2);
					int currentPage = -1;
					HashMap<Integer, DataType> layout = new HashMap<Integer, DataType>();
					HashMap<String, String> cols2Types = meta.getCols2TypesForTable(schema, name);
					for (Map.Entry entry : meta.getPos2ColForTable(schema, name).entrySet())
					{
						String type = cols2Types.get(entry.getValue());
						DataType value = null;
						if (type.equals("INT"))
						{
							value = new DataType(DataType.INTEGER, 0, 0);
						}
						else if (type.equals("FLOAT"))
						{
							value = new DataType(DataType.DOUBLE, 0, 0);
						}
						else if (type.equals("CHAR"))
						{
							value = new DataType(DataType.VARCHAR, 0, 0);
						}
						else if (type.equals("LONG"))
						{
							value = new DataType(DataType.BIGINT, 0, 0);
						}
						else if (type.equals("DATE"))
						{
							value = new DataType(DataType.DATE, 0, 0);
						}

						layout.put((Integer)entry.getKey(), value);
					}

					Schema sch = new Schema(layout);
					while (!(o instanceof DataEndMarker))
					{
						if (!indexOnly)
						{
							long partialRid = (Long)(((ArrayList<Object>)o).get(0));
							int blockNum = (int)(partialRid >> 32);
							int recNum = (int)(partialRid & 0xFFFFFFFF);
							if (blockNum != currentPage)
							{
								Block b = new Block(in2, blockNum);
								tx.requestPage(b);
								tx.read(b, sch);
							}
							final Row r = sch.getRow(new RID(node, device, blockNum, recNum));
							final ArrayList<Object> row = new ArrayList<Object>(types.size());
							int j = 0;
							while (j < fetchPos.size())
							{
								FieldValue fv = r.getCol(fetchPos.get(j));
								row.add(fv.getValue());
								j++;
							}

							if (!optimize)
							{
								for (final Map.Entry entry : readBuffers.entrySet())
								{
									final BufferedLinkedBlockingQueue q = (BufferedLinkedBlockingQueue)entry.getValue();
									filter = orderedFilters.get(entry.getKey());

									if (filter != null)
									{
										if (filter.passes(row))
										{
											final ArrayList<Object> newRow = new ArrayList<Object>(neededPos.size());
											for (final int pos : neededPos)
											{
												newRow.add(row.get(pos));
											}
											q.put(newRow);
										}
									}
									else
									{
										final ArrayList<Object> newRow = new ArrayList<Object>(neededPos.size());
										for (final int pos : neededPos)
										{
											newRow.add(row.get(pos));
										}
										q.put(newRow);
									}
								}
							}
							else
							{
								if (filter != null)
								{
									if (filter.passes(row))
									{
										final ArrayList<Object> newRow = new ArrayList<Object>(neededPos.size());
										for (final int pos : neededPos)
										{
											newRow.add(row.get(pos));
										}
										if (!forceDone)
										{
											readBuffer.put(newRow);
										}
										else
										{
											readBuffer.put(new DataEndMarker());
											return;
										}
									}
								}
								else
								{
									final ArrayList<Object> newRow = new ArrayList<Object>(neededPos.size());
									for (final int pos : neededPos)
									{
										newRow.add(row.get(pos));
									}
									if (!forceDone)
									{
										readBuffer.put(newRow);
									}
									else
									{
										readBuffer.put(new DataEndMarker());
										return;
									}
								}
							}

							o = child.next(TableScanOperator.this);
						}
						else
						{
							filter = orderedFilters.get(parents.get(0));
							filter.updateCols2Pos(child.getCols2Pos());

							if (filter != null)
							{
								if (!filter.passes((ArrayList<Object>)o))
								{
									o = child.next(TableScanOperator.this);
									continue;
								}
							}

							final ArrayList<Object> row = new ArrayList<Object>(pos2Col.size());
							for (final String col : pos2Col.values())
							{
								row.add(((ArrayList<Object>)o).get(child.getCols2Pos().get(col)));
							}

							if (!forceDone)
							{
								readBuffer.put(row);
							}
							else
							{
								readBuffer.put(new DataEndMarker());
								return;
							}
							o = child.next(TableScanOperator.this);
						}
					}

					// System.out.println("TableScanOperator read " + count +
					// " rows based on a RID list");
				}
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				System.exit(1);
			}
		}
	}
}
