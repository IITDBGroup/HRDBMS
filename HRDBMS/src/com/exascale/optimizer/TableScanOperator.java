package com.exascale.optimizer;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import com.exascale.filesystem.Block;
import com.exascale.filesystem.RID;
import com.exascale.managers.BufferManager;
import com.exascale.managers.BufferManager.RequestPagesThread;
import com.exascale.managers.FileManager;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.managers.LockManager;
import com.exascale.managers.ResourceManager;
import com.exascale.misc.BufferedLinkedBlockingQueue;
import com.exascale.misc.DataEndMarker;
import com.exascale.misc.HParms;
import com.exascale.misc.MultiHashMap;
import com.exascale.misc.MyDate;
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
	private static int PREFETCH_REQUEST_SIZE_STATIC;
	private static int PAGES_IN_ADVANCE_STATIC;
	public static AtomicInteger tsoCount = new AtomicInteger(0);

	private static sun.misc.Unsafe unsafe;
	private static long offset;

	public static MultiHashMap<Block, HashSet<HashMap<Filter, Filter>>> noResults = new MultiHashMap<Block, HashSet<HashMap<Filter, Filter>>>();
	public static AtomicInteger skippedPages = new AtomicInteger(0);
	static
	{
		try
		{
			HParms hparms = HRDBMSWorker.getHParms();
			PREFETCH_REQUEST_SIZE_STATIC = Integer.parseInt(hparms.getProperty("prefetch_request_size")); // 80
			PAGES_IN_ADVANCE_STATIC = Integer.parseInt(hparms.getProperty("pages_in_advance")); // 40
			final Field f = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
			f.setAccessible(true);
			unsafe = (sun.misc.Unsafe)f.get(null);
			final Field fieldToUpdate = ArrayList.class.getDeclaredField("elementData");
			// get unsafe offset to this field
			offset = unsafe.objectFieldOffset(fieldToUpdate);
		}
		catch (Exception e)
		{
			HRDBMSWorker.logger.debug("", e);
		}
	}
	private int PREFETCH_REQUEST_SIZE;

	private int PAGES_IN_ADVANCE;
	private HashMap<String, String> cols2Types = new HashMap<String, String>();
	private HashMap<String, Integer> cols2Pos = new HashMap<String, Integer>();
	private TreeMap<Integer, String> pos2Col = new TreeMap<Integer, String>();
	private String name;
	private String schema;
	private transient ArrayList<String> ins;
	private ArrayList<Operator> parents = new ArrayList<Operator>();
	public transient volatile BufferedLinkedBlockingQueue readBuffer;
	private transient volatile HashMap<Operator, BufferedLinkedBlockingQueue> readBuffers;
	private boolean startDone = false;
	private transient boolean optimize;
	private transient HashMap<Operator, HashSet<HashMap<Filter, Filter>>> filters = new HashMap<Operator, HashSet<HashMap<Filter, Filter>>>();
	protected HashMap<Operator, CNFFilter> orderedFilters = new HashMap<Operator, CNFFilter>();
	private MetaData meta;
	private transient HashMap<Operator, Operator> opParents = new HashMap<Operator, Operator>();
	private ArrayList<Integer> neededPos;
	private ArrayList<Integer> fetchPos;
	private String[] midPos2Col;
	private HashMap<String, String> midCols2Types;
	private boolean set = false;
	private transient PartitionMetaData partMeta; // OK now that clone won't
	// happen at runtime?
	private transient HashMap<Operator, ArrayList<Integer>> activeDevices = new HashMap<Operator, ArrayList<Integer>>();
	private transient HashMap<Operator, ArrayList<Integer>> activeNodes = new HashMap<Operator, ArrayList<Integer>>();
	public ArrayList<Integer> devices = new ArrayList<Integer>();
	private int node;
	private boolean phase2Done = false;
	public HashMap<Integer, Operator> device2Child = new HashMap<Integer, Operator>();
	private ArrayList<Operator> children = new ArrayList<Operator>();
	private transient ArrayList<String> randomIns;
	private transient HashMap<String, Integer> ins2Device;
	private boolean indexOnly = false;
	private transient volatile boolean forceDone;
	public Transaction tx;
	private String alias = "";
	public boolean getRID = false;
	private HashMap<String, String> tableCols2Types;
	private TreeMap<Integer, String> tablePos2Col;
	private HashMap<String, Integer> tableCols2Pos;
	private boolean releaseLocks = false;
	private boolean sample = false;
	private long sPer;
	private Index scanIndex = null;
	private transient AtomicLong received;
	private transient volatile boolean demReceived;
	private int tType = 0;

	private volatile transient HashSet<Integer> referencesHash = null;

	public TableScanOperator(String schema, String name, MetaData meta, HashMap<String, Integer> cols2Pos, TreeMap<Integer, String> pos2Col, HashMap<String, String> cols2Types, TreeMap<Integer, String> tablePos2Col, HashMap<String, String> tableCols2Types, HashMap<String, Integer> tableCols2Pos) throws Exception
	{
		this.meta = meta;
		this.name = name;
		this.schema = schema;
		this.cols2Types = (HashMap<String, String>)cols2Types.clone();
		this.cols2Pos = (HashMap<String, Integer>)cols2Pos.clone();
		this.pos2Col = (TreeMap<Integer, String>)pos2Col.clone();
		this.tableCols2Types = tableCols2Types;
		this.tablePos2Col = tablePos2Col;
		this.tableCols2Pos = tableCols2Pos;
		received = new AtomicLong(0);
	}

	// private static long MAX_PAGES;

	// static
	// {
	// MAX_PAGES =
	// (Long.parseLong(HRDBMSWorker.getHParms().getProperty("bp_pages")) /
	// MetaData.getNumDevices()) / 15;
	// }

	public TableScanOperator(String schema, String name, MetaData meta, Transaction tx) throws Exception
	{
		this.meta = meta;
		this.name = name;
		this.schema = schema;
		cols2Types = meta.getCols2TypesForTable(schema, name, tx);
		cols2Pos = meta.getCols2PosForTable(schema, name, tx);
		pos2Col = MetaData.cols2PosFlip(cols2Pos);
		tableCols2Types = (HashMap<String, String>)cols2Types.clone();
		tablePos2Col = (TreeMap<Integer, String>)pos2Col.clone();
		tableCols2Pos = (HashMap<String, Integer>)cols2Pos.clone();
		received = new AtomicLong(0);
	}

	public TableScanOperator(String schema, String name, MetaData meta, Transaction tx, boolean releaseLocks) throws Exception
	{
		this.meta = meta;
		this.name = name;
		this.schema = schema;
		cols2Types = meta.getCols2TypesForTable(schema, name, tx);
		cols2Pos = meta.getCols2PosForTable(schema, name, tx);
		pos2Col = MetaData.cols2PosFlip(cols2Pos);
		tableCols2Types = (HashMap<String, String>)cols2Types.clone();
		tablePos2Col = (TreeMap<Integer, String>)pos2Col.clone();
		tableCols2Pos = (HashMap<String, Integer>)cols2Pos.clone();
		this.releaseLocks = releaseLocks;
		received = new AtomicLong(0);
	}

	public TableScanOperator(String schema, String name, MetaData meta, Transaction tx, boolean releaseLocks, HashMap<String, Integer> cols2Pos, TreeMap<Integer, String> pos2Col) throws Exception
	{
		this.meta = meta;
		this.name = name;
		this.schema = schema;
		cols2Types = meta.getCols2TypesForTable(schema, name, tx);
		this.cols2Pos = cols2Pos;
		this.pos2Col = pos2Col;
		tableCols2Types = (HashMap<String, String>)cols2Types.clone();
		tablePos2Col = (TreeMap<Integer, String>)pos2Col.clone();
		tableCols2Pos = (HashMap<String, Integer>)cols2Pos.clone();
		this.releaseLocks = releaseLocks;
		received = new AtomicLong(0);
	}

	public static TableScanOperator deserialize(InputStream in, HashMap<Long, Object> prev) throws Exception
	{
		TableScanOperator value = (TableScanOperator)unsafe.allocateInstance(TableScanOperator.class);
		prev.put(OperatorUtils.readLong(in), value);
		value.cols2Types = OperatorUtils.deserializeStringHM(in, prev);
		value.cols2Pos = OperatorUtils.deserializeStringIntHM(in, prev);
		value.pos2Col = OperatorUtils.deserializeTM(in, prev);
		value.name = OperatorUtils.readString(in, prev);
		value.schema = OperatorUtils.readString(in, prev);
		value.parents = OperatorUtils.deserializeALOp(in, prev);
		value.startDone = OperatorUtils.readBool(in);
		value.orderedFilters = OperatorUtils.deserializeHMOpCNF(in, prev);
		value.meta = new MetaData();
		value.neededPos = OperatorUtils.deserializeALI(in, prev);
		value.fetchPos = OperatorUtils.deserializeALI(in, prev);
		value.midPos2Col = OperatorUtils.deserializeStringArray(in, prev);
		value.midCols2Types = OperatorUtils.deserializeStringHM(in, prev);
		value.set = OperatorUtils.readBool(in);
		value.devices = OperatorUtils.deserializeALI(in, prev);
		value.node = OperatorUtils.readInt(in);
		value.phase2Done = OperatorUtils.readBool(in);
		value.device2Child = OperatorUtils.deserializeHMIntOp(in, prev);
		value.children = OperatorUtils.deserializeALOp(in, prev);
		value.indexOnly = OperatorUtils.readBool(in);
		value.tx = new Transaction(OperatorUtils.readLong(in));
		value.alias = OperatorUtils.readString(in, prev);
		value.getRID = OperatorUtils.readBool(in);
		value.tableCols2Types = OperatorUtils.deserializeStringHM(in, prev);
		value.tablePos2Col = OperatorUtils.deserializeTM(in, prev);
		value.tableCols2Pos = OperatorUtils.deserializeStringIntHM(in, prev);
		value.releaseLocks = OperatorUtils.readBool(in);
		value.sample = OperatorUtils.readBool(in);
		value.sPer = OperatorUtils.readLong(in);
		value.scanIndex = OperatorUtils.deserializeIndex(in, prev);
		value.received = new AtomicLong(0);
		value.demReceived = false;
		value.tType = OperatorUtils.readInt(in);
		return value;
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
			list = new ArrayList<Integer>();
			list.add(i);
			activeNodes.put(op, list);
			return;
		}

		list.add(i);
	}

	public void addFilter(ArrayList<Filter> filters, Operator op, Operator opParent, Transaction tx) throws Exception
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
				// Transaction t = new Transaction(Transaction.ISOLATION_RR);
				orderedFilters.put(op, new CNFFilter(f, meta, cols2Pos, this));
				// t.commit();
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
		orderedFilters.put(op, new CNFFilter(f, meta, cols2Pos, this));
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
			retval = new TableScanOperator(schema, name, meta, cols2Pos, pos2Col, cols2Types, tablePos2Col, tableCols2Types, tableCols2Pos);
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.error("", e);
			return null;
		}
		if (neededPos != null)
		{
			retval.neededPos = (ArrayList<Integer>)neededPos.clone();
		}
		if (fetchPos != null)
		{
			retval.fetchPos = (ArrayList<Integer>)fetchPos.clone();
		}
		if (midPos2Col != null)
		{
			retval.midPos2Col = midPos2Col.clone();
		}
		if (midCols2Types != null)
		{
			retval.midCols2Types = (HashMap<String, String>)midCols2Types.clone();
		}
		retval.cols2Pos = (HashMap<String, Integer>)cols2Pos.clone();
		retval.pos2Col = (TreeMap<Integer, String>)pos2Col.clone();
		retval.cols2Types = (HashMap<String, String>)cols2Types.clone();
		retval.set = set;
		retval.partMeta = partMeta;
		retval.phase2Done = phase2Done;
		retval.node = node;
		retval.sample = sample;
		retval.sPer = sPer;
		retval.scanIndex = scanIndex;
		retval.tType = tType;
		if (devices != null)
		{
			retval.devices = (ArrayList<Integer>)devices.clone();
		}
		retval.indexOnly = indexOnly;
		if (alias != null && !alias.equals(""))
		{
			retval.setAlias(alias);
		}
		retval.getRID = getRID;
		retval.tx = tx;
		return retval;
	}

	@Override
	public void close() throws Exception
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
				throw e;
			}
		}

		if (readBuffer != null)
		{
			readBuffer.close();
		}

		if (readBuffers != null)
		{
			for (BufferedLinkedBlockingQueue readBuffer : readBuffers.values())
			{
				readBuffer.close();
			}
		}

		if (releaseLocks)
		{
			tx.releaseLocksAndPins();
		}

		ins = null;
		neededPos = null;
		activeDevices = null;
		randomIns = null;
		tablePos2Col = null;
		readBuffers = null;
		fetchPos = null;
		activeNodes = null;
		ins2Device = null;
		tableCols2Pos = null;
		filters = null;
		midPos2Col = null;
		devices = null;
		tableCols2Types = null;
		orderedFilters = null;
		midCols2Types = null;
		device2Child = null;
		cols2Pos = null;
		cols2Types = null;
		pos2Col = null;
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
			if (alias != null && !alias.equals(""))
			{
				if (!alias.equals(((TableScanOperator)rhs).alias))
				{
					return false;
				}
			}
			else
			{
				if (((TableScanOperator)rhs).alias != null && !((TableScanOperator)rhs).alias.equals(""))
				{
					return false;
				}
			}
			return (schema.equals(((TableScanOperator)rhs).schema) && name.equals(((TableScanOperator)rhs).name) && node == ((TableScanOperator)rhs).node);
		}
	}

	public Operator firstParent()
	{
		if (parents.size() > 0)
		{
			return parents.get(0);
		}
		else
		{
			return null;
		}
	}

	public String getAlias()
	{
		return alias;
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

	public ArrayList<Integer> getDevicesMatchingRangeFilters(ArrayList<Filter> rangeFilters) throws Exception
	{
		final ArrayList<Integer> retval = new ArrayList<Integer>();
		ArrayList<Integer> deviceList = null;
		if (partMeta.allDevices())
		{
			final int num = partMeta.getNumDevices();
			deviceList = new ArrayList<Integer>(num);
			int i = 0;
			while (i < num)
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

	public String[] getMidPos2Col()
	{
		return midPos2Col;
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

	public ArrayList<Integer> getNodeGroupsMatchingRangeFilters(ArrayList<Filter> rangeFilters) throws Exception
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

	public ArrayList<Integer> getNodeList()
	{
		return partMeta.nodeSet();
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

	public ArrayList<Integer> getNodesMatchingRangeFilters(ArrayList<Filter> rangeFilters) throws Exception
	{
		final ArrayList<Integer> retval = new ArrayList<Integer>();
		ArrayList<Integer> nodeList = null;
		if (partMeta.allNodes())
		{
			final int num = partMeta.getNumNodes();
			nodeList = new ArrayList<Integer>(num);
			int i = 0;

			if (partMeta.noNodeGroupSet())
			{
				while (i < num)
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

	public void getRID()
	{
		getRID = true;
		cols2Types.put("_RID1", "INT");
		cols2Types.put("_RID2", "INT");
		cols2Types.put("_RID3", "INT");
		cols2Types.put("_RID4", "INT");

		HashMap<String, Integer> newCols2Pos = new HashMap<String, Integer>();
		TreeMap<Integer, String> newPos2Col = new TreeMap<Integer, String>();
		newCols2Pos.put("_RID1", 0);
		newCols2Pos.put("_RID2", 1);
		newCols2Pos.put("_RID3", 2);
		newCols2Pos.put("_RID4", 3);
		newPos2Col.put(0, "_RID1");
		newPos2Col.put(1, "_RID2");
		newPos2Col.put(2, "_RID3");
		newPos2Col.put(3, "_RID4");
		for (Map.Entry entry : cols2Pos.entrySet())
		{
			newCols2Pos.put((String)entry.getKey(), (Integer)entry.getValue() + 4);
			newPos2Col.put((Integer)entry.getValue() + 4, (String)entry.getKey());
		}
		cols2Pos = newCols2Pos;
		pos2Col = newPos2Col;
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

	public HashMap<String, Integer> getTableCols2Pos()
	{
		return tableCols2Pos;
	}

	public int getType()
	{
		return tType;
	}

	@Override
	public int hashCode()
	{
		int hash = 23;
		hash = hash * 31 + schema.hashCode();
		hash = hash * 31 + name.hashCode();
		hash = hash * 31 + node;
		return hash;
		// return schema.hashCode() + name.hashCode();
	}

	public boolean isGetRID()
	{
		return getRID;
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

			if (o instanceof Exception)
			{
				throw (Exception)o;
			}

			return o;
		}
		else
		{
			Object o = readBuffer.take();

			if (o instanceof DataEndMarker)
			{
				demReceived = true;
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
			else
			{
				received.getAndIncrement();
			}

			if (o instanceof Exception)
			{
				throw (Exception)o;
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
	public long numRecsReceived()
	{
		return received.get();
	}

	@Override
	public Operator parent() throws UnsupportedOperationException
	{
		throw new UnsupportedOperationException("TableScanOperator does not support parent()");
	}

	public ArrayList<Operator> parents()
	{
		if (parents.size() == 0)
		{
			ArrayList<Operator> retval = new ArrayList<Operator>();
			{
				retval.add(null);
			}

			return retval;
		}

		return parents;
	}

	public boolean phase2Done()
	{
		return phase2Done;
	}

	public void rebuild()
	{
		opParents = new HashMap<Operator, Operator>();
	}

	@Override
	public boolean receivedDEM()
	{
		return demReceived;
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
	public void reset() throws Exception
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
				throw e;
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
					throw e;
				}
			}

			readBuffer.clear();
			forceDone = false;
			init();
		}
	}

	@Override
	public void serialize(OutputStream out, IdentityHashMap<Object, Long> prev) throws Exception
	{
		Long id = prev.get(this);
		if (id != null)
		{
			OperatorUtils.serializeReference(id, out);
			return;
		}

		OperatorUtils.writeType(78, out);
		prev.put(this, OperatorUtils.writeID(out));
		OperatorUtils.serializeStringHM(cols2Types, out, prev);
		OperatorUtils.serializeStringIntHM(cols2Pos, out, prev);
		OperatorUtils.serializeTM(pos2Col, out, prev);
		OperatorUtils.writeString(name, out, prev);
		OperatorUtils.writeString(schema, out, prev);
		OperatorUtils.serializeALOp(parents, out, prev);
		OperatorUtils.writeBool(startDone, out);
		OperatorUtils.serializeHMOpCNF(orderedFilters, out, prev);
		// recreate meta
		OperatorUtils.serializeALI(neededPos, out, prev);
		OperatorUtils.serializeALI(fetchPos, out, prev);
		OperatorUtils.serializeStringArray(midPos2Col, out, prev);
		OperatorUtils.serializeStringHM(midCols2Types, out, prev);
		OperatorUtils.writeBool(set, out);
		OperatorUtils.serializeALI(devices, out, prev);
		OperatorUtils.writeInt(node, out);
		OperatorUtils.writeBool(phase2Done, out);
		OperatorUtils.serializeHMIntOp(device2Child, out, prev);
		OperatorUtils.serializeALOp(children, out, prev);
		OperatorUtils.writeBool(indexOnly, out);
		OperatorUtils.writeLong(tx.number(), out); // notice type
		OperatorUtils.writeString(alias, out, prev);
		OperatorUtils.writeBool(getRID, out);
		OperatorUtils.serializeStringHM(tableCols2Types, out, prev);
		OperatorUtils.serializeTM(tablePos2Col, out, prev);
		OperatorUtils.serializeStringIntHM(tableCols2Pos, out, prev);
		OperatorUtils.writeBool(releaseLocks, out);
		OperatorUtils.writeBool(sample, out);
		OperatorUtils.writeLong(sPer, out);
		OperatorUtils.serializeIndex(scanIndex, out, prev);
		OperatorUtils.writeInt(tType, out);
	}

	public void setAlias(String alias)
	{
		this.alias = alias;
		TreeMap<Integer, String> newPos2Col = new TreeMap<Integer, String>();
		HashMap<String, Integer> newCols2Pos = new HashMap<String, Integer>();
		HashMap<String, String> newCols2Types = new HashMap<String, String>();
		for (Map.Entry entry : pos2Col.entrySet())
		{
			String val = (String)entry.getValue();
			val = val.substring(val.indexOf('.') + 1);
			newPos2Col.put((Integer)entry.getKey(), alias + "." + val);
			newCols2Pos.put(alias + "." + val, (Integer)entry.getKey());
		}

		for (Map.Entry entry : cols2Types.entrySet())
		{
			String val = (String)entry.getKey();
			val = val.substring(val.indexOf('.') + 1);
			newCols2Types.put(alias + "." + val, (String)entry.getValue());
		}

		pos2Col = newPos2Col;
		cols2Pos = newCols2Pos;
		cols2Types = newCols2Types;
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

	public void setIndexScan(Index scanIndex)
	{
		this.scanIndex = scanIndex;
		scanIndex.setTransaction(new Transaction(0));
	}

	public void setMetaData(Transaction t) throws Exception
	{
		set = true;
		partMeta = meta.getPartMeta(schema, name, t);
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
		midPos2Col = new String[fetchPos.size()];
		midCols2Types = new HashMap<String, String>();
		for (final int pos : fetchPos)
		{
			fetchCols2Pos.put(pos2Col.get(pos), i);
			midPos2Col[i] = pos2Col.get(pos);
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

	@Override
	public void setPlan(Plan plan)
	{
	}

	public void setSample(long sPer)
	{
		sample = true;
		this.sPer = sPer;
	}

	public void setTransaction(Transaction tx)
	{
		this.tx = tx;
	}

	public void setType(int type)
	{
		tType = type;
	}

	@Override
	public synchronized void start() throws Exception
	{
		// HRDBMSWorker.logger.debug("Starting " + TableScanOperator.this);
		ins = new ArrayList<String>();
		readBuffers = new HashMap<Operator, BufferedLinkedBlockingQueue>();
		optimize = false;
		randomIns = new ArrayList<String>();
		ins2Device = new HashMap<String, Integer>();
		forceDone = false;

		if (!startDone)
		{
			// HRDBMSWorker.logger.debug(TableScanOperator.this +
			// " did need to be started");
			startDone = true;

			// HRDBMSWorker.logger.debug(TableScanOperator.this + " had " +
			// children.size() + " children to start");
			for (final Operator o : children)
			{
				try
				{
					o.start();
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.error("", e);
					throw e;
				}
			}

			if (devices.size() == 0)
			{
				final String in = schema + "." + name + ".tbl";
				ins.add(in);
				// HRDBMSWorker.logger.debug(TableScanOperator.this +
				// " had 0 devices");
			}
			else
			{
				// HRDBMSWorker.logger.debug(TableScanOperator.this + " had " +
				// devices.size() + " devices");
				for (final int device : devices)
				{
					if (children.size() == 0)
					{
						final String in = meta.getDevicePath(device) + schema + "." + name + ".tbl";
						ins.add(in);
					}
					else
					{
						final String in = meta.getDevicePath(device) + schema + "." + name + ".tbl";
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
				// HRDBMSWorker.logger.debug("WARNING: " +
				// TableScanOperator.this + " had " + parents.size() +
				// " parents");
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
		String table2 = null;
		if (alias == null || alias.equals(""))
		{
			table2 = name;
		}
		else
		{
			table2 = alias;
		}
		String retval = "TableScanOperator(" + node + ":" + devices + "): " + schema + "." + table2;
		for (final Map.Entry entry : orderedFilters.entrySet())
		{
			if (entry.getValue() != null)
			{
				retval += (", (" + entry.getValue().toString()) + ")";
			}
		}
		
		retval += (" : " + cols2Pos);

		return retval;
	}

	private boolean canAnythingInRangeSatisfyFilters(ArrayList<Filter> filters, Object lowLE, Object highLE) throws Exception
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
				lowLE = new MyDate(Integer.MIN_VALUE);
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
				highLE = new MyDate(Integer.MAX_VALUE);
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
				throw e;
			}
		}

		return true;
	}

	private void init()
	{
		final InitThread t = new InitThread();
		t.start();
	}

	private boolean needThisColForFilter(String col, CNFFilter filter)
	{
		if (referencesHash == null)
		{
			HashSet<Integer> temp = new HashSet<Integer>();
			for (String s : filter.getReferencesHash())
			{
				temp.add(s.hashCode());
			}

			referencesHash = temp;
		}

		return referencesHash.contains(col.hashCode());
	}

	public final class ReaderThread extends ThreadPoolThread
	{
		private String in;
		private String in2;
		private Index scan;
		int myMaxBlock = 0;
		int start = 0;

		public ReaderThread(String in)
		{
			this.in = in;
		}

		public ReaderThread(String in2, boolean marker)
		{
			this.in2 = in2;
		}

		public ReaderThread(String in, Index scan)
		{
			this.in = in;
			this.scan = scan;
		}

		public ReaderThread(String in, int start, int max)
		{
			this.in = in;
			this.start = start;
			this.myMaxBlock = max;
		}

		public void colTableRT()
		{
			new ArrayList<ReaderThread>();
			CNFFilter filter = orderedFilters.get(parents.get(0));
			boolean neededPosNeeded = true;
			int get = 0;
			int skip = 0;
			boolean checkNoResults = (filter != null && !(filter instanceof NullCNFFilter) && !sample);
			HashSet<HashMap<Filter, Filter>> hshm = null;
			if (checkNoResults)
			{
				hshm = filter.getHSHM();
				if (hshm == null)
				{
					checkNoResults = false;
				}
			}

			if (sample)
			{
				get = (int)sPer;
				skip = 100 - get;
			}

			if (scan != null)
			{
				try
				{
					Index index = new Index(in, scan.getKeys(), scan.getTypes(), scan.getOrders());
					index.open();
					index.scan(filter, sample, get, skip, readBuffer, midPos2Col, pos2Col, tx);
					return;
				}
				catch (Exception e)
				{
					HRDBMSWorker.logger.error("", e);
					try
					{
						readBuffer.put(e);
					}
					catch (Exception f)
					{
					}
					return;
				}
			}

			if (filter != null)
			{
				if (neededPos.size() == fetchPos.size())
				{
					int i = 0;
					neededPosNeeded = false;
					for (int pos : neededPos)
					{
						if (pos != i)
						{
							neededPosNeeded = true;
							break;
						}

						i++;
					}
				}
			}

			ArrayList<String> types = new ArrayList<String>(midPos2Col.length);
			for (final String entry : midPos2Col)
			{
				types.add(midCols2Types.get(entry));
			}

			ArrayList<Integer> cols = new ArrayList<Integer>(fetchPos.size());
			HashMap<Integer, Integer> rowToIterator = new HashMap<Integer, Integer>();

			if (!getRID)
			{
				int w = 0;
				while (w < fetchPos.size())
				{
					cols.add(fetchPos.get(w++));
				}

				Collections.sort(cols);
				int pos = 0;
				for (int col : cols)
				{
					int index = fetchPos.indexOf(col);
					rowToIterator.put(index, pos);
					pos++;
				}
			}
			else
			{
				int w = 0;
				while (w < fetchPos.size())
				{
					int col = fetchPos.get(w++);
					if (col >= 4)
					{
						cols.add(col - 4);
					}
				}

				Collections.sort(cols);
				int pos = 0;
				for (int col : cols)
				{
					int index = fetchPos.indexOf(col + 4);
					rowToIterator.put(index, pos);
					pos++;
				}
			}

			try
			{
				if (in2 == null)
				{
					LockManager.sLock(new Block(in, -1), tx.number());
					Integer numBlocks = FileManager.numBlocks.get(in);
					if (numBlocks == null)
					{
						FileManager.getFile(in);
						numBlocks = FileManager.numBlocks.get(in);
					}

					if (numBlocks == 0)
					{
						throw new Exception("Unable to open file " + in);
					}

					HashMap<Integer, DataType> layout = new HashMap<Integer, DataType>();
					for (Map.Entry entry : tablePos2Col.entrySet())
					{
						String type = tableCols2Types.get(entry.getValue());
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

					int onPage = 1;
					BufferManager.registerInterest(this, in, onPage, numBlocks - 1);

					int lastRequested = onPage - 1;
					// long count = 0;
					ArrayList<Object> row = new ArrayList<Object>(fetchPos.size());
					int get2 = get;
					int skip2 = skip;
					int get3 = get;
					int skip3 = skip;

					PREFETCH_REQUEST_SIZE = PREFETCH_REQUEST_SIZE_STATIC * layout.size() / cols.size();
					if (PREFETCH_REQUEST_SIZE < layout.size() * 2)
					{
						PREFETCH_REQUEST_SIZE = layout.size() * 2;
					}
					int MAX_PAGES_IN_ADVANCE = PREFETCH_REQUEST_SIZE * 2;

					RequestPagesThread raThread = null;

					while (onPage < numBlocks)
					{
						if (lastRequested - onPage < MAX_PAGES_IN_ADVANCE)
						{
							if (raThread != null)
							{
								raThread.join();
							}

							BufferManager.updateProgress(this, onPage);
							
							if (!sample)
							{
								// Block[] toRequest = new Block[lastRequested +
								// PREFETCH_REQUEST_SIZE < numBlocks ?
								// PREFETCH_REQUEST_SIZE : numBlocks -
								// lastRequested - 1];
								ArrayList<Block> toRequest = new ArrayList<Block>();
								int i = 0;
								final int length = lastRequested + PREFETCH_REQUEST_SIZE < numBlocks ? PREFETCH_REQUEST_SIZE : numBlocks - lastRequested - 1;
								while (i < length)
								{
									if ((lastRequested + i + 1) % layout.size() == 1)
									{
										Block block = new Block(in, lastRequested + i + 1);
										if (hshm != null)
										{
											Set<HashSet<HashMap<Filter, Filter>>> filters = noResults.get(block);
											if (filter != null && filters.contains(hshm))
											{
												i++;
												continue;
											}
										}

										toRequest.add(block);
									}

									i++;
								}

								if (toRequest.size() > 0)
								{
									Block[] toRequest2 = toRequest.toArray(new Block[toRequest.size()]);
									raThread = tx.requestPages(toRequest2, cols, layout.size());
								}

								lastRequested += length;
							}
							else
							{
								ArrayList<Block> toRequest = new ArrayList<Block>();
								int i = 0;
								int length = lastRequested + PREFETCH_REQUEST_SIZE < numBlocks ? PREFETCH_REQUEST_SIZE : numBlocks - lastRequested - 1;
								while (i < length)
								{
									if ((lastRequested + i + 1) % layout.size() == 1)
									{
										if (skip3 == 0)
										{
											get3 = get - 1;
											skip3 = skip;
										}
										else if (get3 == 0)
										{
											skip3--;
											i += layout.size();
											continue;
										}
										else
										{
											get3--;
										}

										toRequest.add(new Block(in, lastRequested + i + 1));
										i += layout.size();
									}
									else
									{
										i++;
									}
								}

								if (toRequest.size() > 0)
								{
									Block[] toRequest2 = new Block[toRequest.size()];
									int j = 0;
									int z = 0;
									final int limit = toRequest.size();
									// for (Block b : toRequest)
									while (z < limit)
									{
										Block b = toRequest.get(z++);
										toRequest2[j] = b;
										j++;
									}

									raThread = tx.requestPages(toRequest2, cols, layout.size());
								}

								lastRequested += length;
							}
						}

						if (sample && skip2 == 0)
						{
							get2 = get - 1;
							skip2 = skip;
						}
						else if (sample && get2 == 0)
						{
							skip2--;
							onPage += layout.size();
							continue;
						}
						else if (sample)
						{
							get2--;
						}

						Block thisBlock = new Block(in, onPage);
						if (hshm != null)
						{
							Set<HashSet<HashMap<Filter, Filter>>> filters = noResults.get(thisBlock);
							if (filter != null && filters.contains(hshm))
							{
								skippedPages.getAndIncrement();
								onPage += layout.size();
								continue;
							}
						}

						tx.read(new Block(in, onPage), sch, cols, true);
						Iterator rit = null;
						if (!getRID)
						{
							onPage += layout.size();
							rit = sch.colTableIterator();
						}
						else
						{
							onPage += layout.size();
							rit = sch.colTableIteratorWithRIDs();
						}

						boolean hadResults = false;
						outer: while (rit.hasNext())
						{
							Object o = rit.next();
							ArrayList<FieldValue> r = null;
							RID rid = null;
							if (getRID)
							{
								Map.Entry entry = (Map.Entry)o;
								rid = (RID)entry.getKey();
								r = (ArrayList<FieldValue>)entry.getValue();
							}
							else
							{
								r = (ArrayList<FieldValue>)o;
							}

							int j = 0;
							int size = 0;
							try
							{
								size = fetchPos.size();
							}
							catch(Exception e)
							{
								if (forceDone)
								{
									checkNoResults = false;
									BufferManager.unregisterInterest(this);
									readBuffer.put(new DataEndMarker());
									return;
								}
								else
								{
									throw e;
								}
							}
							row.clear();
							while (j < size)
							{
								if (!getRID)
								{
									row.add(r.get(rowToIterator.get(j)).getValue());
								}
								else
								{
									if (j >= 4)
									{

										row.add(r.get(rowToIterator.get(j)).getValue());
									}
									else if (j == 0)
									{
										row.add(rid.getNode());
									}
									else if (j == 1)
									{
										row.add(rid.getDevice());
									}
									else if (j == 2)
									{
										row.add(rid.getBlockNum());
									}
									else if (j == 3)
									{
										row.add(rid.getRecNum());
									}
								}
								j++;
							}

							{
								if (filter != null)
								{
									if (filter.passes(row))
									{
										hadResults = true;

										if (neededPosNeeded)
										{
											final ArrayList<Object> newRow = new ArrayList<Object>(neededPos.size());
											int z = 0;
											final int limit = neededPos.size();
											// for (final int pos : neededPos)
											while (z < limit)
											{
												newRow.add(row.get(neededPos.get(z++)));
											}
											if (!forceDone)
											{
												readBuffer.put(newRow);
											}
											else
											{
												checkNoResults = false;
												BufferManager.unregisterInterest(this);
												readBuffer.put(new DataEndMarker());
												return;
											}
										}
										else
										{
											if (!forceDone)
											{
												readBuffer.put(row);
												row = new ArrayList<Object>(fetchPos.size());
											}
											else
											{
												checkNoResults = false;
												BufferManager.unregisterInterest(this);
												readBuffer.put(new DataEndMarker());
												return;
											}
										}
									}
								}
								else
								{
									hadResults = true;
									// final ArrayList<Object> newRow = new
									// ArrayList<Object>(neededPos.size());
									// for (final int pos : neededPos)
									// {
									// newRow.add(row.get(pos));
									// }
									if (!forceDone)
									{
										readBuffer.put(row);
										row = new ArrayList<Object>(fetchPos.size());
									}
									else
									{
										checkNoResults = false;
										BufferManager.unregisterInterest(this);
										readBuffer.put(new DataEndMarker());
										return;
									}
								}
							}
						}

						if (checkNoResults && !hadResults)
						{
							noResults.multiPut(thisBlock, hshm);
						}
					}

					BufferManager.unregisterInterest(this);
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
						readBuffer.put(e);
						return;
					}
					// @?Parallel
					int device = ins2Device.get(in2);
					int currentPage = -1;
					HashMap<Integer, DataType> layout = new HashMap<Integer, DataType>();
					for (Map.Entry entry : tablePos2Col.entrySet())
					{
						String type = tableCols2Types.get(entry.getValue());
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
					// long count = 0;
					ArrayList<Object> row = new ArrayList<Object>(fetchPos.size());
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
								tx.requestPage(b, cols);
								tx.read(b, sch, cols, false);
							}
							int node2 = node;
							if (node2 < 0)
							{
								node2 = -1;
							}
							final ArrayList<FieldValue> r = sch.getRowForColTable(new RID(node2, device, blockNum, recNum));
							row.clear();

							int j = 0;
							final int size = fetchPos.size();
							while (j < size)
							{
								try
								{
									if (!getRID)
									{
										row.add(r.get(rowToIterator.get(j)).getValue());
									}
									else
									{
										if (j >= 4)
										{
											row.add(r.get(rowToIterator.get(j)).getValue());
										}
										else if (j == 0)
										{
											row.add(node2);
										}
										else if (j == 1)
										{
											row.add(device);
										}
										else if (j == 2)
										{
											row.add(blockNum);
										}
										else if (j == 3)
										{
											row.add(recNum);
										}
									}
									j++;
								}
								catch (Exception e)
								{
									HRDBMSWorker.logger.debug("", e);
									throw e;
								}
							}

							if (!optimize)
							{
							}
							else
							{
								if (filter != null)
								{
									if (filter.passes(row))
									{
										if (neededPosNeeded)
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
										else
										{
											if (!forceDone)
											{
												readBuffer.put(row);
												row = new ArrayList<Object>(fetchPos.size());
											}
											else
											{
												readBuffer.put(new DataEndMarker());
												return;
											}
										}
									}
								}
								else
								{
									if (!forceDone)
									{
										readBuffer.put(row);
										row = new ArrayList<Object>(fetchPos.size());
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

							if (filter != null)
							{
								filter.updateCols2Pos(child.getCols2Pos());
								if (!filter.passes((ArrayList<Object>)o))
								{
									o = child.next(TableScanOperator.this);
									continue;
								}
							}

							final ArrayList<Object> row2 = new ArrayList<Object>(pos2Col.size());
							for (final String col : pos2Col.values())
							{
								row2.add(((ArrayList<Object>)o).get(child.getCols2Pos().get(col)));
							}

							if (!forceDone)
							{
								readBuffer.put(row2);
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
				try
				{
					readBuffer.put(e);
				}
				catch (Exception f)
				{
				}
				return;
			}
		}

		public void rowTableRT()
		{
			ArrayList<ReaderThread> secondThreads = new ArrayList<ReaderThread>();
			CNFFilter filter = orderedFilters.get(parents.get(0));
			boolean neededPosNeeded = true;
			int get = 0;
			int skip = 0;
			boolean checkNoResults = (filter != null && !(filter instanceof NullCNFFilter) && !sample);
			HashSet<HashMap<Filter, Filter>> hshm = null;
			if (checkNoResults)
			{
				hshm = filter.getHSHM();
				if (hshm == null)
				{
					checkNoResults = false;
				}
			}

			if (sample)
			{
				get = (int)sPer;
				skip = 100 - get;
			}

			if (scan != null)
			{
				try
				{
					Index index = new Index(in, scan.getKeys(), scan.getTypes(), scan.getOrders());
					index.open();
					index.scan(filter, sample, get, skip, readBuffer, midPos2Col, pos2Col, tx);
					return;
				}
				catch (Exception e)
				{
					HRDBMSWorker.logger.error("", e);
					try
					{
						readBuffer.put(e);
					}
					catch (Exception f)
					{
					}
					return;
				}
			}

			if (filter != null)
			{
				if (neededPos.size() == fetchPos.size())
				{
					int i = 0;
					neededPosNeeded = false;
					for (int pos : neededPos)
					{
						if (pos != i)
						{
							neededPosNeeded = true;
							break;
						}

						i++;
					}
				}
			}
			ArrayList<String> types = new ArrayList<String>(midPos2Col.length);
			for (final String entry : midPos2Col)
			{
				types.add(midCols2Types.get(entry));
			}

			try
			{
				if (in2 == null)
				{
					LockManager.sLock(new Block(in, -1), tx.number());
					// FileManager.getFile(in);
					// HRDBMSWorker.logger.debug("Opened " + in + " for " +
					// TableScanOperator.this);
					Integer numBlocks = FileManager.numBlocks.get(in);
					if (numBlocks == null)
					{
						FileManager.getFile(in);
						numBlocks = FileManager.numBlocks.get(in);
					}
					/*
					 * if (numBlocks > 5000 && myMaxBlock == 0) { int numThreads
					 * = 2; int blocksPerThread = (int)((numBlocks-4000) * 1.0 /
					 * numThreads); myMaxBlock = blocksPerThread + 4000;
					 * ReaderThread thread = new ReaderThread(in, myMaxBlock,
					 * numBlocks); thread.start(); secondThreads.add(thread); }
					 */
					// HRDBMSWorker.logger.debug(in + " has " + numBlocks +
					// " blocks");
					if (numBlocks == 0)
					{
						// HRDBMSWorker.logger.debug("Unable to open file " +
						// in);
						throw new Exception("Unable to open file " + in);
					}
					HashMap<Integer, DataType> layout = new HashMap<Integer, DataType>();
					for (Map.Entry entry : tablePos2Col.entrySet())
					{
						String type = tableCols2Types.get(entry.getValue());
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

					PREFETCH_REQUEST_SIZE = PREFETCH_REQUEST_SIZE_STATIC;
					PAGES_IN_ADVANCE = PAGES_IN_ADVANCE_STATIC;
					Schema[] schemas = new Schema[PREFETCH_REQUEST_SIZE * 4];
					int g = 0;
					while (g < schemas.length)
					{
						schemas[g++] = new Schema(layout);
					}
					final ConcurrentHashMap<Integer, Schema> schemaMap = new ConcurrentHashMap<Integer, Schema>();
					long schemaIndex = 0;

					int onPage = 1;
					if (myMaxBlock != 0)
					{
						numBlocks = myMaxBlock;
					}

					if (start != 0)
					{
						onPage = start;
					}

					BufferManager.registerInterest(this, in, onPage, numBlocks - 1);

					int lastRequested = onPage - 1;
					// long count = 0;
					ArrayList<Object> row = new ArrayList<Object>(fetchPos.size());
					int get2 = get;
					int skip2 = skip;
					int get3 = get;
					int skip3 = skip;

					while (onPage < numBlocks)
					{
						if (lastRequested - onPage < PAGES_IN_ADVANCE)
						{
							BufferManager.updateProgress(this, onPage);
							if (!sample)
							{
								// Block[] toRequest = new Block[lastRequested +
								// PREFETCH_REQUEST_SIZE < numBlocks ?
								// PREFETCH_REQUEST_SIZE : numBlocks -
								// lastRequested - 1];
								ArrayList<Block> toRequest = new ArrayList<Block>();
								int i = 0;
								final int length = lastRequested + PREFETCH_REQUEST_SIZE < numBlocks ? PREFETCH_REQUEST_SIZE : numBlocks - lastRequested - 1;
								while (i < length)
								{
									Block block = new Block(in, lastRequested + i + 1);
									if (hshm != null)
									{
										Set<HashSet<HashMap<Filter, Filter>>> filters = noResults.get(block);
										if (filter != null && filters.contains(hshm))
										{
											i++;
											continue;
										}
									}
									toRequest.add(block);
									i++;
								}

								Block[] toRequest2 = new Block[toRequest.size()];
								i = 0;
								while (i < toRequest2.length)
								{
									toRequest2[i] = toRequest.get(i);
									i++;
								}

								if (!getRID)
								{
									tx.requestPages(toRequest2, schemas, (int)(schemaIndex % (PREFETCH_REQUEST_SIZE * 4)), schemaMap, fetchPos);
									schemaIndex += PREFETCH_REQUEST_SIZE;
								}
								else
								{
									tx.requestPages(toRequest2);
								}
								lastRequested += length;
							}
							else
							{
								ArrayList<Block> toRequest = new ArrayList<Block>();
								int i = 0;
								int length = lastRequested + PREFETCH_REQUEST_SIZE < numBlocks ? PREFETCH_REQUEST_SIZE : numBlocks - lastRequested - 1;
								while (i < length)
								{
									if (skip3 == 0)
									{
										get3 = get - 1;
										skip3 = skip;
									}
									else if (get3 == 0)
									{
										skip3--;
										i++;
										continue;
									}
									else
									{
										get3--;
									}

									toRequest.add(new Block(in, lastRequested + i + 1));
									i++;
								}

								if (toRequest.size() > 0)
								{
									Block[] toRequest2 = new Block[toRequest.size()];
									int j = 0;
									int z = 0;
									final int limit = toRequest.size();
									// for (Block b : toRequest)
									while (z < limit)
									{
										Block b = toRequest.get(z++);
										toRequest2[j] = b;
										j++;
									}

									if (!getRID)
									{
										tx.requestPages(toRequest2, schemas, (int)(schemaIndex % (PREFETCH_REQUEST_SIZE * 4)), schemaMap, fetchPos);
										schemaIndex += PREFETCH_REQUEST_SIZE;
									}
									else
									{
										tx.requestPages(toRequest2);
									}
								}

								lastRequested += length;
							}
						}

						if (sample && skip2 == 0)
						{
							get2 = get - 1;
							skip2 = skip;
						}
						else if (sample && get2 == 0)
						{
							skip2--;
							onPage++;
							continue;
						}
						else if (sample)
						{
							get2--;
						}

						Block thisBlock = new Block(in, onPage);
						if (hshm != null)
						{
							Set<HashSet<HashMap<Filter, Filter>>> filters = noResults.get(thisBlock);
							if (filter != null && filters.contains(hshm))
							{
								skippedPages.getAndIncrement();
								onPage++;
								continue;
							}
						}

						// tx.read(new Block(in, onPage++), sch);
						Schema sch = null;
						RowIterator rit = null;
						if (!getRID)
						{
							sch = schemaMap.get(onPage);
							while (sch == null)
							{
								LockSupport.parkNanos(500);
								sch = schemaMap.get(onPage);
							}

							schemaMap.remove(onPage);
							onPage++;

							synchronized (sch)
							{
								rit = sch.rowIterator(true);
							}
						}
						else
						{
							sch = schemas[0];
							tx.read(new Block(in, onPage++), sch);
							rit = sch.rowIterator(false);
						}

						boolean hadResults = false;
						outer: while (rit.hasNext())
						{
							Row r = rit.next();
							RID rid = null;
							if (getRID)
							{
								rid = r.getRID();
							}
							int j = 0;
							boolean checked = false;
							final int size = fetchPos.size();
							row.clear();
							while (j < size)
							{
								if (!getRID)
								{
									if (filter != null && !needThisColForFilter(midPos2Col[j], filter))
									{
										row.add(null);
									}
									else
									{
										FieldValue fv = r.getCol(fetchPos.get(j));
										if (!checked)
										{
											if (!fv.exists())
											{
												continue outer;
											}

											checked = true;
										}
										row.add(fv.getValue());
									}
								}
								else
								{
									int colNum = fetchPos.get(j);
									if (colNum >= 4)
									{

										FieldValue fv = r.getCol(colNum - 4);
										if (!checked)
										{
											if (!fv.exists())
											{
												continue outer;
											}

											checked = true;
										}
										row.add(fv.getValue());
									}
									else if (colNum == 0)
									{
										row.add(rid.getNode());
									}
									else if (colNum == 1)
									{
										row.add(rid.getDevice());
									}
									else if (colNum == 2)
									{
										row.add(rid.getBlockNum());
									}
									else if (colNum == 3)
									{
										row.add(rid.getRecNum());
									}
								}
								j++;
							}

							// if (!optimize)
							// {
							// for (final Map.Entry entry :
							// readBuffers.entrySet())
							// {
							// final BufferedLinkedBlockingQueue q =
							// (BufferedLinkedBlockingQueue)entry.getValue();
							// filter = orderedFilters.get(entry.getKey());

							// if (filter != null)
							// {
							// if (filter.passes(row))
							// {
							// final ArrayList<Object> newRow = new
							// ArrayList<Object>(neededPos.size());
							// for (final int pos : neededPos)
							// {
							// newRow.add(row.get(pos));
							// }
							// q.put(newRow);
							// }
							// }
							// else
							// {
							// final ArrayList<Object> newRow = new
							// ArrayList<Object>(neededPos.size());
							// for (final int pos : neededPos)
							// {
							// newRow.add(row.get(pos));
							// }
							// q.put(newRow);
							// }
							// }
							// }
							// else
							{
								if (filter != null)
								{
									if (filter.passes(row))
									{
										hadResults = true;
										if (!getRID)
										{
											int i = 0;
											int z = 0;
											final int limit = row.size();
											// for (Object o : row)
											while (z < limit)
											{
												Object o = row.get(z++);
												if (o == null)
												{
													int temp = fetchPos.get(i);
													FieldValue fv = r.getCol(temp);
													if (!fv.exists())
													{
														continue outer;
													}
													// row.set(i,
													// fv.getValue());
													Object[] array = (Object[])unsafe.getObject(row, offset);
													array[i] = fv.getValue();
												}

												i++;
											}
										}

										if (neededPosNeeded)
										{
											final ArrayList<Object> newRow = new ArrayList<Object>(neededPos.size());
											int z = 0;
											final int limit = neededPos.size();
											// for (final int pos : neededPos)
											while (z < limit)
											{
												newRow.add(row.get(neededPos.get(z++)));
											}
											if (!forceDone)
											{
												readBuffer.put(newRow);
											}
											else
											{
												checkNoResults = false;
												BufferManager.unregisterInterest(this);
												readBuffer.put(new DataEndMarker());
												return;
											}
										}
										else
										{
											if (!forceDone)
											{
												readBuffer.put(row);
												row = new ArrayList<Object>(fetchPos.size());
											}
											else
											{
												checkNoResults = false;
												BufferManager.unregisterInterest(this);
												readBuffer.put(new DataEndMarker());
												return;
											}
										}
									}
								}
								else
								{
									hadResults = true;
									// final ArrayList<Object> newRow = new
									// ArrayList<Object>(neededPos.size());
									// for (final int pos : neededPos)
									// {
									// newRow.add(row.get(pos));
									// }
									if (!forceDone)
									{
										readBuffer.put(row);
										row = new ArrayList<Object>(fetchPos.size());
									}
									else
									{
										checkNoResults = false;
										BufferManager.unregisterInterest(this);
										readBuffer.put(new DataEndMarker());
										return;
									}
								}
							}
						}

						if (checkNoResults && !hadResults)
						{
							noResults.multiPut(thisBlock, hshm);
						}
					}

					BufferManager.unregisterInterest(this);

					if (start == 0 && myMaxBlock != 0)
					{
						for (ReaderThread thread : secondThreads)
						{
							thread.join();
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
						readBuffer.put(e);
						return;
					}
					// @?Parallel
					int device = ins2Device.get(in2);
					int currentPage = -1;
					HashMap<Integer, DataType> layout = new HashMap<Integer, DataType>();
					for (Map.Entry entry : tablePos2Col.entrySet())
					{
						String type = tableCols2Types.get(entry.getValue());
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
					// long count = 0;
					ArrayList<Object> row = new ArrayList<Object>(fetchPos.size());
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
								// count++;

								// if (count > MAX_PAGES)
								// {
								// tx.checkpoint(in2, b);
								// count = 1;
								// }
							}
							int node2 = node;
							if (node2 < 0)
							{
								node2 = -1;
							}
							final Row r = sch.getRow(new RID(node2, device, blockNum, recNum));
							row.clear();

							int j = 0;
							final int size = fetchPos.size();
							while (j < size)
							{
								try
								{
									if (!getRID)
									{
										if (filter != null && !needThisColForFilter(midPos2Col[j], filter))
										{
											row.add(null);
										}
										else
										{
											FieldValue fv = r.getCol(fetchPos.get(j));
											row.add(fv.getValue());
										}
									}
									else
									{
										int colNum = fetchPos.get(j);
										if (colNum >= 4)
										{
											FieldValue fv = r.getCol(colNum - 4);
											row.add(fv.getValue());
										}
										else if (colNum == 0)
										{
											row.add(node2);
										}
										else if (colNum == 1)
										{
											row.add(device);
										}
										else if (colNum == 2)
										{
											row.add(blockNum);
										}
										else if (colNum == 3)
										{
											row.add(recNum);
										}
									}
									j++;
								}
								catch (Exception e)
								{
									HRDBMSWorker.logger.debug("", e);
									throw e;
								}
							}

							if (!optimize)
							{
								// for (final Map.Entry entry :
								// readBuffers.entrySet())
								// {
								// final BufferedLinkedBlockingQueue q =
								// (BufferedLinkedBlockingQueue)entry.getValue();
								// filter = orderedFilters.get(entry.getKey());

								// if (filter != null)
								// {
								// if (filter.passes(row))
								// {
								// final ArrayList<Object> newRow = new
								// ArrayList<Object>(neededPos.size());
								// for (final int pos : neededPos)
								// {
								// newRow.add(row.get(pos));
								// }
								// q.put(newRow);
								// }
								// }
								// else
								// {
								// final ArrayList<Object> newRow = new
								// ArrayList<Object>(neededPos.size());
								// for (final int pos : neededPos)
								// {
								// newRow.add(row.get(pos));
								// }
								// q.put(newRow);
								// }
								// }
							}
							else
							{
								if (filter != null)
								{
									if (filter.passes(row))
									{
										if (!getRID)
										{
											int i = 0;
											int z = 0;
											final int limit = row.size();
											// for (Object o2 : row)
											while (z < limit)
											{
												Object o2 = row.get(z++);
												if (o2 == null)
												{
													FieldValue fv = r.getCol(fetchPos.get(i));
													// row.set(i,
													// fv.getValue());
													Object[] array = (Object[])unsafe.getObject(row, offset);
													array[i] = fv.getValue();
												}

												i++;
											}
										}

										if (neededPosNeeded)
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
										else
										{
											if (!forceDone)
											{
												readBuffer.put(row);
												row = new ArrayList<Object>(fetchPos.size());
											}
											else
											{
												readBuffer.put(new DataEndMarker());
												return;
											}
										}
									}
								}
								else
								{
									// final ArrayList<Object> newRow = new
									// ArrayList<Object>(neededPos.size());
									// for (final int pos : neededPos)
									// {
									// newRow.add(row.get(pos));
									// }
									if (!forceDone)
									{
										readBuffer.put(row);
										row = new ArrayList<Object>(fetchPos.size());
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

							if (filter != null)
							{
								filter.updateCols2Pos(child.getCols2Pos());
								if (!filter.passes((ArrayList<Object>)o))
								{
									o = child.next(TableScanOperator.this);
									continue;
								}
							}

							final ArrayList<Object> row2 = new ArrayList<Object>(pos2Col.size());
							for (final String col : pos2Col.values())
							{
								row2.add(((ArrayList<Object>)o).get(child.getCols2Pos().get(col)));
							}

							if (!forceDone)
							{
								readBuffer.put(row2);
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
				try
				{
					readBuffer.put(e);
				}
				catch (Exception f)
				{
				}
				return;
			}
		}

		@Override
		public final void run()
		{
			tsoCount.incrementAndGet();
			
			try
			{
				if (tType == 0)
				{
					rowTableRT();
				}
				else
				{
					colTableRT();
				}
			}
			catch(Exception e)
			{
				HRDBMSWorker.logger.debug("", e);
			}
			
			tsoCount.decrementAndGet();
		}
	}

	private final class InitThread extends ThreadPoolThread
	{
		private final ArrayList<ReaderThread> reads = new ArrayList<ReaderThread>(ins.size());

		@Override
		public void run()
		{
			try
			{
				// HRDBMSWorker.logger.debug("Going to start " + ins.size() +
				// " ReaderThreads for ins for " + TableScanOperator.this);
				if (scanIndex != null)
				{
					String fn = scanIndex.getFileName();
					for (final int device : devices)
					{
						final String in = meta.getDevicePath(device) + fn;
						ReaderThread read = new ReaderThread(in, scanIndex);
						read.start();
						reads.add(read);
					}
				}
				else
				{
					for (final String in : ins)
					{
						final ReaderThread read = new ReaderThread(in);
						read.start();
						reads.add(read);
					}

					// HRDBMSWorker.logger.debug("Going to start " +
					// randomIns.size() + " ReaderThreads for randomIns for " +
					// TableScanOperator.this);
					for (final String in : randomIns)
					{
						final ReaderThread read = new ReaderThread(in, true);
						read.start();
						reads.add(read);
					}
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
				try
				{
					readBuffer.put(e);
				}
				catch (Exception f)
				{
				}
				return;
			}
		}
	}
}
