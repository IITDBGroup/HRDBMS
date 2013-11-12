package com.exascale.optimizer.testing;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import com.exascale.optimizer.testing.MetaData.PartitionMetaData;

public class TableScanOperator implements Operator, Serializable
{
	private HashMap<String, String> cols2Types = new HashMap<String, String>();
	private HashMap<String, Integer> cols2Pos = new HashMap<String, Integer>();
	private TreeMap<Integer, String> pos2Col = new TreeMap<Integer, String>();
	private String name;
	private String schema;
	private ArrayList<BufferedReader> ins = new ArrayList<BufferedReader>();
	private Vector<Operator> parents = new Vector<Operator>();
	public volatile LinkedBlockingQueue readBuffer = new LinkedBlockingQueue(Driver.QUEUE_SIZE);
	private volatile HashMap<Operator, LinkedBlockingQueue> readBuffers = new HashMap<Operator, LinkedBlockingQueue>();
	private boolean startDone = false;
	private boolean closeDone = false;
	private boolean optimize = false;
	public volatile LinkedBlockingQueue queue = new LinkedBlockingQueue(Driver.QUEUE_SIZE);
	private final int NUM_PTHREADS = Runtime.getRuntime().availableProcessors();
	private HashMap<Operator, HashSet<HashMap<Filter, Filter>>> filters = new HashMap<Operator, HashSet<HashMap<Filter, Filter>>>();
	private HashMap<Operator, CNFFilter> orderedFilters = new HashMap<Operator, CNFFilter>();
	private MetaData meta;
	private HashMap<Operator, Operator> opParents = new HashMap<Operator, Operator>();
	private ArrayList<Integer> neededPos;
	private ArrayList<Integer> fetchPos;
	private TreeMap<Integer, String> midPos2Col;
	private HashMap<String, String> midCols2Types;
	private boolean set = false;
	private PartitionMetaData partMeta;
	private HashMap<Operator, ArrayList<Integer>> activeDevices = new HashMap<Operator, ArrayList<Integer>>();
	private HashMap<Operator, ArrayList<Integer>> activeNodes = new HashMap<Operator, ArrayList<Integer>>();
	private ArrayList<Integer> devices = new ArrayList<Integer>();
	private int node;
	private boolean phase2Done = false;
	
	public TableScanOperator clone()
	{
		TableScanOperator retval = null;
		try
		{
			retval = new TableScanOperator(schema, name, meta);
		}
		catch(Exception e)
		{
			e.printStackTrace();
			System.exit(1);
		}
		retval.neededPos = (ArrayList<Integer>)neededPos.clone();
		retval.fetchPos = (ArrayList<Integer>)fetchPos.clone();
		retval.midPos2Col = (TreeMap<Integer, String>)midPos2Col.clone();
		retval.midCols2Types = (HashMap<String, String>)midCols2Types.clone();
		retval.set = set;
		retval.partMeta = partMeta;
		retval.phase2Done = phase2Done;
		return retval;
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
	
	public void setCNFForParent(Operator op, CNFFilter filter)
	{
		orderedFilters.put(op, filter);
	}
	
	public boolean phase2Done()
	{
		return phase2Done;
	}
	
	public void setPhase2Done()
	{
		phase2Done = true;
	}
	
	public void addActiveDevices(ArrayList<Integer> devs)
	{
		devices.addAll(devs);
	}
	
	public ArrayList<Integer> getDeviceList()
	{
		return devices;
	}
	
	public void setNode(int node)
	{
		this.node = node;
	}
	
	public CNFFilter getCNFForParent(Operator op)
	{
		return orderedFilters.get(op);
	}
	
	public ArrayList<String> getReferences()
	{
		ArrayList<String> retval = new ArrayList<String>();
		return retval;
	}
	
	public Vector<Operator> parents()
	{
		return parents;
	}
	
	public String getSchema()
	{
		return schema;
	}
	
	public String getTable()
	{
		return name;
	}
	
	public Operator parent()
	{
		throw new UnsupportedOperationException("TableScanOperator does not support parent()");
	}
	
	public MetaData getMeta()
	{
		return meta;
	}
	
	public Operator firstParent()
	{
		return parents.get(0);
	}
	
	public boolean equals(Object rhs)
	{
		if (rhs == null || (!(rhs instanceof TableScanOperator)))
		{
			return false;
		}
		else
		{
			return (schema.equals(((TableScanOperator)rhs).schema) && name.equals(((TableScanOperator)rhs).name));
		}
	}
	
	public int hashCode()
	{
		return schema.hashCode() + name.hashCode();
	}
	
	public ArrayList<Operator> children()
	{
		ArrayList<Operator> retval = new ArrayList<Operator>();
		return retval;
	}
	
	public long bufferSize()
	{
		return readBuffer.size();
	}
	
	public String toString()
	{
		String retval = "TableScanOperator: " + schema + "." + name;
		for (Map.Entry entry : orderedFilters.entrySet())
		{
			retval += (", (" + entry.getKey().toString() + ", " + entry.getValue().toString()) + ")";
		}
		
		return retval;
	}
	
	public HashMap<String, String> getCols2Types()
	{
		return cols2Types;
	}
	
	public HashMap<String, Integer> getCols2Pos()
	{
		return cols2Pos;
	}
	
	public TreeMap<Integer, String> getPos2Col()
	{
		return pos2Col;
	}
	
	public synchronized void start() throws IOException
	{
		if (!startDone)
		{
			startDone = true;
			
			if (devices.size() == 0)
			{
				BufferedReader in = new BufferedReader(new FileReader(new File(name.toLowerCase() + ".tbl")));
				ins.add(in);
			}
			else
			{
				for (int device : devices)
				{
					BufferedReader in = new BufferedReader(new FileReader(new File(meta.getDevicePath(device) + name.toLowerCase() + ".tbl")));
					ins.add(in);
				}
			}
			
			for (Operator parent : parents)
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
				for (Operator parent : parents)
				{
					readBuffers.put(parent, new LinkedBlockingQueue(Driver.QUEUE_SIZE));
				}
			}
			
			init();
		}
	}
	
	public void addFilter(Vector<Filter> filters, Operator op, Operator opParent)
	{
		opParents.put(opParent, op);
		
		HashSet<HashMap<Filter, Filter>> f = this.filters.get(op);
		if (f == null)
		{
			Operator op2 = opParents.get(op);
			if (op2 != null)
			{
				//parent has changed because of previous addFilter()
				opParents.remove(op);
				f = this.filters.get(op2);
				this.filters.remove(op2);
			}
			else
			{
				f = new HashSet<HashMap<Filter, Filter>>();
				HashMap<Filter, Filter> map = new HashMap<Filter, Filter>();
				for (Filter filter : filters)
				{
					map.put(filter, filter);
				}
			
				f.add(map);
				this.filters.put(op, f);
				orderedFilters.put(op, new CNFFilter(f, meta, cols2Pos));
				return;
			}
		}
		
		HashMap<Filter, Filter> map = new HashMap<Filter, Filter>();
		for (Filter filter : filters)
		{
			map.put(filter, filter);
		}
			
		f.add(map);
		this.filters.put(op, f);
		orderedFilters.put(op, new CNFFilter(f, meta, cols2Pos));
	}
	
	private void init()
	{ 
		int i = 0;
		ParseThread[] threads = new ParseThread[NUM_PTHREADS];
		while (i < NUM_PTHREADS)
		{
			threads[i] = new ParseThread();
			threads[i].start();
			i++;
		}
		
		InitThread t = new InitThread(threads);
		t.start();
	}
	
	public Object next(Operator op) throws Exception
	{
		if (!optimize)
		{
			Object o;
			LinkedBlockingQueue buffer = readBuffers.get(op);
			
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
	
	public synchronized void close() throws IOException
	{
		if (!closeDone)
		{
			closeDone = true;
			for (BufferedReader in : ins)
			{
				in.close();
			}
		}
	}
	
	public void add(Operator op) throws Exception
	{
		throw new UnsupportedOperationException("TableScanOperator does not support children.");
	}
	
	public void removeChild(Operator op)
	{
		throw new UnsupportedOperationException("TableScanOperator does not support removeChild()");
	}
	
	public void removeParent(Operator op)
	{
		parents.remove(op);
	}
	
	public void registerParent(Operator op)
	{
		parents.add(op);
	}
	
	private class InitThread extends Thread
	{
		private ParseThread[] threads;
		private ArrayList<ReaderThread> reads = new ArrayList<ReaderThread>();
		
		public InitThread(ParseThread[] threads)
		{
			this.threads = threads;
		}
		
		public void run()
		{
			try
			{
				for (BufferedReader in : ins)
				{
					ReaderThread read = new ReaderThread(in);
					read.start();
					reads.add(read);
				}
				
				for (ReaderThread read : reads)
				{
					read.join();
				}
				
				queue.put(new DataEndMarker());
				
				int i = 0;
				while (i < NUM_PTHREADS)
				{
					threads[i].join();
					i++;
				}
				
				if (optimize)
				{
					readBuffer.put(new DataEndMarker());
				}
				else
				{
					for (LinkedBlockingQueue q : readBuffers.values())
					{
						q.put(new DataEndMarker());
					}
				}
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(1);
			}
		}
	}
	
	private class ReaderThread extends Thread
	{
		private BufferedReader in;
		
		public ReaderThread(BufferedReader in)
		{
			this.in = in;
		}
		
		public void run()
		{
			try
			{
				int i = 0;
				String line = in.readLine();
				while (line != null)
				{
					StringTokenizer tokens = new StringTokenizer(line, "|", false);
					ArrayList<String> cols = new ArrayList<String>();
					while (tokens.hasMoreTokens())
					{
						cols.add(tokens.nextToken());
					}	
				
					String newLine = cols.get(fetchPos.get(0));
					int j = 1;
					while (j < fetchPos.size())
					{
						newLine += ("|" + cols.get(fetchPos.get(j)));
						j++;
					}
				
					queue.put(newLine);
					i++;
					line = in.readLine();
				
					//if (i % 10000 == 0)
					//{
					//	System.out.println("Read " + i + " records");
					//}
				}
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(1);
			}
		}
	}
	
	private class ParseThread extends Thread
	{
		private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		
		public void run()
		{
			try
			{
				while (true)
				{
					Object o = queue.take();
					
					if (o instanceof DataEndMarker)
					{
						queue.put(o);
						return;
					}
					
					String line = (String)o;
					ArrayList<Object> row = new ArrayList<Object>();
					StringTokenizer tokens = new StringTokenizer(line, "|", false);
					int i = 0;
					try
					{
						for (Map.Entry entry : midPos2Col.entrySet())
						{
							String type = midCols2Types.get(entry.getValue());
							if (type.equals("INT"))
							{
								row.add(Integer.parseInt(tokens.nextToken()));
							}
							else if (type.equals("FLOAT"))
							{
								row.add(Double.parseDouble(tokens.nextToken()));
							}
							else if (type.equals("CHAR"))
							{
								row.add(tokens.nextToken());
							}
							else if (type.equals("LONG"))
							{
								row.add(Long.parseLong(tokens.nextToken()));
							}
							else if (type.equals("DATE"))
							{
								row.add(sdf.parse(tokens.nextToken()));
							}
							
							i++;
						}
					}
					catch(Exception e)
					{
						e.printStackTrace();
						System.err.println(line);
						System.err.println(i);
						System.err.println(schema + "." + name);
						System.err.println(cols2Types);
					}
						
					if (!optimize)
					{
						for (Map.Entry entry : readBuffers.entrySet())
						{
							LinkedBlockingQueue q = (LinkedBlockingQueue)entry.getValue();
							CNFFilter filter = orderedFilters.get(entry.getKey());
							
							if (filter != null)
							{
								if (filter.passes(row))
								{
									ArrayList<Object> newRow = new ArrayList<Object>();
									for (int pos : neededPos)
									{
										newRow.add(row.get(pos));
									}
									q.put(newRow);
								}
							}
							else
							{
								ArrayList<Object> newRow = new ArrayList<Object>();
								for (int pos : neededPos)
								{
									newRow.add(row.get(pos));
								}
								q.put(newRow);
							}
						}
					}
					else
					{
						CNFFilter filter = orderedFilters.get(parents.get(0));
						
						if (filter != null)
						{
							if (filter.passes(row))
							{
								ArrayList<Object> newRow = new ArrayList<Object>();
								for (int pos : neededPos)
								{
									newRow.add(row.get(pos));
								}
								readBuffer.put(newRow);
							}
						}
						else
						{
							ArrayList<Object> newRow = new ArrayList<Object>();
							for (int pos : neededPos)
							{
								newRow.add(row.get(pos));
							}
							readBuffer.put(newRow);
						}
					}
				}
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(1);
			}
		}
	}
	
	public void setNeededCols(ArrayList<String> needed)
	{
		fetchPos = new ArrayList<Integer>();
		for (String col : needed)
		{
			fetchPos.add(cols2Pos.get(col));
		}
		
		for (CNFFilter filter : orderedFilters.values())
		{
			for (String col2 : filter.getReferences())
			{
				int pos = cols2Pos.get(col2);
				if (!fetchPos.contains(pos))
				{
					fetchPos.add(pos);
				}
			}
		}
		
		int i = 0;
		HashMap<String, Integer> fetchCols2Pos = new HashMap<String, Integer>();
		midPos2Col = new TreeMap<Integer, String>();
		midCols2Types = new HashMap<String, String>();
		for (int pos : fetchPos)
		{
			fetchCols2Pos.put(pos2Col.get(pos), i);
			midPos2Col.put(i, pos2Col.get(pos));
			midCols2Types.put(pos2Col.get(pos), cols2Types.get(pos2Col.get(pos)));
			i++;
		}
		
		for (CNFFilter filter : orderedFilters.values())
		{
			filter.updateCols2Pos(fetchCols2Pos);
		}
		
		//calculate how to go from what was fetched to what needs to be output
		neededPos = new ArrayList<Integer>();
		for (String col : needed)
		{
			neededPos.add(fetchCols2Pos.get(col));
		}
		
		//update internal metadata x3
		cols2Pos = new HashMap<String, Integer>();
		pos2Col = new TreeMap<Integer, String>();
		HashMap<String, String> tempCols2Types = new HashMap<String, String>();
		i = 0;
		for (String col : needed)
		{
			cols2Pos.put(col, i);
			pos2Col.put(i, col);
			tempCols2Types.put(col, cols2Types.get(col));
			i++;
		}
		
		cols2Types = tempCols2Types;
	}
	
	public boolean metaDataSet()
	{
		return set;
	}
	
	public void setMetaData()
	{
		set = true;
		partMeta = meta.getPartMeta(schema, name);
	}
	
	public boolean noNodeGroupSet()
	{
		return partMeta.noNodeGroupSet();
	}
	
	public boolean anyNode()
	{
		return partMeta.anyNode();
	}
	
	public void addActiveNodeForParent(int i, Operator op)
	{
		ArrayList<Integer> list = activeNodes.get(op);
		
		if (list == null)
		{
			list = new ArrayList<Integer>();
			list.add(i);
			activeNodes.put(op,  list);
			return;
		}
		
		list.add(i);
	}
	
	public void addActiveDeviceForParent(int i, Operator op)
	{
		ArrayList<Integer> list = activeDevices.get(op);
		
		if (list == null)
		{
			list = new ArrayList<Integer>();
			list.add(i);
			activeDevices.put(op,  list);
			return;
		}
		
		list.add(i);
	}
	
	public void addActiveDevicesForParent(ArrayList<Integer> is, Operator op)
	{
		ArrayList<Integer> list = activeDevices.get(op);
		
		if (list == null)
		{
			activeDevices.put(op, is);
			return;
		}
		
		list.addAll(is);
	}
	
	public HashMap<Integer, ArrayList<Integer>> getNodeGroupHashMap()
	{
		return partMeta.getNodeGroupHashMap();
	}
	
	public boolean isSingleDeviceSet()
	{
		return partMeta.isSingleDeviceSet();
	}
	
	public int getSingleDevice()
	{
		return partMeta.getSingleDevice();
	}
	
	public boolean deviceIsHash()
	{
		return partMeta.deviceIsHash();
	}
	
	public ArrayList<String> getDeviceHash()
	{
		return partMeta.getDeviceHash();
	}
	
	public boolean allDevices()
	{
		return partMeta.allDevices();
	}
	
	public int getNumDevices()
	{
		return partMeta.getNumDevices();
	}
	
	public ArrayList<Integer> deviceSet()
	{
		return partMeta.deviceSet();
	}
	
	public String getDeviceRangeCol()
	{
		return partMeta.getDeviceRangeCol();
	}
	
	public ArrayList<Integer> nodeSet()
	{
		return partMeta.nodeSet();
	}
	
	public boolean allNodes()
	{
		return partMeta.allNodes();
	}
	
	public String getNodeRangeCol()
	{
		return partMeta.getNodeRangeCol();
	}
	
	public ArrayList<String> getNodeHash()
	{
		return partMeta.getNodeHash();
	}
	
	public boolean nodeIsHash()
	{
		return partMeta.nodeIsHash();
	}
	
	public boolean isSingleNodeSet()
	{
		return partMeta.isSingleNodeSet();
	}
	
	public int getSingleNode()
	{
		return partMeta.getSingleNode();
	}
	
	public String getNodeGroupRangeCol()
	{
		return partMeta.getNodeGroupRangeCol();
	}
	
	public ArrayList<Integer> nodeGroupSet()
	{
		return partMeta.nodeGroupSet();
	}
	
	public ArrayList<String> getNodeGroupHash()
	{
		return partMeta.getNodeGroupHash();
	}
	
	public int getNumNodeGroups()
	{
		return partMeta.getNumNodeGroups();
	}
	
	public boolean nodeGroupIsHash()
	{
		return partMeta.nodeGroupIsHash();
	}
	
	public boolean isSingleNodeGroupSet()
	{
		return partMeta.isSingleNodeGroupSet();
	}
	
	public int getSingleNodeGroup()
	{
		return partMeta.getSingleNodeGroup();
	}
	
	public int getNumNodes()
	{
		return partMeta.getNumNodes();
	}
	
	public ArrayList<Integer> getDevicesMatchingRangeFilters(ArrayList<Filter> rangeFilters)
	{
		ArrayList<Integer> retval = new ArrayList<Integer>();
		ArrayList<Integer> deviceList = null;
		if (partMeta.allDevices())
		{
			deviceList = new ArrayList<Integer>();
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
		for (Object leVal : partMeta.getDeviceRanges())
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
	
	public ArrayList<Integer> getNodesMatchingRangeFilters(ArrayList<Filter> rangeFilters)
	{
		ArrayList<Integer> retval = new ArrayList<Integer>();
		ArrayList<Integer> nodeList = null;
		if (partMeta.allNodes())
		{
			nodeList = new ArrayList<Integer>();
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
				Iterator iter = partMeta.getNodeGroupHashMap().values().iterator();
				iter.hasNext();
				int size = ((ArrayList<Integer>)(iter.next())).size();
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
		for (Object leVal : partMeta.getNodeRanges())
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
	
	public ArrayList<Integer> getNodeGroupsMatchingRangeFilters(ArrayList<Filter> rangeFilters)
	{
		ArrayList<Integer> retval = new ArrayList<Integer>();
		ArrayList<Integer> nodeGroupList = partMeta.nodeGroupSet();
		
		Object oldLE = null;
		int i = 0;
		for (Object leVal : partMeta.getNodeGroupRanges())
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
			else if (highLE instanceof Date)
			{
				lowLE = new Date(Long.MIN_VALUE);
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
			else if (lowLE instanceof Date)
			{
				highLE = new Date(Long.MAX_VALUE);
			}
			else if (lowLE instanceof String)
			{
				highLE = "\uFFFF";
			}
		}
		
		for (Filter filter : filters)
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
			
			HashMap<String, Integer> cols2Pos = new HashMap<String, Integer>();
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
			ArrayList<Object> row1 = new ArrayList<Object>();
			row1.add(lowLE);
			ArrayList<Object> row2 = new ArrayList<Object>();
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
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(1);
			}
		}
		
		return true;
	}
	
	public ArrayList<Integer> getNodeList(Operator op)
	{
		return activeNodes.get(op);
	}
	
	public ArrayList<Integer> getDeviceList(Operator op)
	{
		return activeDevices.get(op);
	}
	
	public ArrayList<Object> getNodeGroupRanges()
	{
		return partMeta.getNodeGroupRanges();
	}
	
	public ArrayList<Object> getNodeRanges()
	{
		return partMeta.getNodeRanges();
	}
	
	public ArrayList<Object> getDeviceRanges()
	{
		return partMeta.getDeviceRanges();
	}
}
