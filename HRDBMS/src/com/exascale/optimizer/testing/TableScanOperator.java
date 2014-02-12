package com.exascale.optimizer.testing;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
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
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

import com.exascale.optimizer.testing.MetaData.PartitionMetaData;

public final class TableScanOperator implements Operator, Serializable
{
	protected HashMap<String, String> cols2Types = new HashMap<String, String>();
	protected HashMap<String, Integer> cols2Pos = new HashMap<String, Integer>();
	protected TreeMap<Integer, String> pos2Col = new TreeMap<Integer, String>();
	protected String name;
	protected String schema;
	protected ArrayList<BufferedReader> ins = new ArrayList<BufferedReader>();
	protected ArrayList<Operator> parents = new ArrayList<Operator>();
	public volatile BufferedLinkedBlockingQueue readBuffer = new BufferedLinkedBlockingQueue(Driver.QUEUE_SIZE);
	protected volatile HashMap<Operator, BufferedLinkedBlockingQueue> readBuffers = new HashMap<Operator, BufferedLinkedBlockingQueue>();
	protected boolean startDone = false;
	protected boolean optimize = false;
	protected final int NUM_PTHREADS = ResourceManager.cpus;
	protected HashMap<Operator, HashSet<HashMap<Filter, Filter>>> filters = new HashMap<Operator, HashSet<HashMap<Filter, Filter>>>();
	protected HashMap<Operator, CNFFilter> orderedFilters = new HashMap<Operator, CNFFilter>();
	protected MetaData meta;
	protected HashMap<Operator, Operator> opParents = new HashMap<Operator, Operator>();
	protected ArrayList<Integer> neededPos;
	protected ArrayList<Integer> fetchPos;
	protected TreeMap<Integer, String> midPos2Col;
	protected HashMap<String, String> midCols2Types;
	protected boolean set = false;
	protected PartitionMetaData partMeta;
	protected HashMap<Operator, ArrayList<Integer>> activeDevices = new HashMap<Operator, ArrayList<Integer>>();
	protected HashMap<Operator, ArrayList<Integer>> activeNodes = new HashMap<Operator, ArrayList<Integer>>();
	protected ArrayList<Integer> devices = new ArrayList<Integer>();
	protected int node;
	protected boolean phase2Done = false;
	protected HashMap<Integer, Operator> device2Child = new HashMap<Integer, Operator>();
	protected ArrayList<Operator> children = new ArrayList<Operator>();
	protected ArrayList<BufferedRandomAccessFile> randomIns = new ArrayList<BufferedRandomAccessFile>();
	protected HashMap<BufferedRandomAccessFile, Integer> ins2Device = new HashMap<BufferedRandomAccessFile, Integer>();
	protected boolean indexOnly = false;
	protected volatile boolean forceDone = false;
	
	public void reset()
	{	
		for (Operator o : children)
		{
			try
			{
				o.reset();
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(1);
			}
		}
		
		readBuffer.clear();
		forceDone = false;
		init();
	}
	
	public HashSet<HashMap<Filter, Filter>> getHSHM()
	{
		CNFFilter retval = orderedFilters.get(parents.get(0));
		if (retval == null)
		{
			return null;
		}
		
		return retval.getHSHM();
	}
	
	public void nextAll(Operator op)
	{
		forceDone = true;
	}
	
	public void setChildForDevice(int device, Operator child)
	{
		device2Child.put(device, child);
	}
	
	public CNFFilter getFirstCNF()
	{
		for (CNFFilter cnf : orderedFilters.values())
		{
			return cnf;
		}
		
		return null;
	}
	
	public void cleanupOrderedFilters()
	{
		if (orderedFilters.size() <= 1)
		{
			return;
		}
		CNFFilter theOne = orderedFilters.get(parents.get(0));
		orderedFilters.clear();
		if (theOne != null)
		{
			orderedFilters.put(parents.get(0), theOne);
		}
	}
	
	public void setChildPos(int pos)
	{
	}
	
	public int getChildPos()
	{
		return 0;
	}
	
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
	
	public TableScanOperator(String schema, String name, MetaData meta) throws Exception
	{
		this.meta = meta;
		this.name = name;
		this.schema = schema;
		cols2Types = meta.getCols2TypesForTable(schema, name);
		cols2Pos = meta.getCols2PosForTable(schema, name);
		pos2Col = meta.getPos2ColForTable(schema, name);
	}
	
	public int getNode()
	{
		return node;
	}
	
	public void clearOpParents()
	{
		if (opParents.size() > 0)
		{
			opParents.clear();
		}
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
		ArrayList<String> retval = new ArrayList<String>(0);
		return retval;
	}
	
	public ArrayList<Operator> parents()
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
			return (schema.equals(((TableScanOperator)rhs).schema) && name.equals(((TableScanOperator)rhs).name) && node == ((TableScanOperator)rhs).node);
		}
	}
	
	public int hashCode()
	{
		return schema.hashCode() + name.hashCode();
	}
	
	public ArrayList<Operator> children()
	{
		return children;
	}
	
	public String toString()
	{
		String retval = "TableScanOperator(" + node + ":" + devices + "): " + schema + "." + name;
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
			
			for (Operator o : children)
			{
				try
				{
					o.start();
				}
				catch(Exception e)
				{
					e.printStackTrace();
					System.exit(1);
				}
			}
			
			if (devices.size() == 0)
			{
				BufferedReader in = new BufferedReader(new FileReader(new File(name.toLowerCase() + ".tbl")));
				ins.add(in);
			}
			else
			{
				for (int device : devices)
				{
					if (children.size() == 0)
					{
						BufferedReader in = new BufferedReader(new FileReader(new File(meta.getDevicePath(device) + name.toLowerCase() + ".tbl")));
						ins.add(in);
					}
					else
					{
						BufferedRandomAccessFile in = new BufferedRandomAccessFile(meta.getDevicePath(device) + name.toLowerCase() + ".tbl", "r", 512);
						randomIns.add(in);
						ins2Device.put(in, device);
					}
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
					readBuffers.put(parent, new BufferedLinkedBlockingQueue(Driver.QUEUE_SIZE));
				}
			}
			
			init();
		}
	}
	
	public void addFilter(ArrayList<Filter> filters, Operator op, Operator opParent)
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
	
	protected void init()
	{ 
		int i = 0;
		InitThread t = new InitThread();
		t.start();
	}
	
	public Object next(Operator op) throws Exception
	{
		if (!optimize)
		{
			Object o;
			BufferedLinkedBlockingQueue buffer = readBuffers.get(op);
			
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
	
	public void close() throws IOException
	{
		for (BufferedReader in : ins)
		{
			in.close();
		}
		for (BufferedRandomAccessFile in : randomIns)
		{
			in.close();
		}
		
		for (Operator o : children)
		{
			try
			{
				o.close();
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(1);
			}
		}
	}
	
	public void add(Operator op) throws Exception
	{
		children.add(op);
		op.registerParent(this);
	}
	
	public void removeChild(Operator op)
	{
		children.remove(op);
		op.removeParent(this);
	}
	
	public void removeParent(Operator op)
	{
		parents.remove(op);
	}
	
	public void registerParent(Operator op)
	{
		parents.add(op);
		if (opParents.containsKey(op))
		{
			orderedFilters.put(op, orderedFilters.get(opParents.get(op)));
			opParents.put(op.parent(), op);
		}
	}
	
	protected final class InitThread extends ThreadPoolThread
	{
		protected ArrayList<ReaderThread> reads = new ArrayList<ReaderThread>(ins.size());
		
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
				
				for (BufferedRandomAccessFile in : randomIns)
				{
					ReaderThread read = new ReaderThread(in);
					read.start();
					reads.add(read);
				}
				
				for (ReaderThread read : reads)
				{
					read.join();
				}
				
				if (optimize)
				{
					readBuffer.put(new DataEndMarker());
				}
				else
				{
					for (BufferedLinkedBlockingQueue q : readBuffers.values())
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
	
	protected final class ReaderThread extends ThreadPoolThread
	{
		protected BufferedReader in;
		protected BufferedRandomAccessFile in2;
		
		public ReaderThread(BufferedReader in)
		{
			this.in = in;
		}
		
		public ReaderThread(BufferedRandomAccessFile in2)
		{
			this.in2 = in2;
		}
		
		public final void run()
		{
			ArrayList<String> types = null;
			CNFFilter filter = orderedFilters.get(parents.get(0));
			if (types == null)
			{
				types = new ArrayList<String>(midPos2Col.size());
				for (Map.Entry entry : midPos2Col.entrySet())
				{
					types.add(midCols2Types.get(entry.getValue()));
				}
			}
			
			try
			{
				if (in2 == null)
				{	
					int i = 0;
					String line = in.readLine();
					//@?Parallel
					FastStringTokenizer tokens = new FastStringTokenizer("", "|", false);	
					while (line != null)
					{
						ArrayList<Object> row = new ArrayList<Object>(types.size());
						tokens.reuse(line, "|", false);	
						int j = 0;
						while (j < fetchPos.size())
						{
							String type = types.get(j);
							tokens.setIndex(fetchPos.get(j));
							if (type.equals("INT"))
							{
								row.add(Utils.parseInt(tokens.nextToken()));
							}
							else if (type.equals("FLOAT"))
							{
								row.add(Utils.parseDouble(tokens.nextToken()));
							}
							else if (type.equals("CHAR"))
							{
								row.add(tokens.nextToken());
							}
							else if (type.equals("LONG"))
							{
								row.add(Utils.parseLong(tokens.nextToken()));
							}
							else if (type.equals("DATE"))
							{
								row.add(DateParser.parse(tokens.nextToken()));
							}
							j++;
						}
				
						if (!optimize)
						{
							for (Map.Entry entry : readBuffers.entrySet())
							{
								BufferedLinkedBlockingQueue q = (BufferedLinkedBlockingQueue)entry.getValue();
								filter = orderedFilters.get(entry.getKey());
								
								if (filter != null)
								{
									if (filter.passes(row))
									{
										ArrayList<Object> newRow = new ArrayList<Object>(neededPos.size());
										for (int pos : neededPos)
										{
											newRow.add(row.get(pos));
										}
										q.put(newRow);
									}
								}
								else
								{
									ArrayList<Object> newRow = new ArrayList<Object>(neededPos.size());
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
							if (filter != null)
							{
								if (filter.passes(row))
								{
									ArrayList<Object> newRow = new ArrayList<Object>(neededPos.size());
									for (int pos : neededPos)
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
								ArrayList<Object> newRow = new ArrayList<Object>(neededPos.size());
								for (int pos : neededPos)
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
						
						i++;
						line = in.readLine();
				
						//if (i % 10000 == 0)
						//{
						//	System.out.println("Read " + i + " records");
						//}
					}
				}
				else
				{
					int count = 0;
					Operator child = device2Child.get(ins2Device.get(in2));
					Object o = null;
					try
					{
						o = child.next(TableScanOperator.this);
					}
					catch(Exception e)
					{
						e.printStackTrace();
						System.out.println("ins2Device = " + ins2Device);
						System.out.println("ins2Device.get(in2) = " + ins2Device.get(in2));
						System.out.println("device2Child = " + device2Child);
						System.out.println("device2Child.get(ins2Device.get(in2)) = " + device2Child.get(ins2Device.get(in2)));
						System.exit(1);
					}
					//@?Parallel
					FastStringTokenizer tokens = new FastStringTokenizer("", "|", false);	
					while (!(o instanceof DataEndMarker))
					{
						if (!indexOnly)
						{
							in2.seek((Long)(((ArrayList<Object>)o).get(0)));
							String line = in2.readLine();
							ArrayList<Object> row = new ArrayList<Object>(types.size());
							tokens.reuse(line, "|", false);	
							int j = 0;
							while (j < fetchPos.size())
							{
								String type = types.get(j);
								tokens.setIndex(fetchPos.get(j));
								if (type.equals("INT"))
								{
									row.add(Utils.parseInt(tokens.nextToken()));
								}
								else if (type.equals("FLOAT"))
								{
									row.add(Utils.parseDouble(tokens.nextToken()));
								}
								else if (type.equals("CHAR"))
								{
									row.add(tokens.nextToken());
								}
								else if (type.equals("LONG"))
								{
									row.add(Utils.parseLong(tokens.nextToken()));
								}
								else if (type.equals("DATE"))
								{
									row.add(DateParser.parse(tokens.nextToken()));
								}
								j++;
							}
					
							if (!optimize)
							{
								for (Map.Entry entry : readBuffers.entrySet())
								{
									BufferedLinkedBlockingQueue q = (BufferedLinkedBlockingQueue)entry.getValue();
									filter = orderedFilters.get(entry.getKey());
									
									if (filter != null)
									{
										if (filter.passes(row))
										{
											ArrayList<Object> newRow = new ArrayList<Object>(neededPos.size());
											for (int pos : neededPos)
											{
												newRow.add(row.get(pos));
											}
											q.put(newRow);
										}
									}
									else
									{
										ArrayList<Object> newRow = new ArrayList<Object>(neededPos.size());
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
								if (filter != null)
								{
									if (filter.passes(row))
									{
										ArrayList<Object> newRow = new ArrayList<Object>(neededPos.size());
										for (int pos : neededPos)
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
									ArrayList<Object> newRow = new ArrayList<Object>(neededPos.size());
									for (int pos : neededPos)
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
							
							count++;
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
							
							ArrayList<Object> row = new ArrayList<Object>(pos2Col.size());
							for (String col : pos2Col.values())
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
							count++;
							o = child.next(TableScanOperator.this);
						}
					}
					
					//System.out.println("TableScanOperator read " + count + " rows based on a RID list");
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
		fetchPos = new ArrayList<Integer>(needed.size());
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
		neededPos = new ArrayList<Integer>(needed.size());
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
			list = new ArrayList<Integer>(1);
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
			list = new ArrayList<Integer>(1);
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
	
	protected boolean canAnythingInRangeSatisfyFilters(ArrayList<Filter> filters, Object lowLE, Object highLE)
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
			ArrayList<Object> row1 = new ArrayList<Object>(1);
			row1.add(lowLE);
			ArrayList<Object> row2 = new ArrayList<Object>(1);
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
	
	public void setIndexOnly()
	{
		indexOnly = true;
	}
}
