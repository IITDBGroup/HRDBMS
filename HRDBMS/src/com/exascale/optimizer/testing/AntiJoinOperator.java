package com.exascale.optimizer.testing;

import java.io.IOException;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import com.exascale.optimizer.testing.ResourceManager.DiskBackedArray;
import com.exascale.optimizer.testing.ResourceManager.DiskBackedHashMap;
import com.exascale.optimizer.testing.SemiJoinOperator.ProcessThread;
import com.exascale.optimizer.testing.SemiJoinOperator.ReaderThread;

public class AntiJoinOperator implements Operator, Serializable
{
	protected ArrayList<Operator> children = new ArrayList<Operator>(2);
	protected Operator parent;
	protected HashMap<String, String> cols2Types;
	protected HashMap<String, Integer> cols2Pos;
	protected TreeMap<Integer, String> pos2Col;
	protected MetaData meta;
	protected volatile DiskBackedArray inBuffer;
	protected int NUM_RT_THREADS = 6 * ResourceManager.cpus;
	protected int NUM_PTHREADS = 6 * ResourceManager.cpus;
	protected AtomicLong outCount = new AtomicLong(0);
	protected AtomicLong inCount = new AtomicLong(0);
	protected volatile boolean readersDone = false;
	protected ArrayList<String> cols;
	protected volatile BufferedLinkedBlockingQueue outBuffer = new BufferedLinkedBlockingQueue(Driver.QUEUE_SIZE);
	protected volatile ArrayList<Integer> poses;
	int childPos = -1;
	protected HashSet<HashMap<Filter, Filter>> f = null;
	protected int node;
	protected boolean indexAccess = false;
	protected ArrayList<Index> dynamicIndexes;
    protected SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    protected HashSet<HashMap<Filter, Filter>> hshm;
    protected volatile boolean doReset = false;
    protected int rightChildCard = 16;
    protected ArrayList<DiskBackedHashMap> buckets = new ArrayList<DiskBackedHashMap>();
	protected AtomicLong inCount2 = new AtomicLong(0);
	protected boolean alreadySorted = false;
	protected boolean cardSet = false;
    
    public void setDynamicIndex(ArrayList<Index> indexes)
    {
    	indexAccess = true;
    	this.dynamicIndexes = indexes;
    }
    
    public boolean setRightChildCard(int card)
    {
    	if (cardSet)
    	{
    		return false;
    	}
    	
    	cardSet = true;
    	rightChildCard = card;
    	return true;
    }
	
	public void reset()
	{
		System.out.println("AntiJoinOperator cannot be reset");
		System.exit(1);
	}
	
	public HashSet<HashMap<Filter, Filter>> getHSHM()
	{
		if (f != null)
		{
			return f;
		}
		
		HashSet<HashMap<Filter, Filter>> retval = new HashSet<HashMap<Filter, Filter>>();
		int i = 0;
		for (String col : children.get(1).getPos2Col().values())
		{
			Filter filter = null;
			try
			{
				filter = new Filter(cols.get(i), "E", col);
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(1);
			}
			HashMap<Filter, Filter> hm = new HashMap<Filter, Filter>();
			hm.put(filter,  filter);
			retval.add(hm);
			i++;
		}
		
		return retval;
	}
	
	public void setChildPos(int pos)
	{
		childPos = pos;
	}
	
	public int getChildPos()
	{
		return childPos;
	}
	
	public AntiJoinOperator(String col, MetaData meta)
	{
		this.cols = new ArrayList<String>(1);
		this.cols.add(col);
		this.meta = meta;
	}
	
	public AntiJoinOperator(ArrayList<String> cols, MetaData meta)
	{
		this.cols = cols;
		this.meta = meta;
	}
	
	public AntiJoinOperator(HashSet<HashMap<Filter, Filter>> f, MetaData meta)
	{
		this.f = f;
		this.meta = meta;
		this.cols = new ArrayList<String>(0);
	}
	
	protected AntiJoinOperator(ArrayList<String> cols, HashSet<HashMap<Filter, Filter>> f, MetaData meta)
	{
		this.cols = cols;
		this.f = f;
		this.meta = meta;
	}
	
	public AntiJoinOperator clone()
	{
		AntiJoinOperator retval = new AntiJoinOperator(cols, f, meta);
		retval.node = node;
		retval.indexAccess = indexAccess;
		retval.dynamicIndexes = dynamicIndexes;
		retval.alreadySorted = alreadySorted;
		retval.rightChildCard = rightChildCard;
		retval.cardSet = cardSet;
		return retval;
	}
	
	public int getNode()
	{
		return node;
	}
	
	public void alreadySorted()
	{
		alreadySorted = true;
	}
	
	public void setNode(int node)
	{
		this.node = node;
	}
	
	public ArrayList<String> getLefts()
	{
		if (cols.size() > 0)
		{
			return cols;
		}
		
		ArrayList<String> retval = new ArrayList<String>(f.size());
		for (HashMap<Filter, Filter> filters : f)
		{
			if (filters.size() == 1)
			{
				for (Filter filter : filters.keySet())
				{
					if (filter.op().equals("E"))
					{
						if (children.get(0).getCols2Pos().keySet().contains(filter.leftColumn()))
						{
							retval.add(filter.leftColumn());
						}
						else
						{
							retval.add(filter.rightColumn());
						}
					}
				}
			}
		}
		
		return retval;
	}
	
	public ArrayList<String> getRights()
	{
		if (cols.size() > 0)
		{
			ArrayList<String> retval = new ArrayList<String>(children.get(1).getCols2Pos().keySet().size());
			retval.addAll(children.get(1).getCols2Pos().keySet());
			return retval;
		}
		
		ArrayList<String> retval = new ArrayList<String>(f.size());
		for (HashMap<Filter, Filter> filters : f)
		{
			if (filters.size() == 1)
			{
				for (Filter filter : filters.keySet())
				{
					if (filter.op().equals("E"))
					{
						if (children.get(1).getCols2Pos().keySet().contains(filter.leftColumn()))
						{
							retval.add(filter.leftColumn());
						}
						else
						{
							retval.add(filter.rightColumn());
						}
					}
				}
			}
		}
		
		return retval;
	}
	
	public ArrayList<String> getReferences()
	{
		ArrayList<String> retval = new ArrayList<String>(cols);
		retval.addAll(children.get(1).getCols2Pos().keySet());
		
		if (f != null)
		{
			for (HashMap<Filter, Filter> filters : f)
			{
				for (Filter filter : filters.keySet())
				{
					if (filter.leftIsColumn())
					{
						retval.add(filter.leftColumn());
					}
					
					if (filter.rightIsColumn())
					{
						retval.add(filter.rightColumn());
					}
				}
			}
		}
		return retval;
	}
	
	public ArrayList<Operator> children()
	{
		return children;
	}
	
	public MetaData getMeta()
	{
		return meta;
	}
	
	public Operator parent()
	{
		return parent;
	}
	
	public String toString()
	{
		return "AntiJoinOperator";
	}
	
	public void add(Operator op) throws Exception
	{
		if (children.size() < 2)
		{
			if (childPos == -1)
			{
				children.add(op);
			}
			else
			{
				children.add(childPos, op);
				childPos = -1;
			}
			op.registerParent(this);
			
			if (children.size() == 2 && children.get(0).getCols2Types() != null && children.get(1).getCols2Types() != null)
			{
				cols2Types = children.get(0).getCols2Types();
				cols2Pos = children.get(0).getCols2Pos();
				pos2Col = children.get(0).getPos2Col();
				
				poses = new ArrayList<Integer>(cols.size());
				for (String col : cols)
				{
					poses.add(cols2Pos.get(col));
				}
			}
		}
		else
		{
			throw new Exception("AntiJoinOperator only supports 2 children");
		}
	}
	
	public void removeChild(Operator op)
	{
		childPos = children.indexOf(op);
		children.remove(op);
		op.removeParent(this);
	}
	
	public void removeParent(Operator op)
	{
		parent = null;
	}
	
	public void start() throws Exception 
	{
		if (!indexAccess)
		{
			for (Operator child : children)
			{
				child.start();
			}
		
			inBuffer = ResourceManager.newDiskBackedArray(true, rightChildCard);
			new InitThread().start();
		}
		else
		{
			for (Operator child : children)
			{
				child.start();
			}
		}
	}
	
	protected class InitThread extends ThreadPoolThread
	{
		protected NLSortThread nlSort = null;
		protected NLHashThread nlHash = null;
		protected Filter first = null;
		
		public void run()
		{
			ThreadPoolThread[] threads = null;
			int i = 0;
			CNFFilter cnf = null;
			ThreadPoolThread[] threads2 = new ProcessThread[NUM_PTHREADS];
			if (f != null)
			{
				//System.out.println("Using non-equijoin semijoin!");
				HashMap<String, Integer> tempC2P = (HashMap<String, Integer>)cols2Pos.clone();
				for (Map.Entry entry : children.get(1).getPos2Col().entrySet())
				{
					tempC2P.put((String)entry.getValue(), tempC2P.size());
				}
				
				cnf = new CNFFilter(f, tempC2P);
				
				boolean isHash = false;
				boolean isSort = false;
				
				for (HashMap<Filter, Filter> filters : f)
				{
					if (filters.size() == 1)
					{
						for (Filter filter : filters.keySet())
						{
							if (filter.op().equals("G") || filter.op().equals("GE") || filter.op().equals("L") || filter.op().equals("LE"))
							{
								if (filter.leftIsColumn() && filter.rightIsColumn())
								{
									isSort = true;
								}
							}
							else if (filter.op().equals("E") && filter.leftIsColumn() && filter.rightIsColumn())
							{
								isHash = true;
							}
						}
					}
				}
				
				if (isHash)
				{
					for (HashMap<Filter, Filter> filters : f)
					{
						if (filters.size() == 1)
						{
							for (Filter filter : filters.keySet())
							{
								if (filter.op().equals("E"))
								{
									if (filter.leftIsColumn() && filter.rightIsColumn())
									{
										//System.out.println("NestedLoopJoin qualifies for partial hashing");
										int j = 0;
										ArrayList<NLHashThread> nlThreads = new ArrayList<NLHashThread>(NUM_RT_THREADS);
										while (j < NUM_RT_THREADS)
										{
											nlHash = new NLHashThread(filter);
											nlThreads.add(nlHash);
											first = filter;
											nlHash.start();
											j++;
										}
										readersDone = true;
										for (NLHashThread thread : nlThreads)
										{
											while (true)
											{
												try
												{
													nlHash.join();
													break;
												}
												catch(InterruptedException e)
												{}
											}
										}
									}
								}
								
								break;
							}
						}
						
						if (nlHash != null)
						{
							break;
						}
					}
				}
				else if (isSort)
				{
					i = 0;
					if (alreadySorted)
					{
						NUM_RT_THREADS = 1;
					}
					threads = new ReaderThread[NUM_RT_THREADS];
					while (i < NUM_RT_THREADS)
					{
						threads[i] = new ReaderThread();
						threads[i].start();
						i++;
					}
					for (HashMap<Filter, Filter> filters : f)
					{
						if (filters.size() == 1)
						{
							for (Filter filter : filters.keySet())
							{
								if (filter.op().equals("G") || filter.op().equals("GE") || filter.op().equals("L") || filter.op().equals("LE"))
								{
									if (filter.leftIsColumn() && filter.rightIsColumn())
									{
										//System.out.println("NestedLoopJoin qualifies for sorting");
										nlSort = new NLSortThread(filter);
										i = 0;
										
										while (i < NUM_RT_THREADS) {
											while (true)
											{
												try 
												{
													threads[i].join();
													i++;
													break;
												} 
												catch (InterruptedException e) 
												{
												}
											}
										}
										readersDone = true;
										nlSort.start();
										first = filter;
										while (true)
										{
											try
											{
												nlSort.join();
												break;
											}
											catch(InterruptedException e)
											{}
										}
									}
								}
								
								break;
							}
						}
						
						if (nlSort != null)
						{
							break;
						}
					}
				}
				else
				{
					i = 0;
					threads = new ReaderThread[NUM_RT_THREADS];
					while (i < NUM_RT_THREADS)
					{
						threads[i] = new ReaderThread();
						threads[i].start();
						i++;
					}
				}

				i = 0;
				while (i < NUM_PTHREADS) {
					if (nlSort != null)
					{
						threads2[i] = new ProcessThread(cnf, first);
					}
					else if (nlHash != null)
					{
						threads2[i] = new ProcessThread(cnf, nlHash, first);
					}
					else
					{
						threads2[i] = new ProcessThread(cnf);
					}
					threads2[i].start();
					i++;
				}
			}
			else
			{
				i = 0;
				threads = new ReaderThread[NUM_RT_THREADS];
				while (i < NUM_RT_THREADS)
				{
					threads[i] = new ReaderThread();
					threads[i].start();
					i++;
				}
				
				i = 0;
				while (i < NUM_PTHREADS)
				{
					threads2[i] = new ProcessThread();
					threads2[i].start();
					i++;
				}
				
				i = 0;
				while (i < NUM_RT_THREADS)
				{
					while (true)
					{
						try 
						{
							threads[i].join();
							i++;
							break;
						} 
						catch (InterruptedException e) 
						{
						}
					}
				}
				
				readersDone = true;
				//System.out.println("AntiJoin readers are done");
			}
			
			i = 0;
			while (i < NUM_PTHREADS)
			{
				while (true)
				{
					try 
					{
						threads2[i].join();
						i++;
						break;
					} 
					catch (InterruptedException e) 
					{
					}
				}
			}
			
			if (nlHash != null)
			{
				nlHash.close();
			}
			
			//System.out.println("AntiJoinOperator output " + outCount.get() + " rows");
			
			while (true)
			{
				try
				{
					outBuffer.put(new DataEndMarker());
					inBuffer.close();
					break;
				}
				catch(Exception e)
				{
					e.printStackTrace();
				}
			}
		}
	}
	
	protected class ReaderThread extends ThreadPoolThread
	{	
		public void run()
		{
			try
			{
				Operator child = children.get(1);
				Object o = child.next(AntiJoinOperator.this);
				while (! (o instanceof DataEndMarker))
				{
					inBuffer.add((ArrayList<Object>)o);
					long count = inCount.getAndIncrement();
					//if (count % 10000 == 0)
					//{
					//	System.out.println("AntiJoinOperator has read " + count + " rows");
					//}
					o = child.next(AntiJoinOperator.this);
				}
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(1);
			}
		}
	}
	
	protected class NLHashThread extends ThreadPoolThread
	{
		protected Filter filter;
		protected int pos;
		
		public NLHashThread(Filter filter)
		{
			this.filter = filter;
		}
		
		public void close()
		{
			for (DiskBackedHashMap map : buckets)
			{
				try
				{
					map.close();
				}
				catch(Exception e)
				{
					e.printStackTrace();
					System.exit(1);
				}
			}
		}
		
		public void run()
		{
			try
			{
				pos = children.get(1).getCols2Pos().get(filter.rightColumn());
			}
			catch(Exception e)
			{
				pos = children.get(1).getCols2Pos().get(filter.leftColumn());
			}
			
			try
			{
				Operator child = children.get(1);
				HashMap<String, Integer> childCols2Pos = child.getCols2Pos();
				Object o = child.next(AntiJoinOperator.this);
				ArrayList<Object> key = new ArrayList<Object>(1);
				//@Parallel
				while (! (o instanceof DataEndMarker))
				{
					//inBuffer.add(o);
					long count = inCount2.incrementAndGet();
					
					//if (count % 10000 == 0)
					//{
					//	System.out.println("HashJoinOperator has read " + count + " rows");
					//}
					
					key.clear();
					key.add(((ArrayList<Object>)o).get(pos));
					
					long hash = 0x0EFFFFFFFFFFFFFFL & hash(key);
					writeToHashTable(hash, (ArrayList<Object>)o);
					o = child.next(AntiJoinOperator.this);
				}
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.out.println("Error in partial hash thread.");
				System.out.println("Left child cols to pos is: " + children.get(0).getCols2Pos());
				System.out.println("Right child cols to pos is: " + children.get(1).getCols2Pos());
				System.exit(1);
			}
		}
		
		protected void writeToHashTable(long hash, ArrayList<Object> row) throws Exception
		{
			if (buckets.size() == 0)
			{
				synchronized(this)
				{
					if (buckets.size() == 0)
					{
						buckets.add(ResourceManager.newDiskBackedHashMap(false, rightChildCard));
					}
				}
			}
			int i = 0;
			Object o = buckets.get(i).putIfAbsent(hash, row);
			while (o != null)
			{
				i++;
				
				if (i < buckets.size())
				{
					DiskBackedHashMap bucket = null;
					while (bucket == null)
					{
						bucket = buckets.get(i);
					}
					try
					{
						o = bucket.putIfAbsent(hash, row);
					}
					catch(Exception e)
					{
						e.printStackTrace();
						System.out.println("Buckets is " + buckets);
						System.out.println("Hash is " + hash);
						System.out.println("Row is " + row);
						System.exit(1);
					}
				}
				else
				{
					synchronized(buckets)
					{
						if (i <buckets.size())
						{
							DiskBackedHashMap bucket = buckets.get(i);
							o = bucket.putIfAbsent(hash, row);
						}
						else
						{
							buckets.add(ResourceManager.newDiskBackedHashMap(false, rightChildCard));
							DiskBackedHashMap bucket = buckets.get(i);
							o = bucket.putIfAbsent(hash, row);
						}
					}
					
				}
			}
		}
		
		protected ArrayList<ArrayList<Object>> getCandidates(long hash) throws ClassNotFoundException, IOException
		{
			ArrayList<ArrayList<Object>> retval = new ArrayList<ArrayList<Object>>();
			int i = 0;
			ArrayList<Object> o = (ArrayList<Object>)buckets.get(i).get(hash);
			while (o != null)
			{
				retval.add(o);
				i++;
				if (i < buckets.size())
				{
					o = (ArrayList<Object>)buckets.get(i).get(hash);
				}
				else
				{
					o = null;
				}
			}
			
			return retval;
		}
		
		protected long hash(ArrayList<Object> key)
		{
			long hashCode = 1125899906842597L;
			for (Object e : key)
			{
				long eHash = 1;
				if (e instanceof Integer)
				{
					long i = ((Integer)e).longValue();
					// Spread out values
				    long scaled = i * HashJoinOperator.LARGE_PRIME;

				    // Fill in the lower bits
				    eHash = scaled + HashJoinOperator.LARGE_PRIME2;
				}
				else if (e instanceof Long)
				{
					long i = (Long)e;
					// Spread out values
				    long scaled = i * HashJoinOperator.LARGE_PRIME;

				    // Fill in the lower bits
				    eHash = scaled + HashJoinOperator.LARGE_PRIME2;
				}
				else if (e instanceof String)
				{
					String string = (String)e;
					  long h = 1125899906842597L; // prime
					  int len = string.length();

					  for (int i = 0; i < len; i++) 
					  {
						   h = 31*h + string.charAt(i);
					  }
					  eHash = h;
				}
				else if (e instanceof Double)
				{
					long i = Double.doubleToLongBits((Double)e);
					// Spread out values
				    long scaled = i * HashJoinOperator.LARGE_PRIME;

				    // Fill in the lower bits
				    eHash = scaled + HashJoinOperator.LARGE_PRIME2;
				}
				else if (e instanceof Date)
				{
					long i = ((Date)e).getTime();
					// Spread out values
				    long scaled = i * HashJoinOperator.LARGE_PRIME;

				    // Fill in the lower bits
				    eHash = scaled + HashJoinOperator.LARGE_PRIME2;
				}
				else
				{
					eHash = e.hashCode();
				}
				
			    hashCode = 31*hashCode + (e==null ? 0 : eHash);
			}
			return hashCode;
		}
	}
	
	protected class NLSortThread extends ThreadPoolThread
	{
		protected Filter filter;
		protected boolean vBool;
		protected int pos;
		
		public NLSortThread(Filter filter)
		{
			this.filter = filter;
			//System.out.println("NLSortThread: " + filter);
		}
		
		public void run()
		{
			String vStr;
			if (filter.op().equals("G") || filter.op().equals("GE"))
			{
				vStr = filter.rightColumn();
				vBool = true;
				//System.out.println("VBool set to true");
			}
			else
			{
				vStr = filter.rightColumn();
				vBool = false;
				//System.out.println("VBool set to false");
			}
			
			try
			{
				pos = children.get(1).getCols2Pos().get(vStr);
			}
			catch(Exception e)
			{
				vStr = filter.leftColumn();
				vBool = !vBool;
				pos = children.get(1).getCols2Pos().get(vStr);
			}
			
			try
			{
				if (alreadySorted)
				{
					return;
				}
				if (inBuffer.size() < SortOperator.PARALLEL_SORT_MIN_NUM_ROWS)
				{
					doSequentialSort(0, inBuffer.size()-1);
				}
				else
				{
					ParallelSortThread t = doParallelSort(0, inBuffer.size()-1);
					t.join();
				}
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(1);
			}
		}
		
		protected ParallelSortThread doParallelSort(long left, long right)
		{
			//System.out.println("Starting parallel sort with " + (right-left+1) + " rows");
			ParallelSortThread t = new ParallelSortThread(left, right);
			t.start();
			return t;
		}
	
		protected class ParallelSortThread extends ThreadPoolThread
		{
			protected long left;
			protected long right;
		
			public ParallelSortThread(long left, long right)
			{
				this.left = left;
				this.right = right;
			}
		
			public void run()
			{
				try
				{
					if (((right - left) < SortOperator.PARALLEL_SORT_MIN_NUM_ROWS))
					{
						doSequentialSort(left, right);
					}
					else
					{
						if (right <= left)
						{
							return;
						}
						long pivotIndex = (long)(new Random().nextDouble() * (right - left) + left);
						ArrayList<Object> temp = (ArrayList<Object>)inBuffer.get(pivotIndex);
						inBuffer.update(pivotIndex, (ArrayList<Object>)inBuffer.get(left));
						inBuffer.update(left, temp);
						long lt = left;
						long gt = right;
						ArrayList<Object> v = temp;
						long i = left;
						while (i <= gt) 
						{
							temp = (ArrayList<Object>)inBuffer.get(i);
							int cmp = compare(temp, v);
							if (cmp < 0)
							{
								inBuffer.update(i, (ArrayList<Object>)inBuffer.get(lt));
								inBuffer.update(lt, temp);
								i++;
								lt++;
							}
							else if (cmp > 0)
							{
								inBuffer.update(i, (ArrayList<Object>)inBuffer.get(gt));
								inBuffer.update(gt, temp);
								gt--;
							}
							else
							{
								i++;
							}
						}

						if (lt < gt)
						{
							ParallelSortThread t1 = doParallelSort(left, lt);
							ParallelSortThread t2 = doParallelSort(gt, right);
							t1.join();
							t2.join();
						}
						else if (lt == gt)
						{
							if (gt == right)
							{
								lt--;
							}
							else
							{
								gt++;
							}
						
							ParallelSortThread t1 = doParallelSort(left, lt);
							ParallelSortThread t2 = doParallelSort(gt, right);
							t1.join();
							t2.join();
						}
						else
						{
							long temp2 = gt;
							gt = lt;
							lt = temp2;
							ParallelSortThread t1 = doParallelSort(left, lt);
							ParallelSortThread t2 = doParallelSort(gt, right);
							t1.join();
							t2.join();
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
	
		protected void doSequentialSort(long left, long right) throws Exception
		{
			if (right <= left)
			{
				return;
			}
			long lt = left;
			long gt = right;
			ArrayList<Object> v = (ArrayList<Object>)inBuffer.get(left);
			long i = left;
			while (i <= gt) 
			{
				ArrayList<Object> temp = (ArrayList<Object>)inBuffer.get(i);
				int cmp = compare(temp, v);
				if (cmp < 0)
				{
					inBuffer.update(i, (ArrayList<Object>)inBuffer.get(lt));
					inBuffer.update(lt, temp);
					i++;
					lt++;
				}
				else if (cmp > 0)
				{
					inBuffer.update(i, (ArrayList<Object>)inBuffer.get(gt));
					inBuffer.update(gt, temp);
					gt--;
				}
				else
				{
					i++;
				}
			}

			doSequentialSort(left, lt-1);
			doSequentialSort(gt+1, right);
		}
	
		protected int compare(ArrayList<Object> lhs, ArrayList<Object> rhs) throws Exception
		{
			int result;
			int i = 0;
		
			Object lField = lhs.get(pos);
			Object rField = rhs.get(pos);
			
			if (lField instanceof Integer)
			{
				result = ((Integer)lField).compareTo((Integer)rField);
			}
			else if (lField instanceof Long)
			{
				result = ((Long)lField).compareTo((Long)rField);
			}
			else if (lField instanceof Double)
			{
				result = ((Double)lField).compareTo((Double)rField);
			}
			else if (lField instanceof String)
			{
				result = ((String)lField).compareTo((String)rField);
			}
			else if (lField instanceof Date)
			{
				result = ((Date)lField).compareTo((Date)rField);
			}
			else
			{
				throw new Exception("Unknown type in SortOperator.compare(): " + lField.getClass());
			}	
			
			if (vBool)
			{
				if (result > 0)
				{
					return 1;
				}
				else if (result < 0)
				{
					return -1;
				}
			}
			else
			{
				if (result > 0)
				{
					return -1;
				}
				else if (result < 0)
				{
					return 1;
				}
			}
		
			return 0;
		}
	
		protected long partition(long left, long right, long pivotIndex) throws Exception
		{
			ArrayList<Object> pivotValue = (ArrayList<Object>)inBuffer.get(pivotIndex);
			Object rightRec = inBuffer.get(right);
			inBuffer.update(pivotIndex, (ArrayList<Object>)rightRec);
			inBuffer.update(right,  pivotValue);
	
			long i = left;
			long storeIndex = left;
			boolean allEqual = true;
		
			Random random = new Random(System.currentTimeMillis());
			while (i < right)
			{
				ArrayList<Object> temp = (ArrayList<Object>)inBuffer.get(i);
				int compareResult = compare(temp, pivotValue);
				if (compareResult == -1)
				{
					Object row = inBuffer.get(storeIndex);
					inBuffer.update(i, (ArrayList<Object>)row);
					inBuffer.update(storeIndex, temp);
					storeIndex++;
					allEqual = false;
				}	
				else if (compareResult == 0)
				{
					if (random.nextDouble() < 0.5)
					{
						Object row = inBuffer.get(storeIndex);
						inBuffer.update(i, (ArrayList<Object>)row);
						inBuffer.update(storeIndex, temp);
						storeIndex++;
					}					
				}
				else
				{
					allEqual = false;
				}	
				i++;
			}
		
			if (allEqual)
			{
				return -1;
			}
		
			ArrayList<Object> temp = (ArrayList<Object>)inBuffer.get(storeIndex);
			rightRec = inBuffer.get(right);
			inBuffer.update(storeIndex, (ArrayList<Object>)rightRec);
			inBuffer.update(right,  temp);
			return storeIndex;
		}
	}
	
	protected class ProcessThread extends ThreadPoolThread
	{
		protected CNFFilter cnf = null;
		protected Filter first = null;
		protected NLHashThread nlHash = null;
		
		public ProcessThread()
		{}
		
		public ProcessThread(CNFFilter cnf)
		{
			this.cnf = cnf;
		}
		
		public ProcessThread(CNFFilter cnf, Filter first)
		{
			this.cnf = cnf;
			this.first = first;
		}
		
		public ProcessThread(CNFFilter cnf, NLHashThread nlHash, Filter first)
		{
			this.cnf = cnf;
			this.nlHash = nlHash;
			this.first = first;
		}
		
		public void run()
		{
			if (cnf != null)
			{
				//System.out.println("AntiJoin ProcessThread started using non-equi-join!");
			}
			try
			{
				Operator left = children.get(0);
				Object o = left.next(AntiJoinOperator.this);
				int pos = -1;
				HashMap<String, Integer> tempC2P = null;
				if (first != null)
				{
					try
					{
						pos = left.getCols2Pos().get(first.leftColumn());
					}
					catch(Exception e)
					{
						pos = left.getCols2Pos().get(first.rightColumn());
					}
					tempC2P = (HashMap<String, Integer>)cols2Pos.clone();
					for (Map.Entry entry : children.get(1).getPos2Col().entrySet())
					{
						tempC2P.put((String)entry.getValue(), tempC2P.size());
					}
				}
				ArrayList<Object> obj = new ArrayList<Object>();
				//@Parallel
				while (! (o instanceof DataEndMarker))
				{
					ArrayList<Object> lRow = (ArrayList<Object>)o;
					obj.clear();
					if (cnf == null)
					{
						for (int pos2 : poses)
						{
							obj.add(lRow.get(pos2));
						}
					}
						
					while (true)
					{
						if (nlHash != null)
						{
							ArrayList<Object> key = new ArrayList<Object>(1);
							key.add(lRow.get(pos));
							long hash = 0x0EFFFFFFFFFFFFFFL & nlHash.hash(key);
							boolean passed = false;
							for (ArrayList<Object> rRow : nlHash.getCandidates(hash))
							{
								if (cnf.passes(lRow, rRow)) 
								{	
									passed = true;
									break;
								}
							}
							
							if (passed)
							{
								break;
							}
							
							outBuffer.put(lRow);
							long count = outCount.incrementAndGet();
							if (count % 100000 == 0) 
							{
								System.out
										.println("AntiJoinOperator has output "
												+ count + " rows");
							}
							break;
						}
						
						//System.out.println("Called inBuufer.contains()");
						if (cnf == null && inBuffer.contains(obj))
						{
							break;
						}
						else if (cnf == null)
						{
							if (readersDone)
							{
								//System.out.println("Call completed successfully");
								outBuffer.put(lRow);
								long count = outCount.getAndIncrement();
								//if (count % 10000 == 0)
								//{
								//	System.out.println("AntiJoinOperator has output " + count + " rows");
								//}
								break;
							}
							
							Thread.sleep(1);
						}
						else
						{	
							if (first == null)
							{
								boolean passed = false;
								for (Object orow : inBuffer)
								{
									ArrayList<Object> rRow = (ArrayList<Object>)orow;
									if (cnf.passes(lRow, rRow))
									{
										passed = true;
										break;
									}
								}
								
								if (passed)
								{
									break;
								}
							
								if (readersDone)
								{
									outBuffer.put(lRow);
									long count = outCount.getAndIncrement();
									//if (count % 10000 == 0)
									//{
									//	System.out.println("AntiJoinOperator has output " + count + " rows");
									//}
								}
								
								Thread.sleep(1);
							}
							else
							{
								//data is sorted
								boolean passed = false;
								for (Object orow : inBuffer)
								{
									ArrayList<Object> rRow = (ArrayList<Object>)orow;
									if (cnf.passes(lRow, rRow))
									{
										passed = true;
										break;
									}
									else
									{
										if (!first.passes(lRow, rRow, tempC2P))
										{
											outBuffer.put(lRow);
											long count = outCount.getAndIncrement();
											//if (count % 10000 == 0)
											//{
											//	System.out.println("AntiJoinOperator has output " + count + " rows");
											//}
											
											passed = true;
										}
									}
									
									if (passed)
									{
										break;
									}
								}
							
								outBuffer.put(lRow);
								long count = outCount.getAndIncrement();
								//if (count % 10000 == 0)
								//{
								//	System.out.println("AntiJoinOperator has output " + count + " rows");
								//}
							}
						}
					}
					
					o = left.next(AntiJoinOperator.this);
				}
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(1);
			}
		}
	}
	
	public void nextAll(Operator op) throws Exception
	{
		children.get(0).nextAll(op);
		children.get(1).nextAll(op);
		Object o = next(op);
		while (!(o instanceof DataEndMarker))
		{
			o = next(op);
		}
	}
	
	public Object next(Operator op) throws Exception
	{
		if (indexAccess)
		{
				while (true)
				{
					Object o = children.get(0).next(this);
					if (o instanceof DataEndMarker)
					{
						return o;
					}
					
					if (hshm == null)
					{
						hshm = getHSHM();
					}
					ArrayList<Filter> dynamics = new ArrayList<Filter>(hshm.size());
					int i = 0;
					for (HashMap<Filter, Filter> hm : hshm)
					{
						Filter f = new ArrayList<Filter>(hm.keySet()).get(0);
						String leftCol = null;
						String rightCol = null;
						if (children.get(0).getCols2Pos().keySet().contains(f.leftColumn()))
						{
							leftCol = f.leftColumn();
							Object leftVal = ((ArrayList<Object>)o).get(children.get(0).getCols2Pos().get(leftCol));
							String leftString = null;
							if (leftVal instanceof Integer || leftVal instanceof Long || leftVal instanceof Double)
							{
								leftString = leftVal.toString();
							}
							else if (leftVal instanceof String)
							{
								leftString = "'" + leftVal + "'";
							}	
							else if (leftVal instanceof Date)
							{
								leftString = sdf.format(leftVal);
							}
							Filter f2 = new Filter(leftString, f.op(), f.rightColumn());
							dynamics.add(f2);
						}
						else
						{
							rightCol = f.rightColumn();
							Object leftVal = ((ArrayList<Object>)o).get(children.get(0).getCols2Pos().get(rightCol));
							String leftString = null;
							if (leftVal instanceof Integer || leftVal instanceof Long || leftVal instanceof Double)
							{
								leftString = leftVal.toString();
							}	
							else if (leftVal instanceof String)
							{
								leftString = "'" + leftVal + "'";
							}
							else if (leftVal instanceof Date)
							{
								leftString = sdf.format(leftVal);
							}
							Filter f2 = new Filter(f.leftColumn(), f.op(), leftString);
							dynamics.add(f2);
						}
				
						i++;
					}
		
					synchronized(this)
					{
						if (doReset)
						{
							children.get(1).reset();
						}
						else
						{
							doReset = true;
						}
					
						for (Index index : dynamicIndexes)
						{
							index.setDelayedConditions(dynamics);
						}
				
						boolean retval = false;
						Object o2 = children.get(1).next(this);
						if (o2 instanceof DataEndMarker)
						{
							retval = true;
						}
					
						Operator c1 = children.get(1);
						c1.nextAll(AntiJoinOperator.this);
						
						if (retval)
						{
							return o;
						}
					}
				}
		}
		Object o;
		o = outBuffer.take();
		
		if (o instanceof DataEndMarker)
		{
			o = outBuffer.peek();
			if (o == null)
			{
				outBuffer.put(new DataEndMarker());
				return new DataEndMarker();
			}
			else
			{
				outBuffer.put(new DataEndMarker());
				return o;
			}
		}
		return o;
	}
	
	public void close() throws Exception 
	{
		for (Operator child : children)
		{
			child.close();
		}
		
		if (inBuffer != null)
		{
			inBuffer.close();
		}
	}

	public void registerParent(Operator op) throws Exception
	{
		if (parent == null)
		{
			parent = op;
		}
		else
		{
			throw new Exception("AntiJoinOperator can only have 1 parent.");
		}
	}

	@Override
	public HashMap<String, String> getCols2Types() {
		return cols2Types;
	}

	@Override
	public HashMap<String, Integer> getCols2Pos() {
		return cols2Pos;
	}

	@Override
	public TreeMap<Integer, String> getPos2Col() {
		return pos2Col;
	}
	
	public boolean usesHash()
	{
		System.out.println("In AntiJoin.usesHash() with " + f);
		if (cols.size() > 0)
		{
			return true;
		}
		
		for (HashMap<Filter, Filter> filters : f)
		{
			System.out.println(filters);
			if (filters.size() == 1)
			{
				System.out.println("Size = 1");
				for (Filter filter : filters.keySet())
				{
					if (filter.op().equals("E") && filter.leftIsColumn() && filter.rightIsColumn())
					{
						System.out.println("Uses hash");
						return true;
					}
				}
			}
		}
		
		return false;
	}
	
	public boolean usesSort()
	{
		if (cols.size() > 0)
		{
			return false;
		}
		
		boolean isSort = false;
		
		for (HashMap<Filter, Filter> filters : f)
		{
			if (filters.size() == 1)
			{
				for (Filter filter : filters.keySet())
				{
					if (filter.op().equals("G") || filter.op().equals("GE") || filter.op().equals("L") || filter.op().equals("LE"))
					{
						if (filter.leftIsColumn() && filter.rightIsColumn())
						{
							isSort = true;
						}
					}
					else if (filter.op().equals("E") && filter.leftIsColumn() && filter.rightIsColumn())
					{
						return false;
					}
				}
			}
		}
		
		return isSort;
	}
	
	public ArrayList<Boolean> sortOrders()
	{
		if (cols.size() > 0)
		{
			return null;
		}
		
		boolean isSort = false;

		for (HashMap<Filter, Filter> filters : f)
		{
			if (filters.size() == 1)
			{
				for (Filter filter : filters.keySet())
				{
					if (filter.op().equals("G") || filter.op().equals("GE") || filter.op().equals("L") || filter.op().equals("LE"))
					{
						if (filter.leftIsColumn() && filter.rightIsColumn())
						{
							String vStr;
							boolean vBool;
							if (filter.op().equals("G") || filter.op().equals("GE"))
							{
								vStr = filter.rightColumn();
								vBool = true;
								//System.out.println("VBool set to true");
							}
							else
							{
								vStr = filter.rightColumn();
								vBool = false;
								//System.out.println("VBool set to false");
							}
							
							try
							{
								int pos = children.get(1).getCols2Pos().get(vStr);
							}
							catch(Exception e)
							{
								//vStr = filter.leftColumn();
								vBool = !vBool;
								//pos = children.get(1).getCols2Pos().get(vStr);
							}
							
							ArrayList<Boolean> retval = new ArrayList<Boolean>(1);
							retval.add(vBool);
							return retval;
						}
					}
					else if (filter.op().equals("E") && filter.leftIsColumn() && filter.rightIsColumn())
					{
						return null;
					}
				}
			}
		}
		
		return null;
	}
	
	public ArrayList<String> sortKeys()
	{
		if (cols.size() > 0)
		{
			return null;
		}
		
		boolean isSort = false;

		for (HashMap<Filter, Filter> filters : f)
		{
			if (filters.size() == 1)
			{
				for (Filter filter : filters.keySet())
				{
					if (filter.op().equals("G") || filter.op().equals("GE") || filter.op().equals("L") || filter.op().equals("LE"))
					{
						if (filter.leftIsColumn() && filter.rightIsColumn())
						{
							String vStr;
							if (filter.op().equals("G") || filter.op().equals("GE"))
							{
								vStr = filter.rightColumn();
								//vBool = true;
								//System.out.println("VBool set to true");
							}
							else
							{
								vStr = filter.rightColumn();
								//vBool = false;
								//System.out.println("VBool set to false");
							}
							
							try
							{
								int pos = children.get(1).getCols2Pos().get(vStr);
							}
							catch(Exception e)
							{
								vStr = filter.leftColumn();
								//vBool = !vBool;
								//pos = children.get(1).getCols2Pos().get(vStr);
							}
							
							ArrayList<String> retval = new ArrayList<String>(1);
							retval.add(vStr);
							return retval;
						}
					}
					else if (filter.op().equals("E") && filter.leftIsColumn() && filter.rightIsColumn())
					{
						return null;
					}
				}
			}
		}
		
		return null;
	}
	
	public ArrayList<String> getJoinForChild(Operator op)
	{
		if (cols.size() > 0)
		{
			if (op.getCols2Pos().keySet().containsAll(cols))
			{
				return new ArrayList<String>(cols);
			}
			else
			{
				return new ArrayList<String>(op.getCols2Pos().keySet());
			}
		}
		
		Filter x = null;
		for (HashMap<Filter, Filter> filters : f)
		{
			if (filters.size() == 1)
			{
				for (Filter filter : filters.keySet())
				{
					if (filter.op().equals("E"))
					{
						if (filter.leftIsColumn() && filter.rightIsColumn())
						{
							x = filter;
						}
					}
					
					break;
				}
			}
			
			if (x != null)
			{
				break;
			}
		}
		
		if (op.getCols2Pos().keySet().contains(x.leftColumn()))
		{
			ArrayList<String> retval = new ArrayList<String>(1);
			retval.add(x.leftColumn());
			return retval;
		}
		
		ArrayList<String> retval = new ArrayList<String>(1);
		retval.add(x.rightColumn());
		return retval;
	}
}
