package com.exascale.optimizer.testing;

import java.io.IOException;
import java.io.Serializable;
 
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.ArrayList;
import java.util.Vector;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadLocalRandom; 
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

import com.exascale.optimizer.testing.ResourceManager.DiskBackedArray;
import com.exascale.optimizer.testing.ResourceManager.DiskBackedHashMap;

public final class NestedLoopJoinOperator extends JoinOperator implements Serializable 
{
	protected ArrayList<Operator> children = new ArrayList<Operator>(2);
	protected Operator parent;
	protected HashMap<String, String> cols2Types;
	protected HashMap<String, Integer> cols2Pos;
	protected TreeMap<Integer, String> pos2Col;
	protected MetaData meta;
	protected volatile BufferedLinkedBlockingQueue outBuffer;
	protected volatile DiskBackedArray inBuffer;
	protected int NUM_RT_THREADS = 4 * ResourceManager.cpus;
	protected int NUM_PTHREADS = 4 * ResourceManager.cpus;
	protected AtomicLong outCount = new AtomicLong(0);
	protected volatile boolean readersDone = false;
	protected CNFFilter cnfFilters;
	protected HashSet<HashMap<Filter, Filter>> f;
	protected int childPos = -1;
	protected int node;
	protected boolean indexAccess = false;
	protected ArrayList<Index> dynamicIndexes;
    protected ArrayList<ArrayList<Object>> queuedRows = new ArrayList<ArrayList<Object>>();
    protected MySimpleDateFormat sdf = new MySimpleDateFormat("yyyy-MM-dd");
    protected volatile boolean doReset = false;
    protected int rightChildCard = 16;
    protected ArrayList<DiskBackedHashMap> buckets = new ArrayList<DiskBackedHashMap>();
    protected ReentrantLock bucketsLock = new ReentrantLock();
	protected AtomicLong inCount2 = new AtomicLong(0);
	protected boolean alreadySorted = false;
	protected boolean cardSet = false;
	protected Vector<Operator> clones = new Vector<Operator>();
	protected Vector<AtomicBoolean> lockVector = new Vector<AtomicBoolean>();
	protected ReentrantLock thisLock = new ReentrantLock();
    
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
		System.out.println("NestedLoopJoinOperator cannot be reset");
		System.exit(1);
	}
	
	public void setChildPos(int pos)
	{
		childPos = pos;
	}
	
	public int getChildPos()
	{
		return childPos;
	}

	public NestedLoopJoinOperator(JoinOperator op) {
		this.meta = op.getMeta();
		this.f = op.getHSHMFilter();
	}

	public NestedLoopJoinOperator(ArrayList<Filter> filters, MetaData meta) {
		this.meta = meta;
		this.addFilter(filters);
	}
	
	protected NestedLoopJoinOperator(MetaData meta)
	{
		this.meta = meta;
	}
	
	public NestedLoopJoinOperator clone()
	{
		NestedLoopJoinOperator retval = new NestedLoopJoinOperator(meta);
		retval.f = this.getHSHMFilter();
		retval.node = node;
		retval.indexAccess = indexAccess;
		retval.dynamicIndexes = dynamicIndexes;
		retval.alreadySorted = alreadySorted;
		retval.rightChildCard = rightChildCard;
		retval.cardSet = cardSet;
		return retval;
	}
	
	public HashSet<HashMap<Filter, Filter>> getHSHM()
	{
		return getHSHMFilter();
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
	
	public ArrayList<String> getReferences()
	{
		ArrayList<String> retval = new ArrayList<String>(f.size());
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
		return retval;
	}

	public MetaData getMeta() {
		return meta;
	}

	public HashSet<HashMap<Filter, Filter>> getHSHMFilter() {
		return f;
	}

	public void addJoinCondition(ArrayList<Filter> filters) {
		addFilter(filters);
	}

	public void addJoinCondition(String left, String right) {
		throw new UnsupportedOperationException(
				"NestedLoopJoinOperator does not support addJoinCondition(String, String)");
	}

	public void addFilter(ArrayList<Filter> filters) {
		if (f == null) {
			f = new HashSet<HashMap<Filter, Filter>>();
			HashMap<Filter, Filter> map = new HashMap<Filter, Filter>();
			for (Filter filter : filters) {
				map.put(filter, filter);
			}

			f.add(map);
		} else {
			HashMap<Filter, Filter> map = new HashMap<Filter, Filter>();
			for (Filter filter : filters) {
				map.put(filter, filter);
			}

			f.add(map);
		}
	}

	public ArrayList<Operator> children() {
		return children;
	}

	public Operator parent() {
		return parent;
	}

	public String toString() {
		return "NestedLoopJoinOperator";
	}

	public void add(Operator op) throws Exception {
		if (children.size() < 2) {
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

			if (children.size() == 2 && children.get(0).getCols2Types() != null && children.get(1).getCols2Types() != null) {
				cols2Types = (HashMap<String, String>) children.get(0)
						.getCols2Types().clone();
				cols2Pos = (HashMap<String, Integer>) children.get(0)
						.getCols2Pos().clone();
				pos2Col = (TreeMap<Integer, String>) children.get(0)
						.getPos2Col().clone();

				cols2Types.putAll(children.get(1).getCols2Types());
				for (Map.Entry entry : children.get(1).getPos2Col().entrySet()) {
					cols2Pos.put((String) entry.getValue(), cols2Pos.size());
					pos2Col.put(pos2Col.size(), (String) entry.getValue());
				}
				cnfFilters = new CNFFilter(f, meta, cols2Pos);
			}
		} else {
			throw new Exception(
					"NestedLoopJoinOperator only supports 2 children");
		}
	}

	public void removeChild(Operator op) {
		childPos = children.indexOf(op);
		children.remove(op);
		op.removeParent(this);
	}

	public void removeParent(Operator op) {
		parent = null;
	}

	public void start() throws Exception 
	{
		if (!indexAccess)
		{
			inBuffer = ResourceManager.newDiskBackedArray(rightChildCard);
			for (Operator child : children) {
				child.start();
			}

			outBuffer = new BufferedLinkedBlockingQueue(Driver.QUEUE_SIZE);
			new InitThread().start();
		}
		else
		{
			for (Operator child : children) {
				child.start();
			}
			
			outBuffer = new BufferedLinkedBlockingQueue(Driver.QUEUE_SIZE);
		}
	}

	protected final class InitThread extends ThreadPoolThread {
		protected NLSortThread nlSort = null;
		protected NLHashThread nlHash = null;
		protected Filter first = null;
		
		public void run() {
			ThreadPoolThread[] threads = null;
			
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
				int i = 0;
				if (alreadySorted)
				{
					NUM_RT_THREADS = 1;
				}
				threads = new ReaderThread[NUM_RT_THREADS];
				while (i < NUM_RT_THREADS) {
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
				int i = 0;
				threads = new ReaderThread[NUM_RT_THREADS];
				while (i < NUM_RT_THREADS) {
					threads[i] = new ReaderThread();
					threads[i].start();
					i++;
				}
			}

			int i = 0;
			ThreadPoolThread[] threads2 = new ProcessThread[NUM_PTHREADS];
			while (i < NUM_PTHREADS) {
				if (nlSort == null && nlHash == null)
				{
					threads2[i] = new ProcessThread();
				}
				else if (nlSort != null)
				{
					threads2[i] = new ProcessThread(first);
				}
				else
				{
					threads2[i] = new ProcessThread(nlHash, first);
				}
				threads2[i].start();
				i++;
			}

			if (nlSort == null && nlHash == null)
			{
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
			}

			i = 0;
			while (i < NUM_PTHREADS) {
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

			while (true) {
				try {
					outBuffer.put(new DataEndMarker());
					inBuffer.close();
					break;
				} catch (Exception e) 
				{
					e.printStackTrace();
				}
			}
		}
	}

	protected final class ReaderThread extends ThreadPoolThread {
		public void run() {
			try {
				Operator child = children.get(1);
				Object o = child.next(NestedLoopJoinOperator.this);
				while (!(o instanceof DataEndMarker)) {
					inBuffer.add((ArrayList<Object>)o);
					o = child.next(NestedLoopJoinOperator.this);
				}
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(1);
			}
		}
	}
	
	protected final class NLHashThread extends ThreadPoolThread
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
				Object o = child.next(NestedLoopJoinOperator.this);
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
					o = child.next(NestedLoopJoinOperator.this);
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
		
		protected final void writeToHashTable(long hash, ArrayList<Object> row) throws Exception
		{
			if (buckets.size() == 0)
			{
				synchronized(buckets)
				{
					if (buckets.size() == 0)
					{
						buckets.add(ResourceManager.newDiskBackedHashMap(false, rightChildCard));
					}
				}
			}
			
			int i = 0;
			Object o = 0;
			while (o != null)
			{
				if (i < buckets.size())
				{
					o = null;
					while (o == null)
					{
						o = buckets.get(i);
					}
					o = ((DiskBackedHashMap)o).putIfAbsent(hash, row);
				}
				else
				{
					//synchronized(buckets)
					bucketsLock.lock();
					{
						if (i < buckets.size())
						{
							bucketsLock.unlock();
							o = null;
							while (o == null)
							{
								o = buckets.get(i);
							}
							o = ((DiskBackedHashMap)o).putIfAbsent(hash, row);
						}
						else
						{
							o = ResourceManager.newDiskBackedHashMap(false, rightChildCard / buckets.size());
							((DiskBackedHashMap)o).put(hash, row);
							buckets.add((DiskBackedHashMap)o);
							bucketsLock.unlock();
							o = null;
						}
					}
				}
				
				i++;
			}
			
			return;
		}
		
		protected final ArrayList<ArrayList<Object>> getCandidates(long hash) throws ClassNotFoundException, IOException
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
		
		protected long hash(Object key)
		{
			long eHash;
			if (key == null)
			{
				eHash = 0;
			}
			else
			{
				eHash = MurmurHash.hash64(key.toString());
			}
				
			return eHash;
		}
	}
	
	protected final class NLSortThread extends ThreadPoolThread
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
				ResourceManager.NO_OFFLOAD.getAndIncrement();
				ResourceManager.waitForSync();
				if (inBuffer.size() < SortOperator.PARALLEL_SORT_MIN_NUM_ROWS)
				{
					doSequentialSort(0, inBuffer.size()-1);
				}
				else
				{
					ParallelSortThread t = doParallelSort(0, inBuffer.size()-1);
					t.join();
				}
				ResourceManager.NO_OFFLOAD.getAndDecrement();
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
	
		protected final class ParallelSortThread extends ThreadPoolThread
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
						long pivotIndex = (long)(ThreadLocalRandom.current().nextDouble() * (right - left) + left);
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
			else if (lField instanceof MyDate)
			{
				result = ((MyDate)lField).compareTo((MyDate)rField);
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
	}

	protected final class ProcessThread extends ThreadPoolThread {
		protected Filter first = null;
		protected NLHashThread nlHash = null;
		
		public ProcessThread()
		{}
		
		public ProcessThread(Filter first)
		{
			this.first = first;
		}
		
		public ProcessThread(NLHashThread nlHash, Filter first)
		{
			this.nlHash = nlHash;
			this.first = first;
		}
		
		public void run() {
			try {
				Operator left = children.get(0);
				Object o = left.next(NestedLoopJoinOperator.this);
				int pos = -1;
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
				}
				//@Parallel
				while (!(o instanceof DataEndMarker)) 
				{
					ArrayList<Object> lRow = (ArrayList<Object>) o;
					long i = 0;
					while (true) 
					{
						if (nlHash != null)
						{
							ArrayList<Object> key = new ArrayList<Object>(1);
							try
							{
								key.add(lRow.get(pos));
							}
							catch(Exception e)
							{
								e.printStackTrace();
								System.out.println("pos = " + pos);
								System.out.println("lRow = " + lRow);
								System.out.println("key = " + key);
								System.exit(1);
							}
							long hash = 0x0EFFFFFFFFFFFFFFL & nlHash.hash(key);
							for (ArrayList<Object> rRow : nlHash.getCandidates(hash))
							{
								if (cnfFilters.passes(lRow, rRow)) 
								{
									ArrayList<Object> out = new ArrayList<Object>(
											lRow.size() + rRow.size());
									out.addAll(lRow);
									out.addAll(rRow);
									outBuffer.put(out);
									long count = outCount.incrementAndGet();
									if (count % 100000 == 0) {
										System.out
												.println("NestedLoopJoinOperator has output "
														+ count + " rows");
									}
								}
							}
							
							break;
						}
						
						if (i >= inBuffer.size()) 
						{
							if (readersDone) 
							{
								if (i >= inBuffer.size())
								{
									break;
								}
								
								continue;
							} 
							else 
							{
								LockSupport.parkNanos(75000);
								continue;
							}
						}
						Object orow = inBuffer.get(i);
						i++;
						ArrayList<Object> rRow = (ArrayList<Object>) orow;

						if (orow == null) {
							LockSupport.parkNanos(75000);
							continue;
						}

						if (cnfFilters.passes(lRow, rRow)) 
						{
							ArrayList<Object> out = new ArrayList<Object>(
									lRow.size() + rRow.size());
							out.addAll(lRow);
							out.addAll(rRow);
							outBuffer.put(out);
							long count = outCount.incrementAndGet();
							if (count % 100000 == 0) {
								System.out
										.println("NestedLoopJoinOperator has output "
												+ count + " rows");
							}
						}
						else
						{
							if (first != null)
							{
								if (!first.passes(lRow, rRow, cols2Pos))
								{
									break;
								}
							}
						}
					}

					o = left.next(NestedLoopJoinOperator.this);
				}
			} catch (Exception e) {
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

	public Object next(Operator op) throws Exception {
		if (indexAccess)
		{
			synchronized(queuedRows)
			{
				if (queuedRows.size() > 0)
				{
					return queuedRows.remove(0);
				}
			}

				while (true)
				{
					Object o = children.get(0).next(this);
					if (o instanceof DataEndMarker)
					{
						return o;
					}
					ArrayList<Filter> dynamics = new ArrayList<Filter>(this.cnfFilters.getALAL().size());
					int i = 0;
					for (ArrayList<Filter> al : this.cnfFilters.getALAL())
					{
						Filter f = al.get(0);
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
							else if (leftVal instanceof MyDate)
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
							else if (leftVal instanceof MyDate)
							{
								leftString = sdf.format(leftVal);
							}
							Filter f2 = new Filter(f.leftColumn(), f.op(), leftString);
							dynamics.add(f2);
						}
						
						i++;
					}

					Operator clone = null;	
					clone = getClone();
					synchronized(clone)
					{
						clone.reset();
						for (Index index : dynamicIndexes(children.get(1), clone.children().get(0)))
						{
							index.setDelayedConditions(deepClone(dynamics));
						}
				
						boolean retval = false;
						Object o2 = clone.next(this);
					
						while (!(o2 instanceof DataEndMarker))
						{
							ArrayList<Object> out = new ArrayList<Object>(((ArrayList<Object>)o).size() + ((ArrayList<Object>)o2).size());
							out.addAll((ArrayList<Object>)o);
							out.addAll((ArrayList<Object>)o2);
							synchronized(queuedRows)
							{
								queuedRows.add(out);
							}
							o2 = clone.next(this);
						}
					
						freeClone(clone);
					}	
					synchronized(queuedRows)
					{
						if (queuedRows.size() > 0)
						{
							return queuedRows.remove(0);
						}
					}
				}
		}
		
		Object o;
		o = outBuffer.take();

		if (o instanceof DataEndMarker) {
			o = outBuffer.peek();
			if (o == null) {
				outBuffer.put(new DataEndMarker());
				return new DataEndMarker();
			} else {
				outBuffer.put(new DataEndMarker());
				return o;
			}
		}
		return o;
	}

	public void close() throws Exception {
		for (Operator child : children) {
			child.close();
		}
		
		if (inBuffer != null)
		{
			inBuffer.close();
		}
		
		for (Operator o : clones)
		{
			o.close();
		}
	}

	public void registerParent(Operator op) throws Exception {
		if (parent == null) {
			parent = op;
		} else {
			throw new Exception(
					"NestedLoopJoinOperator can only have 1 parent.");
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
		for (HashMap<Filter, Filter> filters : f)
		{
			if (filters.size() == 1)
			{
				for (Filter filter : filters.keySet())
				{
					if (filter.op().equals("E") && filter.leftIsColumn() && filter.rightIsColumn())
					{
						return true;
					}
				}
			}
		}
		
		return false;
	}
	
	public ArrayList<Boolean> sortOrders()
	{
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
	
	public boolean usesSort()
	{
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
	
	public ArrayList<String> getJoinForChild(Operator op)
	{
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
	
	private Operator clone(Operator op)
	{
		Operator clone = op.clone();
		int i = 0;
		for (Operator o : op.children())
		{
			try
			{
				clone.add(clone(o));
				clone.setChildPos(op.getChildPos());
				if (o instanceof TableScanOperator)
				{
					CNFFilter cnf = ((TableScanOperator) o).getCNFForParent(op);
					if (cnf != null)
					{
						Operator child = clone.children().get(i);
						((TableScanOperator)child).setCNFForParent(clone, cnf);
					}
				}
				
				if (op instanceof TableScanOperator)
				{
					Operator child = clone.children().get(i);
					int device = -1;
					for (Map.Entry entry : (((TableScanOperator) op).device2Child).entrySet())
					{
						if (entry.getValue() == o)
						{
							device = (Integer)entry.getKey();
						}
					}
					((TableScanOperator) clone).setChildForDevice(device, child);
				}
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(1);
			}
			
			i++;
		}
		
		return clone;
	}
	
	private ArrayList<Index> dynamicIndexes(Operator model, Operator actual)
	{
		ArrayList<Index> retval = new ArrayList<Index>(dynamicIndexes.size());
		if (model instanceof IndexOperator)
		{
			if (dynamicIndexes.contains(((IndexOperator) model).index))
			{
				retval.add(((IndexOperator)actual).index);
			}
		}
		else
		{
			int i = 0;
			for (Operator o : model.children())
			{
				retval.addAll(dynamicIndexes(o, actual.children().get(i)));
				i++;
			}
		}
		
		return retval;
	}
	
	private ArrayList<Filter> deepClone(ArrayList<Filter> in)
	{
		ArrayList<Filter> out = new ArrayList<Filter>();
		for (Filter f : in)
		{
			out.add(f.clone());
		}
		
		return out;
	}
	
	private Operator getClone()
	{
		synchronized(clones)
		{
			int i = 0;
			while (i < lockVector.size())
			{
				AtomicBoolean lock = lockVector.get(i);
				if (!lock.get())
				{
					if (lock.compareAndSet(false, true))
					{	
						Operator retval = clones.get(i);
						return retval;
					}
				}
			
				i++;
			}
		
			Operator clone = clone(children.get(1));
			RootOperator root = new RootOperator(meta);
			try
			{
				root.add(clone);
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(1);
			}
			if (clone instanceof TableScanOperator)
			{
				if (((TableScanOperator) children.get(1)).orderedFilters.size() > 0)
				{
					((TableScanOperator) clone).setCNFForParent(root, ((TableScanOperator)children.get(1)).getCNFForParent(this));
				}
			}
			clone = root;
			clones.add(clone);
			lockVector.add(new AtomicBoolean(true));
		
			return clone;
		}
	}
	
	private void freeClone(Operator clone)
	{
		synchronized(clones)
		{
			int i = 0;
			while (i < clones.size())
			{
				Operator o = clones.get(i);
				if (clone == o)
				{
					lockVector.get(i).set(false);
					return;
				}
			
				i++;
			}
		}
	}
}
