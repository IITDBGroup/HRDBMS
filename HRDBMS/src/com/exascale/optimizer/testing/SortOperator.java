package com.exascale.optimizer.testing;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Random;
import java.util.TreeMap;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

import com.exascale.optimizer.testing.ResourceManager.DiskBackedArray;
import com.exascale.optimizer.testing.ResourceManager.DiskBackedHashMap;

public class SortOperator implements Operator, Serializable
{
	protected static int PARALLEL_SORT_MIN_NUM_ROWS = 10000;
	protected Operator child;
	protected Operator parent;
	protected HashMap<String, String> cols2Types;
	protected HashMap<String, Integer> cols2Pos;
	protected TreeMap<Integer, String> pos2Col;
	protected boolean startDone = false;
	protected boolean closeDone = false;
	protected ArrayList<String> sortCols;
	protected ArrayList<Boolean> orders;
	protected volatile boolean sortComplete = false;
	protected SortThread sortThread;
	protected volatile BufferedLinkedBlockingQueue readBuffer = new BufferedLinkedBlockingQueue(Driver.QUEUE_SIZE);
	protected volatile DiskBackedArray result;
	protected volatile int[] sortPos;
	protected final int NUM_RT_THREADS = 1; //6 * ResourceManager.cpus;
	protected MetaData meta;
	protected volatile boolean isClosed = false;
	protected volatile boolean done = false;
	protected int node;
	protected int childCard = 16;
	protected boolean cardSet = false;
	
	public void setChildPos(int pos)
	{
	}
	
	public boolean setChildCard(int card)
	{
		if (cardSet)
		{
			return false;
		}
		
		cardSet = true;
		childCard = card;
		return true;
	}
	
	public void reset()
	{
		child.reset();
		
		if (result != null)
		{
			try
			{
				result.close();
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(1);
			}
		}
		
		sortComplete = false;
		readBuffer.clear();
		sortThread = new SortThread(child);
		sortThread.start();
	}
	
	public int getChildPos()
	{
		return 0;
	}
	
	public SortOperator(ArrayList<String> sortCols, ArrayList<Boolean> orders, MetaData meta)
	{
		this.sortCols = sortCols;
		this.orders = orders;
		this.meta = meta;
	}
	
	public SortOperator clone()
	{
		SortOperator retval = new SortOperator(sortCols, orders, meta);
		retval.node = node;
		retval.childCard = childCard;
		retval.cardSet = cardSet;
		return retval;
	}
	
	public int getNode()
	{
		return node;
	}
	
	public void setNode(int node)
	{
		this.node = node;
	}
	
	public ArrayList<String> getKeys()
	{
		return sortCols;
	}
	
	public ArrayList<Boolean> getOrders()
	{
		return orders;
	}
	
	public ArrayList<String> getReferences()
	{
		return new ArrayList<String>(sortCols);
	}
	
	public boolean done()
	{
		return done;
	}
	
	public long size()
	{
		return result.size();
	}
	
	public MetaData getMeta()
	{
		return meta;
	}
	
	public ArrayList<Operator> children()
	{
		ArrayList<Operator> retval = new ArrayList<Operator>(1);
		retval.add(child);
		return retval;
	}
	
	public Operator parent()
	{
		return parent;
	}
	
	public String toString()
	{
		return "SortOperator";
	}
	
	public synchronized void start() throws Exception 
	{
		if (!startDone)
		{
			startDone = true;
			child.start();
			sortThread = new SortThread(child);
			sortThread.start();
		}
	}
	
	public synchronized void close() throws Exception 
	{
		if (!closeDone)
		{
			closeDone = true;
			child.close();
		}
		
		result.close();
		isClosed = true;
	}
	
	public void add(Operator op) throws Exception
	{
		if (child == null)
		{
			child = op;
			op.registerParent(this);
			cols2Types = child.getCols2Types();
			cols2Pos = child.getCols2Pos();
			pos2Col = child.getPos2Col();
			int i = 0;
			sortPos = new int[sortCols.size()];
			for (String sortCol : sortCols)
			{
				sortPos[i]= cols2Pos.get(sortCol);
				i++;
			}
		}
		else
		{
			throw new Exception("SortOperator only supports 1 child.");
		}
	}
	
	public void removeChild(Operator op)
	{
		if (op == child)
		{
			child = null;
			op.removeParent(this);
		}
	}
	
	public void removeParent(Operator op)
	{
		parent = null;
	}
	
	public void registerParent(Operator op) throws Exception
	{
		if (parent == null)
		{
			parent = op;
		}
		else
		{
			throw new Exception("SortOperator only supports 1 parent.");
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
	
	public void nextAll(Operator op) throws Exception
	{
		child.nextAll(op);
		Object o = next(op);
		while (!(o instanceof DataEndMarker))
		{
			o = next(op);
		}
	}
	
	public Object next(Operator op) throws Exception
	{
		if (!sortComplete)
		{
			sortThread.join();
			//System.out.println("Sort done!");
			sortComplete = true;
		}
		
		Object o;
		o = readBuffer.take();
		
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
	
	protected class SortThread extends ThreadPoolThread
	{
		Operator child;
		ReaderThread[] threads;
		CopyThread ct;
		
		public SortThread(Operator child)
		{
			this.child = child;
		}
		
		public DiskBackedArray getResult()
		{
			return result;
		}
		
		public void run()
		{	
			try
			{
				result = ResourceManager.newDiskBackedArray(childCard);
				int i = 0;
				
				threads = new ReaderThread[NUM_RT_THREADS];
				while (i < NUM_RT_THREADS)
				{
					threads[i] = new ReaderThread(result);
					threads[i].start();
					i++;
				}
				
				i = 0;
				while (i < NUM_RT_THREADS)
				{
					threads[i].join();
					i++;
				}
				
				if (result.size() < PARALLEL_SORT_MIN_NUM_ROWS)
				{
					doSequentialSort(0, result.size()-1);
				}
				else
				{
					ParallelSortThread t = doParallelSort(0, result.size()-1);
					t.join();
				}
				
				done = true;
				
				ct = new CopyThread(result);
				ct.start();
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(1);
			}
		}
		
		//@?Parallel (recursive)
		protected ParallelSortThread doParallelSort(long left, long right)
		{
			//System.out.println("Starting parallel sort with " + (right-left+1) + " rows");
			ParallelSortThread t = new ParallelSortThread(left, right);
			t.start();
			return t;
		}
		
		protected class ReaderThread extends ThreadPoolThread
		{
			protected volatile DiskBackedArray array;
			
			public ReaderThread(DiskBackedArray array)
			{
				this.array = array;
			}
			
			public void run()
			{
				try
				{
					Object o = child.next(SortOperator.this);
					while (! (o instanceof DataEndMarker))
					{
						array.add((ArrayList<Object>)o);
						o = child.next(SortOperator.this);
					}
				}
				catch(Exception e)
				{
					e.printStackTrace();
					System.exit(1);
				}
			}
		}
		
		protected class CopyThread extends ThreadPoolThread
		{
			protected volatile DiskBackedArray array;
			
			public CopyThread(DiskBackedArray array)
			{
				this.array = array;
			}
			
			public void run()
			{
				try
				{
					long size = array.size();
					long i = 0;
					long firstStart = System.currentTimeMillis();
					long start = firstStart;
					long nullCount = 0; //DEBUG
					while (i < size)
					{
						ArrayList<Object> row = null;
						while (row == null)
						{
							try
							{
								row = (ArrayList<Object>)array.get(i);
							}
							catch(Exception e)
							{
								e.printStackTrace();
								System.out.println("Array is " + array);
								System.out.println("isClosed = " + isClosed);
							}
						}
						
						readBuffer.put(row);
						
						i++;
						
						long end = System.currentTimeMillis();
						//if (end - start > 10000)
						//{
						//	System.out.println("CopyThread is moving " + ((i * 1.0) / ((end - firstStart) / 1000.0)) + " records/sec");
						//	start = end;
						//}
					}
					
					//System.out.println(nullCount + "/" + size + " records were null after sort.");
				
					readBuffer.put(new DataEndMarker());
					array.close();
				}
				catch(Exception e)
				{
					e.printStackTrace();
					System.exit(1);
				}
			}
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
					if (((right - left) < PARALLEL_SORT_MIN_NUM_ROWS))
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
						ArrayList<Object> temp = (ArrayList<Object>)result.get(pivotIndex);
						result.update(pivotIndex, (ArrayList<Object>)result.get(left));
						result.update(left, temp);
						long lt = left;
						long gt = right;
						ArrayList<Object> v = temp;
						long i = left;
						while (i <= gt) 
						{
							temp = (ArrayList<Object>)result.get(i);
							int cmp = compare(temp, v);
						    if (cmp < 0)
						    {
						    	result.update(i, (ArrayList<Object>)result.get(lt));
						    	result.update(lt, temp);
						    	i++;
						    	lt++;
						    }
						    else if (cmp > 0)
						    {
						    	result.update(i, (ArrayList<Object>)result.get(gt));
						    	result.update(gt, temp);
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
			ArrayList<Object> v = (ArrayList<Object>)result.get(left);
			long i = left;
			while (i <= gt) 
			{
				ArrayList<Object> temp = (ArrayList<Object>)result.get(i);
				int cmp = compare(temp, v);
			    if (cmp < 0)
			    {
			    	result.update(i, (ArrayList<Object>)result.get(lt));
			    	result.update(lt, temp);
			    	i++;
			    	lt++;
			    }
			    else if (cmp > 0)
			    {
			    	result.update(i, (ArrayList<Object>)result.get(gt));
			    	result.update(gt, temp);
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
			
			for (int pos : sortPos)
			{
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
				
				if (orders.get(i))
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
				
				i++;
			}
			
			return 0;
		}
		
		protected long partition(long left, long right, long pivotIndex) throws Exception
		{
			ArrayList<Object> pivotValue = (ArrayList<Object>)result.get(pivotIndex);
			Object rightRec = result.get(right);
			result.update(pivotIndex, (ArrayList<Object>)rightRec);
			result.update(right,  pivotValue);
		
			long i = left;
			long storeIndex = left;
			boolean allEqual = true;
			
			while (i < right)
			{
				ArrayList<Object> temp = (ArrayList<Object>)result.get(i);
				int compareResult = compare(temp, pivotValue);
				if (compareResult == -1)
				{
					Object row = result.get(storeIndex);
					result.update(i, (ArrayList<Object>)row);
					result.update(storeIndex, temp);
					storeIndex++;
					allEqual = false;
				}
				else if (compareResult == 0)
				{
					if (ThreadLocalRandom.current().nextDouble() < 0.5)
					{
						Object row = result.get(storeIndex);
						result.update(i, (ArrayList<Object>)row);
						result.update(storeIndex, temp);
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
			
			ArrayList<Object> temp = (ArrayList<Object>)result.get(storeIndex);
			rightRec = result.get(right);
			result.update(storeIndex, (ArrayList<Object>)rightRec);
			result.update(right,  temp);
			return storeIndex;
		}
		
	}
}
