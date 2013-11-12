package com.exascale.optimizer.testing;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Random;
import java.util.TreeMap;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import com.exascale.optimizer.testing.ResourceManager.DiskBackedArray;
import com.exascale.optimizer.testing.ResourceManager.DiskBackedHashMap;

public class SortOperator implements Operator, Serializable
{
	protected static int PARALLEL_SORT_MIN_NUM_ROWS = 10000;
	private Operator child;
	private Operator parent;
	private HashMap<String, String> cols2Types;
	private HashMap<String, Integer> cols2Pos;
	private TreeMap<Integer, String> pos2Col;
	private boolean startDone = false;
	private boolean closeDone = false;
	private Vector<String> sortCols;
	private Vector<Boolean> orders;
	private volatile boolean sortComplete = false;
	private SortThread sortThread;
	private volatile LinkedBlockingQueue readBuffer = new LinkedBlockingQueue(Driver.QUEUE_SIZE);
	private volatile DiskBackedArray result;
	private volatile int[] sortPos;
	private final int NUM_RT_THREADS = 6 * Runtime.getRuntime().availableProcessors();
	private MetaData meta;
	private volatile boolean isClosed = false;
	private volatile boolean done = false;
	
	public SortOperator(Vector<String> sortCols, Vector<Boolean> orders, MetaData meta)
	{
		this.sortCols = sortCols;
		this.orders = orders;
		this.meta = meta;
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
		ArrayList<Operator> retval = new ArrayList<Operator>();
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
	
	private class SortThread extends Thread
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
				result = ResourceManager.newDiskBackedArray();
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
		
		private ParallelSortThread doParallelSort(long left, long right)
		{
			//System.out.println("Starting parallel sort with " + (right-left+1) + " rows");
			ParallelSortThread t = new ParallelSortThread(left, right);
			t.start();
			return t;
		}
		
		private class ReaderThread extends Thread
		{
			private volatile DiskBackedArray array;
			
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
						array.add(o);
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
		
		private class CopyThread extends Thread
		{
			private volatile DiskBackedArray array;
			
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
						
						if (row != null)
						{
							readBuffer.put(row);
						}
						else
						{
							System.exit(1);
						}
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
		
		private class ParallelSortThread extends Thread
		{
			private long left;
			private long right;
			
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
						long pivotIndex = (long)(new Random().nextDouble() * (right - left) + left);
						ArrayList<Object> temp = (ArrayList<Object>)result.get(pivotIndex);
						result.update(pivotIndex, result.get(left));
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
						    	result.update(i, result.get(lt));
						    	result.update(lt, temp);
						    	i++;
						    	lt++;
						    }
						    else if (cmp > 0)
						    {
						    	result.update(i, result.get(gt));
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
		
		private void doSequentialSort(long left, long right) throws Exception
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
			    	result.update(i, result.get(lt));
			    	result.update(lt, temp);
			    	i++;
			    	lt++;
			    }
			    else if (cmp > 0)
			    {
			    	result.update(i, result.get(gt));
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
		
		private int compare(ArrayList<Object> lhs, ArrayList<Object> rhs) throws Exception
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
		
		private long partition(long left, long right, long pivotIndex) throws Exception
		{
			ArrayList<Object> pivotValue = (ArrayList<Object>)result.get(pivotIndex);
			Object rightRec = result.get(right);
			result.update(pivotIndex, rightRec);
			result.update(right,  pivotValue);
		
			long i = left;
			long storeIndex = left;
			boolean allEqual = true;
			
			Random random = new Random(System.currentTimeMillis());
			while (i < right)
			{
				ArrayList<Object> temp = (ArrayList<Object>)result.get(i);
				int compareResult = compare(temp, pivotValue);
				if (compareResult == -1)
				{
					Object row = result.get(storeIndex);
					result.update(i, row);
					result.update(storeIndex, temp);
					storeIndex++;
					allEqual = false;
				}
				else if (compareResult == 0)
				{
					if (random.nextDouble() < 0.5)
					{
						Object row = result.get(storeIndex);
						result.update(i, row);
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
			result.update(storeIndex, rightRec);
			result.update(right,  temp);
			return storeIndex;
		}
		
	}
}
