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
import java.util.Vector;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import com.exascale.optimizer.testing.ResourceManager.DiskBackedArray;
import com.exascale.optimizer.testing.ResourceManager.DiskBackedHashMap;

public class NestedLoopJoinOperator extends JoinOperator implements Serializable 
{
	private ArrayList<Operator> children = new ArrayList<Operator>();
	private Operator parent;
	private HashMap<String, String> cols2Types;
	private HashMap<String, Integer> cols2Pos;
	private TreeMap<Integer, String> pos2Col;
	private MetaData meta;
	private volatile LinkedBlockingQueue outBuffer = new LinkedBlockingQueue(
			Driver.QUEUE_SIZE);
	private volatile DiskBackedArray inBuffer;
	private int NUM_RT_THREADS = 4 * Runtime.getRuntime().availableProcessors();
	private int NUM_PTHREADS = 4 * Runtime.getRuntime().availableProcessors();
	private AtomicLong outCount = new AtomicLong(0);
	private volatile boolean readersDone = false;
	private CNFFilter cnfFilters;
	private HashSet<HashMap<Filter, Filter>> f;
	private int childPos = -1;

	public NestedLoopJoinOperator(JoinOperator op) {
		this.meta = op.getMeta();
		this.f = op.getHSHMFilter();
	}

	public NestedLoopJoinOperator(Vector<Filter> filters, MetaData meta) {
		this.meta = meta;
		this.addFilter(filters);
	}
	
	public ArrayList<String> getReferences()
	{
		ArrayList<String> retval = new ArrayList<String>();
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

	public void addJoinCondition(Vector<Filter> filters) {
		addFilter(filters);
	}

	public void addJoinCondition(String left, String right) {
		throw new UnsupportedOperationException(
				"NestedLoopJoinOperator does not support addJoinCondition(String, String)");
	}

	public void addFilter(Vector<Filter> filters) {
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

			if (children.size() == 2) {
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
		inBuffer = ResourceManager.newDiskBackedArray();
		for (Operator child : children) {
			child.start();
		}

		new InitThread().start();

	}

	private class InitThread extends Thread {
		private NLSortThread nlSort = null;
		private NLHashThread nlHash = null;
		private Filter first = null;
		
		public void run() {
			Thread[] threads;
			int i = 0;
			threads = new ReaderThread[NUM_RT_THREADS];
			while (i < NUM_RT_THREADS) {
				threads[i] = new ReaderThread();
				threads[i].start();
				i++;
			}
			
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
									nlHash = new NLHashThread(filter);
									first = filter;
									nlHash.start();
									i = 0;
									while (i < NUM_RT_THREADS) {
										try {
											threads[i].join();
											i++;
										} catch (InterruptedException e) {
										}
									}
									readersDone = true;
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
										try {
											threads[i].join();
											i++;
										} catch (InterruptedException e) {
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

			i = 0;
			Thread[] threads2 = new ProcessThread[NUM_PTHREADS];
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
					try {
						threads[i].join();
						i++;
					} catch (InterruptedException e) {
					}
				}
				readersDone = true;
			}

			i = 0;
			while (i < NUM_PTHREADS) {
				try {
					threads2[i].join();
					i++;
				} catch (InterruptedException e) {
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

	private class ReaderThread extends Thread {
		public void run() {
			try {
				Operator child = children.get(1);
				Object o = child.next(NestedLoopJoinOperator.this);
				while (!(o instanceof DataEndMarker)) {
					inBuffer.add(o);
					o = child.next(NestedLoopJoinOperator.this);
				}
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(1);
			}
		}
	}
	
	private class NLHashThread extends Thread
	{
		private Filter filter;
		private int pos;
		private ArrayList<DiskBackedHashMap> buckets = new ArrayList<DiskBackedHashMap>();
		private AtomicLong inCount = new AtomicLong(0);
		
		public NLHashThread(Filter filter)
		{
			this.filter = filter;
			buckets.add(ResourceManager.newDiskBackedHashMap());
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
				long i = 0;
				while (true)
				{
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
							Thread.sleep(1);
							continue;
						}
					}
					
					ArrayList<Object> o = (ArrayList<Object>)inBuffer.get(i);
					if (o == null)
					{
						Thread.sleep(1);
						continue;
					}
					//inBuffer.add(o);
					long count = inCount.incrementAndGet();
					
					//if (count % 10000 == 0)
					//{
					//	System.out.println("Partial hash has read " + count + " rows");
					//}
					
					ArrayList<Object> key = new ArrayList<Object>();
					key.add(o.get(pos));
					
					long hash = 0x0EFFFFFFFFFFFFFFL & hash(key);
					writeToHashTable(hash, o);
					i++;
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
		
		private void writeToHashTable(long hash, ArrayList<Object> row) throws Exception
		{
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
							buckets.add(ResourceManager.newDiskBackedHashMap());
							DiskBackedHashMap bucket = buckets.get(i);
							o = bucket.putIfAbsent(hash, row);
						}
					}
					
				}
			}
		}
		
		private ArrayList<ArrayList<Object>> getCandidates(long hash) throws ClassNotFoundException, IOException
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
		
		private long hash(ArrayList<Object> key)
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
	
	private class NLSortThread extends Thread
	{
		private Filter filter;
		private boolean vBool;
		private int pos;
		
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
		
		private ParallelSortThread doParallelSort(long left, long right)
		{
			//System.out.println("Starting parallel sort with " + (right-left+1) + " rows");
			ParallelSortThread t = new ParallelSortThread(left, right);
			t.start();
			return t;
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
						inBuffer.update(pivotIndex, inBuffer.get(left));
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
								inBuffer.update(i, inBuffer.get(lt));
								inBuffer.update(lt, temp);
								i++;
								lt++;
							}
							else if (cmp > 0)
							{
								inBuffer.update(i, inBuffer.get(gt));
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
	
		private void doSequentialSort(long left, long right) throws Exception
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
					inBuffer.update(i, inBuffer.get(lt));
					inBuffer.update(lt, temp);
					i++;
					lt++;
				}
				else if (cmp > 0)
				{
					inBuffer.update(i, inBuffer.get(gt));
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
	
		private int compare(ArrayList<Object> lhs, ArrayList<Object> rhs) throws Exception
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
	
		private long partition(long left, long right, long pivotIndex) throws Exception
		{
			ArrayList<Object> pivotValue = (ArrayList<Object>)inBuffer.get(pivotIndex);
			Object rightRec = inBuffer.get(right);
			inBuffer.update(pivotIndex, rightRec);
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
					inBuffer.update(i, row);
					inBuffer.update(storeIndex, temp);
					storeIndex++;
					allEqual = false;
				}	
				else if (compareResult == 0)
				{
					if (random.nextDouble() < 0.5)
					{
						Object row = inBuffer.get(storeIndex);
						inBuffer.update(i, row);
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
			inBuffer.update(storeIndex, rightRec);
			inBuffer.update(right,  temp);
			return storeIndex;
		}
	}

	private class ProcessThread extends Thread {
		private Filter first = null;
		private NLHashThread nlHash = null;
		
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
				while (!(o instanceof DataEndMarker)) 
				{
					ArrayList<Object> lRow = (ArrayList<Object>) o;
					long i = 0;
					while (true) 
					{
						if (nlHash != null)
						{
							ArrayList<Object> key = new ArrayList<Object>();
							key.add(lRow.get(pos));
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
								Thread.sleep(1);
								continue;
							}
						}
						Object orow = inBuffer.get(i);
						i++;
						ArrayList<Object> rRow = (ArrayList<Object>) orow;

						if (orow == null) {
							Thread.sleep(1);
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

	public Object next(Operator op) throws Exception {
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
		
		inBuffer.close();
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
}
