package com.exascale.optimizer.testing;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeMap;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import com.exascale.optimizer.testing.ResourceManager.DiskBackedArray;
import com.exascale.optimizer.testing.ResourceManager.DiskBackedHashMap;

public class HashJoinOperator extends JoinOperator implements Serializable
{
	private ArrayList<Operator> children = new ArrayList<Operator>();
	private Operator parent;
	private HashMap<String, String> cols2Types;
	private HashMap<String, Integer> cols2Pos;
	private TreeMap<Integer, String> pos2Col;
	private MetaData meta;
	private volatile LinkedBlockingQueue outBuffer = new LinkedBlockingQueue(Driver.QUEUE_SIZE);
	private int NUM_RT_THREADS = 2 * Runtime.getRuntime().availableProcessors();
	private int NUM_PTHREADS = 4 * Runtime.getRuntime().availableProcessors();
	private AtomicLong outCount = new AtomicLong(0);
	private volatile boolean readersDone = false;
	private Vector<String> lefts = new Vector<String>();
	private Vector<String> rights = new Vector<String>();
	private volatile ArrayList<DiskBackedHashMap> buckets;
	private CNFFilter cnfFilters;
	private HashSet<HashMap<Filter, Filter>> f;
	//private int HASH_BUCKETS = 5000;
	private int childPos = -1;
	private AtomicLong inCount = new AtomicLong(0);
	protected static final Long LARGE_PRIME =  1125899906842597L;
    protected static final Long LARGE_PRIME2 = 6920451961L;
    private AtomicLong leftCount = new AtomicLong(0);
	
	public HashJoinOperator(String left, String right, MetaData meta)
	{
		this.meta = meta;
		try
		{
			this.addFilter(left, right);
		}
		catch(Exception e)
		{
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	public ArrayList<String> getReferences()
	{
		ArrayList<String> retval = new ArrayList<String>(lefts);
		retval.addAll(rights);
		return retval;
	}
	
	public MetaData getMeta()
	{
		return meta;
	}
	
	public SelectOperator reverseUpdateSelectOperator(SelectOperator op, int pos)
	{
		SelectOperator retval = op.clone();
		if (pos == 0)
		{
			for (Filter filter : retval.getFilter())
			{
				if (filter.leftIsColumn() && rights.contains(filter.leftColumn()))
				{
					filter.updateLeftColumn(getLeftForRight(filter.leftColumn()));
				}
			
				if (filter.rightIsColumn() && rights.contains(filter.rightColumn()))
				{
					filter.updateRightColumn(getLeftForRight(filter.rightColumn()));
				}
			}
		}
		else
		{
			for (Filter filter : op.getFilter())
			{
				if (filter.leftIsColumn() && lefts.contains(filter.leftColumn()))
				{
					filter.updateLeftColumn(getRightForLeft(filter.leftColumn()));
				}
			
				if (filter.rightIsColumn() && lefts.contains(filter.rightColumn()))
				{
					filter.updateRightColumn(getRightForLeft(filter.rightColumn()));
				}
			}
		}
		
		retval.updateReferences();
		return retval;
	}
	
	private String getLeftForRight(String right)
	{
		int i = 0;
		for (String r : rights)
		{
			if (r.equals(right))
			{
				return lefts.get(i);
			}
			
			i++;
		}
		
		return null;
	}
	
	private String getRightForLeft(String left)
	{
		int i = 0;
		for (String l : lefts)
		{
			if (l.equals(left))
			{
				return rights.get(i);
			}
			
			i++;
		}
		
		return null;
	}
	
	public void reverseUpdateReferences(ArrayList<String> references, int pos)
	{
		if (pos == 0)
		{
			int i = 0;
			for (String rName : rights)
			{
				ArrayList<String> temp = new ArrayList<String>();
				temp.add(rName);
				if (references.removeAll(temp))
				{
					String lName = lefts.get(i);
					references.add(lName);
				}
				
				i++;
			}
		}
		else
		{
			int i = 0;
			for (String lName : lefts)
			{
				ArrayList<String> temp = new ArrayList<String>();
				temp.add(lName);
				if (references.removeAll(temp))
				{
					String rName = rights.get(i);
					references.add(rName);
				}
				
				i++;
			}
		}
	}
	
	public HashSet<HashMap<Filter, Filter>> getHSHMFilter()
	{
		return f;
	}
	
	public void addJoinCondition(Vector<Filter> filters)
	{
		throw new UnsupportedOperationException("addJoinCondition(Vector<Filter>) is not supported by HashJoinOperator");
	}
	
	public void addJoinCondition(String left, String right)
	{
		try
		{
			addFilter(left, right);
		}
		catch(Exception e)
		{
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	public void addFilter(String left, String right) throws Exception
	{
		if (f == null)
		{
			f = new HashSet<HashMap<Filter, Filter>>();
			HashMap<Filter, Filter> map = new HashMap<Filter, Filter>();
			map.put(new Filter(left, "E", right), new Filter(left, "E", right));
			f.add(map);
		}
		else
		{
			HashMap<Filter, Filter> map = new HashMap<Filter, Filter>();
			map.put(new Filter(left, "E", right), new Filter(left, "E", right));
			f.add(map);
		}
		lefts.add(left);
		rights.add(right);
	}
	
	public ArrayList<Operator> children()
	{
		return children;
	}
	
	public Operator parent()
	{
		return parent;
	}
	
	public String toString()
	{
		return "HashJoinOperator: " + lefts.toString() + "," + rights.toString();
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
			
			if (children.size() == 2)
			{
				cols2Types = (HashMap<String, String>)children.get(0).getCols2Types().clone();
				cols2Pos = (HashMap<String, Integer>)children.get(0).getCols2Pos().clone();
				pos2Col = (TreeMap<Integer,String>)children.get(0).getPos2Col().clone();
				
				cols2Types.putAll(children.get(1).getCols2Types());
				for (Map.Entry entry : children.get(1).getPos2Col().entrySet())
				{
					cols2Pos.put((String)entry.getValue(), cols2Pos.size());
					pos2Col.put(pos2Col.size(), (String)entry.getValue());
				}
				
				cnfFilters = new CNFFilter(f, meta, cols2Pos);
				int i = 0;
				while (i <lefts.size())
				{
					//swap left/right if needed
					String left = lefts.get(i);
					if (!children.get(0).getCols2Pos().containsKey(left))
					{
						String right = rights.get(i);
						lefts.remove(left);
						rights.remove(right);
						lefts.add(i, right);
						rights.add(i, left);
					}
					
					i++;
				}
			}
		}
		else
		{
			throw new Exception("HashJoinOperator only supports 2 children");
		}
	}
	
	public void removeChild(Operator op)
	{
		childPos = children.indexOf(op);
		if (childPos == -1)
		{
			System.out.println("Child doesn't exist!");
			System.out.println("I am " + this.toString());
			System.out.println("Children is " + children);
			System.out.println("Trying to remove " + op);
		}
		children.remove(op);
		op.removeParent(this);
	}
	
	public void removeParent(Operator op)
	{
		parent = null;
	}
	
	public void start() throws Exception 
	{
		buckets = new ArrayList<DiskBackedHashMap>();
		buckets.add(ResourceManager.newDiskBackedHashMap());
		for (Operator child : children)
		{
			child.start();
		}
		
		new InitThread().start();
		
	}
	
	private class InitThread extends Thread
	{
		public void run()
		{
			Thread[] threads;
			int i = 0;
			threads = new ReaderThread[NUM_RT_THREADS];
			while (i < NUM_RT_THREADS)
			{
				threads[i] = new ReaderThread();
				threads[i].start();
				i++;
			}
			
			i = 0;
			while (i < NUM_RT_THREADS)
			{
				try
				{
					threads[i].join();
					i++;
				}
				catch(InterruptedException e)
				{
				}
			}
			
			readersDone = true;
			
			i = 0;
			Thread[] threads2 = new ProcessThread[NUM_PTHREADS];
			while (i < NUM_PTHREADS)
			{
				threads2[i] = new ProcessThread();
				threads2[i].start();
				i++;
			}
			
			i = 0;
			while (i < NUM_PTHREADS)
			{
				try
				{
					threads2[i].join();
					i++;
				}
				catch(InterruptedException e)
				{
				}
			}
			
			//System.out.println("HashJoinThread processed " + leftCount + " left rows and " + inCount + " right rows");
			
			while (true)
			{
				try
				{
					outBuffer.put(new DataEndMarker());
					for (DiskBackedHashMap bucket : buckets)
					{
						bucket.close();
					}
					break;
				}
				catch(Exception e)
				{
					e.printStackTrace();
				}
			}
		}
	}
	
	private class ReaderThread extends Thread
	{	
		public void run()
		{
			try
			{
				Operator child = children.get(1);
				HashMap<String, Integer> childCols2Pos = child.getCols2Pos();
				Object o = child.next(HashJoinOperator.this);
				while (! (o instanceof DataEndMarker))
				{
					//inBuffer.add(o);
					long count = inCount.incrementAndGet();
					
					//if (count % 10000 == 0)
					//{
					//	System.out.println("HashJoinOperator has read " + count + " rows");
					//}
					
					ArrayList<Object> key = new ArrayList<Object>();
					for (String col : rights)
					{
						int pos = childCols2Pos.get(col);
						key.add(((ArrayList<Object>)o).get(pos));
					}
					
					long hash = 0x0EFFFFFFFFFFFFFFL & hash(key);
					writeToHashTable(hash, (ArrayList<Object>)o);
					o = child.next(HashJoinOperator.this);
				}
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.out.println("Error in hash join reader thread.");
				System.out.println("Lefts is: " + lefts);
				System.out.println("Rights is: " + rights);
				System.out.println("Left child cols to pos is: " + children.get(0).getCols2Pos());
				System.out.println("Right child cols to pos is: " + children.get(1).getCols2Pos());
				System.exit(1);
			}
		}
	}
	
	private class ProcessThread extends Thread
	{
		public void run()
		{
			try
			{
				while (!readersDone)
				{
					Thread.sleep(1);
				}
				
				Operator left = children.get(0);
				HashMap<String, Integer> childCols2Pos = left.getCols2Pos();
				Object o = left.next(HashJoinOperator.this);
				while (! (o instanceof DataEndMarker))
				{
					ArrayList<Object> lRow = (ArrayList<Object>)o;
					ArrayList<Object> key = new ArrayList<Object>();
					for (String col : lefts)
					{
						int pos = childCols2Pos.get(col);
						key.add(lRow.get(pos));
					}
					
					long hash = 0x0EFFFFFFFFFFFFFFL & hash(key);
					ArrayList<ArrayList<Object>> candidates = getCandidates(hash);	
					for (ArrayList<Object> rRow : candidates)
					{
						if (cnfFilters.passes(lRow, rRow))
						{
							ArrayList<Object> out = new ArrayList<Object>(lRow.size() + rRow.size());
							out.addAll(lRow);
							out.addAll(rRow);
							outBuffer.put(out);
							long count = outCount.incrementAndGet();
							//if (count % 100000 == 0)
							//{
							//	System.out.println("HashJoinOperator has output " + count + " rows");
							//}
						}
					}
					
					leftCount.incrementAndGet();
					o = left.next(HashJoinOperator.this);
				}
			}
			catch(Exception e)
			{
				e.printStackTrace();
				e.printStackTrace();
				System.out.println("Error in hash join reader thread.");
				System.out.println("Lefts is: " + lefts);
				System.out.println("Rights is: " + rights);
				System.out.println("Left child cols to pos is: " + children.get(0).getCols2Pos());
				System.out.println("Right child cols to pos is: " + children.get(1).getCols2Pos());
				System.exit(1);
			}
		}
	}
	
	public Object next(Operator op) throws Exception
	{
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
		
		for (DiskBackedHashMap bucket : buckets)
		{
			bucket.close();
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
			throw new Exception("HashJoinOperator can only have 1 parent.");
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
			    long scaled = i * LARGE_PRIME;

			    // Fill in the lower bits
			    eHash = scaled + LARGE_PRIME2;
			}
			else if (e instanceof Long)
			{
				long i = (Long)e;
				// Spread out values
			    long scaled = i * LARGE_PRIME;

			    // Fill in the lower bits
			    eHash = scaled + LARGE_PRIME2;
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
			    long scaled = i * LARGE_PRIME;

			    // Fill in the lower bits
			    eHash = scaled + LARGE_PRIME2;
			}
			else if (e instanceof Date)
			{
				long i = ((Date)e).getTime();
				// Spread out values
			    long scaled = i * LARGE_PRIME;

			    // Fill in the lower bits
			    eHash = scaled + LARGE_PRIME2;
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
