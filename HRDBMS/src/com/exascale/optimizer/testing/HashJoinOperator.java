package com.exascale.optimizer.testing;

import java.io.IOException;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeMap;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

import com.exascale.optimizer.testing.ResourceManager.DiskBackedArray;
import com.exascale.optimizer.testing.ResourceManager.DiskBackedHashMap;

public class HashJoinOperator extends JoinOperator implements Serializable
{
	protected ArrayList<Operator> children = new ArrayList<Operator>(2);
	protected Operator parent;
	protected HashMap<String, String> cols2Types;
	protected HashMap<String, Integer> cols2Pos;
	protected TreeMap<Integer, String> pos2Col;
	protected MetaData meta;
	protected volatile BufferedLinkedBlockingQueue outBuffer = new BufferedLinkedBlockingQueue(Driver.QUEUE_SIZE);
	protected int NUM_RT_THREADS = 2 * ResourceManager.cpus;
	protected int NUM_PTHREADS = 4 * ResourceManager.cpus;
	protected AtomicLong outCount = new AtomicLong(0);
	protected volatile boolean readersDone = false;
	protected ArrayList<String> lefts = new ArrayList<String>();
	protected ArrayList<String> rights = new ArrayList<String>();
	protected volatile ArrayList<DiskBackedHashMap> buckets;
	protected CNFFilter cnfFilters;
	protected HashSet<HashMap<Filter, Filter>> f;
	protected int HASH_BUCKETS = 5000;
	protected int childPos = -1;
	protected AtomicLong inCount = new AtomicLong(0);
	protected static final Long LARGE_PRIME =  1125899906842597L;
    protected static final Long LARGE_PRIME2 = 6920451961L;
    protected AtomicLong leftCount = new AtomicLong(0);
    protected ArrayList<Object> lastRightRow;
    protected int node;
    protected boolean indexAccess = false;
    protected ArrayList<Index> dynamicIndexes;
    protected ArrayList<ArrayList<Object>> queuedRows = new ArrayList<ArrayList<Object>>();
    protected SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    protected volatile boolean doReset = false;
    protected int rightChildCard = 16;
    protected boolean cardSet = false;
    
    public void setDynamicIndex(ArrayList<Index> indexes)
    {
    	indexAccess = true;
    	this.dynamicIndexes = indexes;
    }
    
    public void reset()
	{
		System.out.println("HashJoinOperator cannot be reset");
		System.exit(1);
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
    
    public ArrayList<String> getJoinForChild(Operator op)
    {
    	if (op.getCols2Pos().keySet().containsAll(lefts))
    	{
    		return new ArrayList<String>(lefts);
    	}
    	else
    	{
    		return new ArrayList<String>(rights);
    	}
    }
    
    public void setChildPos(int pos)
	{
		childPos = pos;
	}
	
	public int getChildPos()
	{
		return childPos;
	}
	
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
	
	protected HashJoinOperator(HashSet<HashMap<Filter, Filter>> f, ArrayList<String> lefts, ArrayList<String> rights, MetaData meta)
	{
		this.meta = meta;
		this.f = f;
		this.lefts = lefts;
		this.rights = rights;
	}
	
	public HashJoinOperator clone()
	{
		HashJoinOperator retval = new HashJoinOperator(f, lefts, rights, meta);
		retval.node = node;
		retval.indexAccess = indexAccess;
		retval.dynamicIndexes = dynamicIndexes;
		retval.rightChildCard = rightChildCard;
		retval.cardSet = cardSet;
		return retval;
	}
	
	public HashSet<HashMap<Filter, Filter>> getHSHM()
	{
		if (f != null)
		{
			return getHSHMFilter();
		}
		
		HashSet<HashMap<Filter, Filter>> retval = new HashSet<HashMap<Filter, Filter>>();
		int i = 0;
		for (String col : rights)
		{
			Filter filter = null;
			try
			{
				filter = new Filter(lefts.get(i), "E", col);
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
	
	public int getNode()
	{
		return node;
	}
	
	public void setNode(int node)
	{
		this.node = node;
	}
	
	public ArrayList<String> getLefts()
	{
		return lefts;
	}
	
	public ArrayList<String> getRights()
	{
		return rights;
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
	
	protected String getLeftForRight(String right)
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
	
	protected String getRightForLeft(String left)
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
				ArrayList<String> temp = new ArrayList<String>(1);
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
				ArrayList<String> temp = new ArrayList<String>(1);
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
	
	public void addJoinCondition(ArrayList<Filter> filters)
	{
		throw new UnsupportedOperationException("addJoinCondition(ArrayList<Filter>) is not supported by HashJoinOperator");
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
			
			if (children.size() == 2 && children.get(0).getCols2Types() != null && children.get(1).getCols2Types() != null)
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
		if (!indexAccess)
		{
			buckets = new ArrayList<DiskBackedHashMap>();
			buckets.add(ResourceManager.newDiskBackedHashMap(false, rightChildCard));
			for (Operator child : children)
			{
				child.start();
				//System.out.println("cols2Pos = " + child.getCols2Pos());
			}
		
			new InitThread().start();
		}
		else
		{
			System.out.println("HashJoinOperator is started with index access");
			for (Operator child : children)
			{
				child.start();
				//System.out.println("cols2Pos = " + child.getCols2Pos());
			}
		}
		
	}
	
	protected class InitThread extends ThreadPoolThread
	{
		public void run()
		{
			ThreadPoolThread[] threads;
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
			
			i = 0;
			ThreadPoolThread[] threads2 = new ProcessThread[NUM_PTHREADS];
			while (i < NUM_PTHREADS)
			{
				threads2[i] = new ProcessThread();
				threads2[i].start();
				i++;
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
			
			System.out.println("HashJoinThread processed " + leftCount + " left rows and " + inCount + " right rows and generated " + outCount + " rows");
			
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
	
	protected class ReaderThread extends ThreadPoolThread
	{	
		public void run()
		{
			try
			{
				Operator child = children.get(1);
				HashMap<String, Integer> childCols2Pos = child.getCols2Pos();
				Object o = child.next(HashJoinOperator.this);
				//@Parallel
				while (! (o instanceof DataEndMarker))
				{
					//inBuffer.add(o);
					long count = inCount.incrementAndGet();
					
					//if (count % 10000 == 0)
					//{
					//	System.out.println("HashJoinOperator has read " + count + " rows");
					//}
					
					ArrayList<Object> key = new ArrayList<Object>(rights.size());
					for (String col : rights)
					{
						int pos = childCols2Pos.get(col);
						key.add(((ArrayList<Object>)o).get(pos));
					}
					
					long hash = 0x0EFFFFFFFFFFFFFFL & hash(key);
					writeToHashTable(hash, (ArrayList<Object>)o);
					lastRightRow = (ArrayList<Object>)o;
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
	
	protected class ProcessThread extends ThreadPoolThread
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
				ArrayList<Object> key = new ArrayList<Object>(lefts.size());
				//@Parallel
				while (! (o instanceof DataEndMarker))
				{
					ArrayList<Object> lRow = (ArrayList<Object>)o;
					key.clear();
					for (String col : lefts)
					{
						int pos = childCols2Pos.get(col);
						try
						{
							key.add(lRow.get(pos));
						}
						catch(Exception e)
						{
							System.out.println("Row - " + lRow);
							System.out.println("Cols2Pos = " + childCols2Pos);
							e.printStackTrace();
							System.exit(1);
						}
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
			synchronized(this)
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
					
					ArrayList<Filter> dynamics = new ArrayList<Filter>(rights.size());
					int i = 0;
					for (String right : rights)
					{
						Object leftVal = ((ArrayList<Object>)o).get(children.get(0).getCols2Pos().get(lefts.get(i)));
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
						Filter f = new Filter(leftString, "E", right);
						dynamics.add(f);
						i++;
					}
				
					Operator clone = clone(children.get(1));
					clone.start();
					
					if (!doReset)
					{
						doReset = true;
						for (Index index : dynamicIndexes)
						{
							index.setDelayedConditions(deepClone(dynamics));
						}
						
						children.get(1).nextAll(HashJoinOperator.this);
					}
					
					for (Index index : dynamicIndexes(children.get(1), clone))
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
					
					clone.close();
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
		
		if (buckets != null)
		{
			for (DiskBackedHashMap bucket : buckets)
			{
				bucket.close();
			}
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
	
	protected void writeToHashTable(long hash, ArrayList<Object> row) throws Exception
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
						buckets.add(ResourceManager.newDiskBackedHashMap(false, rightChildCard / buckets.size()));
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
	
	private Operator clone(Operator op)
	{
		Operator clone = op.clone();
		int i = 0;
		for (Operator o : op.children())
		{
			try
			{
				clone.add(clone(o));
				
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
}
