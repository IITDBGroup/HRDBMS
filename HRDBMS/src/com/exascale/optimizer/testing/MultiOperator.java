package com.exascale.optimizer.testing;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeMap;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.exascale.optimizer.testing.AggregateOperator.AggregateResultThread;
import com.exascale.optimizer.testing.ResourceManager.DiskBackedArray;
import com.exascale.optimizer.testing.ResourceManager.DiskBackedHashMap;

public final class MultiOperator implements Operator, Serializable
{
	protected Operator child;
	protected Operator parent;
	protected HashMap<String, String> cols2Types;
	protected HashMap<String, Integer> cols2Pos;
	protected TreeMap<Integer, String> pos2Col;
	protected MetaData meta;
	protected ArrayList<AggregateOperator> ops;
	protected ArrayList<String> groupCols;
	protected static final int ATHREAD_QUEUE_SIZE = BufferedLinkedBlockingQueue.BLOCK_SIZE < 1000 ? 1000 : BufferedLinkedBlockingQueue.BLOCK_SIZE;
	protected volatile BufferedLinkedBlockingQueue inFlight = new BufferedLinkedBlockingQueue(ATHREAD_QUEUE_SIZE);
	protected volatile BufferedLinkedBlockingQueue readBuffer = new BufferedLinkedBlockingQueue(Driver.QUEUE_SIZE);
	protected boolean sorted;
	protected static final int NUM_HGBR_THREADS = ResourceManager.cpus;
	protected int node;
	protected int NUM_GROUPS = 16;
	protected int childCard = 16 * 16;
	protected boolean cardSet = false;
	
	public void reset()
	{
		child.reset();
		inFlight = new BufferedLinkedBlockingQueue(ATHREAD_QUEUE_SIZE);
		readBuffer.clear();
		if (sorted)
		{
			init();
		}
		else
		{
			new HashGroupByThread().start();
		}
	}
	
	public void setChildPos(int pos)
	{
	}
	
	public int getChildPos()
	{
		return 0;
	}
	
	public MultiOperator(ArrayList<AggregateOperator> ops, ArrayList<String> groupCols, MetaData meta, boolean sorted)
	{
		this.ops = ops;
		this.groupCols = groupCols;
		this.meta = meta;
		this.sorted = sorted;
	}
	
	public MultiOperator clone()
	{
		ArrayList<AggregateOperator> opsClone = new ArrayList<AggregateOperator>(ops.size());
		for (AggregateOperator op : ops)
		{
			opsClone.add(op.clone());
		}
		
		MultiOperator retval =  new MultiOperator(opsClone, groupCols, meta, sorted);
		retval.node = node;
		retval.NUM_GROUPS = NUM_GROUPS;
		retval.childCard = childCard;
		retval.cardSet = cardSet;
		return retval;
	}
	
	public boolean setNumGroupsAndChildCard(int groups, int childCard)
	{
		if (cardSet)
		{
			return false;
		}
		
		cardSet = true;
		NUM_GROUPS = groups;
		this.childCard = childCard; 
		for (AggregateOperator op : ops)
		{
			op.setNumGroups(NUM_GROUPS);
			if (op instanceof CountDistinctOperator)
			{
				((CountDistinctOperator)op).setChildCard(childCard);
			}
		}
		
		return true;
	}
	
	public int getNode()
	{
		return node;
	}
	
	public void setNode(int node)
	{
		this.node = node;
	}
	
	public String getAvgCol()
	{
		for (AggregateOperator op : ops)
		{
			if (op instanceof AvgOperator)
			{
				return op.outputColumn();
			}
		}
		
		return null;
	}
	
	public void changeCountsToSums()
	{
		ArrayList<AggregateOperator> remove = new ArrayList<AggregateOperator>();
		ArrayList<AggregateOperator> add = new ArrayList<AggregateOperator>();
		for (AggregateOperator op : ops)
		{
			if (op instanceof CountOperator)
			{
				remove.add(op);
				add.add(new SumOperator(op.getInputColumn(), op.outputColumn(), meta, true));
			}
		}
		
		int i = 0;
		for (AggregateOperator op : remove)
		{
			int pos = ops.indexOf(op);
			ops.remove(pos);
			ops.add(pos, add.get(i));
			i++;
		}
	}
	
	public void updateInputColumns(ArrayList<String> outputs, ArrayList<String> inputs)
	{
		for (AggregateOperator op : ops)
		{
			int index = outputs.indexOf(op.outputColumn());
			op.setInputColumn(inputs.get(index));
		}
	}
	
	public void removeCountDistinct()
	{
		for (AggregateOperator op : ops)
		{
			if (op instanceof CountDistinctOperator)
			{
				groupCols.add(((CountDistinctOperator)op).getInputColumn());
				ops.remove(op);
			}
		}
	}
	
	public void addCount(String outCol)
	{
		ops.add(new CountOperator(outCol, meta));
	}
	
	public void replaceAvgWithSumAndCount(HashMap<String, ArrayList<String>> old2New)
	{
		for (AggregateOperator op : ops)
		{
			if (op instanceof AvgOperator)
			{
				String outCol1 = null;
				String outCol2 = null;
				for (Map.Entry entry : old2New.entrySet())
				{
					outCol1 = ((ArrayList<String>)entry.getValue()).get(0);
					outCol2 = ((ArrayList<String>)entry.getValue()).get(1);
				}
				ops.remove(op);
				ops.add(new SumOperator(op.getInputColumn(), outCol1, meta, false));
				ops.add(new CountOperator(op.getInputColumn(), outCol2, meta));
				return;
			}
		}
	}
	
	public boolean hasAvg()
	{
		for (AggregateOperator op : ops)
		{
			if (op instanceof AvgOperator)
			{
				return true;
			}
		}
		
		return false;
	}
	
	public ArrayList<String> getReferences()
	{
		ArrayList<String> retval = new ArrayList<String>(ops.size());
		for (AggregateOperator op : ops)
		{
			retval.add(op.getInputColumn());
		}
		
		return retval;
	}
	
	public ArrayList<String> getKeys()
	{
		return groupCols;
	}
	
	public ArrayList<String> getOutputCols()
	{
		ArrayList<String> retval = new ArrayList<String>(ops.size());
		for (AggregateOperator op : ops)
		{
			retval.add(op.outputColumn());
		}
		
		return retval;
	}
	
	public ArrayList<String> getInputCols()
	{
		ArrayList<String> retval = new ArrayList<String>(ops.size());
		for (AggregateOperator op : ops)
		{
			retval.add(op.getInputColumn());
		}
		
		return retval;
	}
	
	public Operator parent()
	{
		return parent;
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
	
	public String toString()
	{
		String retval = "MultiOperator: [";
		int i = 0;
		for (String in : getInputCols())
		{
			retval += (in + "->" + getOutputCols().get(i) + "  ");
			i++;
		}
		
		retval += "]";
		return retval;
	}
	
	public void start() throws Exception 
	{
		child.start();
		if (sorted)
		{
			init();
		}
		else
		{
			//System.out.println("HasGroupByThread created via start()");
			new HashGroupByThread().start();
		}
	}
	
	public void setSorted()
	{
		sorted = true;
	}
	
	public void close() throws Exception 
	{
		child.close();
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
	
	public void add(Operator op) throws Exception
	{
		if (child == null)
		{
			child = op;
			op.registerParent(this);
			if (child.getCols2Types() != null)
			{
				HashMap<String, String> tempCols2Types = child.getCols2Types();
				HashMap<String, Integer> tempCols2Pos = child.getCols2Pos();
				cols2Types = new HashMap<String, String>();
				cols2Pos = new HashMap<String, Integer>();
				pos2Col = new TreeMap<Integer, String>();
			
				int i = 0;
				for (String groupCol : groupCols)
				{
					cols2Types.put(groupCol, tempCols2Types.get(groupCol));
					cols2Pos.put(groupCol, i);
					pos2Col.put(i, groupCol);
					i++;
				}
			
				for (AggregateOperator op2 : ops)
				{
					cols2Types.put(op2.outputColumn(), op2.outputType());
					cols2Pos.put(op2.outputColumn(), i);
					pos2Col.put(i, op2.outputColumn());
					i++;
				}
			}
		}
		else
		{
			throw new Exception("MultiOperator only supports 1 child.");
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
			throw new Exception("MultiOperator only supports 1 parent.");
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
	
	protected void init()
	{
		new InitThread().start();
		new CleanerThread().start();
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
	
	protected final class HashGroupByThread extends ThreadPoolThread
	{
		protected volatile ConcurrentHashMap<ArrayList<Object>, ArrayList<Object>> groups = new ConcurrentHashMap<ArrayList<Object>, ArrayList<Object>>(NUM_GROUPS, 0.75f, ResourceManager.cpus * 6);
		protected AggregateResultThread[] threads = new AggregateResultThread[ops.size()];
		
		public void run()
		{
			try
			{
				int i = 0;
				for (AggregateOperator op : ops)
				{
					if (op == null)
					{
						System.out.println("Op is null");
					}
					if (cols2Pos == null)
					{
						System.out.println("Cols2pos is null");
					}
					threads[i] = op.getHashThread(child.getCols2Pos());
					i++;
				}
				
				i = 0;
				HashGroupByReaderThread[] threads2= new HashGroupByReaderThread[NUM_HGBR_THREADS];
				while (i < NUM_HGBR_THREADS)
				{
					threads2[i] = new HashGroupByReaderThread();
					threads2[i].start();
					i++;
				}
				
				i = 0;
				while (i < NUM_HGBR_THREADS)
				{
					threads2[i].join();
					i++;
				}
			
				for (ArrayList<Object> keys : groups.keySet())
				{
					ArrayList<Object> row = new ArrayList<Object>();
					for (Object field : keys)
					{
						row.add(field);
					}
					
					for (AggregateResultThread thread : threads)
					{
						row.add(thread.getResult(keys));
					}
				
					readBuffer.put(row);
				}
			
				readBuffer.put(new DataEndMarker());
				
				for (AggregateResultThread thread : threads)
				{
					thread.close();
				}
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(1);
			}
		}
		
		protected final class HashGroupByReaderThread extends ThreadPoolThread
		{
			protected ArrayList<Integer> groupPos = null;
			
			public void run()
			{
				try
				{
					Object o = child.next(MultiOperator.this);
					while (! (o instanceof DataEndMarker))
					{
						ArrayList<Object> row = (ArrayList<Object>)o;
						ArrayList<Object> groupKeys = new ArrayList<Object>();
						
						if (groupPos == null)
						{
							groupPos = new ArrayList<Integer>(groupCols.size());
							for (String groupCol : groupCols)
							{
								groupPos.add(child.getCols2Pos().get(groupCol));
							}
						}
						
						for (int pos : groupPos)
						{
							groupKeys.add(row.get(pos));
						}
			
						groups.put(groupKeys, groupKeys);
			
						for (AggregateResultThread thread : threads)
						{
							thread.put(row, groupKeys);
						}
		
						o = child.next(MultiOperator.this);
					}
				}
				catch(Exception e)
				{
					e.printStackTrace();
					System.exit(1);
				}
			}
		}
	}
	
	protected final class InitThread extends ThreadPoolThread
	{	
		public void run()
		{
			try
			{
				Object[] groupKeys = new Object[groupCols.size()];
				Object[] oldGroup = null;
				ArrayList<ArrayList<Object>> rows = null;
				boolean newGroup = false;;
			
				Object o = child.next(MultiOperator.this);
				while (! (o instanceof DataEndMarker))
				{
					newGroup = false;
					oldGroup = null;
					ArrayList<Object> row = (ArrayList<Object>)o;
					int i = 0;
					for (String groupCol : groupCols)
					{
						if (row.get(child.getCols2Pos().get(groupCol)).equals(groupKeys[i]))
						{}
						else
						{
							newGroup = true;
							if (oldGroup == null)
							{
								oldGroup = groupKeys.clone();
							}
							groupKeys[i] = row.get(child.getCols2Pos().get(groupCol));
						}
					
						i++;
					}
				
					if (newGroup)
					{
						//DEBUG
						//for (Object obj : groupKeys)
						//{
						//	System.out.println("Key: " + obj);
						//}
						//DEBUG
						
						if (rows != null)
						{
							AggregateThread aggThread = new AggregateThread(oldGroup, ops, rows);
							aggThread.start();
							while (true)
							{
								try
								{
									inFlight.put(aggThread);
									break;
								}
								catch(Exception f)
								{}
							}
						}
						
						rows = new ArrayList<ArrayList<Object>>();
					}
					
					rows.add(row);
					o = child.next(MultiOperator.this);
				}
			
				AggregateThread aggThread = new AggregateThread(groupKeys, ops, rows);
				aggThread.start();
				while (true)
				{
					try
					{
						inFlight.put(aggThread);
						break;
					}
					catch(Exception f)
					{}
				}
				//System.out.println("Last aggregation thread has been started.");
			
				aggThread = new AggregateThread();
				while (true)
				{
					try
					{
						inFlight.put(aggThread);
						break;
					}
					catch(Exception f)
					{}
				}
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(1);
			}
		}
	}
	
	protected final class AggregateThread
	{
		private ArrayList<ThreadPoolThread> threads = new ArrayList<ThreadPoolThread>();
		ArrayList<Object> row = new ArrayList<Object>();
		protected boolean end = false;
		
		public AggregateThread(Object[] groupKeys, ArrayList<AggregateOperator> ops, ArrayList<ArrayList<Object>> rows)
		{
			for (Object o : groupKeys)
			{
				row.add(o);
			}
			
			for (AggregateOperator op : ops)
			{
				ThreadPoolThread thread = op.newProcessingThread(rows, child.getCols2Pos());
				threads.add(thread);
			}
		}
		
		public AggregateThread()
		{
			end = true;
		}
		
		public boolean isEnd()
		{
			return end;
		}
		
		public void start()
		{
			if (end)
			{
				return;
			}
			
			for (ThreadPoolThread thread : threads)
			{
				//thread.start();
				thread.run();
			}
		}
		
		public ArrayList<Object> getResult()
		{
			for (ThreadPoolThread thread : threads)
			{
				AggregateResultThread t = (AggregateResultThread)thread;
				//while (true)
				//{
				//	try
				//	{
				//		t.join();
				//		break;
				//	}
				//	catch(InterruptedException e)
				//	{
				//		continue;
				//	}
				//}
				row.add(t.getResult());
				t.close();
			}
			
			threads.clear();
			return row;
		}
	}
	
	protected final class CleanerThread extends ThreadPoolThread
	{	
		public void run()
		{
			while (true)
			{
				AggregateThread t = null;
				while (true)
				{
					try
					{
						t = (AggregateThread)inFlight.take();
						break;
					}
					catch(Exception e)
					{}
				}
				if (t.isEnd())
				{
					while (true)
					{
						try
						{
							readBuffer.put(new DataEndMarker());
							break;
						}
						catch(Exception e)
						{}
					}
					//System.out.println("MultiOperator marked end of output.");
					return;
				}
			
				while (true)
				{
					try
					{
						readBuffer.put(t.getResult());
						break;
					}
					catch(Exception e)
					{}
				}
				//System.out.println("Picked up aggregation result.");
			}
		}
	}
}
