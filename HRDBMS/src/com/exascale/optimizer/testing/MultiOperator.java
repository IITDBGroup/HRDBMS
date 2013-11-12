package com.exascale.optimizer.testing;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.TreeMap;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.exascale.optimizer.testing.AggregateOperator.AggregateResultThread;
import com.exascale.optimizer.testing.ResourceManager.DiskBackedArray;
import com.exascale.optimizer.testing.ResourceManager.DiskBackedHashMap;

public class MultiOperator implements Operator, Serializable
{
	private Operator child;
	private Operator parent;
	private HashMap<String, String> cols2Types;
	private HashMap<String, Integer> cols2Pos;
	private TreeMap<Integer, String> pos2Col;
	private MetaData meta;
	private boolean startDone = false;
	private boolean closeDone = false;
	private Vector<AggregateOperator> ops;
	private Vector<String> groupCols;
	private volatile ArrayList<AggregateThread> inFlight = new ArrayList<AggregateThread>();
	private volatile LinkedBlockingQueue readBuffer = new LinkedBlockingQueue(Driver.QUEUE_SIZE);
	private boolean sorted;
	private static final int NUM_HGBR_THREADS = 6 * Runtime.getRuntime().availableProcessors();

	
	public MultiOperator(Vector<AggregateOperator> ops, Vector<String> groupCols, MetaData meta, boolean sorted)
	{
		this.ops = ops;
		this.groupCols = groupCols;
		this.meta = meta;
		this.sorted = sorted;
	}
	
	public ArrayList<String> getReferences()
	{
		ArrayList<String> retval = new ArrayList<String>();
		for (AggregateOperator op : ops)
		{
			retval.add(op.getInputColumn());
		}
		
		return retval;
	}
	
	public Vector<String> getKeys()
	{
		return groupCols;
	}
	
	public ArrayList<String> getOutputCols()
	{
		ArrayList<String> retval = new ArrayList<String>();
		for (AggregateOperator op : ops)
		{
			retval.add(op.outputColumn());
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
		ArrayList<Operator> retval = new ArrayList<Operator>();
		retval.add(child);
		return retval;
	}
	
	public String toString()
	{
		return "MultiOperator";
	}
	
	public synchronized void start() throws Exception 
	{
		//System.out.println("In start() in MultiOperator");
		if (!startDone)
		{
			startDone = true;
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
	}
	
	public synchronized void close() throws Exception 
	{
		if (!closeDone)
		{
			closeDone = true;
			child.close();
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
	
	public void add(Operator op) throws Exception
	{
		if (child == null)
		{
			child = op;
			op.registerParent(this);
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
	
	private void init()
	{
		new InitThread().start();
		new CleanerThread().start();
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
	
	private class HashGroupByThread extends Thread
	{
		private volatile ConcurrentHashMap<ArrayList<Object>, ArrayList<Object>> groups = new ConcurrentHashMap<ArrayList<Object>, ArrayList<Object>>();
		private AggregateResultThread[] threads = new AggregateResultThread[ops.size()];
		
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
				
					readBuffer.add(row);
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
		
		private class HashGroupByReaderThread extends Thread
		{
			public void run()
			{
				try
				{
					Object o = child.next(MultiOperator.this);
					while (! (o instanceof DataEndMarker))
					{
						ArrayList<Object> row = (ArrayList<Object>)o;
						ArrayList<Object> groupKeys = new ArrayList<Object>();
						for (String groupCol : groupCols)
						{
							groupKeys.add(row.get(child.getCols2Pos().get(groupCol)));
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
	
	private class InitThread extends Thread
	{	
		public void run()
		{
			try
			{
				Object[] groupKeys = new Object[groupCols.size()];
				Object[] oldGroup = null;
				DiskBackedArray rows = null;
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
							inFlight.add(aggThread);
							aggThread.start();
						}
						
						rows = ResourceManager.newDiskBackedArray();
					}
					
					rows.add(row);
					o = child.next(MultiOperator.this);
				}
			
				AggregateThread aggThread = new AggregateThread(groupKeys, ops, rows);
				inFlight.add(aggThread);
				aggThread.start();
				//System.out.println("Last aggregation thread has been started.");
			
				aggThread = new AggregateThread();
				inFlight.add(aggThread);
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(1);
			}
		}
	}
	
	private class AggregateThread
	{
		private ArrayList<Thread> threads = new ArrayList<Thread>();
		ArrayList<Object> row = new ArrayList<Object>();
		private boolean end = false;
		
		public AggregateThread(Object[] groupKeys, Vector<AggregateOperator> ops, DiskBackedArray rows)
		{
			for (Object o : groupKeys)
			{
				row.add(o);
			}
			
			for (AggregateOperator op : ops)
			{
				Thread thread = op.newProcessingThread(rows, child.getCols2Pos());
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
			
			for (Thread thread : threads)
			{
				thread.start();
			}
		}
		
		public ArrayList<Object> getResult()
		{
			for (Thread thread : threads)
			{
				AggregateResultThread t = (AggregateResultThread)thread;
				while (true)
				{
					try
					{
						t.join();
						break;
					}
					catch(InterruptedException e)
					{
						continue;
					}
				}
				row.add(t.getResult());
				t.close();
			}
			
			threads.clear();
			return row;
		}
	}
	
	private class CleanerThread extends Thread
	{	
		public void run()
		{
			int i = 0;
			while (true)
			{
				while (i >= inFlight.size())
				{
					try
					{
						Thread.currentThread().sleep(1);
					}
					catch(InterruptedException e) {}
				}	
				
				AggregateThread t = inFlight.get(i);
				if (t.isEnd())
				{
					while (true)
					{
						try
						{
							readBuffer.put(new DataEndMarker());
							break;
						}
						catch(InterruptedException e)
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
					catch(InterruptedException e)
					{}
				}
				//System.out.println("Picked up aggregation result.");
				i++;
			}
		}
	}
}
