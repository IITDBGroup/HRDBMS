package com.exascale.optimizer.testing;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.exascale.optimizer.testing.ResourceManager.DiskBackedHashMap;
import com.exascale.optimizer.testing.ResourceManager.DiskBackedHashSet;

public class UnionOperator implements Operator, Serializable
{
	protected ArrayList<Operator> children = new ArrayList<Operator>();
	protected MetaData meta;
	protected HashMap<String, String> cols2Types;
	protected HashMap<String, Integer> cols2Pos;
	protected TreeMap<Integer, String> pos2Col;
	protected Operator parent;
	protected HashMap<String, Integer> childCols2Pos;
	protected int node;
	protected boolean distinct;
	protected BufferedLinkedBlockingQueue buffer = new BufferedLinkedBlockingQueue(Driver.QUEUE_SIZE);
	protected DiskBackedHashSet set;
	protected AtomicLong counter = new AtomicLong(0);
	protected int estimate = 16;
	
	public void setChildPos(int pos)
	{
	}
	
	public void setEstimate(int estimate)
	{
		this.estimate = estimate;
	}
	
	public int getChildPos()
	{
		return 0;
	}
	
	public UnionOperator(boolean distinct, MetaData meta)
	{
		this.meta = meta;
		this.distinct = distinct;
	}
	
	public UnionOperator clone()
	{
		UnionOperator retval = new UnionOperator(distinct, meta);
		retval.node = node;
		retval.estimate = estimate;
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
	
	public ArrayList<String> getReferences()
	{
		ArrayList<String> retval = new ArrayList<String>(0);
		return retval;
	}
	
	public MetaData getMeta()
	{
		return meta;
	}
	
	public Operator parent()
	{
		return parent;
	}
	
	public ArrayList<Operator> children()
	{
		return children;
	}
	
	public String toString()
	{
		return "UnionOperator";
	}
	
	@Override
	public void start() throws Exception 
	{
		for (Operator child : children)
		{
			child.start();
		}
			
		buffer.clear();
		new InitThread().start();
	}
	
	public void reset()
	{
		for (Operator child : children)
		{
			child.reset();
		}
			
		buffer.clear();
		new InitThread().start();
	}
	
	private class InitThread extends ThreadPoolThread
	{
		protected ArrayList<ReadThread> threads = new ArrayList<ReadThread>(children.size());
		
		public void run()
		{
			if (distinct && children.size() > 1)
			{
				set = ResourceManager.newDiskBackedHashSet(true, estimate);
			}
			
			//System.out.println("Union operator has " + children.size() + " children");
			
			for (Operator op : children)
			{
				ReadThread thread = new ReadThread(op);
				threads.add(thread);
				thread.start();
			}
			
			for (ReadThread thread : threads)
			{
				while (true)
				{
					try
					{
						thread.join();
						break;
					}
					catch(InterruptedException e)
					{
					}
				}
			}
			
			if (distinct && children.size() > 1)
			{
				for (Object o : set.getArray())
				{
					while (true)
					{
						try
						{
							buffer.put(o);
							break;
						}
						catch(Exception e)
						{}
					}
				}
				
				//System.out.println("Union operator returned " + set.getArray().size() + " rows");
				
				try
				{
					set.getArray().close();
					set.close();
				}
				catch(Exception e)
				{
					e.printStackTrace();
					System.exit(1);
				}
			}
			else
			{
				//System.out.println("Union operator returned " + counter.get() + " rows");
			}
			
			while (true)
			{
				try
				{
					buffer.put(new DataEndMarker());
					break;
				}
				catch(Exception e)
				{}
			}
		}
	}
	
	private class ReadThread extends ThreadPoolThread
	{
		protected Operator op;
		
		public ReadThread(Operator op)
		{
			this.op = op;
		}
		
		public void run()
		{
			try
			{
				Object o = null;
				o = op.next(UnionOperator.this);
				while (!(o instanceof DataEndMarker))
				{
					if (distinct && children.size() > 1)
					{
						set.add((ArrayList<Object>)o);
					}
					else
					{
						while (true)
						{
							try
							{
								buffer.put(o);
								counter.getAndIncrement();
								break;
							}
							catch(Exception e)
							{}
						}
					}

					o = op.next(UnionOperator.this);
				}
			}
			catch(Exception f)
			{
				f.printStackTrace();
				System.exit(1);
			}
		}
	}
	
	public void nextAll(Operator op) throws Exception
	{
		for (Operator o : children)
		{
			o.nextAll(op);
		}
		
		Object o = next(op);
		while (!(o instanceof DataEndMarker))
		{
			o = next(op);
		}
	}

	public Object next(Operator op) throws Exception 
	{
		Object o;
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

	public void close() throws Exception 
	{
		for (Operator child : children)
		{
			child.close();
		}
	}
	
	public void removeChild(Operator op)
	{
		children.remove(op);
		op.removeParent(this);
	}
	
	public void removeParent(Operator op)
	{
		parent = null;
	}

	@Override
	public void add(Operator op) throws Exception 
	{
		children.add(op);
		op.registerParent(this);
		cols2Pos = op.getCols2Pos();
		cols2Types = op.getCols2Types();
		pos2Col = op.getPos2Col();
	}

	@Override
	public void registerParent(Operator op) throws Exception
	{
		if (parent == null)
		{
			parent = op;
		}
		else
		{
			throw new Exception("UnionOperator only supports 1 parent.");
		}
	}

	public HashMap<String, String> getCols2Types() {
		return cols2Types;
	}

	public HashMap<String, Integer> getCols2Pos() {
		return cols2Pos;
	}

	public TreeMap<Integer, String> getPos2Col() {
		return pos2Col;
	}

}
