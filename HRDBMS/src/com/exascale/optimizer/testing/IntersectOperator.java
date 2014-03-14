package com.exascale.optimizer.testing;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.TreeMap;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.exascale.optimizer.testing.ResourceManager.DiskBackedHashMap;
import com.exascale.optimizer.testing.ResourceManager.DiskBackedHashSet;
import com.exascale.threads.ReadThread;

public final class IntersectOperator implements Operator, Serializable
{
	protected MetaData meta;
	protected ArrayList<Operator> children = new ArrayList<Operator>();
	protected Operator parent;
	protected HashMap<String, String> cols2Types;
	protected HashMap<String, Integer> cols2Pos;
	protected TreeMap<Integer, String> pos2Col;
	protected int node;
	protected ArrayList<DiskBackedHashSet> sets = new ArrayList<DiskBackedHashSet>();
	protected BufferedLinkedBlockingQueue buffer;
	protected int estimate = 16;
	protected volatile boolean inited = false;
	protected volatile boolean startDone = false;
	
	public void reset()
	{
		if (!startDone)
		{
			try
			{
				start();
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(1);
			}
		}
		else
		{
			inited = false;
			for (Operator op : children)
			{
				op.reset();
			}
		
			for (DiskBackedHashSet set : sets)
			{
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
		
			sets = new ArrayList<DiskBackedHashSet>();
			buffer.clear();
			if (!inited)
			{
			}
			else
			{
				System.out.println("IntersectOperator is inited more than once!");
				Thread.dumpStack();
				System.exit(1);
			}
			new InitThread().start();
		}
	}
	
	public void setEstimate(int estimate)
	{
		this.estimate = estimate;
	}
	
	public void setChildPos(int pos)
	{
	}
	
	public int getChildPos()
	{
		return 0;
	}
	
	public MetaData getMeta()
	{
		return meta;
	}
	
	public IntersectOperator clone()
	{
		IntersectOperator retval = new IntersectOperator(meta);
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
	
	public IntersectOperator(MetaData meta)
	{
		this.meta = meta;
	}
	
	protected IntersectOperator()
	{}
	
	public ArrayList<String> getReferences()
	{
		return null;
	}
	
	public ArrayList<Operator> children()
	{
		return children;
	}
	
	public Operator parent()
	{
		return parent;
	}
	
	public void add(Operator op) throws Exception
	{
		children.add(op);
		op.registerParent(this);
		cols2Types = op.getCols2Types();
		cols2Pos = op.getCols2Pos();
		pos2Col = op.getPos2Col();
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
	public void start() throws Exception 
	{
		startDone = true;
		for (Operator op : children)
		{
			op.start();
		}
		
		buffer = new BufferedLinkedBlockingQueue(Driver.QUEUE_SIZE);
		if (!inited)
		{
		}
		else
		{
			System.out.println("IntersectOperator is inited more than once!");
			Thread.dumpStack();
			System.exit(1);
		}
		new InitThread().start();
	}
	
	protected final class ReadThread extends ThreadPoolThread
	{
		protected Operator op;
		protected int i;
		
		public ReadThread(int i)
		{
			this.i = i;
			op = children.get(i);
		}
		
		public void run()
		{
			try
			{
				DiskBackedHashSet set = sets.get(i);
				Object o = op.next(IntersectOperator.this);
				while (!(o instanceof DataEndMarker))
				{
					set.add((ArrayList<Object>)o);
					o = op.next(IntersectOperator.this);
				}
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(1);
			}
		}
	}
	
	protected final class InitThread extends ThreadPoolThread
	{
		protected ArrayList<ReadThread> threads = new ArrayList<ReadThread>(children.size());
		
		public void run()
		{
			if (!inited)
			{
				inited = true;
			}
			else
			{
				System.out.println("IntersectOperator is inited more than once!");
				Thread.dumpStack();
				System.exit(1);
			}
			if (children.size() == 1)
			{
				int count = 0;
				try
				{
					Object o = children.get(0).next(IntersectOperator.this);
					while (!(o instanceof DataEndMarker))
					{
						while (true)
						{
							try
							{
								buffer.put(o);
								count++;
								break;
							}
							catch(Exception e)
							{}
						}
					
						o = children.get(0).next(IntersectOperator.this);
					}
				
					System.out.println("Intersect operator returned " + count + " rows");
					
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
				catch(Exception f)
				{
					f.printStackTrace();
					System.exit(1);
				}
				
				return;
			}
			int i = 0;
			while (i < children.size())
			{
				ReadThread read = new ReadThread(i);
				threads.add(read);
				sets.add(ResourceManager.newDiskBackedHashSet(true, estimate));
				read.start();
				i++;
			}
			
			for (ReadThread read : threads)
			{
				while (true)
				{
					try
					{
						read.join();
						break;
					}
					catch(InterruptedException e)
					{}
				}
			}
			
			i = 0;
			long minCard = Long.MAX_VALUE;
			int minI = -1;
			for (ReadThread read : threads)
			{
				long card = sets.get(i).size();
				if (card < minCard)
				{
					minCard = card;
					minI = i;
				}
				
				i++;
			}
			
			int count = 0;
			for (Object o : sets.get(minI).getArray())
			{
				boolean inAll = true;
				for (DiskBackedHashSet set : sets)
				{
					if (!set.contains(o))
					{
						inAll = false;
						break;
					}
				}
				
				if (inAll)
				{
					while (true)
					{
						try
						{
							buffer.put(o);
							count++;
							break;
						}
						catch(Exception e)
						{}
					}
				}
			}
			
			System.out.println("Intersect operator returned " + count + " rows");
			
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

	public Object next(Operator op2) throws Exception
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

	@Override
	public void close() throws Exception 
	{
		for (DiskBackedHashSet set : sets)
		{
			set.getArray().close();
			set.close();
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
			throw new Exception("IntersectOperator only supports 1 parent.");
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
	
	public String toString()
	{
		return "IntersectOperator";
	}
}
