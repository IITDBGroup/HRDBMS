package com.exascale.optimizer.testing;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;


import com.exascale.optimizer.testing.ResourceManager.DiskBackedHashMap;

public final class TopOperator implements Operator, Serializable
{
	protected Operator child;
	protected Operator parent;
	protected HashMap<String, String> cols2Types;
	protected HashMap<String, Integer> cols2Pos;
	protected TreeMap<Integer, String> pos2Col;
	protected MetaData meta;
	protected AtomicLong remaining;
	protected boolean cleanerStarted = false;
	protected int node;
	
	public long getRemaining()
	{
		return remaining.get();
	}
	
	public void reset()
	{
		System.out.println("TopOperator cannot reset");
		System.exit(1);
	}
	
	public void setChildPos(int pos)
	{
	}
	
	public int getChildPos()
	{
		return 0;
	}
	
	public TopOperator(long numVals, MetaData meta)
	{
		this.remaining = new AtomicLong(numVals);
		this.meta = meta;
	}
	
	public TopOperator clone()
	{
		TopOperator retval = new TopOperator(remaining.get(), meta);
		retval.node = node;
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
		ArrayList<Operator> retval = new ArrayList<Operator>(1);
		retval.add(child);
		return retval;
	}
	
	public String toString()
	{
		return "TopOperator";
	}
	
	@Override
	public void start() throws Exception 
	{
		child.start();
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

	@Override
	public Object next(Operator op) throws Exception 
	{
		long num = remaining.getAndDecrement();
		
		if (num > 0)
		{
			return child.next(this);
		}
		
		CleanerThread ct = new CleanerThread();
		ct.start();
		ct.join();
		return new DataEndMarker();
	}

	@Override
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

	@Override
	public void add(Operator op) throws Exception 
	{
		if (child == null)
		{
			child = op;
			cols2Pos = child.getCols2Pos();
			cols2Types = child.getCols2Types();
			pos2Col = child.getPos2Col();
			child.registerParent(this);
		}
		else
		{
			throw new Exception("RenameOperator only supports 1 child.");
		}
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
			throw new Exception("RenameOperator only supports 1 parent.");
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
	
	protected final class CleanerThread extends ThreadPoolThread
	{
		public void run()
		{
			try
			{
				child.nextAll(TopOperator.this);
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(1);
			}
		}
	}

}
