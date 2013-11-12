package com.exascale.optimizer.testing;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;


import com.exascale.optimizer.testing.ResourceManager.DiskBackedHashMap;

public class TopOperator implements Operator, Serializable
{
	private Operator child;
	private Operator parent;
	private HashMap<String, String> cols2Types;
	private HashMap<String, Integer> cols2Pos;
	private TreeMap<Integer, String> pos2Col;
	private MetaData meta;
	private AtomicLong remaining;
	private boolean cleanerStarted = false;
	
	public TopOperator(long numVals, MetaData meta)
	{
		this.remaining = new AtomicLong(numVals);
		this.meta = meta;
	}
	
	public ArrayList<String> getReferences()
	{
		ArrayList<String> retval = new ArrayList<String>();
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
		ArrayList<Operator> retval = new ArrayList<Operator>();
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

	@Override
	public Object next(Operator op) throws Exception 
	{
		long num = remaining.getAndDecrement();
		
		if (num > 0)
		{
			return child.next(this);
		}
		
		if (!cleanerStarted)
		{
			CleanerThread ct = new CleanerThread();
			ct.start();
			cleanerStarted = true;
			ct.join();
		}
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
	
	private class CleanerThread extends Thread
	{
		public void run()
		{
			try
			{
				Object o = child.next(TopOperator.this);
				while (! (o instanceof DataEndMarker))
				{
					o = child.next(TopOperator.this);
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
