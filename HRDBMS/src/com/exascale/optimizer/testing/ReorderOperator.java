package com.exascale.optimizer.testing;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

import com.exascale.optimizer.testing.ResourceManager.DiskBackedHashMap;

public class ReorderOperator implements Operator, Serializable
{
	private Operator child;
	private Operator parent;
	private HashMap<String, String> cols2Types;
	private HashMap<String, Integer> cols2Pos;
	private TreeMap<Integer, String> pos2Col;
	private Vector<String> order;
	private MetaData meta;
	private boolean nullOp = false;
	
	public ReorderOperator(Vector<String> order, MetaData meta)
	{
		this.order = order;
		this.meta = meta;
	}
	
	public ArrayList<String> getReferences()
	{
		ArrayList<String> retval = new ArrayList<String>(order);
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
		return "ReorderOperator";
	}
	
	@Override
	public void start() throws Exception 
	{
		child.start();
	}

	@Override
	public Object next(Operator op) throws Exception 
	{
		if (nullOp)
		{
			return child.next(this);
		}
		else
		{
			Object o = child.next(this);
			if (o instanceof DataEndMarker)
			{
				return o;
			}
			
			ArrayList<Object> row = (ArrayList<Object>)o;
			ArrayList<Object> retval = new ArrayList<Object>();
			for (String col : order)
			{
				try
				{
					retval.add(row.get(child.getCols2Pos().get(col)));
				}
				catch(Exception e)
				{
					e.printStackTrace();
					System.out.println("Error in ReorderOperator looking for " + col + " in " + child.getCols2Pos());
					System.exit(1);
				}
			}
			
			return retval;
		}
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
			child.registerParent(this);
			pos2Col = child.getPos2Col();
			if (pos2Col.equals(order))
			{
				cols2Types = child.getCols2Types();
				cols2Pos = child.getCols2Pos();
				nullOp = true;
			}
			else
			{
				cols2Types = new HashMap<String, String>();
				cols2Pos = new HashMap<String, Integer>();
				pos2Col = new TreeMap<Integer, String>();
				int i = 0;
				for (String col : order)
				{
					cols2Types.put(col, child.getCols2Types().get(col));
					cols2Pos.put(col, i);
					pos2Col.put(i,  col);
					i++;
				}
			}
		}
		else
		{
			throw new Exception("ReorderOperator only supports 1 child.");
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
			throw new Exception("ReorderOperator only supports 1 parent.");
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
