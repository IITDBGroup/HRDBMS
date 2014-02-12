package com.exascale.optimizer.testing;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

import com.exascale.optimizer.testing.ResourceManager.DiskBackedHashMap;

public final class ProjectOperator implements Operator, Serializable
{
	protected Operator child;
	protected ArrayList<String> cols;
	protected MetaData meta;
	protected HashMap<String, String> cols2Types;
	protected HashMap<String, Integer> cols2Pos;
	protected TreeMap<Integer, String> pos2Col;
	protected Operator parent;
	protected HashMap<String, Integer> childCols2Pos;
	protected int node;
	protected ArrayList<Integer> pos2Get = new ArrayList<Integer>();
	protected ArrayList<Integer> toRemove = new ArrayList<Integer>();
	protected boolean add;
	
	public void setChildPos(int pos)
	{
	}
	
	public void reset()
	{
		child.reset();
	}
	
	public int getChildPos()
	{
		return 0;
	}
	
	public ProjectOperator(ArrayList<String> cols, MetaData meta)
	{
		this.cols = cols;
		this.meta = meta;
	}
	
	public ProjectOperator clone()
	{
		ProjectOperator retval = new ProjectOperator(cols, meta);
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
		ArrayList<String> retval = new ArrayList<String>(cols);
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
		return "ProjectOperator";
	}
	
	@Override
	public void start() throws Exception 
	{
		child.start();
		for (String col : pos2Col.values())
		{
			pos2Get.add(childCols2Pos.get(col));
		}
		int i = 0;
		int removed = 0;
		for (String col : child.getPos2Col().values())
		{
			if (!cols2Pos.containsKey(col))
			{
				toRemove.add(i-removed);
				removed++;
			}
			i++;
		}
		add = pos2Get.size() < childCols2Pos.size() / 2;
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
	//@?Parallel
	public Object next(Operator op) throws Exception 
	{
		Object o = child.next(this);
		if (o instanceof DataEndMarker)
		{
			return o;
		}
		
		ArrayList<Object> row = (ArrayList<Object>)o; 
		if (add)
		{
			ArrayList<Object> retval = new ArrayList<Object>(pos2Get.size());
			for (int pos : pos2Get)
			{
				retval.add(row.get(pos));
			}
		
			return retval;
		}
		else
		{
			for (int remove : toRemove)
			{
				row.remove(remove);
			}
			
			return row;
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
			if (child.getCols2Types() != null)
			{
				childCols2Pos = child.getCols2Pos();
				Map temp = (HashMap<String, String>)child.getCols2Types().clone();
				cols2Types = new HashMap<String, String>();
				Set<Map.Entry> set = temp.entrySet();
				for (Map.Entry entry : set)
				{
					if (cols.contains(entry.getKey()))
					{
						cols2Types.put((String)entry.getKey(), (String)entry.getValue());
					}
				}
			
				temp = (TreeMap<Integer, String>)child.getPos2Col();
				cols2Pos = new HashMap<String, Integer>();
				pos2Col = new TreeMap<Integer, String>();
				set = temp.entrySet();
				int i = 0;
				for (Map.Entry entry : set)
				{
					if (cols.contains(entry.getValue()))
					{
						pos2Col.put(i, (String)entry.getValue());
						cols2Pos.put((String)entry.getValue(), i);
						i++;
					}
				}
			}
		}
		else
		{
			throw new Exception("ProjectOperator only supports 1 child.");
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
			throw new Exception("ProjectOperator only supports 1 parent.");
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
