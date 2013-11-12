package com.exascale.optimizer.testing;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

import com.exascale.optimizer.testing.ResourceManager.DiskBackedHashMap;

public class ProjectOperator implements Operator, Serializable
{
	private Operator child;
	private Vector<String> cols;
	private MetaData meta;
	private HashMap<String, String> cols2Types;
	private HashMap<String, Integer> cols2Pos;
	private TreeMap<Integer, String> pos2Col;
	private Operator parent;
	private HashMap<String, Integer> childCols2Pos;
	private boolean startDone = false;
	private boolean closeDone = false;
	
	public ProjectOperator(Vector<String> cols, MetaData meta)
	{
		this.cols = cols;
		this.meta = meta;
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
		ArrayList<Operator> retval = new ArrayList<Operator>();
		retval.add(child);
		return retval;
	}
	
	public String toString()
	{
		return "ProjectOperator";
	}
	
	@Override
	public synchronized void start() throws Exception 
	{
		if (!startDone)
		{
			startDone = true;
			child.start();
		}
	}

	@Override
	public Object next(Operator op) throws Exception 
	{
		Object o = child.next(this);
		if (o instanceof DataEndMarker)
		{
			return o;
		}
		
		ArrayList<Object> row = (ArrayList<Object>)o; 
		ArrayList<Object> retval = new ArrayList<Object>();
		for (Map.Entry entry : pos2Col.entrySet())
		{
			retval.add(row.get(childCols2Pos.get(entry.getValue())));
		}
		
		return retval;
	}

	@Override
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

	@Override
	public void add(Operator op) throws Exception 
	{
		if (child == null)
		{
			child = op;
			child.registerParent(this);
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
