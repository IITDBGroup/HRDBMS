package com.exascale.optimizer.testing;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

import com.exascale.optimizer.testing.ResourceManager.DiskBackedHashMap;

public class RenameOperator implements Operator, Serializable
{
	private Operator child;
	private Operator parent;
	private HashMap<String, String> cols2Types;
	private HashMap<String, Integer> cols2Pos;
	private TreeMap<Integer, String> pos2Col;
	private HashMap<String, String> old2New = new HashMap<String, String>();
	private MetaData meta;
	private boolean startDone = false;
	private boolean closeDone = false;
	private boolean optimize = false;
	
	public RenameOperator(Vector<String> oldVals, Vector<String> newVals, MetaData meta)
	{
		int i = 0;
		for (String oldVal : oldVals)
		{
			old2New.put(oldVal, newVals.get(i));
			i++;
		}
		
		this.meta = meta;
	}
	
	public ArrayList<String> getReferences()
	{
		ArrayList<String> retval = new ArrayList<String>(old2New.keySet());
		return retval;
	}
	
	public HashMap<String, String> getRenameMap()
	{
		return old2New;
	}
	
	public MetaData getMeta()
	{
		return meta;
	}
	
	public void updateReferences(ArrayList<String> references)
	{
		for (Map.Entry entry : old2New.entrySet())
		{
			ArrayList temp = new ArrayList();
			temp.add(entry.getKey());
			if (references.removeAll(temp))
			{
				references.add((String)entry.getValue());
			}
		}
	}
	
	public void reverseUpdateReferences(ArrayList<String> references)
	{
		for (Map.Entry entry : old2New.entrySet())
		{
			ArrayList temp = new ArrayList();
			temp.add(entry.getValue());
			if (references.removeAll(temp))
			{
				references.add((String)entry.getKey());
			}
		}
	}
	
	public void updateSelectOperator(SelectOperator op)
	{
		for (Filter filter : op.getFilter())
		{
			if (filter.leftIsColumn() && old2New.containsKey(filter.leftColumn()))
			{
				filter.updateLeftColumn(old2New.get(filter.leftColumn()));
			}
			
			if (filter.rightIsColumn() && old2New.containsKey(filter.rightColumn()))
			{
				filter.updateRightColumn(old2New.get(filter.rightColumn()));
			}
		}
		
		op.updateReferences();
	}
	
	public SelectOperator reverseUpdateSelectOperator(SelectOperator op)
	{
		SelectOperator retval = op.clone();
		for (Filter filter : retval.getFilter())
		{
			if (filter.leftIsColumn() && old2New.containsValue(filter.leftColumn()))
			{
				String key = null;
				for (Map.Entry entry : old2New.entrySet())
				{
					if (entry.getValue().equals(filter.leftColumn()))
					{
						key = (String)entry.getKey();
						break;
					}
				}
				filter.updateLeftColumn(key);
			}
			
			if (filter.rightIsColumn() && old2New.containsValue(filter.rightColumn()))
			{
				String key = null;
				for (Map.Entry entry : old2New.entrySet())
				{
					if (entry.getValue().equals(filter.rightColumn()))
					{
						key = (String)entry.getKey();
						break;
					}
				}
				filter.updateRightColumn(key);
			}
		}
		retval.updateReferences();
		return retval;
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
		return "RenameOperator";
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
		return child.next(this);
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
			cols2Types = new HashMap<String, String>();
			for (Map.Entry entry : child.getCols2Types().entrySet())
			{
				String newVal = old2New.get(entry.getKey());
				if (newVal == null)
				{
					cols2Types.put((String)entry.getKey(), (String)entry.getValue());
				}
				else
				{
					cols2Types.put(newVal, (String)entry.getValue());
				}
			}
				
			cols2Pos = new HashMap<String, Integer>();
			for (Map.Entry entry : child.getCols2Pos().entrySet())
			{
				String newVal = old2New.get(entry.getKey());
				if (newVal == null)
				{
					cols2Pos.put((String)entry.getKey(), (Integer)entry.getValue());
				}
				else
				{
					cols2Pos.put(newVal, (Integer)entry.getValue());
				}
			}
			
			pos2Col = new TreeMap<Integer, String>();
			for (Map.Entry entry : child.getPos2Col().entrySet())
			{
				String newVal = old2New.get(entry.getValue());
				if (newVal == null)
				{
					pos2Col.put((Integer)entry.getKey(), (String)entry.getValue());
				}
				else
				{
					pos2Col.put((Integer)entry.getKey(), newVal);
				}
			}
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

}
