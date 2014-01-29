package com.exascale.optimizer.testing;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

import com.exascale.optimizer.testing.ResourceManager.DiskBackedHashMap;

public class RenameOperator implements Operator, Serializable
{
	protected Operator child;
	protected Operator parent;
	protected HashMap<String, String> cols2Types;
	protected HashMap<String, Integer> cols2Pos;
	protected TreeMap<Integer, String> pos2Col;
	protected HashMap<String, String> old2New = new HashMap<String, String>();
	protected MetaData meta;
	protected boolean optimize = false;
	protected ArrayList<String> oldVals;
	protected ArrayList<String> newVals;
	protected int node;
	
	public void reset()
	{
		child.reset();
	}
	
	public void setChildPos(int pos)
	{
	}
	
	public int getChildPos()
	{
		return 0;
	}
	
	public HashMap<String, String> getIndexRenames()
	{
		HashMap<String, String> retval = new HashMap<String, String>();
		int i = 0;
		try
		{
			for (String oldVal : oldVals)
			{
				retval.put(newVals.get(i), oldVal);
				i++;
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
			System.exit(1);
		}
		
		return retval;
	}
	
	public RenameOperator(ArrayList<String> oldVals, ArrayList<String> newVals, MetaData meta)
	{
		this.oldVals = oldVals;
		this.newVals = newVals;
		int i = 0;
		try
		{
			for (String oldVal : oldVals)
			{
				old2New.put(oldVal, newVals.get(i));
				i++;
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
			System.out.println("Old = " + oldVals);
			System.out.println("New = " + newVals);
			System.exit(1);
		}
		
		this.meta = meta;
	}
	
	public RenameOperator clone()
	{
		RenameOperator retval = new RenameOperator((ArrayList<String>)oldVals.clone(), (ArrayList<String>)newVals.clone(), meta);
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
			ArrayList temp = new ArrayList(1);
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
			ArrayList temp = new ArrayList(1);
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
		ArrayList<Operator> retval = new ArrayList<Operator>(1);
		retval.add(child);
		return retval;
	}
	
	public String toString()
	{
		return "RenameOperator: " + old2New;
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
		return child.next(this);
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
