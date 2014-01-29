package com.exascale.optimizer.testing;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.exascale.optimizer.testing.ResourceManager.DiskBackedHashMap;

public class SelectOperator implements Operator, Cloneable, Serializable
{
	protected MetaData meta;
	protected ArrayList<Filter> filters;
	protected Operator child = null;
	protected Operator parent;
	protected HashMap<String, String> cols2Types;
	protected HashMap<String, Integer> cols2Pos;
	protected TreeMap<Integer, String> pos2Col;
	protected AtomicLong passed = new AtomicLong(0);
	protected AtomicLong total = new AtomicLong(0);
	protected ArrayList<String> references = new ArrayList<String>();
	protected int node;
	
	public void reset()
	{
		child.reset();
		passed = new AtomicLong(0);
		total = new AtomicLong(0);
	}
	
	public void setChildPos(int pos)
	{
	}
	
	public int getChildPos()
	{
		return 0;
	}
	
	public double likelihood()
	{
		return meta.likelihood(new ArrayList<Filter>(filters));
	}
	
	public double likelihood(RootOperator op)
	{
		return meta.likelihood(new ArrayList<Filter>(filters), op);
	}
	
	public SelectOperator clone()
	{
		ArrayList<Filter> filtersDeepClone = new ArrayList<Filter>(filters.size());
		for (Filter f : filters)
		{
			filtersDeepClone.add((Filter)f.clone());
		}
		SelectOperator retval = new SelectOperator(filtersDeepClone, meta);
		retval.node = node;
		return retval;
	}
	
	public MetaData getMeta()
	{
		return meta;
	}
	
	public SelectOperator(Filter filter, MetaData meta)
	{
		this.meta = meta;
		this.filters = new ArrayList<Filter>();
		this.filters.add(filter);
		
		if (filter.leftIsColumn() && !references.contains(filter.leftColumn()))
		{
			references.add(filter.leftColumn());
		}
		
		if (filter.rightIsColumn() && !references.contains(filter.rightColumn()))
		{
			references.add(filter.rightColumn());
		}
	}
	
	public void updateReferences()
	{
		references.clear();
		for (Filter filter : filters)
		{
			if (filter.leftIsColumn() && !references.contains(filter.leftColumn()))
			{
				references.add(filter.leftColumn());
			}
			
			if (filter.rightIsColumn() && !references.contains(filter.rightColumn()))
			{
				references.add(filter.rightColumn());
			}
		}
	}
	
	public SelectOperator(ArrayList<Filter> filters, MetaData meta)
	{
		this.filters = filters;
		this.meta = meta;
		
		for (Filter filter : filters)
		{
			if (filter.leftIsColumn() && !references.contains(filter.leftColumn()))
			{
				references.add(filter.leftColumn());
			}
			
			if (filter.rightIsColumn() && !references.contains(filter.rightColumn()))
			{
				references.add(filter.rightColumn());
			}
		}
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
		return references;
	}
	
	public ArrayList<Filter> getFilter()
	{
		return filters;
	}
	
	public ArrayList<Operator> children()
	{
		ArrayList<Operator> retval = new ArrayList<Operator>(1);
		retval.add(child);
		return retval;
	}
	
	public Operator parent()
	{
		return parent;
	}
	
	public void add(Operator op) throws Exception
	{
		if (child == null)
		{
			child = op;
			op.registerParent(this);
			cols2Types = child.getCols2Types();
			cols2Pos = child.getCols2Pos();
			pos2Col = child.getPos2Col();
		}
		else
		{
			throw new Exception("SelectOperator only supports 1 child.");
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

	//@?Parallel
	@Override
	public Object next(Operator op) throws Exception 
	{
		Object o = child.next(this);
		total.getAndIncrement();
		while (! (o instanceof DataEndMarker))
		{
			for (Filter filter : filters)
			{
				if (filter.passes((ArrayList<Object>)o, cols2Pos))
				{
					passed.getAndIncrement();
					return (ArrayList<Object>)o;
				}
			}
			
			o = child.next(this);
			total.getAndIncrement();
		}
		
		//System.out.println("Select operator returned " + passed + "/" + total + " records");
		return o;
	}

	@Override
	public void close() throws Exception 
	{
		child.close();
	}

	public void registerParent(Operator op) throws Exception
	{
		if (parent == null)
		{
			parent = op;
		}
		else
		{
			throw new Exception("SelectOperator only supports 1 parent.");
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
		String retval = "SelectOperator(";
		for (Filter f : filters)
		{
			retval += (f.toString() + "\t");
		}
		retval += ")";
		return retval;
	}
}
