package com.exascale.optimizer.testing;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.exascale.optimizer.testing.ResourceManager.DiskBackedHashMap;

public class SelectOperator implements Operator, Cloneable, Serializable
{
	private MetaData meta;
	private Vector<Filter> filters;
	private Operator child = null;
	private Operator parent;
	private HashMap<String, String> cols2Types;
	private HashMap<String, Integer> cols2Pos;
	private TreeMap<Integer, String> pos2Col;
	private boolean startDone = false;
	private boolean closeDone = false;
	private AtomicLong passed = new AtomicLong(0);
	private AtomicLong total = new AtomicLong(0);
	private ArrayList<String> references = new ArrayList<String>();
	
	public double likelihood()
	{
		return meta.likelihood(new ArrayList<Filter>(filters));
	}
	
	public double likelihood(RootOperator op)
	{
		return meta.likelihood(new ArrayList<Filter>(filters), op);
	}
	
	protected SelectOperator clone()
	{
		Vector<Filter> filtersDeepClone = new Vector<Filter>();
		for (Filter f : filters)
		{
			filtersDeepClone.add((Filter)f.clone());
		}
		SelectOperator retval = new SelectOperator(filtersDeepClone, meta);
		return retval;
	}
	
	public MetaData getMeta()
	{
		return meta;
	}
	
	public SelectOperator(Filter filter, MetaData meta)
	{
		this.meta = meta;
		this.filters = new Vector<Filter>();
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
	
	public SelectOperator(Vector<Filter> filters, MetaData meta)
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
	
	public ArrayList<String> getReferences()
	{
		return references;
	}
	
	public Vector<Filter> getFilter()
	{
		return filters;
	}
	
	public ArrayList<Operator> children()
	{
		ArrayList<Operator> retval = new ArrayList<Operator>();
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
	public synchronized void close() throws Exception 
	{
		if (!closeDone)
		{
			closeDone = true;
			child.close();
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
