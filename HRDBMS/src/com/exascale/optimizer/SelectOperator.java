package com.exascale.optimizer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import com.exascale.misc.DataEndMarker;
import com.exascale.tables.Plan;
import com.exascale.tables.Transaction;

public final class SelectOperator implements Operator, Cloneable, Serializable
{
	private final MetaData meta;
	private final ArrayList<Filter> filters;
	private Operator child = null;
	private Operator parent;
	private HashMap<String, String> cols2Types;
	private HashMap<String, Integer> cols2Pos;
	private TreeMap<Integer, String> pos2Col;
	private AtomicLong passed = new AtomicLong(0);
	private AtomicLong total = new AtomicLong(0);
	private final ArrayList<String> references = new ArrayList<String>();
	private int node;
	private Plan plan;
	
	public void setPlan(Plan plan)
	{
		this.plan = plan;
	}

	public SelectOperator(ArrayList<Filter> filters, MetaData meta)
	{
		this.filters = filters;
		this.meta = meta;

		for (final Filter filter : filters)
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

	@Override
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

	@Override
	public ArrayList<Operator> children()
	{
		final ArrayList<Operator> retval = new ArrayList<Operator>(1);
		retval.add(child);
		return retval;
	}

	@Override
	public SelectOperator clone()
	{
		final ArrayList<Filter> filtersDeepClone = new ArrayList<Filter>(filters.size());
		for (final Filter f : filters)
		{
			filtersDeepClone.add(f.clone());
		}
		final SelectOperator retval = new SelectOperator(filtersDeepClone, meta);
		retval.node = node;
		return retval;
	}

	@Override
	public void close() throws Exception
	{
		child.close();
	}

	@Override
	public int getChildPos()
	{
		return 0;
	}

	@Override
	public HashMap<String, Integer> getCols2Pos()
	{
		return cols2Pos;
	}

	@Override
	public HashMap<String, String> getCols2Types()
	{
		return cols2Types;
	}

	public ArrayList<Filter> getFilter()
	{
		return filters;
	}

	@Override
	public MetaData getMeta()
	{
		return meta;
	}

	@Override
	public int getNode()
	{
		return node;
	}

	@Override
	public TreeMap<Integer, String> getPos2Col()
	{
		return pos2Col;
	}

	@Override
	public ArrayList<String> getReferences()
	{
		return references;
	}

	public double likelihood(Transaction tx) throws Exception
	{
		return meta.likelihood(new ArrayList<Filter>(filters), tx, this);
	}

	public double likelihood(RootOperator op, Transaction tx) throws Exception
	{
		return meta.likelihood(new ArrayList<Filter>(filters), op, tx, this);
	}

	// @?Parallel
	@Override
	public Object next(Operator op) throws Exception
	{
		Object o = child.next(this);
		total.getAndIncrement();
		while (!(o instanceof DataEndMarker))
		{
			if (o instanceof Exception)
			{
				throw (Exception)o;
			}
			for (final Filter filter : filters)
			{
				if (filter.passes((ArrayList<Object>)o, cols2Pos))
				{
					passed.getAndIncrement();
					return o;
				}
			}

			o = child.next(this);
			total.getAndIncrement();
		}

		// System.out.println("Select operator returned " + passed + "/" + total
		// + " records");
		return o;
	}

	@Override
	public void nextAll(Operator op) throws Exception
	{
		child.nextAll(op);
		Object o = next(op);
		while (!(o instanceof DataEndMarker) && !(o instanceof Exception))
		{
			o = next(op);
		}
	}

	@Override
	public Operator parent()
	{
		return parent;
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
			throw new Exception("SelectOperator only supports 1 parent.");
		}
	}

	@Override
	public void removeChild(Operator op)
	{
		if (op == child)
		{
			child = null;
			op.removeParent(this);
		}
	}

	@Override
	public void removeParent(Operator op)
	{
		parent = null;
	}

	@Override
	public void reset() throws Exception
	{
		child.reset();
		passed = new AtomicLong(0);
		total = new AtomicLong(0);
	}

	@Override
	public void setChildPos(int pos)
	{
	}

	@Override
	public void setNode(int node)
	{
		this.node = node;
	}

	@Override
	public void start() throws Exception
	{
		child.start();
	}

	@Override
	public String toString()
	{
		String retval = "SelectOperator(";
		for (final Filter f : filters)
		{
			retval += (f.toString() + "\t");
		}
		retval += ")";
		return retval;
	}

	public void updateReferences()
	{
		references.clear();
		for (final Filter filter : filters)
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
}
