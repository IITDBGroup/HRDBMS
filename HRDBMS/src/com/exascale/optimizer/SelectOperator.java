package com.exascale.optimizer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import com.exascale.misc.DataEndMarker;
import com.exascale.tables.Plan;
import com.exascale.tables.Transaction;

public final class SelectOperator implements Operator, Cloneable, Serializable
{
	private final MetaData meta;
	private ArrayList<Filter> filters;
	private Operator child = null;
	private Operator parent;
	private HashMap<String, String> cols2Types;
	private HashMap<String, Integer> cols2Pos;
	private TreeMap<Integer, String> pos2Col;
	private AtomicLong passed = new AtomicLong(0);
	private AtomicLong total = new AtomicLong(0);
	private ArrayList<String> references = new ArrayList<String>();
	private int node;
	private transient Plan plan;
	private boolean hash = false;
	boolean always = false;
	private HashSet<Object> hashSet;
	private String hashCol = null;
	private int hashPos;
	
	private static int HASH_THRESHOLD = 10;
	
	public void setPlan(Plan plan)
	{
		this.plan = plan;
	}

	public SelectOperator(ArrayList<Filter> filters, MetaData meta)
	{
		this.filters = filters;
		this.meta = meta;

		boolean leftIsAllCol = true;
		boolean leftIsAllLiteral = true;
		boolean rightIsAllCol = true;
		boolean rightIsAllLiteral = true;
		boolean allEqual = true;
		boolean leftAllSameCol = true;
		boolean rightAllSameCol = true;
	
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
			
			if (filter.leftIsColumn())
			{
				leftIsAllLiteral = false;
				if (hashCol == null)
				{
					hashCol = filter.leftColumn();
				}
				else
				{
					if (!filter.leftColumn().equals(hashCol))
					{
						leftAllSameCol = false;
					}
				}
			}
			else
			{
				leftIsAllCol = false;
			}
			
			if (filter.rightIsColumn())
			{
				rightIsAllLiteral = false;
				if (hashCol == null)
				{
					hashCol = filter.rightColumn();
				}
				else
				{
					if (!filter.rightColumn().equals(hashCol))
					{
						rightAllSameCol = false;
					}
				}
			}
			else
			{
				rightIsAllCol = false;
			}
			
			if (!filter.op().equals("E"))
			{
				allEqual = false;
			}
			
			if (filter.alwaysTrue())
			{
				always = true;
			}
		}
		
		if (!always && filters.size() > HASH_THRESHOLD && allEqual && leftIsAllCol && rightIsAllLiteral && leftAllSameCol)
		{
			hash = true;
			hashSet = new HashSet<Object>();
			for (Filter filter : filters)
			{
				Object obj = filter.rightLiteral();
				if (obj instanceof Long)
				{
					obj = new Double((Long)obj);
				}
				
				hashSet.add(obj);
			}
		}
		else if (!always && filters.size() > HASH_THRESHOLD && allEqual && leftIsAllLiteral && rightIsAllCol && rightAllSameCol)
		{
			hash = true;
			hashSet = new HashSet<Object>();
			for (Filter filter : filters)
			{
				Object obj = filter.leftLiteral();
				if (obj instanceof Long)
				{
					obj = new Double((Long)obj);
				}
				
				hashSet.add(obj);
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
			
			if (hash)
			{
				hashPos = cols2Pos.get(hashCol);
			}
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
		cols2Pos = null;
		cols2Types = null;
		pos2Col = null;
		filters = null;
		references = null;
		hashSet = null;
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
			
			if (always)
			{
				passed.getAndIncrement();
				return o;
			}
			
			if (hash)
			{
				ArrayList<Object> row = (ArrayList<Object>)o;
				Object obj = row.get(hashPos);
				if (obj instanceof Long)
				{
					obj = new Double((Long)obj);
				}
				
				if (hashSet.contains(obj))
				{
					passed.getAndIncrement();
					return o;
				}
			}
			else
			{
				for (final Filter filter : filters)
				{
					if (filter.passes((ArrayList<Object>)o, cols2Pos))
					{
						passed.getAndIncrement();
						return o;
					}
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
