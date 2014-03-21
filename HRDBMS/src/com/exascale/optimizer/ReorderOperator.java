package com.exascale.optimizer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeMap;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.misc.DataEndMarker;
import com.exascale.tables.Plan;

public final class ReorderOperator implements Operator, Serializable
{
	private Operator child;
	private Operator parent;
	private HashMap<String, String> cols2Types;
	private HashMap<String, Integer> cols2Pos;
	private TreeMap<Integer, String> pos2Col;
	private final ArrayList<String> order;
	private final MetaData meta;
	private boolean nullOp = false;
	private int node;
	private Plan plan;
	
	public void setPlan(Plan plan)
	{
		this.plan = plan;
	}

	public ReorderOperator(ArrayList<String> order, MetaData meta)
	{
		this.order = order;
		this.meta = meta;
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
				pos2Col = child.getPos2Col();
				new ArrayList<String>(pos2Col.values());
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
					nullOp = false;
					int i = 0;
					for (final String col : order)
					{
						cols2Types.put(col, child.getCols2Types().get(col));
						cols2Pos.put(col, i);
						pos2Col.put(i, col);
						i++;
					}
				}
			}
		}
		else
		{
			throw new Exception("ReorderOperator only supports 1 child.");
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
	public ReorderOperator clone()
	{
		final ReorderOperator retval = new ReorderOperator(order, meta);
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
		final ArrayList<String> retval = new ArrayList<String>(order);
		return retval;
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
			final Object o = child.next(this);
			if (o instanceof DataEndMarker)
			{
				return o;
			}

			final ArrayList<Object> row = (ArrayList<Object>)o;
			final ArrayList<Object> retval = new ArrayList<Object>(order.size());
			for (final String col : order)
			{
				try
				{
					retval.add(row.get(child.getCols2Pos().get(col)));
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.error("Error in ReorderOperator looking for " + col + " in " + child.getCols2Pos(), e);
					System.exit(1);
				}
			}

			return retval;
		}
	}

	@Override
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
			throw new Exception("ReorderOperator only supports 1 parent.");
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
	public void reset()
	{
		child.reset();
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
		return "ReorderOperator: " + order;
	}

}
