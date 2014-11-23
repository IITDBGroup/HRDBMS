package com.exascale.optimizer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.misc.DataEndMarker;
import com.exascale.tables.Plan;

public final class TopOperator implements Operator, Serializable
{
	private Operator child;
	private Operator parent;
	private HashMap<String, String> cols2Types;
	private HashMap<String, Integer> cols2Pos;
	private TreeMap<Integer, String> pos2Col;
	private final MetaData meta;
	private final AtomicLong remaining;
	private int node;
	private volatile boolean demSent = false;
	private Plan plan;
	
	public void setPlan(Plan plan)
	{
		this.plan = plan;
	}

	public TopOperator(long numVals, MetaData meta)
	{
		this.remaining = new AtomicLong(numVals);
		this.meta = meta;
	}

	@Override
	public void add(Operator op) throws Exception
	{
		if (child == null)
		{
			child = op;
			cols2Pos = child.getCols2Pos();
			cols2Types = child.getCols2Types();
			pos2Col = child.getPos2Col();
			child.registerParent(this);
		}
		else
		{
			throw new Exception("RenameOperator only supports 1 child.");
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
	public TopOperator clone()
	{
		final TopOperator retval = new TopOperator(remaining.get(), meta);
		retval.node = node;
		return retval;
	}

	@Override
	public void close() throws Exception
	{
		cols2Pos = null;
		cols2Types = null;
		pos2Col = null;
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
		final ArrayList<String> retval = new ArrayList<String>(0);
		return retval;
	}

	public long getRemaining()
	{
		return remaining.get();
	}

	@Override
	public Object next(Operator op) throws Exception
	{
		final long num = remaining.getAndDecrement();

		if (num > 0)
		{
			final Object retval = child.next(this);
			if (retval instanceof DataEndMarker)
			{
				demSent = true;
				remaining.set(0);
			}
			
			if (retval instanceof Exception)
			{
				throw (Exception)retval;
			}
			return retval;
		}

		if (demSent)
		{
			return new DataEndMarker();
		}
		else
		{
			child.nextAll(TopOperator.this);
			return new DataEndMarker();
		}
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
			throw new Exception("RenameOperator only supports 1 parent.");
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
		HRDBMSWorker.logger.error("TopOperator cannot reset");
		throw new Exception("TopOperator cannot reset");
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
		return "TopOperator";
	}
}
