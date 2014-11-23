package com.exascale.optimizer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeMap;
import com.exascale.misc.DataEndMarker;
import com.exascale.misc.MyDate;
import com.exascale.tables.Plan;

public final class ExtendObjectOperator implements Operator, Serializable
{
	private Operator child;
	private Operator parent;
	private HashMap<String, String> cols2Types;
	private HashMap<String, Integer> cols2Pos;
	private TreeMap<Integer, String> pos2Col;
	private final String name;
	private final MetaData meta;
	private int node;
	private transient Plan plan;
	private Object obj;
	
	public void setPlan(Plan plan)
	{
		this.plan = plan;
	}

	public ExtendObjectOperator(Object obj, String name, MetaData meta)
	{
		this.obj = obj;
		this.meta = meta;
		this.name = name;
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
				cols2Types = (HashMap<String, String>)child.getCols2Types().clone();
				if (obj instanceof MyDate)
				{
					cols2Types.put(name, "DATE");
				}
				else
				{
					cols2Types.put(name, "CHAR");
				}
				cols2Pos = (HashMap<String, Integer>)child.getCols2Pos().clone();
				cols2Pos.put(name, cols2Pos.size());
				pos2Col = (TreeMap<Integer, String>)child.getPos2Col().clone();
				pos2Col.put(pos2Col.size(), name);
			}
		}
		else
		{
			throw new Exception("ExtendDateOperator only supports 1 child.");
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
	public ExtendObjectOperator clone()
	{
		final ExtendObjectOperator retval = new ExtendObjectOperator(obj, name, meta);
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

	public String getOutputCol()
	{
		return name;
	}

	@Override
	public TreeMap<Integer, String> getPos2Col()
	{
		return pos2Col;
	}

	@Override
	public ArrayList<String> getReferences()
	{
		final ArrayList<String> retval = new ArrayList<String>();
		return retval;
	}

	@Override
	public Object next(Operator op) throws Exception
	{
		final Object o = child.next(this);
		if (o instanceof DataEndMarker)
		{
			return o;
		}
		
		if (o instanceof Exception)
		{
			throw (Exception)o;
		}

		final ArrayList<Object> row = (ArrayList<Object>)o;
		row.add(obj);
		return row;
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
			throw new Exception("ExtendDateOperator only supports 1 parent.");
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
		return "ExtendDateOperator";
	}
}
