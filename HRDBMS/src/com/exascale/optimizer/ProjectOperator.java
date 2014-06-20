package com.exascale.optimizer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.misc.DataEndMarker;
import com.exascale.tables.Plan;

public final class ProjectOperator implements Operator, Serializable
{
	private Operator child;
	private final ArrayList<String> cols;
	private final MetaData meta;
	private HashMap<String, String> cols2Types;
	private HashMap<String, Integer> cols2Pos;
	private TreeMap<Integer, String> pos2Col;
	private Operator parent;
	private HashMap<String, Integer> childCols2Pos;
	private int node;
	private final ArrayList<Integer> pos2Get = new ArrayList<Integer>();
	private volatile boolean startDone = false;
	private Plan plan;
	
	public void setPlan(Plan plan)
	{
		this.plan = plan;
	}

	public ProjectOperator(ArrayList<String> cols, MetaData meta)
	{
		this.cols = cols;
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
				childCols2Pos = child.getCols2Pos();
				Map temp = (HashMap<String, String>)child.getCols2Types().clone();
				cols2Types = new HashMap<String, String>();
				Set<Map.Entry> set = temp.entrySet();
				for (final Map.Entry entry : set)
				{
					if (cols.contains(entry.getKey()))
					{
						cols2Types.put((String)entry.getKey(), (String)entry.getValue());
					}
				}

				temp = child.getPos2Col();
				cols2Pos = new HashMap<String, Integer>();
				pos2Col = new TreeMap<Integer, String>();
				set = temp.entrySet();
				int i = 0;
				for (final Map.Entry entry : set)
				{
					if (cols.contains(entry.getValue()))
					{
						pos2Col.put(i, (String)entry.getValue());
						cols2Pos.put((String)entry.getValue(), i);
						i++;
					}
				}
			}
		}
		else
		{
			throw new Exception("ProjectOperator only supports 1 child.");
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
	public ProjectOperator clone()
	{
		final ProjectOperator retval = new ProjectOperator(cols, meta);
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
		final ArrayList<String> retval = new ArrayList<String>(cols);
		return retval;
	}

	@Override
	// @?Parallel
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
		final ArrayList<Object> retval = new ArrayList<Object>(pos2Get.size());
		for (final int pos : pos2Get)
		{
			retval.add(row.get(pos));
		}

		return retval;
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
			throw new Exception("ProjectOperator only supports 1 parent.");
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
		if (!startDone)
		{
			try
			{
				start();
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				throw e;
			}
		}
		else
		{
			child.reset();
		}
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
		startDone = true;
		child.start();
		for (final String col : pos2Col.values())
		{
			pos2Get.add(childCols2Pos.get(col));
		}
	}

	@Override
	public String toString()
	{
		return "ProjectOperator";
	}

}
