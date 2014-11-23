package com.exascale.optimizer;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.TreeMap;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.misc.DataEndMarker;
import com.exascale.misc.DateParser;
import com.exascale.misc.MyDate;
import com.exascale.misc.MySimpleDateFormat;
import com.exascale.tables.Plan;

public final class ConcatOperator implements Operator, Serializable
{
	private Operator child;
	private Operator parent;
	private HashMap<String, String> cols2Types;
	private HashMap<String, Integer> cols2Pos;
	private TreeMap<Integer, String> pos2Col;
	private String col1;
	private String col2;
	private final String name;
	private final MetaData meta;
	private int node;
	private transient Plan plan;
	private Integer colPos1;
	private Integer colPos2;
	
	public void setPlan(Plan plan)
	{
		this.plan = plan;
	}

	public ConcatOperator(String col1, String col2, String name, MetaData meta)
	{
		this.col1 = col1;
		this.col2 = col2;
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
				cols2Types.put(name, "CHAR");
				cols2Pos = (HashMap<String, Integer>)child.getCols2Pos().clone();
				cols2Pos.put(name, cols2Pos.size());
				pos2Col = (TreeMap<Integer, String>)child.getPos2Col().clone();
				pos2Col.put(pos2Col.size(), name);
				colPos1 = cols2Pos.get(col1);
				if (colPos1 == null)
				{
					int count = 0;
					if (col1.startsWith("."))
					{
						col1 = col1.substring(1);
						for (String col3 : cols2Pos.keySet())
						{
							String orig = col3;
							if (col3.contains("."))
							{
								col3 = col3.substring(col3.indexOf('.') + 1);
							}
							
							if (col3.equals(col1))
							{
								col1 = orig;
								count++;
								colPos1 = cols2Pos.get(orig);
							}
							
							if (count > 1)
							{
								throw new Exception("Ambiguous column: " + col1);
							}
						}
					}
				}
				colPos2 = cols2Pos.get(col2);
				if (colPos2 == null)
				{
					int count = 0;
					if (col2.startsWith("."))
					{
						col2 = col2.substring(1);
						for (String col3 : cols2Pos.keySet())
						{
							String orig = col3;
							if (col3.contains("."))
							{
								col3 = col3.substring(col3.indexOf('.') + 1);
							}
							
							if (col3.equals(col2))
							{
								col2 = orig;
								count++;
								colPos2 = cols2Pos.get(orig);
							}
							
							if (count > 1)
							{
								throw new Exception("Ambiguous column: " + col2);
							}
						}
					}
				}
			}
		}
		else
		{
			throw new Exception("ConcatOperator only supports 1 child.");
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
	public ConcatOperator clone()
	{
		final ConcatOperator retval = new ConcatOperator(col1, col2, name, meta);
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
		retval.add(col1);
		retval.add(col2);
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
		row.add((String)row.get(colPos1) + (String)row.get(colPos2));
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
			throw new Exception("ConcatOperator only supports 1 parent.");
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
		return "ConcatOperator";
	}
}
