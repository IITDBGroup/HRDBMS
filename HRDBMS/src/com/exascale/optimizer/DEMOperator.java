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

public final class DEMOperator implements Operator, Serializable
{
	private Operator child;
	private final MetaData meta;
	private HashMap<String, String> cols2Types;
	private HashMap<String, Integer> cols2Pos;
	private TreeMap<Integer, String> pos2Col;
	private Operator parent;
	private int node;
	private transient Plan plan;
	
	public void setPlan(Plan plan)
	{
		this.plan = plan;
	}

	public DEMOperator(MetaData meta)
	{
		this.meta = meta;
		cols2Types = new HashMap<String, String>();
		cols2Pos = new HashMap<String, Integer>();
		pos2Col = new TreeMap<Integer, String>();
	}
	
	public void setCols2Pos(HashMap<String, Integer> cols2Pos)
	{
		this.cols2Pos = (HashMap<String, Integer>)cols2Pos.clone(); 
	}
	
	public void setCols2Types(HashMap<String, String> cols2Types)
	{
		this.cols2Types = (HashMap<String, String>)cols2Types.clone();
	}
	
	public void setPos2Col(TreeMap<Integer, String> pos2Col)
	{
		this.pos2Col = (TreeMap<Integer, String>)pos2Col.clone();
	}

	@Override
	public void add(Operator op) throws Exception
	{
		throw new Exception("DEMOperator does not support children");
	}

	@Override
	public ArrayList<Operator> children()
	{
		final ArrayList<Operator> retval = new ArrayList<Operator>(1);
		return retval;
	}

	@Override
	public DEMOperator clone()
	{
		final DEMOperator retval = new DEMOperator(meta);
		retval.node = node;
		return retval;
	}

	@Override
	public void close() throws Exception
	{
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
		final ArrayList<String> retval = new ArrayList<String>(1);
		return retval;
	}

	@Override
	// @?Parallel
	public Object next(Operator op) throws Exception
	{
		return new DataEndMarker();
	}

	@Override
	public void nextAll(Operator op) throws Exception
	{
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
			throw new Exception("DEMOperator only supports 1 parent.");
		}
	}

	@Override
	public void removeChild(Operator op)
	{
	}

	@Override
	public void removeParent(Operator op)
	{
		parent = null;
	}

	@Override
	public void reset() throws Exception
	{
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
	}

	@Override
	public String toString()
	{
		return "DEMOperator";
	}
}
