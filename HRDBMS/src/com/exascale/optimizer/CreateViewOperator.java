package com.exascale.optimizer;

import java.io.OutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.TreeMap;
import com.exascale.misc.DataEndMarker;
import com.exascale.tables.Plan;
import com.exascale.tables.Transaction;

public final class CreateViewOperator implements Operator, Serializable
{
	private final MetaData meta;
	private HashMap<String, String> cols2Types;
	private HashMap<String, Integer> cols2Pos;
	private TreeMap<Integer, String> pos2Col;
	private Operator parent;
	private int node;
	private final String schema;
	private final String table;
	private boolean done = false;
	private Transaction tx;
	private final String text;

	public CreateViewOperator(final String schema, final String table, final String text, final MetaData meta)
	{
		this.meta = meta;
		this.schema = schema;
		this.table = table;
		this.text = text;
	}

	@Override
	public void add(final Operator op) throws Exception
	{
		throw new Exception("CreateViewOperator does not support children");
	}

	@Override
	public ArrayList<Operator> children()
	{
		final ArrayList<Operator> retval = new ArrayList<Operator>(1);
		return retval;
	}

	@Override
	public CreateViewOperator clone()
	{
		final CreateViewOperator retval = new CreateViewOperator(schema, table, text, meta);
		retval.node = node;
		return retval;
	}

	@Override
	public void close() throws Exception
	{
		cols2Pos = null;
		cols2Types = null;
		pos2Col = null;
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
		final ArrayList<String> retval = new ArrayList<String>();
		return retval;
	}

	@Override
	// @?Parallel
	public Object next(final Operator op) throws Exception
	{
		if (!done)
		{
			done = true;
			MetaData.createView(schema, table, text, tx);
			return 1;
		}
		else
		{
			return new DataEndMarker();
		}
	}

	@Override
	public void nextAll(final Operator op) throws Exception
	{
	}

	@Override
	public long numRecsReceived()
	{
		return 0;
	}

	@Override
	public Operator parent()
	{
		return parent;
	}

	@Override
	public boolean receivedDEM()
	{
		return false;
	}

	@Override
	public void registerParent(final Operator op) throws Exception
	{
		throw new Exception("CreateViewOperator does not support parents.");
	}

	@Override
	public void removeChild(final Operator op)
	{
	}

	@Override
	public void removeParent(final Operator op)
	{
	}

	@Override
	public void reset() throws Exception
	{
		throw new Exception("CreateViewOperator is not resetable");
	}

	@Override
	public void serialize(final OutputStream out, final IdentityHashMap<Object, Long> prev) throws Exception
	{
		throw new Exception("Trying to serialize a create view operator");
	}

	@Override
	public void setChildPos(final int pos)
	{
	}

	@Override
	public void setNode(final int node)
	{
		this.node = node;
	}

	@Override
	public void setPlan(final Plan plan)
	{
	}

	public void setTransaction(final Transaction tx)
	{
		this.tx = tx;
	}

	@Override
	public void start() throws Exception
	{
	}

	@Override
	public String toString()
	{
		return "CreateViewOperator";
	}

}
