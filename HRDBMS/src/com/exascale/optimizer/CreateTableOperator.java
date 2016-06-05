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

public final class CreateTableOperator implements Operator, Serializable
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
	private ArrayList<ColDef> defs;
	private ArrayList<String> pks;
	private final String nodeGroupExp;
	private final String nodeExp;
	private final String deviceExp;
	private final int type;
	private ArrayList<Integer> colOrder;
	private ArrayList<Integer> organization;

	public CreateTableOperator(String schema, String table, ArrayList<ColDef> defs, ArrayList<String> pks, String nodeGroupExp, String nodeExp, String deviceExp, MetaData meta)
	{
		this.meta = meta;
		this.schema = schema;
		this.table = table;
		this.defs = defs;
		this.pks = pks;
		this.nodeGroupExp = nodeGroupExp;
		this.nodeExp = nodeExp;
		this.deviceExp = deviceExp;
		type = 0;
	}

	public CreateTableOperator(String schema, String table, ArrayList<ColDef> defs, ArrayList<String> pks, String nodeGroupExp, String nodeExp, String deviceExp, MetaData meta, int type)
	{
		this.meta = meta;
		this.schema = schema;
		this.table = table;
		this.defs = defs;
		this.pks = pks;
		this.nodeGroupExp = nodeGroupExp;
		this.nodeExp = nodeExp;
		this.deviceExp = deviceExp;
		this.type = type;
	}

	public CreateTableOperator(String schema, String table, ArrayList<ColDef> defs, ArrayList<String> pks, String nodeGroupExp, String nodeExp, String deviceExp, MetaData meta, int type, ArrayList<Integer> colOrder)
	{
		this.meta = meta;
		this.schema = schema;
		this.table = table;
		this.defs = defs;
		this.pks = pks;
		this.nodeGroupExp = nodeGroupExp;
		this.nodeExp = nodeExp;
		this.deviceExp = deviceExp;
		this.type = type;
		this.colOrder = colOrder;
	}

	@Override
	public void add(Operator op) throws Exception
	{
		throw new Exception("CreateTableOperator does not support children");
	}

	@Override
	public ArrayList<Operator> children()
	{
		final ArrayList<Operator> retval = new ArrayList<Operator>(1);
		return retval;
	}

	@Override
	public CreateTableOperator clone()
	{
		final CreateTableOperator retval = new CreateTableOperator(schema, table, defs, pks, nodeGroupExp, nodeExp, deviceExp, meta);
		retval.node = node;
		return retval;
	}

	@Override
	public void close() throws Exception
	{
		cols2Pos = null;
		cols2Types = null;
		pos2Col = null;
		defs = null;
		pks = null;
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
	public Object next(Operator op) throws Exception
	{
		if (!done)
		{
			done = true;
			meta.createTable(schema, table, defs, pks, tx, nodeGroupExp, nodeExp, deviceExp, type, colOrder, organization);
			return 1;
		}
		else
		{
			return new DataEndMarker();
		}
	}

	@Override
	public void nextAll(Operator op) throws Exception
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
	public void registerParent(Operator op) throws Exception
	{
		throw new Exception("CreateTableOperator does not support parents.");
	}

	@Override
	public void removeChild(Operator op)
	{
	}

	@Override
	public void removeParent(Operator op)
	{
	}

	@Override
	public void reset() throws Exception
	{
		throw new Exception("CreateTableOperator is not resetable");
	}

	@Override
	public void serialize(OutputStream out, IdentityHashMap<Object, Long> prev) throws Exception
	{
		throw new Exception("Trying to serialize a create table operator");
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

	public void setOrganization(ArrayList<Integer> organization)
	{
		this.organization = organization;
	}

	@Override
	public void setPlan(Plan plan)
	{
	}

	public void setTransaction(Transaction tx)
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
		return "CreateTableOperator";
	}
}
