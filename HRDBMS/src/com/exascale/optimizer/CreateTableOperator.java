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

	public CreateTableOperator(final String schema, final String table, final ArrayList<ColDef> defs, final ArrayList<String> pks, final String nodeGroupExp, final String nodeExp, final String deviceExp, final MetaData meta)
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

	public CreateTableOperator(final String schema, final String table, final ArrayList<ColDef> defs, final ArrayList<String> pks, final String nodeGroupExp, final String nodeExp, final String deviceExp, final MetaData meta, final int type)
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

	public CreateTableOperator(final String schema, final String table, final ArrayList<ColDef> defs, final ArrayList<String> pks, final String nodeGroupExp, final String nodeExp, final String deviceExp, final MetaData meta, final int type, final ArrayList<Integer> colOrder)
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
	public void add(final Operator op) throws Exception
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
	public Object next(final Operator op) throws Exception
	{
		if (!done)
		{
			done = true;
			MetaData.tableMetaLock.lock();
			MetaData.createTable(schema, table, defs, pks, tx, nodeGroupExp, nodeExp, deviceExp, type, colOrder, organization);
			MetaData.tableTypeCache.remove(schema + "." + table);
			MetaData.tableExistenceCache.remove(schema + "." + table);
			MetaData.tableMetaLock.unlock();
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
		throw new Exception("CreateTableOperator does not support parents.");
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
		throw new Exception("CreateTableOperator is not resetable");
	}

	@Override
	public void serialize(final OutputStream out, final IdentityHashMap<Object, Long> prev) throws Exception
	{
		throw new Exception("Trying to serialize a create table operator");
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

	public void setOrganization(final ArrayList<Integer> organization)
	{
		this.organization = organization;
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
		return "CreateTableOperator";
	}
}
