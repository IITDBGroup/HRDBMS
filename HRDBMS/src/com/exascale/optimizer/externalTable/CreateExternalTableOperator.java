package com.exascale.optimizer.externalTable;

import java.io.OutputStream;
import java.io.Serializable;
import java.util.*;

import com.exascale.misc.DataEndMarker;
import com.exascale.optimizer.ColDef;
import com.exascale.optimizer.MetaData;
import com.exascale.optimizer.Operator;
import com.exascale.tables.Plan;
import com.exascale.tables.Transaction;

/** Operator corresponding to a create external table statement */
public final class CreateExternalTableOperator implements Operator, Serializable
{
	private final MetaData meta;
	private Map<String, String> cols2Types;
	private Map<String, Integer> cols2Pos;
	private Map<Integer, String> pos2Col;
	private int node;
	private boolean done = false;
	private Transaction tx;
	private final String schema;
	private final String table;
	private List<ColDef> cols;
	private String javaClassName, params;

	public CreateExternalTableOperator(MetaData meta, String schema, String table, List<ColDef> cols, String javaClassName, String params)
	{
		this.meta = meta;
		this.schema = schema;
		this.table = table;
		this.cols = cols;
		this.javaClassName = javaClassName;
		this.params = params;
	}

	@Override
	public void add(Operator op) throws Exception
	{
		throw new Exception("CreateExternalTableOperator does not support children");
		
	}
	
	@Override
	public List<Operator> children()
	{
		final List<Operator> retval = new ArrayList<Operator>(1);
		return retval;
	}
	
	@Override
	public Operator clone()
	{
		final CreateExternalTableOperator retval = new CreateExternalTableOperator(meta, schema, table, cols, javaClassName, params);
		retval.node = node;
		return retval;
	}
	
	@Override
	public void close() throws Exception
	{
		cols2Pos = null;
		cols2Types = null;
		pos2Col = null;
		cols = null;
		params = null;
	}
	
	@Override
	public int getChildPos()
	{
		return 0;
	}
	
	@Override
	public Map<String, Integer> getCols2Pos()
	{
		return cols2Pos;
	}
	
	@Override
	public Map<String, String> getCols2Types()
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
	public Map<Integer, String> getPos2Col()
	{
		return pos2Col;
	}
	
	@Override
	public List<String> getReferences()
	{
		final List<String> retval = new ArrayList<String>();
		return retval;
	}
	
	@Override
	public Object next(Operator op) throws Exception
	{
		if (!done)
		{
			done = true;
			meta.getTableMetaLock().lock();
			meta.createExternalTable(schema, table, cols, tx, javaClassName, params);
			meta.getTableTypeCache().remove(schema + "." + table);
			meta.getTableExistenceCache().remove(schema + "." + table);
			meta.getTableMetaLock().unlock();
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
		return null;
	}
	
	@Override
	public boolean receivedDEM()
	{
		return false;
	}
	
	@Override
	public void registerParent(Operator op) throws Exception
	{
		throw new Exception("CreateExternalTableOperator does not support parents.");		
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
		throw new Exception("CreateExternalTableOperator is not resetable");		
	}
	
	@Override
	public void serialize(OutputStream out, IdentityHashMap<Object, Long> prev) throws Exception
	{
		throw new Exception("Trying to serialize a create external table operator");		
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
	public void setPlan(Plan p)
	{
	}
	
	@Override
	public void start() throws Exception
	{
	}	
	
	public void setTransaction(Transaction tx)
	{
		this.tx = tx;
	}
	
	@Override
	public String toString()
	{
		return "CreateExternalTableOperator";
	}
}