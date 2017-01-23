package com.exascale.optimizer;

import java.io.OutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import com.exascale.misc.DataEndMarker;
import com.exascale.tables.Plan;
import com.exascale.tables.Transaction;

public final class CreateExternalTableOperator implements Operator, Serializable
{
	private final MetaData meta;
	private HashMap<String, String> cols2Types;
	private HashMap<String, Integer> cols2Pos;
	private TreeMap<Integer, String> pos2Col;
	private Operator parent;
	private int node;
	private boolean done = false;
	private Transaction tx;
	private final String schema;
	private final String table;
	private int type;
	private ArrayList<ColDef> cols;
	private String sourceList;
	private String anyString;
	private String filePathIdentifier;
	private String javaClassName;
	private Properties keyValueList;	
	
	
	
	public CreateExternalTableOperator(MetaData meta, String schema, String table, ArrayList<ColDef> cols, String sourceList, String anyString, String filePathIdentifier, String javaClassName, Properties keyValueList)
	{
		this.meta = meta;
		this.schema = schema;
		this.table = table;
		this.cols = cols;
		this.sourceList = sourceList;
		this.anyString = anyString;
		this.filePathIdentifier = filePathIdentifier;
		this.javaClassName = javaClassName;
		this.keyValueList = keyValueList;
	}

	public CreateExternalTableOperator(MetaData meta, String schema, String table, ArrayList<ColDef> cols, String sourceList, String anyString, String filePathIdentifier)
	{
		this.meta = meta;
		this.schema = schema;
		this.table = table;
		this.cols = cols;
		this.sourceList = sourceList;
		this.anyString = anyString;
		this.filePathIdentifier = filePathIdentifier;
	}	
	
	public CreateExternalTableOperator(MetaData meta, String schema, String table, ArrayList<ColDef> cols, String javaClassName, Properties keyValueList)
	{
		this.meta = meta;
		this.schema = schema;
		this.table = table;
		this.cols = cols;
		this.javaClassName = javaClassName;
		this.keyValueList = keyValueList;
	}

	@Override
	public void add(Operator op) throws Exception
	{
		throw new Exception("CreateExternalTableOperator does not support children");
		
	}
	
	@Override
	public ArrayList<Operator> children()
	{
		final ArrayList<Operator> retval = new ArrayList<Operator>(1);
		return retval;
	}
	
	@Override
	public Operator clone()
	{
		final CreateExternalTableOperator retval = new CreateExternalTableOperator(meta, schema, table, cols, sourceList, anyString, filePathIdentifier, javaClassName, keyValueList);
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
		keyValueList = null;		
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
	public Object next(Operator op) throws Exception
	{
		if (!done)
		{
			done = true;
			meta.createExternalTable(schema, table, cols, tx, sourceList, anyString, filePathIdentifier, javaClassName, keyValueList);
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