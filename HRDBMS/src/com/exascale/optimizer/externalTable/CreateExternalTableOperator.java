package com.exascale.optimizer.externalTable;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.TreeMap;

import com.exascale.managers.HRDBMSWorker;
import com.exascale.misc.DataEndMarker;
import com.exascale.optimizer.ColDef;
import com.exascale.optimizer.MetaData;
import com.exascale.optimizer.Operator;
import com.exascale.tables.Plan;
import com.exascale.tables.Transaction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/** Operator corresponding to a create external table statement */
public final class CreateExternalTableOperator implements Operator, Serializable
{
	private final MetaData meta;
	private HashMap<String, String> cols2Types;
	private HashMap<String, Integer> cols2Pos;
	private TreeMap<Integer, String> pos2Col;
	private int node;
	private boolean done = false;
	private Transaction tx;
	private final String schema;
	private final String table;
	private ArrayList<ColDef> cols;
	private String javaClassName, params;

	public CreateExternalTableOperator(MetaData meta, String schema, String table, ArrayList<ColDef> cols, String javaClassName, String params)
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
	public ArrayList<Operator> children()
	{
		final ArrayList<Operator> retval = new ArrayList<Operator>(1);
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
			Configuration conf = new Configuration();
			conf.addResource(new Path("core-site.xml"));
			conf.addResource(new Path("hdfs-site.xml"));

			//BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
			HRDBMSWorker.logger.info("Enter the file path...");
			String filePath = "hdfs://17.17.0.5:9000/user/root/input/log4j.properties";
			try {
				Path path = new Path(filePath);
				FileSystem fs = path.getFileSystem(conf);
				FSDataInputStream inputStream = fs.open(path);
				HRDBMSWorker.logger.info("isavail " + inputStream.available());
				fs.close();
			} catch(Exception e) {
				HRDBMSWorker.logger.warn("banned", e);
			}
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