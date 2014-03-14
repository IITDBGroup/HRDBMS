package com.exascale.optimizer.testing;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.exascale.optimizer.testing.ResourceManager.DiskBackedHashMap;

public final class IndexOperator implements Operator, Cloneable, Serializable
{
	protected MetaData meta;
	protected Operator parent;
	protected HashMap<String, String> cols2Types = new HashMap<String, String>();
	protected HashMap<String, Integer> cols2Pos = new HashMap<String, Integer>();
	protected TreeMap<Integer, String> pos2Col = new TreeMap<Integer, String>();
	protected int node;
	protected Index index;
	protected int device = -1;
	protected volatile boolean forceDone = false;
	protected volatile Boolean startCalled = false;
	
	public void setChildPos(int pos)
	{
	}
	
	public void setDevice(int device)
	{
		this.device = device;
	}
	
	public int getChildPos()
	{
		return 0;
	}
	
	public IndexOperator(Index index, MetaData meta)
	{
		this.index = index;
		this.meta = meta;
		String col = "_RID";
		cols2Types.put(col, "LONG");
		cols2Pos.put(col, 0);
		pos2Col.put(0, col);
	}
	
	public IndexOperator clone()
	{
		Index i2 = index.clone();
		IndexOperator retval = new IndexOperator(i2, meta);
		retval.cols2Pos = (HashMap<String, Integer>)cols2Pos.clone();
		retval.pos2Col = (TreeMap<Integer, String>)pos2Col.clone();
		retval.cols2Types = (HashMap<String, String>)cols2Types.clone();
		retval.node = node;
		retval.device = device;
		return retval;
	}
	
	public MetaData getMeta()
	{
		return meta;
	}
	
	public int getNode()
	{
		return node;
	}
	
	public void setNode(int node)
	{
		this.node = node;
	}
	
	public ArrayList<String> getReferences()
	{
		return null;
	}
	
	public ArrayList<Operator> children()
	{
		ArrayList<Operator> retval = new ArrayList<Operator>(0);
		return retval;
	}
	
	public Operator parent()
	{
		return parent;
	}
	
	public void add(Operator op) throws Exception
	{
		throw new Exception("IndexOperator does not support children.");
	}
	
	public void removeChild(Operator op)
	{
	}
	
	public void removeParent(Operator op)
	{
		parent = null;
	}
	
	@Override
	public void start() throws Exception 
	{
		synchronized(startCalled)
		{
			if (!startCalled)
			{
				startCalled = true;
			}
			else
			{
				System.out.println("Start called more than once on IndexOperator!");
				System.exit(1);
			}
		}
		index.open(device, meta);
	}
	
	public void nextAll(Operator op) throws Exception
	{
		forceDone = true;
	}

	@Override
	public Object next(Operator op) throws Exception 
	{
		if (!forceDone)
		{
			return index.next();
		}
		else
		{
			return new DataEndMarker();
		}
	}

	@Override
	public void close() throws Exception 
	{
		index.close();
	}

	public void registerParent(Operator op) throws Exception
	{
		if (parent == null)
		{
			parent = op;
		}
		else
		{
			throw new Exception("IndexOperator only supports 1 parent.");
		}
	}

	@Override
	public HashMap<String, String> getCols2Types() {
		return cols2Types;
	}

	@Override
	public HashMap<String, Integer> getCols2Pos() {
		return cols2Pos;
	}

	@Override
	public TreeMap<Integer, String> getPos2Col() {
		return pos2Col;
	}
	
	public String toString()
	{
		return "IndexOperator(" + index + ")";
	}
	
	public Index getIndex()
	{
		return index;
	}
	
	public String getFileName()
	{
		return index.getFileName();
	}
	
	public Filter getFilter()
	{
		return index.getFilter();
	}
	
	public void addSecondaryFilter(Filter f)
	{
		index.addSecondaryFilter(f);
	}
	
	public ArrayList<String> getReferencedCols()
	{
		return index.getReferencedCols();
	}
	
	public ArrayList<Filter> getSecondary()
	{
		return index.getSecondary();
	}
	
	public void setIndex(Index index)
	{
		this.index = index;
	}
	
	public void setIndexOnly(ArrayList<String> references, ArrayList<String> types)
	{
		ArrayList<String> orderedReferences = index.setIndexOnly(references, types);
		int i = 0;
		cols2Types = new HashMap<String, String>();
		cols2Pos = new HashMap<String, Integer>();
		pos2Col = new TreeMap<Integer, String>();
		for (String col : orderedReferences)
		{
			pos2Col.put(i, col);
			cols2Pos.put(col, i);
			cols2Types.put(col, types.get(references.indexOf(col)));
			i++;
		}
	}
	
	public void reset()
	{
		if (!startCalled)
		{
			try
			{
				start();
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
		}
		else
		{
			index.reset();
			forceDone = false;
		}
	}
}
