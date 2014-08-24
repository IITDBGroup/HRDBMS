package com.exascale.optimizer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeMap;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.misc.DataEndMarker;
import com.exascale.tables.Plan;

public final class IndexOperator implements Operator, Cloneable, Serializable
{
	private final MetaData meta;
	private Operator parent;
	private HashMap<String, String> cols2Types = new HashMap<String, String>();
	private HashMap<String, Integer> cols2Pos = new HashMap<String, Integer>();
	private TreeMap<Integer, String> pos2Col = new TreeMap<Integer, String>();
	private int node;
	protected Index index;
	private int device = -1;
	private volatile boolean forceDone = false;
	private volatile Boolean startCalled = false;
	private DataEndMarker startCalledLock = new DataEndMarker();
	private transient Plan plan;
	
	public void setPlan(Plan plan)
	{
		this.plan = plan;
	}

	public IndexOperator(Index index, MetaData meta)
	{
		this.index = index;
		this.meta = meta;
		final String col = "_RID";
		cols2Types.put(col, "LONG");
		cols2Pos.put(col, 0);
		pos2Col.put(0, col);
	}

	@Override
	public void add(Operator op) throws Exception
	{
		throw new Exception("IndexOperator does not support children.");
	}

	public void addSecondaryFilter(Filter f)
	{
		index.addSecondaryFilter(f);
	}

	@Override
	public ArrayList<Operator> children()
	{
		final ArrayList<Operator> retval = new ArrayList<Operator>(0);
		return retval;
	}

	@Override
	public IndexOperator clone()
	{
		final Index i2 = index.clone();
		final IndexOperator retval = new IndexOperator(i2, meta);
		retval.cols2Pos = (HashMap<String, Integer>)cols2Pos.clone();
		retval.pos2Col = (TreeMap<Integer, String>)pos2Col.clone();
		retval.cols2Types = (HashMap<String, String>)cols2Types.clone();
		retval.node = node;
		retval.device = device;
		return retval;
	}

	@Override
	public void close() throws Exception
	{
		index.close();
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

	public String getFileName()
	{
		return index.getFileName();
	}

	public Filter getFilter()
	{
		return index.getFilter();
	}

	public Index getIndex()
	{
		return index;
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

	public ArrayList<String> getReferencedCols()
	{
		return index.getReferencedCols();
	}

	@Override
	public ArrayList<String> getReferences()
	{
		return null;
	}

	public ArrayList<Filter> getSecondary()
	{
		return index.getSecondary();
	}

	@Override
	public Object next(Operator op) throws Exception
	{
		if (!forceDone)
		{
			Object o = index.next();
			if (o instanceof Exception)
			{
				throw (Exception)o;
			}
			
			return o;
		}
		else
		{
			return new DataEndMarker();
		}
	}

	@Override
	public void nextAll(Operator op) throws Exception
	{
		forceDone = true;
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
			throw new Exception("IndexOperator only supports 1 parent.");
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
		if (!startCalled)
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
			index.reset();
			forceDone = false;
		}
	}

	@Override
	public void setChildPos(int pos)
	{
	}

	public void setDevice(int device)
	{
		this.device = device;
	}

	public void setIndex(Index index)
	{
		this.index = index;
	}

	public void setIndexOnly(ArrayList<String> references, ArrayList<String> types)
	{
		final ArrayList<String> orderedReferences = index.setIndexOnly(references, types);
		int i = 0;
		cols2Types = new HashMap<String, String>();
		cols2Pos = new HashMap<String, Integer>();
		pos2Col = new TreeMap<Integer, String>();
		for (final String col : orderedReferences)
		{
			pos2Col.put(i, col);
			cols2Pos.put(col, i);
			cols2Types.put(col, types.get(references.indexOf(col)));
			i++;
		}
	}

	@Override
	public void setNode(int node)
	{
		this.node = node;
	}

	@Override
	public void start() throws Exception
	{
		synchronized (startCalledLock)
		{
			if (!startCalled)
			{
				startCalled = true;
			}
			else
			{
				HRDBMSWorker.logger.error("Start called more than once on IndexOperator!");
				throw new Exception("Start called more than once on IndexOperator!");
			}
		}
		index.open(device, meta);
	}

	@Override
	public String toString()
	{
		return "IndexOperator(" + index + ")";
	}
}
