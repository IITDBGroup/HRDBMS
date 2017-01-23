package com.exascale.optimizer;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.misc.DataEndMarker;
import com.exascale.tables.Plan;

public final class RenameOperator implements Operator, Serializable
{
	private static sun.misc.Unsafe unsafe;

	static
	{
		try
		{
			final Field f = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
			f.setAccessible(true);
			unsafe = (sun.misc.Unsafe)f.get(null);
		}
		catch (Exception e)
		{
			unsafe = null;
		}
	}
	private Operator child;
	private Operator parent;
	private HashMap<String, String> cols2Types;
	private HashMap<String, Integer> cols2Pos;
	private TreeMap<Integer, String> pos2Col;
	private HashMap<String, String> old2New = new HashMap<String, String>();
	private transient final MetaData meta;
	private ArrayList<String> oldVals;

	private ArrayList<String> newVals;

	private int node;
	private transient AtomicLong received;
	private transient volatile boolean demReceived;

	public RenameOperator(ArrayList<String> oldVals, ArrayList<String> newVals, MetaData meta) throws Exception
	{
		this.oldVals = oldVals;
		this.newVals = newVals;
		int i = 0;
		try
		{
			for (final String oldVal : oldVals)
			{
				old2New.put(oldVal, newVals.get(i));
				i++;
			}
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.error("Old = " + oldVals, e);
			HRDBMSWorker.logger.error("New = " + newVals);
			throw e;
		}

		this.meta = meta;
		received = new AtomicLong(0);
	}

	public static RenameOperator deserialize(InputStream in, HashMap<Long, Object> prev) throws Exception
	{
		RenameOperator value = (RenameOperator)unsafe.allocateInstance(RenameOperator.class);
		prev.put(OperatorUtils.readLong(in), value);
		value.child = OperatorUtils.deserializeOperator(in, prev);
		value.parent = OperatorUtils.deserializeOperator(in, prev);
		value.cols2Types = OperatorUtils.deserializeStringHM(in, prev);
		value.cols2Pos = OperatorUtils.deserializeStringIntHM(in, prev);
		value.pos2Col = OperatorUtils.deserializeTM(in, prev);
		value.old2New = OperatorUtils.deserializeStringHM(in, prev);
		value.oldVals = OperatorUtils.deserializeALS(in, prev);
		value.newVals = OperatorUtils.deserializeALS(in, prev);
		value.node = OperatorUtils.readInt(in);
		value.received = new AtomicLong(0);
		value.demReceived = false;
		return value;
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
				cols2Types = new HashMap<String, String>();
				for (final Map.Entry entry : child.getCols2Types().entrySet())
				{
					final String newVal = old2New.get(entry.getKey());
					if (newVal == null)
					{
						cols2Types.put((String)entry.getKey(), (String)entry.getValue());
					}
					else
					{
						cols2Types.put(newVal, (String)entry.getValue());
					}
				}

				cols2Pos = new HashMap<String, Integer>();
				for (final Map.Entry entry : child.getCols2Pos().entrySet())
				{
					final String newVal = old2New.get(entry.getKey());
					if (newVal == null)
					{
						cols2Pos.put((String)entry.getKey(), (Integer)entry.getValue());
					}
					else
					{
						cols2Pos.put(newVal, (Integer)entry.getValue());
					}
				}

				pos2Col = new TreeMap<Integer, String>();
				for (final Map.Entry entry : child.getPos2Col().entrySet())
				{
					final String newVal = old2New.get(entry.getValue());
					if (newVal == null)
					{
						pos2Col.put((Integer)entry.getKey(), (String)entry.getValue());
					}
					else
					{
						pos2Col.put((Integer)entry.getKey(), newVal);
					}
				}
			}
		}
		else
		{
			throw new Exception("RenameOperator only supports 1 child.");
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
	public RenameOperator clone()
	{
		try
		{
			final RenameOperator retval = new RenameOperator((ArrayList<String>)oldVals.clone(), (ArrayList<String>)newVals.clone(), meta);
			retval.node = node;
			return retval;
		}
		catch (Exception e)
		{
			return null;
		}
	}

	@Override
	public void close() throws Exception
	{
		old2New = null;
		oldVals = null;
		newVals = null;
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

	public HashMap<String, String> getIndexRenames() throws Exception
	{
		final HashMap<String, String> retval = new HashMap<String, String>();
		int i = 0;
		try
		{
			for (final String oldVal : oldVals)
			{
				retval.put(newVals.get(i), oldVal);
				i++;
			}
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.error("", e);
			throw e;
		}

		return retval;
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
		final ArrayList<String> retval = new ArrayList<String>(old2New.keySet());
		return retval;
	}

	public HashMap<String, String> getRenameMap()
	{
		return old2New;
	}

	@Override
	public Object next(Operator op) throws Exception
	{
		Object o = child.next(this);
		if (o instanceof DataEndMarker)
		{
			demReceived = true;
		}
		else
		{
			received.getAndIncrement();
		}
		if (o instanceof Exception)
		{
			throw (Exception)o;
		}

		return o;
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
	public long numRecsReceived()
	{
		return received.get();
	}

	@Override
	public Operator parent()
	{
		return parent;
	}

	@Override
	public boolean receivedDEM()
	{
		return demReceived;
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
			throw new Exception("RenameOperator only supports 1 parent.");
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

	public void reverseUpdateReferences(ArrayList<String> references)
	{
		for (final Map.Entry entry : old2New.entrySet())
		{
			final ArrayList temp = new ArrayList(1);
			temp.add(entry.getValue());
			if (references.removeAll(temp))
			{
				references.add((String)entry.getKey());
			}
		}
	}

	public SelectOperator reverseUpdateSelectOperator(SelectOperator op)
	{
		final SelectOperator retval = op.clone();
		for (final Filter filter : retval.getFilter())
		{
			if (filter.leftIsColumn() && old2New.containsValue(filter.leftColumn()))
			{
				String key = null;
				for (final Map.Entry entry : old2New.entrySet())
				{
					if (entry.getValue().equals(filter.leftColumn()))
					{
						key = (String)entry.getKey();
						break;
					}
				}
				filter.updateLeftColumn(key);
			}

			if (filter.rightIsColumn() && old2New.containsValue(filter.rightColumn()))
			{
				String key = null;
				for (final Map.Entry entry : old2New.entrySet())
				{
					if (entry.getValue().equals(filter.rightColumn()))
					{
						key = (String)entry.getKey();
						break;
					}
				}
				filter.updateRightColumn(key);
			}
		}
		retval.updateReferences();
		return retval;
	}

	@Override
	public void serialize(OutputStream out, IdentityHashMap<Object, Long> prev) throws Exception
	{
		Long id = prev.get(this);
		if (id != null)
		{
			OperatorUtils.serializeReference(id, out);
			return;
		}

		OperatorUtils.writeType(38, out);
		prev.put(this, OperatorUtils.writeID(out));
		child.serialize(out, prev);
		parent.serialize(out, prev);
		OperatorUtils.serializeStringHM(cols2Types, out, prev);
		OperatorUtils.serializeStringIntHM(cols2Pos, out, prev);
		OperatorUtils.serializeTM(pos2Col, out, prev);
		OperatorUtils.serializeStringHM(old2New, out, prev);
		OperatorUtils.serializeALS(oldVals, out, prev);
		OperatorUtils.serializeALS(newVals, out, prev);
		OperatorUtils.writeInt(node, out);
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
	public void setPlan(Plan plan)
	{
	}

	@Override
	public void start() throws Exception
	{
		child.start();
	}

	@Override
	public String toString()
	{
		return "RenameOperator: " + old2New;
	}

	public void updateReferences(ArrayList<String> references)
	{
		for (final Map.Entry entry : old2New.entrySet())
		{
			final ArrayList temp = new ArrayList(1);
			temp.add(entry.getKey());
			if (references.removeAll(temp))
			{
				references.add((String)entry.getValue());
			}
		}
	}

	public void updateSelectOperator(SelectOperator op)
	{
		for (final Filter filter : op.getFilter())
		{
			if (filter.leftIsColumn() && old2New.containsKey(filter.leftColumn()))
			{
				filter.updateLeftColumn(old2New.get(filter.leftColumn()));
			}

			if (filter.rightIsColumn() && old2New.containsKey(filter.rightColumn()))
			{
				filter.updateRightColumn(old2New.get(filter.rightColumn()));
			}
		}

		op.updateReferences();
	}

}
