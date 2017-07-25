package com.exascale.optimizer;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.misc.DataEndMarker;
import com.exascale.misc.HrdbmsType;
import com.exascale.tables.Plan;

public final class ProjectOperator implements Operator, Serializable
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
		catch (final Exception e)
		{
			unsafe = null;
		}
	}
	private Operator child;
	private List<String> cols;
	private transient final MetaData meta;
	private Map<String, String> cols2Types;
	private Map<String, Integer> cols2Pos;
	private Map<Integer, String> pos2Col;
	private Operator parent;
	private Map<String, Integer> childCols2Pos;
	private int node;

	private transient List<Integer> pos2Get;

	private volatile boolean startDone = false;
	private transient AtomicLong received;
	private transient volatile boolean demReceived;

	public ProjectOperator(final List<String> cols, final MetaData meta)
	{
		this.cols = cols;
		this.meta = meta;
		received = new AtomicLong(0);
	}

	public static ProjectOperator deserialize(final InputStream in, final Map<Long, Object> prev) throws Exception
	{
		final ProjectOperator value = (ProjectOperator)unsafe.allocateInstance(ProjectOperator.class);
		prev.put(OperatorUtils.readLong(in), value);
		value.child = OperatorUtils.deserializeOperator(in, prev);
		value.cols = OperatorUtils.deserializeALS(in, prev);
		value.cols2Types = OperatorUtils.deserializeStringHM(in, prev);
		value.cols2Pos = OperatorUtils.deserializeStringIntHM(in, prev);
		value.pos2Col = OperatorUtils.deserializeTM(in, prev);
		value.parent = OperatorUtils.deserializeOperator(in, prev);
		value.childCols2Pos = OperatorUtils.deserializeStringIntHM(in, prev);
		value.node = OperatorUtils.readInt(in);
		value.startDone = OperatorUtils.readBool(in);
		value.received = new AtomicLong(0);
		value.demReceived = false;
		return value;
	}

	@Override
	public void add(final Operator op) throws Exception
	{
		if (child == null)
		{
			child = op;
			child.registerParent(this);
			if (child.getCols2Types() != null)
			{
				childCols2Pos = child.getCols2Pos();
				Map temp = new HashMap<>(child.getCols2Types());
				cols2Types = new HashMap<String, String>();
				Set<Map.Entry> set = temp.entrySet();
				for (final Map.Entry entry : set)
				{
					if (cols.contains(entry.getKey()))
					{
						cols2Types.put((String)entry.getKey(), (String)entry.getValue());
					}
				}

				temp = child.getPos2Col();
				cols2Pos = new HashMap<String, Integer>();
				pos2Col = new TreeMap<Integer, String>();
				set = temp.entrySet();
				int i = 0;
				for (final Map.Entry entry : set)
				{
					if (cols.contains(entry.getValue()))
					{
						pos2Col.put(i, (String)entry.getValue());
						cols2Pos.put((String)entry.getValue(), i);
						i++;
					}
				}
			}
		}
		else
		{
			throw new Exception("ProjectOperator only supports 1 child.");
		}
	}

	@Override
	public List<Operator> children()
	{
		final List<Operator> retval = new ArrayList<Operator>(1);
		retval.add(child);
		return retval;
	}

	@Override
	public ProjectOperator clone()
	{
		final ProjectOperator retval = new ProjectOperator(cols, meta);
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
		childCols2Pos = null;
		pos2Get = null;
		child.close();
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
		final List<String> retval = new ArrayList<String>(cols);
		return retval;
	}

	@Override
	// @?Parallel
	public Object next(final Operator op) throws Exception
	{
		final Object o = child.next(this);
		if (o instanceof DataEndMarker)
		{
			demReceived = true;
			return o;
		}
		else
		{
			received.getAndIncrement();
		}

		if (o instanceof Exception)
		{
			throw (Exception)o;
		}

		final List<Object> row = (List<Object>)o;
		final List<Object> retval = new ArrayList<Object>(pos2Get.size());
		int z = 0;
		final int limit = pos2Get.size();
		// for (final int pos : pos2Get)
		while (z < limit)
		{
			final int pos = pos2Get.get(z++);
			retval.add(row.get(pos));
		}

		return retval;
	}

	@Override
	public void nextAll(final Operator op) throws Exception
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
	public void registerParent(final Operator op) throws Exception
	{
		if (parent == null)
		{
			parent = op;
		}
		else
		{
			throw new Exception("ProjectOperator only supports 1 parent.");
		}
	}

	@Override
	public void removeChild(final Operator op)
	{
		if (op == child)
		{
			child = null;
			op.removeParent(this);
		}
	}

	@Override
	public void removeParent(final Operator op)
	{
		parent = null;
	}

	@Override
	public void reset() throws Exception
	{
		if (!startDone)
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
			child.reset();
		}
	}

	@Override
	public void serialize(final OutputStream out, final IdentityHashMap<Object, Long> prev) throws Exception
	{
		final Long id = prev.get(this);
		if (id != null)
		{
			OperatorUtils.serializeReference(id, out);
			return;
		}

		OperatorUtils.writeType(HrdbmsType.PROJECT, out);
		prev.put(this, OperatorUtils.writeID(out));
		child.serialize(out, prev);
		OperatorUtils.serializeALS(cols, out, prev);
		OperatorUtils.serializeStringHM(cols2Types, out, prev);
		OperatorUtils.serializeStringIntHM(cols2Pos, out, prev);
		OperatorUtils.serializeTM(pos2Col, out, prev);
		parent.serialize(out, prev);
		OperatorUtils.serializeStringIntHM(childCols2Pos, out, prev);
		OperatorUtils.writeInt(node, out);
		OperatorUtils.writeBool(startDone, out);
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

	@Override
	public void start() throws Exception
	{
		// HRDBMSWorker.logger.debug("ProjectOperator starting with cols = " +
		// cols + " and child cols = " + child.getCols2Pos().keySet());
		pos2Get = new ArrayList<Integer>();
		startDone = true;
		child.start();
		for (final String col : pos2Col.values())
		{
			pos2Get.add(childCols2Pos.get(col));
		}
	}

	@Override
	public String toString()
	{
		return "ProjectOperator: " + cols;
	}

}
