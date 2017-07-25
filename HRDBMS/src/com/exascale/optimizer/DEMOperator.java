package com.exascale.optimizer;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.*;

import com.exascale.misc.DataEndMarker;
import com.exascale.misc.HrdbmsType;
import com.exascale.tables.Plan;

public final class DEMOperator implements Operator, Serializable
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
	private transient final MetaData meta;
	private Map<String, String> cols2Types;
	private Map<String, Integer> cols2Pos;
	private Map<Integer, String> pos2Col;

	private Operator parent;

	private int node;

	public DEMOperator(final MetaData meta)
	{
		this.meta = meta;
		cols2Types = new HashMap<String, String>();
		cols2Pos = new HashMap<String, Integer>();
		pos2Col = new TreeMap<Integer, String>();
	}

	public static DEMOperator deserialize(final InputStream in, final Map<Long, Object> prev) throws Exception
	{
		final DEMOperator value = (DEMOperator)unsafe.allocateInstance(DEMOperator.class);
		prev.put(OperatorUtils.readLong(in), value);
		value.cols2Types = OperatorUtils.deserializeStringHM(in, prev);
		value.cols2Pos = OperatorUtils.deserializeStringIntHM(in, prev);
		value.pos2Col = OperatorUtils.deserializeTM(in, prev);
		value.parent = OperatorUtils.deserializeOperator(in, prev);
		value.node = OperatorUtils.readInt(in);
		return value;
	}

	@Override
	public void add(final Operator op) throws Exception
	{
		throw new Exception("DEMOperator does not support children");
	}

	@Override
	public List<Operator> children()
	{
		final List<Operator> retval = new ArrayList<Operator>(1);
		return retval;
	}

	@Override
	public DEMOperator clone()
	{
		final DEMOperator retval = new DEMOperator(meta);
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
		final List<String> retval = new ArrayList<String>(1);
		return retval;
	}

	@Override
	// @?Parallel
	public Object next(final Operator op) throws Exception
	{
		return new DataEndMarker();
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
		return true;
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
			throw new Exception("DEMOperator only supports 1 parent.");
		}
	}

	@Override
	public void removeChild(final Operator op)
	{
	}

	@Override
	public void removeParent(final Operator op)
	{
		parent = null;
	}

	@Override
	public void reset() throws Exception
	{
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

		OperatorUtils.writeType(HrdbmsType.DEMOPERATOR, out);
		prev.put(this, OperatorUtils.writeID(out));
		OperatorUtils.serializeStringHM(cols2Types, out, prev);
		OperatorUtils.serializeStringIntHM(cols2Pos, out, prev);
		OperatorUtils.serializeTM(pos2Col, out, prev);
		parent.serialize(out, prev);
		OperatorUtils.writeInt(node, out);
	}

	@Override
	public void setChildPos(final int pos)
	{
	}

	public void setCols2Pos(final Map<String, Integer> cols2Pos)
	{
		this.cols2Pos = new HashMap<>(cols2Pos);
	}

	public void setCols2Types(final Map<String, String> cols2Types)
	{
		this.cols2Types = new HashMap<>(cols2Types);
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

	public void setPos2Col(final Map<Integer, String> pos2Col)
	{
		this.pos2Col = new HashMap<>(pos2Col);
	}

	@Override
	public void start() throws Exception
	{
	}

	@Override
	public String toString()
	{
		return "DEMOperator";
	}
}
