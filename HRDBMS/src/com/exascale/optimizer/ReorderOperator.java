package com.exascale.optimizer;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.misc.DataEndMarker;
import com.exascale.misc.HrdbmsType;
import com.exascale.tables.Plan;

public final class ReorderOperator implements Operator, Serializable
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
	private Operator parent;
	private HashMap<String, String> cols2Types;
	private HashMap<String, Integer> cols2Pos;
	private TreeMap<Integer, String> pos2Col;
	private ArrayList<String> order;
	private transient final MetaData meta;

	private boolean nullOp = false;

	private int node;
	private transient AtomicLong received;
	private transient volatile boolean demReceived;

	public ReorderOperator(final ArrayList<String> order, final MetaData meta) throws Exception
	{
		this.order = order;
		if (order.size() == 0)
		{
			throw new Exception("Reorder operator defined with 0 output columns");
		}
		this.meta = meta;
		received = new AtomicLong(0);
	}

	public static ReorderOperator deserialize(final InputStream in, final HashMap<Long, Object> prev) throws Exception
	{
		final ReorderOperator value = (ReorderOperator)unsafe.allocateInstance(ReorderOperator.class);
		prev.put(OperatorUtils.readLong(in), value);
		value.child = OperatorUtils.deserializeOperator(in, prev);
		value.parent = OperatorUtils.deserializeOperator(in, prev);
		value.cols2Types = OperatorUtils.deserializeStringHM(in, prev);
		value.cols2Pos = OperatorUtils.deserializeStringIntHM(in, prev);
		value.pos2Col = OperatorUtils.deserializeTM(in, prev);
		value.order = OperatorUtils.deserializeALS(in, prev);
		value.nullOp = OperatorUtils.readBool(in);
		value.node = OperatorUtils.readInt(in);
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
				pos2Col = child.getPos2Col();

				final ArrayList<String> newOrder = new ArrayList<String>();
				for (final String col : order)
				{
					final Integer pos = child.getCols2Pos().get(col);
					if (pos != null)
					{
						newOrder.add(col);
					}
					else
					{
						int matches = 0;
						String col2;
						if (col.contains("."))
						{
							col2 = col.substring(col.indexOf('.') + 1);
						}
						else
						{
							col2 = col;
						}

						for (final String col3 : child.getCols2Pos().keySet())
						{
							String col4;
							if (col3.contains("."))
							{
								col4 = col3.substring(col3.indexOf('.') + 1);
							}
							else
							{
								col4 = col3;
							}

							if (col2.equals(col4))
							{
								matches++;
								newOrder.add(col3);
							}
						}

						if (matches != 1)
						{
							throw new Exception("Column not found or ambiguous: " + col);
						}
					}
				}

				order = newOrder;
				// new ArrayList<String>(pos2Col.values());
				if (new ArrayList<String>(pos2Col.values()).equals(order))
				{
					cols2Types = child.getCols2Types();
					cols2Pos = child.getCols2Pos();
					nullOp = true;
				}
				else
				{
					cols2Types = new HashMap<String, String>();
					cols2Pos = new HashMap<String, Integer>();
					pos2Col = new TreeMap<Integer, String>();
					nullOp = false;
					int i = 0;
					for (final String col : order)
					{
						cols2Types.put(col, child.getCols2Types().get(col));
						cols2Pos.put(col, i);
						pos2Col.put(i, col);
						i++;
					}
				}
			}
		}
		else
		{
			throw new IllegalStateException("ReorderOperator only supports 1 child.");
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
	public ReorderOperator clone()
	{
		try
		{
			final ReorderOperator retval = new ReorderOperator(order, meta);
			retval.node = node;
			return retval;
		}
		catch (final Exception e)
		{
			return null;
		}
	}

	@Override
	public void close() throws Exception
	{
		cols2Pos = null;
		cols2Types = null;
		pos2Col = null;
		order = null;
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
		final ArrayList<String> retval = new ArrayList<String>(order);
		return retval;
	}

	@Override
	public Object next(final Operator op) throws Exception
	{
		if (nullOp)
		{
			final Object o = child.next(this);
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
		else
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

			final ArrayList<Object> row = (ArrayList<Object>)o;
			final ArrayList<Object> retval = new ArrayList<Object>(order.size());
			int z = 0;
			final int limit = order.size();
			// for (final String col : order)
			while (z < limit)
			{
				final String col = order.get(z++);
				try
				{
					retval.add(row.get(child.getCols2Pos().get(col)));
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.error("Error in ReorderOperator looking for " + col + " in " + child.getCols2Pos(), e);
					throw e;
				}
			}

			return retval;
		}
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
			throw new Exception("ReorderOperator only supports 1 parent.");
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
		child.reset();
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

		OperatorUtils.writeType(HrdbmsType.REORDER, out);
		prev.put(this, OperatorUtils.writeID(out));
		child.serialize(out, prev);
		parent.serialize(out, prev);
		OperatorUtils.serializeStringHM(cols2Types, out, prev);
		OperatorUtils.serializeStringIntHM(cols2Pos, out, prev);
		OperatorUtils.serializeTM(pos2Col, out, prev);
		OperatorUtils.serializeALS(order, out, prev);
		OperatorUtils.writeBool(nullOp, out);
		OperatorUtils.writeInt(node, out);
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
		child.start();
	}

	@Override
	public String toString()
	{
		return "ReorderOperator: " + order;
	}

}
