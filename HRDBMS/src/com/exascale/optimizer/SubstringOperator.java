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
import com.exascale.misc.DataEndMarker;
import com.exascale.tables.Plan;

public final class SubstringOperator implements Operator, Serializable
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
	private String col;
	private String name;
	private int colPos;
	private transient final MetaData meta;
	private int start;

	private int end = -1;

	private int node;
	private transient AtomicLong received;
	private transient volatile boolean demReceived;

	public SubstringOperator(String col, int start, int end, String name, MetaData meta)
	{
		this.col = col;
		this.meta = meta;
		this.name = name;
		this.start = start;
		this.end = end;
		received = new AtomicLong(0);
	}

	public SubstringOperator(String col, int start, String name, MetaData meta)
	{
		this.col = col;
		this.meta = meta;
		this.name = name;
		this.start = start;
		received = new AtomicLong(0);
	}

	public static SubstringOperator deserialize(InputStream in, HashMap<Long, Object> prev) throws Exception
	{
		SubstringOperator value = (SubstringOperator)unsafe.allocateInstance(SubstringOperator.class);
		prev.put(OperatorUtils.readLong(in), value);
		value.child = OperatorUtils.deserializeOperator(in, prev);
		value.parent = OperatorUtils.deserializeOperator(in, prev);
		value.cols2Types = OperatorUtils.deserializeStringHM(in, prev);
		value.cols2Pos = OperatorUtils.deserializeStringIntHM(in, prev);
		value.pos2Col = OperatorUtils.deserializeTM(in, prev);
		value.col = OperatorUtils.readString(in, prev);
		value.name = OperatorUtils.readString(in, prev);
		value.colPos = OperatorUtils.readInt(in);
		value.start = OperatorUtils.readInt(in);
		value.end = OperatorUtils.readInt(in);
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
				cols2Types = (HashMap<String, String>)child.getCols2Types().clone();
				cols2Types.put(name, "CHAR");
				cols2Pos = (HashMap<String, Integer>)child.getCols2Pos().clone();
				cols2Pos.put(name, cols2Pos.size());
				pos2Col = (TreeMap<Integer, String>)child.getPos2Col().clone();
				pos2Col.put(pos2Col.size(), name);
				Integer colPos1 = cols2Pos.get(col);
				if (colPos1 == null)
				{
					int count = 0;
					if (col.startsWith("."))
					{
						col = col.substring(1);
						for (String col3 : cols2Pos.keySet())
						{
							String orig = col3;
							if (col3.contains("."))
							{
								col3 = col3.substring(col3.indexOf('.') + 1);
							}

							if (col3.equals(col))
							{
								col = orig;
								count++;
								colPos1 = cols2Pos.get(orig);
							}

							if (count > 1)
							{
								throw new Exception("Ambiguous column: " + col);
							}
						}
					}
				}

				if (colPos1 == null)
				{
					throw new Exception("Null colPos1 int SubstringOperator");
				}
				colPos = colPos1;
			}
		}
		else
		{
			throw new Exception("SubstringOperator only supports 1 child.");
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
	public SubstringOperator clone()
	{
		final SubstringOperator retval = new SubstringOperator(col, start, end, name, meta);
		retval.node = node;
		return retval;
	}

	@Override
	public void close() throws Exception
	{
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

	public String getOutputCol()
	{
		return name;
	}

	@Override
	public TreeMap<Integer, String> getPos2Col()
	{
		return pos2Col;
	}

	@Override
	public ArrayList<String> getReferences()
	{
		final ArrayList<String> retval = new ArrayList<String>(1);
		retval.add(col);
		return retval;
	}

	@Override
	public Object next(Operator op) throws Exception
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
		final String field = (String)row.get(colPos);
		if (end != -1)
		{
			if (end <= field.length())
			{
				row.add(field.substring(start, end));
			}
			else
			{
				row.add(field.substring(start));
			}
		}
		else
		{
			row.add(field.substring(start));
		}
		return row;
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
			throw new Exception("SubstringOperator only supports 1 parent.");
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

	@Override
	public void serialize(OutputStream out, IdentityHashMap<Object, Long> prev) throws Exception
	{
		Long id = prev.get(this);
		if (id != null)
		{
			OperatorUtils.serializeReference(id, out);
			return;
		}

		OperatorUtils.writeType(47, out);
		prev.put(this, OperatorUtils.writeID(out));
		child.serialize(out, prev);
		parent.serialize(out, prev);
		OperatorUtils.serializeStringHM(cols2Types, out, prev);
		OperatorUtils.serializeStringIntHM(cols2Pos, out, prev);
		OperatorUtils.serializeTM(pos2Col, out, prev);
		OperatorUtils.writeString(col, out, prev);
		OperatorUtils.writeString(name, out, prev);
		OperatorUtils.writeInt(colPos, out);
		OperatorUtils.writeInt(start, out);
		OperatorUtils.writeInt(end, out);
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
		return "SubstringOperator";
	}
}
