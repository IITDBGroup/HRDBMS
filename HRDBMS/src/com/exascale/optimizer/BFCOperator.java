package com.exascale.optimizer;

import java.io.OutputStream;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.TreeMap;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.misc.BufferedFileChannel;
import com.exascale.misc.DataEndMarker;
import com.exascale.misc.MyDate;
import com.exascale.tables.Plan;

public class BFCOperator implements Operator
{
	protected static long offset;
	private static sun.misc.Unsafe unsafe;
	protected static Charset cs = StandardCharsets.UTF_8;

	static
	{
		try
		{
			final Field f = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
			f.setAccessible(true);
			unsafe = (sun.misc.Unsafe)f.get(null);
			final Field fieldToUpdate = String.class.getDeclaredField("value");
			// get unsafe offset to this field
			offset = unsafe.objectFieldOffset(fieldToUpdate);
		}
		catch (Exception e)
		{
			unsafe = null;
		}
	}

	protected CharsetDecoder cd = cs.newDecoder();

	private Operator parent;
	private final MetaData meta;
	private final BufferedFileChannel bfc;
	private final TreeMap<Integer, String> pos2Col;
	private final HashMap<String, Integer> cols2Pos;
	private final HashMap<String, String> cols2Type;
	private final byte[] sizeBuff = new byte[4];
	private byte[] data = null;

	public BFCOperator(BufferedFileChannel bfc, TreeMap<Integer, String> pos2Col, HashMap<String, String> cols2Type, MetaData meta)
	{
		this.bfc = bfc;
		this.pos2Col = pos2Col;
		this.cols2Type = cols2Type;
		this.meta = meta;
		this.cols2Pos = new HashMap<String, Integer>();
		for (Map.Entry entry : pos2Col.entrySet())
		{
			cols2Pos.put((String)entry.getValue(), (Integer)entry.getKey());
		}
	}

	@Override
	public void add(Operator op) throws Exception
	{
		throw new UnsupportedOperationException("A BFCOperator cannot have children");
	}

	@Override
	public ArrayList<Operator> children()
	{
		return null;
	}

	@Override
	public Operator clone()
	{
		return null;
	}

	@Override
	public void close() throws Exception
	{
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
		return cols2Type;
	}

	@Override
	public MetaData getMeta()
	{
		return meta;
	}

	@Override
	public int getNode()
	{
		return 0;
	}

	@Override
	public TreeMap<Integer, String> getPos2Col()
	{
		return pos2Col;
	}

	@Override
	public ArrayList<String> getReferences()
	{
		return null;
	}

	@Override
	public Object next(Operator op) throws Exception
	{
		ByteBuffer bb = ByteBuffer.wrap(sizeBuff);

		if (bfc.read(bb) == -1)
		{
			return new DataEndMarker();
		}

		final int size = bytesToInt(sizeBuff);

		if (data == null || data.length < size)
		{
			data = new byte[size];
		}

		bb = ByteBuffer.wrap(data);
		bb.limit(size);
		bfc.read(bb);

		return fromBytes(data);
	}

	@Override
	public void nextAll(Operator op) throws Exception
	{
		throw new UnsupportedOperationException("BFCOperator does not support nextAll()");
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
		parent = op;
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
	}

	@Override
	public void serialize(OutputStream out, IdentityHashMap<Object, Long> prev) throws Exception
	{
	}

	@Override
	public void setChildPos(int pos)
	{
	}

	@Override
	public void setNode(int node)
	{
	}

	@Override
	public void setPlan(Plan p)
	{
	}

	@Override
	public void start() throws Exception
	{
	}

	private int bytesToInt(byte[] val)
	{
		final int ret = java.nio.ByteBuffer.wrap(val).getInt();
		return ret;
	}

	private Object fromBytes(byte[] val) throws Exception
	{
		final ByteBuffer bb = ByteBuffer.wrap(val);
		final int numFields = bb.getInt();

		if (numFields < 0)
		{
			HRDBMSWorker.logger.error("Negative number of fields in fromBytes()");
			HRDBMSWorker.logger.error("NumFields = " + numFields);
			throw new Exception("Negative number of fields in fromBytes()");
		}

		bb.position(bb.position() + numFields);
		final byte[] bytes = bb.array();
		if (bytes[4] == 5)
		{
			return new DataEndMarker();
		}
		if (bytes[4] == 10)
		{
			final int length = bb.getInt();
			final byte[] temp = new byte[length];
			bb.get(temp);
			try
			{
				final String o = new String(temp, StandardCharsets.UTF_8);
				return new Exception(o);
			}
			catch (final Exception e)
			{
				throw e;
			}
		}
		final ArrayList<Object> retval = new ArrayList<Object>(numFields);
		int i = 0;
		while (i < numFields)
		{
			if (bytes[i + 4] == 0)
			{
				// long
				final Long o = bb.getLong();
				retval.add(o);
			}
			else if (bytes[i + 4] == 1)
			{
				// integer
				final Integer o = bb.getInt();
				retval.add(o);
			}
			else if (bytes[i + 4] == 2)
			{
				// double
				final Double o = bb.getDouble();
				retval.add(o);
			}
			else if (bytes[i + 4] == 3)
			{
				// date
				final MyDate o = new MyDate(bb.getInt());
				retval.add(o);
			}
			else if (bytes[i + 4] == 4)
			{
				// string
				final int length = bb.getInt();
				final byte[] temp = new byte[length];
				final char[] ca = new char[length];
				bb.get(temp);
				try
				{
					String value = (String)unsafe.allocateInstance(String.class);
					int clen = ((sun.nio.cs.ArrayDecoder)cd).decode(temp, 0, length, ca);
					if (clen == ca.length)
					{
						unsafe.putObject(value, offset, ca);
					}
					else
					{
						char[] v = Arrays.copyOf(ca, clen);
						unsafe.putObject(value, offset, v);
					}
					retval.add(value);
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.error("", e);
					throw e;
				}
			}
			else
			{
				HRDBMSWorker.logger.error("Unknown type " + bytes[i + 4] + " in fromBytes()");
				throw new Exception("Unknown type " + bytes[i + 4] + " in fromBytes()");
			}

			i++;
		}

		return retval;
	}
}
