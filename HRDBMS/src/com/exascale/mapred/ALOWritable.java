package com.exascale.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import org.apache.hadoop.io.Writable;
import com.exascale.misc.DataEndMarker;
import com.exascale.misc.MyDate;

public class ALOWritable implements Writable
{
	private static Charset cs = StandardCharsets.UTF_8;
	private static sun.misc.Unsafe unsafe;
	private static long offset;

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
		catch (final Exception e)
		{
			unsafe = null;
		}
	}
	private ArrayList<Object> array;
	private final CharsetDecoder cd = cs.newDecoder();

	private final CharsetEncoder ce = cs.newEncoder();

	public ArrayList<Object> get()
	{
		return array;
	}

	@Override
	public void readFields(final DataInput arg0) throws IOException
	{
		try
		{
			final int size = arg0.readInt();
			final byte[] data = new byte[size];
			arg0.readFully(data);
			array = (ArrayList<Object>)fromBytes(data);
		}
		catch (final Exception e)
		{
			if (e instanceof IOException)
			{
				throw (IOException)e;
			}
			else
			{
				throw new IOException(e.getMessage());
			}
		}
	}

	public void set(final ArrayList<Object> array)
	{
		this.array = array;
	}

	@Override
	public void write(final DataOutput arg0) throws IOException
	{
		arg0.write(toBytes(array));
	}

	private Object fromBytes(final byte[] val) throws Exception
	{
		final ByteBuffer bb = ByteBuffer.wrap(val);
		final int numFields = bb.getInt();

		if (numFields < 0)
		{
			throw new IOException("Negative number of fields in fromBytes()");
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
				// final String o = new String(temp, StandardCharsets.UTF_8);
				final String value = (String)unsafe.allocateInstance(String.class);
				final int clen = ((sun.nio.cs.ArrayDecoder)cd).decode(temp, 0, length, ca);
				if (clen == ca.length)
				{
					unsafe.putObject(value, offset, ca);
				}
				else
				{
					final char[] v = Arrays.copyOf(ca, clen);
					unsafe.putObject(value, offset, v);
				}
				retval.add(value);
			}
			else
			{
				throw new IOException("Unknown type " + bytes[i + 4] + " in fromBytes()");
			}

			i++;
		}

		return retval;
	}

	protected byte[] toBytes(final Object v) throws IOException
	{
		ArrayList<byte[]> bytes = null;
		ArrayList<Object> val = null;
		if (v instanceof ArrayList)
		{
			val = (ArrayList<Object>)v;
			if (val.size() == 0)
			{
				throw new IOException("Empty ArrayList in toBytes()");
			}
		}
		else if (v instanceof Exception)
		{
			final Exception e = (Exception)v;
			byte[] data = null;
			try
			{
				data = e.getMessage().getBytes(StandardCharsets.UTF_8);
			}
			catch (final Exception f)
			{
			}

			final int dataLen = data.length;
			final int recLen = 9 + dataLen;
			final ByteBuffer bb = ByteBuffer.allocate(recLen + 4);
			bb.position(0);
			bb.putInt(recLen);
			bb.putInt(1);
			bb.put((byte)10);
			bb.putInt(dataLen);
			bb.put(data);
			return bb.array();
		}
		else
		{
			final byte[] retval = new byte[9];
			retval[0] = 0;
			retval[1] = 0;
			retval[2] = 0;
			retval[3] = 5;
			retval[4] = 0;
			retval[5] = 0;
			retval[6] = 0;
			retval[7] = 1;
			retval[8] = 5;
			return retval;
		}

		int size = val.size() + 8;
		final byte[] header = new byte[size];
		int i = 8;
		for (final Object o : val)
		{
			if (o instanceof Long)
			{
				header[i] = (byte)0;
				size += 8;
			}
			else if (o instanceof Integer)
			{
				header[i] = (byte)1;
				size += 4;
			}
			else if (o instanceof Double)
			{
				header[i] = (byte)2;
				size += 8;
			}
			else if (o instanceof MyDate)
			{
				header[i] = (byte)3;
				size += 4;
			}
			else if (o instanceof String)
			{
				header[i] = (byte)4;
				// byte[] b = ((String)o).getBytes(StandardCharsets.UTF_8);
				final byte[] ba = new byte[((String)o).length() << 2];
				final char[] value = (char[])unsafe.getObject(o, offset);
				final int blen = ((sun.nio.cs.ArrayEncoder)ce).encode(value, 0, value.length, ba);
				final byte[] b = Arrays.copyOf(ba, blen);
				size += (4 + b.length);
				if (bytes == null)
				{
					bytes = new ArrayList<byte[]>();
					bytes.add(b);
				}
				else
				{
					bytes.add(b);
				}
			}
			else
			{
				throw new IOException("Unknown type " + o.getClass() + " in toBytes()");
			}

			i++;
		}

		final byte[] retval = new byte[size];
		// System.out.println("In toBytes(), row has " + val.size() +
		// " columns, object occupies " + size + " bytes");
		System.arraycopy(header, 0, retval, 0, header.length);
		i = 8;
		final ByteBuffer retvalBB = ByteBuffer.wrap(retval);
		retvalBB.putInt(size - 4);
		retvalBB.putInt(val.size());
		retvalBB.position(header.length);
		int x = 0;
		for (final Object o : val)
		{
			if (retval[i] == 0)
			{
				retvalBB.putLong((Long)o);
			}
			else if (retval[i] == 1)
			{
				retvalBB.putInt((Integer)o);
			}
			else if (retval[i] == 2)
			{
				retvalBB.putDouble((Double)o);
			}
			else if (retval[i] == 3)
			{
				retvalBB.putInt(((MyDate)o).getTime());
			}
			else if (retval[i] == 4)
			{
				final byte[] temp = bytes.get(x);
				x++;
				retvalBB.putInt(temp.length);
				retvalBB.put(temp);
			}

			i++;
		}

		return retval;
	}
}
