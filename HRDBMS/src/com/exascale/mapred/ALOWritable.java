package com.exascale.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.misc.DataEndMarker;
import com.exascale.misc.MyDate;

public class ALOWritable implements Writable
{
	private ArrayList<Object> array;
	
	public ArrayList<Object> get()
	{
		return array;
	}
	
	public void set(ArrayList<Object> array)
	{
		this.array = array;
	}
	
	@Override
	public void readFields(DataInput arg0) throws IOException
	{
		int size = arg0.readInt();
		byte[] data = new byte[size];
		arg0.readFully(data);
		array = (ArrayList<Object>)fromBytes(data);
	}

	@Override
	public void write(DataOutput arg0) throws IOException
	{
		arg0.write(toBytes(array));
	}

	protected byte[] toBytes(Object v) throws IOException
	{
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
			Exception e = (Exception)v;
			byte[] data = null;
			try
			{
				data = e.getMessage().getBytes("UTF-8");
			}
			catch(Exception f)
			{}
			
			int dataLen = data.length;
			int recLen = 9 + dataLen;
			ByteBuffer bb = ByteBuffer.allocate(recLen+4);
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
				size += 8;
			}
			else if (o instanceof String)
			{
				header[i] = (byte)4;
				size += (4 + ((String)o).getBytes("UTF-8").length);
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
				retvalBB.putLong(((MyDate)o).getTime());
			}
			else if (retval[i] == 4)
			{
				byte[] temp = null;
				try
				{
					temp = ((String)o).getBytes("UTF-8");
				}
				catch (final Exception e)
				{
					throw e;
				}
				retvalBB.putInt(temp.length);
				retvalBB.put(temp);
			}

			i++;
		}

		return retval;
	}
	
	private Object fromBytes(byte[] val) throws IOException
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
				final String o = new String(temp, "UTF-8");
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
				final MyDate o = new MyDate(bb.getLong());
				retval.add(o);
			}
			else if (bytes[i + 4] == 4)
			{
				// string
				final int length = bb.getInt();
				final byte[] temp = new byte[length];
				bb.get(temp);
				try
				{
					final String o = new String(temp, "UTF-8");
					retval.add(o);
				}
				catch (final Exception e)
				{
					throw e;
				}
			}
			else
			{
				throw new IOException("Unknown type " + bytes[i + 4] + " in fromBytes()");
			}

			i++;
		}

		return retval;
	}
}
