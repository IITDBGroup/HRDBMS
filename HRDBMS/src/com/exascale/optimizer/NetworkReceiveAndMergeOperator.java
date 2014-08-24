package com.exascale.optimizer;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import com.exascale.compression.CompressedSocket;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.misc.DataEndMarker;
import com.exascale.misc.MyDate;
import com.exascale.threads.ThreadPoolThread;

public final class NetworkReceiveAndMergeOperator extends NetworkReceiveOperator implements Operator, Serializable
{
	private final ArrayList<String> sortCols;

	private final ArrayList<Boolean> orders;

	private int[] sortPos;

	public NetworkReceiveAndMergeOperator(ArrayList<String> sortCols, ArrayList<Boolean> orders, MetaData meta)
	{
		super(meta);
		this.sortCols = sortCols;
		this.orders = orders;
	}

	protected static byte[] intToBytes(int val)
	{
		final byte[] buff = new byte[4];
		buff[0] = (byte)(val >> 24);
		buff[1] = (byte)((val & 0x00FF0000) >> 16);
		buff[2] = (byte)((val & 0x0000FF00) >> 8);
		buff[3] = (byte)((val & 0x000000FF));
		return buff;
	}

	@Override
	public void add(Operator op) throws Exception
	{
		try
		{
			super.add(op);
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.error("", e);
			throw e;
		}
		if (sortPos == null)
		{
			int i = 0;
			sortPos = new int[sortCols.size()];
			for (final String sortCol : sortCols)
			{
				sortPos[i] = cols2Pos.get(sortCol);
				i++;
			}
		}
	}

	@Override
	public NetworkReceiveAndMergeOperator clone()
	{
		final NetworkReceiveAndMergeOperator retval = new NetworkReceiveAndMergeOperator(sortCols, orders, meta);
		retval.node = node;
		return retval;
	}

	@Override
	public MetaData getMeta()
	{
		return meta;
	}

	@Override
	public Object next(Operator op2) throws Exception
	{
		if (!fullyStarted)
		{
			synchronized (this)
			{
				if (!fullyStarted)
				{
					fullyStarted = true;
					for (final Operator op : children)
					{
						final NetworkSendOperator child = (NetworkSendOperator)op;
						child.clearParent();
						final CompressedSocket sock = new CompressedSocket(meta.getHostNameForNode(child.getNode()), WORKER_PORT);
						socks.put(child, sock);
						final OutputStream out = sock.getOutputStream();
						outs.put(child, out);
						final InputStream in = sock.getInputStream();
						ins.put(child, new BufferedInputStream(in, 65536));
						final byte[] command = "REMOTTRE".getBytes("UTF-8");
						final byte[] from = intToBytes(-1);
						final byte[] to = intToBytes(child.getNode());
						final byte[] data = new byte[command.length + from.length + to.length];
						System.arraycopy(command, 0, data, 0, 8);
						System.arraycopy(from, 0, data, 8, 4);
						System.arraycopy(to, 0, data, 12, 4);
						out.write(data);
						out.flush();
						final ObjectOutputStream objOut = new ObjectOutputStream(out);
						objOuts.put(child, objOut);
						// Field[] fields =
						// child.getClass().getDeclaredFields();
						// for (Field field : fields)
						// {
						// printHierarchy("child", field, child);
						// }
						objOut.writeObject(child);
						objOut.flush();
						out.flush();
					}

					new InitThread().start();
				}
			}
		}
		Object o;
		o = outBuffer.take();

		if (o instanceof DataEndMarker)
		{
			o = outBuffer.peek();
			if (o == null)
			{
				outBuffer.put(new DataEndMarker());
				return new DataEndMarker();
			}
			else
			{
				outBuffer.put(new DataEndMarker());
				return o;
			}
		}
		
		if (o instanceof Exception)
		{
			throw (Exception)o;
		}
		return o;
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
			throw new Exception("NetworkReceiveAndMergeOperator only supports 1 parent.");
		}
	}

	@Override
	public String toString()
	{
		return "NetworkReceiveAndMergeOperator(" + node + ")";
	}

	private int bytesToInt(byte[] val)
	{
		final int ret = java.nio.ByteBuffer.wrap(val).getInt();
		return ret;
	}

	private final class InitThread extends ThreadPoolThread
	{
		@Override
		public void run()
		{
			start = System.currentTimeMillis();
			final ReadThread readThread = new ReadThread(children);
			readThread.start();
			while (true)
			{
				try
				{
					readThread.join();
					break;
				}
				catch (final InterruptedException e)
				{
				}
			}
			HRDBMSWorker.logger.debug("NetworkReceiveOperator: " + ((bytes.get() / ((System.currentTimeMillis() - start) / 1000.0)) / (1024.0 * 1024.0)) + "MB/sec");

			while (true)
			{
				try
				{
					outBuffer.put(new DataEndMarker());
					break;
				}
				catch (final Exception e)
				{
				}
			}
		}
	}

	private final class ReadThread extends ThreadPoolThread
	{
		private final ArrayList<Operator> children;
		private final HashMap<Operator, ArrayList<Object>> rows = new HashMap<Operator, ArrayList<Object>>();
		private Map.Entry minEntry;

		public ReadThread(ArrayList<Operator> children)
		{
			this.children = children;
		}

		@Override
		public void run()
		{
			for (final Operator op : children)
			{
				try
				{
					final ArrayList<Object> row = readRow(op);
					if (row != null)
					{
						rows.put(op, row);
					}
				}
				catch(Exception e)
				{
					try
					{
						outBuffer.put(e);
					}
					catch(Exception f)
					{}
					return;
				}
			}

			while (rows.size() > 0)
			{
				minEntry = null;
				for (final Map.Entry entry : rows.entrySet())
				{
					if (minEntry == null)
					{
						minEntry = entry;
					}
					else
					{
						try
						{
							if (compare((ArrayList<Object>)entry.getValue(), (ArrayList<Object>)minEntry.getValue()) < 0)
							{
								minEntry = entry;
							}
						}
						catch (final Exception e)
						{
							HRDBMSWorker.logger.error("", e);
							try
							{
								outBuffer.put(e);
							}
							catch(Exception f)
							{}
							return;
						}
					}
				}

				while (true)
				{
					try
					{
						outBuffer.put(minEntry.getValue());
						break;
					}
					catch (final Exception e)
					{
					}
				}
				try
				{
					final ArrayList<Object> row = readRow((Operator)minEntry.getKey());
					if (row != null)
					{
						rows.put((Operator)minEntry.getKey(), row);
					}
					else
					{
						rows.remove(minEntry.getKey());
					}
				}
				catch(Exception e)
				{
					try
					{
						outBuffer.put(e);
					}
					catch(Exception f)
					{}
					return;
				}
			}
		}

		private int compare(ArrayList<Object> lhs, ArrayList<Object> rhs) throws Exception
		{
			int result;
			int i = 0;

			for (final int pos : sortPos)
			{
				final Object lField = lhs.get(pos);
				final Object rField = rhs.get(pos);

				if (lField instanceof Integer)
				{
					result = ((Integer)lField).compareTo((Integer)rField);
				}
				else if (lField instanceof Long)
				{
					result = ((Long)lField).compareTo((Long)rField);
				}
				else if (lField instanceof Double)
				{
					result = ((Double)lField).compareTo((Double)rField);
				}
				else if (lField instanceof String)
				{
					result = ((String)lField).compareTo((String)rField);
				}
				else if (lField instanceof MyDate)
				{
					result = ((MyDate)lField).compareTo(rField);
				}
				else
				{
					throw new Exception("Unknown type in SortOperator.compare(): " + lField.getClass());
				}

				if (orders.get(i))
				{
					if (result > 0)
					{
						return 1;
					}
					else if (result < 0)
					{
						return -1;
					}
				}
				else
				{
					if (result > 0)
					{
						return -1;
					}
					else if (result < 0)
					{
						return 1;
					}
				}

				i++;
			}

			return 0;
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

		private ArrayList<Object> readRow(Operator op) throws Exception
		{
			try
			{
				final InputStream in = ins.get(op);
				final byte[] sizeBuff = new byte[4];
				while (true)
				{
					int count = 0;
					while (count < 4)
					{
						count += in.read(sizeBuff, count, 4 - count);
					}
					bytes.getAndAdd(4);
					final int size = bytesToInt(sizeBuff);

					final byte[] data = new byte[size];
					count = 0;
					while (count < size)
					{
						count += in.read(data, count, size - count);
					}
					bytes.getAndAdd(size);
					final Object row = fromBytes(data);

					if (row instanceof DataEndMarker)
					{
						return null;
					}
					
					if (row instanceof Exception)
					{
						throw (Exception)row;
					}

					readCounter.getAndIncrement();
					return (ArrayList<Object>)row;
				}
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				throw e;
			}
		}
	}
}
