package com.exascale.mapred;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import com.exascale.misc.MyDate;
import com.exascale.optimizer.MetaData;

public class LoadRecordWriter extends RecordWriter
{
	private static HashMap<String, LoadRecordWriter> writers = new HashMap<String, LoadRecordWriter>();
	private final String table;
	private int lastNode = -1;
	public HashMap<Integer, ArrayList<ArrayList<Object>>> rows = new HashMap<Integer, ArrayList<ArrayList<Object>>>();
	public HashMap<Integer, ArrayList<ArrayList<Object>>> processing;
	private final String portString;
	public LoadThread thread = null;
	private int size = 0;

	public LoadRecordWriter(final String table, final String portString, final String hrdbmsHome)
	{
		this.table = table;
		this.portString = portString;
	}

	public static LoadRecordWriter get(final String table, final String portString, final String hrdbmsHome)
	{
		synchronized (writers)
		{
			LoadRecordWriter retval = writers.get(table);
			if (retval == null)
			{
				retval = new LoadRecordWriter(table, portString, hrdbmsHome);
				writers.put(table, retval);
			}

			return retval;
		}
	}

	private static void getConfirmation(final Socket sock) throws Exception
	{
		final InputStream in = sock.getInputStream();
		final byte[] inMsg = new byte[2];

		int count = 0;
		while (count < 2)
		{
			try
			{
				final int temp = in.read(inMsg, count, 2 - count);
				if (temp == -1)
				{
					in.close();
					throw new Exception();
				}
				else
				{
					count += temp;
				}
			}
			catch (final Exception e)
			{
				in.close();
				throw new Exception();
			}
		}

		final String inStr = new String(inMsg, StandardCharsets.UTF_8);
		if (!inStr.equals("OK"))
		{
			in.close();
			throw new Exception();
		}

		try
		{
			in.close();
		}
		catch (final Exception e)
		{
		}
	}

	private static byte[] intToBytes(final int val)
	{
		final byte[] buff = new byte[4];
		buff[0] = (byte)(val >> 24);
		buff[1] = (byte)((val & 0x00FF0000) >> 16);
		buff[2] = (byte)((val & 0x0000FF00) >> 8);
		buff[3] = (byte)((val & 0x000000FF));
		return buff;
	}

	private static final byte[] rsToBytes(final ArrayList<ArrayList<Object>> rows) throws Exception
	{
		final ByteBuffer[] results = new ByteBuffer[rows.size()];
		int rIndex = 0;
		final ArrayList<byte[]> bytes = new ArrayList<byte[]>();
		for (final ArrayList<Object> val : rows)
		{
			bytes.clear();
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
					final byte[] b = ((String)o).getBytes(StandardCharsets.UTF_8);
					size += (4 + b.length);
					bytes.add(b);
				}
				// else if (o instanceof AtomicLong)
				// {
				// header[i] = (byte)6;
				// size += 8;
				// }
				// else if (o instanceof AtomicBigDecimal)
				// {
				// header[i] = (byte)7;
				// size += 8;
				// }
				else if (o instanceof ArrayList)
				{
					if (((ArrayList)o).size() != 0)
					{
						final Exception e = new Exception("Non-zero size ArrayList in toBytes()");
						throw e;
					}
					header[i] = (byte)8;
				}
				else
				{
					throw new Exception("Unknown type " + o.getClass() + " in toBytes()");
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
				// else if (retval[i] == 6)
				// {
				// retvalBB.putLong(((AtomicLong)o).get());
				// }
				// else if (retval[i] == 7)
				// {
				// retvalBB.putDouble(((AtomicBigDecimal)o).get().doubleValue());
				// }
				else if (retval[i] == 8)
				{
				}

				i++;
			}

			results[rIndex++] = retvalBB;
		}

		int count = 0;
		for (final ByteBuffer bb : results)
		{
			count += bb.capacity();
		}
		final byte[] retval = new byte[count + 4];
		final ByteBuffer temp = ByteBuffer.allocate(4);
		temp.asIntBuffer().put(results.length);
		System.arraycopy(temp.array(), 0, retval, 0, 4);
		int retvalPos = 4;
		for (final ByteBuffer bb : results)
		{
			final byte[] ba = bb.array();
			System.arraycopy(ba, 0, retval, retvalPos, ba.length);
			retvalPos += ba.length;
		}

		return retval;
	}

	@Override
	public synchronized void close(final TaskAttemptContext arg0) throws IOException, InterruptedException
	{
		if (rows.size() > 0)
		{
			flush();
		}
	}

	public synchronized void flush() throws IOException
	{
		String hostname;
		processing = rows;
		rows = new HashMap<Integer, ArrayList<ArrayList<Object>>>();
		size = 0;
		try
		{
			// new MetaData(hrdbmsHome);
			hostname = MetaData.getHostNameForNode(lastNode);
			// LoadReducer.out.println("Node# " + lastNode + " is resolving to "
			// + hostname);
			// LoadReducer.out.flush();
		}
		catch (final Exception e)
		{
			throw new IOException(e.getMessage());
		}

		if (thread != null)
		{
			while (true)
			{
				try
				{
					thread.join();
					break;
				}
				catch (final InterruptedException e)
				{
				}
			}

			if (!thread.getOK())
			{
				throw new IOException();
			}
			thread = null;
		}

		thread = new LoadThread(hostname, portString, table, processing);
		thread.start();
	}

	@Override
	public synchronized void write(final Object arg0, final Object arg1) throws IOException, InterruptedException
	{
		final MyLongWritable key = (MyLongWritable)arg0;
		final int node = (int)(key.get() >> 32);
		final int device = (int)(key.get() & 0x00000000FFFFFFFF);
		if (node != lastNode)
		{
			if (lastNode == -1)
			{
				lastNode = node;
			}
			else
			{
				flush();
				lastNode = node;
			}
		}
		final ALOWritable alo = (ALOWritable)arg1;
		ArrayList<ArrayList<Object>> devRows = rows.get(device);
		if (devRows == null)
		{
			devRows = new ArrayList<ArrayList<Object>>();
			devRows.add(alo.get());
		}
		else
		{
			devRows.add(alo.get());
		}
		rows.put(device, devRows);
		size++;
		if (size > 5000000)
		{
			flush();
		}
	}

	public static class ConfirmationThread extends Thread
	{
		private boolean ok = true;
		private final String hostname;
		private final String portString;
		private final String table;
		private final ArrayList<ArrayList<Object>> rows;
		private final int device;

		public ConfirmationThread(final String hostname, final String portString, final String table, final int device, final ArrayList<ArrayList<Object>> rows)
		{
			this.hostname = hostname;
			this.portString = portString;
			this.table = table;
			this.rows = rows;
			this.device = device;
		}

		public boolean getOK()
		{
			return ok;
		}

		@Override
		public void run()
		{
			try
			{
				// Socket sock = new Socket(hostname,
				// Integer.parseInt(portString));
				final Socket sock = new Socket();
				sock.setReceiveBufferSize(4194304);
				sock.setSendBufferSize(4194304);
				sock.connect(new InetSocketAddress(hostname, Integer.parseInt(portString)));
				final OutputStream out = sock.getOutputStream();
				final byte[] outMsg = "HADOOPLD        ".getBytes(StandardCharsets.UTF_8);
				outMsg[8] = 0;
				outMsg[9] = 0;
				outMsg[10] = 0;
				outMsg[11] = 0;
				outMsg[12] = 0;
				outMsg[13] = 0;
				outMsg[14] = 0;
				outMsg[15] = 0;
				out.write(outMsg);
				final byte[] dev = intToBytes(device);
				out.write(dev);
				final byte[] data = table.getBytes(StandardCharsets.UTF_8);
				final byte[] length = intToBytes(data.length);
				out.write(length);
				out.write(data);
				out.write(rsToBytes(rows));
				out.flush();
				rows.clear();
				getConfirmation(sock);
				out.close();
				sock.close();
			}
			catch (final Exception e)
			{
				ok = false;
			}
		}
	}

	public static class LoadThread extends Thread
	{
		private final boolean ok = true;
		private final String hostname;
		private final String portString;
		private final String table;
		private final HashMap<Integer, ArrayList<ArrayList<Object>>> rows;

		public LoadThread(final String hostname, final String portString, final String table, final HashMap<Integer, ArrayList<ArrayList<Object>>> rows)
		{
			this.hostname = hostname;
			this.portString = portString;
			this.table = table;
			this.rows = rows;
		}

		public boolean getOK()
		{
			return ok;
		}

		@Override
		public void run()
		{
			final ArrayList<ConfirmationThread> threads = new ArrayList<ConfirmationThread>();
			for (final Map.Entry entry : rows.entrySet())
			{
				final ConfirmationThread thread = new ConfirmationThread(hostname, portString, table, (Integer)entry.getKey(), (ArrayList<ArrayList<Object>>)entry.getValue());
				thread.start();
				threads.add(thread);
			}

			for (final ConfirmationThread thread : threads)
			{
				while (true)
				{
					try
					{
						thread.join();
						break;
					}
					catch (final InterruptedException e)
					{
					}
				}

				if (!thread.getOK())
				{
				}
			}
		}
	}
}
