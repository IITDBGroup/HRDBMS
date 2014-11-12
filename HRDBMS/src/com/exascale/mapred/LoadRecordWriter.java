package com.exascale.mapred;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import com.exascale.compression.CompressedSocket;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.misc.AtomicDouble;
import com.exascale.misc.InternalConcurrentHashMap;
import com.exascale.misc.MyDate;
import com.exascale.optimizer.MetaData;
import com.exascale.optimizer.MetaData.PartitionMetaData;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class LoadRecordWriter extends RecordWriter
{
	private static HashMap<String, LoadRecordWriter> writers = new HashMap<String, LoadRecordWriter>();
	private String table;
	private int lastNode = -1;
	public HashMap<Integer, ArrayList<ArrayList<Object>>> rows = new HashMap<Integer, ArrayList<ArrayList<Object>>>();
	public HashMap<Integer, ArrayList<ArrayList<Object>>> processing;
	private String portString;
	private String hrdbmsHome;
	public LoadThread thread = null;
	private int size = 0;
	
	public LoadRecordWriter(String table, String portString, String hrdbmsHome)
	{
		this.table = table;
		this.portString = portString;
		this.hrdbmsHome = hrdbmsHome;
	}
	
	@Override
	public synchronized void close(TaskAttemptContext arg0) throws IOException, InterruptedException
	{
		if (rows.size() > 0)
		{
			flush();
		}
	}

	@Override
	public synchronized void write(Object arg0, Object arg1) throws IOException, InterruptedException
	{
		MyLongWritable key = (MyLongWritable)arg0;
		int node = (int)(key.get() >> 32);
		int device = (int)(key.get() & 0x00000000FFFFFFFF);
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
		ALOWritable alo = (ALOWritable)arg1;
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
		if (size > 100000)
		{
			flush();
		}
	}

	public static LoadRecordWriter get(String table, String portString, String hrdbmsHome)
	{
		synchronized(writers)
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
	
	public synchronized void flush() throws IOException
	{
		String hostname;
		processing = rows;
		rows = new HashMap<Integer, ArrayList<ArrayList<Object>>>();
		size = 0;
		try
		{
			hostname = new MetaData(hrdbmsHome).getHostNameForNode(lastNode);
			//LoadReducer.out.println("Node# " + lastNode + " is resolving to " + hostname);
			//LoadReducer.out.flush();
		}
		catch(Exception e)
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
				catch(InterruptedException e)
				{}
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
	
	public static class LoadThread extends Thread
	{
		private boolean ok = true;
		private String hostname;
		private String portString;
		private String table;
		private HashMap<Integer, ArrayList<ArrayList<Object>>> rows;
		
		public LoadThread(String hostname, String portString, String table, HashMap<Integer, ArrayList<ArrayList<Object>>> rows)
		{
			this.hostname = hostname;
			this.portString = portString;
			this.table = table;
			this.rows = rows;
		}
		
		public void run()
		{
			ArrayList<ConfirmationThread> threads = new ArrayList<ConfirmationThread>();
			for (Map.Entry entry : rows.entrySet())
			{
				ConfirmationThread thread = new ConfirmationThread(hostname, portString, table, (Integer)entry.getKey(), (ArrayList<ArrayList<Object>>)entry.getValue());
				thread.start();
				threads.add(thread);
			}
			
			boolean ok = true;
			for (ConfirmationThread thread : threads)
			{
				while (true)
				{
					try
					{
						thread.join();
						break;
					}
					catch(InterruptedException e)
					{}
				}
				
				if (!thread.getOK())
				{
					ok = false;
				}
			}
		}
		
		public boolean getOK()
		{
			return ok;
		}
	}
	
	public static class ConfirmationThread extends Thread
	{
		private boolean ok = true;
		private String hostname;
		private String portString;
		private String table;
		private ArrayList<ArrayList<Object>> rows;
		private int device;
		
		public ConfirmationThread(String hostname, String portString, String table, int device, ArrayList<ArrayList<Object>> rows)
		{
			this.hostname = hostname;
			this.portString = portString;
			this.table = table;
			this.rows = rows;
			this.device = device;
		}
		
		public void run()
		{
			try
			{
				CompressedSocket sock = new CompressedSocket(hostname, Integer.parseInt(portString));
				OutputStream out = sock.getOutputStream();
				byte[] outMsg = "HADOOPLD        ".getBytes("UTF-8");
				outMsg[8] = 0;
				outMsg[9] = 0;
				outMsg[10] = 0;
				outMsg[11] = 0;
				outMsg[12] = 0;
				outMsg[13] = 0;
				outMsg[14] = 0;
				outMsg[15] = 0;
				out.write(outMsg);
				byte[] dev = intToBytes(device);
				out.write(dev);
				byte[] data = table.getBytes("UTF-8");
				byte[] length = intToBytes(data.length);
				out.write(length);
				out.write(data);
				out.write(rsToBytes(rows));
				out.flush();
				rows.clear();
				getConfirmation(sock);
				out.close();
				sock.close();
			}
			catch(Exception e)
			{
				ok = false;
			}
		}
		
		public boolean getOK()
		{
			return ok;
		}
	}
	
	private static void getConfirmation(Socket sock) throws Exception
	{
		InputStream in = sock.getInputStream();
		byte[] inMsg = new byte[2];
		
		int count = 0;
		while (count < 2)
		{
			try
			{
				int temp = in.read(inMsg, count, 2 - count);
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
		
		String inStr = new String(inMsg, "UTF-8");
		if (!inStr.equals("OK"))
		{
			in.close();
			throw new Exception();
		}
		
		try
		{
			in.close();
		}
		catch(Exception e)
		{}
	}
	
	private static byte[] intToBytes(int val)
	{
		final byte[] buff = new byte[4];
		buff[0] = (byte)(val >> 24);
		buff[1] = (byte)((val & 0x00FF0000) >> 16);
		buff[2] = (byte)((val & 0x0000FF00) >> 8);
		buff[3] = (byte)((val & 0x000000FF));
		return buff;
	}
	
	private static final byte[] rsToBytes(ArrayList<ArrayList<Object>> rows) throws Exception
	{
		final ArrayList<byte[]> results = new ArrayList<byte[]>(rows.size());
		for (ArrayList<Object> val : rows)
		{
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
				else if (o instanceof AtomicLong)
				{
					header[i] = (byte)6;
					size += 8;
				}
				else if (o instanceof AtomicDouble)
				{
					header[i] = (byte)7;
					size += 8;
				}
				else if (o instanceof ArrayList)
				{
					if (((ArrayList)o).size() != 0)
					{
						Exception e = new Exception("Non-zero size ArrayList in toBytes()");
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
						HRDBMSWorker.logger.error("", e);
						throw e;
					}
					retvalBB.putInt(temp.length);
					retvalBB.put(temp);
				}
				else if (retval[i] == 6)
				{
					retvalBB.putLong(((AtomicLong)o).get());
				}
				else if (retval[i] == 7)
				{
					retvalBB.putDouble(((AtomicDouble)o).get());
				}
				else if (retval[i] == 8)
				{
				}

				i++;
			}

			results.add(retval);
		}

		int count = 0;
		for (final byte[] ba : results)
		{
			count += ba.length;
		}
		final byte[] retval = new byte[count + 4];
		ByteBuffer temp = ByteBuffer.allocate(4);
		temp.asIntBuffer().put(results.size());
		System.arraycopy(temp.array(), 0, retval, 0, 4);
		int retvalPos = 4;
		for (final byte[] ba : results)
		{
			System.arraycopy(ba, 0, retval, retvalPos, ba.length);
			retvalPos += ba.length;
		}

		return retval;
	}
}
