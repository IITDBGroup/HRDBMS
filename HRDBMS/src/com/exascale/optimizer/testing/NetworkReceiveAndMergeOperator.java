package com.exascale.optimizer.testing;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeMap;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.exascale.optimizer.testing.ResourceManager.DiskBackedHashMap;
import com.exascale.threads.ReadThread;

public final class NetworkReceiveAndMergeOperator extends NetworkReceiveOperator implements Operator, Serializable
{
	protected final ArrayList<String> sortCols;
	protected final ArrayList<Boolean> orders;
	protected int[] sortPos;
	
	public MetaData getMeta()
	{
		return meta;
	}
	
	public NetworkReceiveAndMergeOperator clone()
	{
		NetworkReceiveAndMergeOperator retval = new NetworkReceiveAndMergeOperator(sortCols, orders, meta);
		retval.node = node;
		return retval;
	}
	
	public NetworkReceiveAndMergeOperator(ArrayList<String> sortCols, ArrayList<Boolean> orders, MetaData meta)
	{
		super(meta);
		this.sortCols = sortCols;
		this.orders = orders;
	}
	
	protected final class ReadThread extends ThreadPoolThread
	{
		protected ArrayList<Operator> children;
		protected HashMap<Operator, ArrayList<Object>> rows = new HashMap<Operator, ArrayList<Object>>();
		protected Map.Entry minEntry;
		
		public ReadThread(ArrayList<Operator> children)
		{
			this.children = children;
		}
		
		public void run()
		{
			for (Operator op : children)
			{
				ArrayList<Object> row = readRow(op);
				if (row != null)
				{
					rows.put(op, row);
				}
			}
			
			while (rows.size() > 0)
			{
				minEntry = null;
				for (Map.Entry entry : rows.entrySet())
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
						catch(Exception e)
						{
							e.printStackTrace();
							System.exit(1);
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
					catch(Exception e)
					{
					}
				}
				ArrayList<Object> row = readRow((Operator)minEntry.getKey());
				if (row != null)
				{
					rows.put((Operator)minEntry.getKey(), row);
				}
				else
				{
					rows.remove(minEntry.getKey());
				}
			}
		}
		
		protected int compare(ArrayList<Object> lhs, ArrayList<Object> rhs) throws Exception
		{
			int result;
			int i = 0;
			
			for (int pos : sortPos)
			{
				Object lField = lhs.get(pos);
				Object rField = rhs.get(pos);
				
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
				else if (lField instanceof Date)
				{
					result = ((Date)lField).compareTo((Date)rField);
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
		
		private ArrayList<Object> readRow(Operator op)
		{
			try
			{
				InputStream in = ins.get(op);
				byte[] sizeBuff = new byte[4];
				while (true)
				{
					int count = 0;
					while (count < 4)
					{
						count += in.read(sizeBuff, count, 4 - count);
					}
					bytes.getAndAdd(4);
					int size = bytesToInt(sizeBuff);
			
					byte[] data = new byte[size];
					count = 0;
					while (count < size)
					{
						count += in.read(data, count, size - count);
					}
					bytes.getAndAdd(size);
					Object row = fromBytes(data);
					
					if (row instanceof DataEndMarker)
					{
						return null;
					}
					
					readCounter.getAndIncrement();
					return (ArrayList<Object>)row;
				}
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(1);
			}
			
			return null;
		}
		
		protected Object fromBytes(byte[] val)
		{	
			ByteBuffer bb = ByteBuffer.wrap(val);
			int numFields = bb.getInt();
			
			if (numFields < 0)
			{
				System.out.println("Negative number of fields in fromBytes()");
				System.out.println("NumFields = " + numFields);
				System.exit(1);
			}
			
			bb.position(bb.position() + numFields);
			byte[] bytes = bb.array();
			if (bytes[4] == 5)
			{
				return new DataEndMarker();
			}
			ArrayList<Object> retval = new ArrayList<Object>(numFields);
			int i = 0;
			while (i < numFields)
			{
				if (bytes[i+4] == 0)
				{
					//long
					Long o = bb.getLong();
					retval.add(o);
				}
				else if (bytes[i+4] == 1)
				{
					//integer
					Integer o = bb.getInt();
					retval.add(o);
				}
				else if (bytes[i+4] == 2)
				{
					//double
					Double o = bb.getDouble();
					retval.add(o);
				}
				else if (bytes[i+4] == 3)
				{
					//date
					Date o = new Date(bb.getLong());
					retval.add(o);
				}
				else if (bytes[i+4] == 4)
				{
					//string
					int length = bb.getInt();
					byte[] temp = new byte[length];
					bb.get(temp);
					try
					{
						String o = new String(temp, "UTF-8");
						retval.add(o);
					}
					catch(Exception e)
					{
						e.printStackTrace();
						System.exit(1);
					}
				}
				else
				{
					System.out.println("Unknown type " + bytes[i+4] + " in fromBytes()");
				}
				
				i++;
			}
			
			return retval;
		}
	}
	
	protected final class InitThread extends ThreadPoolThread
	{
		public void run()
		{
			start = System.currentTimeMillis();
			ReadThread readThread = new ReadThread(children);
			readThread.start();
			while (true)
			{
				try
				{
					readThread.join();
					break;
				}
				catch(InterruptedException e)
				{}
			}
			System.out.println("NetworkReceiveOperator: " + ((bytes.get() / ((System.currentTimeMillis() - start) / 1000.0)) / (1024.0 * 1024.0)) + "MB/sec");
			
			while (true)
			{
				try
				{
					outBuffer.put(new DataEndMarker());
					break;
				}
				catch(Exception e)
				{}
			}
		}
	}
	
	protected int bytesToInt(byte[] val)
	{
		int ret = java.nio.ByteBuffer.wrap(val).getInt();
		return ret;
	}
	
	protected static byte[] intToBytes(int val)
	{
		byte[] buff = new byte[4];
		buff[0] = (byte)(val >> 24);
		buff[1] = (byte)((val & 0x00FF0000) >> 16);
		buff[2] = (byte)((val & 0x0000FF00) >> 8);
		buff[3] = (byte)((val & 0x000000FF));
		return buff;
	}

	public Object next(Operator op2) throws Exception
	{
		if (!fullyStarted)
		{
			synchronized(this)
			{
				if (!fullyStarted)
				{
					fullyStarted = true;
					for (Operator op : children)
					{
						NetworkSendOperator child = (NetworkSendOperator)op;
						CompressedSocket sock = new CompressedSocket(meta.getHostNameForNode(child.getNode()), WORKER_PORT);
						socks.put(child, sock);
						OutputStream out = sock.getOutputStream();
						outs.put(child, out);
						InputStream in = sock.getInputStream();
						ins.put(child, new BufferedInputStream(in, 65536));
						byte[] command = "REMOTTRE".getBytes("UTF-8");
						byte[] from = intToBytes(-1);
						byte[] to = intToBytes(child.getNode());
						byte[] data = new byte[command.length + from.length + to.length];
						System.arraycopy(command, 0, data, 0, 8);
						System.arraycopy(from, 0, data, 8, 4);
						System.arraycopy(to, 0, data, 12, 4);
						out.write(data);
						out.flush();
						ObjectOutputStream objOut = new ObjectOutputStream(out);
						objOuts.put(child, objOut);
						//Field[] fields = child.getClass().getDeclaredFields();
						//for (Field field : fields)
						//{
						//	printHierarchy("child", field, child);
						//}
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
		return o;
	}
	
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
	
	public String toString()
	{
		return "NetworkReceiveAndMergeOperator(" + node + ")";
	}
	
	public void add(Operator op)
	{
		try
		{
			super.add(op);
		}
		catch(Exception e)
		{
			e.printStackTrace();
			System.exit(1);
		}
		if (sortPos == null)
		{
			int i = 0;
			sortPos = new int[sortCols.size()];
			for (String sortCol : sortCols)
			{
				sortPos[i]= cols2Pos.get(sortCol);
				i++;
			}
		}
	}
}
