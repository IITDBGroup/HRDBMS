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
import java.util.TreeMap;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.exascale.optimizer.testing.ResourceManager.DiskBackedHashMap;
import com.exascale.threads.ReadThread;

public class NetworkReceiveOperator implements Operator, Serializable
{
	protected MetaData meta;
	protected ArrayList<Operator> children = new ArrayList<Operator>();
	protected Operator parent;
	protected HashMap<String, String> cols2Types;
	protected HashMap<String, Integer> cols2Pos;
	protected TreeMap<Integer, String> pos2Col;
	protected static final int WORKER_PORT = 3232;
	protected HashMap<Operator, CompressedSocket> socks = new HashMap<Operator, CompressedSocket>();
	protected HashMap<Operator, OutputStream> outs = new HashMap<Operator, OutputStream>();
	protected HashMap<Operator, InputStream> ins = new HashMap<Operator, InputStream>();
	protected HashMap<Operator, ObjectOutputStream> objOuts = new HashMap<Operator, ObjectOutputStream>();
	protected BufferedLinkedBlockingQueue outBuffer = new BufferedLinkedBlockingQueue(Driver.QUEUE_SIZE);
	protected ArrayList<ReadThread> threads = new ArrayList<ReadThread>();
	protected volatile boolean fullyStarted = false;
	protected AtomicLong readCounter = new AtomicLong(0);
	protected AtomicLong bytes = new AtomicLong(0);
	protected long start;
	protected int node;
	
	public void reset()
	{
		System.out.println("NetworkReceiveOperator does not support reset()");
		System.exit(1);
	}
	
	public void setChildPos(int pos)
	{
	}
	
	public int getChildPos()
	{
		return 0;
	}
	
	public MetaData getMeta()
	{
		return meta;
	}
	
	public NetworkReceiveOperator clone()
	{
		NetworkReceiveOperator retval = new NetworkReceiveOperator(meta);
		retval.node = node;
		return retval;
	}
	
	public int getNode()
	{
		return node;
	}
	
	public void setNode(int node)
	{
		this.node = node;
	}
	
	public NetworkReceiveOperator(MetaData meta)
	{
		this.meta = meta;
	}
	
	protected NetworkReceiveOperator()
	{}
	
	public ArrayList<String> getReferences()
	{
		return null;
	}
	
	public ArrayList<Operator> children()
	{
		return children;
	}
	
	public Operator parent()
	{
		return parent;
	}
	
	public void add(Operator op) throws Exception
	{
		children.add(op);
		op.registerParent(this);
		cols2Types = op.getCols2Types();
		cols2Pos = op.getCols2Pos();
		pos2Col = op.getPos2Col();
	}
	
	public void removeChild(Operator op)
	{
		children.remove(op);
		op.removeParent(this);
	}
	
	public void removeParent(Operator op)
	{
		parent = null;
	}
	
	@Override
	public void start() throws Exception 
	{
	}
	
	protected class ReadThread extends ThreadPoolThread
	{
		protected Operator op;
		
		public ReadThread(Operator op)
		{
			this.op = op;
		}
		
		public void run()
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
						try
						{
							int temp = in.read(sizeBuff, count, 4 - count);
							if (temp == -1)
							{
								System.out.println("Early EOF reading from socket connected to " + socks.get(op).getRemoteSocketAddress());
								System.exit(1);
							}
							else
							{
								count += temp;
							}
						}
						catch(Exception e)
						{
							e.printStackTrace();
							System.out.println("Early EOF reading from socket connected to " + socks.get(op).getRemoteSocketAddress());
							System.exit(1);
						}
					}
					bytes.getAndAdd(4);
					int size = bytesToInt(sizeBuff);
					
					byte[] data = new byte[size];
					count = 0;
					while (count < size)
					{
						try
						{
							count += in.read(data, count, size - count);
						}
						catch(Exception e)
						{
							e.printStackTrace();
							System.out.println("Early EOF reading from socket connected to " + socks.get(op).getRemoteSocketAddress());
							System.exit(1);
						}
					}
					bytes.getAndAdd(size);
					Object row = fromBytes(data);
					
					if (row instanceof DataEndMarker)
					{
						return;
					}
					
					outBuffer.put(row);
					readCounter.getAndIncrement();
				}
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(1);
			}
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
					Long o = ResourceManager.internLong(bb.getLong());
					retval.add(o);
				}
				else if (bytes[i+4] == 1)
				{
					//integer
					Integer o = ResourceManager.internInt(bb.getInt());
					retval.add(o);
				}
				else if (bytes[i+4] == 2)
				{
					//double
					Double o = ResourceManager.internDouble(bb.getDouble());
					retval.add(o);
				}
				else if (bytes[i+4] == 3)
				{
					//date
					Date o = ResourceManager.internDate(new Date(bb.getLong()));
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
						String o = ResourceManager.internString(new String(temp, "UTF-8"));
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
	
	protected class InitThread extends ThreadPoolThread
	{
		public void run()
		{
			start = System.currentTimeMillis();
			for (Operator op : children)
			{
				ReadThread readThread = new ReadThread(op);
				threads.add(readThread);
				readThread.start();
			}
			
			for (ReadThread thread : threads)
			{
				while (true)
				{
					try
					{
						thread.join();
						System.out.println("NetworkReceiveOperator: " + ((bytes.get() / ((System.currentTimeMillis() - start) / 1000.0)) / (1024.0 * 1024.0)) + "MB/sec");
						break;
					}
					catch(InterruptedException e)
					{}
				}
			}
			
			//System.out.println("NetworkReceiveOperator received " + readCounter + " rows");
			
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
	
	public void nextAll(Operator op) throws Exception
	{
		Object o = next(op);
		while (!(o instanceof DataEndMarker))
		{
			o = next(op);
		}
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

	@Override
	public void close() throws Exception 
	{
		System.out.println("NetworkReceiveOperator has been closed.");
		for (ObjectOutputStream objOut : objOuts.values())
		{
			objOut.close();
		}
		
		for (OutputStream out : outs.values())
		{
			out.close();
		}
		
		for (InputStream in : ins.values())
		{
			in.close();
		}
		
		for (CompressedSocket sock : socks.values())
		{
			sock.close();
		}
	}

	public void registerParent(Operator op) throws Exception
	{
		if (parent == null)
		{
			parent = op;
		}
		else
		{
			throw new Exception("NetworkReceiveOperator only supports 1 parent.");
		}
	}

	@Override
	public HashMap<String, String> getCols2Types() {
		return cols2Types;
	}

	@Override
	public HashMap<String, Integer> getCols2Pos() {
		return cols2Pos;
	}

	@Override
	public TreeMap<Integer, String> getPos2Col() {
		return pos2Col;
	}
	
	public String toString()
	{
		return "NetworkReceiveOperator(" + node + ")";
	}
	
	protected HashSet<ObjectAndField> printed = new HashSet<ObjectAndField>();
	private class ObjectAndField
	{
		private Object obj;
		private Field field;
		
		public ObjectAndField(Object obj, Field field)
		{
			this.obj = obj;
			this.field = field;
		}
		
		public boolean equals(Object rhs)
		{
			if (rhs != null && rhs instanceof ObjectAndField)
			{
				ObjectAndField r = (ObjectAndField)rhs;
				return obj.equals(r.obj) && field.equals(r.field);
			}
			
			return false;
		}
		
		public int hashCode()
		{
			return obj.hashCode() + field.hashCode();
		}
	}
	protected void printHierarchy(String path, Field field, Object obj)
	{
		try
		{
			ObjectAndField objAndField = new ObjectAndField(obj, field);
			if (printed.contains(objAndField))
			{
				return;
			}
			
			printed.add(objAndField);
			System.out.println(path + "->" + field.getName() + "-" + field.getType());
			if (Collection.class.isInstance(field.get(obj)))
			{
				Collection coll = (Collection)field.get(obj);
				for (Object o : coll)
				{
					Field[] fields = o.getClass().getDeclaredFields();
					for (Field f : fields)
					{
						printHierarchy(path + "->" + field.getName() + "->val", f, o);
					}
				}
			}
			else if (AbstractMap.class.isInstance(field.get(obj)))
			{
				AbstractMap coll = (AbstractMap)field.get(obj);
				for (Object o : coll.keySet())
				{
					Field[] fields = o.getClass().getDeclaredFields();
					for (Field f : fields)
					{
						printHierarchy(path + "->" + field.getName() + "->keyVal", f, o);
					}
				}
				for (Object o : coll.values())
				{
					Field[] fields = o.getClass().getDeclaredFields();
					for (Field f : fields)
					{
						printHierarchy(path + "->" + field.getName() + "->val", f, o);
					}
				}
			}
			if (!field.getType().isPrimitive())
			{
				Field[] fields = field.get(obj).getClass().getDeclaredFields();
				for (Field f : fields)
				{
					printHierarchy(path + "->" + field.getName(), f, field.get(obj));
				}
			}
		}
		catch(Exception e)
		{
		}
	}
}
