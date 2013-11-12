package com.exascale.optimizer.testing;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import com.exascale.optimizer.testing.ResourceManager.DiskBackedHashMap;
import com.exascale.threads.ReadThread;

public class NetworkSendOperator implements Operator, Serializable
{
	private MetaData meta;
	private Operator child;
	private Operator parent;
	private HashMap<String, String> cols2Types;
	private HashMap<String, Integer> cols2Pos;
	private TreeMap<Integer, String> pos2Col;
	private static final int WORKER_PORT = 3232;
	private Socket sock;
	private OutputStream out;
	private int node;
	
	public MetaData getMeta()
	{
		return meta;
	}
	
	public NetworkSendOperator(int node, MetaData meta)
	{
		this.meta = meta;
		this.node = node;
	}
	
	public ArrayList<String> getReferences()
	{
		return null;
	}
	
	public ArrayList<Operator> children()
	{
		ArrayList<Operator> retval = new ArrayList<Operator>();
		retval.add(child);
		return retval;
	}
	
	public Operator parent()
	{
		return parent;
	}
	
	public void add(Operator op) throws Exception
	{
		if (child == null)
		{
			child = op;
			op.registerParent(this);
			cols2Types = op.getCols2Types();
			cols2Pos = op.getCols2Pos();
			pos2Col = op.getPos2Col();
		}
		else
		{
			throw new Exception("NetworkSendOperator only supports 1 child");
		}
	}
	
	public void removeChild(Operator op)
	{
		child = null;
		op.removeParent(this);
	}
	
	public void removeParent(Operator op)
	{
		parent = null;
	}
	
	public void clearParent()
	{
		parent = null;
	}
	
	public int getNode()
	{
		return node;
	}
	
	public void setSocket(Socket sock)
	{
		try
		{
			this.sock = sock;
			out = sock.getOutputStream();
		}
		catch(Exception e)
		{
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	@Override
	public synchronized void start() throws Exception 
	{
		child.start();
		Object o = child.next(this);
		while (!(o instanceof DataEndMarker))
		{
			byte[] obj = toBytes(o);
			byte[] size = intToBytes(obj.length);
			byte[] data = new byte[obj.length + size.length];
			System.arraycopy(size, 0, data, 0, 4);
			System.arraycopy(obj, 0, data, 4, obj.length);
			out.write(data);
		}
		
		byte[] obj = toBytes(o);
		byte[] size = intToBytes(obj.length);
		byte[] data = new byte[obj.length + size.length];
		System.arraycopy(size, 0, data, 0, 4);
		System.arraycopy(obj, 0, data, 4, obj.length);
		out.write(data);
	}
	
	private byte[] toBytes(Object v)
	{
		ArrayList<Object> val;
		if (v instanceof ArrayList)
		{
			val = (ArrayList<Object>)v;
		}
		else
		{
			byte[] retval = new byte[9];
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
		byte[] header = new byte[size];
		int i = 8;
		for (Object o : val)
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
			else if (o instanceof Date)
			{
				header[i] = (byte)3;
				size += 8;
			}
			else if (o instanceof String)
			{
				header[i] = (byte)4;
				size += (4 + ((String)o).length());
			}
			else
			{
				System.out.println("Unknown type " + o.getClass() + " in toyBytes()");
				System.out.println(o);
				System.exit(1);
			}
			
			i++;
		}
		
		byte[] retval = new byte[size];
	//	System.out.println("In toBytes(), row has " + val.size() + " columns, object occupies " + size + " bytes");
		System.arraycopy(header, 0, retval, 0, header.length);
		i = 8;
		ByteBuffer retvalBB = ByteBuffer.wrap(retval);
		retvalBB.putInt(size-4);
		retvalBB.putInt(val.size());
		retvalBB.position(header.length);
		for (Object o : val)
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
				retvalBB.putLong(((Date)o).getTime());	
			}
			else if (retval[i] == 4)
			{
				byte[] temp = null;
				try
				{
					temp = ((String)o).getBytes("UTF-8");
				}
				catch(Exception e)
				{
					e.printStackTrace();
					System.exit(1);
				}
				retvalBB.putInt(temp.length);
				retvalBB.put(temp);
			}
			
			i++;
		}
		
		return retval;
	}
	
	private int bytesToInt(byte[] val)
	{
		int ret = java.nio.ByteBuffer.wrap(val).getInt();
		return ret;
	}
	
	private static byte[] intToBytes(int val)
	{
		byte[] buff = new byte[4];
		buff[0] = (byte)(val >> 24);
		buff[1] = (byte)((val & 0x00FF0000) >> 16);
		buff[2] = (byte)((val & 0x0000FF00) >> 8);
		buff[3] = (byte)((val & 0x000000FF));
		return buff;
	}

	public Object next(Operator op) throws Exception
	{
		throw new Exception("Unsupported operation");
	}

	@Override
	public void close() throws Exception 
	{
		out.close();
		sock.close();
	}

	public void registerParent(Operator op) throws Exception
	{
		if (parent == null)
		{
			parent = op;
		}
		else
		{
			throw new Exception("NetworkSendOperator only supports 1 parent.");
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
		return "NetworkSendOperator";
	}
}
