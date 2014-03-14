package com.exascale.optimizer.testing;

 

import java.io.BufferedOutputStream;
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
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPOutputStream;

import com.exascale.optimizer.testing.ResourceManager.DiskBackedHashMap;
import com.exascale.threads.ReadThread;

public class NetworkSendOperator implements Operator, Serializable
{
	protected MetaData meta;
	protected Operator child;
	protected Operator parent;
	protected HashMap<String, String> cols2Types;
	protected HashMap<String, Integer> cols2Pos;
	protected TreeMap<Integer, String> pos2Col;
	protected static final int WORKER_PORT = 3232;
	protected CompressedSocket sock;
	protected OutputStream out;
	protected int node;
	protected long count = 0;
	protected int numParents;
	protected boolean started = false;
	protected boolean numpSet = false;
	protected boolean cardSet = false;
	
	public void reset()
	{
		System.out.println("NetworkSendOperator does not support reset()");
		System.exit(1);
	}
	
	public boolean setCard()
	{
		if (cardSet)
		{
			return false;
		}
		
		cardSet = true;
		return true;
	}
	
	public boolean notStarted()
	{
		return !started;
	}
	
	public void setChildPos(int pos)
	{
	}
	
	public boolean hasAllConnections()
	{
		return false;
	}
	
	public int getChildPos()
	{
		return 0;
	}
	
	public MetaData getMeta()
	{
		return meta;
	}
	
	public boolean setNumParents()
	{
		if (numpSet)
		{
			return false;
		}
		
		numpSet = true;
		return true;
	}
	
	protected NetworkSendOperator()
	{}
	
	public NetworkSendOperator(int node, MetaData meta)
	{
		this.meta = meta;
		this.node = node;
	}
	
	public NetworkSendOperator clone()
	{
		NetworkSendOperator retval = new NetworkSendOperator(node, meta);
		retval.numParents= numParents;
		retval.numpSet = numpSet;
		retval.cardSet = cardSet;
		return retval;
	}
	
	public void addConnection(int fromNode, CompressedSocket sock)
	{}
	
	public void setNode(int node)
	{
		this.node = node;
	}
	
	public ArrayList<String> getReferences()
	{
		return null;
	}
	
	public ArrayList<Operator> children()
	{
		ArrayList<Operator> retval = new ArrayList<Operator>(1);
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
	
	public void setSocket(CompressedSocket sock)
	{
		try
		{
			this.sock = sock;
			out = new BufferedOutputStream(sock.getOutputStream());
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
		started = true;
		child.start();
		Object o = child.next(this);
		while (!(o instanceof DataEndMarker))
		{
			byte[] obj = toBytes(o);
			out.write(obj);
			count++;
			o = child.next(this);
		}
		
		byte[] obj = toBytes(o);
		out.write(obj);
		out.flush();
		System.out.println("Wrote " + count + " rows");
		child.close();
		Thread.sleep(60 * 1000);
	}
	
	protected byte[] toBytes(Object v)
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
			else if (o instanceof MyDate)
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
				retvalBB.putLong(((MyDate)o).getTime());	
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
	
	public void nextAll(Operator op)
	{}

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
		return "NetworkSendOperator(" + node + ")";
	}
}
