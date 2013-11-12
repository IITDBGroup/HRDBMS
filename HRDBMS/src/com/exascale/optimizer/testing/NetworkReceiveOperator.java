package com.exascale.optimizer.testing;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.InputStream;
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

public class NetworkReceiveOperator implements Operator, Serializable
{
	private MetaData meta;
	private ArrayList<Operator> children = new ArrayList<Operator>();
	private Operator parent;
	private HashMap<String, String> cols2Types;
	private HashMap<String, Integer> cols2Pos;
	private TreeMap<Integer, String> pos2Col;
	private static final int WORKER_PORT = 3232;
	private HashMap<Operator, Socket> socks = new HashMap<Operator, Socket>();
	private HashMap<Operator, OutputStream> outs = new HashMap<Operator, OutputStream>();
	private HashMap<Operator, InputStream> ins = new HashMap<Operator, InputStream>();
	private HashMap<Operator, ObjectOutputStream> objOuts = new HashMap<Operator, ObjectOutputStream>();
	private LinkedBlockingQueue outBuffer = new LinkedBlockingQueue(Driver.QUEUE_SIZE);
	private ArrayList<ReadThread> threads = new ArrayList<ReadThread>();
	
	public MetaData getMeta()
	{
		return meta;
	}
	
	public NetworkReceiveOperator(MetaData meta)
	{
		this.meta = meta;
	}
	
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
	public synchronized void start() throws Exception 
	{
		for (Operator op : children)
		{
			NetworkSendOperator child = (NetworkSendOperator)op;
			child.clearParent();
			Socket sock = new Socket(meta.getHostNameForNode(child.getNode()), WORKER_PORT);
			socks.put(child, sock);
			OutputStream out = sock.getOutputStream();
			outs.put(child, out);
			InputStream in = sock.getInputStream();
			ins.put(child, in);
			byte[] command = "REMOTTRE".getBytes("UTF-8");
			byte[] from = intToBytes(-1);
			byte[] to = intToBytes(child.getNode());
			byte[] data = new byte[command.length + from.length + to.length];
			System.arraycopy(command, 0, data, 0, 8);
			System.arraycopy(from, 0, data, 8, 4);
			System.arraycopy(to, 0, data, 12, 4);
			out.write(data);
			ObjectOutputStream objOut = new ObjectOutputStream(out);
			objOuts.put(child, objOut);
			objOut.writeObject(child);
		}
		
		new InitThread().start();
	}
	
	private class ReadThread extends Thread
	{
		private Operator op;
		
		public ReadThread(Operator op)
		{
			this.op = op;
		}
		
		public void run()
		{
			try
			{
				InputStream in = ins.get(op);
				while (true)
				{
					if (in.available() >= 4)
					{
						byte[] sizeBuff = new byte[4];
						in.read(sizeBuff);
						int size = bytesToInt(sizeBuff);
				
						while (in.available() < size)
						{
							Thread.sleep(1);
						}
					
						byte[] data = new byte[size];
						in.read(data);
						Object row = fromBytes(data);
					
						if (row instanceof DataEndMarker)
						{
							return;
						}
					
						outBuffer.put(row);
					}
					else
					{
						Thread.sleep(1);
					}
				}
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(1);
			}
		}
		
		private Object fromBytes(byte[] val)
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
			ArrayList<Object> retval = new ArrayList<Object>();
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
	
	private class InitThread extends Thread
	{
		public void run()
		{
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
						break;
					}
					catch(InterruptedException e)
					{}
				}
			}
			
			while (true)
			{
				try
				{
					outBuffer.put(new DataEndMarker());
					break;
				}
				catch(InterruptedException e)
				{}
			}
		}
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
		
		for (Socket sock : socks.values())
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
		return "NetworkReceiveOperator";
	}
}
