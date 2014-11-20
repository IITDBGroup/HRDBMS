package com.exascale.optimizer;

import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import com.exascale.compression.CompressedSocket;
import com.exascale.logging.LogRec;
import com.exascale.logging.PrepareLogRec;
import com.exascale.logging.XAAbortLogRec;
import com.exascale.logging.XACommitLogRec;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.managers.LogManager;
import com.exascale.misc.DataEndMarker;
import com.exascale.tables.Plan;
import com.exascale.tables.Transaction;
import com.exascale.threads.HRDBMSThread;

public final class MassDeleteOperator implements Operator, Serializable
{
	private Operator child;
	private final MetaData meta;
	private HashMap<String, String> cols2Types;
	private HashMap<String, Integer> cols2Pos;
	private TreeMap<Integer, String> pos2Col;
	private Operator parent;
	private int node;
	private transient Plan plan;
	private String schema;
	private String table;
	private AtomicInteger num = new AtomicInteger(0);
	private boolean done = false;
	private Transaction tx;
	private boolean logged = true;
	
	public void setPlan(Plan plan)
	{
		this.plan = plan;
	}
	
	public void setTransaction(Transaction tx)
	{
		this.tx = tx;
	}

	public MassDeleteOperator(String schema, String table, MetaData meta)
	{
		this.meta = meta;
		this.schema = schema;
		this.table = table;
	}
	
	public MassDeleteOperator(String schema, String table, MetaData meta, boolean logged)
	{
		this.meta = meta;
		this.schema = schema;
		this.table = table;
		this.logged = logged;
	}

	@Override
	public void add(Operator op) throws Exception
	{
		throw new Exception("MassDeleteOperator does not support children");
	}

	@Override
	public ArrayList<Operator> children()
	{
		final ArrayList<Operator> retval = new ArrayList<Operator>(1);
		return retval;
	}

	@Override
	public MassDeleteOperator clone()
	{
		final MassDeleteOperator retval = new MassDeleteOperator(schema, table, meta);
		retval.node = node;
		return retval;
	}

	@Override
	public void close() throws Exception
	{
	}

	@Override
	public int getChildPos()
	{
		return 0;
	}

	@Override
	public HashMap<String, Integer> getCols2Pos()
	{
		return cols2Pos;
	}

	@Override
	public HashMap<String, String> getCols2Types()
	{
		return cols2Types;
	}

	@Override
	public MetaData getMeta()
	{
		return meta;
	}

	@Override
	public int getNode()
	{
		return node;
	}

	@Override
	public TreeMap<Integer, String> getPos2Col()
	{
		return pos2Col;
	}

	@Override
	public ArrayList<String> getReferences()
	{
		final ArrayList<String> retval = new ArrayList<String>();
		return retval;
	}

	@Override
	// @?Parallel
	public Object next(Operator op) throws Exception
	{
		while (!done)
		{
			Thread.sleep(1);
		}
		
		if (num.get() >= 0)
		{
			int retval = num.get();
			num.set(-1);
			return retval;
		}
		else if (num.get() == Integer.MIN_VALUE)
		{
			throw new Exception("An error occurred during a mass delete operation");
		}
		else
		{
			return new DataEndMarker();
		}
	}

	@Override
	public void nextAll(Operator op) throws Exception
	{
		child.nextAll(op);
		num.set(Integer.MIN_VALUE+1);
	}

	@Override
	public Operator parent()
	{
		return parent;
	}

	@Override
	public void registerParent(Operator op) throws Exception
	{
		throw new Exception("MassDeleteOperator does not support parents.");
	}

	@Override
	public void removeChild(Operator op)
	{
	}

	@Override
	public void removeParent(Operator op)
	{
	}

	@Override
	public void reset() throws Exception
	{
		throw new Exception("MassDeleteOperator is not resetable");
	}

	@Override
	public void setChildPos(int pos)
	{
	}

	@Override
	public void setNode(int node)
	{
		this.node = node;
	}
	
	public String getSchema()
	{
		return schema;
	}
	
	public String getTable()
	{
		return table;
	}

	@Override
	public void start() throws Exception
	{
		//get all nodes that contain data for this table
		ArrayList<Integer> nodes = MetaData.getNodesForTable(schema, table, tx);
		ArrayList<Object> tree = makeTree(nodes);
		//send all of them a mass delete message for this table with this transaction
		ArrayList<String> indexes = MetaData.getIndexFileNamesForTable(schema, table, tx);
		ArrayList<ArrayList<String>> keys = MetaData.getKeys(indexes, tx);
		ArrayList<ArrayList<String>> types = MetaData.getTypes(indexes, tx);
		ArrayList<ArrayList<Boolean>> orders = MetaData.getOrders(indexes, tx);
		boolean ok = sendMassDeletes(tree, tx, MetaData.getCols2TypesForTable(schema, table, tx), MetaData.getPos2ColForTable(schema, table, tx), keys, types, orders, indexes, logged);
		//if anyone responds not ok tell next() to throw an exception
		if (!ok)
		{
			num.set(Integer.MIN_VALUE);
			done = true;
			return;
		}
		//set done
		done = true;
	}
	
	private static ArrayList<Object> makeTree(ArrayList<Integer> nodes)
	{
		int max = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("max_neighbor_nodes"));
		if (nodes.size() <= max)
		{
			ArrayList<Object> retval = new ArrayList<Object>(nodes);
			return retval;
		}
		
		ArrayList<Object> retval = new ArrayList<Object>();
		int i = 0;
		while (i < max)
		{
			retval.add(nodes.get(i));
			i++;
		}
		
		int remaining = nodes.size() - i;
		int perNode = remaining / max + 1;
		
		int j = 0;
		while (i < nodes.size())
		{
			int first = (Integer)retval.get(j);
			retval.remove(j);
			ArrayList<Integer> list = new ArrayList<Integer>(perNode+1);
			list.add(first);
			int k = 0;
			while (k < perNode && i < nodes.size())
			{
				list.add(nodes.get(i));
				i++;
				k++;
			}
			
			retval.add(j, list);
			j++;
		}
		
		if (((ArrayList<Integer>)retval.get(0)).size() <= max)
		{
			return retval;
		}
		
		//more than 2 tier
		i = 0;
		while (i < retval.size())
		{
			ArrayList<Integer> list = (ArrayList<Integer>)retval.remove(i);
			retval.add(i, makeTree(list));
			i++;
		}
		
		return retval;
	}
	
	private boolean sendMassDeletes(ArrayList<Object> tree, Transaction tx, HashMap<String, String> cols2Types, TreeMap<Integer, String> pos2Col, ArrayList<ArrayList<String>> keys, ArrayList<ArrayList<String>> types, ArrayList<ArrayList<Boolean>> orders, ArrayList<String> indexes, boolean logged)
	{
		//all of them should respond OK with delete count
		boolean allOK = true;
		ArrayList<SendMassDeleteThread> threads = new ArrayList<SendMassDeleteThread>();
		for (Object o : tree)
		{
			if (o instanceof Integer)
			{
				ArrayList<Object> list = new ArrayList<Object>(1);
				list.add(o);
				SendMassDeleteThread thread = new SendMassDeleteThread(list, tx, cols2Types, pos2Col, keys, types, orders, indexes, logged);
				threads.add(thread);
			}
			else
			{
				SendMassDeleteThread thread = new SendMassDeleteThread((ArrayList<Object>)o, tx, cols2Types, pos2Col, keys, types, orders, indexes, logged);
				threads.add(thread);
			}
		}
		
		for (SendMassDeleteThread thread : threads)
		{
			thread.start();
		}
		
		for (SendMassDeleteThread thread : threads)
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
			boolean ok = thread.getOK();
			if (!ok)
			{
				allOK = false;
			}
			else
			{
				num.getAndAdd(thread.getNum());
			}
		}
		
		if (allOK)
		{
			return true;
		}
		else
		{
			return false;
		}
	}
	
	private class SendMassDeleteThread extends HRDBMSThread
	{
		private ArrayList<Object> tree;
		private Transaction tx;
		private boolean ok;
		int num;
		private HashMap<String, String> cols2Types;
		private TreeMap<Integer, String> pos2Col;
		private ArrayList<ArrayList<String>> keys;
		private ArrayList<ArrayList<String>> types;
		private ArrayList<ArrayList<Boolean>> orders;
		private ArrayList<String> indexes;
		private boolean logged;
		
		public SendMassDeleteThread(ArrayList<Object> tree, Transaction tx, HashMap<String, String> cols2Types, TreeMap<Integer, String> pos2Col, ArrayList<ArrayList<String>> keys, ArrayList<ArrayList<String>> types, ArrayList<ArrayList<Boolean>> orders, ArrayList<String> indexes, boolean logged)
		{
			this.tree = tree;
			this.tx = tx;
			this.cols2Types = cols2Types;
			this.pos2Col = pos2Col;
			this.keys = keys;
			this.types = types;
			this.orders = orders;
			this.indexes = indexes;
			this.logged = logged;
		}
		
		public int getNum()
		{
			return num;
		}
		
		public boolean getOK()
		{
			return ok;
		}
		
		public void run()
		{
			sendMassDelete(tree, tx, keys, types, orders, indexes);
		}
		
		private void sendMassDelete(ArrayList<Object> tree, Transaction tx, ArrayList<ArrayList<String>> keys, ArrayList<ArrayList<String>> types, ArrayList<ArrayList<Boolean>> orders, ArrayList<String> indexes)
		{
			Object obj = tree.get(0);
			while (obj instanceof ArrayList)
			{
				obj = ((ArrayList)obj).get(0);
			}
			
			Socket sock = null;
			try
			{
				String hostname = new MetaData().getHostNameForNode((Integer)obj, tx);
				sock = new CompressedSocket(hostname, Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
				OutputStream out = sock.getOutputStream();
				byte[] outMsg = "MDELETE         ".getBytes("UTF-8");
				outMsg[8] = 0;
				outMsg[9] = 0;
				outMsg[10] = 0;
				outMsg[11] = 0;
				outMsg[12] = 0;
				outMsg[13] = 0;
				outMsg[14] = 0;
				outMsg[15] = 0;
				out.write(outMsg);
				out.write(longToBytes(tx.number()));
				//write schema and table
				out.write(stringToBytes(schema));
				out.write(stringToBytes(table));
				if (logged)
				{
					out.write((byte)1);
				}
				else
				{
					out.write((byte)0);
				}
				ObjectOutputStream objOut = new ObjectOutputStream(out);
				objOut.writeObject(convertToHosts(tree, tx));
				objOut.writeObject(indexes);
				objOut.writeObject(keys);
				objOut.writeObject(types);
				objOut.writeObject(orders);
				objOut.writeObject(pos2Col);
				objOut.writeObject(cols2Types);
				objOut.flush();
				out.flush();
				getConfirmation(sock);
				int count = 4;
				int off = 0;
				byte[] numBytes = new byte[4];
				while (count > 0)
				{
					int temp = sock.getInputStream().read(numBytes, off, 4-off);
					if (temp == -1)
					{
						ok = false;
						objOut.close();
						sock.close();
					}
					
					count -= temp;
				}
				
				num = bytesToInt(numBytes);
				objOut.close();
				sock.close();
				ok = true;
			}
			catch(Exception e)
			{
				ok = false;
				try
				{
					sock.close();
				}
				catch(Exception f)
				{}
				HRDBMSWorker.logger.debug("", e);
			}
		}
	}
	
	private int bytesToInt(byte[] val)
	{
		final int ret = java.nio.ByteBuffer.wrap(val).getInt();
		return ret;
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
	
	private byte[] stringToBytes(String string)
	{
		byte[] data = null;
		try
		{
			data = string.getBytes("UTF-8");
		}
		catch(Exception e)
		{}
		byte[] len = intToBytes(data.length);
		byte[] retval = new byte[data.length + len.length];
		System.arraycopy(len, 0, retval, 0, len.length);
		System.arraycopy(data, 0, retval, len.length, data.length);
		return retval;
	}
	
	private static byte[] longToBytes(long val)
	{
		final byte[] buff = new byte[8];
		buff[0] = (byte)(val >> 56);
		buff[1] = (byte)((val & 0x00FF000000000000L) >> 48);
		buff[2] = (byte)((val & 0x0000FF0000000000L) >> 40);
		buff[3] = (byte)((val & 0x000000FF00000000L) >> 32);
		buff[4] = (byte)((val & 0x00000000FF000000L) >> 24);
		buff[5] = (byte)((val & 0x0000000000FF0000L) >> 16);
		buff[6] = (byte)((val & 0x000000000000FF00L) >> 8);
		buff[7] = (byte)((val & 0x00000000000000FFL));
		return buff;
	}
	
	private static ArrayList<Object> convertToHosts(ArrayList<Object> tree, Transaction tx) throws Exception
	{
		ArrayList<Object> retval = new ArrayList<Object>();
		int i = 0;
		while (i < tree.size())
		{
			Object obj = tree.get(i);
			if (obj instanceof Integer)
			{
				retval.add(new MetaData().getHostNameForNode((Integer)obj, tx));
			}
			else
			{
				retval.add(convertToHosts((ArrayList<Object>)obj, tx));
			}
			
			i++;
		}
		
		return retval;
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
	}

	@Override
	public String toString()
	{
		return "MassDeleteOperator";
	}

}
