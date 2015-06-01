package com.exascale.optimizer;

import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import com.exascale.filesystem.RID;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.managers.ResourceManager;
import com.exascale.managers.ResourceManager.DiskBackedArray;
import com.exascale.misc.DataEndMarker;
import com.exascale.misc.MultiHashMap;
import com.exascale.tables.Plan;
import com.exascale.tables.Transaction;
import com.exascale.threads.HRDBMSThread;

public final class DeleteOperator implements Operator, Serializable
{
	private Operator child;
	private final MetaData meta;
	private HashMap<String, String> cols2Types;
	private HashMap<String, Integer> cols2Pos;
	private TreeMap<Integer, String> pos2Col;
	private Operator parent;
	private int node;
	private final String schema;
	private final String table;
	private final AtomicInteger num = new AtomicInteger(0);
	private boolean done = false;
	private MultiHashMap map = new MultiHashMap<Integer, RIDAndIndexKeys>();
	private Transaction tx;

	public DeleteOperator(String schema, String table, MetaData meta)
	{
		this.schema = schema;
		this.table = table;
		this.meta = meta;
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

	@Override
	public void add(Operator op) throws Exception
	{
		if (child == null)
		{
			child = op;
			child.registerParent(this);
		}
		else
		{
			throw new Exception("DeleteOperator only supports 1 child.");
		}
	}

	@Override
	public ArrayList<Operator> children()
	{
		final ArrayList<Operator> retval = new ArrayList<Operator>(1);
		retval.add(child);
		return retval;
	}

	@Override
	public DeleteOperator clone()
	{
		final DeleteOperator retval = new DeleteOperator(schema, table, meta);
		retval.node = node;
		return retval;
	}

	@Override
	public void close() throws Exception
	{
		cols2Pos = null;
		cols2Types = null;
		pos2Col = null;
		map = null;
		child.close();
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

	public String getSchema()
	{
		return schema;
	}

	public String getTable()
	{
		return table;
	}

	@Override
	// @?Parallel
	public Object next(Operator op) throws Exception
	{
		while (!done)
		{
			Thread.sleep(1);
		}

		if (num.get() == Integer.MIN_VALUE)
		{
			throw new Exception("An error occured during a delete operation");
		}

		if (num.get() < 0)
		{
			return new DataEndMarker();
		}

		int retval = num.get();
		num.set(-1);
		return retval;
	}

	@Override
	public void nextAll(Operator op) throws Exception
	{
		child.nextAll(op);
		Object o = next(op);
		while (!(o instanceof DataEndMarker) && !(o instanceof Exception))
		{
			o = next(op);
		}
	}

	@Override
	public Operator parent()
	{
		return parent;
	}

	@Override
	public void registerParent(Operator op) throws Exception
	{
		throw new Exception("DeleteOperator does not support parents");
	}

	@Override
	public void removeChild(Operator op)
	{
		if (op == child)
		{
			child = null;
			op.removeParent(this);
		}
	}

	@Override
	public void removeParent(Operator op)
	{
		parent = null;
	}

	@Override
	public void reset() throws Exception
	{
		throw new Exception("DeleteOperator does not support reset()");
	}

	@Override
	public void serialize(OutputStream out, IdentityHashMap<Object, Long> prev) throws Exception
	{
		throw new Exception("Trying to serialize a delete operator");
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

	@Override
	public void setPlan(Plan plan)
	{
	}

	public void setTransaction(Transaction tx)
	{
		this.tx = tx;
	}

	@Override
	public void start() throws Exception
	{
		child.start();
		ArrayList<String> indexes = meta.getIndexFileNamesForTable(schema, table, tx);
		Object o = child.next(this);
		DiskBackedArray dba = ResourceManager.newDiskBackedArray(10);
		ArrayList<ArrayList<String>> keys = meta.getKeys(indexes, tx);
		ArrayList<ArrayList<String>> types = meta.getTypes(indexes, tx);
		ArrayList<ArrayList<Boolean>> orders = meta.getOrders(indexes, tx);
		while (!(o instanceof DataEndMarker))
		{
			dba.add((ArrayList<Object>)o);
			o = child.next(this);
		}

		Iterator it = dba.iterator();
		while (it.hasNext())
		{
			try
			{
				ArrayList<Object> row = (ArrayList<Object>)it.next();
				int node = (Integer)row.get(child.getCols2Pos().get("_RID1"));
				int device = (Integer)row.get(child.getCols2Pos().get("_RID2"));
				int block = (Integer)row.get(child.getCols2Pos().get("_RID3"));
				int rec = (Integer)row.get(child.getCols2Pos().get("_RID4"));
				ArrayList<ArrayList<Object>> indexKeys = new ArrayList<ArrayList<Object>>();
				for (String index : indexes)
				{
					ArrayList<Object> keys2 = new ArrayList<Object>();
					ArrayList<String> cols = MetaData.getColsFromIndexFileName(index, tx, keys, indexes);
					for (String col : cols)
					{
						keys2.add(row.get(child.getCols2Pos().get(col)));
					}

					indexKeys.add(keys2);
				}

				RIDAndIndexKeys raik = new RIDAndIndexKeys(new RID(node, device, block, rec), indexKeys);
				map.multiPut(node, raik);
			}
			catch (Exception e)
			{
				HRDBMSWorker.logger.debug("", e);
				throw e;
			}
			num.incrementAndGet();
			if (map.size() > Integer.parseInt(HRDBMSWorker.getHParms().getProperty("max_neighbor_nodes")))
			{
				flush(indexes, keys, types, orders);
				if (num.get() == Integer.MIN_VALUE)
				{
					done = true;
					return;
				}
			}
			else if (map.totalSize() > Integer.parseInt(HRDBMSWorker.getHParms().getProperty("max_batch")))
			{
				flush(indexes, keys, types, orders);
				if (num.get() == Integer.MIN_VALUE)
				{
					done = true;
					return;
				}
			}
		}

		dba.close();
		if (map.totalSize() > 0)
		{
			flush(indexes, keys, types, orders);
		}

		done = true;
	}

	@Override
	public String toString()
	{
		return "DeleteOperator";
	}

	private void flush(ArrayList<String> indexes, ArrayList<ArrayList<String>> keys, ArrayList<ArrayList<String>> types, ArrayList<ArrayList<Boolean>> orders) throws Exception
	{
		ArrayList<FlushThread> threads = new ArrayList<FlushThread>();
		for (Object o : map.getKeySet())
		{
			int node = (Integer)o;
			Set<RIDAndIndexKeys> list = map.get(node);
			if (node == -1)
			{
				ArrayList<Integer> coords = MetaData.getCoordNodes();

				for (Integer coord : coords)
				{
					threads.add(new FlushThread(list, indexes, coord, keys, types, orders));
				}
			}
			else
			{
				threads.add(new FlushThread(list, indexes, node, keys, types, orders));
			}
		}

		for (FlushThread thread : threads)
		{
			thread.start();
		}

		for (FlushThread thread : threads)
		{
			while (true)
			{
				try
				{
					thread.join();
					break;
				}
				catch (InterruptedException e)
				{
				}
			}

			if (!thread.getOK())
			{
				num.set(Integer.MIN_VALUE);
			}
		}

		map.clear();
	}

	private byte[] stringToBytes(String string)
	{
		byte[] data = null;
		try
		{
			data = string.getBytes(StandardCharsets.UTF_8);
		}
		catch (Exception e)
		{
		}
		byte[] len = intToBytes(data.length);
		byte[] retval = new byte[data.length + len.length];
		System.arraycopy(len, 0, retval, 0, len.length);
		System.arraycopy(data, 0, retval, len.length, data.length);
		return retval;
	}

	private class FlushThread extends HRDBMSThread
	{
		private final Set<RIDAndIndexKeys> list;
		private final ArrayList<String> indexes;
		private boolean ok = true;
		private final int node;
		private final ArrayList<ArrayList<String>> keys;
		private final ArrayList<ArrayList<String>> types;
		private final ArrayList<ArrayList<Boolean>> orders;

		public FlushThread(Set<RIDAndIndexKeys> list, ArrayList<String> indexes, int node, ArrayList<ArrayList<String>> keys, ArrayList<ArrayList<String>> types, ArrayList<ArrayList<Boolean>> orders)
		{
			this.list = list;
			this.indexes = indexes;
			this.node = node;
			this.keys = keys;
			this.types = types;
			this.orders = orders;
		}

		public boolean getOK()
		{
			return ok;
		}

		@Override
		public void run()
		{
			// send schema, table, tx, indexes, and list
			Socket sock = null;
			try
			{
				String hostname = new MetaData().getHostNameForNode(node, tx);
				// sock = new Socket(hostname,
				// Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
				sock = new Socket();
				sock.setReceiveBufferSize(262144);
				sock.setSendBufferSize(262144);
				sock.connect(new InetSocketAddress(hostname, Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number"))));
				OutputStream out = sock.getOutputStream();
				byte[] outMsg = "DELETE          ".getBytes(StandardCharsets.UTF_8);
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
				out.write(stringToBytes(schema));
				out.write(stringToBytes(table));
				ObjectOutputStream objOut = new ObjectOutputStream(out);
				objOut.writeObject(indexes);
				objOut.writeObject(new ArrayList(list));
				objOut.writeObject(keys);
				objOut.writeObject(types);
				objOut.writeObject(orders);
				objOut.flush();
				out.flush();
				getConfirmation(sock);
				objOut.close();
				sock.close();
			}
			catch (Exception e)
			{
				HRDBMSWorker.logger.debug("", e);
				try
				{
					if (sock != null)
					{
						sock.close();
					}
				}
				catch (Exception f)
				{
				}
				ok = false;
			}
		}

		private void getConfirmation(Socket sock) throws Exception
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

			String inStr = new String(inMsg, StandardCharsets.UTF_8);
			if (!inStr.equals("OK"))
			{
				in.close();
				throw new Exception();
			}

			try
			{
				in.close();
			}
			catch (Exception e)
			{
			}
		}
	}
}
