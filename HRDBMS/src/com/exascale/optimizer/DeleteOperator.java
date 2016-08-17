package com.exascale.optimizer;

import java.io.BufferedOutputStream;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import com.exascale.filesystem.RID;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.misc.DataEndMarker;
import com.exascale.misc.HJOMultiHashMap;
import com.exascale.misc.MultiHashMap;
import com.exascale.misc.VHJOMultiHashMap;
import com.exascale.optimizer.MetaData.PartitionMetaData;
import com.exascale.tables.Plan;
import com.exascale.tables.Transaction;
import com.exascale.threads.ConnectionWorker;
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
	private HJOMultiHashMap map = new HJOMultiHashMap<Integer, RIDAndIndexKeys>();
	private Transaction tx;
	private static HashMap<Long, HashMap<String, DeleteOperator>> txDelayedMaps = new HashMap<Long, HashMap<String, DeleteOperator>>();
	private static IdentityHashMap<DeleteOperator, List<RIDAndIndexKeys>> delayedRows = new IdentityHashMap<DeleteOperator, List<RIDAndIndexKeys>>();
	private static HashMap<Long, IdentityHashMap<DeleteOperator, DeleteOperator>> notYetStarted = new HashMap<Long, IdentityHashMap<DeleteOperator, DeleteOperator>>();
	//private static AtomicInteger debugCounter = new AtomicInteger(0);

	public DeleteOperator(String schema, String table, MetaData meta)
	{
		this.schema = schema;
		this.table = table;
		this.meta = meta;
	}
	
	private static boolean registerDelayed(DeleteOperator caller, String schema, String table, int node, Transaction tx, List<RIDAndIndexKeys> rows)
	{
		synchronized(txDelayedMaps)
		{
			String key = schema + "." + table + "~" + node;
			HashMap<String, DeleteOperator> map = txDelayedMaps.get(tx.number());
			if (map == null)
			{
				map = new HashMap<String, DeleteOperator>();
				map.put(key, caller);
				delayedRows.put(caller, rows);
				txDelayedMaps.put(tx.number(), map);
				return true;
			}
			
			DeleteOperator owner = map.get(key);
			if (owner == null)
			{
				map.put(key, caller);
				delayedRows.put(caller, rows);
				return true;
			}
			
			try
			{
				//List<RIDAndIndexKeys> currentList = delayedRows.get(owner);
				//HRDBMSWorker.logger.debug("Retrieved current list for node " + node + " which has size " + currentList.size()); //DEBUG
				//currentList.addAll(rows);
				//HRDBMSWorker.logger.debug("After adding " + rows.size() + " it is now of size " + currentList.size()); //DEBUG
				delayedRows.get(owner).addAll(rows);
			}
			catch(NullPointerException e)
			{
				HRDBMSWorker.logger.debug("Owner = " + owner + ", map.get(key) = " + map.get(key) + ", delayedRows.get(owner) = " + delayedRows.get(owner));
			}
			return false;
		}
	}
	
	private static boolean registerDelayedCantOwn(DeleteOperator caller, String schema, String table, int node, Transaction tx, List<RIDAndIndexKeys> rows)
	{
		synchronized(txDelayedMaps)
		{
			String key = schema + "." + table + "~" + node;
			HashMap<String, DeleteOperator> map = txDelayedMaps.get(tx.number());
			if (map == null)
			{
				return false;
			}
			
			DeleteOperator owner = map.get(key);
			if (owner == null)
			{
				return false;
			}
			
			delayedRows.get(owner).addAll(rows);
			return true;
		}
	}
	
	private static List<RIDAndIndexKeys> deregister(DeleteOperator owner, String schema, String table, int node, Transaction tx)
	{
		synchronized(txDelayedMaps)
		{
			List<RIDAndIndexKeys> retval = delayedRows.remove(owner);
			HashMap<String, DeleteOperator> map = txDelayedMaps.get(tx.number());
			String key = schema + "." + table + "~" + node;
			map.remove(key);
			if (map.size() == 0)
			{
				txDelayedMaps.remove(tx.number());
			}
			
			return retval;
		}
	}
	
	public static void wakeUpDelayed(Transaction tx)
	{
		while (true)
		{
			synchronized(notYetStarted)
			{
				IdentityHashMap<DeleteOperator, DeleteOperator> thisTx = notYetStarted.get(tx.number());
				if (thisTx == null)
				{
					break;
				}
			}
			
			try
			{
				Thread.sleep(1);
			}
			catch(InterruptedException e)
			{}
		}
		
		synchronized(txDelayedMaps)
		{
			//HRDBMSWorker.logger.debug("Entering wakeUpDelayed with " + txDelayedMaps); //DEBUG
			HashMap<String, DeleteOperator> map = txDelayedMaps.get(tx.number());
			
			if (map == null)
			{
				return;
			}
	
			//HRDBMSWorker.logger.debug("Waking up " + map.size() + " threads"); //DEBUG
			for (DeleteOperator io : map.values())
			{
				synchronized(io)
				{
					io.notify();
				}
			}
		}
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
			LockSupport.parkNanos(500);
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
	public long numRecsReceived()
	{
		return 0;
	}

	@Override
	public Operator parent()
	{
		return parent;
	}

	@Override
	public boolean receivedDEM()
	{
		return false;
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
		if (ConnectionWorker.isDelayed(tx))
		{
			//notYetStarted.multiPut(tx.number(), this);
			synchronized(notYetStarted)
			{
				IdentityHashMap<DeleteOperator, DeleteOperator> thisTx = notYetStarted.get(tx.number());
				if (thisTx == null)
				{
					thisTx = new IdentityHashMap<DeleteOperator, DeleteOperator>();
					notYetStarted.put(tx.number(), thisTx);
				}
			
				thisTx.put(this, this);
			}
		}

		ArrayList<String> indexes;
		ArrayList<ArrayList<String>> keys;
		ArrayList<ArrayList<String>> types;
		ArrayList<ArrayList<Boolean>> orders;
		TreeMap<Integer, String> tP2C;
		HashMap<String, String> tC2T;
		int type;
		try
		{
			child.start();
			indexes = meta.getIndexFileNamesForTable(schema, table, tx);
			keys = meta.getKeys(indexes, tx);
			types = meta.getTypes(indexes, tx);
			orders = meta.getOrders(indexes, tx);
			tP2C = meta.getPos2ColForTable(schema, table, tx);
			tC2T = meta.getCols2TypesForTable(schema, table, tx);
			type = meta.getTypeForTable(schema, table, tx);

			Object o = child.next(this);
			while (!(o instanceof DataEndMarker))
			{
				try
				{
					ArrayList<Object> row = (ArrayList<Object>)o;
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

					RID rid = new RID(node, device, block, rec);
					//HRDBMSWorker.logger.debug("About to delete RID = " + rid); //DEBUG
					RIDAndIndexKeys raik = new RIDAndIndexKeys(rid, indexKeys);
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
					flush(indexes, keys, types, orders, tP2C, tC2T, type);
					if (num.get() == Integer.MIN_VALUE)
					{
						done = true;
						return;
					}
				}
				else if (map.totalSize() > Integer.parseInt(HRDBMSWorker.getHParms().getProperty("max_batch")))
				{
					flush(indexes, keys, types, orders, tP2C, tC2T, type);
					if (num.get() == Integer.MIN_VALUE)
					{
						done = true;
						return;
					}
				}

				o = child.next(this);
			}
		}
		catch (Exception e)
		{
			if (ConnectionWorker.isDelayed(tx))
			{
				//notYetStarted.remove(tx.number(), this);
				synchronized(notYetStarted)
				{
					IdentityHashMap<DeleteOperator, DeleteOperator> thisTx = notYetStarted.get(tx.number());
					thisTx.remove(this);
					if (thisTx.size() == 0)
					{
						notYetStarted.remove(tx.number());
					}
				}
			}
			
			throw e;
		}
		
		if (ConnectionWorker.isDelayed(tx))
		{
			//notYetStarted.remove(tx.number(), this);
			synchronized(notYetStarted)
			{
				IdentityHashMap<DeleteOperator, DeleteOperator> thisTx = notYetStarted.get(tx.number());
				thisTx.remove(this);
				if (thisTx.size() == 0)
				{
					notYetStarted.remove(tx.number());
				}
			}
		}

		//if (map.totalSize() > 0)
		//{
		//	flush(indexes, keys, types, orders, tP2C, tC2T, type);
		//}
		
		if (!ConnectionWorker.isDelayed(tx) && map.totalSize() > 0)
		{
			flush(indexes, keys, types, orders, tP2C, tC2T, type);
		}
		else if (ConnectionWorker.isDelayed(tx) && map.totalSize() > 0)
		{
			delayedFlush(indexes, keys, types, orders, tP2C, tC2T, type);
		}

		done = true;
	}

	@Override
	public String toString()
	{
		return "DeleteOperator";
	}

	private void flush(ArrayList<String> indexes, ArrayList<ArrayList<String>> keys, ArrayList<ArrayList<String>> types, ArrayList<ArrayList<Boolean>> orders, TreeMap<Integer, String> tP2C, HashMap<String, String> tC2T, int type) throws Exception
	{
		//HRDBMSWorker.logger.debug("Non-delayed flush for " + map.size() + " nodes and " + map.totalSize() + " rows"); //DEBUG
		ArrayList<FlushThread> threads = new ArrayList<FlushThread>();
		for (Object o : map.getKeySet())
		{
			int node = (Integer)o;
			List<RIDAndIndexKeys> list = map.get(node);
			//HRDBMSWorker.logger.debug(list.size() + " rows for node " + node); //DEBUG
			if (node == -1)
			{
				ArrayList<Integer> coords = MetaData.getCoordNodes();

				for (Integer coord : coords)
				{
					threads.add(new FlushThread(list, indexes, coord, keys, types, orders, tP2C, tC2T, type));
				}
			}
			else
			{
				threads.add(new FlushThread(list, indexes, node, keys, types, orders, tP2C, tC2T, type));
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
	
	private void delayedFlush(ArrayList<String> indexes, ArrayList<ArrayList<String>> keys, ArrayList<ArrayList<String>> types, ArrayList<ArrayList<Boolean>> orders, TreeMap<Integer, String> pos2Col, HashMap<String, String> cols2Types, int type) throws Exception
	{
		//HRDBMSWorker.logger.debug("Delayed flush with " + map.totalSize() + " entries"); //DEBUG
		FlushThread thread = null;
		int node = -1;
		boolean realOwner = false;
		int realNode = -1;
		HashSet copy = new HashSet(map.getKeySet());
		for (Object o : copy)
		{
			node = (Integer)o;
			if (node == -1)
			{
				HRDBMSWorker.logger.debug("Delayed flush in delete operation against system metadata is not allowed!");
				throw new Exception("Delayed flush in delete operation against system metadata is not allowed!");
			}
			List<RIDAndIndexKeys> list = map.get(node);
			//HRDBMSWorker.logger.debug(list.size() + " rows for node " + node);
			
			if (!realOwner)
			{
				boolean owner = DeleteOperator.registerDelayed(this, schema, table, node, tx, list);
				if (owner)
				{
					realOwner = true;
					realNode = node;
					//HRDBMSWorker.logger.debug("We will own node " + realNode); //DEBUG
				}
				
				map.multiRemove(o);
			}
			else
			{
				boolean handled = DeleteOperator.registerDelayedCantOwn(this, schema, table, node, tx, list);
				if (handled)
				{
					//HRDBMSWorker.logger.debug("Someone else wil own node " + node); //DEBUG
					map.multiRemove(o);
				}
				//else
				//{
				//	HRDBMSWorker.logger.debug("Found no one to own node " + node); //DEBUG
				//}
			}
		}
		
		if (map.size() > 0)
		{
			//HRDBMSWorker.logger.debug("After processing map was size " + map.size()); //DEBUG
			flush(indexes, keys, types, orders, pos2Col, cols2Types, type);
		}
		//else
		//{
		//	HRDBMSWorker.logger.debug("After processing map was empty"); //DEBUG
		//}
		
		if (!realOwner)
		{
			return;
		}

		//HRDBMSWorker.logger.debug("Sleeping for node " + realNode);
		synchronized(this)
		{
			try
			{
				wait();
			}
			catch(InterruptedException e)
			{}
		}
		
		List<RIDAndIndexKeys> list = DeleteOperator.deregister(this, schema, table, realNode, tx);
		//HRDBMSWorker.logger.debug("Woke up for node " + realNode + " with " + list.size() + " rows"); //DEBUG
		thread = new FlushThread(list, indexes, realNode, keys, types, orders, pos2Col, cols2Types, type);
		thread.start();

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
			//HRDBMSWorker.logger.debug("IO setting num to MIN_VALUE");
			num.set(Integer.MIN_VALUE);
		}
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
		private final List<RIDAndIndexKeys> list;
		private final ArrayList<String> indexes;
		private boolean ok = true;
		private final int node;
		private final ArrayList<ArrayList<String>> keys;
		private final ArrayList<ArrayList<String>> types;
		private final ArrayList<ArrayList<Boolean>> orders;
		private final TreeMap<Integer, String> tP2C;
		private final HashMap<String, String> tC2T;
		private final int type;

		public FlushThread(List<RIDAndIndexKeys> list, ArrayList<String> indexes, int node, ArrayList<ArrayList<String>> keys, ArrayList<ArrayList<String>> types, ArrayList<ArrayList<Boolean>> orders, TreeMap<Integer, String> tP2C, HashMap<String, String> tC2T, int type)
		{
			this.list = list;
			this.indexes = indexes;
			this.node = node;
			this.keys = keys;
			this.types = types;
			this.orders = orders;
			this.tP2C = tP2C;
			this.tC2T = tC2T;
			this.type = type;
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
			//int count = debugCounter.addAndGet(list.size()); //DEBUG
			//HRDBMSWorker.logger.debug("Flushed " + count + " deletes");
			try
			{
				String hostname = new MetaData().getHostNameForNode(node, tx);
				// sock = new Socket(hostname,
				// Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
				sock = new Socket();
				sock.setReceiveBufferSize(4194304);
				sock.setSendBufferSize(4194304);
				sock.connect(new InetSocketAddress(hostname, Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number"))));
				OutputStream out = new BufferedOutputStream(sock.getOutputStream());
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
				out.write(intToBytes(type));
				//ObjectOutputStream objOut = new ObjectOutputStream(out);
				IdentityHashMap<Object, Long> prev = new IdentityHashMap<Object, Long>();
				OperatorUtils.serializeALS(indexes, out, prev);
				//objOut.writeObject(indexes);
				OperatorUtils.serializeALRAIK(new ArrayList(list), out, prev);
				//objOut.writeObject(new ArrayList(list));
				OperatorUtils.serializeALALS(keys, out, prev);
				//objOut.writeObject(keys);
				OperatorUtils.serializeALALS(types, out, prev);
				//objOut.writeObject(types);
				OperatorUtils.serializeALALB(orders, out, prev);
				//objOut.writeObject(orders);
				OperatorUtils.serializeTM(tP2C, out, prev);
				//objOut.writeObject(tP2C);
				OperatorUtils.serializeStringHM(tC2T, out, prev);
				//objOut.writeObject(tC2T);
				//objOut.flush();
				out.flush();
				getConfirmation(sock);
				//objOut.close();
				out.close();
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
