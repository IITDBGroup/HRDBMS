package com.exascale.optimizer;

import java.io.BufferedOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import com.exascale.filesystem.RID;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.misc.DataEndMarker;
import com.exascale.misc.HJOMultiHashMap;
import com.exascale.tables.Plan;
import com.exascale.tables.Transaction;
import com.exascale.threads.ConnectionWorker;
import com.exascale.threads.HRDBMSThread;

public final class DeleteOperator implements Operator, Serializable
{
	private static Map<Long, Map<String, DeleteOperator>> txDelayedMaps = new HashMap<Long, Map<String, DeleteOperator>>();
	private static IdentityHashMap<DeleteOperator, List<RIDAndIndexKeys>> delayedRows = new IdentityHashMap<DeleteOperator, List<RIDAndIndexKeys>>();
	private static Map<Long, IdentityHashMap<DeleteOperator, DeleteOperator>> notYetStarted = new HashMap<Long, IdentityHashMap<DeleteOperator, DeleteOperator>>();
	// private static AtomicInteger debugCounter = new AtomicInteger(0);
	private Operator child;
	private final MetaData meta;
	private Map<String, String> cols2Types;
	private Map<String, Integer> cols2Pos;
	private Map<Integer, String> pos2Col;
	private Operator parent;
	private int node;
	private final String schema;
	private final String table;
	private final AtomicInteger num = new AtomicInteger(0);
	private boolean done = false;
	private HJOMultiHashMap map = new HJOMultiHashMap<Integer, RIDAndIndexKeys>();
	private Transaction tx;

	public DeleteOperator(final String schema, final String table, final MetaData meta)
	{
		this.schema = schema;
		this.table = table;
		this.meta = meta;
	}

	public static void wakeUpDelayed(final Transaction tx)
	{
		while (true)
		{
			synchronized (notYetStarted)
			{
				final IdentityHashMap<DeleteOperator, DeleteOperator> thisTx = notYetStarted.get(tx.number());
				if (thisTx == null)
				{
					break;
				}
			}

			try
			{
				Thread.sleep(1);
			}
			catch (final InterruptedException e)
			{
			}
		}

		synchronized (txDelayedMaps)
		{
			// HRDBMSWorker.logger.debug("Entering wakeUpDelayed with " +
			// txDelayedMaps); //DEBUG
			final Map<String, DeleteOperator> map = txDelayedMaps.get(tx.number());

			if (map == null)
			{
				return;
			}

			// HRDBMSWorker.logger.debug("Waking up " + map.size() + "
			// threads"); //DEBUG
			for (final DeleteOperator io : map.values())
			{
				synchronized (io)
				{
					io.notify();
				}
			}
		}
	}

	private static List<RIDAndIndexKeys> deregister(final DeleteOperator owner, final String schema, final String table, final int node, final Transaction tx)
	{
		synchronized (txDelayedMaps)
		{
			final List<RIDAndIndexKeys> retval = delayedRows.remove(owner);
			final Map<String, DeleteOperator> map = txDelayedMaps.get(tx.number());
			final String key = schema + "." + table + "~" + node;
			map.remove(key);
			if (map.size() == 0)
			{
				txDelayedMaps.remove(tx.number());
			}

			return retval;
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

	private static byte[] longToBytes(final long val)
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

	private static boolean registerDelayed(final DeleteOperator caller, final String schema, final String table, final int node, final Transaction tx, final List<RIDAndIndexKeys> rows)
	{
		synchronized (txDelayedMaps)
		{
			final String key = schema + "." + table + "~" + node;
			Map<String, DeleteOperator> map = txDelayedMaps.get(tx.number());
			if (map == null)
			{
				map = new HashMap<String, DeleteOperator>();
				map.put(key, caller);
				delayedRows.put(caller, rows);
				txDelayedMaps.put(tx.number(), map);
				return true;
			}

			final DeleteOperator owner = map.get(key);
			if (owner == null)
			{
				map.put(key, caller);
				delayedRows.put(caller, rows);
				return true;
			}

			try
			{
				// List<RIDAndIndexKeys> currentList = delayedRows.get(owner);
				// HRDBMSWorker.logger.debug("Retrieved current list for node "
				// + node + " which has size " + currentList.size()); //DEBUG
				// currentList.addAll(rows);
				// HRDBMSWorker.logger.debug("After adding " + rows.size() + "
				// it is now of size " + currentList.size()); //DEBUG
				delayedRows.get(owner).addAll(rows);
			}
			catch (final NullPointerException e)
			{
				HRDBMSWorker.logger.debug("Owner = " + owner + ", map.get(key) = " + map.get(key) + ", delayedRows.get(owner) = " + delayedRows.get(owner));
			}
			return false;
		}
	}

	private static boolean registerDelayedCantOwn(final DeleteOperator caller, final String schema, final String table, final int node, final Transaction tx, final List<RIDAndIndexKeys> rows)
	{
		synchronized (txDelayedMaps)
		{
			final String key = schema + "." + table + "~" + node;
			final Map<String, DeleteOperator> map = txDelayedMaps.get(tx.number());
			if (map == null)
			{
				return false;
			}

			final DeleteOperator owner = map.get(key);
			if (owner == null)
			{
				return false;
			}

			delayedRows.get(owner).addAll(rows);
			return true;
		}
	}

	private static byte[] stringToBytes(final String string)
	{
		byte[] data = null;
		try
		{
			data = string.getBytes(StandardCharsets.UTF_8);
		}
		catch (final Exception e)
		{
		}
		final byte[] len = intToBytes(data.length);
		final byte[] retval = new byte[data.length + len.length];
		System.arraycopy(len, 0, retval, 0, len.length);
		System.arraycopy(data, 0, retval, len.length, data.length);
		return retval;
	}

	@Override
	public void add(final Operator op) throws Exception
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
	public List<Operator> children()
	{
		final List<Operator> retval = new ArrayList<Operator>(1);
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
	public Map<String, Integer> getCols2Pos()
	{
		return cols2Pos;
	}

	@Override
	public Map<String, String> getCols2Types()
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
	public Map<Integer, String> getPos2Col()
	{
		return pos2Col;
	}

	@Override
	public List<String> getReferences()
	{
		final List<String> retval = new ArrayList<String>();
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
	public Object next(final Operator op) throws Exception
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

		final int retval = num.get();
		num.set(-1);
		return retval;
	}

	@Override
	public void nextAll(final Operator op) throws Exception
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
	public void registerParent(final Operator op) throws Exception
	{
		throw new Exception("DeleteOperator does not support parents");
	}

	@Override
	public void removeChild(final Operator op)
	{
		if (op == child)
		{
			child = null;
			op.removeParent(this);
		}
	}

	@Override
	public void removeParent(final Operator op)
	{
		parent = null;
	}

	@Override
	public void reset() throws Exception
	{
		throw new Exception("DeleteOperator does not support reset()");
	}

	@Override
	public void serialize(final OutputStream out, final IdentityHashMap<Object, Long> prev) throws Exception
	{
		throw new Exception("Trying to serialize a delete operator");
	}

	@Override
	public void setChildPos(final int pos)
	{
	}

	@Override
	public void setNode(final int node)
	{
		this.node = node;
	}

	@Override
	public void setPlan(final Plan plan)
	{
	}

	public void setTransaction(final Transaction tx)
	{
		this.tx = tx;
	}

	@Override
	public void start() throws Exception
	{
		if (ConnectionWorker.isDelayed(tx))
		{
			// notYetStarted.multiPut(tx.number(), this);
			synchronized (notYetStarted)
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

		List<String> indexes;
		List<List<String>> keys;
		List<List<String>> types;
		List<List<Boolean>> orders;
		Map<Integer, String> tP2C;
		Map<String, String> tC2T;
		int type;
		try
		{
			child.start();
			indexes = MetaData.getIndexFileNamesForTable(schema, table, tx);
			keys = MetaData.getKeys(indexes, tx);
			types = MetaData.getTypes(indexes, tx);
			orders = MetaData.getOrders(indexes, tx);
			tP2C = MetaData.getPos2ColForTable(schema, table, tx);
			tC2T = MetaData.getCols2TypesForTable(schema, table, tx);
			type = MetaData.getTypeForTable(schema, table, tx);

			Object o = child.next(this);
			while (!(o instanceof DataEndMarker))
			{
				try
				{
					final List<Object> row = (List<Object>)o;
					final int node = (Integer)row.get(child.getCols2Pos().get("_RID1"));
					final int device = (Integer)row.get(child.getCols2Pos().get("_RID2"));
					final int block = (Integer)row.get(child.getCols2Pos().get("_RID3"));
					final int rec = (Integer)row.get(child.getCols2Pos().get("_RID4"));
					final List<List<Object>> indexKeys = new ArrayList<List<Object>>();
					for (final String index : indexes)
					{
						final List<Object> keys2 = new ArrayList<Object>();
						final List<String> cols = MetaData.getColsFromIndexFileName(index, tx, keys, indexes);
						for (final String col : cols)
						{
							keys2.add(row.get(child.getCols2Pos().get(col)));
						}

						indexKeys.add(keys2);
					}

					final RID rid = new RID(node, device, block, rec);
					// HRDBMSWorker.logger.debug("About to delete RID = " +
					// rid); //DEBUG
					final RIDAndIndexKeys raik = new RIDAndIndexKeys(rid, indexKeys);
					map.multiPut(node, raik);
				}
				catch (final Exception e)
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
		catch (final Exception e)
		{
			if (ConnectionWorker.isDelayed(tx))
			{
				// notYetStarted.remove(tx.number(), this);
				synchronized (notYetStarted)
				{
					final IdentityHashMap<DeleteOperator, DeleteOperator> thisTx = notYetStarted.get(tx.number());
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
			// notYetStarted.remove(tx.number(), this);
			synchronized (notYetStarted)
			{
				final IdentityHashMap<DeleteOperator, DeleteOperator> thisTx = notYetStarted.get(tx.number());
				thisTx.remove(this);
				if (thisTx.size() == 0)
				{
					notYetStarted.remove(tx.number());
				}
			}
		}

		// if (map.totalSize() > 0)
		// {
		// flush(indexes, keys, types, orders, tP2C, tC2T, type);
		// }

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

	private void delayedFlush(final List<String> indexes, final List<List<String>> keys, final List<List<String>> types, final List<List<Boolean>> orders, final Map<Integer, String> pos2Col, final Map<String, String> cols2Types, final int type) throws Exception
	{
		// HRDBMSWorker.logger.debug("Delayed flush with " + map.totalSize() + "
		// entries"); //DEBUG
		FlushThread thread = null;
		int node = -1;
		boolean realOwner = false;
		int realNode = -1;
		final Set copy = new HashSet(map.getKeySet());
		for (final Object o : copy)
		{
			node = (Integer)o;
			if (node == -1)
			{
				HRDBMSWorker.logger.debug("Delayed flush in delete operation against system metadata is not allowed!");
				throw new Exception("Delayed flush in delete operation against system metadata is not allowed!");
			}
			final List<RIDAndIndexKeys> list = map.get(node);
			// HRDBMSWorker.logger.debug(list.size() + " rows for node " +
			// node);

			if (!realOwner)
			{
				final boolean owner = DeleteOperator.registerDelayed(this, schema, table, node, tx, list);
				if (owner)
				{
					realOwner = true;
					realNode = node;
					// HRDBMSWorker.logger.debug("We will own node " +
					// realNode); //DEBUG
				}

				map.multiRemove(o);
			}
			else
			{
				final boolean handled = DeleteOperator.registerDelayedCantOwn(this, schema, table, node, tx, list);
				if (handled)
				{
					// HRDBMSWorker.logger.debug("Someone else wil own node " +
					// node); //DEBUG
					map.multiRemove(o);
				}
				// else
				// {
				// HRDBMSWorker.logger.debug("Found no one to own node " +
				// node); //DEBUG
				// }
			}
		}

		if (map.size() > 0)
		{
			// HRDBMSWorker.logger.debug("After processing map was size " +
			// map.size()); //DEBUG
			flush(indexes, keys, types, orders, pos2Col, cols2Types, type);
		}
		// else
		// {
		// HRDBMSWorker.logger.debug("After processing map was empty"); //DEBUG
		// }

		if (!realOwner)
		{
			return;
		}

		// HRDBMSWorker.logger.debug("Sleeping for node " + realNode);
		synchronized (this)
		{
			try
			{
				wait();
			}
			catch (final InterruptedException e)
			{
			}
		}

		final List<RIDAndIndexKeys> list = DeleteOperator.deregister(this, schema, table, realNode, tx);
		// HRDBMSWorker.logger.debug("Woke up for node " + realNode + " with " +
		// list.size() + " rows"); //DEBUG
		thread = new FlushThread(list, indexes, realNode, keys, types, orders, pos2Col, cols2Types, type);
		thread.start();

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
			// HRDBMSWorker.logger.debug("IO setting num to MIN_VALUE");
			num.set(Integer.MIN_VALUE);
		}
	}

	private void flush(final List<String> indexes, final List<List<String>> keys, final List<List<String>> types, final List<List<Boolean>> orders, final Map<Integer, String> tP2C, final Map<String, String> tC2T, final int type) throws Exception
	{
		// HRDBMSWorker.logger.debug("Non-delayed flush for " + map.size() + "
		// nodes and " + map.totalSize() + " rows"); //DEBUG
		final List<FlushThread> threads = new ArrayList<FlushThread>();
		for (final Object o : map.getKeySet())
		{
			final int node = (Integer)o;
			final List<RIDAndIndexKeys> list = map.get(node);
			// HRDBMSWorker.logger.debug(list.size() + " rows for node " +
			// node); //DEBUG
			if (node == -1)
			{
				final List<Integer> coords = MetaData.getCoordNodes();

				for (final Integer coord : coords)
				{
					threads.add(new FlushThread(list, indexes, coord, keys, types, orders, tP2C, tC2T, type));
				}
			}
			else
			{
				threads.add(new FlushThread(list, indexes, node, keys, types, orders, tP2C, tC2T, type));
			}
		}

		for (final FlushThread thread : threads)
		{
			thread.start();
		}

		for (final FlushThread thread : threads)
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
				num.set(Integer.MIN_VALUE);
			}
		}

		map.clear();
	}

	private class FlushThread extends HRDBMSThread
	{
		private final List<RIDAndIndexKeys> list;
		private final List<String> indexes;
		private boolean ok = true;
		private final int node;
		private final List<List<String>> keys;
		private final List<List<String>> types;
		private final List<List<Boolean>> orders;
		private final Map<Integer, String> tP2C;
		private final Map<String, String> tC2T;
		private final int type;

		public FlushThread(final List<RIDAndIndexKeys> list, final List<String> indexes, final int node, final List<List<String>> keys, final List<List<String>> types, final List<List<Boolean>> orders, final Map<Integer, String> tP2C, final Map<String, String> tC2T, final int type)
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
			// int count = debugCounter.addAndGet(list.size()); //DEBUG
			// HRDBMSWorker.logger.debug("Flushed " + count + " deletes");
			try
			{
				// new MetaData();
				final String hostname = MetaData.getHostNameForNode(node, tx);
				// sock = new Socket(hostname,
				// Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
				sock = new Socket();
				sock.setReceiveBufferSize(4194304);
				sock.setSendBufferSize(4194304);
				sock.connect(new InetSocketAddress(hostname, Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number"))));
				final OutputStream out = new BufferedOutputStream(sock.getOutputStream());
				final byte[] outMsg = "DELETE          ".getBytes(StandardCharsets.UTF_8);
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
				// ObjectOutputStream objOut = new ObjectOutputStream(out);
				final IdentityHashMap<Object, Long> prev = new IdentityHashMap<Object, Long>();
				OperatorUtils.serializeALS(indexes, out, prev);
				// objOut.writeObject(indexes);
				OperatorUtils.serializeALRAIK(new ArrayList(list), out, prev);
				// objOut.writeObject(new ArrayList(list));
				OperatorUtils.serializeALALS(keys, out, prev);
				// objOut.writeObject(keys);
				OperatorUtils.serializeALALS(types, out, prev);
				// objOut.writeObject(types);
				OperatorUtils.serializeALALB(orders, out, prev);
				// objOut.writeObject(orders);
				OperatorUtils.serializeTM(tP2C, out, prev);
				// objOut.writeObject(tP2C);
				OperatorUtils.serializeStringHM(tC2T, out, prev);
				// objOut.writeObject(tC2T);
				// objOut.flush();
				out.flush();
				getConfirmation(sock);
				// objOut.close();
				out.close();
				sock.close();
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.debug("", e);
				try
				{
					if (sock != null)
					{
						sock.close();
					}
				}
				catch (final Exception f)
				{
				}
				ok = false;
			}
		}

		private void getConfirmation(final Socket sock) throws Exception
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
	}
}
