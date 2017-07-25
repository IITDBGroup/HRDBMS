package com.exascale.optimizer;

import java.io.BufferedOutputStream;
import java.io.InputStream;
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
import java.util.Map;
import java.util.TreeMap;
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

public final class UpdateOperator implements Operator, Serializable
{
	private static Map<Long, IdentityHashMap<UpdateOperator, UpdateOperator>> notYetStarted = new HashMap<Long, IdentityHashMap<UpdateOperator, UpdateOperator>>();
	private static Map<Long, Map<String, UpdateOperator>> txDelayedMaps = new HashMap<Long, Map<String, UpdateOperator>>();
	private static IdentityHashMap<UpdateOperator, List<RIDAndIndexKeys>> delayedRows = new IdentityHashMap<UpdateOperator, List<RIDAndIndexKeys>>();
	private static IdentityHashMap<UpdateOperator, List<List<Object>>> delayedRows2 = new IdentityHashMap<UpdateOperator, List<List<Object>>>();
	private Operator child;
	private final MetaData meta;
	private Map<String, String> cols2Types;
	private Map<String, Integer> cols2Pos;
	private Map<Integer, String> pos2Col;
	private Operator parent;
	private int node;
	private transient Plan plan;
	private final String schema;
	private final String table;
	private final AtomicInteger num = new AtomicInteger(0);
	private boolean done = false;
	private HJOMultiHashMap map = new HJOMultiHashMap<Integer, RIDAndIndexKeys>();
	private HJOMultiHashMap map2 = new HJOMultiHashMap<Integer, List<Object>>();
	private Transaction tx;
	private List<Column> cols;
	private List<String> buildList;
	private List<List<String>> keys;
	private List<List<String>> types;
	private List<List<Boolean>> orders;

	public UpdateOperator(final String schema, final String table, final List<Column> cols, final List<String> buildList, final MetaData meta)
	{
		this.schema = schema;
		this.table = table;
		this.meta = meta;
		this.cols = cols;
		this.buildList = buildList;
	}

	public static void wakeUpDelayed(final Transaction tx)
	{
		while (true)
		{
			synchronized (notYetStarted)
			{
				final IdentityHashMap<UpdateOperator, UpdateOperator> thisTx = notYetStarted.get(tx.number());
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
			final Map<String, UpdateOperator> map = txDelayedMaps.get(tx.number());

			if (map == null)
			{
				return;
			}

			// HRDBMSWorker.logger.debug("Waking up " + map.size() + "
			// threads"); //DEBUG
			for (final UpdateOperator io : map.values())
			{
				synchronized (io)
				{
					io.notify();
				}
			}
		}
	}

	private static void cast(final List<Object> row, final Map<Integer, String> pos2Col, final Map<String, String> cols2Types)
	{
		int i = 0;
		final int size = pos2Col.size();
		while (i < size)
		{
			final String type = cols2Types.get(pos2Col.get(i));
			final Object o = row.get(i);
			if (type.equals("INT"))
			{
				if (o instanceof Long)
				{
					row.remove(i);
					row.add(i, ((Long)o).intValue());
				}
				else if (o instanceof Double)
				{
					row.remove(i);
					row.add(i, ((Double)o).intValue());
				}
			}
			else if (type.equals("LONG"))
			{
				if (o instanceof Integer)
				{
					row.remove(i);
					row.add(i, ((Integer)o).longValue());
				}
				else if (o instanceof Double)
				{
					row.remove(i);
					row.add(i, ((Double)o).longValue());
				}
			}
			else if (type.equals("FLOAT"))
			{
				if (o instanceof Integer)
				{
					row.remove(i);
					row.add(i, ((Integer)o).doubleValue());
				}
				else if (o instanceof Long)
				{
					row.remove(i);
					row.add(i, ((Long)o).doubleValue());
				}
			}

			i++;
		}
	}

	private static List deregister(final UpdateOperator owner, final String schema, final String table, final int node, final Transaction tx)
	{
		final List retval = new ArrayList(2);
		synchronized (txDelayedMaps)
		{
			final List<RIDAndIndexKeys> retval1 = delayedRows.remove(owner);
			retval.add(retval1);
			final List<List<Object>> retval2 = delayedRows2.remove(owner);
			retval.add(retval2);
			final Map<String, UpdateOperator> map = txDelayedMaps.get(tx.number());
			final String key = schema + "." + table + "~" + node;
			map.remove(key);
			if (map.size() == 0)
			{
				txDelayedMaps.remove(tx.number());
			}
		}
		return retval;
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

	private static boolean registerDelayed(final UpdateOperator caller, final String schema, final String table, final int node, final Transaction tx, final List<RIDAndIndexKeys> rows, final List<List<Object>> rows2)
	{
		synchronized (txDelayedMaps)
		{
			final String key = schema + "." + table + "~" + node;
			Map<String, UpdateOperator> map = txDelayedMaps.get(tx.number());
			if (map == null)
			{
				map = new HashMap<String, UpdateOperator>();
				map.put(key, caller);

				if (rows != null)
				{
					delayedRows.put(caller, rows);
				}

				if (rows2 != null)
				{
					delayedRows2.put(caller, rows2);
				}
				txDelayedMaps.put(tx.number(), map);
				return true;
			}

			final UpdateOperator owner = map.get(key);
			if (owner == null)
			{
				map.put(key, caller);
				if (rows != null)
				{
					delayedRows.put(caller, rows);
				}

				if (rows2 != null)
				{
					delayedRows2.put(caller, rows2);
				}
				return true;
			}

			try
			{
				final List<RIDAndIndexKeys> myDelayedRows = delayedRows.get(owner);
				if (myDelayedRows != null)
				{
					myDelayedRows.addAll(rows);
				}
				else
				{
					delayedRows.put(owner, rows);
				}

				final List<List<Object>> myDelayedRows2 = delayedRows2.get(owner);
				if (myDelayedRows2 != null)
				{
					myDelayedRows2.addAll(rows2);
				}
				else
				{
					delayedRows2.put(owner, rows2);
				}
			}
			catch (final NullPointerException e)
			{
				HRDBMSWorker.logger.debug("Owner = " + owner + ", map.get(key) = " + map.get(key) + ", delayedRows.get(owner) = " + delayedRows.get(owner));
			}
			return false;
		}
	}

	private static boolean registerDelayedCantOwn(final UpdateOperator caller, final String schema, final String table, final int node, final Transaction tx, final List<RIDAndIndexKeys> rows, final List<List<Object>> rows2)
	{
		synchronized (txDelayedMaps)
		{
			final String key = schema + "." + table + "~" + node;
			final Map<String, UpdateOperator> map = txDelayedMaps.get(tx.number());
			if (map == null)
			{
				return false;
			}

			final UpdateOperator owner = map.get(key);
			if (owner == null)
			{
				return false;
			}

			final List<RIDAndIndexKeys> myDelayedRows = delayedRows.get(owner);
			if (myDelayedRows != null)
			{
				myDelayedRows.addAll(rows);
			}
			else
			{
				delayedRows.put(owner, rows);
			}

			final List<List<Object>> myDelayedRows2 = delayedRows2.get(owner);
			if (myDelayedRows2 != null)
			{
				myDelayedRows2.addAll(rows2);
			}
			else
			{
				delayedRows2.put(owner, rows2);
			}
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
			throw new Exception("UpdateOperator only supports 1 child.");
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
	public UpdateOperator clone()
	{
		final UpdateOperator retval = new UpdateOperator(schema, table, cols, buildList, meta);
		retval.node = node;
		return retval;
	}

	@Override
	public void close() throws Exception
	{
		map = null;
		map2 = null;
		cols2Pos = null;
		cols2Types = null;
		pos2Col = null;
		cols = null;
		buildList = null;
		keys = null;
		types = null;
		orders = null;
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

	public Plan getPlan()
	{
		return plan;
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
			throw new Exception("An error occured during an update operation");
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
		throw new Exception("UpdateOperator does not support parents");
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
		throw new Exception("UpdateOperator does not support reset()");
	}

	@Override
	public void serialize(final OutputStream out, final IdentityHashMap<Object, Long> prev) throws Exception
	{
		throw new Exception("Tried to call serialize on update operator");
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
		this.plan = plan;
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
				IdentityHashMap<UpdateOperator, UpdateOperator> thisTx = notYetStarted.get(tx.number());
				if (thisTx == null)
				{
					thisTx = new IdentityHashMap<UpdateOperator, UpdateOperator>();
					notYetStarted.put(tx.number(), thisTx);
				}

				thisTx.put(this, this);
			}
		}

		int type = -1;
		PartitionMetaData spmd = null;
		List<String> indexes = null;
		try
		{
			child.start();
			indexes = MetaData.getIndexFileNamesForTable(schema, table, tx);
			keys = MetaData.getKeys(indexes, tx);
			types = MetaData.getTypes(indexes, tx);
			orders = MetaData.getOrders(indexes, tx);
			final Map<Integer, Integer> pos2Length = new HashMap<Integer, Integer>();
			cols2Pos = MetaData.getCols2PosForTable(schema, table, tx);
			pos2Col = MetaData.cols2PosFlip(cols2Pos);
			// new MetaData();
			cols2Types = MetaData.getCols2TypesForTable(schema, table, tx);
			spmd = new MetaData().getPartMeta(schema, table, tx);
			type = MetaData.getTypeForTable(schema, table, tx);
			for (final Map.Entry entry : cols2Types.entrySet())
			{
				if (entry.getValue().equals("CHAR"))
				{
					final int length = MetaData.getLengthForCharCol(schema, table, (String)entry.getKey(), tx);
					pos2Length.put(cols2Pos.get(entry.getKey()), length);
				}
			}

			final PartitionMetaData pmeta = new PartitionMetaData(schema, table, tx);

			final int numNodes = MetaData.numWorkerNodes;
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

					final RIDAndIndexKeys raik = new RIDAndIndexKeys(new RID(node, device, block, rec), indexKeys);
					map.multiPut(node, raik);

					final List<Object> row2 = new ArrayList<Object>();
					for (final String col : pos2Col.values())
					{
						boolean contains = false;
						int index = -1;
						final String col1 = col.substring(col.indexOf('.') + 1);
						int i = 0;
						for (final Column col2 : cols)
						{
							if (col2.getColumn().equals(col1))
							{
								contains = true;
								index = i;
							}

							i++;
						}

						if (!contains)
						{
							row2.add(row.get(child.getCols2Pos().get(col)));
						}
						else
						{
							String toGet = buildList.get(index);
							final Integer indx = child.getCols2Pos().get(toGet);
							if (indx != null)
							{
								row2.add(row.get(indx));
							}
							else
							{
								if (toGet.contains("."))
								{
									toGet = toGet.substring(toGet.indexOf('.') + 1);
								}

								for (final Map.Entry entry : child.getCols2Pos().entrySet())
								{
									String temp = (String)entry.getKey();
									if (temp.contains("."))
									{
										temp = temp.substring(temp.indexOf('.') + 1);
									}

									if (temp.equals(toGet))
									{
										row2.add(row.get((Integer)entry.getValue()));
										break;
									}
								}
							}
						}
					}

					cast(row2, pos2Col, cols2Types);
					for (final Map.Entry entry : pos2Length.entrySet())
					{
						if (((String)row2.get((Integer)entry.getKey())).length() > (Integer)entry.getValue())
						{
							num.set(Integer.MIN_VALUE);
							return;
						}
					}
					final List<Integer> nodes = MetaData.determineNode(schema, table, row2, tx, pmeta, cols2Pos, numNodes);
					for (final Integer n : nodes)
					{
						plan.addNode(n);
						map2.multiPut(n, row2);
					}
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.debug("", e);
					throw e;
				}
				num.incrementAndGet();
				if (map.size() > Integer.parseInt(HRDBMSWorker.getHParms().getProperty("max_neighbor_nodes")))
				{
					flush(indexes, spmd, cols2Pos, pos2Col, cols2Types, type);
					if (num.get() == Integer.MIN_VALUE)
					{
						done = true;
						return;
					}
				}
				else if (map.totalSize() > Integer.parseInt(HRDBMSWorker.getHParms().getProperty("max_batch")))
				{
					flush(indexes, spmd, cols2Pos, pos2Col, cols2Types, type);
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
					final IdentityHashMap<UpdateOperator, UpdateOperator> thisTx = notYetStarted.get(tx.number());
					thisTx.remove(this);
					if (thisTx.size() == 0)
					{
						notYetStarted.remove(tx.number());
					}
				}
			}

			HRDBMSWorker.logger.debug("", e);
			throw e;
		}

		if (ConnectionWorker.isDelayed(tx))
		{
			// notYetStarted.remove(tx.number(), this);
			synchronized (notYetStarted)
			{
				final IdentityHashMap<UpdateOperator, UpdateOperator> thisTx = notYetStarted.get(tx.number());
				thisTx.remove(this);
				if (thisTx.size() == 0)
				{
					notYetStarted.remove(tx.number());
				}
			}
		}

		if (!ConnectionWorker.isDelayed(tx) && map.totalSize() > 0)
		{
			flush(indexes, spmd, cols2Pos, pos2Col, cols2Types, type);
		}
		else if (ConnectionWorker.isDelayed(tx) && map.totalSize() > 0)
		{
			delayedFlush(indexes, spmd, cols2Pos, pos2Col, cols2Types, type);
		}

		done = true;
	}

	@Override
	public String toString()
	{
		return "UpdateOperator";
	}

	private void delayedFlush(final List<String> indexes, final PartitionMetaData spmd, final Map<String, Integer> cols2Pos, final Map<Integer, String> pos2Col, final Map<String, String> cols2Types, final int type) throws Exception
	{
		FlushThread thread = null;
		int node = -1;
		boolean realOwner = false;
		int realNode = -1;
		HashSet copy = new HashSet(map.getKeySet());
		for (final Object o : copy)
		{
			node = (Integer)o;
			if (node < 0)
			{
				throw new Exception("Delayed flush not allowed for system metadata");
			}

			final List<RIDAndIndexKeys> list = map.get(node);
			final List<List<Object>> list2 = map2.get(node);

			if (!realOwner)
			{
				final boolean owner = UpdateOperator.registerDelayed(this, schema, table, node, tx, list, list2);
				if (owner)
				{
					realOwner = true;
					realNode = node;
				}

				map.multiRemove(o);
				map2.multiRemove(node);
			}
			else
			{
				final boolean handled = UpdateOperator.registerDelayedCantOwn(this, schema, table, node, tx, list, list2);
				if (handled)
				{
					map.multiRemove(o);
					map2.multiRemove(node);
				}
			}
		}

		if (map2.size() > 0)
		{
			copy = new HashSet(map.getKeySet());
			for (final Object o : copy)
			{
				node = (Integer)o;
				final List<RIDAndIndexKeys> list = map.get(node);
				final List<List<Object>> list2 = map2.get(node);

				final boolean handled = UpdateOperator.registerDelayedCantOwn(this, schema, table, node, tx, list, list2);
				if (handled)
				{
					map.multiRemove(o);
					map2.multiRemove(node);
				}
			}
		}

		if (map.size() > 0 || map2.size() > 0)
		{
			flush(indexes, spmd, cols2Pos, pos2Col, cols2Types, type);
		}

		if (!realOwner)
		{
			return;
		}

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

		final List lists = UpdateOperator.deregister(this, schema, table, realNode, tx);
		final List<RIDAndIndexKeys> list = (List<RIDAndIndexKeys>)lists.get(0);
		final List<List<Object>> list2 = (List<List<Object>>)lists.get(1);
		thread = new FlushThread(list, indexes, node, list2, cols2Pos, spmd, pos2Col, cols2Types, type);
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
			num.set(Integer.MIN_VALUE);
		}
	}

	private void flush(final List<String> indexes, final PartitionMetaData spmd, final Map<String, Integer> cols2Pos, final Map<Integer, String> pos2Col, final Map<String, String> cols2Types, final int type) throws Exception
	{
		final List<FlushThread> threads = new ArrayList<FlushThread>();
		for (final Object o : map.getKeySet())
		{
			final int node = (Integer)o;
			final List<RIDAndIndexKeys> list = map.get(node);
			final List<List<Object>> list2 = map2.get(node);
			map2.multiRemove(node);
			if (node == -1)
			{
				final List<Integer> coords = MetaData.getCoordNodes();

				for (final Integer coord : coords)
				{
					threads.add(new FlushThread(list, indexes, coord, list2, cols2Pos, spmd, pos2Col, cols2Types, type));
				}
			}
			else
			{
				threads.add(new FlushThread(list, indexes, node, list2, cols2Pos, spmd, pos2Col, cols2Types, type));
			}
		}

		for (final FlushThread thread : threads)
		{
			thread.start();
		}

		final List<FlushThread2> threads2 = new ArrayList<FlushThread2>();
		for (final Object o : map2.getKeySet())
		{
			final int node = (Integer)o;
			final List<List<Object>> list = map2.get(node);
			if (node == -1)
			{
				final List<Integer> coords = MetaData.getCoordNodes();

				for (final Integer coord : coords)
				{
					threads2.add(new FlushThread2(list, indexes, coord, cols2Pos, spmd, pos2Col, cols2Types, type));
				}
			}
			else
			{
				threads2.add(new FlushThread2(list, indexes, node, cols2Pos, spmd, pos2Col, cols2Types, type));
			}
		}

		for (final FlushThread2 thread : threads2)
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

		for (final FlushThread2 thread : threads2)
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

		map2.clear();

		// TODO update global unique indexes here
	}

	private class FlushThread extends HRDBMSThread
	{
		private final List<RIDAndIndexKeys> list;
		private final List<String> indexes;
		private boolean ok = true;
		private final int node;
		private final List<List<Object>> list2;
		private final Map<String, Integer> cols2Pos;
		private final PartitionMetaData pmd;
		private final Map<String, String> cols2Types;
		private final int type;
		private final Map<Integer, String> pos2Col;

		public FlushThread(final List<RIDAndIndexKeys> list, final List<String> indexes, final int node, final List<List<Object>> list2, final Map<String, Integer> cols2Pos, final PartitionMetaData pmd, final Map<Integer, String> pos2Col, final Map<String, String> cols2Types, final int type)
		{
			if (list != null)
			{
				this.list = list;
			}
			else
			{
				this.list = new ArrayList<RIDAndIndexKeys>();
			}
			this.indexes = indexes;
			this.node = node;
			if (list2 != null)
			{
				this.list2 = list2;
			}
			else
			{
				this.list2 = new ArrayList<List<Object>>();
			}
			this.cols2Pos = cols2Pos;
			this.pmd = pmd;
			this.pos2Col = pos2Col;
			this.cols2Types = cols2Types;
			this.type = type;
		}

		public boolean getOK()
		{
			return ok;
		}

		@SuppressWarnings({ "rawtypes", "unchecked" })
		@Override
		public void run()
		{
			// send schema, table, tx, indexes, and list
			Socket sock = null;
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
				OutputStream out = sock.getOutputStream();
				out = new BufferedOutputStream(out);
				final byte[] outMsg = "UPDATE          ".getBytes(StandardCharsets.UTF_8);
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
				out.write(stringToBytes(pmd.getNGExp()));
				out.write(stringToBytes(pmd.getNExp()));
				out.write(stringToBytes(pmd.getDExp()));
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
				OperatorUtils.serializeALALO(new ArrayList(list2), out, prev);
				// objOut.writeObject(new ArrayList(list2));
				OperatorUtils.serializeStringIntHM(cols2Pos, out, prev);
				// objOut.writeObject(cols2Pos);
				// objOut.writeObject(pmd);
				OperatorUtils.serializeTM(pos2Col, out, prev);
				// objOut.writeObject(pos2Col);
				if (cols2Types == null)
				{
					HRDBMSWorker.logger.debug("UO FT is about to serialize a null cols2Types");
				}
				OperatorUtils.serializeStringHM(cols2Types, out, prev);
				// objOut.writeObject(cols2Types);
				// objOut.flush();
				out.flush();
				getConfirmation(sock);
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

	private class FlushThread2 extends HRDBMSThread
	{
		private final List<List<Object>> list;
		private final List<String> indexes;
		private boolean ok = true;
		private final int node;
		private final Map<String, Integer> cols2Pos;
		private final PartitionMetaData spmd;
		private final Map<Integer, String> pos2Col;
		private final Map<String, String> cols2Types;
		private final int type;

		public FlushThread2(final List<List<Object>> list, final List<String> indexes, final int node, final Map<String, Integer> cols2Pos, final PartitionMetaData spmd, final Map<Integer, String> pos2Col, final Map<String, String> cols2Types, final int type)
		{
			this.list = list;
			this.indexes = indexes;
			this.node = node;
			this.cols2Pos = cols2Pos;
			this.spmd = spmd;
			this.pos2Col = pos2Col;
			this.cols2Types = cols2Types;
			this.type = type;
		}

		public boolean getOK()
		{
			return ok;
		}

		@Override
		public void run()
		{
			// send schema, table, tx, indexes, list, and cols2Pos
			Socket sock = null;
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
				OutputStream out = sock.getOutputStream();
				out = new BufferedOutputStream(out);
				final byte[] outMsg = "INSERT          ".getBytes(StandardCharsets.UTF_8);
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
				out.write(stringToBytes(spmd.getNGExp()));
				out.write(stringToBytes(spmd.getNExp()));
				out.write(stringToBytes(spmd.getDExp()));
				final IdentityHashMap<Object, Long> prev = new IdentityHashMap<Object, Long>();
				OperatorUtils.serializeALS(indexes, out, prev);
				OperatorUtils.serializeALALO(new ArrayList(list), out, prev);
				OperatorUtils.serializeALALS(keys, out, prev);
				OperatorUtils.serializeALALS(types, out, prev);
				OperatorUtils.serializeALALB(orders, out, prev);
				OperatorUtils.serializeStringIntHM(cols2Pos, out, prev);
				OperatorUtils.serializeTM(pos2Col, out, prev);
				if (cols2Types == null)
				{
					HRDBMSWorker.logger.debug("UO FT2 is about to serialize a null cols2Types");
				}
				OperatorUtils.serializeStringHM(cols2Types, out, prev);
				// ObjectOutputStream objOut = new ObjectOutputStream(out);
				// objOut.writeObject(indexes);
				// objOut.writeObject(new ArrayList(list));
				// objOut.writeObject(keys);
				// objOut.writeObject(types);
				// objOut.writeObject(orders);
				// objOut.writeObject(cols2Pos);
				// objOut.writeObject(pos2Col);
				// objOut.writeObject(cols2Types);
				// objOut.flush();
				out.flush();
				getConfirmation(sock);
				// objOut.close();
				sock.close();
			}
			catch (final Exception e)
			{
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
				HRDBMSWorker.logger.debug("", e);
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
