package com.exascale.optimizer;

import java.io.BufferedOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.misc.DataEndMarker;
import com.exascale.misc.HJOMultiHashMap;
import com.exascale.tables.Plan;
import com.exascale.tables.Transaction;
import com.exascale.threads.ConnectionWorker;
import com.exascale.threads.HRDBMSThread;

public final class InsertOperator implements Operator, Serializable
{
	private static ConcurrentHashMap<String, InetSocketAddress> addrCache = new ConcurrentHashMap<String, InetSocketAddress>();
	private static Map<Long, Map<String, InsertOperator>> txDelayedMaps = new HashMap<Long, Map<String, InsertOperator>>();
	private static IdentityHashMap<InsertOperator, List<List<Object>>> delayedRows = new IdentityHashMap<InsertOperator, List<List<Object>>>();
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
	private HJOMultiHashMap map = new HJOMultiHashMap<Integer, List<Object>>();
	private Transaction tx;
	private transient String[] colTypes;

	public InsertOperator(final String schema, final String table, final MetaData meta)
	{
		this.schema = schema;
		this.table = table;
		this.meta = meta;
	}

	public static void wakeUpDelayed(final Transaction tx)
	{
		synchronized (txDelayedMaps)
		{
			final Map<String, InsertOperator> map = txDelayedMaps.get(tx.number());

			if (map == null)
			{
				return;
			}

			for (final InsertOperator io : map.values())
			{
				synchronized (io)
				{
					io.notify();
				}
			}
		}
	}

	private void cast(final List<Object> row, final Map<Integer, String> pos2Col, final Map<String, String> cols2Types)
	{
		int i = 0;
		final int size = pos2Col.size();
		while (i < size)
		{
			final String type = colTypes[i];
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

	private static List<List<Object>> deregister(final InsertOperator owner, final String schema, final String table, final int node, final Transaction tx)
	{
		synchronized (txDelayedMaps)
		{
			final List<List<Object>> retval = delayedRows.remove(owner);
			final Map<String, InsertOperator> map = txDelayedMaps.get(tx.number());
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

	private static boolean registerDelayed(final InsertOperator caller, final String schema, final String table, final int node, final Transaction tx, final List<List<Object>> rows)
	{
		synchronized (txDelayedMaps)
		{
			final String key = schema + "." + table + "~" + node;
			Map<String, InsertOperator> map = txDelayedMaps.get(tx.number());
			if (map == null)
			{
				map = new HashMap<String, InsertOperator>();
				map.put(key, caller);
				delayedRows.put(caller, rows);
				txDelayedMaps.put(tx.number(), map);
				return true;
			}

			final InsertOperator owner = map.get(key);
			if (owner == null)
			{
				map.put(key, caller);
				delayedRows.put(caller, rows);
				return true;
			}

			delayedRows.get(owner).addAll(rows);
			return false;
		}
	}

	private static boolean registerDelayedCantOwn(final InsertOperator caller, final String schema, final String table, final int node, final Transaction tx, final List<List<Object>> rows)
	{
		synchronized (txDelayedMaps)
		{
			final String key = schema + "." + table + "~" + node;
			final Map<String, InsertOperator> map = txDelayedMaps.get(tx.number());
			if (map == null)
			{
				return false;
			}

			final InsertOperator owner = map.get(key);
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
			throw new Exception("InsertOperator only supports 1 child.");
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
	public InsertOperator clone()
	{
		final InsertOperator retval = new InsertOperator(schema, table, meta);
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

		// HRDBMSWorker.logger.debug("IO is about to return: " + num.get());

		if (num.get() == Integer.MIN_VALUE)
		{
			throw new Exception("An error occured during an insert operation");
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
		throw new Exception("InsertOperator does not support parents");
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
		throw new Exception("InsertOperator does not support reset()");
	}

	@Override
	public void serialize(final OutputStream out, final IdentityHashMap<Object, Long> prev) throws Exception
	{
		throw new Exception("Tried to call serialize on insert operator");
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
		child.start();
		final List<String> indexes = MetaData.getIndexFileNamesForTable(schema, table, tx);
		final Map<Integer, Integer> pos2Length = new HashMap<Integer, Integer>();
		final Map<String, Integer> cols2Pos = MetaData.getCols2PosForTable(schema, table, tx);
		final Map<Integer, String> pos2Col = MetaData.cols2PosFlip(cols2Pos);
		// new MetaData();
		final Map<String, String> cols2Types = MetaData.getCols2TypesForTable(schema, table, tx);
		final int size = pos2Col.size();
		colTypes = new String[size];
		int i = 0;
		while (i < size)
		{
			colTypes[i] = cols2Types.get(pos2Col.get(i));
			i++;
		}
		final PartitionMetaData spmd = new MetaData().getPartMeta(schema, table, tx);
		final List<List<String>> keys = MetaData.getKeys(indexes, tx);
		final List<List<String>> types = MetaData.getTypes(indexes, tx);
		final List<List<Boolean>> orders = MetaData.getOrders(indexes, tx);
		final int type = MetaData.getTypeForTable(schema, table, tx);
		for (final Map.Entry entry : cols2Types.entrySet())
		{
			if (entry.getValue().equals("CHAR"))
			{
				final int length = MetaData.getLengthForCharCol(schema, table, (String)entry.getKey(), tx);
				pos2Length.put(cols2Pos.get(entry.getKey()), length);
			}
		}

		Object o = child.next(this);
		final PartitionMetaData pmeta = new PartitionMetaData(schema, table, tx);
		final int numNodes = MetaData.numWorkerNodes;
		MasterFlushThread mft = null;
		
		while (!(o instanceof DataEndMarker))
		{
			final List<Object> row = (List<Object>)o;
			cast(row, pos2Col, cols2Types);
			for (final Map.Entry entry : pos2Length.entrySet())
			{
				if (((String)row.get((Integer)entry.getKey())).length() > (Integer)entry.getValue())
				{
					num.set(Integer.MIN_VALUE);
					done = true;
					return;
				}
			}
			final List<Integer> nodes = MetaData.determineNode(schema, table, row, tx, pmeta, cols2Pos, numNodes);
			for (final Integer node : nodes)
			{
				plan.addNode(node);
				map.multiPut(node, row);
				num.incrementAndGet();
			}
			if (map.size() > Integer.parseInt(HRDBMSWorker.getHParms().getProperty("max_neighbor_nodes")))
			{
				if (mft != null)
				{
					mft.join();
					if (num.get() == Integer.MIN_VALUE)
					{
						done = true;
						return;
					}
				}
				
				mft = flush(indexes, cols2Pos, spmd, keys, types, orders, pos2Col, cols2Types, type);
				map = new HJOMultiHashMap<Integer, List<Object>>();
			}
			else if (map.totalSize() > Integer.parseInt(HRDBMSWorker.getHParms().getProperty("max_batch")))
			{
				if (mft != null)
				{
					mft.join();
					if (num.get() == Integer.MIN_VALUE)
					{
						done = true;
						return;
					}
				}
				
				mft = flush(indexes, cols2Pos, spmd, keys, types, orders, pos2Col, cols2Types, type);
				map = new HJOMultiHashMap<Integer, List<Object>>();
			}

			o = child.next(this);
		}

		if (!ConnectionWorker.isDelayed(tx) && map.totalSize() > 0)
		{
			if (mft != null)
			{
				mft.join();
				if (num.get() == Integer.MIN_VALUE)
				{
					done = true;
					return;
				}
			}
			
			mft = flush(indexes, cols2Pos, spmd, keys, types, orders, pos2Col, cols2Types, type);
			mft.join();
		}
		else if (ConnectionWorker.isDelayed(tx) && map.totalSize() > 0)
		{
			if (mft != null)
			{
				mft.join();
				if (num.get() == Integer.MIN_VALUE)
				{
					done = true;
					return;
				}
			}
			
			delayedFlush(indexes, cols2Pos, spmd, keys, types, orders, pos2Col, cols2Types, type);
		}

		done = true;
	}

	@Override
	public String toString()
	{
		return "InsertOperator";
	}

	private void delayedFlush(final List<String> indexes, final Map<String, Integer> cols2Pos, final PartitionMetaData spmd, final List<List<String>> keys, final List<List<String>> types, final List<List<Boolean>> orders, final Map<Integer, String> pos2Col, final Map<String, String> cols2Types, final int type)
	{
		FlushThread thread = null;
		int node = -1;
		boolean realOwner = false;
		int realNode = -1;
		final Set copy = new HashSet(map.getKeySet());
		for (final Object o : copy)
		{
			node = (Integer)o;
			final List<List<Object>> list = map.get(node);

			if (!realOwner)
			{
				final boolean owner = InsertOperator.registerDelayed(this, schema, table, node, tx, list);
				if (owner)
				{
					realOwner = true;
					realNode = node;
				}

				map.multiRemove(o);
			}
			else
			{
				final boolean handled = InsertOperator.registerDelayedCantOwn(this, schema, table, node, tx, list);
				if (handled)
				{
					map.multiRemove(o);
				}
			}
		}

		if (map.size() > 0)
		{
			while (true)
			{
				try
				{
					flush(indexes, cols2Pos, spmd, keys, types, orders, pos2Col, cols2Types, type).join();
					if (num.get() == Integer.MIN_VALUE)
					{
						done = true;
						return;
					}
					break;
				}
				catch(InterruptedException e)
				{}
			}
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

		final List<List<Object>> list = InsertOperator.deregister(this, schema, table, realNode, tx);
		thread = new FlushThread(list, indexes, realNode, cols2Pos, spmd, keys, types, orders, pos2Col, cols2Types, type);
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

	private MasterFlushThread flush(final List<String> indexes, final Map<String, Integer> cols2Pos, final PartitionMetaData spmd, final List<List<String>> keys, final List<List<String>> types, final List<List<Boolean>> orders, final Map<Integer, String> pos2Col, final Map<String, String> cols2Types, final int type)
	{
		MasterFlushThread mft = new MasterFlushThread(indexes, cols2Pos, spmd, keys, types, orders, pos2Col, cols2Types, type, map);
		mft.start();
		return mft;
	}
	
	private class MasterFlushThread extends HRDBMSThread
	{
		private List<String> indexes;
		private Map<String, Integer> cols2Pos;
		private PartitionMetaData spmd;
		private List<List<String>> keys;
		private List<List<String>> types;
		private List<List<Boolean>> orders;
		private Map<Integer, String> pos2Col;
		private Map<String, String> cols2Types;
		private int type;
		private HJOMultiHashMap localMap;
		
		public MasterFlushThread(final List<String> indexes, final Map<String, Integer> cols2Pos, final PartitionMetaData spmd, final List<List<String>> keys, final List<List<String>> types, final List<List<Boolean>> orders, final Map<Integer, String> pos2Col, final Map<String, String> cols2Types, final int type, HJOMultiHashMap localMap)
		{
			this.indexes = indexes;
			this.cols2Pos = cols2Pos;
			this.spmd = spmd;
			this.keys = keys;
			this.types = types;
			this.orders = orders;
			this.pos2Col = pos2Col;
			this.cols2Types = cols2Types;
			this.type = type;
			this.localMap = localMap;
		}
		
		public void run()
		{
			final List<FlushThread> threads = new ArrayList<FlushThread>();
			for (final Object o : localMap.getKeySet())
			{
				final int node = (Integer)o;
				final List<List<Object>> list = localMap.get(node);
				FlushThread thread = new FlushThread(list, indexes, node, cols2Pos, spmd, keys, types, orders, pos2Col, cols2Types, type);
				threads.add(thread);
				thread.start();
			}

			//for (final FlushThread thread : threads)
			//{
			//	thread.start();
			//}

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

			localMap.clear();
		}
	}

	private class FlushThread extends HRDBMSThread
	{
		private final List<List<Object>> list;
		private final List<String> indexes;
		private boolean ok = true;
		private final int node;
		private final Map<String, Integer> cols2Pos;
		private final PartitionMetaData spmd;
		private final List<List<String>> keys;
		private final List<List<String>> types;
		private final List<List<Boolean>> orders;
		private final Map<Integer, String> pos2Col;
		private final Map<String, String> cols2Types;
		private final int type;

		public FlushThread(final List<List<Object>> list, final List<String> indexes, final int node, final Map<String, Integer> cols2Pos, final PartitionMetaData spmd, final List<List<String>> keys, final List<List<String>> types, final List<List<Boolean>> orders, final Map<Integer, String> pos2Col, final Map<String, String> cols2Types, final int type)
		{
			this.list = list;
			this.indexes = indexes;
			this.node = node;
			this.cols2Pos = cols2Pos;
			this.spmd = spmd;
			this.keys = keys;
			this.orders = orders;
			this.types = types;
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
				InetSocketAddress inetAddr = addrCache.get(hostname);
				if (inetAddr == null)
				{
					inetAddr = new InetSocketAddress(hostname, Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
					addrCache.put(hostname, inetAddr);
				}
				sock.connect(inetAddr);
				final OutputStream out = new BufferedOutputStream(sock.getOutputStream());
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
				OperatorUtils.serializeALALO(list, out, prev);
				OperatorUtils.serializeALALS(keys, out, prev);
				OperatorUtils.serializeALALS(types, out, prev);
				OperatorUtils.serializeALALB(orders, out, prev);
				OperatorUtils.serializeStringIntHM(cols2Pos, out, prev);
				OperatorUtils.serializeTM(pos2Col, out, prev);
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
				out.close();
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
