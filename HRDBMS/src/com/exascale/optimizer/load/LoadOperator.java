package com.exascale.optimizer.load;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import com.exascale.misc.HrdbmsType;
import com.exascale.optimizer.*;
import com.exascale.optimizer.externalTable.ExternalTableScanOperator;
import com.exascale.threads.ThreadPoolThread;
import com.google.common.collect.Lists;
import org.antlr.v4.runtime.misc.Pair;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.misc.DataEndMarker;
import com.exascale.misc.LOMultiHashMap;
import com.exascale.misc.ScalableStampedRWLock;
import com.exascale.tables.Plan;
import com.exascale.tables.Transaction;

public final class LoadOperator implements Operator, Serializable
{
	static long MAX_QUEUED = Long.parseLong(HRDBMSWorker.getHParms().getProperty("max_queued_load_flush_threads"));
	static int PORT_NUMBER = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number"));
	static int MAX_BATCH = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("max_batch"));
	private static sun.misc.Unsafe unsafe;
	private transient MetaData meta;
	private HashMap<String, String> cols2Types;
	private HashMap<String, Integer> cols2Pos;
	private TreeMap<Integer, String> pos2Col;
	private Operator parent, child;
	private int node;
	private transient Plan plan;
	private String schema;
	private String table;
	private transient final AtomicLong num = new AtomicLong(0);
	private volatile transient boolean done = false;
	private transient final LOMultiHashMap map = new LOMultiHashMap<Long, ArrayList<Object>>();
	private Transaction tx;
	private boolean replace;
	private String delimiter;
	private String glob;
	protected boolean phase2Done = false;
	private transient final List<FlushThread> fThreads = new ArrayList<FlushThread>();
	private transient Map<Pair, AtomicInteger> waitTill;
	private volatile transient List<FlushThread> waitThreads;
	private transient ScalableStampedRWLock lock;

	static
	{
		try
		{
			final Field f = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
			f.setAccessible(true);
			unsafe = (sun.misc.Unsafe)f.get(null);
		}
		catch (final Exception e)
		{
			unsafe = null;
		}
	}

	/**TODO - get rid of unused parameters */
	public LoadOperator(final String schema, final String table, final boolean replace, final String delimiter, final String glob, final MetaData meta)
	{
		this.schema = schema;
		this.table = table;
		this.replace = replace;
		this.delimiter = delimiter;
		this.glob = glob;
		this.meta = meta;
	}

	@Override
	public void start() throws Exception
	{
		if (child != null) {
			child.start();
		} else {
			throw new UnsupportedOperationException("Please load using an external table.");
			// See prior commit for code to reintroduce this feature.
		}

		lock = new ScalableStampedRWLock();
		waitTill = new ConcurrentHashMap<>(64 * 16 * 1024, 0.75f, 64);
		waitThreads = new ArrayList<>();
		if (replace)
		{
			throw new UnsupportedOperationException("Replace option no longer supported");
			// See prior commit for MassDeleteOperator code to reintroduce this feature.
		}

		cols2Pos = MetaData.getCols2PosForTable(schema, table, tx);
		pos2Col = MetaData.cols2PosFlip(cols2Pos);
		cols2Types = MetaData.getCols2TypesForTable(schema, table, tx);
		final int type = MetaData.getTypeForTable(schema, table, tx);

		final ArrayList<String> indexes = MetaData.getIndexFileNamesForTable(schema, table, tx);
		final List<String> indexNames = new ArrayList<>();
		for (final String s : indexes)
		{
			final int start = s.indexOf('.') + 1;
			final int end = s.indexOf('.', start);
			indexNames.add(s.substring(start, end));
		}
		// DEBUG
		// if (indexes.size() == 0)
		// {
		// Exception e = new Exception();
		// HRDBMSWorker.logger.debug("No indexes found", e);
		// }
		// DEBUG
		final ArrayList<ArrayList<String>> keys = MetaData.getKeys(indexes, tx);
		// DEBUG
		// HRDBMSWorker.logger.debug("Keys = " + keys);
		// DEBUG
		final ArrayList<ArrayList<String>> types = MetaData.getTypes(indexes, tx);
		// DEBUG
		// HRDBMSWorker.logger.debug("Types = " + types);
		// DEBUG
		final ArrayList<ArrayList<Boolean>> orders = MetaData.getOrders(indexes, tx);
		// DEBUG
		// HRDBMSWorker.logger.debug("Orders = " + orders);
		// DEBUG

		final HashMap<Integer, Integer> pos2Length = new HashMap<>();
		for (final Map.Entry entry : cols2Types.entrySet())
		{
			if (entry.getValue().equals("CHAR"))
			{
				final int length = MetaData.getLengthForCharCol(schema, table, (String)entry.getKey(), tx);
				pos2Length.put(cols2Pos.get(entry.getKey()), length);
			}
		}

		final List<ReadThread> threads = new ArrayList<>();
		final PartitionMetaData spmd = new MetaData().getPartMeta(schema, table, tx);

		threads.add(new ExternalReadThread(this, pos2Length, indexes, spmd, keys, types, orders, type));
		threads.forEach(ThreadPoolThread::start);

		boolean allOK = true;
		for (final ReadThread thread : threads)
		{
			while (true)
			{
				try
				{
					thread.join();
					if (!thread.getOK())
					{
						allOK = false;
					}

					num.getAndAdd(thread.getNum());
					break;
				}
				catch (final InterruptedException e)
				{
				}
			}
		}

		if (!allOK)
		{
			num.set(Long.MIN_VALUE);
		}

		// Note that we cluster the data on loading, but don't keep it up to date with new inserts
		MetaData.cluster(schema, table, tx, pos2Col, cols2Types, type);

		for (final String index : indexNames)
		{
			MetaData.populateIndex(schema, index, table, tx, cols2Pos);
		}

		done = true;
	}

	@Override
	// @?Parallel
	public Object next(final Operator op) throws Exception
	{
		while (!done)
		{
			LockSupport.parkNanos(500);
		}

		if (num.get() == Long.MIN_VALUE)
		{
			throw new Exception("An error occurred during a load operation");
		}

		if (num.get() < 0)
		{
			return new DataEndMarker();
		}

		final long retval = num.get();
		num.set(-1);
		return new Integer((int)retval);
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
	public void close() throws Exception
	{
		child.close();
	}

	@Override
	public void reset() throws Exception
	{
		throw new Exception("LoadOperator does not support reset()");
	}

	@Override
	public void add(final Operator op) throws Exception
	{
		if(child != null) {
			throw new IllegalStateException("LoadOperator only supports 1 child");
		}

		child = op;
		child.registerParent(this);
	}

	@Override
	public ArrayList<Operator> children()
	{
		return child == null ? Lists.newArrayList() : Lists.newArrayList(child);
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

	public Plan getPlan()
	{
		return plan;
	}

	@Override
	public TreeMap<Integer, String> getPos2Col()
	{
		return pos2Col;
	}

	@Override
	public ArrayList<String> getReferences()
	{
		return Lists.newArrayList();  //TODO needed?
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
	public long numRecsReceived()
	{
		return num.get();
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
		if(parent != null) {
			throw new IllegalStateException("LoadOperator can only support one parent");
		}
		parent = op;
	}

	@Override
	public void removeChild(final Operator op)
	{
		child.removeParent(this);
		child = null;
	}

	@Override
	public void removeParent(final Operator op)
	{
		parent = null;
	}

	@Override
	public LoadOperator clone()
	{
		final LoadOperator retval = new LoadOperator(schema, table, replace, delimiter, glob, meta);
		retval.node = node;
		return retval;
	}

	@Override
	public void serialize(final OutputStream out, final IdentityHashMap<Object, Long> prev) throws Exception
	{
		final Long id = prev.get(this);
		if (id != null)
		{
			OperatorUtils.serializeReference(id, out);
			return;
		}

		OperatorUtils.writeType(HrdbmsType.LOADO, out);
		prev.put(this, OperatorUtils.writeID(out));
		child.serialize(out, prev);
		parent.serialize(out, prev);
		OperatorUtils.serializeStringHM(cols2Types, out, prev);
		OperatorUtils.serializeStringIntHM(cols2Pos, out, prev);
		OperatorUtils.serializeTM(pos2Col, out, prev);
		OperatorUtils.writeString(schema, out, prev);
		OperatorUtils.writeString(table, out, prev);
		OperatorUtils.writeString(delimiter, out, prev);
		OperatorUtils.writeString(glob, out, prev);
		OperatorUtils.writeLong(tx.number(), out);
		OperatorUtils.writeBool(replace, out);
		OperatorUtils.writeInt(node, out);
		OperatorUtils.writeBool(phase2Done, out);
	}

	public static LoadOperator deserialize(final InputStream in, final HashMap<Long, Object> prev) throws Exception
	{
		final LoadOperator value = (LoadOperator)unsafe.allocateInstance(LoadOperator.class);
		prev.put(OperatorUtils.readLong(in), value);
		value.child = OperatorUtils.deserializeOperator(in, prev);
		value.parent = OperatorUtils.deserializeOperator(in, prev);
		value.cols2Types = OperatorUtils.deserializeStringHM(in, prev);
		value.cols2Pos = OperatorUtils.deserializeStringIntHM(in, prev);
		value.pos2Col = OperatorUtils.deserializeTM(in, prev);
		value.schema = OperatorUtils.readString(in, prev);
		value.table = OperatorUtils.readString(in, prev);
		value.delimiter = OperatorUtils.readString(in, prev);
		value.glob = OperatorUtils.readString(in, prev);
		value.tx = new Transaction(OperatorUtils.readLong(in));
		value.replace = OperatorUtils.readBool(in);
		value.node = OperatorUtils.readInt(in);
		value.phase2Done = OperatorUtils.readBool(in);
		value.meta = new MetaData();
		return value;
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
	public String toString()
	{
		return "LoadOperator";
	}

	public void phase2Done() { phase2Done = true; }
	public boolean isPhase2Done() { return phase2Done; }

	// Note that all the Thread classes in this package were once inner classes of LoadOperator.  They were broken out
	// into top-level classes to improve maintainability.  But there's still weird interplay between all the classes.
	// Hence, these getters and setters:
	Transaction getTransaction() { return tx; }

	Map<Pair, AtomicInteger> getWaitTill() { return waitTill; }

	ScalableStampedRWLock getLock() { return lock; }

	LOMultiHashMap getMap() { return map; }

	List<FlushThread> getWaitThreads() { return waitThreads; }

	void setWaitThreads(List<FlushThread> waitThreads) { this.waitThreads = waitThreads; }

	List<FlushThread> getFlushThreads() { return fThreads; }

	String getDelimiter() { return delimiter; }

	ExternalTableScanOperator getChild() { return child == null ? null : (ExternalTableScanOperator) child; }
}