package com.exascale.optimizer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Vector;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.managers.ResourceManager;
import com.exascale.misc.BloomFilter;
import com.exascale.misc.BufferedFileChannel;
import com.exascale.misc.BufferedLinkedBlockingQueue;
import com.exascale.misc.DataEndMarker;
import com.exascale.misc.HJOMultiHashMap;
import com.exascale.misc.MurmurHash;
import com.exascale.misc.MyDate;
import com.exascale.misc.MySimpleDateFormat;
import com.exascale.misc.ScalableStampedReentrantRWLock;
import com.exascale.misc.VHJOMultiHashMap;
import com.exascale.tables.Plan;
import com.exascale.tables.Schema;
import com.exascale.threads.HRDBMSThread;
import com.exascale.threads.TempThread;
import com.exascale.threads.ThreadPoolThread;

public final class HashJoinOperator extends JoinOperator implements Serializable
{
	private static sun.misc.Unsafe unsafe;
	private static AtomicInteger numHJO = new AtomicInteger(0);
	private static int SHIFT;

	static
	{
		try
		{
			final Field f = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
			f.setAccessible(true);
			unsafe = (sun.misc.Unsafe)f.get(null);
			SHIFT = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("hjo_bucket_size_shift"));
			HRDBMSWorker.logger.debug("HJO SHIFT is " + SHIFT);
		}
		catch (Exception e)
		{
			unsafe = null;
		}
	}

	private ArrayList<Operator> children = new ArrayList<Operator>(2);

	private Operator parent;
	private HashMap<String, String> cols2Types;
	private HashMap<String, Integer> cols2Pos;
	private TreeMap<Integer, String> pos2Col;
	private MetaData meta;
	private transient volatile BufferedLinkedBlockingQueue outBuffer;
	private transient int NUM_RT_THREADS;
	private transient int NUM_PTHREADS;
	// private final AtomicLong outCount = new AtomicLong(0);
	private transient volatile boolean readersDone;
	private ArrayList<String> lefts = new ArrayList<String>();
	private ArrayList<String> rights = new ArrayList<String>();
	private transient volatile VHJOMultiHashMap<Long, byte[]> buckets;
	private transient ReentrantLock bucketsLock;
	private CNFFilter cnfFilters;
	private HashSet<HashMap<Filter, Filter>> f;
	private int childPos = -1;
	// private final AtomicLong inCount = new AtomicLong(0);
	// private final AtomicLong leftCount = new AtomicLong(0);
	private int node;
	private boolean indexAccess = false;
	private ArrayList<Index> dynamicIndexes;
	private transient ArrayList<ArrayList<Object>> queuedRows;
	private transient MySimpleDateFormat sdf;
	private int rightChildCard = 16;
	private int leftChildCard = 16;
	private boolean cardSet = false;
	private transient Vector<Operator> clones;
	private transient Vector<AtomicBoolean> lockVector;
	private transient ArrayList<String> externalFiles;
	private transient Boolean semi = null;
	private transient Boolean anti = null;
	private transient volatile BloomFilter bf = null;
	private transient volatile BloomFilter bf2 = null;
	private transient AtomicLong received;
	private transient volatile boolean demReceived;
	private long txnum;
	
	public void setTXNum(long txnum)
	{
		this.txnum = txnum;
	}

	public HashJoinOperator(String left, String right, MetaData meta) throws Exception
	{
		this.meta = meta;
		try
		{
			this.addFilter(left, right);
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.error("", e);
			throw e;
		}
		received = new AtomicLong(0);
	}

	private HashJoinOperator(HashSet<HashMap<Filter, Filter>> f, ArrayList<String> lefts, ArrayList<String> rights, MetaData meta)
	{
		this.meta = meta;
		this.f = f;
		this.lefts = lefts;
		this.rights = rights;
		received = new AtomicLong(0);
	}

	public static HashJoinOperator deserialize(InputStream in, HashMap<Long, Object> prev) throws Exception
	{
		HashJoinOperator value = (HashJoinOperator)unsafe.allocateInstance(HashJoinOperator.class);
		prev.put(OperatorUtils.readLong(in), value);
		value.children = OperatorUtils.deserializeALOp(in, prev);
		value.parent = OperatorUtils.deserializeOperator(in, prev);
		value.cols2Types = OperatorUtils.deserializeStringHM(in, prev);
		value.cols2Pos = OperatorUtils.deserializeStringIntHM(in, prev);
		value.pos2Col = OperatorUtils.deserializeTM(in, prev);
		value.meta = new MetaData();
		value.lefts = OperatorUtils.deserializeALS(in, prev);
		value.rights = OperatorUtils.deserializeALS(in, prev);
		value.cnfFilters = OperatorUtils.deserializeCNF(in, prev);
		value.f = OperatorUtils.deserializeHSHM(in, prev);
		value.childPos = OperatorUtils.readInt(in);
		value.node = OperatorUtils.readInt(in);
		value.indexAccess = OperatorUtils.readBool(in);
		value.dynamicIndexes = OperatorUtils.deserializeALIndx(in, prev);
		value.rightChildCard = OperatorUtils.readInt(in);
		value.cardSet = OperatorUtils.readBool(in);
		value.received = new AtomicLong(0);
		value.demReceived = false;
		value.leftChildCard = OperatorUtils.readInt(in);
		value.txnum = OperatorUtils.readLong(in);
		return value;
	}

	private static long hash(Object key) throws Exception
	{
		long eHash;
		if (key == null)
		{
			eHash = 0;
		}
		else
		{
			if (key instanceof ArrayList)
			{
				byte[] data = toBytes(key);
				eHash = MurmurHash.hash64(data, data.length);
			}
			else
			{
				byte[] data = key.toString().getBytes(StandardCharsets.UTF_8);
				eHash = MurmurHash.hash64(data, data.length);
			}
		}

		return eHash;
	}

	private static final byte[] toBytes(Object v) throws Exception
	{
		ArrayList<byte[]> bytes = null;
		ArrayList<Object> val;
		if (v instanceof ArrayList)
		{
			val = (ArrayList<Object>)v;
		}
		else
		{
			final byte[] retval = new byte[9];
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

		int size = 0;
		final byte[] header = new byte[val.size()];
		int i = 0;
		int z = 0;
		int limit = val.size();
		// for (final Object o : val)
		while (z < limit)
		{
			Object o = val.get(z++);
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
				size += 4;
			}
			else if (o instanceof String)
			{
				header[i] = (byte)4;
				byte[] b = ((String)o).getBytes(StandardCharsets.UTF_8);
				size += (4 + b.length);

				if (bytes == null)
				{
					bytes = new ArrayList<byte[]>();
					bytes.add(b);
				}
				else
				{
					bytes.add(b);
				}
			}
			// else if (o instanceof AtomicLong)
			// {
			// header[i] = (byte)6;
			// size += 8;
			// }
			// else if (o instanceof AtomicBigDecimal)
			// {
			// header[i] = (byte)7;
			// size += 8;
			// }
			else if (o instanceof ArrayList)
			{
				if (((ArrayList)o).size() != 0)
				{
					Exception e = new Exception("Non-zero size ArrayList in toBytes()");
					HRDBMSWorker.logger.error("Non-zero size ArrayList in toBytes()", e);
					throw e;
				}
				header[i] = (byte)8;
			}
			else
			{
				HRDBMSWorker.logger.error("Unknown type " + o.getClass() + " in toBytes()");
				HRDBMSWorker.logger.error(o);
				throw new Exception("Unknown type " + o.getClass() + " in toBytes()");
			}

			i++;
		}

		final byte[] retval = new byte[size];
		// System.out.println("In toBytes(), row has " + val.size() +
		// " columns, object occupies " + size + " bytes");
		// System.arraycopy(header, 0, retval, 0, header.length);
		i = 0;
		final ByteBuffer retvalBB = ByteBuffer.wrap(retval);
		// retvalBB.putInt(size - 4);
		// retvalBB.putInt(val.size());
		// retvalBB.position(header.length);
		int x = 0;
		z = 0;
		limit = val.size();
		// for (final Object o : val)
		while (z < limit)
		{
			Object o = val.get(z++);
			if (header[i] == 0)
			{
				retvalBB.putLong((Long)o);
			}
			else if (header[i] == 1)
			{
				retvalBB.putInt((Integer)o);
			}
			else if (header[i] == 2)
			{
				retvalBB.putDouble((Double)o);
			}
			else if (header[i] == 3)
			{
				retvalBB.putInt(((MyDate)o).getTime());
			}
			else if (header[i] == 4)
			{
				byte[] temp = bytes.get(x);
				x++;
				retvalBB.putInt(temp.length);
				retvalBB.put(temp);
			}
			// else if (retval[i] == 6)
			// {
			// retvalBB.putLong(((AtomicLong)o).get());
			// }
			// else if (retval[i] == 7)
			// {
			// retvalBB.putDouble(((AtomicBigDecimal)o).get().doubleValue());
			// }
			else if (header[i] == 8)
			{
			}

			i++;
		}

		return retval;
	}

	@Override
	public void add(Operator op) throws Exception
	{
		if (children.size() < 2)
		{
			if (childPos == -1)
			{
				children.add(op);
			}
			else
			{
				children.add(childPos, op);
				childPos = -1;
			}
			op.registerParent(this);

			if (children.size() == 2 && children.get(0).getCols2Types() != null && children.get(1).getCols2Types() != null)
			{
				cols2Types = (HashMap<String, String>)children.get(0).getCols2Types().clone();
				cols2Pos = (HashMap<String, Integer>)children.get(0).getCols2Pos().clone();
				pos2Col = (TreeMap<Integer, String>)children.get(0).getPos2Col().clone();

				cols2Types.putAll(children.get(1).getCols2Types());
				for (final Map.Entry entry : children.get(1).getPos2Col().entrySet())
				{
					cols2Pos.put((String)entry.getValue(), cols2Pos.size());
					pos2Col.put(pos2Col.size(), (String)entry.getValue());
				}

				cnfFilters = new CNFFilter(f, meta, cols2Pos, null, this);
				int i = 0;
				final int size = lefts.size();
				while (i < size)
				{
					// swap left/right if needed
					final String left = lefts.get(i);
					if (!children.get(0).getCols2Pos().containsKey(left))
					{
						final String right = rights.get(i);
						lefts.remove(left);
						rights.remove(right);
						lefts.add(i, right);
						rights.add(i, left);
					}

					i++;
				}
			}
		}
		else
		{
			throw new Exception("HashJoinOperator only supports 2 children");
		}
	}

	public void addFilter(String left, String right) throws Exception
	{
		if (f == null)
		{
			f = new HashSet<HashMap<Filter, Filter>>();
			final HashMap<Filter, Filter> map = new HashMap<Filter, Filter>();
			map.put(new Filter(left, "E", right), new Filter(left, "E", right));
			f.add(map);
		}
		else
		{
			final HashMap<Filter, Filter> map = new HashMap<Filter, Filter>();
			map.put(new Filter(left, "E", right), new Filter(left, "E", right));
			f.add(map);
		}
		lefts.add(left);
		rights.add(right);
	}

	@Override
	public void addJoinCondition(ArrayList<Filter> filters) throws UnsupportedOperationException
	{
		throw new UnsupportedOperationException("addJoinCondition(ArrayList<Filter>) is not supported by HashJoinOperator");
	}

	@Override
	public void addJoinCondition(String left, String right) throws Exception
	{
		try
		{
			addFilter(left, right);
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.error("", e);
			throw e;
		}
	}

	@Override
	public ArrayList<Operator> children()
	{
		return children;
	}

	@Override
	public HashJoinOperator clone()
	{
		final HashJoinOperator retval = new HashJoinOperator(f, lefts, rights, meta);
		retval.node = node;
		retval.indexAccess = indexAccess;
		retval.dynamicIndexes = dynamicIndexes;
		retval.rightChildCard = rightChildCard;
		retval.leftChildCard = leftChildCard;
		retval.cardSet = cardSet;
		retval.txnum = txnum;
		return retval;
	}

	@Override
	public void close() throws Exception
	{
		for (final Operator child : children)
		{
			child.close();
		}

		for (final Operator o : clones)
		{
			o.close();
		}

		if (outBuffer != null)
		{
			outBuffer.close();
		}

		lefts = null;
		rights = null;
		cnfFilters = null;
		f = null;
		dynamicIndexes = null;
		queuedRows = null;
		clones = null;
		lockVector = null;
		cols2Pos = null;
		cols2Types = null;
		pos2Col = null;
	}

	@Override
	public int getChildPos()
	{
		return childPos;
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

	public HashSet<HashMap<Filter, Filter>> getHSHM() throws Exception
	{
		if (f != null)
		{
			return getHSHMFilter();
		}

		final HashSet<HashMap<Filter, Filter>> retval = new HashSet<HashMap<Filter, Filter>>();
		int i = 0;
		for (final String col : rights)
		{
			Filter filter = null;
			try
			{
				filter = new Filter(lefts.get(i), "E", col);
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				throw e;
			}
			final HashMap<Filter, Filter> hm = new HashMap<Filter, Filter>();
			hm.put(filter, filter);
			retval.add(hm);
			i++;
		}

		return retval;
	}

	@Override
	public HashSet<HashMap<Filter, Filter>> getHSHMFilter()
	{
		return f;
	}

	@Override
	public boolean getIndexAccess()
	{
		return indexAccess;
	}

	@Override
	public ArrayList<String> getJoinForChild(Operator op)
	{
		if (op.getCols2Pos().keySet().containsAll(lefts))
		{
			return new ArrayList<String>(lefts);
		}
		else
		{
			return new ArrayList<String>(rights);
		}
	}

	public ArrayList<String> getLefts()
	{
		return lefts;
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
		final ArrayList<String> retval = new ArrayList<String>(lefts);
		retval.addAll(rights);
		return retval;
	}

	public ArrayList<String> getRights()
	{
		return rights;
	}

	@Override
	public Object next(Operator op) throws Exception
	{
		if (indexAccess)
		{
			synchronized (queuedRows)
			{
				if (queuedRows.size() > 0)
				{
					Object obj = queuedRows.remove(0);
					if (obj instanceof Exception)
					{
						throw (Exception)obj;
					}

					return obj;
				}
			}

			while (true)
			{
				final Object o = children.get(0).next(this);
				if (o instanceof DataEndMarker)
				{
					return o;
				}

				if (o instanceof Exception)
				{
					throw (Exception)o;
				}

				final ArrayList<Filter> dynamics = new ArrayList<Filter>(rights.size());
				int i = 0;
				for (final String right : rights)
				{
					final Object leftVal = ((ArrayList<Object>)o).get(children.get(0).getCols2Pos().get(lefts.get(i)));
					String leftString = null;
					if (leftVal instanceof Integer || leftVal instanceof Long || leftVal instanceof Double)
					{
						leftString = leftVal.toString();
					}
					else if (leftVal instanceof String)
					{
						leftString = "'" + leftVal + "'";
					}
					else if (leftVal instanceof MyDate)
					{
						leftString = sdf.format(leftVal);
					}
					final Filter f = new Filter(leftString, "E", right);
					dynamics.add(f);
					i++;
				}

				Operator clone = null;

				clone = getClone();
				synchronized (clone)
				{
					clone.reset();
					for (final Index index : dynamicIndexes(children.get(1), clone.children().get(0)))
					{
						index.setDelayedConditions(deepClone(dynamics));
					}

					Object o2 = clone.next(this);

					while (!(o2 instanceof DataEndMarker))
					{
						final ArrayList<Object> out = new ArrayList<Object>(((ArrayList<Object>)o).size() + ((ArrayList<Object>)o2).size());
						out.addAll((ArrayList<Object>)o);
						out.addAll((ArrayList<Object>)o2);
						synchronized (queuedRows)
						{
							queuedRows.add(out);
						}
						o2 = clone.next(this);
					}

					freeClone(clone);
				}
				synchronized (queuedRows)
				{
					if (queuedRows.size() > 0)
					{
						return queuedRows.remove(0);
					}
				}
			}
		}

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

		if (o instanceof Exception)
		{
			throw (Exception)o;
		}
		return o;
	}

	@Override
	public void nextAll(Operator op) throws Exception
	{
		children.get(0).nextAll(op);
		children.get(1).nextAll(op);
		Object o = next(op);
		while (!(o instanceof DataEndMarker) && !(o instanceof Exception))
		{
			o = next(op);
		}
	}

	@Override
	public long numRecsReceived()
	{
		return received.get();
	}

	@Override
	public Operator parent()
	{
		return parent;
	}

	@Override
	public boolean receivedDEM()
	{
		return demReceived;
	}

	@Override
	public void registerParent(Operator op) throws Exception
	{
		if (parent == null)
		{
			parent = op;
		}
		else
		{
			throw new Exception("HashJoinOperator can only have 1 parent.");
		}
	}

	@Override
	public void removeChild(Operator op)
	{
		childPos = children.indexOf(op);
		if (childPos == -1)
		{
			HRDBMSWorker.logger.error("Child doesn't exist!");
			HRDBMSWorker.logger.error("I am " + this.toString());
			HRDBMSWorker.logger.error("Children is " + children);
			HRDBMSWorker.logger.error("Trying to remove " + op);
		}
		children.remove(op);
		op.removeParent(this);
	}

	@Override
	public void removeParent(Operator op)
	{
		parent = null;
	}

	@Override
	public void reset()
	{
		HRDBMSWorker.logger.error("HashJoinOperator cannot be reset");
	}

	public void reverseUpdateReferences(ArrayList<String> references, int pos)
	{
		if (pos == 0)
		{
			int i = 0;
			for (final String rName : rights)
			{
				final ArrayList<String> temp = new ArrayList<String>(1);
				temp.add(rName);
				if (references.removeAll(temp))
				{
					final String lName = lefts.get(i);
					references.add(lName);
				}

				i++;
			}
		}
		else
		{
			int i = 0;
			for (final String lName : lefts)
			{
				final ArrayList<String> temp = new ArrayList<String>(1);
				temp.add(lName);
				if (references.removeAll(temp))
				{
					final String rName = rights.get(i);
					references.add(rName);
				}

				i++;
			}
		}
	}

	public SelectOperator reverseUpdateSelectOperator(SelectOperator op, int pos)
	{
		final SelectOperator retval = op.clone();
		if (pos == 0)
		{
			for (final Filter filter : retval.getFilter())
			{
				if (filter.leftIsColumn() && rights.contains(filter.leftColumn()))
				{
					filter.updateLeftColumn(getLeftForRight(filter.leftColumn()));
				}

				if (filter.rightIsColumn() && rights.contains(filter.rightColumn()))
				{
					filter.updateRightColumn(getLeftForRight(filter.rightColumn()));
				}
			}
		}
		else
		{
			for (final Filter filter : op.getFilter())
			{
				if (filter.leftIsColumn() && lefts.contains(filter.leftColumn()))
				{
					filter.updateLeftColumn(getRightForLeft(filter.leftColumn()));
				}

				if (filter.rightIsColumn() && lefts.contains(filter.rightColumn()))
				{
					filter.updateRightColumn(getRightForLeft(filter.rightColumn()));
				}
			}
		}

		retval.updateReferences();
		return retval;
	}

	@Override
	public void serialize(OutputStream out, IdentityHashMap<Object, Long> prev) throws Exception
	{
		Long id = prev.get(this);
		if (id != null)
		{
			OperatorUtils.serializeReference(id, out);
			return;
		}

		OperatorUtils.writeType(60, out);
		prev.put(this, OperatorUtils.writeID(out));
		OperatorUtils.serializeALOp(children, out, prev);
		parent.serialize(out, prev);
		OperatorUtils.serializeStringHM(cols2Types, out, prev);
		OperatorUtils.serializeStringIntHM(cols2Pos, out, prev);
		OperatorUtils.serializeTM(pos2Col, out, prev);
		// create new meta on receive side
		OperatorUtils.serializeALS(lefts, out, prev);
		OperatorUtils.serializeALS(rights, out, prev);
		OperatorUtils.serializeCNF(cnfFilters, out, prev);
		OperatorUtils.serializeHSHM(f, out, prev);
		OperatorUtils.writeInt(childPos, out);
		OperatorUtils.writeInt(node, out);
		OperatorUtils.writeBool(indexAccess, out);
		OperatorUtils.serializeALIndx(dynamicIndexes, out, prev);
		OperatorUtils.writeInt(rightChildCard, out);
		OperatorUtils.writeBool(cardSet, out);
		OperatorUtils.writeInt(leftChildCard, out);
		OperatorUtils.writeLong(txnum, out);
	}

	public void setAnti()
	{
		anti = new Boolean(true);
	}

	@Override
	public void setChildPos(int pos)
	{
		childPos = pos;
	}

	public void setCNF(HashSet<HashMap<Filter, Filter>> hshm) throws Exception
	{
		cnfFilters = new CNFFilter(hshm, meta, cols2Pos, null, this);
	}

	public void setDynamicIndex(ArrayList<Index> indexes)
	{
		indexAccess = true;
		this.dynamicIndexes = indexes;
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

	public boolean setRightChildCard(int card, int card2)
	{
		if (cardSet)
		{
			return false;
		}

		cardSet = true;
		rightChildCard = card;
		leftChildCard = card2;
		return true;
	}

	public void setSemi()
	{
		semi = new Boolean(true);
	}

	@Override
	public void start() throws Exception
	{
		HRDBMSWorker.logger.debug("Starting HJO(" + leftChildCard + ", " + rightChildCard + ")");
		NUM_RT_THREADS = ResourceManager.cpus;
		NUM_PTHREADS = ResourceManager.cpus;
		readersDone = false;
		bucketsLock = new ReentrantLock();
		queuedRows = new ArrayList<ArrayList<Object>>();
		sdf = new MySimpleDateFormat("yyyy-MM-dd");
		clones = new Vector<Operator>();
		lockVector = new Vector<AtomicBoolean>();

		if (!indexAccess)
		{
			for (final Operator child : children)
			{
				child.start();
				// System.out.println("cols2Pos = " + child.getCols2Pos());
			}

			outBuffer = new BufferedLinkedBlockingQueue(ResourceManager.QUEUE_SIZE);

			if (ResourceManager.criticalMem())
			{
				new ExternalThread().start();
			}
			else if (rightChildCard > ResourceManager.QUEUE_SIZE * Double.parseDouble(HRDBMSWorker.getHParms().getProperty("hash_external_factor")))
			{
				HRDBMSWorker.logger.debug("External HJO factor: " + (rightChildCard / (ResourceManager.QUEUE_SIZE * Double.parseDouble(HRDBMSWorker.getHParms().getProperty("hash_external_factor")))));
				// double percentInMem = ResourceManager.QUEUE_SIZE *
				// Double.parseDouble(HRDBMSWorker.getHParms().getProperty("hash_external_factor"))
				// / rightChildCard;
				// percentInMem = percentInMem / 8;
				double percentInMem = 0;
				new ExternalThread(percentInMem).start();
			}
			else
			{
				buckets = new VHJOMultiHashMap<Long, byte[]>();
				new InitThread().start();
			}
		}
		else
		{
			HRDBMSWorker.logger.debug("HashJoinOperator is started with index access");
			for (final Operator child : children)
			{
				child.start();
				// System.out.println("cols2Pos = " + child.getCols2Pos());
			}

			outBuffer = new BufferedLinkedBlockingQueue(ResourceManager.QUEUE_SIZE);
		}

	}

	@Override
	public String toString()
	{
		return "HashJoinOperator: " + lefts.toString() + "," + rights.toString();
	}

	private Operator clone(Operator op)
	{
		final Operator clone = op.clone();
		int i = 0;
		for (final Operator o : op.children())
		{
			try
			{
				clone.add(clone(o));
				clone.setChildPos(op.getChildPos());
				if (o instanceof TableScanOperator)
				{
					final CNFFilter cnf = ((TableScanOperator)o).getCNFForParent(op);
					if (cnf != null)
					{
						final Operator child = clone.children().get(i);
						((TableScanOperator)child).setCNFForParent(clone, cnf);
					}
				}

				if (op instanceof TableScanOperator)
				{
					final Operator child = clone.children().get(i);
					int device = -1;

					for (final Map.Entry entry : (((TableScanOperator)op).device2Child).entrySet())
					{
						if (entry.getValue() == o)
						{
							device = (Integer)entry.getKey();
						}
					}
					((TableScanOperator)clone).setChildForDevice(device, child);
				}
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				return null;
			}

			i++;
		}

		return clone;
	}

	private ArrayList<FileChannel> createChannels(ArrayList<RandomAccessFile> files)
	{
		ArrayList<FileChannel> retval = new ArrayList<FileChannel>(files.size());
		for (RandomAccessFile raf : files)
		{
			retval.add(raf.getChannel());
		}

		return retval;
	}

	private ArrayList<RandomAccessFile> createFiles(ArrayList<String> fns) throws Exception
	{
		ArrayList<RandomAccessFile> retval = new ArrayList<RandomAccessFile>(fns.size());
		for (String fn : fns)
		{
			while (true)
			{
				try
				{
					RandomAccessFile raf = new RandomAccessFile(fn, "rw");
					retval.add(raf);
					break;
				}
				catch (FileNotFoundException e)
				{
					ResourceManager.panic = true;
					try
					{
						Thread.sleep(Integer.parseInt(HRDBMSWorker.getHParms().getProperty("rm_sleep_time_ms")) / 2);
					}
					catch (Exception f)
					{
					}
				}
			}
		}

		return retval;
	}

	private ArrayList<String> createFNs(int num, int extra)
	{
		ArrayList<String> retval = new ArrayList<String>(num);
		int i = 0;
		while (i < num)
		{
			String fn = ResourceManager.TEMP_DIRS.get(i % ResourceManager.TEMP_DIRS.size()) + this.hashCode() + "" + System.currentTimeMillis() + i + "_" + extra + ".exthash";
			retval.add(fn);
			i++;
		}

		return retval;
	}

	private ArrayList<Filter> deepClone(ArrayList<Filter> in)
	{
		final ArrayList<Filter> out = new ArrayList<Filter>();
		for (final Filter f : in)
		{
			out.add(f.clone());
		}

		return out;
	}

	private ArrayList<Index> dynamicIndexes(Operator model, Operator actual)
	{
		final ArrayList<Index> retval = new ArrayList<Index>(dynamicIndexes.size());
		if (model instanceof IndexOperator)
		{
			if (dynamicIndexes.contains(((IndexOperator)model).index))
			{
				retval.add(((IndexOperator)actual).index);
			}
		}
		else
		{
			int i = 0;
			for (final Operator o : model.children())
			{
				retval.addAll(dynamicIndexes(o, actual.children().get(i)));
				i++;
			}
		}

		return retval;
	}

	private void external(double percentInMem)
	{
		numHJO.incrementAndGet();
		try
		{
			// int numBins = (int)((((((long)leftChildCard)
			// +((long)rightChildCard)) * 1.0 / 755000000.0) * 76800000.0) /
			// (ResourceManager.QUEUE_SIZE *
			// Double.parseDouble(HRDBMSWorker.getHParms().getProperty("external_factor"))));
			// numBins *= numHJO.get();
			// if (numHJO.get() != 1)
			// {
			// numBins *= 2;
			// }

			// if (numBins < 2)
			// {
			// numBins = 2;
			// }

			// if (numBins > 8192)
			// {
			// numBins = 8192;
			// }
			// HRDBMSWorker.logger.debug("Using numBins = " + numBins);
			int numBins = (int)(leftChildCard / 100000);
			if (numBins < 2)
			{
				numBins = 2;
			}
			
			if (numBins > 1600)
			{
				numBins = 1600;
			}
			
			int inMemBins = (int)(numBins * percentInMem);
			byte[] types1 = new byte[children.get(0).getPos2Col().size()];
			int j = 0;
			for (String col : children.get(0).getPos2Col().values())
			{
				String type = children.get(0).getCols2Types().get(col);
				if (type.equals("INT"))
				{
					types1[j] = (byte)1;
				}
				else if (type.equals("FLOAT"))
				{
					types1[j] = (byte)2;
				}
				else if (type.equals("CHAR"))
				{
					types1[j] = (byte)4;
				}
				else if (type.equals("LONG"))
				{
					types1[j] = (byte)0;
				}
				else if (type.equals("DATE"))
				{
					types1[j] = (byte)3;
				}
				else
				{
					outBuffer.put(new Exception("Unknown type: " + type));
					return;
				}

				j++;
			}

			byte[] types2 = new byte[children.get(1).getPos2Col().size()];
			j = 0;
			for (String col : children.get(1).getPos2Col().values())
			{
				String type = children.get(1).getCols2Types().get(col);
				if (type.equals("INT"))
				{
					types2[j] = (byte)1;
				}
				else if (type.equals("FLOAT"))
				{
					types2[j] = (byte)2;
				}
				else if (type.equals("CHAR"))
				{
					types2[j] = (byte)4;
				}
				else if (type.equals("LONG"))
				{
					types2[j] = (byte)0;
				}
				else if (type.equals("DATE"))
				{
					types2[j] = (byte)3;
				}
				else
				{
					outBuffer.put(new Exception("Unknown type: " + type));
					return;
				}

				j++;
			}

			externalFiles = new ArrayList<String>(numBins << 1);
			ArrayList<String> fns1 = createFNs(numBins, 0);
			externalFiles.addAll(fns1);
			ArrayList<RandomAccessFile> files1 = createFiles(fns1);
			ArrayList<FileChannel> channels1 = createChannels(files1);
			LeftThread thread1 = new LeftThread(files1, channels1, numBins, types1, inMemBins);
			thread1.start();
			ArrayList<String> fns2 = createFNs(numBins, 1);
			externalFiles.addAll(fns2);
			ArrayList<RandomAccessFile> files2 = createFiles(fns2);
			ArrayList<FileChannel> channels2 = createChannels(files2);
			RightThread thread2 = new RightThread(files2, channels2, numBins, types2, inMemBins);
			thread2.start();
			while (true)
			{
				try
				{
					thread1.join();
					break;
				}
				catch (InterruptedException e)
				{
				}
			}
			if (!thread1.getOK())
			{
				outBuffer.put(thread1.getException());
				return;
			}

			ArrayList<ArrayList<ArrayList<Object>>> lbins = thread1.getBins();

			ReadDataThread thread3 = new ReadDataThread(channels1.get(0), types1, lbins, inMemBins, 0);
			thread3.setPriority(Thread.MAX_PRIORITY);
			thread3.start();
			ReadDataThread thread4 = new ReadDataThread(channels1.get(1), types1, lbins, inMemBins, 1);
			thread4.setPriority(Thread.MAX_PRIORITY-1);
			thread4.start();
			ArrayList<ReadDataThread> leftThreads = new ArrayList<ReadDataThread>();
			leftThreads.add(thread3);
			leftThreads.add(thread4);

			while (true)
			{
				try
				{
					thread2.join();
					break;
				}
				catch (InterruptedException e)
				{
				}
			}
			if (!thread2.getOK())
			{
				outBuffer.put(thread2.getException());
				return;
			}

			ArrayList<ArrayList<ArrayList<Object>>> rbins = thread2.getBins();

			HashDataThread thread5 = new HashDataThread(channels2.get(0), types2, rbins, inMemBins, 0);
			thread5.setPriority(Thread.MAX_PRIORITY);
			thread5.start();
			HashDataThread thread6 = new HashDataThread(channels2.get(1), types2, rbins, inMemBins, 1);
			thread6.setPriority(Thread.MAX_PRIORITY-1);
			thread6.start();
			ArrayList<HashDataThread> rightThreads = new ArrayList<HashDataThread>();
			rightThreads.add(thread5);
			rightThreads.add(thread6);

			int i = 2;
			ArrayList<ExternalProcessThread> epThreads = new ArrayList<ExternalProcessThread>();

			int maxPar = Runtime.getRuntime().availableProcessors();
			
			if (maxPar > Integer.parseInt(HRDBMSWorker.getHParms().getProperty("hjo_max_par")))
			{
				maxPar = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("hjo_max_par"));
			}
			
			int numPar1 = -1;
			int numPar2 = -1;

			while (true)
			{
				if (leftThreads.size() == 0)
				{
					break;
				}
				ReadDataThread left = leftThreads.remove(0);
				HashDataThread right = rightThreads.remove(0);
				left.join();
				if (!left.getOK())
				{
					outBuffer.put(left.getException());
					return;
				}
				right.join();
				if (!right.getOK())
				{
					outBuffer.put(right.getException());
					return;
				}

				if (numPar1 == -1)
				{
					int leftSize = left.data.size();
					numPar2 = leftSize / 100000;
					if (numPar2 == 0)
					{
						numPar2 = 1;
					}

					if (numPar2 > maxPar)
					{
						numPar2 = maxPar;
					}

					numPar1 = maxPar / numPar2;
				}

				ExternalProcessThread ept = new ExternalProcessThread(left, right, types2, numPar2);

				if (epThreads.size() >= numPar1)
				{
					int k = numPar1 - 1;
					while (k >= 0)
					{
						if (epThreads.get(k).isDone())
						{
							epThreads.get(k).join();
							epThreads.remove(k);
						}

						k--;
					}

					if (epThreads.size() >= numPar1)
					{
						epThreads.get(0).join();
						epThreads.remove(0);
					}
				}

				int pri = Thread.MAX_PRIORITY - epThreads.size();
				if (pri < Thread.NORM_PRIORITY)
				{
					pri = Thread.NORM_PRIORITY;
				}
				ept.setPriority(pri);
				ept.start();
				epThreads.add(ept);

				if (i < numBins)
				{
					ReadDataThread left2 = new ReadDataThread(channels1.get(i), types1, lbins, inMemBins, i);
					HashDataThread right2 = new HashDataThread(channels2.get(i), types2, rbins, inMemBins, i);
					int lp = Thread.MAX_PRIORITY - leftThreads.size();
					int rp = Thread.MAX_PRIORITY - rightThreads.size();
					if (lp < Thread.NORM_PRIORITY)
					{
						lp = Thread.NORM_PRIORITY;
					}
					if (rp < Thread.NORM_PRIORITY)
					{
						rp = Thread.NORM_PRIORITY;
					}
					left2.setPriority(lp);
					right2.setPriority(rp);
					i++;
					left2.start();
					right2.start();
					leftThreads.add(left2);
					rightThreads.add(right2);
				}
			}

			for (ExternalProcessThread ept : epThreads)
			{
				ept.join();
			}

			outBuffer.put(new DataEndMarker());

			for (FileChannel fc : channels1)
			{
				try
				{
					fc.close();
				}
				catch (Exception e)
				{
				}
			}

			for (FileChannel fc : channels2)
			{
				try
				{
					fc.close();
				}
				catch (Exception e)
				{
				}
			}

			for (RandomAccessFile raf : files1)
			{
				try
				{
					raf.close();
				}
				catch (Exception e)
				{
				}
			}

			for (RandomAccessFile raf : files2)
			{
				try
				{
					raf.close();
				}
				catch (Exception e)
				{
				}
			}

			for (String fn : externalFiles)
			{
				try
				{
					new File(fn).delete();
				}
				catch (Exception e)
				{
				}
			}
		}
		catch (Exception e)
		{
			HRDBMSWorker.logger.debug("", e);
			outBuffer.put(e);
		}

		numHJO.decrementAndGet();
	}

	private void freeClone(Operator clone) throws Exception
	{
		synchronized (clones)
		{
			int i = 0;
			final int size = clones.size();
			while (i < size)
			{
				final Operator o = clones.get(i);
				if (clone == o)
				{
					if (!lockVector.get(i).get())
					{
						Exception e = new Exception("About to unlock an unlocked lock");
						HRDBMSWorker.logger.error("About to unlock an unlocked lock", e);
						throw e;
					}
					lockVector.get(i).set(false);
					return;
				}

				i++;
			}
		}
	}

	private final Object fromBytes(byte[] val, byte[] types) throws Exception
	{
		final ByteBuffer bb = ByteBuffer.wrap(val);
		final int numFields = types.length;

		if (numFields == 0)
		{
			return new ArrayList<Object>();
		}

		final ArrayList<Object> retval = new ArrayList<Object>(numFields);
		int i = 0;
		while (i < numFields)
		{
			if (types[i] == 0)
			{
				// long
				final Long o = getCLong(bb);
				retval.add(o);
			}
			else if (types[i] == 1)
			{
				// integer
				final Integer o = getCInt(bb);
				retval.add(o);
			}
			else if (types[i] == 2)
			{
				// double
				final Double o = bb.getDouble();
				retval.add(o);
			}
			else if (types[i] == 3)
			{
				// date
				final MyDate o = new MyDate(getMedium(bb));
				retval.add(o);
			}
			else if (types[i] == 4)
			{
				// string
				int length = getCInt(bb);
				
				byte[] temp = new byte[length];
				bb.get(temp);
				
				if (!Schema.CVarcharFV.compress)
				{
					try
					{
						final String o = new String(temp, StandardCharsets.UTF_8);
						retval.add(o);
					}
					catch (final Exception e)
					{
						HRDBMSWorker.logger.error("", e);
						throw e;
					}
				}
				else
				{
					byte[] out = new byte[temp.length << 1];
					int clen = Schema.CVarcharFV.decompress(temp, temp.length, out);
					temp = new byte[clen];
					System.arraycopy(out, 0, temp, 0, clen);
					final String o = new String(temp, StandardCharsets.UTF_8);
					retval.add(o);
				}
			}
			else
			{
				HRDBMSWorker.logger.error("Unknown type in fromBytes(): " + types[i]);
				HRDBMSWorker.logger.debug("So far the row is " + retval);
				throw new Exception("Unknown type in fromBytes(): " + types[i]);
			}

			i++;
		}

		return retval;
	}
	
	private final Object fromBytes2(byte[] val, byte[] types) throws Exception
	{
		final ByteBuffer bb = ByteBuffer.wrap(val);
		final int numFields = types.length;

		if (numFields == 0)
		{
			return new ArrayList<Object>();
		}

		final ArrayList<Object> retval = new ArrayList<Object>(numFields);
		int i = 0;
		while (i < numFields)
		{
			if (types[i] == 0)
			{
				// long
				final Long o = bb.getLong();
				retval.add(o);
			}
			else if (types[i] == 1)
			{
				// integer
				final Integer o = bb.getInt();
				retval.add(o);
			}
			else if (types[i] == 2)
			{
				// double
				final Double o = bb.getDouble();
				retval.add(o);
			}
			else if (types[i] == 3)
			{
				// date
				final MyDate o = new MyDate(bb.getInt());
				retval.add(o);
			}
			else if (types[i] == 4)
			{
				// string
				int length = bb.getInt();
				
				byte[] temp = new byte[length];
				bb.get(temp);
				
				try
				{
					final String o = new String(temp, StandardCharsets.UTF_8);
					retval.add(o);
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.error("", e);
					throw e;
				}
			}
			else
			{
				HRDBMSWorker.logger.error("Unknown type in fromBytes(): " + types[i]);
				HRDBMSWorker.logger.debug("So far the row is " + retval);
				throw new Exception("Unknown type in fromBytes(): " + types[i]);
			}

			i++;
		}

		return retval;
	}

	private final Object fromBytes(byte[] val, byte[] types, int[] poses) throws Exception
	{
		final ByteBuffer bb = ByteBuffer.wrap(val);
		final int numFields = types.length;

		if (numFields == 0)
		{
			return new ArrayList<Object>();
		}

		final ArrayList<Object> retval = new ArrayList<Object>(poses.length);
		int[] offsets = new int[types.length];
		int i = 0;
		int pos = 0;
		while (i < offsets.length)
		{
			offsets[i] = pos;
			if (types[i] == 2)
			{
				pos += 8;
			}
			else if (types[i] == 0)
			{
				pos += ((bb.get(pos) & 0xff) >>> 4);
			}
			else if (types[i] == 3)
			{
				pos += 3;
			}
			else if (types[i] == 1)
			{
				pos += ((bb.get(pos) & 0xff) >>> 5);
			}
			else
			{
				// type 4
				int lenlen = ((bb.get(pos) & 0xff) >>> 5);
				int length = getCInt(bb, pos);
				pos += (lenlen + length);
			}

			i++;
		}

		for (int pos2 : poses)
		{
			pos = offsets[pos2];
			if (types[pos2] == 0)
			{
				// long
				final Long o = getCLong(bb, pos);
				retval.add(o);
			}
			else if (types[pos2] == 1)
			{
				// integer
				final Integer o = getCInt(bb, pos);
				retval.add(o);
			}
			else if (types[pos2] == 2)
			{
				// double
				final Double o = bb.getDouble(pos);
				retval.add(o);
			}
			else if (types[pos2] == 3)
			{
				// date
				final MyDate o = new MyDate(getMedium(bb, pos));
				retval.add(o);
			}
			else
			{
				// string
				int lenlen = ((bb.get(pos) & 0xff) >>> 5);
				int length = getCInt(bb, pos);
				byte[] temp = new byte[length];
				bb.position(pos + lenlen);
				bb.get(temp);
				
				if (!Schema.CVarcharFV.compress)
				{
					try
					{
						final String o = new String(temp, StandardCharsets.UTF_8);
						retval.add(o);
					}
					catch (final Exception e)
					{
						HRDBMSWorker.logger.error("", e);
						throw e;
					}
				}
				else
				{
					byte[] out = new byte[temp.length << 1];
					int clen = Schema.CVarcharFV.decompress(temp, temp.length, out);
					temp = new byte[clen];
					System.arraycopy(out, 0, temp, 0, clen);
					final String o = new String(temp, StandardCharsets.UTF_8);
					retval.add(o);
				}
			}
		}

		return retval;
	}
	
	private static int getMedium(ByteBuffer bb, int pos)
	{
		//int retval = (hb[pos] << 16) | ((hb[pos + 1] & 0xff) << 8) | ((hb[pos + 2] & 0xff));
		bb.position(pos);
		int retval = ((bb.getShort() & 0xffff) << 8);
		retval += (bb.get() & 0xff);
		return retval;
	}
	
	private static int getMedium(ByteBuffer bb)
	{
		int retval = ((bb.getShort() & 0xffff) << 8);
		retval += (bb.get() & 0xff);
		return retval;
	}
	
	private static long getCLong(ByteBuffer bb)
	{
		int temp = (bb.get() & 0xff);
		int length = (temp >>> 4);
		if (length == 1)
		{
			return (temp & 0x0f);
		}
		else if (length == 9)
		{
			return bb.getLong();
		}
		else
		{
			long retval = (temp & 0x0f);
			if (length == 2)
			{
				retval = (retval << 8);
				retval += (bb.get() & 0xff);
			}
			else if (length == 3)
			{
				retval = (retval << 16);
				retval += (bb.getShort() & 0xffff);
			}
			else if (length == 4)
			{
				retval = (retval << 24);
				retval += getMedium(bb);
			}
			else if (length == 5)
			{
				retval = (retval << 32);
				retval += (bb.getInt() & 0xffffffffl);
			}
			else if (length == 6)
			{
				retval = (retval << 40);
				retval += ((bb.get() & 0xffl) << 32);
				retval += (bb.getInt() & 0xffffffffl);
			}
			else if (length == 7)
			{
				retval = (retval << 48);
				retval += ((bb.getShort() & 0xffffl) << 32);
				retval += (bb.getInt() & 0xffffffffl);
			}
			else
			{
				retval = (retval << 56);
				retval += ((getMedium(bb) & 0xffffffl) << 32);
				retval += (bb.getInt() & 0xffffffffl);
			}
			
			return retval;
		}
	}
	
	private static int getCInt(ByteBuffer bb)
	{
		int temp = (bb.get() & 0xff);
		int length = (temp >>> 5);
		if (length == 1)
		{
			return (temp & 0x1f);
		}
		else if (length == 5)
		{
			return bb.getInt();
		}
		else
		{
			int retval = (temp & 0x1f);
			if (length == 2)
			{
				retval = (retval << 8);
				retval += (bb.get() & 0xff);
			}
			else if (length == 3)
			{
				retval = (retval << 16);
				retval += (bb.getShort() & 0xffff);
			}
			else
			{
				retval = (retval << 24);
				retval += getMedium(bb);
			}
			
			return retval;
		}
	}
	
	private static long getCLong(ByteBuffer bb, int pos)
	{
		int temp = (bb.get(pos) & 0xff);
		int length = (temp >>> 4);
		if (length == 1)
		{
			return (temp & 0x0f);
		}
		else if (length == 9)
		{
			return bb.getLong(pos+1);
		}
		else
		{
			long retval = (temp & 0x0f);
			if (length == 2)
			{
				retval = (retval << 8);
				retval += (bb.get(pos+1) & 0xff);
			}
			else if (length == 3)
			{
				retval = (retval << 16);
				retval += (bb.getShort(pos+1) & 0xffff);
			}
			else if (length == 4)
			{
				retval = (retval << 24);
				retval += getMedium(bb, pos+1);
			}
			else if (length == 5)
			{
				retval = (retval << 32);
				retval += (bb.getInt(pos+1) & 0xffffffffl);
			}
			else if (length == 6)
			{
				retval = (retval << 40);
				retval += ((bb.get(pos+1) & 0xffl) << 32);
				retval += (bb.getInt(pos+2) & 0xffffffffl);
			}
			else if (length == 7)
			{
				retval = (retval << 48);
				retval += ((bb.getShort(pos+1) & 0xffffl) << 32);
				retval += (bb.getInt(pos+3) & 0xffffffffl);
			}
			else
			{
				retval = (retval << 56);
				retval += ((getMedium(bb, pos+1) & 0xffffffl) << 32);
				retval += (bb.getInt(pos+4) & 0xffffffffl);
			}
			
			return retval;
		}
	}
	
	private static int getCInt(ByteBuffer bb, int pos)
	{
		int temp = (bb.get(pos) & 0xff);
		int length = (temp >>> 5);
		if (length == 1)
		{
			return (temp & 0x1f);
		}
		else if (length == 5)
		{
			return bb.getInt(pos+1);
		}
		else
		{
			int retval = (temp & 0x1f);
			if (length == 2)
			{
				retval = (retval << 8);
				retval += (bb.get(pos+1) & 0xff);
			}
			else if (length == 3)
			{
				retval = (retval << 16);
				retval += (bb.getShort(pos+1) & 0xffff);
			}
			else
			{
				retval = (retval << 24);
				retval += getMedium(bb, pos+1);
			}
			
			return retval;
		}
	}

	private List<ArrayList<Object>> getCandidates(List<byte[]> bCandidates, byte[] types) throws Exception
	{
		ArrayList<ArrayList<Object>> retval = new ArrayList<ArrayList<Object>>(bCandidates.size());
		int z = 0;
		final int limit = bCandidates.size();
		while (z < limit)
		{
			byte[] b = bCandidates.get(z++);
			try
			{
				retval.add((ArrayList<Object>)fromBytes2(b, types));
			}
			catch(Exception e)
			{
				HRDBMSWorker.logger.debug("", e);
				throw e;
			}
		}

		return retval;
	}

	private final ArrayList<ArrayList<Object>> getCandidates(long hash, byte[] types) throws Exception
	{
		final ArrayList<ArrayList<Object>> retval = new ArrayList<ArrayList<Object>>();
		List<byte[]> list = buckets.get(hash);
		int z = 0;
		final int limit = list.size();
		while (z < limit)
		{
			retval.add((ArrayList<Object>)fromBytes2(list.get(z++), types));
		}

		return retval;
	}

	private Operator getClone() throws Exception
	{
		synchronized (clones)
		{
			int i = 0;
			final int size = lockVector.size();
			while (i < size)
			{
				final AtomicBoolean lock = lockVector.get(i);

				if (!lock.get())
				{
					if (lock.compareAndSet(false, true))
					{
						final Operator retval = clones.get(i);
						return retval;
					}
				}

				i++;
			}

			Operator clone = clone(children.get(1));
			final RootOperator root = new RootOperator(meta);
			try
			{
				root.add(clone);
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				throw e;
			}
			if (clone instanceof TableScanOperator)
			{
				if (((TableScanOperator)children.get(1)).orderedFilters.size() > 0)
				{
					((TableScanOperator)clone).setCNFForParent(root, ((TableScanOperator)children.get(1)).getCNFForParent(this));
				}
			}
			clone = root;
			clones.add(clone);
			lockVector.add(new AtomicBoolean(true));

			return clone;
		}
	}

	private String getLeftForRight(String right)
	{
		int i = 0;
		for (final String r : rights)
		{
			if (r.equals(right))
			{
				return lefts.get(i);
			}

			i++;
		}

		return null;
	}

	private String getRightForLeft(String left)
	{
		int i = 0;
		for (final String l : lefts)
		{
			if (l.equals(left))
			{
				return rights.get(i);
			}

			i++;
		}

		return null;
	}

	private void process(ReadDataThread left, HashDataThread right, byte[] types, int start, int end) throws Exception
	{
		ArrayList<ArrayList<Object>> probe = left.getData();
		HJOMultiHashMap<Long, byte[]> table = right.getData();
		final HashMap<String, Integer> childCols2Pos = children.get(0).getCols2Pos();

		int[] poses = new int[lefts.size()];
		int i = 0;
		for (String col : lefts)
		{
			poses[i] = childCols2Pos.get(col);
			i++;
		}
		final ArrayList<Object> key = new ArrayList<Object>(lefts.size());

		int z = start;
		while (z < end)
		{
			ArrayList<Object> row = probe.get(z++);
			i = 0;
			key.clear();
			for (int pos : poses)
			{
				try
				{
					key.add(row.get(pos));
					i++;
				}
				catch (Exception e)
				{
					HRDBMSWorker.logger.debug("Failed to find a column in " + childCols2Pos);
					throw e;
				}
			}

			final long hash = hash(key);
			List<ArrayList<Object>> candidates = getCandidates(table.get(hash), types);
			boolean found = false;
			final int limit = candidates.size();
			int at = 0;
			// for (final ArrayList<Object> rRow : candidates)
			while (at < limit)
			{
				ArrayList<Object> rRow = candidates.get(at);
				if (cnfFilters.passes(row, rRow))
				{
					if (semi != null)
					{
						outBuffer.put(row);
						break;
					}
					else if (anti != null)
					{
						found = true;
						break;
					}
					else
					{
						final ArrayList<Object> out = new ArrayList<Object>(row.size() + rRow.size());
						out.addAll(row);
						out.addAll(rRow);
						outBuffer.put(out);
						// if (rhsUnique)
						// {
						// break;
						// }
					}
				}

				at++;
			}

			if (anti != null && !found)
			{
				outBuffer.put(row);
			}
		}

		probe = null;
		table = null;
	}

	private final byte[] rsToBytes(ArrayList<ArrayList<Object>> rows, final byte[] types) throws Exception
	{
		final ArrayList<ByteBuffer> results = new ArrayList<ByteBuffer>(rows.size());
		ArrayList<byte[]> bytes = new ArrayList<byte[]>();
		final ArrayList<Integer> stringCols = new ArrayList<Integer>(rows.get(0).size());
		int startSize = 4;
		int a = 0;
		for (byte b : types)
		{
			if (b == 4)
			{
				startSize += 4;
				stringCols.add(a);
			}
			else if (b == 1 || b == 3)
			{
				startSize += 4;
			}
			else
			{
				startSize += 8;
			}

			a++;
		}

		for (ArrayList<Object> val : rows)
		{
			int size = startSize;
			for (int y : stringCols)
			{
				Object o = val.get(y);
				byte[] b = ((String)o).getBytes(StandardCharsets.UTF_8);
				if (Schema.CVarcharFV.compress)
				{
					byte[] out = new byte[b.length * 3 + 1];
					int clen = Schema.CVarcharFV.compress(b, b.length, out);
					b = new byte[clen];
					System.arraycopy(out, 0, b, 0, clen);
				}
				size += b.length;
				bytes.add(b);
			}

			final byte[] retval = new byte[size];
			final ByteBuffer retvalBB = ByteBuffer.wrap(retval);
			//retvalBB.putInt(size - 4);
			int x = 0;
			int i = 0;
			for (final Object o : val)
			{
				if (types[i] == 0)
				{
					//retvalBB.putLong((Long)o);
					putCLong(retvalBB, (Long)o);
				}
				else if (types[i] == 1)
				{
					//retvalBB.putInt((Integer)o);
					putCInt(retvalBB, (Integer)o);
				}
				else if (types[i] == 2)
				{
					retvalBB.putDouble((Double)o);
				}
				else if (types[i] == 3)
				{
					int value = ((MyDate)o).getTime();
					//retvalBB.putInt(value);
					putMedium(retvalBB, value);
				}
				else if (types[i] == 4)
				{
					byte[] temp = bytes.get(x++);
					//retvalBB.putInt(temp.length);
					putCInt(retvalBB, temp.length);
					retvalBB.put(temp);
				}
				else
				{
					throw new Exception("Unknown type: " + types[i]);
				}

				i++;
			}

			results.add(retvalBB);
			bytes.clear();
		}

		int count = 0;
		for (ByteBuffer bb : results)
		{
			count += (bb.position() + 4);
		}
		final byte[] retval = new byte[count];
		ByteBuffer retvalBB = ByteBuffer.wrap(retval);
		int retvalPos = 0;
		for (ByteBuffer bb : results)
		{
			byte[] ba = bb.array();
			retvalBB.position(retvalPos);
			retvalBB.putInt(bb.position());
			retvalPos += 4;
			System.arraycopy(ba, 0, retval, retvalPos, bb.position());
			retvalPos += bb.position();
		}

		return retval;
	}
	
	private static void putCLong(ByteBuffer bb, long val)
	{
		if (val <= 15)
		{
			//1 byte
			val |= 0x10;
			bb.put((byte)val);
		}
		else if (val <= 4095)
		{
			//2 bytes
			val |= 0x2000;
			bb.putShort((short)val);
		}
		else if (val <= 0xfffff)
		{
			//3 bytes
			val |= 0x300000;
			putMedium(bb, (int)val);
		}
		else if (val <= 0xfffffff)
		{
			//4 bytes
			val |= 0x40000000;
			bb.putInt((int)val);
		}
		else if (val <= 0xfffffffffl)
		{
			//5 bytes
			val |= 0x5000000000l;
			bb.put((byte)(val >>> 32));
			bb.putInt((int)val);
		}
		else if (val <= 0xfffffffffffl)
		{
			//6 bytes
			val |= 0x600000000000l;
			bb.putShort((short)(val >>> 32));
			bb.putInt((int)val);
		}
		else if (val <= 0xfffffffffffffl)
		{
			//7 bytes
			val |= 0x70000000000000l;
			putMedium(bb, (int)(val >> 32));
			bb.putInt((int)val);
		}
		else if (val <= 0xfffffffffffffffl)
		{
			//8 bytes
			val |= 0x8000000000000000l;
			bb.putLong(val);
		}
		else
		{
			//9 bytes
			bb.put((byte)0x90);
			bb.putLong(val);
		}
	}
	
	private static void putCInt(ByteBuffer bb, int val)
	{
		if (val <= 31)
		{
			//1 byte
			val |= 0x20;
			bb.put((byte)val);
		}
		else if (val <= 8191)
		{
			//2 bytes
			val |= 0x4000;
			bb.putShort((short)val);
		}
		else if (val <= 0x1fffff)
		{
			//3 bytes
			val |= 0x600000;
			putMedium(bb, val);
		}
		else if (val <= 0x1FFFFFFF)
		{
			//4 bytes
			val |= 0x80000000;
			bb.putInt(val);
		}
		else
		{
			//5 bytes
			bb.put((byte)0xa0);
			bb.putInt(val);
		}
	}
	
	private static void putMedium(ByteBuffer bb, int val)
	{
		bb.put((byte)((val & 0xff0000) >> 16));
		bb.put((byte)((val & 0xff00) >> 8));
		bb.put((byte)(val & 0xff));
	}

	private final void writeToHashTable(long hash, byte[] row) throws Exception
	{
		buckets.multiPut(hash, row);
	}

	private class EPT2Thread extends HRDBMSThread
	{
		private final ReadDataThread left;
		private final HashDataThread right;
		private final byte[] types;
		private final int start;
		private final int end;

		public EPT2Thread(ReadDataThread left, HashDataThread right, byte[] types, int start, int end)
		{
			this.left = left;
			this.right = right;
			this.types = types;
			this.start = start;
			this.end = end;
		}

		@Override
		public void run()
		{
			try
			{
				process(left, right, types, start, end);
			}
			catch (Exception e)
			{
				// HRDBMSWorker.logger.debug(HashJoinOperator.this +
				// ": ExternalProcessThread #" + left.index +
				// " completed with exception");
				HRDBMSWorker.logger.debug("", e);
				outBuffer.put(e);
			}
		}
	}

	private class ExternalProcessThread extends HRDBMSThread
	{
		private final ReadDataThread left;
		private final HashDataThread right;
		private final byte[] types;
		private final int par;
		private int pri;

		public ExternalProcessThread(ReadDataThread left, HashDataThread right, byte[] types, int par)
		{
			this.left = left;
			this.right = right;
			this.types = types;
			this.par = par;
		}
		
		public void setPriority(int pri)
		{
			this.pri = pri;
		}

		@Override
		public void run()
		{
			if (pri != -1)
			{
				Thread.currentThread().setPriority(pri);
			}
			try
			{
				if (par == 1)
				{
					process(left, right, types, 0, left.data.size());
					left.clear();
					right.clear();
					return;
				}

				int i = 0;
				int size = left.data.size();
				int per = size / par;

				if (per == 0 && size != 0)
				{
					per = 1;
				}

				int pos = 0;
				ArrayList<EPT2Thread> threads = new ArrayList<EPT2Thread>(par);
				while (i < par)
				{
					int end = pos + per;
					if (end > size)
					{
						end = size;
					}

					if (i == par - 1)
					{
						end = size;
					}

					EPT2Thread thread = new EPT2Thread(left, right, types, pos, end);
					thread.start();
					threads.add(thread);
					pos = end;

					if (pos >= size)
					{
						break;
					}

					i++;
				}

				for (EPT2Thread thread : threads)
				{
					thread.join();
				}

				// HRDBMSWorker.logger.debug(HashJoinOperator.this +
				// ": ExternalProcessThread #" + left.index + " completed");
				left.clear();
				right.clear();
			}
			catch (Exception e)
			{
				// HRDBMSWorker.logger.debug(HashJoinOperator.this +
				// ": ExternalProcessThread #" + left.index +
				// " completed with exception");
				HRDBMSWorker.logger.debug("", e);
				outBuffer.put(e);
			}
		}
	}

	private class ExternalThread extends HRDBMSThread
	{
		private final double percentInMem;

		public ExternalThread()
		{
			percentInMem = 0;
		}

		public ExternalThread(double percentInMem)
		{
			this.percentInMem = percentInMem;
		}

		@Override
		public void run()
		{
			external(percentInMem);
		}
	}

	private class FlushBinThread extends HRDBMSThread
	{
		private final byte[] types;
		private ArrayList<ArrayList<Object>> bin;
		private final FileChannel fc;
		private boolean ok = true;
		private Exception e;
		private ByteBuffer direct;
		private boolean force = false;

		public FlushBinThread(ArrayList<ArrayList<Object>> bin, byte[] types, FileChannel fc, ByteBuffer direct)
		{
			this.types = types;
			this.bin = bin;
			this.fc = fc;
			this.direct = direct;
		}
		
		public FlushBinThread(ArrayList<ArrayList<Object>> bin, byte[] types, FileChannel fc, ByteBuffer direct, boolean force)
		{
			this.types = types;
			this.bin = bin;
			this.fc = fc;
			this.direct = direct;
			this.force = force;
		}

		public Exception getException()
		{
			return e;
		}

		public boolean getOK()
		{
			return ok;
		}

		@Override
		public void run()
		{
			//Thread.currentThread().setPriority(Thread.MAX_PRIORITY);
			try
			{
				byte[] data = rsToBytes(bin, types);
				//HRDBMSWorker.logger.debug("HJO bin flush size = " + (data.length * 1.0 / 1048576) + "MB");
				bin = null;
				
				if (direct == null)
				{
					// fc.position(fc.size());
					ByteBuffer bb = ByteBuffer.wrap(data);
					fc.write(bb);
				}
				else
				{
					if (direct.position() + data.length <= 8 * 1024 * 1024)
					{
						try
						{
							direct.put(data);
						}
						catch(Exception e)
						{
							HRDBMSWorker.logger.debug("", e);
							HRDBMSWorker.logger.debug("Capacity: " + direct.capacity() + " Limit: " + direct.limit() + " Position: " + direct.position() + " Write size: " + data.length);
						}
						
						if (force)
						{
							direct.limit(direct.position());
							direct.position(0);
							fc.write(direct);
							direct.limit(direct.capacity());
						}
					}
					else
					{
						if (direct.position() > 0)
						{
							direct.limit(direct.position());
							direct.position(0);
							fc.write(direct);
							direct.limit(direct.capacity());
						}
						
						if (!force && data.length <= 8 * 1024 * 1024)
						{
							direct.position(0);
							direct.put(data);
						}
						else
						{
							ByteBuffer bb = ByteBuffer.wrap(data);
							fc.write(bb);
						}
					}
				}
			}
			catch (Exception e)
			{
				HRDBMSWorker.logger.debug("", e);
				ok = false;
				this.e = e;
			}
		}
	}

	private class HashDataThread extends HRDBMSThread
	{
		private final FileChannel fc;
		private boolean ok = true;
		private Exception e;
		private HJOMultiHashMap<Long, byte[]> data;
		private final byte[] types;
		private final ArrayList<ArrayList<ArrayList<Object>>> bins;
		private final int binsInMem;
		private final int index;
		private int pri;

		public HashDataThread(FileChannel fc, byte[] types, ArrayList<ArrayList<ArrayList<Object>>> bins, int binsInMem, int index) throws Exception
		{
			this.fc = new BufferedFileChannel(fc, 8*1024*1024);
			this.types = types;
			data = new HJOMultiHashMap<Long, byte[]>();
			this.bins = bins;
			this.binsInMem = binsInMem;
			this.index = index;
		}
		
		public void setPriority(int pri)
		{
			this.pri = pri;
		}

		public void clear()
		{
			data = null;
		}

		public HJOMultiHashMap<Long, byte[]> getData()
		{
			return data;
		}

		public Exception getException()
		{
			return e;
		}

		public boolean getOK()
		{
			return ok;
		}

		@Override
		public void run()
		{
			// HRDBMSWorker.logger.debug(HashJoinOperator.this +
			// ": HashDataThread #" + index + " starting");
			Thread.currentThread().setPriority(pri);
			try
			{
				if (index < binsInMem)
				{
					final HashMap<String, Integer> childCols2Pos = children.get(1).getCols2Pos();

					int[] poses = new int[rights.size()];
					int i = 0;
					for (String col : rights)
					{
						poses[i] = childCols2Pos.get(col);
						i++;
					}

					final ArrayList<Object> key = new ArrayList<Object>(rights.size());
					for (ArrayList<Object> row : bins.get(index))
					{
						i = 0;
						key.clear();
						for (int pos : poses)
						{
							try
							{
								key.add(row.get(pos));
								i++;
							}
							catch (Exception e)
							{
								HRDBMSWorker.logger.debug("Failed to find a column in " + childCols2Pos);
								throw e;
							}
						}

						final long hash = hash(key);
						data.multiPut(hash, toBytes(row));
					}

					// HRDBMSWorker.logger.debug(HashJoinOperator.this +
					// ": HashDataThread #" + index + " completed");
					return;
				}
				fc.position(0);
				ByteBuffer bb1 = ByteBuffer.allocate(4);
				final HashMap<String, Integer> childCols2Pos = children.get(1).getCols2Pos();

				int[] poses = new int[rights.size()];
				int i = 0;
				for (String col : rights)
				{
					poses[i] = childCols2Pos.get(col);
					i++;
				}

				while (true)
				{
					bb1.position(0);
					if (fc.read(bb1) == -1)
					{
						// HRDBMSWorker.logger.debug(HashJoinOperator.this +
						// ": HashDataThread #" + index + " completed");
						return;
					}
					bb1.position(0);
					int length = bb1.getInt();
					ByteBuffer bb = ByteBuffer.allocate(length);
					fc.read(bb);
					ArrayList<Object> row2 = new ArrayList<Object>();
					ArrayList<Object> row = null;
					try
					{
						row = (ArrayList<Object>)fromBytes(bb.array(), types);
					}
					catch(Exception e)
					{
						HRDBMSWorker.logger.debug("", e);
					}
					
					for (int pos : poses)
					{
						row2.add(row.get(pos));
					}

					final long hash = hash(row2);
					final long hash2 = 0x7FFFFFFFFFFFFFFFL & hash;
					if (bf2 != null && !bf2.passes(hash2))
					{
						continue;
					}
					data.multiPut(hash, toBytes(row));
				}
			}
			catch (Throwable e)
			{
				// HRDBMSWorker.logger.debug(HashJoinOperator.this +
				// ": HashDataThread #" + index + " completed with exception");
				HRDBMSWorker.logger.debug("", e);
				ok = false;
				if (e instanceof Exception)
				{
					this.e = (Exception)e;
				}
				else
				{
					this.e = new Exception();
				}
			}
		}
	}

	private final class InitThread extends ThreadPoolThread
	{
		@Override
		public void run()
		{
			ThreadPoolThread[] threads;
			int i = 0;
			threads = new ReaderThread[NUM_RT_THREADS];
			while (i < NUM_RT_THREADS)
			{
				threads[i] = new ReaderThread();
				threads[i].start();
				i++;
			}

			i = 0;
			while (i < NUM_RT_THREADS)
			{
				while (true)
				{
					try
					{
						threads[i].join();
						i++;
						break;
					}
					catch (final InterruptedException e)
					{
					}
				}
			}

			readersDone = true;

			i = 0;
			final ThreadPoolThread[] threads2 = new ProcessThread[NUM_PTHREADS];
			while (i < NUM_PTHREADS)
			{
				threads2[i] = new ProcessThread();
				threads2[i].start();
				i++;
			}

			i = 0;
			while (i < NUM_PTHREADS)
			{
				while (true)
				{
					try
					{
						threads2[i].join();
						i++;
						break;
					}
					catch (final InterruptedException e)
					{
					}
				}
			}

			// HRDBMSWorker.logger.debug("Hash join operator " +
			// HashJoinOperator.this + " processed " + leftCount +
			// " left rows and " + inCount + " right rows and output " +
			// outCount + " rows");

			while (true)
			{
				try
				{
					outBuffer.put(new DataEndMarker());
					buckets.clear();
					break;
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.error("", e);
				}
			}
		}
	}

	private class LeftThread extends HRDBMSThread
	{
		private final ArrayList<FileChannel> channels;
		private final int numBins;
		private final byte[] types;
		private boolean ok = true;
		private Exception e;
		private final BloomFilter tempBF;
		private final int inMemBins;
		private ArrayList<ArrayList<ArrayList<Object>>> bins;
		private ScalableStampedReentrantRWLock rwLock;
		private ArrayList<ByteBuffer> directs;

		public LeftThread(ArrayList<RandomAccessFile> files, ArrayList<FileChannel> channels, int numBins, byte[] types, int inMemBins)
		{
			this.channels = channels;
			this.numBins = numBins;
			this.types = types;
			tempBF = new BloomFilter();
			this.inMemBins = inMemBins;
		}

		public ArrayList<ArrayList<ArrayList<Object>>> getBins()
		{
			return bins;
		}

		public Exception getException()
		{
			return e;
		}

		public boolean getOK()
		{
			return ok;
		}

		@Override
		public void run()
		{
			rwLock = new ScalableStampedReentrantRWLock();
			bins = new ArrayList<ArrayList<ArrayList<Object>>>();
			directs = new ArrayList<ByteBuffer>();
			ConcurrentHashMap<Integer, FlushBinThread> threads = new ConcurrentHashMap<Integer, FlushBinThread>();
			int size = (int)(ResourceManager.QUEUE_SIZE * Double.parseDouble(HRDBMSWorker.getHParms().getProperty("external_factor")) / (numBins >> SHIFT));
			int i = 0;
			while (i < numBins)
			{
				bins.add(new ArrayList<ArrayList<Object>>(size));
				directs.add(null);
				i++;
			}
			
			if (HRDBMSWorker.getHParms().getProperty("use_direct_buffers_for_flush").equals("true"))
			{
				i = 0;
				while (i < numBins)
				{
					directs.set(i, TempThread.getDirect());
					i++;
				}
			}
			
			i = 0;
			ArrayList<SubLeftThread> slThreads = new ArrayList<SubLeftThread>();
			while (i < 1)
			{
				SubLeftThread t = new SubLeftThread(this, threads, size);
				t.start();
				slThreads.add(t);
				i++;
			}

			try
			{
				final Operator child = children.get(0);
				final HashMap<String, Integer> childCols2Pos = child.getCols2Pos();
				Object o = child.next(HashJoinOperator.this);
				if (o instanceof DataEndMarker)
				{
					demReceived = true;
					// HRDBMSWorker.logger.debug(HashJoinOperator.this +
					// " LeftThread received DEM");
				}
				else
				{
					received.getAndIncrement();
				}

				int[] poses = new int[lefts.size()];
				i = 0;
				for (String col : lefts)
				{
					poses[i] = childCols2Pos.get(col);
					i++;
				}
				final ArrayList<Object> key = new ArrayList<Object>(lefts.size());
				while (!(o instanceof DataEndMarker))
				{
					i = 0;
					key.clear();
					for (int pos : poses)
					{
						try
						{
							key.add(((ArrayList<Object>)o).get(pos));
							i++;
						}
						catch (Exception e)
						{
							HRDBMSWorker.logger.debug("Failed to find a column in " + childCols2Pos);
							throw e;
						}
					}

					final long hash = 0x7FFFFFFFFFFFFFFFL & hash(key);
					if (tempBF != null)
					{
						tempBF.add(hash);
					}
					if (anti == null && bf != null && !bf.passes(hash))
					{
						o = child.next(HashJoinOperator.this);
						if (o instanceof DataEndMarker)
						{
							demReceived = true;
							// HRDBMSWorker.logger.debug(HashJoinOperator.this +
							// " LeftThread received DEM");
						}
						else
						{
							received.getAndIncrement();
						}
						continue;
					}
					else if (anti != null && bf != null && !bf.passes(hash, key))
					{
						o = child.next(HashJoinOperator.this);
						if (o instanceof DataEndMarker)
						{
							demReceived = true;
							// HRDBMSWorker.logger.debug(HashJoinOperator.this +
							// " LeftThread received DEM");
						}
						else
						{
							received.getAndIncrement();
						}
						continue;
					}

					int x = (int)(hash % numBins);
					
					rwLock.readLock().lock();
					ArrayList<ArrayList<Object>> bin = bins.get(x);
					synchronized(bin)
					{
						// writeToHashTable(hash, (ArrayList<Object>)o);
						bin.add((ArrayList<Object>)o);
					}
					rwLock.readLock().unlock();

					if (x >= inMemBins && bin.size() >= size)
					{
						rwLock.writeLock().lock();
						bin = bins.get(x);
						if (bin.size() >= size)
						{
							FlushBinThread thread = new FlushBinThread(bin, types, channels.get(x), directs.get(x));
							if (threads.putIfAbsent(x, thread) != null)
							{
								threads.get(x).join();
								if (!threads.get(x).getOK())
								{
									rwLock.writeLock().unlock();
									throw threads.get(x).getException();
								}

								threads.put(x, thread);
							}
							TempThread.start(thread, txnum);
							bins.set(x, new ArrayList<ArrayList<Object>>(size));
						}
						rwLock.writeLock().unlock();
					}

					o = child.next(HashJoinOperator.this);
					if (o instanceof DataEndMarker)
					{
						demReceived = true;
						// HRDBMSWorker.logger.debug(HashJoinOperator.this +
						// " LeftThread received DEM");
					}
					else
					{
						received.getAndIncrement();
					}
				}
				
				for (SubLeftThread t : slThreads)
				{
					t.join();
					
					if (!t.getOK())
					{
						throw t.getException();
					}
				}

				if (tempBF != null)
				{
					bf2 = tempBF;
				}

				i = 0;
				for (ArrayList<ArrayList<Object>> bin : bins)
				{
					if (i >= inMemBins && bin.size() > 0)
					{
						FlushBinThread thread = new FlushBinThread(bin, types, channels.get(i), directs.get(i), true);
						if (threads.putIfAbsent(i, thread) != null)
						{
							threads.get(i).join();
							if (!threads.get(i).getOK())
							{
								throw threads.get(i).getException();
							}

							threads.put(i, thread);
						}
						TempThread.start(thread, txnum);
					}

					i++;
				}

				for (FlushBinThread thread : threads.values())
				{
					thread.join();
					if (!thread.getOK())
					{
						throw thread.getException();
					}
				}

				// HRDBMSWorker.logger.debug(HashJoinOperator.this +
				// " LeftThread wrote everything to disk");

				// everything is written
				i = 0;
				while (i < numBins)
				{
					ByteBuffer bb = directs.get(i);
					if (bb != null)
					{
						TempThread.freeDirect(bb);
					}
					
					i++;
				}
				
				i = numBins - 1;
				while (i >= inMemBins)
				{
					bins.remove(i);
					i--;
				}
			}
			catch (final Exception e)
			{
				ok = false;
				this.e = e;
			}
		}
	}
	
	private final class SubLeftThread extends HRDBMSThread
	{
		private LeftThread leftThread;
		private ConcurrentHashMap<Integer, FlushBinThread> threads;
		private final int size;
		private boolean ok = true;
		private Exception e;
		
		public SubLeftThread(LeftThread leftThread, ConcurrentHashMap<Integer, FlushBinThread> threads, int size)
		{
			this.leftThread = leftThread;
			this.threads = threads;
			this.size = size;
		}
		
		public boolean getOK()
		{
			return ok;
		}
		
		public Exception getException()
		{
			return e;
		}
		
		public void run()
		{
			try
			{
				final Operator child = children.get(0);
				final HashMap<String, Integer> childCols2Pos = child.getCols2Pos();
				Object o = child.next(HashJoinOperator.this);
				if (o instanceof DataEndMarker)
				{
					demReceived = true;
					// HRDBMSWorker.logger.debug(HashJoinOperator.this +
					// " LeftThread received DEM");
				}
				else
				{
					received.getAndIncrement();
				}

				int[] poses = new int[lefts.size()];
				int i = 0;
				for (String col : lefts)
				{
					poses[i] = childCols2Pos.get(col);
					i++;
				}
				final ArrayList<Object> key = new ArrayList<Object>(lefts.size());
				while (!(o instanceof DataEndMarker))
				{
					i = 0;
					key.clear();
					for (int pos : poses)
					{
						try
						{
							key.add(((ArrayList<Object>)o).get(pos));
							i++;
						}
						catch (Exception e)
						{
							HRDBMSWorker.logger.debug("Failed to find a column in " + childCols2Pos);
							throw e;
						}
					}

					final long hash = 0x7FFFFFFFFFFFFFFFL & hash(key);
					if (leftThread.tempBF != null)
					{
						leftThread.tempBF.add(hash); 
					}
					if (anti == null && bf != null && !bf.passes(hash)) 
					{
						o = child.next(HashJoinOperator.this);
						if (o instanceof DataEndMarker)
						{
							demReceived = true;
							// HRDBMSWorker.logger.debug(HashJoinOperator.this +
							// " LeftThread received DEM");
						}
						else
						{
							received.getAndIncrement();
						}
						continue;
					}
					else if (anti != null && bf != null && !bf.passes(hash, key)) 
					{
						o = child.next(HashJoinOperator.this);
						if (o instanceof DataEndMarker)
						{
							demReceived = true;
							// HRDBMSWorker.logger.debug(HashJoinOperator.this +
							// " LeftThread received DEM");
						}
						else
						{
							received.getAndIncrement();
						}
						continue;
					}

					int x = (int)(hash % leftThread.numBins);
					
					leftThread.rwLock.readLock().lock();
					ArrayList<ArrayList<Object>> bin = leftThread.bins.get(x);
					synchronized(bin)
					{
						// writeToHashTable(hash, (ArrayList<Object>)o);
						bin.add((ArrayList<Object>)o);
					}
					leftThread.rwLock.readLock().unlock();

					if (x >= leftThread.inMemBins && bin.size() >= size)
					{
						leftThread.rwLock.writeLock().lock();
						bin = leftThread.bins.get(x);
						if (bin.size() >= size)
						{
							FlushBinThread thread = new FlushBinThread(bin, leftThread.types, leftThread.channels.get(x), leftThread.directs.get(x));
							if (threads.putIfAbsent(x, thread) != null)
							{
								threads.get(x).join();
								if (!threads.get(x).getOK())
								{
									leftThread.rwLock.writeLock().unlock();
									throw threads.get(x).getException();
								}

								threads.put(x, thread);
							}
							TempThread.start(thread, txnum);
							leftThread.bins.set(x, new ArrayList<ArrayList<Object>>(size));
						}
						leftThread.rwLock.writeLock().unlock();
					}

					o = child.next(HashJoinOperator.this);
					if (o instanceof DataEndMarker)
					{
						demReceived = true;
						// HRDBMSWorker.logger.debug(HashJoinOperator.this +
						// " LeftThread received DEM");
					}
					else
					{
						received.getAndIncrement();
					}
				}
			}
			catch(Exception e)
			{
				ok = false;
				this.e = e;
			}
		}
		
	}

	private final class ProcessThread extends ThreadPoolThread
	{
		@Override
		public void run()
		{
			try
			{
				byte[] types2 = new byte[children.get(1).getPos2Col().size()];
				int j = 0;
				for (String col : children.get(1).getPos2Col().values())
				{
					String type = children.get(1).getCols2Types().get(col);
					if (type.equals("INT"))
					{
						types2[j] = (byte)1;
					}
					else if (type.equals("FLOAT"))
					{
						types2[j] = (byte)2;
					}
					else if (type.equals("CHAR"))
					{
						types2[j] = (byte)4;
					}
					else if (type.equals("LONG"))
					{
						types2[j] = (byte)0;
					}
					else if (type.equals("DATE"))
					{
						types2[j] = (byte)3;
					}
					else
					{
						outBuffer.put(new Exception("Unknown type: " + type));
						return;
					}

					j++;
				}

				while (!readersDone)
				{
					LockSupport.parkNanos(500);
				}

				final Operator left = children.get(0);
				final HashMap<String, Integer> childCols2Pos = left.getCols2Pos();
				Object o = left.next(HashJoinOperator.this);
				if (o instanceof DataEndMarker)
				{
					demReceived = true;
					// HRDBMSWorker.logger.debug(HashJoinOperator.this +
					// " ProcessThread received DEM");
				}
				else
				{
					received.getAndIncrement();
				}
				final ArrayList<Object> key = new ArrayList<Object>(lefts.size());
				// @Parallel
				int[] poses = new int[lefts.size()];
				int i = 0;
				for (String col : lefts)
				{
					poses[i] = childCols2Pos.get(col);
					;
					i++;
				}

				while (!(o instanceof DataEndMarker))
				{
					final ArrayList<Object> lRow = (ArrayList<Object>)o;
					key.clear();
					i = 0;
					for (int pos : poses)
					{
						try
						{
							key.add(lRow.get(pos));
							i++;
						}
						catch (final Exception e)
						{
							HRDBMSWorker.logger.error("Row - " + lRow, e);
							HRDBMSWorker.logger.error("Cols2Pos = " + childCols2Pos);
							HRDBMSWorker.logger.error("Children are " + HashJoinOperator.this.children.get(0) + " and " + HashJoinOperator.this.children.get(1));
							outBuffer.put(e);
							return;
						}
					}

					final long hash = 0x7FFFFFFFFFFFFFFFL & hash(key);
					final ArrayList<ArrayList<Object>> candidates = getCandidates(hash, types2);

					boolean found = false;
					int at = 0;
					final int limit = candidates.size();
					// for (final ArrayList<Object> rRow : candidates)
					while (at < limit)
					{
						ArrayList<Object> rRow = candidates.get(at);
						if (cnfFilters.passes(lRow, rRow))
						{
							if (semi != null)
							{
								outBuffer.put(lRow);
								break;
							}
							else if (anti != null)
							{
								found = true;
								break;
							}
							else
							{
								final ArrayList<Object> out = new ArrayList<Object>(lRow.size() + rRow.size());
								out.addAll(lRow);
								out.addAll(rRow);
								outBuffer.put(out);
								// if (rhsUnique)
								// {
								// break;
								// }
							}
						}

						at++;
					}

					if (anti != null && !found)
					{
						outBuffer.put(lRow);
					}

					// leftCount.incrementAndGet();
					o = left.next(HashJoinOperator.this);
					if (o instanceof DataEndMarker)
					{
						demReceived = true;
						// HRDBMSWorker.logger.debug(HashJoinOperator.this +
						// " ProcessThread received DEM");
					}
					else
					{
						received.getAndIncrement();
					}
				}
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("Error in hash join reader thread.", e);
				try
				{
					outBuffer.put(e);
				}
				catch (Exception f)
				{
				}
				return;
			}
		}
	}

	private class ReadDataThread extends HRDBMSThread
	{
		private final FileChannel fc;
		private boolean ok = true;
		private Exception e;
		private ArrayList<ArrayList<Object>> data;
		private final byte[] types;
		private final ArrayList<ArrayList<ArrayList<Object>>> bins;
		private final int inMemBins;
		public int index;
		private int pri;

		public ReadDataThread(FileChannel fc, byte[] types, ArrayList<ArrayList<ArrayList<Object>>> bins, int inMemBins, int index) throws Exception
		{
			this.fc = new BufferedFileChannel(fc, 8*1024*1024);
			data = new ArrayList<ArrayList<Object>>();
			this.types = types;
			this.bins = bins;
			this.inMemBins = inMemBins;
			this.index = index;
		}
		
		public void setPriority(int pri)
		{
			this.pri = pri;
		}

		public void clear()
		{
			data = null;
		}

		public ArrayList<ArrayList<Object>> getData()
		{
			return data;
		}

		public Exception getException()
		{
			return e;
		}

		public boolean getOK()
		{
			return ok;
		}

		@Override
		public void run()
		{
			// HRDBMSWorker.logger.debug(HashJoinOperator.this +
			// ": ReadDataThread #" + index + " started");
			Thread.currentThread().setPriority(pri);
			if (index < inMemBins)
			{
				data = bins.get(index);
				// HRDBMSWorker.logger.debug(HashJoinOperator.this +
				// ": ReadDataThread #" + index + " completed");
				return;
			}

			try
			{
				fc.position(0);
				ByteBuffer bb1 = ByteBuffer.allocate(4);
				ArrayList<Object> key = new ArrayList<Object>(lefts.size());
				int[] poses = new int[lefts.size()];
				final Operator child = children.get(0);
				final HashMap<String, Integer> childCols2Pos = child.getCols2Pos();
				int i = 0;
				for (String col : lefts)
				{
					poses[i] = childCols2Pos.get(col);
					i++;
				}
				while (true)
				{
					bb1.position(0);
					if (fc.read(bb1) == -1)
					{
						// HRDBMSWorker.logger.debug(HashJoinOperator.this +
						// ": ReadDataThread #" + index + " completed");
						return;
					}
					bb1.position(0);
					int length = bb1.getInt();
					ByteBuffer bb = ByteBuffer.allocate(length);
					fc.read(bb);
					ArrayList<Object> row = null;
					try
					{
						row = (ArrayList<Object>)fromBytes(bb.array(), types);
					}
					catch(Exception e)
					{
						HRDBMSWorker.logger.debug("", e);
					}
					key.clear();
					for (int pos : poses)
					{
						key.add(row.get(pos));
					}

					final long hash = 0x7FFFFFFFFFFFFFFFL & hash(key);
					if (anti == null && bf != null && !bf.passes(hash))
					{
						continue;
					}
					else if (anti != null && bf != null && !bf.passes(hash, key))
					{
						continue;
					}
					data.add(row);
				}
			}
			catch (Throwable e)
			{
				// HRDBMSWorker.logger.debug(HashJoinOperator.this +
				// ": ReadDataThread #" + index + " completed with exception");
				HRDBMSWorker.logger.debug("", e);
				ok = false;
				if (e instanceof Exception)
				{
					this.e = (Exception)e;
				}
				else
				{
					this.e = new Exception();
				}
			}
		}
	}

	private final class ReaderThread extends ThreadPoolThread
	{
		@Override
		public void run()
		{
			try
			{
				final Operator child = children.get(1);
				final HashMap<String, Integer> childCols2Pos = child.getCols2Pos();
				Object o = child.next(HashJoinOperator.this);
				if (o instanceof DataEndMarker)
				{
					demReceived = true;
					// HRDBMSWorker.logger.debug(HashJoinOperator.this +
					// " ReaderThread received DEM");
				}
				else
				{
					received.getAndIncrement();
				}
				// @Parallel
				int[] poses = new int[rights.size()];
				int i = 0;
				for (String col : rights)
				{
					poses[i] = childCols2Pos.get(col);
					;
					i++;
				}
				final ArrayList<Object> key = new ArrayList<Object>(rights.size());
				while (!(o instanceof DataEndMarker))
				{
					// inCount.incrementAndGet();

					// if (count % 10000 == 0)
					// {
					// System.out.println("HashJoinOperator has read " + count +
					// " rows");
					// }

					i = 0;
					key.clear();
					for (int pos : poses)
					{
						try
						{
							key.add(((ArrayList<Object>)o).get(pos));
							i++;
						}
						catch (Exception e)
						{
							HRDBMSWorker.logger.debug("Failed to find a column in " + childCols2Pos);
							throw e;
						}
					}

					final long hash = 0x7FFFFFFFFFFFFFFFL & hash(key);
					writeToHashTable(hash, toBytes(o));
					o = child.next(HashJoinOperator.this);
					if (o instanceof DataEndMarker)
					{
						demReceived = true;
						// HRDBMSWorker.logger.debug(HashJoinOperator.this +
						// " ReaderThread received DEM");
					}
					else
					{
						received.getAndIncrement();
					}
				}
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("Error in hash join reader thread.", e);
				try
				{
					outBuffer.put(e);
				}
				catch (Exception f)
				{
				}
				return;
			}
		}
	}

	private class RightThread extends HRDBMSThread
	{
		private final ArrayList<FileChannel> channels;
		private final int numBins;
		private final byte[] types;
		private boolean ok = true;
		private Exception e;
		private BloomFilter tempBF;
		private final int inMemBins;
		private ArrayList<ArrayList<ArrayList<Object>>> bins;
		private ScalableStampedReentrantRWLock rwLock;
		private ArrayList<ByteBuffer> directs;

		public RightThread(ArrayList<RandomAccessFile> files, ArrayList<FileChannel> channels, int numBins, byte[] types, int inMemBins)
		{
			this.channels = channels;
			this.numBins = numBins;
			this.types = types;
			this.inMemBins = inMemBins;

			if (anti == null)
			{
				tempBF = new BloomFilter();
			}
			else
			{
				boolean doIt = true;
				HashSet<HashMap<Filter, Filter>> hshm = cnfFilters.getHSHM();
				for (HashMap<Filter, Filter> hm : hshm)
				{
					if (hm.size() != 1)
					{
						doIt = false;
						break;
					}

					for (Filter f : hm.keySet())
					{
						if (!f.op().equals("E"))
						{
							doIt = false;
							break;
						}
					}
				}

				if (doIt)
				{
					tempBF = new BloomFilter(true);
					bf = tempBF;
				}
			}
		}

		public ArrayList<ArrayList<ArrayList<Object>>> getBins()
		{
			return bins;
		}

		public Exception getException()
		{
			return e;
		}

		public boolean getOK()
		{
			return ok;
		}

		@Override
		public void run()
		{
			rwLock = new ScalableStampedReentrantRWLock();
			bins = new ArrayList<ArrayList<ArrayList<Object>>>();
			directs = new ArrayList<ByteBuffer>();
			ConcurrentHashMap<Integer, FlushBinThread> threads = new ConcurrentHashMap<Integer, FlushBinThread>();
			int size = (int)(ResourceManager.QUEUE_SIZE * Double.parseDouble(HRDBMSWorker.getHParms().getProperty("external_factor")) / (numBins >> SHIFT));
			int i = 0;

			while (i < numBins)
			{
				bins.add(new ArrayList<ArrayList<Object>>(size));
				directs.add(null);
				i++;
			}
			
			if (HRDBMSWorker.getHParms().getProperty("use_direct_buffers_for_flush").equals("true"))
			{
				i = 0;
				while (i < numBins)
				{
					directs.set(i, TempThread.getDirect());
					i++;
				}
			}
			
			ArrayList<SubRightThread> srThreads = new ArrayList<SubRightThread>();
			i = 0;
			while (i < 1)
			{
				SubRightThread t = new SubRightThread(this, threads, size);
				t.start();
				srThreads.add(t);
				i++;
			}

			try
			{
				final Operator child = children.get(1);
				final HashMap<String, Integer> childCols2Pos = child.getCols2Pos();
				Object o = child.next(HashJoinOperator.this);
				if (o instanceof DataEndMarker)
				{
					// HRDBMSWorker.logger.debug(HashJoinOperator.this +
					// " RightThread received DEM");
					demReceived = true;
				}
				else
				{
					received.getAndIncrement();
				}

				int[] poses = new int[rights.size()];
				i = 0;
				for (String col : rights)
				{
					poses[i] = childCols2Pos.get(col);
					i++;
				}
				final ArrayList<Object> key = new ArrayList<Object>(rights.size());
				
				while (!(o instanceof DataEndMarker))
				{
					i = 0;
					key.clear();
					for (int pos : poses)
					{
						try
						{
							key.add(((ArrayList<Object>)o).get(pos));
							i++;
						}
						catch (Exception e)
						{
							HRDBMSWorker.logger.debug("Failed to find a column in " + childCols2Pos);
							throw e;
						}
					}

					final long hash = 0x7FFFFFFFFFFFFFFFL & hash(key);
					if (anti == null)
					{
						tempBF.add(hash);
					}
					else if (tempBF != null)
					{
						tempBF.add(hash, (ArrayList<Object>)key.clone());
					}

					if (bf2 != null && !bf2.passes(hash))
					{
						o = child.next(HashJoinOperator.this);
						if (o instanceof DataEndMarker)
						{
							demReceived = true;
							// HRDBMSWorker.logger.debug(HashJoinOperator.this +
							// " RightThread received DEM");
						}
						else
						{
							received.getAndIncrement();
						}
						continue;
					}
					int x = (int)(hash % numBins);
					
					rwLock.readLock().lock();
					ArrayList<ArrayList<Object>> bin = bins.get(x);
					synchronized(bin)
					{
						// writeToHashTable(hash, (ArrayList<Object>)o);
						bin.add((ArrayList<Object>)o);
					}
					rwLock.readLock().unlock();

					if (x >= inMemBins && bin.size() >= size)
					{
						rwLock.writeLock().lock();
						bin = bins.get(x);
						if (bin.size() >= size)
						{
							FlushBinThread thread = new FlushBinThread(bin, types, channels.get(x), directs.get(x));
							if (threads.putIfAbsent(x, thread) != null)
							{
								threads.get(x).join();
								if (!threads.get(x).getOK())
								{
									rwLock.writeLock().unlock();
									throw threads.get(x).getException();
								}

								threads.put(x, thread);
							}
							TempThread.start(thread, txnum);
							bins.set(x, new ArrayList<ArrayList<Object>>(size));
						}
						
						rwLock.writeLock().unlock();
					}

					o = child.next(HashJoinOperator.this);
					if (o instanceof DataEndMarker)
					{
						demReceived = true;
						// HRDBMSWorker.logger.debug(HashJoinOperator.this +
						// " RightThread received DEM");
					}
					else
					{
						received.getAndIncrement();
					}
				}
				
				for (SubRightThread t : srThreads)
				{
					t.join();
					if (!t.getOK())
					{
						throw t.getException();
					}
				}

				bf = tempBF;

				i = 0;
				for (ArrayList<ArrayList<Object>> bin : bins)
				{
					if (i >= inMemBins && bin.size() > 0)
					{
						FlushBinThread thread = new FlushBinThread(bin, types, channels.get(i), directs.get(i), true);
						if (threads.putIfAbsent(i, thread) != null)
						{
							threads.get(i).join();
							if (!threads.get(i).getOK())
							{
								throw threads.get(i).getException();
							}

							threads.put(i, thread);
						}
						TempThread.start(thread, txnum);
					}

					i++;
				}

				for (FlushBinThread thread : threads.values())
				{
					thread.join();
					if (!thread.getOK())
					{
						throw thread.getException();
					}
				}

				// HRDBMSWorker.logger.debug(HashJoinOperator.this +
				// " RightThread wrote everything to disk");
				
				// everything is written
				i = 0;
				while (i < numBins)
				{
					ByteBuffer bb = directs.get(i);
					if (bb != null)
					{
						TempThread.freeDirect(bb);
					}
					
					i++;
				}
				
				i = numBins - 1;
				while (i >= inMemBins)
				{
					bins.remove(i);
					i--;
				}
			}
			catch (final Exception e)
			{
				ok = false;
				this.e = e;
			}
		}
	}
	
	private final class SubRightThread extends HRDBMSThread
	{
		private RightThread rightThread;
		private ConcurrentHashMap<Integer, FlushBinThread> threads;
		private final int size;
		private boolean ok = true;
		private Exception e;
		
		public SubRightThread(RightThread rightThread, ConcurrentHashMap<Integer, FlushBinThread> threads, int size)
		{
			this.rightThread = rightThread;
			this.threads = threads;
			this.size = size;
		}
		
		public boolean getOK()
		{
			return ok;
		}
		
		public Exception getException()
		{
			return e;
		}
		
		public void run()
		{
			try
			{
				final Operator child = children.get(1);
				final HashMap<String, Integer> childCols2Pos = child.getCols2Pos();
				Object o = child.next(HashJoinOperator.this);
				if (o instanceof DataEndMarker)
				{
					// HRDBMSWorker.logger.debug(HashJoinOperator.this +
					// " RightThread received DEM");
					demReceived = true;
				}
				else
				{
					received.getAndIncrement();
				}

				int[] poses = new int[rights.size()];
				int i = 0;
				for (String col : rights)
				{
					poses[i] = childCols2Pos.get(col);
					i++;
				}
				final ArrayList<Object> key = new ArrayList<Object>(rights.size());
				
				while (!(o instanceof DataEndMarker))
				{
					i = 0;
					key.clear();
					for (int pos : poses)
					{
						try
						{
							key.add(((ArrayList<Object>)o).get(pos));
							i++;
						}
						catch (Exception e)
						{
							HRDBMSWorker.logger.debug("Failed to find a column in " + childCols2Pos);
							throw e;
						}
					}

					final long hash = 0x7FFFFFFFFFFFFFFFL & hash(key);
					if (anti == null)
					{
						rightThread.tempBF.add(hash);
					}
					else if (rightThread.tempBF != null)
					{
						rightThread.tempBF.add(hash, (ArrayList<Object>)key.clone());
					}

					if (bf2 != null && !bf2.passes(hash))
					{
						o = child.next(HashJoinOperator.this);
						if (o instanceof DataEndMarker)
						{
							demReceived = true;
							// HRDBMSWorker.logger.debug(HashJoinOperator.this +
							// " RightThread received DEM");
						}
						else
						{
							received.getAndIncrement();
						}
						continue;
					}
					int x = (int)(hash % rightThread.numBins);
					
					rightThread.rwLock.readLock().lock();
					ArrayList<ArrayList<Object>> bin = rightThread.bins.get(x);
					synchronized(bin)
					{
						// writeToHashTable(hash, (ArrayList<Object>)o);
						bin.add((ArrayList<Object>)o);
					}
					rightThread.rwLock.readLock().unlock();

					if (x >= rightThread.inMemBins && bin.size() >= size)
					{
						rightThread.rwLock.writeLock().lock();
						bin = rightThread.bins.get(x);
						if (bin.size() >= size)
						{
							FlushBinThread thread = new FlushBinThread(bin, rightThread.types, rightThread.channels.get(x), rightThread.directs.get(x));
							if (threads.putIfAbsent(x, thread) != null)
							{
								threads.get(x).join();
								if (!threads.get(x).getOK())
								{
									rightThread.rwLock.writeLock().unlock();
									throw threads.get(x).getException();
								}

								threads.put(x, thread);
							}
							TempThread.start(thread, txnum);
							rightThread.bins.set(x, new ArrayList<ArrayList<Object>>(size));
						}
						
						rightThread.rwLock.writeLock().unlock();
					}

					o = child.next(HashJoinOperator.this);
					if (o instanceof DataEndMarker)
					{
						demReceived = true;
						// HRDBMSWorker.logger.debug(HashJoinOperator.this +
						// " RightThread received DEM");
					}
					else
					{
						received.getAndIncrement();
					}
				}
			}
			catch(Exception e)
			{
				ok = false;
				this.e = e;
			}
		}
	}
}
