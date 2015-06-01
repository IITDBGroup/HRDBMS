package com.exascale.optimizer;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.Vector;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.managers.ResourceManager;
import com.exascale.managers.ResourceManager.DiskBackedArray;
import com.exascale.managers.ResourceManager.DiskBackedHashMap;
import com.exascale.misc.BufferedLinkedBlockingQueue;
import com.exascale.misc.DataEndMarker;
import com.exascale.misc.MurmurHash;
import com.exascale.misc.MyDate;
import com.exascale.misc.MySimpleDateFormat;
import com.exascale.tables.Plan;
import com.exascale.threads.ThreadPoolThread;

public final class SemiJoinOperator implements Operator, Serializable
{
	private static sun.misc.Unsafe unsafe;

	static
	{
		try
		{
			final Field f = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
			f.setAccessible(true);
			unsafe = (sun.misc.Unsafe)f.get(null);
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
	private transient volatile DiskBackedArray inBuffer;
	private transient int NUM_RT_THREADS;
	private transient int NUM_PTHREADS;
	// private final AtomicLong outCount = new AtomicLong(0);
	// private final AtomicLong inCount = new AtomicLong(0);
	private transient volatile boolean readersDone;
	private ArrayList<String> cols;
	private transient volatile BufferedLinkedBlockingQueue outBuffer;
	private volatile ArrayList<Integer> poses;
	private int childPos = -1;
	private HashSet<HashMap<Filter, Filter>> f = null;
	private int node;
	private boolean indexAccess = false;
	private ArrayList<Index> dynamicIndexes;
	private transient MySimpleDateFormat sdf;
	private transient HashSet<HashMap<Filter, Filter>> hshm;
	private int rightChildCard = 16;
	private transient ArrayList<DiskBackedHashMap> buckets;
	private transient ReentrantLock bucketsLock;
	// private final AtomicLong inCount2 = new AtomicLong(0);
	private boolean alreadySorted = false;
	private boolean cardSet = false;

	private transient Vector<Operator> clones;

	private transient Vector<AtomicBoolean> lockVector;

	public SemiJoinOperator(ArrayList<String> cols, MetaData meta)
	{
		this.cols = cols;
		this.meta = meta;
	}

	public SemiJoinOperator(HashSet<HashMap<Filter, Filter>> f, MetaData meta)
	{
		this.f = f;
		this.meta = meta;
		this.cols = new ArrayList<String>(0);
	}

	public SemiJoinOperator(String col, MetaData meta)
	{
		this.cols = new ArrayList<String>(1);
		this.cols.add(col);
		this.meta = meta;
	}

	private SemiJoinOperator(ArrayList<String> cols, HashSet<HashMap<Filter, Filter>> f, MetaData meta)
	{
		this.f = f;
		this.cols = cols;
		this.meta = meta;
	}

	public static SemiJoinOperator deserialize(InputStream in, HashMap<Long, Object> prev) throws Exception
	{
		SemiJoinOperator value = (SemiJoinOperator)unsafe.allocateInstance(SemiJoinOperator.class);
		prev.put(OperatorUtils.readLong(in), value);
		value.children = OperatorUtils.deserializeALOp(in, prev);
		value.parent = OperatorUtils.deserializeOperator(in, prev);
		value.cols2Types = OperatorUtils.deserializeStringHM(in, prev);
		value.cols2Pos = OperatorUtils.deserializeStringIntHM(in, prev);
		value.pos2Col = OperatorUtils.deserializeTM(in, prev);
		value.meta = new MetaData();
		value.cols = OperatorUtils.deserializeALS(in, prev);
		value.poses = OperatorUtils.deserializeALI(in, prev);
		value.childPos = OperatorUtils.readInt(in);
		value.f = OperatorUtils.deserializeHSHM(in, prev);
		value.node = OperatorUtils.readInt(in);
		value.indexAccess = OperatorUtils.readBool(in);
		value.dynamicIndexes = OperatorUtils.deserializeALIndx(in, prev);
		value.rightChildCard = OperatorUtils.readInt(in);
		value.alreadySorted = OperatorUtils.readBool(in);
		value.cardSet = OperatorUtils.readBool(in);
		return value;
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

		int size = val.size() + 8;
		final byte[] header = new byte[size];
		int i = 8;
		for (final Object o : val)
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
		System.arraycopy(header, 0, retval, 0, header.length);
		i = 8;
		final ByteBuffer retvalBB = ByteBuffer.wrap(retval);
		retvalBB.putInt(size - 4);
		retvalBB.putInt(val.size());
		retvalBB.position(header.length);
		int x = 0;
		for (final Object o : val)
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
			else if (retval[i] == 8)
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
				cols2Types = children.get(0).getCols2Types();
				cols2Pos = children.get(0).getCols2Pos();
				pos2Col = children.get(0).getPos2Col();

				poses = new ArrayList<Integer>(cols.size());
				for (final String col : cols)
				{
					poses.add(cols2Pos.get(col));
				}
			}
		}
		else
		{
			throw new Exception("SemiJoinOperator only supports 2 children");
		}
	}

	public void alreadySorted()
	{
		alreadySorted = true;
	}

	@Override
	public ArrayList<Operator> children()
	{
		return children;
	}

	@Override
	public SemiJoinOperator clone()
	{
		final SemiJoinOperator retval = new SemiJoinOperator(cols, f, meta);
		retval.node = node;
		retval.indexAccess = indexAccess;
		retval.dynamicIndexes = dynamicIndexes;
		retval.alreadySorted = alreadySorted;
		retval.rightChildCard = rightChildCard;
		retval.cardSet = cardSet;
		return retval;
	}

	@Override
	public void close() throws Exception
	{
		for (final Operator child : children)
		{
			child.close();
		}

		if (inBuffer != null)
		{
			inBuffer.close();
		}

		for (final Operator o : clones)
		{
			o.close();
		}

		if (outBuffer != null)
		{
			outBuffer.close();
		}

		cols2Pos = null;
		cols2Types = null;
		pos2Col = null;
		cols = null;
		poses = null;
		f = null;
		dynamicIndexes = null;
		hshm = null;
		buckets = null;
		clones = null;
		lockVector = null;
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
			return f;
		}

		final HashSet<HashMap<Filter, Filter>> retval = new HashSet<HashMap<Filter, Filter>>();
		int i = 0;
		for (final String col : children.get(1).getPos2Col().values())
		{
			Filter filter = null;
			try
			{
				filter = new Filter(cols.get(i), "E", col);
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

	public boolean getIndexAccess()
	{
		return indexAccess;
	}

	public ArrayList<String> getJoinForChild(Operator op)
	{
		if (cols.size() > 0)
		{
			if (op.getCols2Pos().keySet().containsAll(cols))
			{
				return new ArrayList<String>(cols);
			}
			else
			{
				return new ArrayList<String>(op.getCols2Pos().keySet());
			}
		}

		Filter x = null;
		for (final HashMap<Filter, Filter> filters : f)
		{
			if (filters.size() == 1)
			{
				for (final Filter filter : filters.keySet())
				{
					if (filter.op().equals("E"))
					{
						if (filter.leftIsColumn() && filter.rightIsColumn())
						{
							x = filter;
						}
					}

					break;
				}
			}

			if (x != null)
			{
				break;
			}
		}

		if (x == null)
		{
			return null;
		}

		if (op.getCols2Pos().keySet().contains(x.leftColumn()))
		{
			final ArrayList<String> retval = new ArrayList<String>(1);
			retval.add(x.leftColumn());
			return retval;
		}

		final ArrayList<String> retval = new ArrayList<String>(1);
		retval.add(x.rightColumn());
		return retval;
	}

	public ArrayList<String> getLefts()
	{
		if (cols.size() > 0)
		{
			return cols;
		}

		final ArrayList<String> retval = new ArrayList<String>(f.size());
		for (final HashMap<Filter, Filter> filters : f)
		{
			if (filters.size() == 1)
			{
				for (final Filter filter : filters.keySet())
				{
					if (filter.op().equals("E"))
					{
						if (children.get(0).getCols2Pos().keySet().contains(filter.leftColumn()))
						{
							retval.add(filter.leftColumn());
						}
						else
						{
							retval.add(filter.rightColumn());
						}
					}
				}
			}
		}

		return retval;
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
		final ArrayList<String> retval = new ArrayList<String>(cols);
		retval.addAll(children.get(1).getCols2Pos().keySet());

		if (f != null)
		{
			for (final HashMap<Filter, Filter> filters : f)
			{
				for (final Filter filter : filters.keySet())
				{
					if (filter.leftIsColumn())
					{
						retval.add(filter.leftColumn());
					}

					if (filter.rightIsColumn())
					{
						retval.add(filter.rightColumn());
					}
				}
			}
		}
		return retval;
	}

	public ArrayList<String> getRights()
	{
		if (cols.size() > 0)
		{
			final ArrayList<String> retval = new ArrayList<String>(children.get(1).getCols2Pos().keySet());
			return retval;
		}

		final ArrayList<String> retval = new ArrayList<String>(f.size());
		for (final HashMap<Filter, Filter> filters : f)
		{
			if (filters.size() == 1)
			{
				for (final Filter filter : filters.keySet())
				{
					if (filter.op().equals("E"))
					{
						if (children.get(1).getCols2Pos().keySet().contains(filter.leftColumn()))
						{
							retval.add(filter.leftColumn());
						}
						else
						{
							retval.add(filter.rightColumn());
						}
					}
				}
			}
		}

		return retval;
	}

	@Override
	public Object next(Operator op) throws Exception
	{
		if (indexAccess)
		{
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

				if (hshm == null)
				{
					hshm = getHSHM();
				}
				final ArrayList<Filter> dynamics = new ArrayList<Filter>(hshm.size());
				for (final HashMap<Filter, Filter> hm : hshm)
				{
					final Filter f = new ArrayList<Filter>(hm.keySet()).get(0);
					String leftCol = null;
					String rightCol = null;
					if (children.get(0).getCols2Pos().keySet().contains(f.leftColumn()))
					{
						leftCol = f.leftColumn();
						final Object leftVal = ((ArrayList<Object>)o).get(children.get(0).getCols2Pos().get(leftCol));
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
						final Filter f2 = new Filter(leftString, f.op(), f.rightColumn());
						dynamics.add(f2);
					}
					else
					{
						rightCol = f.rightColumn();
						final Object leftVal = ((ArrayList<Object>)o).get(children.get(0).getCols2Pos().get(rightCol));
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
						final Filter f2 = new Filter(f.leftColumn(), f.op(), leftString);
						dynamics.add(f2);
					}
				}

				Operator clone = null;
				boolean retval = false;
				clone = getClone();
				synchronized (clone)
				{
					clone.reset();
					for (final Index index : dynamicIndexes(children.get(1), clone.children().get(0)))
					{
						index.setDelayedConditions(deepClone(dynamics));
					}

					final Object o2 = clone.next(this);
					if (!(o2 instanceof DataEndMarker))
					{
						retval = true;
					}

					clone.nextAll(SemiJoinOperator.this);
					freeClone(clone);
				}
				if (retval)
				{
					return o;
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
	public Operator parent()
	{
		return parent;
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
			throw new Exception("SemiJoinOperator can only have 1 parent.");
		}
	}

	@Override
	public void removeChild(Operator op)
	{
		childPos = children.indexOf(op);
		if (childPos == -1)
		{
			Exception e = new Exception();
			HRDBMSWorker.logger.error("Removing a non-existent child!", e);
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
	public void reset() throws Exception
	{
		HRDBMSWorker.logger.error("SemiJoinOperator cannot be reset");
		throw new Exception("SemiJoinOperator cannot be reset");
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

		OperatorUtils.writeType(57, out);
		prev.put(this, OperatorUtils.writeID(out));
		OperatorUtils.serializeALOp(children, out, prev);
		parent.serialize(out, prev);
		OperatorUtils.serializeStringHM(cols2Types, out, prev);
		OperatorUtils.serializeStringIntHM(cols2Pos, out, prev);
		OperatorUtils.serializeTM(pos2Col, out, prev);
		// create new meta on receive side
		OperatorUtils.serializeALS(cols, out, prev);
		OperatorUtils.serializeALI(poses, out, prev);
		OperatorUtils.writeInt(childPos, out);
		OperatorUtils.serializeHSHM(f, out, prev);
		OperatorUtils.writeInt(node, out);
		OperatorUtils.writeBool(indexAccess, out);
		OperatorUtils.serializeALIndx(dynamicIndexes, out, prev);
		OperatorUtils.writeInt(rightChildCard, out);
		OperatorUtils.writeBool(alreadySorted, out);
		OperatorUtils.writeBool(cardSet, out);
	}

	@Override
	public void setChildPos(int pos)
	{
		childPos = pos;
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

	public boolean setRightChildCard(int card)
	{
		if (cardSet)
		{
			return false;
		}

		cardSet = true;
		rightChildCard = card;
		return true;
	}

	public ArrayList<String> sortKeys()
	{
		if (cols.size() > 0)
		{
			return null;
		}

		for (final HashMap<Filter, Filter> filters : f)
		{
			if (filters.size() == 1)
			{
				for (final Filter filter : filters.keySet())
				{
					if (filter.op().equals("G") || filter.op().equals("GE") || filter.op().equals("L") || filter.op().equals("LE"))
					{
						if (filter.leftIsColumn() && filter.rightIsColumn())
						{
							String vStr;
							if (filter.op().equals("G") || filter.op().equals("GE"))
							{
								vStr = filter.rightColumn();
								// vBool = true;
								// System.out.println("VBool set to true");
							}
							else
							{
								vStr = filter.rightColumn();
								// vBool = false;
								// System.out.println("VBool set to false");
							}

							try
							{
								children.get(1).getCols2Pos().get(vStr);
							}
							catch (final Exception e)
							{
								vStr = filter.leftColumn();
								// vBool = !vBool;
								// pos =
								// children.get(1).getCols2Pos().get(vStr);
							}

							final ArrayList<String> retval = new ArrayList<String>(1);
							retval.add(vStr);
							return retval;
						}
					}
					else if (filter.op().equals("E") && filter.leftIsColumn() && filter.rightIsColumn())
					{
						return null;
					}
				}
			}
		}

		return null;
	}

	public ArrayList<Boolean> sortOrders()
	{
		if (cols.size() > 0)
		{
			return null;
		}

		for (final HashMap<Filter, Filter> filters : f)
		{
			if (filters.size() == 1)
			{
				for (final Filter filter : filters.keySet())
				{
					if (filter.op().equals("G") || filter.op().equals("GE") || filter.op().equals("L") || filter.op().equals("LE"))
					{
						if (filter.leftIsColumn() && filter.rightIsColumn())
						{
							String vStr;
							boolean vBool;
							if (filter.op().equals("G") || filter.op().equals("GE"))
							{
								vStr = filter.rightColumn();
								vBool = true;
								// System.out.println("VBool set to true");
							}
							else
							{
								vStr = filter.rightColumn();
								vBool = false;
								// System.out.println("VBool set to false");
							}

							try
							{
								children.get(1).getCols2Pos().get(vStr);
							}
							catch (final Exception e)
							{
								// vStr = filter.leftColumn();
								vBool = !vBool;
								// pos =
								// children.get(1).getCols2Pos().get(vStr);
							}

							final ArrayList<Boolean> retval = new ArrayList<Boolean>(1);
							retval.add(vBool);
							return retval;
						}
					}
					else if (filter.op().equals("E") && filter.leftIsColumn() && filter.rightIsColumn())
					{
						return null;
					}
				}
			}
		}

		return null;
	}

	@Override
	public void start() throws Exception
	{
		NUM_RT_THREADS = 6 * ResourceManager.cpus;
		NUM_PTHREADS = 6 * ResourceManager.cpus;
		readersDone = false;
		sdf = new MySimpleDateFormat("yyyy-MM-dd");
		buckets = new ArrayList<DiskBackedHashMap>();
		bucketsLock = new ReentrantLock();
		clones = new Vector<Operator>();
		lockVector = new Vector<AtomicBoolean>();

		if (!indexAccess)
		{
			for (final Operator child : children)
			{
				child.start();
			}

			inBuffer = ResourceManager.newDiskBackedArray(true, rightChildCard);
			outBuffer = new BufferedLinkedBlockingQueue(ResourceManager.QUEUE_SIZE);
			new InitThread().start();
		}
		else
		{
			for (final Operator child : children)
			{
				child.start();
			}

			outBuffer = new BufferedLinkedBlockingQueue(ResourceManager.QUEUE_SIZE);
		}
	}

	@Override
	public String toString()
	{
		return "SemiJoinOperator";
	}

	public boolean usesHash()
	{
		if (cols.size() > 0)
		{
			return true;
		}

		for (final HashMap<Filter, Filter> filters : f)
		{
			if (filters.size() == 1)
			{
				for (final Filter filter : filters.keySet())
				{
					if (filter.op().equals("E") && filter.leftIsColumn() && filter.rightIsColumn())
					{
						return true;
					}
				}
			}
		}

		return false;
	}

	public boolean usesSort()
	{
		if (cols.size() > 0)
		{
			return false;
		}

		boolean isSort = false;

		for (final HashMap<Filter, Filter> filters : f)
		{
			if (filters.size() == 1)
			{
				for (final Filter filter : filters.keySet())
				{
					if (filter.op().equals("G") || filter.op().equals("GE") || filter.op().equals("L") || filter.op().equals("LE"))
					{
						if (filter.leftIsColumn() && filter.rightIsColumn())
						{
							isSort = true;
						}
					}
					else if (filter.op().equals("E") && filter.leftIsColumn() && filter.rightIsColumn())
					{
						return false;
					}
				}
			}
		}

		return isSort;
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

	private void freeClone(Operator clone)
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
					lockVector.get(i).set(false);
					return;
				}

				i++;
			}
		}
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

	private final class InitThread extends ThreadPoolThread
	{
		private NLSortThread nlSort = null;
		private NLHashThread nlHash = null;
		private Filter first = null;

		@Override
		public void run()
		{
			ThreadPoolThread[] threads = null;

			int i = 0;
			CNFFilter cnf = null;
			final ThreadPoolThread[] threads2 = new ProcessThread[NUM_PTHREADS];
			if (f != null)
			{
				// System.out.println("Using non-equijoin semijoin!");
				final HashMap<String, Integer> tempC2P = (HashMap<String, Integer>)cols2Pos.clone();
				for (final Map.Entry entry : children.get(1).getPos2Col().entrySet())
				{
					tempC2P.put((String)entry.getValue(), tempC2P.size());
				}

				cnf = new CNFFilter(f, tempC2P);

				boolean isHash = false;
				boolean isSort = false;

				for (final HashMap<Filter, Filter> filters : f)
				{
					if (filters.size() == 1)
					{
						for (final Filter filter : filters.keySet())
						{
							if (filter.op().equals("G") || filter.op().equals("GE") || filter.op().equals("L") || filter.op().equals("LE"))
							{
								if (filter.leftIsColumn() && filter.rightIsColumn())
								{
									isSort = true;
								}
							}
							else if (filter.op().equals("E") && filter.leftIsColumn() && filter.rightIsColumn())
							{
								isHash = true;
							}
						}
					}
				}

				if (isHash)
				{
					for (final HashMap<Filter, Filter> filters : f)
					{
						if (filters.size() == 1)
						{
							for (final Filter filter : filters.keySet())
							{
								if (filter.op().equals("E"))
								{
									if (filter.leftIsColumn() && filter.rightIsColumn())
									{
										// System.out.println("NestedLoopJoin qualifies for partial hashing");
										int j = 0;
										final ArrayList<NLHashThread> nlThreads = new ArrayList<NLHashThread>(NUM_RT_THREADS);
										while (j < NUM_RT_THREADS)
										{
											nlHash = new NLHashThread(filter);
											nlThreads.add(nlHash);
											first = filter;
											nlHash.start();
											j++;
										}
										readersDone = true;
										for (final NLHashThread thread : nlThreads)
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
										}
									}
								}

								break;
							}
						}

						if (nlHash != null)
						{
							break;
						}
					}
				}
				else if (isSort)
				{
					i = 0;
					if (alreadySorted)
					{
						NUM_RT_THREADS = 1;
					}
					threads = new ReaderThread[NUM_RT_THREADS];
					while (i < NUM_RT_THREADS)
					{
						threads[i] = new ReaderThread();
						threads[i].start();
						i++;
					}

					for (final HashMap<Filter, Filter> filters : f)
					{
						if (filters.size() == 1)
						{
							for (final Filter filter : filters.keySet())
							{
								if (filter.op().equals("G") || filter.op().equals("GE") || filter.op().equals("L") || filter.op().equals("LE"))
								{
									if (filter.leftIsColumn() && filter.rightIsColumn())
									{
										// System.out.println("NestedLoopJoin qualifies for sorting");
										nlSort = new NLSortThread(filter);
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
										nlSort.start();
										first = filter;
										while (true)
										{
											try
											{
												nlSort.join();
												break;
											}
											catch (final InterruptedException e)
											{
											}
										}
									}
								}

								break;
							}
						}

						if (nlSort != null)
						{
							break;
						}
					}
				}
				else
				{
					i = 0;
					threads = new ReaderThread[NUM_RT_THREADS];
					while (i < NUM_RT_THREADS)
					{
						threads[i] = new ReaderThread();
						threads[i].start();
						i++;
					}
				}

				i = 0;
				while (i < NUM_PTHREADS)
				{
					if (nlSort != null)
					{
						threads2[i] = new ProcessThread(cnf, first);
					}
					else if (nlHash != null)
					{
						threads2[i] = new ProcessThread(cnf, nlHash, first);
					}
					else
					{
						threads2[i] = new ProcessThread(cnf);
					}
					threads2[i].start();
					i++;
				}
			}
			else
			{
				i = 0;
				threads = new ReaderThread[NUM_RT_THREADS];
				while (i < NUM_RT_THREADS)
				{
					threads[i] = new ReaderThread();
					threads[i].start();
					i++;
				}

				i = 0;
				while (i < NUM_PTHREADS)
				{
					threads2[i] = new ProcessThread();
					threads2[i].start();
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
				// System.out.println("SemiJoin readers are done");
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

			if (nlHash != null)
			{
				try
				{
					nlHash.close();
				}
				catch (Exception e)
				{
				}
			}

			// System.out.println("SemiJoinOperator output " + outCount.get() +
			// " rows");

			while (true)
			{
				try
				{
					outBuffer.put(new DataEndMarker());
					break;
				}
				catch (Exception e)
				{
				}
				try
				{
					inBuffer.close();
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.error("", e);
				}
			}
		}
	}

	private final class NLHashThread extends ThreadPoolThread
	{
		private final Filter filter;
		private int pos;

		public NLHashThread(Filter filter)
		{
			this.filter = filter;
		}

		public void close() throws Exception
		{
			for (final DiskBackedHashMap map : buckets)
			{
				try
				{
					map.close();
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.error("", e);
					throw e;
				}
			}
		}

		@Override
		public void run()
		{
			try
			{
				pos = children.get(1).getCols2Pos().get(filter.rightColumn());
			}
			catch (final Exception e)
			{
				pos = children.get(1).getCols2Pos().get(filter.leftColumn());
			}

			try
			{
				final Operator child = children.get(1);
				child.getCols2Pos();
				Object o = child.next(SemiJoinOperator.this);
				final ArrayList<Object> key = new ArrayList<Object>(1);
				// @Parallel
				while (!(o instanceof DataEndMarker))
				{
					// inCount2.incrementAndGet();

					// if (count % 10000 == 0)
					// {
					// System.out.println("HashJoinOperator has read " + count +
					// " rows");
					// }

					key.clear();
					key.add(((ArrayList<Object>)o).get(pos));

					final long hash = 0x0EFFFFFFFFFFFFFFL & hash(key);
					writeToHashTable(hash, (ArrayList<Object>)o);
					o = child.next(SemiJoinOperator.this);
				}
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("Error in partial hash thread.", e);
				HRDBMSWorker.logger.error("Left child cols to pos is: " + children.get(0).getCols2Pos());
				HRDBMSWorker.logger.error("Right child cols to pos is: " + children.get(1).getCols2Pos());
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

		private final ArrayList<Object> getCandidate(long hash, int pos) throws Exception
		{
			int size = buckets.size();

			if (size == 0)
			{
				return null;
			}

			if (pos >= size)
			{
				return null;
			}

			DiskBackedHashMap dbhm = buckets.get(pos);
			return dbhm.get(hash);
		}

		private long hash(Object key) throws Exception
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

		private final void writeToHashTable(long hash, ArrayList<Object> row) throws Exception
		{
			if (buckets.size() == 0)
			{
				synchronized (buckets)
				{
					if (buckets.size() == 0)
					{
						buckets.add(ResourceManager.newDiskBackedHashMap(false, rightChildCard));
					}
				}
			}

			int i = 0;
			int min = -1;
			int max = buckets.size() - 1;
			while (max - min >= 4)
			{
				boolean exists = buckets.get(((max - min) >> 1) + min).exists(hash);
				if (!exists)
				{
					max = ((max - min) >> 1) + min - 1;
				}
				else
				{
					min = ((max - min) >> 1) + min;
				}
			}

			// if (max - min == 1)
			// {
			// Object o = buckets.get(max).get(hash);
			// if (o != null)
			// {
			// min = max;
			// }
			// }

			i = min + 1;
			Object o = 0;
			while (o != null)
			{
				if (i < buckets.size())
				{
					o = null;
					while (o == null)
					{
						o = buckets.get(i);
					}
					o = ((DiskBackedHashMap)o).putIfAbsent(hash, row);
				}
				else
				{
					// synchronized(buckets)
					bucketsLock.lock();
					try
					{
						if (i < buckets.size())
						{
							bucketsLock.unlock();
							o = null;
							while (o == null)
							{
								o = buckets.get(i);
							}
							o = ((DiskBackedHashMap)o).putIfAbsent(hash, row);
						}
						else
						{
							o = ResourceManager.newDiskBackedHashMap(false, rightChildCard / buckets.size());
							((DiskBackedHashMap)o).put(hash, row);
							buckets.add((DiskBackedHashMap)o);
							bucketsLock.unlock();
							o = null;
						}
					}
					catch (Exception e)
					{
						bucketsLock.unlock();
						throw e;
					}
				}

				i++;
			}

			return;
		}
	}

	private final class NLSortThread extends ThreadPoolThread
	{
		private final Filter filter;

		private boolean vBool;
		private int pos;

		public NLSortThread(Filter filter)
		{
			this.filter = filter;
			// System.out.println("NLSortThread: " + filter);
		}

		@Override
		public void run()
		{
			String vStr;
			if (filter.op().equals("G") || filter.op().equals("GE"))
			{
				vStr = filter.rightColumn();
				vBool = true;
				// System.out.println("VBool set to true");
			}
			else
			{
				vStr = filter.rightColumn();
				vBool = false;
				// System.out.println("VBool set to false");
			}

			try
			{
				pos = children.get(1).getCols2Pos().get(vStr);
			}
			catch (final Exception e)
			{
				vStr = filter.leftColumn();
				vBool = !vBool;
				pos = children.get(1).getCols2Pos().get(vStr);
			}

			try
			{
				if (alreadySorted)
				{
					return;
				}
				ResourceManager.NO_OFFLOAD.getAndIncrement();
				ResourceManager.waitForSync();
				if (inBuffer.size() < SortOperator.PARALLEL_SORT_MIN_NUM_ROWS)
				{
					doSequentialSort(0, inBuffer.size() - 1);
				}
				else
				{
					final ParallelSortThread t = doParallelSort(0, inBuffer.size() - 1);
					t.join();
				}
				ResourceManager.NO_OFFLOAD.getAndDecrement();
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
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

		private int compare(ArrayList<Object> lhs, ArrayList<Object> rhs) throws Exception
		{
			int result;
			final Object lField = lhs.get(pos);
			final Object rField = rhs.get(pos);

			if (lField instanceof Integer)
			{
				result = ((Integer)lField).compareTo((Integer)rField);
			}
			else if (lField instanceof Long)
			{
				result = ((Long)lField).compareTo((Long)rField);
			}
			else if (lField instanceof Double)
			{
				result = ((Double)lField).compareTo((Double)rField);
			}
			else if (lField instanceof String)
			{
				result = ((String)lField).compareTo((String)rField);
			}
			else if (lField instanceof MyDate)
			{
				result = ((MyDate)lField).compareTo(rField);
			}
			else
			{
				throw new Exception("Unknown type in SortOperator.compare(): " + lField.getClass());
			}

			if (vBool)
			{
				if (result > 0)
				{
					return 1;
				}
				else if (result < 0)
				{
					return -1;
				}
			}
			else
			{
				if (result > 0)
				{
					return -1;
				}
				else if (result < 0)
				{
					return 1;
				}
			}

			return 0;
		}

		private ParallelSortThread doParallelSort(long left, long right)
		{
			// System.out.println("Starting parallel sort with " +
			// (right-left+1) + " rows");
			final ParallelSortThread t = new ParallelSortThread(left, right);
			t.start();
			return t;
		}

		private void doSequentialSort(long left, long right) throws Exception
		{
			if (right <= left)
			{
				return;
			}
			long lt = left;
			long gt = right;
			final ArrayList<Object> v = (ArrayList<Object>)inBuffer.get(left);
			long i = left;
			while (i <= gt)
			{
				final ArrayList<Object> temp = (ArrayList<Object>)inBuffer.get(i);
				final int cmp = compare(temp, v);
				if (cmp < 0)
				{
					inBuffer.update(i, (ArrayList<Object>)inBuffer.get(lt));
					inBuffer.update(lt, temp);
					i++;
					lt++;
				}
				else if (cmp > 0)
				{
					inBuffer.update(i, (ArrayList<Object>)inBuffer.get(gt));
					inBuffer.update(gt, temp);
					gt--;
				}
				else
				{
					i++;
				}
			}

			doSequentialSort(left, lt - 1);
			doSequentialSort(gt + 1, right);
		}

		private final class ParallelSortThread extends ThreadPoolThread
		{
			private final long left;
			private final long right;

			public ParallelSortThread(long left, long right)
			{
				this.left = left;
				this.right = right;
			}

			@Override
			public void run()
			{
				try
				{
					if (((right - left) < SortOperator.PARALLEL_SORT_MIN_NUM_ROWS))
					{
						doSequentialSort(left, right);
					}
					else
					{
						if (right <= left)
						{
							return;
						}
						final long pivotIndex = (long)(ThreadLocalRandom.current().nextDouble() * (right - left) + left);
						ArrayList<Object> temp = (ArrayList<Object>)inBuffer.get(pivotIndex);
						inBuffer.update(pivotIndex, (ArrayList<Object>)inBuffer.get(left));
						inBuffer.update(left, temp);
						long lt = left;
						long gt = right;
						final ArrayList<Object> v = temp;
						long i = left;
						while (i <= gt)
						{
							temp = (ArrayList<Object>)inBuffer.get(i);
							final int cmp = compare(temp, v);
							if (cmp < 0)
							{
								inBuffer.update(i, (ArrayList<Object>)inBuffer.get(lt));
								inBuffer.update(lt, temp);
								i++;
								lt++;
							}
							else if (cmp > 0)
							{
								inBuffer.update(i, (ArrayList<Object>)inBuffer.get(gt));
								inBuffer.update(gt, temp);
								gt--;
							}
							else
							{
								i++;
							}
						}

						if (lt < gt)
						{
							final ParallelSortThread t1 = doParallelSort(left, lt);
							final ParallelSortThread t2 = doParallelSort(gt, right);
							t1.join();
							t2.join();
						}
						else if (lt == gt)
						{
							if (gt == right)
							{
								lt--;
							}
							else
							{
								gt++;
							}

							final ParallelSortThread t1 = doParallelSort(left, lt);
							final ParallelSortThread t2 = doParallelSort(gt, right);
							t1.join();
							t2.join();
						}
						else
						{
							final long temp2 = gt;
							gt = lt;
							lt = temp2;
							final ParallelSortThread t1 = doParallelSort(left, lt);
							final ParallelSortThread t2 = doParallelSort(gt, right);
							t1.join();
							t2.join();
						}
					}
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.error("", e);
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
	}

	private final class ProcessThread extends ThreadPoolThread
	{
		private CNFFilter cnf = null;
		private Filter first = null;
		private NLHashThread nlHash = null;

		public ProcessThread()
		{
		}

		public ProcessThread(CNFFilter cnf)
		{
			this.cnf = cnf;
		}

		public ProcessThread(CNFFilter cnf, Filter first)
		{
			this.cnf = cnf;
			this.first = first;
		}

		public ProcessThread(CNFFilter cnf, NLHashThread nlHash, Filter first)
		{
			this.cnf = cnf;
			this.nlHash = nlHash;
			this.first = first;
		}

		@Override
		public void run()
		{
			if (cnf != null)
			{
				// System.out.println("SemiJoin ProcessThread started using non-equi-join!");
			}
			try
			{
				final Operator left = children.get(0);
				Object o = left.next(SemiJoinOperator.this);
				int pos = -1;
				HashMap<String, Integer> tempC2P = null;
				if (first != null)
				{
					try
					{
						pos = left.getCols2Pos().get(first.leftColumn());
					}
					catch (final Exception e)
					{
						pos = left.getCols2Pos().get(first.rightColumn());
					}
					tempC2P = (HashMap<String, Integer>)cols2Pos.clone();
					for (final Map.Entry entry : children.get(1).getPos2Col().entrySet())
					{
						tempC2P.put((String)entry.getValue(), tempC2P.size());
					}
				}
				final ArrayList<Object> obj = new ArrayList<Object>(poses.size());
				final ArrayList<Object> key = new ArrayList<Object>(1);
				// @Parallel
				while (!(o instanceof DataEndMarker))
				{
					final ArrayList<Object> lRow = (ArrayList<Object>)o;
					obj.clear();
					if (cnf == null)
					{
						for (final int pos2 : poses)
						{
							obj.add(lRow.get(pos2));
						}
					}

					while (true)
					{
						if (nlHash != null)
						{
							key.clear();
							key.add(lRow.get(pos));
							final long hash = 0x0EFFFFFFFFFFFFFFL & nlHash.hash(key);
							int i = 0;
							ArrayList<Object> rRow = nlHash.getCandidate(hash, i);
							// for (final ArrayList<Object> rRow :
							// nlHash.getCandidates(hash))
							while (rRow != null)
							{
								if (cnf.passes(lRow, rRow))
								{
									outBuffer.put(lRow);
									// final long count =
									// outCount.incrementAndGet();
									// if (count % 100000 == 0)
									// {
									// HRDBMSWorker.logger.debug("SemiJoinOperator has output "
									// + count + " rows");
									// }

									break;
								}

								i++;
								rRow = nlHash.getCandidate(hash, i);
							}

							break;
						}

						// System.out.println("Called inBuufer.contains()");
						if (cnf == null && inBuffer.contains(obj))
						{
							// System.out.println("Call completed successfully");
							outBuffer.put(lRow);
							// outCount.getAndIncrement();
							// if (count % 10000 == 0)
							// {
							// System.out.println("SemiJoinOperator has output "
							// + count + " rows");
							// }
							break;
						}
						else if (cnf == null)
						{
							// System.out.println("Call completed unsuccessfully.");
							// System.out.println("Current size is " +
							// inBuffer.size());
							if (readersDone)
							{
								break;
							}

							LockSupport.parkNanos(75000);
						}
						else
						{
							if (first == null)
							{
								boolean passed = false;
								for (final Object orow : inBuffer)
								{
									final ArrayList<Object> rRow = (ArrayList<Object>)orow;
									if (cnf.passes(lRow, rRow))
									{
										outBuffer.put(lRow);
										// outCount.getAndIncrement();
										// if (count % 10000 == 0)
										// {
										// System.out.println("SemiJoinOperator has output "
										// + count + " rows");
										// }
										passed = true;
										break;
									}
								}

								if (passed)
								{
									break;
								}

								if (readersDone)
								{
									break;
								}

								LockSupport.parkNanos(75000);
							}
							else
							{
								// data is sorted
								for (final Object orow : inBuffer)
								{
									final ArrayList<Object> rRow = (ArrayList<Object>)orow;
									if (cnf.passes(lRow, rRow))
									{
										outBuffer.put(lRow);
										// outCount.getAndIncrement();
										// if (count % 10000 == 0)
										// {
										// System.out.println("SemiJoinOperator has output "
										// + count + " rows");
										// }
										break;
									}
									else
									{
										if (!first.passes(lRow, rRow, tempC2P))
										{
											break;
										}
									}
								}
							}
						}
					}

					o = left.next(SemiJoinOperator.this);
				}
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
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

	private final class ReaderThread extends ThreadPoolThread
	{
		@Override
		public void run()
		{
			try
			{
				final Operator child = children.get(1);
				Object o = child.next(SemiJoinOperator.this);
				while (!(o instanceof DataEndMarker))
				{
					inBuffer.add((ArrayList<Object>)o);
					// long count = inCount.getAndIncrement();
					// if (count % 10000 == 0)
					// {
					// System.out.println("SemiJoinOperator has read " + count +
					// " rows");
					// }
					o = child.next(SemiJoinOperator.this);
				}
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
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
}
