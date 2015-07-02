package com.exascale.optimizer;

import java.io.File;
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
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.managers.ResourceManager;
import com.exascale.misc.BufferedLinkedBlockingQueue;
import com.exascale.misc.DataEndMarker;
import com.exascale.misc.MultiHashMap;
import com.exascale.misc.MurmurHash;
import com.exascale.misc.MyDate;
import com.exascale.misc.MySimpleDateFormat;
import com.exascale.tables.Plan;
import com.exascale.threads.HRDBMSThread;
import com.exascale.threads.ThreadPoolThread;

public final class HashJoinOperator extends JoinOperator implements Serializable
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
	private transient volatile BufferedLinkedBlockingQueue outBuffer;
	private transient int NUM_RT_THREADS;
	private transient int NUM_PTHREADS;
	// private final AtomicLong outCount = new AtomicLong(0);
	private transient volatile boolean readersDone;
	private ArrayList<String> lefts = new ArrayList<String>();
	private ArrayList<String> rights = new ArrayList<String>();
	private transient volatile ArrayList<ConcurrentHashMap<Long, ArrayList<Object>>> buckets;
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
	private boolean cardSet = false;
	private transient Vector<Operator> clones;
	private transient Vector<AtomicBoolean> lockVector;
	private transient ArrayList<String> externalFiles;
	private transient Boolean semi = null;
	private transient Boolean anti = null;

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
	}

	private HashJoinOperator(HashSet<HashMap<Filter, Filter>> f, ArrayList<String> lefts, ArrayList<String> rights, MetaData meta)
	{
		this.meta = meta;
		this.f = f;
		this.lefts = lefts;
		this.rights = rights;
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
		return value;
	}
	
	public void setSemi()
	{
		semi = new Boolean(true);
	}
	
	public void setAnti()
	{
		anti = new Boolean(true);
	}
	
	public void setCNF(HashSet<HashMap<Filter, Filter>> hshm) throws Exception
	{
		cnfFilters = new CNFFilter(hshm, meta, cols2Pos, null, this);
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
		retval.cardSet = cardSet;
		return retval;
	}

	@Override
	public void close() throws Exception
	{
		if (externalFiles != null)
		{
			for (String fn : externalFiles)
			{
				try
				{
					new File(fn).delete();
				}
				catch(Exception e)
				{}
			}
		}
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

	@Override
	public void start() throws Exception
	{
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
			
			if (rightChildCard > ResourceManager.QUEUE_SIZE * Double.parseDouble(HRDBMSWorker.getHParms().getProperty("hash_external_factor")))
			{
				new ExternalThread().start();
			}
			else
			{
				buckets = new ArrayList<ConcurrentHashMap<Long, ArrayList<Object>>>();
				buckets.add(new ConcurrentHashMap<Long, ArrayList<Object>>(rightChildCard));
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
	
	private class ExternalThread extends HRDBMSThread 
	{
		public void run()
		{
			external();
		}
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
	
	private ArrayList<RandomAccessFile> createFiles(ArrayList<String> fns) throws Exception
	{
		ArrayList<RandomAccessFile> retval = new ArrayList<RandomAccessFile>(fns.size());
		for (String fn : fns)
		{
			RandomAccessFile raf = new RandomAccessFile(fn, "rw");
			retval.add(raf);
		}
		
		return retval;
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
	
	private void external()
	{
		try
		{
			int numBins = 4096;
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
			LeftThread thread1 = new LeftThread(files1, channels1, numBins, types1);
			thread1.start();
			ArrayList<String> fns2 = createFNs(numBins, 1);
			externalFiles.addAll(fns2);
			ArrayList<RandomAccessFile> files2 = createFiles(fns2);
			ArrayList<FileChannel> channels2 = createChannels(files2);
			RightThread thread2 = new RightThread(files2, channels2, numBins, types2);
			thread2.start();
			while (true)
			{
				try
				{
					thread1.join();
					break;
				}
				catch(InterruptedException e)
				{}
			}
			if (!thread1.getOK())
			{
				outBuffer.put(thread1.getException());
				return;
			}
			
			ReadDataThread thread3 = new ReadDataThread(channels1.get(0), types1);
			thread3.start();
			ReadDataThread thread4 = new ReadDataThread(channels1.get(1), types1);
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
				catch(InterruptedException e)
				{}
			}	
			if (!thread2.getOK())
			{
				outBuffer.put(thread2.getException());
				return;
			}
		
			HashDataThread thread5 = new HashDataThread(channels2.get(0), types2);
			thread5.start();
			HashDataThread thread6 = new HashDataThread(channels2.get(1), types2);
			thread6.start();
			ArrayList<HashDataThread> rightThreads = new ArrayList<HashDataThread>();
			rightThreads.add(thread5);
			rightThreads.add(thread6);
		
			int i = 2;
			ArrayList<ExternalProcessThread> epThreads = new ArrayList<ExternalProcessThread>();
			
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
				ExternalProcessThread ept = new ExternalProcessThread(left, right);
				if (epThreads.size() > 7)
				{
					epThreads.get(0).join();
					epThreads.remove(0);
					
					if (epThreads.get(0).isDone())
					{
						epThreads.get(0).join();
						epThreads.remove(0);
						
						if (epThreads.get(0).isDone())
						{
							epThreads.get(0).join();
							epThreads.remove(0);
							
							if (epThreads.get(0).isDone())
							{
								epThreads.get(0).join();
								epThreads.remove(0);
								
								if (epThreads.get(0).isDone())
								{
									epThreads.get(0).join();
									epThreads.remove(0);
									
									if (epThreads.get(0).isDone())
									{
										epThreads.get(0).join();
										epThreads.remove(0);
										
										if (epThreads.get(0).isDone())
										{
											epThreads.get(0).join();
											epThreads.remove(0);
											
											if (epThreads.get(0).isDone())
											{
												epThreads.get(0).join();
												epThreads.remove(0);
											}
										}
									}
								}
							}
						}
					}
				}
				ept.start();
				epThreads.add(ept);
				
				if (i < numBins)
				{
					ReadDataThread left2 = new ReadDataThread(channels1.get(i), types1);
					HashDataThread right2 = new HashDataThread(channels2.get(i++), types2);
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
				catch(Exception e)
				{}
			}
		
			for (FileChannel fc : channels2)
			{
				try
				{
					fc.close();
				}
				catch(Exception e)
				{}
			}
		
			for (RandomAccessFile raf : files1)
			{
				try
				{
					raf.close();
				}
				catch(Exception e)
				{}
			}
		
			for (RandomAccessFile raf : files2)
			{
				try
				{
					raf.close();
				}
				catch(Exception e)
				{}
			}	
		}
		catch(Exception e)
		{
			outBuffer.put(e);
		}
	}
	
	private class ExternalProcessThread extends HRDBMSThread
	{
		private ReadDataThread left;
		private HashDataThread right;
		
		public ExternalProcessThread(ReadDataThread left, HashDataThread right)
		{
			this.left = left;
			this.right = right;
		}
		
		public void run()
		{
			try
			{
				process(left, right);
			}
			catch(Exception e)
			{
				outBuffer.put(e);
			}
		}
	}
	
	private void process(ReadDataThread left, HashDataThread right) throws Exception
	{
		ArrayList<ArrayList<Object>> probe = left.getData();
		MultiHashMap<Long, ArrayList<Object>> table = right.getData();
		final HashMap<String, Integer> childCols2Pos = children.get(0).getCols2Pos();
		
		int[] poses = new int[lefts.size()];
		int i = 0;
		for (String col : lefts)
		{
			poses[i] = childCols2Pos.get(col);
			i++;
		}
		final ArrayList<Object> key = new ArrayList<Object>(lefts.size());
		
		for (ArrayList<Object> row : probe)
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

			final long hash = 0x0EFFFFFFFFFFFFFFL & hash(key);
			Set<ArrayList<Object>> candidates = table.get(hash);
			boolean found = false;
			for (final ArrayList<Object> rRow : candidates)
			{
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
					}
					else
					{
						final ArrayList<Object> out = new ArrayList<Object>(row.size() + rRow.size());
						out.addAll(row);
						out.addAll(rRow);
						outBuffer.put(out);
					}
				}
			}
			
			if (anti != null && !found)
			{
				outBuffer.put(row);
			}
		}
		
		left.clear();
		right.clear();
		probe = null;
		table = null;
	}
	
	private class HashDataThread extends HRDBMSThread
	{
		private FileChannel fc;
		private boolean ok = true;
		private Exception e;
		private MultiHashMap<Long, ArrayList<Object>> data;
		private byte[] types;
		
		public HashDataThread(FileChannel fc, byte[] types)
		{
			this.fc = fc;
			this.types = types;
			data = new MultiHashMap<Long, ArrayList<Object>>();
		}
		
		public MultiHashMap<Long, ArrayList<Object>> getData()
		{
			return data;
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
				
				final ArrayList<Object> key = new ArrayList<Object>(rights.size());
				while (true)
				{
					bb1.position(0);
					if (fc.read(bb1) == -1)
					{
						return;
					}
					bb1.position(0);
					int length = bb1.getInt();
					ByteBuffer bb = ByteBuffer.allocate(length);
					fc.read(bb);
					ArrayList<Object> row = (ArrayList<Object>)fromBytes(bb.array(), types);
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

					final long hash = 0x0EFFFFFFFFFFFFFFL & hash(key);
					data.multiPut(hash, row);
				}
			}
			catch(Exception e)
			{
				ok = false;
				this.e = e;
			}
		}
		
		public void clear()
		{
			data = null;
		}
	}
	
	private class ReadDataThread extends HRDBMSThread
	{
		private FileChannel fc;
		private boolean ok = true;
		private Exception e;
		private ArrayList<ArrayList<Object>> data;
		private byte[] types;
		
		public ReadDataThread(FileChannel fc, byte[] types)
		{
			this.fc = fc;
			data = new ArrayList<ArrayList<Object>>();
			this.types = types;
		}
		
		public ArrayList<ArrayList<Object>> getData()
		{
			return data;
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
				fc.position(0);
				ByteBuffer bb1 = ByteBuffer.allocate(4);
				while (true)
				{
					bb1.position(0);
					if (fc.read(bb1) == -1)
					{
						return;
					}
					bb1.position(0);
					int length = bb1.getInt();
					ByteBuffer bb = ByteBuffer.allocate(length);
					fc.read(bb);
					ArrayList<Object> row = (ArrayList<Object>)fromBytes(bb.array(), types);
					data.add(row);
				}
			}
			catch(Exception e)
			{
				ok = false;
				this.e = e;
			}
		}
		
		public void clear()
		{
			data = null;
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
				final MyDate o = new MyDate(bb.getLong());
				retval.add(o);
			}
			else if (types[i] == 4)
			{
				// string
				final int length = bb.getInt();
				final byte[] temp = new byte[length];
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
	
	private class RightThread extends HRDBMSThread
	{
		private ArrayList<FileChannel> channels;
		private ArrayList<RandomAccessFile> files;
		private int numBins;
		private byte[] types;
		private boolean ok = true;
		private Exception e;
		
		public RightThread(ArrayList<RandomAccessFile> files, ArrayList<FileChannel> channels, int numBins, byte[] types)
		{
			this.channels = channels;
			this.files = files;
			this.numBins = numBins;
			this.types = types;
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
			ArrayList<ArrayList<ArrayList<Object>>> bins = new ArrayList<ArrayList<ArrayList<Object>>>();
			HashMap<Integer, FlushBinThread> threads = new HashMap<Integer, FlushBinThread>();
			int size = (int)(ResourceManager.QUEUE_SIZE * Double.parseDouble(HRDBMSWorker.getHParms().getProperty("external_factor")) / (numBins >> 5));
			int i = 0;
			while (i < numBins)
			{
				bins.add(new ArrayList<ArrayList<Object>>(size));
				i++;
			}
			
			try
			{
				final Operator child = children.get(1);
				final HashMap<String, Integer> childCols2Pos = child.getCols2Pos();
				Object o = child.next(HashJoinOperator.this);
				
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

					final long hash = 0x0EFFFFFFFFFFFFFFL & hash(key);
					int x = (int)(hash % numBins);
					ArrayList<ArrayList<Object>> bin = bins.get(x);
					//writeToHashTable(hash, (ArrayList<Object>)o);
					bin.add((ArrayList<Object>)o);
					
					if (bin.size() == size)
					{
						FlushBinThread thread = new FlushBinThread(bin, types, channels.get(x));
						if (threads.putIfAbsent(x, thread) != null)
						{
							threads.get(x).join();
							if (!threads.get(x).getOK())
							{
								throw threads.get(x).getException();
							}
							
							threads.put(x,  thread);
						}
						thread.start();
						bins.set(x, new ArrayList<ArrayList<Object>>(size));
					}
					
					o = child.next(HashJoinOperator.this);
				}
				
				i = 0;
				for (ArrayList<ArrayList<Object>> bin : bins)
				{
					if (bin.size() > 0)
					{
						FlushBinThread thread = new FlushBinThread(bin, types, channels.get(i));
						if (threads.putIfAbsent(i, thread) != null)
						{
							threads.get(i).join();
							if (!threads.get(i).getOK())
							{
								throw threads.get(i).getException();
							}
							
							threads.put(i,  thread);
						}
						thread.start();
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
				
				//everything is written
			}
			catch (final Exception e)
			{
				ok = false;
				this.e = e;
			}
		}
	}
	
	private final byte[] rsToBytes(ArrayList<ArrayList<Object>> rows, final byte[] types) throws Exception
	{
		final ArrayList<byte[]> results = new ArrayList<byte[]>(rows.size());
		final ArrayList<byte[]> bytes = new ArrayList<byte[]>();
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
			else if (b == 1)
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
				size += b.length;
				bytes.add(b);
			}

			final byte[] retval = new byte[size];
			final ByteBuffer retvalBB = ByteBuffer.wrap(retval);
			retvalBB.putInt(size - 4);
			int x = 0;
			int i = 0;
			for (final Object o : val)
			{
				if (types[i] == 0)
				{
					retvalBB.putLong((Long)o);
				}
				else if (types[i] == 1)
				{
					retvalBB.putInt((Integer)o);
				}
				else if (types[i] == 2)
				{
					retvalBB.putDouble((Double)o);
				}
				else if (types[i] == 3)
				{
					retvalBB.putLong(((MyDate)o).getTime());
				}
				else if (types[i] == 4)
				{
					byte[] temp = bytes.get(x++);
					retvalBB.putInt(temp.length);
					retvalBB.put(temp);
				}
				else
				{
					throw new Exception("Unknown type: " + types[i]);
				}

				i++;
			}

			results.add(retval);
			bytes.clear();
		}

		int count = 0;
		for (final byte[] ba : results)
		{
			count += ba.length;
		}
		final byte[] retval = new byte[count];
		int retvalPos = 0;
		for (final byte[] ba : results)
		{
			System.arraycopy(ba, 0, retval, retvalPos, ba.length);
			retvalPos += ba.length;
		}

		return retval;
	}
	
	private class LeftThread extends HRDBMSThread
	{
		private ArrayList<FileChannel> channels;
		private ArrayList<RandomAccessFile> files;
		private int numBins;
		private byte[] types;
		private boolean ok = true;
		private Exception e;
		
		public LeftThread(ArrayList<RandomAccessFile> files, ArrayList<FileChannel> channels, int numBins, byte[] types)
		{
			this.channels = channels;
			this.files = files;
			this.numBins = numBins;
			this.types = types;
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
			ArrayList<ArrayList<ArrayList<Object>>> bins = new ArrayList<ArrayList<ArrayList<Object>>>();
			HashMap<Integer, FlushBinThread> threads = new HashMap<Integer, FlushBinThread>();
			int size = (int)(ResourceManager.QUEUE_SIZE * Double.parseDouble(HRDBMSWorker.getHParms().getProperty("external_factor")) / (numBins >> 5));
			int i = 0;
			while (i < numBins)
			{
				bins.add(new ArrayList<ArrayList<Object>>(size));
				i++;
			}
			
			try
			{
				final Operator child = children.get(0);
				final HashMap<String, Integer> childCols2Pos = child.getCols2Pos();
				Object o = child.next(HashJoinOperator.this);
				
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

					final long hash = 0x0EFFFFFFFFFFFFFFL & hash(key);
					int x = (int)(hash % numBins);
					ArrayList<ArrayList<Object>> bin = bins.get(x);
					//writeToHashTable(hash, (ArrayList<Object>)o);
					bin.add((ArrayList<Object>)o);
					
					if (bin.size() == size)
					{
						FlushBinThread thread = new FlushBinThread(bin, types, channels.get(x));
						if (threads.putIfAbsent(x, thread) != null)
						{
							threads.get(x).join();
							if (!threads.get(x).getOK())
							{
								throw threads.get(x).getException();
							}
							
							threads.put(x,  thread);
						}
						thread.start();
						bins.set(x, new ArrayList<ArrayList<Object>>(size));
					}
					
					o = child.next(HashJoinOperator.this);
				}
				
				i = 0;
				for (ArrayList<ArrayList<Object>> bin : bins)
				{
					if (bin.size() > 0)
					{
						FlushBinThread thread = new FlushBinThread(bin, types, channels.get(i));
						if (threads.putIfAbsent(i, thread) != null)
						{
							threads.get(i).join();
							if (!threads.get(i).getOK())
							{
								throw threads.get(i).getException();
							}
							
							threads.put(i,  thread);
						}
						thread.start();
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
				
				//everything is written
			}
			catch (final Exception e)
			{
				ok = false;
				this.e = e;
			}
		}
	}
	
	private class FlushBinThread extends HRDBMSThread
	{
		private byte[] types;
		private ArrayList<ArrayList<Object>> bin;
		private FileChannel fc;
		private boolean ok = true;
		private Exception e;
		
		public FlushBinThread(ArrayList<ArrayList<Object>> bin, byte[] types, FileChannel fc)
		{
			this.types = types;
			this.bin = bin;
			this.fc = fc;
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
				byte[] data = rsToBytes(bin, types);
				bin.clear();
				fc.position(fc.size());
				ByteBuffer bb = ByteBuffer.wrap(data);
				fc.write(bb);
			}
			catch(Exception e)
			{
				ok = false;
				this.e = e;
			}
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

	private final ArrayList<ArrayList<Object>> getCandidates(long hash) throws Exception
	{
		final ArrayList<ArrayList<Object>> retval = new ArrayList<ArrayList<Object>>();
		int i = 0;
		if (buckets.size() == 0)
		{
			return retval;
		}
		ConcurrentHashMap<Long, ArrayList<Object>> dbhm = buckets.get(i);
		ArrayList<Object> o = dbhm.get(hash);
		while (o != null)
		{
			retval.add(o);
			i++;
			if (i < buckets.size())
			{
				o = buckets.get(i).get(hash);
			}
			else
			{
				o = null;
			}
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

	private final void writeToHashTable(long hash, ArrayList<Object> row) throws Exception
	{
		int i = 0;
		int min = -1;
		int max = buckets.size() - 1;
		while (max - min >= 4)
		{
			boolean exists = buckets.get(((max - min) >> 1) + min).containsKey(hash);
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

				o = ((ConcurrentHashMap<Long, ArrayList<Object>>)o).putIfAbsent(hash, row);
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
						o = ((ConcurrentHashMap<Long, ArrayList<Object>>)o).putIfAbsent(hash, row);
					}
					else
					{
						//o = ResourceManager.newDiskBackedHashMap(false, rightChildCard / buckets.size());
						o = new ConcurrentHashMap<Long, ArrayList<Object>>(rightChildCard / buckets.size());
						((ConcurrentHashMap<Long, ArrayList<Object>>)o).put(hash, row);
						buckets.add((ConcurrentHashMap<Long, ArrayList<Object>>)o);
						// HRDBMSWorker.logger.debug("There are now " +
						// buckets.size() + " buckets");
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

					for (final ConcurrentHashMap<Long, ArrayList<Object>> bucket : buckets)
					{
						bucket.clear();
					}
					break;
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.error("", e);
				}
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
				while (!readersDone)
				{
					LockSupport.parkNanos(75000);
				}

				final Operator left = children.get(0);
				final HashMap<String, Integer> childCols2Pos = left.getCols2Pos();
				Object o = left.next(HashJoinOperator.this);
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

					final long hash = 0x0EFFFFFFFFFFFFFFL & hash(key);
					final ArrayList<ArrayList<Object>> candidates = getCandidates(hash);

					boolean found = false;
					for (final ArrayList<Object> rRow : candidates)
					{
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
							}
							else
							{
								final ArrayList<Object> out = new ArrayList<Object>(lRow.size() + rRow.size());
								out.addAll(lRow);
								out.addAll(rRow);
								outBuffer.put(out);
							}
						}
					}
					
					if (anti != null && !found)
					{
						outBuffer.put(lRow);
					}

					// leftCount.incrementAndGet();
					o = left.next(HashJoinOperator.this);
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

					final long hash = 0x0EFFFFFFFFFFFFFFL & hash(key);
					writeToHashTable(hash, (ArrayList<Object>)o);
					o = child.next(HashJoinOperator.this);
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
}
