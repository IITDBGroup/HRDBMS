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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.managers.ResourceManager;
import com.exascale.misc.*;
import com.exascale.tables.Plan;
import com.exascale.tables.Schema;
import com.exascale.threads.HRDBMSThread;
import com.exascale.threads.TempThread;
import com.exascale.threads.ThreadPoolThread;

public final class SortOperator implements Operator, Serializable
{
	static final int PARALLEL_SORT_MIN_NUM_ROWS = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("parallel_sort_min_rows")); // 50000

	private static sun.misc.Unsafe unsafe;
	private static long edOff;

	static
	{
		try
		{
			final Field f = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
			f.setAccessible(true);
			unsafe = (sun.misc.Unsafe)f.get(null);
			final Field fieldToRead = ArrayList.class.getDeclaredField("elementData");
			// get unsafe offset to this field
			edOff = unsafe.objectFieldOffset(fieldToRead);
		}
		catch (final Exception e)
		{
			unsafe = null;
		}
	}
	private Operator child;
	private Operator parent;
	private HashMap<String, String> cols2Types;
	private HashMap<String, Integer> cols2Pos;
	private TreeMap<Integer, String> pos2Col;
	private boolean startDone = false;
	private transient boolean closeDone;
	private ArrayList<String> sortCols;
	private boolean[] orders;
	private transient SortThread sortThread;
	private transient volatile BufferedLinkedBlockingQueue readBuffer;
	private transient volatile ArrayList<ArrayList<Object>> result;
	private volatile int[] sortPos;
	private transient int NUM_RT_THREADS;
	private transient final MetaData meta;
	private transient volatile boolean isClosed;
	private transient volatile boolean done;
	private int node;
	private long childCard = 16;
	private boolean cardSet = false;
	private transient ArrayList<String> externalFiles;
	private long limit = 0;
	private transient AtomicLong received;
	private transient volatile boolean demReceived;
	private long txnum;

	public SortOperator(final ArrayList<String> sortCols, final ArrayList<Boolean> orders, final MetaData meta)
	{
		this.sortCols = sortCols;
		this.orders = new boolean[orders.size()];
		int i = 0;
		for (final boolean b : orders)
		{
			this.orders[i++] = b;
		}
		this.meta = meta;
		received = new AtomicLong(0);
	}

	public SortOperator(final ArrayList<String> sortCols, final boolean[] orders, final MetaData meta)
	{
		this.sortCols = sortCols;
		this.orders = orders;
		this.meta = meta;
		received = new AtomicLong(0);
	}

	public static SortOperator deserialize(final InputStream in, final HashMap<Long, Object> prev) throws Exception
	{
		final SortOperator value = (SortOperator)unsafe.allocateInstance(SortOperator.class);
		prev.put(OperatorUtils.readLong(in), value);
		value.child = OperatorUtils.deserializeOperator(in, prev);
		value.parent = OperatorUtils.deserializeOperator(in, prev);
		value.cols2Types = OperatorUtils.deserializeStringHM(in, prev);
		value.cols2Pos = OperatorUtils.deserializeStringIntHM(in, prev);
		value.pos2Col = OperatorUtils.deserializeTM(in, prev);
		value.startDone = OperatorUtils.readBool(in);
		value.sortCols = OperatorUtils.deserializeALS(in, prev);
		value.orders = OperatorUtils.deserializeBoolArray(in, prev);
		value.sortPos = OperatorUtils.deserializeIntArray(in, prev);
		value.node = OperatorUtils.readInt(in);
		value.childCard = OperatorUtils.readLong(in);
		value.cardSet = OperatorUtils.readBool(in);
		value.limit = OperatorUtils.readLong(in);
		value.received = new AtomicLong(0);
		value.demReceived = false;
		value.txnum = OperatorUtils.readLong(in);
		return value;
	}

	public static void init()
	{
		HRDBMSWorker.logger.debug("SortOperator inited");
	}

	@Override
	public void add(final Operator op) throws Exception
	{
		if (child == null)
		{
			child = op;
			op.registerParent(this);
			cols2Types = child.getCols2Types();
			cols2Pos = child.getCols2Pos();
			pos2Col = child.getPos2Col();
			int i = 0;
			sortPos = new int[sortCols.size()];
			final ArrayList<String> newSortCols = new ArrayList<String>();
			for (final String sortCol : sortCols)
			{
				try
				{
					final Integer pos = cols2Pos.get(sortCol);
					if (pos != null)
					{
						sortPos[i] = pos;
						newSortCols.add(sortCol);
					}
					else
					{
						String sortCol2;
						if (sortCol.contains("."))
						{
							sortCol2 = sortCol.substring(sortCol.indexOf('.') + 1);
						}
						else
						{
							sortCol2 = sortCol;
						}

						int matches = 0;
						for (final String col : cols2Pos.keySet())
						{
							String col2;
							if (col.contains("."))
							{
								col2 = col.substring(col.indexOf('.') + 1);
							}
							else
							{
								col2 = col;
							}

							if (col2.equals(sortCol2))
							{
								matches++;
								newSortCols.add(sortCol);

								if (matches == 1)
								{
									sortPos[i] = cols2Pos.get(col);
								}
								else
								{
									throw new Exception("Ambiguous column: " + sortCol);
								}
							}
						}
					}
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.debug("Could not find " + sortCol + " in " + cols2Pos);
					throw e;
				}
				i++;
			}

			sortCols = newSortCols;
		}
		else
		{
			throw new Exception("SortOperator only supports 1 child.");
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
	public SortOperator clone()
	{
		final SortOperator retval = new SortOperator((ArrayList<String>)sortCols.clone(), orders.clone(), meta);
		retval.node = node;
		retval.childCard = childCard;
		retval.cardSet = cardSet;
		retval.limit = limit;
		retval.txnum = txnum;
		return retval;
	}

	@Override
	public synchronized void close() throws Exception
	{
		if (!closeDone)
		{
			closeDone = true;
			child.close();
		}

		if (externalFiles != null)
		{
			for (final String fn : externalFiles)
			{
				new File(fn).delete();
			}
		}
		// result.close();
		// result = null;

		if (readBuffer != null)
		{
			readBuffer.close();
		}

		sortCols = null;
		orders = null;
		cols2Pos = null;
		cols2Types = null;
		pos2Col = null;
		isClosed = true;
	}

	public boolean done()
	{
		return done;
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

	public ArrayList<String> getKeys()
	{
		return sortCols;
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

	public ArrayList<Boolean> getOrders()
	{
		final ArrayList<Boolean> retval = new ArrayList<Boolean>(orders.length);
		for (final boolean b : orders)
		{
			retval.add(b);
		}

		return retval;
	}

	@Override
	public TreeMap<Integer, String> getPos2Col()
	{
		return pos2Col;
	}

	@Override
	public ArrayList<String> getReferences()
	{
		return new ArrayList<String>(sortCols);
	}

	@Override
	public Object next(final Operator op) throws Exception
	{
		Object o;
		o = readBuffer.take();

		if (o instanceof DataEndMarker)
		{
			o = readBuffer.peek();
			if (o == null)
			{
				readBuffer.put(new DataEndMarker());
				return new DataEndMarker();
			}
			else
			{
				readBuffer.put(new DataEndMarker());
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
	public void nextAll(final Operator op) throws Exception
	{
		// child.nextAll(op);
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
	public void registerParent(final Operator op) throws Exception
	{
		if (parent == null)
		{
			parent = op;
		}
		else
		{
			throw new Exception("SortOperator only supports 1 parent.");
		}
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
		if (!startDone)
		{
			try
			{
				start();
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				throw e;
			}
		}
		else
		{
			child.reset();

			if (result != null)
			{
				try
				{
					result.clear();
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.error("", e);
					throw e;
				}
			}

			readBuffer.clear();
			sortThread = new SortThread(child);
			sortThread.start();
		}
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

		OperatorUtils.writeType(HrdbmsType.SORT, out);
		prev.put(this, OperatorUtils.writeID(out));
		child.serialize(out, prev);
		parent.serialize(out, prev);
		OperatorUtils.serializeStringHM(cols2Types, out, prev);
		OperatorUtils.serializeStringIntHM(cols2Pos, out, prev);
		OperatorUtils.serializeTM(pos2Col, out, prev);
		OperatorUtils.writeBool(startDone, out);
		OperatorUtils.serializeALS(sortCols, out, prev);
		OperatorUtils.serializeBoolArray(orders, out, prev);
		OperatorUtils.serializeIntArray(sortPos, out, prev);
		OperatorUtils.writeInt(node, out);
		OperatorUtils.writeLong(childCard, out);
		OperatorUtils.writeBool(cardSet, out);
		OperatorUtils.writeLong(limit, out);
		OperatorUtils.writeLong(txnum, out);
	}

	public boolean setChildCard(final long card)
	{
		if (cardSet)
		{
			return false;
		}

		cardSet = true;
		childCard = card;
		return true;
	}

	@Override
	public void setChildPos(final int pos)
	{
	}

	public void setLimit(final long limit)
	{
		this.limit = limit;
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

	public void setTXNum(final long txnum)
	{
		this.txnum = txnum;
	}

	public long size()
	{
		return result.size();
	}

	@Override
	public synchronized void start() throws Exception
	{
		NUM_RT_THREADS = 1; // must be 1
		closeDone = false;
		isClosed = false;
		done = false;

		if (!startDone)
		{
			startDone = true;
			child.start();
			sortThread = new SortThread(child);
			readBuffer = new BufferedLinkedBlockingQueue(ResourceManager.QUEUE_SIZE);
			sortThread.start();
		}
	}

	@Override
	public String toString()
	{
		return "SortOperator";
	}

	private void limitSort() throws Exception
	{
		try
		{
			final ReverseComparator cmp = new ReverseComparator();
			final AuxPairingHeap<ArrayList<Object>> bh = new AuxPairingHeap<ArrayList<Object>>(cmp);
			Object o = child.next(this);
			if (o instanceof DataEndMarker)
			{
				demReceived = true;
			}
			else
			{
				received.getAndIncrement();
			}
			while (!(o instanceof DataEndMarker))
			{
				if (o instanceof Exception)
				{
					readBuffer.put(o);
					return;
				}

				final ArrayList<Object> row = (ArrayList<Object>)o;
				if (bh.size() < limit)
				{
					bh.insert(row);
				}
				else
				{
					final int result = cmp.compare(row, bh.findMin());
					if (result > 0)
					{
						bh.insert(row);
						bh.extractMin();
					}
				}

				o = child.next(this);
				if (o instanceof DataEndMarker)
				{
					demReceived = true;
				}
				else
				{
					received.getAndIncrement();
				}
			}

			// sort and write to queue
			final ArrayList<ArrayList<Object>> result = new ArrayList<ArrayList<Object>>();
			while (bh.size() > 0)
			{
				result.add(bh.extractMin());
			}

			Collections.reverse(result);

			for (final ArrayList<Object> row : result)
			{
				readBuffer.put(row);
			}

			readBuffer.put(new DataEndMarker());
		}
		catch (final Exception e)
		{
			readBuffer.put(e);
		}
	}

	private class ALOO
	{
		private final ArrayList<Object> alo;
		private final Object op;

		public ALOO(final ArrayList<Object> alo, final Object op)
		{
			this.alo = alo;
			this.op = op;
		}

		@Override
		public boolean equals(final Object o)
		{
			return this == o;
		}

		public ArrayList<Object> getALO()
		{
			return alo;
		}

		public Object getOp()
		{
			return op;
		}
	}

	private class MergeComparator implements Comparator<Object>
	{
		@Override
		public int compare(final Object l2, final Object r2)
		{
			final ALOO l = (ALOO)l2;
			final ALOO r = (ALOO)r2;
			if (l == r)
			{
				return 0;
			}

			final ArrayList<Object> lhs = l.getALO();
			final ArrayList<Object> rhs = r.getALO();
			int result = 0;
			int i = 0;

			for (final int pos : sortPos)
			{
				Object lField = lhs.get(pos);
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
					lField = null;
					lField.toString();
				}

				if (orders[i])
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

				i++;
			}

			return 0;
		}
	}

	private class ReverseComparator implements Comparator<Object>
	{
		@Override
		public int compare(final Object l, final Object r)
		{
			if (l == r)
			{
				return 0;
			}

			final ArrayList<Object> lhs = (ArrayList<Object>)l;
			final ArrayList<Object> rhs = (ArrayList<Object>)r;

			int result = 0;
			int i = 0;

			for (final int pos : sortPos)
			{
				Object lField = lhs.get(pos);
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
					lField = null;
					lField.toString();
				}

				if (orders[i])
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
				else
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

				i++;
			}

			return 0;
		}
	}

	private final class SortThread extends ThreadPoolThread
	{
		Operator child;

		ReaderThread[] threads;

		CopyThread ct;

		public SortThread(final Operator child)
		{
			this.child = child;
		}

		@Override
		public void run()
		{
			try
			{
				if (limit > 0)
				{
					limitSort();
					return;
				}

				if (ResourceManager.criticalMem())
				{
					externalSort(0);
					return;
				}

				if (childCard > ResourceManager.QUEUE_SIZE * Double.parseDouble(HRDBMSWorker.getHParms().getProperty("external_factor")))
				{
					// double percentInMem = (ResourceManager.QUEUE_SIZE *
					// Double.parseDouble(HRDBMSWorker.getHParms().getProperty("hash_external_factor"))
					// / 2) / childCard;
					// percentInMem = percentInMem / 4;
					final double percentInMem = 0;
					externalSort(percentInMem);
					return;
				}

				// if (childCard > ResourceManager.QUEUE_SIZE *
				// Double.parseDouble(HRDBMSWorker.getHParms().getProperty("external_factor"))
				// / 2)
				// {
				// mediumSort();
				// return;
				// }

				if (result == null)
				{
					// result = ResourceManager.newDiskBackedArray(childCard);
					result = new ArrayList<ArrayList<Object>>((int)childCard);
				}
				int i = 0;

				threads = new ReaderThread[NUM_RT_THREADS];
				while (i < NUM_RT_THREADS)
				{
					threads[i] = new ReaderThread(result);
					threads[i].start();
					i++;
				}

				i = 0;
				while (i < NUM_RT_THREADS)
				{
					threads[i].join();
					i++;
				}

				if (result.size() > 0)
				{
					// ResourceManager.NO_OFFLOAD.getAndIncrement();
					// ResourceManager.waitForSync();
					if (result.size() < PARALLEL_SORT_MIN_NUM_ROWS)
					{
						// doSequentialSort(0, result.size() - 1);
						result.sort(new MergeComparator2());
					}
					else
					{
						// final ParallelSortThread t = doParallelSort(0,
						// result.size() - 1);
						// t.join();
						final Object[] underlying = (Object[])unsafe.getObject(result, edOff);
						Arrays.parallelSort(underlying, 0, result.size(), new MergeComparator2());
					}
					// ResourceManager.NO_OFFLOAD.getAndDecrement();
				}

				done = true;

				ct = new CopyThread();
				ct.start();
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				try
				{
					readBuffer.put(e);
				}
				catch (final Exception f)
				{
				}
				return;
			}
		}

		private void externalSort(final double percentInMem)
		{
			final ConcurrentHashMap<WriteDataThread, WriteDataThread> threads = new ConcurrentHashMap<WriteDataThread, WriteDataThread>();
			final ArrayList<ArrayList<ArrayList<Object>>> bins = new ArrayList<ArrayList<ArrayList<Object>>>();
			HRDBMSWorker.logger.debug("Doing external sort");
			// double factor =
			// Double.parseDouble(HRDBMSWorker.getHParms().getProperty("external_factor"));
			// int size = (int)(ResourceManager.QUEUE_SIZE * factor);
			final int size = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("sort_bucket_size"));
			final int numInMem = 0;
			externalFiles = new ArrayList<String>();
			result = new ArrayList<ArrayList<Object>>(size);
			final ConcurrentHashMap<String, HRDBMSThread> map = new ConcurrentHashMap<String, HRDBMSThread>();
			final ConcurrentHashMap<String, RandomAccessFile> map2 = new ConcurrentHashMap<String, RandomAccessFile>();
			final ConcurrentHashMap<String, FileChannel> map3 = new ConcurrentHashMap<String, FileChannel>();
			boolean done = false;
			final byte[] types = new byte[pos2Col.size()];
			int j = 0;
			for (final String col : pos2Col.values())
			{
				final String type = cols2Types.get(col);
				if (type.equals("INT"))
				{
					types[j] = (byte)1;
				}
				else if (type.equals("FLOAT"))
				{
					types[j] = (byte)2;
				}
				else if (type.equals("CHAR"))
				{
					types[j] = (byte)4;
				}
				else if (type.equals("LONG"))
				{
					types[j] = (byte)0;
				}
				else if (type.equals("DATE"))
				{
					types[j] = (byte)3;
				}
				else
				{
					readBuffer.put(new Exception("Unknown type: " + type));
					return;
				}

				j++;
			}

			j = 0;
			while (!done)
			{
				int i = 0;
				while (i < size)
				{
					try
					{
						final Object o = child.next(SortOperator.this);
						if (o instanceof DataEndMarker)
						{
							demReceived = true;
							break;
						}
						else
						{
							received.getAndIncrement();
						}

						if (o instanceof Exception)
						{
							readBuffer.put(o);
							return;
						}

						result.add((ArrayList<Object>)o);
					}
					catch (final Exception e)
					{
						readBuffer.put(e);
						return;
					}

					i++;
				}

				if (i < size)
				{
					done = true;
				}

				if (i == 0)
				{
					break;
				}

				if (result.size() < PARALLEL_SORT_MIN_NUM_ROWS)
				{
					try
					{
						// doSequentialSort(0, result.size() - 1);
						result.sort(new MergeComparator2());
					}
					catch (final Exception e)
					{
						readBuffer.put(e);
						return;
					}
				}
				else
				{
					// final ParallelSortThread t = doParallelSort(0,
					// result.size() - 1);
					// while (true)
					// {
					// try
					// {
					// t.join();
					// break;
					// }
					// catch(InterruptedException e)
					// {}
					// }
					final Object[] underlying = (Object[])unsafe.getObject(result, edOff);
					Arrays.parallelSort(underlying, 0, result.size(), new MergeComparator2());
				}

				final String fn = ResourceManager.TEMP_DIRS.get(j % ResourceManager.TEMP_DIRS.size()) + this.hashCode() + "" + System.currentTimeMillis() + ".extsrt";
				externalFiles.add(fn);

				try
				{
					if (j < numInMem)
					{
						bins.add(result);
						result = new ArrayList<ArrayList<Object>>(size);
					}
					else
					{
						final byte[] data = rsToBytes(result, types);
						final WriteDataThread thread = new WriteDataThread(fn, data, map2, map3);
						TempThread.start(thread, txnum);
						threads.put(thread, thread);
						map.put(fn, thread);
						result.clear();
					}
					j++;
				}
				catch (final Exception e)
				{
					readBuffer.put(e);
					return;
				}
			}

			result = null;
			// at this point all the threads are listed in the map
			// some might be running still
			for (final Map.Entry entry : map.entrySet())
			{
				while (true)
				{
					try
					{
						((WriteDataThread)entry.getValue()).join();
						if (!((WriteDataThread)entry.getValue()).getOK())
						{
							readBuffer.put(((WriteDataThread)entry.getValue()).getException());
							return;
						}
						break;
					}
					catch (final InterruptedException e)
					{
					}
				}

				try
				{
					final ReadDataThread thread = new ReadDataThread((String)entry.getKey(), 0, size / map.size(), map2, map3, types);
					thread.doStart();
					map.put((String)entry.getKey(), thread);
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.debug("", e);
				}
			}

			// reads for all first chunks are started
			for (final Map.Entry entry : map.entrySet())
			{
				while (true)
				{
					try
					{
						((ReadDataThread)entry.getValue()).join();
						if (!((ReadDataThread)entry.getValue()).getOK())
						{
							readBuffer.put(((ReadDataThread)entry.getValue()).getException());
							return;
						}
						break;
					}
					catch (final InterruptedException e)
					{
					}
				}
			}

			int z = 0;
			while (z < bins.size())
			{
				map.put(Integer.toString(z), new ReadDataThread("", 0, 0, map2, map3, types, z, bins));
				z++;
			}

			mergeIntoResult(map.values());
			readBuffer.put(new DataEndMarker());

			for (final FileChannel fc : map3.values())
			{
				try
				{
					fc.close();
				}
				catch (final Exception e)
				{
				}
			}

			for (final RandomAccessFile raf : map2.values())
			{
				try
				{
					raf.close();
				}
				catch (final Exception e)
				{
				}
			}
		}

		private final Object fromBytes(final byte[] val, final byte[] types) throws Exception
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
					final int length = getCInt(bb);

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
						final byte[] out = new byte[temp.length << 1];
						final int clen = Schema.CVarcharFV.decompress(temp, temp.length, out);
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

		private int getCInt(final ByteBuffer bb)
		{
			final int temp = (bb.get() & 0xff);
			final int length = (temp >>> 5);
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

		private long getCLong(final ByteBuffer bb)
		{
			final int temp = (bb.get() & 0xff);
			final int length = (temp >>> 4);
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

		private int getMedium(final ByteBuffer bb)
		{
			int retval = ((bb.getShort() & 0xffff) << 8);
			retval += (bb.get() & 0xff);
			return retval;
		}

		private void mergeIntoResult(final Collection<HRDBMSThread> readThreads)
		{
			final AuxPairingHeap<ALOO> rows = new AuxPairingHeap<ALOO>(new MergeComparator());
			ALOO minEntry;
			for (final HRDBMSThread op : readThreads)
			{
				try
				{
					final ArrayList<Object> row = ((ReadDataThread)op).readRow();
					if (row != null)
					{
						rows.insert(new ALOO(row, op));
					}
				}
				catch (final Exception e)
				{
					try
					{
						HRDBMSWorker.logger.debug("", e);
						readBuffer.put(e);
					}
					catch (final Exception f)
					{
					}
					return;
				}
			}

			while (rows.size() > 0)
			{
				minEntry = rows.extractMin();

				while (true)
				{
					try
					{
						readBuffer.put(minEntry.getALO());
						break;
					}
					catch (final Exception e)
					{
					}
				}
				try
				{
					final ArrayList<Object> row = ((ReadDataThread)minEntry.getOp()).readRow();
					if (row != null)
					{
						rows.insert(new ALOO(row, minEntry.getOp()));
					}
				}
				catch (final Exception e)
				{
					try
					{
						HRDBMSWorker.logger.debug("", e);
						readBuffer.put(e);
					}
					catch (final Exception f)
					{
					}
					return;
				}
			}
		}

		private void putCInt(final ByteBuffer bb, int val)
		{
			if (val <= 31)
			{
				// 1 byte
				val |= 0x20;
				bb.put((byte)val);
			}
			else if (val <= 8191)
			{
				// 2 bytes
				val |= 0x4000;
				bb.putShort((short)val);
			}
			else if (val <= 0x1fffff)
			{
				// 3 bytes
				val |= 0x600000;
				putMedium(bb, val);
			}
			else if (val <= 0x1FFFFFFF)
			{
				// 4 bytes
				val |= 0x80000000;
				bb.putInt(val);
			}
			else
			{
				// 5 bytes
				bb.put((byte)0xa0);
				bb.putInt(val);
			}
		}

		private void putCLong(final ByteBuffer bb, long val)
		{
			if (val <= 15)
			{
				// 1 byte
				val |= 0x10;
				bb.put((byte)val);
			}
			else if (val <= 4095)
			{
				// 2 bytes
				val |= 0x2000;
				bb.putShort((short)val);
			}
			else if (val <= 0xfffff)
			{
				// 3 bytes
				val |= 0x300000;
				putMedium(bb, (int)val);
			}
			else if (val <= 0xfffffff)
			{
				// 4 bytes
				val |= 0x40000000;
				bb.putInt((int)val);
			}
			else if (val <= 0xfffffffffl)
			{
				// 5 bytes
				val |= 0x5000000000l;
				bb.put((byte)(val >>> 32));
				bb.putInt((int)val);
			}
			else if (val <= 0xfffffffffffl)
			{
				// 6 bytes
				val |= 0x600000000000l;
				bb.putShort((short)(val >>> 32));
				bb.putInt((int)val);
			}
			else if (val <= 0xfffffffffffffl)
			{
				// 7 bytes
				val |= 0x70000000000000l;
				putMedium(bb, (int)(val >> 32));
				bb.putInt((int)val);
			}
			else if (val <= 0xfffffffffffffffl)
			{
				// 8 bytes
				val |= 0x8000000000000000l;
				bb.putLong(val);
			}
			else
			{
				// 9 bytes
				bb.put((byte)0x90);
				bb.putLong(val);
			}
		}

		private void putMedium(final ByteBuffer bb, final int val)
		{
			bb.put((byte)((val & 0xff0000) >> 16));
			bb.put((byte)((val & 0xff00) >> 8));
			bb.put((byte)(val & 0xff));
		}

		private final byte[] rsToBytes(final ArrayList<ArrayList<Object>> rows, final byte[] types) throws Exception
		{
			final ByteBuffer[] results = new ByteBuffer[rows.size()];
			int rIndex = 0;
			final ArrayList<byte[]> bytes = new ArrayList<byte[]>();
			final ArrayList<Integer> stringCols = new ArrayList<Integer>(rows.get(0).size());
			int startSize = 4;
			int a = 0;
			for (final byte b : types)
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

			for (final ArrayList<Object> val : rows)
			{
				int size = startSize;
				for (final int y : stringCols)
				{
					final Object o = val.get(y);
					byte[] b = ((String)o).getBytes(StandardCharsets.UTF_8);
					if (Schema.CVarcharFV.compress)
					{
						final byte[] out = new byte[b.length * 3 + 1];
						final int clen = Schema.CVarcharFV.compress(b, b.length, out);
						b = new byte[clen];
						System.arraycopy(out, 0, b, 0, clen);
					}
					size += b.length;
					bytes.add(b);
				}

				final byte[] retval = new byte[size];
				final ByteBuffer retvalBB = ByteBuffer.wrap(retval);
				// retvalBB.putInt(size - 4);
				int x = 0;
				int i = 0;
				for (final Object o : val)
				{
					if (types[i] == 0)
					{
						// retvalBB.putLong((Long)o);
						putCLong(retvalBB, (Long)o);
					}
					else if (types[i] == 1)
					{
						// retvalBB.putInt((Integer)o);
						putCInt(retvalBB, (Integer)o);
					}
					else if (types[i] == 2)
					{
						retvalBB.putDouble((Double)o);
					}
					else if (types[i] == 3)
					{
						final int value = ((MyDate)o).getTime();
						// retvalBB.putInt(value);
						putMedium(retvalBB, value);
					}
					else if (types[i] == 4)
					{
						final byte[] temp = bytes.get(x++);
						// retvalBB.putInt(temp.length);
						putCInt(retvalBB, temp.length);
						retvalBB.put(temp);
					}
					else
					{
						throw new Exception("Unknown type: " + types[i]);
					}

					i++;
				}

				results[rIndex++] = retvalBB;
				bytes.clear();
			}

			int count = 0;
			for (final ByteBuffer bb : results)
			{
				count += (bb.position() + 4);
			}
			final byte[] retval = new byte[count];
			final ByteBuffer retvalBB = ByteBuffer.wrap(retval);
			int retvalPos = 0;
			for (final ByteBuffer bb : results)
			{
				final byte[] ba = bb.array();
				retvalBB.position(retvalPos);
				retvalBB.putInt(bb.position());
				retvalPos += 4;
				System.arraycopy(ba, 0, retval, retvalPos, bb.position());
				retvalPos += bb.position();
			}

			return retval;
		}

		private final class CopyThread extends ThreadPoolThread
		{
			@Override
			public void run()
			{
				try
				{
					final long size = result.size();
					long i = 0;
					// long nullCount = 0; //DEBUG
					while (i < size)
					{
						ArrayList<Object> row = null;
						while (row == null)
						{
							try
							{
								row = result.get((int)i);
							}
							catch (final Exception e)
							{
								HRDBMSWorker.logger.error("Array is " + result, e);
								HRDBMSWorker.logger.error("isClosed = " + isClosed);
								readBuffer.put(e);
								return;
							}
						}

						readBuffer.put(row);

						i++;
					}

					// System.out.println(nullCount + "/" + size +
					// " records were null after sort.");

					readBuffer.put(new DataEndMarker());
					result = null;
					// array.close();
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.error("", e);
					try
					{
						readBuffer.put(e);
					}
					catch (final Exception f)
					{
					}
					return;
				}
			}
		}

		private class MergeComparator2 implements Comparator<Object>
		{
			@Override
			public int compare(final Object l, final Object r)
			{
				if (l == r)
				{
					return 0;
				}

				final ArrayList<Object> lhs = (ArrayList<Object>)l;
				final ArrayList<Object> rhs = (ArrayList<Object>)r;

				int result = 0;
				int i = 0;

				for (final int pos : sortPos)
				{
					Object lField = lhs.get(pos);
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
						lField = null;
						lField.toString();
					}

					if (orders[i])
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

					i++;
				}

				return 0;
			}
		}

		private class ReadDataThread extends HRDBMSThread
		{
			private String fn;
			private long offset;
			private int numRecs;
			public ArrayList<ArrayList<Object>> data;
			private boolean ok = true;
			private Exception e;
			private int readOffset = 0;
			private ReadDataThread second;
			private boolean started = false;
			private ConcurrentHashMap<String, RandomAccessFile> map2;
			private ConcurrentHashMap<String, FileChannel> map3;
			private byte[] types;
			private final boolean inMem;

			public ReadDataThread(final String fn, final long offset, final int numRecs, final ConcurrentHashMap<String, RandomAccessFile> map2, final ConcurrentHashMap<String, FileChannel> map3, final byte[] types) throws Exception
			{
				if (fn == null)
				{
					throw new Exception("Null fn");
				}
				this.data = new ArrayList<ArrayList<Object>>(numRecs);
				this.offset = offset;
				this.map2 = map2;
				this.map3 = map3;
				this.types = types;
				this.fn = fn;
				this.numRecs = numRecs;
				if (this.numRecs == 0)
				{
					this.numRecs = 1;
				}
				inMem = false;
			}

			public ReadDataThread(final String fn, final long offset, final int numRecs, final ConcurrentHashMap<String, RandomAccessFile> map2, final ConcurrentHashMap<String, FileChannel> map3, final byte[] types, final int index, final ArrayList<ArrayList<ArrayList<Object>>> bins)
			{
				this.data = bins.get(index);
				inMem = true;
			}

			public void doStart() throws Exception
			{
				if (inMem)
				{
					throw new Exception("Trying to start in mem thread");
				}

				if (fn == null)
				{
					throw new Exception("Filename is null at start");
				}
				this.start();
			}

			public Exception getException()
			{
				return e;
			}

			public boolean getOK()
			{
				return ok;
			}

			public ArrayList<Object> readRow() throws Exception
			{
				if (inMem)
				{
					if (data == null)
					{
						return null;
					}

					if (readOffset < data.size())
					{
						return data.get(readOffset++);
					}

					data = null;
					return null;
				}

				if (readOffset < data.size())
				{
					if (!started && offset != -1)
					{
						second = new ReadDataThread(fn, offset, numRecs, map2, map3, types);
						second.doStart();
						started = true;
					}
					return data.get(readOffset++);
				}

				if (data == null)
				{
					return null;
				}

				if (second == null)
				{
					data = null;
					return null;
				}

				while (true)
				{
					try
					{
						second.join();
						if (!second.getOK())
						{
							throw second.getException();
						}

						break;
					}
					catch (final InterruptedException e)
					{
					}
				}

				readOffset = 0;
				this.data = second.data;
				started = false;
				this.offset = second.offset;
				second = null;
				return readRow();
			}

			@Override
			public void run()
			{
				try
				{
					final FileChannel f = map3.get(fn);
					final FileChannel fc = new BufferedFileChannel(f, 1 * 1024 * 1024);
					//
					fc.position(offset);
					int i = 0;
					final ByteBuffer bb1 = ByteBuffer.allocate(4);

					while (i < numRecs)
					{
						bb1.position(0);
						if (fc.read(bb1) == -1)
						{
							offset = -1;
							return;
						}
						bb1.position(0);
						final int length = bb1.getInt();
						final ByteBuffer bb = ByteBuffer.allocate(length);
						fc.read(bb);
						try
						{
							final ArrayList<Object> row = (ArrayList<Object>)fromBytes(bb.array(), types);
							data.add(row);
						}
						catch (final Exception e)
						{
							HRDBMSWorker.logger.debug("", e);
							throw e;
						}
						i++;
					}

					offset = fc.position();
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.debug("", e);
					ok = false;
					this.e = e;
				}
			}
		}

		private final class ReaderThread extends ThreadPoolThread
		{
			private volatile ArrayList<ArrayList<Object>> array;

			public ReaderThread(final ArrayList<ArrayList<Object>> array)
			{
				this.array = array;
			}

			@Override
			public void run()
			{
				try
				{
					Object o = child.next(SortOperator.this);
					if (o instanceof DataEndMarker)
					{
						demReceived = true;
					}
					else
					{
						received.getAndIncrement();
					}
					while (!(o instanceof DataEndMarker))
					{
						array.add((ArrayList<Object>)o);
						o = child.next(SortOperator.this);
						if (o instanceof DataEndMarker)
						{
							demReceived = true;
						}
						else
						{
							received.getAndIncrement();
						}
					}
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.error("", e);
					try
					{
						readBuffer.put(e);
					}
					catch (final Exception f)
					{
					}
					return;
				}
			}
		}

		private class WriteDataThread extends HRDBMSThread
		{
			private final String fn;
			private byte[] data;
			private Exception e;
			private boolean ok = true;
			private final ConcurrentHashMap<String, RandomAccessFile> map2;
			private final ConcurrentHashMap<String, FileChannel> map3;

			public WriteDataThread(final String fn, final byte[] data, final ConcurrentHashMap<String, RandomAccessFile> map2, final ConcurrentHashMap<String, FileChannel> map3)
			{
				this.fn = fn;
				this.data = data;
				this.map2 = map2;
				this.map3 = map3;
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
				try
				{
					RandomAccessFile raf = null;
					while (true)
					{
						try
						{
							raf = new RandomAccessFile(fn, "rw");
							break;
						}
						catch (final FileNotFoundException e)
						{
							ResourceManager.panic = true;
							try
							{
								Thread.sleep(Integer.parseInt(HRDBMSWorker.getHParms().getProperty("rm_sleep_time_ms")) / 2);
							}
							catch (final Exception f)
							{
							}
						}
					}
					map2.put(fn, raf);
					final FileChannel fc = raf.getChannel();
					map3.put(fn, fc);
					final ByteBuffer bb = ByteBuffer.wrap(data);
					fc.write(bb);
					data = null;
				}
				catch (final Exception e)
				{
					ok = false;
					this.e = e;
				}
			}
		}
	}
}
