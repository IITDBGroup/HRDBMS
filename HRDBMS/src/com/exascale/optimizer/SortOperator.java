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
import com.exascale.misc.BinomialHeap;
import com.exascale.misc.BufferedFileChannel;
import com.exascale.misc.BufferedLinkedBlockingQueue;
import com.exascale.misc.DataEndMarker;
import com.exascale.misc.MyDate;
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
		catch (Exception e)
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
	private ArrayList<Boolean> orders;
	private transient SortThread sortThread;
	private transient volatile BufferedLinkedBlockingQueue readBuffer;
	private transient volatile ArrayList<ArrayList<Object>> result;
	private volatile int[] sortPos;
	private transient int NUM_RT_THREADS;
	private transient final MetaData meta;
	private transient volatile boolean isClosed;
	private transient volatile boolean done;
	private int node;
	private int childCard = 16;
	private boolean cardSet = false;
	private transient ArrayList<String> externalFiles;
	private long limit = 0;
	private transient AtomicLong received;
	private transient volatile boolean demReceived;
	private long txnum;

	public SortOperator(ArrayList<String> sortCols, ArrayList<Boolean> orders, MetaData meta)
	{
		this.sortCols = sortCols;
		this.orders = orders;
		this.meta = meta;
		received = new AtomicLong(0);
	}

	public static SortOperator deserialize(InputStream in, HashMap<Long, Object> prev) throws Exception
	{
		SortOperator value = (SortOperator)unsafe.allocateInstance(SortOperator.class);
		prev.put(OperatorUtils.readLong(in), value);
		value.child = OperatorUtils.deserializeOperator(in, prev);
		value.parent = OperatorUtils.deserializeOperator(in, prev);
		value.cols2Types = OperatorUtils.deserializeStringHM(in, prev);
		value.cols2Pos = OperatorUtils.deserializeStringIntHM(in, prev);
		value.pos2Col = OperatorUtils.deserializeTM(in, prev);
		value.startDone = OperatorUtils.readBool(in);
		value.sortCols = OperatorUtils.deserializeALS(in, prev);
		value.orders = OperatorUtils.deserializeALB(in, prev);
		value.sortPos = OperatorUtils.deserializeIntArray(in, prev);
		value.node = OperatorUtils.readInt(in);
		value.childCard = OperatorUtils.readInt(in);
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
	public void add(Operator op) throws Exception
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
			ArrayList<String> newSortCols = new ArrayList<String>();
			for (final String sortCol : sortCols)
			{
				try
				{
					Integer pos = cols2Pos.get(sortCol);
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
						for (String col : cols2Pos.keySet())
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
				catch (Exception e)
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
		final SortOperator retval = new SortOperator((ArrayList<String>)sortCols.clone(), (ArrayList<Boolean>)orders.clone(), meta);
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
			for (String fn : externalFiles)
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
		return orders;
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
	public Object next(Operator op) throws Exception
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
	public void nextAll(Operator op) throws Exception
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
	public void registerParent(Operator op) throws Exception
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
	public void serialize(OutputStream out, IdentityHashMap<Object, Long> prev) throws Exception
	{
		Long id = prev.get(this);
		if (id != null)
		{
			OperatorUtils.serializeReference(id, out);
			return;
		}

		OperatorUtils.writeType(44, out);
		prev.put(this, OperatorUtils.writeID(out));
		child.serialize(out, prev);
		parent.serialize(out, prev);
		OperatorUtils.serializeStringHM(cols2Types, out, prev);
		OperatorUtils.serializeStringIntHM(cols2Pos, out, prev);
		OperatorUtils.serializeTM(pos2Col, out, prev);
		OperatorUtils.writeBool(startDone, out);
		OperatorUtils.serializeALS(sortCols, out, prev);
		OperatorUtils.serializeALB(orders, out, prev);
		OperatorUtils.serializeIntArray(sortPos, out, prev);
		OperatorUtils.writeInt(node, out);
		OperatorUtils.writeInt(childCard, out);
		OperatorUtils.writeBool(cardSet, out);
		OperatorUtils.writeLong(limit, out);
		OperatorUtils.writeLong(txnum, out);
	}

	public boolean setChildCard(int card)
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
	public void setChildPos(int pos)
	{
	}

	public void setLimit(long limit)
	{
		this.limit = limit;
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

	public void setTXNum(long txnum)
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
			ReverseComparator cmp = new ReverseComparator();
			BinomialHeap<ArrayList<Object>> bh = new BinomialHeap<ArrayList<Object>>(cmp);
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

				ArrayList<Object> row = (ArrayList<Object>)o;
				if (bh.size() < limit)
				{
					bh.insert(row);
				}
				else
				{
					int result = cmp.compare(row, bh.peek());
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
			ArrayList<ArrayList<Object>> result = new ArrayList<ArrayList<Object>>();
			while (bh.size() > 0)
			{
				result.add(bh.extractMin());
			}

			Collections.reverse(result);

			for (ArrayList<Object> row : result)
			{
				readBuffer.put(row);
			}

			readBuffer.put(new DataEndMarker());
		}
		catch (Exception e)
		{
			readBuffer.put(e);
		}
	}

	private void mediumSort()
	{
		ArrayList<ArrayList<ArrayList<Object>>> results = new ArrayList<ArrayList<ArrayList<Object>>>();
		double factor = Double.parseDouble(HRDBMSWorker.getHParms().getProperty("external_factor"));
		int size = (int)(ResourceManager.QUEUE_SIZE * factor);
		result = new ArrayList<ArrayList<Object>>(size);
		boolean done = false;
		byte[] types = new byte[pos2Col.size()];
		int j = 0;
		for (String col : pos2Col.values())
		{
			String type = cols2Types.get(col);
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
					Object o = child.next(SortOperator.this);
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
				catch (Exception e)
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
				catch (Exception e)
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
				Object[] underlying = (Object[])unsafe.getObject(result, edOff);
				Arrays.parallelSort(underlying, 0, result.size(), new MergeComparator2());
			}

			results.add(result);
			result = new ArrayList<ArrayList<Object>>(size);
		}

		result = null;
		mergeIntoResult(results);
		readBuffer.put(new DataEndMarker());
		results = null;
	}

	private void mergeIntoResult(ArrayList<ArrayList<ArrayList<Object>>> results)
	{
		int[] poses = new int[results.size()];
		BinomialHeap<ALOO> rows = new BinomialHeap<ALOO>(new MergeComparator());
		ALOO minEntry;
		int i = 0;
		for (ArrayList<ArrayList<Object>> result : results)
		{
			try
			{
				final ArrayList<Object> row = readRow(result, i, poses);
				if (row != null)
				{
					rows.insert(new ALOO(row, i));
				}
			}
			catch (Exception e)
			{
				try
				{
					HRDBMSWorker.logger.debug("", e);
					readBuffer.put(e);
				}
				catch (Exception f)
				{
				}
				return;
			}

			i++;
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
				// final ArrayList<Object> row = minEntry.getOp().readRow();
				int slot = (Integer)minEntry.getOp();
				final ArrayList<Object> row = readRow(results.get(slot), slot, poses);

				if (row != null)
				{
					rows.insert(new ALOO(row, slot));
				}
			}
			catch (Exception e)
			{
				try
				{
					HRDBMSWorker.logger.debug("", e);
					readBuffer.put(e);
				}
				catch (Exception f)
				{
				}
				return;
			}
		}
	}

	private ArrayList<Object> readRow(ArrayList<ArrayList<Object>> result, int i, int[] poses)
	{
		int pos = poses[i];
		if (pos < result.size())
		{
			ArrayList<Object> retval = result.get(pos);
			poses[i]++;
			return retval;
		}

		return null;
	}

	private class ALOO
	{
		private final ArrayList<Object> alo;
		private final Object op;

		public ALOO(ArrayList<Object> alo, Object op)
		{
			this.alo = alo;
			this.op = op;
		}

		@Override
		public boolean equals(Object o)
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
		public int compare(Object l2, Object r2)
		{
			ALOO l = (ALOO)l2;
			ALOO r = (ALOO)r2;
			if (l == r)
			{
				return 0;
			}

			ArrayList<Object> lhs = l.getALO();
			ArrayList<Object> rhs = r.getALO();
			int result = 0;
			int i = 0;

			for (final int pos : sortPos)
			{
				Object lField = lhs.get(pos);
				Object rField = rhs.get(pos);

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

				if (orders.get(i))
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

	private class MergeComparator2 implements Comparator<Object>
	{
		@Override
		public int compare(Object l, Object r)
		{
			if (l == r)
			{
				return 0;
			}

			ArrayList<Object> lhs = (ArrayList<Object>)l;
			ArrayList<Object> rhs = (ArrayList<Object>)r;

			int result = 0;
			int i = 0;

			for (final int pos : sortPos)
			{
				Object lField = lhs.get(pos);
				Object rField = rhs.get(pos);

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

				if (orders.get(i))
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
		public int compare(Object l, Object r)
		{
			if (l == r)
			{
				return 0;
			}

			ArrayList<Object> lhs = (ArrayList<Object>)l;
			ArrayList<Object> rhs = (ArrayList<Object>)r;

			int result = 0;
			int i = 0;

			for (final int pos : sortPos)
			{
				Object lField = lhs.get(pos);
				Object rField = rhs.get(pos);

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

				if (orders.get(i))
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

		public SortThread(Operator child)
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

				if (childCard > ResourceManager.QUEUE_SIZE * Double.parseDouble(HRDBMSWorker.getHParms().getProperty("external_factor")) && childCard < ResourceManager.QUEUE_SIZE * Double.parseDouble(HRDBMSWorker.getHParms().getProperty("hash_external_factor")) / 2)
				{
					mediumSort();
					return;
				}

				if (childCard > ResourceManager.QUEUE_SIZE * Double.parseDouble(HRDBMSWorker.getHParms().getProperty("external_factor")))
				{
					// double percentInMem = (ResourceManager.QUEUE_SIZE *
					// Double.parseDouble(HRDBMSWorker.getHParms().getProperty("hash_external_factor"))
					// / 2) / childCard;
					// percentInMem = percentInMem / 4;
					double percentInMem = 0;
					externalSort(percentInMem);
					return;
				}

				if (result == null)
				{
					// result = ResourceManager.newDiskBackedArray(childCard);
					result = new ArrayList<ArrayList<Object>>(childCard);
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
						Object[] underlying = (Object[])unsafe.getObject(result, edOff);
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
				catch (Exception f)
				{
				}
				return;
			}
		}

		private void externalSort(double percentInMem)
		{
			ConcurrentHashMap<WriteDataThread, WriteDataThread> threads = new ConcurrentHashMap<WriteDataThread, WriteDataThread>();
			ArrayList<ArrayList<ArrayList<Object>>> bins = new ArrayList<ArrayList<ArrayList<Object>>>();
			HRDBMSWorker.logger.debug("Doing external sort");
			// double factor =
			// Double.parseDouble(HRDBMSWorker.getHParms().getProperty("external_factor"));
			// int size = (int)(ResourceManager.QUEUE_SIZE * factor);
			int size = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("sort_bucket_size"));
			int numInMem = (int)((childCard * percentInMem) / size);
			externalFiles = new ArrayList<String>();
			result = new ArrayList<ArrayList<Object>>(size);
			ConcurrentHashMap<String, HRDBMSThread> map = new ConcurrentHashMap<String, HRDBMSThread>();
			ConcurrentHashMap<String, RandomAccessFile> map2 = new ConcurrentHashMap<String, RandomAccessFile>();
			ConcurrentHashMap<String, FileChannel> map3 = new ConcurrentHashMap<String, FileChannel>();
			boolean done = false;
			byte[] types = new byte[pos2Col.size()];
			int j = 0;
			for (String col : pos2Col.values())
			{
				String type = cols2Types.get(col);
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
						Object o = child.next(SortOperator.this);
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
					catch (Exception e)
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
					catch (Exception e)
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
					Object[] underlying = (Object[])unsafe.getObject(result, edOff);
					Arrays.parallelSort(underlying, 0, result.size(), new MergeComparator2());
				}

				String fn = ResourceManager.TEMP_DIRS.get(j % ResourceManager.TEMP_DIRS.size()) + this.hashCode() + "" + System.currentTimeMillis() + ".extsrt";
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
						byte[] data = rsToBytes(result, types);
						WriteDataThread thread = new WriteDataThread(fn, data, map2, map3);
						TempThread.start(thread, txnum);
						threads.put(thread, thread);
						map.put(fn, thread);
						result.clear();
					}
					j++;
				}
				catch (Exception e)
				{
					readBuffer.put(e);
					return;
				}
			}

			result = null;
			// at this point all the threads are listed in the map
			// some might be running still
			for (Map.Entry entry : map.entrySet())
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
					catch (InterruptedException e)
					{
					}
				}

				try
				{
					ReadDataThread thread = new ReadDataThread((String)entry.getKey(), 0, size / map.size(), map2, map3, types);
					thread.doStart();
					map.put((String)entry.getKey(), thread);
				}
				catch (Exception e)
				{
					HRDBMSWorker.logger.debug("", e);
				}
			}

			// reads for all first chunks are started
			for (Map.Entry entry : map.entrySet())
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
					catch (InterruptedException e)
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

			for (FileChannel fc : map3.values())
			{
				try
				{
					fc.close();
				}
				catch (Exception e)
				{
				}
			}

			for (RandomAccessFile raf : map2.values())
			{
				try
				{
					raf.close();
				}
				catch (Exception e)
				{
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

		private int getCInt(ByteBuffer bb)
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

		private long getCLong(ByteBuffer bb)
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

		private int getMedium(ByteBuffer bb)
		{
			int retval = ((bb.getShort() & 0xffff) << 8);
			retval += (bb.get() & 0xff);
			return retval;
		}

		private void mergeIntoResult(Collection<HRDBMSThread> readThreads)
		{
			BinomialHeap<ALOO> rows = new BinomialHeap<ALOO>(new MergeComparator());
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
				catch (Exception e)
				{
					try
					{
						HRDBMSWorker.logger.debug("", e);
						readBuffer.put(e);
					}
					catch (Exception f)
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
				catch (Exception e)
				{
					try
					{
						HRDBMSWorker.logger.debug("", e);
						readBuffer.put(e);
					}
					catch (Exception f)
					{
					}
					return;
				}
			}
		}

		private void putCInt(ByteBuffer bb, int val)
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

		private void putCLong(ByteBuffer bb, long val)
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

		private void putMedium(ByteBuffer bb, int val)
		{
			bb.put((byte)((val & 0xff0000) >> 16));
			bb.put((byte)((val & 0xff00) >> 8));
			bb.put((byte)(val & 0xff));
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
						int value = ((MyDate)o).getTime();
						// retvalBB.putInt(value);
						putMedium(retvalBB, value);
					}
					else if (types[i] == 4)
					{
						byte[] temp = bytes.get(x++);
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
					catch (Exception f)
					{
					}
					return;
				}
			}
		}

		private class MergeComparator2 implements Comparator<Object>
		{
			@Override
			public int compare(Object l, Object r)
			{
				if (l == r)
				{
					return 0;
				}

				ArrayList<Object> lhs = (ArrayList<Object>)l;
				ArrayList<Object> rhs = (ArrayList<Object>)r;

				int result = 0;
				int i = 0;

				for (final int pos : sortPos)
				{
					Object lField = lhs.get(pos);
					Object rField = rhs.get(pos);

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

					if (orders.get(i))
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

			public ReadDataThread(String fn, long offset, int numRecs, ConcurrentHashMap<String, RandomAccessFile> map2, ConcurrentHashMap<String, FileChannel> map3, byte[] types) throws Exception
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

			public ReadDataThread(String fn, long offset, int numRecs, ConcurrentHashMap<String, RandomAccessFile> map2, ConcurrentHashMap<String, FileChannel> map3, byte[] types, int index, ArrayList<ArrayList<ArrayList<Object>>> bins)
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
					catch (InterruptedException e)
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
					FileChannel f = map3.get(fn);
					FileChannel fc = new BufferedFileChannel(f, 1 * 1024 * 1024);
					//
					fc.position(offset);
					int i = 0;
					ByteBuffer bb1 = ByteBuffer.allocate(4);

					while (i < numRecs)
					{
						bb1.position(0);
						if (fc.read(bb1) == -1)
						{
							offset = -1;
							return;
						}
						bb1.position(0);
						int length = bb1.getInt();
						ByteBuffer bb = ByteBuffer.allocate(length);
						fc.read(bb);
						try
						{
							ArrayList<Object> row = (ArrayList<Object>)fromBytes(bb.array(), types);
							data.add(row);
						}
						catch (Exception e)
						{
							HRDBMSWorker.logger.debug("", e);
							throw e;
						}
						i++;
					}

					offset = fc.position();
				}
				catch (Exception e)
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

			public ReaderThread(ArrayList<ArrayList<Object>> array)
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
					catch (Exception f)
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

			public WriteDataThread(String fn, byte[] data, ConcurrentHashMap<String, RandomAccessFile> map2, ConcurrentHashMap<String, FileChannel> map3)
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
					map2.put(fn, raf);
					FileChannel fc = raf.getChannel();
					map3.put(fn, fc);
					ByteBuffer bb = ByteBuffer.wrap(data);
					fc.write(bb);
					data = null;
				}
				catch (Exception e)
				{
					ok = false;
					this.e = e;
				}
			}
		}
	}
}
