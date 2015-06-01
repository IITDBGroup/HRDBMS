package com.exascale.optimizer;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.managers.ResourceManager;
import com.exascale.misc.BufferedLinkedBlockingQueue;
import com.exascale.misc.DataEndMarker;
import com.exascale.optimizer.AggregateOperator.AggregateResultThread;
import com.exascale.tables.Plan;
import com.exascale.threads.ThreadPoolThread;

public final class MultiOperator implements Operator, Serializable
{
	private static final int ATHREAD_QUEUE_SIZE = 1000;

	private static final int NUM_HGBR_THREADS = ResourceManager.cpus;

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

	private Operator child;
	private Operator parent;
	private HashMap<String, String> cols2Types;
	private HashMap<String, Integer> cols2Pos;
	private TreeMap<Integer, String> pos2Col;
	private transient final MetaData meta;
	private ArrayList<AggregateOperator> ops;
	private ArrayList<String> groupCols;
	private transient volatile BufferedLinkedBlockingQueue inFlight;
	private transient volatile BufferedLinkedBlockingQueue readBuffer;
	private boolean sorted;
	private int node;
	private int NUM_GROUPS = 16;
	private int childCard = 16 * 16;

	private boolean cardSet = false;

	private volatile boolean startDone = false;

	public MultiOperator(ArrayList<AggregateOperator> ops, ArrayList<String> groupCols, MetaData meta, boolean sorted)
	{
		this.ops = ops;
		this.groupCols = groupCols;
		this.meta = meta;
		this.sorted = sorted;
	}

	public static MultiOperator deserialize(InputStream in, HashMap<Long, Object> prev) throws Exception
	{
		MultiOperator value = (MultiOperator)unsafe.allocateInstance(MultiOperator.class);
		prev.put(OperatorUtils.readLong(in), value);
		value.child = OperatorUtils.deserializeOperator(in, prev);
		value.parent = OperatorUtils.deserializeOperator(in, prev);
		value.cols2Types = OperatorUtils.deserializeStringHM(in, prev);
		value.cols2Pos = OperatorUtils.deserializeStringIntHM(in, prev);
		value.pos2Col = OperatorUtils.deserializeTM(in, prev);
		value.ops = OperatorUtils.deserializeALAgOp(in, prev);
		value.groupCols = OperatorUtils.deserializeALS(in, prev);
		value.sorted = OperatorUtils.readBool(in);
		value.node = OperatorUtils.readInt(in);
		value.NUM_GROUPS = OperatorUtils.readInt(in);
		value.childCard = OperatorUtils.readInt(in);
		value.cardSet = OperatorUtils.readBool(in);
		value.startDone = OperatorUtils.readBool(in);
		return value;
	}

	@Override
	public void add(Operator op) throws Exception
	{
		if (child == null)
		{
			child = op;
			op.registerParent(this);
			if (child.getCols2Types() != null)
			{
				try
				{
					final HashMap<String, String> tempCols2Types = child.getCols2Types();
					child.getCols2Pos();
					cols2Types = new HashMap<String, String>();
					cols2Pos = new HashMap<String, Integer>();
					pos2Col = new TreeMap<Integer, String>();

					int i = 0;
					ArrayList<String> newGroupCols = new ArrayList<String>();
					for (final String groupCol : groupCols)
					{
						if (!groupCol.startsWith("."))
						{
							cols2Types.put(groupCol, tempCols2Types.get(groupCol));
							cols2Pos.put(groupCol, i);
							pos2Col.put(i, groupCol);
							newGroupCols.add(groupCol);
						}
						else
						{
							int matches = 0;
							for (String col : tempCols2Types.keySet())
							{
								String col2 = col.substring(col.indexOf('.'));
								if (col2.equals(groupCol))
								{
									cols2Types.put(col, tempCols2Types.get(col));
									cols2Pos.put(col, i);
									pos2Col.put(i, col);
									newGroupCols.add(col);
									matches++;
								}
							}

							if (matches != 1)
							{
								HRDBMSWorker.logger.debug("Could not find " + groupCol + " in " + tempCols2Types.keySet());
								throw new Exception("Column does not exist or is ambiguous: " + groupCol);
							}
						}
						i++;
					}

					groupCols = newGroupCols;
					for (final AggregateOperator op2 : ops)
					{
						cols2Types.put(op2.outputColumn(), op2.outputType());
						cols2Pos.put(op2.outputColumn(), i);
						pos2Col.put(i, op2.outputColumn());
						i++;
					}
				}
				catch (Exception e)
				{
					HRDBMSWorker.logger.debug("", e);
					throw e;
				}
			}
		}
		else
		{
			throw new Exception("MultiOperator only supports 1 child.");
		}
	}

	public void addCount(String outCol)
	{
		ops.add(new CountOperator(outCol, meta));
	}

	public void changeCountsToSums()
	{
		final ArrayList<AggregateOperator> remove = new ArrayList<AggregateOperator>();
		final ArrayList<AggregateOperator> add = new ArrayList<AggregateOperator>();
		for (final AggregateOperator op : ops)
		{
			if (op instanceof CountOperator)
			{
				remove.add(op);
				add.add(new SumOperator(op.getInputColumn(), op.outputColumn(), meta, true));
			}
		}

		int i = 0;
		for (final AggregateOperator op : remove)
		{
			final int pos = ops.indexOf(op);
			ops.remove(pos);
			ops.add(pos, add.get(i));
			i++;
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
	public MultiOperator clone()
	{
		final ArrayList<AggregateOperator> opsClone = new ArrayList<AggregateOperator>(ops.size());
		for (final AggregateOperator op : ops)
		{
			opsClone.add(op.clone());
		}

		final MultiOperator retval = new MultiOperator(opsClone, groupCols, meta, sorted);
		retval.node = node;
		retval.NUM_GROUPS = NUM_GROUPS;
		retval.childCard = childCard;
		retval.cardSet = cardSet;
		return retval;
	}

	@Override
	public void close() throws Exception
	{
		child.close();

		if (inFlight != null)
		{
			inFlight.close();
		}

		if (readBuffer != null)
		{
			readBuffer.close();
		}

		ops = null;
		groupCols = null;
		cols2Pos = null;
		cols2Types = null;
		pos2Col = null;
	}

	public boolean existsCountDistinct()
	{
		for (final AggregateOperator op : ops)
		{
			if (op instanceof CountDistinctOperator)
			{
				return true;
			}
		}

		return false;
	}

	public String getAvgCol()
	{
		for (final AggregateOperator op : ops)
		{
			if (op instanceof AvgOperator)
			{
				return op.outputColumn();
			}
		}

		return null;
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

	public ArrayList<String> getInputCols()
	{
		final ArrayList<String> retval = new ArrayList<String>(ops.size());
		for (final AggregateOperator op : ops)
		{
			retval.add(op.getInputColumn());
		}

		return retval;
	}

	public ArrayList<String> getKeys()
	{
		return groupCols;
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

	public ArrayList<String> getOutputCols()
	{
		final ArrayList<String> retval = new ArrayList<String>(ops.size());
		for (final AggregateOperator op : ops)
		{
			retval.add(op.outputColumn());
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
		final ArrayList<String> retval = new ArrayList<String>(ops.size());
		for (final AggregateOperator op : ops)
		{
			retval.add(op.getInputColumn());
		}

		for (String col : groupCols)
		{
			if (!retval.contains(col))
			{
				retval.add(col);
			}
		}

		return retval;
	}

	public boolean hasAvg()
	{
		for (final AggregateOperator op : ops)
		{
			if (op instanceof AvgOperator)
			{
				return true;
			}
		}

		return false;
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
		child.nextAll(op);
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
			throw new Exception("MultiOperator only supports 1 parent.");
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

	public void replaceAvgWithSumAndCount(HashMap<String, ArrayList<String>> old2New)
	{
		for (final AggregateOperator op : (ArrayList<AggregateOperator>)ops.clone())
		{
			if (op instanceof AvgOperator)
			{
				String outCol1 = null;
				String outCol2 = null;
				for (final Map.Entry entry : old2New.entrySet())
				{
					outCol1 = ((ArrayList<String>)entry.getValue()).get(0);
					outCol2 = ((ArrayList<String>)entry.getValue()).get(1);
				}
				ops.remove(op);
				ops.add(new SumOperator(op.getInputColumn(), outCol1, meta, false));
				ops.add(new CountOperator(op.getInputColumn(), outCol2, meta));
				return;
			}
		}
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
			if (inFlight != null)
			{
				inFlight.close();
			}
			inFlight = new BufferedLinkedBlockingQueue(ATHREAD_QUEUE_SIZE);
			readBuffer.clear();
			if (sorted)
			{
				init();
			}
			else
			{
				new HashGroupByThread().start();
			}
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

		OperatorUtils.writeType(33, out);
		prev.put(this, OperatorUtils.writeID(out));
		child.serialize(out, prev);
		parent.serialize(out, prev);
		OperatorUtils.serializeStringHM(cols2Types, out, prev);
		OperatorUtils.serializeStringIntHM(cols2Pos, out, prev);
		OperatorUtils.serializeTM(pos2Col, out, prev);
		OperatorUtils.serializeALAgOp(ops, out, prev);
		OperatorUtils.serializeALS(groupCols, out, prev);
		OperatorUtils.writeBool(sorted, out);
		OperatorUtils.writeInt(node, out);
		OperatorUtils.writeInt(NUM_GROUPS, out);
		OperatorUtils.writeInt(childCard, out);
		OperatorUtils.writeBool(cardSet, out);
		OperatorUtils.writeBool(startDone, out);
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

	public boolean setNumGroupsAndChildCard(int groups, int childCard)
	{
		if (cardSet)
		{
			return false;
		}

		cardSet = true;
		NUM_GROUPS = groups;
		this.childCard = childCard;
		for (final AggregateOperator op : ops)
		{
			op.setNumGroups(NUM_GROUPS);
			if (op instanceof CountDistinctOperator)
			{
				((CountDistinctOperator)op).setChildCard(childCard);
			}
		}

		return true;
	}

	@Override
	public void setPlan(Plan plan)
	{
	}

	public void setSorted()
	{
		sorted = true;
	}

	@Override
	public void start() throws Exception
	{
		startDone = true;
		child.start();
		inFlight = new BufferedLinkedBlockingQueue(ATHREAD_QUEUE_SIZE);
		readBuffer = new BufferedLinkedBlockingQueue(ResourceManager.QUEUE_SIZE);
		if (sorted)
		{
			init();
		}
		else
		{
			// System.out.println("HasGroupByThread created via start()");
			new HashGroupByThread().start();
		}
	}

	@Override
	public String toString()
	{
		String retval = "MultiOperator: [";
		int i = 0;
		for (final String in : getInputCols())
		{
			retval += (in + "->" + getOutputCols().get(i) + "  ");
			i++;
		}

		retval += ("] group by " + groupCols);
		return retval;
	}

	public void updateInputColumns(ArrayList<String> outputs, ArrayList<String> inputs)
	{
		for (final AggregateOperator op : ops)
		{
			final int index = outputs.indexOf(op.outputColumn());
			op.setInputColumn(inputs.get(index));
		}
	}

	private void init()
	{
		new InitThread().start();
		new CleanerThread().start();
	}

	public final class AggregateThread
	{
		private final ArrayList<ThreadPoolThread> threads = new ArrayList<ThreadPoolThread>();
		ArrayList<Object> row = new ArrayList<Object>();
		private boolean end = false;

		public AggregateThread()
		{
			end = true;
		}

		public AggregateThread(Object[] groupKeys, ArrayList<AggregateOperator> ops, ArrayList<ArrayList<Object>> rows)
		{
			for (final Object o : groupKeys)
			{
				row.add(o);
			}

			for (final AggregateOperator op : ops)
			{
				final ThreadPoolThread thread = op.newProcessingThread(rows, child.getCols2Pos());
				threads.add(thread);
			}
		}

		public ArrayList<Object> getResult()
		{
			for (final ThreadPoolThread thread : threads)
			{
				final AggregateResultThread t = (AggregateResultThread)thread;
				// while (true)
				// {
				// try
				// {
				// t.join();
				// break;
				// }
				// catch(InterruptedException e)
				// {
				// continue;
				// }
				// }
				row.add(t.getResult());
				t.close();
			}

			threads.clear();
			return row;
		}

		public boolean isEnd()
		{
			return end;
		}

		public void start()
		{
			if (end)
			{
				return;
			}

			for (final ThreadPoolThread thread : threads)
			{
				thread.start();
			}
		}
	}

	private final class CleanerThread extends ThreadPoolThread
	{
		@Override
		public void run()
		{
			while (true)
			{
				AggregateThread t = null;
				while (true)
				{
					try
					{
						t = (AggregateThread)inFlight.take();
						break;
					}
					catch (final Exception e)
					{
					}
				}
				if (t.isEnd())
				{
					while (true)
					{
						try
						{
							readBuffer.put(new DataEndMarker());
							break;
						}
						catch (final Exception e)
						{
						}
					}
					// System.out.println("MultiOperator marked end of output.");
					return;
				}

				while (true)
				{
					try
					{
						readBuffer.put(t.getResult());
						break;
					}
					catch (final Exception e)
					{
					}
				}
				// System.out.println("Picked up aggregation result.");
			}
		}
	}

	private final class HashGroupByThread extends ThreadPoolThread
	{
		// private volatile DiskBackedHashSet groups =
		// ResourceManager.newDiskBackedHashSet(true, NUM_GROUPS > 0 ?
		// NUM_GROUPS : 16);
		private volatile ConcurrentHashMap<ArrayList<Object>, ArrayList<Object>> groups = new ConcurrentHashMap<ArrayList<Object>, ArrayList<Object>>(NUM_GROUPS, 0.75f, 6 * ResourceManager.cpus);

		private final AggregateResultThread[] threads = new AggregateResultThread[ops.size()];

		@Override
		public void run()
		{
			try
			{
				int i = 0;
				for (final AggregateOperator op : ops)
				{
					threads[i] = op.getHashThread(child.getCols2Pos());
					i++;
				}

				i = 0;
				final HashGroupByReaderThread[] threads2 = new HashGroupByReaderThread[NUM_HGBR_THREADS];
				while (i < NUM_HGBR_THREADS)
				{
					threads2[i] = new HashGroupByReaderThread();
					threads2[i].start();
					i++;
				}

				i = 0;
				while (i < NUM_HGBR_THREADS)
				{
					threads2[i].join();
					i++;
				}

				// groups.close();

				for (final Object k : groups.keySet())
				// for (Object k : groups.getArray())
				{
					final ArrayList<Object> keys = (ArrayList<Object>)k;
					final ArrayList<Object> row = new ArrayList<Object>();
					for (final Object field : keys)
					{
						row.add(field);
					}

					for (final AggregateResultThread thread : threads)
					{
						row.add(thread.getResult(keys));
					}

					readBuffer.put(row);
				}

				// groups.getArray().close();
				readBuffer.put(new DataEndMarker());

				for (final AggregateResultThread thread : threads)
				{
					thread.close();
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

		private final class HashGroupByReaderThread extends ThreadPoolThread
		{
			private ArrayList<Integer> groupPos = null;

			@Override
			public void run()
			{
				try
				{
					Object o = child.next(MultiOperator.this);
					while (!(o instanceof DataEndMarker))
					{
						final ArrayList<Object> row = (ArrayList<Object>)o;
						final ArrayList<Object> groupKeys = new ArrayList<Object>();

						if (groupPos == null)
						{
							groupPos = new ArrayList<Integer>(groupCols.size());
							for (final String groupCol : groupCols)
							{
								groupPos.add(child.getCols2Pos().get(groupCol));
							}
						}

						try
						{
							for (final int pos : groupPos)
							{
								groupKeys.add(row.get(pos));
							}
						}
						catch (Exception e)
						{
							HRDBMSWorker.logger.debug("Trying to group on " + groupCols);
							HRDBMSWorker.logger.debug("Child.getCols2Pos() = " + child.getCols2Pos());
							throw e;
						}

						groups.put(groupKeys, groupKeys);
						// groups.add(groupKeys);

						for (final AggregateResultThread thread : threads)
						{
							thread.put(row, groupKeys);
						}

						o = child.next(MultiOperator.this);
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
	}

	private final class InitThread extends ThreadPoolThread
	{
		@Override
		public void run()
		{
			try
			{
				final Object[] groupKeys = new Object[groupCols.size()];
				Object[] oldGroup = null;
				ArrayList<ArrayList<Object>> rows = null;
				boolean newGroup = false;
				;

				Object o = child.next(MultiOperator.this);
				while (!(o instanceof DataEndMarker))
				{
					newGroup = false;
					oldGroup = null;
					final ArrayList<Object> row = (ArrayList<Object>)o;
					int i = 0;
					for (final String groupCol : groupCols)
					{
						if (row.get(child.getCols2Pos().get(groupCol)).equals(groupKeys[i]))
						{
						}
						else
						{
							newGroup = true;
							if (oldGroup == null)
							{
								oldGroup = groupKeys.clone();
							}
							groupKeys[i] = row.get(child.getCols2Pos().get(groupCol));
						}

						i++;
					}

					if (newGroup)
					{
						if (rows != null)
						{
							final AggregateThread aggThread = new AggregateThread(oldGroup, ops, rows);
							aggThread.start();
							while (true)
							{
								try
								{
									inFlight.put(aggThread);
									break;
								}
								catch (final Exception f)
								{
								}
							}
						}

						rows = new ArrayList<ArrayList<Object>>();
					}

					rows.add(row);
					o = child.next(MultiOperator.this);
				}

				AggregateThread aggThread = new AggregateThread(groupKeys, ops, rows);
				aggThread.start();
				while (true)
				{
					try
					{
						inFlight.put(aggThread);
						break;
					}
					catch (final Exception f)
					{
					}
				}
				// System.out.println("Last aggregation thread has been started.");

				aggThread = new AggregateThread();
				while (true)
				{
					try
					{
						inFlight.put(aggThread);
						break;
					}
					catch (final Exception f)
					{
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
}
