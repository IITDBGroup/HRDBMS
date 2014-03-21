package com.exascale.optimizer;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeMap;
import java.util.Vector;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
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

public final class NestedLoopJoinOperator extends JoinOperator implements Serializable
{
	private final ArrayList<Operator> children = new ArrayList<Operator>(2);

	private Operator parent;

	private HashMap<String, String> cols2Types;

	private HashMap<String, Integer> cols2Pos;

	private TreeMap<Integer, String> pos2Col;

	private final MetaData meta;
	private volatile BufferedLinkedBlockingQueue outBuffer;
	private volatile DiskBackedArray inBuffer;
	private int NUM_RT_THREADS = 4 * ResourceManager.cpus;
	private final int NUM_PTHREADS = 4 * ResourceManager.cpus;
	private final AtomicLong outCount = new AtomicLong(0);
	private volatile boolean readersDone = false;
	private CNFFilter cnfFilters;
	private HashSet<HashMap<Filter, Filter>> f;
	private int childPos = -1;
	private int node;
	private boolean indexAccess = false;
	private ArrayList<Index> dynamicIndexes;
	private final ArrayList<ArrayList<Object>> queuedRows = new ArrayList<ArrayList<Object>>();
	private final MySimpleDateFormat sdf = new MySimpleDateFormat("yyyy-MM-dd");
	private int rightChildCard = 16;
	private final ArrayList<DiskBackedHashMap> buckets = new ArrayList<DiskBackedHashMap>();
	private final ReentrantLock bucketsLock = new ReentrantLock();
	private final AtomicLong inCount2 = new AtomicLong(0);
	private boolean alreadySorted = false;
	private boolean cardSet = false;
	private final Vector<Operator> clones = new Vector<Operator>();
	private final Vector<AtomicBoolean> lockVector = new Vector<AtomicBoolean>();
	private final ReentrantLock thisLock = new ReentrantLock();
	private Plan plan;
	
	public void setPlan(Plan plan)
	{
		this.plan = plan;
	}

	public NestedLoopJoinOperator(ArrayList<Filter> filters, MetaData meta)
	{
		this.meta = meta;
		this.addFilter(filters);
	}

	public NestedLoopJoinOperator(JoinOperator op)
	{
		this.meta = op.getMeta();
		this.f = op.getHSHMFilter();
	}

	private NestedLoopJoinOperator(MetaData meta)
	{
		this.meta = meta;
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
				cnfFilters = new CNFFilter(f, meta, cols2Pos);
			}
		}
		else
		{
			throw new Exception("NestedLoopJoinOperator only supports 2 children");
		}
	}

	public void addFilter(ArrayList<Filter> filters)
	{
		if (f == null)
		{
			f = new HashSet<HashMap<Filter, Filter>>();
			final HashMap<Filter, Filter> map = new HashMap<Filter, Filter>();
			for (final Filter filter : filters)
			{
				map.put(filter, filter);
			}

			f.add(map);
		}
		else
		{
			final HashMap<Filter, Filter> map = new HashMap<Filter, Filter>();
			for (final Filter filter : filters)
			{
				map.put(filter, filter);
			}

			f.add(map);
		}
	}

	@Override
	public void addJoinCondition(ArrayList<Filter> filters)
	{
		addFilter(filters);
	}

	@Override
	public void addJoinCondition(String left, String right)
	{
		throw new UnsupportedOperationException("NestedLoopJoinOperator does not support addJoinCondition(String, String)");
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
	public NestedLoopJoinOperator clone()
	{
		final NestedLoopJoinOperator retval = new NestedLoopJoinOperator(meta);
		retval.f = this.getHSHMFilter();
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

	public HashSet<HashMap<Filter, Filter>> getHSHM()
	{
		return getHSHMFilter();
	}

	@Override
	public HashSet<HashMap<Filter, Filter>> getHSHMFilter()
	{
		return f;
	}

	@Override
	public ArrayList<String> getJoinForChild(Operator op)
	{
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
		final ArrayList<String> retval = new ArrayList<String>(f.size());
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
		return retval;
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
					return queuedRows.remove(0);
				}
			}

			while (true)
			{
				final Object o = children.get(0).next(this);
				if (o instanceof DataEndMarker)
				{
					return o;
				}
				final ArrayList<Filter> dynamics = new ArrayList<Filter>(this.cnfFilters.getALAL().size());
				for (final ArrayList<Filter> al : this.cnfFilters.getALAL())
				{
					final Filter f = al.get(0);
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
		return o;
	}

	@Override
	public void nextAll(Operator op) throws Exception
	{
		children.get(0).nextAll(op);
		children.get(1).nextAll(op);
		Object o = next(op);
		while (!(o instanceof DataEndMarker))
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
			throw new Exception("NestedLoopJoinOperator can only have 1 parent.");
		}
	}

	@Override
	public void removeChild(Operator op)
	{
		childPos = children.indexOf(op);
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
		HRDBMSWorker.logger.error("NestedLoopJoinOperator cannot be reset");
		System.exit(1);
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
		if (!indexAccess)
		{
			inBuffer = ResourceManager.newDiskBackedArray(rightChildCard);
			for (final Operator child : children)
			{
				child.start();
			}

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
		return "NestedLoopJoinOperator";
	}

	public boolean usesHash()
	{
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
				System.exit(1);
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
			while (i < clones.size())
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

	private Operator getClone()
	{
		synchronized (clones)
		{
			int i = 0;
			while (i < lockVector.size())
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
				System.exit(1);
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
												nlHash.join();
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
				int i = 0;
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
				int i = 0;
				threads = new ReaderThread[NUM_RT_THREADS];
				while (i < NUM_RT_THREADS)
				{
					threads[i] = new ReaderThread();
					threads[i].start();
					i++;
				}
			}

			int i = 0;
			final ThreadPoolThread[] threads2 = new ProcessThread[NUM_PTHREADS];
			while (i < NUM_PTHREADS)
			{
				if (nlSort == null && nlHash == null)
				{
					threads2[i] = new ProcessThread();
				}
				else if (nlSort != null)
				{
					threads2[i] = new ProcessThread(first);
				}
				else
				{
					threads2[i] = new ProcessThread(nlHash, first);
				}
				threads2[i].start();
				i++;
			}

			if (nlSort == null && nlHash == null)
			{
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
				nlHash.close();
			}

			while (true)
			{
				try
				{
					outBuffer.put(new DataEndMarker());
					inBuffer.close();
					break;
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

		public void close()
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
					System.exit(1);
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
				Object o = child.next(NestedLoopJoinOperator.this);
				final ArrayList<Object> key = new ArrayList<Object>(1);
				// @Parallel
				while (!(o instanceof DataEndMarker))
				{
					inCount2.incrementAndGet();

					// if (count % 10000 == 0)
					// {
					// System.out.println("HashJoinOperator has read " + count +
					// " rows");
					// }

					key.clear();
					key.add(((ArrayList<Object>)o).get(pos));

					final long hash = 0x0EFFFFFFFFFFFFFFL & hash(key);
					writeToHashTable(hash, (ArrayList<Object>)o);
					o = child.next(NestedLoopJoinOperator.this);
				}
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("Error in partial hash thread.", e);
				HRDBMSWorker.logger.error("Left child cols to pos is: " + children.get(0).getCols2Pos());
				HRDBMSWorker.logger.error("Right child cols to pos is: " + children.get(1).getCols2Pos());
				System.exit(1);
			}
		}

		private final ArrayList<ArrayList<Object>> getCandidates(long hash) throws ClassNotFoundException, IOException
		{
			final ArrayList<ArrayList<Object>> retval = new ArrayList<ArrayList<Object>>();
			int i = 0;
			ArrayList<Object> o = buckets.get(i).get(hash);
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

		private long hash(Object key)
		{
			long eHash;
			if (key == null)
			{
				eHash = 0;
			}
			else
			{
				eHash = MurmurHash.hash64(key.toString());
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
				System.exit(1);
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
					System.exit(1);
				}
			}
		}
	}

	private final class ProcessThread extends ThreadPoolThread
	{
		private Filter first = null;
		private NLHashThread nlHash = null;

		public ProcessThread()
		{
		}

		public ProcessThread(Filter first)
		{
			this.first = first;
		}

		public ProcessThread(NLHashThread nlHash, Filter first)
		{
			this.nlHash = nlHash;
			this.first = first;
		}

		@Override
		public void run()
		{
			try
			{
				final Operator left = children.get(0);
				Object o = left.next(NestedLoopJoinOperator.this);
				int pos = -1;
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
				}
				// @Parallel
				while (!(o instanceof DataEndMarker))
				{
					final ArrayList<Object> lRow = (ArrayList<Object>)o;
					long i = 0;
					while (true)
					{
						if (nlHash != null)
						{
							final ArrayList<Object> key = new ArrayList<Object>(1);
							try
							{
								key.add(lRow.get(pos));
							}
							catch (final Exception e)
							{
								HRDBMSWorker.logger.error("pos = " + pos, e);
								HRDBMSWorker.logger.error("lRow = " + lRow);
								HRDBMSWorker.logger.error("key = " + key);
								System.exit(1);
							}
							final long hash = 0x0EFFFFFFFFFFFFFFL & nlHash.hash(key);
							for (final ArrayList<Object> rRow : nlHash.getCandidates(hash))
							{
								if (cnfFilters.passes(lRow, rRow))
								{
									final ArrayList<Object> out = new ArrayList<Object>(lRow.size() + rRow.size());
									out.addAll(lRow);
									out.addAll(rRow);
									outBuffer.put(out);
									final long count = outCount.incrementAndGet();
									if (count % 100000 == 0)
									{
										HRDBMSWorker.logger.debug("NestedLoopJoinOperator has output " + count + " rows");
									}
								}
							}

							break;
						}

						if (i >= inBuffer.size())
						{
							if (readersDone)
							{
								if (i >= inBuffer.size())
								{
									break;
								}

								continue;
							}
							else
							{
								LockSupport.parkNanos(75000);
								continue;
							}
						}
						final Object orow = inBuffer.get(i);
						i++;
						final ArrayList<Object> rRow = (ArrayList<Object>)orow;

						if (orow == null)
						{
							LockSupport.parkNanos(75000);
							continue;
						}

						if (cnfFilters.passes(lRow, rRow))
						{
							final ArrayList<Object> out = new ArrayList<Object>(lRow.size() + rRow.size());
							out.addAll(lRow);
							out.addAll(rRow);
							outBuffer.put(out);
							final long count = outCount.incrementAndGet();
							if (count % 100000 == 0)
							{
								HRDBMSWorker.logger.debug("NestedLoopJoinOperator has output " + count + " rows");
							}
						}
						else
						{
							if (first != null)
							{
								if (!first.passes(lRow, rRow, cols2Pos))
								{
									break;
								}
							}
						}
					}

					o = left.next(NestedLoopJoinOperator.this);
				}
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				System.exit(1);
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
				Object o = child.next(NestedLoopJoinOperator.this);
				while (!(o instanceof DataEndMarker))
				{
					inBuffer.add((ArrayList<Object>)o);
					o = child.next(NestedLoopJoinOperator.this);
				}
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				System.exit(1);
			}
		}
	}
}
