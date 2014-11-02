package com.exascale.optimizer;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeMap;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.managers.ResourceManager;
import com.exascale.managers.ResourceManager.DiskBackedHashMap;
import com.exascale.misc.BufferedLinkedBlockingQueue;
import com.exascale.misc.DataEndMarker;
import com.exascale.misc.MurmurHash;
import com.exascale.misc.MyDate;
import com.exascale.misc.MySimpleDateFormat;
import com.exascale.tables.Plan;
import com.exascale.tables.Transaction;
import com.exascale.threads.ThreadPoolThread;

public final class HashJoinOperator extends JoinOperator implements Serializable
{
	private final ArrayList<Operator> children = new ArrayList<Operator>(2);

	private Operator parent;

	private HashMap<String, String> cols2Types;

	private HashMap<String, Integer> cols2Pos;
	private TreeMap<Integer, String> pos2Col;
	private final MetaData meta;
	private volatile BufferedLinkedBlockingQueue outBuffer;
	private final int NUM_RT_THREADS = ResourceManager.cpus;
	private final int NUM_PTHREADS = ResourceManager.cpus;
	private final AtomicLong outCount = new AtomicLong(0);
	private volatile boolean readersDone = false;
	private ArrayList<String> lefts = new ArrayList<String>();
	private ArrayList<String> rights = new ArrayList<String>();
	private volatile ArrayList<DiskBackedHashMap> buckets;
	private final ReentrantLock bucketsLock = new ReentrantLock();
	private CNFFilter cnfFilters;
	private HashSet<HashMap<Filter, Filter>> f;
	private int childPos = -1;
	private final AtomicLong inCount = new AtomicLong(0);
	private final AtomicLong leftCount = new AtomicLong(0);
	private int node;
	private boolean indexAccess = false;
	private ArrayList<Index> dynamicIndexes;
	private final ArrayList<ArrayList<Object>> queuedRows = new ArrayList<ArrayList<Object>>();
	private final MySimpleDateFormat sdf = new MySimpleDateFormat("yyyy-MM-dd");
	private int rightChildCard = 16;
	private boolean cardSet = false;
	private final Vector<Operator> clones = new Vector<Operator>();
	private final Vector<AtomicBoolean> lockVector = new Vector<AtomicBoolean>();
	private final ReentrantLock thisLock = new ReentrantLock();
	private transient Plan plan;
	
	public void setPlan(Plan plan)
	{
		this.plan = plan;
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
	}

	private HashJoinOperator(HashSet<HashMap<Filter, Filter>> f, ArrayList<String> lefts, ArrayList<String> rights, MetaData meta)
	{
		this.meta = meta;
		this.f = f;
		this.lefts = lefts;
		this.rights = rights;
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
				while (i < lefts.size())
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
		for (final Operator child : children)
		{
			child.close();
		}

		if (buckets != null)
		{
			for (final DiskBackedHashMap bucket : buckets)
			{
				bucket.close();
			}
		}

		for (final Operator o : clones)
		{
			o.close();
		}
		
		if (outBuffer != null)
		{
			outBuffer.close();
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

	@Override
	public void start() throws Exception
	{
		if (!indexAccess)
		{
			buckets = new ArrayList<DiskBackedHashMap>();
			buckets.add(ResourceManager.newDiskBackedHashMap(false, rightChildCard));
			for (final Operator child : children)
			{
				child.start();
				// System.out.println("cols2Pos = " + child.getCols2Pos());
			}

			outBuffer = new BufferedLinkedBlockingQueue(ResourceManager.QUEUE_SIZE);
			new InitThread().start();
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
			while (i < clones.size())
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

	private Operator getClone() throws Exception
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
				catch(Exception e)
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

			while (true)
			{
				try
				{
					outBuffer.put(new DataEndMarker());
		
					for (final DiskBackedHashMap bucket : buckets)
					{
						bucket.close();
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
				while (!(o instanceof DataEndMarker))
				{
					final ArrayList<Object> lRow = (ArrayList<Object>)o;
					key.clear();
					for (final String col : lefts)
					{
						final int pos = childCols2Pos.get(col);
						try
						{
							key.add(lRow.get(pos));
						}
						catch (final Exception e)
						{
							HRDBMSWorker.logger.error("Row - " + lRow, e);
							HRDBMSWorker.logger.error("Cols2Pos = " + childCols2Pos);
							HRDBMSWorker.logger.error("Trying to get pos = " + pos);
							HRDBMSWorker.logger.error("Children are " + HashJoinOperator.this.children.get(0) + " and " + HashJoinOperator.this.children.get(1));
							outBuffer.put(e);
							return;
						}
					}

					final long hash = 0x0EFFFFFFFFFFFFFFL & hash(key);
					final ArrayList<ArrayList<Object>> candidates = getCandidates(hash);

					for (final ArrayList<Object> rRow : candidates)
					{
						if (cnfFilters.passes(lRow, rRow))
						{
							final ArrayList<Object> out = new ArrayList<Object>(lRow.size() + rRow.size());
							out.addAll(lRow);
							out.addAll(rRow);
							outBuffer.put(out);
							outCount.incrementAndGet();
						}
					}

					leftCount.incrementAndGet();
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
				catch(Exception f)
				{}
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
				while (!(o instanceof DataEndMarker))
				{
					inCount.incrementAndGet();

					// if (count % 10000 == 0)
					// {
					// System.out.println("HashJoinOperator has read " + count +
					// " rows");
					// }

					final ArrayList<Object> key = new ArrayList<Object>(rights.size());
					for (final String col : rights)
					{
						try
						{
							final int pos = childCols2Pos.get(col);
							key.add(((ArrayList<Object>)o).get(pos));
						}
						catch(Exception e)
						{
							HRDBMSWorker.logger.debug("Failed to find " + col + " in " + childCols2Pos);
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
				catch(Exception f)
				{}
				return;
			}
		}
	}
}
