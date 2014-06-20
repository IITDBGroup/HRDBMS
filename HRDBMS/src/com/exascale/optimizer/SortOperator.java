package com.exascale.optimizer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.managers.ResourceManager;
import com.exascale.managers.ResourceManager.DiskBackedArray;
import com.exascale.misc.BufferedLinkedBlockingQueue;
import com.exascale.misc.DataEndMarker;
import com.exascale.misc.MyDate;
import com.exascale.tables.Plan;
import com.exascale.threads.ThreadPoolThread;

public final class SortOperator implements Operator, Serializable
{
	static final int PARALLEL_SORT_MIN_NUM_ROWS = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("parallel_sort_min_rows")); // 50000

	private Operator child;
	private Operator parent;
	private HashMap<String, String> cols2Types;
	private HashMap<String, Integer> cols2Pos;
	private TreeMap<Integer, String> pos2Col;
	private boolean startDone = false;
	private boolean closeDone = false;
	private final ArrayList<String> sortCols;
	private final ArrayList<Boolean> orders;
	private volatile boolean sortComplete = false;
	private SortThread sortThread;
	private volatile BufferedLinkedBlockingQueue readBuffer;
	private volatile DiskBackedArray result;
	private volatile int[] sortPos;
	private final int NUM_RT_THREADS = 1; // 6 * ResourceManager.cpus;
	private final MetaData meta;
	private volatile boolean isClosed = false;
	private volatile boolean done = false;
	private int node;
	private int childCard = 16;
	private boolean cardSet = false;
	private Plan plan;
	
	public void setPlan(Plan plan)
	{
		this.plan = plan;
	}

	public SortOperator(ArrayList<String> sortCols, ArrayList<Boolean> orders, MetaData meta)
	{
		this.sortCols = sortCols;
		this.orders = orders;
		this.meta = meta;
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
			for (final String sortCol : sortCols)
			{
				sortPos[i] = cols2Pos.get(sortCol);
				i++;
			}
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
		final SortOperator retval = new SortOperator(sortCols, orders, meta);
		retval.node = node;
		retval.childCard = childCard;
		retval.cardSet = cardSet;
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

		result.close();
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
		if (!sortComplete)
		{
			sortThread.join();
			// System.out.println("Sort done!");
			sortComplete = true;
		}

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

			sortComplete = false;
			readBuffer.clear();
			sortThread = new SortThread(child);
			sortThread.start();
		}
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

	@Override
	public void setNode(int node)
	{
		this.node = node;
	}

	public long size()
	{
		return result.size();
	}

	@Override
	public synchronized void start() throws Exception
	{
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
				if (result == null)
				{
					result = ResourceManager.newDiskBackedArray(childCard);
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
					ResourceManager.NO_OFFLOAD.getAndIncrement();
					ResourceManager.waitForSync();
					if (result.size() < PARALLEL_SORT_MIN_NUM_ROWS)
					{
						doSequentialSort(0, result.size() - 1);
					}
					else
					{
						final ParallelSortThread t = doParallelSort(0, result.size() - 1);
						t.join();
					}
					ResourceManager.NO_OFFLOAD.getAndDecrement();
				}

				done = true;

				ct = new CopyThread(result);
				ct.start();
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				try
				{
					readBuffer.put(e);
				}
				catch(Exception f)
				{}
				return;
			}
		}

		private int compare(ArrayList<Object> lhs, ArrayList<Object> rhs) throws Exception
		{
			int result;
			int i = 0;

			for (final int pos : sortPos)
			{
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

		// @?Parallel (recursive)
		private ParallelSortThread doParallelSort(long left, long right)
		{
			// System.out.println("Starting parallel sort with " +
			// (right-left+1) + " rows");
			final ParallelSortThread t = new ParallelSortThread(left, right);
			t.start();
			return t;
		}

		private final void doSequentialSort(long left, long right) throws Exception
		{
			if (right <= left)
			{
				return;
			}
			long lt = left;
			long gt = right;
			final ArrayList<Object> v = (ArrayList<Object>)result.get(left);
			long i = left;
			while (i <= gt)
			{
				final ArrayList<Object> temp = (ArrayList<Object>)result.get(i);
				final int cmp = compare(temp, v);
				if (cmp < 0)
				{
					result.update(i, (ArrayList<Object>)result.get(lt));
					result.update(lt, temp);
					i++;
					lt++;
				}
				else if (cmp > 0)
				{
					result.update(i, (ArrayList<Object>)result.get(gt));
					result.update(gt, temp);
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

		private final class CopyThread extends ThreadPoolThread
		{
			private volatile DiskBackedArray array;

			public CopyThread(DiskBackedArray array)
			{
				this.array = array;
			}

			@Override
			public void run()
			{
				try
				{
					final long size = array.size();
					long i = 0;
					System.currentTimeMillis();
					// long nullCount = 0; //DEBUG
					while (i < size)
					{
						ArrayList<Object> row = null;
						while (row == null)
						{
							try
							{
								row = (ArrayList<Object>)array.get(i);
							}
							catch (final Exception e)
							{
								HRDBMSWorker.logger.error("Array is " + array, e);
								HRDBMSWorker.logger.error("isClosed = " + isClosed);
								readBuffer.put(e);
								return;
							}
						}

						readBuffer.put(row);

						i++;

						System.currentTimeMillis();
					}

					// System.out.println(nullCount + "/" + size +
					// " records were null after sort.");

					readBuffer.put(new DataEndMarker());
					// array.close();
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.error("", e);
					try
					{
						readBuffer.put(e);
					}
					catch(Exception f)
					{}
					return;
				}
			}
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
					if (((right - left) < PARALLEL_SORT_MIN_NUM_ROWS))
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
						ArrayList<Object> temp = (ArrayList<Object>)result.get(pivotIndex);
						result.update(pivotIndex, (ArrayList<Object>)result.get(left));
						result.update(left, temp);
						long lt = left;
						long gt = right;
						final ArrayList<Object> v = temp;
						long i = left;
						while (i <= gt)
						{
							temp = (ArrayList<Object>)result.get(i);
							final int cmp = compare(temp, v);
							if (cmp < 0)
							{
								result.update(i, (ArrayList<Object>)result.get(lt));
								result.update(lt, temp);
								i++;
								lt++;
							}
							else if (cmp > 0)
							{
								result.update(i, (ArrayList<Object>)result.get(gt));
								result.update(gt, temp);
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
						readBuffer.put(e);
					}
					catch(Exception f)
					{}
					return;
				}
			}
		}

		private final class ReaderThread extends ThreadPoolThread
		{
			private volatile DiskBackedArray array;

			public ReaderThread(DiskBackedArray array)
			{
				this.array = array;
			}

			@Override
			public void run()
			{
				try
				{
					Object o = child.next(SortOperator.this);
					while (!(o instanceof DataEndMarker))
					{
						array.add((ArrayList<Object>)o);
						o = child.next(SortOperator.this);
					}
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.error("", e);
					try
					{
						readBuffer.put(e);
					}
					catch(Exception f)
					{}
					return;
				}
			}
		}
	}
}
