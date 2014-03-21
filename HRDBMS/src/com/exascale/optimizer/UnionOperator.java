package com.exascale.optimizer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.managers.ResourceManager;
import com.exascale.managers.ResourceManager.DiskBackedHashSet;
import com.exascale.misc.BufferedLinkedBlockingQueue;
import com.exascale.misc.DataEndMarker;
import com.exascale.tables.Plan;
import com.exascale.threads.ThreadPoolThread;

public final class UnionOperator implements Operator, Serializable
{
	private final ArrayList<Operator> children = new ArrayList<Operator>();

	private final MetaData meta;

	private HashMap<String, String> cols2Types;
	private HashMap<String, Integer> cols2Pos;
	private TreeMap<Integer, String> pos2Col;
	private Operator parent;
	private int node;
	private boolean distinct;
	private BufferedLinkedBlockingQueue buffer;
	private DiskBackedHashSet set;
	private final AtomicLong counter = new AtomicLong(0);
	private int estimate = 16;
	private volatile boolean inited = false;
	private ArrayList<ReadThread> threads;
	private boolean startDone = false;
	private Plan plan;
	
	public void setPlan(Plan plan)
	{
		this.plan = plan;
	}

	public UnionOperator(boolean distinct, MetaData meta)
	{
		this.meta = meta;
		this.distinct = distinct;
	}

	@Override
	public void add(Operator op) throws Exception
	{
		children.add(op);
		op.registerParent(this);
		cols2Pos = op.getCols2Pos();
		cols2Types = op.getCols2Types();
		pos2Col = op.getPos2Col();
	}

	@Override
	public ArrayList<Operator> children()
	{
		return children;
	}

	@Override
	public UnionOperator clone()
	{
		final UnionOperator retval = new UnionOperator(distinct, meta);
		retval.node = node;
		retval.estimate = estimate;
		return retval;
	}

	@Override
	public void close() throws Exception
	{
		for (final ThreadPoolThread thread : threads)
		{
			thread.kill();
		}

		for (final Operator child : children)
		{
			child.close();
		}
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

	@Override
	public TreeMap<Integer, String> getPos2Col()
	{
		return pos2Col;
	}

	@Override
	public ArrayList<String> getReferences()
	{
		final ArrayList<String> retval = new ArrayList<String>(0);
		return retval;
	}

	@Override
	public Object next(Operator op) throws Exception
	{
		Object o;
		o = buffer.take();

		if (o instanceof DataEndMarker)
		{
			o = buffer.peek();
			if (o == null)
			{
				buffer.put(new DataEndMarker());
				return new DataEndMarker();
			}
			else
			{
				buffer.put(new DataEndMarker());
				return o;
			}
		}
		return o;
	}

	@Override
	public void nextAll(Operator op) throws Exception
	{
		for (final Operator o : children)
		{
			o.nextAll(op);
		}

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
			throw new Exception("UnionOperator only supports 1 parent.");
		}
	}

	@Override
	public void removeChild(Operator op)
	{
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
		if (!startDone)
		{
			try
			{
				start();
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				System.exit(1);
			}
		}
		else
		{
			for (final Operator child : children)
			{
				child.reset();
			}

			buffer.clear();
			inited = false;
			if (!inited)
			{
			}
			else
			{
				Exception e = new Exception();
				HRDBMSWorker.logger.error("UnionOperator has been inited more than once!", e);
				System.exit(1);
			}
			new InitThread().start();
		}
	}

	@Override
	public void setChildPos(int pos)
	{
	}

	public void setDistinct(boolean distinct)
	{
		this.distinct = distinct;
	}

	public void setEstimate(int estimate)
	{
		this.estimate = estimate;
	}

	@Override
	public void setNode(int node)
	{
		this.node = node;
	}

	@Override
	public void start() throws Exception
	{
		startDone = true;
		for (final Operator child : children)
		{
			child.start();
		}

		buffer = new BufferedLinkedBlockingQueue(ResourceManager.QUEUE_SIZE);
		if (!inited)
		{
		}
		else
		{
			Exception e = new Exception();
			HRDBMSWorker.logger.error("UnionOperator has been inited more than once!", e);
			System.exit(1);
		}
		new InitThread().start();
	}

	@Override
	public String toString()
	{
		return "UnionOperator";
	}

	private final class InitThread extends ThreadPoolThread
	{
		@Override
		public void run()
		{
			if (!inited)
			{
				inited = true;
			}
			else
			{
				Exception e = new Exception();
				HRDBMSWorker.logger.error("UnionOperator has been inited more than once!", e);
				System.exit(1);
			}

			threads = new ArrayList<ReadThread>(children.size());

			if (distinct && children.size() > 1)
			{
				set = ResourceManager.newDiskBackedHashSet(true, estimate);
			}

			// System.out.println("Union operator has " + children.size() +
			// " children");

			for (final Operator op : children)
			{
				final ReadThread thread = new ReadThread(op);
				threads.add(thread);
				thread.start();
			}

			for (final ReadThread thread : threads)
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

			if (distinct && children.size() > 1)
			{
				for (final Object o : set.getArray())
				{
					while (true)
					{
						try
						{
							buffer.put(o);
							break;
						}
						catch (final Exception e)
						{
						}
					}
				}

				// System.out.println("Union operator returned " +
				// set.getArray().size() + " rows");

				try
				{
					set.getArray().close();
					set.close();
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.error("", e);
					System.exit(1);
				}
			}
			else
			{
				// System.out.println("Union operator returned " + counter.get()
				// + " rows");
			}

			while (true)
			{
				try
				{
					buffer.put(new DataEndMarker());
					break;
				}
				catch (final Exception e)
				{
				}
			}
		}
	}

	private final class ReadThread extends ThreadPoolThread
	{
		private final Operator op;

		public ReadThread(Operator op)
		{
			this.op = op;
		}

		@Override
		public void run()
		{
			try
			{
				Object o = null;
				o = op.next(UnionOperator.this);
				while (!(o instanceof DataEndMarker))
				{
					if (distinct && children.size() > 1)
					{
						set.add((ArrayList<Object>)o);
					}
					else
					{
						while (true)
						{
							try
							{
								buffer.put(o);
								counter.getAndIncrement();
								break;
							}
							catch (final Exception e)
							{
							}
						}
					}

					o = op.next(UnionOperator.this);
				}
			}
			catch (final Exception f)
			{
				HRDBMSWorker.logger.error("", f);
				System.exit(1);
			}
		}
	}

}
