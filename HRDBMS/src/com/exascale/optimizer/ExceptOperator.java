package com.exascale.optimizer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeMap;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.managers.ResourceManager;
import com.exascale.managers.ResourceManager.DiskBackedArray;
import com.exascale.managers.ResourceManager.DiskBackedHashSet;
import com.exascale.misc.BufferedLinkedBlockingQueue;
import com.exascale.misc.DataEndMarker;
import com.exascale.tables.Plan;
import com.exascale.threads.ThreadPoolThread;

public final class ExceptOperator implements Operator, Serializable
{
	private MetaData meta;

	private final ArrayList<Operator> children = new ArrayList<Operator>();

	private Operator parent;
	private HashMap<String, String> cols2Types;
	private HashMap<String, Integer> cols2Pos;
	private TreeMap<Integer, String> pos2Col;
	private int node;
	private ArrayList<DiskBackedHashSet> sets = new ArrayList<DiskBackedHashSet>();
	private BufferedLinkedBlockingQueue buffer;
	private int estimate = 16;
	private volatile boolean inited = false;
	private volatile boolean startDone = false;
	private Plan plan;
	private int childPos = -1;
	private boolean estimateSet = false;
	
	public void setPlan(Plan plan)
	{
		this.plan = plan;
	}

	public ExceptOperator(MetaData meta)
	{
		this.meta = meta;
	}

	@Override
	public void add(Operator op) throws Exception
	{
		if (children.size() >= 2)
		{
			throw new Exception("ExceptOperator only supports 2 children!");
		}
		
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
		cols2Types = op.getCols2Types();
		cols2Pos = op.getCols2Pos();
		pos2Col = op.getPos2Col();
	}

	@Override
	public ArrayList<Operator> children()
	{
		return children;
	}

	@Override
	public ExceptOperator clone()
	{
		final ExceptOperator retval = new ExceptOperator(meta);
		retval.node = node;
		retval.estimate = estimate;
		return retval;
	}

	@Override
	public void close() throws Exception
	{
		try
		{
			for (final DiskBackedHashSet set : sets)
			{
				set.getArray().close();
				set.close();
			}
		}
		catch(Exception e)
		{}
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
		return new ArrayList<String>();
	}

	@Override
	public Object next(Operator op2) throws Exception
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
		
		if (o instanceof Exception)
		{
			throw (Exception)o;
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
			throw new Exception("ExceptOperator only supports 1 parent.");
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
				buffer.put(e);
				return;
			}
		}
		else
		{
			inited = false;
			for (final Operator op : children)
			{
				op.reset();
			}

			for (final DiskBackedHashSet set : sets)
			{
				try
				{
					set.getArray().close();
					set.close();
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.error("", e);
				}
			}

			sets = new ArrayList<DiskBackedHashSet>();
			buffer.clear();
			if (!inited)
			{
			}
			else
			{
				Exception e = new Exception("ExceptOperator is inited more than once!");
				HRDBMSWorker.logger.error("ExceptOperator is inited more than once!");
				buffer.put(e);
				return;
			}
			new InitThread().start();
		}
	}

	@Override
	public void setChildPos(int pos)
	{
		childPos = pos;
	}

	public boolean setEstimate(int estimate)
	{
		if (estimateSet)
		{
			return false;
		}
		this.estimate = estimate;
		estimateSet = true;
		return true;
	}

	@Override
	public void setNode(int node)
	{
		this.node = node;
	}

	@Override
	public void start() throws Exception
	{
		if (children.size() < 2)
		{
			throw new Exception("ExceptOperator requires 2 children but only has " + children.size());
		}
		
		startDone = true;
		for (final Operator op : children)
		{
			op.start();
		}

		buffer = new BufferedLinkedBlockingQueue(ResourceManager.QUEUE_SIZE);
		if (!inited)
		{
		}
		else
		{
			Exception e = new Exception("ExceptOperator is inited more than once!");
			HRDBMSWorker.logger.error("ExceptOperator is inited more than once!", e);
			buffer.put(e);
			return;
		}
		new InitThread().start();
	}

	@Override
	public String toString()
	{
		return "ExceptOperator";
	}

	private final class InitThread extends ThreadPoolThread
	{
		private final ArrayList<ReadThread> threads = new ArrayList<ReadThread>(children.size());

		@Override
		public void run()
		{
			if (!inited)
			{
				inited = true;
			}
			else
			{
				Exception e = new Exception("ExceptOperator is inited more than once!");
				HRDBMSWorker.logger.error("ExceptOperator is inited more than once!", e);
				try
				{
					buffer.put(e);
				}
				catch(Exception f)
				{}
				return;
			}
			
			int i = 0;
			while (i < children.size())
			{
				final ReadThread read = new ReadThread(i);
				threads.add(read);
				sets.add(ResourceManager.newDiskBackedHashSet(true, estimate));
				read.start();
				i++;
			}

			for (final ReadThread read : threads)
			{
				while (true)
				{
					try
					{
						read.join();
						break;
					}
					catch (final InterruptedException e)
					{
					}
				}
			}

			int count = 0;
			for (Object row : sets.get(0).getArray())
			{
				try
				{
					if (!sets.get(1).contains(row))
					{
						buffer.put(row);
						count++;
					}
				}
				catch(Exception e)
				{
					try
					{
						buffer.put(e);
					}
					catch(Exception f)
					{}
					return;
				}
			}

			HRDBMSWorker.logger.debug("Except operator returned " + count + " rows");

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
		private final int i;

		public ReadThread(int i)
		{
			this.i = i;
			op = children.get(i);
		}

		@Override
		public void run()
		{
			try
			{
				final DiskBackedHashSet set = sets.get(i);
				Object o = op.next(ExceptOperator.this);
				while (!(o instanceof DataEndMarker))
				{
					set.add((ArrayList<Object>)o);
					o = op.next(ExceptOperator.this);
				}
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				try
				{
					buffer.put(e);
				}
				catch(Exception f)
				{}
				return;
			}
		}
	}
}
