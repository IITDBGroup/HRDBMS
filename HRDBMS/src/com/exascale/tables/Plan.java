package com.exascale.tables;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.misc.DataEndMarker;
import com.exascale.optimizer.CNFFilter;
import com.exascale.optimizer.Operator;
import com.exascale.optimizer.TableScanOperator;
import com.exascale.threads.HRDBMSThread;

public class Plan implements Serializable
{
	private transient final long time;
	private transient final boolean reserved;
	private final List<Operator> trees;
	private transient final ConcurrentHashMap<Integer, Integer> touchedNodes = new ConcurrentHashMap<Integer, Integer>();
	private int updateCount2 = 0;

	// private Map<Integer, Integer> old2New = null;

	public Plan(final boolean reserved, final List<Operator> trees)
	{
		time = System.currentTimeMillis();
		this.reserved = reserved;
		this.trees = trees;
	}

	public Plan(final Plan p)
	{
		this.time = p.time;
		this.reserved = p.reserved;
		this.trees = cloneArray(p.trees);
		// old2New = new HashMap<Integer, Integer>();
		// for (Operator tree : trees)
		// {
		// updateIDs(tree);
		// }
		//
		// old2New = null;
	}

	public void addNode(final int node)
	{
		touchedNodes.put(node, node);
	}

	public List<Operator> cloneArray(final List<Operator> source)
	{
		final List<Operator> retval = new ArrayList<Operator>(source.size());
		for (final Operator tree : source)
		{
			retval.add(clone(tree));
		}

		return retval;
	}

	@Override
	public synchronized boolean equals(final Object rhs)
	{
		if (rhs == null)
		{
			return false;
		}

		if (!(rhs instanceof Plan))
		{
			return false;
		}

		final Plan r = (Plan)rhs;
		return trees.equals(r.trees) && touchedNodes.equals(r.touchedNodes);
	}

	/*
	 * private void updateIDs(Operator op) { if (op instanceof
	 * NetworkHashAndSendOperator) { NetworkHashAndSendOperator s =
	 * (NetworkHashAndSendOperator)op; int old = s.getID(); Integer replace =
	 * old2New.get(old); if (replace != null) { s.setID(replace); } else {
	 * replace = Phase4.id.getAndIncrement(); s.setID(replace); old2New.put(old,
	 * replace); } } else if (op instanceof NetworkHashReceiveAndMergeOperator)
	 * { NetworkHashReceiveAndMergeOperator s =
	 * (NetworkHashReceiveAndMergeOperator)op; int old = s.getID(); Integer
	 * replace = old2New.get(old); if (replace != null) { s.setID(replace); }
	 * else { replace = Phase4.id.getAndIncrement(); s.setID(replace);
	 * old2New.put(old, replace); } } else if (op instanceof
	 * NetworkHashReceiveOperator) { NetworkHashReceiveOperator s =
	 * (NetworkHashReceiveOperator)op; int old = s.getID(); Integer replace =
	 * old2New.get(old); if (replace != null) { s.setID(replace); } else {
	 * replace = Phase4.id.getAndIncrement(); s.setID(replace); old2New.put(old,
	 * replace); } } else if (op instanceof NetworkSendMultipleOperator) {
	 * NetworkSendMultipleOperator s = (NetworkSendMultipleOperator)op; int old
	 * = s.getID(); Integer replace = old2New.get(old); if (replace != null) {
	 * s.setID(replace); } else { replace = Phase4.id.getAndIncrement();
	 * s.setID(replace); old2New.put(old, replace); } } else if (op instanceof
	 * NetworkSendRROperator) { NetworkSendRROperator s =
	 * (NetworkSendRROperator)op; int old = s.getID(); Integer replace =
	 * old2New.get(old); if (replace != null) { s.setID(replace); } else {
	 * replace = Phase4.id.getAndIncrement(); s.setID(replace); old2New.put(old,
	 * replace); } }
	 *
	 * for (Operator o : op.children()) { updateIDs(o); } }
	 */

	public Operator execute() throws Exception
	{
		int i = 0;
		final int size = trees.size();
		while (i < size - 1)
		{
			trees.get(i).start();
			while (!(trees.get(i).next(trees.get(i)) instanceof DataEndMarker))
			{
			}
			trees.get(i).close();
			i++;
		}

		final Operator retval = trees.get(size - 1);
		retval.start();
		return retval;
	}

	public List<SubXAThread> executeMultiNoResult() throws Exception
	{
		int i = 0;
		final int size = trees.size();
		final List<SubXAThread> threads = new ArrayList<SubXAThread>(trees.size() - 1);
		while (i < size - 1)
		{
			final SubXAThread thread = new SubXAThread(trees.get(i));
			thread.start();
			threads.add(thread);
			i++;
		}

		final Operator retval = trees.get(size - 1);
		retval.start();
		updateCount2 = (Integer)retval.next(retval);
		retval.close();
		return threads;
	}

	public int executeNoResult() throws Exception
	{
		int i = 0;
		final int size = trees.size();
		while (i < size - 1)
		{
			trees.get(i).start();
			while (!(trees.get(i).next(trees.get(i)) instanceof DataEndMarker))
			{
			}
			trees.get(i).close();
			i++;
		}

		final Operator retval = trees.get(size - 1);
		retval.start();
		Object o = retval.next(retval);
		int updateCount = 0;
		if (o instanceof Integer)
		{
			updateCount = (Integer)o;
		}

		retval.close();
		return updateCount;
	}

	public long getTimeStamp()
	{
		return time;
	}

	public synchronized ArrayList<Integer> getTouchedNodes()
	{
		return new ArrayList<Integer>(touchedNodes.keySet());
	}

	public List<Operator> getTrees()
	{
		return trees;
	}

	public int getUpdateCount()
	{
		return updateCount2;
	}

	@Override
	public synchronized int hashCode()
	{
		int hash = 17;
		hash = hash * 23 + trees.hashCode();
		hash = hash * 23 + touchedNodes.hashCode();
		return hash;
	}

	public boolean isReserved()
	{
		return reserved;
	}

	public void setSample(final long sPer)
	{
		for (final Operator op : trees)
		{
			setSample(op, sPer);
		}
	}

	private Operator clone(final Operator op)
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

	private void setSample(final Operator op, final long sPer)
	{
		if (op instanceof TableScanOperator)
		{
			((TableScanOperator)op).setSample(sPer);
		}
		else
		{
			for (final Operator o : op.children())
			{
				setSample(o, sPer);
			}
		}
	}

	public static class SubXAThread extends HRDBMSThread
	{
		private final Operator tree;
		private boolean ok = true;
		private Exception e;
		int updateCount = 0;

		public SubXAThread(final Operator tree)
		{
			this.tree = tree;
		}

		public Exception getException()
		{
			return e;
		}

		public boolean getOK()
		{
			return ok;
		}

		public int getUpdateCount()
		{
			return updateCount;
		}

		@Override
		public void run()
		{
			try
			{
				tree.start();
				while (true)
				{
					final Object o = tree.next(tree);
					if (o instanceof DataEndMarker)
					{
						break;
					}

					if (o instanceof Integer)
					{
						updateCount = (Integer)o;
					}
				}
				tree.close();
			}
			catch (final Exception e)
			{
				ok = false;
				this.e = e;
			}
		}
	}
}
