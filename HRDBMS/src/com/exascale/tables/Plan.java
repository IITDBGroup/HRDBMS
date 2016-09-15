package com.exascale.tables;

import java.io.Serializable;
import java.util.ArrayList;
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
	private final ArrayList<Operator> trees;
	private transient final ConcurrentHashMap<Integer, Integer> touchedNodes = new ConcurrentHashMap<Integer, Integer>();
	private int updateCount2 = 0;

	// private HashMap<Integer, Integer> old2New = null;

	public Plan(boolean reserved, ArrayList<Operator> trees)
	{
		time = System.currentTimeMillis();
		this.reserved = reserved;
		this.trees = trees;
	}

	public Plan(Plan p)
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
	
	public int getUpdateCount()
	{
		return updateCount2;
	}

	public void addNode(int node)
	{
		touchedNodes.put(node, node);
	}

	public ArrayList<Operator> cloneArray(ArrayList<Operator> source)
	{
		ArrayList<Operator> retval = new ArrayList<Operator>(source.size());
		for (Operator tree : source)
		{
			retval.add(clone(tree));
		}

		return retval;
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

	@Override
	public synchronized boolean equals(Object rhs)
	{
		if (rhs == null)
		{
			return false;
		}

		if (!(rhs instanceof Plan))
		{
			return false;
		}

		Plan r = (Plan)rhs;
		return trees.equals(r.trees) && touchedNodes.equals(r.touchedNodes);
	}

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

		Operator retval = trees.get(size - 1);
		retval.start();
		return retval;
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

		Operator retval = trees.get(size - 1);
		retval.start();
		int updateCount = (Integer)retval.next(retval);
		retval.close();
		return updateCount;
	}
	
	public ArrayList<SubXAThread> executeMultiNoResult() throws Exception
	{
		int i = 0;
		final int size = trees.size();
		ArrayList<SubXAThread> threads = new ArrayList<SubXAThread>(trees.size() - 1);
		while (i < size - 1)
		{
			SubXAThread thread = new SubXAThread(trees.get(i));
			thread.start();
			threads.add(thread);
			i++;
		}

		Operator retval = trees.get(size - 1);
		retval.start();
		updateCount2 = (Integer)retval.next(retval);
		retval.close();
		return threads;
	}
	
	public static class SubXAThread extends HRDBMSThread
	{
		private Operator tree;
		private boolean ok = true;
		private Exception e;
		int updateCount = 0;
		
		public SubXAThread(Operator tree)
		{
			this.tree = tree;
		}
		
		public void run()
		{
			try
			{
				tree.start();
				while (true)
				{
					Object o = tree.next(tree);
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
			catch(Exception e)
			{
				ok = false;
				this.e = e;
			}
		}
		
		public int getUpdateCount()
		{
			return updateCount;
		}
		
		public boolean getOK()
		{
			return ok;
		}
		
		public Exception getException()
		{
			return e;
		}
	}

	public long getTimeStamp()
	{
		return time;
	}

	public synchronized ArrayList<Integer> getTouchedNodes()
	{
		return new ArrayList<Integer>(touchedNodes.keySet());
	}

	public ArrayList<Operator> getTrees()
	{
		return trees;
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

	public void setSample(long sPer)
	{
		for (Operator op : trees)
		{
			setSample(op, sPer);
		}
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

	private void setSample(Operator op, long sPer)
	{
		if (op instanceof TableScanOperator)
		{
			((TableScanOperator)op).setSample(sPer);
		}
		else
		{
			for (Operator o : op.children())
			{
				setSample(o, sPer);
			}
		}
	}
}
