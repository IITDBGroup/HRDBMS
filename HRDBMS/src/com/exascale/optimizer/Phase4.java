package com.exascale.optimizer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.managers.ResourceManager;
import com.exascale.tables.Transaction;

public final class Phase4
{
	private static final int MAX_LOCAL_NO_HASH_PRODUCT = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("max_local_no_hash_product")); // 1000000
	// private static final int MAX_LOCAL_LEFT_HASH =
	// (int)(ResourceManager.QUEUE_SIZE *
	// Double.parseDouble(HRDBMSWorker.getHParms().getProperty("hash_external_factor")));
	private static final int MAX_LOCAL_LEFT_HASH = 2;
	private static final int MAX_LOCAL_SORT = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("max_local_sort")); // 1000000
	private static final int MAX_RR = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("max_rr"));
	public static AtomicInteger id = new AtomicInteger(0);
	private final RootOperator root;
	private final MetaData meta;
	private final int MAX_INCOMING_CONNECTIONS = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("max_neighbor_nodes")); // 100
	private final Transaction tx;
	private int colSuffix = 0;

	private final HashMap<Operator, Long> cCache = new HashMap<Operator, Long>();

	private boolean lt = true;

	public Phase4(final RootOperator root, final Transaction tx)
	{
		this.root = root;
		this.tx = tx;
		meta = root.getMeta();
	}

	public static void clearOpParents(final Operator op, final HashSet<Operator> touched)
	{
		if (op instanceof NetworkSendOperator)
		{
			if (touched.contains(op))
			{
				return;
			}

			touched.add(op);
		}

		if (op instanceof TableScanOperator)
		{
			((TableScanOperator)op).clearOpParents();
		}
		else
		{
			for (final Operator o : op.children())
			{
				clearOpParents(o, touched);
			}
		}
	}

	private static double concatPath(Operator op)
	{
		double retval = 0;
		int shift = 0;
		while (!(op instanceof RootOperator))
		{
			long i = 0;
			for (final Operator o : op.parent().children())
			{
				if (o == op)
				{
					retval += i * Math.pow(2.0, shift);
					shift += 20;
					break;
				}

				i++;
			}

			op = op.parent();
		}

		return retval;
	}

	private static int getStartingNode(final long numNodes) throws Exception
	{
		if (numNodes >= MetaData.numWorkerNodes)
		{
			return 0;
		}

		final int range = (int)(MetaData.numWorkerNodes - numNodes);
		if (range < 0)
		{
			return 0;
		}
		return (int)(Math.random() * range);
	}

	private static boolean handleTop(final NetworkReceiveOperator receive) throws Exception
	{
		if (receive.children().size() == 1)
		{
			pushAcross(receive);
			return true;
		}
		final TopOperator parent = (TopOperator)receive.parent();
		final ArrayList<Operator> children = (ArrayList<Operator>)receive.children().clone();
		final HashMap<Operator, Operator> send2Child = new HashMap<Operator, Operator>();
		final HashMap<Operator, CNFFilter> send2CNF = new HashMap<Operator, CNFFilter>();
		for (final Operator child : children)
		{
			send2Child.put(child, child.children().get(0));
			if (child.children().get(0) instanceof TableScanOperator)
			{
				final CNFFilter cnf = ((TableScanOperator)child.children().get(0)).getCNFForParent(child);
				if (cnf != null)
				{
					send2CNF.put(child, cnf);
				}
			}
			child.removeChild(child.children().get(0));
		}

		for (final Map.Entry entry : send2Child.entrySet())
		{
			final Operator pClone = parent.clone();
			try
			{
				pClone.add((Operator)entry.getValue());
				pClone.setNode(((Operator)entry.getValue()).getNode());
				if (send2CNF.containsKey(entry.getKey()))
				{
					((TableScanOperator)entry.getValue()).setCNFForParent(pClone, send2CNF.get(entry.getKey()));
				}
				((Operator)entry.getKey()).add(pClone);
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				throw e;
			}
		}

		// cCache.clear();
		return false;
	}

	private static void pushAcross(final NetworkReceiveOperator receive) throws Exception
	{
		final Operator parent = receive.parent();
		final Operator grandParent = parent.parent();
		final ArrayList<Operator> children = receive.children();
		final HashMap<Operator, Operator> send2Child = new HashMap<Operator, Operator>();
		final HashMap<Operator, CNFFilter> send2CNF = new HashMap<Operator, CNFFilter>();
		for (final Operator child : children)
		{
			send2Child.put(child, child.children().get(0));
			if (child.children().get(0) instanceof TableScanOperator)
			{
				final CNFFilter cnf = ((TableScanOperator)child.children().get(0)).getCNFForParent(child);
				if (cnf != null)
				{
					send2CNF.put(child, cnf);
				}
			}
			child.removeChild(child.children().get(0));
		}
		parent.removeChild(receive);
		grandParent.removeChild(parent);

		try
		{
			for (final Map.Entry entry : send2Child.entrySet())
			{
				final Operator pClone = parent.clone();
				pClone.add((Operator)entry.getValue());
				pClone.setNode(((Operator)entry.getValue()).getNode());
				if (send2CNF.containsKey(entry.getKey()))
				{
					((TableScanOperator)entry.getValue()).setCNFForParent(pClone, send2CNF.get(entry.getKey()));
				}
				((Operator)entry.getKey()).add(pClone);
				receive.removeChild((Operator)entry.getKey());
				receive.add((Operator)entry.getKey());
			}
			grandParent.add(receive);
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.error("", e);
			throw e;
		}
		// cCache.clear();
	}

	public long card(final Operator op) throws Exception
	{
		final Long r = cCache.get(op);
		if (r != null)
		{
			return r;
		}

		return notCached(op);
	}

	public void optimize() throws Exception
	{
		allUnionsHave2Children(root, new HashSet<Operator>());
		allIntersectionsHave2Children(root, new HashSet<Operator>());
		pushUpReceives();
		redistributeSorts();
		makeHierarchicalForAll(root, new HashSet<Operator>());
		removeLocalSendReceive(root, new HashSet<Operator>());
		removeDuplicateReorders(root, new HashSet<Operator>());
		// HRDBMSWorker.logger.debug("Before removing hashes");
		// Phase1.printTree(root, 0);
		removeUnneededHash();
		// HRDBMSWorker.logger.debug("After removing unneeded hashing");
		// Phase1.printTree(root, 0);
		clearOpParents(root, new HashSet<Operator>());
		cleanupOrderedFilters(root, new HashSet<Operator>());
		// HRDBMSWorker.logger.debug("Exiting P4:");
		// Phase1.printTree(root, 0);
		// sanityCheck(root, -1);
		swapLeftRight(root, new HashSet<Operator>());
	}

	private void allIntersectionsHave2Children(Operator op, final HashSet<Operator> touched) throws Exception
	{
		if (op instanceof NetworkSendOperator)
		{
			if (touched.contains(op))
			{
				return;
			}

			touched.add(op);
		}

		if (!(op instanceof IntersectOperator))
		{
			for (final Operator o : op.children())
			{
				allIntersectionsHave2Children(o, touched);
			}
		}
		else
		{
			if (op.children().size() <= 2)
			{
				for (final Operator o : op.children())
				{
					allIntersectionsHave2Children(o, touched);
				}
			}
			else
			{
				final ArrayList<Operator> remainder = new ArrayList<Operator>();
				int i = 0;
				for (final Operator o : op.children())
				{
					if (i < 2)
					{
					}
					else
					{
						op.removeChild(o);
						remainder.add(o);
					}

					i++;
				}

				final Operator parent = op.parent();
				parent.removeChild(op);
				final Operator orig = op;
				while (remainder.size() != 0)
				{
					final Operator newOp = orig.clone();
					newOp.add(op);
					newOp.add(remainder.remove(0));
					op = newOp;
				}

				parent.add(op);

				for (final Operator o : orig.children())
				{
					allIntersectionsHave2Children(o, touched);
				}
			}
		}
	}

	private void allUnionsHave2Children(Operator op, final HashSet<Operator> touched) throws Exception
	{
		if (op instanceof NetworkSendOperator)
		{
			if (touched.contains(op))
			{
				return;
			}

			touched.add(op);
		}

		if (!(op instanceof UnionOperator))
		{
			for (final Operator o : op.children())
			{
				allUnionsHave2Children(o, touched);
			}
		}
		else
		{
			if (op.children().size() <= 2)
			{
				for (final Operator o : op.children())
				{
					allUnionsHave2Children(o, touched);
				}
			}
			else
			{
				final ArrayList<Operator> remainder = new ArrayList<Operator>();
				int i = 0;
				for (final Operator o : op.children())
				{
					if (i < 2)
					{
					}
					else
					{
						op.removeChild(o);
						remainder.add(o);
					}

					i++;
				}

				final Operator parent = op.parent();
				parent.removeChild(op);
				final Operator orig = op;
				while (remainder.size() != 0)
				{
					final Operator newOp = orig.clone();
					newOp.add(op);
					newOp.add(remainder.remove(0));
					op = newOp;
				}

				parent.add(op);

				for (final Operator o : orig.children())
				{
					allUnionsHave2Children(o, touched);
				}
			}
		}
	}

	private long cardHJO(final Operator op) throws Exception
	{
		final HashSet<HashMap<Filter, Filter>> hshm = ((HashJoinOperator)op).getHSHM();
		double max = -1;
		for (final HashMap<Filter, Filter> hm : hshm)
		{
			final double temp = meta.likelihood(new ArrayList<Filter>(hm.keySet()), tx, op);
			if (temp > max)
			{
				max = temp;
			}
		}
		long retval = (long)(card(op.children().get(0)) * max * card(op.children().get(1)));
		if (retval < card(op.children().get(0)))
		{
			retval = card(op.children().get(0));
		}

		if (retval < card(op.children().get(1)))
		{
			retval = card(op.children().get(1));
		}
		if (retval == 0)
		{
			retval = 1;
		}
		cCache.put(op, retval);
		return retval;
	}

	private long cardMO(final Operator op) throws Exception
	{
		final long groupCard = meta.getColgroupCard(((MultiOperator)op).getKeys(), root, tx, op);
		final long card = card(op.children().get(0));
		if (groupCard > card)
		{
			cCache.put(op, card);
			return card;
		}

		final long retval = groupCard;
		cCache.put(op, retval);
		return retval;
	}

	private long cardNL(final Operator op) throws Exception
	{
		final HashSet<HashMap<Filter, Filter>> hshm = ((NestedLoopJoinOperator)op).getHSHM();
		double max = -1;
		for (final HashMap<Filter, Filter> hm : hshm)
		{
			final double temp = meta.likelihood(new ArrayList<Filter>(hm.keySet()), tx, op);
			if (temp > max)
			{
				max = temp;
			}
		}
		long retval = (long)(card(op.children().get(0)) * max * card(op.children().get(1)));
		if (retval < card(op.children().get(0)))
		{
			retval = card(op.children().get(0));
		}

		if (retval < card(op.children().get(1)))
		{
			retval = card(op.children().get(1));
		}
		if (retval == 0)
		{
			retval = 1;
		}
		cCache.put(op, retval);
		return retval;
	}

	private long cardNorm(final Operator op) throws Exception
	{
		long retval = card(op.children().get(0));
		if (retval == 0)
		{
			retval = 1;
		}
		cCache.put(op, retval);
		return retval;
	}

	private long cardRX(final Operator op) throws Exception
	{
		long retval = 0;
		for (final Operator o : op.children())
		{
			retval += card(o);
		}

		if (retval == 0)
		{
			retval = 1;
		}
		cCache.put(op, retval);
		return retval;
	}

	private long cardSelect(final Operator op) throws Exception
	{
		long retval = (long)(((SelectOperator)op).likelihood(root, tx) * card(op.children().get(0)));
		if (retval == 0)
		{
			retval = 1;
		}
		cCache.put(op, retval);
		return retval;
	}

	private long cardSetI(final Operator op) throws Exception
	{
		long lCard = card(op.children().get(0));
		long rCard = card(op.children().get(1));

		if (lCard <= rCard)
		{
			if (lCard == 0)
			{
				lCard = 1;
			}
			cCache.put(op, lCard);
			return lCard;
		}
		else
		{
			if (rCard == 0)
			{
				rCard = 1;
			}
			cCache.put(op, rCard);
			return rCard;
		}
	}

	private long cardTop(final Operator op) throws Exception
	{
		long retval = ((TopOperator)op).getRemaining();
		long retval2 = card(op.children().get(0));

		if (retval2 < retval)
		{
			if (retval2 == 0)
			{
				retval2 = 1;
			}
			cCache.put(op, retval2);
			return retval2;
		}
		else
		{
			if (retval == 0)
			{
				retval = 1;
			}
			cCache.put(op, retval);
			return retval;
		}
	}

	private long cardTSO(final Operator op) throws Exception
	{
		final HashSet<HashMap<Filter, Filter>> hshm = ((TableScanOperator)op).getHSHM();
		if (hshm != null)
		{
			long retval = (long)(MetaData.getTableCard(((TableScanOperator)op).getSchema(), ((TableScanOperator)op).getTable(), tx) * meta.likelihood(hshm, root, tx, op) * (1.0 / ((TableScanOperator)op).getNumNodes()));
			if (retval == 0)
			{
				retval = 1;
			}
			cCache.put(op, retval);
			return retval;
		}

		long retval = (long)((1.0 / ((TableScanOperator)op).getNumNodes()) * MetaData.getTableCard(((TableScanOperator)op).getSchema(), ((TableScanOperator)op).getTable(), tx));
		if (retval == 0)
		{
			retval = 1;
		}
		cCache.put(op, retval);
		return retval;
	}

	private long cardTX(final Operator op) throws Exception
	{
		long retval = card(op.children().get(0)) / ((NetworkHashAndSendOperator)op).parents().size();
		if (retval == 0)
		{
			retval = 1;
		}
		cCache.put(op, retval);
		return retval;
	}

	private long cardTXRR(final Operator op) throws Exception
	{
		long retval = card(op.children().get(0)) / ((NetworkSendRROperator)op).parents().size();
		if (retval == 0)
		{
			retval = 1;
		}
		cCache.put(op, retval);
		return retval;
	}

	private long cardUnion(final Operator op) throws Exception
	{
		long retval = 0;
		for (final Operator o : op.children())
		{
			retval += card(o);
		}

		if (retval == 0)
		{
			retval = 1;
		}
		cCache.put(op, retval);
		return retval;
	}

	private long cardX(final Operator op) throws Exception
	{
		long retval = card(op.children().get(0)) * card(op.children().get(1));
		if (retval == 0)
		{
			retval = 1;
		}
		if (retval < 0)
		{
			retval = Long.MAX_VALUE;
		}
		cCache.put(op, retval);
		return retval;
	}

	private void cleanupOrderedFilters(final Operator op, final HashSet<Operator> touched)
	{
		if (op instanceof NetworkSendOperator)
		{
			if (touched.contains(op))
			{
				return;
			}

			touched.add(op);
		}

		if (op instanceof TableScanOperator)
		{
			((TableScanOperator)op).cleanupOrderedFilters();
		}
		else
		{
			for (final Operator o : op.children())
			{
				cleanupOrderedFilters(o, touched);
			}
		}
	}

	private Operator cloneTree(final Operator op, final int level) throws Exception
	{
		final Operator clone = op.clone();
		if (level == 0)
		{
			for (final Operator o : op.children())
			{
				try
				{
					final Operator child = cloneTree(o, level + 1);
					clone.add(child);
					clone.setChildPos(op.getChildPos());
					if (o instanceof TableScanOperator)
					{
						final CNFFilter cnf = ((TableScanOperator)o).getCNFForParent(op);
						if (cnf != null)
						{
							((TableScanOperator)child).setCNFForParent(clone, cnf);
						}
					}
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.error("", e);
					throw e;
				}
			}
		}

		return clone;
	}

	private boolean doHashAnti(final NetworkReceiveOperator receive) throws Exception
	{
		final Operator parent = receive.parent();
		final Operator grandParent = parent.parent();
		// card(parent.children().get(0));
		// card(parent.children().get(1));
		ArrayList<String> join = ((AntiJoinOperator)parent).getJoinForChild(receive);
		if (join == null)
		{
			return false;
		}
		Operator other = null;
		for (final Operator o : parent.children())
		{
			if (o != receive)
			{
				other = o;
			}
		}
		join = ((AntiJoinOperator)parent).getJoinForChild(other);
		if (join == null)
		{
			return false;
		}
		join = ((AntiJoinOperator)parent).getJoinForChild(receive);
		verify2ReceivesForHash(parent);
		for (final Operator o : parent.children())
		{
			if (o != receive)
			{
				other = o;
			}
		}

		parent.removeChild(receive);
		grandParent.removeChild(parent);
		ArrayList<Operator> sends = new ArrayList<Operator>(receive.children().size());
		final int starting = getStartingNode(MetaData.numWorkerNodes);
		int ID = id.getAndIncrement();

		CNFFilter cnf = null;
		for (final Operator child : (ArrayList<Operator>)receive.children().clone())
		{
			final int node = child.getNode();
			final Operator grandChild = child.children().get(0);

			if (grandChild instanceof TableScanOperator)
			{
				cnf = ((TableScanOperator)grandChild).getCNFForParent(child);
			}
			child.removeChild(grandChild);

			final NetworkHashAndSendOperator send = new NetworkHashAndSendOperator(join, MetaData.numWorkerNodes, ID, starting, meta);
			try
			{
				send.add(grandChild);
				if (cnf != null)
				{
					((TableScanOperator)grandChild).setCNFForParent(send, cnf);
				}
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				throw e;
			}
			send.setNode(node);
			sends.add(send);
		}

		int i = starting;
		ArrayList<Operator> receives = new ArrayList<Operator>();
		while (i < MetaData.numWorkerNodes)
		{
			final NetworkHashReceiveOperator receive2 = new NetworkHashReceiveOperator(ID, meta);
			receive2.setNode(i);
			receives.add(receive2);
			i++;
		}

		for (final Operator receive2 : receives)
		{
			for (final Operator send : sends)
			{
				try
				{
					receive2.add(send);
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.error("", e);
					throw e;
				}
			}
		}

		final ArrayList<Operator> parents = new ArrayList<Operator>(receives.size());
		for (final Operator receive2 : receives)
		{
			final Operator clone = cloneTree(parent, 0);
			try
			{
				clone.add(receive2);
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				throw e;
			}
			clone.setNode(receive2.getNode());
			parents.add(clone);
			for (final Operator o : (ArrayList<Operator>)clone.children().clone())
			{
				if (!o.equals(receive2))
				{
					clone.removeChild(o);
				}
			}
		}

		Operator otherChild = null;
		for (final Operator child : parent.children())
		{
			if (!child.equals(receive))
			{
				otherChild = child;
			}
		}

		join = ((AntiJoinOperator)parent).getJoinForChild(otherChild);
		sends = new ArrayList<Operator>(otherChild.children().size());
		ID = id.getAndIncrement();
		for (final Operator child : (ArrayList<Operator>)otherChild.children().clone())
		{
			final int node = child.getNode();
			final Operator grandChild = child.children().get(0);
			cnf = null;
			if (grandChild instanceof TableScanOperator)
			{
				cnf = ((TableScanOperator)grandChild).getCNFForParent(child);
			}
			child.removeChild(grandChild);

			final NetworkHashAndSendOperator send = new NetworkHashAndSendOperator(join, MetaData.numWorkerNodes, ID, starting, meta);
			try
			{
				send.add(grandChild);
				if (cnf != null)
				{
					((TableScanOperator)grandChild).setCNFForParent(send, cnf);
				}
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				throw e;
			}
			send.setNode(node);
			sends.add(send);
		}

		i = starting;
		receives = new ArrayList<Operator>();
		while (i < MetaData.numWorkerNodes)
		{
			final NetworkHashReceiveOperator receive2 = new NetworkHashReceiveOperator(ID, meta);
			receive2.setNode(i);
			receives.add(receive2);
			i++;
		}

		for (final Operator receive2 : receives)
		{
			for (final Operator send : sends)
			{
				try
				{
					receive2.add(send);
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.error("", e);
					throw e;
				}
			}
		}

		final ArrayList<NetworkSendOperator> sends2 = new ArrayList<NetworkSendOperator>(parents.size());
		for (final Operator clone : parents)
		{
			for (final Operator receive2 : receives)
			{
				if (clone.getNode() == receive2.getNode())
				{
					try
					{
						clone.add(receive2);
					}
					catch (final Exception e)
					{
						HRDBMSWorker.logger.error("", e);
						throw e;
					}
				}
			}

			final NetworkSendOperator send = new NetworkSendOperator(clone.getNode(), meta);
			sends2.add(send);
			try
			{
				send.add(clone);
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				throw e;
			}
		}

		final NetworkReceiveOperator r = new NetworkReceiveOperator(meta);
		r.setNode(grandParent.getNode());
		for (final NetworkSendOperator send : sends2)
		{
			try
			{
				r.add(send);
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				throw e;
			}
		}
		try
		{
			grandParent.add(r);
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.error("", e);
			throw e;
		}
		// makeHierarchical2(r);
		// makeHierarchical(r);
		// cCache.clear();
		return false;
	}

	private boolean doHashSemi(final NetworkReceiveOperator receive) throws Exception
	{
		final Operator parent = receive.parent();
		final Operator grandParent = parent.parent();
		// card(parent.children().get(0));
		// card(parent.children().get(1));
		ArrayList<String> join = ((SemiJoinOperator)parent).getJoinForChild(receive);
		if (join == null)
		{
			return false;
		}
		Operator other = null;
		for (final Operator o : parent.children())
		{
			if (o != receive)
			{
				other = o;
			}
		}
		join = ((SemiJoinOperator)parent).getJoinForChild(other);
		if (join == null)
		{
			return false;
		}
		join = ((SemiJoinOperator)parent).getJoinForChild(receive);
		verify2ReceivesForHash(parent);
		for (final Operator o : parent.children())
		{
			if (o != receive)
			{
				other = o;
			}
		}

		parent.removeChild(receive);
		grandParent.removeChild(parent);
		ArrayList<Operator> sends = new ArrayList<Operator>(receive.children().size());
		final int starting = getStartingNode(MetaData.numWorkerNodes);
		int ID = id.getAndIncrement();

		CNFFilter cnf = null;
		for (final Operator child : (ArrayList<Operator>)receive.children().clone())
		{
			final int node = child.getNode();
			final Operator grandChild = child.children().get(0);

			if (grandChild instanceof TableScanOperator)
			{
				cnf = ((TableScanOperator)grandChild).getCNFForParent(child);
			}
			child.removeChild(grandChild);

			final NetworkHashAndSendOperator send = new NetworkHashAndSendOperator(join, MetaData.numWorkerNodes, ID, starting, meta);
			try
			{
				send.add(grandChild);
				if (cnf != null)
				{
					((TableScanOperator)grandChild).setCNFForParent(send, cnf);
				}
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				throw e;
			}
			send.setNode(node);
			sends.add(send);
		}

		int i = starting;
		ArrayList<Operator> receives = new ArrayList<Operator>();
		while (i < MetaData.numWorkerNodes)
		{
			final NetworkHashReceiveOperator receive2 = new NetworkHashReceiveOperator(ID, meta);
			receive2.setNode(i);
			receives.add(receive2);
			i++;
		}

		for (final Operator receive2 : receives)
		{
			for (final Operator send : sends)
			{
				try
				{
					receive2.add(send);
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.error("", e);
					throw e;
				}
			}
		}

		final ArrayList<Operator> parents = new ArrayList<Operator>(receives.size());
		for (final Operator receive2 : receives)
		{
			final Operator clone = cloneTree(parent, 0);
			try
			{
				clone.add(receive2);
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				throw e;
			}
			clone.setNode(receive2.getNode());
			parents.add(clone);
			for (final Operator o : (ArrayList<Operator>)clone.children().clone())
			{
				if (!o.equals(receive2))
				{
					clone.removeChild(o);
				}
			}
		}

		Operator otherChild = null;
		for (final Operator child : parent.children())
		{
			if (!child.equals(receive))
			{
				otherChild = child;
			}
		}

		join = ((SemiJoinOperator)parent).getJoinForChild(otherChild);
		sends = new ArrayList<Operator>(otherChild.children().size());
		ID = id.getAndIncrement();
		for (final Operator child : (ArrayList<Operator>)otherChild.children().clone())
		{
			final int node = child.getNode();
			final Operator grandChild = child.children().get(0);
			cnf = null;
			if (grandChild instanceof TableScanOperator)
			{
				cnf = ((TableScanOperator)grandChild).getCNFForParent(child);
			}
			child.removeChild(grandChild);

			final NetworkHashAndSendOperator send = new NetworkHashAndSendOperator(join, MetaData.numWorkerNodes, ID, starting, meta);
			try
			{
				send.add(grandChild);
				if (cnf != null)
				{
					((TableScanOperator)grandChild).setCNFForParent(send, cnf);
				}
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				throw e;
			}
			send.setNode(node);
			sends.add(send);
		}

		i = starting;
		receives = new ArrayList<Operator>();
		while (i < MetaData.numWorkerNodes)
		{
			final NetworkHashReceiveOperator receive2 = new NetworkHashReceiveOperator(ID, meta);
			receive2.setNode(i);
			receives.add(receive2);
			i++;
		}

		for (final Operator receive2 : receives)
		{
			for (final Operator send : sends)
			{
				try
				{
					receive2.add(send);
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.error("", e);
					throw e;
				}
			}
		}

		final ArrayList<NetworkSendOperator> sends2 = new ArrayList<NetworkSendOperator>(parents.size());
		for (final Operator clone : parents)
		{
			for (final Operator receive2 : receives)
			{
				if (clone.getNode() == receive2.getNode())
				{
					try
					{
						clone.add(receive2);
					}
					catch (final Exception e)
					{
						HRDBMSWorker.logger.error("", e);
						throw e;
					}
				}
			}

			final NetworkSendOperator send = new NetworkSendOperator(clone.getNode(), meta);
			sends2.add(send);
			try
			{
				send.add(clone);
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				throw e;
			}
		}

		final NetworkReceiveOperator r = new NetworkReceiveOperator(meta);
		r.setNode(grandParent.getNode());
		for (final NetworkSendOperator send : sends2)
		{
			try
			{
				r.add(send);
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				throw e;
			}
		}
		try
		{
			grandParent.add(r);
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.error("", e);
			throw e;
		}
		// makeHierarchical2(r);
		// makeHierarchical(r);
		// cCache.clear();
		return false;
	}

	private boolean doNonHashSemi(final NetworkReceiveOperator receive) throws Exception
	{
		final Operator parent = receive.parent();
		Operator right = parent.children().get(0);
		Operator left = parent.children().get(1);

		if (isAllAny(left, new HashSet<Operator>()) && left.children().size() == 1)
		{
			verify2ReceivesForSemi(parent);
			right = parent.children().get(0);
			left = parent.children().get(1);
			final Operator grandParent = parent.parent();
			parent.removeChild(left);
			grandParent.removeChild(parent);
			final ArrayList<Operator> grandChildren = new ArrayList<Operator>();
			for (final Operator child : (ArrayList<Operator>)right.children().clone())
			{
				final Operator grandChild = child.children().get(0);
				child.removeChild(grandChild);
				if (!(grandChild instanceof NetworkReceiveOperator))
				{
					grandChildren.add(grandChild);
				}
				else
				{
					grandChildren.addAll(getGrandChildren(grandChild));
				}
			}

			for (final Operator o : grandChildren)
			{
				if (o.parent() != null)
				{
					o.parent().removeChild(o);
				}
			}

			final ArrayList<NetworkSendOperator> sends2 = new ArrayList<NetworkSendOperator>(grandChildren.size());
			for (final Operator grandChild : grandChildren)
			{
				final Operator clone = cloneTree(parent, 0);
				clone.setNode(grandChild.getNode());
				final Operator leftClone = fullyCloneTree(left.children().get(0).children().get(0));
				setNodeForTree(leftClone, grandChild.getNode(), new HashSet<Operator>());
				try
				{
					clone.add(leftClone);
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.error("", e);
					throw e;
				}

				for (final Operator o : (ArrayList<Operator>)clone.children().clone())
				{
					if (!o.equals(leftClone))
					{
						clone.removeChild(o);
					}
				}

				try
				{
					clone.add(grandChild);
					if (grandChild instanceof TableScanOperator)
					{
						final CNFFilter cnf = ((TableScanOperator)grandChild).getFirstCNF();
						if (cnf != null)
						{
							((TableScanOperator)grandChild).setCNFForParent(clone, cnf);
						}
					}
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.error("", e);
					throw e;
				}

				final NetworkSendOperator send2 = new NetworkSendOperator(clone.getNode(), meta);
				try
				{
					send2.add(clone);
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.error("", e);
					throw e;
				}

				sends2.add(send2);
			}

			final NetworkReceiveOperator r = new NetworkReceiveOperator(meta);
			r.setNode(grandParent.getNode());

			for (final NetworkSendOperator send : sends2)
			{
				try
				{
					r.add(send);
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.error("", e);
					throw e;
				}
			}

			try
			{
				grandParent.add(r);
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				throw e;
			}

			// makeHierarchical(r);
			// cCache.clear();
			return false;
		}

		return false;
	}

	private void doSortRedistribution(final SortOperator op, final long card) throws Exception
	{
		long numNodes = card / MAX_LOCAL_SORT;
		numNodes++;
		if (numNodes > MAX_RR)
		{
			numNodes = MAX_RR;
		}
		final int starting = getStartingNode(numNodes);
		final Operator parent = op.parent();
		parent.removeChild(op);
		final Operator child = op.children().get(0);
		op.removeChild(child);
		final int ID = id.getAndIncrement();
		final NetworkSendRROperator rr = new NetworkSendRROperator(ID, meta);
		try
		{
			rr.add(child);
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.error("", e);
			throw e;
		}
		rr.setNode(child.getNode());

		final ArrayList<NetworkSendOperator> sends = new ArrayList<NetworkSendOperator>();
		int i = 0;
		while (i < numNodes && starting + i < MetaData.numWorkerNodes)
		{
			try
			{
				final NetworkHashReceiveOperator receive = new NetworkHashReceiveOperator(ID, meta);
				receive.setNode(starting + i);
				receive.add(rr);
				final SortOperator sort2 = op.clone();
				sort2.add(receive);
				sort2.setNode(starting + i);
				final NetworkSendOperator send = new NetworkSendOperator(starting + i, meta);
				send.add(sort2);
				sends.add(send);
				i++;
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				throw e;
			}
		}

		final NetworkReceiveAndMergeOperator receive = new NetworkReceiveAndMergeOperator((ArrayList<String>)op.getKeys().clone(), (ArrayList<Boolean>)op.getOrders().clone(), meta);
		receive.setNode(parent.getNode());
		for (final NetworkSendOperator send : sends)
		{
			receive.add(send);
		}

		try
		{
			parent.add(receive);
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.error("", e);
			throw e;
		}

		if (parent instanceof TopOperator)
		{
			handleTop(receive);
		}
		makeHierarchical(receive);
		// cCache.clear();
	}

	private Operator fullyCloneTree(final Operator op) throws Exception
	{
		final Operator clone = op.clone();

		for (final Operator o : op.children())
		{
			try
			{
				final Operator child = fullyCloneTree(o);
				clone.add(child);
				clone.setChildPos(op.getChildPos());
				if (o instanceof TableScanOperator)
				{
					final CNFFilter cnf = ((TableScanOperator)o).getCNFForParent(op);
					if (cnf != null)
					{
						((TableScanOperator)child).setCNFForParent(clone, cnf);
					}
				}
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				throw e;
			}
		}

		return clone;
	}

	private ArrayList<Operator> getGrandChildren(final Operator op)
	{
		final ArrayList<Operator> retval = new ArrayList<Operator>();
		for (final Operator o : (ArrayList<Operator>)op.children().clone())
		{
			final Operator grandChild = o.children().get(0);
			if (!(grandChild instanceof NetworkReceiveOperator))
			{
				retval.add(grandChild);
			}
			else
			{
				retval.addAll(getGrandChildren(grandChild));
			}
		}

		return retval;
	}

	private SortOperator getLocalSort(final Operator op) throws Exception
	{
		SortOperator retval = null;
		if (op instanceof SortOperator)
		{
			if (op.getNode() == -1)
			{
				retval = (SortOperator)op;
			}
		}

		if (op.getNode() == -1)
		{
			for (final Operator o : op.children())
			{
				final SortOperator s = getLocalSort(o);
				if (s != null)
				{
					if (retval == null)
					{
						retval = s;
					}
					else
					{
						final Exception e = new Exception("Found more than 1 sort on the coord node!");
						HRDBMSWorker.logger.error("Found more than 1 sort on the coord node!", e);
						throw e;
					}
				}
			}
		}

		return retval;
	}

	private void getReceives(final Operator op, final int level, final HashMap<NetworkReceiveOperator, Integer> result)
	{
		if (!(op instanceof NetworkReceiveOperator))
		{
			if (op.getNode() == -1)
			{
				for (final Operator child : op.children())
				{
					getReceives(child, level + 1, result);
				}
			}

			return;
		}
		else
		{
			if (op.getNode() == -1)
			{
				result.put((NetworkReceiveOperator)op, level);
				for (final Operator child : op.children())
				{
					getReceives(child, level + 1, result);
				}
			}

			return;
		}
	}

	private ArrayList<TableScanOperator> getTables(final Operator op, final HashSet<Operator> touched)
	{
		if (touched.contains(op))
		{
			return new ArrayList<TableScanOperator>();
		}

		if (op instanceof TableScanOperator)
		{
			final ArrayList<TableScanOperator> retval = new ArrayList<TableScanOperator>();
			retval.add((TableScanOperator)op);
			return retval;
		}

		touched.add(op);

		if (op.children().size() == 1)
		{
			return getTables(op.children().get(0), touched);
		}

		final ArrayList<TableScanOperator> retval = new ArrayList<TableScanOperator>();
		for (final Operator o : op.children())
		{
			retval.addAll(getTables(o, touched));
		}

		return retval;
	}

	private boolean handleExcept(final NetworkReceiveOperator receive) throws Exception
	{
		final Operator parent = receive.parent();
		verify2ReceivesForHash(parent);
		final Operator grandParent = parent.parent();
		// card(parent.children().get(0));
		// card(parent.children().get(1));
		parent.removeChild(receive);
		grandParent.removeChild(parent);
		ArrayList<Operator> sends = new ArrayList<Operator>(receive.children().size());
		final int starting = getStartingNode(MetaData.numWorkerNodes);
		int ID = id.getAndIncrement();
		for (final Operator o : parent.children())
		{
			if (o != receive)
			{
			}
		}

		CNFFilter cnf = null;
		for (final Operator child : (ArrayList<Operator>)receive.children().clone())
		{
			final int node = child.getNode();
			final Operator grandChild = child.children().get(0);

			if (grandChild instanceof TableScanOperator)
			{
				cnf = ((TableScanOperator)grandChild).getCNFForParent(child);
			}
			child.removeChild(grandChild);

			final NetworkHashAndSendOperator send = new NetworkHashAndSendOperator(new ArrayList<String>(grandChild.getPos2Col().values()), MetaData.numWorkerNodes, ID, starting, meta);
			try
			{
				send.add(grandChild);
				if (cnf != null)
				{
					((TableScanOperator)grandChild).setCNFForParent(send, cnf);
				}
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				throw e;
			}
			send.setNode(node);
			sends.add(send);
		}

		int i = starting;
		ArrayList<Operator> receives = new ArrayList<Operator>();
		while (i < MetaData.numWorkerNodes)
		{
			final NetworkHashReceiveOperator receive2 = new NetworkHashReceiveOperator(ID, meta);
			receive2.setNode(i);
			receives.add(receive2);
			i++;
		}

		for (final Operator receive2 : receives)
		{
			for (final Operator send : sends)
			{
				try
				{
					receive2.add(send);
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.error("", e);
					throw e;
				}
			}
		}

		final ArrayList<Operator> parents = new ArrayList<Operator>(receives.size());
		for (final Operator receive2 : receives)
		{
			final Operator clone = cloneTree(parent, 0);
			try
			{
				clone.add(receive2);
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				throw e;
			}
			clone.setNode(receive2.getNode());
			parents.add(clone);
			for (final Operator o : (ArrayList<Operator>)clone.children().clone())
			{
				if (!o.equals(receive2))
				{
					clone.removeChild(o);
				}
			}
		}

		Operator otherChild = null;
		for (final Operator child : parent.children())
		{
			if (!child.equals(receive))
			{
				otherChild = child;
			}
		}

		sends = new ArrayList<Operator>(otherChild.children().size());
		ID = id.getAndIncrement();
		for (final Operator child : (ArrayList<Operator>)otherChild.children().clone())
		{
			final int node = child.getNode();
			final Operator grandChild = child.children().get(0);
			cnf = null;
			if (grandChild instanceof TableScanOperator)
			{
				cnf = ((TableScanOperator)grandChild).getCNFForParent(child);
			}
			child.removeChild(grandChild);

			final NetworkHashAndSendOperator send = new NetworkHashAndSendOperator(new ArrayList<String>(grandChild.getPos2Col().values()), MetaData.numWorkerNodes, ID, starting, meta);
			try
			{
				send.add(grandChild);
				if (cnf != null)
				{
					((TableScanOperator)grandChild).setCNFForParent(send, cnf);
				}
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				throw e;
			}
			send.setNode(node);
			sends.add(send);
		}

		i = starting;
		receives = new ArrayList<Operator>();
		while (i < MetaData.numWorkerNodes)
		{
			final NetworkHashReceiveOperator receive2 = new NetworkHashReceiveOperator(ID, meta);
			receive2.setNode(i);
			receives.add(receive2);
			i++;
		}

		for (final Operator receive2 : receives)
		{
			for (final Operator send : sends)
			{
				try
				{
					receive2.add(send);
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.error("", e);
					throw e;
				}
			}
		}

		final ArrayList<NetworkSendOperator> sends2 = new ArrayList<NetworkSendOperator>(parents.size());
		for (final Operator clone : parents)
		{
			for (final Operator receive2 : receives)
			{
				if (clone.getNode() == receive2.getNode())
				{
					try
					{
						clone.add(receive2);
					}
					catch (final Exception e)
					{
						HRDBMSWorker.logger.error("", e);
						throw e;
					}
				}
			}

			final NetworkSendOperator send = new NetworkSendOperator(clone.getNode(), meta);
			sends2.add(send);
			try
			{
				send.add(clone);
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				throw e;
			}
		}

		final NetworkReceiveOperator r = new NetworkReceiveOperator(meta);
		r.setNode(grandParent.getNode());
		for (final NetworkSendOperator send : sends2)
		{
			try
			{
				r.add(send);
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				throw e;
			}
		}
		try
		{
			grandParent.add(r);
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.error("", e);
			throw e;
		}
		// makeHierarchical2(r);
		// makeHierarchical(r);
		// cCache.clear();
		return false;
	}

	private boolean handleMulti(final NetworkReceiveOperator receive) throws Exception
	{
		if (receive.children().size() == 1)
		{
			pushAcross(receive);
			return true;
		}

		if (receive.children().get(0).children().get(0) instanceof RenameOperator && receive.children().get(0).children().get(0).children().get(0) instanceof MultiOperator)
		{
			return false;
		}

		final MultiOperator parent = (MultiOperator)receive.parent();

		final long pCard = card(parent);
		if ((!noLargeUpstreamJoins(parent) || upstreamRedistSort(parent) || card(receive) > MAX_LOCAL_SORT || pCard > (long)(ResourceManager.QUEUE_SIZE * Double.parseDouble(HRDBMSWorker.getHParms().getProperty("sort_gb_factor")))) && parent.getKeys().size() > 0)
		{
			final ArrayList<String> cols2 = new ArrayList<String>(parent.getKeys());
			final int starting = getStartingNode(MetaData.numWorkerNodes);
			final int ID = Phase4.id.getAndIncrement();
			final ArrayList<NetworkHashAndSendOperator> sends = new ArrayList<NetworkHashAndSendOperator>(receive.children().size());
			for (Operator o : (ArrayList<Operator>)receive.children().clone())
			{
				final Operator temp = o.children().get(0);
				o.removeChild(temp);
				receive.removeChild(o);
				o = temp;
				CNFFilter cnf = null;
				if (o instanceof TableScanOperator)
				{
					cnf = ((TableScanOperator)o).getCNFForParent(receive);
				}

				final NetworkHashAndSendOperator send = new NetworkHashAndSendOperator(cols2, MetaData.numWorkerNodes, ID, starting, meta);
				try
				{
					send.add(o);
					if (cnf != null)
					{
						((TableScanOperator)o).setCNFForParent(send, cnf);
					}
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.error("", e);
					throw e;
				}
				send.setNode(o.getNode());
				sends.add(send);
			}

			int i = 0;
			final ArrayList<NetworkHashReceiveOperator> receives = new ArrayList<NetworkHashReceiveOperator>();
			while (i < MetaData.numWorkerNodes)
			{
				final NetworkHashReceiveOperator hrec = new NetworkHashReceiveOperator(ID, meta);
				hrec.setNode(i + starting);
				for (final NetworkHashAndSendOperator send : sends)
				{
					try
					{
						hrec.add(send);
					}
					catch (final Exception e)
					{
						HRDBMSWorker.logger.error("", e);
						throw e;
					}
				}
				receives.add(hrec);
				i++;
			}

			final Operator grandParent = parent.parent();
			grandParent.removeChild(parent);
			parent.removeChild(receive);
			for (final NetworkHashReceiveOperator hrec : receives)
			{
				final MultiOperator clone = parent.clone();
				clone.setNode(hrec.getNode());
				try
				{
					clone.add(hrec);
					final NetworkSendOperator send = new NetworkSendOperator(hrec.getNode(), meta);
					send.add(clone);
					receive.add(send);
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.error("", e);
					throw e;
				}
			}

			try
			{
				grandParent.add(receive);
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				throw e;
			}
			// cCache.clear();
			// makeHierarchical(receive);
			return true;
		}

		if (parent.existsCountDistinct())
		{
			if (parent.getKeys().size() == 0 && parent.getOutputCols().size() == 1)
			{
				final int starting = getStartingNode(MetaData.numWorkerNodes);
				final int ID = Phase4.id.getAndIncrement();
				final ArrayList<NetworkHashAndSendOperator> sends = new ArrayList<NetworkHashAndSendOperator>(receive.children().size());
				for (Operator o : (ArrayList<Operator>)receive.children().clone())
				{
					final Operator temp = o.children().get(0);
					o.removeChild(temp);
					receive.removeChild(o);
					o = temp;
					CNFFilter cnf = null;
					if (o instanceof TableScanOperator)
					{
						cnf = ((TableScanOperator)o).getCNFForParent(receive);
					}

					final String input = parent.getInputCols().get(0);
					final ArrayList<String> cols2 = new ArrayList<String>();
					cols2.add(input);
					final NetworkHashAndSendOperator send = new NetworkHashAndSendOperator(cols2, MetaData.numWorkerNodes, ID, starting, meta);
					try
					{
						send.add(o);
						if (cnf != null)
						{
							((TableScanOperator)o).setCNFForParent(send, cnf);
						}
					}
					catch (final Exception e)
					{
						HRDBMSWorker.logger.error("", e);
						throw e;
					}
					send.setNode(o.getNode());
					sends.add(send);
				}

				int i = 0;
				final ArrayList<NetworkHashReceiveOperator> receives = new ArrayList<NetworkHashReceiveOperator>();
				while (i < MetaData.numWorkerNodes)
				{
					final NetworkHashReceiveOperator hrec = new NetworkHashReceiveOperator(ID, meta);
					hrec.setNode(i + starting);
					for (final NetworkHashAndSendOperator send : sends)
					{
						try
						{
							hrec.add(send);
						}
						catch (final Exception e)
						{
							HRDBMSWorker.logger.error("", e);
							throw e;
						}
					}
					receives.add(hrec);
					i++;
				}

				final Operator grandParent = parent.parent();
				grandParent.removeChild(parent);
				parent.removeChild(receive);
				for (final NetworkHashReceiveOperator hrec : receives)
				{
					final MultiOperator clone = parent.clone();
					clone.setNode(hrec.getNode());
					try
					{
						clone.add(hrec);
						final NetworkSendOperator send = new NetworkSendOperator(hrec.getNode(), meta);
						send.add(clone);
						receive.add(send);
					}
					catch (final Exception e)
					{
						HRDBMSWorker.logger.error("", e);
						throw e;
					}
				}

				try
				{
					final ArrayList<String> old = new ArrayList<String>();
					final ArrayList<String> newCols = new ArrayList<String>();
					old.add(receive.getPos2Col().get(0));
					newCols.add("_Q" + colSuffix++);
					final RenameOperator rename = new RenameOperator(old, newCols, meta);
					rename.add(receive);
					parent.changeCD2Add();
					final ArrayList<String> newInputs = new ArrayList<String>();
					newInputs.add("_Q" + (colSuffix - 1));
					parent.updateInputColumns(parent.getInputCols(), newInputs);
					parent.add(rename);
					grandParent.add(parent);
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.error("", e);
					throw e;
				}
				// makeHierarchical2(receive);
				// makeHierarchical(receive);
			}
			return false;
		}

		final ArrayList<Operator> children = receive.children();
		final HashMap<Operator, Operator> send2Child = new HashMap<Operator, Operator>();
		final HashMap<Operator, CNFFilter> send2CNF = new HashMap<Operator, CNFFilter>();
		for (final Operator child : children)
		{
			send2Child.put(child, child.children().get(0));
			if (child.children().get(0) instanceof TableScanOperator)
			{
				final CNFFilter cnf = ((TableScanOperator)child.children().get(0)).getCNFForParent(child);
				if (cnf != null)
				{
					send2CNF.put(child, cnf);
				}
			}
			child.removeChild(child.children().get(0));
		}

		final Operator orig = parent.parent();
		MultiOperator pClone;
		final ArrayList<String> cols = new ArrayList<String>(parent.getPos2Col().values());
		ArrayList<String> oldCols = null;
		ArrayList<String> newCols = null;

		for (final Map.Entry entry : send2Child.entrySet())
		{
			pClone = parent.clone();
			if (pClone.getOutputCols().size() == 0)
			{
				pClone.addCount("_Q" + colSuffix++);
			}
			while (pClone.hasAvg())
			{
				final String avgCol = pClone.getAvgCol();
				final ArrayList<String> newCols2 = new ArrayList<String>(2);
				final String newCol1 = "_Q" + colSuffix++;
				final String newCol2 = "_Q" + colSuffix++;
				newCols2.add(newCol1);
				newCols2.add(newCol2);
				final HashMap<String, ArrayList<String>> old2News = new HashMap<String, ArrayList<String>>();
				old2News.put(avgCol, newCols2);
				pClone.replaceAvgWithSumAndCount(old2News);
				parent.replaceAvgWithSumAndCount(old2News);
				final Operator grandParent = parent.parent();
				grandParent.removeChild(parent);
				final ExtendOperator extend = new ExtendOperator("/," + old2News.get(avgCol).get(0) + "," + old2News.get(avgCol).get(1), avgCol, meta);
				try
				{
					extend.add(parent);
					extend.setNode(parent.getNode());
					grandParent.add(extend);
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.error("", e);
					throw e;
				}
			}

			// colSuffix = oldSuffix;
			oldCols = new ArrayList(pClone.getOutputCols());
			newCols = new ArrayList(pClone.getInputCols());
			final HashMap<String, String> old2New2 = new HashMap<String, String>();
			int counter = 10;
			int i = 0;
			for (final String col : oldCols)
			{
				if (!old2New2.containsValue(newCols.get(i)))
				{
					old2New2.put(col, newCols.get(i));
				}
				else
				{
					String new2 = newCols.get(i) + counter++;
					while (old2New2.containsValue(new2))
					{
						new2 = newCols.get(i) + counter++;
					}

					old2New2.put(col, new2);
				}

				i++;
			}
			newCols = new ArrayList<String>(oldCols.size());
			for (final String col : oldCols)
			{
				newCols.add(old2New2.get(col));
			}

			try
			{
				pClone.add((Operator)entry.getValue());
				pClone.setNode(((Operator)entry.getValue()).getNode());
				if (send2CNF.containsKey(entry.getKey()))
				{
					((TableScanOperator)entry.getValue()).setCNFForParent(pClone, send2CNF.get(entry.getKey()));
				}
				RenameOperator rename = null;
				rename = new RenameOperator(oldCols, newCols, meta);
				rename.add(pClone);
				rename.setNode(pClone.getNode());

				((Operator)entry.getKey()).add(rename);
				receive.removeChild((Operator)entry.getKey());
				receive.add((Operator)entry.getKey());
				parent.removeChild(receive);
				parent.add(receive);
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				throw e;
			}
		}

		parent.changeCountsToSums();
		parent.updateInputColumns(oldCols, newCols);
		try
		{
			final Operator child = parent.children().get(0);
			parent.removeChild(child);
			parent.add(child);
			Operator grandParent = parent.parent();
			grandParent.removeChild(parent);
			grandParent.add(parent);
			while (!grandParent.equals(orig))
			{
				final Operator next = grandParent.parent();
				next.removeChild(grandParent);
				if (next.equals(orig))
				{
					final ReorderOperator order = new ReorderOperator(cols, meta);
					order.add(grandParent);
					order.setNode(grandParent.getNode());
					orig.add(order);
					grandParent = next;
				}
				else
				{
					next.add(grandParent);
					grandParent = next;
				}
			}
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.error("", e);
			throw e;
		}
		// cCache.clear();
		return false;
	}

	private boolean handleSort(NetworkReceiveOperator receive) throws Exception
	{
		if (receive.children().size() == 1)
		{
			pushAcross(receive);
			return true;
		}
		final SortOperator parent = (SortOperator)receive.parent();
		final Operator grandParent = parent.parent();
		final ArrayList<Operator> children = (ArrayList<Operator>)receive.children().clone();
		final HashMap<Operator, Operator> send2Child = new HashMap<Operator, Operator>();
		final HashMap<Operator, CNFFilter> send2CNF = new HashMap<Operator, CNFFilter>();
		for (final Operator child : children)
		{
			send2Child.put(child, child.children().get(0));
			if (child.children().get(0) instanceof TableScanOperator)
			{
				final CNFFilter cnf = ((TableScanOperator)child.children().get(0)).getCNFForParent(child);
				if (cnf != null)
				{
					send2CNF.put(child, cnf);
				}
			}
			child.removeChild(child.children().get(0));
			receive.removeChild(child);
		}
		parent.removeChild(receive);
		grandParent.removeChild(parent);
		receive = new NetworkReceiveAndMergeOperator((ArrayList<String>)parent.getKeys().clone(), (ArrayList<Boolean>)parent.getOrders().clone(), meta);

		try
		{
			for (final Map.Entry entry : send2Child.entrySet())
			{
				final Operator pClone = parent.clone();
				pClone.add((Operator)entry.getValue());
				pClone.setNode(((Operator)entry.getValue()).getNode());
				if (send2CNF.containsKey(entry.getKey()))
				{
					((TableScanOperator)entry.getValue()).setCNFForParent(pClone, send2CNF.get(entry.getKey()));
				}
				((Operator)entry.getKey()).add(pClone);
				receive.add((Operator)entry.getKey());
			}
			grandParent.add(receive);
			receive.setNode(grandParent.getNode());
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.error("", e);
			throw e;
		}

		// cCache.clear();
		return false;
	}

	private boolean handleUnion(final NetworkReceiveOperator receive) throws Exception
	{
		if (receive.parent().children().size() == 1)
		{
			final DEMOperator dummy = new DEMOperator(meta);
			receive.parent().add(dummy);
			dummy.setNode(receive.parent().getNode());
			dummy.setCols2Pos(receive.parent().getCols2Pos());
			dummy.setPos2Col(receive.parent().getPos2Col());
			dummy.setCols2Types(receive.parent().getCols2Types());
		}

		final Operator parent = receive.parent();
		verify2ReceivesForHash(parent);
		final Operator grandParent = parent.parent();
		// card(parent.children().get(0));
		// card(parent.children().get(1));
		parent.removeChild(receive);
		grandParent.removeChild(parent);
		ArrayList<Operator> sends = new ArrayList<Operator>(receive.children().size());
		final int starting = getStartingNode(MetaData.numWorkerNodes);
		int ID = id.getAndIncrement();
		for (final Operator o : parent.children())
		{
			if (o != receive)
			{
			}
		}

		CNFFilter cnf = null;
		for (final Operator child : (ArrayList<Operator>)receive.children().clone())
		{
			final int node = child.getNode();
			final Operator grandChild = child.children().get(0);

			if (grandChild instanceof TableScanOperator)
			{
				cnf = ((TableScanOperator)grandChild).getCNFForParent(child);
			}
			child.removeChild(grandChild);

			final NetworkHashAndSendOperator send = new NetworkHashAndSendOperator(new ArrayList<String>(grandChild.getPos2Col().values()), MetaData.numWorkerNodes, ID, starting, meta);
			try
			{
				send.add(grandChild);
				if (cnf != null)
				{
					((TableScanOperator)grandChild).setCNFForParent(send, cnf);
				}
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				throw e;
			}
			send.setNode(node);
			sends.add(send);
		}

		int i = starting;
		ArrayList<Operator> receives = new ArrayList<Operator>();
		while (i < MetaData.numWorkerNodes)
		{
			final NetworkHashReceiveOperator receive2 = new NetworkHashReceiveOperator(ID, meta);
			receive2.setNode(i);
			receives.add(receive2);
			i++;
		}

		for (final Operator receive2 : receives)
		{
			for (final Operator send : sends)
			{
				try
				{
					receive2.add(send);
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.error("", e);
					throw e;
				}
			}
		}

		final ArrayList<Operator> parents = new ArrayList<Operator>(receives.size());
		for (final Operator receive2 : receives)
		{
			final Operator clone = cloneTree(parent, 0);
			try
			{
				clone.add(receive2);
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				throw e;
			}
			clone.setNode(receive2.getNode());
			parents.add(clone);
			for (final Operator o : (ArrayList<Operator>)clone.children().clone())
			{
				if (!o.equals(receive2))
				{
					clone.removeChild(o);
				}
			}
		}

		Operator otherChild = null;
		for (final Operator child : parent.children())
		{
			if (!child.equals(receive))
			{
				otherChild = child;
			}
		}

		sends = new ArrayList<Operator>(otherChild.children().size());
		ID = id.getAndIncrement();
		for (final Operator child : (ArrayList<Operator>)otherChild.children().clone())
		{
			final int node = child.getNode();
			final Operator grandChild = child.children().get(0);
			cnf = null;
			if (grandChild instanceof TableScanOperator)
			{
				cnf = ((TableScanOperator)grandChild).getCNFForParent(child);
			}
			child.removeChild(grandChild);

			final NetworkHashAndSendOperator send = new NetworkHashAndSendOperator(new ArrayList<String>(grandChild.getPos2Col().values()), MetaData.numWorkerNodes, ID, starting, meta);
			try
			{
				send.add(grandChild);
				if (cnf != null)
				{
					((TableScanOperator)grandChild).setCNFForParent(send, cnf);
				}
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				throw e;
			}
			send.setNode(node);
			sends.add(send);
		}

		i = starting;
		receives = new ArrayList<Operator>();
		while (i < MetaData.numWorkerNodes)
		{
			final NetworkHashReceiveOperator receive2 = new NetworkHashReceiveOperator(ID, meta);
			receive2.setNode(i);
			receives.add(receive2);
			i++;
		}

		for (final Operator receive2 : receives)
		{
			for (final Operator send : sends)
			{
				try
				{
					receive2.add(send);
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.error("", e);
					throw e;
				}
			}
		}

		final ArrayList<NetworkSendOperator> sends2 = new ArrayList<NetworkSendOperator>(parents.size());
		for (final Operator clone : parents)
		{
			for (final Operator receive2 : receives)
			{
				if (clone.getNode() == receive2.getNode())
				{
					try
					{
						clone.add(receive2);
					}
					catch (final Exception e)
					{
						HRDBMSWorker.logger.error("", e);
						throw e;
					}
				}
			}

			final NetworkSendOperator send = new NetworkSendOperator(clone.getNode(), meta);
			sends2.add(send);
			try
			{
				send.add(clone);
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				throw e;
			}
		}

		final NetworkReceiveOperator r = new NetworkReceiveOperator(meta);
		r.setNode(grandParent.getNode());
		for (final NetworkSendOperator send : sends2)
		{
			try
			{
				r.add(send);
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				throw e;
			}
		}
		try
		{
			grandParent.add(r);
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.error("", e);
			throw e;
		}
		// makeHierarchical2(r);
		// makeHierarchical(r);
		// cCache.clear();
		return false;
	}

	private boolean isAllAny(final Operator op, final HashSet<Operator> touched)
	{
		if (touched.contains(op))
		{
			return true;
		}

		if (op instanceof TableScanOperator)
		{
			touched.add(op);
			return ((TableScanOperator)op).anyNode();
		}
		else
		{
			touched.add(op);
			for (final Operator o : op.children())
			{
				if (!isAllAny(o, touched))
				{
					return false;
				}
			}

			return true;
		}
	}

	private void makeHierarchical(final NetworkReceiveOperator receive) throws Exception
	{
		if ((receive instanceof NetworkHashReceiveAndMergeOperator) || (receive instanceof NetworkHashReceiveOperator))
		{
			return;
		}

		if (receive.children().size() > MAX_INCOMING_CONNECTIONS)
		{
			int numMiddle = receive.children().size() / MAX_INCOMING_CONNECTIONS;
			if (receive.children().size() % MAX_INCOMING_CONNECTIONS != 0)
			{
				numMiddle++;
			}
			int numPerMiddle = receive.children().size() / numMiddle;
			if (receive.children().size() % numMiddle != 0)
			{
				numPerMiddle++;
			}

			final ArrayList<Operator> sends = (ArrayList<Operator>)receive.children().clone();
			for (final Operator send : sends)
			{
				receive.removeChild(send);
			}

			NetworkReceiveOperator newReceive = null;
			if (receive instanceof NetworkReceiveAndMergeOperator)
			{
				newReceive = receive.clone();
			}
			else
			{
				newReceive = new NetworkReceiveOperator(meta);
			}

			int i = 0;
			final ArrayList<Integer> notUsed = new ArrayList<Integer>();
			while (i < MetaData.numWorkerNodes)
			{
				notUsed.add(i++);
			}

			i = 0;
			while (sends.size() > 0)
			{
				try
				{
					newReceive.add(sends.get(0));
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.error("", e);
					throw e;
				}
				sends.remove(0);
				i++;

				if (i == numPerMiddle)
				{
					final int slot = ThreadLocalRandom.current().nextInt(notUsed.size());
					final int node = notUsed.get(slot);
					notUsed.remove(slot);

					newReceive.setNode(node);
					final NetworkSendOperator newSend = new NetworkSendOperator(node, meta);
					try
					{
						newSend.add(newReceive);
						receive.add(newSend);
					}
					catch (final Exception e)
					{
						HRDBMSWorker.logger.error("", e);
						throw e;
					}
					if (receive instanceof NetworkReceiveAndMergeOperator)
					{
						newReceive = receive.clone();
					}
					else
					{
						newReceive = new NetworkReceiveOperator(meta);
					}
					i = 0;
				}
			}

			if (i != 0)
			{
				final int slot = ThreadLocalRandom.current().nextInt(notUsed.size());
				final int node = notUsed.get(slot);
				notUsed.remove(slot);
				newReceive.setNode(node);
				final NetworkSendOperator newSend = new NetworkSendOperator(node, meta);
				try
				{
					newSend.add(newReceive);
					receive.add(newSend);
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.error("", e);
					throw e;
				}
			}
			makeHierarchical(receive);
		}
	}

	private void makeHierarchicalForAll(final Operator op, final HashSet<Operator> visited) throws Exception
	{
		if (op instanceof NetworkSendOperator)
		{
			if (visited.contains(op))
			{
				return;
			}

			visited.add(op);
		}

		if (op instanceof NetworkReceiveOperator && (op.getClass().equals(NetworkReceiveOperator.class) || op.getClass().equals(NetworkReceiveAndMergeOperator.class)))
		{
			makeHierarchical((NetworkReceiveOperator)op);
		}

		for (final Operator o : op.children())
		{
			makeHierarchicalForAll(o, visited);
		}
	}

	private boolean noLargeUpstreamJoins(final Operator op) throws Exception
	{
		Operator o = op.parent();
		while (!(o instanceof RootOperator))
		{
			if (o instanceof ProductOperator)
			{
				long l = card(o.children().get(0));
				long r = card(o.children().get(1));
				if (l == 0)
				{
					l = 1;
				}

				if (r == 0)
				{
					r = 1;
				}
				if (l * r > 0 && l * r <= MAX_LOCAL_NO_HASH_PRODUCT && noLargeUpstreamJoins(o))
				{
					return true;
				}

				return false;
			}

			if (o instanceof HashJoinOperator)
			{
				if (card(o.children().get(0)) + card(o.children().get(1)) <= MAX_LOCAL_LEFT_HASH && noLargeUpstreamJoins(o))
				{
					return true;
				}

				return false;
			}

			if (o instanceof NestedLoopJoinOperator)
			{
				long l = card(o.children().get(0));
				long r = card(o.children().get(1));
				if (l == 0)
				{
					l = 1;
				}

				if (r == 0)
				{
					r = 1;
				}
				if (((NestedLoopJoinOperator)o).usesHash())
				{
					if (l + r <= MAX_LOCAL_LEFT_HASH && noLargeUpstreamJoins(o))
					{
						return true;
					}
				}
				else if (((NestedLoopJoinOperator)o).usesSort())
				{
					if (card(o) <= MAX_LOCAL_NO_HASH_PRODUCT && r <= MAX_LOCAL_SORT && noLargeUpstreamJoins(o))
					{
						return true;
					}
				}
				else if (l * r > 0 && l * r <= MAX_LOCAL_NO_HASH_PRODUCT && noLargeUpstreamJoins(o))
				{
					return true;
				}

				return false;
			}

			if (o instanceof SemiJoinOperator)
			{
				long l = card(o.children().get(0));
				long r = card(o.children().get(1));
				if (l == 0)
				{
					l = 1;
				}

				if (r == 0)
				{
					r = 1;
				}
				if (((SemiJoinOperator)o).usesHash())
				{
					if (l + r <= MAX_LOCAL_LEFT_HASH && noLargeUpstreamJoins(o))
					{
						return true;
					}
				}
				else if (((SemiJoinOperator)o).usesSort())
				{
					if (card(o) <= MAX_LOCAL_NO_HASH_PRODUCT && r <= MAX_LOCAL_SORT && noLargeUpstreamJoins(o))
					{
						return true;
					}
				}
				else if (l * r > 0 && l * r <= MAX_LOCAL_NO_HASH_PRODUCT && noLargeUpstreamJoins(o))
				{
					return true;
				}

				return false;
			}

			if (o instanceof AntiJoinOperator)
			{
				long l = card(o.children().get(0));
				long r = card(o.children().get(1));
				if (l == 0)
				{
					l = 1;
				}

				if (r == 0)
				{
					r = 1;
				}
				if (((AntiJoinOperator)o).usesHash())
				{
					if (l + r <= MAX_LOCAL_LEFT_HASH && noLargeUpstreamJoins(o))
					{
						return true;
					}
				}
				else if (((AntiJoinOperator)o).usesSort())
				{
					if (card(o) <= MAX_LOCAL_NO_HASH_PRODUCT && r <= MAX_LOCAL_SORT && noLargeUpstreamJoins(o))
					{
						return true;
					}
				}
				else if (l * r > 0 && l * r <= MAX_LOCAL_NO_HASH_PRODUCT && noLargeUpstreamJoins(o))
				{
					return true;
				}

				return false;
			}

			o = o.parent();
		}

		return true;
	}

	private long notCached(final Operator op) throws Exception
	{
		if (op instanceof DummyOperator)
		{
			return 1;
		}

		if (op instanceof DEMOperator)
		{
			return 0;
		}

		if (op instanceof HashJoinOperator)
		{
			return cardHJO(op);
		}

		if (op instanceof IntersectOperator)
		{
			return cardSetI(op);
		}

		if (op instanceof MultiOperator)
		{
			return cardMO(op);
		}

		if (op instanceof NestedLoopJoinOperator)
		{
			return cardNL(op);
		}

		if (op instanceof NetworkReceiveOperator)
		{
			return cardRX(op);
		}

		if (op instanceof NetworkHashAndSendOperator)
		{
			return cardTX(op);
		}

		if (op instanceof NetworkSendRROperator)
		{
			return cardTXRR(op);
		}

		if (op instanceof ProductOperator)
		{
			return cardX(op);
		}

		if (op instanceof SelectOperator)
		{
			return cardSelect(op);
		}

		if (op instanceof TopOperator)
		{
			return cardTop(op);
		}

		if (op instanceof UnionOperator)
		{
			return cardUnion(op);
		}

		if (op instanceof TableScanOperator)
		{
			return cardTSO(op);
		}

		return cardNorm(op);
	}

	private ArrayList<NetworkReceiveOperator> order(final HashMap<NetworkReceiveOperator, Integer> receives)
	{
		final ArrayList<NetworkReceiveOperator> retval = new ArrayList<NetworkReceiveOperator>(receives.size());
		while (receives.size() > 0)
		{
			NetworkReceiveOperator maxReceive = null;
			int maxLevel = Integer.MIN_VALUE;
			double minConcatPath = Double.MAX_VALUE;
			for (final Map.Entry entry : receives.entrySet())
			{
				if ((Integer)entry.getValue() > maxLevel)
				{
					maxLevel = (Integer)entry.getValue();
					maxReceive = (NetworkReceiveOperator)entry.getKey();
					minConcatPath = concatPath((Operator)entry.getKey());
				}
				else if (lt)
				{
					if ((Integer)entry.getValue() == maxLevel && concatPath((Operator)entry.getKey()) < minConcatPath)
					{
						maxLevel = (Integer)entry.getValue();
						maxReceive = (NetworkReceiveOperator)entry.getKey();
						minConcatPath = concatPath((Operator)entry.getKey());
					}
				}
				else
				{
					if ((Integer)entry.getValue() == maxLevel && concatPath((Operator)entry.getKey()) > minConcatPath)
					{
						maxLevel = (Integer)entry.getValue();
						maxReceive = (NetworkReceiveOperator)entry.getKey();
						minConcatPath = concatPath((Operator)entry.getKey());
					}
				}
			}

			receives.remove(maxReceive);
			retval.add(maxReceive);
		}

		lt = !lt;
		return retval;
	}

	private void pushUpReceives() throws Exception
	{
		final HashSet<NetworkReceiveOperator> completed = new HashSet<NetworkReceiveOperator>();
		boolean workToDo = true;
		while (workToDo)
		{
			workToDo = false;
			final HashMap<NetworkReceiveOperator, Integer> receives = new HashMap<NetworkReceiveOperator, Integer>();
			getReceives(root, 0, receives);
			for (final NetworkReceiveOperator receive : order(receives))
			{
				if (completed.contains(receive))
				{
					continue;
				}
				if (!treeContains(root, receive, new HashSet<Operator>()))
				{
					continue;
				}

				final Operator op = receive.parent();
				if (op instanceof SelectOperator || op instanceof YearOperator || op instanceof SubstringOperator || op instanceof ProjectOperator || op instanceof ExtendOperator || op instanceof RenameOperator || op instanceof ReorderOperator || op instanceof CaseOperator || op instanceof ExtendObjectOperator || op instanceof DateMathOperator || op instanceof ConcatOperator)
				{
					if (op instanceof SelectOperator)
					{
						int count = 1;
						Operator parent = op.parent();
						while (parent instanceof SelectOperator)
						{
							count++;
							parent = parent.parent();
						}

						if (count >= 20)
						{
							continue;
						}
					}
					pushAcross(receive);
					workToDo = true;
					break;
				}
				else if (op instanceof SortOperator)
				{
					// if (!eligible.contains(receive))
					// {
					// continue;
					// }
					if (!handleSort(receive))
					{
						completed.add(receive);
					}
					workToDo = true;
					break;
				}
				else if (op instanceof MultiOperator)
				{
					// if (!eligible.contains(receive))
					// {
					// continue;
					// }
					if (!handleMulti(receive))
					{
						completed.add(receive);
					}
					workToDo = true;
					break;
				}
				else if (op instanceof ProductOperator)
				{
					op.parent();
					// checkOrder(op);
					long l = card(op.children().get(0));
					long r = card(op.children().get(1));
					if (l == 0)
					{
						l = 1;
					}

					if (r == 0)
					{
						r = 1;
					}

					if (l * r > 0 && l * r <= MAX_LOCAL_NO_HASH_PRODUCT && noLargeUpstreamJoins(op))
					{
						continue;
					}

					if (!redistributeProduct(receive))
					{
						completed.add(receive);
					}

					workToDo = true;
					break;
				}
				else if (op instanceof HashJoinOperator)
				{
					op.parent();
					if (card(op.children().get(0)) + card(op.children().get(1)) <= MAX_LOCAL_LEFT_HASH && noLargeUpstreamJoins(op))
					{
						continue;
					}

					if (!redistributeHash(receive))
					{
						completed.add(receive);
					}

					workToDo = true;
					break;
				}
				else if (op instanceof UnionOperator)
				{
					op.parent();

					if (noLargeUpstreamJoins(op))
					{
						long card = 0;
						for (final Operator child : op.children())
						{
							card += card(child);
						}

						if (card <= MAX_LOCAL_LEFT_HASH / 2)
						{
							continue;
						}
					}

					if (!handleUnion(receive))
					{
						completed.add(receive);
					}

					workToDo = true;
					break;
				}
				else if (op instanceof IntersectOperator)
				{
					op.parent();
					if (card(op.children().get(0)) + card(op.children().get(1)) <= MAX_LOCAL_LEFT_HASH / 2 && noLargeUpstreamJoins(op))
					{
						continue;
					}

					if (!handleExcept(receive))
					{
						completed.add(receive);
					}

					workToDo = true;
					break;
				}
				else if (op instanceof ExceptOperator)
				{
					op.parent();
					if (card(op.children().get(0)) + card(op.children().get(1)) <= MAX_LOCAL_LEFT_HASH / 2 && noLargeUpstreamJoins(op))
					{
						continue;
					}

					if (!handleExcept(receive))
					{
						completed.add(receive);
					}

					workToDo = true;
					break;
				}
				else if (op instanceof NestedLoopJoinOperator)
				{
					op.parent();
					// checkOrder(op);
					long l = card(op.children().get(0));
					long r = card(op.children().get(1));
					if (l == 0)
					{
						l = 1;
					}

					if (r == 0)
					{
						r = 1;
					}
					if (((NestedLoopJoinOperator)op).usesHash())
					{
						if (l + r <= MAX_LOCAL_LEFT_HASH && noLargeUpstreamJoins(op))
						{
							continue;
						}
					}
					else if (((NestedLoopJoinOperator)op).usesSort())
					{
						if (card(op) <= MAX_LOCAL_NO_HASH_PRODUCT && r <= MAX_LOCAL_SORT && noLargeUpstreamJoins(op))
						{
							continue;
						}
					}
					else if (l * r > 0 && l * r <= MAX_LOCAL_NO_HASH_PRODUCT && noLargeUpstreamJoins(op))
					{
						continue;
					}

					if (!redistributeNL(receive))
					{
						completed.add(receive);
					}

					workToDo = true;
					break;
				}
				else if (op instanceof SemiJoinOperator)
				{
					long l = card(op.children().get(0));
					long r = card(op.children().get(1));
					if (l == 0)
					{
						l = 1;
					}

					if (r == 0)
					{
						r = 1;
					}
					op.parent();
					if (((SemiJoinOperator)op).usesHash())
					{
						if (l + r <= MAX_LOCAL_LEFT_HASH && noLargeUpstreamJoins(op))
						{
							HRDBMSWorker.logger.debug("SemiJoin uses partial hash, but is not pushed down because...");
							HRDBMSWorker.logger.debug("Left card = " + card(op.children().get(0)));
							HRDBMSWorker.logger.debug("Right card = " + card(op.children().get(1)));
							continue;
						}
					}
					else if (((SemiJoinOperator)op).usesSort())
					{
						if (card(op) <= MAX_LOCAL_NO_HASH_PRODUCT && r <= MAX_LOCAL_SORT && noLargeUpstreamJoins(op))
						{
							HRDBMSWorker.logger.debug("SemiJoin uses sort, but is not pushed down because...");
							HRDBMSWorker.logger.debug("Card = " + card(op));
							continue;
						}
					}
					else if (l * r > 0 && l * r <= MAX_LOCAL_NO_HASH_PRODUCT && noLargeUpstreamJoins(op))
					{
						HRDBMSWorker.logger.debug("SemiJoin uses NL, but is not pushed down because...");
						HRDBMSWorker.logger.debug("Left card = " + card(op.children().get(0)));
						HRDBMSWorker.logger.debug("Right card = " + card(op.children().get(1)));
						continue;
					}

					HRDBMSWorker.logger.debug("SemiJoin is pushed down");
					if (!redistributeSemi(receive))
					{
						completed.add(receive);
					}

					workToDo = true;
					break;
				}
				else if (op instanceof AntiJoinOperator)
				{
					long l = card(op.children().get(0));
					long r = card(op.children().get(1));
					if (l == 0)
					{
						l = 1;
					}

					if (r == 0)
					{
						r = 1;
					}
					op.parent();
					if (((AntiJoinOperator)op).usesHash())
					{
						if (l + r <= MAX_LOCAL_LEFT_HASH && noLargeUpstreamJoins(op))
						{
							// HRDBMSWorker.logger.debug("AntiJoin uses partial
							// hash, but is not pushed down because...");
							// HRDBMSWorker.logger.debug("Left card = " +
							// card(op.children().get(0)));
							// HRDBMSWorker.logger.debug("Right card = " +
							// card(op.children().get(1)));
							continue;
						}
					}
					else if (((AntiJoinOperator)op).usesSort())
					{
						if (card(op) <= MAX_LOCAL_NO_HASH_PRODUCT && r <= MAX_LOCAL_SORT && noLargeUpstreamJoins(op))
						{
							// HRDBMSWorker.logger.debug("AntiJoin uses sort,
							// but is not pushed down because...");
							// HRDBMSWorker.logger.debug("Card = " + card(op));
							continue;
						}
					}
					else if (l * r > 0 && l * r <= MAX_LOCAL_NO_HASH_PRODUCT && noLargeUpstreamJoins(op))
					{
						// HRDBMSWorker.logger.debug("AntiJoin uses NL, but is
						// not pushed down because...");
						// HRDBMSWorker.logger.debug("Left card = " +
						// card(op.children().get(0)));
						// HRDBMSWorker.logger.debug("Right card = " +
						// card(op.children().get(1)));
						continue;
					}

					// HRDBMSWorker.logger.debug("AntiJoin is pushed down");
					if (!redistributeAnti(receive))
					{
						completed.add(receive);
					}

					workToDo = true;
					break;
				}
				else if (op instanceof TopOperator)
				{
					// if (!eligible.contains(receive))
					// {
					// continue;
					// }
					if (!handleTop(receive))
					{
						completed.add(receive);
					}
					workToDo = true;
					break;
				}
			}
		}
	}

	private boolean redistributeAnti(final NetworkReceiveOperator receive) throws Exception
	{
		if (((AntiJoinOperator)receive.parent()).usesHash())
		{
			return doHashAnti(receive);
		}

		return doNonHashSemi(receive);
	}

	private boolean redistributeHash(final NetworkReceiveOperator receive) throws Exception
	{
		final Operator parent = receive.parent();
		final Operator grandParent = parent.parent();
		// card(parent.children().get(0));
		// card(parent.children().get(1));
		ArrayList<String> join = ((JoinOperator)parent).getJoinForChild(receive);
		verify2ReceivesForHash(parent);

		parent.removeChild(receive);
		grandParent.removeChild(parent);
		ArrayList<Operator> sends = new ArrayList<Operator>(receive.children().size());
		final int starting = getStartingNode(MetaData.numWorkerNodes);
		int ID = id.getAndIncrement();

		CNFFilter cnf = null;
		for (final Operator child : (ArrayList<Operator>)receive.children().clone())
		{
			final int node = child.getNode();
			final Operator grandChild = child.children().get(0);

			if (grandChild instanceof TableScanOperator)
			{
				cnf = ((TableScanOperator)grandChild).getCNFForParent(child);
			}
			child.removeChild(grandChild);

			final NetworkHashAndSendOperator send = new NetworkHashAndSendOperator(join, MetaData.numWorkerNodes, ID, starting, meta);
			try
			{
				send.add(grandChild);
				if (cnf != null)
				{
					((TableScanOperator)grandChild).setCNFForParent(send, cnf);
				}
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				throw e;
			}
			send.setNode(node);
			sends.add(send);
		}

		int i = starting;
		ArrayList<Operator> receives = new ArrayList<Operator>();
		while (i < MetaData.numWorkerNodes)
		{
			final NetworkHashReceiveOperator receive2 = new NetworkHashReceiveOperator(ID, meta);
			receive2.setNode(i);
			receives.add(receive2);
			i++;
		}

		for (final Operator receive2 : receives)
		{
			for (final Operator send : sends)
			{
				try
				{
					receive2.add(send);
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.error("", e);
					throw e;
				}
			}
		}

		final ArrayList<Operator> parents = new ArrayList<Operator>(receives.size());
		for (final Operator receive2 : receives)
		{
			final Operator clone = cloneTree(parent, 0);
			try
			{
				clone.add(receive2);
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				throw e;
			}
			clone.setNode(receive2.getNode());
			parents.add(clone);
			for (final Operator o : (ArrayList<Operator>)clone.children().clone())
			{
				if (!o.equals(receive2))
				{
					clone.removeChild(o);
				}
			}
		}

		Operator otherChild = null;
		for (final Operator child : parent.children())
		{
			if (!child.equals(receive))
			{
				otherChild = child;
			}
		}

		join = ((JoinOperator)parent).getJoinForChild(otherChild);
		sends = new ArrayList<Operator>(otherChild.children().size());
		ID = id.getAndIncrement();
		for (final Operator child : (ArrayList<Operator>)otherChild.children().clone())
		{
			final int node = child.getNode();
			final Operator grandChild = child.children().get(0);
			cnf = null;
			if (grandChild instanceof TableScanOperator)
			{
				cnf = ((TableScanOperator)grandChild).getCNFForParent(child);
			}
			child.removeChild(grandChild);

			final NetworkHashAndSendOperator send = new NetworkHashAndSendOperator(join, MetaData.numWorkerNodes, ID, starting, meta);
			try
			{
				send.add(grandChild);
				if (cnf != null)
				{
					((TableScanOperator)grandChild).setCNFForParent(send, cnf);
				}
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				throw e;
			}
			send.setNode(node);
			sends.add(send);
		}

		i = starting;
		receives = new ArrayList<Operator>();
		while (i < MetaData.numWorkerNodes)
		{
			final NetworkHashReceiveOperator receive2 = new NetworkHashReceiveOperator(ID, meta);
			receive2.setNode(i);
			receives.add(receive2);
			i++;
		}

		for (final Operator receive2 : receives)
		{
			for (final Operator send : sends)
			{
				try
				{
					receive2.add(send);
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.error("", e);
					throw e;
				}
			}
		}

		final ArrayList<NetworkSendOperator> sends2 = new ArrayList<NetworkSendOperator>(parents.size());
		for (final Operator clone : parents)
		{
			for (final Operator receive2 : receives)
			{
				if (clone.getNode() == receive2.getNode())
				{
					try
					{
						clone.add(receive2);
					}
					catch (final Exception e)
					{
						HRDBMSWorker.logger.error("", e);
						throw e;
					}
				}
			}

			final NetworkSendOperator send = new NetworkSendOperator(clone.getNode(), meta);
			sends2.add(send);
			try
			{
				send.add(clone);
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				throw e;
			}
		}

		final NetworkReceiveOperator r = new NetworkReceiveOperator(meta);
		r.setNode(grandParent.getNode());
		for (final NetworkSendOperator send : sends2)
		{
			try
			{
				r.add(send);
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				throw e;
			}
		}
		try
		{
			grandParent.add(r);
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.error("", e);
			throw e;
		}
		// makeHierarchical2(r);
		// makeHierarchical(r);
		// cCache.clear();
		return false;
	}

	private boolean redistributeNL(final NetworkReceiveOperator receive) throws Exception
	{
		if (((NestedLoopJoinOperator)receive.parent()).usesHash())
		{
			return redistributeHash(receive);
		}

		return redistributeProduct(receive);
	}

	private boolean redistributeProduct(final NetworkReceiveOperator receive) throws Exception
	{
		final Operator parent = receive.parent();
		Operator left = parent.children().get(0);
		Operator right = parent.children().get(1);

		if (isAllAny(left, new HashSet<Operator>()) && left.children().size() == 1)
		{
			verify2ReceivesForProduct(parent);
			left = parent.children().get(0);
			right = parent.children().get(1);
			final Operator grandParent = parent.parent();
			parent.removeChild(left);
			grandParent.removeChild(parent);
			final ArrayList<Operator> grandChildren = new ArrayList<Operator>();
			for (final Operator child : (ArrayList<Operator>)right.children().clone())
			{
				final Operator grandChild = child.children().get(0);
				if (!(grandChild instanceof NetworkReceiveOperator))
				{
					grandChildren.add(grandChild);
				}
				else
				{
					grandChildren.addAll(getGrandChildren(grandChild));
				}
			}

			for (final Operator o : grandChildren)
			{
				if (o.parent() != null)
				{
					o.parent().removeChild(o);
				}
			}

			final ArrayList<NetworkSendOperator> sends2 = new ArrayList<NetworkSendOperator>(grandChildren.size());
			for (final Operator grandChild : grandChildren)
			{
				final Operator clone = cloneTree(parent, 0);
				clone.setNode(grandChild.getNode());
				final Operator leftClone = fullyCloneTree(left.children().get(0).children().get(0));
				setNodeForTree(leftClone, grandChild.getNode(), new HashSet<Operator>());
				try
				{
					clone.add(leftClone);
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.error("", e);
					throw e;
				}

				for (final Operator o : (ArrayList<Operator>)clone.children().clone())
				{
					if (!o.equals(leftClone))
					{
						clone.removeChild(o);
					}
				}

				try
				{
					clone.add(grandChild);
					if (grandChild instanceof TableScanOperator)
					{
						final CNFFilter cnf = ((TableScanOperator)grandChild).getFirstCNF();
						if (cnf != null)
						{
							((TableScanOperator)grandChild).setCNFForParent(clone, cnf);
						}
					}
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.error("", e);
					throw e;
				}

				final NetworkSendOperator send2 = new NetworkSendOperator(clone.getNode(), meta);
				try
				{
					send2.add(clone);
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.error("", e);
					throw e;
				}

				sends2.add(send2);
			}

			final NetworkReceiveOperator r = new NetworkReceiveOperator(meta);
			r.setNode(grandParent.getNode());

			for (final NetworkSendOperator send : sends2)
			{
				try
				{
					r.add(send);
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.error("", e);
					throw e;
				}
			}

			try
			{
				grandParent.add(r);
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				throw e;
			}

			// makeHierarchical(r);
			// cCache.clear();
			return false;
		}

		return false;
	}

	private boolean redistributeSemi(final NetworkReceiveOperator receive) throws Exception
	{
		if (((SemiJoinOperator)receive.parent()).usesHash())
		{
			return doHashSemi(receive);
		}

		return doNonHashSemi(receive);
	}

	private void redistributeSorts() throws Exception
	{
		final SortOperator sort = getLocalSort(root);
		if (sort != null)
		{
			final long card = card(sort);
			if (card > MAX_LOCAL_SORT)
			{
				doSortRedistribution(sort, card);
			}
		}
	}

	private void removeDuplicateReorders(final Operator op, final HashSet<Operator> touched) throws Exception
	{
		if (op instanceof NetworkSendOperator)
		{
			if (touched.contains(op))
			{
				return;
			}

			touched.add(op);
		}

		if (!(op instanceof ReorderOperator))
		{
			for (final Operator o : op.children())
			{
				removeDuplicateReorders(o, touched);
			}
		}
		else
		{
			if (op.children().get(0) instanceof ReorderOperator)
			{
				// only need last one
				final Operator child = op.children().get(0);
				final Operator grandChild = child.children().get(0);
				CNFFilter cnf = null;
				if (grandChild instanceof TableScanOperator)
				{
					cnf = ((TableScanOperator)grandChild).getCNFForParent(child);
				}

				child.removeChild(grandChild);
				op.removeChild(child);
				op.add(grandChild);
				if (cnf != null)
				{
					((TableScanOperator)grandChild).setCNFForParent(op, cnf);
				}

				touched.remove(op);
				removeDuplicateReorders(op, touched);
			}
			else
			{
				for (final Operator o : op.children())
				{
					removeDuplicateReorders(o, touched);
				}
			}
		}
	}

	private void removeLocalSendReceive(final Operator op, final HashSet<Operator> visited) throws Exception
	{
		if (op instanceof NetworkSendOperator)
		{
			if (visited.contains(op))
			{
				return;
			}

			visited.add(op);
		}

		if (op instanceof NetworkReceiveOperator && op.getClass().equals(NetworkReceiveOperator.class))
		{
			if (op.children().size() == 1)
			{
				final Operator send = op.children().get(0);
				if (send.getNode() == op.getNode())
				{
					final Operator parent = op.parent();
					parent.removeChild(op);
					final Operator child = send.children().get(0);
					send.removeChild(child);
					try
					{
						parent.add(child);
					}
					catch (final Exception e)
					{
						HRDBMSWorker.logger.error("", e);
						throw e;
					}
					removeLocalSendReceive(child, visited);
				}
			}
			else
			{
				for (final Operator send : op.children())
				{
					if (send.getNode() != op.getNode())
					{
						for (final Operator s : op.children())
						{
							removeLocalSendReceive(s, visited);
						}

						return;
					}
				}

				final Operator parent = op.parent();
				parent.removeChild(op);
				final Operator union = new UnionOperator(false, meta);
				try
				{
					for (final Operator send : op.children())
					{
						final Operator child = send.children().get(0);
						send.removeChild(child);
						union.add(child);
						union.setNode(child.getNode());
					}
					parent.add(union);
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.error("", e);
					throw e;
				}
				removeLocalSendReceive(union, visited);
			}
		}
		else
		{
			for (final Operator o : (ArrayList<Operator>)op.children().clone())
			{
				removeLocalSendReceive(o, visited);
			}
		}
	}

	private void removeUnneededHash() throws Exception
	{
		final ArrayList<TableScanOperator> tables = getTables(root, new HashSet<Operator>());
		// HashSet<TableScanOperator> temp = new
		// HashSet<TableScanOperator>(tables);
		for (final TableScanOperator table : tables)
		{
			if (table.noNodeGroupSet() && table.allNodes() && table.nodeIsHash())
			{
				ArrayList<String> current = (ArrayList<String>)table.getNodeHash().clone();
				// fix current
				final int i = 0;
				for (final String col : current)
				{
					if (table.getAlias() != null && !table.getAlias().equals(""))
					{
						if (!col.contains("."))
						{
							current.remove(i);
							current.add(i, table.getAlias() + "." + col);
						}
					}
					else
					{
						if (!col.contains("."))
						{
							current.remove(i);
							current.add(i, table.getTable() + "." + col);
						}
					}
				}
				// HRDBMSWorker.logger.debug("Looking at " + table);
				// HRDBMSWorker.logger.debug("Original hash is " + current);
				Operator up = table.firstParent();
				doneWithTable: while (true)
				{
					while (!(up instanceof NetworkSendOperator))
					{
						if (up instanceof RenameOperator)
						{
							// change names in current if need be
							final HashMap<String, String> old2New = ((RenameOperator)up).getRenameMap();
							for (final String col : (ArrayList<String>)current.clone())
							{
								final String newName = old2New.get(col);
								if (newName != null)
								{
									final int index = current.indexOf(col);
									current.remove(index);
									current.add(index, newName);
								}
							}
							// HRDBMSWorker.logger.debug("Current has changed to
							// "
							// + current);
						}

						if (up instanceof MultiOperator)
						{
							// take things out of current except for ones that
							// are also in the group by
							current.retainAll(((MultiOperator)up).getKeys());
							// HRDBMSWorker.logger.debug("Current has changed to
							// "
							// + current);
						}

						if (up instanceof RootOperator)
						{
							break doneWithTable;
						}

						up = up.parent();
					}

					if (up instanceof NetworkHashAndSendOperator)
					{
						if (((NetworkHashAndSendOperator)up).parents().size() == MetaData.numWorkerNodes)
						{
							if (((NetworkHashAndSendOperator)up).getHashCols().equals(current) && current.size() > 0)
							{
								// HRDBMSWorker.logger.debug("Removing " + up);
								// remove sends and receives
								Operator grandParent = null;
								for (final Operator parent : (ArrayList<Operator>)((NetworkHashAndSendOperator)up).parents().clone())
								{
									if (parent.getNode() == table.getNode())
									{
										grandParent = parent.parent();
										grandParent.removeChild(parent);
										CNFFilter cnf = null;
										parent.removeChild(up);

										final Operator child = up.children().get(0);
										if (child instanceof TableScanOperator)
										{
											cnf = ((TableScanOperator)child).getCNFForParent(up);
										}
										up.removeChild(child);
										grandParent.add(child);

										if (cnf != null)
										{
											((TableScanOperator)child).setCNFForParent(grandParent, cnf);
										}
									}
								}

								// set up to grandparent that is on my node
								up = grandParent;
							}
							else
							{
								// HRDBMSWorker.logger.debug("Hashes don't
								// match: "
								// + up);
								current = (ArrayList<String>)((NetworkHashAndSendOperator)up).getHashCols().clone();
								// HRDBMSWorker.logger.debug("Current has
								// changed to "
								// + current);
								// set up to the parent that is on my node
								for (final Operator parent : ((NetworkHashAndSendOperator)up).parents())
								{
									if (parent.getNode() == table.getNode())
									{
										up = parent;
									}
								}
							}
						}
						else
						{
							// HRDBMSWorker.logger.debug("Hash and send does not
							// use all nodes. Size = "
							// +
							// ((NetworkHashAndSendOperator)up).parents().size());
							// HRDBMSWorker.logger.debug(up);
							break;
						}
					}
					else
					{
						// HRDBMSWorker.logger.debug("Not a hash and send: " +
						// up);
						break;
					}
				}
			}
		}
	}

	private void setNodeForTree(final Operator op, final int node, final HashSet<Operator> touched)
	{
		if (touched.contains(op))
		{
			return;
		}

		touched.add(op);
		op.setNode(node);
		for (final Operator o : op.children())
		{
			setNodeForTree(o, node, touched);
		}
	}

	private void swapHJO(final Operator op) throws Exception
	{
		HRDBMSWorker.logger.debug("Swapping HJ");
		final Operator left = op.children().get(0);
		final Operator right = op.children().get(1);
		final ArrayList<String> origOrder = new ArrayList<String>(op.getPos2Col().values());
		op.removeChild(left);
		op.removeChild(right);
		op.add(right);
		op.add(left);
		final Operator parent = op.parent();
		parent.removeChild(op);
		final ReorderOperator reorder = new ReorderOperator(origOrder, meta);
		reorder.add(op);
		parent.add(reorder);
	}

	private void swapLeftRight(final Operator op, final HashSet<Operator> touched) throws Exception
	{
		if (op instanceof NetworkSendOperator)
		{
			if (touched.contains(op))
			{
				return;
			}

			touched.add(op);
		}

		if (op instanceof HashJoinOperator)
		{
			if (card(op.children().get(0)) < card(op.children().get(1)))
			{
				// swap
				swapHJO(op);
			}
		}
		else if (op instanceof NestedLoopJoinOperator)
		{
			if (card(op.children().get(0)) < card(op.children().get(1)))
			{
				// swap
				swapNL(op);
			}
		}

		for (final Operator o : (ArrayList<Operator>)op.children().clone())
		{
			swapLeftRight(o, touched);
		}
	}

	private void swapNL(final Operator op) throws Exception
	{
		HRDBMSWorker.logger.debug("Swapping NL");
		final Operator left = op.children().get(0);
		final Operator right = op.children().get(1);
		final ArrayList<String> origOrder = new ArrayList<String>(op.getPos2Col().values());
		op.removeChild(left);
		op.removeChild(right);
		op.add(right);
		op.add(left);
		final Operator parent = op.parent();
		parent.removeChild(op);
		final ReorderOperator reorder = new ReorderOperator(origOrder, meta);
		reorder.add(op);
		parent.add(reorder);
	}

	/*
	 * private void sanityCheck(Operator op, int node) throws Exception { if (op
	 * instanceof NetworkSendOperator) { node = op.getNode(); for (Operator o :
	 * op.children()) { sanityCheck(o, node); } } else { if (op.getNode() !=
	 * node) { HRDBMSWorker.logger.debug("P4 sanity check failed");
	 * HRDBMSWorker.logger.debug("Parent is " + op.parent() + " (" +
	 * op.parent().getNode() + ")"); HRDBMSWorker.logger.debug("Children are..."
	 * ); for (Operator o : op.parent().children()) { if (o == op) {
	 * HRDBMSWorker.logger.debug("***** " + o + " (" + o.getNode() + ") *****");
	 * } else { HRDBMSWorker.logger.debug(o + " (" + o.getNode() + ")"); } }
	 * throw new Exception("P4 sanity check failed"); }
	 *
	 * for (Operator o : op.children()) { sanityCheck(o, node); } } }
	 */

	private boolean treeContains(final Operator root, final Operator op, final HashSet<Operator> touched)
	{
		if (touched.contains(root))
		{
			return false;
		}

		touched.add(root);

		if (root.equals(op))
		{
			return true;
		}

		for (final Operator o : root.children())
		{
			if (treeContains(o, op, touched))
			{
				return true;
			}
		}

		return false;
	}

	private boolean upstreamRedistSort(final Operator op) throws Exception
	{
		Operator o = op.parent();
		while (!(o instanceof RootOperator))
		{
			if (o instanceof SortOperator)
			{
				final long card = card(o);
				if (card > MAX_LOCAL_SORT)
				{
					return true;
				}
			}

			o = o.parent();
		}

		return false;
	}

	private void verify2ReceivesForHash(final Operator op) throws Exception
	{
		try
		{
			if (!(op.children().get(0) instanceof NetworkReceiveOperator))
			{
				final Operator child = op.children().get(0);
				op.removeChild(child);
				final NetworkSendOperator send = new NetworkSendOperator(op.getNode(), meta);
				send.add(child);
				final NetworkReceiveOperator receive = new NetworkReceiveOperator(meta);
				receive.setNode(op.getNode());
				receive.add(send);
				op.add(receive);
				// cCache.clear();
			}

			if (!(op.children().get(1) instanceof NetworkReceiveOperator))
			{
				final Operator child = op.children().get(1);
				op.removeChild(child);
				final NetworkSendOperator send = new NetworkSendOperator(op.getNode(), meta);
				send.add(child);
				final NetworkReceiveOperator receive = new NetworkReceiveOperator(meta);
				receive.setNode(op.getNode());
				receive.add(send);
				op.add(receive);
				// cCache.clear();
			}
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.error("", e);
			throw e;
		}
	}

	private void verify2ReceivesForProduct(final Operator op) throws Exception
	{
		try
		{
			if (!(op.children().get(0) instanceof NetworkReceiveOperator))
			{
				final Operator child = op.children().get(0);
				op.removeChild(child);
				final NetworkSendOperator send = new NetworkSendOperator(-1, meta);
				send.add(child);
				final NetworkReceiveOperator receive = new NetworkReceiveOperator(meta);
				receive.setNode(op.getNode());
				receive.add(send);
				op.add(receive);
				// cCache.clear();
			}

			if (!(op.children().get(1) instanceof NetworkReceiveOperator))
			{
				final Operator child = op.children().get(1);
				op.removeChild(child);
				final int ID = id.getAndIncrement();
				final NetworkSendRROperator send = new NetworkSendRROperator(ID, meta);
				send.setNode(child.getNode());
				send.add(child);

				final long numNodes = card(child) / MAX_LOCAL_SORT + 1;
				final int starting = getStartingNode(numNodes);
				final ArrayList<NetworkSendOperator> sends = new ArrayList<NetworkSendOperator>();
				int i = 0;
				while (i < numNodes && starting + i < MetaData.numWorkerNodes)
				{
					final NetworkHashReceiveOperator receive = new NetworkHashReceiveOperator(ID, meta);
					receive.setNode(starting + i);
					receive.add(send);
					final NetworkSendOperator send2 = new NetworkSendOperator(starting + i, meta);
					final ReorderOperator reorder = new ReorderOperator(new ArrayList<String>(receive.getPos2Col().values()), meta);
					reorder.setNode(starting + i);
					reorder.add(receive);
					send2.add(reorder);
					sends.add(send2);
					i++;
				}

				final NetworkReceiveOperator receive = new NetworkReceiveOperator(meta);
				receive.setNode(op.getNode());

				for (final NetworkSendOperator send2 : sends)
				{
					receive.add(send2);
				}
				op.add(receive);
				// cCache.clear();
			}
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.error("", e);
			throw e;
		}
	}

	private void verify2ReceivesForSemi(final Operator op) throws Exception
	{
		try
		{
			if (!(op.children().get(1) instanceof NetworkReceiveOperator))
			{
				final Operator child = op.children().get(1);
				op.removeChild(child);
				final NetworkSendOperator send = new NetworkSendOperator(-1, meta);
				send.add(child);
				final NetworkReceiveOperator receive = new NetworkReceiveOperator(meta);
				receive.setNode(op.getNode());
				receive.add(send);
				op.add(receive);
				// cCache.clear();
			}

			if (!(op.children().get(0) instanceof NetworkReceiveOperator))
			{
				final Operator child = op.children().get(0);
				op.removeChild(child);
				final int ID = id.getAndIncrement();
				final NetworkSendRROperator send = new NetworkSendRROperator(ID, meta);
				send.setNode(child.getNode());
				send.add(child);

				final long numNodes = card(child) / MAX_LOCAL_SORT + 1;
				final int starting = getStartingNode(numNodes);
				final ArrayList<NetworkSendOperator> sends = new ArrayList<NetworkSendOperator>();
				int i = 0;
				while (i < numNodes && starting + i < MetaData.numWorkerNodes)
				{
					final NetworkHashReceiveOperator receive = new NetworkHashReceiveOperator(ID, meta);
					receive.setNode(starting + i);
					receive.add(send);
					final NetworkSendOperator send2 = new NetworkSendOperator(starting + i, meta);
					final ReorderOperator reorder = new ReorderOperator(new ArrayList<String>(receive.getPos2Col().values()), meta);
					reorder.setNode(starting + i);
					reorder.add(receive);
					send2.add(reorder);
					sends.add(send2);
					i++;
				}

				final NetworkReceiveOperator receive = new NetworkReceiveOperator(meta);
				receive.setNode(op.getNode());

				for (final NetworkSendOperator send2 : sends)
				{
					receive.add(send2);
				}
				op.add(receive);
				// cCache.clear();
			}
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.error("", e);
			throw e;
		}
	}
}