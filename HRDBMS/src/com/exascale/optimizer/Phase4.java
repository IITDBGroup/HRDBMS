package com.exascale.optimizer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.optimizer.MetaData.PartitionMetaData;
import com.exascale.tables.Transaction;

public final class Phase4
{
	private final RootOperator root;
	private final MetaData meta;
	private final int MAX_INCOMING_CONNECTIONS = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("max_neighbor_nodes")); // 100
	private static final int MAX_LOCAL_NO_HASH_PRODUCT = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("max_local_no_hash_product")); // 1000000
	private static final int MAX_LOCAL_LEFT_HASH = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("max_local_left_hash")); // 1000000
	private static final int MIN_LOCAL_LEFT_HASH = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("min_local_left_hash")); // 500000
	private static final int MAX_LOCAL_SORT = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("max_local_sort")); // 1000000
	private static final int MAX_CARD_BEFORE_HASH = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("max_card_before_hash")); // 500000
	private static final int MIN_CARD_BEFORE_HASH = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("min_card_for_hash")); // 250000
	protected static AtomicInteger id = new AtomicInteger(0);
	private final HashSet<Integer> usedNodes = new HashSet<Integer>();
	private Transaction tx;

	private final HashMap<Operator, Long> cCache = new HashMap<Operator, Long>();

	private boolean lt = true;

	public Phase4(RootOperator root, Transaction tx)
	{
		this.root = root;
		this.tx = tx;
		meta = root.getMeta();
	}

	public static void clearOpParents(Operator op)
	{
		if (op instanceof TableScanOperator)
		{
			((TableScanOperator)op).clearOpParents();
		}
		else
		{
			for (final Operator o : op.children())
			{
				clearOpParents(o);
			}
		}
	}

	public long card(Operator op) throws Exception
	{
		final Long r = cCache.get(op);
		if (r != null)
		{
			return r;
		}

		if (op instanceof AntiJoinOperator)
		{
			final long retval = (long)((1 - meta.likelihood(((AntiJoinOperator)op).getHSHM(), root, tx, op)) * card(op.children().get(0)));
			cCache.put(op, retval);
			return retval;
		}

		if (op instanceof CaseOperator)
		{
			final long retval = card(op.children().get(0));
			cCache.put(op, retval);
			return retval;
		}
		
		if (op instanceof ConcatOperator)
		{
			final long retval = card(op.children().get(0));
			cCache.put(op, retval);
			return retval;
		}
		
		if (op instanceof DateMathOperator)
		{
			final long retval = card(op.children().get(0));
			cCache.put(op, retval);
			return retval;
		}
		
		if (op instanceof ExceptOperator)
		{
			long card = card(op.children().get(0));
			cCache.put(op, card);
			return card;
		}

		if (op instanceof ExtendOperator)
		{
			final long retval = card(op.children().get(0));
			cCache.put(op, retval);
			return retval;
		}
		
		if (op instanceof ExtendObjectOperator)
		{
			final long retval = card(op.children().get(0));
			cCache.put(op,  retval);
			return retval;
		}

		if (op instanceof HashJoinOperator)
		{
			final long retval = (long)(card(op.children().get(0)) * card(op.children().get(1)) * meta.likelihood(((HashJoinOperator)op).getHSHM(), root, tx, op));
			cCache.put(op, retval);
			return retval;
		}
		
		if (op instanceof IntersectOperator)
		{
			long lCard = card(op.children().get(0));
			long rCard = card(op.children().get(1));
			
			if (lCard <= rCard)
			{
				cCache.put(op, lCard);
				return lCard;
			}
			else
			{
				cCache.put(op, rCard);
				return rCard;
			}
		}

		if (op instanceof MultiOperator)
		{
			// return card(op.children().get(0));
			final long groupCard = meta.getColgroupCard(((MultiOperator)op).getKeys(), root, tx, op);
			if (groupCard > card(op.children().get(0)))
			{
				final long retval = card(op.children().get(0));
				cCache.put(op, retval);
				return retval;
			}

			final long retval = groupCard;
			cCache.put(op, retval);
			return retval;
		}

		if (op instanceof NestedLoopJoinOperator)
		{
			final long retval = (long)(card(op.children().get(0)) * card(op.children().get(1)) * meta.likelihood(((NestedLoopJoinOperator)op).getHSHM(), root, tx, op));
			cCache.put(op, retval);
			return retval;
		}

		if (op instanceof NetworkReceiveOperator)
		{
			long retval = 0;
			for (final Operator o : op.children())
			{
				retval += card(o);
			}

			cCache.put(op, retval);
			return retval;
		}

		if (op instanceof NetworkHashAndSendOperator)
		{
			final long retval = card(op.children().get(0)) / ((NetworkHashAndSendOperator)op).parents().size();
			cCache.put(op, retval);
			return retval;
		}

		if (op instanceof NetworkSendRROperator)
		{
			final long retval = card(op.children().get(0)) / ((NetworkSendRROperator)op).parents().size();
			cCache.put(op, retval);
			return retval;
		}

		if (op instanceof NetworkSendOperator)
		{
			final long retval = card(op.children().get(0));
			cCache.put(op, retval);
			return retval;
		}

		if (op instanceof ProductOperator)
		{
			final long retval = card(op.children().get(0)) * card(op.children().get(1));
			cCache.put(op, retval);
			return retval;
		}

		if (op instanceof ProjectOperator)
		{
			final long retval = card(op.children().get(0));
			cCache.put(op, retval);
			return retval;
		}

		if (op instanceof RenameOperator)
		{
			final long retval = card(op.children().get(0));
			cCache.put(op, retval);
			return retval;
		}

		if (op instanceof ReorderOperator)
		{
			final long retval = card(op.children().get(0));
			return retval;
		}

		if (op instanceof RootOperator)
		{
			final long retval = card(op.children().get(0));
			return retval;
		}

		if (op instanceof SelectOperator)
		{
			final long retval = (long)(((SelectOperator)op).likelihood(root, tx) * card(op.children().get(0)));
			cCache.put(op, retval);
			return retval;
		}

		if (op instanceof SemiJoinOperator)
		{
			final long retval = (long)(meta.likelihood(((SemiJoinOperator)op).getHSHM(), root, tx, op) * card(op.children().get(0)));
			cCache.put(op, retval);
			return retval;
		}

		if (op instanceof SortOperator)
		{
			final long retval = card(op.children().get(0));
			return retval;
		}

		if (op instanceof SubstringOperator)
		{
			final long retval = card(op.children().get(0));
			return retval;
		}

		if (op instanceof TopOperator)
		{
			final long retval = ((TopOperator)op).getRemaining();
			final long retval2 = card(op.children().get(0));

			if (retval2 < retval)
			{
				cCache.put(op, retval2);
				return retval2;
			}
			else
			{
				cCache.put(op, retval);
				return retval;
			}
		}

		if (op instanceof UnionOperator)
		{
			long retval = 0;
			for (final Operator o : op.children())
			{
				retval += card(o);
			}

			cCache.put(op, retval);
			return retval;
		}

		if (op instanceof YearOperator)
		{
			final long retval = card(op.children().get(0));
			cCache.put(op, retval);
			return retval;
		}

		if (op instanceof TableScanOperator)
		{
			final HashSet<HashMap<Filter, Filter>> hshm = ((TableScanOperator)op).getHSHM();
			if (hshm != null)
			{
				final long retval = (long)(meta.getTableCard(((TableScanOperator)op).getSchema(), ((TableScanOperator)op).getTable(), tx) * meta.likelihood(hshm, root, tx, op) * (1.0 / ((TableScanOperator)op).getNumNodes()));
				cCache.put(op, retval);
				return retval;
			}

			final long retval = (long)((1.0 / ((TableScanOperator)op).getNumNodes()) * meta.getTableCard(((TableScanOperator)op).getSchema(), ((TableScanOperator)op).getTable(), tx));
			cCache.put(op, retval);
			return retval;
		}

		HRDBMSWorker.logger.error("Unknown operator in card() in Phase4: " + op.getClass());
		throw new Exception("Unknown operator in card() in Phase4: " + op.getClass());
	}

	public void optimize() throws Exception
	{
		if (meta.getNumNodes(tx) > 1)
		{
			pushUpReceives();
			redistributeSorts();
			clearOpParents(root);
			cleanupOrderedFilters(root);
		}
	}

	private void checkOrder(Operator op) throws Exception
	{
		final Operator parent = op;
		final Operator left = parent.children().get(0);
		final Operator right = parent.children().get(1);
		if (card(left) > card(right))
		{
			// switch
			try
			{
				parent.removeChild(right);
				parent.add(left);
				parent.removeChild(left);
				parent.add(right);
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				throw e;
			}

			Operator p = parent.parent();
			while (!(p instanceof RootOperator))
			{
				for (final Operator o : (ArrayList<Operator>)p.children().clone())
				{
					p.removeChild(o);
					try
					{
						p.add(o);
					}
					catch (final Exception e)
					{
						HRDBMSWorker.logger.error("", e);
						throw e;
					}
				}

				p = p.parent();
			}

			cCache.clear();
		}
	}

	private void cleanupOrderedFilters(Operator op)
	{
		if (op instanceof TableScanOperator)
		{
			((TableScanOperator)op).cleanupOrderedFilters();
		}
		else
		{
			for (final Operator o : op.children())
			{
				cleanupOrderedFilters(o);
			}
		}
	}

	private Operator cloneTree(Operator op, int level) throws Exception
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

	private long concatPath(Operator op)
	{
		long retval = 0;
		long i = 0;
		int shift = 0;
		while (!(op instanceof RootOperator))
		{
			for (final Operator o : op.parent().children())
			{
				if (o == op)
				{
					retval += (i << shift);
					shift += 7;
					break;
				}

				i++;
			}

			op = op.parent();
		}

		return retval;
	}

	private boolean doHashAnti(NetworkReceiveOperator receive) throws Exception
	{
		final Operator parent = receive.parent();
		verify2ReceivesForHash(parent);
		final Operator grandParent = parent.parent();
		long card = card(parent.children().get(0)) + card(parent.children().get(1));
		ArrayList<String> join = ((AntiJoinOperator)parent).getJoinForChild(receive);
		parent.removeChild(receive);
		grandParent.removeChild(parent);
		ArrayList<Operator> sends = new ArrayList<Operator>(receive.children().size());
		int starting = getStartingNode(card / MIN_LOCAL_LEFT_HASH + 1);
		int ID = id.getAndIncrement();
		Operator other = null;
		for (final Operator o : parent.children())
		{
			if (o != receive)
			{
				other = o;
			}
		}
		String t = meta.getTableForCol(((AntiJoinOperator)parent).getJoinForChild(other).get(0), parent);
		String schema = t.substring(0, t.indexOf('.'));
		t = t.substring(t.indexOf('.') + 1);
		PartitionMetaData pmeta = null;
		if (t != null)
		{
			pmeta = meta.getPartMeta(schema, t, tx);
		}

		if (pmeta != null && isLocal(other.children().get(0).children().get(0)) && pmeta.noNodeGroupSet() && pmeta.nodeIsHash() && join.containsAll(pmeta.getNodeHash()) && pmeta.allNodes())
		{
			starting = 0;
			card = Long.MAX_VALUE;
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
			t = meta.getTableForCol(join.get(0), grandChild);
			schema = t.substring(0, t.indexOf('.'));
			t = t.substring(t.indexOf('.') + 1);
			pmeta = null;
			if (t != null)
			{
				pmeta = meta.getPartMeta(schema, t, tx);
			}

			if (pmeta != null && isLocal(grandChild) && pmeta.noNodeGroupSet() && pmeta.nodeIsHash() && join.containsAll(pmeta.getNodeHash()) && pmeta.allNodes())
			{
				starting = 0;
				card = Long.MAX_VALUE;
			}

			final NetworkHashAndSendOperator send = new NetworkHashAndSendOperator(join, card / MIN_LOCAL_LEFT_HASH + 1, ID, starting, meta, tx);
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
		while (i < (card / MIN_LOCAL_LEFT_HASH + 1) + starting && i < meta.getNumNodes(tx))
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

			final NetworkHashAndSendOperator send = new NetworkHashAndSendOperator(join, card / MIN_LOCAL_LEFT_HASH + 1, ID, starting, meta, tx);
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
		while (i < (card / MIN_LOCAL_LEFT_HASH + 1) + starting && i < meta.getNumNodes(tx))
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
		r.setNode(-1);
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
		makeHierarchical2(r);
		makeHierarchical(r);
		cCache.clear();
		return false;
	}

	private boolean doHashSemi(NetworkReceiveOperator receive) throws Exception
	{
		final Operator parent = receive.parent();
		verify2ReceivesForHash(parent);
		final Operator grandParent = parent.parent();
		long card = card(parent.children().get(0)) + card(parent.children().get(1));
		ArrayList<String> join = ((SemiJoinOperator)parent).getJoinForChild(receive);
		parent.removeChild(receive);
		grandParent.removeChild(parent);
		ArrayList<Operator> sends = new ArrayList<Operator>(receive.children().size());
		int starting = getStartingNode(card / MIN_LOCAL_LEFT_HASH + 1);
		int ID = id.getAndIncrement();
		Operator other = null;
		for (final Operator o : parent.children())
		{
			if (o != receive)
			{
				other = o;
			}
		}
		String t = meta.getTableForCol(((SemiJoinOperator)parent).getJoinForChild(other).get(0), other);
		String schema = t.substring(0, t.indexOf('.'));
		t = t.substring(t.indexOf('.') + 1);
		PartitionMetaData pmeta = null;
		if (t != null)
		{
			pmeta = meta.getPartMeta(schema, t, tx);
		}

		if (pmeta != null && isLocal(other.children().get(0).children().get(0)) && pmeta.noNodeGroupSet() && pmeta.nodeIsHash() && join.containsAll(pmeta.getNodeHash()) && pmeta.allNodes())
		{
			starting = 0;
			card = Long.MAX_VALUE;
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
			t = meta.getTableForCol(join.get(0), grandChild);
			schema = t.substring(0, t.indexOf('.'));
			t = t.substring(t.indexOf('.') + 1);
			pmeta = null;
			if (t != null)
			{
				pmeta = meta.getPartMeta(schema, t, tx);
			}

			if (pmeta != null && isLocal(grandChild) && pmeta.noNodeGroupSet() && pmeta.nodeIsHash() && join.containsAll(pmeta.getNodeHash()) && pmeta.allNodes())
			{
				starting = 0;
				card = Long.MAX_VALUE;
			}

			final NetworkHashAndSendOperator send = new NetworkHashAndSendOperator(join, card / MIN_LOCAL_LEFT_HASH + 1, ID, starting, meta, tx);
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
		while (i < (card / MIN_LOCAL_LEFT_HASH + 1) + starting && i < meta.getNumNodes(tx))
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

			final NetworkHashAndSendOperator send = new NetworkHashAndSendOperator(join, card / MIN_LOCAL_LEFT_HASH + 1, ID, starting, meta, tx);
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
		while (i < (card / MIN_LOCAL_LEFT_HASH + 1) + starting && i < meta.getNumNodes(tx))
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
		r.setNode(-1);
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
		makeHierarchical2(r);
		makeHierarchical(r);
		cCache.clear();
		return false;
	}

	private boolean doNonHashSemi(NetworkReceiveOperator receive) throws Exception
	{
		final Operator parent = receive.parent();
		verify2ReceivesForSemi(parent);
		final Operator right = parent.children().get(0);
		final Operator left = parent.children().get(1);

		if (isAllAny(left))
		{
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

			final ArrayList<NetworkSendOperator> sends2 = new ArrayList<NetworkSendOperator>(grandChildren.size());
			for (final Operator grandChild : grandChildren)
			{
				final Operator clone = cloneTree(parent, 0);
				clone.setNode(grandChild.getNode());
				final Operator leftClone = fullyCloneTree(left.children().get(0).children().get(0));
				setNodeForTree(leftClone, grandChild.getNode());
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
			r.setNode(-1);

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

			makeHierarchical(r);
			cCache.clear();
			return false;
		}

		// send left (right) data to nodes and perform product there
		boolean parentUsesSort = false;
		ArrayList<String> sortKeys = null;
		ArrayList<Boolean> orders = null;
		if (parent instanceof SemiJoinOperator)
		{
			if (((SemiJoinOperator)parent).usesSort())
			{
				parentUsesSort = true;
				((SemiJoinOperator)parent).alreadySorted();
				sortKeys = ((SemiJoinOperator)parent).sortKeys();
				orders = ((SemiJoinOperator)parent).sortOrders();
			}
		}

		if (parent instanceof AntiJoinOperator)
		{
			if (((AntiJoinOperator)parent).usesSort())
			{
				parentUsesSort = true;
				((AntiJoinOperator)parent).alreadySorted();
				sortKeys = ((AntiJoinOperator)parent).sortKeys();
				orders = ((AntiJoinOperator)parent).sortOrders();
			}
		}
		final Operator grandParent = parent.parent();
		parent.removeChild(left);
		grandParent.removeChild(parent);
		final ArrayList<NetworkSendMultipleOperator> sends = new ArrayList<NetworkSendMultipleOperator>(left.children().size());
		final int ID = id.getAndIncrement();
		for (final Operator child : (ArrayList<Operator>)left.children().clone())
		{
			final int node = child.getNode();
			final Operator grandChild = child.children().get(0);
			CNFFilter cnf = null;
			if (grandChild instanceof TableScanOperator)
			{
				cnf = ((TableScanOperator)grandChild).getCNFForParent(child);
			}
			child.removeChild(grandChild);
			final NetworkSendMultipleOperator send = new NetworkSendMultipleOperator(ID, meta);
			try
			{
				if (!parentUsesSort)
				{
					send.add(grandChild);
					if (cnf != null)
					{
						((TableScanOperator)grandChild).setCNFForParent(send, cnf);
					}
				}
				else
				{
					final SortOperator sort = new SortOperator(sortKeys, orders, meta);
					sort.add(grandChild);
					sort.setNode(node);
					if (cnf != null)
					{
						((TableScanOperator)grandChild).setCNFForParent(sort, cnf);
					}
					send.add(sort);
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

		final ArrayList<NetworkReceiveOperator> receives = new ArrayList<NetworkReceiveOperator>(grandChildren.size());
		for (final Operator grandChild : grandChildren)
		{
			final Operator clone = cloneTree(parent, 0);
			clone.setNode(grandChild.getNode());
			NetworkReceiveOperator receive2 = null;
			if (!parentUsesSort)
			{
				receive2 = new NetworkHashReceiveOperator(ID, meta);
			}
			else
			{
				receive2 = new NetworkHashReceiveAndMergeOperator(ID, sortKeys, orders, meta);
			}

			try
			{
				clone.add(receive2);
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				throw e;
			}

			receive2.setNode(grandChild.getNode());
			receives.add(receive2);
			for (final Operator o : (ArrayList<Operator>)clone.children().clone())
			{
				if (!o.equals(receive2))
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
		}

		final ArrayList<NetworkSendOperator> sends2 = new ArrayList<NetworkSendOperator>(receives.size());
		for (final NetworkReceiveOperator receive2 : receives)
		{
			for (final NetworkSendMultipleOperator send : sends)
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

			final NetworkSendOperator send2 = new NetworkSendOperator(receive2.getNode(), meta);
			send2.setNode(receive2.getNode());
			try
			{
				send2.add(receive2.parent());
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				throw e;
			}

			sends2.add(send2);
		}

		final NetworkReceiveOperator r = new NetworkReceiveOperator(meta);
		r.setNode(-1);

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

		for (final NetworkReceiveOperator receive2 : receives)
		{
			Operator p = receive2.parent();
			Operator c = receive2;
			while (!(p instanceof RootOperator))
			{
				p.removeChild(c);
				try
				{
					p.add(c);
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.error("", e);
					throw e;
				}

				c = p;
				p = p.parent();
			}
		}

		makeHierarchical3(r);
		makeHierarchical(r);
		cCache.clear();
		return false;
	}

	private void doSortRedistribution(SortOperator op) throws Exception
	{
		final long numNodes = card(op) / MAX_LOCAL_SORT;
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
		rr.setNode(-1);

		final ArrayList<NetworkSendOperator> sends = new ArrayList<NetworkSendOperator>();
		int i = 0;
		while (i < numNodes && starting + i < meta.getNumNodes(tx))
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

		final NetworkReceiveAndMergeOperator receive = new NetworkReceiveAndMergeOperator(op.getKeys(), op.getOrders(), meta);
		receive.setNode(-1);
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
		makeHierarchical4(receive);
		if (parent instanceof TopOperator)
		{
			handleTop(receive);
		}
		makeHierarchical(receive);
		cCache.clear();
	}

	private Operator fullyCloneTree(Operator op) throws Exception
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

	private ArrayList<Operator> getGrandChildren(Operator op)
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

	private SortOperator getLocalSort(Operator op) throws Exception
	{
		SortOperator retval = null;
		if (op instanceof SortOperator)
		{
			if (op.getNode() == -1)
			{
				if (retval == null)
				{
					retval = (SortOperator)op;
				}
				else
				{
					Exception e = new Exception("Found more than 1 sort on the coord node!");
					HRDBMSWorker.logger.error("Found more than 1 sort on the coord node!", e);
					throw e;
				}
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
						Exception e = new Exception("Found more than 1 sort on the coord node!");
						HRDBMSWorker.logger.error("Found more than 1 sort on the coord node!", e);
						throw e;
					}
				}
			}
		}

		return retval;
	}

	private HashMap<NetworkReceiveOperator, Integer> getReceives(Operator op, int level)
	{
		final HashMap<NetworkReceiveOperator, Integer> retval = new HashMap<NetworkReceiveOperator, Integer>();
		if (!(op instanceof NetworkReceiveOperator))
		{
			if (op.getNode() == -1)
			{
				for (final Operator child : op.children())
				{
					retval.putAll(getReceives(child, level + 1));
				}
			}

			return retval;
		}
		else
		{
			if (op.getNode() == -1)
			{
				retval.put((NetworkReceiveOperator)op, level);
				for (final Operator child : op.children())
				{
					retval.putAll(getReceives(child, level + 1));
				}
			}

			return retval;
		}
	}

	private int getStartingNode(long numNodes) throws Exception
	{
		if (numNodes >= meta.getNumNodes(tx))
		{
			return 0;
		}

		final int range = (int)(meta.getNumNodes(tx) - numNodes);
		return (int)(Math.random() * range);
	}

	private boolean handleMulti(NetworkReceiveOperator receive) throws Exception
	{
		if (receive.children().size() == 1)
		{
			pushAcross(receive);
			return true;
		}
		final MultiOperator parent = (MultiOperator)receive.parent();
		final long card = card(parent);
		if (card > MAX_CARD_BEFORE_HASH)
		{
			final ArrayList<String> cols2 = new ArrayList<String>(parent.getKeys());
			final int starting = getStartingNode(card / MIN_CARD_BEFORE_HASH);
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

				final NetworkHashAndSendOperator send = new NetworkHashAndSendOperator(cols2, card / MIN_CARD_BEFORE_HASH, ID, starting, meta, tx);
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
			while (i < card / MIN_CARD_BEFORE_HASH && i < meta.getNumNodes(tx))
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
			cCache.clear();
			return true;
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

		final int oldSuffix = Phase3.colSuffix;
		final Operator orig = parent.parent();
		MultiOperator pClone;
		final ArrayList<String> cols = new ArrayList<String>(parent.getPos2Col().values());
		ArrayList<String> oldCols = null;
		ArrayList<String> newCols = null;

		for (final Map.Entry entry : send2Child.entrySet())
		{
			pClone = parent.clone();
			pClone.removeCountDistinct();
			if (pClone.getOutputCols().size() == 0)
			{
				pClone.addCount("_P" + Phase3.colSuffix++);
			}
			while (pClone.hasAvg())
			{
				final String avgCol = pClone.getAvgCol();
				final ArrayList<String> newCols2 = new ArrayList<String>(2);
				final String newCol1 = "_P" + Phase3.colSuffix++;
				final String newCol2 = "_P" + Phase3.colSuffix++;
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
					grandParent.add(extend);
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.error("", e);
					throw e;
				}
			}

			Phase3.colSuffix = oldSuffix;
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
				if (send2CNF.containsKey(entry.getKey()))
				{
					((TableScanOperator)entry.getValue()).setCNFForParent(pClone, send2CNF.get(entry.getKey()));
				}
				RenameOperator rename = null;
				rename = new RenameOperator(oldCols, newCols, meta);
				rename.add(pClone);

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
		cCache.clear();
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
		receive = new NetworkReceiveAndMergeOperator(parent.getKeys(), parent.getOrders(), meta);

		try
		{
			for (final Map.Entry entry : send2Child.entrySet())
			{
				final Operator pClone = parent.clone();
				pClone.add((Operator)entry.getValue());
				if (send2CNF.containsKey(entry.getKey()))
				{
					((TableScanOperator)entry.getValue()).setCNFForParent(pClone, send2CNF.get(entry.getKey()));
				}
				((Operator)entry.getKey()).add(pClone);
				receive.add((Operator)entry.getKey());
			}
			grandParent.add(receive);
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.error("", e);
			throw e;
		}

		cCache.clear();
		return false;
	}

	private boolean handleTop(NetworkReceiveOperator receive) throws Exception
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

		cCache.clear();
		return false;
	}

	private boolean isAllAny(Operator op)
	{
		if (op instanceof TableScanOperator)
		{
			return ((TableScanOperator)op).anyNode();
		}
		else
		{
			for (final Operator o : op.children())
			{
				if (!isAllAny(o))
				{
					return false;
				}
			}

			return true;
		}
	}

	private boolean isLocal(Operator op)
	{
		if (op instanceof NetworkSendOperator)
		{
			return false;
		}

		for (final Operator o : op.children())
		{
			if (!isLocal(o))
			{
				return false;
			}
		}

		return true;
	}

	private void makeHierarchical(NetworkReceiveOperator receive) throws Exception
	{
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

			NetworkReceiveOperator newReceive = new NetworkReceiveOperator(meta);
			int i = 0;
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
					int node = Math.abs(ThreadLocalRandom.current().nextInt()) % meta.getNumNodes(tx);
					while (usedNodes.contains(node))
					{
						node = Math.abs(ThreadLocalRandom.current().nextInt()) % meta.getNumNodes(tx);
					}
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
					newReceive = new NetworkReceiveOperator(meta);
					i = 0;
				}
			}
			makeHierarchical(receive);
			cCache.clear();
		}
	}

	private void makeHierarchical2(NetworkReceiveOperator op) throws Exception
	{
		// NetworkHashAndSend -> NetworkHashReceive
		ArrayList<NetworkHashReceiveOperator> lreceives = new ArrayList<NetworkHashReceiveOperator>();
		ArrayList<NetworkHashReceiveOperator> lreceives2 = new ArrayList<NetworkHashReceiveOperator>();
		ArrayList<NetworkHashReceiveOperator> rreceives = new ArrayList<NetworkHashReceiveOperator>();
		ArrayList<NetworkHashReceiveOperator> rreceives2 = new ArrayList<NetworkHashReceiveOperator>();
		for (final Operator child : op.children())
		{
			if (child.children().get(0).children().get(0) instanceof NetworkHashReceiveOperator)
			{
				lreceives.add((NetworkHashReceiveOperator)child.children().get(0).children().get(0));
			}
			if (child.children().get(0).children().get(1) instanceof NetworkHashReceiveOperator)
			{
				rreceives.add((NetworkHashReceiveOperator)child.children().get(0).children().get(1));
			}
		}

		boolean dol = true;
		boolean dor = true;
		if (lreceives.size() == 0)
		{
			dol = false;
		}
		if (rreceives.size() == 0)
		{
			dor = false;
		}

		final ArrayList<NetworkHashAndSendOperator> lsends = new ArrayList<NetworkHashAndSendOperator>(lreceives.get(0).children().size());
		if (dol)
		{
			for (final Operator o : lreceives.get(0).children())
			{
				lsends.add((NetworkHashAndSendOperator)o);
			}
		}

		final ArrayList<NetworkHashAndSendOperator> rsends = new ArrayList<NetworkHashAndSendOperator>(rreceives.get(0).children().size());
		if (dor)
		{
			for (final Operator o : rreceives.get(0).children())
			{
				rsends.add((NetworkHashAndSendOperator)o);
			}
		}

		ArrayList<NetworkHashReceiveOperator> generic = null;
		if (dol)
		{
			generic = lreceives;
		}
		else
		{
			generic = rreceives;
		}
		while (generic.size() > Phase3.MAX_INCOMING_CONNECTIONS)
		{
			int numMiddle = generic.size() / Phase3.MAX_INCOMING_CONNECTIONS;
			if (generic.size() % Phase3.MAX_INCOMING_CONNECTIONS != 0)
			{
				numMiddle++;
			}
			int numPerMiddle = generic.size() / numMiddle;
			if (generic.size() % numMiddle != 0)
			{
				numPerMiddle++;
			}

			final int lstarting = getStartingNode(numMiddle);
			final int rstarting = getStartingNode(numMiddle);
			NetworkHashReceiveOperator newLReceive = null;
			NetworkHashReceiveOperator newRReceive = null;
			if (dol)
			{
				newLReceive = new NetworkHashReceiveOperator(lsends.get(0).getID(), meta);
				newLReceive.setNode(lstarting);
			}
			if (dor)
			{
				newRReceive = new NetworkHashReceiveOperator(rsends.get(0).getID(), meta);
				newRReceive.setNode(rstarting);
			}
			if (dol)
			{
				lreceives2.add(newLReceive);
			}
			if (dor)
			{
				rreceives2.add(newRReceive);
			}
			int i = 1;
			if (dol)
			{
				for (final Operator rr : lsends)
				{
					try
					{
						newLReceive.add(rr);
					}
					catch (final Exception e)
					{
						HRDBMSWorker.logger.error("", e);
						throw e;
					}
				}
			}
			if (dor)
			{
				for (final Operator rr : rsends)
				{
					try
					{
						newRReceive.add(rr);
					}
					catch (final Exception e)
					{
						HRDBMSWorker.logger.error("", e);
						throw e;
					}
				}
			}
			int newLID = id.getAndIncrement();
			int newRID = id.getAndIncrement();
			NetworkHashAndSendOperator newLRR = null;
			NetworkHashAndSendOperator newRRR = null;
			if (dol)
			{
				newLRR = lsends.get(0).clone();
				newLRR.setID(newLID);
			}
			if (dor)
			{
				newRRR = rsends.get(0).clone();
				newRRR.setID(newRID);
			}
			if (dol)
			{
				newLRR.setNode(lstarting);
			}
			if (dor)
			{
				newRRR.setNode(rstarting);
			}

			try
			{
				if (dol)
				{
					newLRR.add(newLReceive);
				}
				if (dor)
				{
					newRRR.add(newRReceive);
				}
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				throw e;
			}

			int j = 0;
			int a = 0;
			for (final NetworkHashReceiveOperator lreceive : generic)
			{
				final NetworkHashReceiveOperator rreceive = rreceives.get(j);
				if (dol)
				{
					lreceive.setNode(newLRR.getStarting() + a + j * numMiddle);
					setNodeUpward(lreceive);
				}
				if (dor)
				{
					rreceive.setNode(newRRR.getStarting() + a + j * numMiddle);
					setNodeUpward(rreceive);
				}
				if (dol)
				{
					for (final Operator child : (ArrayList<Operator>)lreceive.children().clone())
					{
						lreceive.removeChild(child);
					}
				}
				if (dor)
				{
					for (final Operator child : (ArrayList<Operator>)rreceive.children().clone())
					{
						rreceive.removeChild(child);
					}
				}
				try
				{
					if (dol)
					{
						lreceive.add(newLRR);
					}
					if (dor)
					{
						rreceive.add(newRRR);
					}
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.error("", e);
					throw e;
				}

				if (dol)
				{
					lreceive.setID(newLID);
				}
				if (dor)
				{
					rreceive.setID(newRID);
				}
				j++;

				if (j == numPerMiddle)
				{
					if (dol)
					{
						newLReceive = new NetworkHashReceiveOperator(lsends.get(0).getID(), meta);
						newLReceive.setNode(lstarting + i);
					}
					if (dor)
					{
						newRReceive = new NetworkHashReceiveOperator(rsends.get(0).getID(), meta);
						newRReceive.setNode(rstarting + i);
					}
					if (dol)
					{
						lreceives2.add(newLReceive);
					}
					if (dor)
					{
						rreceives2.add(newRReceive);
					}
					if (dol)
					{
						for (final Operator rr : lsends)
						{
							try
							{
								newLReceive.add(rr);
							}
							catch (final Exception e)
							{
								HRDBMSWorker.logger.error("", e);
								throw e;
							}
						}
					}
					if (dor)
					{
						for (final Operator rr : rsends)
						{
							try
							{
								newRReceive.add(rr);
							}
							catch (final Exception e)
							{
								HRDBMSWorker.logger.error("", e);
								throw e;
							}
						}
					}

					newLID = id.getAndIncrement();
					newRID = id.getAndIncrement();
					if (dol)
					{
						newLRR = lsends.get(0).clone();
						newLRR.setID(newLID);
					}
					if (dor)
					{
						newRRR = rsends.get(0).clone();
						newRRR.setID(newRID);
					}
					if (dol)
					{
						newLRR.setNode(lstarting + i);
					}
					if (dor)
					{
						newRRR.setNode(rstarting + i);
					}
					i++;

					try
					{
						if (dol)
						{
							newLRR.add(newLReceive);
						}
						if (dor)
						{
							newRRR.add(newRReceive);
						}
					}
					catch (final Exception e)
					{
						HRDBMSWorker.logger.error("", e);
						throw e;
					}

					j = 0;
					a++;
				}
			}

			if (dol)
			{
				for (final NetworkHashAndSendOperator rr : lsends)
				{
					rr.setStarting(lstarting);
					rr.setNumNodes(numMiddle);
				}
			}
			if (dor)
			{
				for (final NetworkHashAndSendOperator rr : rsends)
				{
					rr.setStarting(rstarting);
					rr.setNumNodes(numMiddle);
				}
			}

			lreceives = lreceives2;
			rreceives = rreceives2;
			lreceives2 = new ArrayList<NetworkHashReceiveOperator>();
			rreceives2 = new ArrayList<NetworkHashReceiveOperator>();
		}
		cCache.clear();
	}

	private void makeHierarchical3(NetworkReceiveOperator op) throws Exception
	{
		// NetworkSendMultiple -> NetworkHashReceive
		ArrayList<NetworkHashReceiveOperator> receives = new ArrayList<NetworkHashReceiveOperator>();
		ArrayList<NetworkHashReceiveOperator> receives2 = new ArrayList<NetworkHashReceiveOperator>();
		for (final Operator child : op.children())
		{
			for (final Operator o : child.children().get(0).children())
			{
				if (o instanceof NetworkHashReceiveOperator)
				{
					receives.add((NetworkHashReceiveOperator)o);
				}
			}
		}

		final ArrayList<NetworkSendMultipleOperator> sends = new ArrayList<NetworkSendMultipleOperator>(receives.get(0).children().size());
		for (final Operator o : receives.get(0).children())
		{
			sends.add((NetworkSendMultipleOperator)o);
		}

		while (receives.size() > Phase3.MAX_INCOMING_CONNECTIONS)
		{
			int numMiddle = receives.size() / Phase3.MAX_INCOMING_CONNECTIONS;
			if (receives.size() % Phase3.MAX_INCOMING_CONNECTIONS != 0)
			{
				numMiddle++;
			}
			int numPerMiddle = receives.size() / numMiddle;
			if (receives.size() % numMiddle != 0)
			{
				numPerMiddle++;
			}

			NetworkHashReceiveOperator newReceive = new NetworkHashReceiveOperator(sends.get(0).getID(), meta);
			int node = Math.abs(ThreadLocalRandom.current().nextInt()) % meta.getNumNodes(tx);
			while (usedNodes.contains(node))
			{
				node = Math.abs(ThreadLocalRandom.current().nextInt()) % meta.getNumNodes(tx);
			}
			newReceive.setNode(node);
			receives2.add(newReceive);
			for (final Operator rr : sends)
			{
				try
				{
					newReceive.add(rr);
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.error("", e);
					throw e;
				}
			}

			int newID = id.getAndIncrement();
			NetworkSendMultipleOperator newRR = new NetworkSendMultipleOperator(newID, meta);
			newRR.setNode(node);

			try
			{
				newRR.add(newReceive);
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				throw e;
			}

			int j = 0;
			for (final NetworkHashReceiveOperator receive : receives)
			{
				for (final Operator child : (ArrayList<Operator>)receive.children().clone())
				{
					receive.removeChild(child);
				}
				try
				{
					receive.add(newRR);
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.error("", e);
					throw e;
				}

				receive.setID(newID);
				j++;

				if (j == numPerMiddle)
				{
					newReceive = new NetworkHashReceiveOperator(sends.get(0).getID(), meta);
					node = Math.abs(ThreadLocalRandom.current().nextInt()) % meta.getNumNodes(tx);
					while (usedNodes.contains(node))
					{
						node = Math.abs(ThreadLocalRandom.current().nextInt()) % meta.getNumNodes(tx);
					}
					newReceive.setNode(node);
					receives2.add(newReceive);
					for (final Operator rr : sends)
					{
						try
						{
							newReceive.add(rr);
						}
						catch (final Exception e)
						{
							HRDBMSWorker.logger.error("", e);
							throw e;
						}
					}

					newID = id.getAndIncrement();
					newRR = new NetworkSendMultipleOperator(newID, meta);
					newRR.setNode(node);

					try
					{
						newRR.add(newReceive);
					}
					catch (final Exception e)
					{
						HRDBMSWorker.logger.error("", e);
						throw e;
					}

					j = 0;
				}
			}

			receives = receives2;
			receives2 = new ArrayList<NetworkHashReceiveOperator>();
		}

		cCache.clear();
	}

	private void makeHierarchical4(NetworkReceiveOperator op) throws Exception
	{
		// NetworkSendRR -> NetworkHashReceive
		ArrayList<NetworkHashReceiveOperator> receives = new ArrayList<NetworkHashReceiveOperator>();
		ArrayList<NetworkHashReceiveOperator> receives2 = new ArrayList<NetworkHashReceiveOperator>();
		for (final Operator child : op.children())
		{
			receives.add((NetworkHashReceiveOperator)(child.children().get(0).children().get(0)));
		}

		while (receives.size() > Phase3.MAX_INCOMING_CONNECTIONS)
		{
			int numMiddle = receives.size() / Phase3.MAX_INCOMING_CONNECTIONS;
			if (receives.size() % Phase3.MAX_INCOMING_CONNECTIONS != 0)
			{
				numMiddle++;
			}
			int numPerMiddle = receives.size() / numMiddle;
			if (receives.size() % numMiddle != 0)
			{
				numPerMiddle++;
			}

			final NetworkSendRROperator rr = (NetworkSendRROperator)receives.get(0).children().get(0);
			NetworkHashReceiveOperator newReceive = new NetworkHashReceiveOperator(rr.getID(), meta);
			int node = Math.abs(ThreadLocalRandom.current().nextInt()) % meta.getNumNodes(tx);
			while (usedNodes.contains(node))
			{
				node = Math.abs(ThreadLocalRandom.current().nextInt()) % meta.getNumNodes(tx);
			}
			newReceive.setNode(node);
			receives2.add(newReceive);
			try
			{
				newReceive.add(rr);
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				throw e;
			}

			int newID = id.getAndIncrement();
			NetworkSendRROperator newRR = new NetworkSendRROperator(newID, meta);
			newRR.setNode(node);

			try
			{
				newRR.add(newReceive);
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				throw e;
			}

			int j = 0;
			for (final NetworkHashReceiveOperator receive : receives)
			{
				receive.removeChild(receive.children().get(0));
				try
				{
					receive.add(newRR);
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.error("", e);
					throw e;
				}

				receive.setID(newID);
				j++;

				if (j == numPerMiddle)
				{
					newReceive = new NetworkHashReceiveOperator(rr.getID(), meta);
					node = Math.abs(ThreadLocalRandom.current().nextInt()) % meta.getNumNodes(tx);
					while (usedNodes.contains(node))
					{
						node = Math.abs(ThreadLocalRandom.current().nextInt()) % meta.getNumNodes(tx);
					}
					newReceive.setNode(node);
					try
					{
						newReceive.add(rr);
					}
					catch (final Exception e)
					{
						HRDBMSWorker.logger.error("", e);
						throw e;
					}

					newID = id.getAndIncrement();
					newRR = new NetworkSendRROperator(newID, meta);
					newRR.setNode(node);

					try
					{
						newRR.add(newReceive);
					}
					catch (final Exception e)
					{
						HRDBMSWorker.logger.error("", e);
						throw e;
					}

					j = 0;
				}
			}

			receives = receives2;
			receives2 = new ArrayList<NetworkHashReceiveOperator>();
		}

		cCache.clear();
	}

	private boolean noLargeUpstreamJoins(Operator op) throws Exception
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
				if (l * r <= MAX_LOCAL_NO_HASH_PRODUCT && noLargeUpstreamJoins(o))
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
				if (((NestedLoopJoinOperator)o).usesHash() && card(o.children().get(0)) + card(o.children().get(1)) <= MAX_LOCAL_LEFT_HASH && noLargeUpstreamJoins(o))
				{
					return true;
				}
				else if (((NestedLoopJoinOperator)o).usesSort() && card(o) <= MAX_LOCAL_NO_HASH_PRODUCT && card(o.children().get(1)) <= MAX_LOCAL_SORT && noLargeUpstreamJoins(o))
				{
					return true;
				}
				else if (l * r <= MAX_LOCAL_NO_HASH_PRODUCT && noLargeUpstreamJoins(o))
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
				if (((SemiJoinOperator)o).usesHash() && card(o.children().get(0)) + card(o.children().get(1)) <= MAX_LOCAL_LEFT_HASH && noLargeUpstreamJoins(o))
				{
					return true;
				}
				else if (((SemiJoinOperator)o).usesSort() && card(o) <= MAX_LOCAL_NO_HASH_PRODUCT && card(o.children().get(1)) <= MAX_LOCAL_SORT && noLargeUpstreamJoins(o))
				{
					return true;
				}
				else if (l * r <= MAX_LOCAL_NO_HASH_PRODUCT && noLargeUpstreamJoins(o))
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
				if (((AntiJoinOperator)o).usesHash() && card(o.children().get(0)) + card(o.children().get(1)) <= MAX_LOCAL_LEFT_HASH && noLargeUpstreamJoins(o))
				{
					return true;
				}
				else if (((AntiJoinOperator)o).usesSort() && card(o) <= MAX_LOCAL_NO_HASH_PRODUCT && card(o.children().get(1)) <= MAX_LOCAL_SORT && noLargeUpstreamJoins(o))
				{
					return true;
				}
				else if (l * r <= MAX_LOCAL_NO_HASH_PRODUCT && noLargeUpstreamJoins(o))
				{
					return true;
				}

				return false;
			}

			o = o.parent();
		}

		return true;
	}

	private ArrayList<NetworkReceiveOperator> order(HashMap<NetworkReceiveOperator, Integer> receives)
	{
		final ArrayList<NetworkReceiveOperator> retval = new ArrayList<NetworkReceiveOperator>(receives.size());
		while (receives.size() > 0)
		{
			NetworkReceiveOperator maxReceive = null;
			int maxLevel = Integer.MIN_VALUE;
			long minConcatPath = Long.MAX_VALUE;
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

	private void pushAcross(NetworkReceiveOperator receive) throws Exception
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
		cCache.clear();
	}

	private void pushUpReceives() throws Exception
	{
		final HashSet<NetworkReceiveOperator> completed = new HashSet<NetworkReceiveOperator>();
		final HashSet<NetworkReceiveOperator> eligible = new HashSet<NetworkReceiveOperator>();
		boolean workToDo = true;
		while (workToDo)
		{
			workToDo = false;
			final HashMap<NetworkReceiveOperator, Integer> receives = getReceives(root, 0);
			for (final NetworkReceiveOperator receive : order(receives))
			{
				if (completed.contains(receive))
				{
					continue;
				}
				if (!treeContains(root, receive))
				{
					continue;
				}

				final Operator op = receive.parent();
				if (op instanceof SelectOperator || op instanceof YearOperator || op instanceof SubstringOperator || op instanceof ProjectOperator || op instanceof ExtendOperator || op instanceof RenameOperator || op instanceof ReorderOperator || op instanceof CaseOperator || op instanceof ExtendObjectOperator || op instanceof DateMathOperator || op instanceof ConcatOperator)
				{
					pushAcross(receive);
					workToDo = true;
					break;
				}
				else if (op instanceof SortOperator)
				{
					if (!eligible.contains(op))
					{
						continue;
					}
					if (!handleSort(receive))
					{
						completed.add(receive);
					}
					workToDo = true;
					break;
				}
				else if (op instanceof MultiOperator)
				{
					if (!eligible.contains(op))
					{
						continue;
					}
					if (!handleMulti(receive))
					{
						completed.add(receive);
					}
					workToDo = true;
					break;
				}
				else if (op instanceof ProductOperator)
				{
					final Operator parent = op.parent();
					checkOrder(op);
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

					if (l * r <= MAX_LOCAL_NO_HASH_PRODUCT && noLargeUpstreamJoins(op))
					{
						continue;
					}

					if (!redistributeProduct(receive))
					{
						completed.add(receive);
					}
					eligible.add((NetworkReceiveOperator)parent.children().get(0));
					workToDo = true;
					break;
				}
				else if (op instanceof HashJoinOperator)
				{
					final Operator parent = op.parent();
					if (card(op.children().get(0)) + card(op.children().get(1)) <= MAX_LOCAL_LEFT_HASH && noLargeUpstreamJoins(op))
					{
						continue;
					}

					if (!redistributeHash(receive))
					{
						completed.add(receive);
					}
					eligible.add((NetworkReceiveOperator)parent.children().get(0));
					workToDo = true;
					break;
				}
				else if (op instanceof UnionOperator)
				{
					final Operator parent = op.parent();
					if (card(op.children().get(0)) + card(op.children().get(1)) <= MAX_LOCAL_LEFT_HASH && noLargeUpstreamJoins(op))
					{
						continue;
					}

					if (!handleUnion(receive))
					{
						completed.add(receive);
					}
					eligible.add((NetworkReceiveOperator)parent.children().get(0));
					workToDo = true;
					break;
				}
				else if (op instanceof IntersectOperator)
				{
					final Operator parent = op.parent();
					if (card(op.children().get(0)) + card(op.children().get(1)) <= MAX_LOCAL_LEFT_HASH && noLargeUpstreamJoins(op))
					{
						continue;
					}

					if (!handleExcept(receive))
					{
						completed.add(receive);
					}
					eligible.add((NetworkReceiveOperator)parent.children().get(0));
					workToDo = true;
					break;
				}
				else if (op instanceof ExceptOperator)
				{
					final Operator parent = op.parent();
					if (card(op.children().get(0)) + card(op.children().get(1)) <= MAX_LOCAL_LEFT_HASH && noLargeUpstreamJoins(op))
					{
						continue;
					}

					if (!handleExcept(receive))
					{
						completed.add(receive);
					}
					eligible.add((NetworkReceiveOperator)parent.children().get(0));
					workToDo = true;
					break;
				}
				else if (op instanceof NestedLoopJoinOperator)
				{
					final Operator parent = op.parent();
					checkOrder(op);
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
					if (((NestedLoopJoinOperator)op).usesHash() && card(op.children().get(0)) + card(op.children().get(1)) <= MAX_LOCAL_LEFT_HASH && noLargeUpstreamJoins(op))
					{
						continue;
					}
					else if (((NestedLoopJoinOperator)op).usesSort() && card(op) <= MAX_LOCAL_NO_HASH_PRODUCT && card(op.children().get(1)) <= MAX_LOCAL_SORT && noLargeUpstreamJoins(op))
					{
						continue;
					}
					else if (l * r <= MAX_LOCAL_NO_HASH_PRODUCT && noLargeUpstreamJoins(op))
					{
						continue;
					}

					if (!redistributeNL(receive))
					{
						completed.add(receive);
					}
					eligible.add((NetworkReceiveOperator)parent.children().get(0));
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
					// System.out.println("Attempting to push down SemiJoin");
					final Operator parent = op.parent();
					if (((SemiJoinOperator)op).usesHash() && card(op.children().get(0)) + card(op.children().get(1)) <= MAX_LOCAL_LEFT_HASH && noLargeUpstreamJoins(op))
					{
						// System.out.println("SemiJoin uses partial hash, but is not pushed down because...");
						// System.out.println("Left card = " +
						// card(op.children().get(0)));
						// System.out.println("Right card = " +
						// card(op.children().get(1)));
						continue;
					}
					else if (((SemiJoinOperator)op).usesSort() && card(op) <= MAX_LOCAL_NO_HASH_PRODUCT && card(op.children().get(1)) <= MAX_LOCAL_SORT && noLargeUpstreamJoins(op))
					{
						// System.out.println("SemiJoin uses sort, but is not pushed down because...");
						// System.out.println("Card = " + card(op));
						continue;
					}
					else if (l * r <= MAX_LOCAL_NO_HASH_PRODUCT && noLargeUpstreamJoins(op))
					{
						// System.out.println("SemiJoin uses NL, but is not pushed down because...");
						// System.out.println("Left card = " +
						// card(op.children().get(0)));
						// System.out.println("Right card = " +
						// card(op.children().get(1)));
						continue;
					}

					// System.out.println("SemiJoin is pushed down");
					if (!redistributeSemi(receive))
					{
						completed.add(receive);
					}
					eligible.add((NetworkReceiveOperator)parent.children().get(0));
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
					// System.out.println("Attempting to push down AntiJoin");
					final Operator parent = op.parent();
					if (((AntiJoinOperator)op).usesHash() && card(op.children().get(0)) + card(op.children().get(1)) <= MAX_LOCAL_LEFT_HASH && noLargeUpstreamJoins(op))
					{
						// System.out.println("AntiJoin uses partial hash, but is not pushed down because...");
						// System.out.println("Left card = " +
						// card(op.children().get(0)));
						// System.out.println("Right card = " +
						// card(op.children().get(1)));
						continue;
					}
					else if (((AntiJoinOperator)op).usesSort() && card(op) <= MAX_LOCAL_NO_HASH_PRODUCT && card(op.children().get(1)) <= MAX_LOCAL_SORT && noLargeUpstreamJoins(op))
					{
						// System.out.println("AntiJoin uses sort, but is not pushed down because...");
						// System.out.println("Card = " + card(op));
						continue;
					}
					else if (l * r <= MAX_LOCAL_NO_HASH_PRODUCT && noLargeUpstreamJoins(op))
					{
						// System.out.println("AntiJoin uses NL, but is not pushed down because...");
						// System.out.println("Left card = " +
						// card(op.children().get(0)));
						// System.out.println("Right card = " +
						// card(op.children().get(1)));
						continue;
					}

					// System.out.println("AntiJoin is pushed down");
					if (!redistributeAnti(receive))
					{
						completed.add(receive);
					}
					eligible.add((NetworkReceiveOperator)parent.children().get(0));
					workToDo = true;
					break;
				}
				else if (op instanceof TopOperator)
				{
					if (!eligible.contains(op))
					{
						continue;
					}
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

	private boolean redistributeAnti(NetworkReceiveOperator receive) throws Exception
	{
		if (((AntiJoinOperator)receive.parent()).usesHash())
		{
			return doHashAnti(receive);
		}

		return doNonHashSemi(receive);
	}

	private boolean redistributeHash(NetworkReceiveOperator receive) throws Exception
	{
		final Operator parent = receive.parent();
		verify2ReceivesForHash(parent);
		final Operator grandParent = parent.parent();
		long card = card(parent.children().get(0)) + card(parent.children().get(1));
		ArrayList<String> join = ((JoinOperator)parent).getJoinForChild(receive);
		parent.removeChild(receive);
		grandParent.removeChild(parent);
		ArrayList<Operator> sends = new ArrayList<Operator>(receive.children().size());
		int starting = getStartingNode(card / MIN_LOCAL_LEFT_HASH + 1);
		int ID = id.getAndIncrement();
		Operator other = null;
		for (final Operator o : parent.children())
		{
			if (o != receive)
			{
				other = o;
			}
		}
		String t = meta.getTableForCol(((JoinOperator)parent).getJoinForChild(other).get(0), other);
		String schema = t.substring(0, t.indexOf('.'));
		t = t.substring(t.indexOf('.') + 1);
		PartitionMetaData pmeta = null;
		if (t != null)
		{
			pmeta = meta.getPartMeta(schema, t, tx);
		}

		if (pmeta != null && isLocal(other.children().get(0).children().get(0)) && pmeta.noNodeGroupSet() && pmeta.nodeIsHash() && join.containsAll(pmeta.getNodeHash()) && pmeta.allNodes())
		{
			starting = 0;
			card = Long.MAX_VALUE;
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
			t = meta.getTableForCol(join.get(0), grandChild);
			schema = t.substring(0, t.indexOf('.'));
			t = t.substring(t.indexOf('.') + 1);
			pmeta = null;
			if (t != null)
			{
				pmeta = meta.getPartMeta(schema, t, tx);
			}

			if (pmeta != null && isLocal(grandChild) && pmeta.noNodeGroupSet() && pmeta.nodeIsHash() && join.containsAll(pmeta.getNodeHash()) && pmeta.allNodes())
			{
				starting = 0;
				card = Long.MAX_VALUE;
			}

			final NetworkHashAndSendOperator send = new NetworkHashAndSendOperator(join, card / MIN_LOCAL_LEFT_HASH + 1, ID, starting, meta, tx);
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
		while (i < (card / MIN_LOCAL_LEFT_HASH + 1) + starting && i < meta.getNumNodes(tx))
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

			final NetworkHashAndSendOperator send = new NetworkHashAndSendOperator(join, card / MIN_LOCAL_LEFT_HASH + 1, ID, starting, meta, tx);
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
		while (i < (card / MIN_LOCAL_LEFT_HASH + 1) + starting && i < meta.getNumNodes(tx))
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
		r.setNode(-1);
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
		makeHierarchical2(r);
		makeHierarchical(r);
		cCache.clear();
		return false;
	}
	
	private boolean handleExcept(NetworkReceiveOperator receive) throws Exception
	{
		final Operator parent = receive.parent();
		verify2ReceivesForHash(parent);
		final Operator grandParent = parent.parent();
		long card = card(parent.children().get(0)) + card(parent.children().get(1));
		parent.removeChild(receive);
		grandParent.removeChild(parent);
		ArrayList<Operator> sends = new ArrayList<Operator>(receive.children().size());
		int starting = getStartingNode(card / MIN_LOCAL_LEFT_HASH + 1);
		int ID = id.getAndIncrement();
		Operator other = null;
		for (final Operator o : parent.children())
		{
			if (o != receive)
			{
				other = o;
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

			final NetworkHashAndSendOperator send = new NetworkHashAndSendOperator(new ArrayList<String>(grandChild.getPos2Col().values()), card / MIN_LOCAL_LEFT_HASH + 1, ID, starting, meta, tx);
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
		while (i < (card / MIN_LOCAL_LEFT_HASH + 1) + starting && i < meta.getNumNodes(tx))
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

			final NetworkHashAndSendOperator send = new NetworkHashAndSendOperator(new ArrayList<String>(grandChild.getPos2Col().values()), card / MIN_LOCAL_LEFT_HASH + 1, ID, starting, meta, tx);
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
		while (i < (card / MIN_LOCAL_LEFT_HASH + 1) + starting && i < meta.getNumNodes(tx))
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
		r.setNode(-1);
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
		makeHierarchical2(r);
		makeHierarchical(r);
		cCache.clear();
		return false;
	}
	
	private boolean handleUnion(NetworkReceiveOperator receive) throws Exception
	{
		final Operator parent = receive.parent();
		verify2ReceivesForHash(parent);
		final Operator grandParent = parent.parent();
		long card = card(parent.children().get(0)) + card(parent.children().get(1));
		parent.removeChild(receive);
		grandParent.removeChild(parent);
		ArrayList<Operator> sends = new ArrayList<Operator>(receive.children().size());
		int starting = getStartingNode(card / MIN_LOCAL_LEFT_HASH + 1);
		int ID = id.getAndIncrement();
		Operator other = null;
		for (final Operator o : parent.children())
		{
			if (o != receive)
			{
				other = o;
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

			final NetworkHashAndSendOperator send = new NetworkHashAndSendOperator(new ArrayList<String>(grandChild.getPos2Col().values()), card / MIN_LOCAL_LEFT_HASH + 1, ID, starting, meta, tx);
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
		while (i < (card / MIN_LOCAL_LEFT_HASH + 1) + starting && i < meta.getNumNodes(tx))
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

			final NetworkHashAndSendOperator send = new NetworkHashAndSendOperator(new ArrayList<String>(grandChild.getPos2Col().values()), card / MIN_LOCAL_LEFT_HASH + 1, ID, starting, meta, tx);
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
		while (i < (card / MIN_LOCAL_LEFT_HASH + 1) + starting && i < meta.getNumNodes(tx))
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
		r.setNode(-1);
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
		makeHierarchical2(r);
		makeHierarchical(r);
		cCache.clear();
		return false;
	}

	private boolean redistributeNL(NetworkReceiveOperator receive) throws Exception
	{
		if (((NestedLoopJoinOperator)receive.parent()).usesHash())
		{
			return redistributeHash(receive);
		}

		return redistributeProduct(receive);
	}

	private boolean redistributeProduct(NetworkReceiveOperator receive) throws Exception
	{
		final Operator parent = receive.parent();
		verify2ReceivesForProduct(parent);
		final Operator left = parent.children().get(0);
		final Operator right = parent.children().get(1);

		if (isAllAny(left))
		{
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

			final ArrayList<NetworkSendOperator> sends2 = new ArrayList<NetworkSendOperator>(grandChildren.size());
			for (final Operator grandChild : grandChildren)
			{
				final Operator clone = cloneTree(parent, 0);
				clone.setNode(grandChild.getNode());
				final Operator leftClone = fullyCloneTree(left.children().get(0).children().get(0));
				setNodeForTree(leftClone, grandChild.getNode());
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
			r.setNode(-1);

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

			makeHierarchical(r);
			cCache.clear();
			return false;
		}

		// send left data to nodes and perform product there
		final Operator grandParent = parent.parent();
		parent.removeChild(left);
		grandParent.removeChild(parent);
		final ArrayList<NetworkSendMultipleOperator> sends = new ArrayList<NetworkSendMultipleOperator>(left.children().size());
		final int ID = id.getAndIncrement();
		for (final Operator child : (ArrayList<Operator>)left.children().clone())
		{
			final int node = child.getNode();
			final Operator grandChild = child.children().get(0);
			CNFFilter cnf = null;
			if (grandChild instanceof TableScanOperator)
			{
				cnf = ((TableScanOperator)grandChild).getCNFForParent(child);
			}
			child.removeChild(grandChild);
			final NetworkSendMultipleOperator send = new NetworkSendMultipleOperator(ID, meta);
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

		final ArrayList<NetworkHashReceiveOperator> receives = new ArrayList<NetworkHashReceiveOperator>(grandChildren.size());
		for (final Operator grandChild : grandChildren)
		{
			final Operator clone = cloneTree(parent, 0);
			clone.setNode(grandChild.getNode());
			final NetworkHashReceiveOperator receive2 = new NetworkHashReceiveOperator(ID, meta);
			try
			{
				clone.add(receive2);
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				throw e;
			}

			receive2.setNode(grandChild.getNode());
			receives.add(receive2);
			for (final Operator o : (ArrayList<Operator>)clone.children().clone())
			{
				if (!o.equals(receive2))
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
		}

		final ArrayList<NetworkSendOperator> sends2 = new ArrayList<NetworkSendOperator>(receives.size());
		for (final NetworkHashReceiveOperator receive2 : receives)
		{
			for (final NetworkSendMultipleOperator send : sends)
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

			final NetworkSendOperator send2 = new NetworkSendOperator(receive2.getNode(), meta);
			send2.setNode(receive2.getNode());
			try
			{
				send2.add(receive2.parent());
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				throw e;
			}

			sends2.add(send2);
		}

		final NetworkReceiveOperator r = new NetworkReceiveOperator(meta);
		r.setNode(-1);

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

		for (final NetworkHashReceiveOperator receive2 : receives)
		{
			Operator p = receive2.parent();
			Operator c = receive2;
			while (!(p instanceof RootOperator))
			{
				p.removeChild(c);
				try
				{
					p.add(c);
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.error("", e);
					throw e;
				}

				c = p;
				p = p.parent();
			}
		}

		makeHierarchical3(r);
		makeHierarchical(r);
		cCache.clear();
		return false;
	}

	private boolean redistributeSemi(NetworkReceiveOperator receive) throws Exception
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
		if (sort != null && card(sort) > MAX_LOCAL_SORT)
		{
			doSortRedistribution(sort);
		}
	}

	private void setNodeForTree(Operator op, int node)
	{
		op.setNode(node);
		for (final Operator o : op.children())
		{
			setNodeForTree(o, node);
		}
	}

	private void setNodeUpward(Operator op)
	{
		final int node = op.getNode();
		op = op.parent();
		while (!(op instanceof NetworkSendOperator))
		{
			op.setNode(node);
			op = op.parent();
		}
		op.setNode(node);
	}

	private boolean treeContains(Operator root, Operator op)
	{
		if (root.equals(op))
		{
			return true;
		}

		for (final Operator o : root.children())
		{
			if (treeContains(o, op))
			{
				return true;
			}
		}

		return false;
	}

	private void verify2ReceivesForHash(Operator op) throws Exception
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
				receive.setNode(-1);
				receive.add(send);
				op.add(receive);
				cCache.clear();
			}

			if (!(op.children().get(1) instanceof NetworkReceiveOperator))
			{
				final Operator child = op.children().get(1);
				op.removeChild(child);
				final NetworkSendOperator send = new NetworkSendOperator(-1, meta);
				send.add(child);
				final NetworkReceiveOperator receive = new NetworkReceiveOperator(meta);
				receive.setNode(-1);
				receive.add(send);
				op.add(receive);
				cCache.clear();
			}
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.error("", e);
			throw e;
		}
	}

	private void verify2ReceivesForProduct(Operator op) throws Exception
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
				receive.setNode(-1);
				receive.add(send);
				op.add(receive);
				cCache.clear();
			}

			if (!(op.children().get(1) instanceof NetworkReceiveOperator))
			{
				final Operator child = op.children().get(1);
				op.removeChild(child);
				final int ID = id.getAndIncrement();
				final NetworkSendRROperator send = new NetworkSendRROperator(ID, meta);
				send.setNode(-1);
				send.add(child);

				final long numNodes = card(child) / MAX_LOCAL_SORT + 1;
				final int starting = getStartingNode(numNodes);
				final ArrayList<NetworkSendOperator> sends = new ArrayList<NetworkSendOperator>();
				int i = 0;
				while (i < numNodes && starting + i < meta.getNumNodes(tx))
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
				receive.setNode(-1);

				for (final NetworkSendOperator send2 : sends)
				{
					receive.add(send2);
				}
				op.add(receive);
				makeHierarchical4(receive);
				cCache.clear();
			}
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.error("", e);
			throw e;
		}
	}

	private void verify2ReceivesForSemi(Operator op) throws Exception
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
				receive.setNode(-1);
				receive.add(send);
				op.add(receive);
				cCache.clear();
			}

			if (!(op.children().get(0) instanceof NetworkReceiveOperator))
			{
				final Operator child = op.children().get(0);
				op.removeChild(child);
				final int ID = id.getAndIncrement();
				final NetworkSendRROperator send = new NetworkSendRROperator(ID, meta);
				send.setNode(-1);
				send.add(child);

				final long numNodes = card(child) / MAX_LOCAL_SORT + 1;
				final int starting = getStartingNode(numNodes);
				final ArrayList<NetworkSendOperator> sends = new ArrayList<NetworkSendOperator>();
				int i = 0;
				while (i < numNodes && starting + i < meta.getNumNodes(tx))
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
				receive.setNode(-1);

				for (final NetworkSendOperator send2 : sends)
				{
					receive.add(send2);
				}
				op.add(receive);
				makeHierarchical4(receive);
				cCache.clear();
			}
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.error("", e);
			throw e;
		}
	}
}