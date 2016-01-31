package com.exascale.optimizer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.managers.ResourceManager;
import com.exascale.optimizer.MetaData.PartitionMetaData;
import com.exascale.tables.Transaction;

public final class Phase3
{
	protected static final int MAX_INCOMING_CONNECTIONS = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("max_neighbor_nodes")); // 100
	private static final int MAX_LOCAL_NO_HASH_PRODUCT = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("max_local_no_hash_product")); // 1000000
	private static final int MAX_LOCAL_LEFT_HASH = (int)(ResourceManager.QUEUE_SIZE * Double.parseDouble(HRDBMSWorker.getHParms().getProperty("hash_external_factor")));
	private static final int MAX_LOCAL_SORT = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("max_local_sort")); // 1000000
	private static Random random = new Random(System.currentTimeMillis());
	private static final long MAX_GB = (long)(ResourceManager.QUEUE_SIZE * Double.parseDouble(HRDBMSWorker.getHParms().getProperty("sort_gb_factor")));
	protected int colSuffix = 0;
	private final RootOperator root;
	private final MetaData meta;
	private final Transaction tx;

	public Phase3(RootOperator root, Transaction tx)
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

	private static boolean containsAll(ArrayList<String> fs, ArrayList<String> hs, PartitionMetaData pmd) throws Exception
	{
		ArrayList<String> swizzled = new ArrayList<String>();
		String table = pmd.getTable();
		for (String s : hs)
		{
			if (s.contains("."))
			{
				throw new Exception("A partitioning column contains a period in its name");
			}

			swizzled.add(table + "." + s);
		}

		return fs.containsAll(swizzled);
	}

	public long card(Operator op) throws Exception
	{
		if (op instanceof AntiJoinOperator)
		{
			return card(op.children().get(0));
		}

		if (op instanceof CaseOperator)
		{
			return card(op.children().get(0));
		}

		if (op instanceof ConcatOperator)
		{
			return card(op.children().get(0));
		}

		if (op instanceof DateMathOperator)
		{
			return card(op.children().get(0));
		}

		if (op instanceof ExceptOperator)
		{
			return card(op.children().get(0));
		}

		if (op instanceof ExtendOperator)
		{
			return card(op.children().get(0));
		}

		if (op instanceof ExtendObjectOperator)
		{
			return card(op.children().get(0));
		}

		if (op instanceof HashJoinOperator)
		{
			HashSet<HashMap<Filter, Filter>> hshm = ((HashJoinOperator)op).getHSHM();
			double max = -1;
			for (HashMap<Filter, Filter> hm : hshm)
			{
				double temp = meta.likelihood(new ArrayList<Filter>(hm.keySet()), tx, op);
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
			return retval;
		}

		if (op instanceof IntersectOperator)
		{
			long lCard = card(op.children().get(0));
			long rCard = card(op.children().get(1));

			if (lCard <= rCard)
			{
				if (lCard == 0)
				{
					lCard = 1;
				}
				return lCard;
			}
			else
			{
				if (rCard == 0)
				{
					rCard = 1;
				}
				return rCard;
			}
		}

		if (op instanceof MultiOperator)
		{
			// return card(op.children().get(0));
			final long groupCard = meta.getColgroupCard(((MultiOperator)op).getKeys(), root, tx, op);
			if (groupCard > card(op.children().get(0)))
			{
				return card(op.children().get(0));
			}

			return groupCard;
		}

		if (op instanceof NestedLoopJoinOperator)
		{
			HashSet<HashMap<Filter, Filter>> hshm = ((NestedLoopJoinOperator)op).getHSHM();
			double max = -1;
			for (HashMap<Filter, Filter> hm : hshm)
			{
				double temp = meta.likelihood(new ArrayList<Filter>(hm.keySet()), tx, op);
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
			return retval;
		}

		if (op instanceof NetworkReceiveOperator)
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
			return retval;
		}

		if (op instanceof NetworkHashAndSendOperator)
		{
			return card(op.children().get(0)) / ((NetworkHashAndSendOperator)op).parents().size();
		}

		if (op instanceof NetworkSendRROperator)
		{
			return card(op.children().get(0)) / ((NetworkSendRROperator)op).parents().size();
		}

		if (op instanceof NetworkSendOperator)
		{
			return card(op.children().get(0));
		}

		if (op instanceof ProductOperator)
		{
			long temp = card(op.children().get(0)) * card(op.children().get(1));
			if (temp < 0)
			{
				return Long.MAX_VALUE;
			}
			else
			{
				return temp;
			}
		}

		if (op instanceof ProjectOperator)
		{
			return card(op.children().get(0));
		}

		if (op instanceof RenameOperator)
		{
			return card(op.children().get(0));
		}

		if (op instanceof ReorderOperator)
		{
			return card(op.children().get(0));
		}

		if (op instanceof RootOperator)
		{
			return card(op.children().get(0));
		}

		if (op instanceof SelectOperator)
		{
			return (long)(((SelectOperator)op).likelihood(root, tx) * card(op.children().get(0)));
		}

		if (op instanceof SemiJoinOperator)
		{
			return card(op.children().get(0));
		}

		if (op instanceof SortOperator)
		{
			return card(op.children().get(0));
		}

		if (op instanceof SubstringOperator)
		{
			return card(op.children().get(0));
		}

		if (op instanceof TopOperator)
		{
			long retval = ((TopOperator)op).getRemaining();
			long retval2 = card(op.children().get(0));

			if (retval2 < retval)
			{
				if (retval2 == 0)
				{
					retval2 = 1;
				}
				return retval2;
			}
			else
			{
				if (retval == 0)
				{
					retval = 1;
				}
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

			if (retval == 0)
			{
				retval = 1;
			}
			return retval;
		}

		if (op instanceof YearOperator)
		{
			return card(op.children().get(0));
		}

		if (op instanceof TableScanOperator)
		{
			final HashSet<HashMap<Filter, Filter>> hshm = ((TableScanOperator)op).getHSHM();
			if (hshm != null)
			{
				return (long)(meta.getTableCard(((TableScanOperator)op).getSchema(), ((TableScanOperator)op).getTable(), tx) * meta.likelihood(hshm, root, tx, op) * (1.0 / ((TableScanOperator)op).getNumNodes()));
			}

			return (long)((1.0 / ((TableScanOperator)op).getNumNodes()) * meta.getTableCard(((TableScanOperator)op).getSchema(), ((TableScanOperator)op).getTable(), tx));
		}

		HRDBMSWorker.logger.error("Unknown operator in card() in Phase3: " + op.getClass());
		throw new Exception("Unknown operator in card() in Phase3: " + op.getClass());
	}

	public void optimize() throws Exception
	{
		pushDownGB(root);
		final ArrayList<NetworkReceiveOperator> receives = getReceives(root);
		for (final NetworkReceiveOperator receive : receives)
		{
			makeHierarchical(receive);
		}
		pushUpReceives();
		// collapseDuplicates(root);
		assignNodes(root, -1);
		cleanupStrandedTables(root);
		assignNodes(root, -1);
		removeLocalSendReceive(root);
		clearOpParents(root);
		cleanupOrderedFilters(root);
		// HRDBMSWorker.logger.debug("Upon exiting P3:");
		// Phase1.printTree(root, 0);
		// sanityCheck(root, -1);
	}

	private void assignNodes(Operator op, int node) throws Exception
	{
		if (op instanceof NetworkSendOperator)
		{
			node = ((NetworkSendOperator)op).getNode();
		}

		/*
		 * if (op instanceof TableScanOperator) { if (node != op.getNode()) {
		 * System.out.println("Nodes do not match!"); Driver.printTree(0, root);
		 * System.exit(1); }
		 *
		 * return; }
		 */

		op.setNode(node);
		for (final Operator o : op.children())
		{
			if (o == null)
			{
				HRDBMSWorker.logger.error("Null child in assignNodes()");
				HRDBMSWorker.logger.error("Parent is " + op);
				HRDBMSWorker.logger.error("Children are " + op.children());
				throw new Exception("Null child in assignNodes()");
			}
			assignNodes(o, node);
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

	private void cleanupStrandedTables(Operator op) throws Exception
	{
		if (op instanceof TableScanOperator)
		{
			if (op.getNode() == -1 && !((TableScanOperator)op).getSchema().equals("SYS"))
			{
				TableScanOperator table = (TableScanOperator)op;
				if (!table.anyNode())
				{
					throw new Exception("Cleaning up a stranded table that is not type ANY");
				}
				final Operator parent = ((TableScanOperator)op).firstParent();
				final CNFFilter cnf = ((TableScanOperator)op).getCNFForParent(parent);
				parent.removeChild(op);
				final Operator send = new NetworkSendOperator(getPositiveRandomInt() % MetaData.numWorkerNodes, meta);
				op.setNode(send.getNode());
				final Operator receive = new NetworkReceiveOperator(meta);
				try
				{
					send.add(op);
					if (cnf != null)
					{
						((TableScanOperator)op).setCNFForParent(send, cnf);
					}
					receive.add(send);
					parent.add(receive);
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.error("", e);
					throw e;
				}
			}
		}
		else
		{
			for (final Operator o : (ArrayList<Operator>)op.children().clone())
			{
				cleanupStrandedTables(o);
			}
		}
	}

	private Operator cloneTree(Operator op) throws Exception
	{
		final Operator clone = op.clone();
		for (final Operator o : op.children())
		{
			try
			{
				final Operator child = cloneTree(o);
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

	private boolean containsNoSend(Operator op)
	{
		if (op instanceof NetworkReceiveOperator)
		{
			for (Operator o : op.children())
			{
				if (!containsNoSend(o.children().get(0)))
				{
					return false;
				}
			}

			return true;
		}

		return containsNoSend2(op);
	}

	private boolean containsNoSend2(Operator op)
	{
		if (op.children().size() == 0)
		{
			return true;
		}
		else if (op instanceof NetworkSendOperator)
		{
			return false;
		}
		else
		{
			for (Operator o : op.children())
			{
				if (!containsNoSend2(o))
				{
					return false;
				}
			}

			return true;
		}
	}

	private boolean containsOnlyNormalSend(Operator op)
	{
		if (op instanceof NetworkHashAndSendOperator || op instanceof NetworkSendMultipleOperator || op instanceof NetworkSendRROperator)
		{
			return false;
		}

		for (final Operator o : op.children())
		{
			if (!containsOnlyNormalSend(o))
			{
				return false;
			}
		}

		return true;
	}

	private void doLeft(MultiOperator parent, HashJoinOperator hjop, Operator l, Operator r, MultiOperator pClone) throws Exception
	{
		final int oldSuffix = colSuffix;
		final Operator orig = parent.parent();
		final ArrayList<String> cols = new ArrayList<String>(parent.getPos2Col().values());
		ArrayList<String> oldCols = null;
		ArrayList<String> newCols = null;

		while (pClone.hasAvg())
		{
			final String avgCol = pClone.getAvgCol();
			final ArrayList<String> newCols2 = new ArrayList<String>(2);
			final String newCol1 = "_P" + colSuffix++;
			final String newCol2 = "_P" + colSuffix++;
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

		colSuffix = oldSuffix;
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
			RenameOperator rename = null;
			rename = new RenameOperator(oldCols, newCols, meta);
			rename.add(pClone);
			hjop.add(rename);
			hjop.add(r);
			parent.add(hjop);
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.error("", e);
			throw e;
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
	}

	private void doPushDownGB(MultiOperator mop, HashJoinOperator hjop) throws Exception
	{
		if (mop.existsCountDistinct())
		{
			HRDBMSWorker.logger.debug("But it had a count distinct");
			return;
		}

		ArrayList<String> group1 = (ArrayList<String>)mop.getKeys().clone();
		ArrayList<String> group2 = (ArrayList<String>)group1.clone();
		for (String col : hjop.getLefts())
		{
			if (!group1.contains(col))
			{
				group1.add(col);
			}
		}

		for (String col : hjop.getRights())
		{
			if (!group2.contains(col))
			{
				group2.add(col);
			}
		}

		mop.removeChild(hjop);
		Operator l = hjop.children().get(0);
		Operator r = hjop.children().get(1);
		hjop.removeChild(r);
		hjop.removeChild(l);

		MultiOperator newOp = new MultiOperator(mop.clone().getOps(), group1, meta, false);
		HRDBMSWorker.logger.debug("Group1 is " + group1);
		HRDBMSWorker.logger.debug("Group2 is " + group2);
		HRDBMSWorker.logger.debug("Input cols are " + mop.getRealInputCols());
		HRDBMSWorker.logger.debug("L contains " + l.getCols2Pos().keySet());
		HRDBMSWorker.logger.debug("R contains " + r.getCols2Pos().keySet());
		HRDBMSWorker.logger.debug("HJOP contains " + hjop.getCols2Pos().keySet());
		if (l.getCols2Pos().keySet().containsAll(group1) && l.getCols2Pos().keySet().containsAll(mop.getRealInputCols()))
		{
			long prev = card(l);
			newOp.add(l);
			long newCard = card(newOp);

			HRDBMSWorker.logger.debug("Considering l with reduction " + (newCard * 1.0) / (prev * 1.0));
			if (newCard < MAX_GB && (newCard * 1.0) / (prev * 1.0) <= 0.5)
			{
				doLeft(mop, hjop, l, r, newOp);
				HRDBMSWorker.logger.debug("Doing pushdown across the left side");
				return;
			}
			else
			{
				HRDBMSWorker.logger.debug("Decided against it");
				newOp.removeChild(l);
			}
		}

		if (r.getCols2Pos().keySet().containsAll(group2) && l.getCols2Pos().keySet().containsAll(mop.getRealInputCols()))
		{
			newOp = new MultiOperator(mop.clone().getOps(), group2, meta, false);
			long prev = card(r);
			newOp.add(r);
			long newCard = card(newOp);

			HRDBMSWorker.logger.debug("Considering r with reduction " + (newCard * 1.0) / (prev * 1.0));
			if (newCard < MAX_GB && (newCard * 1.0) / (prev * 1.0) <= 0.5)
			{
				doRight(mop, hjop, l, r, newOp);
				HRDBMSWorker.logger.debug("Doing pushdown across the right side");
				return;
			}
			else
			{
				HRDBMSWorker.logger.debug("Decided against it");
				newOp.removeChild(r);
			}
		}

		hjop.add(l);
		hjop.add(r);
		mop.add(hjop);
	}

	private void doRight(MultiOperator parent, HashJoinOperator hjop, Operator l, Operator r, MultiOperator pClone) throws Exception
	{
		final int oldSuffix = colSuffix;
		final Operator orig = parent.parent();
		final ArrayList<String> cols = new ArrayList<String>(parent.getPos2Col().values());
		ArrayList<String> oldCols = null;
		ArrayList<String> newCols = null;

		while (pClone.hasAvg())
		{
			final String avgCol = pClone.getAvgCol();
			final ArrayList<String> newCols2 = new ArrayList<String>(2);
			final String newCol1 = "_P" + colSuffix++;
			final String newCol2 = "_P" + colSuffix++;
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

		colSuffix = oldSuffix;
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
			RenameOperator rename = null;
			rename = new RenameOperator(oldCols, newCols, meta);
			rename.add(pClone);
			hjop.add(l);
			hjop.add(rename);
			parent.add(hjop);
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.error("", e);
			throw e;
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
	}

	private ArrayList<ArrayList<String>> getCurrentHash(Operator op) throws Exception
	{
		if (op instanceof TableScanOperator)
		{
			PartitionMetaData pmd = meta.getPartMeta(((TableScanOperator)op).getSchema(), ((TableScanOperator)op).getTable(), tx);
			if (pmd.nodeIsHash())
			{
				ArrayList<String> retval = (ArrayList<String>)pmd.getNodeHash().clone();
				ArrayList<ArrayList<String>> r = new ArrayList<ArrayList<String>>();
				for (String s : (ArrayList<String>)retval.clone())
				{
					if (!s.contains("."))
					{
						retval.remove(s);
						String newName = "";
						if (((TableScanOperator)op).getAlias() != null && !((TableScanOperator)op).getAlias().equals(""))
						{
							newName += ((TableScanOperator)op).getAlias();
						}
						else
						{
							newName += ((TableScanOperator)op).getTable();
						}

						newName += ("." + s);
						retval.add(newName);
					}
				}
				r.add(retval);
				return r;
			}
			else
			{
				ArrayList<String> als = new ArrayList<String>();
				ArrayList<ArrayList<String>> retval = new ArrayList<ArrayList<String>>();
				retval.add(als);
				return retval;
			}
		}

		if (op instanceof NetworkSendMultipleOperator || op instanceof NetworkSendRROperator)
		{
			ArrayList<String> als = new ArrayList<String>();
			ArrayList<ArrayList<String>> retval = new ArrayList<ArrayList<String>>();
			retval.add(als);
			return retval;
		}

		if (op instanceof NetworkHashAndSendOperator)
		{
			ArrayList<ArrayList<String>> retval = new ArrayList<ArrayList<String>>();
			retval.add((ArrayList<String>)((NetworkHashAndSendOperator)op).getHashCols().clone());
			return retval;
		}

		ArrayList<ArrayList<String>> retval = new ArrayList<ArrayList<String>>();
		for (Operator o : op.children())
		{
			retval.addAll(getCurrentHash(o));
		}

		return retval;
	}

	private int getNode(Operator op) throws Exception
	{
		if (op instanceof TableScanOperator)
		{
			return op.getNode();
		}
		else
		{
			final int retval = getNode(op.children().get(0));
			for (final Operator o : op.children())
			{
				if (retval != getNode(o))
				{
					HRDBMSWorker.logger.error("Tree with multiple nodes!");
					throw new Exception("Tree with multiple nodes!");
				}
			}

			return retval;
		}
	}

	private int getPositiveRandomInt()
	{
		while (true)
		{
			int retval = Math.abs(random.nextInt());
			if (retval >= 0)
			{
				return retval;
			}
		}
	}

	private ArrayList<NetworkReceiveOperator> getReceives(Operator op)
	{
		final ArrayList<NetworkReceiveOperator> retval = new ArrayList<NetworkReceiveOperator>();
		if (!(op instanceof NetworkReceiveOperator))
		{
			for (final Operator child : op.children())
			{
				retval.addAll(getReceives(child));
			}

			return retval;
		}
		else
		{
			retval.add((NetworkReceiveOperator)op);
			for (final Operator child : op.children())
			{
				retval.addAll(getReceives(child));
			}

			return retval;
		}
	}

	private int getStartingNode(long numNodes) throws Exception
	{
		if (numNodes >= MetaData.numWorkerNodes)
		{
			return 0;
		}

		final int range = (int)(MetaData.numWorkerNodes - numNodes);
		return (int)(Math.random() * range);
	}

	private ArrayList<TableScanOperator> getTableOperators(Operator op)
	{
		if (op instanceof TableScanOperator)
		{
			final ArrayList<TableScanOperator> retval = new ArrayList<TableScanOperator>(1);
			retval.add((TableScanOperator)op);
			return retval;
		}

		final ArrayList<TableScanOperator> retval = new ArrayList<TableScanOperator>();
		for (final Operator o : op.children())
		{
			retval.addAll(getTableOperators(o));
		}

		return retval;
	}

	private boolean handleAnti(NetworkReceiveOperator receive) throws Exception
	{
		// if (MetaData.numWorkerNodes == 1)
		// {
		// pushAcross2(receive);
		// return true;
		// }

		if (isAllAny(receive.parent().children().get(0)) && isAllAny(receive.parent().children().get(1)))
		{
			if (containsNoSend(receive.parent().children().get(0)) && containsNoSend(receive.parent().children().get(1)))
			{
				final ArrayList<TableScanOperator> left = getTableOperators(receive.parent().children().get(0));
				final ArrayList<TableScanOperator> right = getTableOperators(receive.parent().children().get(1));
				final int node = left.get(0).getNode();
				for (final TableScanOperator table : left)
				{
					table.setNode(node);
					setNodeForSend(table, node);
				}
				for (final TableScanOperator table : right)
				{
					table.setNode(node);
					setNodeForSend(table, node);
				}

				pushAcross2(receive);
				return true;
			}
		}

		if (isAllAny(receive))
		{
			if (!containsNoSend(receive))
			{
				return false;
			}
			final AntiJoinOperator parent = (AntiJoinOperator)receive.parent();
			boolean onRight;
			if (parent.children().get(0).equals(receive))
			{
				onRight = false;
			}
			else
			{
				onRight = true;
			}

			if (!onRight)
			{
				return false;
			}
			parent.removeChild(receive);
			final Operator op = parent.children().get(0);
			parent.removeChild(op);
			final Operator grandParent = parent.parent();
			grandParent.removeChild(parent);
			Operator leftTree;
			CNFFilter cnf = null;
			try
			{
				if (receive.children().size() == 1)
				{
					leftTree = receive.children().get(0).children().get(0);
					if (leftTree instanceof TableScanOperator)
					{
						cnf = ((TableScanOperator)leftTree).getCNFForParent(((TableScanOperator)leftTree).firstParent());
					}
					receive.children().get(0).removeChild(leftTree);
				}
				else if (receive.children().size() == 0)
				{
					throw new Exception("Receive operator with no children");
				}
				else
				{
					leftTree = new UnionOperator(false, meta);
					for (final Operator child : (ArrayList<Operator>)receive.children().clone())
					{
						if (child.children().size() == 0)
						{
							throw new Exception("Send operator with no children");
						}
						for (Operator childOfSend : child.children())
						{
							child.removeChild(childOfSend);
							leftTree.add(childOfSend); // FIXED
						}
					}
				}
				makeLocal(op, parent, leftTree, onRight, grandParent, cnf);
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				throw e;
			}
			return false;
		}
		else
		{
			final AntiJoinOperator parent = (AntiJoinOperator)receive.parent();
			if (!containsNoSend(parent.children().get(0)) || !containsNoSend(parent.children().get(1)))
			{
				return false;
			}
			final ArrayList<String> lefts = parent.getLefts();
			final ArrayList<String> rights = parent.getRights();
			int i = 0;
			final HashSet<String> tables = new HashSet<String>();
			while (i < lefts.size())
			{
				String temp = meta.getTableForCol(lefts.get(i), parent);
				if (temp == null)
				{
					return false;
				}
				tables.add(temp);
				temp = meta.getTableForCol(rights.get(i), parent);
				if (temp == null)
				{
					return false;
				}
				tables.add(temp);
				i++;
			}

			if (tables.size() != 2)
			{
				return false;
			}

			final ArrayList<PartitionMetaData> pmetas = new ArrayList<PartitionMetaData>(tables.size());
			for (String table : tables)
			{
				String schema = table.substring(0, table.indexOf('.'));
				table = table.substring(table.indexOf('.') + 1);
				pmetas.add(meta.getPartMeta(schema, table, tx));
			}

			ArrayList<ArrayList<String>> lhash = getCurrentHash(parent.children().get(0));
			ArrayList<ArrayList<String>> rhash = getCurrentHash(parent.children().get(1));

			if (pmetas.get(0).noNodeGroupSet() && pmetas.get(1).noNodeGroupSet())
			{
				if (!needsRehash(lhash, lefts) && !needsRehash(rhash, rights))
				{
					pushAcross2(receive);
					return true;
				}
				else if (!needsRehash(lhash, rights) && !needsRehash(rhash, lefts))
				{
					pushAcross2(receive);
					return true;
				}
				else
				{
					return false;
				}
			}
			else
			{
				return false;
			}
		}
	}

	private boolean handleExcept(NetworkReceiveOperator receive) throws Exception
	{
		if (receive.parent().children().size() == 1)
		{
			pushAcross(receive);
			return true;
		}

		boolean ok = true;
		for (Operator child : receive.parent().children())
		{
			if (!isAllAny(child) || !containsNoSend(child))
			{
				ok = false;
				break;
			}
		}

		if (!ok)
		{
			return false;
		}

		final ArrayList<TableScanOperator> tables = new ArrayList<TableScanOperator>();
		for (Operator child : receive.parent().children())
		{
			tables.addAll(getTableOperators(child));
		}

		final int node = tables.get(0).getNode();
		for (final TableScanOperator table : tables)
		{
			table.setNode(node);
			setNodeForSend(table, node);
		}

		pushAcross2(receive);
		return true;
	}

	private boolean handleHash(NetworkReceiveOperator receive) throws Exception
	{
		// if (MetaData.numWorkerNodes == 1)
		// {
		// pushAcross2(receive);
		// return true;
		// }
		// HRDBMSWorker.logger.debug(receive.parent());

		if (isAllAny(receive.parent().children().get(0)) && isAllAny(receive.parent().children().get(1)))
		{
			// HRDBMSWorker.logger.debug("Both are any");
			if (containsNoSend(receive.parent().children().get(0)) && containsNoSend(receive.parent().children().get(1)))
			{
				final ArrayList<TableScanOperator> left = getTableOperators(receive.parent().children().get(0));
				final ArrayList<TableScanOperator> right = getTableOperators(receive.parent().children().get(1));
				final int node = left.get(0).getNode();
				for (final TableScanOperator table : left)
				{
					table.setNode(node);
					setNodeForSend(table, node);
				}
				for (final TableScanOperator table : right)
				{
					table.setNode(node);
					setNodeForSend(table, node);
				}

				pushAcross2(receive);
				return true;
			}
		}

		if (isAllAny(receive))
		{
			// HRDBMSWorker.logger.debug("1 is any");
			if (!containsNoSend(receive))
			{
				return false;
			}
			final HashJoinOperator parent = (HashJoinOperator)receive.parent();
			boolean onRight;
			if (parent.children().get(0).equals(receive))
			{
				onRight = false;
			}
			else
			{
				onRight = true;
			}
			parent.removeChild(receive);
			final Operator op = parent.children().get(0);
			parent.removeChild(op);
			final Operator grandParent = parent.parent();
			grandParent.removeChild(parent);
			Operator leftTree;
			CNFFilter cnf = null;
			try
			{
				if (receive.children().size() == 1)
				{
					leftTree = receive.children().get(0).children().get(0);
					if (leftTree instanceof TableScanOperator)
					{
						cnf = ((TableScanOperator)leftTree).getCNFForParent(((TableScanOperator)leftTree).firstParent());
					}
					receive.children().get(0).removeChild(leftTree);
				}
				else if (receive.children().size() == 0)
				{
					throw new Exception("Receive operator with no children");
				}
				else
				{
					leftTree = new UnionOperator(false, meta);
					for (final Operator child : (ArrayList<Operator>)receive.children().clone())
					{
						if (child.children().size() == 0)
						{
							throw new Exception("Send operator with no children");
						}
						for (Operator childOfSend : child.children())
						{
							child.removeChild(childOfSend);
							leftTree.add(childOfSend); // FIXED - was (child)
						}
					}
				}
				makeLocal(op, parent, leftTree, onRight, grandParent, cnf);
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				throw e;
			}
			return true;
		}
		else
		{
			// HRDBMSWorker.logger.debug("None are any");
			final HashJoinOperator parent = (HashJoinOperator)receive.parent();
			if (!containsNoSend(parent.children().get(0)) || !containsNoSend(parent.children().get(1)))
			{
				return false;
			}
			final ArrayList<String> lefts = parent.getLefts();
			final ArrayList<String> rights = parent.getRights();
			int i = 0;
			final HashSet<String> tables = new HashSet<String>();
			while (i < lefts.size())
			{
				String name = meta.getTableForCol(lefts.get(i), parent);
				if (name == null)
				{
					return false;
				}
				tables.add(name);
				name = meta.getTableForCol(rights.get(i), parent);
				if (name == null)
				{
					return false;
				}
				tables.add(name);
				i++;
			}

			if (tables.size() != 2)
			{
				return false;
			}

			final ArrayList<PartitionMetaData> pmetas = new ArrayList<PartitionMetaData>(tables.size());
			for (String table : tables)
			{
				String schema = table.substring(0, table.indexOf('.'));
				table = table.substring(table.indexOf('.') + 1);
				pmetas.add(meta.getPartMeta(schema, table, tx));
			}

			ArrayList<ArrayList<String>> lhash = getCurrentHash(parent.children().get(0));
			ArrayList<ArrayList<String>> rhash = getCurrentHash(parent.children().get(1));

			if (pmetas.get(0).noNodeGroupSet() && pmetas.get(1).noNodeGroupSet())
			{
				// System.out.println("Hash: no node group");
				if (!needsRehash(lhash, lefts) && !needsRehash(rhash, rights))
				{
					// System.out.println("Hash: hash equal");
					pushAcross2(receive);
					return true;
				}
				else if (!needsRehash(lhash, rights) && !needsRehash(rhash, lefts))
				{
					// System.out.println("Hash: hash equal");
					pushAcross2(receive);
					return true;
				}
				else
				{
					return false;
				}
			}
			else
			{
				return false;
			}
		}
	}

	private boolean handleIntersect(NetworkReceiveOperator receive) throws Exception
	{
		if (receive.parent().children().size() == 1)
		{
			pushAcross(receive);
			return true;
		}

		boolean ok = true;
		for (Operator child : receive.parent().children())
		{
			if (!isAllAny(child) || !containsNoSend(child))
			{
				ok = false;
				break;
			}
		}

		if (!ok)
		{
			return false;
		}

		final ArrayList<TableScanOperator> tables = new ArrayList<TableScanOperator>();
		for (Operator child : receive.parent().children())
		{
			tables.addAll(getTableOperators(child));
		}

		final int node = tables.get(0).getNode();
		for (final TableScanOperator table : tables)
		{
			table.setNode(node);
			setNodeForSend(table, node);
		}

		pushAcross2(receive);
		return true;
	}

	private boolean handleMulti(NetworkReceiveOperator receive) throws Exception
	{
		if (receive.children().size() == 1)
		{
			pushAcross(receive);
			return true;
		}

		final MultiOperator parent = (MultiOperator)receive.parent();

		if (containsOnlyNormalSend(receive))
		{
			int i = 0;
			boolean doIt = true;
			final HashSet<String> tables = new HashSet<String>();
			final HashMap<String, ArrayList<String>> table2Cols = new HashMap<String, ArrayList<String>>();
			while (i < parent.getKeys().size())
			{
				final String name = meta.getTableForCol(parent.getKeys().get(i), parent);
				if (name == null)
				{
					doIt = false;
					break;
				}
				tables.add(name);
				ArrayList<String> cols = table2Cols.get(name);
				if (cols == null)
				{
					cols = new ArrayList<String>();
				}
				cols.add(parent.getKeys().get(i));
				table2Cols.put(name, cols);
				i++;
			}

			if (i == 0)
			{
				doIt = false;
			}

			if (doIt)
			{
				for (String table : tables)
				{
					String schema = table.substring(0, table.indexOf('.'));
					table = table.substring(table.indexOf('.') + 1);
					final PartitionMetaData partMeta = meta.getPartMeta(schema, table, tx);
					if (partMeta.nodeIsHash() && partMeta.noNodeGroupSet())
					{
						try
						{
							if (containsAll(table2Cols.get(schema + "." + table), partMeta.getNodeHash(), partMeta))
							{
							}
							else
							{
								doIt = false;
								break;
							}
						}
						catch (Exception e)
						{
							HRDBMSWorker.logger.debug("Table2Cols = " + table2Cols);
							HRDBMSWorker.logger.debug("Table = " + table);
							HRDBMSWorker.logger.debug("PartMeta = " + partMeta);
							HRDBMSWorker.logger.debug("PartMeta.getNodeHash() = " + partMeta.getNodeHash());
							throw e;
						}
					}
					else if (partMeta.nodeIsHash() && partMeta.nodeGroupIsHash())
					{
						if (containsAll(table2Cols.get(schema + "." + table), partMeta.getNodeHash(), partMeta) && containsAll(table2Cols.get(schema + "." + table), partMeta.getNodeGroupHash(), partMeta))
						{
						}
						else
						{
							doIt = false;
							break;
						}
					}
					else
					{
						doIt = false;
						break;
					}
				}
			}

			if (doIt)
			{
				pushAcross(receive);
				return true;
			}
		}

		card(parent);
		if (!noLargeUpstreamJoins(parent) && parent.getKeys().size() > 0)
		{
			final ArrayList<String> cols2 = new ArrayList<String>(parent.getKeys());
			boolean rehash = false;
			for (Operator o : receive.children())
			{
				ArrayList<ArrayList<String>> currentHash = getCurrentHash(o);
				if (needsRehash(currentHash, cols2))
				{
					rehash = true;
					break;
				}
			}
			if (rehash)
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
				makeHierarchical2(receive);
				makeHierarchical(receive);
				return true;
			}
			else
			{
				for (Operator o : (ArrayList<Operator>)receive.children().clone())
				{
					final Operator temp = o.children().get(0);
					o.removeChild(temp);
					receive.removeChild(o);
					CNFFilter cnf = null;
					if (temp instanceof TableScanOperator)
					{
						cnf = ((TableScanOperator)temp).getCNFForParent(receive);
					}

					try
					{
						MultiOperator clone = parent.clone();
						clone.add(temp);
						clone.setNode(temp.getNode());
						if (cnf != null)
						{
							((TableScanOperator)o).setCNFForParent(clone, cnf);
						}

						o.add(temp);
						receive.add(o);
					}
					catch (final Exception e)
					{
						HRDBMSWorker.logger.error("", e);
						throw e;
					}
				}

				final Operator grandParent = parent.parent();
				grandParent.removeChild(parent);
				parent.removeChild(receive);

				try
				{
					grandParent.add(receive);
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.error("", e);
					throw e;
				}

				return true;
			}
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

					String input = parent.getInputCols().get(0);
					ArrayList<String> cols2 = new ArrayList<String>();
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
					ArrayList<String> old = new ArrayList<String>();
					ArrayList<String> newCols = new ArrayList<String>();
					old.add(receive.getPos2Col().get(0));
					newCols.add("_P" + colSuffix++);
					RenameOperator rename = new RenameOperator(old, newCols, meta);
					rename.add(receive);
					parent.changeCD2Add();
					ArrayList<String> newInputs = new ArrayList<String>();
					newInputs.add("_P" + (colSuffix - 1));
					parent.updateInputColumns(parent.getOutputCols(), newInputs);
					parent.add(rename);
					grandParent.add(parent);
					rename.setNode(parent.getNode());
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.error("", e);
					throw e;
				}
				makeHierarchical2(receive);
				makeHierarchical(receive);
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

		final int oldSuffix = colSuffix;
		final Operator orig = parent.parent();
		MultiOperator pClone;
		final ArrayList<String> cols = new ArrayList<String>(parent.getPos2Col().values());
		ArrayList<String> oldCols = null;
		ArrayList<String> newCols = null;

		for (final Map.Entry entry : send2Child.entrySet())
		{
			pClone = parent.clone();
			while (pClone.hasAvg())
			{
				final String avgCol = pClone.getAvgCol();
				final ArrayList<String> newCols2 = new ArrayList<String>(2);
				final String newCol1 = "_P" + colSuffix++;
				final String newCol2 = "_P" + colSuffix++;
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

			colSuffix = oldSuffix;
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
		return false;
	}

	private boolean handleNested(NetworkReceiveOperator receive) throws Exception
	{
		// if (MetaData.numWorkerNodes == 1)
		// {
		// pushAcross2(receive);
		// return true;
		// }

		if (isAllAny(receive.parent().children().get(0)) && isAllAny(receive.parent().children().get(1)))
		{
			if (containsNoSend(receive.parent().children().get(0)) && containsNoSend(receive.parent().children().get(1)))
			{
				final ArrayList<TableScanOperator> left = getTableOperators(receive.parent().children().get(0));
				final ArrayList<TableScanOperator> right = getTableOperators(receive.parent().children().get(1));
				final int node = left.get(0).getNode();
				for (final TableScanOperator table : left)
				{
					table.setNode(node);
					setNodeForSend(table, node);
				}
				for (final TableScanOperator table : right)
				{
					table.setNode(node);
					setNodeForSend(table, node);
				}

				pushAcross2(receive);
				return true;
			}
		}

		if (isAllAny(receive))
		{
			if (!containsNoSend(receive))
			{
				return false;
			}
			final NestedLoopJoinOperator parent = (NestedLoopJoinOperator)receive.parent();
			boolean onRight;
			if (parent.children().get(0).equals(receive))
			{
				onRight = false;
			}
			else
			{
				onRight = true;
			}
			parent.removeChild(receive);
			final Operator op = parent.children().get(0);
			parent.removeChild(op);
			final Operator grandParent = parent.parent();
			grandParent.removeChild(parent);
			Operator leftTree;
			CNFFilter cnf = null;
			try
			{
				if (receive.children().size() == 1)
				{
					leftTree = receive.children().get(0).children().get(0);
					if (leftTree instanceof TableScanOperator)
					{
						cnf = ((TableScanOperator)leftTree).getCNFForParent(((TableScanOperator)leftTree).firstParent());
					}
					receive.children().get(0).removeChild(leftTree);
				}
				else if (receive.children().size() == 0)
				{
					throw new Exception("Receive operator with no children");
				}
				else
				{
					leftTree = new UnionOperator(false, meta);
					for (final Operator child : (ArrayList<Operator>)receive.children().clone())
					{
						if (child.children().size() == 0)
						{
							throw new Exception("Send operator with no children");
						}
						for (Operator childOfSend : child.children())
						{
							child.removeChild(childOfSend);
							leftTree.add(childOfSend); // FIXED
						}
					}
				}
				makeLocal(op, parent, leftTree, onRight, grandParent, cnf);
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				throw e;
			}
			return false;
		}
		else
		{
			final NestedLoopJoinOperator parent = (NestedLoopJoinOperator)receive.parent();
			if (!containsNoSend(parent.children().get(0)) || !containsNoSend(parent.children().get(1)))
			{
				return false;
			}
			if (!parent.usesHash())
			{
				return false;
			}

			final ArrayList<String> lefts = parent.getJoinForChild(parent.children().get(0));
			final ArrayList<String> rights = parent.getJoinForChild(parent.children().get(1));

			if (lefts == null || rights == null)
			{
				return false;
			}
			int i = 0;
			final HashSet<String> tables = new HashSet<String>();
			while (i < lefts.size())
			{
				String temp = meta.getTableForCol(lefts.get(i), parent);
				if (temp == null)
				{
					return false;
				}
				tables.add(temp);
				temp = meta.getTableForCol(rights.get(i), parent);
				if (temp == null)
				{
					return false;
				}
				tables.add(temp);
				i++;
			}

			if (tables.size() != 2)
			{
				return false;
			}

			final ArrayList<PartitionMetaData> pmetas = new ArrayList<PartitionMetaData>(tables.size());
			for (String table : tables)
			{
				String schema = table.substring(0, table.indexOf('.'));
				table = table.substring(table.indexOf('.') + 1);
				pmetas.add(meta.getPartMeta(schema, table, tx));
			}

			ArrayList<ArrayList<String>> lhash = getCurrentHash(parent.children().get(0));
			ArrayList<ArrayList<String>> rhash = getCurrentHash(parent.children().get(1));

			if (pmetas.get(0).noNodeGroupSet() && pmetas.get(1).noNodeGroupSet())
			{
				if (!needsRehash(lhash, lefts) && !needsRehash(rhash, rights))
				{
					pushAcross2(receive);
					return true;
				}
				else if (!needsRehash(lhash, rights) && !needsRehash(rhash, lefts))
				{
					pushAcross2(receive);
					return true;
				}
				else
				{
					return false;
				}
			}
			else
			{
				return false;
			}
		}
	}

	private boolean handleProduct(NetworkReceiveOperator receive) throws Exception
	{
		// if (MetaData.numWorkerNodes == 1)
		// {
		// pushAcross2(receive);
		// return true;
		// }

		if (isAllAny(receive.parent().children().get(0)) && isAllAny(receive.parent().children().get(1)))
		{
			if (containsNoSend(receive.parent().children().get(0)) && containsNoSend(receive.parent().children().get(1)))
			{
				final ArrayList<TableScanOperator> left = getTableOperators(receive.parent().children().get(0));
				final ArrayList<TableScanOperator> right = getTableOperators(receive.parent().children().get(1));
				final int node = left.get(0).getNode();
				for (final TableScanOperator table : left)
				{
					table.setNode(node);
					setNodeForSend(table, node);
				}
				for (final TableScanOperator table : right)
				{
					table.setNode(node);
					setNodeForSend(table, node);
				}

				pushAcross2(receive);
				return true;
			}
		}

		if (isAllAny(receive))
		{
			if (!containsNoSend(receive))
			{
				return false;
			}

			final ProductOperator parent = (ProductOperator)receive.parent();
			boolean onRight;
			if (parent.children().get(0).equals(receive))
			{
				onRight = false;
			}
			else
			{
				onRight = true;
			}
			parent.removeChild(receive);
			final Operator op = parent.children().get(0);
			parent.removeChild(op);
			final Operator grandParent = parent.parent();
			grandParent.removeChild(parent);
			Operator leftTree;
			CNFFilter cnf = null;
			try
			{
				if (receive.children().size() == 1)
				{
					leftTree = receive.children().get(0).children().get(0);
					if (leftTree instanceof TableScanOperator)
					{
						cnf = ((TableScanOperator)leftTree).getCNFForParent(((TableScanOperator)leftTree).firstParent());
					}
					receive.children().get(0).removeChild(leftTree);
				}
				else if (receive.children().size() == 0)
				{
					throw new Exception("Receive operator with no children");
				}
				else
				{
					leftTree = new UnionOperator(false, meta);
					for (final Operator child : (ArrayList<Operator>)receive.children().clone())
					{
						if (child.children().size() == 0)
						{
							throw new Exception("Send operator with no children");
						}
						for (Operator childOfSend : child.children())
						{
							child.removeChild(childOfSend);
							leftTree.add(childOfSend); // FIXED
						}
					}
				}
				makeLocal(op, parent, leftTree, onRight, grandParent, cnf);
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				throw e;
			}
			return true;
		}
		else
		{
			return false;
		}
	}

	private boolean handleSemi(NetworkReceiveOperator receive) throws Exception
	{
		// if (MetaData.numWorkerNodes == 1)
		// {
		// pushAcross2(receive);
		// return true;
		// }

		// HRDBMSWorker.logger.debug("Handle semi");
		if (isAllAny(receive.parent().children().get(0)) && isAllAny(receive.parent().children().get(1)))
		{
			// HRDBMSWorker.logger.debug("Both are any");
			if (containsNoSend(receive.parent().children().get(0)) && containsNoSend(receive.parent().children().get(1)))
			{
				final ArrayList<TableScanOperator> left = getTableOperators(receive.parent().children().get(0));
				final ArrayList<TableScanOperator> right = getTableOperators(receive.parent().children().get(1));
				final int node = left.get(0).getNode();
				for (final TableScanOperator table : left)
				{
					table.setNode(node);
					setNodeForSend(table, node);
				}
				for (final TableScanOperator table : right)
				{
					table.setNode(node);
					setNodeForSend(table, node);
				}

				pushAcross2(receive);
				return true;
			}
		}

		if (isAllAny(receive))
		{
			// HRDBMSWorker.logger.debug("1 is any");
			if (!containsNoSend(receive))
			{
				return false;
			}
			final SemiJoinOperator parent = (SemiJoinOperator)receive.parent();
			boolean onRight;
			if (parent.children().get(0).equals(receive))
			{
				onRight = false;
			}
			else
			{
				onRight = true;
			}

			if (!onRight)
			{
				return false;
			}
			parent.removeChild(receive);
			final Operator op = parent.children().get(0);
			parent.removeChild(op);
			final Operator grandParent = parent.parent();
			grandParent.removeChild(parent);
			Operator leftTree;
			CNFFilter cnf = null;
			try
			{
				if (receive.children().size() == 1)
				{
					leftTree = receive.children().get(0).children().get(0);
					if (leftTree instanceof TableScanOperator)
					{
						cnf = ((TableScanOperator)leftTree).getCNFForParent(((TableScanOperator)leftTree).firstParent());
					}
					receive.children().get(0).removeChild(leftTree);
				}
				else if (receive.children().size() == 0)
				{
					throw new Exception("Receive operator with no children");
				}
				else
				{
					leftTree = new UnionOperator(false, meta);
					for (final Operator child : (ArrayList<Operator>)receive.children().clone())
					{
						if (child.children().size() == 0)
						{
							throw new Exception("Send operator with no children");
						}
						for (Operator childOfSend : child.children())
						{
							child.removeChild(childOfSend);
							leftTree.add(childOfSend); // FIXED
						}
					}
				}
				makeLocal(op, parent, leftTree, onRight, grandParent, cnf);
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				throw e;
			}
			return false;
		}
		else
		{
			// HRDBMSWorker.logger.debug("None are any");
			final SemiJoinOperator parent = (SemiJoinOperator)receive.parent();
			if (!containsNoSend(parent.children().get(0)) || !containsNoSend(parent.children().get(1)))
			{
				// HRDBMSWorker.logger.debug("Both sides must not have sends, but at least 1 of them does");
				return false;
			}
			final ArrayList<String> lefts = parent.getLefts();
			final ArrayList<String> rights = parent.getRights();
			int i = 0;
			final HashSet<String> tables = new HashSet<String>();
			while (i < lefts.size())
			{
				String temp = meta.getTableForCol(lefts.get(i), parent);
				if (temp == null)
				{
					// HRDBMSWorker.logger.debug("Could not figure out table for "
					// + lefts.get(i));
					return false;
				}
				tables.add(temp);
				temp = meta.getTableForCol(rights.get(i), parent);
				if (temp == null)
				{
					// HRDBMSWorker.logger.debug("Could not figure out table for "
					// + rights.get(i));
					return false;
				}
				tables.add(temp);
				i++;
			}

			if (tables.size() != 2)
			{
				// HRDBMSWorker.logger.debug("More than 2 tables");
				return false;
			}

			final ArrayList<PartitionMetaData> pmetas = new ArrayList<PartitionMetaData>(tables.size());
			for (String table : tables)
			{
				String schema = table.substring(0, table.indexOf('.'));
				table = table.substring(table.indexOf('.') + 1);
				pmetas.add(meta.getPartMeta(schema, table, tx));
			}

			ArrayList<ArrayList<String>> lhash = getCurrentHash(parent.children().get(0));
			ArrayList<ArrayList<String>> rhash = getCurrentHash(parent.children().get(1));

			if (pmetas.get(0).noNodeGroupSet() && pmetas.get(1).noNodeGroupSet())
			{
				// HRDBMSWorker.logger.debug("No node groups");
				// HRDBMSWorker.logger.debug("Lefts = " + lefts);
				// HRDBMSWorker.logger.debug("Rights = " + rights);
				// HRDBMSWorker.logger.debug("Lhash = " + lhash);
				// HRDBMSWorker.logger.debug("Rhash = " + rhash);
				if (!needsRehash(lhash, lefts) && !needsRehash(rhash, rights))
				{
					pushAcross2(receive);
					return true;
				}
				else if (!needsRehash(lhash, rights) && !needsRehash(rhash, lefts))
				{
					pushAcross2(receive);
					return true;
				}
				else
				{
					// HRDBMSWorker.logger.debug("Returning false");
					return false;
				}
			}
			else
			{
				return false;
			}
		}
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

		return false;
	}

	private boolean handleUnion(NetworkReceiveOperator receive) throws Exception
	{
		if (receive.parent().children().size() == 1)
		{
			pushAcross(receive);
			return true;
		}

		boolean ok = true;
		for (Operator child : receive.parent().children())
		{
			if (!isAllAny(child) || !containsNoSend(child))
			{
				ok = false;
				break;
			}
		}

		if (!ok)
		{
			if (((UnionOperator)receive.parent()).isDistinct())
			{
				return false;
			}

			Operator parent = receive.parent();
			for (Operator child : parent.children())
			{
				if (child.getClass() != NetworkReceiveOperator.class)
				{
					return false;
				}
			}

			Operator grandParent = parent.parent();
			parent.removeChild(receive);
			grandParent.add(receive);

			for (Operator child : parent.children())
			{
				parent.removeChild(child);
				for (Operator grandChild : child.children())
				{
					child.removeChild(grandChild);
					receive.add(grandChild);
				}
			}

			return true;
		}

		final ArrayList<TableScanOperator> tables = new ArrayList<TableScanOperator>();
		for (Operator child : receive.parent().children())
		{
			tables.addAll(getTableOperators(child));
		}

		final int node = tables.get(0).getNode();
		for (final TableScanOperator table : tables)
		{
			table.setNode(node);
			setNodeForSend(table, node);
		}

		pushAcross2(receive);
		return true;
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
					int node = Math.abs(ThreadLocalRandom.current().nextInt()) % MetaData.numWorkerNodes;
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
		}
	}

	private void makeHierarchical2(NetworkReceiveOperator op) throws Exception
	{
		// NetworkHashAndSend -> NetworkHashReceive
		ArrayList<NetworkHashReceiveOperator> lreceives = new ArrayList<NetworkHashReceiveOperator>();
		ArrayList<NetworkReceiveOperator> lreceives2 = new ArrayList<NetworkReceiveOperator>();
		ArrayList<NetworkHashReceiveOperator> rreceives = new ArrayList<NetworkHashReceiveOperator>();
		ArrayList<NetworkReceiveOperator> rreceives2 = new ArrayList<NetworkReceiveOperator>();
		ArrayList<NetworkHashAndSendOperator> lsends2 = new ArrayList<NetworkHashAndSendOperator>();
		ArrayList<NetworkHashAndSendOperator> rsends2 = new ArrayList<NetworkHashAndSendOperator>();
		int lstart = 0;
		int rstart = 0;
		for (final Operator child : op.children())
		{
			if (child.children().get(0).children().get(0) instanceof NetworkHashReceiveOperator)
			{
				lreceives.add((NetworkHashReceiveOperator)child.children().get(0).children().get(0));
			}
			if (child.children().size() > 1)
			{
				if (child.children().get(0).children().get(1) instanceof NetworkHashReceiveOperator)
				{
					rreceives.add((NetworkHashReceiveOperator)child.children().get(0).children().get(1));
				}
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

		final ArrayList<NetworkHashAndSendOperator> rsends = new ArrayList<NetworkHashAndSendOperator>();
		if (dor)
		{
			for (final Operator o : rreceives.get(0).children())
			{
				rsends.add((NetworkHashAndSendOperator)o);
			}
		}

		if (dol)
		{
			if (lsends.size() > Phase3.MAX_INCOMING_CONNECTIONS)
			{
				int numMiddle = Phase3.MAX_INCOMING_CONNECTIONS;
				int lstarting = getStartingNode(numMiddle);
				lstart = lstarting;
				int numPerMiddle = lsends.size() / numMiddle;
				if (lsends.size() % numMiddle != 0)
				{
					numPerMiddle++;
				}

				int count = 0;
				NetworkReceiveOperator current = new NetworkReceiveOperator(meta);
				lreceives2.add(current);
				current.setNode(lstarting);
				for (NetworkHashAndSendOperator send : lsends)
				{
					Operator child = send.children().get(0);
					send.removeChild(child);
					NetworkSendOperator newSend = new NetworkSendOperator(child.getNode(), meta);
					newSend.add(child);
					current.add(newSend);
					count++;

					if (count == numPerMiddle)
					{
						lstarting++;
						current = new NetworkReceiveOperator(meta);
						lreceives2.add(current);
					}
				}
			}
		}

		if (dor)
		{
			if (rsends.size() > Phase3.MAX_INCOMING_CONNECTIONS)
			{
				int numMiddle = Phase3.MAX_INCOMING_CONNECTIONS;
				int rstarting = getStartingNode(numMiddle);
				rstart = rstarting;
				int numPerMiddle = rsends.size() / numMiddle;
				if (rsends.size() % numMiddle != 0)
				{
					numPerMiddle++;
				}

				int count = 0;
				NetworkReceiveOperator current = new NetworkReceiveOperator(meta);
				rreceives2.add(current);
				current.setNode(rstarting);
				for (NetworkHashAndSendOperator send : rsends)
				{
					Operator child = send.children().get(0);
					send.removeChild(child);
					NetworkSendOperator newSend = new NetworkSendOperator(child.getNode(), meta);
					newSend.add(child);
					current.add(newSend);
					count++;

					if (count == numPerMiddle)
					{
						rstarting++;
						current = new NetworkReceiveOperator(meta);
						rreceives2.add(current);
					}
				}
			}
		}

		if (lreceives2.size() > 0)
		{
			for (NetworkReceiveOperator r : lreceives2)
			{
				makeHierarchical(r);
				NetworkHashAndSendOperator send = new NetworkHashAndSendOperator(lsends.get(0).getHashCols(), lsends.get(0).getID(), lstart, lreceives.size(), meta);
				send.add(r);
				send.setNode(r.getNode());
				lsends2.add(send);
			}
		}

		if (rreceives2.size() > 0)
		{
			for (NetworkReceiveOperator r : rreceives2)
			{
				makeHierarchical(r);
				NetworkHashAndSendOperator send = new NetworkHashAndSendOperator(rsends.get(0).getHashCols(), rsends.get(0).getID(), rstart, rreceives.size(), meta);
				send.add(r);
				send.setNode(r.getNode());
				rsends2.add(send);
			}
		}

		if (lsends2.size() > 0)
		{
			for (NetworkHashReceiveOperator r : lreceives)
			{
				Operator parent = r.parent();
				parent.removeChild(r);
				NetworkHashReceiveOperator newReceive = r.clone();
				for (NetworkHashAndSendOperator s : lsends2)
				{
					newReceive.add(s);
				}

				parent.add(newReceive);
			}
		}

		if (rsends2.size() > 0)
		{
			for (NetworkHashReceiveOperator r : rreceives)
			{
				Operator parent = r.parent();
				parent.removeChild(r);
				NetworkHashReceiveOperator newReceive = r.clone();
				for (NetworkHashAndSendOperator s : rsends2)
				{
					newReceive.add(s);
				}

				parent.add(newReceive);
			}
		}
	}

	private void makeLocal(Operator tree1, Operator local, Operator tree2, boolean onRight, Operator grandParent, CNFFilter cnf) throws Exception
	{
		// Driver.printTree(0, root);
		// System.out.println("makeLocal()");
		boolean doAdd = false;
		final ArrayList<Operator> inserts = new ArrayList<Operator>();
		final ArrayList<Operator> finals = new ArrayList<Operator>();
		Operator insert = tree1;
		while (true)
		{
			if (insert instanceof NetworkReceiveOperator)
			{
				inserts.addAll(insert.children());
				break;
			}
			else if (insert instanceof NetworkSendOperator)
			{
				if (insert.children().get(0) instanceof NetworkReceiveOperator)
				{
					insert = insert.children().get(0);
				}
				else
				{
					finals.add(insert.children().get(0));
					break;
				}
			}
			else
			{
				finals.add(insert);
				break;
			}
		}

		while (inserts.size() != 0)
		{
			insert = inserts.get(0);
			inserts.remove(0);
			while (true)
			{
				if (insert instanceof NetworkReceiveOperator)
				{
					inserts.addAll(insert.children());
					break;
				}
				else if (insert instanceof NetworkSendOperator)
				{
					if (insert.children().get(0) instanceof NetworkReceiveOperator)
					{
						insert = insert.children().get(0);
					}
					else
					{
						finals.add(insert.children().get(0));
						break;
					}
				}
				else
				{
					finals.add(insert);
					break;
				}
			}
		}

		for (final Operator op : finals)
		{
			Operator parent = null;
			if (op instanceof TableScanOperator)
			{
				parent = ((TableScanOperator)op).firstParent();
			}
			else
			{
				parent = op.parent();
			}

			if (parent != null)
			{
				parent.removeChild(op);
			}
			final Operator local2 = local.clone();
			try
			{
				if (onRight)
				{
					local2.add(op);
					final Operator cloneTree = cloneTree(tree2);
					local2.add(cloneTree);
					if (cnf != null)
					{
						((TableScanOperator)cloneTree).setCNFForParent(local2, cnf);
					}
					if (parent != null)
					{
						parent.add(local2);
						// setNodeForTablesAndSends(local2, parent.getNode());
						Operator next = parent.parent();
						while (!next.equals(tree1))
						{
							next.removeChild(parent);
							next.add(parent);
							parent = next;
							next = parent.parent();
						}

						next.removeChild(parent);
						next.add(parent);
						doAdd = true;
					}
					else
					{
						// setNodeForTablesAndSends(local2.children().get(0),
						// getNode(local2.children().get(1)));
						grandParent.add(local2);
					}
				}
				else
				{
					final Operator cloneTree = cloneTree(tree2);
					local2.add(cloneTree);
					local2.add(op);
					if (cnf != null)
					{
						((TableScanOperator)cloneTree).setCNFForParent(local2, cnf);
					}
					if (parent != null)
					{
						parent.add(local2);
						// setNodeForTablesAndSends(local2, parent.getNode());
						Operator next = parent.parent();
						while (!next.equals(tree1))
						{
							next.removeChild(parent);
							next.add(parent);
							parent = next;
							next = parent.parent();
						}

						next.removeChild(parent);
						next.add(parent);
						doAdd = true;
					}
					else
					{
						// setNodeForTablesAndSends(local2.children().get(1),
						// getNode(local2.children().get(0)));
						grandParent.add(local2);
					}
				}
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				throw e;
			}
		}

		if (doAdd)
		{
			try
			{
				grandParent.add(tree1);
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				throw e;
			}
		}
		// Driver.printTree(0, root);
	}

	private boolean needsRehash(ArrayList<ArrayList<String>> current, ArrayList<String> toHash)
	{
		for (ArrayList<String> hash : current)
		{
			if (toHash.equals(hash) && hash.size() > 0)
			{
				return false;
			}
		}

		return true;
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

	private void pruneTree(Operator op, int node)
	{
		if (op instanceof NetworkSendOperator)
		{
			if (op.getNode() != node)
			{
				op.parent().removeChild(op);
			}
		}
		else
		{
			for (final Operator o : (ArrayList<Operator>)op.children().clone())
			{
				pruneTree(o, node);
			}
		}
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
	}

	private void pushAcross2(NetworkReceiveOperator receive) throws Exception
	{
		final Operator parent = receive.parent();
		final Operator grandParent = parent.parent();
		if (grandParent == null)
		{
			Exception e = new Exception("Isolated tree");
			HRDBMSWorker.logger.error("Parent = " + parent, e);
			HRDBMSWorker.logger.error("Grandparent = " + grandParent);
			HRDBMSWorker.logger.error("Isolated tree");
			throw e;
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
		parent.removeChild(receive);
		try
		{
			grandParent.removeChild(parent);
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.error("Parent = " + parent, e);
			HRDBMSWorker.logger.error("Grandparent = " + grandParent);
			HRDBMSWorker.logger.error("Isolated tree");
			throw e;
		}

		try
		{
			for (final Map.Entry entry : send2Child.entrySet())
			{
				final Operator pClone = cloneTree(parent);
				pClone.add((Operator)entry.getValue());
				if (send2CNF.containsKey(entry.getKey()))
				{
					((TableScanOperator)entry.getValue()).setCNFForParent(pClone, send2CNF.get(entry.getKey()));
				}
				((Operator)entry.getKey()).add(pClone);
				receive.removeChild((Operator)entry.getKey());
				receive.add((Operator)entry.getKey());
				// setNodeForTablesAndSends(pClone,
				// getNode((Operator)entry.getValue()));
				pruneTree(pClone, getNode((Operator)entry.getValue()));
			}
			grandParent.add(receive);
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.error("", e);
			throw e;
		}
	}

	private void pushDownGB(Operator op) throws Exception
	{
		if (op instanceof MultiOperator)
		{
			Operator child = op.children().get(0);
			if (child instanceof HashJoinOperator)
			{
				MultiOperator mop = (MultiOperator)op;
				HashJoinOperator hjop = (HashJoinOperator)child;
				doPushDownGB(mop, hjop);
			}
		}

		for (Operator o : op.children())
		{
			pushDownGB(o);
		}
	}

	private void pushUpReceives() throws Exception
	{
		boolean workToDo = true;
		ArrayList<NetworkReceiveOperator> completed = new ArrayList<NetworkReceiveOperator>();
		while (workToDo)
		{
			workToDo = false;
			ArrayList<NetworkReceiveOperator> receives = getReceives(root);
			for (final NetworkReceiveOperator receive : receives)
			{
				while (true)
				{
					if (!treeContains(root, receive))
					{
						break;
					}
					assignNodes(root, -1);
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
								break;
							}
						}
						pushAcross(receive);
						// HRDBMSWorker.logger.debug("Push across");
						// Phase1.printTree(root, 0);
						workToDo = true;
					}
					else if (receive instanceof NetworkHashReceiveAndMergeOperator || receive instanceof NetworkHashReceiveOperator || receive instanceof NetworkReceiveAndMergeOperator)
					{
						break;
					}
					else if (op instanceof SortOperator)
					{
						workToDo = true;
						if (!handleSort(receive))
						{
							// HRDBMSWorker.logger.debug("Sort false");
							// Phase1.printTree(root, 0);
							break;
						}
						else
						{
							// HRDBMSWorker.logger.debug("Sort true");
							// Phase1.printTree(root, 0);
						}
					}
					else if (op instanceof UnionOperator)
					{
						if (!handleUnion(receive))
						{
							break;
						}
						workToDo = true;
					}
					else if (op instanceof ExceptOperator)
					{
						if (!handleExcept(receive))
						{
							break;
						}
						workToDo = true;
					}
					else if (op instanceof IntersectOperator)
					{
						if (!handleIntersect(receive))
						{
							break;
						}
						workToDo = true;
					}
					else if (op instanceof MultiOperator)
					{
						if (!completed.contains(receive))
						{
							workToDo = true;
							if (!handleMulti(receive))
							{
								completed.add(receive);
								// HRDBMSWorker.logger.debug("Multi false");
								// Phase1.printTree(root, 0);
								break;
							}
							else
							{
								// HRDBMSWorker.logger.debug("Multi true");
								// Phase1.printTree(root, 0);
							}
						}
						else
						{
							break;
						}
					}
					else if (op instanceof ProductOperator)
					{
						if (!handleProduct(receive))
						{
							break;
						}
						workToDo = true;
						// HRDBMSWorker.logger.debug("Product");
						// Phase1.printTree(root, 0);
					}
					else if (op instanceof HashJoinOperator)
					{
						if (!handleHash(receive))
						{
							break;
						}
						workToDo = true;
						// HRDBMSWorker.logger.debug("Hash join");
						// Phase1.printTree(root, 0);
					}
					else if (op instanceof NestedLoopJoinOperator)
					{
						if (!handleNested(receive))
						{
							break;
						}
						workToDo = true;
						// HRDBMSWorker.logger.debug("Nested loop");
						// Phase1.printTree(root, 0);
					}
					else if (op instanceof SemiJoinOperator)
					{
						if (!handleSemi(receive))
						{
							break;
						}
						workToDo = true;
						// HRDBMSWorker.logger.debug("Semi");
						// Phase1.printTree(root, 0);
					}
					else if (op instanceof AntiJoinOperator)
					{
						if (!handleAnti(receive))
						{
							break;
						}
						workToDo = true;
						// HRDBMSWorker.logger.debug("Anti");
						// Phase1.printTree(root, 0);
					}
					else if (op instanceof TopOperator)
					{
						if (!completed.contains(receive))
						{
							workToDo = true;
							completed.add(receive);
							if (!handleTop(receive))
							{
								// HRDBMSWorker.logger.debug("Top false");
								// Phase1.printTree(root, 0);
								break;
							}
							else
							{
								// HRDBMSWorker.logger.debug("Top true");
								// Phase1.printTree(root, 0);
							}
						}
						else
						{
							break;
						}
					}
					else
					{
						break;
					}
				}
			}
		}
	}

	private void removeLocalSendReceive(Operator op) throws Exception
	{
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
					removeLocalSendReceive(child);
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
							removeLocalSendReceive(s);
						}

						return;
					}
				}

				final Operator parent = op.parent();
				parent.removeChild(op);
				final Operator union = new UnionOperator(false, meta);
				try
				{
					if (op.children().size() == 0)
					{
						throw new Exception("Receive operator with no children");
					}
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
				removeLocalSendReceive(union);
			}
		}
		else
		{
			for (final Operator o : (ArrayList<Operator>)op.children().clone())
			{
				removeLocalSendReceive(o);
			}
		}
	}

	/*
	 * private void sanityCheck(Operator op, int node) throws Exception { if (op
	 * instanceof NetworkSendOperator) { node = op.getNode(); for (Operator o :
	 * op.children()) { sanityCheck(o, node); } } else { if (op.getNode() !=
	 * node) { HRDBMSWorker.logger.debug("P3 sanity check failed");
	 * Phase1.printTree(root, 0); throw new Exception("P3 sanity check failed");
	 * }
	 * 
	 * for (Operator o : op.children()) { sanityCheck(o, node); } } }
	 */

	private void setNodeForSend(TableScanOperator table, int node)
	{
		Operator op = table.firstParent();
		while (!(op instanceof NetworkSendOperator))
		{
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
}