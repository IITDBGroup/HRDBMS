package com.exascale.optimizer;

import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.optimizer.MetaData.PartitionMetaData;
import com.exascale.tables.Transaction;

/** Employs standard optimizations based on assumption of a single-node execution plan. */
public final class Phase1
{
	private final RootOperator root;
	private boolean pushdownHadResults;
	private final Transaction tx;
	private final Operator clone;
	private final MetaData meta = new MetaData();
	private final HashSet<Operator> done2 = new HashSet<Operator>();
	private final HashSet<Operator> done = new HashSet<Operator>();
	private final HashMap<String, String> ftLookup = new HashMap<String, String>();
	public HashMap<ArrayList<Filter>, Double> likelihoodCache = new HashMap<ArrayList<Filter>, Double>();

	public Phase1(final RootOperator root, final Transaction tx) throws Exception
	{
		this.root = root;
		this.tx = tx;
		this.clone = cloneTree(root);
	}

	private static boolean anyHope(Operator op)
	{
		while (op instanceof SelectOperator)
		{
			op = op.children().get(0);
		}

		if (op instanceof TableScanOperator)
		{
			return false;
		}

		return true;
	}

	private static Operator getSubtreeForCol(final String col, final ArrayList<Operator> subtrees)
	{
		for (final Operator op : subtrees)
		{
			if (op.getCols2Pos().containsKey(col))
			{
				return op;
			}
		}

		HRDBMSWorker.logger.debug("Can't find subtree for column " + col + " in...");
		for (final Operator op : subtrees)
		{
			HRDBMSWorker.logger.debug(op.getCols2Pos().keySet());
		}
		return null;
	}

	public long card(final Operator op) throws Exception
	{
		if (op instanceof HashJoinOperator)
		{
			long retval = cardHJO(op);
			if (retval < 1)
			{
				retval = 1;
			}

			return retval;
		}

		if (op instanceof IntersectOperator)
		{
			long retval = cardSetI(op);
			if (retval < 1)
			{
				retval = 1;
			}

			return retval;
		}

		if (op instanceof MultiOperator)
		{
			long retval = cardMO(op);
			if (retval < 1)
			{
				retval = 1;
			}

			return retval;
		}

		if (op instanceof NestedLoopJoinOperator)
		{
			long retval = cardNL(op);
			if (retval < 1)
			{
				retval = 1;
			}

			return retval;
		}

		if (op instanceof NetworkReceiveOperator)
		{
			long retval = cardRX(op);
			if (retval < 1)
			{
				retval = 1;
			}

			return retval;
		}

		if (op instanceof NetworkHashAndSendOperator)
		{
			long retval = card(op.children().get(0)) / ((NetworkHashAndSendOperator)op).parents().size();
			if (retval < 1)
			{
				retval = 1;
			}

			return retval;
		}

		if (op instanceof NetworkSendRROperator)
		{
			long retval = card(op.children().get(0)) / ((NetworkSendRROperator)op).parents().size();
			if (retval < 1)
			{
				retval = 1;
			}

			return retval;
		}

		if (op instanceof ProductOperator)
		{
			long retval = cardX(op);
			if (retval < 1)
			{
				retval = 1;
			}

			return retval;
		}

		if (op instanceof SelectOperator)
		{
			long retval = (long)(((SelectOperator)op).likelihood(root, tx) * card(op.children().get(0)));
			if (retval < 1)
			{
				retval = 1;
			}

			return retval;
		}

		if (op instanceof TopOperator)
		{
			long retval = cardTop(op);
			if (retval < 1)
			{
				retval = 1;
			}

			return retval;
		}

		if (op instanceof UnionOperator)
		{
			long retval = cardUnion(op);
			if (retval < 1)
			{
				retval = 1;
			}

			return retval;
		}

		if (op instanceof TableScanOperator)
		{
			long retval = cardTSO(op);
			if (retval < 1)
			{
				retval = 1;
			}

			return retval;
		}

		long retval = card(op.children().get(0));
		if (retval < 1)
		{
			retval = 1;
		}

		return retval;
	}

	public void optimize() throws Exception
	{
		// HRDBMSWorker.logger.debug("Upon entering P1:"); //DEBUG
		// printTree(root, 0); //DEBUG
		// Driver.printTree(0, root); //DEBUG
		do
		{
			pushdownHadResults = false;
			pushDownSelects2(root, System.currentTimeMillis());
		} while (pushdownHadResults);

		reorderProducts(root);

		do
		{
			pushdownHadResults = false;
			pushDownSelects(root, System.currentTimeMillis());
		} while (pushdownHadResults);

		combineProductsAndSelects(root);
		mergeSelectsAndTableScans(root);
		removeUnneededOps(root);
		projectForSemiAnti(root);
		pushDownProjects();

		// HRDBMSWorker.logger.debug("Upon exiting P1:"); //DEBUG
		// printTree(root, 0); //DEBUG
	}

	private double adjust(final Operator left, final Operator right, final double r) throws Exception
	{
		double retval = r;
		if (card(left) < card(right))
		{
			Operator op = left;
			while (true)
			{
				if (op instanceof SelectOperator)
				{
					retval *= ((SelectOperator)op).likelihood(tx);
				}
				else if (op instanceof ProjectOperator)
				{
				}
				else if (op instanceof RenameOperator)
				{
				}
				else if (op instanceof ReorderOperator)
				{
				}
				else if (op instanceof TableScanOperator)
				{
					// HRDBMSWorker.logger.debug("Adjust is returning " +
					// retval);
					return retval;
				}
				else
				{
					// HRDBMSWorker.logger.debug("Adjust is returning " + r);
					return r;
				}

				op = op.children().get(0);
			}
		}
		else
		{
			Operator op = right;
			while (true)
			{
				if (op instanceof SelectOperator)
				{
					retval *= ((SelectOperator)op).likelihood(tx);
				}
				else if (op instanceof ProjectOperator)
				{
				}
				else if (op instanceof RenameOperator)
				{
				}
				else if (op instanceof ReorderOperator)
				{
				}
				else if (op instanceof TableScanOperator)
				{
					// HRDBMSWorker.logger.debug("Adjust is returning " +
					// retval);
					return retval;
				}
				else
				{
					// HRDBMSWorker.logger.debug("Adjust is returning " + r);
					return r;
				}

				op = op.children().get(0);
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
		return retval;
	}

	private long cardMO(final Operator op) throws Exception
	{
		final long groupCard = meta.getColgroupCard(((MultiOperator)op).getKeys(), root, tx, op);
		if (groupCard > card(op.children().get(0)))
		{
			return card(op.children().get(0));
		}

		return groupCard;
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

	private long cardTSO(final Operator op) throws Exception
	{
		final PartitionMetaData pmd = meta.getPartMeta(((TableScanOperator)op).getSchema(), ((TableScanOperator)op).getTable(), tx);
		final int numNodes = pmd.getNumNodes();
		return (long)((1.0 / numNodes) * MetaData.getTableCard(((TableScanOperator)op).getSchema(), ((TableScanOperator)op).getTable(), tx));
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
		return retval;
	}

	private long cardX(final Operator op) throws Exception
	{
		final long temp = card(op.children().get(0)) * card(op.children().get(1));
		if (temp < 0)
		{
			return Long.MAX_VALUE;
		}
		else
		{
			return temp;
		}
	}

	private Operator cloneTree(final Operator op) throws Exception
	{
		final Operator clone = op.clone();
		for (final Operator o : op.children())
		{
			try
			{
				final Operator child = cloneTree(o);
				clone.add(child);
				clone.setChildPos(op.getChildPos());
				// TODO Do we need to do the same logic as below for ExternalTableScanOperator?
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

	private void combineProductsAndSelects(final Operator op) throws Exception
	{
		if (op instanceof SelectOperator)
		{
			final ArrayList<Filter> filters = ((SelectOperator)op).getFilter();
			boolean isJoin = true;
			for (final Filter filter : filters)
			{
				if (!filter.leftIsColumn())
				{
					isJoin = false;
					break;
				}

				if (!filter.rightIsColumn())
				{
					isJoin = false;
					break;
				}
			}

			if (!isJoin)
			{
				for (final Operator child2 : op.children())
				{
					combineProductsAndSelects(child2);
				}

				return;
			}

			Operator child = op.children().get(0);
			while (child instanceof SelectOperator)
			{
				child = child.children().get(0);
			}

			if (child instanceof JoinOperator)
			{
				final JoinOperator join = JoinOperator.manufactureJoin((JoinOperator)child, (SelectOperator)op, op.getMeta());
				final ArrayList<Operator> children = (ArrayList<Operator>)child.children().clone();
				for (final Operator child2 : children)
				{
					child.removeChild(child2);
				}

				final Operator productParent = child.parent();
				if (productParent != op)
				{
					productParent.removeChild(child);
				}

				for (final Operator child2 : children)
				{
					join.add(child2);
				}

				if (productParent != op)
				{
					productParent.add(join);
				}

				final Operator selectChild = op.children().get(0);
				op.removeChild(selectChild);
				final Operator selectParent = op.parent();
				selectParent.removeChild(op);
				if (selectChild instanceof JoinOperator)
				{
					selectParent.add(join);
					combineProductsAndSelects(join);
				}
				else
				{
					selectParent.add(selectChild);
					combineProductsAndSelects(selectChild);
				}
			}
			else
			{
				for (final Operator child2 : op.children())
				{
					combineProductsAndSelects(child2);
				}
			}
		}
		else
		{
			while (true)
			{
				try
				{
					for (final Operator child2 : op.children())
					{
						combineProductsAndSelects(child2);
					}

					break;
				}
				catch (final ConcurrentModificationException e)
				{
					continue;
				}
			}
		}
	}

	private ArrayList<Operator> getLeaves(final Operator op)
	{
		if (op.children().size() == 0)
		{
			final ArrayList<Operator> retval = new ArrayList<Operator>(1);
			retval.add(op);
			return retval;
		}

		final ArrayList<Operator> retval = new ArrayList<Operator>();
		for (final Operator o : op.children())
		{
			retval.addAll(getLeaves(o));
		}

		return retval;
	}

	private SelectOperator getMostPromisingSelect(final ArrayList<SelectOperator> selects, final ArrayList<Operator> subtrees, final Operator current) throws Exception
	{
		double minLikelihood = Double.MAX_VALUE;
		SelectOperator minSelect = null;
		if (current == null)
		{
			// HRDBMSWorker.logger.debug("Current is null");
			final HashSet<SubtreePair> pairs = new HashSet<SubtreePair>();
			outer: for (final SelectOperator select : selects)
			{
				Double likelihood = likelihoodCache.get(select.getFilter());
				if (likelihood == null)
				{
					likelihood = meta.likelihood(new ArrayList<Filter>(select.getFilter()), tx, clone);
					likelihoodCache.put(select.getFilter(), likelihood);
				}
				// HRDBMSWorker.logger.debug("Initial likelihood for " + select
				// + " is " + likelihood);
				final ArrayList<Filter> filters = select.getFilter();
				Operator left = null;
				Operator right = null;
				final ArrayList<String> lefts = new ArrayList<String>();
				final ArrayList<String> rights = new ArrayList<String>();
				for (final Filter filter : filters)
				{
					if (filter.leftIsColumn() && filter.rightIsColumn())
					{
						left = getSubtreeForCol(filter.leftColumn(), subtrees);
						right = getSubtreeForCol(filter.rightColumn(), subtrees);

						final SubtreePair pair = new SubtreePair(left, right);
						if (pairs.contains(pair))
						{
							continue outer;
						}
						else
						{
							pairs.add(pair);
						}

						if (filters.size() == 1 && filter.op().equals("E"))
						{
							lefts.add(filter.leftColumn());
							rights.add(filter.rightColumn());
						}

						final long cl = card(left);
						final long cr = card(right);
						// HRDBMSWorker.logger.debug("CL = " + cl + ", CR = " +
						// cr);
						likelihood *= cl;
						likelihood *= cr;
						likelihood = adjust(left, right, likelihood);
						// HRDBMSWorker.logger.debug("After card/adjust
						// likelihood for " + select + " is " + likelihood);
						break;
					}
				}

				/*
				 * if (left != null) { for (SelectOperator s2 : selects) { if
				 * (s2 != select) { ArrayList<Filter> filters2 = s2.getFilter();
				 * Operator l2 = null; Operator r2 = null; for (Filter f2 :
				 * filters2) { if (f2.leftIsColumn() && f2.rightIsColumn()) { l2
				 * = getSubtreeForCol(f2.leftColumn(), subtrees); r2 =
				 * getSubtreeForCol(f2.rightColumn(), subtrees); if ((l2 == left
				 * && r2 == right) || (l2 == right && r2 == left)) { if (l2 ==
				 * left && r2 == right) { if (filters2.size() == 1 &&
				 * f2.op().equals("E") && lefts.size() > 0) {
				 * lefts.add(f2.leftColumn()); rights.add(f2.rightColumn()); }
				 * else { lefts.clear(); rights.clear(); } } else { if
				 * (filters2.size() == 1 && f2.op().equals("E") && lefts.size()
				 * > 0) { rights.add(f2.leftColumn());
				 * lefts.add(f2.rightColumn()); } else { lefts.clear();
				 * rights.clear(); } } Double temp =
				 * likelihoodCache.get(s2.getFilter()); if (temp == null) { temp
				 * = meta.likelihood(new ArrayList<Filter>(s2.getFilter()), tx,
				 * clone); likelihoodCache.put(s2.getFilter(), temp); }
				 * likelihood *= temp; break; } } } } } }
				 */

				/*
				 * if (lefts.size() > 0 && rights.size() > 0) { String leftTable
				 * = getTable(left); String rightTable = getTable(right);
				 *
				 * if (leftTable != null && rightTable != null) { String lschema
				 * = leftTable.substring(0, leftTable.indexOf('.')); String
				 * ltable = leftTable.substring(leftTable.indexOf('.') + 1);
				 * String rschema = rightTable.substring(0,
				 * rightTable.indexOf('.')); String rtable =
				 * rightTable.substring(rightTable.indexOf('.') + 1);
				 * PartitionMetaData lpmd = meta.getPartMeta(lschema, ltable,
				 * tx); PartitionMetaData rpmd = meta.getPartMeta(rschema,
				 * rtable, tx);
				 *
				 * if (lpmd.noNodeGroupSet() && rpmd.noNodeGroupSet()) { if
				 * (lpmd.getNodeHash() != null &&
				 * lefts.equals(lpmd.getNodeHash()) && rpmd.getNodeHash() !=
				 * null && rights.equals(rpmd.getNodeHash())) { if (likelihood <
				 * minColocated) { minColocated = likelihood; minSelect2 =
				 * select; } } } } }
				 */

				// HRDBMSWorker.logger.debug("Estimated join cardinality = " +
				// likelihood); //DEBUG

				if (likelihood < minLikelihood)
				{
					minLikelihood = likelihood;
					minSelect = select;
				}
			}

			/*
			 * if (minColocated <= minLikelihood * 2) { //
			 * HRDBMSWorker.logger.debug("Chose " + minColocated); //DEBUG
			 * minSelect = minSelect2; } else { // HRDBMSWorker.logger.debug(
			 * "Chose " + minLikelihood); //DEBUG }
			 */

			selects.remove(minSelect);
			// HRDBMSWorker.logger.debug("Most promising select is " + minSelect
			// + " with a score of " + minLikelihood);
			return minSelect;
		}

		// HRDBMSWorker.logger.debug("Current is not null");
		final HashSet<SubtreePair> pairs = new HashSet<SubtreePair>();
		outer2: for (final SelectOperator select : selects)
		{
			Double likelihood = Double.MAX_VALUE;
			Operator left = null;
			Operator right = null;
			final ArrayList<Filter> filters = select.getFilter();
			for (final Filter filter : filters)
			{
				if (filter.leftIsColumn() && filter.rightIsColumn())
				{
					if (current.getCols2Pos().containsKey(filter.leftColumn()))
					{
						likelihood = likelihoodCache.get(select.getFilter());
						if (likelihood == null)
						{
							likelihood = meta.likelihood(new ArrayList<Filter>(select.getFilter()), tx, clone);
							likelihoodCache.put(select.getFilter(), likelihood);
						}
						// HRDBMSWorker.logger.debug("Initial likelihood for " +
						// select + " is " + likelihood);
						left = getSubtreeForCol(filter.leftColumn(), subtrees);
						right = getSubtreeForCol(filter.rightColumn(), subtrees);
						final SubtreePair pair = new SubtreePair(left, right);
						if (pairs.contains(pair))
						{
							continue outer2;
						}
						else
						{
							pairs.add(pair);
						}
						final long cl = card(left);
						final long cr = card(right);
						// HRDBMSWorker.logger.debug("CL = " + cl + ", CR = " +
						// cr);
						likelihood *= cl;
						likelihood *= cr;
						likelihood = adjust(left, right, likelihood);
						// HRDBMSWorker.logger.debug("After card/adjust
						// likelihood for " + select + " is " + likelihood);
					}
					else if (current.getCols2Pos().containsKey(filter.rightColumn()))
					{
						likelihood = likelihoodCache.get(select.getFilter());
						if (likelihood == null)
						{
							likelihood = meta.likelihood(new ArrayList<Filter>(select.getFilter()), tx, clone);
							likelihoodCache.put(select.getFilter(), likelihood);
						}
						// HRDBMSWorker.logger.debug("Initial likelihood for " +
						// select + " is " + likelihood);
						left = getSubtreeForCol(filter.leftColumn(), subtrees);
						right = getSubtreeForCol(filter.rightColumn(), subtrees);
						final SubtreePair pair = new SubtreePair(left, right);
						if (pairs.contains(pair))
						{
							continue outer2;
						}
						else
						{
							pairs.add(pair);
						}
						final long cl = card(left);
						final long cr = card(right);
						// HRDBMSWorker.logger.debug("CL = " + cl + ", CR = " +
						// cr);
						likelihood *= cl;
						likelihood *= cr;
						likelihood = adjust(left, right, likelihood);
						// HRDBMSWorker.logger.debug("After card/adjust
						// likelihood for " + select + " is " + likelihood);
					}

					break;
				}
			}

			/*
			 * if (left != null) { for (SelectOperator s2 : selects) { if (s2 !=
			 * select) { ArrayList<Filter> filters2 = s2.getFilter(); Operator
			 * l2 = null; Operator r2 = null; for (Filter f2 : filters2) { if
			 * (f2.leftIsColumn() && f2.rightIsColumn()) { l2 =
			 * getSubtreeForCol(f2.leftColumn(), subtrees); r2 =
			 * getSubtreeForCol(f2.rightColumn(), subtrees); if ((l2 == left &&
			 * r2 == right) || (l2 == right && r2 == left)) { Double temp =
			 * likelihoodCache.get(s2.getFilter()); if (temp == null) { temp =
			 * meta.likelihood(new ArrayList<Filter>(s2.getFilter()), tx,
			 * clone); likelihoodCache.put(s2.getFilter(), temp); }
			 *
			 * likelihood *= temp; break; } } } } } }
			 */

			// HRDBMSWorker.logger.debug("Estimated join cardinality = " +
			// likelihood); //DEBUG

			if (likelihood < minLikelihood)
			{
				minLikelihood = likelihood;
				minSelect = select;
			}
		}

		if (minSelect != null)
		{
			// HRDBMSWorker.logger.debug("Chose " + minLikelihood); //DEBUG
			// HRDBMSWorker.logger.debug("Most promising select is " + minSelect
			// + " with a score of " + minLikelihood);
			selects.remove(minSelect);
			return minSelect;
		}

		pairs.clear();
		outer3: for (final SelectOperator select : selects)
		{
			Double likelihood = likelihoodCache.get(select.getFilter());
			if (likelihood == null)
			{
				likelihood = meta.likelihood(new ArrayList<Filter>(select.getFilter()), tx, clone);
				likelihoodCache.put(select.getFilter(), likelihood);
			}
			// HRDBMSWorker.logger.debug("Initial likelihood for " + select + "
			// is " + likelihood);
			final ArrayList<Filter> filters = select.getFilter();
			Operator left = null;
			Operator right = null;
			final ArrayList<String> lefts = new ArrayList<String>();
			final ArrayList<String> rights = new ArrayList<String>();
			for (final Filter filter : filters)
			{
				if (filter.leftIsColumn() && filter.rightIsColumn())
				{
					left = getSubtreeForCol(filter.leftColumn(), subtrees);
					right = getSubtreeForCol(filter.rightColumn(), subtrees);
					final SubtreePair pair = new SubtreePair(left, right);
					if (pairs.contains(pair))
					{
						continue outer3;
					}
					else
					{
						pairs.add(pair);
					}

					if (filters.size() == 1 && filter.op().equals("E"))
					{
						lefts.add(filter.leftColumn());
						rights.add(filter.rightColumn());
					}

					final long cl = card(left);
					final long cr = card(right);
					// HRDBMSWorker.logger.debug("CL = " + cl + ", CR = " + cr);
					likelihood *= cl;
					likelihood *= cr;
					likelihood = adjust(left, right, likelihood);
					// HRDBMSWorker.logger.debug("After card/adjust likelihood
					// for " + select + " is " + likelihood);
					break;
				}
			}

			/*
			 * if (left != null) { for (SelectOperator s2 : selects) { if (s2 !=
			 * select) { ArrayList<Filter> filters2 = s2.getFilter(); Operator
			 * l2 = null; Operator r2 = null; for (Filter f2 : filters2) { if
			 * (f2.leftIsColumn() && f2.rightIsColumn()) { l2 =
			 * getSubtreeForCol(f2.leftColumn(), subtrees); r2 =
			 * getSubtreeForCol(f2.rightColumn(), subtrees); if ((l2 == left &&
			 * r2 == right) || (l2 == right && r2 == left)) { if (l2 == left &&
			 * r2 == right) { if (filters2.size() == 1 && f2.op().equals("E") &&
			 * lefts.size() > 0) { lefts.add(f2.leftColumn());
			 * rights.add(f2.rightColumn()); } else { lefts.clear();
			 * rights.clear(); } } else { if (filters2.size() == 1 &&
			 * f2.op().equals("E") && lefts.size() > 0) {
			 * rights.add(f2.leftColumn()); lefts.add(f2.rightColumn()); } else
			 * { lefts.clear(); rights.clear(); } } Double temp =
			 * likelihoodCache.get(s2.getFilter()); if (temp == null) { temp =
			 * meta.likelihood(new ArrayList<Filter>(s2.getFilter()), tx,
			 * clone); likelihoodCache.put(s2.getFilter(), temp); } likelihood
			 * *= temp; break; } } } } } }
			 */

			/*
			 * if (lefts.size() > 0 && rights.size() > 0) { String leftTable =
			 * getTable(left); String rightTable = getTable(right);
			 *
			 * if (leftTable != null && rightTable != null) { String lschema =
			 * leftTable.substring(0, leftTable.indexOf('.')); String ltable =
			 * leftTable.substring(leftTable.indexOf('.') + 1); String rschema =
			 * rightTable.substring(0, rightTable.indexOf('.')); String rtable =
			 * rightTable.substring(rightTable.indexOf('.') + 1);
			 * PartitionMetaData lpmd = meta.getPartMeta(lschema, ltable, tx);
			 * PartitionMetaData rpmd = meta.getPartMeta(rschema, rtable, tx);
			 *
			 * if (lpmd.noNodeGroupSet() && rpmd.noNodeGroupSet()) { if
			 * (lpmd.getNodeHash() != null && lefts.equals(lpmd.getNodeHash())
			 * && rpmd.getNodeHash() != null &&
			 * rights.equals(rpmd.getNodeHash())) { if (likelihood <
			 * minColocated) { minColocated = likelihood; minSelect2 = select; }
			 * } } } }
			 */

			// HRDBMSWorker.logger.debug("Estimated join cardinality = " +
			// likelihood); //DEBUG

			if (likelihood < minLikelihood)
			{
				minLikelihood = likelihood;
				minSelect = select;
			}
		}

		/*
		 * if (minColocated <= minLikelihood * 2) { //
		 * HRDBMSWorker.logger.debug("Chose " + minColocated); //DEBUG minSelect
		 * = minSelect2; } else { // HRDBMSWorker.logger.debug("Chose " +
		 * minLikelihood); //DEBUG }
		 */

		selects.remove(minSelect);
		// HRDBMSWorker.logger.debug("Most promising select is " + minSelect + "
		// with a score of " + minLikelihood);
		return minSelect;
	}

	private void getReferences(final Operator o, final HashSet<String> references)
	{
		references.addAll(o.getReferences());
		for (final Operator op : o.children())
		{
			getReferences(op, references);
		}
	}

	private void mergeSelectsAndTableScans(Operator op) throws Exception
	{
		try
		{
			if (op instanceof SelectOperator)
			{
				// System.out.println("Found a SelectOperator");
				final Operator child = op.children().get(0);
				if (child instanceof TableScanOperator)
				{
					// System.out.println("That had a TableScanOperator as a
					// child");
					final Operator parent = op;
					op = child;
					final Operator grandParent = parent.parent();
					((TableScanOperator)op).addFilter(((SelectOperator)parent).getFilter(), grandParent, grandParent.parent(), tx);
					parent.removeChild(op);
					grandParent.removeChild(parent);
					grandParent.add(op);
					mergeSelectsAndTableScans(grandParent);
					pushdownHadResults = true;
				}
				else
				{
					for (final Operator child2 : op.children())
					{
						mergeSelectsAndTableScans(child2);
					}
				}
			}
			else
			{
				int i = 0;
				while (i < op.children().size())
				{
					final Operator child = op.children().get(i);
					mergeSelectsAndTableScans(child);
					i++;
				}
			}
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.error("", e);
			throw e;
		}
	}

	private void projectForSemiAnti(final Operator op) throws Exception
	{
		if (op instanceof SemiJoinOperator)
		{
			final HashSet<HashMap<Filter, Filter>> hshm = ((SemiJoinOperator)op).getHSHM();
			final HashSet<String> cols = new HashSet<String>();
			for (final HashMap<Filter, Filter> hm : hshm)
			{
				for (final Filter f : hm.keySet())
				{
					if (f.leftIsColumn())
					{
						final String l = f.leftColumn();
						cols.add(l);
					}

					if (f.rightIsColumn())
					{
						final String r = f.rightColumn();
						cols.add(r);
					}
				}
			}

			final ArrayList<String> toProject = new ArrayList<String>();
			for (final String col : cols)
			{
				if (op.children().get(1).getCols2Pos().keySet().contains(col))
				{
					toProject.add(col);
				}
			}

			if (toProject.size() < op.children().get(1).getCols2Pos().size())
			{
				final ProjectOperator project = new ProjectOperator(toProject, meta);
				final Operator right = op.children().get(1);
				op.removeChild(right);
				project.add(right);
				op.add(project);
			}
		}
		else if (op instanceof AntiJoinOperator)
		{
			final HashSet<HashMap<Filter, Filter>> hshm = ((AntiJoinOperator)op).getHSHM();
			final HashSet<String> cols = new HashSet<String>();
			for (final HashMap<Filter, Filter> hm : hshm)
			{
				for (final Filter f : hm.keySet())
				{
					if (f.leftIsColumn())
					{
						final String l = f.leftColumn();
						cols.add(l);
					}

					if (f.rightIsColumn())
					{
						final String r = f.rightColumn();
						cols.add(r);
					}
				}
			}

			final ArrayList<String> toProject = new ArrayList<String>();
			for (final String col : cols)
			{
				if (op.children().get(1).getCols2Pos().keySet().contains(col))
				{
					toProject.add(col);
				}
			}

			if (toProject.size() < op.children().get(1).getCols2Pos().size())
			{
				final ProjectOperator project = new ProjectOperator(toProject, meta);
				final Operator right = op.children().get(1);
				op.removeChild(right);
				project.add(right);
				op.add(project);
			}
		}
		else if (op instanceof TableScanOperator)
		{
			return;
		}
		else
		{
			for (final Operator o : op.children())
			{
				projectForSemiAnti(o);
			}
		}
	}

	private void pushDownProjects() throws Exception
	{
		final HashSet<String> references = new HashSet<String>();
		for (final Operator o : root.children())
		{
			getReferences(o, references);
		}

		final ArrayList<Operator> leaves = getLeaves(root);

		for (final Operator op : leaves)
		{
			if (op instanceof TableScanOperator)
			{
				try
				{
					final TableScanOperator table = (TableScanOperator)op;
					final HashMap<String, Integer> cols2Pos = table.getCols2Pos();
					final TreeMap<Integer, String> pos2Col = table.getPos2Col();
					final ArrayList<String> needed = new ArrayList<String>(references.size());
					for (final String col : references)
					{
						if (cols2Pos.containsKey(col))
						{
							if (table.getAlias() == null || table.getAlias().equals(""))
							{
								needed.add(col);
							}
							else
							{
								needed.add(table.getAlias() + "." + col.substring(col.indexOf('.') + 1));
							}
						}
					}

					if (needed.size() == 0)
					{
						if (table.getAlias() == null || table.getAlias().equals(""))
						{
							needed.add(pos2Col.get(0));
						}
						else
						{
							final String col = pos2Col.get(0);
							needed.add(table.getAlias() + "." + col.substring(col.indexOf('.') + 1));
						}
					}
					table.setNeededCols(needed);
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.error("", e);
					throw e;
				}
			}
		}

		final ArrayList<Operator> leaves2 = new ArrayList<Operator>();
		for (Operator op : leaves)
		{
			while (!(op instanceof RootOperator))
			{
				try
				{
					if (op instanceof AbstractTableScanOperator)
					{
						final ArrayList<Operator> parents = (ArrayList<Operator>)((AbstractTableScanOperator)op).parents().clone();
						for (final Operator op2 : parents)
						{
							op2.removeChild(op);
							op2.add(op);
							int i = 1;
							while (i < ((AbstractTableScanOperator)op).parents().size())
							{
								leaves2.add(((AbstractTableScanOperator)op).parents().get(i));
								i++;
							}
						}

						op = ((AbstractTableScanOperator)op).parents().get(0);
					}
					else
					{
						final Operator parent = op.parent();
						parent.removeChild(op);
						parent.add(op);
						op = parent;
					}
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.error("", e);
					throw e;
				}
			}

			for (Operator op2 : leaves2)
			{
				while (!(op2 instanceof RootOperator))
				{
					try
					{
						final Operator parent = op2.parent();
						parent.removeChild(op2);
						parent.add(op2);
						op2 = parent;
					}
					catch (final Exception e)
					{
						HRDBMSWorker.logger.error("", e);
						throw e;
					}
				}
			}
		}
	}

	private void pushDownSelects(final Operator op, final long time) throws Exception
	{
		if (System.currentTimeMillis() - time > 2000)
		{
			return;
		}

		if (op instanceof SelectOperator)
		{
			if (!anyHope(op))
			{
				return;
			}
			if (done.contains(op))
			{
				pushDownSelects(op.children().get(0), time);
				return;
			}

			final ArrayList<Operator> children = op.children();
			final Operator child = children.get(0);

			// look at opportunity to push down across child
			if (child instanceof TableScanOperator)
			{
				done.add(op);
				return;
			}

			int i = 0;
			while (i < child.children().size())
			// for (Operator grandChild : grandChildren)
			{
				final Operator grandChild = child.children().get(i);
				// can I push down to be a parent of this operator?
				final ArrayList<String> references = ((SelectOperator)op).getReferences();
				if (child instanceof RenameOperator)
				{
					((RenameOperator)child).reverseUpdateReferences(references);
				}

				if (child instanceof HashJoinOperator)
				{
					((HashJoinOperator)child).reverseUpdateReferences(references, i);
				}

				final HashMap<String, Integer> gcCols2Pos = grandChild.getCols2Pos();
				boolean ok = true;
				for (final String reference : references)
				{
					if (!gcCols2Pos.containsKey(reference))
					{
						// System.out.println("Choosing not to push down " + op
						// + " because " + grandChild + " cols2Pos is " +
						// gcCols2Pos + " which does not contain " + reference);
						ok = false;
						break;
					}
				}

				if (ok)
				{
					final Operator parent = op.parent();

					if (parent != null)
					{
						parent.removeChild(op);
						op.removeChild(child);
					}

					Operator newOp = null;
					if (child instanceof RenameOperator)
					{
						// revereseUpdate must create a new SelectOperator
						newOp = ((RenameOperator)child).reverseUpdateSelectOperator((SelectOperator)op);
					}
					else if (child instanceof HashJoinOperator)
					{
						newOp = ((HashJoinOperator)child).reverseUpdateSelectOperator((SelectOperator)op, i);
					}
					else
					{
						newOp = ((SelectOperator)op).clone();
					}

					child.removeChild(grandChild);
					try
					{
						newOp.add(grandChild);
						child.add(newOp);

						if (parent != null)
						{
							parent.add(child);
						}
					}
					catch (final Exception e)
					{
						HRDBMSWorker.logger.error("", e);
						throw e;
					}

					pushdownHadResults = true;

					pushDownSelects(newOp, time);
				}
				else
				{
					done.add(op);
					final Operator selectChild = op.children().get(0);
					if (selectChild != null)
					{
						pushDownSelects(selectChild, time);
					}
				}

				i++;
			}
		}
		else
		{
			int i = 0;
			while (i < op.children().size())
			{
				final Operator child = op.children().get(i);
				pushDownSelects(child, time);
				i++;
			}
		}
	}

	private void pushDownSelects2(final Operator op, final long time) throws Exception
	{
		if (System.currentTimeMillis() - time > 2000)
		{
			return;
		}

		if (op instanceof SelectOperator)
		{
			if (!anyHope(op))
			{
				return;
			}
			if (done2.contains(op))
			{
				pushDownSelects2(op.children().get(0), time);
				return;
			}

			final SelectOperator sop = (SelectOperator)op;
			final ArrayList<Filter> filters = sop.getFilter();

			for (final Filter filter : filters)
			{
				if (filter.leftIsColumn() && filter.rightIsColumn())
				{
					String ltable = ftLookup.get(filter.leftColumn());
					if (ltable == null)
					{
						ltable = meta.getTableForCol(filter.leftColumn(), root);
						ftLookup.put(filter.leftColumn(), ltable);
					}
					String rtable = ftLookup.get(filter.rightColumn());
					if (rtable == null)
					{
						rtable = meta.getTableForCol(filter.rightColumn(), root);
						ftLookup.put(filter.rightColumn(), rtable);
					}

					if (ltable != null && rtable != null && ltable.contains(".") && rtable.contains(".") && ltable.length() > 2 && rtable.length() > 2 && ltable.equals(rtable))
					{
					}
					else
					{
						final Operator child = op.children().get(0);
						if (child instanceof ProductOperator)
						{
							done2.add(op);
							pushDownSelects2(child, time);
							return;
						}
					}
				}
			}

			final ArrayList<Operator> children = op.children();
			final Operator child = children.get(0);

			// look at opportunity to push down across child
			if (child instanceof TableScanOperator)
			{
				done2.add(op);
				return;
			}

			int i = 0;
			while (i < child.children().size())
			// for (Operator grandChild : grandChildren)
			{
				final Operator grandChild = child.children().get(i);
				// can I push down to be a parent of this operator?
				final ArrayList<String> references = ((SelectOperator)op).getReferences();
				if (child instanceof RenameOperator)
				{
					((RenameOperator)child).reverseUpdateReferences(references);
				}

				if (child instanceof HashJoinOperator)
				{
					((HashJoinOperator)child).reverseUpdateReferences(references, i);
				}

				final HashMap<String, Integer> gcCols2Pos = grandChild.getCols2Pos();
				boolean ok = true;
				for (final String reference : references)
				{
					if (!gcCols2Pos.containsKey(reference))
					{
						// System.out.println("Choosing not to push down " + op
						// + " because " + grandChild + " cols2Pos is " +
						// gcCols2Pos + " which does not contain " + reference);
						ok = false;
						break;
					}
				}

				if (ok)
				{
					final Operator parent = op.parent();

					if (parent != null)
					{
						parent.removeChild(op);
						op.removeChild(child);
					}

					Operator newOp = null;
					if (child instanceof RenameOperator)
					{
						// revereseUpdate must create a new SelectOperator
						newOp = ((RenameOperator)child).reverseUpdateSelectOperator((SelectOperator)op);
					}
					else if (child instanceof HashJoinOperator)
					{
						newOp = ((HashJoinOperator)child).reverseUpdateSelectOperator((SelectOperator)op, i);
					}
					else
					{
						newOp = ((SelectOperator)op).clone();
					}

					child.removeChild(grandChild);
					try
					{
						newOp.add(grandChild);
						child.add(newOp);

						if (parent != null)
						{
							parent.add(child);
						}
					}
					catch (final Exception e)
					{
						HRDBMSWorker.logger.error("", e);
						throw e;
					}

					pushdownHadResults = true;
					pushDownSelects2(newOp, time);
				}
				else
				{
					done2.add(op);
					final Operator selectChild = op.children().get(0);
					if (selectChild != null)
					{
						pushDownSelects2(selectChild, time);
					}
				}

				i++;
			}
		}
		else
		{
			int i = 0;
			while (i < op.children().size())
			{
				final Operator child = op.children().get(i);
				pushDownSelects2(child, time);
				i++;
			}
		}
	}

	private void removeUnneededOps(final Operator op) throws Exception
	{
		if (!(op instanceof MultiOperator))
		{
			for (final Operator o : op.children())
			{
				removeUnneededOps(o);
			}

			return;
		}

		final MultiOperator mop = (MultiOperator)op;
		final Operator child = mop.children().get(0);
		if (child instanceof ReorderOperator || child instanceof ProjectOperator || child instanceof SortOperator)
		{
			mop.removeChild(child);
			final Operator grandChild = child.children().get(0);
			child.removeChild(grandChild);
			mop.add(grandChild);
			removeUnneededOps(mop);
		}
		else
		{
			for (final Operator o : mop.children())
			{
				removeUnneededOps(o);
			}
		}
	}

	private void reorderProducts(final Operator op) throws Exception
	{
		try
		{
			if (op instanceof SelectOperator)
			{
				final ArrayList<SelectOperator> selects = new ArrayList<SelectOperator>();
				selects.add((SelectOperator)op);
				reorderProducts(op.children().get(0), selects, op);
			}
			else
			{
				int i = 0;
				while (i < op.children().size())
				{
					reorderProducts(op.children().get(i));
					i++;
				}
			}
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.error("", e);
			throw e;
		}
	}

	private void reorderProducts(Operator op, final ArrayList<SelectOperator> selects, final Operator top) throws Exception
	{
		while (op instanceof SelectOperator)
		{
			selects.add((SelectOperator)op);
			op = op.children().get(0);
		}

		if (!(op instanceof ProductOperator))
		{
			int i = 0;
			while (i < op.children().size())
			{
				reorderProducts(op.children().get(i));
				i++;
			}

			return;
		}

		final ArrayList<Operator> subtrees = new ArrayList<Operator>();
		final ArrayList<Operator> queued = new ArrayList<Operator>();
		queued.add(op);
		while (queued.size() != 0)
		{
			op = queued.remove(0);
			while (true)
			{
				final Operator left = op.children().get(0);
				final Operator right = op.children().get(1);
				if ((!(left instanceof ProductOperator)) && (!(right instanceof ProductOperator)))
				{
					subtrees.add(left);
					subtrees.add(right);
					op.removeChild(left);
					op.removeChild(right);
					break;
				}
				else if (!(left instanceof ProductOperator))
				{
					subtrees.add(left);
					op.removeChild(left);
					op.removeChild(right);
					op = right;
				}
				else if (!(right instanceof ProductOperator))
				{
					subtrees.add(right);
					op.removeChild(right);
					op.removeChild(left);
					op = left;
				}
				else
				{
					op = left;
					queued.add(right);
					op.removeChild(left);
					op.removeChild(right);
				}
			}
		}

		final ArrayList<Operator> backup = (ArrayList<Operator>)subtrees.clone();

		// stuff above top
		// string of selects
		// logically product all...
		// subtrees
		final Operator onTop = top.parent();
		for (final SelectOperator select : selects)
		{
			select.parent().removeChild(select);
		}
		selects.get(selects.size() - 1).removeChild(selects.get(selects.size() - 1).children().get(0));
		final ArrayList<SelectOperator> selectsCopy = (ArrayList<SelectOperator>)selects.clone();
		transitive(selectsCopy);

		Operator newProd = null;
		final ArrayList<SelectOperator> delay = new ArrayList<SelectOperator>();
		while (selectsCopy.size() > 0)
		{
			final SelectOperator select = getMostPromisingSelect(selectsCopy, subtrees, newProd);
			final ArrayList<Filter> filters = select.getFilter();
			int i = 0;
			Operator theLeft = null;
			Operator theRight = null;
			while (i < filters.size())
			{
				final Filter filter = filters.get(i);
				if (filter.leftIsColumn() && filter.rightIsColumn())
				{
					Operator left = null;
					Operator right = null;
					if (newProd == null)
					{
						left = getSubtreeForCol(filter.leftColumn(), subtrees);
						right = getSubtreeForCol(filter.rightColumn(), subtrees);
						if (filter.op().equals("E") && card(left) < card(right))
						{
							final Operator temp = left;
							left = right;
							right = temp;
						}
					}
					else
					{
						if (newProd.getCols2Pos().containsKey(filter.leftColumn()))
						{
							left = getSubtreeForCol(filter.leftColumn(), subtrees);
							right = getSubtreeForCol(filter.rightColumn(), subtrees);
						}
						else
						{
							right = getSubtreeForCol(filter.leftColumn(), subtrees);
							left = getSubtreeForCol(filter.rightColumn(), subtrees);
						}
					}

					if (left == null || right == null)
					{
						theLeft = null;
						break;
					}

					if (left == right)
					{
						i++;
						continue;
					}

					if (theLeft != null && theRight != null && !((left == theLeft && right == theRight) || (left == theRight && right == theLeft)))
					{
						theLeft = null;
						break;
					}

					if (theLeft == null)
					{
						theLeft = left;
						theRight = right;
					}
				}

				i++;
			}

			if (theLeft == null)
			{
				delay.add(select);
			}
			else
			{
				newProd = new ProductOperator(select.getMeta());
				newProd.add(theLeft);
				newProd.add(theRight);
				subtrees.remove(theLeft);
				subtrees.remove(theRight);
				Operator temp = select.clone();
				temp.add(newProd);
				newProd = temp;

				for (final SelectOperator s2 : (ArrayList<SelectOperator>)selectsCopy.clone())
				{
					if (s2 != select)
					{
						final ArrayList<Filter> f2s = s2.getFilter();
						boolean ok = true;
						for (final Filter f2 : f2s)
						{
							if (f2.leftIsColumn())
							{
								if (!newProd.getCols2Pos().keySet().contains(f2.leftColumn()))
								{
									ok = false;
									break;
								}
							}

							if (f2.rightIsColumn())
							{
								if (!newProd.getCols2Pos().keySet().contains(f2.rightColumn()))
								{
									ok = false;
									break;
								}
							}
						}

						if (ok)
						{
							temp = s2.clone();
							temp.add(newProd);
							newProd = temp;
							selectsCopy.remove(s2);
						}
					}
				}

				subtrees.add(newProd);
			}
		}

		while (subtrees.size() > 1)
		{
			newProd = new ProductOperator(subtrees.get(0).getMeta());
			newProd.add(subtrees.get(0));
			subtrees.remove(0);
			newProd.add(subtrees.get(0));
			subtrees.remove(0);
			subtrees.add(newProd);
		}

		// for (final SelectOperator select : selects)
		// {
		// select.add(subtrees.get(0));
		// subtrees.remove(0);
		// subtrees.add(select);
		// }

		// everything needs to be removed and readded for all the operators in
		// the rest of the top
		Operator next = subtrees.get(0);
		for (final SelectOperator sop : delay)
		{
			sop.add(next);
			next = sop;
		}
		onTop.add(next);
		Operator o = onTop.parent();
		if (o != null)
		{
			while (!(o instanceof RootOperator))
			{
				int i = 0;
				while (i < o.children().size())
				{
					final Operator child = o.children().get(i);
					o.removeChild(child);
					o.add(child);
					i++;
				}

				o = o.parent();
			}

			int i = 0;
			while (i < o.children().size())
			{
				final Operator child = o.children().get(i);
				o.removeChild(child);
				o.add(child);
				i++;
			}
		}
		// call reorderProducts() on everything that was originally in subtrees

		for (final Operator operator : backup)
		{
			reorderProducts(operator);
		}
	}

	private void transitive(final ArrayList<SelectOperator> list) throws Exception
	{
		final ConcurrentHashMap<Filter, Filter> set = new ConcurrentHashMap<Filter, Filter>();
		for (final SelectOperator op : list)
		{
			final ArrayList<Filter> ors = op.getFilter();
			if (ors.size() == 1)
			{
				final Filter f = ors.get(0);
				if (f.op().equals("E") && f.leftIsColumn() && f.rightIsColumn())
				{
					set.put(f, f);
				}
			}
		}

		boolean didWork = true;
		while (didWork)
		{
			didWork = false;
			for (final Filter f1 : set.keySet())
			{
				for (final Filter f2 : set.keySet())
				{
					if (f1.leftColumn().equals(f2.leftColumn()))
					{
						if (!f1.rightColumn().equals(f2.rightColumn()))
						{
							final Filter f3 = new Filter(f1.getRightString(), "E", f2.getRightString());
							if (!set.contains(f3) && !set.contains(new Filter(f2.getRightString(), "E", f1.getRightString())))
							{
								set.put(f3, f3);
								list.add(new SelectOperator(f3, meta));
								didWork = true;
								HRDBMSWorker.logger.debug("Added a transitive filter");
							}
						}
					}
					else if (f1.leftColumn().equals(f2.rightColumn()))
					{
						if (!f1.rightColumn().equals(f2.leftColumn()))
						{
							final Filter f3 = new Filter(f1.getRightString(), "E", f2.getLeftString());
							if (!set.contains(f3) && !set.contains(new Filter(f2.getLeftString(), "E", f1.getRightString())))
							{
								set.put(f3, f3);
								list.add(new SelectOperator(f3, meta));
								didWork = true;
								HRDBMSWorker.logger.debug("Added a transitive filter");
							}
						}
					}
					else if (f1.rightColumn().equals(f2.rightColumn()))
					{
						if (!f1.leftColumn().equals(f2.leftColumn()))
						{
							final Filter f3 = new Filter(f1.getLeftString(), "E", f2.getLeftString());
							if (!set.contains(f3) && !set.contains(new Filter(f2.getLeftString(), "E", f1.getLeftString())))
							{
								set.put(f3, f3);
								list.add(new SelectOperator(f3, meta));
								didWork = true;
								HRDBMSWorker.logger.debug("Added a transitive filter");
							}
						}
					}
					else if (f1.rightColumn().equals(f2.leftColumn()))
					{
						if (!f1.leftColumn().equals(f2.rightColumn()))
						{
							final Filter f3 = new Filter(f1.getLeftString(), "E", f2.getRightString());
							if (!set.contains(f3) && !set.contains(new Filter(f2.getRightString(), "E", f1.getLeftString())))
							{
								set.put(f3, f3);
								list.add(new SelectOperator(f3, meta));
								didWork = true;
								HRDBMSWorker.logger.debug("Added a transitive filter");
							}
						}
					}
				}
			}
		}
	}

	private class SubtreePair
	{
		private final Operator left;
		private final Operator right;

		public SubtreePair(final Operator left, final Operator right)
		{
			this.left = left;
			this.right = right;
		}

		@Override
		public boolean equals(final Object o)
		{
			if (o == null)
			{
				return false;
			}
			final SubtreePair rhs = (SubtreePair)o;

			return (this.left == rhs.left && this.right == rhs.right) || (this.left == rhs.right && this.right == rhs.left);
		}

		@Override
		public int hashCode()
		{
			return left.hashCode() + right.hashCode();
		}
	}
}
