package com.exascale.optimizer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.tables.Transaction;

public final class Phase5
{
	private final RootOperator root;

	private final MetaData meta;
	private final HashMap<Operator, Long> cCache = new HashMap<Operator, Long>();
	private Transaction tx;

	public Phase5(RootOperator root, Transaction tx)
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
			cCache.put(op, retval);
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
			long groupCard;
			if (((MultiOperator)op).getKeys().size() == 0)
			{
				groupCard = 1;
			}
			else
			{
				groupCard = meta.getColgroupCard(((MultiOperator)op).getKeys(), root, tx, op);
			}
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
			cCache.put(op, retval);
			return retval;
		}

		if (op instanceof RootOperator)
		{
			final long retval = card(op.children().get(0));
			cCache.put(op, retval);
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
			cCache.put(op, retval);
			return retval;
		}

		if (op instanceof SubstringOperator)
		{
			final long retval = card(op.children().get(0));
			cCache.put(op, retval);
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
		
		if (op instanceof DummyOperator)
		{
			return 1;
		}
		
		if (op instanceof DEMOperator)
		{
			return 0;
		}

		if (op instanceof TableScanOperator)
		{
			final HashSet<HashMap<Filter, Filter>> hshm = ((TableScanOperator)op).getHSHM();
			if (hshm != null)
			{
				if (op.children().size() == 0)
				{
					final long retval = (long)(meta.getTableCard(((TableScanOperator)op).getSchema(), ((TableScanOperator)op).getTable(), tx) * meta.likelihood(hshm, root, tx, op) * (1.0 / ((TableScanOperator)op).getNumNodes()));
					cCache.put(op, retval);
					return retval;
				}
				else
				{
					final Operator origOp = op;
					if (op.children().get(0) instanceof SortOperator)
					{
						op = op.children().get(0);
					}

					if (op.children().get(0) instanceof UnionOperator)
					{
						double sum = 0;
						for (final Operator x : op.children().get(0).children())
						{
							double l = meta.likelihood(((IndexOperator)x).getFilter(), root, tx, origOp);
							for (final Filter f : ((IndexOperator)x).getSecondary())
							{
								l *= meta.likelihood(f, root, tx, origOp);
							}

							sum += l;
						}

						if (sum > 1)
						{
							sum = 1;
						}

						op = origOp;
						final long retval = (long)(meta.likelihood(hshm, root, tx, op) * sum * (1.0 / ((TableScanOperator)op).getNumNodes()) * meta.getTableCard(((TableScanOperator)op).getSchema(), ((TableScanOperator)op).getTable(), tx));
						cCache.put(op, retval);
						return retval;
					}
					else
					{
						double z = 1;
						for (final Operator x : op.children().get(0).children())
						{
							double sum = 0;
							for (final Operator y : x.children())
							{
								double l = meta.likelihood(((IndexOperator)y).getFilter(), root, tx, origOp);
								for (final Filter f : ((IndexOperator)y).getSecondary())
								{
									l *= meta.likelihood(f, root, tx, origOp);
								}

								sum += l;
							}

							if (sum > 1)
							{
								sum = 1;
							}

							z *= sum;
						}

						op = origOp;
						final long retval = (long)(meta.likelihood(hshm, root, tx, op) * z * (1.0 / ((TableScanOperator)op).getNumNodes()) * meta.getTableCard(((TableScanOperator)op).getSchema(), ((TableScanOperator)op).getTable(), tx));
						cCache.put(op, retval);
						return retval;
					}
				}
			}

			if (op.children().size() == 0)
			{
				final long retval = (long)((1.0 / ((TableScanOperator)op).getNumNodes()) * meta.getTableCard(((TableScanOperator)op).getSchema(), ((TableScanOperator)op).getTable(), tx));
				cCache.put(op, retval);
				return retval;
			}
			else
			{
				final Operator origOp = op;
				if (op.children().get(0) instanceof SortOperator)
				{
					op = op.children().get(0);
				}

				if (op.children().get(0) instanceof UnionOperator)
				{
					double sum = 0;
					for (final Operator x : op.children().get(0).children())
					{
						double l = meta.likelihood(((IndexOperator)x).getFilter(), root, tx, origOp);
						for (final Filter f : ((IndexOperator)x).getSecondary())
						{
							l *= meta.likelihood(f, root, tx, origOp);
						}

						sum += l;
					}

					if (sum > 1)
					{
						sum = 1;
					}

					op = origOp;
					final long retval = (long)(sum * (1.0 / ((TableScanOperator)op).getNumNodes()) * meta.getTableCard(((TableScanOperator)op).getSchema(), ((TableScanOperator)op).getTable(), tx));
					cCache.put(op, retval);
					return retval;
				}
				else
				{
					double z = 1;
					for (final Operator x : op.children().get(0).children())
					{
						double sum = 0;
						for (final Operator y : x.children())
						{
							double l = meta.likelihood(((IndexOperator)y).getFilter(), root, tx, origOp);
							for (final Filter f : ((IndexOperator)y).getSecondary())
							{
								l *= meta.likelihood(f, root, tx, origOp);
							}

							sum += l;
						}

						if (sum > 1)
						{
							sum = 1;
						}

						z *= sum;
					}

					op = origOp;
					final long retval = (long)(z * (1.0 / ((TableScanOperator)op).getNumNodes()) * meta.getTableCard(((TableScanOperator)op).getSchema(), ((TableScanOperator)op).getTable(), tx));
					cCache.put(op, retval);
					return retval;
				}
			}
		}

		HRDBMSWorker.logger.error("Unknown operator in card() in Phase5: " + op.getClass());
		throw new Exception("Unknown operator in card() in Phase5: " + op.getClass());
	}

	public void optimize() throws Exception
	{
		//addIndexesToTableScans();
		//turnOffDistinctUnion(root, false);
		setCards(root);
		//addIndexesToJoins();
		setNumParents(root);
		setSpecificCoord(root);
	}
	
	private void setSpecificCoord(Operator op) throws Exception
	{
		if (op.getNode() == -1)
		{
			op.setNode(MetaData.myCoordNum());
		}
		
		for (Operator o : op.children())
		{
			setSpecificCoord(o);
		}
	}

	private void addIndexesToJoins() throws Exception
	{
		final ArrayList<Operator> x = getJoins(root);
		final HashSet<Operator> joins = new HashSet<Operator>(x);
		final ArrayList<Operator> joins2 = getEligibleJoins(joins);
		final ArrayList<Operator> joins3 = filtersEnough(joins2);
		final ArrayList<Operator> joins4 = hasIndex(joins3);
		for (final Operator j : joins4)
		{
			// System.out.println("Using index for " + j);
			if (j instanceof HashJoinOperator)
			{
				((HashJoinOperator)j).setDynamicIndex(getDynamicIndex(j));
			}
			else if (j instanceof NestedLoopJoinOperator)
			{
				((NestedLoopJoinOperator)j).setDynamicIndex(getDynamicIndex(j));
			}
			else if (j instanceof SemiJoinOperator)
			{
				((SemiJoinOperator)j).setDynamicIndex(getDynamicIndex(j));
			}
			else if (j instanceof AntiJoinOperator)
			{
				((AntiJoinOperator)j).setDynamicIndex(getDynamicIndex(j));
			}
		}
	}

	private void addIndexesToTableScans() throws Exception
	{
		final ArrayList<TableScanOperator> s = getTableScans(root);
		final HashSet<TableScanOperator> set = new HashSet<TableScanOperator>(s);

		for (final TableScanOperator table : set)
		{
			CNFFilter cnf = table.getFirstCNF();
			if (cnf != null)
			{
				cnf = cnf.clone();
				table.setCNFForParent(table.firstParent(), cnf);
				if (cnf != null)
				{
					useIndexes(table.getSchema(), table.getTable(), cnf, table);
					if (table.children().size() > 0)
					{
						reuseIndexes(table);
						compoundIndexes(table);
						reuseCompoundIndexes(table);
						doubleCheckCNF(table);
						final boolean indexOnly = indexOnly(table);
						// System.out.println("Using indexes.");
						// printIndexes(table);
						if (!indexOnly)
						{
							addSort(table);
						}
						correctForDevices(table);
					}
				}

				clearOpParents(table);
				cleanupOrderedFilters(table);
			}
		}
	}

	private void addIndexForJoin(Operator op, Index index) throws Exception
	{
		Operator o = op;
		if (o instanceof SortOperator)
		{
			o = o.children().get(0);
		}

		if (o instanceof IntersectOperator)
		{
			final IndexOperator io = new IndexOperator(index, meta);
			try
			{
				o.add(io);
				io.setNode(o.getNode());
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				throw e;
			}

			return;
		}

		final IntersectOperator intersect = new IntersectOperator(meta);
		final Operator parent = o.parent();
		parent.removeChild(o);
		try
		{
			intersect.add(o);
			intersect.setNode(o.getNode());
			final UnionOperator union = new UnionOperator(true, meta);
			final IndexOperator io = new IndexOperator(index, meta);
			union.add(io);
			intersect.add(union);
			union.setNode(intersect.getNode());
			io.setNode(union.getNode());
			parent.add(intersect);
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.error("", e);
			throw e;
		}

		// cCache.clear();
	}

	private void addSort(TableScanOperator table) throws Exception
	{
		final Operator child = table.children().get(0);
		table.removeChild(child);
		final ArrayList<String> cols = new ArrayList<String>(1);
		cols.add(child.getPos2Col().get(0));
		final ArrayList<Boolean> orders = new ArrayList<Boolean>(1);
		orders.add(true);
		final SortOperator sort = new SortOperator(cols, orders, meta);
		try
		{
			sort.add(child);
			sort.setNode(child.getNode());
			table.add(sort);
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.error("", e);
			throw e;
		}

		// cCache.clear();
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
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				throw e;
			}
		}

		return clone;
	}

	private void compoundIndexes(TableScanOperator table) throws Exception
	{
		if (table.children().get(0) instanceof UnionOperator)
		{
			return;
		}

		final HashSet<String> cols = new HashSet<String>();
		for (final Operator op : table.children().get(0).children())
		{
			if (op.children().size() > 1)
			{
				continue;
			}

			final IndexOperator index = (IndexOperator)op.children().get(0);
			cols.addAll(index.getReferencedCols());
		}

		Index index = meta.getBestCompoundIndex(cols, table.getSchema(), table.getTable(), tx);
		while (cols.size() > 0 && index != null)
		{
			for (final Operator op : table.children().get(0).children())
			{
				if (op.children().size() > 1)
				{
					continue;
				}

				final IndexOperator index2 = (IndexOperator)op.children().get(0);
				if (index.getCols().containsAll(index2.getReferencedCols()))
				{
					if (!index.getFileName().equals(index2.getFileName()))
					{
						// replace existing index with new one
						final Index newIndex = index.clone();
						newIndex.setCondition(index2.getFilter());
						for (final Filter filter : index2.getSecondary())
						{
							newIndex.addSecondaryFilter(filter);
						}

						index2.setIndex(newIndex);
					}
				}
			}

			cols.removeAll(index.getCols());
			index = meta.getBestCompoundIndex(cols, table.getSchema(), table.getTable(), tx);
		}
	}

	private void correctForDevices(TableScanOperator table) throws Exception
	{
		for (final Operator child : (ArrayList<Operator>)table.children().clone())
		{
			table.removeChild(child);
			final ArrayList<Integer> devices = table.getDeviceList();
			for (final int device : devices)
			{
				final Operator clone = cloneTree(child);
				try
				{
					table.add(clone);
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.error("", e);
					throw e;
				}

				setDevice(clone, device);
				table.setChildForDevice(device, clone);
			}
		}

		// cCache.clear();
	}

	private void doubleCheckCNF(TableScanOperator table)
	{
		final HashMap<String, Index> cols2Indexes = getCols2Indexes(table);
		final CNFFilter cnf = table.getCNFForParent(table.firstParent());
		final HashSet<HashMap<Filter, Filter>> hshm = cnf.getHSHM();
		for (final HashMap<Filter, Filter> hm : (HashSet<HashMap<Filter, Filter>>)hshm.clone())
		{
			if (hm.size() > 1)
			{
				continue;
			}

			for (final Filter f : hm.keySet())
			{
				final ArrayList<String> references = new ArrayList<String>(2);
				if (f.leftIsColumn())
				{
					references.add(f.leftColumn());
				}

				if (f.rightIsColumn())
				{
					references.add(f.rightColumn());
				}

				Index indexToUse = null;
				boolean doIt = true;
				for (final String col : references)
				{
					if (indexToUse == null)
					{
						if (cols2Indexes.containsKey(col))
						{
							indexToUse = cols2Indexes.get(col);
						}
						else
						{
							doIt = false;
							break;
						}
					}
					else
					{
						if (cols2Indexes.containsKey(col))
						{
							if (indexToUse.equals(cols2Indexes.get(col)))
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
					indexToUse.addSecondaryFilter(f);
					hshm.remove(hm);
				}
			}
		}

		cnf.setHSHM(hshm);
	}

	private ArrayList<Operator> filtersEnough(ArrayList<Operator> joins) throws Exception
	{
		final ArrayList<Operator> retval = new ArrayList<Operator>();
		for (final Operator op : joins)
		{
			if (op instanceof HashJoinOperator)
			{
				long leftCard = card(op.children().get(0));
				final long rightCard = card(op.children().get(1));
				// System.out.println("Right card = " + rightCard);
				if (leftCard == 0)
				{
					leftCard = 1;
				}
				// System.out.println("Left card = " + leftCard);
				//System.out.println(meta.likelihood(((HashJoinOperator)op).getHSHM(), root) * leftCard);
				// System.out.println("Before multiplier: " +
				// meta.likelihood(((HashJoinOperator)op).getHSHM(), root));
				if (leftCard < rightCard && meta.likelihood(((HashJoinOperator)op).getHSHM(), root, tx, op) * leftCard <= 1.0 / 400.0)
				{
					retval.add(op);
				}
				// else if (memoryUsageTooHigh(op.children().get(1),
				// op.children().get(0)))
				// {
				// retval.add(op);
				// System.out.println("Memory usage too high for hash table. " +
				// op);
				// }
			}
			else if (op instanceof NestedLoopJoinOperator)
			{
				final HashSet<HashMap<Filter, Filter>> hshm = ((NestedLoopJoinOperator)op).getHSHM();
				boolean doIt = true;
				for (final HashMap<Filter, Filter> hm : hshm)
				{
					if (hm.size() > 1)
					{
						doIt = false;
						break;
					}
				}

				if (doIt)
				{
					final long rightCard = card(op.children().get(1));
					// System.out.println("Right card = " + rightCard);
					long leftCard = card(op.children().get(0));
					if (leftCard == 0)
					{
						leftCard = 1;
					}
					// System.out.println("Left card = " + leftCard);
					//System.out.println(meta.likelihood(hshm, root) * leftCard);
					// System.out.println("Before multiplier: " +
					// meta.likelihood(hshm, root));
					if (leftCard < rightCard && meta.likelihood(hshm, root, tx, op) * leftCard <= 1.0 / 400.0)
					{
						retval.add(op);
					}
					// else if (memoryUsageTooHigh(op.children().get(1),
					// op.children().get(0)))
					// {
					// retval.add(op);
					// System.out.println("Memory usage too high for hash table. "
					// + op);
					// }
				}
			}
			else if (op instanceof SemiJoinOperator)
			{
				final HashSet<HashMap<Filter, Filter>> hshm = ((SemiJoinOperator)op).getHSHM();
				boolean doIt = true;
				for (final HashMap<Filter, Filter> hm : hshm)
				{
					if (hm.size() > 1)
					{
						doIt = false;
						break;
					}
				}

				if (doIt)
				{
					final long rightCard = card(op.children().get(1));
					// System.out.println("Right card = " + rightCard);
					long leftCard = card(op.children().get(0));
					if (leftCard == 0)
					{
						leftCard = 1;
					}
					// System.out.println("Left card = " + leftCard);
					//System.out.println(meta.likelihood(hshm, root) * leftCard);
					// System.out.println("Before multiplier: " +
					// meta.likelihood(hshm, root));
					if (leftCard < rightCard && meta.likelihood(hshm, root, tx, op) * leftCard <= 1.0 / 400.0)
					{
						retval.add(op);
					}
					// else if (memoryUsageTooHigh(op.children().get(1),
					// op.children().get(0)))
					// {
					// retval.add(op);
					// System.out.println("Memory usage too high for hash table. "
					// + op);
					// }
				}
			}
			else if (op instanceof AntiJoinOperator)
			{
				final HashSet<HashMap<Filter, Filter>> hshm = ((AntiJoinOperator)op).getHSHM();
				boolean doIt = true;
				for (final HashMap<Filter, Filter> hm : hshm)
				{
					if (hm.size() > 1)
					{
						doIt = false;
						break;
					}
				}

				if (doIt)
				{
					final long rightCard = card(op.children().get(1));
					// System.out.println("Right card = " + rightCard);
					long leftCard = card(op.children().get(0));
					if (leftCard == 0)
					{
						leftCard = 1;
					}
					// System.out.println("Left card = " + leftCard);
					//System.out.println(meta.likelihood(hshm, root) * leftCard);
					// System.out.println("Before multiplier: " +
					// meta.likelihood(hshm, root));
					if (leftCard < rightCard && meta.likelihood(hshm, root, tx, op) * leftCard <= 1.0 / 400.0)
					{
						retval.add(op);
					}
					// else if (memoryUsageTooHigh(op.children().get(1),
					// op.children().get(0)))
					// {
					// retval.add(op);
					// System.out.println("Memory usage too high for hash table. "
					// + op);
					// }
				}
			}
		}

		return retval;
	}

	private HashMap<String, Index> getCols2Indexes(Operator op)
	{
		final HashMap<String, Index> retval = new HashMap<String, Index>();
		if (op instanceof IndexOperator)
		{
			final Index index = ((IndexOperator)op).getIndex();
			final ArrayList<String> cols = index.getCols();
			for (final String col : cols)
			{
				retval.put(col, index);
			}

			return retval;
		}
		else
		{
			for (final Operator o : op.children())
			{
				retval.putAll(getCols2Indexes(o));
			}

			return retval;
		}
	}

	private ArrayList<Index> getDynamicIndex(Operator op) throws Exception
	{
		HashSet<HashMap<Filter, Filter>> hshm = null;
		final ArrayList<Index> retval = new ArrayList<Index>();
		if (op instanceof HashJoinOperator)
		{
			hshm = ((HashJoinOperator)op).getHSHM();
		}
		else if (op instanceof NestedLoopJoinOperator)
		{
			hshm = ((NestedLoopJoinOperator)op).getHSHM();
		}
		else if (op instanceof SemiJoinOperator)
		{
			hshm = ((SemiJoinOperator)op).getHSHM();
		}
		else if (op instanceof AntiJoinOperator)
		{
			hshm = ((AntiJoinOperator)op).getHSHM();
		}

		final ArrayList<String> cols = new ArrayList<String>(hshm.size());
		for (final HashMap<Filter, Filter> hm : hshm)
		{
			final Filter f = (Filter)new ArrayList(hm.keySet()).get(0);
			if (op.children().get(1).getCols2Pos().keySet().contains(f.leftColumn()))
			{
				cols.add(f.leftColumn());
			}

			if (op.children().get(1).getCols2Pos().keySet().contains(f.rightColumn()))
			{
				cols.add(f.rightColumn());
			}
		}

		final boolean rename = reverseUpdateReferences(op.children().get(1), cols);
		HashMap<String, String> renames = null;
		if (rename)
		{
			renames = getIndexRenames(op.children().get(1));
		}

		final TableScanOperator table = getTables(op.children().get(1)).get(0);
		final ArrayList<Index> indexes = meta.getIndexesForTable(table.getSchema(), table.getTable(), tx);
		final Index index = this.getIndexFor(indexes, cols);

		// if index is already being used, set it to run delayed and return
		// else add new index into operator tree, set it to run delayed and
		// return
		if (table.children().size() > 0)
		{
			for (final Operator o : table.children())
			{
				Index i = getIndexForJoin(o, index.getFileName());
				if (i != null)
				{
					retval.add(i);
					i.runDelayed();
				}
				else
				{
					i = index.clone();
					addIndexForJoin(o, i);
					retval.add(i);
					i.runDelayed();
				}

				if (rename)
				{
					i.setRenames(renames);
				}
			}
		}
		else
		{
			final Index i = index.clone();
			i.runDelayed();
			if (rename)
			{
				i.setRenames(renames);
			}
			final IndexOperator io = new IndexOperator(i, meta);
			final ArrayList<String> cols2 = new ArrayList<String>(1);
			cols2.add(new ArrayList<String>(io.getPos2Col().values()).get(0));
			final ArrayList<Boolean> orders = new ArrayList<Boolean>(1);
			orders.add(true);
			final SortOperator sort = new SortOperator(cols2, orders, meta);
			final UnionOperator union = new UnionOperator(true, meta);
			try
			{
				union.add(io);
				sort.add(union);
				table.add(sort);
				io.setNode(table.getNode());
				sort.setNode(table.getNode());
				union.setNode(table.getNode());
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				throw e;
			}

			correctForDevices(table);
			return getIndexes(table);
		}

		return retval;
	}

	private ArrayList<Operator> getEligibleJoins(HashSet<Operator> joins)
	{
		final ArrayList<Operator> retval = new ArrayList<Operator>();
		for (final Operator op : joins)
		{
			final ArrayList<TableScanOperator> tables = getTables(op.children().get(1));
			if (tables.size() == 1 && tables.get(0).getNode() == op.getNode())
			{
				if (!interveningSend(op, tables.get(0)) && !interveningTop(op, tables.get(0)))
				{
					retval.add(op);
				}
			}
		}

		return retval;
	}

	private ArrayList<Index> getIndexes(Operator op)
	{
		final ArrayList<Index> retval = new ArrayList<Index>();
		if (op instanceof IndexOperator)
		{
			retval.add(((IndexOperator)op).getIndex());
		}
		else
		{
			for (final Operator o : op.children())
			{
				retval.addAll(getIndexes(o));
			}
		}

		return retval;
	}

	private Index getIndexFor(ArrayList<Index> indexes, ArrayList<String> cols)
	{
		for (final Index index : indexes)
		{
			if (cols.contains(index.getCols().get(0)) && index.getCols().containsAll(cols))
			{
				return index;
			}
		}

		for (final Index index : indexes)
		{
			if (index.getCols().containsAll(cols))
			{
				return index;
			}
		}

		return null;
	}

	private Index getIndexFor(ArrayList<Index> indexes, String col)
	{
		for (final Index index : indexes)
		{
			if (index.startsWith(col))
			{
				return index;
			}
		}

		for (final Index index : indexes)
		{
			if (index.contains(col))
			{
				return index;
			}
		}

		return null;
	}

	private Index getIndexFor(ArrayList<Index> indexes, String col1, String col2)
	{
		for (final Index index : indexes)
		{
			if (index.startsWith(col1) || index.startsWith(col2) && index.getCols().contains(col1) && index.getCols().contains(col2))
			{
				return index;
			}
		}

		for (final Index index : indexes)
		{
			if (index.contains(col1) && index.contains(col2))
			{
				return index;
			}
		}

		return null;
	}

	private Index getIndexForJoin(Operator op, String fileName)
	{
		if (op instanceof UnionOperator)
		{
			if (op.children().size() != 1)
			{
				return null;
			}

			if (((IndexOperator)op.children().get(0)).getFileName().equals(fileName))
			{
				return ((IndexOperator)op.children().get(0)).getIndex();
			}
			else
			{
				return null;
			}
		}
		else
		{
			for (final Operator o : op.children())
			{
				final Index i = getIndexForJoin(o, fileName);
				if (i != null)
				{
					return i;
				}
			}

			return null;
		}
	}

	private HashMap<String, String> getIndexRenames(Operator op) throws Exception
	{
		if (op instanceof RenameOperator)
		{
			return ((RenameOperator)op).getIndexRenames();
		}
		else
		{
			return getIndexRenames(op.children().get(0));
		}
	}

	private ArrayList<Operator> getJoins(Operator op) throws Exception
	{
		final ArrayList<Operator> retval = new ArrayList<Operator>(MetaData.numWorkerNodes);
		if (op instanceof HashJoinOperator || op instanceof NestedLoopJoinOperator || op instanceof SemiJoinOperator || op instanceof AntiJoinOperator)
		{
			retval.add(op);
		}

		for (final Operator o : op.children())
		{
			retval.addAll(getJoins(o));
		}

		return retval;
	}

	private ArrayList<TableScanOperator> getTables(Operator op)
	{
		final ArrayList<TableScanOperator> retval = new ArrayList<TableScanOperator>();
		if (op instanceof TableScanOperator)
		{
			retval.add((TableScanOperator)op);
			return retval;
		}

		for (final Operator o : op.children())
		{
			retval.addAll(getTables(o));
		}

		return retval;
	}

	private ArrayList<TableScanOperator> getTableScans(Operator op) throws Exception
	{
		ArrayList<TableScanOperator> retval = null;
		if (op instanceof TableScanOperator)
		{
			retval = new ArrayList<TableScanOperator>(1);
			retval.add((TableScanOperator)op);
			return retval;
		}

		retval = new ArrayList<TableScanOperator>(MetaData.numWorkerNodes);
		for (final Operator o : op.children())
		{
			retval.addAll(getTableScans(o));
		}

		return retval;
	}

	private ArrayList<Operator> hasIndex(ArrayList<Operator> joins) throws Exception
	{
		final ArrayList<Operator> retval = new ArrayList<Operator>();
		for (final Operator op : joins)
		{
			HashSet<HashMap<Filter, Filter>> hshm = null;
			if (op instanceof HashJoinOperator)
			{
				hshm = ((HashJoinOperator)op).getHSHM();
			}
			else if (op instanceof NestedLoopJoinOperator)
			{
				hshm = ((NestedLoopJoinOperator)op).getHSHM();
			}
			else if (op instanceof SemiJoinOperator)
			{
				hshm = ((SemiJoinOperator)op).getHSHM();
			}
			else if (op instanceof AntiJoinOperator)
			{
				hshm = ((AntiJoinOperator)op).getHSHM();
			}

			final ArrayList<String> cols = new ArrayList<String>(hshm.size());
			for (final HashMap<Filter, Filter> hm : hshm)
			{
				final Filter f = (Filter)new ArrayList(hm.keySet()).get(0);
				if (op.children().get(1).getCols2Pos().keySet().contains(f.leftColumn()))
				{
					cols.add(f.leftColumn());
				}

				if (op.children().get(1).getCols2Pos().keySet().contains(f.rightColumn()))
				{
					cols.add(f.rightColumn());
				}
			}

			reverseUpdateReferences(op.children().get(1), cols);

			final TableScanOperator table = getTables(op.children().get(1)).get(0);
			final ArrayList<Index> indexes = meta.getIndexesForTable(table.getSchema(), table.getTable(), tx);
			final Index index = this.getIndexFor(indexes, cols);
			if (index == null)
			{
				HRDBMSWorker.logger.info("Wanted to use an index on " + cols + " but none existed.");
			}
			else
			{
				retval.add(op);
			}
		}

		return retval;
	}

	private boolean indexOnly(TableScanOperator table) throws Exception
	{
		if (table.isGetRID())
		{
			return false;
		}
		UnionOperator union = null;
		if (!(table.children().get(0) instanceof UnionOperator))
		{
			if (table.children().get(0).children().size() == 1)
			{
				union = (UnionOperator)table.children().get(0).children().get(0);
				// System.out.println("Union parent = " + union.parent());
			}
			else
			{
				return false;
			}
		}
		else
		{
			union = (UnionOperator)table.children().get(0);
			// System.out.println("Union parent = " + union.parent());
		}

		if (union.children().size() != 1)
		{
			return false;
		}

		final IndexOperator index = (IndexOperator)union.children().get(0);
		final ArrayList<String> references = new ArrayList<String>();
		references.addAll(table.getCols2Pos().keySet());
		for (final String col : table.getCNFForParent(table.firstParent()).getReferences())
		{
			if (!references.contains(col))
			{
				references.add(col);
			}
		}
		if (index.getIndex().getCols().containsAll(references))
		{
			// index only access
			HashMap<String, String> cols2Types = null;
			try
			{
				cols2Types = meta.getCols2TypesForTable(table.getSchema(), table.getTable(), tx);
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				throw e;
			}
			final ArrayList<String> types = new ArrayList<String>(references.size());
			for (final String col : references)
			{
				types.add(cols2Types.get(col));
			}
			index.setIndexOnly(references, types);
			table.setIndexOnly();
			Operator o = index.parent();
			Operator child = index;
			try
			{
				while (o != table)
				{
					// System.out.println("Parent = " + o);
					// System.out.println("Child = " + child);
					o.removeChild(child);
					o.add(child);
					child = o;
					o = o.parent();
				}

				// System.out.println("Parent = " + o);
				// System.out.println("Child = " + child);
				o.removeChild(child);
				o.add(child);
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				throw e;
			}
			// cCache.clear();
			return true;
		}
		else
		{
			return false;
		}
	}

	private boolean interveningSend(Operator op, TableScanOperator table)
	{
		Operator o = table.firstParent();
		while (o != op)
		{
			if (o instanceof NetworkSendOperator)
			{
				return true;
			}

			o = o.parent();
		}

		return false;
	}

	private boolean interveningTop(Operator op, TableScanOperator table)
	{
		Operator o = table.firstParent();
		while (o != op)
		{
			if (o instanceof TopOperator)
			{
				return true;
			}

			o = o.parent();
		}

		return false;
	}

	private void reuseCompoundIndexes(TableScanOperator table)
	{
		if (table.children().get(0) instanceof UnionOperator)
		{
			return;
		}

		final HashMap<String, IndexOperator> file2Index = new HashMap<String, IndexOperator>();
		for (final Operator op : (ArrayList<Operator>)table.children().get(0).children().clone())
		{
			if (op.children().size() > 1)
			{
				continue;
			}

			final IndexOperator index = (IndexOperator)op.children().get(0);
			if (!file2Index.containsKey(index.getFileName()))
			{
				file2Index.put(index.getFileName(), index);
			}
			else
			{
				file2Index.get(index.getFileName()).addSecondaryFilter(index.getFilter());
				for (final Filter filter : index.getSecondary())
				{
					file2Index.get(index.getFileName()).addSecondaryFilter(filter);
				}
				table.children().get(0).removeChild(op);
			}
		}

		// cCache.clear();
	}

	private void reuseIndexes(TableScanOperator table)
	{
		if (table.children().get(0) instanceof UnionOperator)
		{
			return;
		}

		final HashMap<String, IndexOperator> file2Index = new HashMap<String, IndexOperator>();
		for (final Operator op : (ArrayList<Operator>)table.children().get(0).children().clone())
		{
			if (op.children().size() > 1)
			{
				continue;
			}

			final IndexOperator index = (IndexOperator)op.children().get(0);
			if (!file2Index.containsKey(index.getFileName()))
			{
				file2Index.put(index.getFileName(), index);
			}
			else
			{
				file2Index.get(index.getFileName()).addSecondaryFilter(index.getFilter());
				table.children().get(0).removeChild(op);
			}
		}

		// cCache.clear();
	}

	private boolean reverseUpdateReferences(Operator op, ArrayList<String> cols)
	{
		if (op instanceof TableScanOperator)
		{
			return false;
		}

		if (op instanceof RenameOperator)
		{
			((RenameOperator)op).reverseUpdateReferences(cols);
			return true;
		}

		return reverseUpdateReferences(op.children().get(0), cols);
	}

	private void setCardForIntersectAndUnion(Operator op, int card)
	{
		if (op instanceof IntersectOperator)
		{
			((IntersectOperator)op).setEstimate(card);
		}
		else if (op instanceof UnionOperator)
		{
			((UnionOperator)op).setEstimate(card);
		}

		for (final Operator o : op.children())
		{
			setCardForIntersectAndUnion(o, card);
		}
	}

	private void setCards(Operator op) throws Exception
	{
		if (op instanceof MultiOperator)
		{
			final long xl = card(op);
			final long yl = card(op.children().get(0));
			int x = 1;
			int y = 1;
			if (xl > Integer.MAX_VALUE)
			{
				x = Integer.MAX_VALUE;
			}
			else
			{
				x = (int)xl;
			}
			if (yl > Integer.MAX_VALUE)
			{
				y = Integer.MAX_VALUE;
			}
			else
			{
				y = (int)yl;
			}

			if (!((MultiOperator)op).setNumGroupsAndChildCard(x, y))
			{
				return;
			}
		}
		else if (op instanceof AntiJoinOperator)
		{
			final long xl = card(op.children().get(1));
			int x = 1;
			if (xl > Integer.MAX_VALUE)
			{
				x = Integer.MAX_VALUE;
			}
			else
			{
				x = (int)xl;
			}

			if (!((AntiJoinOperator)op).setRightChildCard(x))
			{
				return;
			}
		}
		else if (op instanceof SemiJoinOperator)
		{
			final long xl = card(op.children().get(1));
			int x = 1;
			if (xl > Integer.MAX_VALUE)
			{
				x = Integer.MAX_VALUE;
			}
			else
			{
				x = (int)xl;
			}

			if (!((SemiJoinOperator)op).setRightChildCard(x))
			{
				return;
			}
		}
		else if (op instanceof HashJoinOperator)
		{
			final long xl = card(op.children().get(1));
			int x = 1;
			if (xl > Integer.MAX_VALUE)
			{
				x = Integer.MAX_VALUE;
			}
			else
			{
				x = (int)xl;
			}

			if (!((HashJoinOperator)op).setRightChildCard(x))
			{
				return;
			}
		}
		else if (op instanceof NestedLoopJoinOperator)
		{
			final long xl = card(op.children().get(1));
			int x = 1;
			if (xl > Integer.MAX_VALUE)
			{
				x = Integer.MAX_VALUE;
			}
			else
			{
				x = (int)xl;
			}

			if (!((NestedLoopJoinOperator)op).setRightChildCard(x))
			{
				return;
			}
		}
		else if (op instanceof ProductOperator)
		{
			final long xl = card(op.children().get(1));
			int x = 1;
			if (xl > Integer.MAX_VALUE)
			{
				x = Integer.MAX_VALUE;
			}
			else
			{
				x = (int)xl;
			}

			if (!((ProductOperator)op).setRightChildCard(x))
			{
				return;
			}
		}
		else if (op instanceof SortOperator)
		{
			final long xl = card(op.children().get(0));
			int x = 1;
			if (xl > Integer.MAX_VALUE)
			{
				x = Integer.MAX_VALUE;
			}
			else
			{
				x = (int)xl;
			}

			if (!((SortOperator)op).setChildCard(x))
			{
				return;
			}
		}
		else if (op instanceof UnionOperator)
		{
			long xl;
			if (op.children().size() == 2)
			{
				xl = card(op.children().get(0)) + card(op.children().get(1));
			}
			else
			{
				xl = card(op.children().get(0));
			}
			int x = 1;
			if (xl > Integer.MAX_VALUE)
			{
				x = Integer.MAX_VALUE;
			}
			else
			{
				x = (int)xl;
			}

			if (!((UnionOperator)op).setEstimate(x))
			{
				return;
			}
		}
		else if (op instanceof IntersectOperator)
		{
			final long xl1 = card(op.children().get(0));
			final long xl2 = card(op.children().get(1));
			long xl;
			if (xl1 >= xl2)
			{
				xl = xl1;
			}
			else
			{
				xl = xl2;
			}
			int x = 1;
			if (xl > Integer.MAX_VALUE)
			{
				x = Integer.MAX_VALUE;
			}
			else
			{
				x = (int)xl;
			}

			if (!((IntersectOperator)op).setEstimate(x))
			{
				return;
			}
		}
		else if (op instanceof ExceptOperator)
		{
			final long xl1 = card(op.children().get(0));
			final long xl2 = card(op.children().get(1));
			long xl;
			if (xl1 >= xl2)
			{
				xl = xl1;
			}
			else
			{
				xl = xl2;
			}
			int x = 1;
			if (xl > Integer.MAX_VALUE)
			{
				x = Integer.MAX_VALUE;
			}
			else
			{
				x = (int)xl;
			}

			if (!((ExceptOperator)op).setEstimate(x))
			{
				return;
			}
		}
		else if (op instanceof NetworkSendOperator)
		{
			if (!((NetworkSendOperator)op).setCard())
			{
				return;
			}
		}

		if (op instanceof TableScanOperator)
		{
			if (op.children().size() > 0)
			{
				final long xl = card(op);
				int x = 1;
				if (xl > Integer.MAX_VALUE)
				{
					x = Integer.MAX_VALUE;
				}
				else
				{
					x = (int)xl;
				}

				setCardForIntersectAndUnion(op, x);
			}
		}
		else
		{
			for (final Operator o : op.children())
			{
				setCards(o);
			}
		}
	}

	private void setDevice(Operator op, int device)
	{
		if (op instanceof IndexOperator)
		{
			((IndexOperator)op).setDevice(device);
		}
		else
		{
			for (final Operator o : op.children())
			{
				setDevice(o, device);
			}
		}
	}

	private void setNumParents(Operator op)
	{
		if (op instanceof NetworkSendOperator)
		{
			if (!((NetworkSendOperator)op).setNumParents())
			{
				return;
			}
			((NetworkSendOperator)op).clearParent();
		}

		for (final Operator o : op.children())
		{
			setNumParents(o);
		}
	}

	private void turnOffDistinctUnion(Operator op, boolean seenIntersect)
	{
		if (op instanceof UnionOperator)
		{
			// System.out.println("UnionOperator");
			if (seenIntersect)
			{
				// System.out.println("With a parent intersect operator");
				((UnionOperator)op).setDistinct(false);
			}
			else
			{
				// System.out.println("Without a parent intersect operator");
			}
		}
		else if (op instanceof IntersectOperator)
		{
			seenIntersect = true;
			// System.out.println("IntersectOperator");
		}

		for (final Operator o : op.children())
		{
			turnOffDistinctUnion(o, seenIntersect);
		}
	}

	private void useIndexes(String schema, String table, CNFFilter cnf, TableScanOperator tOp) throws Exception
	{
		final HashSet<HashMap<Filter, Filter>> hshm = cnf.getHSHM();
		// System.out.println("HSHM is " + hshm);
		final ArrayList<Index> available = meta.getIndexesForTable(schema, table, tx);
		for (final HashMap<Filter, Filter> hm : (HashSet<HashMap<Filter, Filter>>)hshm.clone())
		{
			// System.out.println("Looking at " + hm);
			final ArrayList<Index> indexes = new ArrayList<Index>();
			boolean doIt = true;
			double likely = 0;
			for (final Filter f : hm.keySet())
			{
				final double l = meta.likelihood(f, root, tx, tOp);
				// System.out.println("Likelihood of " + f + " = " + l);
				likely += l;
			}

			if (hm.size() == 1)
			{
				String col = null;
				Filter f = new ArrayList<Filter>(hm.keySet()).get(0);
				if (f.leftIsColumn())
				{
					col = f.leftColumn();
				}
				else
				{
					col = f.rightColumn();
				}
				for (final HashMap<Filter, Filter> hm2 : hshm)
				{
					if (hm != hm2 && hm2.size() == 1)
					{
						String col2 = null;
						f = new ArrayList<Filter>(hm2.keySet()).get(0);
						if (f.leftIsColumn())
						{
							col2 = f.leftColumn();
						}
						else
						{
							col2 = f.rightColumn();
						}

						if (col.equals(col2))
						{
							likely *= meta.likelihood(f, root, tx, tOp);
						}
					}
				}
			}

			if (likely > (3.0 / 20.0))
			{
				// System.out.println("Does not filter enough");
				continue;
			}

			for (final Filter f : hm.keySet())
			{
				Index index = null;
				String column = null;
				if (f.leftIsColumn())
				{
					column = f.leftColumn();
					if (f.rightIsColumn())
					{
						index = getIndexFor(available, f.leftColumn(), f.rightColumn());
					}
					else
					{
						index = getIndexFor(available, f.leftColumn());
					}
				}
				else
				{
					column = f.rightColumn();
					index = getIndexFor(available, f.rightColumn());
				}

				if (index == null)
				{
					// System.out.println("There is no matching index for " +
					// f);
					doIt = false;
					if (f.leftIsColumn() && f.rightIsColumn())
					{
						HRDBMSWorker.logger.info("Wanted to use index on " + f.leftColumn() + " and " + f.rightColumn() + " but none existed.");
					}
					else
					{
						HRDBMSWorker.logger.info("Wanted to use index on " + column + " but none existed.");
					}
					break;
				}
				else
				{
					index = index.clone();
					indexes.add(index);
					index.setCondition(f);
				}
			}

			if (doIt)
			{
				hshm.remove(hm);
				final UnionOperator union = new UnionOperator(true, meta);
				try
				{
					for (final Index index : indexes)
					{
						final IndexOperator iOp = new IndexOperator(index, meta);
						union.add(iOp);
					}

					if (tOp.children().size() == 0)
					{
						tOp.add(union);
						union.setNode(tOp.getNode());
					}
					else if (tOp.children().get(0) instanceof IntersectOperator)
					{
						tOp.children().get(0).add(union);
						union.setNode(tOp.children().get(0).getNode());
					}
					else
					{
						final IntersectOperator intersect = new IntersectOperator(meta);
						intersect.add(union);
						final Operator otherChild = tOp.children().get(0);
						tOp.removeChild(otherChild);
						intersect.add(otherChild);
						tOp.add(intersect);
						intersect.setNode(tOp.getNode());
						union.setNode(tOp.getNode());
					}
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.error("", e);
					throw e;
				}
			}
		}

		cnf.setHSHM(hshm);
		// cCache.clear();
	}
}