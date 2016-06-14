package com.exascale.optimizer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.managers.ResourceManager;
import com.exascale.tables.Transaction;

public final class Phase5
{
	public static final long MAX_GB = (long)(ResourceManager.QUEUE_SIZE * Double.parseDouble(HRDBMSWorker.getHParms().getProperty("sort_gb_factor")));

	private final RootOperator root;
	private final MetaData meta;
	private final HashMap<Operator, Long> cCache = new HashMap<Operator, Long>();
	private final Transaction tx;
	private final HashMap<ArrayList<Filter>, Double> likelihoodCache;
	private final HashMap<String, Integer> typeCache = new HashMap<String, Integer>();

	public Phase5(RootOperator root, Transaction tx, HashMap<ArrayList<Filter>, Double> likelihoodCache)
	{
		this.root = root;
		this.tx = tx;
		this.likelihoodCache = likelihoodCache;
		meta = root.getMeta();
	}

	public static void clearOpParents(Operator op, HashSet<Operator> touched)
	{
		if (touched.contains(op))
		{
			return;
		}

		touched.add(op);
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

	public long card(Operator op) throws Exception
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
		setTableTypes(root, new HashSet<Operator>());
		addIndexesToTableScans();
		// addIndexesToJoins();
		turnOffDistinctUnion(root, false, new HashSet<Operator>());
		largeGBs(root, new HashSet<Operator>());
		setCards(root, new HashSet<Operator>());
		setNumParents(root);
		// sanityCheck(root, -1);
		setSpecificCoord(root, new HashSet<Operator>());
		// Phase1.printTree(root, 0);
		sortLimit(root, new HashSet<Operator>());
		indexOnlyScan(root, new HashSet<Operator>());
		pruneTree(root, new IdentityHashMap<Operator, Operator>());
	}

	public void printTree(Operator op, int indent) throws Exception
	{
		String line = "";
		int i = 0;
		while (i < indent)
		{
			line += " ";
			i++;
		}

		line += "(" + card(op) + ") ";
		line += op;
		HRDBMSWorker.logger.debug(line);

		if (op.children().size() > 0)
		{
			line = "";
			i = 0;
			while (i < indent)
			{
				line += " ";
				i++;
			}

			line += "(";
			HRDBMSWorker.logger.debug(line);

			for (Operator child : op.children())
			{
				printTree(child, indent + 3);
			}

			line = "";
			i = 0;
			while (i < indent)
			{
				line += " ";
				i++;
			}

			line += ")";
			HRDBMSWorker.logger.debug(line);
		}
	}

	private void addIndexesToTableScans() throws Exception
	{
		final ArrayList<TableScanOperator> s = getTableScans(root, new HashSet<Operator>());
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
						System.out.println("Using indexes.");
						// printIndexes(table);
						if (!indexOnly)
						{
							addSort(table);
						}
						correctForDevices(table);
					}
				}

				clearOpParents(table, new HashSet<Operator>());
				cleanupOrderedFilters(table, new HashSet<Operator>());
			}
		}
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

	private long cardHJO(Operator op) throws Exception
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
		cCache.put(op, retval);
		return retval;
	}

	private long cardMO(Operator op) throws Exception
	{
		long groupCard;
		if (((MultiOperator)op).getKeys().size() == 0)
		{
			groupCard = 1;
		}
		else
		{
			groupCard = meta.getColgroupCard(((MultiOperator)op).getKeys(), root, tx, op);
		}
		long card = card(op.children().get(0));
		if (groupCard > card)
		{
			cCache.put(op, card);
			if (card == 0)
			{
				HRDBMSWorker.logger.debug("Child of MultiOp had card = 0");
			}
			return card;
		}

		final long retval = groupCard;
		cCache.put(op, retval);
		if (retval == 0)
		{
			HRDBMSWorker.logger.debug("Colgroup card was 0");
		}
		return retval;
	}

	private long cardNL(Operator op) throws Exception
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
		cCache.put(op, retval);
		return retval;
	}

	private long cardNorm(Operator op) throws Exception
	{
		long retval = card(op.children().get(0));
		if (retval == 0)
		{
			retval = 1;
		}
		cCache.put(op, retval);
		return retval;
	}

	private long cardRX(Operator op) throws Exception
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

	private long cardSelect(Operator op) throws Exception
	{
		Double likelihood = likelihoodCache.get(((SelectOperator)op).getFilter());
		if (likelihood == null)
		{
			likelihood = meta.likelihood(new ArrayList<Filter>(((SelectOperator)op).getFilter()), tx, root);
			likelihoodCache.put(((SelectOperator)op).getFilter(), likelihood);
		}

		// final long retval = (long)(((SelectOperator)op).likelihood(root,
		// tx) * card(op.children().get(0)));
		long retval = (long)(likelihood * card(op.children().get(0)));
		if (retval == 0)
		{
			retval = 1;
		}
		cCache.put(op, retval);
		return retval;
	}

	private long cardSetI(Operator op) throws Exception
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

	private long cardTop(Operator op) throws Exception
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

	private long cardTSO(Operator op) throws Exception
	{
		final HashSet<HashMap<Filter, Filter>> hshm = ((TableScanOperator)op).getHSHM();
		if (hshm != null)
		{
			if (op.children().size() == 0)
			{
				long retval = (long)(meta.getTableCard(((TableScanOperator)op).getSchema(), ((TableScanOperator)op).getTable(), tx) * meta.likelihood(hshm, root, tx, op) * (1.0 / ((TableScanOperator)op).getNumNodes()));
				if (retval == 0)
				{
					retval = 1;
				}
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
					long retval = (long)(meta.likelihood(hshm, root, tx, op) * sum * (1.0 / ((TableScanOperator)op).getNumNodes()) * meta.getTableCard(((TableScanOperator)op).getSchema(), ((TableScanOperator)op).getTable(), tx));
					if (retval == 0)
					{
						retval = 1;
					}
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
					long retval = (long)(meta.likelihood(hshm, root, tx, op) * z * (1.0 / ((TableScanOperator)op).getNumNodes()) * meta.getTableCard(((TableScanOperator)op).getSchema(), ((TableScanOperator)op).getTable(), tx));
					if (retval == 0)
					{
						retval = 1;
					}
					cCache.put(op, retval);
					return retval;
				}
			}
		}

		if (op.children().size() == 0)
		{
			long retval = (long)((1.0 / ((TableScanOperator)op).getNumNodes()) * meta.getTableCard(((TableScanOperator)op).getSchema(), ((TableScanOperator)op).getTable(), tx));
			if (retval == 0)
			{
				retval = 1;
			}
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
				long retval = (long)(sum * (1.0 / ((TableScanOperator)op).getNumNodes()) * meta.getTableCard(((TableScanOperator)op).getSchema(), ((TableScanOperator)op).getTable(), tx));
				if (retval == 0)
				{
					retval = 1;
				}
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
				long retval = (long)(z * (1.0 / ((TableScanOperator)op).getNumNodes()) * meta.getTableCard(((TableScanOperator)op).getSchema(), ((TableScanOperator)op).getTable(), tx));
				if (retval == 0)
				{
					retval = 1;
				}
				cCache.put(op, retval);
				return retval;
			}
		}
	}

	private long cardTX(Operator op) throws Exception
	{
		long retval = card(op.children().get(0)) / ((NetworkHashAndSendOperator)op).parents().size();
		if (retval == 0)
		{
			retval = 1;
		}
		cCache.put(op, retval);
		return retval;
	}

	private long cardTXRR(Operator op) throws Exception
	{
		long retval = card(op.children().get(0)) / ((NetworkSendRROperator)op).parents().size();
		if (retval == 0)
		{
			retval = 1;
		}
		cCache.put(op, retval);
		return retval;
	}

	private long cardUnion(Operator op) throws Exception
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

	private long cardX(Operator op) throws Exception
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

	private void cleanupOrderedFilters(Operator op, HashSet<Operator> touched)
	{
		if (touched.contains(op))
		{
			return;
		}

		touched.add(op);
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
				if (index.getKeys().containsAll(index2.getReferencedCols()))
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

			cols.removeAll(index.getKeys());
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

	private HashMap<String, Index> getCols2Indexes(Operator op)
	{
		final HashMap<String, Index> retval = new HashMap<String, Index>();
		if (op instanceof IndexOperator)
		{
			if (!(op.parent() instanceof UnionOperator) || op.parent().children().size() == 1)
			{
				final Index index = ((IndexOperator)op).getIndex();
				final ArrayList<String> cols = index.getKeys();
				for (final String col : cols)
				{
					if (retval.containsKey(col))
					{
						Index currentIndex = retval.get(col);
						int currentI = currentIndex.getKeys().indexOf(col);
						int newI = index.getKeys().indexOf(col);
						if (newI < currentI)
						{
							retval.put(col, index);
						}
					}
					else
					{
						retval.put(col, index);
					}
				}
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

	private ArrayList<TableScanOperator> getTableScans(Operator op, HashSet<Operator> touched) throws Exception
	{
		ArrayList<TableScanOperator> retval = null;
		if (touched.contains(op))
		{
			return new ArrayList<TableScanOperator>();
		}

		touched.add(op);
		if (op instanceof TableScanOperator)
		{
			retval = new ArrayList<TableScanOperator>(1);
			retval.add((TableScanOperator)op);
			return retval;
		}

		if (op.children().size() == 1)
		{
			return getTableScans(op.children().get(0), touched);
		}

		retval = new ArrayList<TableScanOperator>(MetaData.numWorkerNodes);
		for (final Operator o : op.children())
		{
			retval.addAll(getTableScans(o, touched));
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
		if (index.getIndex().getKeys().containsAll(references))
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

	private void indexOnlyScan(Operator op, HashSet<Operator> touched) throws Exception
	{
		if (op instanceof NetworkSendOperator)
		{
			if (touched.contains(op))
			{
				return;
			}

			touched.add(op);
		}

		if (op instanceof TableScanOperator && op.children().size() == 0)
		{
			Index best = null;
			TableScanOperator top = (TableScanOperator)op;
			if (top.getType() == 0)
			{
				String[] needed = top.getMidPos2Col();
				ArrayList<String> needed2 = new ArrayList<String>(needed.length);
				for (String n : needed)
				{
					needed2.add(n);
				}

				// if exists index with all these cols, use it
				final ArrayList<Index> available = meta.getIndexesForTable(top.getSchema(), top.getTable(), tx);
				int bestSize = Integer.MAX_VALUE;
				for (Index index : available)
				{
					if (index.getKeys().containsAll(needed2))
					{
						if (index.getKeys().size() < bestSize)
						{
							best = index;
							bestSize = index.getKeys().size();
						}
					}
				}
			}

			if (best != null)
			{
				top.setIndexScan(best);
			}
		}

		for (Operator o : op.children())
		{
			indexOnlyScan(o, touched);
		}
	}

	private void largeGBs(Operator op, HashSet<Operator> touched) throws Exception
	{
		if (op instanceof NetworkSendOperator)
		{
			if (touched.contains(op))
			{
				return;
			}

			touched.add(op);
		}

		if (op instanceof MultiOperator)
		{
			long card = card(op);
			HRDBMSWorker.logger.debug("MultiOp with card = " + card);
			if (card > MAX_GB)
			{
				HRDBMSWorker.logger.debug("External MO by factor " + ((card * 1.0) / MAX_GB) + "x");
				MultiOperator mop = (MultiOperator)op;
				mop.setExternal();
			}
		}

		for (Operator o : op.children())
		{
			largeGBs(o, touched);
		}
	}

	private long notCached(Operator op) throws Exception
	{
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
			return cardTSO(op);
		}

		return cardNorm(op);
	}

	private void pruneTree(Operator op, IdentityHashMap<Operator, Operator> covered)
	{
		if (op instanceof NetworkHashReceiveOperator)
		{
			Operator o = op.children().get(0);
			if (covered.containsKey(o))
			{
				return;
			}
			else
			{
				((NetworkHashReceiveOperator)op).setSend();
				for (Operator o2 : op.children())
				{
					covered.put(o2, o2);
				}
			}
		}
		else if (op instanceof NetworkHashReceiveAndMergeOperator)
		{
			Operator o = op.children().get(0);
			if (covered.containsKey(o))
			{
				return;
			}
			else
			{
				((NetworkHashReceiveAndMergeOperator)op).setSend();
				for (Operator o2 : op.children())
				{
					covered.put(o2, o2);
				}
			}
		}

		for (Operator o : op.children())
		{
			pruneTree(o, covered);
		}
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

	private void setCardForIntersectAndUnion(Operator op, long card)
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

	private void setCards(Operator op, HashSet<Operator> touched) throws Exception
	{
		if (op instanceof NetworkSendOperator)
		{
			if (touched.contains(op))
			{
				return;
			}

			touched.add(op);
		}

		if (op instanceof MultiOperator)
		{
			final long xl = card(op);
			final long yl = card(op.children().get(0));

			if (!((MultiOperator)op).setNumGroupsAndChildCard(xl, yl))
			{
				return;
			}
		}
		else if (op instanceof AntiJoinOperator)
		{
			final long xl = card(op.children().get(1));
			final long x2 = card(op.children().get(0));

			if (!((AntiJoinOperator)op).setRightChildCard(xl, x2))
			{
				return;
			}
		}
		else if (op instanceof SemiJoinOperator)
		{
			final long xl = card(op.children().get(1));
			final long x2 = card(op.children().get(0));

			if (!((SemiJoinOperator)op).setRightChildCard(xl, x2))
			{
				return;
			}
		}
		else if (op instanceof HashJoinOperator)
		{
			final long xl = card(op.children().get(1));
			final long x2 = card(op.children().get(0));

			if (!((HashJoinOperator)op).setRightChildCard(xl, x2))
			{
				return;
			}
		}
		else if (op instanceof NestedLoopJoinOperator)
		{
			final long xl = card(op.children().get(1));
			final long x2 = card(op.children().get(0));

			if (!((NestedLoopJoinOperator)op).setRightChildCard(xl, x2))
			{
				return;
			}
		}
		else if (op instanceof ProductOperator)
		{
			final long xl = card(op.children().get(1));
			final long x2 = card(op.children().get(0));

			if (!((ProductOperator)op).setRightChildCard(xl, x2))
			{
				return;
			}
		}
		else if (op instanceof SortOperator)
		{
			final long xl = card(op.children().get(0));

			if (!((SortOperator)op).setChildCard(xl))
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

			if (!((UnionOperator)op).setEstimate(xl))
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

			if (!((IntersectOperator)op).setEstimate(xl))
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

			if (!((ExceptOperator)op).setEstimate(xl))
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
				setCardForIntersectAndUnion(op, xl);
			}
		}
		else
		{
			for (final Operator o : op.children())
			{
				setCards(o, touched);
			}
		}
	}

	/*
	 * private void sanityCheck(Operator op, int node) throws Exception { if (op
	 * instanceof NetworkSendOperator) { node = op.getNode(); for (Operator o :
	 * op.children()) { sanityCheck(o, node); } } else { if (op.getNode() !=
	 * node) { HRDBMSWorker.logger.debug("P5 sanity check failed");
	 * HRDBMSWorker.logger.debug("Parent is " + op.parent() + " (" +
	 * op.parent().getNode() + ")"); HRDBMSWorker.logger.debug("Children are..."
	 * ); for (Operator o : op.parent().children()) { if (o == op) {
	 * HRDBMSWorker.logger.debug("***** " + o + " (" + o.getNode() + ") *****");
	 * } else { HRDBMSWorker.logger.debug(o + " (" + o.getNode() + ")"); } }
	 * throw new Exception("P5 sanity check failed"); }
	 *
	 * for (Operator o : op.children()) { sanityCheck(o, node); } } }
	 */

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

	private void setSpecificCoord(Operator op, HashSet<Operator> touched) throws Exception
	{
		if (op instanceof NetworkSendOperator)
		{
			if (touched.contains(op))
			{
				return;
			}

			touched.add(op);
		}
		if (op.getNode() == -1)

		{
			op.setNode(MetaData.myCoordNum());
		}

		for (Operator o : op.children())
		{
			setSpecificCoord(o, touched);
		}
	}

	private void setTableTypes(Operator op, HashSet<Operator> touched) throws Exception
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
			TableScanOperator top = (TableScanOperator)op;
			Integer type = typeCache.get(top.getSchema() + "." + top.getTable());
			if (type == null)
			{
				type = meta.getTypeForTable(top.getSchema(), top.getTable(), tx);
				typeCache.put(top.getSchema() + "." + top.getTable(), type);
			}

			top.setType(type);
			return;
		}

		for (Operator o : op.children())
		{
			setTableTypes(o, touched);
		}
	}

	private void sortLimit(Operator op, HashSet<Operator> touched)
	{
		if (op instanceof NetworkSendOperator)
		{
			if (touched.contains(op))
			{
				return;
			}

			touched.add(op);
		}

		if (op instanceof TopOperator)
		{
			if (op.children().get(0) instanceof SortOperator)
			{
				SortOperator sop = (SortOperator)op.children().get(0);
				TopOperator top = (TopOperator)op;
				long limit = top.getRemaining();

				if (limit < ResourceManager.QUEUE_SIZE * Double.parseDouble(HRDBMSWorker.getHParms().getProperty("hash_external_factor")) / 2)
				{
					sop.setLimit(limit);
				}
			}
		}

		for (Operator o : op.children())
		{
			sortLimit(o, touched);
		}
	}

	private void turnOffDistinctUnion(Operator op, boolean seenIntersect, HashSet<Operator> touched)
	{
		if (op instanceof NetworkSendOperator)
		{
			if (touched.contains(op))
			{
				return;
			}

			touched.add(op);
		}

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
			turnOffDistinctUnion(o, seenIntersect, touched);
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
				if (f.op().equals("NE"))
				{
					likely += 1;
				}

				if (f.op().equals("NL"))
				{
					likely += 1;
				}

				if (f.op().equals("LI"))
				{
					if (!f.leftIsColumn())
					{
						String literal = (String)f.leftLiteral();
						if (literal.startsWith("%"))
						{
							likely += 1;
						}
					}

					if (!f.rightIsColumn())
					{
						String literal = (String)f.rightLiteral();
						if (literal.startsWith("%"))
						{
							likely += 1;
						}
					}
				}
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
							if (f.op().equals("NE"))
							{
								likely += 1;
							}

							if (f.op().equals("NL"))
							{
								likely += 1;
							}

							if (f.op().equals("LI"))
							{
								if (!f.leftIsColumn())
								{
									String literal = (String)f.leftLiteral();
									if (literal.startsWith("%"))
									{
										likely += 1;
									}
								}

								if (!f.rightIsColumn())
								{
									String literal = (String)f.rightLiteral();
									if (literal.startsWith("%"))
									{
										likely += 1;
									}
								}
							}
						}
					}
				}
			}

			for (final Filter f : hm.keySet())
			{
				Index index = null;
				String lcolumn = null;
				String rcolumn = null;
				if (f.leftIsColumn())
				{
					lcolumn = f.leftColumn();
					if (f.rightIsColumn())
					{
						rcolumn = f.rightColumn();
						// index = getIndexFor(available, f.leftColumn(),
						// f.rightColumn());
					}
					else
					{
						index = getIndexFor(available, f.leftColumn());
					}
				}
				else
				{
					lcolumn = f.rightColumn();
					index = getIndexFor(available, f.rightColumn());
				}

				if (index == null)
				{
					// System.out.println("There is no matching index for " +
					// f);
					doIt = false;

					if (rcolumn == null)
					{
						// if (likely > (12.0 / 53.0) || !f.op().equals("E"))
						// if (likely > (1.0 / 10.0))
						// {
						// }
						// else
						// {
						// HRDBMSWorker.logger.debug("Wanted to use an index on
						// " + lcolumn + " but none existed");
						// }
						if (likely <= (1.0 / 10.0) && tOp.getType() == 0)
						{
							HRDBMSWorker.logger.debug("Wanted to use an index on " + lcolumn + " but none existed");
						}
						else if (likely <= 0.0003 && tOp.getType() != 0)
						{
							HRDBMSWorker.logger.debug("Wanted to use an index on " + lcolumn + " but none existed");
						}
					}

					break;
				}
				else
				{
					// determine if this index is better than a tablescan
					if (rcolumn == null)
					{
						if (index.getKeys().get(0).equals(lcolumn))
						{
							// if (likely > (12.0 / 53.0) ||
							// !f.op().equals("E"))
							if (likely > (1.0 / 10.0))
							{
								doIt = false;
							}

							if (likely > 0.0003 && tOp.getType() != 0)
							{
								doIt = false;
							}
						}
						else
						{
							doIt = false;
						}
					}
					else
					{
						doIt = false;
					}

					if (doIt)
					{
						index = index.clone();
						indexes.add(index);
						index.setCondition(f);
					}
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

						for (Operator o : union.children())
						{
							o.setNode(tOp.getNode());
						}
					}
					else if (tOp.children().get(0) instanceof IntersectOperator)
					{
						tOp.children().get(0).add(union);
						union.setNode(tOp.children().get(0).getNode());

						for (Operator o : union.children())
						{
							o.setNode(tOp.getNode());
						}
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

						for (Operator o : union.children())
						{
							o.setNode(tOp.getNode());
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

		cnf.setHSHM(hshm);
		// cCache.clear();
	}
}