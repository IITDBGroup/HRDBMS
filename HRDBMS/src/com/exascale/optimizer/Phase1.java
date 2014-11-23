package com.exascale.optimizer;

import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.optimizer.MetaData.PartitionMetaData;
import com.exascale.tables.Transaction;

public final class Phase1
{
	private final RootOperator root;
	private boolean pushdownHadResults;
	private final int ADDITIONAL_PUSHDOWNS = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("phase1_additional_pushdowns")); // 25
	private Transaction tx;
	private final Operator clone;
	private final MetaData meta = new MetaData();

	public Phase1(RootOperator root, Transaction tx) throws Exception
	{
		this.root = root;
		this.tx = tx;
		this.clone = cloneTree(root);
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

	public void optimize() throws Exception
	{
		reorderProducts(root);
		// Driver.printTree(0, root); //DEBUG
		do
		{
			pushdownHadResults = false;
			pushDownSelects(root);
		} while (pushdownHadResults);
		int i = 0;
		while (i < ADDITIONAL_PUSHDOWNS)
		{
			pushDownSelects(root);
			i++;
		}
		// Driver.printTree(0, root); //DEBUG
		combineProductsAndSelects(root);
		// Driver.printTree(0, root); //DEBUG
		removeDuplicateTables();
		// Driver.printTree(0, root); //DEBUG
		pushUpSelects(root);
		// Driver.printTree(0, root); //DEBUG
		do
		{
			pushdownHadResults = false;
			pushDownSelects(root);
			mergeSelectsAndTableScans(root);
		} while (pushdownHadResults);
		i = 0;
		while (i < ADDITIONAL_PUSHDOWNS)
		{
			pushDownSelects(root);
			mergeSelectsAndTableScans(root);
			i++;
		}

		pushDownProjects();
	}

	private void combineProductsAndSelects(Operator op) throws Exception
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
				if (selectChild instanceof ProductOperator)
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

	private ArrayList<Operator> getLeaves(Operator op)
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

	private SelectOperator getMostPromisingSelect(ArrayList<SelectOperator> selects, ArrayList<Operator> subtrees, Operator current) throws Exception
	{
		double minLikelihood = Double.MAX_VALUE;
		SelectOperator minSelect = null;

		if (current == null)
		{
			for (final SelectOperator select : selects)
			{
				final double likelihood = meta.likelihood(new ArrayList<Filter>(select.getFilter()), tx, clone);
				if (likelihood < minLikelihood)
				{
					minLikelihood = likelihood;
					minSelect = select;
				}
			}

			selects.remove(minSelect);
			return minSelect;
		}

		for (final SelectOperator select : selects)
		{
			final ArrayList<Filter> filters = select.getFilter();
			for (final Filter filter : filters)
			{
				if (filter.leftIsColumn() && filter.rightIsColumn())
				{
					if (current.getCols2Pos().containsKey(filter.leftColumn()))
					{
						final double likelihood = meta.likelihood(new ArrayList<Filter>(select.getFilter()), tx, clone);
						if (likelihood < minLikelihood)
						{
							minLikelihood = likelihood;
							minSelect = select;
						}
					}

					if (current.getCols2Pos().containsKey(filter.rightColumn()))
					{
						final double likelihood = meta.likelihood(new ArrayList<Filter>(select.getFilter()), tx, clone);
						if (likelihood < minLikelihood)
						{
							minLikelihood = likelihood;
							minSelect = select;
						}
					}
				}
			}
		}

		if (minSelect != null)
		{
			selects.remove(minSelect);
			return minSelect;
		}

		for (final SelectOperator select : selects)
		{
			final double likelihood = meta.likelihood(new ArrayList<Filter>(select.getFilter()), tx, clone);
			if (likelihood < minLikelihood)
			{
				minLikelihood = likelihood;
				minSelect = select;
			}
		}

		selects.remove(minSelect);
		return minSelect;
	}

	private void getReferences(Operator o, HashSet<String> references)
	{
		references.addAll(o.getReferences());
		for (final Operator op : o.children())
		{
			getReferences(op, references);
		}
	}

	private Operator getSubtreeForCol(String col, ArrayList<Operator> subtrees)
	{
		for (final Operator op : subtrees)
		{
			if (op.getCols2Pos().containsKey(col))
			{
				return op;
			}
		}

		return null;
	}

	private HashMap<TableScanOperator, TableScanOperator> getTableList(Operator op)
	{
		HashMap<TableScanOperator, TableScanOperator> retval = null;
		if (op instanceof TableScanOperator)
		{
			retval = new HashMap<TableScanOperator, TableScanOperator>();
			retval.put((TableScanOperator)op, (TableScanOperator)op);
		}

		for (final Operator child : op.children())
		{
			if (retval == null)
			{
				retval = getTableList(child);
			}
			else
			{
				retval.putAll(getTableList(child));
			}
		}

		return retval;
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
					// System.out.println("That had a TableScanOperator as a child");
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

	private void pushDownProjects() throws Exception
	{
		final HashSet<String> references = new HashSet<String>();
		for (final Operator o : root.children())
		{
			getReferences(o, references);
		}

		final ArrayList<Operator> leaves = getLeaves(root);
		final HashSet hs = new HashSet();
		hs.addAll(leaves);
		leaves.clear();
		leaves.addAll(hs);

		for (final Operator op : leaves)
		{
			if (op instanceof TableScanOperator)
			{
				try
				{
					final TableScanOperator table = (TableScanOperator)op;
					final HashMap<String, Integer> cols2Pos = table.getCols2Pos();
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
							needed.add(new ArrayList<String>(cols2Pos.keySet()).get(0));
						}
						else
						{
							String col = new ArrayList<String>(cols2Pos.keySet()).get(0);
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
					if (op instanceof TableScanOperator)
					{
						final ArrayList<Operator> parents = (ArrayList<Operator>)((TableScanOperator)op).parents().clone();
						for (final Operator op2 : parents)
						{
							op2.removeChild(op);
							op2.add(op);
							int i = 1;
							while (i < ((TableScanOperator)op).parents().size())
							{
								leaves2.add(((TableScanOperator)op).parents().get(i));
								i++;
							}
						}

						op = ((TableScanOperator)op).parents().get(0);
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

	private void pushDownSelects(Operator op) throws Exception
	{
		if (op instanceof SelectOperator)
		{
			final ArrayList<Operator> children = op.children();
			Operator child = null;
			// SelectOperators only have 1 child
			for (final Operator o : children)
			{
				child = o;
			}

			// look at opportunity to push down across child
			if (child instanceof TableScanOperator)
			{
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

					if (!(child instanceof SelectOperator))
					{
						pushdownHadResults = true;
					}

					pushDownSelects(newOp);
				}
				else
				{
					final Operator selectChild = op.children().get(0);
					if (selectChild != null)
					{
						pushDownSelects(selectChild);
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
				pushDownSelects(child);
				i++;
			}
		}
	}

	private void pushUpSelects(Operator op) throws Exception
	{
		if (op instanceof SelectOperator)
		{
			final Operator parent = op.parent();
			if (!(parent instanceof RootOperator))
			{
				final ArrayList<String> references = ((SelectOperator)op).getReferences();
				boolean ok = true;
				if (parent instanceof RenameOperator)
				{
					((RenameOperator)parent).updateReferences(references);
				}

				// parent output has to have all the columns the select
				// references
				final HashMap<String, Integer> parentCols2Pos = parent.getCols2Pos();
				for (final String reference : references)
				{
					if (!parentCols2Pos.containsKey(reference))
					{
						ok = false;
						break;
					}
				}

				if (ok)
				{
					// go ahead and move
					final ArrayList<Operator> next = (ArrayList<Operator>)op.children().clone();
					if (parent instanceof RenameOperator)
					{
						((RenameOperator)parent).updateSelectOperator((SelectOperator)op);
					}

					final ArrayList<Operator> children = (ArrayList<Operator>)op.children().clone();
					parent.removeChild(op);
					final Operator grandParent = parent.parent();
					grandParent.removeChild(parent);

					try
					{
						for (final Operator child : children)
						{
							op.removeChild(child);
							parent.add(child);
						}
						op.add(parent);
						grandParent.add(op);
					}
					catch (final Exception e)
					{
						HRDBMSWorker.logger.error("", e);
						throw e;
					}

					pushUpSelects(op, next);
					for (final Operator o : next)
					{
						pushUpSelects(o);
					}
				}
				else
				{
					for (final Operator child : op.children())
					{
						pushUpSelects(child);
					}
				}
			}
		}
		else
		{
			for (final Operator child : (ArrayList<Operator>)op.children().clone())
			{
				pushUpSelects(child);
			}
		}
	}

	private void pushUpSelects(Operator op, ArrayList<Operator> next) throws Exception
	{
		if (op instanceof SelectOperator)
		{
			final Operator parent = op.parent();
			if (!(parent instanceof RootOperator))
			{
				final ArrayList<String> references = ((SelectOperator)op).getReferences();
				boolean ok = true;
				if (parent instanceof RenameOperator)
				{
					((RenameOperator)parent).updateReferences(references);
				}

				// parent output has to have all the columns the select
				// references
				final HashMap<String, Integer> parentCols2Pos = parent.getCols2Pos();
				for (final String reference : references)
				{
					if (!parentCols2Pos.containsKey(reference))
					{
						ok = false;
						break;
					}
				}

				if (ok)
				{
					// go ahead and move
					if (parent instanceof RenameOperator)
					{
						((RenameOperator)parent).updateSelectOperator((SelectOperator)op);
					}

					final ArrayList<Operator> children = (ArrayList<Operator>)op.children().clone();
					parent.removeChild(op);
					final Operator grandParent = parent.parent();
					grandParent.removeChild(parent);

					try
					{
						for (final Operator child : children)
						{
							op.removeChild(child);
							parent.add(child);
						}
						op.add(parent);
						grandParent.add(op);
					}
					catch (final Exception e)
					{
						HRDBMSWorker.logger.error("", e);
						throw e;
					}

					pushUpSelects(op, next);
				}
				else
				{
					return;
				}
			}
		}
		else
		{
			HRDBMSWorker.logger.error("This should never happen!");
			throw new Exception("This should never happen!");
		}
	}

	private void removeDuplicateTables() throws Exception
	{
		final HashMap<TableScanOperator, TableScanOperator> tables = getTableList(root);
		replaceTableScanOperators(root, tables);
	}

	private void reorderProducts(Operator op) throws Exception
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

	private void reorderProducts(Operator op, ArrayList<SelectOperator> selects, Operator top) throws Exception
	{
		if (op instanceof SelectOperator)
		{
			selects.add((SelectOperator)op);
			reorderProducts(op.children().get(0), selects, top);
			return;
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

		Operator newProd = null;
		while (selectsCopy.size() > 0)
		{
			final SelectOperator select = getMostPromisingSelect(selectsCopy, subtrees, newProd);
			final ArrayList<Filter> filters = select.getFilter();
			int i = 0;
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

					if (left == null || right == null || left.equals(right))
					{
						i++;
						continue;
					}

					newProd = new ProductOperator(select.getMeta());
					newProd.add(left);
					newProd.add(right);
					subtrees.remove(left);
					subtrees.remove(right);
					subtrees.add(newProd);
					break;
				}

				i++;
			}
		}

		// if only 1 in subtrees, just stick selects on top and attach to the
		// rest of the top
		// else product remaining subtrees and then do that
		while (subtrees.size() > 1)
		{
			newProd = new ProductOperator(subtrees.get(0).getMeta());
			newProd.add(subtrees.get(0));
			subtrees.remove(0);
			newProd.add(subtrees.get(0));
			subtrees.remove(0);
			subtrees.add(newProd);
		}

		for (final SelectOperator select : selects)
		{
			select.add(subtrees.get(0));
			subtrees.remove(0);
			subtrees.add(select);
		}

		// everything needs to be removed and readded for all the operators in
		// the rest of the top
		onTop.add(subtrees.get(0));
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

	private void replaceTableScanOperators(Operator op, HashMap<TableScanOperator, TableScanOperator> tables) throws Exception
	{
		if (op instanceof TableScanOperator)
		{
			final TableScanOperator lookup = tables.get(op);
			if (lookup != op)
			{
				// replace op with lookup and fix pointers
				final Operator parent = ((TableScanOperator)op).firstParent();
				parent.removeChild(op);
				parent.add(lookup);
			}

			return;
		}

		final ArrayList<Operator> children = (ArrayList<Operator>)op.children().clone();
		for (final Operator child : children)
		{
			replaceTableScanOperators(child, tables);
		}
	}
}
