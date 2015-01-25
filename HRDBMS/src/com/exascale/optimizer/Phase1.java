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
		//HRDBMSWorker.logger.debug("Upon entering P1:"); //DEBUG
		//printTree(root, 0); //DEBUG
		// Driver.printTree(0, root); //DEBUG
		do
		{
			pushdownHadResults = false;
			pushDownSelects2(root);
		} while (pushdownHadResults);
		int i = 0;
		while (i < ADDITIONAL_PUSHDOWNS)
		{
			pushDownSelects2(root);
			i++;
		}
		
		//HRDBMSWorker.logger.debug("Immediately prior to reorderProducts()"); //DEBUG
		//printTree(root, 0); //DEBUG
		reorderProducts(root);
		//HRDBMSWorker.logger.debug("After reorderProducts()"); //DEBUG
		//printTree(root, 0); //DEBUG
		
		do
		{
			pushdownHadResults = false;
			pushDownSelects(root);
		} while (pushdownHadResults);
		i = 0;
		while (i < ADDITIONAL_PUSHDOWNS)
		{
			pushDownSelects(root);
			i++;
		}
		
		// Driver.printTree(0, root); //DEBUG
		combineProductsAndSelects(root);
		//HRDBMSWorker.logger.debug("After combine...");
		//printTree(root, 0); //DEBUG
		// Driver.printTree(0, root); //DEBUG
		//removeDuplicateTables();
		// Driver.printTree(0, root); //DEBUG
		//pushUpSelects(root);
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
		removeUnneededOps(root);
		
		projectForSemiAnti(root);
		
		//HRDBMSWorker.logger.debug("Upon exiting P1:"); //DEBUG
		//printTree(root, 0); //DEBUG
	}
	
	private void projectForSemiAnti(Operator op) throws Exception
	{
		if (op instanceof SemiJoinOperator)
		{
			HashSet<HashMap<Filter, Filter>> hshm = ((SemiJoinOperator)op).getHSHM();
			HashSet<String> cols = new HashSet<String>();
			for (HashMap<Filter, Filter> hm : hshm)
			{
				for (Filter f : hm.keySet())
				{
					if (f.leftIsColumn())
					{
						String l = f.leftColumn();
						cols.add(l);
					}
					
					if (f.rightIsColumn())
					{
						String r = f.rightColumn();
						cols.add(r);
					}
				}
			}
			
			ArrayList<String> toProject = new ArrayList<String>();
			for (String col : cols)
			{
				if (op.children().get(1).getCols2Pos().keySet().contains(col))
				{
					toProject.add(col);
				}
			}
			
			if (toProject.size() < op.children().get(1).getCols2Pos().size())
			{
				ProjectOperator project = new ProjectOperator(toProject, meta);
				Operator right = op.children().get(1);
				op.removeChild(right);
				project.add(right);
				op.add(project);
			}
		}
		else if (op instanceof AntiJoinOperator)
		{
			HashSet<HashMap<Filter, Filter>> hshm = ((AntiJoinOperator)op).getHSHM();
			HashSet<String> cols = new HashSet<String>();
			for (HashMap<Filter, Filter> hm : hshm)
			{
				for (Filter f : hm.keySet())
				{
					if (f.leftIsColumn())
					{
						String l = f.leftColumn();
						cols.add(l);
					}
					
					if (f.rightIsColumn())
					{
						String r = f.rightColumn();
						cols.add(r);
					}
				}
			}
			
			ArrayList<String> toProject = new ArrayList<String>();
			for (String col : cols)
			{
				if (op.children().get(1).getCols2Pos().keySet().contains(col))
				{
					toProject.add(col);
				}
			}
			
			if (toProject.size() < op.children().get(1).getCols2Pos().size())
			{
				ProjectOperator project = new ProjectOperator(toProject, meta);
				Operator right = op.children().get(1);
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
			for (Operator o : op .children())
			{
				projectForSemiAnti(o);
			}
		}
	}
	
	private void removeUnneededOps(Operator op) throws Exception
	{
		if (!(op instanceof MultiOperator))
		{
			for (Operator o : op.children())
			{
				removeUnneededOps(o);
			}
			
			return;
		}
		
		MultiOperator mop = (MultiOperator)op;
		Operator child = mop.children().get(0);
		if (child instanceof ReorderOperator || child instanceof ProjectOperator || child instanceof SortOperator)
		{
			mop.removeChild(child);
			Operator grandChild = child.children().get(0);
			child.removeChild(grandChild);
			mop.add(grandChild);
			removeUnneededOps(mop);
		}
		else
		{
			for (Operator o : mop.children())
			{
				removeUnneededOps(o);
			}
		}
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
	
	private String getTable(Operator op)
	{
		if (op instanceof TableScanOperator)
		{
			return ((TableScanOperator)op).getSchema() + "." + ((TableScanOperator)op).getTable();
		}
		else
		{
			HashSet<String> retval = new HashSet<String>();
			for (Operator o : op.children())
			{
				String ret = getTable(o);
				if (ret == null)
				{
					return null;
				}
				else
				{
					retval.add(ret);
				}
			}
			
			if (retval.size() == 1)
			{
				return (String)retval.toArray()[0];
			}
			else
			{
				return null;
			}
		}
	}

	private SelectOperator getMostPromisingSelect(ArrayList<SelectOperator> selects, ArrayList<Operator> subtrees, Operator current) throws Exception
	{
		double minLikelihood = Double.MAX_VALUE;
		double minColocated = Double.MAX_VALUE;
		SelectOperator minSelect = null;
		SelectOperator minSelect2 = null;

		if (current == null)
		{
			//HRDBMSWorker.logger.debug("Current is null");
			HashSet<SubtreePair> pairs = new HashSet<SubtreePair>();
			outer: for (final SelectOperator select : selects)
			{
				double likelihood = meta.likelihood(new ArrayList<Filter>(select.getFilter()), tx, clone);
				ArrayList<Filter> filters = select.getFilter();
				Operator left = null;
				Operator right = null;
				ArrayList<String> lefts = new ArrayList<String>();
				ArrayList<String> rights = new ArrayList<String>();
				for (Filter filter : filters)
				{
					if (filter.leftIsColumn() && filter.rightIsColumn())
					{
						left = getSubtreeForCol(filter.leftColumn(), subtrees);
						right = getSubtreeForCol(filter.rightColumn(), subtrees);
						
						SubtreePair pair = new SubtreePair(left, right);
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
						
						likelihood *= (card(left) * card(right));
						break;
					}
				}
				
				if (left != null)
				{
					for (SelectOperator s2 : selects)
					{
						if (s2 != select)
						{
							ArrayList<Filter> filters2 = s2.getFilter();
							Operator l2 = null;
							Operator r2 = null;
							for (Filter f2 : filters2)
							{
								if (f2.leftIsColumn() && f2.rightIsColumn())
								{
									l2 = getSubtreeForCol(f2.leftColumn(), subtrees);
									r2 = getSubtreeForCol(f2.rightColumn(), subtrees);
									if ((l2 == left && r2 == right) || (l2 == right && r2 == left))
									{
										if (l2 == left && r2 == right)
										{
											if (filters2.size() == 1 && f2.op().equals("E") && lefts.size() > 0)
											{
												lefts.add(f2.leftColumn());
												rights.add(f2.rightColumn());
											}
											else
											{
												lefts.clear();
												rights.clear();
											}
										}
										else
										{
											if (filters2.size() == 1 && f2.op().equals("E") && lefts.size() > 0)
											{
												rights.add(f2.leftColumn());
												lefts.add(f2.rightColumn());
											}
											else
											{
												lefts.clear();
												rights.clear();
											}
										}
										likelihood *= meta.likelihood(new ArrayList<Filter>(s2.getFilter()), tx, clone);
										break;
									}
								}
							}
						}
					}
				}
				
				if (lefts.size() > 0 && rights.size() > 0)
				{
					String leftTable = getTable(left);
					String rightTable = getTable(right);
					
					if (leftTable != null && rightTable != null)
					{
						String lschema = leftTable.substring(0, leftTable.indexOf('.'));
						String ltable = leftTable.substring(leftTable.indexOf('.') + 1);
						String rschema = rightTable.substring(0, rightTable.indexOf('.'));
						String rtable = rightTable.substring(rightTable.indexOf('.') + 1);
						PartitionMetaData lpmd = meta.getPartMeta(lschema, ltable, tx);
						PartitionMetaData rpmd = meta.getPartMeta(rschema, rtable, tx);
						
						if (lpmd.noNodeGroupSet() && rpmd.noNodeGroupSet())
						{
							if (lpmd.getNodeHash() != null && lefts.equals(lpmd.getNodeHash()) && rpmd.getNodeHash() != null && rights.equals(rpmd.getNodeHash()))
							{
								if (likelihood < minColocated)
								{
									minColocated = likelihood;
									minSelect2 = select;
								}
							}
						}
					}
				}
			
				//HRDBMSWorker.logger.debug("Estimated join cardinality = " + likelihood); //DEBUG
				
				if (likelihood < minLikelihood)
				{
					minLikelihood = likelihood;
					minSelect = select;
				}
			}
			
			if (minColocated <= minLikelihood * 2)
			{
				//HRDBMSWorker.logger.debug("Chose " + minColocated); //DEBUG
				minSelect = minSelect2;
			}
			else
			{
				//HRDBMSWorker.logger.debug("Chose " + minLikelihood); //DEBUG
			}

			selects.remove(minSelect);
			return minSelect;
		}

		//HRDBMSWorker.logger.debug("Current is not null");
		HashSet<SubtreePair> pairs = new HashSet<SubtreePair>();
		outer2: for (final SelectOperator select : selects)
		{
			double likelihood = Double.MAX_VALUE;
			Operator left = null;
			Operator right = null;
			final ArrayList<Filter> filters = select.getFilter();
			for (final Filter filter : filters)
			{
				if (filter.leftIsColumn() && filter.rightIsColumn())
				{
					if (current.getCols2Pos().containsKey(filter.leftColumn()))
					{
						likelihood = meta.likelihood(new ArrayList<Filter>(select.getFilter()), tx, clone);
						left = getSubtreeForCol(filter.leftColumn(), subtrees);
						right = getSubtreeForCol(filter.rightColumn(), subtrees);
						SubtreePair pair = new SubtreePair(left, right);
						if (pairs.contains(pair))
						{
							continue outer2;
						}
						else
						{
							pairs.add(pair);
						}
						likelihood *= (card(left) * card(right));
					}
					else if (current.getCols2Pos().containsKey(filter.rightColumn()))
					{
						likelihood = meta.likelihood(new ArrayList<Filter>(select.getFilter()), tx, clone);
						left = getSubtreeForCol(filter.leftColumn(), subtrees);
						right = getSubtreeForCol(filter.rightColumn(), subtrees);
						SubtreePair pair = new SubtreePair(left, right);
						if (pairs.contains(pair))
						{
							continue outer2;
						}
						else
						{
							pairs.add(pair);
						}
						likelihood *= (card(left) * card(right));
					}
					
					break;
				}
			}
			
			if (left != null)
			{
				for (SelectOperator s2 : selects)
				{
					if (s2 != select)
					{
						ArrayList<Filter> filters2 = s2.getFilter();
						Operator l2 = null;
						Operator r2 = null;
						for (Filter f2 : filters2)
						{
							if (f2.leftIsColumn() && f2.rightIsColumn())
							{
								l2 = getSubtreeForCol(f2.leftColumn(), subtrees);
								r2 = getSubtreeForCol(f2.rightColumn(), subtrees);
								if ((l2 == left && r2 == right) || (l2 == right && r2 == left))
								{
									likelihood *= meta.likelihood(new ArrayList<Filter>(s2.getFilter()), tx, clone);
									break;
								}
							}
						}
					}
				}
			}
			
			//HRDBMSWorker.logger.debug("Estimated join cardinality = " + likelihood); //DEBUG
			
			if (likelihood < minLikelihood)
			{
				minLikelihood = likelihood;
				minSelect = select;
			}
		}

		if (minSelect != null)
		{
			//HRDBMSWorker.logger.debug("Chose " + minLikelihood); //DEBUG
			selects.remove(minSelect);
			return minSelect;
		}

		pairs.clear();
		outer3: for (final SelectOperator select : selects)
		{
			double likelihood = meta.likelihood(new ArrayList<Filter>(select.getFilter()), tx, clone);
			ArrayList<Filter> filters = select.getFilter();
			Operator left = null;
			Operator right = null;
			ArrayList<String> lefts = new ArrayList<String>();
			ArrayList<String> rights = new ArrayList<String>();
			for (Filter filter : filters)
			{
				if (filter.leftIsColumn() && filter.rightIsColumn())
				{
					left = getSubtreeForCol(filter.leftColumn(), subtrees);
					right = getSubtreeForCol(filter.rightColumn(), subtrees);
					SubtreePair pair = new SubtreePair(left, right);
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
					
					likelihood *= (card(left) * card(right));
					break;
				}
			}
			
			if (left != null)
			{
				for (SelectOperator s2 : selects)
				{
					if (s2 != select)
					{
						ArrayList<Filter> filters2 = s2.getFilter();
						Operator l2 = null;
						Operator r2 = null;
						for (Filter f2 : filters2)
						{
							if (f2.leftIsColumn() && f2.rightIsColumn())
							{
								l2 = getSubtreeForCol(f2.leftColumn(), subtrees);
								r2 = getSubtreeForCol(f2.rightColumn(), subtrees);
								if ((l2 == left && r2 == right) || (l2 == right && r2 == left))
								{
									if (l2 == left && r2 == right)
									{
										if (filters2.size() == 1 && f2.op().equals("E") && lefts.size() > 0)
										{
											lefts.add(f2.leftColumn());
											rights.add(f2.rightColumn());
										}
										else
										{
											lefts.clear();
											rights.clear();
										}
									}
									else
									{
										if (filters2.size() == 1 && f2.op().equals("E") && lefts.size() > 0)
										{
											rights.add(f2.leftColumn());
											lefts.add(f2.rightColumn());
										}
										else
										{
											lefts.clear();
											rights.clear();
										}
									}
									likelihood *= meta.likelihood(new ArrayList<Filter>(s2.getFilter()), tx, clone);
									break;
								}
							}
						}
					}
				}
			}
			
			if (lefts.size() > 0 && rights.size() > 0)
			{
				String leftTable = getTable(left);
				String rightTable = getTable(right);
				
				if (leftTable != null && rightTable != null)
				{
					String lschema = leftTable.substring(0, leftTable.indexOf('.'));
					String ltable = leftTable.substring(leftTable.indexOf('.') + 1);
					String rschema = rightTable.substring(0, rightTable.indexOf('.'));
					String rtable = rightTable.substring(rightTable.indexOf('.') + 1);
					PartitionMetaData lpmd = meta.getPartMeta(lschema, ltable, tx);
					PartitionMetaData rpmd = meta.getPartMeta(rschema, rtable, tx);
					
					if (lpmd.noNodeGroupSet() && rpmd.noNodeGroupSet())
					{
						if (lpmd.getNodeHash() != null && lefts.equals(lpmd.getNodeHash()) && rpmd.getNodeHash() != null && rights.equals(rpmd.getNodeHash()))
						{
							if (likelihood < minColocated)
							{
								minColocated = likelihood;
								minSelect2 = select;
							}
						}
					}
				}
			}
			
			//HRDBMSWorker.logger.debug("Estimated join cardinality = " + likelihood); //DEBUG
			
			if (likelihood < minLikelihood)
			{
				minLikelihood = likelihood;
				minSelect = select;
			}
		}
		
		if (minColocated <= minLikelihood * 2)
		{
			//HRDBMSWorker.logger.debug("Chose " + minColocated); //DEBUG
			minSelect = minSelect2;
		}
		else
		{
			//HRDBMSWorker.logger.debug("Chose " + minLikelihood); //DEBUG
		}

		selects.remove(minSelect);
		return minSelect;
	}
	
	private class SubtreePair
	{
		private Operator left;
		private Operator right;
		
		public SubtreePair(Operator left, Operator right)
		{
			this.left = left;
			this.right = right;
		}
		
		public int hashCode()
		{
			return left.hashCode() + right.hashCode();
		}
		
		public boolean equals(Object o)
		{
			SubtreePair rhs = (SubtreePair)o;
			
			return (this.left == rhs.left && this.right == rhs.right) || (this.left == rhs.right && this.right == rhs.left);
		}
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

		HRDBMSWorker.logger.debug("Can't find subtree for column " + col + " in...");
		for (Operator op : subtrees)
		{
			HRDBMSWorker.logger.debug(op.getCols2Pos().keySet());
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
	
	private void pushDownSelects2(Operator op) throws Exception
	{
		if (op instanceof SelectOperator)
		{
			SelectOperator sop = (SelectOperator)op;
			ArrayList<Filter> filters = sop.getFilter();
			
			for (Filter filter : filters)
			{
				if (filter.leftIsColumn() && filter.rightIsColumn())
				{
					String ltable = meta.getTableForCol(filter.leftColumn(), root);
					String rtable = meta.getTableForCol(filter.rightColumn(), root);
					
					if (ltable != null && rtable != null && ltable.contains(".") && rtable.contains(".") && ltable.length() > 2 && rtable.length() > 2 && ltable.equals(rtable))
					{}
					else
					{
						Operator child = op.children().get(0);
						if (child instanceof ProductOperator)
						{
							int i = 0;
							while (i < op.children().size())
							{
								child = op.children().get(i);
								pushDownSelects2(child);
								i++;
							}
						
							return;
						}
					}
				}
			}
			
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

					pushDownSelects2(newOp);
				}
				else
				{
					final Operator selectChild = op.children().get(0);
					if (selectChild != null)
					{
						pushDownSelects2(selectChild);
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
				pushDownSelects2(child);
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
		ArrayList<SelectOperator> delay = new ArrayList<SelectOperator>();
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
							Operator temp = left;
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
				
				for (SelectOperator s2 : (ArrayList<SelectOperator>)selectsCopy.clone())
				{
					if (s2 != select)
					{
						ArrayList<Filter> f2s = s2.getFilter();
						boolean ok = true;
						for (Filter f2 : f2s)
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

		//for (final SelectOperator select : selects)
		//{
		//	select.add(subtrees.get(0));
		//	subtrees.remove(0);
		//	subtrees.add(select);
		//}

		// everything needs to be removed and readded for all the operators in
		// the rest of the top
		Operator next = subtrees.get(0);
		for (SelectOperator sop : delay)
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
	
	public long card(Operator op) throws Exception
	{
		if (op instanceof AntiJoinOperator)
		{
			return (long)((1 - meta.likelihood(((AntiJoinOperator)op).getHSHM(), root, tx, op)) * card(op.children().get(0)));
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
			final long retval = (long)(card(op.children().get(0)) * card(op.children().get(1)) * max); 
			return retval;
		}
		
		if (op instanceof IntersectOperator)
		{
			long lCard = card(op.children().get(0));
			long rCard = card(op.children().get(1));
			
			if (lCard <= rCard)
			{
				return lCard;
			}
			else
			{
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
			final long retval = (long)(card(op.children().get(0)) * card(op.children().get(1)) * max); 
			return retval;
		}

		if (op instanceof NetworkReceiveOperator)
		{
			long retval = 0;
			for (final Operator o : op.children())
			{
				retval += card(o);
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
			return card(op.children().get(0)) * card(op.children().get(1));
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
			return (long)(meta.likelihood(((SemiJoinOperator)op).getHSHM(), root, tx, op) * card(op.children().get(0)));
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
			final long retval = ((TopOperator)op).getRemaining();
			final long retval2 = card(op.children().get(0));

			if (retval2 < retval)
			{
				return retval2;
			}
			else
			{
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

			return retval;
		}

		if (op instanceof YearOperator)
		{
			return card(op.children().get(0));
		}

		if (op instanceof TableScanOperator)
		{
			PartitionMetaData pmd = meta.getPartMeta(((TableScanOperator)op).getSchema(), ((TableScanOperator)op).getTable(), tx);
			int numNodes = pmd.getNumNodes();
			return (long)((1.0 / numNodes) * meta.getTableCard(((TableScanOperator)op).getSchema(), ((TableScanOperator)op).getTable(), tx));
		}

		HRDBMSWorker.logger.error("Unknown operator in card() in Phase1: " + op.getClass());
		throw new Exception("Unknown operator in card() in Phase1: " + op.getClass());
	}
	
	public static void printTree(Operator op, int indent)
	{
		String line = "";
		int i = 0;
		while (i < indent)
		{
			line += " ";
			i++;
		}

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
}
