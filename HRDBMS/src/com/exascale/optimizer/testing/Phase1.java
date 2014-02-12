package com.exascale.optimizer.testing;

import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.ArrayList;

import com.exascale.optimizer.testing.MetaData.PartitionMetaData;

public final class Phase1 
{
	protected RootOperator root;
	protected boolean pushdownHadResults;
	protected int ADDITIONAL_PUSHDOWNS = 25;

	public Phase1(RootOperator root)
	{
		this.root = root;
	}
	
	public void optimize() throws Exception
	{
		//System.out.println("Final generated is " + root.getGenerated());
		long start = System.currentTimeMillis();
		reorderProducts(root);
		//Driver.printTree(0,  root); //DEBUG
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
		//Driver.printTree(0,  root); //DEBUG
		combineProductsAndSelects(root);
		//Driver.printTree(0, root); //DEBUG
		removeDuplicateTables();
		//Driver.printTree(0, root); //DEBUG
		pushUpSelects(root);
		//Driver.printTree(0, root); //DEBUG
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
	
	protected void pushDownProjects()
	{
		HashSet<String> references = new HashSet<String>();
		for (Operator o : root.children())
		{
			getReferences(o, references);
		}
		
		ArrayList<Operator> leaves = getLeaves(root);
		HashSet hs = new HashSet();
		hs.addAll(leaves);
		leaves.clear();
		leaves.addAll(hs);
		
		for (Operator op : leaves)
		{
			if (op instanceof TableScanOperator)
			{
				try
				{
					TableScanOperator table = (TableScanOperator)op;
					HashMap<String, Integer> cols2Pos = root.getMeta().getCols2PosForTable(table.getSchema(), table.getTable());
					ArrayList<String> needed = new ArrayList<String>(references.size());
					for (String col : references)
					{
						if (cols2Pos.containsKey(col))
						{
							needed.add(col);
						}
					}
					
					if (needed.size() == 0)
					{
						needed.add(new ArrayList<String>(cols2Pos.keySet()).get(0));
					}
					table.setNeededCols(needed);
				}
				catch(Exception e)
				{
					e.printStackTrace();
					System.exit(1);
				}
			}
		}
		
		ArrayList<Operator> leaves2 = new ArrayList<Operator>();
		for (Operator op : leaves)
		{
			while (! (op instanceof RootOperator))
			{
				try
				{
					if (op instanceof TableScanOperator)
					{
						ArrayList<Operator> parents = (ArrayList<Operator>)((TableScanOperator)op).parents().clone(); 
						for (Operator op2 : parents)
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
						Operator parent = op.parent();
						parent.removeChild(op);
						parent.add(op);
						op = parent;
					}
				}
				catch(Exception e)
				{
					e.printStackTrace();
					System.exit(1);
				}
			}
			
			for (Operator op2 : leaves2)
			{
				while (! (op2 instanceof RootOperator))
				{
					try
					{
						Operator parent = op2.parent();
						parent.removeChild(op2);
						parent.add(op2);
						op2 = parent;
					}
					catch(Exception e)
					{
						e.printStackTrace();
						System.exit(1);
					}
				}
			}
		}
	}
	
	protected ArrayList <Operator> getLeaves(Operator op)
	{
		if (op.children().size() == 0)
		{
			ArrayList<Operator> retval = new ArrayList<Operator>(1);
			retval.add(op);
			return retval;
		}
		
		ArrayList<Operator> retval = new ArrayList<Operator>();
		for (Operator o : op.children())
		{
			retval.addAll(getLeaves(o));
		}
		
		return retval;
	}
	
	protected void getReferences(Operator o, HashSet<String> references)
	{
		references.addAll(o.getReferences());
		for (Operator op : o.children())
		{
			getReferences(op, references);
		}
	}
	
	protected void reorderProducts(Operator op)
	{
		try
		{
			if (op instanceof SelectOperator)
			{
				ArrayList<SelectOperator> selects = new ArrayList<SelectOperator>();
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
		catch(Exception e)
		{
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	protected void reorderProducts(Operator op, ArrayList<SelectOperator> selects, Operator top) throws Exception
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
		
		ArrayList<Operator> subtrees = new ArrayList<Operator>();
		ArrayList<Operator> queued = new ArrayList<Operator>();
		queued.add(op);
		while (queued.size() != 0)
		{
			op = queued.remove(0);
			while (true)
			{
				Operator left = op.children().get(0);
				Operator right = op.children().get(1);
				if ((!(left instanceof ProductOperator)) && (!(right instanceof ProductOperator)))
				{
					subtrees.add(left);
					subtrees.add(right);
					op.removeChild(left);
					op.removeChild(right);
					break;
				}
				else if (! (left instanceof ProductOperator))
				{
					subtrees.add(left);
					op.removeChild(left);
					op.removeChild(right);
					op = right;
				}
				else if (! (right instanceof ProductOperator))
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
		
		ArrayList<Operator> backup = (ArrayList<Operator>)subtrees.clone();
		
		//stuff above top
		//string of selects
		//logically product all...
		//subtrees
		Operator onTop = top.parent();
		for (SelectOperator select : selects)
		{
			select.parent().removeChild(select);
		}
		selects.get(selects.size() - 1).removeChild(selects.get(selects.size() - 1).children().get(0));
		ArrayList<SelectOperator> selectsCopy = (ArrayList<SelectOperator>)selects.clone();
		
		Operator newProd = null;
		while (selectsCopy.size() > 0)
		{
			SelectOperator select = getMostPromisingSelect(selectsCopy, subtrees, newProd);
			ArrayList<Filter> filters = select.getFilter();
			int i = 0;
			while (i < filters.size())
			{
				Filter filter = filters.get(i);
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
		
		//if only 1 in subtrees, just stick selects on top and attach to the rest of the top
		//else product remaining subtrees and then do that
		while (subtrees.size() > 1)
		{
			newProd = new ProductOperator(subtrees.get(0).getMeta());
			newProd.add(subtrees.get(0));
			subtrees.remove(0);
			newProd.add(subtrees.get(0));
			subtrees.remove(0);
			subtrees.add(newProd);
		}
		
		for (SelectOperator select : selects)
		{
			select.add(subtrees.get(0));
			subtrees.remove(0);
			subtrees.add(select);
		}
		
		//everything needs to be removed and readded for all the operators in the rest of the top
		onTop.add(subtrees.get(0));
		Operator o = onTop.parent();
		if (o != null)
		{
			while (! (o instanceof RootOperator))
			{
				int i = 0;
				while (i < o.children().size())
				{
					Operator child = o.children().get(i);
					o.removeChild(child);
					o.add(child);
					i++;
				}
			
				o = o.parent();
			}
		
			int i = 0;
			while (i < o.children().size())
			{
				Operator child = o.children().get(i);
				o.removeChild(child);
				o.add(child);
				i++;
			}
		}
		//call reorderProducts() on everything that was originally in subtrees
		
		for (Operator operator : backup)
		{
			reorderProducts(operator);
		}
	}
	
	protected Operator getSubtreeForCol(String col, ArrayList<Operator> subtrees)
	{
		for (Operator op : subtrees)
		{
			if (op.getCols2Pos().containsKey(col))
			{
				return op;
			}
		}
		
		return null;
	}
	
	protected void combineProductsAndSelects(Operator op) throws Exception
	{
		if (op instanceof SelectOperator)
		{
			ArrayList<Filter> filters = ((SelectOperator)op).getFilter();
			boolean isJoin = true;
			for (Filter filter : filters)
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
				for (Operator child2 : op.children())
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
				JoinOperator join = JoinOperator.manufactureJoin((JoinOperator)child, (SelectOperator)op, op.getMeta());
				ArrayList<Operator> children = (ArrayList<Operator>)child.children().clone();
				for (Operator child2 : children)
				{
					child.removeChild(child2);
				}
				
				Operator productParent = child.parent();
				if (productParent != op)
				{
					productParent.removeChild(child);
				}
				
				for (Operator child2 : children)
				{
					join.add(child2);
				}
				
				if (productParent != op)
				{
					productParent.add(join);
				}
				
				Operator selectChild = op.children().get(0);
				op.removeChild(selectChild);
				Operator selectParent = op.parent();
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
				for (Operator child2 : op.children())
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
					for (Operator child2 : op.children())
					{
						combineProductsAndSelects(child2);
					}
					
					break;
				}
				catch(ConcurrentModificationException e)
				{
					continue;
				}
			}
		}
	}
	
	protected void mergeSelectsAndTableScans(Operator op)
	{
		try
		{
			if (op instanceof SelectOperator)
			{
				//System.out.println("Found a SelectOperator");
				Operator child = op.children().get(0);
				if (child instanceof TableScanOperator)
				{
					//System.out.println("That had a TableScanOperator as a child");
					Operator parent = op;
					op = child;
					Operator grandParent = parent.parent();
					((TableScanOperator)op).addFilter(((SelectOperator)parent).getFilter(), grandParent, grandParent.parent());
					parent.removeChild(op);
					grandParent.removeChild(parent);
					grandParent.add(op);
					mergeSelectsAndTableScans(grandParent);
					pushdownHadResults = true;
				}
				else
				{
					for (Operator child2 : op.children())
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
					Operator child = op.children().get(i);
					mergeSelectsAndTableScans(child);
					i++;
				}
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	protected void pushDownSelects(Operator op)
	{
		if (op instanceof SelectOperator)
		{
			ArrayList<Operator> children = op.children();
			Operator child = null;
			//SelectOperators only have 1 child
			for (Operator o : children)
			{
				child = o;
			}
			
			//look at opportunity to push down across child
			if (child instanceof TableScanOperator)
			{
				return;
			}
			
			int i = 0;
			while (i < child.children().size())
			//for (Operator grandChild : grandChildren)
			{
				Operator grandChild = child.children().get(i);
				//can I push down to be a parent of this operator?
				ArrayList<String> references = ((SelectOperator)op).getReferences();
				if (child instanceof RenameOperator)
				{
					((RenameOperator)child).reverseUpdateReferences(references);
				}
				
				if (child instanceof HashJoinOperator)
				{
					((HashJoinOperator)child).reverseUpdateReferences(references, i);
				}
				
				HashMap<String, Integer> gcCols2Pos = grandChild.getCols2Pos();
				boolean ok = true;
				for (String reference : references)
				{
					if (!gcCols2Pos.containsKey(reference))
					{
						//System.out.println("Choosing not to push down " + op + " because " + grandChild + " cols2Pos is " + gcCols2Pos + " which does not contain " + reference);
						ok = false;
						break;
					}
				}
				
				if (ok)
				{
					Operator parent = op.parent();
					
					if (parent != null)
					{
						parent.removeChild(op);
						op.removeChild(child);
					}
					
					Operator newOp = null;
					if (child instanceof RenameOperator)
					{
						//revereseUpdate must create a new SelectOperator
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
					catch(Exception e)
					{
						e.printStackTrace();
						System.exit(1);
					}
					
					if (! (child instanceof SelectOperator))
					{
						pushdownHadResults = true;
					}
					
					pushDownSelects(newOp);
				}
				else
				{
					Operator selectChild = op.children().get(0);
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
				Operator child = op.children().get(i);
				pushDownSelects(child);
				i++;
			}
		}
	} 
	
	protected void pushUpSelects(Operator op)
	{
		if (op instanceof SelectOperator)
		{
			Operator parent = op.parent();
			if (! (parent instanceof RootOperator))
			{
				ArrayList<String> references = ((SelectOperator)op).getReferences();
				boolean ok = true;
				if (parent instanceof RenameOperator)
				{
					((RenameOperator)parent).updateReferences(references);
				}
				
				//parent output has to have all the columns the select references
				HashMap<String, Integer> parentCols2Pos = parent.getCols2Pos();
				for (String reference : references)
				{
					if (!parentCols2Pos.containsKey(reference))
					{
						ok = false;
						break;
					}
				}
				
				if (ok)
				{
					//go ahead and move
					ArrayList<Operator> next = (ArrayList<Operator>)op.children().clone();
					if (parent instanceof RenameOperator)
					{
						((RenameOperator)parent).updateSelectOperator((SelectOperator)op);
					}
					
					ArrayList<Operator> children = (ArrayList<Operator>)op.children().clone();
					parent.removeChild(op);
					Operator grandParent = parent.parent();
					grandParent.removeChild(parent);
					
					try
					{
						for (Operator child : children)
						{
							op.removeChild(child);
							parent.add(child);
						}
						op.add(parent);
						grandParent.add(op);
					}
					catch(Exception e)
					{
						e.printStackTrace();
						System.exit(1);
					}
					
					pushUpSelects(op, next);
					for (Operator o : next)
					{
						pushUpSelects(o);
					}
				}
				else
				{
					for (Operator child : op.children())
					{
						pushUpSelects(child);
					}
				}
			}
		}
		else
		{
			for (Operator child : (ArrayList<Operator>)op.children().clone())
			{
				pushUpSelects(child);
			}
		}
	} 
	
	protected void pushUpSelects(Operator op, ArrayList<Operator> next)
	{
		if (op instanceof SelectOperator)
		{
			Operator parent = op.parent();
			if (! (parent instanceof RootOperator))
			{
				ArrayList<String> references = ((SelectOperator)op).getReferences();
				boolean ok = true;
				if (parent instanceof RenameOperator)
				{
					((RenameOperator)parent).updateReferences(references);
				}
				
				//parent output has to have all the columns the select references
				HashMap<String, Integer> parentCols2Pos = parent.getCols2Pos();
				for (String reference : references)
				{
					if (!parentCols2Pos.containsKey(reference))
					{
						ok = false;
						break;
					}
				}
				
				if (ok)
				{
					//go ahead and move
					if (parent instanceof RenameOperator)
					{
						((RenameOperator)parent).updateSelectOperator((SelectOperator)op);
					}
					
					ArrayList<Operator> children = (ArrayList<Operator>)op.children().clone();
					parent.removeChild(op);
					Operator grandParent = parent.parent();
					grandParent.removeChild(parent);
					
					try
					{
						for (Operator child : children)
						{
							op.removeChild(child);
							parent.add(child);
						}
						op.add(parent);
						grandParent.add(op);
					}
					catch(Exception e)
					{
						e.printStackTrace();
						System.exit(1);
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
			System.out.println("This should never happen!");
			System.exit(1);
		}
	} 
	
	protected void removeDuplicateTables() throws Exception
	{
		HashMap<TableScanOperator, TableScanOperator> tables = getTableList(root);
		replaceTableScanOperators(root, tables);
	}
	
	protected void replaceTableScanOperators(Operator op, HashMap<TableScanOperator, TableScanOperator> tables) throws Exception
	{
		if (op instanceof TableScanOperator)
		{
			TableScanOperator lookup = tables.get(op);
			if (lookup != op)
			{
				//replace op with lookup and fix pointers
				Operator parent = ((TableScanOperator)op).firstParent();
				parent.removeChild(op);
				parent.add(lookup);
			}
			
			return;
		}
		
		ArrayList<Operator> children = (ArrayList<Operator>)op.children().clone();
		for (Operator child : children)
		{
			replaceTableScanOperators(child, tables);
		}
	}
	
	protected HashMap<TableScanOperator, TableScanOperator> getTableList(Operator op)
	{
		HashMap<TableScanOperator, TableScanOperator> retval = null;
		if (op instanceof TableScanOperator)
		{
			retval = new HashMap<TableScanOperator, TableScanOperator>();
			retval.put((TableScanOperator)op, (TableScanOperator)op);
		}
		
		for (Operator child : op.children())
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
	
	protected SelectOperator getMostPromisingSelect(ArrayList<SelectOperator> selects, ArrayList<Operator> subtrees, Operator current)
	{
		double minLikelihood = Double.MAX_VALUE;
		SelectOperator minSelect = null;
		
		if (current == null)
		{
			for (SelectOperator select: selects)
			{
				double likelihood = select.likelihood(root);
				if (likelihood < minLikelihood)
				{
					minLikelihood = likelihood;
					minSelect = select;
				}
				else if (likelihood == minLikelihood)
				{
					ArrayList<Filter> filters = select.getFilter();
					if (filters.size() == 1)
					{
						if (filters.get(0).leftIsColumn())
						{
							String t = root.getMeta().getTableForCol(filters.get(0).leftColumn());
							if (t != null)
							{
								PartitionMetaData pmeta = root.getMeta().getPartMeta("TPCH", t);
								if (pmeta.noNodeGroupSet() && pmeta.nodeIsHash() && pmeta.getNodeHash().contains(filters.get(0).leftColumn()))
								{
									minSelect = select;
								}
							}
						}
						else if (filters.get(0).rightIsColumn())
						{
							String t = root.getMeta().getTableForCol(filters.get(0).rightColumn());
							if (t != null)
							{
								PartitionMetaData pmeta = root.getMeta().getPartMeta("TPCH", t);
								if (pmeta.noNodeGroupSet() && pmeta.nodeIsHash() && pmeta.getNodeHash().contains(filters.get(0).rightColumn()))
								{
									minSelect = select;
								}
							}
						}
					}
				}
			}
			
			selects.remove(minSelect);
			return minSelect;
		}
		
		for (SelectOperator select : selects)
		{
			ArrayList<Filter> filters = select.getFilter();
			for (Filter filter : filters)
			{
				if (filter.leftIsColumn() && filter.rightIsColumn())
				{
					if (current.getCols2Pos().containsKey(filter.leftColumn()))
					{
						double likelihood = select.likelihood(root);
						if (likelihood < minLikelihood)
						{
							minLikelihood = likelihood;
							minSelect = select;
						}
					}
					
					if (current.getCols2Pos().containsKey(filter.rightColumn()))
					{
						double likelihood = select.likelihood(root);
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
		
		for (SelectOperator select: selects)
		{
			double likelihood = select.likelihood(root);
			if (likelihood < minLikelihood)
			{
				minLikelihood = likelihood;
				minSelect = select;
			}
		}
		
		selects.remove(minSelect);
		return minSelect;
	}
}
