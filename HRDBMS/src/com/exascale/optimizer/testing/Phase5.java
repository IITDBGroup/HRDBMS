package com.exascale.optimizer.testing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.ArrayList;

import com.exascale.optimizer.testing.MetaData.PartitionMetaData;

public class Phase5
{
	private static long WORKER_MEMORY = 37L * 1024L * 1024L * 1024L;
	protected RootOperator root;
	protected MetaData meta;
	protected HashMap<Operator, Long> cCache = new HashMap<Operator, Long>();
	
	public Phase5(RootOperator root)
	{
		this.root = root;
		meta = root.getMeta();
	}
	
	public void optimize()
	{
		addIndexesToTableScans();
		setCards(root);
		addIndexesToJoins();
		setNumParents(root);
	}
	
	private void setNumParents(Operator op)
	{
		if (op instanceof NetworkSendOperator)
		{
			if (!((NetworkSendOperator) op).setNumParents())
			{
				return;
			}
			((NetworkSendOperator)op).clearParent();
		}
		
		for (Operator o : op.children())
		{
			setNumParents(o);
		}
	}
	
	private void setCards(Operator op)
	{
		if (op instanceof MultiOperator)
		{
			long xl = card(op);
			long yl = card(op.children().get(0));
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
			long xl = card(op.children().get(1));
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
			long xl = card(op.children().get(1));
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
			long xl = card(op.children().get(1));
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
			long xl = card(op.children().get(1));
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
			long xl = card(op.children().get(1));
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
			long xl = card(op.children().get(0));
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
				long xl = card(op);
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
			for (Operator o : op.children())
			{
				setCards(o);
			}
		}
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
		
		for (Operator o : op.children())
		{
			setCardForIntersectAndUnion(o, card);
		}
	}
	
	private void addIndexesToJoins()
	{
		ArrayList<Operator> x = getJoins(root);
		HashSet<Operator> joins = new HashSet<Operator>(x);
		ArrayList<Operator> joins2 = getEligibleJoins(joins);
		ArrayList<Operator> joins3 = filtersEnough(joins2);
		ArrayList<Operator> joins4 = hasIndex(joins3);
		for (Operator j : joins4)
		{
			//System.out.println("Using index for " + j);
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
	
	private ArrayList<Index> getDynamicIndex(Operator op)
	{
		HashSet<HashMap<Filter, Filter>> hshm = null;
		ArrayList<Index> retval = new ArrayList<Index>();
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
		
		ArrayList<String> cols = new ArrayList<String>(hshm.size());
		for (HashMap<Filter, Filter> hm : hshm)
		{
			Filter f = (Filter)new ArrayList(hm.keySet()).get(0);
			if (op.children().get(1).getCols2Pos().keySet().contains(f.leftColumn()))
			{
				cols.add(f.leftColumn());
			}
			
			if (op.children().get(1).getCols2Pos().keySet().contains(f.rightColumn()))
			{
				cols.add(f.rightColumn());
			}
		}
		
		boolean rename = reverseUpdateReferences(op.children().get(1), cols);
		HashMap<String, String> renames = null;
		if (rename)
		{
			renames = getIndexRenames(op.children().get(1));
		}
		
		TableScanOperator table = getTables(op.children().get(1)).get(0);
		ArrayList<Index> indexes = meta.getIndexesForTable(table.getSchema(), table.getTable());
		Index index = this.getIndexFor(indexes, cols);
		
		//if index is already being used, set it to run delayed and return
		//else add new index into operator tree, set it to run delayed and return
		if (table.children().size() > 0)
		{
			for (Operator o : table.children())
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
			Index i = index.clone();
			i.runDelayed();
			if (rename)
			{
				i.setRenames(renames);
			}
			IndexOperator io = new IndexOperator(i, meta);
			ArrayList<String> cols2 = new ArrayList<String>(1);
			cols2.add(new ArrayList<String>(io.getPos2Col().values()).get(0));
			ArrayList<Boolean> orders = new ArrayList<Boolean>(1);
			orders.add(true);
			SortOperator sort = new SortOperator(cols2, orders, meta);
			UnionOperator union = new UnionOperator(true, meta);
			try
			{
				union.add(io);
				sort.add(union);
				table.add(sort);
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(1);
			}
			
			correctForDevices(table);
			return getIndexes(table);
		}
		
		return retval;
	}
	
	private HashMap<String, String> getIndexRenames(Operator op)
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
	
	private ArrayList<Index> getIndexes(Operator op)
	{
		ArrayList<Index> retval = new ArrayList<Index>();
		if (op instanceof IndexOperator)
		{
			retval.add(((IndexOperator)op).getIndex());
		}
		else
		{
			for (Operator o : op.children())
			{
				retval.addAll(getIndexes(o));
			}
		}
		
		return retval;
	}
	
	private void addIndexForJoin(Operator op, Index index)
	{
		Operator o = op;
		if (o instanceof SortOperator)
		{
			o = o.children().get(0);
		}
		
		if (o instanceof IntersectOperator)
		{
			IndexOperator io = new IndexOperator(index, meta);
			try
			{
				o.add(io);
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(1);
			}
			
			return;
		}
		
		IntersectOperator intersect = new IntersectOperator(meta);
		Operator parent = o.parent();
		parent.removeChild(o);
		try
		{
			intersect.add(o);
			UnionOperator union = new UnionOperator(true, meta);
			IndexOperator io = new IndexOperator(index, meta);
			union.add(io);
			intersect.add(union);
			parent.add(intersect);
		}
		catch(Exception e)
		{
			e.printStackTrace();
			System.exit(1);
		}
		
		//cCache.clear();
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
			for (Operator o : op.children())
			{
				Index i = getIndexForJoin(o, fileName);
				if (i != null)
				{
					return i;
				}
			}
			
			return null;
		}
	}
	
	private ArrayList<Operator> hasIndex(ArrayList<Operator> joins)
	{
		ArrayList<Operator> retval = new ArrayList<Operator>();
		for (Operator op : joins)
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
			
			ArrayList<String> cols = new ArrayList<String>(hshm.size());
			for (HashMap<Filter, Filter> hm : hshm)
			{
				Filter f = (Filter)new ArrayList(hm.keySet()).get(0);
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
			
			TableScanOperator table = getTables(op.children().get(1)).get(0);
			ArrayList<Index> indexes = meta.getIndexesForTable(table.getSchema(), table.getTable());
			Index index = this.getIndexFor(indexes, cols);
			if (index == null)
			{
				System.out.println("WARNING: Wanted to use an index on " + cols + " but none existed.");
			}
			else
			{
				retval.add(op);
			}
		}
		
		return retval;
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
	
	private ArrayList<Operator> filtersEnough(ArrayList<Operator> joins)
	{
		ArrayList<Operator> retval = new ArrayList<Operator>();
		for (Operator op : joins)
		{
			if (op instanceof HashJoinOperator)
			{
				long leftCard = card(op.children().get(0));
				long rightCard = card(op.children().get(1));
				//System.out.println("Right card = " + rightCard);
				if (leftCard == 0)
				{
					leftCard = 1;
				}
				//System.out.println("Left card = " + leftCard);
				System.out.println(meta.likelihood(((HashJoinOperator)op).getHSHM(), root) * leftCard);
				System.out.println("Before multiplier: " + meta.likelihood(((HashJoinOperator)op).getHSHM(), root));
				if (leftCard < rightCard && meta.likelihood(((HashJoinOperator)op).getHSHM(), root) * leftCard <= 1.0 / 400.0)
				{
					retval.add(op);
				}
				//else if (memoryUsageTooHigh(op.children().get(1), op.children().get(0)))
				//{
				//	retval.add(op);
				//	System.out.println("Memory usage too high for hash table. " + op);
				//}
			}
			else if (op instanceof NestedLoopJoinOperator)
			{
				HashSet<HashMap<Filter, Filter>> hshm = ((NestedLoopJoinOperator)op).getHSHM();
				boolean doIt = true;
				for (HashMap<Filter, Filter> hm : hshm)
				{
					if (hm.size() > 1)
					{
						doIt = false;
						break;
					}
				}
				
				if (doIt)
				{
					long rightCard = card(op.children().get(1));
					//System.out.println("Right card = " + rightCard);
					long leftCard = card(op.children().get(0));
					if (leftCard == 0)
					{
						leftCard = 1;
					}
					//System.out.println("Left card = " + leftCard);
					System.out.println(meta.likelihood(hshm, root) * leftCard);
					System.out.println("Before multiplier: " + meta.likelihood(hshm, root));
					if (leftCard < rightCard && meta.likelihood(hshm, root) * leftCard <= 1.0 / 400.0)
					{
						retval.add(op);
					}
					//else if (memoryUsageTooHigh(op.children().get(1), op.children().get(0)))
					//{
					//	retval.add(op);
					//	System.out.println("Memory usage too high for hash table. " + op);
					//}
				}
			}
			else if (op instanceof SemiJoinOperator)
			{
				HashSet<HashMap<Filter, Filter>> hshm = ((SemiJoinOperator)op).getHSHM();
				boolean doIt = true;
				for (HashMap<Filter, Filter> hm : hshm)
				{
					if (hm.size() > 1)
					{
						doIt = false;
						break;
					}
				}
				
				if (doIt)
				{
					long rightCard = card(op.children().get(1));
					//System.out.println("Right card = " + rightCard);
					long leftCard = card(op.children().get(0));
					if (leftCard == 0)
					{
						leftCard = 1;
					}
				//	System.out.println("Left card = " + leftCard);
					System.out.println(meta.likelihood(hshm, root) * leftCard);
					System.out.println("Before multiplier: " + meta.likelihood(hshm, root));
					if (leftCard < rightCard && meta.likelihood(hshm, root) * leftCard <= 1.0 / 400.0)
					{
						retval.add(op);
					}
					//else if (memoryUsageTooHigh(op.children().get(1), op.children().get(0)))
					//{
					//	retval.add(op);
					//	System.out.println("Memory usage too high for hash table. " + op);
					//}
				}
			}
			else if (op instanceof AntiJoinOperator)
			{
				HashSet<HashMap<Filter, Filter>> hshm = ((AntiJoinOperator)op).getHSHM();
				boolean doIt = true;
				for (HashMap<Filter, Filter> hm : hshm)
				{
					if (hm.size() > 1)
					{
						doIt = false;
						break;
					}
				}
				
				if (doIt)
				{
					long rightCard = card(op.children().get(1));
					//System.out.println("Right card = " + rightCard);
					long leftCard = card(op.children().get(0));
					if (leftCard == 0)
					{
						leftCard = 1;
					}
					//System.out.println("Left card = " + leftCard);
					System.out.println(meta.likelihood(hshm, root) * leftCard);
					System.out.println("Before multiplier: " + meta.likelihood(hshm, root));
					if (leftCard < rightCard && meta.likelihood(hshm, root) * leftCard <= 1.0 / 400.0)
					{
						retval.add(op);
					}
					//else if (memoryUsageTooHigh(op.children().get(1), op.children().get(0)))
					//{
					//	retval.add(op);
					//	System.out.println("Memory usage too high for hash table. " + op);
					//}
				}
			}
		}
		
		return retval;
	}
	
	private boolean memoryUsageTooHigh(Operator op, Operator left)
	{
		long calc = 48 * card(op);
		//System.out.println("Caclulated as needing " + (calc * 1.0 / (1024*1024*1024 * 1.0)) + "GB");
		if (48L * card(op) * 15L * 4500000L > WORKER_MEMORY * card(left))
		{
			return true;
		}
		else
		{
			return false;
		}
	}
	
	private ArrayList<Operator> getEligibleJoins(HashSet<Operator> joins)
	{
		ArrayList<Operator> retval = new ArrayList<Operator>();
		for (Operator op : joins)
		{
			ArrayList<TableScanOperator> tables = getTables(op.children().get(1));
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
	
	private ArrayList<TableScanOperator> getTables(Operator op)
	{
		ArrayList<TableScanOperator> retval = new ArrayList<TableScanOperator>();
		if (op instanceof TableScanOperator)
		{
			retval.add((TableScanOperator)op);
			return retval;
		}
		
		for (Operator o : op.children())
		{
			retval.addAll(getTables(o));
		}
		
		return retval;
	}
	
	private ArrayList<Operator> getJoins(Operator op)
	{
		ArrayList<Operator> retval = new ArrayList<Operator>();
		if (op instanceof HashJoinOperator || op instanceof NestedLoopJoinOperator || op instanceof SemiJoinOperator || op instanceof AntiJoinOperator)
		{
			retval.add(op);
		}
		
		for (Operator o : op.children())
		{
			retval.addAll(getJoins(o));
		}
		
		return retval;
	}
	
	private void addIndexesToTableScans()
	{
		ArrayList<TableScanOperator> s = getTableScans(root);
		HashSet<TableScanOperator> set = new HashSet<TableScanOperator>(s);
		
		for (TableScanOperator table : set)
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
						boolean indexOnly = indexOnly(table);
						//System.out.println("Using indexes.");
						//printIndexes(table);
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
	
	private void correctForDevices(TableScanOperator table)
	{
		for (Operator child : (ArrayList<Operator>)table.children().clone())
		{
			table.removeChild(child);
			ArrayList<Integer> devices = table.getDeviceList();
			for (int device : devices)
			{
				Operator clone = cloneTree(child);
				try
				{
					table.add(clone);
				}
				catch(Exception e)
				{
					e.printStackTrace();
					System.exit(1);
				}
				
				setDevice(clone, device);
				table.setChildForDevice(device, clone);
			}
		}
		
		//cCache.clear();
	}
	
	private void setDevice(Operator op, int device)
	{
		if (op instanceof IndexOperator)
		{
			((IndexOperator)op).setDevice(device);
		}
		else
		{
			for (Operator o : op.children())
			{
				setDevice(o, device);
			}
		}
	}
	
	private Operator cloneTree(Operator op)
	{
		Operator clone = op.clone();
		for (Operator o : op.children())
		{
			try
			{
				Operator child = cloneTree(o);
				clone.add(child);
				clone.setChildPos(op.getChildPos());
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(1);
			}
		}
		
		return clone;
	}
	
	private void useIndexes(String schema, String table, CNFFilter cnf, TableScanOperator tOp)
	{
		HashSet<HashMap<Filter, Filter>> hshm = cnf.getHSHM();
		ArrayList<Index> available = meta.getIndexesForTable(schema, table);
		for (HashMap<Filter, Filter> hm : (HashSet<HashMap<Filter, Filter>>)hshm.clone())
		{
			ArrayList<Index> indexes = new ArrayList<Index>();
			boolean doIt = true;
			double likely = 0;
			for (Filter f : hm.keySet())
			{
				likely += meta.likelihood(f, root);
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
				for (HashMap<Filter, Filter> hm2 : hshm)
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
							likely *= meta.likelihood(f, root);
						}
					}
				}
			}
			
			if (likely > (3.0 / 20.0))
			{
				continue;
			}
			
			for (Filter f : hm.keySet())
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
					doIt = false;
					if (f.leftIsColumn() && f.rightIsColumn())
					{
						System.out.println("WARNING: wanted to use index on " + f.leftColumn() + " and " + f.rightColumn() + " but none existed.");
					}
					else
					{
						System.out.println("WARNING: wanted to use index on " + column + " but none existed.");
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
				UnionOperator union = new UnionOperator(true, meta);
				try
				{
					for (Index index : indexes)
					{
						IndexOperator iOp = new IndexOperator(index, meta);
						union.add(iOp);
					}
				
					if (tOp.children().size() == 0)
					{
						tOp.add(union);
					}
					else if (tOp.children().get(0) instanceof IntersectOperator)
					{
						tOp.children().get(0).add(union);
					}
					else
					{
						IntersectOperator intersect = new IntersectOperator(meta);
						intersect.add(union);
						Operator otherChild = tOp.children().get(0);
						tOp.removeChild(otherChild);
						intersect.add(otherChild);
						tOp.add(intersect);
					}
				}
				catch(Exception e)
				{
					e.printStackTrace();
					System.exit(1);
				}
			}
		}
		
		cnf.setHSHM(hshm);
		//cCache.clear();
	}
	
	private Index getIndexFor(ArrayList<Index> indexes, String col)
	{
		for (Index index : indexes)
		{
			if (index.startsWith(col))
			{
				return index;
			}
		}
		
		for (Index index : indexes)
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
		for (Index index : indexes)
		{
			if (index.startsWith(col1) || index.startsWith(col2) && index.getCols().contains(col1) && index.getCols().contains(col2))
			{
				return index;
			}
		}
		
		for (Index index : indexes)
		{
			if (index.contains(col1) && index.contains(col2))
			{
				return index;
			}
		}
		
		return null;
	}
	
	private Index getIndexFor(ArrayList<Index> indexes, ArrayList<String> cols)
	{
		for (Index index : indexes)
		{
			if (cols.contains(index.getCols().get(0)) && index.getCols().containsAll(cols))
			{
				return index;
			}
		}
		
		for (Index index : indexes)
		{
			if (index.getCols().containsAll(cols))
			{
				return index;
			}
		}
		
		return null;
	}
	
	private ArrayList<TableScanOperator> getTableScans(Operator op)
	{
		ArrayList<TableScanOperator> retval = new ArrayList<TableScanOperator>();
		if (op instanceof TableScanOperator)
		{
			retval.add((TableScanOperator)op);
			return retval;
		}
		
		for (Operator o : op.children())
		{
			retval.addAll(getTableScans(o));
		}
		
		return retval;
	}
	
	private void addSort(TableScanOperator table)
	{
		Operator child = table.children().get(0);
		table.removeChild(child);
		ArrayList<String> cols = new ArrayList<String>(1);
		cols.add(child.getPos2Col().get(0));
		ArrayList<Boolean> orders = new ArrayList<Boolean>(1);
		orders.add(true);
		SortOperator sort = new SortOperator(cols, orders, meta);
		try
		{
			sort.add(child);
			table.add(sort);
		}
		catch(Exception e)
		{
			e.printStackTrace();
			System.exit(1);
		}
		
		//cCache.clear();
	}
	
	private void printIndexes(Operator op)
	{
		if (op instanceof IndexOperator)
		{
			System.out.println(op);
		}
		else
		{
			for (Operator o : op.children())
			{
				printIndexes(o);
			}
		}
	}
	
	public static void clearOpParents(Operator op)
	{
		if (op instanceof TableScanOperator)
		{
			((TableScanOperator)op).clearOpParents();
		}
		else
		{
			for (Operator o : op.children())
			{
				clearOpParents(o);
			}
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
			for (Operator o : op.children())
			{
				cleanupOrderedFilters(o);
			}
		}
	}
	
	private void reuseIndexes(TableScanOperator table)
	{
		if (table.children().get(0) instanceof UnionOperator)
		{
			return;
		}
	
		HashMap<String, IndexOperator> file2Index = new HashMap<String, IndexOperator>();
		for (Operator op : (ArrayList<Operator>)table.children().get(0).children().clone())
		{
			if (op.children().size() > 1)
			{
				continue;
			}
			
			IndexOperator index = (IndexOperator)op.children().get(0);
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
		
		//cCache.clear();
	}
	
	private void compoundIndexes(TableScanOperator table)
	{
		if (table.children().get(0) instanceof UnionOperator)
		{
			return;
		}
	
		HashSet<String> cols = new HashSet<String>();
		for (Operator op : table.children().get(0).children())
		{
			if (op.children().size() > 1)
			{
				continue;
			}
			
			IndexOperator index = (IndexOperator)op.children().get(0);
			cols.addAll(index.getReferencedCols());
		}
		
		Index index = meta.getBestCompoundIndex(cols, table.getSchema(), table.getTable());
		while (cols.size() > 0 && index != null)
		{
			for (Operator op : table.children().get(0).children())
			{
				if (op.children().size() > 1)
				{
					continue;
				}
				
				IndexOperator index2 = (IndexOperator)op.children().get(0);
				if (index.getCols().containsAll(index2.getReferencedCols()))
				{
					if (!index.getFileName().equals(index2.getFileName()))
					{
						//replace existing index with new one
						Index newIndex = index.clone();
						newIndex.setCondition(index2.getFilter());
						for (Filter filter : index2.getSecondary())
						{
							newIndex.addSecondaryFilter(filter);
						}
						
						index2.setIndex(newIndex);
					}
				}
			}
			
			cols.removeAll(index.getCols());
			index = meta.getBestCompoundIndex(cols, table.getSchema(), table.getTable());
		}
	}
	
	private void reuseCompoundIndexes(TableScanOperator table)
	{
		if (table.children().get(0) instanceof UnionOperator)
		{
			return;
		}
	
		HashMap<String, IndexOperator> file2Index = new HashMap<String, IndexOperator>();
		for (Operator op : (ArrayList<Operator>)table.children().get(0).children().clone())
		{
			if (op.children().size() > 1)
			{
				continue;
			}
			
			IndexOperator index = (IndexOperator)op.children().get(0);
			if (!file2Index.containsKey(index.getFileName()))
			{
				file2Index.put(index.getFileName(), index);
			}
			else
			{
				file2Index.get(index.getFileName()).addSecondaryFilter(index.getFilter());
				for (Filter filter : index.getSecondary())
				{
					file2Index.get(index.getFileName()).addSecondaryFilter(filter);
				}
				table.children().get(0).removeChild(op);
			}
		}
		
		//cCache.clear();
	}
	
	private boolean indexOnly(TableScanOperator table)
	{
		UnionOperator union = null;
		if (!(table.children().get(0) instanceof UnionOperator))
		{
			if (table.children().get(0).children().size() == 1)
			{
				union = (UnionOperator)table.children().get(0).children().get(0);
				System.out.println("Union parent = " + union.parent());
			}
			else
			{
				return false;
			}
		}
		else
		{
			union = (UnionOperator)table.children().get(0);
			System.out.println("Union parent = " + union.parent());
		}
		
		if (union.children().size() != 1)
		{
			return false;
		}
		
		IndexOperator index = (IndexOperator)union.children().get(0);
		ArrayList<String> references = new ArrayList<String>();
		references.addAll(table.getCols2Pos().keySet());
		for (String col : table.getCNFForParent(table.firstParent()).getReferences())
		{
			if (!references.contains(col))
			{
				references.add(col);
			}
		}
		if (index.getIndex().getCols().containsAll(references))
		{
			//index only access
			HashMap<String, String> cols2Types = null;
			try
			{
				cols2Types = meta.getCols2TypesForTable(table.getSchema(), table.getTable());
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(1);
			}
			ArrayList<String> types = new ArrayList<String>(references.size());
			for (String col : references)
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
					System.out.println("Parent = " + o);
					System.out.println("Child = " + child);
					o.removeChild(child);
					o.add(child);
					child = o;
					o = o.parent();
				}
			
				System.out.println("Parent = " + o);
				System.out.println("Child = " + child);
				o.removeChild(child);
				o.add(child);
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(1);
			}
			//cCache.clear();
			return true;
		}
		else
		{
			return false;
		}
	}
	
	private HashMap<String, Index> getCols2Indexes(Operator op)
	{
		HashMap<String, Index> retval = new HashMap<String, Index>();
		if (op instanceof IndexOperator)
		{
			Index index = ((IndexOperator)op).getIndex();
			ArrayList<String> cols = index.getCols();
			for (String col : cols)
			{
				retval.put(col,  index);
			}
			
			return retval;
		}
		else
		{
			for (Operator o : op.children())
			{
				retval.putAll(getCols2Indexes(o));
			}
			
			return retval;
		}
	}
	
	private void doubleCheckCNF(TableScanOperator table)
	{
		HashMap<String, Index> cols2Indexes = getCols2Indexes(table);
		CNFFilter cnf = table.getCNFForParent(table.firstParent());
		HashSet<HashMap<Filter, Filter>> hshm = cnf.getHSHM();
		for (HashMap<Filter, Filter> hm : (HashSet<HashMap<Filter, Filter>>)hshm.clone())
		{
			if (hm.size() > 1)
			{
				continue;
			}
			
			for (Filter f : hm.keySet())
			{
				ArrayList<String> references = new ArrayList<String>(2);
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
				for (String col : references)
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
	
	public long card(Operator op)
	{
		if (op instanceof AntiJoinOperator)
		{
			long retval = (long)((1 - meta.likelihood(((AntiJoinOperator)op).getHSHM(), root)) * card(op.children().get(0)));
			cCache.put(op,  retval);
			return retval;
		}
		
		if (op instanceof CaseOperator)
		{
			long retval = card(op.children().get(0));
			cCache.put(op,  retval);
			return retval;
		}
		
		if (op instanceof ExtendOperator)
		{
			long retval = card(op.children().get(0));
			cCache.put(op,  retval);
			return retval;
		}
		
		if (op instanceof HashJoinOperator)
		{
			long retval = (long)(card(op.children().get(0)) * card(op.children().get(1)) * meta.likelihood(((HashJoinOperator)op).getHSHM(), root));
			cCache.put(op,  retval);
			return retval;
		}
		
		if (op instanceof MultiOperator)
		{
			//return card(op.children().get(0));
			long groupCard = meta.getColgroupCard(((MultiOperator)op).getKeys(), root);
			if (groupCard > card(op.children().get(0)))
			{
				long retval = card(op.children().get(0));
				cCache.put(op,  retval);
				return retval;
			}
			
			long retval = groupCard;
			cCache.put(op,  retval);
			return retval;
		}
		
		if (op instanceof NestedLoopJoinOperator)
		{
			long retval = (long)(card(op.children().get(0)) * card(op.children().get(1)) * meta.likelihood(((NestedLoopJoinOperator)op).getHSHM(), root));
			cCache.put(op,  retval);
			return retval;
		}
		
		if (op instanceof NetworkReceiveOperator)
		{
			long retval = 0;
			for (Operator o : op.children())
			{
				retval += card(o);
			}
			
			cCache.put(op,  retval);
			return retval;
		}
		
		if (op instanceof NetworkHashAndSendOperator)
		{
			long retval = card(op.children().get(0)) / ((NetworkHashAndSendOperator)op).parents().size();
			cCache.put(op,  retval);
			return retval;
		}
		
		if (op instanceof NetworkSendRROperator)
		{
			long retval = card(op.children().get(0)) / ((NetworkSendRROperator)op).parents().size();
			cCache.put(op,  retval);
			return retval;
		}
		
		if (op instanceof NetworkSendOperator)
		{
			long retval = card(op.children().get(0));
			cCache.put(op,  retval);
			return retval;
		}
		
		if (op instanceof ProductOperator)
		{
			long retval = card(op.children().get(0)) * card(op.children().get(1));
			cCache.put(op,  retval);
			return retval;
		}
		
		if (op instanceof ProjectOperator)
		{
			long retval = card(op.children().get(0));
			cCache.put(op,  retval);
			return retval;
		}
		
		if (op instanceof RenameOperator)
		{
			long retval = card(op.children().get(0));
			cCache.put(op,  retval);
			return retval;
		}
		
		if (op instanceof ReorderOperator)
		{
			long retval = card(op.children().get(0));
			cCache.put(op, retval);
			return retval;
		}
		
		if (op instanceof RootOperator)
		{
			long retval = card(op.children().get(0));
			cCache.put(op,  retval);
			return retval;
		}
		
		if (op instanceof SelectOperator)
		{
			long retval = (long)(((SelectOperator)op).likelihood(root) * card(op.children().get(0)));
			cCache.put(op,  retval);
			return retval;
		}
		
		if (op instanceof SemiJoinOperator)
		{
			long retval = (long)(meta.likelihood(((SemiJoinOperator)op).getHSHM(), root) * card(op.children().get(0)));
			cCache.put(op,  retval);
			return retval;
		}
		
		if (op instanceof SortOperator)
		{
			long retval = card(op.children().get(0));
			cCache.put(op,  retval);
			return retval;
		}
		
		if (op instanceof SubstringOperator)
		{
			long retval = card(op.children().get(0));
			cCache.put(op,  retval);
			return retval;
		}
		
		if (op instanceof TopOperator)
		{
			long retval = ((TopOperator)op).getRemaining();
			long retval2 = card(op.children().get(0));
			
			if (retval2 < retval)
			{
				cCache.put(op,  retval2);
				return retval2;
			}
			else
			{
				cCache.put(op,  retval);
				return retval;
			}
		}
		
		if (op instanceof UnionOperator)
		{
			long retval = 0;
			for (Operator o : op.children())
			{
				retval += card(o);
			}
			
			cCache.put(op,  retval);
			return retval;
		}
		
		if (op instanceof YearOperator)
		{
			long retval = card(op.children().get(0));
			cCache.put(op,  retval);
			return retval;
		}
		
		if (op instanceof TableScanOperator)
		{
			HashSet<HashMap<Filter, Filter>> hshm = ((TableScanOperator)op).getHSHM();
			if (hshm != null)
			{
				if (op.children().size() == 0)
				{
					long retval = (long)(meta.getTableCard(((TableScanOperator)op).getSchema(), ((TableScanOperator)op).getTable()) * meta.likelihood(hshm, root) * (1.0 / ((TableScanOperator)op).getNumNodes()));
					cCache.put(op,  retval);
					return retval;
				}
				else
				{
					Operator origOp = op;
					if (op.children().get(0) instanceof SortOperator)
					{
						op = op.children().get(0);
					}
					
					if (op.children().get(0) instanceof UnionOperator)
					{
						double sum = 0;
						for (Operator x : op.children().get(0).children())
						{
							double l = meta.likelihood(((IndexOperator)x).getFilter(), root);
							for (Filter f : ((IndexOperator)x).getSecondary())
							{
								l *= meta.likelihood(f, root);
							}
							
							sum += l;
						}
						
						if (sum > 1)
						{
							sum = 1;
						}
						
						op = origOp;
						long retval = (long)(meta.likelihood(hshm, root) * sum * (1.0 / ((TableScanOperator)op).getNumNodes()) * meta.getTableCard(((TableScanOperator)op).getSchema(), ((TableScanOperator)op).getTable()));
						cCache.put(op,  retval);
						return retval;
					}
					else
					{
						double z = 1;
						for (Operator x : op.children().get(0).children())
						{
							double sum = 0;
							for (Operator y : x.children())
							{
								double l = meta.likelihood(((IndexOperator)y).getFilter(), root);
								for (Filter f : ((IndexOperator)y).getSecondary())
								{
									l *= meta.likelihood(f, root);
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
						long retval = (long)(meta.likelihood(hshm, root) * z * (1.0 / ((TableScanOperator)op).getNumNodes()) * meta.getTableCard(((TableScanOperator)op).getSchema(), ((TableScanOperator)op).getTable()));
						cCache.put(op,  retval);
						return retval;
					}
				}
			}
			
			if (op.children().size() == 0)
			{
				long retval = (long)((1.0 / ((TableScanOperator)op).getNumNodes()) * meta.getTableCard(((TableScanOperator)op).getSchema(), ((TableScanOperator)op).getTable()));
				cCache.put(op,  retval);
				return retval;
			}
			else
			{
				Operator origOp = op;
				if (op.children().get(0) instanceof SortOperator)
				{
					op = op.children().get(0);
				}
				
				if (op.children().get(0) instanceof UnionOperator)
				{
					double sum = 0;
					for (Operator x : op.children().get(0).children())
					{
						double l = meta.likelihood(((IndexOperator)x).getFilter(), root);
						for (Filter f : ((IndexOperator)x).getSecondary())
						{
							l *= meta.likelihood(f, root);
						}
						
						sum += l;
					}
					
					if (sum > 1)
					{
						sum = 1;
					}
					
					op = origOp;
					long retval = (long)(sum * (1.0 / ((TableScanOperator)op).getNumNodes()) * meta.getTableCard(((TableScanOperator)op).getSchema(), ((TableScanOperator)op).getTable()));
					cCache.put(op,  retval);
					return retval;
				}
				else
				{
					double z = 1;
					for (Operator x : op.children().get(0).children())
					{
						double sum = 0;
						for (Operator y : x.children())
						{
							double l = meta.likelihood(((IndexOperator)y).getFilter(), root);
							for (Filter f : ((IndexOperator)y).getSecondary())
							{
								l *= meta.likelihood(f, root);
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
					long retval = (long)(z * (1.0 / ((TableScanOperator)op).getNumNodes()) * meta.getTableCard(((TableScanOperator)op).getSchema(), ((TableScanOperator)op).getTable()));
					cCache.put(op,  retval);
					return retval;
				}
			}
		}
		
		System.out.println("Unknown operator in card() in Phase5: " + op.getClass());
		System.exit(1);
		return 0;
	}
}