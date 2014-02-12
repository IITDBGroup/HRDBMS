package com.exascale.optimizer.testing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.ArrayList;
import java.util.concurrent.ThreadLocalRandom;

import com.exascale.optimizer.testing.MetaData.PartitionMetaData;

public final class Phase3 
{
	protected RootOperator root;
	protected MetaData meta;
	protected static final int MAX_INCOMING_CONNECTIONS = 100;
	protected static final int MAX_CARD_BEFORE_HASH = 500000;
	protected static final int MIN_CARD_BEFORE_HASH = 250000;
	protected HashSet<Integer> usedNodes = new HashSet<Integer>();
	protected static int colSuffix = 0;
	
	public Phase3(RootOperator root)
	{
		this.root = root;
		meta = root.getMeta();
	}
	
	public void optimize()
	{
		ArrayList<NetworkReceiveOperator> receives = getReceives(root);
		for (NetworkReceiveOperator receive : receives)
		{
			makeHierarchical(receive);
		}
		pushUpReceives();
		collapseDuplicates(root);
		assignNodes(root, -1);
		cleanupStrandedTables(root);
		assignNodes(root, -1);
		removeLocalSendReceive(root);
		clearOpParents(root);
		cleanupOrderedFilters(root);
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
	
	private void makeHierarchical(NetworkReceiveOperator receive)
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
			
			ArrayList<Operator> sends = (ArrayList<Operator>)receive.children().clone();
			for (Operator send : sends)
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
				catch(Exception e)
				{
					e.printStackTrace();
					System.exit(1);
				}
				sends.remove(0);
				i++;
				
				if (i == numPerMiddle)
				{
					int node = Math.abs(ThreadLocalRandom.current().nextInt()) % meta.getNumNodes();
					while (usedNodes.contains(node))
					{
						node = Math.abs(ThreadLocalRandom.current().nextInt()) % meta.getNumNodes();
					}
					NetworkSendOperator newSend = new NetworkSendOperator(node, meta);
					try
					{
						newSend.add(newReceive);
						receive.add(newSend);
					}
					catch(Exception e)
					{
						e.printStackTrace();
						System.exit(1);
					}
					newReceive = new NetworkReceiveOperator(meta);
					i = 0;
				}
			}
			makeHierarchical(receive);
		}
	}
	
	private void pushUpReceives()
	{
		ArrayList<NetworkReceiveOperator> receives = getReceives(root);
		HashSet<NetworkReceiveOperator> completed = new HashSet<NetworkReceiveOperator>();
		boolean workToDo = true;
		while (workToDo)
		{
			workToDo = false;
			for (NetworkReceiveOperator receive : receives)
			{
				if (!treeContains(root, receive))
				{
					continue;
				}
				if (completed.contains(receive))
				{
					continue;
				}
				while (true)
				{
					assignNodes(root, -1);
					workToDo = true;
					completed.add(receive);
					Operator op = receive.parent();
					if (op instanceof SelectOperator || op instanceof YearOperator || op instanceof SubstringOperator || op instanceof ProjectOperator || op instanceof ExtendOperator || op instanceof RenameOperator || op instanceof ReorderOperator || op instanceof CaseOperator)
					{
						pushAcross(receive);
					}
					else if (op instanceof SortOperator)
					{
						if (!handleSort(receive))
						{
							break;
						}
					}
					else if (op instanceof MultiOperator)
					{
						if (!handleMulti(receive))
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
					}
					else if (op instanceof HashJoinOperator)
					{
						if (!handleHash(receive))
						{
							break;
						}
					}
					else if (op instanceof NestedLoopJoinOperator)
					{
						if (!handleNested(receive))
						{
							break;
						}
					}
					else if (op instanceof SemiJoinOperator)
					{
						if (!handleSemi(receive))
						{
							break;
						}
					}
					else if (op instanceof AntiJoinOperator)
					{
						if (!handleAnti(receive))
						{
							break;
						}
					}
					else if (op instanceof TopOperator)
					{
						if (!handleTop(receive))
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
			
			receives = getReceives(root);
		}
	}
	
	private void pushAcross(NetworkReceiveOperator receive)
	{
		Operator parent = receive.parent();
		Operator grandParent = parent.parent();
		ArrayList<Operator> children = (ArrayList<Operator>)receive.children();
		HashMap<Operator, Operator> send2Child = new HashMap<Operator, Operator>();
		HashMap<Operator, CNFFilter> send2CNF = new HashMap<Operator, CNFFilter>();
		for (Operator child : children)
		{
			send2Child.put(child, ((ArrayList<Operator>)child.children()).get(0));
			if (child.children().get(0) instanceof TableScanOperator)
			{
				CNFFilter cnf = ((TableScanOperator)child.children().get(0)).getCNFForParent(child);
				if (cnf != null)
				{
					send2CNF.put(child, cnf);
				}
			}
			child.removeChild(((ArrayList<Operator>)child.children()).get(0));
		}
		parent.removeChild(receive);
		grandParent.removeChild(parent);
		
		try
		{
			for (Map.Entry entry : send2Child.entrySet())
			{
				Operator pClone = parent.clone();
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
		catch(Exception e)
		{
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	private void pushAcross2(NetworkReceiveOperator receive)
	{
		Operator parent = receive.parent();
		Operator grandParent = parent.parent();
		if (grandParent == null)
		{
			Thread.dumpStack();
			System.out.println("Parent = " + parent);
			System.out.println("Grandparent = " + grandParent);
			Driver.printTree(0, root);
			System.out.println("Isolated tree");
			Driver.printTree(0, parent);
			System.exit(1);
		}
		ArrayList<Operator> children = (ArrayList<Operator>)receive.children();
		HashMap<Operator, Operator> send2Child = new HashMap<Operator, Operator>();
		HashMap<Operator, CNFFilter> send2CNF = new HashMap<Operator, CNFFilter>();
		for (Operator child : children)
		{
			send2Child.put(child, ((ArrayList<Operator>)child.children()).get(0));
			if (child.children().get(0) instanceof TableScanOperator)
			{
				CNFFilter cnf = ((TableScanOperator)child.children().get(0)).getCNFForParent(child);
				if (cnf != null)
				{
					send2CNF.put(child, cnf);
				}
			}
			child.removeChild(((ArrayList<Operator>)child.children()).get(0));
		}
		parent.removeChild(receive);
		try
		{
			grandParent.removeChild(parent);
		}
		catch(Exception e)
		{
			e.printStackTrace();
			System.out.println("Parent = " + parent);
			System.out.println("Grandparent = " + grandParent);
			Driver.printTree(0, root);
			System.out.println("Isolated tree");
			Driver.printTree(0, parent);
			System.exit(1);
		}
		
		try
		{
			for (Map.Entry entry : send2Child.entrySet())
			{
				Operator pClone = cloneTree(parent);
				pClone.add((Operator)entry.getValue());
				if (send2CNF.containsKey(entry.getKey()))
				{
					((TableScanOperator)entry.getValue()).setCNFForParent(pClone, send2CNF.get(entry.getKey()));
				}
				((Operator)entry.getKey()).add(pClone);
				receive.removeChild((Operator)entry.getKey());
				receive.add((Operator)entry.getKey());
				//setNodeForTablesAndSends(pClone, getNode((Operator)entry.getValue()));
				pruneTree(pClone, getNode((Operator)entry.getValue()));
			}
			grandParent.add(receive);
		}
		catch(Exception e)
		{
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	private ArrayList<NetworkReceiveOperator> getReceives(Operator op)
	{
		ArrayList<NetworkReceiveOperator> retval = new ArrayList<NetworkReceiveOperator>();
		if (!(op instanceof NetworkReceiveOperator)  || (op instanceof NetworkHashReceiveOperator))
		{
			for (Operator child : op.children())
			{
				retval.addAll(getReceives(child));
			}
			
			return retval;
		}
		else
		{
			retval.add((NetworkReceiveOperator)op);
			for (Operator child : op.children())
			{
				retval.addAll(getReceives(child));
			}
			
			return retval;
		}
	}
	
	private boolean handleSort(NetworkReceiveOperator receive)
	{
		if (receive.children().size() == 1)
		{
			pushAcross(receive);
			return true;
		}
		SortOperator parent = (SortOperator)receive.parent();
		Operator grandParent = parent.parent();
		ArrayList<Operator> children = (ArrayList<Operator>)receive.children().clone();
		HashMap<Operator, Operator> send2Child = new HashMap<Operator, Operator>();
		HashMap<Operator, CNFFilter> send2CNF = new HashMap<Operator, CNFFilter>();
		for (Operator child : children)
		{
			send2Child.put(child, ((ArrayList<Operator>)child.children()).get(0));
			if (child.children().get(0) instanceof TableScanOperator)
			{
				CNFFilter cnf = ((TableScanOperator)child.children().get(0)).getCNFForParent(child);
				if (cnf != null)
				{
					send2CNF.put(child, cnf);
				}
			}
			child.removeChild(((ArrayList<Operator>)child.children()).get(0));
			receive.removeChild(child);
		}
		parent.removeChild(receive);
		grandParent.removeChild(parent);
		receive = new NetworkReceiveAndMergeOperator(parent.getKeys(), parent.getOrders(), meta);
		
		try
		{
			for (Map.Entry entry : send2Child.entrySet())
			{
				Operator pClone = parent.clone();
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
		catch(Exception e)
		{
			e.printStackTrace();
			System.exit(1);
		}
		
		return false;
	}
	
	private boolean handleTop(NetworkReceiveOperator receive)
	{
		if (receive.children().size() == 1)
		{
			pushAcross(receive);
			return true;
		}
		TopOperator parent = (TopOperator)receive.parent();
		ArrayList<Operator> children = (ArrayList<Operator>)receive.children().clone();
		HashMap<Operator, Operator> send2Child = new HashMap<Operator, Operator>();
		HashMap<Operator, CNFFilter> send2CNF = new HashMap<Operator, CNFFilter>();
		for (Operator child : children)
		{
			send2Child.put(child, ((ArrayList<Operator>)child.children()).get(0));
			if (child.children().get(0) instanceof TableScanOperator)
			{
				CNFFilter cnf = ((TableScanOperator)child.children().get(0)).getCNFForParent(child);
				if (cnf != null)
				{
					send2CNF.put(child, cnf);
				}
			}
			child.removeChild(((ArrayList<Operator>)child.children()).get(0));
		}
		
		for (Map.Entry entry : send2Child.entrySet())
		{
			Operator pClone = parent.clone();
			try
			{
				pClone.add((Operator)entry.getValue());
				if (send2CNF.containsKey(entry.getKey()))
				{
					((TableScanOperator)entry.getValue()).setCNFForParent(pClone, send2CNF.get(entry.getKey()));
				}
				((Operator)entry.getKey()).add(pClone);
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(1);
			}
		}
		
		return false;
	}
	
	private boolean containsOnlyNormalSend(Operator op)
	{
		if (op instanceof NetworkHashAndSendOperator || op instanceof NetworkSendMultipleOperator || op instanceof NetworkSendRROperator)
		{
			return false;
		}
		
		for (Operator o : op.children())
		{
			if (!containsOnlyNormalSend(o))
			{
				return false;
			}
		}
		
		return true;
	}
	
	private boolean handleMulti(NetworkReceiveOperator receive)
	{
		if (receive.children().size() == 1)
		{
			pushAcross(receive);
			return true;
		}
		
		MultiOperator parent = (MultiOperator)receive.parent();
		
		if (containsOnlyNormalSend(receive))
		{
			int i = 0;
			boolean doIt = true;
			HashSet<String> tables = new HashSet<String>();
			HashMap<String, ArrayList<String>> table2Cols = new HashMap<String, ArrayList<String>>();
			while (i < parent.getKeys().size())
			{
				String name = meta.getTableForCol(parent.getKeys().get(i));
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
			
			if (doIt)
			{
				for (String table : tables)
				{
					PartitionMetaData partMeta = meta.getPartMeta("TPCH", table);
					if (partMeta.nodeIsHash() && partMeta.noNodeGroupSet())
					{
						if (table2Cols.get(table).containsAll(partMeta.getNodeHash()))
						{
						}
						else
						{
							doIt = false;
							break;
						}
					}
					else if (partMeta.nodeIsHash() && partMeta.nodeGroupIsHash())
					{
						if (table2Cols.get(table).containsAll(partMeta.getNodeHash()) && table2Cols.get(table).containsAll(partMeta.getNodeGroupHash()))
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
		
		long card = card(parent);
		NetworkHashReceiveOperator nhro = null;
		if (card > MAX_CARD_BEFORE_HASH)
		{
			ArrayList<String> cols2 = new ArrayList<String>(parent.getKeys());
			int starting = getStartingNode(card / MIN_CARD_BEFORE_HASH);
			int ID = Phase4.id++;
			ArrayList<NetworkHashAndSendOperator> sends = new ArrayList<NetworkHashAndSendOperator>(receive.children().size());
			for (Operator o : (ArrayList<Operator>)receive.children().clone())
			{
				Operator temp = o.children().get(0);
				o.removeChild(temp);
				receive.removeChild(o);
				o = temp;
				CNFFilter cnf = null;
				if (o instanceof TableScanOperator)
				{
					cnf = ((TableScanOperator)o).getCNFForParent(receive);
				}
				
					NetworkHashAndSendOperator send = new NetworkHashAndSendOperator(cols2, card / MIN_CARD_BEFORE_HASH, ID, starting, meta);
					try
					{
						send.add(o);
						if (cnf != null)
						{
							((TableScanOperator)o).setCNFForParent(send, cnf);
						}
					}
					catch(Exception e)
					{
						e.printStackTrace();
						System.exit(1);
					}
					send.setNode(o.getNode());
					sends.add(send);
			}
			
			int i = 0;
			ArrayList<NetworkHashReceiveOperator> receives = new ArrayList<NetworkHashReceiveOperator>();
			while (i < card / MIN_CARD_BEFORE_HASH && i < meta.getNumNodes())
			{
				NetworkHashReceiveOperator hrec = new NetworkHashReceiveOperator(ID, meta);
				hrec.setNode(i + starting);
				for (NetworkHashAndSendOperator send : sends)
				{
					try
					{
						hrec.add(send);
					}
					catch(Exception e)
					{
						e.printStackTrace();
						System.exit(1);
					}
				}
				receives.add(hrec);
				i++;
			}
			
			Operator grandParent = parent.parent();
			grandParent.removeChild(parent);
			parent.removeChild(receive);
			for (NetworkHashReceiveOperator hrec : receives)
			{
				MultiOperator clone = parent.clone();
				clone.setNode(hrec.getNode());
				try
				{
					clone.add(hrec);
					NetworkSendOperator send = new NetworkSendOperator(hrec.getNode(), meta);
					send.add(clone);
					receive.add(send);
				}
				catch(Exception e)
				{
					e.printStackTrace();
					System.exit(1);
				}
			}

			try
			{
				grandParent.add(receive);
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(1);
			}
			return true;
		}

		ArrayList<Operator> children = (ArrayList<Operator>)receive.children();
		HashMap<Operator, Operator> send2Child = new HashMap<Operator, Operator>();
		HashMap<Operator, CNFFilter> send2CNF = new HashMap<Operator, CNFFilter>();
		for (Operator child : children)
		{
			send2Child.put(child, ((ArrayList<Operator>)child.children()).get(0));
			if (child.children().get(0) instanceof TableScanOperator)
			{
				CNFFilter cnf = ((TableScanOperator)child.children().get(0)).getCNFForParent(child);
				if (cnf != null)
				{
					send2CNF.put(child, cnf);
				}
			}
			child.removeChild(((ArrayList<Operator>)child.children()).get(0));
		}
		
		int oldSuffix = colSuffix;
		Operator orig = parent.parent();
		MultiOperator pClone;
		ArrayList<String> cols = new ArrayList<String>(parent.getPos2Col().values());
		ArrayList<String> oldCols = null;
		ArrayList<String> newCols = null;
		
		for (Map.Entry entry : send2Child.entrySet())
		{
			pClone = (MultiOperator)parent.clone();
			pClone.removeCountDistinct();
			if (pClone.getOutputCols().size() == 0)
			{
				pClone.addCount("_P" + colSuffix++);
			}
			while (pClone.hasAvg())
			{
				String avgCol = pClone.getAvgCol();
				ArrayList<String> newCols2 = new ArrayList<String>(2);
				String newCol1 = "_P" + colSuffix++;
				String newCol2 = "_P" + colSuffix++;
				newCols2.add(newCol1);
				newCols2.add(newCol2);
				HashMap<String, ArrayList<String>> old2News = new HashMap<String, ArrayList<String>>();
				old2News.put(avgCol, newCols2);
				pClone.replaceAvgWithSumAndCount(old2News);
				parent.replaceAvgWithSumAndCount(old2News);
				Operator grandParent = parent.parent();
				grandParent.removeChild(parent);
				ExtendOperator extend = new ExtendOperator("/," + old2News.get(avgCol).get(0) + "," + old2News.get(avgCol).get(1), avgCol, meta);
				try
				{
					extend.add(parent);
					grandParent.add(extend);
				}
				catch(Exception e)
				{
					e.printStackTrace();
					System.exit(1);
				}
			}
			
			colSuffix = oldSuffix;
			oldCols =  new ArrayList(pClone.getOutputCols());
			newCols = new ArrayList(pClone.getInputCols());
			HashMap<String, String> old2New2 = new HashMap<String, String>();
			int counter = 10;
			int i = 0;
			for (String col : oldCols)
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
			for (String col : oldCols)
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
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(1);
			}
		}
		
		parent.changeCountsToSums();
		parent.updateInputColumns(oldCols, newCols);
		try
		{
			Operator child = parent.children().get(0);
			parent.removeChild(child);
			parent.add(child);
			Operator grandParent = parent.parent();
			grandParent.removeChild(parent);
			grandParent.add(parent);
			while (!grandParent.equals(orig))
			{
				Operator next = grandParent.parent();
				next.removeChild(grandParent);
				if (next.equals(orig))
				{
					ReorderOperator order = new ReorderOperator(cols, meta);
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
		catch(Exception e)
		{
			e.printStackTrace();
			System.exit(1);
		}
		return false;
	}
	
	private int getStartingNode(long numNodes)
	{
		if (numNodes >= meta.getNumNodes())
		{
			return 0;
		}
		
		int range = (int)(meta.getNumNodes() - numNodes);
		return (int)(Math.random() * range);
	}
	
	private boolean handleHash(NetworkReceiveOperator receive)
	{
		if (meta.getNumNodes() == 1)
		{
			pushAcross2(receive);
			return true;
		}
		
		if (isAllAny(receive.parent().children().get(0)) && isAllAny(receive.parent().children().get(1)))
		{
			System.out.println("Both are any");
			ArrayList<TableScanOperator> left = getTableOperators(receive.parent().children().get(0));
			ArrayList<TableScanOperator> right = getTableOperators(receive.parent().children().get(1));
			int node = left.get(0).getNode();
			for (TableScanOperator table : left)
			{
				table.setNode(node);
				setNodeForSend(table, node);
			}
			for (TableScanOperator table : right)
			{
				table.setNode(node);
				setNodeForSend(table, node);
			}
			
			pushAcross2(receive);
			return true;
		}
		else if (isAllAny(receive))
		{
			HashJoinOperator parent = (HashJoinOperator)receive.parent();
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
			Operator op = parent.children().get(0);
			parent.removeChild(op);
			Operator grandParent = parent.parent();
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
						cnf = ((TableScanOperator) leftTree).getCNFForParent(((TableScanOperator) leftTree).firstParent());
					}
					receive.children().get(0).removeChild(leftTree);
				}
				else
				{
					leftTree = new UnionOperator(false, meta);
					for (Operator child : (ArrayList<Operator>)receive.children().clone())
					{
						receive.removeChild(child);
						leftTree.add(child);
					}
				}
				makeLocal(op, parent, leftTree, onRight, grandParent, cnf);
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(1);
			}
			return false;
		}
		else
		{
			HashJoinOperator parent = (HashJoinOperator)receive.parent();
			ArrayList<String> lefts = parent.getLefts();
			ArrayList<String> rights = parent.getRights();
			int i = 0;
			HashSet<String> tables = new HashSet<String>();
			while (i < lefts.size())
			{
				String name = meta.getTableForCol(lefts.get(i));
				if (name == null)
				{
					return false;
				}
				tables.add(name);
				name = meta.getTableForCol(rights.get(i));
				if (name == null)
				{
					return false;
				}
				tables.add(name);
				i++;
			}
			
			if (tables.size() != 2 )
			{
				return false;
			}
			
			ArrayList<PartitionMetaData> pmetas = new ArrayList<PartitionMetaData>(tables.size());
			for (String table : tables)
			{
				pmetas.add(meta.getPartMeta("TPCH", table));
			}
			
			if (pmetas.get(0).noNodeGroupSet() && pmetas.get(1).noNodeGroupSet())
			{
				//System.out.println("Hash: no node group");
				if (pmetas.get(0).getNodeHash() != null && lefts.containsAll(pmetas.get(0).getNodeHash()) && pmetas.get(1).getNodeHash() != null && rights.containsAll(pmetas.get(1).getNodeHash()))
				{
					//System.out.println("Hash: hash equal");
					pushAcross2(receive);
					return true;
				}
				else if (pmetas.get(0).getNodeHash() != null && rights.containsAll(pmetas.get(0).getNodeHash()) && pmetas.get(1).getNodeHash() != null && lefts.containsAll(pmetas.get(1).getNodeHash()))
				{
					//System.out.println("Hash: hash equal");
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
				//System.out.println("Hash: is hash group");
				if (pmetas.get(0).getNodeHash() != null && lefts.containsAll(pmetas.get(0).getNodeHash()) && pmetas.get(1).getNodeHash() != null && rights.containsAll(pmetas.get(1).getNodeHash()) && pmetas.get(0).getNodeGroupHash() != null & lefts.containsAll(pmetas.get(0).getNodeGroupHash()) && pmetas.get(1).getNodeGroupHash() != null && rights.containsAll(pmetas.get(1).getNodeGroupHash()))
				{
					//System.out.println("nodegroup and node hash equal");
					pushAcross2(receive);
					return true;
				}
				else if (pmetas.get(0).getNodeHash() != null && rights.containsAll(pmetas.get(0).getNodeHash()) && pmetas.get(1).getNodeHash() != null && lefts.containsAll(pmetas.get(1).getNodeHash()) && pmetas.get(0).getNodeGroupHash() != null & rights.containsAll(pmetas.get(0).getNodeGroupHash()) && pmetas.get(1).getNodeGroupHash() != null && lefts.containsAll(pmetas.get(1).getNodeGroupHash()))
				{
					//System.out.println("nodegroup and node hash equal");
					pushAcross2(receive);
					return true;
				}
				else
				{
					return false;
				}
			}
		}
	}
	
	private boolean handleProduct(NetworkReceiveOperator receive)
	{
		if (meta.getNumNodes() == 1)
		{
			pushAcross2(receive);
			return true;
		}
		
		if (isAllAny(receive.parent().children().get(0)) && isAllAny(receive.parent().children().get(1)))
		{
			ArrayList<TableScanOperator> left = getTableOperators(receive.parent().children().get(0));
			ArrayList<TableScanOperator> right = getTableOperators(receive.parent().children().get(1));
			int node = left.get(0).getNode();
			for (TableScanOperator table : left)
			{
				table.setNode(node);
				setNodeForSend(table, node);
			}
			for (TableScanOperator table : right)
			{
				table.setNode(node);
				setNodeForSend(table, node);
			}
			
			pushAcross2(receive);
			return true;
		}
		else if (isAllAny(receive))
		{
			ProductOperator parent = (ProductOperator)receive.parent();
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
			Operator op = parent.children().get(0);
			parent.removeChild(op);
			Operator grandParent = parent.parent();
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
						cnf = ((TableScanOperator) leftTree).getCNFForParent(((TableScanOperator) leftTree).firstParent());
					}
					receive.children().get(0).removeChild(leftTree);
				}
				else
				{
					leftTree = new UnionOperator(false, meta);
					for (Operator child : (ArrayList<Operator>)receive.children().clone())
					{
						receive.removeChild(child);
						leftTree.add(child);
					}
				}
				makeLocal(op, parent, leftTree, onRight, grandParent, cnf);
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(1);
			}
			return false;
		}
		else
		{
			return false;
		}
	}
	
	private boolean handleNested(NetworkReceiveOperator receive)
	{
		if (meta.getNumNodes() == 1)
		{
			pushAcross2(receive);
			return true;
		}
		
		if (isAllAny(receive.parent().children().get(0)) && isAllAny(receive.parent().children().get(1)))
		{
			ArrayList<TableScanOperator> left = getTableOperators(receive.parent().children().get(0));
			ArrayList<TableScanOperator> right = getTableOperators(receive.parent().children().get(1));
			int node = left.get(0).getNode();
			for (TableScanOperator table : left)
			{
				table.setNode(node);
				setNodeForSend(table, node);
			}
			for (TableScanOperator table : right)
			{
				table.setNode(node);
				setNodeForSend(table, node);
			}
			
			pushAcross2(receive);
			return true;
		}
		else if (isAllAny(receive))
		{
			NestedLoopJoinOperator parent = (NestedLoopJoinOperator)receive.parent();
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
			Operator op = parent.children().get(0);
			parent.removeChild(op);
			Operator grandParent = parent.parent();
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
						cnf = ((TableScanOperator) leftTree).getCNFForParent(((TableScanOperator) leftTree).firstParent());
					}
					receive.children().get(0).removeChild(leftTree);
				}
				else
				{
					leftTree = new UnionOperator(false, meta);
					for (Operator child : (ArrayList<Operator>)receive.children().clone())
					{
						receive.removeChild(child);
						leftTree.add(child);
					}
				}
				makeLocal(op, parent, leftTree, onRight, grandParent, cnf);
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(1);
			}
			return false;
		}
		else 
		{
			NestedLoopJoinOperator parent = (NestedLoopJoinOperator)receive.parent();
			if (!parent.usesHash())
			{
				return false;
			}
			
			ArrayList<String> lefts = parent.getJoinForChild(parent.children().get(0));
			ArrayList<String> rights = parent.getJoinForChild(parent.children().get(1));
			int i = 0;
			HashSet<String> tables = new HashSet<String>();
			while (i < lefts.size())
			{
				tables.add(meta.getTableForCol(lefts.get(i)));
				tables.add(meta.getTableForCol(rights.get(i)));
				i++;
			}
			
			if (tables.size() != 2)
			{
				return false;
			}
			
			ArrayList<PartitionMetaData> pmetas = new ArrayList<PartitionMetaData>(tables.size());
			for (String table : tables)
			{
				pmetas.add(meta.getPartMeta("TPCH", table));
			}
			
			if (pmetas.get(0).noNodeGroupSet() && pmetas.get(1).noNodeGroupSet())
			{
				if (pmetas.get(0).getNodeHash() != null && lefts.containsAll(pmetas.get(0).getNodeHash()) && pmetas.get(1).getNodeHash() != null && rights.containsAll(pmetas.get(1).getNodeHash()))
				{
					pushAcross2(receive);
					return true;
				}
				else if (pmetas.get(0).getNodeHash() != null && rights.containsAll(pmetas.get(0).getNodeHash()) && pmetas.get(1).getNodeHash() != null && lefts.containsAll(pmetas.get(1).getNodeHash()))
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
				if (pmetas.get(0).getNodeHash() != null && lefts.containsAll(pmetas.get(0).getNodeHash()) && pmetas.get(1).getNodeHash() != null && rights.containsAll(pmetas.get(1).getNodeHash()) && pmetas.get(0).getNodeGroupHash() != null & lefts.containsAll(pmetas.get(0).getNodeGroupHash()) && pmetas.get(1).getNodeGroupHash() != null && rights.containsAll(pmetas.get(1).getNodeGroupHash()))
				{
					pushAcross2(receive);
					return true;
				}
				else if (pmetas.get(0).getNodeHash() != null && rights.containsAll(pmetas.get(0).getNodeHash()) && pmetas.get(1).getNodeHash() != null && lefts.containsAll(pmetas.get(1).getNodeHash()) && pmetas.get(0).getNodeGroupHash() != null & rights.containsAll(pmetas.get(0).getNodeGroupHash()) && pmetas.get(1).getNodeGroupHash() != null && lefts.containsAll(pmetas.get(1).getNodeGroupHash()))
				{
					pushAcross2(receive);
					return true;
				}
				else
				{
					return false;
				}
			}
		}
	}
	
	private boolean handleSemi(NetworkReceiveOperator receive)
	{
		if (meta.getNumNodes() == 1)
		{
			pushAcross2(receive);
			return true;
		}
		
		if (isAllAny(receive.parent().children().get(0)) && isAllAny(receive.parent().children().get(1)))
		{
			ArrayList<TableScanOperator> left = getTableOperators(receive.parent().children().get(0));
			ArrayList<TableScanOperator> right = getTableOperators(receive.parent().children().get(1));
			int node = left.get(0).getNode();
			for (TableScanOperator table : left)
			{
				table.setNode(node);
				setNodeForSend(table, node);
			}
			for (TableScanOperator table : right)
			{
				table.setNode(node);
				setNodeForSend(table, node);
			}
			
			pushAcross2(receive);
			return true;
		}
		else if (isAllAny(receive))
		{
			SemiJoinOperator parent = (SemiJoinOperator)receive.parent();
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
			Operator op = parent.children().get(0);
			parent.removeChild(op);
			Operator grandParent = parent.parent();
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
						cnf = ((TableScanOperator) leftTree).getCNFForParent(((TableScanOperator) leftTree).firstParent());
					}
					receive.children().get(0).removeChild(leftTree);
				}
				else
				{
					leftTree = new UnionOperator(false, meta);
					for (Operator child : (ArrayList<Operator>)receive.children().clone())
					{
						receive.removeChild(child);
						leftTree.add(child);
					}
				}
				makeLocal(op, parent, leftTree, onRight, grandParent, cnf);
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(1);
			}
			return false;
		}
		else
		{
			SemiJoinOperator parent = (SemiJoinOperator)receive.parent();
			ArrayList<String> lefts = parent.getLefts();
			ArrayList<String> rights = parent.getRights();
			int i = 0;
			HashSet<String> tables = new HashSet<String>();
			while (i < lefts.size())
			{
				tables.add(meta.getTableForCol(lefts.get(i)));
				tables.add(meta.getTableForCol(rights.get(i)));
				i++;
			}
			
			if (tables.size() != 2)
			{
				return false;
			}
			
			ArrayList<PartitionMetaData> pmetas = new ArrayList<PartitionMetaData>(tables.size());
			for (String table : tables)
			{
				pmetas.add(meta.getPartMeta("TPCH", table));
			}
			
			if (pmetas.get(0).noNodeGroupSet() && pmetas.get(1).noNodeGroupSet())
			{
				if (pmetas.get(0).getNodeHash() != null && lefts.containsAll(pmetas.get(0).getNodeHash()) && pmetas.get(1).getNodeHash() != null && rights.containsAll(pmetas.get(1).getNodeHash()))
				{
					pushAcross2(receive);
					return true;
				}
				else if (pmetas.get(0).getNodeHash() != null && rights.containsAll(pmetas.get(0).getNodeHash()) && pmetas.get(1).getNodeHash() != null && lefts.containsAll(pmetas.get(1).getNodeHash()))
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
				if (pmetas.get(0).getNodeHash() != null && lefts.containsAll(pmetas.get(0).getNodeHash()) && pmetas.get(1).getNodeHash() != null && rights.containsAll(pmetas.get(1).getNodeHash()) && pmetas.get(0).getNodeGroupHash() != null & lefts.containsAll(pmetas.get(0).getNodeGroupHash()) && pmetas.get(1).getNodeGroupHash() != null && rights.containsAll(pmetas.get(1).getNodeGroupHash()))
				{
					pushAcross2(receive);
					return true;
				}
				else if (pmetas.get(0).getNodeHash() != null && rights.containsAll(pmetas.get(0).getNodeHash()) && pmetas.get(1).getNodeHash() != null && lefts.containsAll(pmetas.get(1).getNodeHash()) && pmetas.get(0).getNodeGroupHash() != null & rights.containsAll(pmetas.get(0).getNodeGroupHash()) && pmetas.get(1).getNodeGroupHash() != null && lefts.containsAll(pmetas.get(1).getNodeGroupHash()))
				{
					pushAcross2(receive);
					return true;
				}
				else
				{
					return false;
				}
			}
		}
	}
	
	private boolean handleAnti(NetworkReceiveOperator receive)
	{
		if (meta.getNumNodes() == 1)
		{
			pushAcross2(receive);
			return true;
		}
		
		if (isAllAny(receive.parent().children().get(0)) && isAllAny(receive.parent().children().get(1)))
		{
			ArrayList<TableScanOperator> left = getTableOperators(receive.parent().children().get(0));
			ArrayList<TableScanOperator> right = getTableOperators(receive.parent().children().get(1));
			int node = left.get(0).getNode();
			for (TableScanOperator table : left)
			{
				table.setNode(node);
				setNodeForSend(table, node);
			}
			for (TableScanOperator table : right)
			{
				table.setNode(node);
				setNodeForSend(table, node);
			}
			
			pushAcross2(receive);
			return true;
		}
		else if (isAllAny(receive))
		{
			AntiJoinOperator parent = (AntiJoinOperator)receive.parent();
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
			Operator op = parent.children().get(0);
			parent.removeChild(op);
			Operator grandParent = parent.parent();
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
						cnf = ((TableScanOperator) leftTree).getCNFForParent(((TableScanOperator) leftTree).firstParent());
					}
					receive.children().get(0).removeChild(leftTree);
				}
				else
				{
					leftTree = new UnionOperator(false, meta);
					for (Operator child : (ArrayList<Operator>)receive.children().clone())
					{
						receive.removeChild(child);
						leftTree.add(child);
					}
				}
				makeLocal(op, parent, leftTree, onRight, grandParent, cnf);
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(1);
			}
			return false;
		}
		else
		{
			AntiJoinOperator parent = (AntiJoinOperator)receive.parent();
			ArrayList<String> lefts = parent.getLefts();
			ArrayList<String> rights = parent.getRights();
			int i = 0;
			HashSet<String> tables = new HashSet<String>();
			while (i < lefts.size())
			{
				tables.add(meta.getTableForCol(lefts.get(i)));
				tables.add(meta.getTableForCol(rights.get(i)));
				i++;
			}
			
			if (tables.size() != 2)
			{
				return false;
			}
			
			ArrayList<PartitionMetaData> pmetas = new ArrayList<PartitionMetaData>(tables.size());
			for (String table : tables)
			{
				pmetas.add(meta.getPartMeta("TPCH", table));
			}
			
			if (pmetas.get(0).noNodeGroupSet() && pmetas.get(1).noNodeGroupSet())
			{
				if (pmetas.get(0).getNodeHash() != null && lefts.containsAll(pmetas.get(0).getNodeHash()) && pmetas.get(1).getNodeHash() != null && rights.containsAll(pmetas.get(1).getNodeHash()))
				{
					pushAcross2(receive);
					return true;
				}
				else if (pmetas.get(0).getNodeHash() != null && rights.containsAll(pmetas.get(0).getNodeHash()) && pmetas.get(1).getNodeHash() != null && lefts.containsAll(pmetas.get(1).getNodeHash()))
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
				if (pmetas.get(0).getNodeHash() != null && lefts.containsAll(pmetas.get(0).getNodeHash()) && pmetas.get(1).getNodeHash() != null && rights.containsAll(pmetas.get(1).getNodeHash()) && pmetas.get(0).getNodeGroupHash() != null & lefts.containsAll(pmetas.get(0).getNodeGroupHash()) && pmetas.get(1).getNodeGroupHash() != null && rights.containsAll(pmetas.get(1).getNodeGroupHash()))
				{
					pushAcross2(receive);
					return true;
				}
				else if (pmetas.get(0).getNodeHash() != null && rights.containsAll(pmetas.get(0).getNodeHash()) && pmetas.get(1).getNodeHash() != null && lefts.containsAll(pmetas.get(1).getNodeHash()) && pmetas.get(0).getNodeGroupHash() != null & rights.containsAll(pmetas.get(0).getNodeGroupHash()) && pmetas.get(1).getNodeGroupHash() != null && lefts.containsAll(pmetas.get(1).getNodeGroupHash()))
				{
					pushAcross2(receive);
					return true;
				}
				else
				{
					return false;
				}
			}
		}
	}
	
	private void collapseDuplicates(Operator op)
	{
		if (op instanceof NetworkReceiveOperator)
		{
			for (Operator send : op.children())
			{
				int node = send.getNode();
				if (send.children().get(0) instanceof NetworkReceiveOperator)
				{
					boolean doIt = true;
					for (Operator send2 : send.children().get(0).children())
					{
						int node2 = send2.getNode();
						if (node2 != node)
						{
							doIt = false;
							break;
						}
					}
					if (doIt)
					{
						Operator parent = op.parent();
						parent.removeChild(op);
						Operator receive = send.children().get(0);
						send.removeChild(receive);
					
						//attach other children of top receive
						for (Operator o : (ArrayList<Operator>)op.children().clone())
						{
							if (!o.equals(send))
							{
								op.removeChild(o);
								try
								{
									receive.add(o);
								}
								catch(Exception e)
								{
									e.printStackTrace();
									System.exit(1);
								}
							}
						}
						
						try
						{
							parent.add(receive);
						}
						catch(Exception e)
						{
							e.printStackTrace();
							System.exit(1);
						}
						
						//for (Operator o : (ArrayList<Operator>)parent.children().clone())
						//{
						//	collapseDuplicates(o);
						//}
						collapseDuplicates(receive);
						break;
					}
					else
					{
						for (Operator o : send.children())
						{
							collapseDuplicates(o);
						}
					}
				}
				else
				{
					
					for (Operator o : send.children())
					{
						collapseDuplicates(o);
					}
				}
			}
		}
		else
		{
			try
			{
				for (Operator o : (ArrayList<Operator>)op.children().clone())
				{
					collapseDuplicates(o);
				}
			}
			catch(Exception e)
			{
				e.printStackTrace();
				Driver.printTree(0, root);
				System.out.println("Op is " + op);
				System.out.println("Op.children is " + op.children());
				System.exit(1);
			}
		}
	}
	
	private void assignNodes(Operator op, int node)
	{
		if (op instanceof NetworkSendOperator)
		{
			node = ((NetworkSendOperator) op).getNode();
		}
		
		/*
		if (op instanceof TableScanOperator)
		{
			if (node != op.getNode())
			{
				System.out.println("Nodes do not match!");
				Driver.printTree(0, root);
				System.exit(1);
			}
			
			return;
		}
		*/
		
		op.setNode(node);
		for (Operator o : op.children())
		{
			if (o == null)
			{
				System.out.println("Null child in assignNodes()");
				System.out.println("Parent is " + op);
				System.out.println("Children are " + op.children());
				System.exit(1);
			}
			assignNodes(o, node);
		}
	}
	
	private void removeLocalSendReceive(Operator op)
	{
		if (op instanceof NetworkReceiveOperator)
		{
			if (op.children().size() == 1)
			{
				Operator send = op.children().get(0);
				if (send.getNode() == op.getNode())
				{
					Operator parent = op.parent();
					parent.removeChild(op);
					Operator child = send.children().get(0);
					send.removeChild(child);
					try
					{
						parent.add(child);
					}
					catch(Exception e)
					{
						e.printStackTrace();
						System.exit(1);
					}
					removeLocalSendReceive(child);
				}
			}
			else
			{
				for (Operator send : op.children())
				{
					if (send.getNode() != op.getNode())
					{
						for (Operator s : op.children())
						{
							removeLocalSendReceive(s);
						}
						
						return;
					}
				}
				
				Operator parent = op.parent();
				parent.removeChild(op);
				Operator union = new UnionOperator(false, meta);
				try
				{
					for (Operator send : op.children())
					{
						Operator child = send.children().get(0);
						send.removeChild(child);
						union.add(child);
					}
					parent.add(union);
				}
				catch(Exception e)
				{
					e.printStackTrace();
					System.exit(1);
				}
				removeLocalSendReceive(union);
			}
		}
		else
		{
			for (Operator o : (ArrayList<Operator>)op.children().clone())
			{
				removeLocalSendReceive(o);
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
				if (o instanceof TableScanOperator)
				{
					CNFFilter cnf = ((TableScanOperator) o).getCNFForParent(op);
					if (cnf != null)
					{
						((TableScanOperator)child).setCNFForParent(clone, cnf);
					}
				}
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(1);
			}
		}
		
		return clone;
	}
	
	private boolean isAllAny(Operator op)
	{
		if (op instanceof TableScanOperator)
		{
			return ((TableScanOperator)op).anyNode();
		}
		else
		{
			for (Operator o : op.children())
			{
				if (!isAllAny(o))
				{
					return false;
				}
			}
			
			return true;
		}
	}
	
	private ArrayList<TableScanOperator> getTableOperators(Operator op)
	{
		if (op instanceof TableScanOperator)
		{
			ArrayList<TableScanOperator> retval = new ArrayList<TableScanOperator>(1);
			retval.add((TableScanOperator)op);
			return retval;
		}
		
		ArrayList<TableScanOperator> retval = new ArrayList<TableScanOperator>();
		for (Operator o : op.children())
		{
			retval.addAll(getTableOperators(o));
		}
		
		return retval;
	}
	
	private void setNodeForSend(TableScanOperator table, int node)
	{
		Operator op = table.firstParent();
		while (!(op instanceof NetworkSendOperator))
		{
			op = op.parent();
		}

		op.setNode(node);
	}
	
	private void setNodeForTablesAndSends(Operator op, int node)
	{
		if (op instanceof TableScanOperator)
		{
			op.setNode(node);
			setNodeForSend((TableScanOperator)op, node);
		}
		else
		{
			for (Operator o : op.children())
			{
				setNodeForTablesAndSends(o, node);
			}
		}
	}
	
	private void makeLocal(Operator tree1, Operator local, Operator tree2, boolean onRight, Operator grandParent, CNFFilter cnf)
	{
		//Driver.printTree(0,  root);
		//System.out.println("makeLocal()");
		boolean doAdd = false;
		ArrayList<Operator> inserts = new ArrayList<Operator>();
		ArrayList<Operator> finals = new ArrayList<Operator>();
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
		
		for (Operator op : finals)
		{
			Operator parent = null;
			if (op instanceof TableScanOperator)
			{
				parent = ((TableScanOperator) op).firstParent();
			}
			else
			{
				parent = op.parent();
			}
			
			if (parent != null)
			{
				parent.removeChild(op);
			}
			Operator local2 = local.clone();
			try
			{
				if (onRight)
				{
					local2.add(op);
					Operator cloneTree = cloneTree(tree2);
					local2.add(cloneTree);
					if (cnf != null)
					{
						((TableScanOperator)cloneTree).setCNFForParent(local2, cnf);
					}
					if (parent != null)
					{
						parent.add(local2);
						//setNodeForTablesAndSends(local2, parent.getNode());
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
						//setNodeForTablesAndSends(local2.children().get(0), getNode(local2.children().get(1)));
						grandParent.add(local2);
					}
				}
				else
				{
					Operator cloneTree = cloneTree(tree2);
					local2.add(cloneTree);
					local2.add(op);
					if (cnf != null)
					{
						((TableScanOperator)cloneTree).setCNFForParent(local2, cnf);
					}
					if (parent != null)
					{
						parent.add(local2);
						//setNodeForTablesAndSends(local2, parent.getNode());
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
						//setNodeForTablesAndSends(local2.children().get(1), getNode(local2.children().get(0)));
						grandParent.add(local2);
					}
				}
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(1);
			}
		}
		
		if (doAdd)
		{
			try
			{
				grandParent.add(tree1);
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(1);
			}
		}
		//Driver.printTree(0,  root);
	}
	
	private boolean treeContains(Operator root, Operator op)
	{
		if (root.equals(op))
		{
			return true;
		}
		
		for (Operator o : root.children())
		{
			if (treeContains(o, op))
			{
				return true;
			}
		}
		
		return false;
	}
	
	private int getNode(Operator op)
	{
		if (op instanceof TableScanOperator)
		{
			return op.getNode();
		}
		else
		{
			int retval = getNode(op.children().get(0));
			for (Operator o : op.children())
			{
				if (retval != getNode(o))
				{
					System.out.println("Tree with multiple nodes!");
					System.exit(1);
				}
			}
			
			return retval;
		}
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
			for (Operator o : (ArrayList<Operator>)op.children().clone())
			{
				pruneTree(o, node);
			}
		}
	}
	
	private void cleanupStrandedTables(Operator op)
	{
		if (op instanceof TableScanOperator)
		{
			if (op.getNode() == -1)
			{
				Operator parent = ((TableScanOperator)op).firstParent();
				CNFFilter cnf = ((TableScanOperator)op).getCNFForParent(parent);
				parent.removeChild(op);
				Operator send = new NetworkSendOperator(Math.abs(new Random(System.currentTimeMillis()).nextInt()) % meta.getNumNodes(), meta);
				op.setNode(send.getNode());
				Operator receive = new NetworkReceiveOperator(meta);
				try
				{
					send.add(op);
					if (cnf != null)
					{
						((TableScanOperator) op).setCNFForParent(send,  cnf);
					}
					receive.add(send);
					parent.add(receive);
				}
				catch(Exception e)
				{
					e.printStackTrace();
					System.exit(1);
				}
			}
		}
		else
		{
			for (Operator o : (ArrayList<Operator>)op.children().clone())
			{
				cleanupStrandedTables(o);
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
	
	public long card(Operator op)
	{
		if (op instanceof AntiJoinOperator)
		{
			return (long)((1 - meta.likelihood(((AntiJoinOperator)op).getHSHM(), root)) * card(op.children().get(0)));
		}
		
		if (op instanceof CaseOperator)
		{
			return card(op.children().get(0));
		}
		
		if (op instanceof ExtendOperator)
		{
			return card(op.children().get(0));
		}
		
		if (op instanceof HashJoinOperator)
		{
			long retval = (long)(card(op.children().get(0)) * card(op.children().get(1)) * meta.likelihood(((HashJoinOperator)op).getHSHM(), root));
			return retval;
		}
		
		if (op instanceof MultiOperator)
		{
			//return card(op.children().get(0));
			long groupCard = meta.getColgroupCard(((MultiOperator)op).getKeys(), root);
			if (groupCard > card(op.children().get(0)))
			{
				return card(op.children().get(0));
			}
			
			return groupCard;
		}
		
		if (op instanceof NestedLoopJoinOperator)
		{
			return (long)(card(op.children().get(0)) * card(op.children().get(1)) * meta.likelihood(((NestedLoopJoinOperator)op).getHSHM(), root));
		}
		
		if (op instanceof NetworkReceiveOperator)
		{
			long retval = 0;
			for (Operator o : op.children())
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
			return (long)(((SelectOperator)op).likelihood(root) * card(op.children().get(0)));
		}
		
		if (op instanceof SemiJoinOperator)
		{
			return (long)(meta.likelihood(((SemiJoinOperator)op).getHSHM(), root) * card(op.children().get(0)));
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
			for (Operator o : op.children())
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
			HashSet<HashMap<Filter, Filter>> hshm = ((TableScanOperator)op).getHSHM();
			if (hshm != null)
			{
				return (long)(meta.getTableCard(((TableScanOperator)op).getSchema(), ((TableScanOperator)op).getTable()) * meta.likelihood(hshm, root) * (1.0 / ((TableScanOperator)op).getNumNodes()));
			}
			
			return (long)((1.0 / ((TableScanOperator)op).getNumNodes()) * meta.getTableCard(((TableScanOperator)op).getSchema(), ((TableScanOperator)op).getTable()));
		}
		
		System.out.println("Unknown operator in card() in Phase3: " + op.getClass());
		System.exit(1);
		return 0;
	}
}