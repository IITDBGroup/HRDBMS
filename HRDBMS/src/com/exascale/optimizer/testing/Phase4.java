package com.exascale.optimizer.testing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.ArrayList;
import java.util.concurrent.ThreadLocalRandom;

import com.exascale.optimizer.testing.MetaData.PartitionMetaData;

public class Phase4 
{
	protected RootOperator root;
	protected MetaData meta;
	protected final int MAX_INCOMING_CONNECTIONS = 100;
	protected static final int MAX_LOCAL_NO_HASH_PRODUCT = 1000000;
	protected static final int MAX_LOCAL_LEFT_HASH = 1000000;
	protected static final int MIN_LOCAL_LEFT_HASH = 500000;
	protected static final int MAX_LOCAL_SORT = 1000000;
	protected static final int MAX_CARD_BEFORE_HASH = 500000;
	protected static final int MIN_CARD_BEFORE_HASH = 250000;
	protected static int id = 0;
	protected static int iNode = 0;
	protected HashSet<Integer> usedNodes = new HashSet<Integer>();
	protected HashMap<Operator, Long> cCache = new HashMap<Operator, Long>();
	
	public Phase4(RootOperator root)
	{
		this.root = root;
		meta = root.getMeta();
	}
	
	public void optimize()
	{
		if (meta.getNumNodes() > 1)
		{
			pushUpReceives();
			redistributeSorts();
			clearOpParents(root);
			cleanupOrderedFilters(root);
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
	
	private void redistributeSorts()
	{
		SortOperator sort = getLocalSort(root);
		if (sort != null && card(sort) > MAX_LOCAL_SORT)
		{
			doSortRedistribution(sort);
		}
	}
	
	private SortOperator getLocalSort(Operator op)
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
					System.err.println("Found more than 1 sort on the coord node!");
					Thread.dumpStack();
					System.exit(1);
				}
			}
		}
		
		if (op.getNode() == -1)
		{
			for (Operator o : op.children())
			{
				SortOperator s = getLocalSort(o);
				if (s != null)
				{
					if (retval == null)
					{
						retval = (SortOperator)s;
					}
					else
					{
						System.err.println("Found more than 1 sort on the coord node!");
						Thread.dumpStack();
						System.exit(1);
					}
				}
			}
		}
		
		return retval;
	}
	
	private void pushUpReceives()
	{
		HashSet<NetworkReceiveOperator> completed = new HashSet<NetworkReceiveOperator>();
		HashSet<NetworkReceiveOperator> eligible = new HashSet<NetworkReceiveOperator>();
		boolean workToDo = true;
		while (workToDo)
		{
			workToDo = false;
			HashMap<NetworkReceiveOperator, Integer> receives = getReceives(root, 0);
			for (NetworkReceiveOperator receive : order(receives))
			{
				if (!treeContains(root, receive))
				{
					continue;
				}
				if (completed.contains(receive))
				{
					continue;
				}

				Operator op = receive.parent();
				if (op instanceof SelectOperator || op instanceof YearOperator || op instanceof SubstringOperator || op instanceof ProjectOperator || op instanceof ExtendOperator || op instanceof RenameOperator || op instanceof ReorderOperator || op instanceof CaseOperator)
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
					Operator parent = op.parent();
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
					Operator parent = op.parent();
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
				else if (op instanceof NestedLoopJoinOperator)
				{
					Operator parent = op.parent();
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
					System.out.println("Attempting to push down SemiJoin");
					Operator parent = op.parent();
					if (((SemiJoinOperator)op).usesHash() && card(op.children().get(0)) + card(op.children().get(1)) <= MAX_LOCAL_LEFT_HASH && noLargeUpstreamJoins(op))
					{
						//System.out.println("SemiJoin uses partial hash, but is not pushed down because...");
						//System.out.println("Left card = " + card(op.children().get(0)));
						//System.out.println("Right card = " + card(op.children().get(1)));
						continue;
					}
					else if (((SemiJoinOperator)op).usesSort() && card(op) <= MAX_LOCAL_NO_HASH_PRODUCT && card(op.children().get(1)) <= MAX_LOCAL_SORT && noLargeUpstreamJoins(op))
					{
						//System.out.println("SemiJoin uses sort, but is not pushed down because...");
						//System.out.println("Card = " + card(op));
						continue;
					}
					else if (l * r <= MAX_LOCAL_NO_HASH_PRODUCT && noLargeUpstreamJoins(op))
					{
						//System.out.println("SemiJoin uses NL, but is not pushed down because...");
						//System.out.println("Left card = " + card(op.children().get(0)));
						//System.out.println("Right card = " + card(op.children().get(1)));
						continue;
					}
						
					System.out.println("SemiJoin is pushed down");
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
					System.out.println("Attempting to push down AntiJoin");
					Operator parent = op.parent();
					if (((AntiJoinOperator)op).usesHash() && card(op.children().get(0)) + card(op.children().get(1)) <= MAX_LOCAL_LEFT_HASH && noLargeUpstreamJoins(op))
					{
						//System.out.println("AntiJoin uses partial hash, but is not pushed down because...");
						//System.out.println("Left card = " + card(op.children().get(0)));
						//System.out.println("Right card = " + card(op.children().get(1)));
						continue;
					}
					else if (((AntiJoinOperator)op).usesSort() && card(op) <= MAX_LOCAL_NO_HASH_PRODUCT && card(op.children().get(1)) <= MAX_LOCAL_SORT && noLargeUpstreamJoins(op))
					{
						//System.out.println("AntiJoin uses sort, but is not pushed down because...");
						//System.out.println("Card = " + card(op));
						continue;
					}
					else if (l * r <= MAX_LOCAL_NO_HASH_PRODUCT && noLargeUpstreamJoins(op))
					{
						//System.out.println("AntiJoin uses NL, but is not pushed down because...");
						//System.out.println("Left card = " + card(op.children().get(0)));
						//System.out.println("Right card = " + card(op.children().get(1)));
						continue;
					}
						
					System.out.println("AntiJoin is pushed down");
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
	
	private HashMap<NetworkReceiveOperator, Integer> getReceives(Operator op, int level)
	{
		HashMap<NetworkReceiveOperator, Integer> retval = new HashMap<NetworkReceiveOperator, Integer>();
		if (!(op instanceof NetworkReceiveOperator))
		{
			if (op.getNode() == -1)
			{
				for (Operator child : op.children())
				{
					retval.putAll(getReceives(child, level+1));
				}
			}
			
			return retval;
		}
		else
		{
			if (op.getNode() == -1)
			{
				retval.put((NetworkReceiveOperator)op, level);
				for (Operator child : op.children())
				{
					retval.putAll(getReceives(child, level + 1));
				}
			}
			
			return retval;
		}
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
	
	private long concatPath(Operator op)
	{
		long retval = 0;
		long i = 0;
		int shift = 0;
		while (!(op instanceof RootOperator))
		{
			for (Operator o : op.parent().children())
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
	
	private boolean lt = true;
	
	private ArrayList<NetworkReceiveOperator> order(HashMap<NetworkReceiveOperator, Integer> receives)
	{
		ArrayList<NetworkReceiveOperator> retval = new ArrayList<NetworkReceiveOperator>(receives.size());
		while (receives.size() > 0)
		{
			NetworkReceiveOperator maxReceive = null;
			int maxLevel = Integer.MIN_VALUE;
			long minConcatPath = Long.MAX_VALUE;
			for (Map.Entry entry : receives.entrySet())
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
		cCache.clear();
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
		
		cCache.clear();
		return false;
	}
	
	private boolean handleMulti(NetworkReceiveOperator receive)
	{
		if (receive.children().size() == 1)
		{
			pushAcross(receive);
			return true;
		}
		MultiOperator parent = (MultiOperator)receive.parent();
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
			cCache.clear();
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
		
		int oldSuffix = Phase3.colSuffix;
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
				pClone.addCount("_P" + Phase3.colSuffix++);
			}
			while (pClone.hasAvg())
			{
				String avgCol = pClone.getAvgCol();
				ArrayList<String> newCols2 = new ArrayList<String>(2);
				String newCol1 = "_P" + Phase3.colSuffix++;
				String newCol2 = "_P" + Phase3.colSuffix++;
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
			
			Phase3.colSuffix = oldSuffix;
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
		cCache.clear();
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
		
		cCache.clear();
		return false;
	}
	
	public long card(Operator op)
	{
		Long r = cCache.get(op);
		if (r != null)
		{
			return r;
		}
		
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
			return retval;
		}
		
		if (op instanceof RootOperator)
		{
			long retval = card(op.children().get(0));
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
			return retval;
		}
		
		if (op instanceof SubstringOperator)
		{
			long retval = card(op.children().get(0));
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
				long retval = (long)(meta.getTableCard(((TableScanOperator)op).getSchema(), ((TableScanOperator)op).getTable()) * meta.likelihood(hshm, root) * (1.0 / ((TableScanOperator)op).getNumNodes()));
				cCache.put(op,  retval);
				return retval;
			}
			
			long retval = (long)((1.0 / ((TableScanOperator)op).getNumNodes()) * meta.getTableCard(((TableScanOperator)op).getSchema(), ((TableScanOperator)op).getTable()));
			cCache.put(op,  retval);
			return retval;
		}
		
		System.out.println("Unknown operator in card() in Phase4: " + op.getClass());
		System.exit(1);
		return 0;
	}
	
	private boolean noLargeUpstreamJoins(Operator op)
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
	
	private boolean isLocal(Operator op)
	{
		if (op instanceof NetworkSendOperator)
		{
			return false;
		}
		
		for (Operator o : op.children())
		{
			if (!isLocal(o))
			{
				return false;
			}
		}
		
		return true;
	}
	
	private boolean redistributeHash(NetworkReceiveOperator receive)
	{
		Operator parent = receive.parent();
		verify2ReceivesForHash(parent);
		Operator grandParent = parent.parent();
		long card = card(parent.children().get(0)) + card(parent.children().get(1));
		ArrayList<String> join = ((JoinOperator)parent).getJoinForChild(receive);
		parent.removeChild(receive);
		grandParent.removeChild(parent);
		ArrayList<Operator> sends = new ArrayList<Operator>(receive.children().size());
		int starting = getStartingNode(card / MIN_LOCAL_LEFT_HASH + 1);
		int ID = id++;
		Operator other = null;
		for (Operator o : parent.children())
		{
			if (o != receive)
			{
				other = o;
			}
		}
		String t = meta.getTableForCol(((JoinOperator)parent).getJoinForChild(other).get(0));
		PartitionMetaData pmeta = null;
		if (t != null)
		{
			pmeta = meta.getPartMeta("TPCH", t);
		}
		
		if (pmeta != null && isLocal(other.children().get(0).children().get(0)) && pmeta.noNodeGroupSet() && pmeta.nodeIsHash() && join.containsAll(pmeta.getNodeHash()) && pmeta.allNodes())
		{
			starting = 0;
			card = Long.MAX_VALUE;
		}
		
		CNFFilter cnf = null;
		for (Operator child : (ArrayList<Operator>)receive.children().clone())
		{
			int node = child.getNode();
			Operator grandChild = child.children().get(0);
			
			if (grandChild instanceof TableScanOperator)
			{
				cnf = ((TableScanOperator)grandChild).getCNFForParent(child);
			}
			child.removeChild(grandChild);
			t = meta.getTableForCol(join.get(0));
			pmeta = null;
			if (t != null)
			{
				pmeta = meta.getPartMeta("TPCH", t);
			}
			
			if (pmeta != null && isLocal(grandChild) && pmeta.noNodeGroupSet() && pmeta.nodeIsHash() && join.containsAll(pmeta.getNodeHash()) && pmeta.allNodes())
			{
				starting = 0;
				card = Long.MAX_VALUE;
			}
			
			NetworkHashAndSendOperator send = new NetworkHashAndSendOperator(join, card / MIN_LOCAL_LEFT_HASH + 1, ID, starting, meta);
			try
			{
				send.add(grandChild);
				if (cnf != null)
				{
					((TableScanOperator)grandChild).setCNFForParent(send, cnf);
				}
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(1);
			}
			send.setNode(node);
			sends.add(send);
		}
		
		int i = starting;
		ArrayList<Operator> receives = new ArrayList<Operator>();
		while (i < (card / MIN_LOCAL_LEFT_HASH + 1) + starting && i < meta.getNumNodes())
		{
			NetworkHashReceiveOperator receive2 = new NetworkHashReceiveOperator(ID, meta);
			receive2.setNode(i);
			receives.add(receive2);
			i++;
		}
		
		for (Operator receive2 : receives)
		{
			for (Operator send : sends)
			{
				try
				{
					receive2.add(send);
				}
				catch(Exception e)
				{
					e.printStackTrace();
					System.exit(1);
				}
			}
		}
		
		ArrayList<Operator> parents = new ArrayList<Operator>(receives.size());
		for (Operator receive2 : receives)
		{
			Operator clone = cloneTree(parent, 0);
			try
			{
				clone.add(receive2);
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(1);
			}
			clone.setNode(receive2.getNode());
			parents.add(clone);
			for (Operator o : (ArrayList<Operator>)clone.children().clone())
			{
				if (!o.equals(receive2))
				{
					clone.removeChild(o);
				}
			}
		}
		
		Operator otherChild = null;
		for (Operator child : parent.children())
		{
			if (!child.equals(receive))
			{
				otherChild = child;
			}
		}
		
		join = ((JoinOperator)parent).getJoinForChild(otherChild);
		sends = new ArrayList<Operator>(otherChild.children().size());
		ID = id++;
		for (Operator child : (ArrayList<Operator>)otherChild.children().clone())
		{
			int node = child.getNode();
			Operator grandChild = child.children().get(0);
			cnf = null;
			if (grandChild instanceof TableScanOperator)
			{
				cnf = ((TableScanOperator)grandChild).getCNFForParent(child);
			}
			child.removeChild(grandChild);
			
			NetworkHashAndSendOperator send = new NetworkHashAndSendOperator(join, card / MIN_LOCAL_LEFT_HASH + 1, ID, starting, meta);
			try
			{
				send.add(grandChild);
				if (cnf != null)
				{
					((TableScanOperator)grandChild).setCNFForParent(send, cnf);
				}
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(1);
			}
			send.setNode(node);
			sends.add(send);
		}
		
		i = starting;
		receives = new ArrayList<Operator>();
		while (i < (card / MIN_LOCAL_LEFT_HASH + 1) + starting && i < meta.getNumNodes())
		{
			NetworkHashReceiveOperator receive2 = new NetworkHashReceiveOperator(ID, meta);
			receive2.setNode(i);
			receives.add(receive2);
			i++;
		}
		
		for (Operator receive2 : receives)
		{
			for (Operator send : sends)
			{
				try
				{
					receive2.add(send);
				}
				catch(Exception e)
				{
					e.printStackTrace();
					System.exit(1);
				}
			}
		}
		
		ArrayList<NetworkSendOperator> sends2 = new ArrayList<NetworkSendOperator>(parents.size());
		for (Operator clone : parents)
		{
			for (Operator receive2 : receives)
			{
				if (clone.getNode() == receive2.getNode())
				{
					try
					{
						clone.add(receive2);
					}
					catch(Exception e)
					{
						e.printStackTrace();
						System.exit(1);
					}
				}
			}
			
			NetworkSendOperator send = new NetworkSendOperator(clone.getNode(), meta);
			sends2.add(send);
			try
			{
				send.add(clone);
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(1);
			}
		}
		
		NetworkReceiveOperator r = new NetworkReceiveOperator(meta);
		r.setNode(-1);
		for (NetworkSendOperator send : sends2)
		{
			try
			{
				r.add(send);
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(1);
			}
		}
		try
		{
			grandParent.add(r);
		}
		catch(Exception e)
		{
			e.printStackTrace();
			System.exit(1);
		}
		makeHierarchical2(r);
		makeHierarchical(r);
		cCache.clear();
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
	
	private Operator cloneTree(Operator op, int level)
	{
		Operator clone = op.clone();
		if (level == 0)
		{
			for (Operator o : op.children())
			{
				try
				{
					Operator child = cloneTree(o, level+1);
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
		}
		
		return clone;
	}
	
	private Operator fullyCloneTree(Operator op)
	{
		Operator clone = op.clone();
		
		for (Operator o : op.children())
		{
			try
			{
				Operator child = fullyCloneTree(o);
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
			cCache.clear();
		}
	}
	
	private void checkOrder(Operator op)
	{
		Operator parent = op;
		Operator left = parent.children().get(0);
		Operator right = parent.children().get(1);
		if (card(left) > card(right))
		{
			//switch
			try
			{
				parent.removeChild(right);
				parent.add(left);
				parent.removeChild(left);
				parent.add(right);
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(1);
			}
			
			Operator p = parent.parent();
			while (!(p instanceof RootOperator))
			{
				for (Operator o : (ArrayList<Operator>)p.children().clone())
				{
					p.removeChild(o);
					try
					{
						p.add(o);
					}
					catch(Exception e)
					{
						e.printStackTrace();
						System.exit(1);
					}
				}
				
				p = p.parent();
			}
			
			cCache.clear();
		}
	}
	
	private boolean redistributeProduct(NetworkReceiveOperator receive)
	{
		Operator parent = receive.parent();
		verify2ReceivesForProduct(parent);
		Operator left = parent.children().get(0);
		Operator right = parent.children().get(1);
		
		if (isAllAny(left))
		{
			Operator grandParent = parent.parent();
			parent.removeChild(left);
			grandParent.removeChild(parent);
			ArrayList<Operator> grandChildren = new ArrayList<Operator>();
			for (Operator child : (ArrayList<Operator>)right.children().clone())
			{
				Operator grandChild = child.children().get(0);
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
			
			ArrayList<NetworkSendOperator> sends2 = new ArrayList<NetworkSendOperator>(grandChildren.size());
			for (Operator grandChild : grandChildren)
			{
				Operator clone = cloneTree(parent, 0);
				clone.setNode(grandChild.getNode());
				Operator leftClone = fullyCloneTree(left.children().get(0).children().get(0));
				setNodeForTree(leftClone, grandChild.getNode());
				try
				{
					clone.add(leftClone);
				}
				catch(Exception e)
				{
					e.printStackTrace();
					System.exit(1);
				}
				
				for (Operator o : (ArrayList<Operator>)clone.children().clone())
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
						CNFFilter cnf = ((TableScanOperator)grandChild).getFirstCNF();
						if (cnf != null)
						{
							((TableScanOperator)grandChild).setCNFForParent(clone, cnf);
						}
					}
				}
				catch(Exception e)
				{
					e.printStackTrace();
					System.exit(1);
				}
				
				NetworkSendOperator send2 = new NetworkSendOperator(clone.getNode(), meta);
				try
				{
					send2.add(clone);
				}
				catch(Exception e)
				{
					e.printStackTrace();
					System.exit(1);
				}
				
				sends2.add(send2);
			}
			
			NetworkReceiveOperator r = new NetworkReceiveOperator(meta);
			r.setNode(-1);
			
			for (NetworkSendOperator send : sends2)
			{
				try
				{
					r.add(send);
				}
				catch(Exception e)
				{
					e.printStackTrace();
					System.exit(1);
				}
			}
			
			try
			{
				grandParent.add(r);
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(1);
			}
			
			makeHierarchical(r);
			cCache.clear();
			return false;
		}
		
		//send left data to nodes and perform product there
		Operator grandParent = parent.parent();
		parent.removeChild(left);
		grandParent.removeChild(parent);
		ArrayList<NetworkSendMultipleOperator> sends = new ArrayList<NetworkSendMultipleOperator>(left.children().size());
		int ID = id++;
		for (Operator child : (ArrayList<Operator>)left.children().clone())
		{
			int node = child.getNode();
			Operator grandChild = child.children().get(0);
			CNFFilter cnf = null;
			if (grandChild instanceof TableScanOperator)
			{
				cnf = ((TableScanOperator)grandChild).getCNFForParent(child);
			}
			child.removeChild(grandChild);
			NetworkSendMultipleOperator send = new NetworkSendMultipleOperator(ID, meta);
			try
			{
				send.add(grandChild);
				if (cnf != null)
				{
					((TableScanOperator)grandChild).setCNFForParent(send, cnf);
				}
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(1);
			}
			send.setNode(node);
			sends.add(send);
		}
		
		ArrayList<Operator> grandChildren = new ArrayList<Operator>();
		for (Operator child : (ArrayList<Operator>)right.children().clone())
		{
			Operator grandChild = child.children().get(0);
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
		
		ArrayList<NetworkHashReceiveOperator> receives = new ArrayList<NetworkHashReceiveOperator>(grandChildren.size());
		for (Operator grandChild : grandChildren)
		{
			Operator clone = cloneTree(parent, 0);
			clone.setNode(grandChild.getNode());
			NetworkHashReceiveOperator receive2 = new NetworkHashReceiveOperator(ID, meta);
			try
			{
				clone.add(receive2);
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(1);
			}
			
			receive2.setNode(grandChild.getNode());
			receives.add(receive2);
			for (Operator o : (ArrayList<Operator>)clone.children().clone())
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
					CNFFilter cnf = ((TableScanOperator)grandChild).getFirstCNF();
					if (cnf != null)
					{
						((TableScanOperator)grandChild).setCNFForParent(clone, cnf);
					}
				}
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(1);
			}
		}
		
		ArrayList<NetworkSendOperator> sends2 = new ArrayList<NetworkSendOperator>(receives.size());
		for (NetworkHashReceiveOperator receive2 : receives)
		{
			for (NetworkSendMultipleOperator send : sends)
			{
				try
				{
					receive2.add(send);
				}
				catch(Exception e)
				{
					e.printStackTrace();
					System.exit(1);
				}
			}
			
			NetworkSendOperator send2 = new NetworkSendOperator(receive2.getNode(), meta);
			send2.setNode(receive2.getNode());
			try
			{
				send2.add(receive2.parent());
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(1);
			}
			
			sends2.add(send2);
		}
		
		NetworkReceiveOperator r = new NetworkReceiveOperator(meta);
		r.setNode(-1);
		
		for (NetworkSendOperator send : sends2)
		{
			try
			{
				r.add(send);
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(1);
			}
		}
		
		try
		{
			grandParent.add(r);
		}
		catch(Exception e)
		{
			e.printStackTrace();
			System.exit(1);
		}
		
		for (NetworkHashReceiveOperator receive2 : receives)
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
				catch(Exception e)
				{
					e.printStackTrace();
					System.exit(1);
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
	
	private ArrayList<Operator> getGrandChildren(Operator op)
	{
		ArrayList<Operator> retval = new ArrayList<Operator>();
		for (Operator o : (ArrayList<Operator>)op.children().clone())
		{
			Operator grandChild = o.children().get(0);
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
	
	private boolean redistributeNL(NetworkReceiveOperator receive)
	{
		if (((NestedLoopJoinOperator)receive.parent()).usesHash())
		{
			return redistributeHash(receive);
		}
		
		return redistributeProduct(receive);
	}
	
	private boolean redistributeSemi(NetworkReceiveOperator receive)
	{
		if (((SemiJoinOperator)receive.parent()).usesHash())
		{
			return doHashSemi(receive);
		}
		
		return doNonHashSemi(receive);
	}
	
	private boolean redistributeAnti(NetworkReceiveOperator receive)
	{
		if (((AntiJoinOperator)receive.parent()).usesHash())
		{
			return doHashAnti(receive);
		}
		
		return doNonHashSemi(receive);
	}
	
	private boolean doHashSemi(NetworkReceiveOperator receive)
	{
		Operator parent = receive.parent();
		verify2ReceivesForHash(parent);
		Operator grandParent = parent.parent();
		long card = card(parent.children().get(0)) + card(parent.children().get(1));
		ArrayList<String> join = ((SemiJoinOperator)parent).getJoinForChild(receive);
		parent.removeChild(receive);
		grandParent.removeChild(parent);
		ArrayList<Operator> sends = new ArrayList<Operator>(receive.children().size());
		int starting = getStartingNode(card / MIN_LOCAL_LEFT_HASH + 1);
		int ID = id++;
		Operator other = null;
		for (Operator o : parent.children())
		{
			if (o != receive)
			{
				other = o;
			}
		}
		String t = meta.getTableForCol(((SemiJoinOperator)parent).getJoinForChild(other).get(0));
		PartitionMetaData pmeta = null;
		if (t != null)
		{
			pmeta = meta.getPartMeta("TPCH", t);
		}
		
		if (pmeta != null && isLocal(other.children().get(0).children().get(0)) && pmeta.noNodeGroupSet() && pmeta.nodeIsHash() && join.containsAll(pmeta.getNodeHash()) && pmeta.allNodes())
		{
			starting = 0;
			card = Long.MAX_VALUE;
		}
		
		CNFFilter cnf = null;
		for (Operator child : (ArrayList<Operator>)receive.children().clone())
		{
			int node = child.getNode();
			Operator grandChild = child.children().get(0);
			
			if (grandChild instanceof TableScanOperator)
			{
				cnf = ((TableScanOperator)grandChild).getCNFForParent(child);
			}
			child.removeChild(grandChild);
			t = meta.getTableForCol(join.get(0));
			pmeta = null;
			if (t != null)
			{
				pmeta = meta.getPartMeta("TPCH", t);
			}
			
			if (pmeta != null && isLocal(grandChild) && pmeta.noNodeGroupSet() && pmeta.nodeIsHash() && join.containsAll(pmeta.getNodeHash()) && pmeta.allNodes())
			{
				starting = 0;
				card = Long.MAX_VALUE;
			}
			
			NetworkHashAndSendOperator send = new NetworkHashAndSendOperator(join, card / MIN_LOCAL_LEFT_HASH + 1, ID, starting, meta);
			try
			{
				send.add(grandChild);
				if (cnf != null)
				{
					((TableScanOperator)grandChild).setCNFForParent(send, cnf);
				}
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(1);
			}
			send.setNode(node);
			sends.add(send);
		}
		
		int i = starting;
		ArrayList<Operator> receives = new ArrayList<Operator>();
		while (i < (card / MIN_LOCAL_LEFT_HASH + 1) + starting && i < meta.getNumNodes())
		{
			NetworkHashReceiveOperator receive2 = new NetworkHashReceiveOperator(ID, meta);
			receive2.setNode(i);
			receives.add(receive2);
			i++;
		}
		
		for (Operator receive2 : receives)
		{
			for (Operator send : sends)
			{
				try
				{
					receive2.add(send);
				}
				catch(Exception e)
				{
					e.printStackTrace();
					System.exit(1);
				}
			}
		}
		
		ArrayList<Operator> parents = new ArrayList<Operator>(receives.size());
		for (Operator receive2 : receives)
		{
			Operator clone = cloneTree(parent, 0);
			try
			{
				clone.add(receive2);
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(1);
			}
			clone.setNode(receive2.getNode());
			parents.add(clone);
			for (Operator o : (ArrayList<Operator>)clone.children().clone())
			{
				if (!o.equals(receive2))
				{
					clone.removeChild(o);
				}
			}
		}
		
		Operator otherChild = null;
		for (Operator child : parent.children())
		{
			if (!child.equals(receive))
			{
				otherChild = child;
			}
		}
		
		join = ((SemiJoinOperator)parent).getJoinForChild(otherChild);
		sends = new ArrayList<Operator>(otherChild.children().size());
		ID = id++;
		for (Operator child : (ArrayList<Operator>)otherChild.children().clone())
		{
			int node = child.getNode();
			Operator grandChild = child.children().get(0);
			cnf = null;
			if (grandChild instanceof TableScanOperator)
			{
				cnf = ((TableScanOperator)grandChild).getCNFForParent(child);
			}
			child.removeChild(grandChild);
			
			NetworkHashAndSendOperator send = new NetworkHashAndSendOperator(join, card / MIN_LOCAL_LEFT_HASH + 1, ID, starting, meta);
			try
			{
				send.add(grandChild);
				if (cnf != null)
				{
					((TableScanOperator)grandChild).setCNFForParent(send, cnf);
				}
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(1);
			}
			send.setNode(node);
			sends.add(send);
		}
		
		i = starting;
		receives = new ArrayList<Operator>();
		while (i < (card / MIN_LOCAL_LEFT_HASH + 1) + starting && i < meta.getNumNodes())
		{
			NetworkHashReceiveOperator receive2 = new NetworkHashReceiveOperator(ID, meta);
			receive2.setNode(i);
			receives.add(receive2);
			i++;
		}
		
		for (Operator receive2 : receives)
		{
			for (Operator send : sends)
			{
				try
				{
					receive2.add(send);
				}
				catch(Exception e)
				{
					e.printStackTrace();
					System.exit(1);
				}
			}
		}
		
		ArrayList<NetworkSendOperator> sends2 = new ArrayList<NetworkSendOperator>(parents.size());
		for (Operator clone : parents)
		{
			for (Operator receive2 : receives)
			{
				if (clone.getNode() == receive2.getNode())
				{
					try
					{
						clone.add(receive2);
					}
					catch(Exception e)
					{
						e.printStackTrace();
						System.exit(1);
					}
				}
			}
			
			NetworkSendOperator send = new NetworkSendOperator(clone.getNode(), meta);
			sends2.add(send);
			try
			{
				send.add(clone);
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(1);
			}
		}
		
		NetworkReceiveOperator r = new NetworkReceiveOperator(meta);
		r.setNode(-1);
		for (NetworkSendOperator send : sends2)
		{
			try
			{
				r.add(send);
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(1);
			}
		}
		try
		{
			grandParent.add(r);
		}
		catch(Exception e)
		{
			e.printStackTrace();
			System.exit(1);
		}
		makeHierarchical2(r);
		makeHierarchical(r);
		cCache.clear();
		return false;
	}
	
	private boolean doHashAnti(NetworkReceiveOperator receive)
	{
		Operator parent = receive.parent();
		verify2ReceivesForHash(parent);
		Operator grandParent = parent.parent();
		long card = card(parent.children().get(0)) + card(parent.children().get(1));
		ArrayList<String> join = ((AntiJoinOperator)parent).getJoinForChild(receive);
		parent.removeChild(receive);
		grandParent.removeChild(parent);
		ArrayList<Operator> sends = new ArrayList<Operator>(receive.children().size());
		int starting = getStartingNode(card / MIN_LOCAL_LEFT_HASH + 1);
		int ID = id++;
		Operator other = null;
		for (Operator o : parent.children())
		{
			if (o != receive)
			{
				other = o;
			}
		}
		String t = meta.getTableForCol(((AntiJoinOperator)parent).getJoinForChild(other).get(0));
		PartitionMetaData pmeta = null;
		if (t != null)
		{
			pmeta = meta.getPartMeta("TPCH", t);
		}
		
		if (pmeta != null && isLocal(other.children().get(0).children().get(0)) && pmeta.noNodeGroupSet() && pmeta.nodeIsHash() && join.containsAll(pmeta.getNodeHash()) && pmeta.allNodes())
		{
			starting = 0;
			card = Long.MAX_VALUE;
		}
		
		CNFFilter cnf = null;
		for (Operator child : (ArrayList<Operator>)receive.children().clone())
		{
			int node = child.getNode();
			Operator grandChild = child.children().get(0);
			
			if (grandChild instanceof TableScanOperator)
			{
				cnf = ((TableScanOperator)grandChild).getCNFForParent(child);
			}
			child.removeChild(grandChild);
			t = meta.getTableForCol(join.get(0));
			pmeta = null;
			if (t != null)
			{
				pmeta = meta.getPartMeta("TPCH", t);
			}
			
			if (pmeta != null && isLocal(grandChild) && pmeta.noNodeGroupSet() && pmeta.nodeIsHash() && join.containsAll(pmeta.getNodeHash()) && pmeta.allNodes())
			{
				starting = 0;
				card = Long.MAX_VALUE;
			}
			
			NetworkHashAndSendOperator send = new NetworkHashAndSendOperator(join, card / MIN_LOCAL_LEFT_HASH + 1, ID, starting, meta);
			try
			{
				send.add(grandChild);
				if (cnf != null)
				{
					((TableScanOperator)grandChild).setCNFForParent(send, cnf);
				}
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(1);
			}
			send.setNode(node);
			sends.add(send);
		}
		
		int i = starting;
		ArrayList<Operator> receives = new ArrayList<Operator>();
		while (i < (card / MIN_LOCAL_LEFT_HASH + 1) + starting && i < meta.getNumNodes())
		{
			NetworkHashReceiveOperator receive2 = new NetworkHashReceiveOperator(ID, meta);
			receive2.setNode(i);
			receives.add(receive2);
			i++;
		}
		
		for (Operator receive2 : receives)
		{
			for (Operator send : sends)
			{
				try
				{
					receive2.add(send);
				}
				catch(Exception e)
				{
					e.printStackTrace();
					System.exit(1);
				}
			}
		}
		
		ArrayList<Operator> parents = new ArrayList<Operator>(receives.size());
		for (Operator receive2 : receives)
		{
			Operator clone = cloneTree(parent, 0);
			try
			{
				clone.add(receive2);
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(1);
			}
			clone.setNode(receive2.getNode());
			parents.add(clone);
			for (Operator o : (ArrayList<Operator>)clone.children().clone())
			{
				if (!o.equals(receive2))
				{
					clone.removeChild(o);
				}
			}
		}
		
		Operator otherChild = null;
		for (Operator child : parent.children())
		{
			if (!child.equals(receive))
			{
				otherChild = child;
			}
		}
		
		join = ((AntiJoinOperator)parent).getJoinForChild(otherChild);
		sends = new ArrayList<Operator>(otherChild.children().size());
		ID = id++;
		for (Operator child : (ArrayList<Operator>)otherChild.children().clone())
		{
			int node = child.getNode();
			Operator grandChild = child.children().get(0);
			cnf = null;
			if (grandChild instanceof TableScanOperator)
			{
				cnf = ((TableScanOperator)grandChild).getCNFForParent(child);
			}
			child.removeChild(grandChild);
			
			NetworkHashAndSendOperator send = new NetworkHashAndSendOperator(join, card / MIN_LOCAL_LEFT_HASH + 1, ID, starting, meta);
			try
			{
				send.add(grandChild);
				if (cnf != null)
				{
					((TableScanOperator)grandChild).setCNFForParent(send, cnf);
				}
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(1);
			}
			send.setNode(node);
			sends.add(send);
		}
		
		i = starting;
		receives = new ArrayList<Operator>();
		while (i < (card / MIN_LOCAL_LEFT_HASH + 1) + starting && i < meta.getNumNodes())
		{
			NetworkHashReceiveOperator receive2 = new NetworkHashReceiveOperator(ID, meta);
			receive2.setNode(i);
			receives.add(receive2);
			i++;
		}
		
		for (Operator receive2 : receives)
		{
			for (Operator send : sends)
			{
				try
				{
					receive2.add(send);
				}
				catch(Exception e)
				{
					e.printStackTrace();
					System.exit(1);
				}
			}
		}
		
		ArrayList<NetworkSendOperator> sends2 = new ArrayList<NetworkSendOperator>(parents.size());
		for (Operator clone : parents)
		{
			for (Operator receive2 : receives)
			{
				if (clone.getNode() == receive2.getNode())
				{
					try
					{
						clone.add(receive2);
					}
					catch(Exception e)
					{
						e.printStackTrace();
						System.exit(1);
					}
				}
			}
			
			NetworkSendOperator send = new NetworkSendOperator(clone.getNode(), meta);
			sends2.add(send);
			try
			{
				send.add(clone);
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(1);
			}
		}
		
		NetworkReceiveOperator r = new NetworkReceiveOperator(meta);
		r.setNode(-1);
		for (NetworkSendOperator send : sends2)
		{
			try
			{
				r.add(send);
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(1);
			}
		}
		try
		{
			grandParent.add(r);
		}
		catch(Exception e)
		{
			e.printStackTrace();
			System.exit(1);
		}
		makeHierarchical2(r);
		makeHierarchical(r);
		cCache.clear();
		return false;
	}
	
	private boolean doNonHashSemi(NetworkReceiveOperator receive)
	{
		Operator parent = receive.parent();
		verify2ReceivesForSemi(parent);
		Operator right = parent.children().get(0);
		Operator left = parent.children().get(1);
		
		if (isAllAny(left))
		{
			Operator grandParent = parent.parent();
			parent.removeChild(left);
			grandParent.removeChild(parent);
			ArrayList<Operator> grandChildren = new ArrayList<Operator>();
			for (Operator child : (ArrayList<Operator>)right.children().clone())
			{
				Operator grandChild = child.children().get(0);
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
			
			ArrayList<NetworkSendOperator> sends2 = new ArrayList<NetworkSendOperator>(grandChildren.size());
			for (Operator grandChild : grandChildren)
			{
				Operator clone = cloneTree(parent, 0);
				clone.setNode(grandChild.getNode());
				Operator leftClone = fullyCloneTree(left.children().get(0).children().get(0));
				setNodeForTree(leftClone, grandChild.getNode());
				try
				{
					clone.add(leftClone);
				}
				catch(Exception e)
				{
					e.printStackTrace();
					System.exit(1);
				}
				
				for (Operator o : (ArrayList<Operator>)clone.children().clone())
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
						CNFFilter cnf = ((TableScanOperator)grandChild).getFirstCNF();
						if (cnf != null)
						{
							((TableScanOperator)grandChild).setCNFForParent(clone, cnf);
						}
					}
				}
				catch(Exception e)
				{
					e.printStackTrace();
					System.exit(1);
				}
				
				NetworkSendOperator send2 = new NetworkSendOperator(clone.getNode(), meta);
				try
				{
					send2.add(clone);
				}
				catch(Exception e)
				{
					e.printStackTrace();
					System.exit(1);
				}
				
				sends2.add(send2);
			}
			
			NetworkReceiveOperator r = new NetworkReceiveOperator(meta);
			r.setNode(-1);
			
			for (NetworkSendOperator send : sends2)
			{
				try
				{
					r.add(send);
				}
				catch(Exception e)
				{
					e.printStackTrace();
					System.exit(1);
				}
			}
			
			try
			{
				grandParent.add(r);
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(1);
			}
			
			makeHierarchical(r);
			cCache.clear();
			return false;
		}
		
		//send left (right) data to nodes and perform product there
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
		Operator grandParent = parent.parent();
		parent.removeChild(left);
		grandParent.removeChild(parent);
		ArrayList<NetworkSendMultipleOperator> sends = new ArrayList<NetworkSendMultipleOperator>(left.children().size());
		int ID = id++;
		for (Operator child : (ArrayList<Operator>)left.children().clone())
		{
			int node = child.getNode();
			Operator grandChild = child.children().get(0);
			CNFFilter cnf = null;
			if (grandChild instanceof TableScanOperator)
			{
				cnf = ((TableScanOperator)grandChild).getCNFForParent(child);
			}
			child.removeChild(grandChild);
			NetworkSendMultipleOperator send = new NetworkSendMultipleOperator(ID, meta);
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
					SortOperator sort = new SortOperator(sortKeys, orders, meta);
					sort.add(grandChild);
					sort.setNode(node);
					if (cnf != null)
					{
						((TableScanOperator)grandChild).setCNFForParent(sort, cnf);
					}
					send.add(sort);
				}
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(1);
			}
			send.setNode(node);
			sends.add(send);
		}
		
		ArrayList<Operator> grandChildren = new ArrayList<Operator>();
		for (Operator child : (ArrayList<Operator>)right.children().clone())
		{
			Operator grandChild = child.children().get(0);
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
		
		ArrayList<NetworkReceiveOperator> receives = new ArrayList<NetworkReceiveOperator>(grandChildren.size());
		for (Operator grandChild : grandChildren)
		{
			Operator clone = cloneTree(parent, 0);
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
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(1);
			}
			
			receive2.setNode(grandChild.getNode());
			receives.add(receive2);
			for (Operator o : (ArrayList<Operator>)clone.children().clone())
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
					CNFFilter cnf = ((TableScanOperator)grandChild).getFirstCNF();
					if (cnf != null)
					{
						((TableScanOperator)grandChild).setCNFForParent(clone, cnf);
					}
				}
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(1);
			}
		}
		
		ArrayList<NetworkSendOperator> sends2 = new ArrayList<NetworkSendOperator>(receives.size());
		for (NetworkReceiveOperator receive2 : receives)
		{
			for (NetworkSendMultipleOperator send : sends)
			{
				try
				{
					receive2.add(send);
				}
				catch(Exception e)
				{
					e.printStackTrace();
					System.exit(1);
				}
			}
			
			NetworkSendOperator send2 = new NetworkSendOperator(receive2.getNode(), meta);
			send2.setNode(receive2.getNode());
			try
			{
				send2.add(receive2.parent());
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(1);
			}
			
			sends2.add(send2);
		}
		
		NetworkReceiveOperator r = new NetworkReceiveOperator(meta);
		r.setNode(-1);
		
		for (NetworkSendOperator send : sends2)
		{
			try
			{
				r.add(send);
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(1);
			}
		}
		
		try
		{
			grandParent.add(r);
		}
		catch(Exception e)
		{
			e.printStackTrace();
			System.exit(1);
		}
		
		for (NetworkReceiveOperator receive2 : receives)
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
				catch(Exception e)
				{
					e.printStackTrace();
					System.exit(1);
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
	
	private void doSortRedistribution(SortOperator op)
	{
		long numNodes = card(op) / MAX_LOCAL_SORT;
		int starting = getStartingNode(numNodes);
		Operator parent = op.parent();
		parent.removeChild(op);
		Operator child = op.children().get(0);
		op.removeChild(child);
		int ID = id++;
		NetworkSendRROperator rr = new NetworkSendRROperator(ID, meta);
		try
		{
			rr.add(child);
		}
		catch(Exception e)
		{
			e.printStackTrace();
			System.exit(1);
		}
		rr.setNode(-1);
		
		ArrayList<NetworkSendOperator> sends = new ArrayList<NetworkSendOperator>();
		int i = 0;
		while (i < numNodes && starting + i < meta.getNumNodes())
		{
			try
			{
				NetworkHashReceiveOperator receive = new NetworkHashReceiveOperator(ID, meta);
				receive.setNode(starting + i);
				receive.add(rr);
				SortOperator sort2 = op.clone();
				sort2.add(receive);
				sort2.setNode(starting + i);
				NetworkSendOperator send = new NetworkSendOperator(starting + i, meta);
				send.add(sort2);
				sends.add(send);
				i++;
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(1);
			}
		}
		
		NetworkReceiveAndMergeOperator receive = new NetworkReceiveAndMergeOperator(op.getKeys(), op.getOrders(), meta);
		receive.setNode(-1);
		for (NetworkSendOperator send : sends)
		{
			receive.add(send);
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
		makeHierarchical4(receive);
		if (parent instanceof TopOperator)
		{
			handleTop(receive);
		}
		makeHierarchical(receive);
		cCache.clear();
	}
	
	private void makeHierarchical2(NetworkReceiveOperator op)
	{
		//NetworkHashAndSend -> NetworkHashReceive
		ArrayList<NetworkHashReceiveOperator> lreceives = new ArrayList<NetworkHashReceiveOperator>();
		ArrayList<NetworkHashReceiveOperator> lreceives2 = new ArrayList<NetworkHashReceiveOperator>();
		ArrayList<NetworkHashReceiveOperator> rreceives = new ArrayList<NetworkHashReceiveOperator>();
		ArrayList<NetworkHashReceiveOperator> rreceives2 = new ArrayList<NetworkHashReceiveOperator>();
		for (Operator child : op.children())
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
				
		ArrayList<NetworkHashAndSendOperator> lsends = new ArrayList<NetworkHashAndSendOperator>(lreceives.get(0).children().size());
		if (dol)
		{
			for (Operator o : lreceives.get(0).children())
			{
				lsends.add((NetworkHashAndSendOperator)o);
			}
		}
		
		ArrayList<NetworkHashAndSendOperator> rsends = new ArrayList<NetworkHashAndSendOperator>(rreceives.get(0).children().size());
		if (dor)
		{
			for (Operator o : rreceives.get(0).children())
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
			
			int lstarting = getStartingNode(numMiddle);
			int rstarting = getStartingNode(numMiddle);
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
				for (Operator rr : lsends)
				{
					try
					{
						newLReceive.add(rr);
					}
					catch(Exception e)
					{
						e.printStackTrace();
						System.exit(1);
					}
				}
			}
			if (dor)
			{
				for (Operator rr : rsends)
				{
					try
					{
						newRReceive.add(rr);
					}
					catch(Exception e)
					{
						e.printStackTrace();
						System.exit(1);
					}
				}
			}			
			int newLID = id++;
			int newRID = id++;
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
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(1);
			}
							
			int j = 0;
			int a = 0;
			for (NetworkHashReceiveOperator lreceive : generic)
			{
				NetworkHashReceiveOperator rreceive = rreceives.get(j);
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
					for (Operator child : (ArrayList<Operator>)lreceive.children().clone())
					{
						lreceive.removeChild(child);
					}
				}
				if (dor)
				{
					for (Operator child : (ArrayList<Operator>)rreceive.children().clone())
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
				catch(Exception e)
				{
					e.printStackTrace();
					System.exit(1);
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
						for (Operator rr : lsends)
						{
							try
							{
								newLReceive.add(rr);
							}
							catch(Exception e)
							{
								e.printStackTrace();
								System.exit(1);
							}
						}
					}
					if (dor)
					{
						for (Operator rr : rsends)
						{
							try
							{
								newRReceive.add(rr);
							}	
							catch(Exception e)
							{
								e.printStackTrace();
								System.exit(1);
							}
						}
					}
									
					newLID = id++;
					newRID = id++;
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
					catch(Exception e)
					{
						e.printStackTrace();
						System.exit(1);
					}
									
					j = 0;
					a++;
				}
			}
			
			if (dol)
			{
				for (NetworkHashAndSendOperator rr : lsends)
				{
					rr.setStarting(lstarting);
					rr.setNumNodes(numMiddle);
				}
			}
			if (dor)
			{
				for (NetworkHashAndSendOperator rr : rsends)
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
	
	private void makeHierarchical3(NetworkReceiveOperator op)
	{
		//NetworkSendMultiple -> NetworkHashReceive
		ArrayList<NetworkHashReceiveOperator> receives = new ArrayList<NetworkHashReceiveOperator>();
		ArrayList<NetworkHashReceiveOperator> receives2 = new ArrayList<NetworkHashReceiveOperator>();
		for (Operator child : op.children())
		{
			for (Operator o : child.children().get(0).children())
			{
				if (o instanceof NetworkHashReceiveOperator)
				{
					receives.add((NetworkHashReceiveOperator)o);
				}
			}
		}
		
		ArrayList<NetworkSendMultipleOperator> sends = new ArrayList<NetworkSendMultipleOperator>(receives.get(0).children().size());
		for (Operator o : receives.get(0).children())
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
			int node = Math.abs(ThreadLocalRandom.current().nextInt()) % meta.getNumNodes();
			while (usedNodes.contains(node))
			{
				node = Math.abs(ThreadLocalRandom.current().nextInt()) % meta.getNumNodes();
			}
			newReceive.setNode(node);
			receives2.add(newReceive);
			for (Operator rr : sends)
			{
				try
				{
					newReceive.add(rr);
				}
				catch(Exception e)
				{
					e.printStackTrace();
					System.exit(1);
				}
			}
					
			int newID = id++;
			NetworkSendMultipleOperator newRR = new NetworkSendMultipleOperator(newID, meta);
			newRR.setNode(node);
					
			try
			{
				newRR.add(newReceive);
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(1);
			}
					
			int j = 0;
			for (NetworkHashReceiveOperator receive : receives)
			{
				for (Operator child : (ArrayList<Operator>)receive.children().clone())
				{
					receive.removeChild(child);
				}
				try
				{
					receive.add(newRR);
				}
				catch(Exception e)
				{
					e.printStackTrace();
					System.exit(1);
				}
						
				receive.setID(newID);
				j++;
							
				if (j == numPerMiddle)
				{
					newReceive = new NetworkHashReceiveOperator(sends.get(0).getID(), meta);
					node = Math.abs(ThreadLocalRandom.current().nextInt()) % meta.getNumNodes();
					while (usedNodes.contains(node))
					{
						node = Math.abs(ThreadLocalRandom.current().nextInt()) % meta.getNumNodes();
					}
					newReceive.setNode(node);
					receives2.add(newReceive);
					for (Operator rr : sends)
					{
						try
						{
							newReceive.add(rr);
						}
						catch(Exception e)
						{
							e.printStackTrace();
							System.exit(1);
						}
					}
							
					newID = id++;
					newRR = new NetworkSendMultipleOperator(newID, meta);
					newRR.setNode(node);
							
					try
					{
						newRR.add(newReceive);
					}
					catch(Exception e)
					{
						e.printStackTrace();
						System.exit(1);
					}
							
					j = 0;
				}
			}
					
			receives = receives2;
			receives2 = new ArrayList<NetworkHashReceiveOperator>();
		}
		
		cCache.clear();
	}
	
	private void makeHierarchical4(NetworkReceiveOperator op)
	{
		//NetworkSendRR -> NetworkHashReceive
		ArrayList<NetworkHashReceiveOperator> receives = new ArrayList<NetworkHashReceiveOperator>();
		ArrayList<NetworkHashReceiveOperator> receives2 = new ArrayList<NetworkHashReceiveOperator>();
		for (Operator child : op.children())
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
			
			NetworkSendRROperator rr = (NetworkSendRROperator)receives.get(0).children().get(0);
			NetworkHashReceiveOperator newReceive = new NetworkHashReceiveOperator(rr.getID(), meta);
			int node = Math.abs(ThreadLocalRandom.current().nextInt()) % meta.getNumNodes();
			while (usedNodes.contains(node))
			{
				node = Math.abs(ThreadLocalRandom.current().nextInt()) % meta.getNumNodes();
			}
			newReceive.setNode(node);
			receives2.add(newReceive);
			try
			{
				newReceive.add(rr);
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(1);
			}
			
			int newID = id++;
			NetworkSendRROperator newRR = new NetworkSendRROperator(newID, meta);
			newRR.setNode(node);
			
			try
			{
				newRR.add(newReceive);
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(1);
			}
			
			int j = 0;
			for (NetworkHashReceiveOperator receive : receives)
			{
				receive.removeChild(receive.children().get(0));
				try
				{
					receive.add(newRR);
				}
				catch(Exception e)
				{
					e.printStackTrace();
					System.exit(1);
				}
				
				receive.setID(newID);
				j++;
					
				if (j == numPerMiddle)
				{
					newReceive = new NetworkHashReceiveOperator(rr.getID(), meta);
					node = Math.abs(ThreadLocalRandom.current().nextInt()) % meta.getNumNodes();
					while (usedNodes.contains(node))
					{
						node = Math.abs(ThreadLocalRandom.current().nextInt()) % meta.getNumNodes();
					}
					newReceive.setNode(node);
					try
					{
						newReceive.add(rr);
					}
					catch(Exception e)
					{
						e.printStackTrace();
						System.exit(1);
					}
					
					newID = id++;
					newRR = new NetworkSendRROperator(newID, meta);
					newRR.setNode(node);
					
					try
					{
						newRR.add(newReceive);
					}
					catch(Exception e)
					{
						e.printStackTrace();
						System.exit(1);
					}
					
					j = 0;
				}
			}
			
			receives = receives2;
			receives2 = new ArrayList<NetworkHashReceiveOperator>();
		}
		
		cCache.clear();
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
	
	private void setNodeForTree(Operator op, int node)
	{
		op.setNode(node);
		for (Operator o : op.children())
		{
			setNodeForTree(o, node);
		}
	}
	
	private void setNodeUpward(Operator op)
	{
		int node = op.getNode();
		op = op.parent();
		while (!(op instanceof NetworkSendOperator))
		{
			op.setNode(node);
			op = op.parent();
		}
		op.setNode(node);
	}
	
	private void verify2ReceivesForSemi(Operator op)
	{
		try
		{
			if (!(op.children().get(1) instanceof NetworkReceiveOperator))
			{
				Operator child = op.children().get(1);
				op.removeChild(child);
				NetworkSendOperator send = new NetworkSendOperator(-1, meta);
				send.add(child);
				NetworkReceiveOperator receive = new NetworkReceiveOperator(meta);
				receive.setNode(-1);
				receive.add(send);
				op.add(receive);
				cCache.clear();
			}
			
			if (!(op.children().get(0) instanceof NetworkReceiveOperator))
			{
				Operator child = op.children().get(0);
				op.removeChild(child);
				int ID = id++;
				NetworkSendRROperator send = new NetworkSendRROperator(ID, meta);
				send.setNode(-1);
				send.add(child);
				
				long numNodes = card(child) / MAX_LOCAL_SORT + 1;
				int starting = getStartingNode(numNodes);
				ArrayList<NetworkSendOperator> sends = new ArrayList<NetworkSendOperator>();
				int i = 0;
				while (i < numNodes && starting + i < meta.getNumNodes())
				{
					NetworkHashReceiveOperator receive = new NetworkHashReceiveOperator(ID, meta);
					receive.setNode(starting + i);
					receive.add(send);
					NetworkSendOperator send2 = new NetworkSendOperator(starting + i, meta);
					ReorderOperator reorder = new ReorderOperator(new ArrayList<String>(receive.getPos2Col().values()), meta);
					reorder.setNode(starting + i);
					reorder.add(receive);
					send2.add(reorder);
					sends.add(send2);
					i++;
				}
				
				NetworkReceiveOperator receive = new NetworkReceiveOperator(meta);
				receive.setNode(-1);
				
				for (NetworkSendOperator send2 : sends)
				{
					receive.add(send2);
				}
				op.add(receive);
				makeHierarchical4(receive);
				cCache.clear();
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	private void verify2ReceivesForProduct(Operator op)
	{
		try
		{
			if (!(op.children().get(0) instanceof NetworkReceiveOperator))
			{
				Operator child = op.children().get(0);
				op.removeChild(child);
				NetworkSendOperator send = new NetworkSendOperator(-1, meta);
				send.add(child);
				NetworkReceiveOperator receive = new NetworkReceiveOperator(meta);
				receive.setNode(-1);
				receive.add(send);
				op.add(receive);
				cCache.clear();
			}
			
			if (!(op.children().get(1) instanceof NetworkReceiveOperator))
			{
				Operator child = op.children().get(1);
				op.removeChild(child);
				int ID = id++;
				NetworkSendRROperator send = new NetworkSendRROperator(ID, meta);
				send.setNode(-1);
				send.add(child);
				
				long numNodes = card(child) / MAX_LOCAL_SORT + 1;
				int starting = getStartingNode(numNodes);
				ArrayList<NetworkSendOperator> sends = new ArrayList<NetworkSendOperator>();
				int i = 0;
				while (i < numNodes && starting + i < meta.getNumNodes())
				{
					NetworkHashReceiveOperator receive = new NetworkHashReceiveOperator(ID, meta);
					receive.setNode(starting + i);
					receive.add(send);
					NetworkSendOperator send2 = new NetworkSendOperator(starting + i, meta);
					ReorderOperator reorder = new ReorderOperator(new ArrayList<String>(receive.getPos2Col().values()), meta);
					reorder.setNode(starting + i);
					reorder.add(receive);
					send2.add(reorder);
					sends.add(send2);
					i++;
				}
				
				NetworkReceiveOperator receive = new NetworkReceiveOperator(meta);
				receive.setNode(-1);
				
				for (NetworkSendOperator send2 : sends)
				{
					receive.add(send2);
				}
				op.add(receive);
				makeHierarchical4(receive);
				cCache.clear();
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	private void verify2ReceivesForHash(Operator op)
	{
		try
		{
			if (!(op.children().get(0) instanceof NetworkReceiveOperator))
			{
				Operator child = op.children().get(0);
				op.removeChild(child);
				NetworkSendOperator send = new NetworkSendOperator(-1, meta);
				send.add(child);
				NetworkReceiveOperator receive = new NetworkReceiveOperator(meta);
				receive.setNode(-1);
				receive.add(send);
				op.add(receive);
				cCache.clear();
			}
		
			if (!(op.children().get(1) instanceof NetworkReceiveOperator))
			{
				Operator child = op.children().get(1);
				op.removeChild(child);
				NetworkSendOperator send = new NetworkSendOperator(-1, meta);
				send.add(child);
				NetworkReceiveOperator receive = new NetworkReceiveOperator(meta);
				receive.setNode(-1);
				receive.add(send);
				op.add(receive);
				cCache.clear();
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
			System.exit(1);
		}
	}
}