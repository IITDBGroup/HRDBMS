package com.exascale.optimizer;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.TreeMap;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.misc.DataEndMarker;
import com.exascale.tables.Plan;

public final class NestedLoopJoinOperator extends JoinOperator implements Serializable
{
	private static sun.misc.Unsafe unsafe;

	static
	{
		try
		{
			final Field f = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
			f.setAccessible(true);
			unsafe = (sun.misc.Unsafe)f.get(null);
		}
		catch (Exception e)
		{
			unsafe = null;
		}
	}

	private ArrayList<Operator> children = new ArrayList<Operator>(2);
	private Operator parent;
	private HashMap<String, String> cols2Types;
	private HashMap<String, Integer> cols2Pos;
	private TreeMap<Integer, String> pos2Col;
	private MetaData meta;
	private CNFFilter cnfFilters;
	private HashSet<HashMap<Filter, Filter>> f;
	private int childPos = -1;
	private int node;
	private boolean indexAccess = false;
	private ArrayList<Index> dynamicIndexes;
	private long rightChildCard = 16;
	private boolean alreadySorted = false;
	private boolean cardSet = false;
	public transient Operator dynamicOp;
	private long leftChildCard = 16;
	private long txnum;

	public NestedLoopJoinOperator(ArrayList<Filter> filters, MetaData meta)
	{
		this.meta = meta;
		this.addFilter(filters);
	}

	public NestedLoopJoinOperator(JoinOperator op)
	{
		this.meta = op.getMeta();
		this.f = op.getHSHMFilter();
	}

	private NestedLoopJoinOperator(MetaData meta)
	{
		this.meta = meta;
	}

	public static NestedLoopJoinOperator deserialize(InputStream in, HashMap<Long, Object> prev) throws Exception
	{
		NestedLoopJoinOperator value = (NestedLoopJoinOperator)unsafe.allocateInstance(NestedLoopJoinOperator.class);
		prev.put(OperatorUtils.readLong(in), value);
		value.children = OperatorUtils.deserializeALOp(in, prev);
		value.parent = OperatorUtils.deserializeOperator(in, prev);
		value.cols2Types = OperatorUtils.deserializeStringHM(in, prev);
		value.cols2Pos = OperatorUtils.deserializeStringIntHM(in, prev);
		value.pos2Col = OperatorUtils.deserializeTM(in, prev);
		value.meta = new MetaData();
		value.cnfFilters = OperatorUtils.deserializeCNF(in, prev);
		value.f = OperatorUtils.deserializeHSHM(in, prev);
		value.childPos = OperatorUtils.readInt(in);
		value.node = OperatorUtils.readInt(in);
		value.indexAccess = OperatorUtils.readBool(in);
		value.dynamicIndexes = OperatorUtils.deserializeALIndx(in, prev);
		value.rightChildCard = OperatorUtils.readLong(in);
		value.alreadySorted = OperatorUtils.readBool(in);
		value.cardSet = OperatorUtils.readBool(in);
		value.leftChildCard = OperatorUtils.readLong(in);
		value.txnum = OperatorUtils.readLong(in);
		return value;
	}

	@Override
	public void add(Operator op) throws Exception
	{
		if (children.size() < 2)
		{
			if (childPos == -1)
			{
				children.add(op);
			}
			else
			{
				children.add(childPos, op);
				childPos = -1;
			}
			op.registerParent(this);

			if (children.size() == 2 && children.get(0).getCols2Types() != null && children.get(1).getCols2Types() != null)
			{
				cols2Types = (HashMap<String, String>)children.get(0).getCols2Types().clone();
				cols2Pos = (HashMap<String, Integer>)children.get(0).getCols2Pos().clone();
				pos2Col = (TreeMap<Integer, String>)children.get(0).getPos2Col().clone();

				cols2Types.putAll(children.get(1).getCols2Types());
				for (final Map.Entry entry : children.get(1).getPos2Col().entrySet())
				{
					cols2Pos.put((String)entry.getValue(), cols2Pos.size());
					pos2Col.put(pos2Col.size(), (String)entry.getValue());
				}

				cnfFilters = new CNFFilter(f, meta, cols2Pos, this);
			}
		}
		else
		{
			throw new Exception("NestedLoopJoinOperator only supports 2 children");
		}
	}

	public void addFilter(ArrayList<Filter> filters)
	{
		if (f == null)
		{
			f = new HashSet<HashMap<Filter, Filter>>();
			final HashMap<Filter, Filter> map = new HashMap<Filter, Filter>();
			for (final Filter filter : filters)
			{
				map.put(filter, filter);
			}

			f.add(map);
		}
		else
		{
			final HashMap<Filter, Filter> map = new HashMap<Filter, Filter>();
			for (final Filter filter : filters)
			{
				map.put(filter, filter);
			}

			f.add(map);
		}
	}

	@Override
	public void addJoinCondition(ArrayList<Filter> filters)
	{
		addFilter(filters);
	}

	@Override
	public void addJoinCondition(String left, String right)
	{
		throw new UnsupportedOperationException("NestedLoopJoinOperator does not support addJoinCondition(String, String)");
	}

	public void alreadySorted()
	{
		alreadySorted = true;
	}

	@Override
	public ArrayList<Operator> children()
	{
		return children;
	}

	@Override
	public NestedLoopJoinOperator clone()
	{
		final NestedLoopJoinOperator retval = new NestedLoopJoinOperator(meta);
		retval.f = this.getHSHMFilter();
		retval.node = node;
		retval.indexAccess = indexAccess;
		retval.dynamicIndexes = dynamicIndexes;
		retval.alreadySorted = alreadySorted;
		retval.rightChildCard = rightChildCard;
		retval.cardSet = cardSet;
		retval.leftChildCard = leftChildCard;
		retval.txnum = txnum;
		return retval;
	}

	@Override
	public void close() throws Exception
	{
		dynamicOp.close();
		cnfFilters = null;
		f = null;
		dynamicIndexes = null;
		cols2Pos = null;
		cols2Types = null;
		pos2Col = null;
	}

	@Override
	public int getChildPos()
	{
		return childPos;
	}

	@Override
	public HashMap<String, Integer> getCols2Pos()
	{
		return cols2Pos;
	}

	@Override
	public HashMap<String, String> getCols2Types()
	{
		return cols2Types;
	}

	public HashSet<HashMap<Filter, Filter>> getHSHM()
	{
		return getHSHMFilter();
	}

	@Override
	public HashSet<HashMap<Filter, Filter>> getHSHMFilter()
	{
		return f;
	}

	@Override
	public boolean getIndexAccess()
	{
		return indexAccess;
	}

	@Override
	public ArrayList<String> getJoinForChild(Operator op)
	{
		Filter x = null;
		for (final HashMap<Filter, Filter> filters : f)
		{
			if (filters.size() == 1)
			{
				for (final Filter filter : filters.keySet())
				{
					if (filter.op().equals("E"))
					{
						if (filter.leftIsColumn() && filter.rightIsColumn())
						{
							x = filter;
						}
					}

					break;
				}
			}

			if (x != null)
			{
				break;
			}
		}

		if (x == null)
		{
			return null;
		}

		if (op.getCols2Pos().keySet().contains(x.leftColumn()))
		{
			final ArrayList<String> retval = new ArrayList<String>(1);
			retval.add(x.leftColumn());
			return retval;
		}

		final ArrayList<String> retval = new ArrayList<String>(1);
		retval.add(x.rightColumn());
		return retval;
	}

	@Override
	public MetaData getMeta()
	{
		return meta;
	}

	@Override
	public int getNode()
	{
		return node;
	}

	@Override
	public TreeMap<Integer, String> getPos2Col()
	{
		return pos2Col;
	}

	@Override
	public ArrayList<String> getReferences()
	{
		final ArrayList<String> retval = new ArrayList<String>(f.size());
		for (final HashMap<Filter, Filter> filters : f)
		{
			for (final Filter filter : filters.keySet())
			{
				if (filter.leftIsColumn())
				{
					retval.add(filter.leftColumn());
				}

				if (filter.rightIsColumn())
				{
					retval.add(filter.rightColumn());
				}
			}
		}
		return retval;
	}

	@Override
	public Object next(Operator op) throws Exception
	{
		return dynamicOp.next(this);
	}

	@Override
	public void nextAll(Operator op) throws Exception
	{
		dynamicOp.nextAll(this);
		Object o = next(op);
		while (!(o instanceof DataEndMarker) && !(o instanceof Exception))
		{
			o = next(op);
		}
	}

	@Override
	public long numRecsReceived()
	{
		return dynamicOp.numRecsReceived();
	}

	@Override
	public Operator parent()
	{
		return parent;
	}

	@Override
	public boolean receivedDEM()
	{
		return dynamicOp.receivedDEM();
	}

	@Override
	public void registerParent(Operator op) throws Exception
	{
		if (parent == null)
		{
			parent = op;
		}
		else
		{
			throw new Exception("NestedLoopJoinOperator can only have 1 parent.");
		}
	}

	@Override
	public void removeChild(Operator op)
	{
		childPos = children.indexOf(op);
		children.remove(op);
		op.removeParent(this);
	}

	@Override
	public void removeParent(Operator op)
	{
		parent = null;
	}

	@Override
	public void reset() throws Exception
	{
		HRDBMSWorker.logger.error("NestedLoopJoinOperator cannot be reset");
		throw new Exception("NestedLoopJoinOperator cannot be reset");
	}

	@Override
	public void serialize(OutputStream out, IdentityHashMap<Object, Long> prev) throws Exception
	{
		Long id = prev.get(this);
		if (id != null)
		{
			OperatorUtils.serializeReference(id, out);
			return;
		}

		OperatorUtils.writeType(59, out);
		prev.put(this, OperatorUtils.writeID(out));
		OperatorUtils.serializeALOp(children, out, prev);
		parent.serialize(out, prev);
		OperatorUtils.serializeStringHM(cols2Types, out, prev);
		OperatorUtils.serializeStringIntHM(cols2Pos, out, prev);
		OperatorUtils.serializeTM(pos2Col, out, prev);
		// create new meta on receive side
		OperatorUtils.serializeCNF(cnfFilters, out, prev);
		OperatorUtils.serializeHSHM(f, out, prev);
		OperatorUtils.writeInt(childPos, out);
		OperatorUtils.writeInt(node, out);
		OperatorUtils.writeBool(indexAccess, out);
		OperatorUtils.serializeALIndx(dynamicIndexes, out, prev);
		OperatorUtils.writeLong(rightChildCard, out);
		OperatorUtils.writeBool(alreadySorted, out);
		OperatorUtils.writeBool(cardSet, out);
		OperatorUtils.writeLong(leftChildCard, out);
		OperatorUtils.writeLong(txnum, out);
	}

	@Override
	public void setChildPos(int pos)
	{
		childPos = pos;
	}

	public void setDynamicIndex(ArrayList<Index> indexes)
	{
		indexAccess = true;
		this.dynamicIndexes = indexes;
	}

	@Override
	public void setNode(int node)
	{
		this.node = node;
	}

	@Override
	public void setPlan(Plan plan)
	{
	}

	public boolean setRightChildCard(long card, long card2)
	{
		if (cardSet)
		{
			return false;
		}

		cardSet = true;
		rightChildCard = card;
		leftChildCard = card2;
		return true;
	}

	public void setTXNum(long txnum)
	{
		this.txnum = txnum;
	}

	public ArrayList<String> sortKeys()
	{
		for (final HashMap<Filter, Filter> filters : f)
		{
			if (filters.size() == 1)
			{
				for (final Filter filter : filters.keySet())
				{
					if (filter.op().equals("G") || filter.op().equals("GE") || filter.op().equals("L") || filter.op().equals("LE"))
					{
						if (filter.leftIsColumn() && filter.rightIsColumn())
						{
							String vStr;
							if (filter.op().equals("G") || filter.op().equals("GE"))
							{
								vStr = filter.rightColumn();
								// vBool = true;
								// System.out.println("VBool set to true");
							}
							else
							{
								vStr = filter.rightColumn();
								// vBool = false;
								// System.out.println("VBool set to false");
							}

							try
							{
								children.get(1).getCols2Pos().get(vStr);
							}
							catch (final Exception e)
							{
								vStr = filter.leftColumn();
								// vBool = !vBool;
								// pos =
								// children.get(1).getCols2Pos().get(vStr);
							}

							final ArrayList<String> retval = new ArrayList<String>(1);
							retval.add(vStr);
							return retval;
						}
					}
					else if (filter.op().equals("E") && filter.leftIsColumn() && filter.rightIsColumn())
					{
						return null;
					}
				}
			}
		}

		return null;
	}

	public ArrayList<Boolean> sortOrders()
	{
		for (final HashMap<Filter, Filter> filters : f)
		{
			if (filters.size() == 1)
			{
				for (final Filter filter : filters.keySet())
				{
					if (filter.op().equals("G") || filter.op().equals("GE") || filter.op().equals("L") || filter.op().equals("LE"))
					{
						if (filter.leftIsColumn() && filter.rightIsColumn())
						{
							String vStr;
							boolean vBool;
							if (filter.op().equals("G") || filter.op().equals("GE"))
							{
								vStr = filter.rightColumn();
								vBool = true;
								// System.out.println("VBool set to true");
							}
							else
							{
								vStr = filter.rightColumn();
								vBool = false;
								// System.out.println("VBool set to false");
							}

							try
							{
								children.get(1).getCols2Pos().get(vStr);
							}
							catch (final Exception e)
							{
								// vStr = filter.leftColumn();
								vBool = !vBool;
								// pos =
								// children.get(1).getCols2Pos().get(vStr);
							}

							final ArrayList<Boolean> retval = new ArrayList<Boolean>(1);
							retval.add(vBool);
							return retval;
						}
					}
					else if (filter.op().equals("E") && filter.leftIsColumn() && filter.rightIsColumn())
					{
						return null;
					}
				}
			}
		}

		return null;
	}

	@Override
	public void start() throws Exception
	{
		boolean usesHash = usesHash();
		boolean usesSort = usesSort();

		if (!usesHash && !usesSort)
		{
			dynamicOp = new ProductOperator(meta);
			((ProductOperator)dynamicOp).setTXNum(txnum);
			Operator left = children.get(0);
			Operator right = children.get(1);
			removeChild(left);
			removeChild(right);
			if (left instanceof TableScanOperator)
			{
				((TableScanOperator)left).rebuild();
			}

			if (right instanceof TableScanOperator)
			{
				((TableScanOperator)right).rebuild();
			}
			dynamicOp.add(left);
			dynamicOp.add(right);
			if (left instanceof TableScanOperator)
			{
				((TableScanOperator)left).setCNFForParent(dynamicOp, ((TableScanOperator)left).getCNFForParent(this));
			}
			if (right instanceof TableScanOperator)
			{
				((TableScanOperator)right).setCNFForParent(dynamicOp, ((TableScanOperator)right).getCNFForParent(this));
			}
			((ProductOperator)dynamicOp).setHSHM(f);
			((ProductOperator)dynamicOp).setRightChildCard(rightChildCard, leftChildCard);
			dynamicOp.start();
		}
		else if (usesHash)
		{
			ArrayList<String> lefts = this.getJoinForChild(children.get(0));
			ArrayList<String> rights = this.getJoinForChild(children.get(1));
			dynamicOp = new HashJoinOperator(lefts.get(0), rights.get(0), meta);
			((HashJoinOperator)dynamicOp).setTXNum(txnum);
			if (lefts.size() > 1)
			{
				int i = 1;
				while (i < lefts.size())
				{
					((HashJoinOperator)dynamicOp).addJoinCondition(lefts.get(i), rights.get(i));
					i++;
				}
			}
			Operator left = children.get(0);
			Operator right = children.get(1);
			removeChild(left);
			removeChild(right);
			if (left instanceof TableScanOperator)
			{
				((TableScanOperator)left).rebuild();
			}

			if (right instanceof TableScanOperator)
			{
				((TableScanOperator)right).rebuild();
			}
			dynamicOp.add(left);
			dynamicOp.add(right);
			if (left instanceof TableScanOperator)
			{
				((TableScanOperator)left).setCNFForParent(dynamicOp, ((TableScanOperator)left).getCNFForParent(this));
			}
			if (right instanceof TableScanOperator)
			{
				((TableScanOperator)right).setCNFForParent(dynamicOp, ((TableScanOperator)right).getCNFForParent(this));
			}
			((HashJoinOperator)dynamicOp).setCNF(f);
			((HashJoinOperator)dynamicOp).setRightChildCard(rightChildCard, leftChildCard);

			dynamicOp.start();
		}
		else
		{
			dynamicOp = new ProductOperator(meta);
			((ProductOperator)dynamicOp).setTXNum(txnum);
			Operator left = children.get(0);
			Operator right = children.get(1);
			removeChild(left);
			removeChild(right);
			if (left instanceof TableScanOperator)
			{
				((TableScanOperator)left).rebuild();
			}

			if (right instanceof TableScanOperator)
			{
				((TableScanOperator)right).rebuild();
			}
			dynamicOp.add(left);
			dynamicOp.add(right);
			if (left instanceof TableScanOperator)
			{
				((TableScanOperator)left).setCNFForParent(dynamicOp, ((TableScanOperator)left).getCNFForParent(this));
			}
			if (right instanceof TableScanOperator)
			{
				((TableScanOperator)right).setCNFForParent(dynamicOp, ((TableScanOperator)right).getCNFForParent(this));
			}
			((ProductOperator)dynamicOp).setHSHM(f);
			((ProductOperator)dynamicOp).setRightChildCard(rightChildCard, leftChildCard);
			// ((ProductOperator)dynamicOp).setSort(sortKeys(), sortOrders());
			// //No sort for now
			dynamicOp.start();
		}
	}

	@Override
	public String toString()
	{
		return "NestedLoopJoinOperator";
	}

	public boolean usesHash()
	{
		for (final HashMap<Filter, Filter> filters : f)
		{
			if (filters.size() == 1)
			{
				for (final Filter filter : filters.keySet())
				{
					if (filter.op().equals("E") && filter.leftIsColumn() && filter.rightIsColumn())
					{
						return true;
					}
				}
			}
		}

		return false;
	}

	public boolean usesSort()
	{
		boolean isSort = false;

		for (final HashMap<Filter, Filter> filters : f)
		{
			if (filters.size() == 1)
			{
				for (final Filter filter : filters.keySet())
				{
					if (filter.op().equals("G") || filter.op().equals("GE") || filter.op().equals("L") || filter.op().equals("LE"))
					{
						if (filter.leftIsColumn() && filter.rightIsColumn())
						{
							isSort = true;
						}
					}
					else if (filter.op().equals("E") && filter.leftIsColumn() && filter.rightIsColumn())
					{
						return false;
					}
				}
			}
		}

		return isSort;
	}
}
