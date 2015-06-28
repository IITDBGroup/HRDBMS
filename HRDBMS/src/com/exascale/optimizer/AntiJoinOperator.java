package com.exascale.optimizer;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.Vector;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.managers.ResourceManager;
import com.exascale.misc.BufferedLinkedBlockingQueue;
import com.exascale.misc.DataEndMarker;
import com.exascale.misc.MurmurHash;
import com.exascale.misc.MyDate;
import com.exascale.misc.MySimpleDateFormat;
import com.exascale.tables.Plan;
import com.exascale.threads.ThreadPoolThread;

public final class AntiJoinOperator implements Operator, Serializable
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
	private ArrayList<String> cols;
	private volatile ArrayList<Integer> poses;
	int childPos = -1;
	private HashSet<HashMap<Filter, Filter>> f = null;
	private int node;
	private boolean indexAccess = false;
	private ArrayList<Index> dynamicIndexes;
	private int rightChildCard = 16;
	private boolean alreadySorted = false;
	private boolean cardSet = false;
	private transient Operator dynamicOp;

	public AntiJoinOperator(ArrayList<String> cols, MetaData meta)
	{
		this.cols = cols;
		this.meta = meta;
	}

	public AntiJoinOperator(HashSet<HashMap<Filter, Filter>> f, MetaData meta)
	{
		this.f = f;
		this.meta = meta;
		this.cols = new ArrayList<String>(0);
	}

	public AntiJoinOperator(String col, MetaData meta)
	{
		this.cols = new ArrayList<String>(1);
		this.cols.add(col);
		this.meta = meta;
	}

	private AntiJoinOperator(ArrayList<String> cols, HashSet<HashMap<Filter, Filter>> f, MetaData meta)
	{
		this.cols = cols;
		this.f = f;
		this.meta = meta;
	}

	public static AntiJoinOperator deserialize(InputStream in, HashMap<Long, Object> prev) throws Exception
	{
		AntiJoinOperator value = (AntiJoinOperator)unsafe.allocateInstance(AntiJoinOperator.class);
		prev.put(OperatorUtils.readLong(in), value);
		value.children = OperatorUtils.deserializeALOp(in, prev);
		value.parent = OperatorUtils.deserializeOperator(in, prev);
		value.cols2Types = OperatorUtils.deserializeStringHM(in, prev);
		value.cols2Pos = OperatorUtils.deserializeStringIntHM(in, prev);
		value.pos2Col = OperatorUtils.deserializeTM(in, prev);
		value.meta = new MetaData();
		value.cols = OperatorUtils.deserializeALS(in, prev);
		value.poses = OperatorUtils.deserializeALI(in, prev);
		value.childPos = OperatorUtils.readInt(in);
		value.f = OperatorUtils.deserializeHSHM(in, prev);
		value.node = OperatorUtils.readInt(in);
		value.indexAccess = OperatorUtils.readBool(in);
		value.dynamicIndexes = OperatorUtils.deserializeALIndx(in, prev);
		value.rightChildCard = OperatorUtils.readInt(in);
		value.alreadySorted = OperatorUtils.readBool(in);
		value.cardSet = OperatorUtils.readBool(in);
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
				cols2Types = children.get(0).getCols2Types();
				cols2Pos = children.get(0).getCols2Pos();
				pos2Col = children.get(0).getPos2Col();

				poses = new ArrayList<Integer>(cols.size());
				for (final String col : cols)
				{
					poses.add(cols2Pos.get(col));
				}
			}
		}
		else
		{
			throw new Exception("AntiJoinOperator only supports 2 children");
		}
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
	public AntiJoinOperator clone()
	{
		final AntiJoinOperator retval = new AntiJoinOperator(cols, f, meta);
		retval.node = node;
		retval.indexAccess = indexAccess;
		retval.dynamicIndexes = dynamicIndexes;
		retval.alreadySorted = alreadySorted;
		retval.rightChildCard = rightChildCard;
		retval.cardSet = cardSet;
		return retval;
	}

	@Override
	public void close() throws Exception
	{
		dynamicOp.close();
		cols2Types = null;
		cols2Pos = null;
		f = null;
		dynamicIndexes = null;
		pos2Col = null;
		cols = null;
		poses = null;
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

	public HashSet<HashMap<Filter, Filter>> getHSHM() throws Exception
	{
		if (f != null)
		{
			return f;
		}

		final HashSet<HashMap<Filter, Filter>> retval = new HashSet<HashMap<Filter, Filter>>();
		int i = 0;
		for (final String col : children.get(1).getPos2Col().values())
		{
			Filter filter = null;
			try
			{
				filter = new Filter(cols.get(i), "E", col);
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				throw e;
			}
			final HashMap<Filter, Filter> hm = new HashMap<Filter, Filter>();
			hm.put(filter, filter);
			retval.add(hm);
			i++;
		}

		return retval;
	}

	public boolean getIndexAccess()
	{
		return indexAccess;
	}

	public ArrayList<String> getJoinForChild(Operator op)
	{
		if (cols.size() > 0)
		{
			if (op.getCols2Pos().keySet().containsAll(cols))
			{
				return new ArrayList<String>(cols);
			}
			else
			{
				return new ArrayList<String>(op.getCols2Pos().keySet());
			}
		}

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

	public ArrayList<String> getLefts()
	{
		if (cols.size() > 0)
		{
			return cols;
		}

		final ArrayList<String> retval = new ArrayList<String>(f.size());
		for (final HashMap<Filter, Filter> filters : f)
		{
			if (filters.size() == 1)
			{
				for (final Filter filter : filters.keySet())
				{
					if (filter.op().equals("E"))
					{
						if (children.get(0).getCols2Pos().keySet().contains(filter.leftColumn()))
						{
							retval.add(filter.leftColumn());
						}
						else
						{
							retval.add(filter.rightColumn());
						}
					}
				}
			}
		}

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
		final ArrayList<String> retval = new ArrayList<String>(cols);
		retval.addAll(children.get(1).getCols2Pos().keySet());

		if (f != null)
		{
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
		}
		return retval;
	}

	public ArrayList<String> getRights()
	{
		if (cols.size() > 0)
		{
			final ArrayList<String> retval = new ArrayList<String>(children.get(1).getCols2Pos().keySet().size());
			retval.addAll(children.get(1).getCols2Pos().keySet());
			return retval;
		}

		final ArrayList<String> retval = new ArrayList<String>(f.size());
		for (final HashMap<Filter, Filter> filters : f)
		{
			if (filters.size() == 1)
			{
				for (final Filter filter : filters.keySet())
				{
					if (filter.op().equals("E"))
					{
						if (children.get(1).getCols2Pos().keySet().contains(filter.leftColumn()))
						{
							retval.add(filter.leftColumn());
						}
						else
						{
							retval.add(filter.rightColumn());
						}
					}
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
	public Operator parent()
	{
		return parent;
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
			throw new Exception("AntiJoinOperator can only have 1 parent.");
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
		HRDBMSWorker.logger.error("AntiJoinOperator cannot be reset");
		throw new Exception("AntiJoinOperator cannot be reset");
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

		OperatorUtils.writeType(1, out);
		prev.put(this, OperatorUtils.writeID(out));
		OperatorUtils.serializeALOp(children, out, prev);
		parent.serialize(out, prev);
		OperatorUtils.serializeStringHM(cols2Types, out, prev);
		OperatorUtils.serializeStringIntHM(cols2Pos, out, prev);
		OperatorUtils.serializeTM(pos2Col, out, prev);
		// create new meta on receive side
		OperatorUtils.serializeALS(cols, out, prev);
		OperatorUtils.serializeALI(poses, out, prev);
		OperatorUtils.writeInt(childPos, out);
		OperatorUtils.serializeHSHM(f, out, prev);
		OperatorUtils.writeInt(node, out);
		OperatorUtils.writeBool(indexAccess, out);
		OperatorUtils.serializeALIndx(dynamicIndexes, out, prev);
		OperatorUtils.writeInt(rightChildCard, out);
		OperatorUtils.writeBool(alreadySorted, out);
		OperatorUtils.writeBool(cardSet, out);
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

	public boolean setRightChildCard(int card)
	{
		if (cardSet)
		{
			return false;
		}

		cardSet = true;
		rightChildCard = card;
		return true;
	}

	public ArrayList<String> sortKeys()
	{
		if (cols.size() > 0)
		{
			return null;
		}

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
		if (cols.size() > 0)
		{
			return null;
		}

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
		
		if (usesHash)
		{
			//HJO w/ existence + filter
			ArrayList<String> lefts = this.getJoinForChild(children.get(0));
			ArrayList<String> rights = this.getJoinForChild(children.get(1));
			dynamicOp = new HashJoinOperator(lefts.get(0), rights.get(0), meta);
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
			dynamicOp.add(left);
			dynamicOp.add(right);
			((HashJoinOperator)dynamicOp).setCNF(getHSHM());
			((HashJoinOperator)dynamicOp).setRightChildCard(rightChildCard);
			((HashJoinOperator)dynamicOp).setAnti();
			dynamicOp.start();
		}
		else if (usesSort)
		{
			//not implemented - add sort to the below
			//prod w/ existence + filter + remove from left
			dynamicOp = new ProductOperator(meta);
			Operator left = children.get(0);
			Operator right = children.get(1);
			removeChild(left);
			removeChild(right);
			dynamicOp.add(left);
			dynamicOp.add(right);
			((ProductOperator)dynamicOp).setHSHM(getHSHM());
			((ProductOperator)dynamicOp).setRightChildCard(rightChildCard);
			((ProductOperator)dynamicOp).setAnti();
			dynamicOp.start();
		}
		else
		{
			//prod w/ existence + filter + remove from left
			dynamicOp = new ProductOperator(meta);
			Operator left = children.get(0);
			Operator right = children.get(1);
			removeChild(left);
			removeChild(right);
			dynamicOp.add(left);
			dynamicOp.add(right);
			((ProductOperator)dynamicOp).setHSHM(getHSHM());
			((ProductOperator)dynamicOp).setRightChildCard(rightChildCard);
			((ProductOperator)dynamicOp).setAnti();
		}
	}

	@Override
	public String toString()
	{
		return "AntiJoinOperator";
	}

	public boolean usesHash()
	{
		// System.out.println("In AntiJoin.usesHash() with " + f);
		if (cols.size() > 0)
		{
			return true;
		}

		for (final HashMap<Filter, Filter> filters : f)
		{
			// System.out.println(filters);
			if (filters.size() == 1)
			{
				// System.out.println("Size = 1");
				for (final Filter filter : filters.keySet())
				{
					if (filter.op().equals("E") && filter.leftIsColumn() && filter.rightIsColumn())
					{
						// System.out.println("Uses hash");
						return true;
					}
				}
			}
		}

		return false;
	}

	public boolean usesSort()
	{
		if (cols.size() > 0)
		{
			return false;
		}

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
