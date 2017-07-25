package com.exascale.optimizer;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import com.exascale.misc.DataEndMarker;
import com.exascale.misc.HrdbmsType;
import com.exascale.tables.Plan;
import com.exascale.tables.Transaction;

public final class SelectOperator implements Operator, Cloneable, Serializable
{
	private static int HASH_THRESHOLD = 10;
	private static sun.misc.Unsafe unsafe;

	static
	{
		try
		{
			final Field f = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
			f.setAccessible(true);
			unsafe = (sun.misc.Unsafe)f.get(null);
		}
		catch (final Exception e)
		{
			unsafe = null;
		}
	}
	private transient final MetaData meta;
	private List<Filter> filters;
	private Operator child = null;
	private Operator parent;
	private Map<String, String> cols2Types;
	private Map<String, Integer> cols2Pos;
	private Map<Integer, String> pos2Col;
	// private AtomicLong passed = new AtomicLong(0);
	// private AtomicLong total = new AtomicLong(0);
	private List<String> references = new ArrayList<String>();
	private int node;
	private boolean hash = false;
	boolean always = false;

	private Set<Object> hashSet;

	private String hashCol = null;

	private int hashPos;
	private transient AtomicLong received;
	private transient volatile boolean demReceived;

	public SelectOperator(final List<Filter> filters, final MetaData meta)
	{
		this.filters = filters;
		this.meta = meta;
		received = new AtomicLong(0);

		boolean leftIsAllCol = true;
		boolean leftIsAllLiteral = true;
		boolean rightIsAllCol = true;
		boolean rightIsAllLiteral = true;
		boolean allEqual = true;
		boolean leftAllSameCol = true;
		boolean rightAllSameCol = true;

		for (final Filter filter : filters)
		{
			if (filter.leftIsColumn() && !references.contains(filter.leftColumn()))
			{
				references.add(filter.leftColumn());
			}

			if (filter.rightIsColumn() && !references.contains(filter.rightColumn()))
			{
				references.add(filter.rightColumn());
			}

			if (filter.leftIsColumn())
			{
				leftIsAllLiteral = false;
				if (hashCol == null)
				{
					hashCol = filter.leftColumn();
				}
				else
				{
					if (!filter.leftColumn().equals(hashCol))
					{
						leftAllSameCol = false;
					}
				}
			}
			else
			{
				leftIsAllCol = false;
			}

			if (filter.rightIsColumn())
			{
				rightIsAllLiteral = false;
				if (hashCol == null)
				{
					hashCol = filter.rightColumn();
				}
				else
				{
					if (!filter.rightColumn().equals(hashCol))
					{
						rightAllSameCol = false;
					}
				}
			}
			else
			{
				rightIsAllCol = false;
			}

			if (!filter.op().equals("E"))
			{
				allEqual = false;
			}

			if (filter.alwaysTrue())
			{
				always = true;
			}
		}

		if (!always && filters.size() > HASH_THRESHOLD && allEqual && leftIsAllCol && rightIsAllLiteral && leftAllSameCol)
		{
			hash = true;
			hashSet = new HashSet<Object>();
			for (final Filter filter : filters)
			{
				Object obj = filter.rightLiteral();
				if (obj instanceof Long)
				{
					obj = new Double((Long)obj);
				}

				hashSet.add(obj);
			}
		}
		else if (!always && filters.size() > HASH_THRESHOLD && allEqual && leftIsAllLiteral && rightIsAllCol && rightAllSameCol)
		{
			hash = true;
			hashSet = new HashSet<Object>();
			for (final Filter filter : filters)
			{
				Object obj = filter.leftLiteral();
				if (obj instanceof Long)
				{
					obj = new Double((Long)obj);
				}

				hashSet.add(obj);
			}
		}
	}

	public SelectOperator(final Filter filter, final MetaData meta)
	{
		this.meta = meta;
		this.filters = new ArrayList<Filter>();
		this.filters.add(filter);

		if (filter.leftIsColumn() && !references.contains(filter.leftColumn()))
		{
			references.add(filter.leftColumn());
		}

		if (filter.rightIsColumn() && !references.contains(filter.rightColumn()))
		{
			references.add(filter.rightColumn());
		}

		received = new AtomicLong(0);
	}

	public static SelectOperator deserialize(final InputStream in, final Map<Long, Object> prev) throws Exception
	{
		final SelectOperator value = (SelectOperator)unsafe.allocateInstance(SelectOperator.class);
		prev.put(OperatorUtils.readLong(in), value);
		value.filters = OperatorUtils.deserializeALF(in, prev);
		value.child = OperatorUtils.deserializeOperator(in, prev);
		value.parent = OperatorUtils.deserializeOperator(in, prev);
		value.cols2Types = OperatorUtils.deserializeStringHM(in, prev);
		value.cols2Pos = OperatorUtils.deserializeStringIntHM(in, prev);
		value.pos2Col = OperatorUtils.deserializeTM(in, prev);
		value.references = OperatorUtils.deserializeALS(in, prev);
		value.node = OperatorUtils.readInt(in);
		value.hash = OperatorUtils.readBool(in);
		value.always = OperatorUtils.readBool(in);
		value.hashSet = OperatorUtils.deserializeHSO(in, prev);
		value.hashCol = OperatorUtils.readString(in, prev);
		value.hashPos = OperatorUtils.readInt(in);
		value.received = new AtomicLong(0);
		value.demReceived = false;
		return value;
	}

	@Override
	public void add(final Operator op) throws Exception
	{
		if (child == null)
		{
			child = op;
			op.registerParent(this);
			cols2Types = child.getCols2Types();
			cols2Pos = child.getCols2Pos();
			pos2Col = child.getPos2Col();

			if (hash)
			{
				hashPos = cols2Pos.get(hashCol);
			}
		}
		else
		{
			throw new Exception("SelectOperator only supports 1 child.");
		}
	}

	@Override
	public List<Operator> children()
	{
		final List<Operator> retval = new ArrayList<Operator>(1);
		retval.add(child);
		return retval;
	}

	@Override
	public SelectOperator clone()
	{
		final List<Filter> filtersDeepClone = new ArrayList<Filter>(filters.size());
		for (final Filter f : filters)
		{
			filtersDeepClone.add(f.clone());
		}
		final SelectOperator retval = new SelectOperator(filtersDeepClone, meta);
		retval.node = node;
		return retval;
	}

	@Override
	public void close() throws Exception
	{
		cols2Pos = null;
		cols2Types = null;
		pos2Col = null;
		filters = null;
		references = null;
		hashSet = null;
		child.close();
	}

	@Override
	public int getChildPos()
	{
		return 0;
	}

	@Override
	public Map<String, Integer> getCols2Pos()
	{
		return cols2Pos;
	}

	@Override
	public Map<String, String> getCols2Types()
	{
		return cols2Types;
	}

	public List<Filter> getFilter()
	{
		return filters;
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
	public Map<Integer, String> getPos2Col()
	{
		return pos2Col;
	}

	@Override
	public List<String> getReferences()
	{
		return references;
	}

	public double likelihood(final RootOperator op, final Transaction tx) throws Exception
	{
		return meta.likelihood(new ArrayList<Filter>(filters), op, tx, this);
	}

	public double likelihood(final Transaction tx) throws Exception
	{
		return meta.likelihood(new ArrayList<Filter>(filters), tx, this);
	}

	// @?Parallel
	@Override
	public Object next(final Operator op) throws Exception
	{
		Object o = child.next(this);
		if (o instanceof DataEndMarker)
		{
			demReceived = true;
		}
		else
		{
			received.getAndIncrement();
		}
		// total.getAndIncrement();
		while (!(o instanceof DataEndMarker))
		{
			if (o instanceof Exception)
			{
				throw (Exception)o;
			}

			if (always)
			{
				// passed.getAndIncrement();
				return o;
			}

			if (hash)
			{
				final List<Object> row = (List<Object>)o;
				Object obj = row.get(hashPos);
				if (obj instanceof Long)
				{
					obj = new Double((Long)obj);
				}

				if (hashSet.contains(obj))
				{
					// passed.getAndIncrement();
					return o;
				}
			}
			else
			{
				int z = 0;
				final int limit = filters.size();
				// for (final Filter filter : filters)
				while (z < limit)
				{
					final Filter filter = filters.get(z++);
					if (filter.passes((List<Object>)o, cols2Pos))
					{
						// passed.getAndIncrement();
						return o;
					}
				}
			}

			o = child.next(this);
			if (o instanceof DataEndMarker)
			{
				demReceived = true;
			}
			else
			{
				received.getAndIncrement();
			}
			// total.getAndIncrement();
		}

		// System.out.println("Select operator returned " + passed + "/" + total
		// + " records");
		return o;
	}

	@Override
	public void nextAll(final Operator op) throws Exception
	{
		child.nextAll(op);
		Object o = next(op);
		while (!(o instanceof DataEndMarker) && !(o instanceof Exception))
		{
			o = next(op);
		}
	}

	@Override
	public long numRecsReceived()
	{
		return received.get();
	}

	@Override
	public Operator parent()
	{
		return parent;
	}

	@Override
	public boolean receivedDEM()
	{
		return demReceived;
	}

	@Override
	public void registerParent(final Operator op) throws Exception
	{
		if (parent == null)
		{
			parent = op;
		}
		else
		{
			throw new Exception("SelectOperator only supports 1 parent.");
		}
	}

	@Override
	public void removeChild(final Operator op)
	{
		if (op == child)
		{
			child = null;
			op.removeParent(this);
		}
	}

	@Override
	public void removeParent(final Operator op)
	{
		parent = null;
	}

	@Override
	public void reset() throws Exception
	{
		child.reset();
		// passed = new AtomicLong(0);
		// total = new AtomicLong(0);
	}

	@Override
	public void serialize(final OutputStream out, final IdentityHashMap<Object, Long> prev) throws Exception
	{
		final Long id = prev.get(this);
		if (id != null)
		{
			OperatorUtils.serializeReference(id, out);
			return;
		}

		OperatorUtils.writeType(HrdbmsType.SELECT, out);
		prev.put(this, OperatorUtils.writeID(out));
		OperatorUtils.serializeALF(filters, out, prev);
		child.serialize(out, prev);
		parent.serialize(out, prev);
		OperatorUtils.serializeStringHM(cols2Types, out, prev);
		OperatorUtils.serializeStringIntHM(cols2Pos, out, prev);
		OperatorUtils.serializeTM(pos2Col, out, prev);
		OperatorUtils.serializeALS(references, out, prev);
		OperatorUtils.writeInt(node, out);
		OperatorUtils.writeBool(hash, out);
		OperatorUtils.writeBool(always, out);
		OperatorUtils.serializeHSO(hashSet, out, prev);
		OperatorUtils.writeString(hashCol, out, prev);
		OperatorUtils.writeInt(hashPos, out);
	}

	@Override
	public void setChildPos(final int pos)
	{
	}

	@Override
	public void setNode(final int node)
	{
		this.node = node;
	}

	@Override
	public void setPlan(final Plan plan)
	{
	}

	@Override
	public void start() throws Exception
	{
		child.start();
	}

	@Override
	public String toString()
	{
		String retval = "SelectOperator(";
		for (final Filter f : filters)
		{
			retval += (f.toString() + "\t");
		}
		retval += ")";
		return retval;
	}

	public void updateReferences()
	{
		references.clear();
		for (final Filter filter : filters)
		{
			if (filter.leftIsColumn() && !references.contains(filter.leftColumn()))
			{
				references.add(filter.leftColumn());
			}

			if (filter.rightIsColumn() && !references.contains(filter.rightColumn()))
			{
				references.add(filter.rightColumn());
			}
		}
	}
}
