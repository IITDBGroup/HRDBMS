package com.exascale.optimizer;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.misc.*;
import com.exascale.tables.Plan;

public final class CaseOperator implements Operator, Serializable
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
		catch (final Exception e)
		{
			unsafe = null;
		}
	}
	private transient final MetaData meta;
	private List<Set<Map<Filter, Filter>>> filters;
	private List<Object> results;
	private Operator child = null;
	private Operator parent;
	private Map<String, String> cols2Types;
	private Map<String, Integer> cols2Pos;
	private Map<Integer, String> pos2Col;
	private String name;
	private String type;
	// private MySimpleDateFormat sdf = new MySimpleDateFormat("yyyy-MM-dd");
	private List<String> origResults;

	private List<String> references = new ArrayList<String>();

	private int node;
	private transient AtomicLong received;
	private transient volatile boolean demReceived;

	public CaseOperator(final List<Set<Map<Filter, Filter>>> filters, final List<String> results, final String name, final String type, final MetaData meta)
	{
		received = new AtomicLong(0);
		this.filters = filters;
		this.name = name;
		this.type = type;
		this.meta = meta;
		this.origResults = results;

		this.results = new ArrayList<Object>(results.size());

		for (final Set<Map<Filter, Filter>> filter : filters)
		{
			for (final Map<Filter, Filter> f : filter)
			{
				for (final Filter f2 : f.keySet())
				{
					if (f2.leftIsColumn())
					{
						references.add(f2.leftColumn());
					}

					if (f2.rightIsColumn())
					{
						references.add(f2.rightColumn());
					}
				}
			}
		}

		for (String val1 : results)
		{
			if (val1.startsWith("DATE('"))
			{
				String temp = val1.substring(6);
				// FastStringTokenizer tokens = new FastStringTokenizer(temp,
				// "'", false);
				final FastStringTokenizer tokens = new FastStringTokenizer(temp, "'", false);
				temp = tokens.nextToken();
				this.results.add(DateParser.parse(temp));
			}
			else if (val1.startsWith("'"))
			{
				val1 = val1.substring(1);
				val1 = val1.substring(0, val1.length() - 1);
				this.results.add(val1);
			}
			else if ((val1.charAt(0) >= '0' && val1.charAt(0) <= '9') || val1.charAt(0) == '-')
			{
				if (val1.contains("."))
				{
					this.results.add(Double.parseDouble(val1));
				}
				else
				{
					this.results.add(Utils.parseLong(val1));
				}
			}
			else
			{
				references.add(val1);
				this.results.add("\u0000" + val1);
			}
		}
	}

	public static CaseOperator deserialize(final InputStream in, final Map<Long, Object> prev) throws Exception
	{
		final CaseOperator value = (CaseOperator)unsafe.allocateInstance(CaseOperator.class);
		prev.put(OperatorUtils.readLong(in), value);
		value.filters = OperatorUtils.deserializeALHSHM(in, prev);
		value.results = OperatorUtils.deserializeALO(in, prev);
		value.child = OperatorUtils.deserializeOperator(in, prev);
		value.parent = OperatorUtils.deserializeOperator(in, prev);
		value.cols2Types = OperatorUtils.deserializeStringHM(in, prev);
		value.cols2Pos = OperatorUtils.deserializeStringIntHM(in, prev);
		value.pos2Col = OperatorUtils.deserializeTM(in, prev);
		value.name = OperatorUtils.readString(in, prev);
		value.type = OperatorUtils.readString(in, prev);
		value.origResults = OperatorUtils.deserializeALS(in, prev);
		value.references = OperatorUtils.deserializeALS(in, prev);
		value.node = OperatorUtils.readInt(in);
		value.received = new AtomicLong(0);
		value.demReceived = false;
		return value;
	}

	private static boolean passesCase(final List<Object> row, final Map<String, Integer> cols2Pos, final Set<Map<Filter, Filter>> ands) throws Exception
	{
		for (final Map<Filter, Filter> ors : ands)
		{
			if (!passesOredCondition(row, cols2Pos, ors))
			{
				return false;
			}
		}

		return true;
	}

	private static boolean passesOredCondition(final List<Object> row, final Map<String, Integer> cols2Pos, final Map<Filter, Filter> filters) throws Exception
	{
		try
		{
			for (final Map.Entry entry : filters.entrySet())
			{
				if (((Filter)entry.getKey()).passes(row, cols2Pos))
				{
					return true;
				}
			}
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.error("", e);
			throw e;
		}

		return false;
	}

	@Override
	public void add(final Operator op) throws Exception
	{
		if (child == null)
		{
			child = op;
			child.registerParent(this);
			if (child.getCols2Types() != null)
			{
				cols2Types = new HashMap<>(child.getCols2Types());
				cols2Types.put(name, type);
				cols2Pos = new HashMap<>(child.getCols2Pos());
				cols2Pos.put(name, cols2Pos.size());
				pos2Col = new HashMap<>(child.getPos2Col());
				pos2Col.put(pos2Col.size(), name);
			}
		}
		else
		{
			throw new Exception("CaseOperator only supports 1 child.");
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
	public CaseOperator clone()
	{
		final CaseOperator retval = new CaseOperator(filters, origResults, name, type, meta);
		retval.node = node;
		return retval;
	}

	@Override
	public void close() throws Exception
	{
		filters = null;
		results = null;
		origResults = null;
		references = null;
		cols2Pos = null;
		cols2Types = null;
		pos2Col = null;
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

	public String getOutputCol()
	{
		return name;
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

	public List<Object> getResults()
	{
		return results;
	}

	public String getType()
	{
		return type;
	}

	@Override
	public Object next(final Operator op) throws Exception
	{
		final Object o = child.next(this);

		if (o instanceof DataEndMarker)
		{
			demReceived = true;
		}
		else
		{
			received.getAndIncrement();
		}

		if (!(o instanceof DataEndMarker) && !(o instanceof Exception))
		{
			int i = 0;
			int z = 0;
			final int limit = filters.size();
			// for (final Set<Map<Filter, Filter>> aCase : filters)
			while (z <= limit)
			{
				if (z == limit || passesCase((List<Object>)o, cols2Pos, filters.get(z++)))
				{
					Object obj = results.get(i);
					if (obj instanceof String && ((String)obj).startsWith("\u0000"))
					{
						obj = getColumn(((String)obj).substring(1), (List<Object>)o);
					}

					if (obj instanceof Integer && type.equals("LONG"))
					{
						obj = new Long((Integer)obj);
					}
					else if (obj instanceof Integer && type.equals("FLOAT"))
					{
						obj = new Double((Integer)obj);
					}
					else if (obj instanceof Long && type.equals("FLOAT"))
					{
						obj = new Double((Long)obj);
					}

					((List<Object>)o).add(obj);
					return o;
				}
				i++;
			}

			return null;
		}
		else if (o instanceof Exception)
		{
			throw (Exception)o;
		}

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
			throw new Exception("CaseOperator only supports 1 parent.");
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

		OperatorUtils.writeType(HrdbmsType.CASE, out);
		prev.put(this, OperatorUtils.writeID(out));
		OperatorUtils.serializeALHSHM(filters, out, prev);
		OperatorUtils.serializeALO(results, out, prev);
		child.serialize(out, prev);
		parent.serialize(out, prev);
		OperatorUtils.serializeStringHM(cols2Types, out, prev);
		OperatorUtils.serializeStringIntHM(cols2Pos, out, prev);
		OperatorUtils.serializeTM(pos2Col, out, prev);
		OperatorUtils.writeString(name, out, prev);
		OperatorUtils.writeString(type, out, prev);
		OperatorUtils.serializeALS(origResults, out, prev);
		OperatorUtils.serializeALS(references, out, prev);
		OperatorUtils.writeInt(node, out);
	}

	@Override
	public void setChildPos(final int pos)
	{
	}

	public void setFilters(final List<Set<Map<Filter, Filter>>> filters)
	{
		this.filters = filters;

		for (final Set<Map<Filter, Filter>> filter : filters)
		{
			for (final Map<Filter, Filter> f : filter)
			{
				for (final Filter f2 : f.keySet())
				{
					if (f2.leftIsColumn())
					{
						references.add(f2.leftColumn());
					}

					if (f2.rightIsColumn())
					{
						references.add(f2.rightColumn());
					}
				}
			}
		}
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

	public void setType(final String type)
	{
		this.type = type;
		if (cols2Types != null)
		{
			cols2Types.put(name, type);
		}
	}

	@Override
	public void start() throws Exception
	{
		child.start();
	}

	@Override
	public String toString()
	{
		return "CaseOperator: filters = " + filters + " results = " + origResults;
	}

	private Object getColumn(String name, final List<Object> row) throws Exception
	{
		Integer pos = cols2Pos.get(name);
		if (pos != null)
		{
			return row.get(pos);
		}

		if (name.indexOf('.') > 0)
		{
			throw new Exception("Column " + name + " not found in CaseOperator");
		}

		if (name.startsWith("."))
		{
			name = name.substring(1);
		}

		int matches = 0;
		for (final Map.Entry entry : cols2Pos.entrySet())
		{
			String name2 = (String)entry.getKey();
			if (name2.contains("."))
			{
				name2 = name2.substring(name2.indexOf('.') + 1);
			}

			if (name.equals(name2))
			{
				matches++;
				pos = (Integer)entry.getValue();
			}
		}

		if (matches == 0)
		{
			throw new Exception("Column " + name + " not found in CaseOperator");
		}

		if (matches > 1)
		{
			throw new Exception("Column " + name + " is ambiguous in CaseOperator");
		}

		return row.get(pos);
	}
}
