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
import java.util.concurrent.atomic.AtomicLong;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.misc.DataEndMarker;
import com.exascale.misc.DateParser;
import com.exascale.misc.FastStringTokenizer;
import com.exascale.misc.Utils;
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
		catch (Exception e)
		{
			unsafe = null;
		}
	}
	private transient final MetaData meta;
	private ArrayList<HashSet<HashMap<Filter, Filter>>> filters;
	private ArrayList<Object> results;
	private Operator child = null;
	private Operator parent;
	private HashMap<String, String> cols2Types;
	private HashMap<String, Integer> cols2Pos;
	private TreeMap<Integer, String> pos2Col;
	private String name;
	private String type;
	// private MySimpleDateFormat sdf = new MySimpleDateFormat("yyyy-MM-dd");
	private ArrayList<String> origResults;

	private ArrayList<String> references = new ArrayList<String>();

	private int node;
	private transient AtomicLong received;
	private transient volatile boolean demReceived;

	public CaseOperator(ArrayList<HashSet<HashMap<Filter, Filter>>> filters, ArrayList<String> results, String name, String type, MetaData meta)
	{
		received = new AtomicLong(0);
		this.filters = filters;
		this.name = name;
		this.type = type;
		this.meta = meta;
		this.origResults = results;

		this.results = new ArrayList<Object>(results.size());

		for (final HashSet<HashMap<Filter, Filter>> filter : filters)
		{
			for (final HashMap<Filter, Filter> f : filter)
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

	public static CaseOperator deserialize(InputStream in, HashMap<Long, Object> prev) throws Exception
	{
		CaseOperator value = (CaseOperator)unsafe.allocateInstance(CaseOperator.class);
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

	@Override
	public void add(Operator op) throws Exception
	{
		if (child == null)
		{
			child = op;
			child.registerParent(this);
			if (child.getCols2Types() != null)
			{
				cols2Types = (HashMap<String, String>)child.getCols2Types().clone();
				cols2Types.put(name, type);
				cols2Pos = (HashMap<String, Integer>)child.getCols2Pos().clone();
				cols2Pos.put(name, cols2Pos.size());
				pos2Col = (TreeMap<Integer, String>)child.getPos2Col().clone();
				pos2Col.put(pos2Col.size(), name);
			}
		}
		else
		{
			throw new Exception("CaseOperator only supports 1 child.");
		}
	}

	@Override
	public ArrayList<Operator> children()
	{
		final ArrayList<Operator> retval = new ArrayList<Operator>(1);
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
	public HashMap<String, Integer> getCols2Pos()
	{
		return cols2Pos;
	}

	@Override
	public HashMap<String, String> getCols2Types()
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
	public TreeMap<Integer, String> getPos2Col()
	{
		return pos2Col;
	}

	@Override
	public ArrayList<String> getReferences()
	{
		return references;
	}

	public ArrayList<Object> getResults()
	{
		return results;
	}

	public String getType()
	{
		return type;
	}

	@Override
	public Object next(Operator op) throws Exception
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
			// for (final HashSet<HashMap<Filter, Filter>> aCase : filters)
			while (z < limit)
			{
				final HashSet<HashMap<Filter, Filter>> aCase = filters.get(z++);
				if (passesCase((ArrayList<Object>)o, cols2Pos, aCase))
				{
					Object obj = results.get(i);
					if (obj instanceof String && ((String)obj).startsWith("\u0000"))
					{
						obj = getColumn(((String)obj).substring(1), (ArrayList<Object>)o);
						if (obj instanceof Integer && type.equals("LONG"))
						{
							((ArrayList<Object>)o).add(new Long((Integer)obj));
						}
						else if (obj instanceof Integer && type.equalsIgnoreCase("FLOAT"))
						{
							((ArrayList<Object>)o).add(new Double((Integer)obj));
						}
						else if (obj instanceof Long && type.equals("FLOAT"))
						{
							((ArrayList<Object>)o).add(new Double((Long)obj));
						}
						else
						{
							((ArrayList<Object>)o).add(obj);
						}
						return o;
					}
					else
					{
						if (obj instanceof Integer && type.equals("LONG"))
						{
							((ArrayList<Object>)o).add(new Long((Integer)obj));
						}
						else if (obj instanceof Integer && type.equalsIgnoreCase("FLOAT"))
						{
							((ArrayList<Object>)o).add(new Double((Integer)obj));
						}
						else if (obj instanceof Long && type.equals("FLOAT"))
						{
							((ArrayList<Object>)o).add(new Double((Long)obj));
						}
						else
						{
							((ArrayList<Object>)o).add(obj);
						}
						return o;
					}
				}
				i++;
			}

			Object obj = results.get(i);
			if (obj instanceof String && ((String)obj).startsWith("\u0000"))
			{
				obj = getColumn(((String)obj).substring(1), (ArrayList<Object>)o);
				if (obj instanceof Integer && type.equals("LONG"))
				{
					((ArrayList<Object>)o).add(new Long((Integer)obj));
				}
				else if (obj instanceof Integer && type.equalsIgnoreCase("FLOAT"))
				{
					((ArrayList<Object>)o).add(new Double((Integer)obj));
				}
				else if (obj instanceof Long && type.equals("FLOAT"))
				{
					((ArrayList<Object>)o).add(new Double((Long)obj));
				}
				else
				{
					((ArrayList<Object>)o).add(obj);
				}
				return o;
			}
			else
			{
				if (obj instanceof Integer && type.equals("LONG"))
				{
					((ArrayList<Object>)o).add(new Long((Integer)obj));
				}
				else if (obj instanceof Integer && type.equalsIgnoreCase("FLOAT"))
				{
					((ArrayList<Object>)o).add(new Double((Integer)obj));
				}
				else if (obj instanceof Long && type.equals("FLOAT"))
				{
					((ArrayList<Object>)o).add(new Double((Long)obj));
				}
				else
				{
					((ArrayList<Object>)o).add(obj);
				}
				return o;
			}
		}
		else if (o instanceof Exception)
		{
			throw (Exception)o;
		}

		return o;
	}

	@Override
	public void nextAll(Operator op) throws Exception
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
	public void registerParent(Operator op) throws Exception
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
	public void removeChild(Operator op)
	{
		if (op == child)
		{
			child = null;
			op.removeParent(this);
		}
	}

	@Override
	public void removeParent(Operator op)
	{
		parent = null;
	}

	@Override
	public void reset() throws Exception
	{
		child.reset();
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

		OperatorUtils.writeType(20, out);
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
	public void setChildPos(int pos)
	{
	}

	public void setFilters(ArrayList<HashSet<HashMap<Filter, Filter>>> filters)
	{
		this.filters = filters;

		for (final HashSet<HashMap<Filter, Filter>> filter : filters)
		{
			for (final HashMap<Filter, Filter> f : filter)
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
	public void setNode(int node)
	{
		this.node = node;
	}

	@Override
	public void setPlan(Plan plan)
	{
	}

	public void setType(String type)
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

	private Object getColumn(String name, ArrayList<Object> row) throws Exception
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
		for (Map.Entry entry : cols2Pos.entrySet())
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

	private boolean passesCase(ArrayList<Object> row, HashMap<String, Integer> cols2Pos, HashSet<HashMap<Filter, Filter>> ands) throws Exception
	{
		for (final HashMap<Filter, Filter> ors : ands)
		{
			if (!passesOredCondition(row, cols2Pos, ors))
			{
				return false;
			}
		}

		return true;
	}

	private boolean passesOredCondition(ArrayList<Object> row, HashMap<String, Integer> cols2Pos, HashMap<Filter, Filter> filters) throws Exception
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
}
