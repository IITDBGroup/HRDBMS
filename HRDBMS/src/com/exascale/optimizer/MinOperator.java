package com.exascale.optimizer;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.*;

import com.exascale.managers.HRDBMSWorker;
import com.exascale.misc.HrdbmsType;
import com.exascale.misc.MyDate;

public final class MinOperator implements AggregateOperator, Serializable
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

	private String input;
	private String output;
	private transient final MetaData meta;
	private boolean isInt;
	private boolean isLong;
	private boolean isFloat;
	private boolean isChar;

	private boolean isDate;

	private long NUM_GROUPS = 16;

	public MinOperator(final String input, final String output, final MetaData meta, final boolean isInt)
	{
		this.input = input;
		this.output = output;
		this.meta = meta;
		if (isInt)
		{
			this.isInt = true;
			this.isLong = false;
			this.isFloat = false;
			this.isChar = false;
			this.isDate = false;
		}
		else
		{
			this.isInt = false;
			this.isLong = false;
			this.isFloat = true;
			this.isChar = false;
			this.isDate = false;
		}
	}

	public static MinOperator deserialize(final InputStream in, final Map<Long, Object> prev) throws Exception
	{
		final MinOperator value = (MinOperator)unsafe.allocateInstance(MinOperator.class);
		prev.put(OperatorUtils.readLong(in), value);
		value.input = OperatorUtils.readString(in, prev);
		value.output = OperatorUtils.readString(in, prev);
		value.NUM_GROUPS = OperatorUtils.readLong(in);
		value.isInt = OperatorUtils.readBool(in);
		value.isLong = OperatorUtils.readBool(in);
		value.isFloat = OperatorUtils.readBool(in);
		value.isChar = OperatorUtils.readBool(in);
		value.isDate = OperatorUtils.readBool(in);
		return value;
	}

	@Override
	public MinOperator clone()
	{
		final MinOperator retval = new MinOperator(input, output, meta, isInt);
		retval.NUM_GROUPS = NUM_GROUPS;
		retval.isInt = isInt;
		retval.isLong = isLong;
		retval.isFloat = isFloat;
		retval.isChar = isChar;
		retval.isDate = isDate;
		return retval;
	}

	@Override
	public AggregateResultThread getHashThread(final Map<String, Integer> cols2Pos) throws Exception
	{
		return new MinHashThread(cols2Pos);
	}

	@Override
	public String getInputColumn()
	{
		return input;
	}

	@Override
	public AggregateResultThread newProcessingThread(final List<List<Object>> rows, final Map<String, Integer> cols2Pos)
	{
		return new MinThread(rows, cols2Pos);
	}

	@Override
	public String outputColumn()
	{
		return output;
	}

	@Override
	public String outputType()
	{
		if (isInt)
		{
			return "INT";
		}
		else if (isLong)
		{
			return "LONG";
		}
		else if (isFloat)
		{
			return "FLOAT";
		}
		else if (isChar)
		{
			return "CHAR";
		}
		else
		{
			return "DATE";
		}
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

		OperatorUtils.writeType(HrdbmsType.MIN, out);
		prev.put(this, OperatorUtils.writeID(out));
		OperatorUtils.writeString(input, out, prev);
		OperatorUtils.writeString(output, out, prev);
		OperatorUtils.writeLong(NUM_GROUPS, out);
		OperatorUtils.writeBool(isInt, out);
		OperatorUtils.writeBool(isLong, out);
		OperatorUtils.writeBool(isFloat, out);
		OperatorUtils.writeBool(isChar, out);
		OperatorUtils.writeBool(isDate, out);
	}

	@Override
	public void setInput(final String col)
	{
		input = col;
	}

	@Override
	public void setInputColumn(final String col)
	{
		input = col;
	}

	public void setIsChar(final boolean isInt)
	{
		this.isChar = isInt;
	}

	public void setIsDate(final boolean isInt)
	{
		this.isDate = isInt;
	}

	public void setIsFloat(final boolean isInt)
	{
		this.isFloat = isInt;
	}

	public void setIsInt(final boolean isInt)
	{
		this.isInt = isInt;
	}

	public void setIsLong(final boolean isInt)
	{
		this.isLong = isInt;
	}

	@Override
	public void setNumGroups(final long groups)
	{
		NUM_GROUPS = groups;
	}

	private final class MinHashThread extends AggregateResultThread
	{
		// private final DiskBackedALOHashMap<AtomicDouble> mins = new
		// DiskBackedALOHashMap<AtomicDouble>(NUM_GROUPS > 0 ? NUM_GROUPS : 16);
		private Map<List<Object>, Object> mins = new HashMap<List<Object>, Object>();
		private final Map<String, Integer> cols2Pos;
		private int pos;

		public MinHashThread(final Map<String, Integer> cols2Pos) throws Exception
		{
			this.cols2Pos = cols2Pos;
			try
			{
				pos = this.cols2Pos.get(input);
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error(cols2Pos, e);
				throw e;
			}
		}

		@Override
		public void close()
		{
			// mins.close();
			mins = null;
		}

		@Override
		public Object getResult(final List<Object> keys)
		{
			return mins.get(keys);
		}

		// @Parallel
		@Override
		public final void put(final List<Object> row, final List<Object> group)
		{
			final Object o = row.get(pos);
			final Comparable val = (Comparable)o;

			synchronized (mins)
			{
				final Object ad = mins.get(group);
				if (ad != null)
				{
					final Comparable min = (Comparable)ad;
					if (val.compareTo(min) < 0)
					{
						mins.put(group, val);
					}

					return;
				}

				mins.put(group, val);
			}
		}
	}

	private final class MinThread extends AggregateResultThread
	{
		private final List<List<Object>> rows;
		private final Map<String, Integer> cols2Pos;
		private Comparable min;

		public MinThread(final List<List<Object>> rows, final Map<String, Integer> cols2Pos)
		{
			this.rows = rows;
			this.cols2Pos = cols2Pos;
		}

		@Override
		public void close()
		{
		}

		@Override
		public Object getResult()
		{
			return min;
		}

		@Override
		public void run()
		{
			final int pos = cols2Pos.get(input);
			min = null;

			int z = 0;
			final int limit = rows.size();
			// for (final Object orow : rows)
			while (z < limit)
			{
				final Object orow = rows.get(z++);
				final List<Object> row = (List<Object>)orow;
				if (isInt)
				{
					final Integer o = (Integer)row.get(pos);
					if (min == null)
					{
						min = o;
					}
					else if (o.compareTo((Integer)min) < 0)
					{
						min = o;
					}
				}
				else if (isLong)
				{
					final Long o = (Long)row.get(pos);
					if (min == null)
					{
						min = o;
					}
					else if (o.compareTo((Long)min) < 0)
					{
						min = o;
					}
				}
				else if (isFloat)
				{
					final Double o = (Double)row.get(pos);
					if (min == null)
					{
						min = o;
					}
					else if (o.compareTo((Double)min) < 0)
					{
						min = o;
					}
				}
				else if (isChar)
				{
					final String o = (String)row.get(pos);
					if (min == null)
					{
						min = o;
					}
					else if (o.compareTo((String)min) < 0)
					{
						min = o;
					}
				}
				else
				{
					final MyDate o = (MyDate)row.get(pos);
					if (min == null)
					{
						min = o;
					}
					else if (o.compareTo(min) < 0)
					{
						min = o;
					}
				}
			}

			// System.out.println("MinThread is terminating.");
		}
	}
}
