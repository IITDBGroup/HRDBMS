package com.exascale.optimizer;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.misc.MyDate;

public final class MaxOperator implements AggregateOperator, Serializable
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

	private String input;
	private String output;
	private transient final MetaData meta;
	private boolean isInt;
	private boolean isLong;
	private boolean isFloat;
	private boolean isChar;

	private boolean isDate;

	private int NUM_GROUPS = 16;

	public MaxOperator(String input, String output, MetaData meta, boolean isInt)
	{
		this.input = input;
		this.output = output;
		this.meta = meta;

		if (isInt)
		{
			isInt = true;
			isLong = false;
			isFloat = false;
			isChar = false;
			isDate = false;
		}
		else
		{
			isInt = false;
			isLong = false;
			isFloat = true;
			isChar = false;
			isDate = false;
		}
	}

	public static MaxOperator deserialize(InputStream in, HashMap<Long, Object> prev) throws Exception
	{
		MaxOperator value = (MaxOperator)unsafe.allocateInstance(MaxOperator.class);
		prev.put(OperatorUtils.readLong(in), value);
		value.input = OperatorUtils.readString(in, prev);
		value.output = OperatorUtils.readString(in, prev);
		value.NUM_GROUPS = OperatorUtils.readInt(in);
		value.isInt = OperatorUtils.readBool(in);
		value.isLong = OperatorUtils.readBool(in);
		value.isFloat = OperatorUtils.readBool(in);
		value.isChar = OperatorUtils.readBool(in);
		value.isDate = OperatorUtils.readBool(in);
		return value;
	}

	@Override
	public MaxOperator clone()
	{
		final MaxOperator retval = new MaxOperator(input, output, meta, isInt);
		retval.NUM_GROUPS = NUM_GROUPS;
		retval.isInt = isInt;
		retval.isLong = isLong;
		retval.isFloat = isFloat;
		retval.isChar = isChar;
		retval.isDate = isDate;
		return retval;
	}

	@Override
	public AggregateResultThread getHashThread(HashMap<String, Integer> cols2Pos) throws Exception
	{
		return new MaxHashThread(cols2Pos);
	}

	@Override
	public String getInputColumn()
	{
		return input;
	}

	@Override
	public AggregateResultThread newProcessingThread(ArrayList<ArrayList<Object>> rows, HashMap<String, Integer> cols2Pos)
	{
		return new MaxThread(rows, cols2Pos);
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
	public void serialize(OutputStream out, IdentityHashMap<Object, Long> prev) throws Exception
	{
		Long id = prev.get(this);
		if (id != null)
		{
			OperatorUtils.serializeReference(id, out);
			return;
		}

		OperatorUtils.writeType(54, out);
		prev.put(this, OperatorUtils.writeID(out));
		OperatorUtils.writeString(input, out, prev);
		OperatorUtils.writeString(output, out, prev);
		OperatorUtils.writeInt(NUM_GROUPS, out);
		OperatorUtils.writeBool(isInt, out);
		OperatorUtils.writeBool(isLong, out);
		OperatorUtils.writeBool(isFloat, out);
		OperatorUtils.writeBool(isChar, out);
		OperatorUtils.writeBool(isDate, out);
	}

	@Override
	public void setInput(String col)
	{
		input = col;
	}

	@Override
	public void setInputColumn(String col)
	{
		input = col;
	}

	public void setIsChar(boolean isInt)
	{
		this.isChar = isInt;
	}

	public void setIsDate(boolean isInt)
	{
		this.isDate = isInt;
	}

	public void setIsFloat(boolean isInt)
	{
		this.isFloat = isInt;
	}

	public void setIsInt(boolean isInt)
	{
		this.isInt = isInt;
	}

	public void setIsLong(boolean isInt)
	{
		this.isLong = isInt;
	}

	@Override
	public void setNumGroups(int groups)
	{
		NUM_GROUPS = groups;
	}

	private final class MaxHashThread extends AggregateResultThread
	{
		// private final DiskBackedALOHashMap<AtomicDouble> maxes = new
		// DiskBackedALOHashMap<AtomicDouble>(NUM_GROUPS > 0 ? NUM_GROUPS : 16);
		private final HashMap<ArrayList<Object>, Object> maxes = new HashMap<ArrayList<Object>, Object>();
		private final HashMap<String, Integer> cols2Pos;
		private int pos;

		public MaxHashThread(HashMap<String, Integer> cols2Pos) throws Exception
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
			// maxes.close();
		}

		@Override
		public Object getResult(ArrayList<Object> keys)
		{
			return maxes.get(keys);
		}

		// @Parallel
		@Override
		public final void put(ArrayList<Object> row, ArrayList<Object> group)
		{
			final Object o = row.get(pos);
			Comparable val = (Comparable)o;
			synchronized (maxes)
			{
				Object ad = maxes.get(group);
				if (ad != null)
				{
					Comparable max = (Comparable)ad;
					if (val.compareTo(max) > 0)
					{
						maxes.put(group, val);
					}

					return;
				}

				maxes.put(group, val);
			}
		}
	}

	private final class MaxThread extends AggregateResultThread
	{
		private final ArrayList<ArrayList<Object>> rows;
		private final HashMap<String, Integer> cols2Pos;
		private Comparable max;

		public MaxThread(ArrayList<ArrayList<Object>> rows, HashMap<String, Integer> cols2Pos)
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
			return max;
		}

		@Override
		public void run()
		{
			final int pos = cols2Pos.get(input);
			max = null;

			int z = 0;
			final int limit = rows.size();
			// for (final Object orow : rows)
			while (z < limit)
			{
				final Object orow = rows.get(z++);
				final ArrayList<Object> row = (ArrayList<Object>)orow;
				if (isInt)
				{
					final Integer o = (Integer)row.get(pos);
					if (max == null)
					{
						max = o;
					}
					else if (o.compareTo((Integer)max) > 0)
					{
						max = o;
					}
				}
				else if (isLong)
				{
					final Long o = (Long)row.get(pos);
					if (max == null)
					{
						max = o;
					}
					else if (o.compareTo((Long)max) > 0)
					{
						max = o;
					}
				}
				else if (isFloat)
				{
					final Double o = (Double)row.get(pos);
					if (max == null)
					{
						max = o;
					}
					else if (o.compareTo((Double)max) > 0)
					{
						max = o;
					}
				}
				else if (isChar)
				{
					final String o = (String)row.get(pos);
					if (max == null)
					{
						max = o;
					}
					else if (o.compareTo((String)max) > 0)
					{
						max = o;
					}
				}
				else
				{
					final MyDate o = (MyDate)row.get(pos);
					if (max == null)
					{
						max = o;
					}
					else if (o.compareTo(max) > 0)
					{
						max = o;
					}
				}
			}

			// System.out.println("MaxThread is terminating.");
		}
	}
}
