package com.exascale.optimizer;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import com.exascale.managers.ResourceManager;

public final class CountOperator implements AggregateOperator, Serializable
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

	private String output;
	private transient final MetaData meta;

	private String input;

	private long NUM_GROUPS = 16;

	public CountOperator(final String output, final MetaData meta)
	{
		this.output = output;
		this.meta = meta;
	}

	public CountOperator(final String input, final String output, final MetaData meta)
	{
		this.input = input;
		this.output = output;
		this.meta = meta;
	}

	public static CountOperator deserialize(final InputStream in, final HashMap<Long, Object> prev) throws Exception
	{
		final CountOperator value = (CountOperator)unsafe.allocateInstance(CountOperator.class);
		prev.put(OperatorUtils.readLong(in), value);
		value.input = OperatorUtils.readString(in, prev);
		value.output = OperatorUtils.readString(in, prev);
		value.NUM_GROUPS = OperatorUtils.readLong(in);
		return value;
	}

	@Override
	public CountOperator clone()
	{
		return new CountOperator(input, output, meta);
	}

	@Override
	public AggregateResultThread getHashThread(final HashMap<String, Integer> cols2Pos)
	{
		return new CountHashThread(cols2Pos);
	}

	@Override
	public String getInputColumn()
	{
		if (input != null)
		{
			return input;
		}

		return output;
	}

	public String getRealInputColumn()
	{
		return input;
	}

	@Override
	public AggregateResultThread newProcessingThread(final ArrayList<ArrayList<Object>> rows, final HashMap<String, Integer> cols2Pos)
	{
		return new CountThread(rows, cols2Pos);
	}

	@Override
	public String outputColumn()
	{
		return output;
	}

	@Override
	public String outputType()
	{
		return "LONG";
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

		OperatorUtils.writeType(53, out);
		prev.put(this, OperatorUtils.writeID(out));
		OperatorUtils.writeString(input, out, prev);
		OperatorUtils.writeString(output, out, prev);
		OperatorUtils.writeLong(NUM_GROUPS, out);
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

	@Override
	public void setNumGroups(final long groups)
	{
		NUM_GROUPS = groups;
	}

	private final class CountHashThread extends AggregateResultThread
	{
		// private final DiskBackedALOHashMap<AtomicLong> results = new
		// DiskBackedALOHashMap<AtomicLong>(NUM_GROUPS > 0 ? NUM_GROUPS : 16);
		private ConcurrentHashMap<ArrayList<Object>, AtomicLong> results = new ConcurrentHashMap<ArrayList<Object>, AtomicLong>(NUM_GROUPS <= Integer.MAX_VALUE ? (int)NUM_GROUPS : Integer.MAX_VALUE, 0.75f, 6 * ResourceManager.cpus);
		private int pos;

		public CountHashThread(final HashMap<String, Integer> cols2Pos)
		{
			if (input != null)
			{
				pos = cols2Pos.get(input);
			}
		}

		@Override
		public void close()
		{
			// results.close();
			results = null;
		}

		@Override
		public Object getResult(final ArrayList<Object> keys)
		{
			return results.get(keys).longValue();
		}

		// @Parallel
		@Override
		public final void put(final ArrayList<Object> row, final ArrayList<Object> group)
		{
			row.get(pos);
			// TODO only do following if val not null
			final AtomicLong al = results.get(group);
			if (al != null)
			{
				al.incrementAndGet();
				return;
			}

			if (results.putIfAbsent(group, new AtomicLong(1)) != null)
			{
				results.get(group).incrementAndGet();
			}
		}
	}

	private final class CountThread extends AggregateResultThread
	{
		private final ArrayList<ArrayList<Object>> rows;
		private long result;
		private int pos;

		public CountThread(final ArrayList<ArrayList<Object>> rows, final HashMap<String, Integer> cols2Pos)
		{
			this.rows = rows;
			if (input != null)
			{
				pos = cols2Pos.get(input);
			}
		}

		@Override
		public void close()
		{
		}

		@Override
		public Object getResult()
		{
			return new Long(result);
		}

		@Override
		public void run()
		{
			if (input == null)
			{
				result = rows.size();
			}
			else
			{
				result = 0;
				int z = 0;
				final int limit = rows.size();
				// for (final Object o : rows)
				while (z < limit)
				{
					final Object o = rows.get(z++);
					final ArrayList<Object> row = (ArrayList<Object>)o;
					row.get(pos);
					result++; // TODO only increment if not null
				}
			}
		}
	}
}
