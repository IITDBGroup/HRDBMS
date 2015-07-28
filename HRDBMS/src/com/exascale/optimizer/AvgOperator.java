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
import com.exascale.misc.BigDecimalReplacement;

public final class AvgOperator implements AggregateOperator, Serializable
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

	private int NUM_GROUPS = 16;

	public AvgOperator(String input, String output, MetaData meta)
	{
		this.input = input;
		this.output = output;
		this.meta = meta;
	}

	public static AvgOperator deserialize(InputStream in, HashMap<Long, Object> prev) throws Exception
	{
		AvgOperator value = (AvgOperator)unsafe.allocateInstance(AvgOperator.class);
		prev.put(OperatorUtils.readLong(in), value);
		value.input = OperatorUtils.readString(in, prev);
		value.output = OperatorUtils.readString(in, prev);
		value.NUM_GROUPS = OperatorUtils.readInt(in);
		return value;
	}

	@Override
	public AvgOperator clone()
	{
		return new AvgOperator(input, output, meta);
	}

	@Override
	public AggregateResultThread getHashThread(HashMap<String, Integer> cols2Pos)
	{
		return new AvgHashThread(cols2Pos);
	}

	@Override
	public String getInputColumn()
	{
		return input;
	}

	@Override
	public AggregateResultThread newProcessingThread(ArrayList<ArrayList<Object>> rows, HashMap<String, Integer> cols2Pos)
	{
		return new AvgThread(rows, cols2Pos);
	}

	@Override
	public String outputColumn()
	{
		return output;
	}

	@Override
	public String outputType()
	{
		return "FLOAT";
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

		OperatorUtils.writeType(51, out);
		prev.put(this, OperatorUtils.writeID(out));
		OperatorUtils.writeString(input, out, prev);
		OperatorUtils.writeString(output, out, prev);
		OperatorUtils.writeInt(NUM_GROUPS, out);
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

	@Override
	public void setNumGroups(int groups)
	{
		NUM_GROUPS = groups;
	}

	private final class AvgHashThread extends AggregateResultThread
	{
		// private final DiskBackedALOHashMap<AtomicDouble> sums = new
		// DiskBackedALOHashMap<AtomicDouble>(NUM_GROUPS > 0 ? NUM_GROUPS : 16);
		// private final DiskBackedALOHashMap<AtomicLong> counts = new
		// DiskBackedALOHashMap<AtomicLong>(NUM_GROUPS > 0 ? NUM_GROUPS : 16);
		private final HashMap<ArrayList<Object>, BigDecimalReplacement> sums = new HashMap<ArrayList<Object>, BigDecimalReplacement>();
		private final ConcurrentHashMap<ArrayList<Object>, AtomicLong> counts = new ConcurrentHashMap<ArrayList<Object>, AtomicLong>(NUM_GROUPS, 0.75f, 6 * ResourceManager.cpus);
		private final int pos;

		public AvgHashThread(HashMap<String, Integer> cols2Pos)
		{
			pos = cols2Pos.get(input);
		}

		// public final void addToSum(AtomicBigDecimal ad, BigDecimalReplacement
		// amount)
		// {
		// final AtomicBigDecimal newSum = ad;
		// for (;;)
		// {
		// final BigDecimalReplacement oldVal = newSum.get();
		// if (newSum.compareAndSet(oldVal, oldVal.add(amount)))
		// {
		// return;
		// }
		// }
		// }

		@Override
		public void close()
		{
			// sums.close();
			// counts.close();
		}

		@Override
		public Object getResult(ArrayList<Object> keys)
		{
			return sums.get(keys).doubleValue() / counts.get(keys).get();
		}

		// @Parallel
		@Override
		public final void put(ArrayList<Object> row, ArrayList<Object> group)
		{
			final Double v = ((Number)row.get(pos)).doubleValue();
			BigDecimalReplacement val = new BigDecimalReplacement(v);
			synchronized (sums)
			{
				final BigDecimalReplacement ad = sums.get(group);
				if (ad != null)
				{
					ad.add(val);
				}
				else
				{
					sums.put(group, val);
				}
			}

			final AtomicLong al = counts.get(group);
			if (al != null)
			{
				al.incrementAndGet();
				return;
			}
			if (counts.putIfAbsent(group, new AtomicLong(1)) != null)
			{
				counts.get(group).incrementAndGet();
			}
		}
	}

	private final class AvgThread extends AggregateResultThread
	{
		private final ArrayList<ArrayList<Object>> rows;
		private final HashMap<String, Integer> cols2Pos;
		private BigDecimalReplacement result;
		private double result2;

		public AvgThread(ArrayList<ArrayList<Object>> rows, HashMap<String, Integer> cols2Pos)
		{
			this.rows = rows;
			this.cols2Pos = cols2Pos;
		}

		@Override
		public void close()
		{
		}

		@Override
		public Double getResult()
		{
			return result2;
		}

		@Override
		public void run()
		{
			final int pos = cols2Pos.get(input);
			long numRows = 0;
			result = new BigDecimalReplacement(0);

			int z = 0;
			final int limit = rows.size();
			//for (final Object orow : rows)
			while (z < limit)
			{
				Object orow = rows.get(z++);
				final ArrayList<Object> row = (ArrayList<Object>)orow;
				numRows++;
				result.add(new BigDecimalReplacement((Double)row.get(pos)));
			}

			result2 = result.doubleValue() / numRows;
			// System.out.println("AvgThread is terminating.");
		}
	}
}
