package com.exascale.optimizer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import com.exascale.misc.AtomicDouble;

public final class AvgOperator implements AggregateOperator, Serializable
{
	private String input;

	private final String output;

	private final MetaData meta;
	private int NUM_GROUPS = 16;

	public AvgOperator(String input, String output, MetaData meta)
	{
		this.input = input;
		this.output = output;
		this.meta = meta;
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
		private final ConcurrentHashMap<ArrayList<Object>, AtomicDouble> sums = new ConcurrentHashMap<ArrayList<Object>, AtomicDouble>(NUM_GROUPS > 0 ? NUM_GROUPS : 16, 1.0f);
		private final ConcurrentHashMap<ArrayList<Object>, AtomicLong> counts = new ConcurrentHashMap<ArrayList<Object>, AtomicLong>(NUM_GROUPS > 0 ? NUM_GROUPS : 16, 1.0f);
		private final int pos;

		public AvgHashThread(HashMap<String, Integer> cols2Pos)
		{
			pos = cols2Pos.get(input);
		}

		public final void addToSum(AtomicDouble ad, double amount)
		{
			final AtomicDouble newSum = ad;
			for (;;)
			{
				final double oldVal = newSum.get();
				if (newSum.compareAndSet(oldVal, oldVal + amount))
				{
					return;
				}
			}
		}

		@Override
		public void close()
		{
			// sums.close();
			// counts.close();
		}

		@Override
		public Object getResult(ArrayList<Object> keys)
		{
			return new Double(sums.get(keys).get() / counts.get(keys).get());
		}

		// @Parallel
		@Override
		public final void put(ArrayList<Object> row, ArrayList<Object> group)
		{
			final Double val = (Double)row.get(pos);
			final AtomicDouble ad = sums.get(group);
			if (ad != null)
			{
				addToSum(ad, val);
			}
			else
			{
				if (sums.putIfAbsent(group, new AtomicDouble(val)) != null)
				{
					addToSum(sums.get(group), val);
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
		private double result;

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
			return result;
		}

		@Override
		public void run()
		{
			final int pos = cols2Pos.get(input);
			long numRows = 0;
			result = 0;

			for (final Object orow : rows)
			{
				final ArrayList<Object> row = (ArrayList<Object>)orow;
				numRows++;
				result += (Double)row.get(pos);
			}

			result /= numRows;
			// System.out.println("AvgThread is terminating.");
		}
	}
}
