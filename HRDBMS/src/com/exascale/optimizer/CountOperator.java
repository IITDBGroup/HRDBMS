package com.exascale.optimizer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public final class CountOperator implements AggregateOperator, Serializable
{
	private final String output;

	private final MetaData meta;

	private String input;
	private int NUM_GROUPS = 16;

	public CountOperator(String output, MetaData meta)
	{
		this.output = output;
		this.meta = meta;
	}

	public CountOperator(String input, String output, MetaData meta)
	{
		this.input = input;
		this.output = output;
		this.meta = meta;
	}

	@Override
	public CountOperator clone()
	{
		return new CountOperator(input, output, meta);
	}

	@Override
	public AggregateResultThread getHashThread(HashMap<String, Integer> cols2Pos)
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

	@Override
	public AggregateResultThread newProcessingThread(ArrayList<ArrayList<Object>> rows, HashMap<String, Integer> cols2Pos)
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
	public void setInputColumn(String col)
	{
		input = col;
	}

	@Override
	public void setNumGroups(int groups)
	{
		NUM_GROUPS = groups;
	}

	private final class CountHashThread extends AggregateResultThread
	{
		// private final DiskBackedALOHashMap<AtomicLong> results = new
		// DiskBackedALOHashMap<AtomicLong>(NUM_GROUPS > 0 ? NUM_GROUPS : 16);
		private final ConcurrentHashMap<ArrayList<Object>, AtomicLong> results = new ConcurrentHashMap<ArrayList<Object>, AtomicLong>(NUM_GROUPS > 0 ? NUM_GROUPS : 16, 1.0f);
		private int pos;

		public CountHashThread(HashMap<String, Integer> cols2Pos)
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
		}

		@Override
		public Object getResult(ArrayList<Object> keys)
		{
			return results.get(keys).longValue();
		}

		// @Parallel
		@Override
		public final void put(ArrayList<Object> row, ArrayList<Object> group)
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

		public CountThread(ArrayList<ArrayList<Object>> rows, HashMap<String, Integer> cols2Pos)
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
				for (final Object o : rows)
				{
					final ArrayList<Object> row = (ArrayList<Object>)o;
					row.get(pos);
					result++; // TODO only increment if not null
				}
			}
		}
	}
}
