package com.exascale.optimizer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.managers.ResourceManager;
import com.exascale.managers.ResourceManager.DiskBackedHashSet;

public final class CountDistinctOperator implements AggregateOperator, Serializable
{
	private String input;

	private final String output;

	private final MetaData meta;
	private int NUM_GROUPS = 16;
	private int childCard = 16 * 16;

	public CountDistinctOperator(String input, String output, MetaData meta)
	{
		this.input = input;
		this.output = output;
		this.meta = meta;
	}

	@Override
	public CountDistinctOperator clone()
	{
		return new CountDistinctOperator(input, output, meta);
	}

	@Override
	public AggregateResultThread getHashThread(HashMap<String, Integer> cols2Pos)
	{
		return new CountDistinctHashThread(cols2Pos);
	}

	@Override
	public String getInputColumn()
	{
		return input;
	}

	@Override
	public AggregateResultThread newProcessingThread(ArrayList<ArrayList<Object>> rows, HashMap<String, Integer> cols2Pos)
	{
		return new CountDistinctThread(rows, cols2Pos);
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

	public void setChildCard(int card)
	{
		childCard = card;
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

	private final class CountDistinctHashThread extends AggregateResultThread
	{
		// private final DiskBackedALOHashMap<AtomicLong> results = new
		// DiskBackedALOHashMap<AtomicLong>(NUM_GROUPS > 0 ? NUM_GROUPS : 16);
		private final ConcurrentHashMap<ArrayList<Object>, AtomicLong> results = new ConcurrentHashMap<ArrayList<Object>, AtomicLong>(NUM_GROUPS > 0 ? NUM_GROUPS : 16, 1.0f);
		private final DiskBackedHashSet hashSet = ResourceManager.newDiskBackedHashSet(false, childCard);
		private final int pos;

		public CountDistinctHashThread(HashMap<String, Integer> cols2Pos)
		{
			pos = cols2Pos.get(input);
		}

		@Override
		public void close()
		{
			try
			{
				hashSet.close();
				// results.close();
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
			}
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
			final ArrayList<Object> consolidated = new ArrayList<Object>();
			consolidated.addAll(group);
			consolidated.add(row.get(pos));
			if (hashSet.add(consolidated))
			{
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
	}

	private final class CountDistinctThread extends AggregateResultThread
	{
		private final ArrayList<ArrayList<Object>> rows;
		private long result;
		private final int pos;
		private final HashSet<Object> distinct = new HashSet<Object>();

		public CountDistinctThread(ArrayList<ArrayList<Object>> rows, HashMap<String, Integer> cols2Pos)
		{
			this.rows = rows;
			pos = cols2Pos.get(input);
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
			for (final Object o : rows)
			{
				final ArrayList<Object> row = (ArrayList<Object>)o;
				final Object val = row.get(pos);
				distinct.add(val);
			}

			result = distinct.size();
		}
	}
}
