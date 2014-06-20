package com.exascale.optimizer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.misc.AtomicDouble;

public final class MaxOperator implements AggregateOperator, Serializable
{
	private String input;

	private final String output;

	private final MetaData meta;
	private boolean isInt;
	private int NUM_GROUPS = 16;

	public MaxOperator(String input, String output, MetaData meta, boolean isInt)
	{
		this.input = input;
		this.output = output;
		this.meta = meta;
		this.isInt = isInt;
	}

	@Override
	public MaxOperator clone()
	{
		final MaxOperator retval = new MaxOperator(input, output, meta, isInt);
		retval.NUM_GROUPS = NUM_GROUPS;
		return retval;
	}
	
	public void setIsInt(boolean isInt)
	{
		this.isInt = isInt;
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
			return "LONG";
		}
		else
		{
			return "FLOAT";
		}
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

	private final class MaxHashThread extends AggregateResultThread
	{
		// private final DiskBackedALOHashMap<AtomicDouble> maxes = new
		// DiskBackedALOHashMap<AtomicDouble>(NUM_GROUPS > 0 ? NUM_GROUPS : 16);
		private final ConcurrentHashMap<ArrayList<Object>, AtomicDouble> maxes = new ConcurrentHashMap<ArrayList<Object>, AtomicDouble>(NUM_GROUPS > 0 ? NUM_GROUPS : 16);
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
			if (isInt)
			{
				return new Long((long)maxes.get(keys).doubleValue());
			}

			return maxes.get(keys).doubleValue();
		}

		// @Parallel
		@Override
		public final void put(ArrayList<Object> row, ArrayList<Object> group)
		{
			final Object o = row.get(pos);
			Double val;
			if (o instanceof Integer)
			{
				val = new Double((Integer)o);
			}
			else if (o instanceof Long)
			{
				val = new Double((Long)o);
			}
			else
			{
				val = (Double)o;
			}

			AtomicDouble ad = maxes.get(group);
			if (ad != null)
			{
				double max = ad.get();
				if (val > max)
				{
					while (!ad.compareAndSet(max, val))
					{
						max = ad.get();
						if (val <= max)
						{
							break;
						}
					}
				}

				return;
			}

			if (maxes.putIfAbsent(group, new AtomicDouble(val)) != null)
			{
				ad = maxes.get(group);
				double max = ad.get();
				if (val > max)
				{
					while (!ad.compareAndSet(max, val))
					{
						max = ad.get();
						if (val <= max)
						{
							break;
						}
					}
				}

				return;
			}
		}
	}

	private final class MaxThread extends AggregateResultThread
	{
		private final ArrayList<ArrayList<Object>> rows;
		private final HashMap<String, Integer> cols2Pos;
		private double max;

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
			if (isInt)
			{
				return new Long((long)max);
			}

			return new Double(max);
		}

		@Override
		public void run()
		{
			final int pos = cols2Pos.get(input);
			max = Double.NEGATIVE_INFINITY;

			for (final Object orow : rows)
			{
				final ArrayList<Object> row = (ArrayList<Object>)orow;
				if (isInt)
				{
					final Object o = row.get(pos);
					if (o instanceof Integer)
					{
						if ((Integer)o > max)
						{
							max = (Integer)o;
						}
					}
					else if (o instanceof Long)
					{
						if ((Long)o > max)
						{
							max = (Long)o;
						}
					}
					else
					{
						//TODO should be fixed when MAX and MIN are updated
						HRDBMSWorker.logger.error("Unknown class type in MaxOperator.");
						System.exit(1);
					}
				}
				else
				{
					final Double o = (Double)row.get(pos);
					if (o > max)
					{
						max = o;
					}
				}
			}

			// System.out.println("MaxThread is terminating.");
		}
	}
}
