package com.exascale.optimizer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.misc.AtomicDouble;

public final class MinOperator implements AggregateOperator, Serializable
{
	private String input;

	private final String output;

	private final MetaData meta;
	private boolean isInt;
	private int NUM_GROUPS = 16;

	public MinOperator(String input, String output, MetaData meta, boolean isInt)
	{
		this.input = input;
		this.output = output;
		this.meta = meta;
		this.isInt = isInt;
	}

	@Override
	public MinOperator clone()
	{
		final MinOperator retval = new MinOperator(input, output, meta, isInt);
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
		return new MinHashThread(cols2Pos);
	}

	@Override
	public String getInputColumn()
	{
		return input;
	}

	@Override
	public AggregateResultThread newProcessingThread(ArrayList<ArrayList<Object>> rows, HashMap<String, Integer> cols2Pos)
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

	private final class MinHashThread extends AggregateResultThread
	{
		// private final DiskBackedALOHashMap<AtomicDouble> mins = new
		// DiskBackedALOHashMap<AtomicDouble>(NUM_GROUPS > 0 ? NUM_GROUPS : 16);
		private final ConcurrentHashMap<ArrayList<Object>, AtomicDouble> mins = new ConcurrentHashMap<ArrayList<Object>, AtomicDouble>(NUM_GROUPS > 0 ? NUM_GROUPS : 16, 1.0f);
		private final HashMap<String, Integer> cols2Pos;
		private int pos;

		public MinHashThread(HashMap<String, Integer> cols2Pos) throws Exception
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
		}

		@Override
		public Object getResult(ArrayList<Object> keys)
		{
			if (isInt)
			{
				return new Long((long)mins.get(keys).doubleValue());
			}

			return mins.get(keys).doubleValue();
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

			AtomicDouble ad = mins.get(group);
			if (ad != null)
			{
				double min = ad.get();
				if (val < min)
				{
					while (!ad.compareAndSet(min, val))
					{
						min = ad.get();
						if (val >= min)
						{
							break;
						}
					}
				}

				return;
			}

			if (mins.putIfAbsent(group, new AtomicDouble(val)) != null)
			{
				ad = mins.get(group);
				double min = ad.get();
				if (val < min)
				{
					while (!ad.compareAndSet(min, val))
					{
						min = ad.get();
						if (val >= min)
						{
							break;
						}
					}
				}

				return;
			}
		}
	}

	private final class MinThread extends AggregateResultThread
	{
		private final ArrayList<ArrayList<Object>> rows;
		private final HashMap<String, Integer> cols2Pos;
		private double min;

		public MinThread(ArrayList<ArrayList<Object>> rows, HashMap<String, Integer> cols2Pos)
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
				return new Long((long)min);
			}

			return new Double(min);
		}

		@Override
		public void run()
		{
			final int pos = cols2Pos.get(input);
			min = Double.POSITIVE_INFINITY;

			for (final Object orow : rows)
			{
				final ArrayList<Object> row = (ArrayList<Object>)orow;
				if (isInt)
				{
					final Object o = row.get(pos);
					if (o instanceof Integer)
					{
						if ((Integer)o < min)
						{
							min = (Integer)o;
						}
					}
					else if (o instanceof Long)
					{
						if ((Long)o < min)
						{
							min = (Long)o;
						}
					}
					else
					{
						//TODO should be fixed by MAX MIN update
						HRDBMSWorker.logger.error("Unknown class type in MinOperator.");
						System.exit(1);
					}
				}
				else
				{
					final Double o = (Double)row.get(pos);
					if (o < min)
					{
						min = o;
					}
				}
			}

			// System.out.println("MinThread is terminating.");
		}
	}
}
