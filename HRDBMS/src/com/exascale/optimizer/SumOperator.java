package com.exascale.optimizer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.misc.AtomicDouble;

public final class SumOperator implements AggregateOperator, Serializable
{
	private String input;

	private final String output;

	private final MetaData meta;
	private boolean isInt;
	private int NUM_GROUPS = 16;

	public SumOperator(String input, String output, MetaData meta, boolean isInt)
	{
		this.input = input;
		this.output = output;
		this.meta = meta;
		this.isInt = isInt;
	}
	
	public void setInput(String col)
	{
		input = col;
	}

	@Override
	public SumOperator clone()
	{
		final SumOperator retval = new SumOperator(input, output, meta, isInt);
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
		return new SumHashThread(cols2Pos);
	}

	@Override
	public String getInputColumn()
	{
		return input;
	}

	@Override
	public AggregateResultThread newProcessingThread(ArrayList<ArrayList<Object>> rows, HashMap<String, Integer> cols2Pos)
	{
		return new SumThread(rows, cols2Pos);
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

	private final class SumHashThread extends AggregateResultThread
	{
		// private final DiskBackedALOHashMap<AtomicDouble> results = new
		// DiskBackedALOHashMap<AtomicDouble>(NUM_GROUPS > 0 ? NUM_GROUPS : 16);
		private final ConcurrentHashMap<ArrayList<Object>, AtomicDouble> results = new ConcurrentHashMap<ArrayList<Object>, AtomicDouble>(NUM_GROUPS > 0 ? NUM_GROUPS : 16, 1.0f);
		private final HashMap<String, Integer> cols2Pos;
		private int pos;

		public SumHashThread(HashMap<String, Integer> cols2Pos) throws Exception
		{
			this.cols2Pos = cols2Pos;
			try
			{
				pos = this.cols2Pos.get(input);
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error(cols2Pos, e);
				HRDBMSWorker.logger.error(input);
				throw e;
			}
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
			// results.close();
		}

		@Override
		public Object getResult(ArrayList<Object> keys) throws Exception
		{
			if (isInt)
			{
				return (long)results.get(keys).get();
			}

			try
			{
				final AtomicDouble ad = results.get(keys);
				return ad.get();
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("Error looking up result for key = " + keys + " in SumOperator", e);
				for (final Object key : keys)
				{
					HRDBMSWorker.logger.error("Types are " + key.getClass());
				}
				for (final ArrayList<Object> keys2 : results.keySet())
				{
					for (final Object key : keys2)
					{
						HRDBMSWorker.logger.error("Result types are " + key.getClass());
					}
					break;
				}
				throw e;
			}
		}

		// @Parallel
		@Override
		public final void put(ArrayList<Object> row, ArrayList<Object> group) throws Exception
		{
			Object o = null;
			try
			{
				o = row.get(pos);
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("Pos is " + pos, e);
				HRDBMSWorker.logger.error("Cols2Pos is " + cols2Pos);
				HRDBMSWorker.logger.error("Input is " + input);
				HRDBMSWorker.logger.error("Row is " + row);
				throw e;
			}
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
				try
				{
					val = (Double)o;
				}
				catch(Exception e)
				{
					HRDBMSWorker.logger.debug("Was expecting a numeric in position " + pos + " of " + row);
					HRDBMSWorker.logger.debug("Cols2Pos is " + cols2Pos);
					HRDBMSWorker.logger.debug("Input is " + input);
					throw e;
				}
			}

			final AtomicDouble ad = results.get(group);
			if (ad != null)
			{
				try
				{
					addToSum(ad, val);
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.error("Error calling addAndGet() in SumOperator", e);
					HRDBMSWorker.logger.error("ad = " + ad);
					HRDBMSWorker.logger.error("val = " + val);
					throw e;
				}
				return;
			}
			if (results.putIfAbsent(group, new AtomicDouble(val)) != null)
			{
				addToSum(results.get(group), val);
			}
		}
	}

	private final class SumThread extends AggregateResultThread
	{
		private final ArrayList<ArrayList<Object>> rows;
		private final HashMap<String, Integer> cols2Pos;
		private double result;

		public SumThread(ArrayList<ArrayList<Object>> rows, HashMap<String, Integer> cols2Pos)
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
				return (long)result;
			}

			return result;
		}

		@Override
		public void run()
		{
			final int pos = cols2Pos.get(input);
			result = 0;

			for (final Object orow : rows)
			{
				final ArrayList<Object> row = (ArrayList<Object>)orow;
				if (isInt)
				{
					final Object o = row.get(pos);
					if (o instanceof Integer)
					{
						result += (Integer)o;
					}
					else if (o instanceof Long)
					{
						result += (Long)o;
					}
					else
					{
						HRDBMSWorker.logger.error("Unknown class type in integer summation.");
						System.exit(1);
					}
				}
				else
				{
					final Double o = (Double)row.get(pos);
					result += o;
				}
			}

			// System.out.println("SumThread is terminating.");
		}
	}
}
