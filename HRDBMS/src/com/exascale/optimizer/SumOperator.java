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
import com.exascale.managers.HRDBMSWorker;
import com.exascale.misc.BigDecimalReplacement;

public final class SumOperator implements AggregateOperator, Serializable
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

	private int NUM_GROUPS = 16;

	public SumOperator(String input, String output, MetaData meta, boolean isInt)
	{
		this.input = input;
		this.output = output;
		this.meta = meta;
		this.isInt = isInt;
	}

	public static SumOperator deserialize(InputStream in, HashMap<Long, Object> prev) throws Exception
	{
		SumOperator value = (SumOperator)unsafe.allocateInstance(SumOperator.class);
		prev.put(OperatorUtils.readLong(in), value);
		value.input = OperatorUtils.readString(in, prev);
		value.output = OperatorUtils.readString(in, prev);
		value.NUM_GROUPS = OperatorUtils.readInt(in);
		value.isInt = OperatorUtils.readBool(in);
		return value;
	}

	@Override
	public SumOperator clone()
	{
		final SumOperator retval = new SumOperator(input, output, meta, isInt);
		retval.NUM_GROUPS = NUM_GROUPS;
		return retval;
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
	public void serialize(OutputStream out, IdentityHashMap<Object, Long> prev) throws Exception
	{
		Long id = prev.get(this);
		if (id != null)
		{
			OperatorUtils.serializeReference(id, out);
			return;
		}

		OperatorUtils.writeType(56, out);
		prev.put(this, OperatorUtils.writeID(out));
		OperatorUtils.writeString(input, out, prev);
		OperatorUtils.writeString(output, out, prev);
		OperatorUtils.writeInt(NUM_GROUPS, out);
		OperatorUtils.writeBool(isInt, out);
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

	public void setIsInt(boolean isInt)
	{
		this.isInt = isInt;
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
		private final ConcurrentHashMap<ArrayList<Object>, Object> results = new ConcurrentHashMap<ArrayList<Object>, Object>();
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
			// results.close();
		}

		@Override
		public Object getResult(ArrayList<Object> keys) throws Exception
		{
			if (isInt)
			{
				return ((AtomicLong)results.get(keys)).get();
			}

			try
			{
				final BigDecimalReplacement ad = (BigDecimalReplacement)results.get(keys);
				return ad.doubleValue();
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

			if (!isInt)
			{
				BigDecimalReplacement val;
				if (o instanceof Integer)
				{
					val = new BigDecimalReplacement((Integer)o);
				}
				else if (o instanceof Long)
				{
					val = new BigDecimalReplacement((Long)o);
				}
				else
				{
					try
					{
						val = new BigDecimalReplacement((Double)o);
					}
					catch (Exception e)
					{
						HRDBMSWorker.logger.debug("Was expecting a numeric in position " + pos + " of " + row);
						HRDBMSWorker.logger.debug("Cols2Pos is " + cols2Pos);
						HRDBMSWorker.logger.debug("Input is " + input);
						throw e;
					}
				}

				// final AtomicBigDecimal ad =
				// (AtomicBigDecimal)results.get(group);
				//synchronized (results)
				//{
				//	sdf
				//	BigDecimalReplacement ad = (BigDecimalReplacement)results.get(group);
				//	if (ad != null)
				//	{
				//		ad.add(val);
				//		return;
				//	}
				//	else
				//	{
				//		results.put(group, val);
				//		return;
				//	}
				//}
				final BigDecimalReplacement ad = (BigDecimalReplacement)results.get(group);
				if (ad != null)
				{
					ad.add(val);
					return;
				}
				
				if (results.putIfAbsent(group, val) != null)
				{
					((BigDecimalReplacement)results.get(group)).add(val);
				}
			}
			else
			{
				Long val;
				if (o instanceof Integer)
				{
					val = new Long((Integer)o);
				}
				else
				{
					val = (Long)o;
				}

				/*
				synchronized (results)
				{
					final Long ad = (Long)results.get(group);
					if (ad != null)
					{
						results.put(group, ad + val);
						return;
					}

					results.put(group, val);
				}
				*/
				final AtomicLong al = (AtomicLong)results.get(group);
				if (al != null)
				{
					al.addAndGet(val);
					return;
				}
				if (results.putIfAbsent(group, new AtomicLong(val)) != null)
				{
					((AtomicLong)results.get(group)).addAndGet(val);
				}
			}
		}
	}

	private final class SumThread extends AggregateResultThread
	{
		private final ArrayList<ArrayList<Object>> rows;
		private final HashMap<String, Integer> cols2Pos;
		private BigDecimalReplacement result;

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
				return (long)result.doubleValue();
			}

			return result.doubleValue();
		}

		@Override
		public void run()
		{
			final int pos = cols2Pos.get(input);
			result = new BigDecimalReplacement(0);

			int z = 0;
			final int limit = rows.size();
			//for (final Object orow : rows)
			while (z < limit)
			{
				final Object orow = rows.get(z++);
				final ArrayList<Object> row = (ArrayList<Object>)orow;
				if (isInt)
				{
					final Object o = row.get(pos);
					if (o instanceof Integer)
					{
						result.add(new BigDecimalReplacement((Integer)o));
					}
					else if (o instanceof Long)
					{
						result.add(new BigDecimalReplacement((Long)o));
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
					result.add(new BigDecimalReplacement(o));
				}
			}

			// System.out.println("SumThread is terminating.");
		}
	}
}
