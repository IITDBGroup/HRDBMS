package com.exascale.optimizer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.misc.AtomicDouble;
import com.exascale.misc.MyDate;

public final class MaxOperator implements AggregateOperator, Serializable
{
	private String input;

	private final String output;

	private final MetaData meta;
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
	
	public void setInput(String col)
	{
		input = col;
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
	
	public void setIsInt(boolean isInt)
	{
		this.isInt = isInt;
	}
	
	public void setIsLong(boolean isInt)
	{
		this.isLong = isInt;
	}
	
	public void setIsFloat(boolean isInt)
	{
		this.isFloat = isInt;
	}
	
	public void setIsChar(boolean isInt)
	{
		this.isChar = isInt;
	}
	
	public void setIsDate(boolean isInt)
	{
		this.isDate = isInt;
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
		private final ConcurrentHashMap<ArrayList<Object>, AtomicReference> maxes = new ConcurrentHashMap<ArrayList<Object>, AtomicReference>(NUM_GROUPS > 0 ? NUM_GROUPS : 16);
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
			return maxes.get(keys).get();
		}

		// @Parallel
		@Override
		public final void put(ArrayList<Object> row, ArrayList<Object> group)
		{
			final Object o = row.get(pos);
			Comparable val = (Comparable)o;
			AtomicReference ad = maxes.get(group);
			if (ad != null)
			{
				Comparable max = (Comparable)ad.get();
				if (val.compareTo(max) > 0)
				{
					while (!ad.compareAndSet(max, val))
					{
						max = (Comparable)ad.get();
						if (val.compareTo(max) < 1)
						{
							break;
						}
					}
				}

				return;
			}

			if (maxes.putIfAbsent(group, new AtomicReference(val)) != null)
			{
				ad = maxes.get(group);
				Comparable max = (Comparable)ad.get();
				if (val.compareTo(max) > 0)
				{
					while (!ad.compareAndSet(max, val))
					{
						max = (Comparable)ad.get();
						if (val.compareTo(max) < 1)
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

			for (final Object orow : rows)
			{
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
					else if (o.compareTo((MyDate)max) > 0)
					{
						max = o;
					}
				}
			}

			// System.out.println("MaxThread is terminating.");
		}
	}
}
