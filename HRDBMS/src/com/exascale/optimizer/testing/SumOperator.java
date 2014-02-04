package com.exascale.optimizer.testing;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import com.exascale.optimizer.testing.ResourceManager.DiskBackedArray;

public class SumOperator implements AggregateOperator, Serializable
{
	protected String input;
	protected String output;
	protected MetaData meta;
	protected boolean isInt;
	protected int NUM_GROUPS = 16;
	
	public SumOperator(String input, String output, MetaData meta, boolean isInt)
	{
		this.input = input;
		this.output = output;
		this.meta = meta;
		this.isInt = isInt;
	}
	
	public void setNumGroups(int groups)
	{
		NUM_GROUPS = groups;
	}
	
	public SumOperator clone()
	{
		SumOperator retval = new SumOperator(input, output, meta, isInt);
		retval.NUM_GROUPS = NUM_GROUPS;
		return retval;
	}
	
	public void setInputColumn(String col)
	{
		input = col;
	}
	
	public String getInputColumn()
	{
		return input;
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
	public AggregateResultThread newProcessingThread(ArrayList<ArrayList<Object>> rows, HashMap<String, Integer> cols2Pos) 
	{
		return new SumThread(rows, cols2Pos);
	}
	
	public AggregateResultThread getHashThread(HashMap<String, Integer> cols2Pos)
	{
		return new SumHashThread(cols2Pos);
	}

	protected class SumThread extends AggregateResultThread
	{
		protected ArrayList<ArrayList<Object>> rows;
		protected HashMap<String, Integer> cols2Pos;
		protected BigDecimal result;
		
		public SumThread(ArrayList<ArrayList<Object>> rows, HashMap<String, Integer> cols2Pos)
		{
			this.rows = rows;
			this.cols2Pos = cols2Pos;
		}
		
		public Object getResult()
		{
			if (isInt)
			{
				return ResourceManager.internLong(new Long(result.longValue()));
			}
			
			return ResourceManager.internDouble(new Double(result.doubleValue()));
		}
		
		public void run()
		{
			int pos = cols2Pos.get(input);
			result = new BigDecimal(0);
			
			for (Object orow : rows)
			{
				ArrayList<Object> row = (ArrayList<Object>)orow;
				if (isInt)
				{
					Object o = row.get(pos);
					if (o instanceof Integer)
					{
						result = result.add(new BigDecimal((Integer)o));
					}
					else if (o instanceof Long)
					{
						result = result.add(new BigDecimal((Long)o));
					}
					else
					{
						System.out.println("Unknown class type in integer summation.");
						System.exit(1);
					}
				}
				else
				{
					Double o = (Double)row.get(pos);
					result = result.add(new BigDecimal((Double)o));
				}
			}
			
			//System.out.println("SumThread is terminating.");
		}
		
		public void close()
		{
		}
	}
	
	protected class SumHashThread extends AggregateResultThread
	{
		protected volatile ConcurrentHashMap<ArrayList<Object>, AtomicReference<BigDecimal>> results = new ConcurrentHashMap<ArrayList<Object>, AtomicReference<BigDecimal>>(NUM_GROUPS, 0.75f, ResourceManager.cpus * 6);
		protected HashMap<String, Integer> cols2Pos;
		protected int pos;
		
		public SumHashThread(HashMap<String, Integer> cols2Pos)
		{
			this.cols2Pos = cols2Pos;
			try
			{
				pos = this.cols2Pos.get(input);
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.err.println(cols2Pos);
				System.err.println(input);
				System.exit(1);
			}
		}
		
		//@Parallel
		public void put(ArrayList<Object> row, ArrayList<Object> group)
		{
			Object o = row.get(pos);
			Double val;
			if (o instanceof Integer)
			{
				val = ResourceManager.internDouble(new Double((Integer)o));
			}
			else if (o instanceof Long)
			{
				val = ResourceManager.internDouble(new Double((Long)o));
			}
			else
			{
				val = (Double)o;
			}
			
			AtomicReference ad = results.get(group);
			if (ad != null)
			{
				try
				{
					addToSum(group, val);
				}
				catch(Exception e)
				{
					e.printStackTrace();
					System.out.println("Error calling addAndGet() in SumOperator");
					System.out.println("ad = " + ad);
					System.out.println("val = " + val);
					System.exit(1);
				}
				return;
			}
			if (results.putIfAbsent(group, new AtomicReference(new BigDecimal(val))) != null)
			{
				addToSum(group, val);
			}
		}
		
		public Object getResult(ArrayList<Object> keys)
		{
			if (isInt)
			{
				return ResourceManager.internLong(new Long(results.get(keys).get().longValue()));
			}
			
			return ResourceManager.internDouble(new Double(results.get(keys).get().doubleValue()));
		}
		
		public void close()
		{}
		
		public void addToSum(ArrayList<Object> key, double amount) 
		{
			BigDecimal val = new BigDecimal(amount);
		    AtomicReference<BigDecimal> newSum = results.get(key);
		    for (;;) 
		    {
		       BigDecimal oldVal = newSum.get();
		       if (newSum.compareAndSet(oldVal, oldVal.add(val)))
		            return;
		    }
		}
	}
}
