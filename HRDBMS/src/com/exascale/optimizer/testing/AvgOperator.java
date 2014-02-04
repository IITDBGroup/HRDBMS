package com.exascale.optimizer.testing;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.exascale.optimizer.testing.AggregateOperator.AggregateResultThread;
import com.exascale.optimizer.testing.ResourceManager.DiskBackedArray;

public class AvgOperator implements AggregateOperator, Serializable
{
	protected String input;
	protected String output;
	protected MetaData meta;
	protected int NUM_GROUPS = 16;
	
	public AvgOperator(String input, String output, MetaData meta)
	{
		this.input = input;
		this.output = output;
		this.meta = meta;
	}
	
	public void setNumGroups(int groups)
	{
		NUM_GROUPS = groups;
	}
	
	public AvgOperator clone()
	{
		return new AvgOperator(input, output, meta);
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
		return "FLOAT";
	}

	@Override
	public AggregateResultThread newProcessingThread(ArrayList<ArrayList<Object>> rows, HashMap<String, Integer> cols2Pos) 
	{
		return new AvgThread(rows, cols2Pos);
	}
	
	public AggregateResultThread getHashThread(HashMap<String, Integer> cols2Pos)
	{
		return new AvgHashThread(cols2Pos);
	}

	protected class AvgThread extends AggregateResultThread
	{
		protected ArrayList<ArrayList<Object>> rows;
		protected HashMap<String, Integer> cols2Pos;
		protected BigDecimal result;
		
		public AvgThread(ArrayList<ArrayList<Object>> rows, HashMap<String, Integer> cols2Pos)
		{
			this.rows = rows;
			this.cols2Pos = cols2Pos;
		}
		
		public Double getResult()
		{
			return result.doubleValue();
		}
		
		public void run()
		{
			int pos = cols2Pos.get(input);
			long numRows = 0;
			result = new BigDecimal(0);
			
			for (Object orow : rows)
			{
				ArrayList<Object> row = (ArrayList<Object>)orow;
				numRows++;
				result = result.add(new BigDecimal((Double)row.get(pos)));
			}
			
			result = result.divide(new BigDecimal(numRows), 16, RoundingMode.HALF_UP);
			//System.out.println("AvgThread is terminating.");
		}
		
		public void close()
		{
		}
	}
	
	protected class AvgHashThread extends AggregateResultThread
	{
		protected volatile ConcurrentHashMap<ArrayList<Object>, AtomicReference<BigDecimal>> sums = new ConcurrentHashMap<ArrayList<Object>, AtomicReference<BigDecimal>>(NUM_GROUPS, 0.75f, ResourceManager.cpus * 6);
		protected volatile ConcurrentHashMap<ArrayList<Object>, AtomicLong> counts = new ConcurrentHashMap<ArrayList<Object>, AtomicLong>(NUM_GROUPS, 0.75f, ResourceManager.cpus * 6);
		protected HashMap<String, Integer> cols2Pos;
		protected int pos;
		
		public AvgHashThread(HashMap<String, Integer> cols2Pos)
		{
			this.cols2Pos = cols2Pos;
			pos = cols2Pos.get(input);
		}
		
		//@Parallel
		public void put(ArrayList<Object> row, ArrayList<Object> group)
		{
			Double val = (Double)row.get(pos);
			AtomicReference<BigDecimal> ad = sums.get(group);
			if (ad != null)
			{
				addToSum(group, val);
			}
			else
			{
				if (sums.putIfAbsent(group, new AtomicReference(new BigDecimal(val))) != null)
				{
					addToSum(group, val);
				}
			}
			
			AtomicLong al = counts.get(group);
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
		
		public Object getResult(ArrayList<Object> keys)
		{
			return ResourceManager.internDouble(new Double(sums.get(keys).get().divide(new BigDecimal(counts.get(keys).get()), 16, RoundingMode.HALF_UP).doubleValue()));
		}
		
		public void close()
		{}
		
		public void addToSum(ArrayList<Object> key, double amount) 
		{
			BigDecimal val = new BigDecimal(amount);
		    AtomicReference<BigDecimal> newSum = sums.get(key);
		    for (;;) 
		    {
		       BigDecimal oldVal = newSum.get();
		       if (newSum.compareAndSet(oldVal, oldVal.add(val)))
		            return;
		    }
		}
	}
}
