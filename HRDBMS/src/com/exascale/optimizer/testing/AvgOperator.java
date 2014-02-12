package com.exascale.optimizer.testing;

import java.io.Serializable;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.exascale.optimizer.testing.AggregateOperator.AggregateResultThread;
import com.exascale.optimizer.testing.ResourceManager.DiskBackedArray;

public final class AvgOperator implements AggregateOperator, Serializable
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

	protected final class AvgThread extends AggregateResultThread
	{
		protected ArrayList<ArrayList<Object>> rows;
		protected HashMap<String, Integer> cols2Pos;
		protected double result;
		
		public AvgThread(ArrayList<ArrayList<Object>> rows, HashMap<String, Integer> cols2Pos)
		{
			this.rows = rows;
			this.cols2Pos = cols2Pos;
		}
		
		public Double getResult()
		{
			return result;
		}
		
		public void run()
		{
			int pos = cols2Pos.get(input);
			long numRows = 0;
			result = 0;
			
			for (Object orow : rows)
			{
				ArrayList<Object> row = (ArrayList<Object>)orow;
				numRows++;
				result += (Double)row.get(pos);
			}
			
			result /= numRows;
			//System.out.println("AvgThread is terminating.");
		}
		
		public void close()
		{
		}
	}
	
	protected final class AvgHashThread extends AggregateResultThread
	{
		protected final ConcurrentHashMap<ArrayList<Object>, AtomicDouble> sums = new ConcurrentHashMap<ArrayList<Object>, AtomicDouble>(NUM_GROUPS, 0.75f, ResourceManager.cpus * 6);
		protected final ConcurrentHashMap<ArrayList<Object>, AtomicLong> counts = new ConcurrentHashMap<ArrayList<Object>, AtomicLong>(NUM_GROUPS, 0.75f, ResourceManager.cpus * 6);
		protected final HashMap<String, Integer> cols2Pos;
		protected final int pos;
		
		public AvgHashThread(HashMap<String, Integer> cols2Pos)
		{
			this.cols2Pos = cols2Pos;
			pos = cols2Pos.get(input);
		}
		
		//@Parallel
		public final void put(ArrayList<Object> row, ArrayList<Object> group)
		{
			Double val = (Double)row.get(pos);
			AtomicDouble ad = sums.get(group);
			if (ad != null)
			{
				addToSum(group, val);
			}
			else
			{
				if (sums.putIfAbsent(group, new AtomicDouble(val)) != null)
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
			return new Double(sums.get(keys).get() / counts.get(keys).get());
		}
		
		public void close()
		{}
		
		public final void addToSum(ArrayList<Object> key, double amount) 
		{
		    AtomicDouble newSum = sums.get(key);
		    for (;;) 
		    {
		       double oldVal = newSum.get();
		       if (newSum.compareAndSet(oldVal, oldVal + amount))
		            return;
		    }
		}
	}
}
