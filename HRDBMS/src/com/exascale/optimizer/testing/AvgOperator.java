package com.exascale.optimizer.testing;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.exascale.optimizer.testing.AggregateOperator.AggregateResultThread;
import com.exascale.optimizer.testing.ResourceManager.DiskBackedArray;

public class AvgOperator implements AggregateOperator, Serializable
{
	private String input;
	private String output;
	private MetaData meta;
	
	public AvgOperator(String input, String output, MetaData meta)
	{
		this.input = input;
		this.output = output;
		this.meta = meta;
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
	public AggregateResultThread newProcessingThread(DiskBackedArray rows, HashMap<String, Integer> cols2Pos) 
	{
		return new AvgThread(rows, cols2Pos);
	}
	
	public AggregateResultThread getHashThread(HashMap<String, Integer> cols2Pos)
	{
		return new AvgHashThread(cols2Pos);
	}

	private class AvgThread extends AggregateResultThread
	{
		private DiskBackedArray rows;
		private HashMap<String, Integer> cols2Pos;
		private double result;
		
		public AvgThread(DiskBackedArray rows, HashMap<String, Integer> cols2Pos)
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
			
			result = result / (1.0 * numRows);
			//System.out.println("AvgThread is terminating.");
		}
		
		public void close()
		{
			try
			{
				rows.close();
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
		}
	}
	
	private class AvgHashThread extends AggregateResultThread
	{
		private volatile ConcurrentHashMap<ArrayList<Object>, AtomicDouble> sums = new ConcurrentHashMap<ArrayList<Object>, AtomicDouble>();
		private volatile ConcurrentHashMap<ArrayList<Object>, AtomicLong> counts = new ConcurrentHashMap<ArrayList<Object>, AtomicLong>();
		private HashMap<String, Integer> cols2Pos;
		private int pos;
		
		public AvgHashThread(HashMap<String, Integer> cols2Pos)
		{
			this.cols2Pos = cols2Pos;
			pos = cols2Pos.get(input);
		}
		
		public void put(ArrayList<Object> row, ArrayList<Object> group)
		{
			Double val = (Double)row.get(pos);
			AtomicDouble ad = sums.get(group);
			if (ad != null)
			{
				ad.addAndGet(val);
			}
			else
			{
				if (sums.putIfAbsent(group, new AtomicDouble(val)) != null)
				{
					sums.get(group).addAndGet(val);
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
			return new Double(sums.get(keys).doubleValue() / counts.get(keys).doubleValue());
		}
		
		public void close()
		{}
	}
}
