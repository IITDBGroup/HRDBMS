package com.exascale.optimizer.testing;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.exascale.optimizer.testing.AggregateOperator.AggregateResultThread;
import com.exascale.optimizer.testing.ResourceManager.DiskBackedArray;

public class CountOperator implements AggregateOperator, Serializable
{
	private String output;
	private MetaData meta;
	
	public CountOperator(String output, MetaData meta)
	{
		this.output = output;
		this.meta = meta;
	}
	
	public String getInputColumn()
	{
		return "";
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

	@Override
	public AggregateResultThread newProcessingThread(DiskBackedArray rows, HashMap<String, Integer> cols2Pos) 
	{
		return new CountThread(rows, cols2Pos);
	}
	
	public AggregateResultThread getHashThread(HashMap<String, Integer> cols2Pos)
	{
		return new CountHashThread(cols2Pos);
	}

	private class CountThread extends AggregateResultThread
	{
		private DiskBackedArray rows;
		private long result;
		
		public CountThread(DiskBackedArray rows, HashMap<String, Integer> cols2Pos)
		{
			this.rows = rows;
		}
		
		public Object getResult()
		{
			return new Long(result);
		}
		
		public void run()
		{
			result = rows.size();
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
	
	private class CountHashThread extends AggregateResultThread
	{
		private volatile ConcurrentHashMap<ArrayList<Object>, AtomicLong> results = new ConcurrentHashMap<ArrayList<Object>, AtomicLong>();
		private HashMap<String, Integer> cols2Pos;
		
		public CountHashThread(HashMap<String, Integer> cols2Pos)
		{
			this.cols2Pos = cols2Pos;
		}
		
		public void put(ArrayList<Object> row, ArrayList<Object> group)
		{
			AtomicLong al = results.get(group);
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
		
		public Object getResult(ArrayList<Object> keys)
		{
			return ((AtomicLong)results.get(keys)).longValue();
		}
		
		public void close()
		{}
	}
}
