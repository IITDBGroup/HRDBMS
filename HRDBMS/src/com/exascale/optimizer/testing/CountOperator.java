package com.exascale.optimizer.testing;

import java.io.Serializable;
import com.exascale.optimizer.testing.ResourceManager.DiskBackedALOHashMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.exascale.optimizer.testing.AggregateOperator.AggregateResultThread;
import com.exascale.optimizer.testing.ResourceManager.DiskBackedArray;

public final class CountOperator implements AggregateOperator, Serializable
{
	protected String output;
	protected MetaData meta;
	protected String input;
	protected int NUM_GROUPS = 16;
	
	public CountOperator(String output, MetaData meta)
	{
		this.output = output;
		this.meta = meta;
	}
	
	public void setNumGroups(int groups)
	{
		NUM_GROUPS = groups;
	}
	
	public CountOperator(String input, String output, MetaData meta)
	{
		this.input = input;
		this.output = output;
		this.meta = meta;
	}
	
	public CountOperator clone()
	{
		return new CountOperator(input, output, meta);
	}
	
	public void setInputColumn(String col)
	{
		input = col;
	}
	
	public String getInputColumn()
	{
		if (input != null)
		{
			return input;
		}
		
		return output;
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
	public AggregateResultThread newProcessingThread(ArrayList<ArrayList<Object>> rows, HashMap<String, Integer> cols2Pos) 
	{
		return new CountThread(rows, cols2Pos);
	}
	
	public AggregateResultThread getHashThread(HashMap<String, Integer> cols2Pos)
	{
		return new CountHashThread(cols2Pos);
	}

	protected final class CountThread extends AggregateResultThread
	{
		protected ArrayList<ArrayList<Object>> rows;
		protected long result;
		protected int pos;
		
		public CountThread(ArrayList<ArrayList<Object>> rows, HashMap<String, Integer> cols2Pos)
		{
			this.rows = rows;
			if (input != null)
			{
				pos = cols2Pos.get(input);
			}
		}
		
		public Object getResult()
		{
			return new Long(result);
		}
		
		public void run()
		{
			if (input == null)
			{
				result = rows.size();
			}
			else
			{
				result = 0;
				for (Object o : rows)
				{
					ArrayList<Object> row = (ArrayList<Object>)o;
					Object val = row.get(pos);
					result++; //TODO only increment if not null
				}
			}
		}
		
		public void close()
		{
		}
	}
	
	protected final class CountHashThread extends AggregateResultThread
	{
		//protected final DiskBackedALOHashMap<AtomicLong> results = new DiskBackedALOHashMap<AtomicLong>(NUM_GROUPS > 0 ? NUM_GROUPS : 16);
		protected final ConcurrentHashMap<ArrayList<Object>, AtomicLong> results = new ConcurrentHashMap<ArrayList<Object>, AtomicLong>(NUM_GROUPS > 0 ? NUM_GROUPS : 16, 1.0f);
		protected final HashMap<String, Integer> cols2Pos;
		protected int pos;
		
		public CountHashThread(HashMap<String, Integer> cols2Pos)
		{
			this.cols2Pos = cols2Pos;
			if (input != null)
			{
				pos = cols2Pos.get(input);
			}
		}
		
		//@Parallel
		public final void put(ArrayList<Object> row, ArrayList<Object> group)
		{
			Object val = row.get(pos);
			//TODO only do following if val not null
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
		{
			//results.close();
		}
	}
}
