package com.exascale.optimizer.testing;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.exascale.optimizer.testing.AggregateOperator.AggregateResultThread;
import com.exascale.optimizer.testing.ResourceManager.DiskBackedArray;
import com.exascale.optimizer.testing.ResourceManager.DiskBackedHashSet;

public class CountDistinctOperator implements AggregateOperator, Serializable
{
	private String input;
	private String output;
	private MetaData meta;
	
	public CountDistinctOperator(String input, String output, MetaData meta)
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
		return "LONG";
	}

	@Override
	public AggregateResultThread newProcessingThread(DiskBackedArray rows, HashMap<String, Integer> cols2Pos) 
	{
		return new CountDistinctThread(rows, cols2Pos);
	}
	
	public AggregateResultThread getHashThread(HashMap<String, Integer> cols2Pos)
	{
		return new CountDistinctHashThread(cols2Pos);
	}

	private class CountDistinctThread extends AggregateResultThread
	{
		private DiskBackedArray rows;
		private HashMap<String, Integer> cols2Pos;
		private long result;
		private int pos;
		private DiskBackedHashSet distinct = ResourceManager.newDiskBackedHashSet();
		
		public CountDistinctThread(DiskBackedArray rows, HashMap<String, Integer> cols2Pos)
		{
			this.cols2Pos = cols2Pos;
			this.rows = rows;
			pos = cols2Pos.get(input);
		}
		
		public Object getResult()
		{
			return new Long(result);
		}
		
		public void run()
		{
			for (Object o : rows)
			{
				ArrayList<Object> row = (ArrayList<Object>)o;
				Object val = row.get(pos);
				distinct.add(val);
			}
			
			result = distinct.size();
		}
		
		public void close()
		{
			try
			{
				rows.close();
				distinct.close();
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
		}
	}
	
	private class CountDistinctHashThread extends AggregateResultThread
	{
		private volatile ConcurrentHashMap<ArrayList<Object>, DiskBackedHashSet> results = new ConcurrentHashMap<ArrayList<Object>, DiskBackedHashSet>();
		private HashMap<String, Integer> cols2Pos;
		int pos;
		
		public CountDistinctHashThread(HashMap<String, Integer> cols2Pos)
		{
			this.cols2Pos = cols2Pos;
			pos = cols2Pos.get(input);
		}
		
		public void put(ArrayList<Object> row, ArrayList<Object> group)
		{
			DiskBackedHashSet distinct = results.get(group);
			if (distinct != null)
			{
				distinct.add(row.get(pos));
				return;
			}
			
			DiskBackedHashSet set = ResourceManager.newDiskBackedHashSet();
			results.putIfAbsent(group, set);
			DiskBackedHashSet set2 = results.get(group);
			set2.add(row.get(pos));
			if (set2 != set)
			{
				try
				{
					set.close();
				}
				catch(Exception e)
				{
					e.printStackTrace();
				}
			}
		}
	
		public Object getResult(ArrayList<Object> keys)
		{
			long retval = results.get(keys).size();
			return retval;
		}
		
		public void close()
		{
			for (DiskBackedHashSet set : results.values())
			{
				try
				{
					set.close();
				}
				catch(Exception e)
				{
					e.printStackTrace();
				}
			}
		}
	}
}
