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
	protected String input;
	protected String output;
	protected MetaData meta;
	protected int NUM_GROUPS = 16;
	protected int childCard = 16 * 16;
	
	public CountDistinctOperator(String input, String output, MetaData meta)
	{
		this.input = input;
		this.output = output;
		this.meta = meta;
	}
	
	public void setNumGroups(int groups)
	{
		NUM_GROUPS = groups;
	}
	
	public void setChildCard(int card)
	{
		childCard = card;
	}
	
	public CountDistinctOperator clone()
	{
		return new CountDistinctOperator(input, output, meta);
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
		return "LONG";
	}

	@Override
	public AggregateResultThread newProcessingThread(ArrayList<ArrayList<Object>> rows, HashMap<String, Integer> cols2Pos) 
	{
		return new CountDistinctThread(rows, cols2Pos);
	}
	
	public AggregateResultThread getHashThread(HashMap<String, Integer> cols2Pos)
	{
		return new CountDistinctHashThread(cols2Pos);
	}

	protected class CountDistinctThread extends AggregateResultThread
	{
		protected ArrayList<ArrayList<Object>> rows;
		protected HashMap<String, Integer> cols2Pos;
		protected long result;
		protected int pos;
		protected HashSet<Object> distinct = new HashSet<Object>();
		
		public CountDistinctThread(ArrayList<ArrayList<Object>> rows, HashMap<String, Integer> cols2Pos)
		{
			this.cols2Pos = cols2Pos;
			this.rows = rows;
			pos = cols2Pos.get(input);
		}
		
		public Object getResult()
		{
			return ResourceManager.internLong(new Long(result));
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
		}
	}
	
	protected class CountDistinctHashThread extends AggregateResultThread
	{
		protected volatile ConcurrentHashMap<ArrayList<Object>, AtomicLong> results = new ConcurrentHashMap<ArrayList<Object>, AtomicLong>(NUM_GROUPS, 0.75f, ResourceManager.cpus * 6);
		protected volatile DiskBackedHashSet hashSet = ResourceManager.newDiskBackedHashSet(false, childCard);
		protected HashMap<String, Integer> cols2Pos;
		int pos;
		
		public CountDistinctHashThread(HashMap<String, Integer> cols2Pos)
		{
			this.cols2Pos = cols2Pos;
			pos = cols2Pos.get(input);
		}
		
		//@Parallel
		public void put(ArrayList<Object> row, ArrayList<Object> group)
		{
			ArrayList<Object> consolidated = new ArrayList<Object>();
			consolidated.addAll(group);
			consolidated.add(row.get(pos));
			if (hashSet.add(consolidated))
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
		}
	
		public Object getResult(ArrayList<Object> keys)
		{
			return ((AtomicLong)results.get(keys)).longValue();
		}
		
		public void close()
		{
			try
			{
				hashSet.close();
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
		}
	}
}
