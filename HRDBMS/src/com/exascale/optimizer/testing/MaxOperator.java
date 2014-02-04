package com.exascale.optimizer.testing;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

import com.exascale.optimizer.testing.ResourceManager.DiskBackedArray;

public class MaxOperator implements AggregateOperator, Serializable
{
	protected String input;
	protected String output;
	protected MetaData meta;
	protected boolean isInt;
	protected int NUM_GROUPS = 16;
	
	public MaxOperator(String input, String output, MetaData meta, boolean isInt)
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
	
	public MaxOperator clone()
	{
		MaxOperator retval = new MaxOperator(input, output, meta, isInt);
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
		return new MaxThread(rows, cols2Pos);
	}
	
	public AggregateResultThread getHashThread(HashMap<String, Integer> cols2Pos)
	{
		return new MaxHashThread(cols2Pos);
	}

	protected class MaxThread extends AggregateResultThread
	{
		protected ArrayList<ArrayList<Object>> rows;
		protected HashMap<String, Integer> cols2Pos;
		protected double max;
		
		public MaxThread(ArrayList<ArrayList<Object>> rows, HashMap<String, Integer> cols2Pos)
		{
			this.rows = rows;
			this.cols2Pos = cols2Pos;
		}
		
		public Object getResult()
		{
			if (isInt)
			{
				return ResourceManager.internLong(new Long((long)max));
			}
			
			return ResourceManager.internDouble(new Double(max));
		}
		
		public void run()
		{
			int pos = cols2Pos.get(input);
			max = Double.NEGATIVE_INFINITY;
			
			for (Object orow : rows)
			{
				ArrayList<Object> row = (ArrayList<Object>)orow;
				if (isInt)
				{
					Object o = row.get(pos);
					if (o instanceof Integer)
					{
						if ((Integer)o > max)
						{
							max = (Integer)o;
						}
					}
					else if (o instanceof Long)
					{
						if ((Long)o > max)
						{
							max = (Long)o;
						}
					}
					else
					{
						System.out.println("Unknown class type in MaxOperator.");
						System.exit(1);
					}
				}
				else
				{
					Double o = (Double)row.get(pos);
					if (o > max)
					{
						max = o;
					}
				}
			}
			
			//System.out.println("MaxThread is terminating.");
		}
		
		public void close()
		{
		}
	}
	
	protected class MaxHashThread extends AggregateResultThread
	{
		protected volatile ConcurrentHashMap<ArrayList<Object>, AtomicDouble> maxes = new ConcurrentHashMap<ArrayList<Object>, AtomicDouble>(NUM_GROUPS, 0.75f, ResourceManager.cpus * 6);
		protected HashMap<String, Integer> cols2Pos;
		protected int pos;
		
		public MaxHashThread(HashMap<String, Integer> cols2Pos)
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
			
			AtomicDouble ad = maxes.get(group);
			if (ad != null)
			{
				double max = ad.get();
				if (val > max)
				{
					while (!ad.compareAndSet(max, val))
					{
						max = ad.get();
						if (val <= max)
						{
							break;
						}
					}
				}
				
				return;
			}

			if (maxes.putIfAbsent(group, new AtomicDouble(val)) != null)
			{
				ad = maxes.get(group);
				double max = ad.get();
				if (val > max)
				{
					while (!ad.compareAndSet(max, val))
					{
						max = ad.get();
						if (val <= max)
						{
							break;
						}
					}
				}
					
				return;
			}
		}
		
		public Object getResult(ArrayList<Object> keys)
		{
			if (isInt)
			{
				return ResourceManager.internLong(new Long((long)maxes.get(keys).doubleValue()));
			}
			
			return maxes.get(keys).doubleValue();
		}
		
		public void close()
		{}
	}
}
