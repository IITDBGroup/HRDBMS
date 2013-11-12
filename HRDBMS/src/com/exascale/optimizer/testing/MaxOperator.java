package com.exascale.optimizer.testing;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

import com.exascale.optimizer.testing.ResourceManager.DiskBackedArray;

public class MaxOperator implements AggregateOperator, Serializable
{
	private String input;
	private String output;
	private MetaData meta;
	private boolean isInt;
	
	public MaxOperator(String input, String output, MetaData meta, boolean isInt)
	{
		this.input = input;
		this.output = output;
		this.meta = meta;
		this.isInt = isInt;
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
	public AggregateResultThread newProcessingThread(DiskBackedArray rows, HashMap<String, Integer> cols2Pos) 
	{
		return new MaxThread(rows, cols2Pos);
	}
	
	public AggregateResultThread getHashThread(HashMap<String, Integer> cols2Pos)
	{
		return new MaxHashThread(cols2Pos);
	}

	private class MaxThread extends AggregateResultThread
	{
		private DiskBackedArray rows;
		private HashMap<String, Integer> cols2Pos;
		private double max;
		
		public MaxThread(DiskBackedArray rows, HashMap<String, Integer> cols2Pos)
		{
			this.rows = rows;
			this.cols2Pos = cols2Pos;
		}
		
		public Object getResult()
		{
			if (isInt)
			{
				return new Long((long)max);
			}
			
			return new Double(max);
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
	
	private class MaxHashThread extends AggregateResultThread
	{
		private volatile ConcurrentHashMap<ArrayList<Object>, AtomicDouble> maxes = new ConcurrentHashMap<ArrayList<Object>, AtomicDouble>();
		private HashMap<String, Integer> cols2Pos;
		private int pos;
		
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
		
		public void put(ArrayList<Object> row, ArrayList<Object> group)
		{
			Object o = row.get(pos);
			Double val;
			if (o instanceof Integer)
			{
				val = new Double((Integer)o);
			}
			else if (o instanceof Long)
			{
				val = new Double((Long)o);
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
				return new Long((long)maxes.get(keys).doubleValue());
			}
			
			return maxes.get(keys).doubleValue();
		}
		
		public void close()
		{}
	}
}
