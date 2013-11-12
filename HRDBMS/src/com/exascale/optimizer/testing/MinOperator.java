package com.exascale.optimizer.testing;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

import com.exascale.optimizer.testing.ResourceManager.DiskBackedArray;

public class MinOperator implements AggregateOperator, Serializable
{
	private String input;
	private String output;
	private MetaData meta;
	private boolean isInt;
	
	public MinOperator(String input, String output, MetaData meta, boolean isInt)
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
		return new MinThread(rows, cols2Pos);
	}
	
	public AggregateResultThread getHashThread(HashMap<String, Integer> cols2Pos)
	{
		return new MinHashThread(cols2Pos);
	}

	private class MinThread extends AggregateResultThread
	{
		private DiskBackedArray rows;
		private HashMap<String, Integer> cols2Pos;
		private double min;
		
		public MinThread(DiskBackedArray rows, HashMap<String, Integer> cols2Pos)
		{
			this.rows = rows;
			this.cols2Pos = cols2Pos;
		}
		
		public Object getResult()
		{
			if (isInt)
			{
				return new Long((long)min);
			}
			
			return new Double(min);
		}
		
		public void run()
		{
			int pos = cols2Pos.get(input);
			min = Double.POSITIVE_INFINITY;
			
			for (Object orow : rows)
			{
				ArrayList<Object> row = (ArrayList<Object>)orow;
				if (isInt)
				{
					Object o = row.get(pos);
					if (o instanceof Integer)
					{
						if ((Integer)o < min)
						{
							min = (Integer)o;
						}
					}
					else if (o instanceof Long)
					{
						if ((Long)o < min)
						{
							min = (Long)o;
						}
					}
					else
					{
						System.out.println("Unknown class type in MinOperator.");
						System.exit(1);
					}
				}
				else
				{
					Double o = (Double)row.get(pos);
					if (o < min)
					{
						min = o;
					}
				}
			}
			
			//System.out.println("MinThread is terminating.");
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
	
	private class MinHashThread extends AggregateResultThread
	{
		private volatile ConcurrentHashMap<ArrayList<Object>, AtomicDouble> mins = new ConcurrentHashMap<ArrayList<Object>, AtomicDouble>();
		private HashMap<String, Integer> cols2Pos;
		private int pos;
		
		public MinHashThread(HashMap<String, Integer> cols2Pos)
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
			
			AtomicDouble ad = mins.get(group);
			if (ad != null)
			{
				double min = ad.get();
				if (val < min)
				{
					while (!ad.compareAndSet(min, val))
					{
						min = ad.get();
						if (val >= min)
						{
							break;
						}
					}
				}
				
				return;
			}

			if (mins.putIfAbsent(group, new AtomicDouble(val)) != null)
			{
				ad = mins.get(group);
				double min = ad.get();
				if (val < min)
				{
					while (!ad.compareAndSet(min, val))
					{
						min = ad.get();
						if (val >= min)
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
				return new Long((long)mins.get(keys).doubleValue());
			}
			
			return mins.get(keys).doubleValue();
		}
		
		public void close()
		{}
	}
}
