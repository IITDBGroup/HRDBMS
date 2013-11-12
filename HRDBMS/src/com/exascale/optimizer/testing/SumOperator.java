package com.exascale.optimizer.testing;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

import com.exascale.optimizer.testing.ResourceManager.DiskBackedArray;

public class SumOperator implements AggregateOperator, Serializable
{
	private String input;
	private String output;
	private MetaData meta;
	private boolean isInt;
	
	public SumOperator(String input, String output, MetaData meta, boolean isInt)
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
		return new SumThread(rows, cols2Pos);
	}
	
	public AggregateResultThread getHashThread(HashMap<String, Integer> cols2Pos)
	{
		return new SumHashThread(cols2Pos);
	}

	private class SumThread extends AggregateResultThread
	{
		private DiskBackedArray rows;
		private HashMap<String, Integer> cols2Pos;
		private double result;
		
		public SumThread(DiskBackedArray rows, HashMap<String, Integer> cols2Pos)
		{
			this.rows = rows;
			this.cols2Pos = cols2Pos;
		}
		
		public Object getResult()
		{
			if (isInt)
			{
				return new Long((long)result);
			}
			
			return new Double(result);
		}
		
		public void run()
		{
			int pos = cols2Pos.get(input);
			result = 0;
			
			for (Object orow : rows)
			{
				ArrayList<Object> row = (ArrayList<Object>)orow;
				if (isInt)
				{
					Object o = row.get(pos);
					if (o instanceof Integer)
					{
						result += (Integer)o;
					}
					else if (o instanceof Long)
					{
						result += (Long)o;
					}
					else
					{
						System.out.println("Unknown class type in integer summation.");
						System.exit(1);
					}
				}
				else
				{
					Double o = (Double)row.get(pos);
					result += o;
				}
			}
			
			//System.out.println("SumThread is terminating.");
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
	
	private class SumHashThread extends AggregateResultThread
	{
		private volatile ConcurrentHashMap<ArrayList<Object>, AtomicDouble> results = new ConcurrentHashMap<ArrayList<Object>, AtomicDouble>();
		private HashMap<String, Integer> cols2Pos;
		private int pos;
		
		public SumHashThread(HashMap<String, Integer> cols2Pos)
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
			
			AtomicDouble ad = results.get(group);
			if (ad != null)
			{
				try
				{
					ad.addAndGet(val);
				}
				catch(Exception e)
				{
					e.printStackTrace();
					System.out.println("Error calling addAndGet() in SumOperator");
					System.out.println("ad = " + ad);
					System.out.println("val = " + val);
					System.exit(1);
				}
				return;
			}
			if (results.putIfAbsent(group, new AtomicDouble(val)) != null)
			{
				results.get(group).addAndGet(val);
			}
		}
		
		public Object getResult(ArrayList<Object> keys)
		{
			if (isInt)
			{
				return new Long((long)results.get(keys).doubleValue());
			}
			
			return results.get(keys).doubleValue();
		}
		
		public void close()
		{}
	}
}
