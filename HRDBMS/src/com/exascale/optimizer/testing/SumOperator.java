package com.exascale.optimizer.testing;

import java.io.Serializable;

import com.exascale.optimizer.testing.ResourceManager.DiskBackedALOHashMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import com.exascale.optimizer.testing.ResourceManager.DiskBackedArray;

public final class SumOperator implements AggregateOperator, Serializable
{
	protected String input;
	protected String output;
	protected MetaData meta;
	protected boolean isInt;
	protected int NUM_GROUPS = 16;
	
	public SumOperator(String input, String output, MetaData meta, boolean isInt)
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
	
	public SumOperator clone()
	{
		SumOperator retval = new SumOperator(input, output, meta, isInt);
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
		return new SumThread(rows, cols2Pos);
	}
	
	public AggregateResultThread getHashThread(HashMap<String, Integer> cols2Pos)
	{
		return new SumHashThread(cols2Pos);
	}

	protected final class SumThread extends AggregateResultThread
	{
		protected ArrayList<ArrayList<Object>> rows;
		protected HashMap<String, Integer> cols2Pos;
		protected double result;
		
		public SumThread(ArrayList<ArrayList<Object>> rows, HashMap<String, Integer> cols2Pos)
		{
			this.rows = rows;
			this.cols2Pos = cols2Pos;
		}
		
		public Object getResult()
		{
			if (isInt)
			{
				return (long)result;
			}
			
			return result;
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
					result += (Double)o;
				}
			}
			
			//System.out.println("SumThread is terminating.");
		}
		
		public void close()
		{
		}
	}
	
	protected final class SumHashThread extends AggregateResultThread
	{
		//protected final DiskBackedALOHashMap<AtomicDouble> results = new DiskBackedALOHashMap<AtomicDouble>(NUM_GROUPS > 0 ? NUM_GROUPS : 16);
		protected final ConcurrentHashMap<ArrayList<Object>, AtomicDouble> results = new ConcurrentHashMap<ArrayList<Object>, AtomicDouble>(NUM_GROUPS > 0 ? NUM_GROUPS : 16, 1.0f);
		protected final HashMap<String, Integer> cols2Pos;
		protected int pos;
		
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
				System.err.println(input);
				System.exit(1);
			}
		}
		
		//@Parallel
		public final void put(ArrayList<Object> row, ArrayList<Object> group)
		{
			Object o = null;
			try
			{
				o = row.get(pos);
			}
			catch(Exception e)
			{
				System.out.println("Pos is " + pos);
				System.out.println("Cols2Pos is " + cols2Pos);
				System.out.println("Input is " + input);
				System.out.println("Row is " + row);
				e.printStackTrace();
				System.exit(1);
			}
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
					addToSum(ad, val);
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
				addToSum(results.get(group), val);
			}
		}
		
		public Object getResult(ArrayList<Object> keys)
		{
			if (isInt)
			{
				return (long)results.get(keys).get();
			}
			
			try
			{
				AtomicDouble ad = results.get(keys);
				return ad.get();
			}
			catch(Exception e)
			{
				System.out.println("Error looking up result for key = " + keys + " in SumOperator");
				for (Object key : keys)
				{
					System.out.println("Types are " + key.getClass());
				}
				for (ArrayList<Object> keys2 : results.keySet())
				{
					for (Object key :keys2)
					{
						System.out.println("Result types are " + key.getClass());
					}
					break;
				}
				System.exit(1);
			}
			return null;
		}
		
		public void close()
		{
			//results.close();
		}
		
		public final void addToSum(AtomicDouble ad, double amount) 
		{
		    AtomicDouble newSum = ad;
		    for (;;) 
		    {
		       double oldVal = newSum.get();
		       if (newSum.compareAndSet(oldVal, oldVal + amount))
		            return;
		    }
		}
	}
}
