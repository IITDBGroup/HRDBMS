package com.exascale.optimizer.testing;

import java.util.ArrayList;
import java.util.HashMap;

import com.exascale.optimizer.testing.ResourceManager.DiskBackedArray;

public interface AggregateOperator 
{
	public String outputColumn();
	public String getInputColumn();
	public String outputType();
	public AggregateResultThread newProcessingThread(ArrayList<ArrayList<Object>> rows, HashMap<String, Integer> cols2Pos);
	public AggregateResultThread getHashThread(HashMap<String, Integer> cols2Pos);
	public AggregateOperator clone();
	public void setInputColumn(String col);
	public void setNumGroups(int groups);
	
	public abstract class AggregateResultThread extends ThreadPoolThread
	{
		public Object getResult()
		{
			return null;
		}
		
		public Object getResult(ArrayList<Object> keys)
		{
			return null;
		}
		
		public void put(ArrayList<Object> row, ArrayList<Object> groupKeys)
		{
		}
		
		public void close()
		{}
		
		public void run()
		{}
	}
}
