package com.exascale.optimizer.testing;

import java.util.ArrayList;
import java.util.HashMap;

import com.exascale.optimizer.testing.ResourceManager.DiskBackedArray;

public interface AggregateOperator 
{
	public String outputColumn();
	public String getInputColumn();
	public String outputType();
	public AggregateResultThread newProcessingThread(DiskBackedArray rows, HashMap<String, Integer> cols2Pos);
	public AggregateResultThread getHashThread(HashMap<String, Integer> cols2Pos);
	
	public abstract class AggregateResultThread extends Thread
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
	}
}
