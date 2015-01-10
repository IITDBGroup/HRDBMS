package com.exascale.optimizer;

import java.util.ArrayList;

public class OrderBy
{
	private ArrayList<SortKey> keys;
	
	public OrderBy(ArrayList<SortKey> keys)
	{
		this.keys = keys;
	}
	
	public ArrayList<SortKey> getKeys()
	{
		return keys;
	}
	
	public OrderBy clone()
	{
		ArrayList<SortKey> newKeys = new ArrayList<SortKey>();
		for (SortKey key : keys)
		{
			newKeys.add(key.clone());
		}
		
		return new OrderBy(newKeys);
	}
}
