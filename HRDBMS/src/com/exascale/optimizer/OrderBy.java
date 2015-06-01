package com.exascale.optimizer;

import java.util.ArrayList;

public class OrderBy
{
	private final ArrayList<SortKey> keys;

	public OrderBy(ArrayList<SortKey> keys)
	{
		this.keys = keys;
	}

	@Override
	public OrderBy clone()
	{
		ArrayList<SortKey> newKeys = new ArrayList<SortKey>();
		for (SortKey key : keys)
		{
			newKeys.add(key.clone());
		}

		return new OrderBy(newKeys);
	}

	public ArrayList<SortKey> getKeys()
	{
		return keys;
	}
}
