package com.exascale.optimizer;

import java.util.ArrayList;

public class OrderBy
{
	private final ArrayList<SortKey> keys;

	public OrderBy(final ArrayList<SortKey> keys)
	{
		this.keys = keys;
	}

	@Override
	public OrderBy clone()
	{
		final ArrayList<SortKey> newKeys = new ArrayList<SortKey>();
		for (final SortKey key : keys)
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
