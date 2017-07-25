package com.exascale.optimizer;

import java.util.ArrayList;
import java.util.List;

public class OrderBy
{
	private final List<SortKey> keys;

	public OrderBy(final List<SortKey> keys)
	{
		this.keys = keys;
	}

	@Override
	public OrderBy clone()
	{
		final List<SortKey> newKeys = new ArrayList<SortKey>();
		for (final SortKey key : keys)
		{
			newKeys.add(key.clone());
		}

		return new OrderBy(newKeys);
	}

	public List<SortKey> getKeys()
	{
		return keys;
	}
}
