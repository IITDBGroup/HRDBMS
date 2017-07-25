package com.exascale.optimizer;

import java.util.ArrayList;
import java.util.List;

public class GroupBy
{
	private final List<Column> cols;

	public GroupBy(final List<Column> cols)
	{
		this.cols = cols;
	}

	@Override
	public GroupBy clone()
	{
		final List<Column> newCols = new ArrayList<Column>();
		for (final Column col : cols)
		{
			newCols.add(col.clone());
		}

		return new GroupBy(newCols);
	}

	public List<Column> getCols()
	{
		return cols;
	}
}
