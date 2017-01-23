package com.exascale.optimizer;

import java.util.ArrayList;

public class GroupBy
{
	private final ArrayList<Column> cols;

	public GroupBy(ArrayList<Column> cols)
	{
		this.cols = cols;
	}

	@Override
	public GroupBy clone()
	{
		ArrayList<Column> newCols = new ArrayList<Column>();
		for (Column col : cols)
		{
			newCols.add(col.clone());
		}

		return new GroupBy(newCols);
	}

	public ArrayList<Column> getCols()
	{
		return cols;
	}
}
