package com.exascale.optimizer;

import java.util.ArrayList;

public class PrimaryKey
{
	private final ArrayList<Column> cols;

	public PrimaryKey(final ArrayList<Column> cols)
	{
		this.cols = cols;
	}

	public ArrayList<Column> getCols()
	{
		return cols;
	}
}
