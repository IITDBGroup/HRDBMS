package com.exascale.optimizer;

import java.util.ArrayList;
import java.util.List;

public class PrimaryKey
{
	private final List<Column> cols;

	public PrimaryKey(final List<Column> cols)
	{
		this.cols = cols;
	}

	public List<Column> getCols()
	{
		return cols;
	}
}
