package com.exascale.optimizer;

import java.util.ArrayList;
import java.util.List;

/** Represents a common table expression */
public class CTE
{
	private final String name;
	private final List<Column> cols;
	private final FullSelect select;

	public CTE(final String name, final List<Column> cols, final FullSelect select)
	{
		this.name = name;
		this.cols = cols;
		this.select = select;
	}

	public List<Column> getCols()
	{
		return cols;
	}

	public String getName()
	{
		return name;
	}

	public FullSelect getSelect()
	{
		return select;
	}
}
