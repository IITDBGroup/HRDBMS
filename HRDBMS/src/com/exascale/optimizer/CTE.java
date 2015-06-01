package com.exascale.optimizer;

import java.util.ArrayList;

public class CTE
{
	private final String name;
	private final ArrayList<Column> cols;
	private final FullSelect select;

	public CTE(String name, ArrayList<Column> cols, FullSelect select)
	{
		this.name = name;
		this.cols = cols;
		this.select = select;
	}

	public ArrayList<Column> getCols()
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
