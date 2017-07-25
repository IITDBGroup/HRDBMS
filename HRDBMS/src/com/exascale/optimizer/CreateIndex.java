package com.exascale.optimizer;

import java.util.ArrayList;
import java.util.List;

public class CreateIndex extends SQLStatement
{
	private final TableName index;
	private final TableName table;
	private final List<IndexDef> cols;
	private final boolean unique;

	public CreateIndex(final TableName index, final TableName table, final List<IndexDef> cols, final boolean unique)
	{
		this.index = index;
		this.table = table;
		this.cols = cols;
		this.unique = unique;
	}

	public List<IndexDef> getCols()
	{
		return cols;
	}

	public TableName getIndex()
	{
		return index;
	}

	public TableName getTable()
	{
		return table;
	}

	public boolean getUnique()
	{
		return unique;
	}
}
