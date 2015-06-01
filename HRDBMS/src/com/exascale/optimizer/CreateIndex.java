package com.exascale.optimizer;

import java.util.ArrayList;

public class CreateIndex extends SQLStatement
{
	private final TableName index;
	private final TableName table;
	private final ArrayList<IndexDef> cols;
	private final boolean unique;

	public CreateIndex(TableName index, TableName table, ArrayList<IndexDef> cols, boolean unique)
	{
		this.index = index;
		this.table = table;
		this.cols = cols;
		this.unique = unique;
	}

	public ArrayList<IndexDef> getCols()
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
