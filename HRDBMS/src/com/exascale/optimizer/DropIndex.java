package com.exascale.optimizer;

public class DropIndex extends SQLStatement
{
	private final TableName index;

	public DropIndex(final TableName index)
	{
		this.index = index;
	}

	public TableName getIndex()
	{
		return index;
	}
}
