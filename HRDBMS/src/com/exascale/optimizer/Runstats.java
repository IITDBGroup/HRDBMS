package com.exascale.optimizer;

public class Runstats extends SQLStatement
{
	private final TableName table;

	public Runstats(final TableName table)
	{
		this.table = table;
	}

	public TableName getTable()
	{
		return table;
	}
}
