package com.exascale.optimizer;

public class Runstats extends SQLStatement
{
	private TableName table;
	
	public Runstats(TableName table)
	{
		this.table = table;
	}
	
	public TableName getTable()
	{
		return table;
	}
}
