package com.exascale.optimizer;

public class SingleTable
{
	private final TableName table;
	private String alias;

	public SingleTable(TableName table)
	{
		this.table = table;
	}

	public SingleTable(TableName table, String alias)
	{
		this.table = table;
		this.alias = alias;
	}

	@Override
	public SingleTable clone()
	{
		return new SingleTable(table.clone(), alias);
	}

	public String getAlias()
	{
		return alias;
	}

	public TableName getName()
	{
		return table;
	}
}
