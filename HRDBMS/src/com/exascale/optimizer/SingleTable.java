package com.exascale.optimizer;

public class SingleTable
{
	private TableName table;
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
	
	public TableName getName()
	{
		return table;
	}
	
	public String getAlias()
	{
		return alias;
	}
}
