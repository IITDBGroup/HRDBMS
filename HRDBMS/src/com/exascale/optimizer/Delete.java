package com.exascale.optimizer;

public class Delete extends SQLStatement
{
	private TableName table;
	private Where where;
	
	public Delete(TableName table, Where where)
	{
		this.table = table;
		this.where = where;
	}
	
	public TableName getTable()
	{
		return table;
	}
	
	public Where getWhere()
	{
		return where;
	}
}
