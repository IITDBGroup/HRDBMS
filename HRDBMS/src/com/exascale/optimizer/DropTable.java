package com.exascale.optimizer;

public class DropTable extends SQLStatement
{
	private TableName table;
	
	public DropTable(TableName table)
	{
		this.table = table;
	}
	
	public TableName getTable()
	{
		return table;
	}
}
