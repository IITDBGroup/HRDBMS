package com.exascale.optimizer;

public class DropIndex extends SQLStatement
{
	private TableName index;
	
	public DropIndex(TableName index)
	{
		this.index = index;
	}
	
	public TableName getIndex()
	{
		return index;
	}
}
