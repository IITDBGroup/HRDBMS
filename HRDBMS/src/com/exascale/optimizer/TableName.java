package com.exascale.optimizer;

public class TableName
{
	private String schema;
	private String name;
	
	public TableName(String name)
	{
		this.name = name;
	}
	
	public TableName(String schema, String name)
	{
		this.schema = schema;
		this.name = name;
	}
	
	public String getSchema()
	{
		return schema;
	}
	
	public String getName()
	{
		return name;
	}
}
