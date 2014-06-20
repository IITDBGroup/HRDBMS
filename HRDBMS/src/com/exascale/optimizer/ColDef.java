package com.exascale.optimizer;

public class ColDef
{
	private Column col;
	private String type;
	private boolean nullable;
	private boolean pk;
	
	public ColDef(Column col, String type, boolean nullable, boolean pk)
	{
		this.col = col;
		this.type = type;
		this.nullable = nullable;
		this.pk = pk;
	}
	
	public Column getCol()
	{
		return col;
	}
	
	public String getType()
	{
		return type;
	}
	
	public boolean isNullable()
	{
		return nullable;
	}
	
	public boolean isPK()
	{
		return pk;
	}
}
