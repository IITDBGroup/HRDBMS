package com.exascale.optimizer;

import java.io.Serializable;

public class ColDef implements Serializable
{
	private final Column col;
	private final String type;
	private final boolean nullable;
	private final boolean pk;

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
