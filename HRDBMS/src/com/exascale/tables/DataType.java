package com.exascale.tables;

import java.io.Serializable;

public class DataType implements Serializable
{
	public static final int SMALLINT = 0, INTEGER = 1, BIGINT = 2, DECIMAL = 4, VARCHAR = 6, FLOAT = 7, DOUBLE = 8, BINARY = 9, VARBINARY = 10, DATE = 11, TIME = 12, TIMESTAMP = 13;
	private final int type;
	private final int length;

	private final int scale;

	public DataType(int type, int length, int scale)
	{
		this.type = type;
		this.length = length;
		this.scale = scale;
	}

	public int getLength()
	{
		return length;
	}

	public int getScale()
	{
		return scale;
	}

	public int getType()
	{
		return type;
	}
}
