package com.exascale.tables;

import java.util.ArrayList;
import java.util.List;

public class InternalResultSet
{
	private final List<List<Object>> data;
	private int pos = -1;

	public InternalResultSet(final List<List<Object>> data)
	{
		this.data = data;
	}

	public Integer getInt(final int colPos)
	{
		return (Integer)(data.get(pos).get(colPos - 1));
	}

	public String getString(final int colPos)
	{
		return (String)(data.get(pos).get(colPos - 1));
	}

	public boolean next()
	{
		pos++;
		if (pos < data.size())
		{
			return true;
		}
		else
		{
			return false;
		}
	}
}
