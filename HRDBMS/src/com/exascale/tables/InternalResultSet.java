package com.exascale.tables;

import java.util.ArrayList;

public class InternalResultSet
{
	protected ArrayList<ArrayList<Object>> data;
	protected int pos = -1;
	
	public InternalResultSet(ArrayList<ArrayList<Object>> data)
	{
		this.data = data;
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
	
	public Integer getInt(int colPos)
	{
		return (Integer)(data.get(pos).get(colPos-1));
	}
	
	public String getString(int colPos)
	{
		return (String)(data.get(pos).get(colPos-1));
	}
}
