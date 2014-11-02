package com.exascale.optimizer;

import java.io.Serializable;

public class Column implements Serializable
{
	private String table;
	private String column;
	
	public Column(String column)
	{
		this.column = column;
	}
	
	public Column(String table, String column)
	{
		this.table = table;
		this.column = column;
	}
	
	public void setTable(String table)
	{
		this.table = table;
	}
	
	public void setColumn(String column)
	{
		this.column = column;
	}
	
	public String getTable()
	{
		return table;
	}
	
	public String getColumn()
	{
		return column;
	}
	
	public boolean equals(Object o)
	{
		if (o == null || !(o instanceof Column))
		{
			return false;
		}
		
		Column rhs = (Column)o;
		
		if (table == null)
		{
			if (rhs.table != null)
			{
				return false;
			}
		}
		else
		{
			if (!rhs.table.equals(table))
			{
				return false;
			}
		}
		
		if (!column.equals(rhs.column))
		{
			return false;
		}
		
		return true;
	}
}
