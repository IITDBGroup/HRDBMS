package com.exascale.optimizer;

public class Column
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
