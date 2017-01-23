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

	@Override
	public Column clone()
	{
		return new Column(table, column);
	}

	@Override
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

	public String getColumn()
	{
		return column;
	}

	public String getTable()
	{
		return table;
	}

	@Override
	public int hashCode()
	{
		int hash = 23;
		hash = 31 * hash + column.hashCode();
		hash = 31 * hash + (table != null ? table.hashCode() : 1);
		return hash;
	}

	public void setColumn(String column)
	{
		this.column = column;
	}

	public void setTable(String table)
	{
		this.table = table;
	}
}
