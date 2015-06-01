package com.exascale.optimizer;

public class SelectListEntry
{
	private Column col;
	private Expression exp;
	private final String name;
	private final boolean isCol;

	public SelectListEntry(Column col, String name)
	{
		this.col = col;
		this.name = name;
		isCol = true;
	}

	public SelectListEntry(Expression exp, String name)
	{
		this.exp = exp;
		this.name = name;
		isCol = false;
	}

	@Override
	public SelectListEntry clone()
	{
		if (isCol)
		{
			return new SelectListEntry(col.clone(), name);
		}
		else
		{
			return new SelectListEntry(exp.clone(), name);
		}
	}

	public Column getColumn()
	{
		return col;
	}

	public Expression getExpression()
	{
		return exp;
	}

	public String getName()
	{
		return name;
	}

	public boolean isColumn()
	{
		return isCol;
	}
}
