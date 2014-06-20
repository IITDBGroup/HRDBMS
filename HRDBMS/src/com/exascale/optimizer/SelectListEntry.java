package com.exascale.optimizer;

public class SelectListEntry
{
	private Column col;
	private Expression exp;
	private String name;
	private boolean isCol;
	
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
	
	public boolean isColumn()
	{
		return isCol;
	}
	
	public Expression getExpression()
	{
		return exp;
	}
	
	public String getName()
	{
		return name;
	}
	
	public Column getColumn()
	{
		return col;
	}
}
