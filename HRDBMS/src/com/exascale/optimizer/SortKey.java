package com.exascale.optimizer;

public class SortKey
{
	private int num;
	private Column col;
	private boolean isCol;
	private boolean direction;
	
	public SortKey(int num, boolean direction)
	{
		this.num = num;
		isCol = false;
		this.direction = direction;
	}
	
	public SortKey(Column col, boolean direction)
	{
		this.col = col;
		isCol = true;
		this.direction = direction;
	}
	
	public SortKey clone()
	{
		if (isCol)
		{
			return new SortKey(col.clone(), direction);
		}
		else
		{
			return new SortKey(num, direction);
		}
	}
	
	public boolean isColumn()
	{
		return isCol;
	}
	
	public Column getColumn()
	{
		return col;
	}
	
	public boolean getDirection()
	{
		return direction;
	}
	
	public int getNum()
	{
		return num;
	}
}
