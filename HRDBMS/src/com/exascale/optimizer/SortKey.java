package com.exascale.optimizer;

public class SortKey
{
	private int num;
	private Column col;
	private final boolean isCol;
	private final boolean direction;

	public SortKey(final Column col, final boolean direction)
	{
		this.col = col;
		isCol = true;
		this.direction = direction;
	}

	public SortKey(final int num, final boolean direction)
	{
		this.num = num;
		isCol = false;
		this.direction = direction;
	}

	@Override
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

	public boolean isColumn()
	{
		return isCol;
	}
}
