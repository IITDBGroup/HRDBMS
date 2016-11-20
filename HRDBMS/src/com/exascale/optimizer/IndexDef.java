package com.exascale.optimizer;

import java.io.Serializable;

public class IndexDef implements Serializable
{
	private final Column col;
	private final boolean dir;

	public IndexDef(final Column col, final boolean dir)
	{
		this.col = col;
		this.dir = dir;
	}

	public Column getCol()
	{
		return col;
	}

	public boolean isAsc()
	{
		return dir;
	}
}
