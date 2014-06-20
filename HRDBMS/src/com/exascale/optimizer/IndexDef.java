package com.exascale.optimizer;

public class IndexDef
{
	private Column col;
	private boolean dir;
	
	public IndexDef(Column col, boolean dir)
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
