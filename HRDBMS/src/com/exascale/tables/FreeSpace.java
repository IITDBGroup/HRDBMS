package com.exascale.tables;

public class FreeSpace 
{
	protected int startOffset;
	protected int endOffset;
	
	public FreeSpace(int start, int end)
	{
		startOffset = start;
		endOffset = end;
	}
	
	public int getStart()
	{
		return startOffset;
	}
	
	public int getEnd()
	{
		return endOffset;
	}
	
	public void setStart(int start)
	{
		startOffset = start;
	}
	
	public void setEnd(int end)
	{
		endOffset = end;
	}
}
