package com.exascale;

public class FreeSpace 
{
	private int startOffset;
	private int endOffset;
	
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
