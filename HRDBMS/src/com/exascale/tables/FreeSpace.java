package com.exascale.tables;

public class FreeSpace
{
	private int startOffset;
	private int endOffset;

	public FreeSpace(int start, int end)
	{
		startOffset = start;
		endOffset = end;
	}

	public int getEnd()
	{
		return endOffset;
	}

	public int getStart()
	{
		return startOffset;
	}

	public void setEnd(int end)
	{
		endOffset = end;
	}

	public void setStart(int start)
	{
		startOffset = start;
	}
}
