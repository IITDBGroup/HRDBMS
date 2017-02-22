package com.exascale.tables;

public class FreeSpace
{
	private int startOffset;
	private int endOffset;

	public FreeSpace(final int start, final int end)
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

	public void setEnd(final int end)
	{
		endOffset = end;
	}

	public void setStart(final int start)
	{
		startOffset = start;
	}

	@Override
	public String toString()
	{
		return "(" + startOffset + ", " + endOffset + ")";
	}
}
