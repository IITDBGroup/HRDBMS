package com.exascale.tables;

import com.exascale.filesystem.RID;

public class RIDChange
{
	private final RID oldRID;
	private final RID newRID;

	public RIDChange(final RID oldRID, final RID newRID)
	{
		this.oldRID = oldRID;
		this.newRID = newRID;
	}

	public RID getNew()
	{
		return newRID;
	}

	public RID getOld()
	{
		return oldRID;
	}
}
