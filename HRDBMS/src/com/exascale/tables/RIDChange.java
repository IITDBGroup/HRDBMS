package com.exascale.tables;

import com.exascale.filesystem.RID;

public class RIDChange 
{
	protected RID oldRID;
	protected RID newRID;
	
	public RIDChange(RID oldRID, RID newRID)
	{
		this.oldRID = oldRID;
		this.newRID = newRID;
	}
	
	public RID getOld()
	{
		return oldRID;
	}
	
	public RID getNew()
	{
		return newRID;
	}
}
