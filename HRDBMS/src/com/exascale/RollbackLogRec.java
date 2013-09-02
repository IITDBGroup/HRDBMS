package com.exascale;

import java.nio.ByteBuffer;

public class RollbackLogRec extends LogRec
{
	public RollbackLogRec(long txnum)
	{
		super(LogRec.ROLLB, txnum, ByteBuffer.allocate(28));
	}
}
