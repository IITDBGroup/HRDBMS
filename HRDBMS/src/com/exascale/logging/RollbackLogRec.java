package com.exascale.logging;

import java.nio.ByteBuffer;

public class RollbackLogRec extends LogRec
{
	public RollbackLogRec(final long txnum)
	{
		super(LogRec.ROLLB, txnum, ByteBuffer.allocate(28));
	}
}
