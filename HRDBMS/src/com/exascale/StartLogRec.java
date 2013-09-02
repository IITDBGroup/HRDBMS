package com.exascale;

import java.nio.ByteBuffer;

public class StartLogRec extends LogRec
{
	public StartLogRec(long txnum)
	{
		super(LogRec.COMMIT, txnum, ByteBuffer.allocate(28));
	}
}
