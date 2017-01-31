package com.exascale.logging;

import java.nio.ByteBuffer;

public class StartLogRec extends LogRec
{
	public StartLogRec(final long txnum)
	{
		super(LogRec.START, txnum, ByteBuffer.allocate(28));
	}
}
