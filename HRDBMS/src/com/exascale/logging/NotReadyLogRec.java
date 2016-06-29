package com.exascale.logging;

import java.nio.ByteBuffer;

public class NotReadyLogRec extends LogRec
{
	public NotReadyLogRec(long txnum)
	{
		super(LogRec.NOTREADY, txnum, ByteBuffer.allocate(28));
	}
}
