package com.exascale.logging;

import java.nio.ByteBuffer;

public class CommitLogRec extends LogRec
{
	public CommitLogRec(final long txnum)
	{
		super(LogRec.COMMIT, txnum, ByteBuffer.allocate(28));
	}
}
