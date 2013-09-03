package com.exascale;

import java.nio.ByteBuffer;

public class CommitLogRec extends LogRec
{
	public CommitLogRec(long txnum)
	{
		super(LogRec.COMMIT, txnum, ByteBuffer.allocate(28));
	}
}
