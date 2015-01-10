package com.exascale.logging;

import java.nio.ByteBuffer;
import com.exascale.filesystem.Block;
import com.exascale.managers.FileManager;
import com.exascale.managers.HRDBMSWorker;

public class TruncateLogRec extends LogRec
{
	private Block b;

	public TruncateLogRec(long txnum, Block b) throws Exception
	{
		super(LogRec.EXTEND, txnum, ByteBuffer.allocate(32 + b.toString().getBytes("UTF-8").length));
		this.b = b;

		final ByteBuffer buff = this.buffer();
		buff.position(28);
		byte[] bbytes = b.toString().getBytes("UTF-8");
		final int blen = bbytes.length;
		buff.putInt(blen);
		try
		{
			buff.put(bbytes);
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.error("Error converting bytes to UTF-8 string in TruncateLogRec constructor.", e);
			return;
		}
	}
	
	public Block getBlock()
	{
		return b;
	}
	
	public void redo()
	{
		
	}
	
	public void undo()
	{
		
	}
}
