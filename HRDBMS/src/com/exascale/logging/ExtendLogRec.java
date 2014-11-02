package com.exascale.logging;

import java.nio.ByteBuffer;
import com.exascale.filesystem.Block;
import com.exascale.managers.FileManager;
import com.exascale.managers.HRDBMSWorker;

public class ExtendLogRec extends LogRec
{
	private Block b;

	public ExtendLogRec(long txnum, Block b) throws Exception
	{
		super(LogRec.EXTEND, txnum, ByteBuffer.allocate(32 + b.toString().getBytes("UTF-8").length));
		this.b = b;
		
		if (b.number() == 0)
		{
			throw new Exception("Can't write an extend log record for page 0");
		}

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
			HRDBMSWorker.logger.error("Error converting bytes to UTF-8 string in ExtendLogRec constructor.", e);
			return;
		}
	}
	
	public Block getBlock()
	{
		return b;
	}
	
	public void redo()
	{
		try
		{
			FileManager.redoExtend(b);
		}
		catch(Exception e)
		{
			HRDBMSWorker.logger.fatal("", e);
			System.exit(1);
		}
	}
	
	public void undo()
	{
		try
		{
			FileManager.trim(b.fileName(), b.number());
		}
		catch(Exception e)
		{}
	}
}
