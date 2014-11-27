package com.exascale.threads;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import com.exascale.filesystem.Block;
import com.exascale.filesystem.Page;
import com.exascale.managers.FileManager;
import com.exascale.managers.HRDBMSWorker;

public class ReadThread extends HRDBMSThread
{
	private final Page p;
	private final Block b;
	private final ByteBuffer bb;
	private boolean ok = true;

	public ReadThread(Page p, Block b, ByteBuffer bb)
	{
		this.description = "Read thread for buffer Manager";
		this.setWait(false);
		this.p = p;
		this.b = b;
		this.bb = bb;
	}

	@Override
	public void run()
	{
		bb.clear();
		bb.position(0);
		try
		{
			final FileChannel fc = FileManager.getFile(b.fileName());
			//if (b.number() * bb.capacity() >= fc.size())
			//{
			//	HRDBMSWorker.logger.debug("Tried to read from " + b.fileName() + " at block = " + b.number() + " but it was past the range of the file");
			//	ok = false;
			//}
			
			fc.read(bb, b.number() * bb.capacity());
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.warn("I/O error occurred trying to read file: " + b.fileName() + ":" + b.number(), e);
			ok = false;
			this.terminate();
			return;
		}
		p.setReady();
		this.terminate();
		return;
	}
	
	public boolean getOK()
	{
		return ok;
	}
}