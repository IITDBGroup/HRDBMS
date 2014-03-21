package com.exascale.threads;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import com.exascale.filesystem.Block;
import com.exascale.filesystem.Page;
import com.exascale.managers.FileManager;

public class ReadThread extends HRDBMSThread
{
	private final Page p;
	private final Block b;
	private final ByteBuffer bb;

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
			fc.read(bb, b.number() * bb.capacity());
		}
		catch (final IOException e)
		{
			this.terminate();
			return;
		}
		p.setReady();
		this.terminate();
		return;
	}
}