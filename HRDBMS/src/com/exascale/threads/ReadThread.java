package com.exascale.threads;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import com.exascale.filesystem.Block;
import com.exascale.filesystem.Page;
import com.exascale.managers.FileManager;

public class ReadThread extends HRDBMSThread
{
	protected Page p;
	protected Block b;
	protected ByteBuffer bb;
	
	public ReadThread(Page p, Block b, ByteBuffer bb)
	{
		this.description = "Read thread for buffer Manager";
		this.setWait(false);
		this.p = p;
		this.b = b;
		this.bb = bb;
	}
	
	public void run()
	{
		bb.clear();
		bb.position(0);
		try
		{
			FileChannel fc = FileManager.getFile(b.fileName());
			fc.read(bb, b.number() * bb.capacity());
		}
		catch(IOException e)
		{
			this.terminate();
			return;
		}
		p.setReady();
		this.terminate();
		return;
	}
}