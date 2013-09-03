package com.exascale;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class ReadThread extends HRDBMSThread
{
	private Page p;
	private Block b;
	private ByteBuffer bb;
	
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
	}
}