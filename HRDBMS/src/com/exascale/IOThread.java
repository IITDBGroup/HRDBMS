package com.exascale;

public class IOThread extends HRDBMSThread
{
	private Block[] bs;
	private long txnum;
	
	public IOThread(Block b, long txnum)
	{
		this.setWait(false);
		this.description = ("I/O for " + b);
		this.bs = new Block[1];
		bs[0] = b;
		this.txnum = txnum;
	}
	
	public IOThread(Block[] bs, long txnum)
	{
		this.setWait(false);
		this.description = ("I/O for multiple blocks");
		this.bs = bs;
		this.txnum = txnum;
	}
	
	public void run()
	{
		try
		{
			for (Block b : bs)
			{
				BufferManager.pin(b, txnum);
			}
		}
		catch(Exception e)
		{
			e.printStackTrace(System.err);
		}
		
		this.terminate();
	}
}
