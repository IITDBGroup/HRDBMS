package com.exascale;

import java.nio.ByteBuffer;

public class InsertLogRec extends LogRec
{
	private Block b;
	private int off;
	private byte[] before;
	private byte[] after;
	
	public InsertLogRec(long txnum, Block b, int off, byte[] before, byte[] after)
	{
		super(LogRec.INSERT, txnum, ByteBuffer.allocate(28 + b.toString().length() + 12 + 2 * before.length));
		this.b = b;
		this.off = off;
		this.before = before;
		this.after = after;
		
		ByteBuffer buff = this.buffer();
		buff.position(28);
		int blen = b.toString().length();
		buff.putInt(blen);
		try
		{
			buff.put(b.toString().getBytes("UTF-8"));
		}
		catch(Exception e)
		{
			e.printStackTrace(System.err);
			return;
		}
		
		buff.putInt(before.length);
		buff.put(before);
		buff.put(after);
		buff.putInt(off);
	}
	
	public void undo()
	{
		String cmd = "REQUEST PAGE " + this.txnum() + "~" + b.toString();
		while (true)
		{
			try
			{
				BufferManager.getInputQueue().put(cmd);
				break;
			}
			catch(InterruptedException e)
			{
				continue;
			}
		}
		
		Page p = null;
		while (p == null)
		{
			p = BufferManager.getPage(b);
		}
		
		p.write(off, before, this.txnum(), this.lsn());
		p.unpin(this.txnum());
	}
	
	public void redo()
	{
		String cmd = "REQUEST PAGE " + this.txnum() + "~" + b.toString();
		while (true)
		{
			try
			{
				BufferManager.getInputQueue().put(cmd);
				break;
			}
			catch(InterruptedException e)
			{
				continue;
			}
		}
		
		Page p = null;
		while (p == null)
		{
			p = BufferManager.getPage(b);
		}
		
		p.write(off, after, this.txnum(), this.lsn());
		p.unpin(this.txnum());
	}
}
