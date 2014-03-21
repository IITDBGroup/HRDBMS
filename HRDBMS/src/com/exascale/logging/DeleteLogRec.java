package com.exascale.logging;

import java.nio.ByteBuffer;
import com.exascale.filesystem.Block;
import com.exascale.filesystem.Page;
import com.exascale.managers.BufferManager;
import com.exascale.managers.HRDBMSWorker;

public class DeleteLogRec extends LogRec
{
	private Block b;
	private int off;
	private byte[] before;
	private byte[] after;

	public DeleteLogRec(long txnum, Block b, int off, byte[] before, byte[] after)
	{
		super(LogRec.DELETE, txnum, ByteBuffer.allocate(28 + b.toString().length() + 8 + 2 * before.length));
		this.b = b;
		this.off = off;
		this.before = before;
		this.after = after;

		final ByteBuffer buff = this.buffer();
		buff.position(28);
		final int blen = b.toString().length();
		buff.putInt(blen);
		try
		{
			buff.put(b.toString().getBytes("UTF-8"));
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.error("Error converting bytes to UTF-8 string in DeleteLogRec constructor.", e);
			return;
		}

		buff.putInt(before.length);
		buff.put(before);
		buff.put(after);
		buff.putInt(off);
	}

	@Override
	public void redo()
	{
		final String cmd = "REQUEST PAGE " + this.txnum() + "~" + b.toString();
		while (true)
		{
			try
			{
				BufferManager.getInputQueue().put(cmd);
				break;
			}
			catch (final InterruptedException e)
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

	@Override
	public void undo()
	{
		final String cmd = "REQUEST PAGE " + this.txnum() + "~" + b.toString();
		while (true)
		{
			try
			{
				BufferManager.getInputQueue().put(cmd);
				break;
			}
			catch (final InterruptedException e)
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
}