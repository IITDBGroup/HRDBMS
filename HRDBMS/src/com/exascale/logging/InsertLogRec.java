package com.exascale.logging;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import com.exascale.filesystem.Block;
import com.exascale.filesystem.Page;
import com.exascale.managers.BufferManager;
import com.exascale.managers.HRDBMSWorker;

public class InsertLogRec extends LogRec
{
	private static Charset cs = StandardCharsets.UTF_8;
	private static sun.misc.Unsafe unsafe;
	private static long offset;
	static
	{
		try
		{
			final Field f = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
			f.setAccessible(true);
			unsafe = (sun.misc.Unsafe)f.get(null);
			final Field fieldToUpdate = String.class.getDeclaredField("value");
			// get unsafe offset to this field
			offset = unsafe.objectFieldOffset(fieldToUpdate);
		}
		catch (final Exception e)
		{
			unsafe = null;
		}
	}
	private Block b;
	private int off;
	private byte[] before;
	private byte[] after;

	private final CharsetEncoder ce = cs.newEncoder();

	public InsertLogRec(long txnum, Block b, int off, byte[] before, byte[] after) throws Exception
	{
		super(LogRec.INSERT, txnum, ByteBuffer.allocate(28 + b.toString().getBytes(StandardCharsets.UTF_8).length + 12 + 2 * before.length));
		if (before.length != after.length)
		{
			throw new Exception("Before and after images length do not match");
		}

		this.b = b;
		this.off = off;
		this.before = before;
		this.after = after;

		final ByteBuffer buff = this.buffer();
		buff.position(28);
		// byte[] bbytes = b.toString().getBytes("UTF-8");
		// final int blen = bbytes.length;
		String string = b.toString();
		byte[] ba = new byte[string.length() << 2];
		char[] value = (char[])unsafe.getObject(string, offset);
		int blen = ((sun.nio.cs.ArrayEncoder)ce).encode(value, 0, value.length, ba);
		byte[] bbytes = Arrays.copyOf(ba, blen);
		buff.putInt(blen);
		try
		{
			buff.put(bbytes);
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.error("Error converting bytes to UTF-8 string in InsertLogRec constructor.", e);
			return;
		}

		buff.putInt(before.length);
		buff.put(before);
		buff.put(after);
		buff.putInt(off);
	}

	public byte[] getAfter()
	{
		return after;
	}

	public byte[] getBefore()
	{
		return before;
	}

	public Block getBlock()
	{
		return b;
	}

	@Override
	public int getEnd()
	{
		return off + before.length;
	}

	@Override
	public int getOffset()
	{
		return off;
	}

	@Override
	public void redo() throws Exception
	{
		// HRDBMSWorker.logger.debug("Redoing change at " + b + "@" + off +
		// " for a length of " + before.length);
		if (b.number() < 0)
		{
			Exception e = new Exception("Negative block number requested");
			HRDBMSWorker.logger.debug("", e);
		}

		BufferManager.requestPage(b, txnum());

		Page p = null;
		while (p == null)
		{
			p = BufferManager.getPage(b, txnum());
		}

		p.write(off, after, this.txnum(), this.lsn());
		p.unpin(this.txnum());
	}

	@Override
	public void undo() throws Exception
	{
		// HRDBMSWorker.logger.debug("Undoing change at " + b + "@" + off +
		// " for a length of " + before.length);
		if (b.number() < 0)
		{
			Exception e = new Exception("Negative block number requested");
			HRDBMSWorker.logger.debug("", e);
		}

		BufferManager.requestPage(b, txnum());

		Page p = null;
		while (p == null)
		{
			p = BufferManager.getPage(b, txnum());
		}

		p.write(off, before, this.txnum(), this.lsn());
		p.unpin(this.txnum());
	}
}
