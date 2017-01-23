package com.exascale.logging;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import com.exascale.filesystem.Block;
import com.exascale.managers.HRDBMSWorker;

public class TruncateLogRec extends LogRec
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

	private final CharsetEncoder ce = cs.newEncoder();

	public TruncateLogRec(long txnum, Block b) throws Exception
	{
		super(LogRec.TRUNCATE, txnum, ByteBuffer.allocate(32 + b.toString().getBytes(StandardCharsets.UTF_8).length));
		this.b = b;

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
			HRDBMSWorker.logger.error("Error converting bytes to UTF-8 string in TruncateLogRec constructor.", e);
			return;
		}
	}

	public Block getBlock()
	{
		return b;
	}

	@Override
	public void redo()
	{

	}

	@Override
	public void undo()
	{

	}
}
