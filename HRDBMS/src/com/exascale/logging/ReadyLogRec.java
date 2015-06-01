package com.exascale.logging;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class ReadyLogRec extends LogRec
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

	private final CharsetEncoder ce = cs.newEncoder();

	public ReadyLogRec(long txnum, String xaHost) throws UnsupportedEncodingException
	{
		super(LogRec.READY, txnum, ByteBuffer.allocate(28 + 4 + xaHost.getBytes(StandardCharsets.UTF_8).length));
		this.buffer().position(28);
		// byte[] data = xaHost.getBytes("UTF-8");
		// int length = data.length;
		byte[] ba = new byte[xaHost.length() << 2];
		char[] value = (char[])unsafe.getObject(xaHost, offset);
		int blen = ((sun.nio.cs.ArrayEncoder)ce).encode(value, 0, value.length, ba);
		byte[] data = Arrays.copyOf(ba, blen);
		this.buffer().putInt(blen);
		this.buffer().put(data);
	}

	public String getHost()
	{
		this.buffer().position(28);
		int length = this.buffer.getInt();
		byte[] data = new byte[length];
		this.buffer.get(data);
		try
		{
			return new String(data, StandardCharsets.UTF_8);
		}
		catch (Exception e)
		{
			return null;
		}
	}
}
