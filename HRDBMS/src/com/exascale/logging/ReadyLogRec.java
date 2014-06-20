package com.exascale.logging;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

public class ReadyLogRec extends LogRec
{
	public ReadyLogRec(long txnum, String xaHost) throws UnsupportedEncodingException
	{
		super(LogRec.READY, txnum, ByteBuffer.allocate(28 + 4 + xaHost.getBytes("UTF-8").length));
		this.buffer().position(28);
		byte[] data = xaHost.getBytes("UTF-8");
		int length = data.length;
		this.buffer().putInt(length);
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
			return new String(data, "UTF-8");
		}
		catch(Exception e)
		{
			return null;
		}
	}
}
