package com.exascale.compression;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.zip.Deflater;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;

public final class CompressedOutputStream extends FilterOutputStream
{
	private static LZ4Factory factory;
	static
	{
		factory = LZ4Factory.nativeInstance();
	}
	private final byte[] buff = new byte[3 * 128 * 1024];
	private int index = 0;
	private LZ4Compressor compress = factory.fastCompressor();
	private byte[] outBuff;
	
	public CompressedOutputStream(OutputStream out)
	{
		super(out);
		outBuff = new byte[compress.maxCompressedLength(3*128*1024) + 8];
	}

	public void write(byte[] b) throws IOException
	{
		int toWrite = b.length;
		int ableToWrite = 3*128*1024 - index;
		int bIndex = 0;
		
		while(true)
		{
			if (toWrite < ableToWrite)
			{
				System.arraycopy(b, bIndex, buff, index, toWrite);
				index += toWrite;
				return;
			}
			else if (toWrite == ableToWrite)
			{
				System.arraycopy(b, bIndex, buff, index, toWrite);
				flushFull();
				return;
			}
			else
			{
				System.arraycopy(b, bIndex, buff, index, ableToWrite);
				flushFull();
				toWrite -= ableToWrite;
				bIndex += ableToWrite;
				ableToWrite = 3 * 128 * 1024;
			}
		}
	}
	
	private void flushFull() throws IOException
	{
		int compLen = compress.compress(buff, 0, 3*128*1024, outBuff, 8);
		ByteBuffer bb = ByteBuffer.wrap(outBuff);
		bb.position(0);
		bb.putInt(compLen + 4);
		bb.putInt(3*128*1024);
		out.write(outBuff, 0, compLen+8);
		index = 0;
	}
	
	public void flush() throws IOException
	{
		int compLen = compress.compress(buff, 0, index, outBuff, 8);
		ByteBuffer bb = ByteBuffer.wrap(outBuff);
		bb.position(0);
		bb.putInt(compLen + 4);
		bb.putInt(index);
		out.write(outBuff, 0, compLen+8);
		index = 0;
	}
}
