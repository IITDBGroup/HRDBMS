package com.exascale.compression;

import java.io.EOFException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import net.jpountz.lz4.LZ4SafeDecompressor;

public final class CompressedInputStream extends FilterInputStream
{
	private static LZ4Factory factory;
	static
	{
		factory = LZ4Factory.nativeInstance();
	}
	private byte[] buff = new byte[3*128*1024];
	private int index = 0;
	private int limit = 0;
	private byte[] temp = new byte[4];
	private LZ4Compressor compress = factory.fastCompressor();
	private LZ4FastDecompressor decompress = factory.fastDecompressor();
	private byte[] inBuff;

	public CompressedInputStream(InputStream in)
	{
		super(in);
		inBuff = new byte[compress.maxCompressedLength(3*128*1024)];
	}
	
	public int read(byte[] b, int off, int len) throws IOException
	{
		int read = 0;
		while (len > 0)
		{
			if (index >= limit)
			{
				index = 0;
				int tempIndex = 0;
				int toRead = 4;
				while (toRead > 0)
				{
					int temp2 = in.read(temp, tempIndex, toRead);
					toRead -= temp2;
					tempIndex += temp2;
				}
					
				ByteBuffer bb = ByteBuffer.wrap(temp);
				int remaining = bb.getInt();
				tempIndex = 0;
				toRead = 4;
				while (toRead > 0)
				{
					int temp2 = in.read(temp, tempIndex, toRead);
					toRead -= temp2;
					tempIndex += temp2;
				}
				bb.position(0);
				int original = bb.getInt();
				tempIndex = 0;
				toRead = remaining-4;
				while (toRead > 0)
				{
					int temp2 = in.read(inBuff, tempIndex, toRead);
					toRead -= temp2;
					tempIndex += temp2;
				}
				decompress.decompress(inBuff, buff, original);
				limit = original;
			}
		
			int available = limit - index;
			if (available >= len)
			{
				System.arraycopy(buff, index, b, off, len);
				index += len;
				read += len;
				return read;
			}
			else
			{
				System.arraycopy(buff, index, b, off, available);
				len -= available;
				off += available;
				read += available;
				index += available;
			}
		}
		
		return read;
	}
}