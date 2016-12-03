package com.exascale.compression;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

public final class CompressedInputStream extends FilterInputStream
{
	private static LZ4Factory factory;

	static
	{
		factory = LZ4Factory.nativeInstance();
	}
	private final byte[] buff = new byte[128 * 1024];
	private int index = 0;
	private int limit = 0;
	private final byte[] temp = new byte[4];
	private final LZ4Compressor compress = factory.fastCompressor();
	private final LZ4FastDecompressor decompress = factory.fastDecompressor();
	private final byte[] inBuff;

	public CompressedInputStream(final InputStream in)
	{
		super(in);
		inBuff = new byte[compress.maxCompressedLength(128 * 1024)];
	}

	@Override
	public int available() throws IOException
	{
		return limit - index + in.available();
	}

	@Override
	public int read() throws IOException
	{
		final byte[] buff = new byte[1];
		read(buff, 0, 1);
		return buff[0];
	}

	@Override
	public int read(final byte[] b, int off, int len) throws IOException
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
					final int temp2 = in.read(temp, tempIndex, toRead);
					toRead -= temp2;
					tempIndex += temp2;
				}

				final ByteBuffer bb = ByteBuffer.wrap(temp);
				final int remaining = bb.getInt();
				tempIndex = 0;
				toRead = 4;
				while (toRead > 0)
				{
					final int temp2 = in.read(temp, tempIndex, toRead);
					toRead -= temp2;
					tempIndex += temp2;
				}
				bb.position(0);
				final int original = bb.getInt();
				tempIndex = 0;
				toRead = remaining - 4;
				while (toRead > 0)
				{
					final int temp2 = in.read(inBuff, tempIndex, toRead);
					toRead -= temp2;
					tempIndex += temp2;
				}
				decompress.decompress(inBuff, buff, original);
				limit = original;
			}

			final int available = limit - index;
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