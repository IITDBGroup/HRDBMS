package com.exascale.testing;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4SafeDecompressor;

public class Decompress
{
	public static void main(final String[] args)
	{
		try
		{
			final LZ4Factory factory = LZ4Factory.nativeInstance();
			final LZ4SafeDecompressor decomp = factory.safeDecompressor();
			final RandomAccessFile raf = new RandomAccessFile(args[0], "r");
			final FileChannel fc = raf.getChannel();
			final ByteBuffer bb = ByteBuffer.allocate((int)fc.size());
			fc.read(bb, 0);
			final byte[] target = new byte[128 * 1024 * 3]; // max 3 pages
			final int bytes = decomp.decompress(bb.array(), 0, (int)fc.size(), target, 0, 128 * 1024 * 3);
			final byte[] target2 = new byte[bytes];
			System.arraycopy(target, 0, target2, 0, bytes);
			final ByteBuffer bb2 = ByteBuffer.wrap(target2);
			final RandomAccessFile raf2 = new RandomAccessFile("/tmp/dump", "rw");
			final FileChannel fc2 = raf2.getChannel();
			fc2.write(bb2);
			fc2.close();
			raf2.close();
			fc.close();
			raf.close();
		}
		catch (final Exception e)
		{
			e.printStackTrace();
		}
	}
}
