package com.exascale.testing;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4SafeDecompressor;

public class Decompress
{
	public static void main(String[] args)
	{
		try
		{
			LZ4Factory factory = LZ4Factory.nativeInstance();
			LZ4SafeDecompressor decomp = factory.safeDecompressor();
			RandomAccessFile raf = new RandomAccessFile(args[0], "r");
			FileChannel fc = raf.getChannel();
			ByteBuffer bb = ByteBuffer.allocate((int)fc.size());
			fc.read(bb, 0);
			byte[] target = new byte[128 * 1024 * 3]; // max 3 pages
			int bytes = decomp.decompress(bb.array(), 0, (int)fc.size(), target, 0, 128 * 1024 * 3);
			byte[] target2 = new byte[bytes];
			System.arraycopy(target, 0, target2, 0, bytes);
			ByteBuffer bb2 = ByteBuffer.wrap(target2);
			RandomAccessFile raf2 = new RandomAccessFile("/tmp/dump", "rw");
			FileChannel fc2 = raf2.getChannel();
			fc2.write(bb2);
			fc2.close();
			raf2.close();
			fc.close();
			raf.close();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}
}
