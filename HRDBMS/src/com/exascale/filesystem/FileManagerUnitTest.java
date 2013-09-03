package com.exascale;

import java.nio.ByteBuffer;

public class FileManagerUnitTest 
{
	public static void main(String[] args) throws Exception
	{
		try
		{
			HRDBMSWorker.hparms = HParms.getHParms();
			System.out.println(HRDBMSWorker.hparms); 
		}
		catch(Exception e)
		{
			System.err.println("Could not load HParms");
			e.printStackTrace(System.err);
			System.exit(-1);
		}
		
		new FileManager();
		Block b1 = new Block("/home/hrdbms/data/file1", 0);
		Block b2 = new Block("/home/hrdbms/data/file1", 1);
		ByteBuffer buffer = ByteBuffer.allocate(Page.BLOCK_SIZE);
		
		int i = 0;
		while (i < Page.BLOCK_SIZE)
		{
			buffer.put((byte)'A');
			i++;
		}
		
		long start = System.currentTimeMillis();
		FileManager.write(b1, buffer);
		long end = System.currentTimeMillis();
		System.out.println("FileManager: Write block took " + (end - start) + " ms");
		start = System.currentTimeMillis();
		FileManager.write(b2, buffer);
		end = System.currentTimeMillis();
		System.out.println("FileManager: Write block took " + (end - start) + " ms");
		buffer.clear();
		start = System.nanoTime();
		Page p = new Page();
		FileManager.read(p, b1,  buffer);
		end = System.nanoTime();
		System.out.println("FileManager: Read block took " + ((end - start) * 1.0 / 1000000.0) + " ms");
		
		i = 0;
		buffer.position(0);
		while (!p.isReady()) {}
		
		do
		{
			byte one = buffer.get();
			if (one != ((byte)'A'))
			{
				System.out.println("FileManager: Write and read back test = FAILED");
				return;
			}
			i++;
		}
		while (i < Page.BLOCK_SIZE);
		
		System.out.println("FileManager: Write and read back test = PASSED");
	}
}
