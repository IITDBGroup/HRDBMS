package com.exascale;

public class BufferManagerUnitTest 
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
		HRDBMSWorker.addThread(new BufferManager(false));
		Block b1 = new Block("/home/hrdbms/data/file1", 0);
		Block b2 = new Block("/home/hrdbms/data/file1", 1);
		
		String cmd = "REQUEST PAGE 1~/home/hrdbms/data/file1~0";
		long start = System.currentTimeMillis();
		BufferManager.getInputQueue().put(cmd);
		int i = 0;
		Page p = null;
		while (p == null)
		{
			p = BufferManager.getPage(b1);
			i++;
		}
		long end = System.currentTimeMillis();
		System.out.println("getPage() took " + (end - start) + " ms");
		
		byte[] buff = p.read(0,  16);
		i = 0;
		while (i < 16)
		{
			if (buff[i] != ((byte)'A'))
			{
				System.out.println("BufferManager: Read page 1 test = FAILED");
				return;
			}
			i++;
		}
		
		System.out.println("BufferManager: Read page 1 test = PASSED");
		
		cmd = "REQUEST PAGES 1~2~/home/hrdbms/data/file1~0~/home/hrdbms/data/file1~1";
		BufferManager.getInputQueue().put(cmd);
		
		i = 0;
		Page p1 = null;
		Page p2 = null;
		
		while (p1 == null || p2 == null)
		{
			p1 = BufferManager.getPage(b1);
			p2 = BufferManager.getPage(b2);
			i++;
		}
		
		byte[] buff1 = p1.read(0,  16);
		byte[] buff2 = p2.read(16, 16);
		i = 0;
		while (i < 16)
		{
			if (buff1[i] != ((byte)'A') || buff2[i] != ((byte)'A'))
			{
				System.out.println("BufferManager: Request pages test = FAILED");
				return;
			}
			buff2[i] = (byte)'B';
			i++;
		}
		
		System.out.println("BufferManager: Read pages test = PASSED");
		
		p1.write(0, buff2, 1, LogManager.getLSN());
		buff1 = p1.read(0,  16);
		i = 0;
		while (i < 16)
		{
			if (buff1[i] != ((byte)'B'))
			{
				System.out.println("BufferManager: Write and read back test = FAILED");
				return;
			}
			i++;
		}
		
		System.out.println("BufferManager: Write and read back test = PASSED");
		BufferManager.unpin(p1, 1);
		System.exit(0);
	}
}
