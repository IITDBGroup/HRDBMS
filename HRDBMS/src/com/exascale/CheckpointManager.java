package com.exascale;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;

public class CheckpointManager extends HRDBMSThread
{	
	public CheckpointManager()
	{
		this.setWait(true);
		this.description = "Checkpoint Manager";
	}
	
	public void run()
	{
		long sleep = Long.parseLong(HRDBMSWorker.getHParms().getProperty("checkpoint_freq_sec"));
		
		while (true)
		{
			try
			{
				Thread.sleep(sleep * 1000);
			}
			catch(InterruptedException e) {}
		
			synchronized(Transaction.txList)
			{
				try
				{
					BufferManager.flushAll();
				}
				catch(Exception e)
				{
					e.printStackTrace(System.err);
					this.terminate();
				}
				LogRec rec = new NQCheckLogRec(Transaction.txList);
				LogManager.write(rec);
				try
				{
					LogManager.flush(rec.lsn());
				}
				catch(IOException e)
				{
					e.printStackTrace(System.err);
					this.terminate();
				}
			}
		}
	}
}
