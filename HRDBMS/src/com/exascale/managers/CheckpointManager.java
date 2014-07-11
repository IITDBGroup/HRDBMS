package com.exascale.managers;

import java.io.IOException;
import java.util.HashSet;
import com.exascale.logging.LogRec;
import com.exascale.logging.NQCheckLogRec;
import com.exascale.tables.Transaction;
import com.exascale.threads.HRDBMSThread;

public class CheckpointManager extends HRDBMSThread
{
	public CheckpointManager()
	{
		HRDBMSWorker.logger.info("Starting initialization of the Checkpoint Manager.");
		this.setWait(true);
		this.description = "Checkpoint Manager";
	}

	@Override
	public void run()
	{
		final long sleep = Long.parseLong(HRDBMSWorker.getHParms().getProperty("checkpoint_freq_sec"));
		HRDBMSWorker.logger.info("Checkpoint Manager initialization complete.");
		while (true)
		{
			try
			{
				Thread.sleep(sleep * 1000);
			}
			catch (final InterruptedException e)
			{
			}

			synchronized (Transaction.txList)
			{
				try
				{
					BufferManager.flushAll();
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.error("Error occurred while flushing the bufferpool in the Checkpoint Manager.", e);
					this.terminate();
					return;
				}
				LogRec rec = new NQCheckLogRec(new HashSet<Long>(Transaction.txList.keySet()));
				LogManager.write(rec);
				try
				{
					LogManager.flush(rec.lsn());
				}
				catch (final IOException e)
				{
					HRDBMSWorker.logger.error("Error flushing the log in Checkpoint Manager.", e);
					this.terminate();
					return;
				}
				
				if (HRDBMSWorker.type == HRDBMSWorker.TYPE_COORD || HRDBMSWorker.type == HRDBMSWorker.TYPE_MASTER)
				{
					String filename = HRDBMSWorker.getHParms().getProperty("log_dir");
					if (!filename.endsWith("/"))
					{
						filename += "/";
					}
					filename += "xa.log";
					HashSet<Long> open = new HashSet<Long>();
					for (Transaction tx : XAManager.txs.getKeySet())
					{
						open.add(tx.number());
					}
					rec = new NQCheckLogRec(open);
					LogManager.write(rec, filename);
					try
					{
						LogManager.flush(rec.lsn(), filename);
					}
					catch (final IOException e)
					{
						HRDBMSWorker.logger.error("Error flushing the log in Checkpoint Manager.", e);
						this.terminate();
						return;
					}
				}
			}
		}
	}
}
