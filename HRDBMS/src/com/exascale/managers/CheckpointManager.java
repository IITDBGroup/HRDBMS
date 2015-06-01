package com.exascale.managers;

import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
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

	public void doCheckpoint()
	{
		// block all log writes
		// go through log and archive anything that is not part of an open
		// transaction
		// rewrite current log
		// for all blocks in bufferpool, if not pinned - flush, otherwise copy
		// and rollback and flush

		Transaction.txListLock.writeLock().lock();
		{
			try
			{
				HRDBMSWorker.logger.debug("Checkpoint is starting");
				if (HRDBMSWorker.type == HRDBMSWorker.TYPE_COORD || HRDBMSWorker.type == HRDBMSWorker.TYPE_MASTER)
				{
					String filename = HRDBMSWorker.getHParms().getProperty("log_dir");
					if (!filename.endsWith("/"))
					{
						filename += "/";
					}
					filename += "xa.log";
					RandomAccessFile f = LogManager.archive(Transaction.txList.keySet(), filename, true);
					FileChannel fc = LogManager.getFile(filename);
					FileChannel fc2 = f.getChannel();
					fc.truncate(0);
					fc2.position(0);
					fc.transferFrom(fc2, 0, fc2.size());
					fc2.close();
					f.close();
					LogRec rec = new NQCheckLogRec(new HashSet<Long>(Transaction.txList.keySet()));
					LogManager.write(rec, filename);
					LogManager.flush(rec.lsn(), filename);
				}
				RandomAccessFile f = LogManager.archive(Transaction.txList.keySet());
				FileChannel fc2 = f.getChannel();
				BufferManager.flushAll(fc2);
				FileChannel fc = LogManager.getFile(LogManager.filename);
				fc.truncate(0);
				fc2.position(0);
				fc.transferFrom(fc2, 0, fc2.size());
				fc2.close();
				f.close();
				LogRec rec = new NQCheckLogRec(new HashSet<Long>(Transaction.txList.keySet()));
				LogManager.write(rec);
				LogManager.flush(rec.lsn());
				HRDBMSWorker.logger.debug("Checkpoint is complete");
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("Error occurred during checkpoint", e);
				System.exit(1);
				return;
			}
		}
		Transaction.txListLock.writeLock().unlock();
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

			doCheckpoint();
		}
	}
}
