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
	private final long offset;

	public CheckpointManager(long offset)
	{
		HRDBMSWorker.logger.info("Starting initialization of the Checkpoint Manager.");
		this.setWait(true);
		this.description = "Checkpoint Manager";
		this.offset = offset;
	}

	public void doCheckpoint()
	{
		// block all log writes
		// go through log and archive anything that is not part of an open
		// transaction
		// rewrite current log
		// for all blocks in bufferpool, if not pinned - flush, otherwise copy
		// and rollback and flush

		for (SubBufferManager sbm : BufferManager.managers)
		{
			sbm.lock.lock();
		}

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
		for (SubBufferManager sbm : BufferManager.managers)
		{
			sbm.lock.unlock();
		}
	}

	@Override
	public void run()
	{
		final long sleep = Long.parseLong(HRDBMSWorker.getHParms().getProperty("checkpoint_freq_sec")) * 1000;
		HRDBMSWorker.logger.info("Checkpoint Manager initialization complete.");
		long i = 0;
		long start = System.currentTimeMillis() - offset;

		while (true)
		{
			try
			{
				i++;
				Thread.sleep(i * sleep + start - System.currentTimeMillis());
			}
			catch (final Exception e)
			{
			}

			doCheckpoint();
		}
	}
}
