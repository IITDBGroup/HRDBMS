package com.exascale.managers;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import com.exascale.filesystem.Block;
import com.exascale.filesystem.Page;
import com.exascale.logging.ArchiveIterator;
import com.exascale.logging.CommitLogRec;
import com.exascale.logging.DeleteLogRec;
import com.exascale.logging.ExtendLogRec;
import com.exascale.logging.ForwardLogIterator;
import com.exascale.logging.InsertLogRec;
import com.exascale.logging.LogIterator;
import com.exascale.logging.LogRec;
import com.exascale.logging.NotReadyLogRec;
import com.exascale.logging.PartialLogIterator;
import com.exascale.logging.PrepareLogRec;
import com.exascale.logging.ReadyLogRec;
import com.exascale.logging.RollbackLogRec;
import com.exascale.logging.TruncateLogRec;
import com.exascale.logging.XAAbortLogRec;
import com.exascale.logging.XACommitLogRec;
import com.exascale.tables.Transaction;
import com.exascale.threads.HRDBMSThread;

public class LogManager extends HRDBMSThread
{
	private static AtomicLong last_lsn;
	public static Map<String, FileChannel> openFiles = new HashMap<String, FileChannel>();
	public static String filename;
	public static ConcurrentHashMap<String, ArrayDeque<LogRec>> logs = new ConcurrentHashMap<String, ArrayDeque<LogRec>>();
	// private static BlockingQueue<String> in = new
	// LinkedBlockingQueue<String>();
	public static Boolean noArchive = false;
	// public static Object noArchiveLock = new Object();
	public static int openIters = 0;
	public static volatile boolean recoverDone = false;

	static
	{
		last_lsn = new AtomicLong(System.currentTimeMillis() * 1000000);
	}

	public LogManager()
	{
		HRDBMSWorker.logger.info("Starting initialization of the Log Manager.");
		this.setWait(true);
		this.description = "Log Manager";
	}

	public static RandomAccessFile archive(Set<Long> txList) throws Exception
	{
		return archive(txList, filename, false);
	}

	public static RandomAccessFile archive(Set<Long> txList, String fn, boolean force) throws Exception
	{
		// go through log and archive anything that is not part of an open
		// transaction
		// rewrite current log
		while (true)
		{
			// synchronized(noArchiveLock)
			Transaction.txListLock.writeLock().lock();
			{
				try
				{
					if (noArchive)
					{
						continue;
					}

					final ArrayDeque<LogRec> list = logs.get(fn);

					if (list != null)
					{
						LogRec last = null;
						// synchronized (Transaction.txList)
						{
							last = list.peekLast();
						}

						if (last != null)
						{
							flush(last.lsn(), fn);
						}
					}

					final File table = new File(fn + ".new");
					RandomAccessFile f = null;
					while (true)
					{
						try
						{
							f = new RandomAccessFile(table, "rwd");
							break;
						}
						catch (FileNotFoundException e)
						{
							ResourceManager.panic = true;
							try
							{
								Thread.sleep(Integer.parseInt(HRDBMSWorker.getHParms().getProperty("rm_sleep_time_ms")) / 2);
							}
							catch (Exception g)
							{
							}
						}
					}
					FileChannel fc2 = f.getChannel();
					fc2.truncate(0);
					Iterator<LogRec> iter = new ForwardLogIterator(fn);
					ArrayList<LogRec> toArchive = new ArrayList<LogRec>();
					ArrayList<LogRec> toKeep = new ArrayList<LogRec>();
					while (iter.hasNext())
					{
						LogRec rec = iter.next();
						long txnum = rec.txnum();
						if (txList.contains(txnum))
						{
							toKeep.add(rec);
						}
						else
						{
							toArchive.add(rec);
						}
					}

					if ((force || HRDBMSWorker.getHParms().getProperty("archive").equals("true")) && toArchive.size() > 0)
					{
						String name = HRDBMSWorker.getHParms().getProperty("archive_dir");
						if (!name.endsWith("/"))
						{
							name += "/";
						}

						name += (fn.substring(fn.lastIndexOf('/') + 1) + toArchive.get(0).lsn() + ".archive");
						final File t = new File(name);
						RandomAccessFile f1 = null;
						while (true)
						{
							try
							{
								f1 = new RandomAccessFile(t, "rwd");
								break;
							}
							catch (FileNotFoundException e)
							{
								ResourceManager.panic = true;
								try
								{
									Thread.sleep(Integer.parseInt(HRDBMSWorker.getHParms().getProperty("rm_sleep_time_ms")) / 2);
								}
								catch (Exception g)
								{
								}
							}
						}
						FileChannel fc1 = f1.getChannel();

						fc1.position(0);
						int total = 0;
						for (LogRec rec : toArchive)
						{
							total += (8 + rec.buffer().limit());
						}

						final ByteBuffer size = ByteBuffer.allocate(total);
						size.position(0);
						for (LogRec rec : toArchive)
						{
							size.putInt(rec.size());
							rec.buffer().position(0);
							size.put(rec.buffer());
							size.putInt(rec.size());
						}

						size.position(0);
						fc1.write(size);
						fc1.close();
						f1.close();
					}

					fc2.position(0);
					long tot = 0;
					for (LogRec rec : toKeep)
					{
						tot += (8 + rec.buffer().limit());
					}

					int total = 0;
					if (tot > Integer.MAX_VALUE)
					{
						for (LogRec rec : toKeep)
						{
							ByteBuffer bb = ByteBuffer.allocate(8 + rec.buffer().limit());
							bb.putInt(rec.size());
							rec.buffer().position(0);
							bb.put(rec.buffer());
							bb.putInt(rec.size());
							bb.position(0);
							fc2.write(bb);
						}

						Transaction.txListLock.writeLock().unlock();
						return f;
					}
					else
					{
						total = (int)tot;
					}
					final ByteBuffer size = ByteBuffer.allocate(total);
					size.position(0);
					for (LogRec rec : toKeep)
					{
						size.putInt(rec.size());
						rec.buffer().position(0);
						size.put(rec.buffer());
						size.putInt(rec.size());
					}

					size.position(0);
					fc2.write(size);

					Transaction.txListLock.writeLock().unlock();
					return f;
				}
				catch (Exception e)
				{
					Transaction.txListLock.writeLock().unlock();
					throw e;
				}
			}
		}
	}

	public static Iterator<LogRec> archiveIterator(String fn, boolean flush)
	{
		try
		{
			return new ArchiveIterator(fn);
		}
		catch (Exception e)
		{
			return null;
		}
	}

	public static void commit(long txnum) throws Exception
	{
		commit(txnum, filename);
	}

	public static void commit(long txnum, String fn) throws Exception
	{
		final LogRec rec = new CommitLogRec(txnum);
		write(rec, fn);
		flush(rec.lsn(), fn);
	}

	public static void commitNoFlush(long txnum) throws Exception
	{
		commitNoFlush(txnum, filename);
	}

	public static void commitNoFlush(long txnum, String fn) throws Exception
	{
		final LogRec rec = new CommitLogRec(txnum);
		write(rec, fn);
		flushNoForce(rec.lsn(), fn);
	}

	public static DeleteLogRec delete(long txnum, Block b, int off, byte[] before, byte[] after) throws Exception
	{
		return delete(txnum, b, off, before, after, filename);
	}

	public static DeleteLogRec delete(long txnum, Block b, int off, byte[] before, byte[] after, String fn) throws Exception
	{
		final DeleteLogRec rec = new DeleteLogRec(txnum, b, off, before.clone(), after.clone());
		write(rec, fn);
		return rec;
	}

	public static ExtendLogRec extend(long txnum, Block b) throws Exception
	{
		return extend(txnum, b, filename);
	}

	public static ExtendLogRec extend(long txnum, Block b, String fn) throws Exception
	{
		final ExtendLogRec rec = new ExtendLogRec(txnum, b);
		write(rec, fn);
		return rec;
	}

	public static void flush(long lsn) throws IOException
	{
		flush(lsn, filename);
	}

	public static void flush(long lsn, String fn) throws IOException
	{
		final ArrayDeque<LogRec> list = logs.get(fn);
		// synchronized (noArchiveLock)
		Transaction.txListLock.writeLock().lock();
		{
			try
			{
				HashMap<BlockAndTransaction, ArrayList<LogRec>> toWrite = new HashMap<BlockAndTransaction, ArrayList<LogRec>>();
				TreeMap<LogRec, LogRec> ordered = new TreeMap<LogRec, LogRec>();
				final FileChannel fc = getFile(fn);
				// synchronized (Transaction.txList)
				{
					while (list.size() > 0)
					{
						LogRec rec = list.getFirst();
						if (rec.lsn() <= lsn)
						{
							ordered.put(rec, rec);
							if (rec.type() == LogRec.INSERT)
							{
								InsertLogRec ins = (InsertLogRec)rec;
								Block block = ins.getBlock();
								long t = ins.txnum();
								BlockAndTransaction key = new BlockAndTransaction(block, t);
								ArrayList<LogRec> myToWrite = toWrite.get(key);
								if (myToWrite == null)
								{
									myToWrite = new ArrayList<LogRec>();
									myToWrite.add(ins);
									toWrite.put(key, myToWrite);
								}
								else
								{
									myToWrite.add(ins);
								}
							}
							else if (rec.type() == LogRec.DELETE)
							{
								DeleteLogRec ins = (DeleteLogRec)rec;
								Block block = ins.getBlock();
								long t = ins.txnum();
								BlockAndTransaction key = new BlockAndTransaction(block, t);
								ArrayList<LogRec> myToWrite = toWrite.get(key);
								if (myToWrite == null)
								{
									myToWrite = new ArrayList<LogRec>();
									myToWrite.add(ins);
									toWrite.put(key, myToWrite);
								}
								else
								{
									myToWrite.add(ins);
								}
							}

							list.removeFirst();
						}
						else
						{
							break;
						}
					}
				}

				if (ordered.size() == 0)
				{
					Transaction.txListLock.writeLock().unlock();
					return;
				}

				try
				{
					// consolidate stuff in toWrite
					for (Map.Entry<BlockAndTransaction, ArrayList<LogRec>> entry : toWrite.entrySet())
					{
						ArrayList<LogRec> value = entry.getValue();
						int size = value.size();
						if (size == 1)
						{
							continue;
						}

						PageRegions regions = new PageRegions(value.get(0));
						int i = 1;
						while (i < size)
						{
							regions.add(value.get(i++));
						}

						ArrayList<LogRec> recs = regions.generateLogRecs();

						for (LogRec rec : recs)
						{
							ordered.remove(rec);
							ordered.put(rec, rec);
						}

						recs = regions.generateRemovals();
						for (LogRec rec : recs)
						{
							ordered.remove(rec);
						}
					}

					synchronized (fc)
					{
						fc.position(fc.size());
						int total = 0;
						for (LogRec rec : ordered.keySet())
						{
							total += (8 + rec.buffer().limit());
						}

						final ByteBuffer size = ByteBuffer.allocate(total);
						size.position(0);
						for (LogRec rec : ordered.keySet())
						{
							if (rec.size() < 28)
							{
								throw new Exception("Tried to flush log rec of size " + rec.size() + ". Record was of type " + rec.type());
							}
							size.putInt(rec.size());
							rec.buffer().position(0);
							size.put(rec.buffer());
							size.putInt(rec.size());
						}

						size.position(0);
						fc.write(size);
						fc.force(false);
					}
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.fatal("Log flush failure!", e);
					System.exit(1);
				}
			}
			catch (Exception e)
			{
				Transaction.txListLock.writeLock().unlock();
				throw e;
			}
		}
		Transaction.txListLock.writeLock().unlock();
	}

	public static void flushNoForce(long lsn, String fn) throws IOException
	{
		final ArrayDeque<LogRec> list = logs.get(fn);
		// synchronized (noArchiveLock)
		Transaction.txListLock.writeLock().lock();
		{
			try
			{
				HashMap<BlockAndTransaction, ArrayList<LogRec>> toWrite = new HashMap<BlockAndTransaction, ArrayList<LogRec>>();
				TreeMap<LogRec, LogRec> ordered = new TreeMap<LogRec, LogRec>();
				final FileChannel fc = getFile(fn);
				// synchronized (Transaction.txList)
				{
					while (list.size() > 0)
					{
						LogRec rec = list.getFirst();
						if (rec.lsn() <= lsn)
						{
							ordered.put(rec, rec);
							if (rec.type() == LogRec.INSERT)
							{
								InsertLogRec ins = (InsertLogRec)rec;
								Block block = ins.getBlock();
								long t = ins.txnum();
								BlockAndTransaction key = new BlockAndTransaction(block, t);
								ArrayList<LogRec> myToWrite = toWrite.get(key);
								if (myToWrite == null)
								{
									myToWrite = new ArrayList<LogRec>();
									myToWrite.add(ins);
									toWrite.put(key, myToWrite);
								}
								else
								{
									myToWrite.add(ins);
								}
							}
							else if (rec.type() == LogRec.DELETE)
							{
								DeleteLogRec ins = (DeleteLogRec)rec;
								Block block = ins.getBlock();
								long t = ins.txnum();
								BlockAndTransaction key = new BlockAndTransaction(block, t);
								ArrayList<LogRec> myToWrite = toWrite.get(key);
								if (myToWrite == null)
								{
									myToWrite = new ArrayList<LogRec>();
									myToWrite.add(ins);
									toWrite.put(key, myToWrite);
								}
								else
								{
									myToWrite.add(ins);
								}
							}

							list.removeFirst();
						}
						else
						{
							break;
						}
					}
				}

				if (ordered.size() == 0)
				{
					Transaction.txListLock.writeLock().unlock();
					return;
				}

				try
				{
					// consolidate stuff in toWrite
					for (Map.Entry<BlockAndTransaction, ArrayList<LogRec>> entry : toWrite.entrySet())
					{
						ArrayList<LogRec> value = entry.getValue();
						int size = value.size();
						if (size == 1)
						{
							continue;
						}

						PageRegions regions = new PageRegions(value.get(0));
						int i = 1;
						while (i < size)
						{
							regions.add(value.get(i++));
						}

						ArrayList<LogRec> recs = regions.generateLogRecs();

						for (LogRec rec : recs)
						{
							ordered.remove(rec);
							ordered.put(rec, rec);
						}

						recs = regions.generateRemovals();
						for (LogRec rec : recs)
						{
							ordered.remove(rec);
						}
					}

					synchronized (fc)
					{
						fc.position(fc.size());
						int total = 0;
						for (LogRec rec : ordered.keySet())
						{
							total += (8 + rec.buffer().limit());
						}

						final ByteBuffer size = ByteBuffer.allocate(total);
						size.position(0);
						for (LogRec rec : ordered.keySet())
						{
							if (rec.size() < 28)
							{
								throw new Exception("Tried to flush log rec of size " + rec.size() + ". Record was of type " + rec.type());
							}
							size.putInt(rec.size());
							rec.buffer().position(0);
							size.put(rec.buffer());
							size.putInt(rec.size());
						}

						size.position(0);
						fc.write(size);
						// fc.force(false);
					}
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.fatal("Log flush failure!", e);
					System.exit(1);
				}
			}
			catch (Exception e)
			{
				Transaction.txListLock.writeLock().unlock();
				throw e;
			}
		}
		Transaction.txListLock.writeLock().unlock();
	}

	public static Iterator<LogRec> forwardIterator()
	{
		return forwardIterator(filename);
	}

	public static Iterator<LogRec> forwardIterator(String fn)
	{
		try
		{
			return new ForwardLogIterator(fn);
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.error("Error creating ForwardLogIterator.", e);
			return null;
		}
	}

	// public static void flushAll() throws IOException
	// {
	// final ArrayDeque<LogRec> list = logs.get(filename);
	// if (list == null)
	// {
	// return;
	// }
	//
	// LogRec last = null;
	// synchronized (Transaction.txList)
	// {
	// last = list.peekLast();
	// }
	//
	// if (last != null)
	// {
	// flush(last.lsn());
	// }
	// }

	public static synchronized FileChannel getFile(String filename) throws IOException
	{
		FileChannel fc = openFiles.get(filename);

		if (fc == null)
		{
			final File table = new File(filename);
			RandomAccessFile f = null;
			while (true)
			{
				try
				{
					f = new RandomAccessFile(table, "rw");
					break;
				}
				catch (FileNotFoundException e)
				{
					ResourceManager.panic = true;
					try
					{
						Thread.sleep(Integer.parseInt(HRDBMSWorker.getHParms().getProperty("rm_sleep_time_ms")) / 2);
					}
					catch (Exception g)
					{
					}
				}
			}
			fc = f.getChannel();
			openFiles.put(filename, fc);
			logs.put(filename, new ArrayDeque<LogRec>());
		}

		return fc;
	}

	public static long getLSN()
	{
		return last_lsn.incrementAndGet();
	}

	public static InsertLogRec insert(long txnum, Block b, int off, byte[] before, byte[] after) throws Exception
	{
		return insert(txnum, b, off, before, after, filename);
	}

	public static InsertLogRec insert(long txnum, Block b, int off, byte[] before, byte[] after, String fn) throws Exception
	{
		final InsertLogRec rec = new InsertLogRec(txnum, b, off, before.clone(), after.clone());
		write(rec, fn);
		return rec;
	}

	public static Iterator<LogRec> iterator()
	{
		return iterator(filename);
	}

	public static Iterator<LogRec> iterator(String fn)
	{
		try
		{
			return new LogIterator(fn);
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.error("Error creating LogIterator.", e);
			return null;
		}
	}

	public static Iterator<LogRec> iterator(String fn, boolean flush)
	{
		if (flush)
		{
			return iterator(fn);
		}

		try
		{
			return new LogIterator(fn, false);
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.error("Error creating LogIterator.", e);
			return null;
		}
	}

	public static void notReady(long txnum) throws IOException
	{
		notReady(txnum, filename);
	}

	public static void notReady(long txnum, String fn) throws IOException
	{
		final LogRec rec = new NotReadyLogRec(txnum);
		write(rec, fn);
		flush(rec.lsn(), fn);
	}

	public static Iterator<LogRec> partialIterator()
	{
		return partialIterator(filename);
	}

	public static Iterator<LogRec> partialIterator(String fn)
	{
		try
		{
			return new PartialLogIterator(fn);
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.error("Error creating PartialLogIterator.", e);
			return null;
		}
	}

	public static Iterator<LogRec> partialIterator(String fn, boolean flush)
	{
		if (flush)
		{
			return partialIterator(fn);
		}

		try
		{
			return new PartialLogIterator(fn, false);
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.error("Error creating PartialLogIterator.", e);
			return null;
		}
	}

	public static void ready(long txnum, String host) throws IOException
	{
		ready(txnum, host, filename);
	}

	public static void ready(long txnum, String host, String fn) throws IOException
	{
		final LogRec rec = new ReadyLogRec(txnum, host);
		write(rec, fn);
		flush(rec.lsn(), fn);
	}

	public static void rollback(long txnum) throws Exception
	{
		rollback(txnum, filename);
	}

	public static void rollback(long txnum, String fn) throws Exception
	{
		LogRec rec = new RollbackLogRec(txnum);
		write(rec, fn);
		flush(rec.lsn(), fn);
		final Iterator<LogRec> iter = iterator(fn);
		while (iter.hasNext())
		{
			rec = iter.next();
			if (rec.txnum() == txnum)
			{
				if (rec.type() == LogRec.START)
				{
					((LogIterator)iter).close();
					return;
				}

				rec.rebuild().undo();
			}
		}

		((LogIterator)iter).close();
	}

	public static TruncateLogRec truncate(long txnum, Block b) throws Exception
	{
		return truncate(txnum, b, filename);
	}

	public static TruncateLogRec truncate(long txnum, Block b, String fn) throws Exception
	{
		final TruncateLogRec rec = new TruncateLogRec(txnum, b);
		write(rec, fn);
		return rec;
	}

	public static long write(LogRec rec)
	{
		return write(rec, filename);
	}

	public static long write(LogRec rec, String fn)
	{
		final ArrayDeque<LogRec> list = logs.get(fn);
		rec.setTimeStamp(System.currentTimeMillis());
		long retval;
		retval = getLSN();
		rec.setLSN(retval);
		Transaction.txListLock.writeLock().lock();
		{
			list.add(rec);
		}
		Transaction.txListLock.writeLock().unlock();

		return retval;
	}

	public void recover() throws Exception
	{
		recover(filename);
	}

	public void recover(String fn) throws Exception
	{
		String oldFN = null;
		if (HRDBMSWorker.type == HRDBMSWorker.TYPE_COORD || HRDBMSWorker.type == HRDBMSWorker.TYPE_MASTER)
		{
			oldFN = fn;
			fn = fn.substring(0, fn.length() - 10) + "xa.log";
		}

		final HashSet<Long> commitList = new HashSet<Long>();
		final HashSet<Long> rollbackList = new HashSet<Long>();
		final HashSet<Long> needsCommit = new HashSet<Long>();
		HashSet<String> truncated = new HashSet<String>();
		HashMap<String, Long> trunc2LSN = new HashMap<String, Long>();
		Transaction.txListLock.writeLock().lock();
		while (fn != null)
		{
			logs.get(fn);
			{
				try
				{
					final Iterator<LogRec> iter = iterator(fn, false);
					while (iter.hasNext())
					{
						final LogRec rec = iter.next();
						if (rec.type() == LogRec.COMMIT)
						{
							if (rollbackList.contains(rec.txnum()))
							{
								HRDBMSWorker.logger.debug("Found a COMMIT record for " + rec.txnum() + ", but we already processed an abort");
							}
							else
							{
								commitList.add(rec.txnum());
								HRDBMSWorker.logger.debug("Saw COMMIT for " + rec.txnum());
							}
						}
						else if (rec.type() == LogRec.ROLLB || rec.type() == LogRec.NOTREADY)
						{
							if (commitList.contains(rec.txnum()))
							{
								HRDBMSWorker.logger.debug("Found an abort record for " + rec.txnum() + " but we already processed a COMMIT");
							}
							else
							{
								rollbackList.add(rec.txnum());
								HRDBMSWorker.logger.debug("Saw ROLLB/NOTREADY for " + rec.txnum());
							}
						}
						else if (rec.type() == LogRec.TRUNCATE)
						{
							if (commitList.contains(rec.txnum()))
							{
								TruncateLogRec trunc = (TruncateLogRec)rec.rebuild();
								truncated.add(trunc.getBlock().fileName());
								trunc2LSN.put(trunc.getBlock().fileName(), trunc.lsn());
							}
						}
						else if (rec.type() == LogRec.READY)
						{
							if (rollbackList.contains(rec.txnum()) || commitList.contains(rec.txnum()))
							{
							}
							else
							{
								ReadyLogRec xa = (ReadyLogRec)rec.rebuild();
								if (XAManager.askXAManager(xa))
								{
									commitList.add(rec.txnum());
									needsCommit.add(rec.txnum());
									HRDBMSWorker.logger.debug("Saw READY for " + rec.txnum() + " decided it was a COMMIT");
								}
								else
								{
									rollbackList.add(rec.txnum());
									HRDBMSWorker.logger.debug("Saw READY for " + rec.txnum() + " decided it was a ROLLB");
								}
							}
						}
						else if (rec.type() == LogRec.XACOMMIT)
						{
							if (rollbackList.contains(rec.txnum()))
							{
								HRDBMSWorker.logger.debug("Found an XACOMMIT record for " + rec.txnum() + " but we already processed an abort");
							}
							else
							{
								if (!commitList.contains(rec.txnum()))
								{
									HRDBMSWorker.logger.debug("Saw XACOMMIT for " + rec.txnum());
									XACommitLogRec xa = (XACommitLogRec)rec.rebuild();
									// XAManager.phase2(xa.txnum(),
									// xa.getNodes());
									XAManager.in.put("COMMIT");
									XAManager.in.put(xa.txnum());
									XAManager.in.put(xa.getNodes());
									commitList.add(rec.txnum());
								}
							}
						}
						else if (rec.type() == LogRec.XAABORT)
						{
							if (commitList.contains(rec.txnum()))
							{
								HRDBMSWorker.logger.debug("Found an XAABORT record for " + rec.txnum() + " but we already processed a COMMIT");
							}
							else
							{
								if (!rollbackList.contains(rec.txnum()))
								{
									HRDBMSWorker.logger.debug("Saw XAABORT for " + rec.txnum());
									XAAbortLogRec xa = (XAAbortLogRec)rec.rebuild();
									// XAManager.rollbackP2(xa.txnum(),
									// xa.getNodes());
									XAManager.in.put("ROLLBACK");
									XAManager.in.put(xa.txnum());
									XAManager.in.put(xa.getNodes());
									rollbackList.add(rec.txnum());
								}
							}
						}
						else if (rec.type() == LogRec.PREPARE)
						{
							if (rollbackList.contains(rec.txnum()) || commitList.contains(rec.txnum()))
							{
							}
							else
							{
								HRDBMSWorker.logger.debug("Saw PREPARE for " + rec.txnum());
								PrepareLogRec xa = (PrepareLogRec)rec.rebuild();
								XAManager.rollback(xa.txnum(), xa.getNodes());
								rollbackList.add(rec.txnum());
							}
						}
						// else if (rec.type() == LogRec.INSERT || rec.type() ==
						// LogRec.DELETE || rec.type() == LogRec.EXTEND)
						// {
						// if ((!commitList.contains(rec.txnum())) &&
						// (!rollbackList.contains(rec.txnum())))
						// {
						// HRDBMSWorker.logger.debug("Saw stranded changes for "
						// +
						// rec.txnum());
						// rec.rebuild().undo();
						// }
						// }
					}

					final Iterator<LogRec> iter2 = forwardIterator(fn);
					((LogIterator)iter).close();

					while (iter2.hasNext())
					{
						final LogRec rec = iter2.next();
						if (rec.type() == LogRec.INSERT)
						{
							if (commitList.contains(rec.txnum()))
							{
								InsertLogRec i = (InsertLogRec)rec.rebuild();
								if (!truncated.contains(i.getBlock().fileName()) || i.lsn() > trunc2LSN.get(i.getBlock().fileName()))
								{
									i.redo();
								}
							}
						}
						else if (rec.type() == LogRec.DELETE)
						{
							if (commitList.contains(rec.txnum()))
							{
								DeleteLogRec i = (DeleteLogRec)rec.rebuild();
								if (!truncated.contains(i.getBlock().fileName()) || i.lsn() > trunc2LSN.get(i.getBlock().fileName()))
								{
									i.redo();
								}
							}
						}
						else if (rec.type() == LogRec.EXTEND)
						{
							if (commitList.contains(rec.txnum()))
							{
								ExtendLogRec i = (ExtendLogRec)rec.rebuild();
								if (!truncated.contains(i.getBlock().fileName()) || i.lsn() > trunc2LSN.get(i.getBlock().fileName()))
								{
									i.redo();
								}
							}
						}
					}

					((ForwardLogIterator)iter2).close();
					for (long txnum : needsCommit)
					{
						LogManager.commit(txnum);
					}

					needsCommit.clear();
					// final LogRec rec = new NQCheckLogRec(new
					// HashSet<Long>());
					// write(rec, fn);
					// flush(rec.lsn(), fn);
					fn = oldFN;
					oldFN = null;
				}
				catch (Exception e)
				{
					Transaction.txListLock.writeLock().unlock();
					throw e;
				}
			}
		}
		Transaction.txListLock.writeLock().unlock();
	}

	// public static void writeStartRecIfNeeded(long txnum)
	// {
	// ArrayDeque<LogRec> list = logs.get(filename);
	// synchronized (Transaction.txList)
	// {
	// for (LogRec rec : list)
	// {
	// if (rec.txnum() == txnum)
	// {
	// if (rec.type() == LogRec.START)
	// {
	// return;
	// }
	// }
	// }
	// }
	//
	// Iterator<LogRec> iter = partialIterator(filename, false);
	// while (iter.hasNext())
	// {
	// LogRec rec = iter.next();
	// if (rec.txnum() == txnum)
	// {
	// ((PartialLogIterator)iter).close();
	// return;
	// }
	// }
	//
	// ((PartialLogIterator)iter).close();
	// write(new StartLogRec(txnum), filename);
	// }

	// public static void writeStartRecIfNeeded(long txnum, String filename)
	// {
	// ArrayDeque<LogRec> list = logs.get(filename);
	// synchronized (Transaction.txList)
	// {
	// for (LogRec rec : list)
	// {
	// if (rec.txnum() == txnum)
	// {
	// if (rec.type() == LogRec.START)
	// {
	// return;
	// }
	// }
	// }
	// }
	//
	// Iterator<LogRec> iter = iterator(filename, false);
	// while (iter.hasNext())
	// {
	// LogRec rec = iter.next();
	// if (rec.txnum() == txnum)
	// {
	// if (rec.type() == LogRec.START)
	// {
	// ((LogIterator)iter).close();
	// return;
	// }
	// }
	// }

	// ((LogIterator)iter).close();
	//
	// write(new StartLogRec(txnum), filename);
	// }

	@Override
	public void run()
	{
		filename = HRDBMSWorker.getHParms().getProperty("log_dir");
		if (!filename.endsWith("/"))
		{
			filename += "/";
		}
		String filename2 = new String(filename);
		filename += "active.log";
		filename2 += "xa.log";
		File log = new File(filename);
		if (!log.exists())
		{
			try
			{
				log.createNewFile();
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("Error creating the active log file.", e);
				// in = null;
				System.exit(1);
				return;
			}
		}
		try
		{
			getFile(filename);
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.error("Error getting a FileChannel for " + filename, e);
			// in = null;
			System.exit(1);
			return;
		}

		if (HRDBMSWorker.type == HRDBMSWorker.TYPE_COORD || HRDBMSWorker.type == HRDBMSWorker.TYPE_MASTER)
		{
			log = new File(filename2);
			if (!log.exists())
			{
				try
				{
					log.createNewFile();
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.error("Error creating the xa log file.", e);
					// in = null;
					System.exit(1);
					return;
				}
			}
			try
			{
				getFile(filename2);
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("Error getting a FileChannel for " + filename2, e);
				// in = null;
				System.exit(1);
				return;
			}
		}

		try
		{
			recover(); // sync on everything possible to delay every synchronous
			// method call
			LockManager.verifyClear();
			recoverDone = true;
			XAManager.rP1 = true;
			HRDBMSWorker.checkpoint.doCheckpoint();
		}
		catch (final Throwable e)
		{
			HRDBMSWorker.logger.error("Error during log recovery.", e);
			// in = null;
			System.exit(1);
			return;
		}

		HRDBMSWorker.logger.info("Log Manager initialization complete.");
		/*
		 * while (true) { final String msg = in.poll(); if (msg != null) {
		 * processMessage(msg); }
		 *
		 * for (final Map.Entry<String, ArrayDeque<LogRec>> entry :
		 * logs.entrySet()) { final ArrayDeque<LogRec> list = entry.getValue();
		 * final String fn = entry.getKey(); LogRec last = null; last =
		 * list.peekLast();
		 *
		 * if (last != null) { try { flush(last.lsn(), fn); } catch (Exception
		 * e) { HRDBMSWorker.logger.fatal("Failure flushing logs", e);
		 * System.exit(1); } } }
		 *
		 * try { Thread.sleep(sleepSecs * 1000); } catch (final
		 * InterruptedException e) { } }
		 */
	}

	private static class BlockAndTransaction
	{
		private final Block b;
		private final long t;

		public BlockAndTransaction(Block b, long t)
		{
			this.b = b;
			this.t = t;
		}

		@Override
		public boolean equals(Object rhs)
		{
			if (rhs == null)
			{
				return false;
			}

			if (!(rhs instanceof BlockAndTransaction))
			{
				return false;
			}

			BlockAndTransaction r = (BlockAndTransaction)rhs;
			return b.equals(r.b) && t == r.t;
		}

		@Override
		public int hashCode()
		{
			int retval = 17;
			retval = 23 * retval + b.hashCode();
			retval = 23 * retval + new Long(t).hashCode();
			return retval;
		}
	}

	private static class PageRegions
	{
		private final HashMap<Integer, LogRec> starts = new HashMap<Integer, LogRec>();
		private final ArrayList<LogRec> remove = new ArrayList<LogRec>();
		private final SparseByteArray before = new SparseByteArray();
		private final SparseByteArray after = new SparseByteArray();

		public PageRegions(LogRec rec)
		{
			if (rec.type() == LogRec.INSERT)
			{
				InsertLogRec ins = (InsertLogRec)rec;
				starts.put(ins.getOffset(), ins);
				before.putBefore(ins.getOffset(), ins.getBefore());
				after.put(ins.getOffset(), ins.getAfter());
				return;
			}

			DeleteLogRec ins = (DeleteLogRec)rec;
			starts.put(ins.getOffset(), ins);
			before.putBefore(ins.getOffset(), ins.getBefore());
			after.put(ins.getOffset(), ins.getAfter());
			return;
		}

		public void add(LogRec rec)
		{
			if (rec.type() == LogRec.INSERT)
			{
				InsertLogRec ins = (InsertLogRec)rec;
				if (!starts.containsKey(ins.getOffset()))
				{
					starts.put(ins.getOffset(), ins);
				}
				else
				{
					remove.add(ins);
				}

				before.putBefore(ins.getOffset(), ins.getBefore());
				after.put(ins.getOffset(), ins.getAfter());
				return;
			}

			DeleteLogRec ins = (DeleteLogRec)rec;
			if (!starts.containsKey(ins.getOffset()))
			{
				starts.put(ins.getOffset(), ins);
			}
			else
			{
				remove.add(ins);
			}

			before.putBefore(ins.getOffset(), ins.getBefore());
			after.put(ins.getOffset(), ins.getAfter());
			return;
		}

		public ArrayList<LogRec> generateLogRecs() throws Exception
		{
			ArrayList<LogRec> retval = new ArrayList<LogRec>();
			ArrayList<Integer> offs = before.getOffsets();
			for (int off : offs)
			{
				LogRec rec = starts.remove(off);
				if (rec.type() == LogRec.INSERT)
				{
					InsertLogRec newRec = new InsertLogRec(rec.txnum(), ((InsertLogRec)rec).getBlock(), ((InsertLogRec)rec).getOffset(), before.getArray(off), after.getArray(off));
					newRec.setLSN(rec.lsn());
					newRec.setTimeStamp(rec.getTimeStamp());
					retval.add(newRec);
				}
				else
				{
					DeleteLogRec newRec = new DeleteLogRec(rec.txnum(), ((DeleteLogRec)rec).getBlock(), ((DeleteLogRec)rec).getOffset(), before.getArray(off), after.getArray(off));
					newRec.setLSN(rec.lsn());
					newRec.setTimeStamp(rec.getTimeStamp());
					retval.add(newRec);
				}
			}

			return retval;
		}

		public ArrayList<LogRec> generateRemovals()
		{
			ArrayList<LogRec> retval = new ArrayList<LogRec>();
			for (LogRec rec : starts.values())
			{
				retval.add(rec);
			}

			for (LogRec rec : remove)
			{
				retval.add(rec);
			}

			return retval;
		}
	}

	private static class SparseByteArray
	{
		private final Byte[] bytes;

		public SparseByteArray()
		{
			bytes = new Byte[Page.BLOCK_SIZE];
			int i = 0;
			while (i < Page.BLOCK_SIZE)
			{
				bytes[i] = null;
				i++;
			}
		}

		public byte[] getArray(int off)
		{
			int i = off;
			while (i < Page.BLOCK_SIZE && bytes[i] != null)
			{
				i++;
			}

			int length = i - off;
			byte[] retval = new byte[length];
			i = off;
			int j = 0;
			while (j < length)
			{
				retval[j] = bytes[i];
				i++;
				j++;
			}

			return retval;
		}

		public ArrayList<Integer> getOffsets()
		{
			ArrayList<Integer> retval = new ArrayList<Integer>();
			int offset = -1;
			int i = 0;
			while (i < Page.BLOCK_SIZE)
			{
				if (offset == -1 && bytes[i] != null)
				{
					offset = i;
					retval.add(offset);
				}
				else if (offset != -1 && bytes[i] == null)
				{
					offset = -1;
				}

				i++;
			}

			return retval;
		}

		public void put(int off, byte[] array)
		{
			int i = off;
			int j = 0;
			final int size = array.length;
			while (j < size)
			{
				bytes[i] = array[j];
				i++;
				j++;
			}
		}

		public void putBefore(int off, byte[] array)
		{
			int i = off;
			int j = 0;
			final int size = array.length;
			while (j < size)
			{
				if (bytes[i] == null)
				{
					bytes[i] = array[j];
				}

				j++;
				i++;
			}
		}
	}
}