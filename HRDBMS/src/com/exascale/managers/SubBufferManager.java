package com.exascale.managers;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import com.exascale.exceptions.BufferPoolExhaustedException;
import com.exascale.filesystem.Block;
import com.exascale.filesystem.Page;
import com.exascale.misc.MultiHashMap;
import com.exascale.misc.ScalableStampedReentrantRWLock;
import com.exascale.tables.Schema;
import com.exascale.tables.Transaction;
import com.exascale.threads.HRDBMSThread;
import com.exascale.threads.Read3Thread;
import com.exascale.threads.ReadThread;

public class SubBufferManager
{
	Page[] bp;
	private final int numAvailable;
	private int numNotTouched;
	private final Map<Block, Integer> pageLookup;
	private final boolean log;
	private final MultiHashMap<Long, Page> myBuffers;
	private volatile int clock = 0;
	private final ScalableStampedReentrantRWLock lock2 = new ScalableStampedReentrantRWLock();
	public Lock lock;

	public SubBufferManager(final boolean log)
	{
		this.log = log;
		myBuffers = new MultiHashMap<Long, Page>();

		numAvailable = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("bp_pages")) / BufferManager.mLength;
		numNotTouched = numAvailable;
		bp = new Page[numAvailable];
		clock = numAvailable / 2;
		int i = 0;
		while (i < numAvailable)
		{
			bp[i] = new Page();
			i++;
		}

		pageLookup = new HashMap<Block, Integer>(numAvailable);
		lock = lock2.writeLock();
	}

	public boolean cleanPage(final int i) throws Exception
	{
		try
		{
			lock.lock();
			if (i >= bp.length)
			{
				lock.unlock();
				return false;
			}

			final Page p = bp[i];
			if (p.isModified() && !p.isPinned())
			{
				p.setNotModified();
				FileManager.writeDelayed(p.block(), p.buffer());
				lock.unlock();
				return true;
			}

			lock.unlock();
			return false;
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.debug("", e);
			lock.unlock();
			throw e;
		}
	}

	public Set<String> flushAll(final FileChannel fc) throws Exception
	{
		final List<Page> toPut = new ArrayList<Page>();
		for (final Page p : bp)
		{
			if (p.isModified())
			{
				toPut.add(p);
			}
		}

		// start put threads
		PutThread putThread = null;
		putThread = new PutThread(toPut);
		putThread.start();

		final Set<String> retval = new HashSet<String>();
		for (final Page p : toPut)
		{
			retval.add(p.block().fileName());
		}

		Exception e = null;
		putThread.join();
		e = putThread.getException();

		if (e != null)
		{
			throw e;
		}

		final int desired = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("bp_pages")) / BufferManager.mLength;
		int i = bp.length - 1;
		final int starting = bp.length;
		int toTrim = 0;

		while (i >= 0 && starting - toTrim > desired && !bp[i].isModified() && !bp[i].isPinned())
		{
			pageLookup.remove(bp[i].block());
			toTrim++;
			i--;
		}

		if (toTrim > 0)
		{
			final int newLength = starting - toTrim;

			final Page[] bp2 = new Page[newLength];
			System.arraycopy(bp, 0, bp2, 0, newLength);
			bp = bp2;
			HRDBMSWorker.logger.debug("Trimmed " + toTrim + " pages off of an SBP");
		}

		return retval;
	}

	public Page getPage(final Block b, final long txnum)
	{
		lock.lock();
		try
		{
			final Integer index = pageLookup.get(b);

			if (index == null)
			{
				lock.unlock();
				return null;
			}

			final Page retval = bp[index];

			if (!retval.isPinnedBy(txnum))
			{
				lock.unlock();
				return null;
			}

			lock.unlock();
			return retval;
		}
		catch (final Throwable e)
		{
			HRDBMSWorker.logger.debug("", e);
			lock.unlock();
			throw e;
		}
	}

	public void invalidateFile(final String fn) throws Exception
	{
		lock.lock();
		try
		{
			for (final Page p : bp)
			{
				if (p.block() != null)
				{
					if (p.block().fileName().contains("/" + fn))
					{
						p.setPinTime(-1);
						pageLookup.remove(p.block());
						p.assignToBlock(null, log);
					}
				}
			}

			lock.unlock();
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.debug("", e);
			lock.unlock();
			throw e;
		}
	}

	public ReadThread pin(final Block b, final long txnum) throws Exception
	{
		ReadThread retval = null;
		// Transaction.txListLock.readLock().lock();
		{
			try
			{
				while (true)
				{
					// if (wait)
					// {
					// LockSupport.parkNanos(500);
					// }

					lock.lock();
					try
					{
						int index = findExistingPage(b);
						if (index == -1)
						{
							// if
							// (!Transaction.txListLock.tryLock())
							// {
							// wait = true;
							// continue;
							// }

							index = chooseUnpinnedPage(b.fileName());

							if (index == -1)
							{
								HRDBMSWorker.logger.error("Buffer pool exhausted.");
								lock.unlock();
								throw new BufferPoolExhaustedException();
							}

							if (numNotTouched > 0)
							{
								numNotTouched--;
							}

							if (bp[index].block() != null)
							{
								bp[index].setPinTime(-1);
								pageLookup.remove(bp[index].block());
							}
							try
							{
								retval = bp[index].assignToBlock(b, log);
							}
							catch (final Exception e)
							{
								// Transaction.txListLock.unlock();
								lock.unlock();
								throw e;
							}

							pageLookup.put(b, index);
							final long lsn = LogManager.getLSN();
							bp[index].pin(lsn, txnum);
							myBuffers.multiPut(txnum, bp[index]);
							// Transaction.txListLock.unlock();
							lock.unlock();
							return retval;
						}
						else
						{
							// if (!Transaction.txListLock.readLock().tryLock())
							// {
							// wait = true;
							// continue;
							// }

							final long lsn = LogManager.getLSN();
							bp[index].pin(lsn, txnum);
							myBuffers.multiPut(txnum, bp[index]);
							// Transaction.txListLock.readLock().unlock();
							lock.unlock();
							return retval;
						}

					}
					catch (final Exception e)
					{
						HRDBMSWorker.logger.debug("", e);
						lock.unlock();
						throw e;
					}
				}
			}
			catch (final Exception e)
			{
				// Transaction.txListLock.readLock().unlock();
				throw e;
			}
		}
	}

	public ReadThread pin(final Block b, final long txnum, final List<Integer> cols, final int layoutSize) throws Exception
	{
		ReadThread retval = null;
		// Transaction.txListLock.readLock().lock();
		{
			try
			{
				while (true)
				{
					// if (wait)
					// {
					// LockSupport.parkNanos(500);
					// }

					lock.lock();
					try
					{
						int index = findExistingPage(b);
						if (index == -1)
						{
							// if
							// (!Transaction.txListLock.tryLock())
							// {
							// wait = true;
							// continue;
							// }

							index = chooseUnpinnedPage(b.fileName());

							if (index == -1)
							{
								HRDBMSWorker.logger.error("Buffer pool exhausted.");
								lock.unlock();
								throw new BufferPoolExhaustedException();
							}

							if (numNotTouched > 0)
							{
								numNotTouched--;
							}

							if (bp[index].block() != null)
							{
								bp[index].setPinTime(-1);
								pageLookup.remove(bp[index].block());
							}
							try
							{
								retval = bp[index].assignToBlock(b, log, cols, layoutSize);
							}
							catch (final Exception e)
							{
								// Transaction.txListLock.unlock();
								lock.unlock();
								throw e;
							}

							pageLookup.put(b, index);
							final long lsn = LogManager.getLSN();
							bp[index].pin(lsn, txnum);
							myBuffers.multiPut(txnum, bp[index]);
							// Transaction.txListLock.unlock();
							lock.unlock();
							return retval;
						}
						else
						{
							// if (!Transaction.txListLock.readLock().tryLock())
							// {
							// wait = true;
							// continue;
							// }

							final long lsn = LogManager.getLSN();
							bp[index].pin(lsn, txnum);
							myBuffers.multiPut(txnum, bp[index]);
							// Transaction.txListLock.readLock().unlock();
							lock.unlock();
							return retval;
						}

					}
					catch (final Exception e)
					{
						HRDBMSWorker.logger.debug("", e);
						lock.unlock();
						throw e;
					}
				}
			}
			catch (final Exception e)
			{
				// Transaction.txListLock.readLock().unlock();
				throw e;
			}
		}
	}

	public ReadThread pin(final Block b, final long txnum, final List<Integer> cols, final int layoutSize, final int rank, final int rankSize) throws Exception
	{
		ReadThread retval = null;
		// Transaction.txListLock.readLock().lock();
		{
			try
			{
				while (true)
				{
					// if (wait)
					// {
					// LockSupport.parkNanos(500);
					// }

					lock.lock();
					try
					{
						int index = findExistingPage(b);
						if (index == -1)
						{
							// if
							// (!Transaction.txListLock.tryLock())
							// {
							// wait = true;
							// continue;
							// }

							index = chooseUnpinnedPage(b.fileName());

							if (index == -1)
							{
								HRDBMSWorker.logger.error("Buffer pool exhausted.");
								lock.unlock();
								throw new BufferPoolExhaustedException();
							}

							if (numNotTouched > 0)
							{
								numNotTouched--;
							}

							if (bp[index].block() != null)
							{
								bp[index].setPinTime(-1);
								pageLookup.remove(bp[index].block());
							}
							try
							{
								retval = bp[index].assignToBlock(b, log, cols, layoutSize, rank, rankSize);
							}
							catch (final Exception e)
							{
								// Transaction.txListLock.unlock();
								lock.unlock();
								throw e;
							}

							pageLookup.put(b, index);
							final long lsn = LogManager.getLSN();
							bp[index].pin(lsn, txnum);
							myBuffers.multiPut(txnum, bp[index]);
							// Transaction.txListLock.unlock();
							lock.unlock();
							return retval;
						}
						else
						{
							// if (!Transaction.txListLock.readLock().tryLock())
							// {
							// wait = true;
							// continue;
							// }

							final long lsn = LogManager.getLSN();
							bp[index].pin(lsn, txnum);
							myBuffers.multiPut(txnum, bp[index]);
							// Transaction.txListLock.readLock().unlock();
							lock.unlock();
							return retval;
						}

					}
					catch (final Exception e)
					{
						HRDBMSWorker.logger.debug("", e);
						lock.unlock();
						throw e;
					}
				}
			}
			catch (final Exception e)
			{
				// Transaction.txListLock.readLock().unlock();
				throw e;
			}
		}
	}

	public ReadThread pin(final Block b, final long txnum, final int rank, final int rankSize) throws Exception
	{
		ReadThread retval = null;
		// Transaction.txListLock.readLock().lock();
		{
			try
			{
				while (true)
				{
					// if (wait)
					// {
					// LockSupport.parkNanos(500);
					// }

					lock.lock();
					try
					{
						int index = findExistingPage(b);
						if (index == -1)
						{
							// if
							// (!Transaction.txListLock.tryLock())
							// {
							// wait = true;
							// continue;
							// }

							index = chooseUnpinnedPage(b.fileName());

							if (index == -1)
							{
								HRDBMSWorker.logger.error("Buffer pool exhausted.");
								lock.unlock();
								throw new BufferPoolExhaustedException();
							}

							if (numNotTouched > 0)
							{
								numNotTouched--;
							}

							if (bp[index].block() != null)
							{
								bp[index].setPinTime(-1);
								pageLookup.remove(bp[index].block());
							}
							try
							{
								retval = bp[index].assignToBlock(b, log, rank, rankSize);
							}
							catch (final Exception e)
							{
								// Transaction.txListLock.unlock();
								lock.unlock();
								throw e;
							}

							pageLookup.put(b, index);
							final long lsn = LogManager.getLSN();
							bp[index].pin(lsn, txnum);
							myBuffers.multiPut(txnum, bp[index]);
							// Transaction.txListLock.unlock();
							lock.unlock();
							return retval;
						}
						else
						{
							// if (!Transaction.txListLock.readLock().tryLock())
							// {
							// wait = true;
							// continue;
							// }

							final long lsn = LogManager.getLSN();
							bp[index].pin(lsn, txnum);
							myBuffers.multiPut(txnum, bp[index]);
							// Transaction.txListLock.readLock().unlock();
							lock.unlock();
							return retval;
						}

					}
					catch (final Exception e)
					{
						HRDBMSWorker.logger.debug("", e);
						lock.unlock();
						throw e;
					}
				}
			}
			catch (final Exception e)
			{
				// Transaction.txListLock.readLock().unlock();
				throw e;
			}
		}
	}

	public ReadThread pin(final Block b, final long txnum, final int num, final int rank, final int rankSize) throws Exception
	{
		// int start = b.number();
		// int end = start + num - 1;
		// String fn2 = b.fileName();
		// HRDBMSWorker.logger.debug("Pinning " + fn2 + ":" + start + "-" +
		// end);
		ReadThread retval = null;
		{
			try
			{
				while (true)
				{
					// if (wait)
					// {
					// LockSupport.parkNanos(500);
					// }

					lock.lock();
					try
					{
						int i = 0;
						final String fn = b.fileName();
						int page = b.number();
						boolean ok = true;
						while (i < num)
						{
							final Block block = new Block(fn, page++);
							final int index = findExistingPage(block);
							if (index != -1)
							{
								ok = false;
								break;
							}
							// else
							// {
							// HRDBMSWorker.logger.debug("Couldn't find " +
							// block + " in " + pageLookup); //DEBUG
							// }

							i++;
						}

						if (!ok)
						{
							retval = bail(b, num, txnum);
							lock.unlock();
							return retval;
						}

						{
							// if
							// (!Transaction.txListLock.tryLock())
							// {
							// wait = true;
							// continue;
							// }

							final List<Integer> indexes = chooseUnpinnedPages(num);

							if (numNotTouched > 0)
							{
								numNotTouched -= num;
							}

							if (numNotTouched < 0)
							{
								numNotTouched = 0;
							}

							for (final int index : indexes)
							{
								if (bp[index].block() != null)
								{
									bp[index].setPinTime(-1);
									pageLookup.remove(bp[index].block());
								}
							}

							try
							{
								retval = bp[indexes.get(0)].assignToBlocks(b, num, log, indexes, bp, rank, rankSize);
							}
							catch (final Exception e)
							{
								// Transaction.txListLock.unlock();
								throw e;
							}

							i = 0;
							page = b.number();
							while (i < num)
							{
								final Block nb = new Block(fn, page++);
								// HRDBMSWorker.logger.debug(nb + " in slot " +
								// indexes.get(i));
								pageLookup.put(nb, indexes.get(i));
								i++;
							}

							i = 0;
							while (i < num)
							{
								final long lsn = LogManager.getLSN();
								bp[indexes.get(i)].pin(lsn, txnum);
								i++;
							}

							i = 0;
							while (i < num)
							{
								myBuffers.multiPut(txnum, bp[indexes.get(i)]);
								i++;
							}

							// Transaction.txListLock.unlock();
							lock.unlock();
							// HRDBMSWorker.logger.debug("Short SBM pin3
							// ending");
							return retval;
						}
					}
					catch (final Throwable e)
					{
						HRDBMSWorker.logger.debug("", e);
						lock.unlock();
						throw e;
					}
				}
			}
			catch (final Exception e)
			{
				// Transaction.txListLock.readLock().unlock();
				HRDBMSWorker.logger.debug("", e);
				throw e;
			}
		}
	}

	public void pin(final Block b, final Transaction tx, final Schema schema, final ConcurrentHashMap<Integer, Schema> schemaMap, final List<Integer> fetchPos) throws Exception
	{
		// Transaction.txListLock.readLock().lock();
		final long txnum = tx.number();
		{
			try
			{
				while (true)
				{
					// if (wait)
					// {
					// LockSupport.parkNanos(500);
					// }

					lock.lock();
					try
					{
						int index = findExistingPage(b);
						if (index == -1)
						{
							// if
							// (!Transaction.txListLock.tryLock())
							// {
							// wait = true;
							// continue;
							// }

							index = chooseUnpinnedPage(b.fileName());

							if (index == -1)
							{
								HRDBMSWorker.logger.error("Buffer pool exhausted.");
								lock.unlock();
								throw new BufferPoolExhaustedException();
							}

							if (numNotTouched > 0)
							{
								numNotTouched--;
							}

							if (bp[index].block() != null)
							{
								bp[index].setPinTime(-1);
								pageLookup.remove(bp[index].block());
							}
							try
							{
								bp[index].assignToBlock(b, log, schema, schemaMap, tx, fetchPos);
							}
							catch (final Exception e)
							{
								// Transaction.txListLock.unlock();
								lock.unlock();
								throw e;
							}
							pageLookup.put(b, index);
							final long lsn = LogManager.getLSN();
							bp[index].pin(lsn, txnum);
							myBuffers.multiPut(txnum, bp[index]);
							// Transaction.txListLock.unlock();
							lock.unlock();
							return;
						}
						else
						{
							// if (!Transaction.txListLock.readLock().tryLock())
							// {
							// wait = true;
							// continue;
							// }

							final long lsn = LogManager.getLSN();
							bp[index].pin(lsn, txnum);
							myBuffers.multiPut(txnum, bp[index]);
							new ParseThread(bp[index], schema, tx, schemaMap, fetchPos).start();
							// Transaction.txListLock.readLock().unlock();
							lock.unlock();
							return;
						}
					}
					catch (final Exception e)
					{
						HRDBMSWorker.logger.debug("", e);
						lock.unlock();
						throw e;
					}
				}
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.debug("", e);
				// Transaction.txListLock.readLock().unlock();
				throw e;
			}
		}
	}

	public void pin(final Block b, final Transaction tx, final Schema schema, final ConcurrentHashMap<Integer, Schema> schemaMap, final List<Integer> fetchPos, final int rank, final int rankSize) throws Exception
	{
		// Transaction.txListLock.readLock().lock();
		final long txnum = tx.number();
		{
			try
			{
				while (true)
				{
					// if (wait)
					// {
					// LockSupport.parkNanos(500);
					// }

					lock.lock();
					try
					{
						int index = findExistingPage(b);
						if (index == -1)
						{
							// if
							// (!Transaction.txListLock.tryLock())
							// {
							// wait = true;
							// continue;
							// }

							index = chooseUnpinnedPage(b.fileName());

							if (index == -1)
							{
								HRDBMSWorker.logger.error("Buffer pool exhausted.");
								lock.unlock();
								throw new BufferPoolExhaustedException();
							}

							if (numNotTouched > 0)
							{
								numNotTouched--;
							}

							if (bp[index].block() != null)
							{
								bp[index].setPinTime(-1);
								pageLookup.remove(bp[index].block());
							}
							try
							{
								bp[index].assignToBlock(b, log, schema, schemaMap, tx, fetchPos, rank, rankSize);
							}
							catch (final Exception e)
							{
								// Transaction.txListLock.unlock();
								lock.unlock();
								throw e;
							}
							pageLookup.put(b, index);
							final long lsn = LogManager.getLSN();
							bp[index].pin(lsn, txnum);
							myBuffers.multiPut(txnum, bp[index]);
							// Transaction.txListLock.unlock();
							lock.unlock();
							return;
						}
						else
						{
							// if (!Transaction.txListLock.readLock().tryLock())
							// {
							// wait = true;
							// continue;
							// }

							final long lsn = LogManager.getLSN();
							bp[index].pin(lsn, txnum);
							myBuffers.multiPut(txnum, bp[index]);
							new ParseThread(bp[index], schema, tx, schemaMap, fetchPos).start();
							// Transaction.txListLock.readLock().unlock();
							lock.unlock();
							return;
						}
					}
					catch (final Exception e)
					{
						HRDBMSWorker.logger.debug("", e);
						lock.unlock();
						throw e;
					}
				}
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.debug("", e);
				// Transaction.txListLock.readLock().unlock();
				throw e;
			}
		}
	}

	public Read3Thread pin3(final Block b, final long txnum) throws Exception
	{
		// Transaction.txListLock.readLock().lock();
		// HRDBMSWorker.logger.debug("Short SBM pin3 starting");
		Read3Thread retval = null;
		{
			try
			{
				while (true)
				{
					// if (wait)
					// {
					// LockSupport.parkNanos(500);
					// }

					lock.lock();
					try
					{
						final Block b2 = new Block(b.fileName(), b.number() + 1);
						final Block b3 = new Block(b.fileName(), b.number() + 2);
						int index = findExistingPage(b);
						int index2 = findExistingPage(b2);
						int index3 = findExistingPage(b3);

						if (index != -1 || index2 != -1 || index3 != -1)
						{
							final List<ReadThread> threads = new ArrayList<ReadThread>();
							ReadThread thread = pin(b, txnum);
							if (thread != null)
							{
								threads.add(thread);
							}
							thread = pin(b2, txnum);
							if (thread != null)
							{
								threads.add(thread);
							}
							thread = pin(b3, txnum);
							if (thread != null)
							{
								threads.add(thread);
							}
							lock.unlock();

							if (threads.size() == 0)
							{
								return null;
							}
							else
							{
								final Read3Thread thread2 = new Read3Thread(threads);
								thread2.start();
								return thread2;
							}
						}

						{
							// if
							// (!Transaction.txListLock.tryLock())
							// {
							// wait = true;
							// continue;
							// }

							final List<Integer> indexes = chooseUnpinnedPages(b.fileName());

							if (indexes == null)
							{
								HRDBMSWorker.logger.error("Buffer pool exhausted.");
								lock.unlock();
								throw new BufferPoolExhaustedException();
							}

							if (numNotTouched > 0)
							{
								numNotTouched -= 3;
							}

							if (numNotTouched < 0)
							{
								numNotTouched = 0;
							}

							index = indexes.get(0);
							index2 = indexes.get(1);
							index3 = indexes.get(2);

							if (bp[index].block() != null)
							{
								bp[index].setPinTime(-1);
								pageLookup.remove(bp[index].block());
							}
							if (bp[index2].block() != null)
							{
								bp[index2].setPinTime(-1);
								pageLookup.remove(bp[index2].block());
							}
							if (bp[index3].block() != null)
							{
								bp[index3].setPinTime(-1);
								pageLookup.remove(bp[index3].block());
							}
							try
							{
								retval = bp[index].assignToBlock3(b, b2, b3, log, bp[index2], bp[index3]);
							}
							catch (final Exception e)
							{
								// Transaction.txListLock.unlock();
								lock.unlock();
								throw e;
							}

							pageLookup.put(b, index);
							pageLookup.put(b2, index2);
							pageLookup.put(b3, index3);
							long lsn = LogManager.getLSN();
							bp[index].pin(lsn, txnum);
							lsn = LogManager.getLSN();
							bp[index2].pin(lsn, txnum);
							lsn = LogManager.getLSN();
							bp[index3].pin(lsn, txnum);
							myBuffers.multiPut(txnum, bp[index]);
							myBuffers.multiPut(txnum, bp[index2]);
							myBuffers.multiPut(txnum, bp[index3]);
							// Transaction.txListLock.unlock();
							lock.unlock();
							// HRDBMSWorker.logger.debug("Short SBM pin3
							// ending");
							return retval;
						}
					}
					catch (final Exception e)
					{
						HRDBMSWorker.logger.debug("", e);
						lock.unlock();
						throw e;
					}
				}
			}
			catch (final Exception e)
			{
				// Transaction.txListLock.readLock().unlock();
				HRDBMSWorker.logger.debug("", e);
				throw e;
			}
		}
	}

	public Read3Thread pin3(final Block b, final long txnum, final List<Integer> cols, final int layoutSize) throws Exception
	{
		// Transaction.txListLock.readLock().lock();
		// HRDBMSWorker.logger.debug("Short SBM pin3 starting");
		Read3Thread retval = null;
		{
			try
			{
				while (true)
				{
					// if (wait)
					// {
					// LockSupport.parkNanos(500);
					// }

					lock.lock();
					try
					{
						final Block b2 = new Block(b.fileName(), b.number() + 1);
						final Block b3 = new Block(b.fileName(), b.number() + 2);
						int index = findExistingPage(b);
						int index2 = findExistingPage(b2);
						int index3 = findExistingPage(b3);

						if (index != -1 || index2 != -1 || index3 != -1)
						{
							final List<ReadThread> threads = new ArrayList<ReadThread>();
							ReadThread thread = pin(b, txnum, cols, layoutSize);
							if (thread != null)
							{
								threads.add(thread);
							}
							thread = pin(b2, txnum, cols, layoutSize);
							if (thread != null)
							{
								threads.add(thread);
							}
							thread = pin(b3, txnum, cols, layoutSize);
							if (thread != null)
							{
								threads.add(thread);
							}
							lock.unlock();

							if (threads.size() == 0)
							{
								return null;
							}
							else
							{
								final Read3Thread thread2 = new Read3Thread(threads);
								thread2.start();
								return thread2;
							}
						}

						{
							// if
							// (!Transaction.txListLock.tryLock())
							// {
							// wait = true;
							// continue;
							// }

							final List<Integer> indexes = chooseUnpinnedPages(b.fileName());

							if (indexes == null)
							{
								HRDBMSWorker.logger.error("Buffer pool exhausted.");
								lock.unlock();
								throw new BufferPoolExhaustedException();
							}

							if (numNotTouched > 0)
							{
								numNotTouched -= 3;
							}

							if (numNotTouched < 0)
							{
								numNotTouched = 0;
							}

							index = indexes.get(0);
							index2 = indexes.get(1);
							index3 = indexes.get(2);

							if (bp[index].block() != null)
							{
								bp[index].setPinTime(-1);
								pageLookup.remove(bp[index].block());
							}
							if (bp[index2].block() != null)
							{
								bp[index2].setPinTime(-1);
								pageLookup.remove(bp[index2].block());
							}
							if (bp[index3].block() != null)
							{
								bp[index3].setPinTime(-1);
								pageLookup.remove(bp[index3].block());
							}
							try
							{
								retval = bp[index].assignToBlock3(b, b2, b3, log, bp[index2], bp[index3], cols, layoutSize);
							}
							catch (final Exception e)
							{
								// Transaction.txListLock.unlock();
								lock.unlock();
								throw e;
							}

							pageLookup.put(b, index);
							pageLookup.put(b2, index2);
							pageLookup.put(b3, index3);
							long lsn = LogManager.getLSN();
							bp[index].pin(lsn, txnum);
							lsn = LogManager.getLSN();
							bp[index2].pin(lsn, txnum);
							lsn = LogManager.getLSN();
							bp[index3].pin(lsn, txnum);
							myBuffers.multiPut(txnum, bp[index]);
							myBuffers.multiPut(txnum, bp[index2]);
							myBuffers.multiPut(txnum, bp[index3]);
							// Transaction.txListLock.unlock();
							lock.unlock();
							// HRDBMSWorker.logger.debug("Short SBM pin3
							// ending");
							return retval;
						}
					}
					catch (final Exception e)
					{
						HRDBMSWorker.logger.debug("", e);
						lock.unlock();
						throw e;
					}
				}
			}
			catch (final Exception e)
			{
				// Transaction.txListLock.readLock().unlock();
				HRDBMSWorker.logger.debug("", e);
				throw e;
			}
		}
	}

	public Read3Thread pin3(final Block b, final long txnum, final int rank, final int rankSize) throws Exception
	{
		// Transaction.txListLock.readLock().lock();
		// HRDBMSWorker.logger.debug("Short SBM pin3 starting");
		Read3Thread retval = null;
		{
			try
			{
				while (true)
				{
					// if (wait)
					// {
					// LockSupport.parkNanos(500);
					// }

					lock.lock();
					try
					{
						final Block b2 = new Block(b.fileName(), b.number() + 1);
						final Block b3 = new Block(b.fileName(), b.number() + 2);
						int index = findExistingPage(b);
						int index2 = findExistingPage(b2);
						int index3 = findExistingPage(b3);

						if (index != -1 || index2 != -1 || index3 != -1)
						{
							final List<ReadThread> threads = new ArrayList<ReadThread>();
							ReadThread thread = pin(b, txnum, rank, rankSize);
							if (thread != null)
							{
								threads.add(thread);
							}
							thread = pin(b2, txnum, rank, rankSize);
							if (thread != null)
							{
								threads.add(thread);
							}
							thread = pin(b3, txnum, rank, rankSize);
							if (thread != null)
							{
								threads.add(thread);
							}
							lock.unlock();

							if (threads.size() == 0)
							{
								return null;
							}
							else
							{
								final Read3Thread thread2 = new Read3Thread(threads);
								thread2.start();
								return thread2;
							}
						}

						{
							// if
							// (!Transaction.txListLock.tryLock())
							// {
							// wait = true;
							// continue;
							// }

							final List<Integer> indexes = chooseUnpinnedPages(b.fileName());

							if (indexes == null)
							{
								HRDBMSWorker.logger.error("Buffer pool exhausted.");
								lock.unlock();
								throw new BufferPoolExhaustedException();
							}

							if (numNotTouched > 0)
							{
								numNotTouched -= 3;
							}

							if (numNotTouched < 0)
							{
								numNotTouched = 0;
							}

							index = indexes.get(0);
							index2 = indexes.get(1);
							index3 = indexes.get(2);

							if (bp[index].block() != null)
							{
								bp[index].setPinTime(-1);
								pageLookup.remove(bp[index].block());
							}
							if (bp[index2].block() != null)
							{
								bp[index2].setPinTime(-1);
								pageLookup.remove(bp[index2].block());
							}
							if (bp[index3].block() != null)
							{
								bp[index3].setPinTime(-1);
								pageLookup.remove(bp[index3].block());
							}
							try
							{
								retval = bp[index].assignToBlock3(b, b2, b3, log, bp[index2], bp[index3], rank, rankSize);
							}
							catch (final Exception e)
							{
								// Transaction.txListLock.unlock();
								lock.unlock();
								throw e;
							}

							pageLookup.put(b, index);
							pageLookup.put(b2, index2);
							pageLookup.put(b3, index3);
							long lsn = LogManager.getLSN();
							bp[index].pin(lsn, txnum);
							lsn = LogManager.getLSN();
							bp[index2].pin(lsn, txnum);
							lsn = LogManager.getLSN();
							bp[index3].pin(lsn, txnum);
							myBuffers.multiPut(txnum, bp[index]);
							myBuffers.multiPut(txnum, bp[index2]);
							myBuffers.multiPut(txnum, bp[index3]);
							// Transaction.txListLock.unlock();
							lock.unlock();
							// HRDBMSWorker.logger.debug("Short SBM pin3
							// ending");
							return retval;
						}
					}
					catch (final Exception e)
					{
						HRDBMSWorker.logger.debug("", e);
						lock.unlock();
						throw e;
					}
				}
			}
			catch (final Exception e)
			{
				// Transaction.txListLock.readLock().unlock();
				HRDBMSWorker.logger.debug("", e);
				throw e;
			}
		}
	}

	public void pin3(final Block b, final Transaction tx, final Schema schema1, final Schema schema2, final Schema schema3, final ConcurrentHashMap<Integer, Schema> schemaMap, final List<Integer> fetchPos) throws Exception
	{
		// Transaction.txListLock.readLock().lock();
		// HRDBMSWorker.logger.debug("Long SBM pin3 starting");
		final long txnum = tx.number();
		{
			try
			{
				while (true)
				{
					// if (wait)
					// {
					// LockSupport.parkNanos(500);
					// }

					lock.lock();
					try
					{
						int index = findExistingPage(b);
						final Block b2 = new Block(b.fileName(), b.number() + 1);
						final Block b3 = new Block(b.fileName(), b.number() + 2);
						int index2 = findExistingPage(b2);
						int index3 = findExistingPage(b3);

						if (index != -1 || index2 != -1 || index3 != -1)
						{
							pin(b, tx, schema1, schemaMap, fetchPos);
							pin(b2, tx, schema2, schemaMap, fetchPos);
							pin(b3, tx, schema3, schemaMap, fetchPos);
							lock.unlock();
							return;
						}

						{
							// if
							// (!Transaction.txListLock.tryLock())
							// {
							// wait = true;
							// continue;
							// }

							final List<Integer> indexes = chooseUnpinnedPages(b.fileName());

							if (indexes == null)
							{
								HRDBMSWorker.logger.error("Buffer pool exhausted.");
								lock.unlock();
								throw new BufferPoolExhaustedException();
							}

							if (numNotTouched > 0)
							{
								numNotTouched -= 3;
							}

							if (numNotTouched < 0)
							{
								numNotTouched = 0;
							}

							index = indexes.get(0);
							index2 = indexes.get(1);
							index3 = indexes.get(2);

							if (bp[index].block() != null)
							{
								bp[index].setPinTime(-1);
								pageLookup.remove(bp[index].block());
							}
							if (bp[index2].block() != null)
							{
								bp[index2].setPinTime(-1);
								pageLookup.remove(bp[index2].block());
							}
							if (bp[index3].block() != null)
							{
								bp[index3].setPinTime(-1);
								pageLookup.remove(bp[index3].block());
							}
							try
							{
								bp[index].assignToBlock3(b, b2, b3, log, schema1, schema2, schema3, schemaMap, tx, fetchPos, bp[index2], bp[index3]);
							}
							catch (final Exception e)
							{
								// Transaction.txListLock.unlock();
								lock.unlock();
								throw e;
							}
							pageLookup.put(b, index);
							pageLookup.put(b2, index2);
							pageLookup.put(b3, index3);
							long lsn = LogManager.getLSN();
							bp[index].pin(lsn, txnum);
							lsn = LogManager.getLSN();
							bp[index2].pin(lsn, txnum);
							lsn = LogManager.getLSN();
							bp[index3].pin(lsn, txnum);
							myBuffers.multiPut(txnum, bp[index]);
							myBuffers.multiPut(txnum, bp[index2]);
							myBuffers.multiPut(txnum, bp[index3]);
							// Transaction.txListLock.unlock();
							lock.unlock();
							// HRDBMSWorker.logger.debug("Long SBM pin3
							// ending");
							return;
						}
					}
					catch (final Exception e)
					{
						HRDBMSWorker.logger.debug("", e);
						lock.unlock();
						throw e;
					}
				}
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.debug("", e);
				// Transaction.txListLock.readLock().unlock();
				throw e;
			}
		}
	}

	public void pin3(final Block b, final Transaction tx, final Schema schema1, final Schema schema2, final Schema schema3, final ConcurrentHashMap<Integer, Schema> schemaMap, final List<Integer> fetchPos, final int rank, final int rankSize) throws Exception
	{
		// Transaction.txListLock.readLock().lock();
		// HRDBMSWorker.logger.debug("Long SBM pin3 starting");
		final long txnum = tx.number();
		{
			try
			{
				while (true)
				{
					// if (wait)
					// {
					// LockSupport.parkNanos(500);
					// }

					lock.lock();
					try
					{
						int index = findExistingPage(b);
						final Block b2 = new Block(b.fileName(), b.number() + 1);
						final Block b3 = new Block(b.fileName(), b.number() + 2);
						int index2 = findExistingPage(b2);
						int index3 = findExistingPage(b3);

						if (index != -1 || index2 != -1 || index3 != -1)
						{
							pin(b, tx, schema1, schemaMap, fetchPos, rank, rankSize);
							pin(b2, tx, schema2, schemaMap, fetchPos, rank, rankSize);
							pin(b3, tx, schema3, schemaMap, fetchPos, rank, rankSize);
							lock.unlock();
							return;
						}

						{
							// if
							// (!Transaction.txListLock.tryLock())
							// {
							// wait = true;
							// continue;
							// }

							final List<Integer> indexes = chooseUnpinnedPages(b.fileName());

							if (indexes == null)
							{
								HRDBMSWorker.logger.error("Buffer pool exhausted.");
								lock.unlock();
								throw new BufferPoolExhaustedException();
							}

							if (numNotTouched > 0)
							{
								numNotTouched -= 3;
							}

							if (numNotTouched < 0)
							{
								numNotTouched = 0;
							}

							index = indexes.get(0);
							index2 = indexes.get(1);
							index3 = indexes.get(2);

							if (bp[index].block() != null)
							{
								bp[index].setPinTime(-1);
								pageLookup.remove(bp[index].block());
							}
							if (bp[index2].block() != null)
							{
								bp[index2].setPinTime(-1);
								pageLookup.remove(bp[index2].block());
							}
							if (bp[index3].block() != null)
							{
								bp[index3].setPinTime(-1);
								pageLookup.remove(bp[index3].block());
							}
							try
							{
								bp[index].assignToBlock3(b, b2, b3, log, schema1, schema2, schema3, schemaMap, tx, fetchPos, bp[index2], bp[index3], rank, rankSize);
							}
							catch (final Exception e)
							{
								// Transaction.txListLock.unlock();
								lock.unlock();
								throw e;
							}
							pageLookup.put(b, index);
							pageLookup.put(b2, index2);
							pageLookup.put(b3, index3);
							long lsn = LogManager.getLSN();
							bp[index].pin(lsn, txnum);
							lsn = LogManager.getLSN();
							bp[index2].pin(lsn, txnum);
							lsn = LogManager.getLSN();
							bp[index3].pin(lsn, txnum);
							myBuffers.multiPut(txnum, bp[index]);
							myBuffers.multiPut(txnum, bp[index2]);
							myBuffers.multiPut(txnum, bp[index3]);
							// Transaction.txListLock.unlock();
							lock.unlock();
							// HRDBMSWorker.logger.debug("Long SBM pin3
							// ending");
							return;
						}
					}
					catch (final Exception e)
					{
						HRDBMSWorker.logger.debug("", e);
						lock.unlock();
						throw e;
					}
				}
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.debug("", e);
				// Transaction.txListLock.readLock().unlock();
				throw e;
			}
		}
	}

	public void pinFromMemory(final Block b, final long txnum, final ByteBuffer data) throws Exception
	{
		// Transaction.txListLock.readLock().lock();
		{
			try
			{
				while (true)
				{
					// if (wait)
					// {
					// LockSupport.parkNanos(500);
					// }

					lock.lock();
					try
					{
						int index;

						// if (!Transaction.txListLock.tryLock())
						// {
						// wait = true;
						// continue;
						// }

						index = chooseUnpinnedPage(b.fileName());

						if (index == -1)
						{
							HRDBMSWorker.logger.error("Buffer pool exhausted.");
							lock.unlock();
							throw new BufferPoolExhaustedException();
						}

						if (numNotTouched > 0)
						{
							numNotTouched--;
						}

						if (bp[index].block() != null)
						{
							bp[index].setPinTime(-1);
							pageLookup.remove(bp[index].block());
						}
						try
						{
							bp[index].assignToBlockFromMemory(b, log, data);
						}
						catch (final Exception e)
						{
							// Transaction.txListLock.unlock();
							lock.unlock();
							throw e;
						}

						pageLookup.put(b, index);
						// HRDBMSWorker.logger.debug("Page lookup has " +
						// pageLookup); //DEBUG
						final long lsn = LogManager.getLSN();
						bp[index].pin(lsn, txnum);
						myBuffers.multiPut(txnum, bp[index]);
						// Transaction.txListLock.unlock();
						lock.unlock();
						return;
					}
					catch (final Throwable e)
					{
						HRDBMSWorker.logger.debug("", e);
						lock.unlock();
						throw e;
					}
				}
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.debug("", e);
				// Transaction.txListLock.readLock().unlock();
				throw e;
			}
		}
	}

	public void pinSync(final Block b, final long txnum) throws Exception
	{
		// Transaction.txListLock.readLock().lock();
		{
			try
			{
				while (true)
				{
					// if (wait)
					// {
					// LockSupport.parkNanos(500);
					// }

					lock.lock();
					try
					{
						int index = findExistingPage(b);
						if (index == -1)
						{
							// if
							// (!Transaction.txListLock.tryLock())
							// {
							// wait = true;
							// continue;
							// }

							index = chooseUnpinnedPage(b.fileName());

							if (index == -1)
							{
								HRDBMSWorker.logger.error("Buffer pool exhausted.");
								lock.unlock();
								throw new BufferPoolExhaustedException();
							}

							if (numNotTouched > 0)
							{
								numNotTouched--;
							}

							if (bp[index].block() != null)
							{
								bp[index].setPinTime(-1);
								pageLookup.remove(bp[index].block());
							}

							bp[index].preAssignToBlock(b, log);
							bp[index].pins.clear();
							pageLookup.put(b, index);
							final long lsn = LogManager.getLSN();
							bp[index].pin(lsn, txnum);
							myBuffers.multiPut(txnum, bp[index]);
							lock.unlock();
							// Transaction.txListLock.unlock();
							try
							{
								bp[index].assignToBlockSync(b, log);
							}
							catch (final Exception e)
							{
								// Transaction.txListLock.unlock();
								lock.unlock();
								throw e;
							}
							return;
						}
						else
						{
							// if (!Transaction.txListLock.readLock().tryLock())
							// {
							// wait = true;
							// continue;
							// }

							final long lsn = LogManager.getLSN();
							bp[index].pin(lsn, txnum);
							myBuffers.multiPut(txnum, bp[index]);
							// Transaction.txListLock.readLock().unlock();
							lock.unlock();
							return;
						}

					}
					catch (final Exception e)
					{
						HRDBMSWorker.logger.debug("", e);
						lock.unlock();
						throw e;
					}
				}
			}
			catch (final Exception e)
			{
				// Transaction.txListLock.readLock().unlock();
				throw e;
			}
		}
	}

	public void pinSync(final Block b, final Transaction tx, final Schema schema, final ConcurrentHashMap<Integer, Schema> schemaMap, final List<Integer> fetchPos) throws Exception
	{
		// Transaction.txListLock.readLock().lock();
		final long txnum = tx.number();
		{
			try
			{
				while (true)
				{
					// if (wait)
					// {
					// LockSupport.parkNanos(500);
					// }

					lock.lock();
					try
					{
						int index = findExistingPage(b);
						if (index == -1)
						{
							// if
							// (!Transaction.txListLock.tryLock())
							// {
							// wait = true;
							// continue;
							// }

							index = chooseUnpinnedPage(b.fileName());

							if (index == -1)
							{
								HRDBMSWorker.logger.error("Buffer pool exhausted.");
								lock.unlock();
								throw new BufferPoolExhaustedException();
							}

							if (numNotTouched > 0)
							{
								numNotTouched--;
							}

							if (bp[index].block() != null)
							{
								bp[index].setPinTime(-1);
								pageLookup.remove(bp[index].block());
							}

							bp[index].preAssignToBlock(b, log);
							bp[index].pins.clear();
							pageLookup.put(b, index);
							final long lsn = LogManager.getLSN();
							bp[index].pin(lsn, txnum);
							myBuffers.multiPut(txnum, bp[index]);
							// Transaction.txListLock.unlock();
							lock.unlock();
							try
							{
								bp[index].assignToBlockSync(b, log, schema, schemaMap, tx, fetchPos);
							}
							catch (final Exception e)
							{
								// Transaction.txListLock.unlock();
								lock.unlock();
								throw e;
							}
							return;
						}
						else
						{
							// if (!Transaction.txListLock.readLock().tryLock())
							// {
							// wait = true;
							// continue;
							// }

							final long lsn = LogManager.getLSN();
							bp[index].pin(lsn, txnum);
							myBuffers.multiPut(txnum, bp[index]);
							new ParseThread(bp[index], schema, tx, schemaMap, fetchPos).start();
							// Transaction.txListLock.readLock().unlock();
							lock.unlock();
							return;
						}
					}
					catch (final Exception e)
					{
						HRDBMSWorker.logger.debug("", e);
						lock.unlock();
						throw e;
					}
				}
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.debug("", e);
				// Transaction.txListLock.readLock().unlock();
				throw e;
			}
		}
	}

	public ReadThread requestConsecutivePages(final Block b, final long txnum, final int num, final int rank, final int rankSize) throws Exception
	{
		try
		{
			return pin(b, txnum, num, rank, rankSize);
		}
		catch (final Throwable e)
		{
			HRDBMSWorker.logger.warn("Error fetching pages", e);
			throw e;
		}
	}

	public void requestPage(final Block b, final long txnum)
	{
		try
		{
			pin(b, txnum);
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.warn("Error fetching pages", e);
		}
	}

	public void throwAwayPage(final Block b) throws Exception
	{
		lock.lock();
		try
		{
			final Page p = bp[pageLookup.get(b)];
			p.setNotModified();
			p.setPinTime(-1);
			pageLookup.remove(p.block());
			lock.unlock();
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.debug("", e);
			lock.unlock();
			throw e;
		}
	}

	public void trimBP()
	{
		lock.lock();
		try
		{
			final int desired = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("bp_pages")) / BufferManager.mLength;
			int i = bp.length - 1;
			final int starting = bp.length;
			int toTrim = 0;

			while (i >= 0 && starting - toTrim > desired && !bp[i].isModified() && !bp[i].isPinned())
			{
				pageLookup.remove(bp[i].block());
				toTrim++;
				i--;
			}

			if (toTrim > 0)
			{
				final int newLength = starting - toTrim;

				final Page[] bp2 = new Page[newLength];
				System.arraycopy(bp, 0, bp2, 0, newLength);
				bp = bp2;
				// HRDBMSWorker.logger.debug("Trimmed " + toTrim + " pages off
				// of an SBP");
			}
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.debug("", e);
		}

		lock.unlock();
	}

	public void unpin(final Page p, final long txnum)
	{
		lock.lock();
		try
		{
			if (p.unpin1(txnum))
			{
				myBuffers.multiRemove(txnum, p);
			}
			lock.unlock();
		}
		catch (final Throwable e)
		{
			HRDBMSWorker.logger.debug("", e);
			lock.unlock();
			throw e;
		}
	}

	public void unpinAll()
	{
		lock.lock();
		try
		{
			for (final long tx : myBuffers.getKeySet())
			{

				for (final Page p : myBuffers.get(tx))
				{
					p.unpin(tx);
				}

				myBuffers.remove(tx);
			}

			lock.unlock();
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.debug("", e);
			lock.unlock();
			throw e;
		}
	}

	public void unpinAll(final long txnum)
	{
		lock.lock();
		try
		{
			for (final Page p : myBuffers.get(txnum))
			{
				p.unpin(txnum);
			}

			myBuffers.remove(txnum);
			lock.unlock();
		}
		catch (final Throwable e)
		{
			HRDBMSWorker.logger.debug("", e);
			lock.unlock();
			throw e;
		}
	}

	public void unpinAllExcept(final long txnum, final String iFn, final Block inUse)
	{
		lock.lock();
		try
		{
			for (final Page p : myBuffers.get(txnum))
			{
				if (p.block().fileName().equals(iFn) && p.block().number() != inUse.number())
				{
					p.unpin(txnum);
					myBuffers.multiRemove(txnum, p);
				}
			}

			lock.unlock();
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.debug("", e);
			lock.unlock();
			throw e;
		}
	}

	public void unpinAllLessThan(final long txnum, final int lt, final String tFn)
	{
		lock.lock();
		try
		{
			final String prefix = tFn.substring(0, tFn.lastIndexOf('/') + 1);
			for (final Page p : myBuffers.get(txnum))
			{
				if (p.block().fileName().startsWith(prefix) && (p.block().fileName().endsWith("indx") || p.block().number() < lt - 1))
				{
					p.unpin(txnum);
					myBuffers.multiRemove(txnum, p);
				}
			}

			lock.unlock();
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.debug("", e);
			lock.unlock();
			throw e;
		}
	}

	public void unpinAllLessThan(final long txnum, final int lt, final String tFn, final boolean flag)
	{
		lock.lock();
		try
		{
			for (final Page p : myBuffers.get(txnum))
			{
				if (p.block().fileName().equals(tFn) && p.block().number() < lt)
				{
					p.unpin(txnum);
					myBuffers.multiRemove(txnum, p);
				}
			}

			lock.unlock();
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.debug("", e);
			lock.unlock();
			throw e;
		}
	}

	public void unpinMyDevice(final long txnum, final String prefix)
	{
		lock.lock();
		try
		{
			for (final Page p : myBuffers.get(txnum))
			{
				if (p.block().fileName().startsWith(prefix))
				{
					p.unpin(txnum);
					myBuffers.multiRemove(txnum, p);
				}
			}

			lock.unlock();
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.debug("", e);
			lock.unlock();
			throw e;
		}
	}

	public void write(final Page p, final int off, final byte[] data)
	{
		p.buffer().position(off);
		p.buffer().put(data);
	}

	private ReadThread bail(final Block b, final int num, final long txnum) throws Exception
	{
		int page = b.number();
		int i = 0;
		final String fn = b.fileName();
		final List<ReadThread> threads = new ArrayList<ReadThread>();
		while (i < num)
		{
			final ReadThread thread = pin(new Block(fn, page++), txnum);
			threads.add(thread);
			i++;
		}

		final ReadThread retval = new ReadThread(threads);
		retval.start();
		return retval;
	}

	private int chooseUnpinnedPage(final String newFN)
	{
		lock.lock();
		try
		{
			if (numNotTouched > 0)
			{
				lock.unlock();
				return numAvailable - numNotTouched;
			}

			// String partial = newFN.substring(newFN.lastIndexOf('/') + 1);

			// int checked = 0;
			if (clock >= bp.length)
			{
				clock = 0;
			}

			final int initialClock = clock;
			boolean start = true;

			while (start || clock != initialClock)
			{
				start = false;
				final Page p = bp[clock];
				final int index = clock;
				clock++;
				if (clock == bp.length)
				{
					clock = 0;
				}
				if (!p.isPinned() && !BufferManager.isInterest(p.block()))
				{
					if ((!XAManager.rP1) || (!XAManager.rP2))
					{
						if (p.block() == null)
						{
							lock.unlock();
							return index;
						}
					}
					else
					{
						lock.unlock();
						return index;
					}
				}
			}

			start = true;

			while (start || clock != initialClock)
			{
				start = false;
				final Page p = bp[clock];
				final int index = clock;
				clock++;
				if (clock == bp.length)
				{
					clock = 0;
				}
				if (!p.isPinned())
				{
					if ((!XAManager.rP1) || (!XAManager.rP2))
					{
						if (p.block() == null)
						{
							lock.unlock();
							return index;
						}
					}
					else
					{
						lock.unlock();
						return index;
					}
				}
			}

			// expand bufferpool
			int newLength = (int)(bp.length * 1.1);
			if (newLength == bp.length)
			{
				newLength = bp.length + 1;
			}

			final Page[] bp2 = new Page[newLength];
			System.arraycopy(bp, 0, bp2, 0, bp.length);
			int z = bp.length;
			final int limit = bp2.length;
			while (z < limit)
			{
				bp2[z++] = new Page();
			}

			bp = bp2;
			lock.unlock();
			return chooseUnpinnedPage(newFN);
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.debug("", e);
			lock.unlock();
			throw e;
		}
	}

	private List<Integer> chooseUnpinnedPages(final int num)
	{
		final List<Integer> retval = new ArrayList<Integer>(num);
		lock.lock();
		int i = 0;
		outer: while (i < num)
		{
			try
			{
				if (numNotTouched - i > 0)
				{
					retval.add(numAvailable - (numNotTouched - i));
					i++;
					continue;
				}

				// String partial = newFN.substring(newFN.lastIndexOf('/') + 1);

				// int checked = 0;
				if (clock >= bp.length)
				{
					clock = 0;
				}

				final int initialClock = clock;
				boolean start = true;
				boolean attempt2 = false;

				while (!attempt2 || clock != initialClock)
				{
					if (clock == initialClock && !start)
					{
						attempt2 = true;
					}
					start = false;
					final Page p = bp[clock];
					final int index = clock;
					clock++;
					if (clock == bp.length)
					{
						clock = 0;
					}
					if (!p.isPinned() && !retval.contains(index) && (attempt2 || (p.block() != null && !BufferManager.isInterest(p.block()))))
					{
						if ((!XAManager.rP1) || (!XAManager.rP2))
						{
							if (p.block() == null)
							{
								retval.add(index);
								i++;
								continue outer;
							}
						}
						else
						{
							retval.add(index);
							i++;
							continue outer;
						}
					}
				}

				expandBP(i, num, retval);
				lock.unlock();
				return retval;
			}
			catch (final Throwable e)
			{
				lock.unlock();
				throw e;
			}
		}

		lock.unlock();
		return retval;
	}

	private List<Integer> chooseUnpinnedPages(final String newFN)
	{
		final List<Integer> retval = new ArrayList<Integer>(3);
		lock.lock();
		int i = 0;
		outer: while (i < 3)
		{
			try
			{
				if (numNotTouched - i > 0)
				{
					retval.add(numAvailable - (numNotTouched - i));
					i++;
					continue;
				}

				// String partial = newFN.substring(newFN.lastIndexOf('/') + 1);

				// int checked = 0;
				if (clock >= bp.length)
				{
					clock = 0;
				}

				final int initialClock = clock;
				boolean start = true;

				while (start || clock != initialClock)
				{
					start = false;
					final Page p = bp[clock];
					final int index = clock;
					clock++;
					if (clock == bp.length)
					{
						clock = 0;
					}
					if (p.block() != null && !p.isPinned() && !BufferManager.isInterest(p.block()) && !retval.contains(index))
					{
						retval.add(index);
						i++;
						continue outer;
					}
				}

				start = true;

				while (start || clock != initialClock)
				{
					start = false;
					final Page p = bp[clock];
					final int index = clock;
					clock++;
					if (clock == bp.length)
					{
						clock = 0;
					}
					if (!p.isPinned() && !retval.contains(index))
					{
						retval.add(index);
						i++;
						continue outer;
					}
				}

				// expand bufferpool
				int newLength = (int)(bp.length * 1.1);
				if (newLength < bp.length + 3)
				{
					newLength = bp.length + 3;
				}

				final Page[] bp2 = new Page[newLength];
				System.arraycopy(bp, 0, bp2, 0, bp.length);
				int z = bp.length;
				final int limit = bp2.length;
				while (z < limit)
				{
					bp2[z++] = new Page();
				}

				int j = bp.length;
				while (i < 3)
				{
					retval.add(j++);
					i++;
				}

				bp = bp2;
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.debug("", e);
				lock.unlock();
				throw e;
			}
		}

		lock.unlock();
		return retval;
	}

	private void expandBP(int i, final int num, final List<Integer> retval)
	{
		// expand bufferpool
		int newLength = (int)(bp.length * 1.1);
		if (newLength < bp.length + num)
		{
			newLength = bp.length + num;
		}

		final Page[] bp2 = new Page[newLength];
		System.arraycopy(bp, 0, bp2, 0, bp.length);
		int z = bp.length;
		final int limit = bp2.length;
		while (z < limit)
		{
			bp2[z++] = new Page();
		}

		int j = bp.length;
		while (i < num)
		{
			retval.add(j++);
			i++;
		}

		bp = bp2;
	}

	private int findExistingPage(final Block b)
	{
		lock.lock();
		try
		{
			final Integer temp = pageLookup.get(b);
			if (temp == null)
			{
				lock.unlock();
				return -1;
			}

			lock.unlock();
			return temp.intValue();
		}
		catch (final Throwable e)
		{
			HRDBMSWorker.logger.debug("", e);
			lock.unlock();
			throw e;
		}
	}

	private class ParseThread extends HRDBMSThread
	{
		private final Page p;
		private final Schema schema;
		private final Transaction tx;
		private final ConcurrentHashMap<Integer, Schema> schemaMap;
		private final List<Integer> fetchPos;

		public ParseThread(final Page p, final Schema schema, final Transaction tx, final ConcurrentHashMap<Integer, Schema> schemaMap, final List<Integer> fetchPos)
		{
			this.p = p;
			this.schema = schema;
			this.tx = tx;
			this.schemaMap = schemaMap;
			this.fetchPos = fetchPos;
		}

		@Override
		public void run()
		{
			try
			{
				tx.read2(p.block(), schema, p);
				schemaMap.put(p.block().number(), schema);
				schema.prepRowIter(fetchPos);
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.debug("", e);
			}
		}
	}

	private class PutThread extends HRDBMSThread
	{
		private final List<Page> toPut;
		private Exception e = null;

		public PutThread(final List<Page> toPut)
		{
			this.toPut = toPut;
		}

		public Exception getException()
		{
			return e;
		}

		@Override
		public void run()
		{
			for (final Page p : toPut)
			{
				try
				{
					FileManager.writeDelayed(p.block(), p.buffer());
					if (!p.isPinned())
					{
						p.setNotModified();
					}
				}
				catch (final Exception e)
				{
					this.e = e;
					return;
				}
			}
		}
	}
}
