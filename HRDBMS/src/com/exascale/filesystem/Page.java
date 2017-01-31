package com.exascale.filesystem;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import com.exascale.managers.FileManager;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.managers.LogManager;
import com.exascale.managers.ResourceManager;
import com.exascale.misc.ScalableStampedReentrantRWLock;
import com.exascale.tables.Schema;
import com.exascale.tables.Transaction;
import com.exascale.threads.Read3Thread;
import com.exascale.threads.ReadThread;

public class Page
{
	public static final int BLOCK_SIZE = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("page_size"));
	private static sun.misc.Unsafe unsafe;
	private static long offset;

	static
	{
		try
		{
			final Field f = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
			f.setAccessible(true);
			unsafe = (sun.misc.Unsafe)f.get(null);
			final Field fieldToUpdate = ByteBuffer.class.getDeclaredField("hb");
			// get unsafe offset to this field
			offset = unsafe.objectFieldOffset(fieldToUpdate);
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.debug("", e);
			unsafe = null;
		}
	}
	// extent size
	private ByteBuffer contents;
	private volatile Block blk;
	public final ConcurrentHashMap<Long, AtomicInteger> pins;
	private volatile long modifiedBy = -1;
	private volatile long timePinned = -1;
	private volatile long lsn;
	private ScalableStampedReentrantRWLock lock;
	public volatile ArrayList<RID> rowIDsAL;
	public volatile int[][] offsetArray;
	private volatile boolean readDone = false;

	public Page()
	{
		try
		{
			this.contents = ByteBuffer.allocate(BLOCK_SIZE);
			lock = new ScalableStampedReentrantRWLock();
		}
		catch (final Throwable e)
		{
			// System.out.println("Bufferpool manager failed to allocate pages
			// for the bufferpool");
			HRDBMSWorker.logger.error("Bufferpool manager failed to allocate pages for the bufferpool", e);
			System.exit(1);
		}
		pins = new ConcurrentHashMap<Long, AtomicInteger>(16, 0.75f, 6 * ResourceManager.cpus);
		readDone = false;
	}

	public ReadThread assignToBlock(final Block b, final boolean log) throws Exception
	{
		lock.writeLock().lock();
		rowIDsAL = null;
		offsetArray = null;
		if (modifiedBy >= 0)
		{
			if (b != null)
			{
				if (log)
				{
					LogManager.flush(lsn);
				}
				FileManager.write(blk, contents);
			}
			modifiedBy = -1;
		}
		blk = b;
		readDone = false;
		ReadThread retval = null;
		if (b != null)
		{
			retval = FileManager.read(this, b, contents);
		}
		pins.clear();
		lock.writeLock().unlock();
		return retval;
	}

	public ReadThread assignToBlock(final Block b, final boolean log, final ArrayList<Integer> cols, final int layoutSize) throws Exception
	{
		lock.writeLock().lock();
		rowIDsAL = null;
		offsetArray = null;
		if (modifiedBy >= 0)
		{
			if (b != null)
			{
				if (log)
				{
					LogManager.flush(lsn);
				}
				FileManager.write(blk, contents);
			}
			modifiedBy = -1;
		}
		blk = b;
		readDone = false;
		ReadThread retval = null;
		if (b != null)
		{
			retval = FileManager.read(this, b, contents, cols, layoutSize);
		}
		pins.clear();
		lock.writeLock().unlock();
		return retval;
	}

	public ReadThread assignToBlock(final Block b, final boolean log, final ArrayList<Integer> cols, final int layoutSize, final int rank, final int rankSize) throws Exception
	{
		lock.writeLock().lock();
		rowIDsAL = null;
		offsetArray = null;
		if (modifiedBy >= 0)
		{
			if (b != null)
			{
				if (log)
				{
					LogManager.flush(lsn);
				}
				FileManager.write(blk, contents);
			}
			modifiedBy = -1;
		}
		blk = b;
		readDone = false;
		ReadThread retval = null;
		if (b != null)
		{
			retval = FileManager.read(this, b, contents, cols, layoutSize, rank, rankSize);
		}
		pins.clear();
		lock.writeLock().unlock();
		return retval;
	}

	public void assignToBlock(final Block b, final boolean log, final boolean flag) throws Exception
	{
		lock.writeLock().lock();
		rowIDsAL = null;
		offsetArray = null;
		if (modifiedBy >= 0)
		{
			if (b != null)
			{
				if (log)
				{
					LogManager.flush(lsn);
				}
				FileManager.write(blk, contents);
			}
			modifiedBy = -1;
		}
		blk = b;
		readDone = false;
		if (b != null)
		{
			FileManager.read(this, b, contents, true);
		}
		pins.clear();
		lock.writeLock().unlock();
	}

	public ReadThread assignToBlock(final Block b, final boolean log, final int rank, final int rankSize) throws Exception
	{
		lock.writeLock().lock();
		rowIDsAL = null;
		offsetArray = null;
		if (modifiedBy >= 0)
		{
			if (b != null)
			{
				if (log)
				{
					LogManager.flush(lsn);
				}
				FileManager.write(blk, contents);
			}
			modifiedBy = -1;
		}
		blk = b;
		readDone = false;
		ReadThread retval = null;
		if (b != null)
		{
			retval = FileManager.read(this, b, contents, rank, rankSize);
		}
		pins.clear();
		lock.writeLock().unlock();
		return retval;
	}

	public void assignToBlock(final Block b, final boolean log, final Schema schema, final ConcurrentHashMap<Integer, Schema> schemaMap, final Transaction tx, final ArrayList<Integer> fetchPos) throws Exception
	{
		lock.writeLock().lock();
		{
			rowIDsAL = null;
			offsetArray = null;
			if (modifiedBy >= 0)
			{
				if (b != null)
				{
					if (log)
					{
						LogManager.flush(lsn);
					}
					FileManager.write(blk, contents);
				}
				modifiedBy = -1;
			}
			blk = b;
			readDone = false;
			pins.clear();

			if (b != null)
			{
				FileManager.read(this, b, contents, schema, schemaMap, tx, fetchPos);
			}
		}
		lock.writeLock().unlock();
	}

	public void assignToBlock(final Block b, final boolean log, final Schema schema, final ConcurrentHashMap<Integer, Schema> schemaMap, final Transaction tx, final ArrayList<Integer> fetchPos, final int rank, final int rankSize) throws Exception
	{
		lock.writeLock().lock();
		{
			rowIDsAL = null;
			offsetArray = null;
			if (modifiedBy >= 0)
			{
				if (b != null)
				{
					if (log)
					{
						LogManager.flush(lsn);
					}
					FileManager.write(blk, contents);
				}
				modifiedBy = -1;
			}
			blk = b;
			readDone = false;
			pins.clear();

			if (b != null)
			{
				FileManager.read(this, b, contents, schema, schemaMap, tx, fetchPos, rank, rankSize);
			}
		}
		lock.writeLock().unlock();
	}

	public Read3Thread assignToBlock3(final Block b, final Block b2, final Block b3, final boolean log, final Page p2, final Page p3) throws Exception
	{
		// HRDBMSWorker.logger.debug("Short Page pin3 starting");
		lock.writeLock().lock();
		Read3Thread retval = null;
		try
		{
			p2.lock.writeLock().lock();
			{
				p3.lock.writeLock().lock();
				{
					rowIDsAL = null;
					offsetArray = null;
					p2.rowIDsAL = null;
					p2.offsetArray = null;
					p3.rowIDsAL = null;
					p3.offsetArray = null;
					if (modifiedBy >= 0)
					{
						if (log)
						{
							LogManager.flush(lsn);
						}
						FileManager.write(blk, contents);
						modifiedBy = -1;
					}
					if (p2.modifiedBy >= 0)
					{
						if (log)
						{
							LogManager.flush(p2.lsn);
						}
						FileManager.write(p2.blk, p2.contents);
						p2.modifiedBy = -1;
					}
					if (p3.modifiedBy >= 0)
					{
						if (log)
						{
							LogManager.flush(p3.lsn);
						}
						FileManager.write(p3.blk, p3.contents);
						p3.modifiedBy = -1;
					}
					blk = b;
					p2.blk = b2;
					p3.blk = b3;
					readDone = false;
					p2.readDone = false;
					p3.readDone = false;
					retval = FileManager.read3(this, p2, p3, b, contents, p2.contents, p3.contents);
					pins.clear();
					p2.pins.clear();
					p3.pins.clear();
				}
				p3.lock.writeLock().unlock();
			}
			p2.lock.writeLock().unlock();
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.debug("", e);
			throw e;
		}

		lock.writeLock().unlock();
		return retval;
		// HRDBMSWorker.logger.debug("Short Page pin3 ending");
	}

	public Read3Thread assignToBlock3(final Block b, final Block b2, final Block b3, final boolean log, final Page p2, final Page p3, final ArrayList<Integer> cols, final int layoutSize) throws Exception
	{
		// HRDBMSWorker.logger.debug("Short Page pin3 starting");
		lock.writeLock().lock();
		Read3Thread retval = null;
		try
		{
			p2.lock.writeLock().lock();
			{
				p3.lock.writeLock().lock();
				{
					rowIDsAL = null;
					offsetArray = null;
					p2.rowIDsAL = null;
					p2.offsetArray = null;
					p3.rowIDsAL = null;
					p3.offsetArray = null;
					if (modifiedBy >= 0)
					{
						if (log)
						{
							LogManager.flush(lsn);
						}
						FileManager.write(blk, contents);
						modifiedBy = -1;
					}
					if (p2.modifiedBy >= 0)
					{
						if (log)
						{
							LogManager.flush(p2.lsn);
						}
						FileManager.write(p2.blk, p2.contents);
						p2.modifiedBy = -1;
					}
					if (p3.modifiedBy >= 0)
					{
						if (log)
						{
							LogManager.flush(p3.lsn);
						}
						FileManager.write(p3.blk, p3.contents);
						p3.modifiedBy = -1;
					}
					blk = b;
					p2.blk = b2;
					p3.blk = b3;
					readDone = false;
					p2.readDone = false;
					p3.readDone = false;
					retval = FileManager.read3(this, p2, p3, b, contents, p2.contents, p3.contents, cols, layoutSize);
					pins.clear();
					p2.pins.clear();
					p3.pins.clear();
				}
				p3.lock.writeLock().unlock();
			}
			p2.lock.writeLock().unlock();
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.debug("", e);
			throw e;
		}

		lock.writeLock().unlock();
		return retval;
		// HRDBMSWorker.logger.debug("Short Page pin3 ending");
	}

	public Read3Thread assignToBlock3(final Block b, final Block b2, final Block b3, final boolean log, final Page p2, final Page p3, final int rank, final int rankSize) throws Exception
	{
		// HRDBMSWorker.logger.debug("Short Page pin3 starting");
		lock.writeLock().lock();
		Read3Thread retval = null;
		try
		{
			p2.lock.writeLock().lock();
			{
				p3.lock.writeLock().lock();
				{
					rowIDsAL = null;
					offsetArray = null;
					p2.rowIDsAL = null;
					p2.offsetArray = null;
					p3.rowIDsAL = null;
					p3.offsetArray = null;
					if (modifiedBy >= 0)
					{
						if (log)
						{
							LogManager.flush(lsn);
						}
						FileManager.write(blk, contents);
						modifiedBy = -1;
					}
					if (p2.modifiedBy >= 0)
					{
						if (log)
						{
							LogManager.flush(p2.lsn);
						}
						FileManager.write(p2.blk, p2.contents);
						p2.modifiedBy = -1;
					}
					if (p3.modifiedBy >= 0)
					{
						if (log)
						{
							LogManager.flush(p3.lsn);
						}
						FileManager.write(p3.blk, p3.contents);
						p3.modifiedBy = -1;
					}
					blk = b;
					p2.blk = b2;
					p3.blk = b3;
					readDone = false;
					p2.readDone = false;
					p3.readDone = false;
					retval = FileManager.read3(this, p2, p3, b, contents, p2.contents, p3.contents, rank, rankSize);
					pins.clear();
					p2.pins.clear();
					p3.pins.clear();
				}
				p3.lock.writeLock().unlock();
			}
			p2.lock.writeLock().unlock();
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.debug("", e);
			throw e;
		}

		lock.writeLock().unlock();
		return retval;
		// HRDBMSWorker.logger.debug("Short Page pin3 ending");
	}

	public void assignToBlock3(final Block b, final Block b2, final Block b3, final boolean log, final Schema schema1, final Schema schema2, final Schema schema3, final ConcurrentHashMap<Integer, Schema> schemaMap, final Transaction tx, final ArrayList<Integer> fetchPos, final Page p2, final Page p3) throws Exception
	{
		// HRDBMSWorker.logger.debug("Long Page pin3 starting");
		try
		{
			lock.writeLock().lock();
			{
				p2.lock.writeLock().lock();
				{
					p3.lock.writeLock().lock();
					{
						rowIDsAL = null;
						offsetArray = null;
						p2.rowIDsAL = null;
						p2.offsetArray = null;
						p3.rowIDsAL = null;
						p3.offsetArray = null;
						if (modifiedBy >= 0)
						{
							if (log)
							{
								LogManager.flush(lsn);
							}
							FileManager.write(blk, contents);
							modifiedBy = -1;
						}
						if (p2.modifiedBy >= 0)
						{
							if (log)
							{
								LogManager.flush(p2.lsn);
							}
							FileManager.write(p2.blk, p2.contents);
							p2.modifiedBy = -1;
						}
						if (p3.modifiedBy >= 0)
						{
							if (log)
							{
								LogManager.flush(p3.lsn);
							}
							FileManager.write(p3.blk, p3.contents);
							p3.modifiedBy = -1;
						}

						blk = b;
						readDone = false;
						pins.clear();
						p2.blk = b2;
						p2.readDone = false;
						p2.pins.clear();
						p3.blk = b3;
						p3.readDone = false;
						p3.pins.clear();

						FileManager.read3(this, p2, p3, b, contents, p2.contents, p3.contents, schema1, schema2, schema3, schemaMap, tx, fetchPos);
					}
					p3.lock.writeLock().unlock();
				}
				p2.lock.writeLock().unlock();
			}
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.debug("", e);
			throw e;
		}
		// HRDBMSWorker.logger.debug("Long Page pin3 ending");
		lock.writeLock().unlock();
	}

	public void assignToBlock3(final Block b, final Block b2, final Block b3, final boolean log, final Schema schema1, final Schema schema2, final Schema schema3, final ConcurrentHashMap<Integer, Schema> schemaMap, final Transaction tx, final ArrayList<Integer> fetchPos, final Page p2, final Page p3, final int rank, final int rankSize) throws Exception
	{
		// HRDBMSWorker.logger.debug("Long Page pin3 starting");
		try
		{
			lock.writeLock().lock();
			{
				p2.lock.writeLock().lock();
				{
					p3.lock.writeLock().lock();
					{
						rowIDsAL = null;
						offsetArray = null;
						p2.rowIDsAL = null;
						p2.offsetArray = null;
						p3.rowIDsAL = null;
						p3.offsetArray = null;
						if (modifiedBy >= 0)
						{
							if (log)
							{
								LogManager.flush(lsn);
							}
							FileManager.write(blk, contents);
							modifiedBy = -1;
						}
						if (p2.modifiedBy >= 0)
						{
							if (log)
							{
								LogManager.flush(p2.lsn);
							}
							FileManager.write(p2.blk, p2.contents);
							p2.modifiedBy = -1;
						}
						if (p3.modifiedBy >= 0)
						{
							if (log)
							{
								LogManager.flush(p3.lsn);
							}
							FileManager.write(p3.blk, p3.contents);
							p3.modifiedBy = -1;
						}

						blk = b;
						readDone = false;
						pins.clear();
						p2.blk = b2;
						p2.readDone = false;
						p2.pins.clear();
						p3.blk = b3;
						p3.readDone = false;
						p3.pins.clear();

						FileManager.read3(this, p2, p3, b, contents, p2.contents, p3.contents, schema1, schema2, schema3, schemaMap, tx, fetchPos, rank, rankSize);
					}
					p3.lock.writeLock().unlock();
				}
				p2.lock.writeLock().unlock();
			}
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.debug("", e);
			throw e;
		}
		// HRDBMSWorker.logger.debug("Long Page pin3 ending");
		lock.writeLock().unlock();
	}

	public void assignToBlockFromMemory(final Block b, final boolean log, final ByteBuffer data) throws Exception
	{
		// FileManager.getFile(b.fileName());

		lock.writeLock().lock();
		rowIDsAL = null;
		offsetArray = null;
		if (modifiedBy >= 0)
		{
			if (log)
			{
				LogManager.flush(lsn);
			}
			FileManager.write(blk, contents);
			modifiedBy = -1;
		}
		blk = b;
		readDone = true;
		// FileManager.read(this, b, contents);
		contents.clear();
		contents.position(0);
		contents.put(data.array());
		pins.clear();
		lock.writeLock().unlock();
	}

	public ReadThread assignToBlocks(final Block b, final int num, final boolean log, final ArrayList<Integer> indexes, final Page[] bp, final int rank, final int rankSize) throws Exception
	{
		// HRDBMSWorker.logger.debug("Short Page pin3 starting");
		ReadThread retval = null;
		try
		{
			this.lock.writeLock().lock();
			rowIDsAL = null;
			offsetArray = null;
			int i = 1;
			while (i < num)
			{
				bp[indexes.get(i)].lock.writeLock().lock();
				bp[indexes.get(i)].rowIDsAL = null;
				bp[indexes.get(i)].offsetArray = null;
				i++;
			}

			if (modifiedBy >= 0)
			{
				flush();
			}

			i = 1;
			int page = b.number() + 1;
			while (i < num)
			{
				final Page p2 = bp[indexes.get(i)];
				if (p2.modifiedBy >= 0)
				{
					p2.flush();
				}

				p2.blk = new Block(b.fileName(), page++);
				p2.readDone = false;
				i++;
			}

			blk = b;
			readDone = false;
			retval = FileManager.read(this, num, indexes, bp, rank, rankSize);
			pins.clear();

			i = 1;
			while (i < num)
			{
				final Page p2 = bp[indexes.get(i)];
				p2.pins.clear();
				i++;
			}
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.debug("", e);
			throw e;
		}

		int i = num - 1;
		while (i >= 1)
		{
			bp[indexes.get(i)].lock.writeLock().unlock();
			i--;
		}
		this.lock.writeLock().unlock();

		return retval;
		// HRDBMSWorker.logger.debug("Short Page pin3 ending");
	}

	public void assignToBlockSync(final Block b, final boolean log) throws Exception
	{
		lock.writeLock().lock();
		rowIDsAL = null;
		offsetArray = null;
		if (b != null)
		{
			FileManager.readSync(this, b, contents);
		}
		lock.writeLock().unlock();
	}

	public void assignToBlockSync(final Block b, final boolean log, final Schema schema, final ConcurrentHashMap<Integer, Schema> schemaMap, final Transaction tx, final ArrayList<Integer> fetchPos) throws Exception
	{
		lock.writeLock().lock();
		{
			rowIDsAL = null;
			offsetArray = null;
			if (b != null)
			{
				FileManager.readSync(this, b, contents, schema, schemaMap, tx, fetchPos);
			}
		}
		lock.writeLock().unlock();
	}

	public Block block()
	{
		return blk;
	}

	public ByteBuffer buffer()
	{
		return contents;
	}

	@Override
	public boolean equals(final Object rhs)
	{
		if (rhs == null)
		{
			return false;
		}

		if (!(rhs instanceof Page))
		{
			return false;
		}

		final Page r = (Page)rhs;
		return blk.equals(r.blk);
	}

	public byte get(final int pos)
	{
		lock.readLock().lock();
		while (!readDone)
		{
		}

		try
		{
			final byte[] hb = (byte[])unsafe.getObject(contents, offset);
			lock.readLock().unlock();
			return hb[pos];
			// return contents.get(pos);
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.debug("Error reading from page " + blk + " trying to read 1 byte at offset " + pos);
			lock.readLock().unlock();
			throw e;
		}
	}

	public void get(final int off, final byte[] buff) throws Exception
	{
		lock.readLock().lock();
		while (!readDone)
		{
		}
		try
		{
			// contents.position(off);
			// contents.get(buff);
			final byte[] hb = (byte[])unsafe.getObject(contents, offset);
			System.arraycopy(hb, off, buff, 0, buff.length);
			lock.readLock().unlock();
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.debug("Error reading from page " + blk + " trying to read " + buff.length + " bytes at offset " + off);
			lock.readLock().unlock();
			throw e;
		}
	}

	public void get(int off, final int[] buff) throws Exception
	{
		lock.readLock().lock();
		while (!readDone)
		{
		}

		// contents.position(off);
		// contents.asIntBuffer().get(buff);
		final byte[] hb = (byte[])unsafe.getObject(contents, offset);
		int i = 0;
		while (i < buff.length)
		{
			buff[i] = ((hb[off]) << 24) | ((hb[off + 1] & 0xff) << 16) | ((hb[off + 2] & 0xff) << 8) | (hb[off + 3] & 0xff);
			i++;
			off += 4;
		}
		lock.readLock().unlock();
	}

	public double getDouble(final int pos)
	{
		lock.readLock().lock();
		while (!readDone)
		{
		}

		final double retval = contents.getDouble(pos);
		lock.readLock().unlock();
		return retval;
		// byte[] hb = (byte[])unsafe.getObject(contents, offset);
		// long retval = (hb[pos] << 56) | ((hb[pos+1] & 0xff) << 48) |
		// ((hb[pos+2] & 0xff) << 40) | ((hb[pos+3] & 0xff) << 32) | ((hb[pos+4]
		// & 0xff) << 24) | ((hb[pos+5] & 0xff) << 16) | ((hb[pos+6] & 0xff) <<
		// 8) | (hb[pos+7] & 0xff);
		// return Double.longBitsToDouble(retval);
	}

	public float getFloat(final int pos)
	{
		lock.readLock().lock();
		while (!readDone)
		{
		}

		final float retval = contents.getFloat(pos);
		lock.readLock().unlock();
		return retval;
		// byte[] hb = (byte[])unsafe.getObject(contents, offset);
		// int retval = (hb[pos] << 24) | ((hb[pos+1] & 0xff) << 16) |
		// ((hb[pos+2] & 0xff) << 8) | (hb[pos+3] & 0xff);
		// return Float.intBitsToFloat(retval);
	}

	public int getInt(final int pos)
	{
		lock.readLock().lock();
		while (!readDone)
		{
		}

		// return contents.getInt(pos);
		final byte[] hb = (byte[])unsafe.getObject(contents, offset);
		final int retval = (hb[pos] << 24) | ((hb[pos + 1] & 0xff) << 16) | ((hb[pos + 2] & 0xff) << 8) | (hb[pos + 3] & 0xff);
		lock.readLock().unlock();
		return retval;
	}

	public long getLong(final int pos)
	{
		lock.readLock().lock();
		while (!readDone)
		{
		}

		// return contents.getLong(pos);
		final byte[] hb = (byte[])unsafe.getObject(contents, offset);
		final long retval = (((long)hb[pos]) << 56) | ((((long)hb[pos + 1]) & 0xff) << 48) | ((((long)hb[pos + 2]) & 0xff) << 40) | ((((long)hb[pos + 3]) & 0xff) << 32) | ((((long)hb[pos + 4]) & 0xff) << 24) | ((((long)hb[pos + 5]) & 0xff) << 16) | ((((long)hb[pos + 6]) & 0xff) << 8) | (((long)hb[pos + 7]) & 0xff);
		lock.readLock().unlock();
		return retval;
	}

	public int getMedium(final int pos)
	{
		lock.readLock().lock();
		while (!readDone)
		{
		}

		// return contents.getInt(pos);
		final byte[] hb = (byte[])unsafe.getObject(contents, offset);
		final int retval = (hb[pos] << 16) | ((hb[pos + 1] & 0xff) << 8) | ((hb[pos + 2] & 0xff));
		lock.readLock().unlock();
		return retval;
	}

	public int[] getMediums(int pos, final int num)
	{
		lock.readLock().lock();
		while (!readDone)
		{
		}

		final byte[] hb = (byte[])unsafe.getObject(contents, offset);
		final int[] retval = new int[num];
		int i = 0;
		while (i < num)
		{
			retval[i] = (hb[pos] << 16) | ((hb[pos + 1] & 0xff) << 8) | ((hb[pos + 2] & 0xff));
			pos += 3;
			i++;
		}
		lock.readLock().unlock();
		return retval;
	}

	public short getShort(final int pos)
	{
		lock.readLock().lock();
		while (!readDone)
		{
		}

		// return contents.getShort(off);
		final byte[] hb = (byte[])unsafe.getObject(contents, offset);
		final short retval = (short)((hb[pos] << 8) | (hb[pos + 1] & 0xff));
		lock.readLock().unlock();
		return retval;
	}

	@Override
	public int hashCode()
	{
		return blk.hashCode();
	}

	public boolean isModified()
	{
		return modifiedBy != -1;
	}

	public boolean isPinned()
	{
		return pins.size() > 0;
	}

	public boolean isPinnedBy(final long txnum)
	{
		return pins.containsKey(txnum);
	}

	public boolean isReady()
	{
		return readDone;
	}

	public void pin(final long lsn, final long txnum)
	{
		lock.writeLock().lock();
		final AtomicInteger numPins = pins.get(txnum);
		if (numPins == null)
		{
			final AtomicInteger prev = pins.putIfAbsent(txnum, new AtomicInteger(1));
			if (prev != null)
			{
				pins.get(txnum).getAndIncrement();
			}
		}
		else
		{
			pins.get(txnum).getAndIncrement();
		}
		this.timePinned = lsn;
		lock.writeLock().unlock();
	}

	public long pinTime()
	{
		return timePinned;
	}

	public void preAssignToBlock(final Block b, final boolean log) throws Exception
	{
		lock.writeLock().lock();
		rowIDsAL = null;
		offsetArray = null;
		if (modifiedBy >= 0)
		{
			if (b != null)
			{
				if (log)
				{
					LogManager.flush(lsn);
				}
				FileManager.write(blk, contents);
			}
			modifiedBy = -1;
		}
		blk = b;
		readDone = false;
		lock.writeLock().unlock();
	}

	public byte[] read(final int off, final int length)
	{
		lock.readLock().lock();
		while (!readDone)
		{
		}
		final byte[] retval = new byte[length];
		final byte[] hb = (byte[])unsafe.getObject(contents, offset);
		System.arraycopy(hb, off, retval, 0, length);
		// contents.position(off);
		// contents.get(retval);
		lock.readLock().unlock();
		return retval;
	}

	public void setNotModified()
	{
		lock.writeLock().lock();
		modifiedBy = -1;
		lock.writeLock().unlock();
	}

	public void setPinTime(final long time)
	{
		timePinned = time;
	}

	public void setReady()
	{
		readDone = true;
	}

	@Override
	public String toString()
	{
		return blk.toString();
	}

	public void unpin(final long txnum)
	{
		lock.writeLock().lock();
		pins.remove(txnum);
		lock.writeLock().unlock();
	}

	public boolean unpin1(final long txnum)
	{
		lock.writeLock().lock();
		final AtomicInteger ai = pins.get(txnum);
		if (ai != null)
		{
			final int newVal = ai.decrementAndGet();
			if (newVal == 0)
			{
				pins.remove(txnum);
				lock.writeLock().unlock();
				return true;
			}

			lock.writeLock().unlock();
			return false;
		}

		lock.writeLock().unlock();
		return true;
	}

	public void write(final int off, final byte[] data, final long txnum, final long lsn) throws Exception
	{
		lock.writeLock().lock();
		while (!readDone)
		{
		}
		if (lsn > 0)
		{
			this.lsn = lsn;
		}

		this.modifiedBy = txnum;

		// LockManager.lock.lock();
		// Long xTx = LockManager.xBlocksToTXs.get(this.blk);
		// if (xTx == null || xTx.longValue() != txnum)
		// {
		// Exception e = new Exception();
		// HRDBMSWorker.logger.debug("Tried to write to page without xLock", e);
		// }
		// LockManager.lock.unlock();
		// BufferManager.write(this, off, data);
		final byte[] hb = (byte[])unsafe.getObject(contents, offset);
		System.arraycopy(data, 0, hb, off, data.length);
		lock.writeLock().unlock();
	}

	public void writeDirect(final int off, final byte[] data)
	{
		// contents.position(off);
		// contents.put(data);
		lock.writeLock().lock();
		final byte[] hb = (byte[])unsafe.getObject(contents, offset);
		System.arraycopy(data, 0, hb, off, data.length);
		lock.writeLock().unlock();
	}

	public void writeShift(final int srcOff, final int destOff, final int len, final long txnum, final long lsn) throws Exception
	{
		lock.writeLock().lock();
		while (!readDone)
		{
		}
		if (lsn > 0)
		{
			this.lsn = lsn;
		}

		this.modifiedBy = txnum;

		final byte[] hb = (byte[])unsafe.getObject(contents, offset);
		System.arraycopy(hb, srcOff, hb, destOff, len);
		lock.writeLock().unlock();
	}

	private void flush() throws Exception
	{
		LogManager.flush(lsn);
		FileManager.write(blk, contents);
		modifiedBy = -1;
	}
}
