package com.exascale.filesystem;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import com.exascale.managers.FileManager;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.managers.LogManager;
import com.exascale.managers.ResourceManager;
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
	private ReentrantLock lock;

	private volatile boolean readDone = false;

	public Page()
	{
		try
		{
			this.contents = ByteBuffer.allocate(BLOCK_SIZE);
			lock = new ReentrantLock();
		}
		catch (final Throwable e)
		{
			// System.out.println("Bufferpool manager failed to allocate pages for the bufferpool");
			HRDBMSWorker.logger.error("Bufferpool manager failed to allocate pages for the bufferpool", e);
			System.exit(1);
		}
		pins = new ConcurrentHashMap<Long, AtomicInteger>(16, 0.75f, 6 * ResourceManager.cpus);
		readDone = false;
	}

	public ReadThread assignToBlock(Block b, boolean log) throws Exception
	{
		lock.lock();
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
		lock.unlock();
		return retval;
	}
	
	public ReadThread assignToBlock(Block b, boolean log, int rank, int rankSize) throws Exception
	{
		lock.lock();
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
		lock.unlock();
		return retval;
	}

	public ReadThread assignToBlock(Block b, boolean log, ArrayList<Integer> cols, int layoutSize) throws Exception
	{
		lock.lock();
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
		lock.unlock();
		return retval;
	}
	
	public ReadThread assignToBlock(Block b, boolean log, ArrayList<Integer> cols, int layoutSize, int rank, int rankSize) throws Exception
	{
		lock.lock();
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
		lock.unlock();
		return retval;
	}

	public void assignToBlock(Block b, boolean log, boolean flag) throws Exception
	{
		lock.lock();
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
		lock.unlock();
	}

	public void assignToBlock(Block b, boolean log, Schema schema, ConcurrentHashMap<Integer, Schema> schemaMap, Transaction tx, ArrayList<Integer> fetchPos) throws Exception
	{
		lock.lock();
		{
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
		lock.unlock();
	}
	
	public void assignToBlock(Block b, boolean log, Schema schema, ConcurrentHashMap<Integer, Schema> schemaMap, Transaction tx, ArrayList<Integer> fetchPos, int rank, int rankSize) throws Exception
	{
		lock.lock();
		{
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
		lock.unlock();
	}

	public Read3Thread assignToBlock3(Block b, Block b2, Block b3, boolean log, Page p2, Page p3) throws Exception
	{
		// HRDBMSWorker.logger.debug("Short Page pin3 starting");
		lock.lock();
		Read3Thread retval = null;
		try
		{
			p2.lock.lock();
			{
				p3.lock.lock();
				{
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
				p3.lock.unlock();
			}
			p2.lock.unlock();
		}
		catch (Exception e)
		{
			HRDBMSWorker.logger.debug("", e);
			throw e;
		}

		lock.unlock();
		return retval;
		// HRDBMSWorker.logger.debug("Short Page pin3 ending");
	}
	
	public ReadThread assignToBlocks(Block b, int num, boolean log, ArrayList<Integer> indexes, Page[] bp, int rank, int rankSize) throws Exception
	{
		// HRDBMSWorker.logger.debug("Short Page pin3 starting");
		ReadThread retval = null;
		try
		{
			this.lock.lock();
			int i = 1;
			while (i < num)
			{
				bp[indexes.get(i)].lock.lock();
				i++;
			}
			
			if (modifiedBy >= 0)
			{
				if (log)
				{
					LogManager.flush(lsn);
				}
				FileManager.write(blk, contents);
				modifiedBy = -1;
			}
			
			i = 1;
			while (i < num)
			{
				Page p2 = bp[indexes.get(i)];
				if (p2.modifiedBy >= 0)
				{
					if (log)
					{
						LogManager.flush(p2.lsn);
					}
					FileManager.write(p2.blk, p2.contents);
					p2.modifiedBy = -1;
				}
				
				i++;
			}
			
			blk = b;
			i = 1;
			int page = b.number() + 1;
			while (i < num)
			{
				Page p2 = bp[indexes.get(i)];
				p2.blk = new Block(b.fileName(), page++);
				p2.readDone = false;
				i++;
			}
			
			readDone = false;
			retval = FileManager.read(this, num, indexes, bp, rank, rankSize);
			pins.clear();
			
			i = 1;
			while (i < num)
			{
				Page p2 = bp[indexes.get(i)];
				p2.pins.clear();
				i++;
			}
		}
		catch (Exception e)
		{
			HRDBMSWorker.logger.debug("", e);
			throw e;
		}
		
		int i = num - 1;
		while (i >= 1)
		{
			bp[indexes.get(i)].lock.unlock();
			i--;
		}
		this.lock.unlock();

		return retval;
		// HRDBMSWorker.logger.debug("Short Page pin3 ending");
	}
	
	public Read3Thread assignToBlock3(Block b, Block b2, Block b3, boolean log, Page p2, Page p3, int rank, int rankSize) throws Exception
	{
		// HRDBMSWorker.logger.debug("Short Page pin3 starting");
		lock.lock();
		Read3Thread retval = null;
		try
		{
			p2.lock.lock();
			{
				p3.lock.lock();
				{
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
				p3.lock.unlock();
			}
			p2.lock.unlock();
		}
		catch (Exception e)
		{
			HRDBMSWorker.logger.debug("", e);
			throw e;
		}

		lock.unlock();
		return retval;
		// HRDBMSWorker.logger.debug("Short Page pin3 ending");
	}

	public Read3Thread assignToBlock3(Block b, Block b2, Block b3, boolean log, Page p2, Page p3, ArrayList<Integer> cols, int layoutSize) throws Exception
	{
		// HRDBMSWorker.logger.debug("Short Page pin3 starting");
		lock.lock();
		Read3Thread retval = null;
		try
		{
			p2.lock.lock();
			{
				p3.lock.lock();
				{
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
				p3.lock.unlock();
			}
			p2.lock.unlock();
		}
		catch (Exception e)
		{
			HRDBMSWorker.logger.debug("", e);
			throw e;
		}

		lock.unlock();
		return retval;
		// HRDBMSWorker.logger.debug("Short Page pin3 ending");
	}

	public void assignToBlock3(Block b, Block b2, Block b3, boolean log, Schema schema1, Schema schema2, Schema schema3, ConcurrentHashMap<Integer, Schema> schemaMap, Transaction tx, ArrayList<Integer> fetchPos, Page p2, Page p3) throws Exception
	{
		// HRDBMSWorker.logger.debug("Long Page pin3 starting");
		try
		{
			lock.lock();
			{
				p2.lock.lock();
				{
					p3.lock.lock();
					{
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
					p3.lock.unlock();
				}
				p2.lock.unlock();
			}
		}
		catch (Exception e)
		{
			HRDBMSWorker.logger.debug("", e);
			throw e;
		}
		// HRDBMSWorker.logger.debug("Long Page pin3 ending");
		lock.unlock();
	}
	
	public void assignToBlock3(Block b, Block b2, Block b3, boolean log, Schema schema1, Schema schema2, Schema schema3, ConcurrentHashMap<Integer, Schema> schemaMap, Transaction tx, ArrayList<Integer> fetchPos, Page p2, Page p3, int rank, int rankSize) throws Exception
	{
		// HRDBMSWorker.logger.debug("Long Page pin3 starting");
		try
		{
			lock.lock();
			{
				p2.lock.lock();
				{
					p3.lock.lock();
					{
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
					p3.lock.unlock();
				}
				p2.lock.unlock();
			}
		}
		catch (Exception e)
		{
			HRDBMSWorker.logger.debug("", e);
			throw e;
		}
		// HRDBMSWorker.logger.debug("Long Page pin3 ending");
		lock.unlock();
	}

	public void assignToBlockFromMemory(Block b, boolean log, ByteBuffer data) throws Exception
	{
		// FileManager.getFile(b.fileName());

		lock.lock();
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
		lock.unlock();
	}

	public void assignToBlockSync(Block b, boolean log) throws Exception
	{
		lock.lock();
		if (b != null)
		{
			FileManager.readSync(this, b, contents);
		}
		lock.unlock();
	}

	public void assignToBlockSync(Block b, boolean log, Schema schema, ConcurrentHashMap<Integer, Schema> schemaMap, Transaction tx, ArrayList<Integer> fetchPos) throws Exception
	{
		lock.lock();
		{
			if (b != null)
			{
				FileManager.readSync(this, b, contents, schema, schemaMap, tx, fetchPos);
			}
		}
		lock.unlock();
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
	public boolean equals(Object rhs)
	{
		if (rhs == null)
		{
			return false;
		}

		if (!(rhs instanceof Page))
		{
			return false;
		}

		Page r = (Page)rhs;
		return blk.equals(r.blk);
	}

	public byte get(int pos)
	{
		lock.lock();
		while (!readDone) {}
		
		try
		{
			byte[] hb = (byte[])unsafe.getObject(contents, offset);
			lock.unlock();
			return hb[pos];
			// return contents.get(pos);
		}
		catch (Exception e)
		{
			HRDBMSWorker.logger.debug("Error reading from page " + blk + " trying to read 1 byte at offset " + pos);
			lock.unlock();
			throw e;
		}
	}

	public void get(int off, byte[] buff) throws Exception
	{
		lock.lock();
		while (!readDone) {}
		try
		{
			// contents.position(off);
			// contents.get(buff);
			byte[] hb = (byte[])unsafe.getObject(contents, offset);
			System.arraycopy(hb, off, buff, 0, buff.length);
			lock.unlock();
		}
		catch (Exception e)
		{
			HRDBMSWorker.logger.debug("Error reading from page " + blk + " trying to read " + buff.length + " bytes at offset " + off);
			lock.unlock();
			throw e;
		}
	}

	public void get(int off, int[] buff) throws Exception
	{
		lock.lock();
		while (!readDone) {}

		// contents.position(off);
		// contents.asIntBuffer().get(buff);
		byte[] hb = (byte[])unsafe.getObject(contents, offset);
		int i = 0;
		while (i < buff.length)
		{
			buff[i] = ((hb[off]) << 24) | ((hb[off + 1] & 0xff) << 16) | ((hb[off + 2] & 0xff) << 8) | (hb[off + 3] & 0xff);
			i++;
			off += 4;
		}
		lock.unlock();
	}

	public double getDouble(int pos)
	{
		lock.lock();
		while (!readDone) {}

		double retval = contents.getDouble(pos);
		lock.unlock();
		return retval;
		// byte[] hb = (byte[])unsafe.getObject(contents, offset);
		// long retval = (hb[pos] << 56) | ((hb[pos+1] & 0xff) << 48) |
		// ((hb[pos+2] & 0xff) << 40) | ((hb[pos+3] & 0xff) << 32) | ((hb[pos+4]
		// & 0xff) << 24) | ((hb[pos+5] & 0xff) << 16) | ((hb[pos+6] & 0xff) <<
		// 8) | (hb[pos+7] & 0xff);
		// return Double.longBitsToDouble(retval);
	}

	public float getFloat(int pos)
	{
		lock.lock();
		while (!readDone) {}

		float retval = contents.getFloat(pos);
		lock.unlock();
		return retval;
		// byte[] hb = (byte[])unsafe.getObject(contents, offset);
		// int retval = (hb[pos] << 24) | ((hb[pos+1] & 0xff) << 16) |
		// ((hb[pos+2] & 0xff) << 8) | (hb[pos+3] & 0xff);
		// return Float.intBitsToFloat(retval);
	}

	public int getInt(int pos)
	{
		lock.lock();
		while (!readDone) {}

		// return contents.getInt(pos);
		byte[] hb = (byte[])unsafe.getObject(contents, offset);
		int retval = (hb[pos] << 24) | ((hb[pos + 1] & 0xff) << 16) | ((hb[pos + 2] & 0xff) << 8) | (hb[pos + 3] & 0xff);
		lock.unlock();
		return retval;
	}

	public long getLong(int pos)
	{
		lock.lock();
		while (!readDone) {}

		// return contents.getLong(pos);
		byte[] hb = (byte[])unsafe.getObject(contents, offset);
		long retval = (((long)hb[pos]) << 56) | ((((long)hb[pos + 1]) & 0xff) << 48) | ((((long)hb[pos + 2]) & 0xff) << 40) | ((((long)hb[pos + 3]) & 0xff) << 32) | ((((long)hb[pos + 4]) & 0xff) << 24) | ((((long)hb[pos + 5]) & 0xff) << 16) | ((((long)hb[pos + 6]) & 0xff) << 8) | (((long)hb[pos + 7]) & 0xff);
		lock.unlock();
		return retval;
	}
	
	private void spin()
	{
		long start = System.currentTimeMillis();
		int i = 0;
		while (!readDone)
		{
			i++;
			
			if (i % 1000000 == 0)
			{
				long now = System.currentTimeMillis();
				
				if (now - start > 15000)
				{
					try
					{
						HRDBMSWorker.logger.debug("Page was pinned but not ready: " + blk + " READY: " + readDone + " PINNED: " + pins);
						new ReadThread(this, blk, contents).run();
						HRDBMSWorker.logger.debug("Reread page");
					}
					catch(Exception e)
					{
						HRDBMSWorker.logger.debug("", e);
					}
				
					start = now;
				}
			}
		}
	}

	public int getMedium(int pos)
	{
		lock.lock();
		while (!readDone) {}

		// return contents.getInt(pos);
		byte[] hb = (byte[])unsafe.getObject(contents, offset);
		int retval = (hb[pos] << 16) | ((hb[pos + 1] & 0xff) << 8) | ((hb[pos + 2] & 0xff));
		lock.unlock();
		return retval;
	}

	public short getShort(int pos)
	{
		lock.lock();
		while (!readDone) {}

		// return contents.getShort(off);
		byte[] hb = (byte[])unsafe.getObject(contents, offset);
		short retval = (short)((hb[pos] << 8) | (hb[pos + 1] & 0xff));
		lock.unlock();
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

	public boolean isPinnedBy(long txnum)
	{
		return pins.containsKey(txnum);
	}

	public boolean isReady()
	{
		return readDone;
	}

	public void pin(long lsn, long txnum)
	{
		lock.lock();
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
		lock.unlock();
	}

	public long pinTime()
	{
		return timePinned;
	}

	public void preAssignToBlock(Block b, boolean log) throws Exception
	{
		lock.lock();
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
		lock.unlock();
	}

	public byte[] read(int off, int length)
	{
		lock.lock();
		while (!readDone) {}
		final byte[] retval = new byte[length];
		byte[] hb = (byte[])unsafe.getObject(contents, offset);
		System.arraycopy(hb, off, retval, 0, length);
		// contents.position(off);
		// contents.get(retval);
		lock.unlock();
		return retval;
	}

	public void setNotModified()
	{
		lock.lock();
		modifiedBy = -1;
		lock.unlock();
	}

	public void setPinTime(long time)
	{
		timePinned = time;
	}

	public void setReady()
	{
		readDone = true;
	}

	public void unpin(long txnum)
	{
		lock.lock();
		pins.remove(txnum);
		lock.unlock();
	}

	public boolean unpin1(long txnum)
	{
		lock.lock();
		AtomicInteger ai = pins.get(txnum);
		if (ai != null)
		{
			int newVal = ai.decrementAndGet();
			if (newVal == 0)
			{
				pins.remove(txnum);
				lock.unlock();
				return true;
			}

			lock.unlock();
			return false;
		}

		lock.unlock();
		return true;
	}

	public void write(int off, byte[] data, long txnum, long lsn) throws Exception
	{
		lock.lock();
		while (!readDone) {}
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
		byte[] hb = (byte[])unsafe.getObject(contents, offset);
		System.arraycopy(data, 0, hb, off, data.length);
		lock.unlock();
	}

	public void writeDirect(int off, byte[] data)
	{
		// contents.position(off);
		// contents.put(data);
		lock.lock();
		byte[] hb = (byte[])unsafe.getObject(contents, offset);
		System.arraycopy(data, 0, hb, off, data.length);
		lock.unlock();
	}

	public void writeShift(int srcOff, int destOff, int len, long txnum, long lsn) throws Exception
	{
		lock.lock();
		while (!readDone) {}
		if (lsn > 0)
		{
			this.lsn = lsn;
		}

		this.modifiedBy = txnum;

		byte[] hb = (byte[])unsafe.getObject(contents, offset);
		System.arraycopy(hb, srcOff, hb, destOff, len);
		lock.unlock();
	}
}
