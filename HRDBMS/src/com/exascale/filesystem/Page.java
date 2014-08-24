package com.exascale.filesystem;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import com.exascale.managers.BufferManager;
import com.exascale.managers.FileManager;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.managers.LogManager;

public class Page
{
	public static final int BLOCK_SIZE = 128 * 1024; // match btrfs compression
														// extent size
	private ByteBuffer contents;
	private Block blk;
	private final ConcurrentHashMap<Long, AtomicInteger> pins;
	private long modifiedBy = -1;
	private long timePinned = -1;
	private long lsn;
	private AtomicBoolean readDone = new AtomicBoolean(false);

	public Page()
	{
		try
		{
			this.contents = ByteBuffer.allocate(BLOCK_SIZE);
		}
		catch (final Throwable e)
		{
			// System.out.println("Bufferpool manager failed to allocate pages for the bufferpool");
			HRDBMSWorker.logger.error("Bufferpool manager failed to allocate pages for the bufferpool");
			System.exit(1);
		}
		pins = new ConcurrentHashMap<Long, AtomicInteger>();
		readDone.set(false);
	}
	
	public synchronized void setNotModified()
	{
		modifiedBy = -1;
	}

	public synchronized void assignToBlock(Block b, boolean log) throws IOException
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
		blk = b;
		readDone.set(false);
		FileManager.read(this, b, contents);
		pins.clear();
	}

	public Block block()
	{
		return blk;
	}

	public ByteBuffer buffer()
	{
		return contents;
	}

	public synchronized byte get(int pos)
	{
		while (!readDone.get())
		{
		}
		try
		{
			contents.position(pos);
			return contents.get();
		}
		catch(Exception e)
		{
			HRDBMSWorker.logger.debug("Error reading from page " + blk + " trying to read 1 byte at offset " + pos);
			throw e;
		}
	}

	public synchronized void get(int off, byte[] buff) throws Exception
	{
		while (!readDone.get())
		{
		}
		try
		{
			contents.position(off);
			contents.get(buff);
		}
		catch(Exception e)
		{
			HRDBMSWorker.logger.debug("Error reading from page " + blk + " trying to read " + buff.length + " bytes at offset " + off);
			throw e;
		}
	}

	public synchronized double getDouble(int off)
	{
		while (!readDone.get())
		{
		}
		contents.position(off);
		return contents.getDouble();
	}

	public synchronized float getFloat(int off)
	{
		while (!readDone.get())
		{
		}
		contents.position(off);
		return contents.getFloat();
	}

	public synchronized int getInt(int pos)
	{
		while (!readDone.get())
		{
		}
		contents.position(pos);
		return contents.getInt();
	}

	public synchronized long getLong(int pos)
	{
		while (!readDone.get())
		{
		}
		contents.position(pos);
		return contents.getLong();
	}

	public synchronized short getShort(int off)
	{
		while (!readDone.get())
		{
		}
		contents.position(off);
		return contents.getShort();
	}

	public boolean isModified()
	{
		return modifiedBy != -1;
	}

	public boolean isModifiedBy(long txnum)
	{
		return modifiedBy == txnum;
	}

	public boolean isPinned()
	{
		return pins.size() > 0;
	}

	public boolean isReady()
	{
		return readDone.get();
	}

	public void pin(long lsn, long txnum)
	{
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
	}

	public long pinTime()
	{
		return timePinned;
	}

	public synchronized byte[] read(int off, int length)
	{
		while (!readDone.get())
		{
		}
		final byte[] retval = new byte[length];
		contents.position(off);
		contents.get(retval);
		return retval;
	}

	public void setPinTime(long time)
	{
		timePinned = time;
	}

	public void setReady()
	{
		readDone.set(true);
	}

	public void unpin(long txnum)
	{
		pins.remove(txnum);
	}

	public synchronized void write(int off, byte[] data, long txnum, long lsn)
	{
		while (!readDone.get())
		{
		}
		if (lsn > 0)
		{
			this.lsn = lsn;
		}

		this.modifiedBy = txnum;
		BufferManager.write(this, off, data);
	}
}
