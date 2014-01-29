package com.exascale.filesystem;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.exascale.managers.BufferManager;
import com.exascale.managers.FileManager;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.managers.LogManager;

public class Page 
{
	public static final int BLOCK_SIZE = 128 * 1024; //match btrfs compression extent size
	protected ByteBuffer contents;
	protected Block blk;
	protected ConcurrentHashMap<Long, AtomicInteger> pins;
	protected long modifiedBy = -1;
	protected long timePinned = -1;
	protected long lsn;
	protected boolean readDone;
	
	public Page()
	{
		try
		{
			this.contents = ByteBuffer.allocateDirect(BLOCK_SIZE);
		}
		catch (Throwable e)
		{
			//System.out.println("Bufferpool manager failed to allocate pages for the bufferpool");
			HRDBMSWorker.logger.error("Bufferpool manager failed to allocate pages for the bufferpool");
			System.exit(1);
		}
		pins = new ConcurrentHashMap<Long, AtomicInteger>();
		readDone = false;
	}
	
	public Block block()
	{
		return blk;
	}
	
	public void pin(long lsn, long txnum)
	{
		AtomicInteger numPins = pins.get(txnum);
		if (numPins == null)
		{
			AtomicInteger prev = pins.putIfAbsent(txnum, new AtomicInteger(1));
			if (prev != null)
			{
				pins.get(txnum).getAndIncrement();
			}
		}
		else
		{
			pins.get (txnum).getAndIncrement();
		}
		this.timePinned = lsn;
	}
	
	public long pinTime()
	{
		return timePinned;
	}
	
	public void setPinTime(long time)
	{
		timePinned = time;
	}
	
	public void unpin(long txnum)
	{
		synchronized(pins)
		{
			pins.remove(txnum);
		}
	}
	
	public boolean isPinned()
	{
		return pins.size() > 0;
	}
	
	public boolean isModifiedBy(long txnum)
	{
		return modifiedBy == txnum;
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
		readDone = false;
		FileManager.read(this, b, contents);
		pins.clear();
	} 
	
	public ByteBuffer buffer()
	{
		return contents;
	}
	

	public synchronized void write(int off, byte[] data, long txnum, long lsn)
	{
		if (lsn > 0)
		{
			this.lsn = lsn;
		}
		
		this.modifiedBy = txnum;
		BufferManager.write(this, off, data);
	}
	
	public boolean isModified()
	{
		return modifiedBy != -1;
	}
	
	public boolean isReady()
	{
		return readDone;
	}
	
	public void setReady()
	{
		readDone = true;
	}
	
	public synchronized byte[] read(int off, int length)
	{
		while (!readDone) {}
		byte[] retval = new byte[length];
		contents.position(off);
		contents.get(retval);
		return retval;
	}
	
	public synchronized byte get(int pos)
	{
		while (!readDone) {}
		contents.position(pos);
		return contents.get();
	}
	
	public synchronized int getInt(int pos)
	{
		while (!readDone) {}
		contents.position(pos);
		return contents.getInt();
	}
	
	public synchronized long getLong(int pos)
	{
		while (!readDone) {}
		contents.position(pos);
		return contents.getLong();
	}
	
	public synchronized void get(int off, byte[] buff)
	{
		while (!readDone) {}
		contents.position(off);
		contents.get(buff);
	}
	
	public synchronized double getDouble(int off)
	{
		while (!readDone) {}
		contents.position(off);
		return contents.getDouble();
	}
	
	public synchronized float getFloat(int off)
	{
		while (!readDone) {}
		contents.position(off);
		return contents.getFloat();
	}
	
	public synchronized short getShort(int off)
	{
		while (!readDone) {}
		contents.position(off);
		return contents.getShort();
	}
}
