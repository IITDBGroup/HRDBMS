package com.exascale;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.HashMap;

public class Page 
{
	public static final int BLOCK_SIZE = 64 * 1024;
	private ByteBuffer contents;
	private Block blk;
	private HashMap<Long, Integer> pins;
	private long modifiedBy = -1;
	private long timePinned = -1;
	private long lsn;
	private static boolean direct = true;
	private boolean readDone;
	
	public Page()
	{
		try
		{
			if (direct)
			{
				this.contents = ByteBuffer.allocateDirect(BLOCK_SIZE);
			}
			else
			{
				this.contents = ByteBuffer.allocate(BLOCK_SIZE);
			}
		}
		catch (Throwable e)
		{
			direct = false;
			this.contents = ByteBuffer.allocate(BLOCK_SIZE);
		}
		pins = new HashMap<Long, Integer>();
		readDone = false;
	}
	
	public Block block()
	{
		return blk;
	}
	
	public synchronized void pin(long lsn, long txnum)
	{
		Integer numPins = pins.get(txnum);
		if (numPins == null)
		{
			pins.put(txnum, 1);
		}
		else
		{
			pins.put(txnum, numPins+1);
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
	
	public synchronized void unpin(long txnum)
	{
		pins.remove(txnum);
	}
	
	public synchronized boolean isPinned()
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
