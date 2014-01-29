package com.exascale.logging;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Iterator;

import com.exascale.managers.HRDBMSWorker;
import com.exascale.managers.LogManager;

public class ForwardLogIterator implements Iterator<LogRec>
{
	protected String filename;
	protected long nextpos;
	protected ByteBuffer sizeBuff = ByteBuffer.allocate(4);
	protected FileChannel fc;
	protected int size;
	
	public ForwardLogIterator(String filename) throws IOException
	{
		this.filename = filename;
		nextpos = 4;
		fc = LogManager.getFile(filename);
	}
	
	public boolean hasNext()
	{
		try
		{
			return nextpos < fc.size();
		}
		catch(IOException e)
		{
			HRDBMSWorker.logger.error("Error getting file size in ForwardLogIterator.hasNext().", e);
			return false;
		}
	}
	
	public LogRec next()
	{
		LogRec retval = null;
		try
		{
			fc.position(nextpos);
			retval = new LogRec(fc);
			nextpos = fc.position() + 8;
		}
		catch(IOException e)
		{
			HRDBMSWorker.logger.error("Error reading log record in ForwardLogIterator.", e);
			return null;
		}
		
		return retval;
	}
	
	public void remove()
	{
		throw new UnsupportedOperationException();
	}
	
	public void close()
	{
	}
}
