package com.exascale;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Iterator;

public class LogIterator implements Iterator<LogRec>
{
	private String filename;
	private long nextpos;
	private ByteBuffer sizeBuff = ByteBuffer.allocate(4);
	private FileChannel fc;
	private int size;
	
	public LogIterator(String filename) throws IOException
	{
		this.filename = filename;
		synchronized(LogManager.noArchive)
		{
			LogManager.openIters++;
			LogManager.noArchive = true;
		}
	
		LogManager.flush(LogManager.logs.get(filename).getLast().lsn(), filename);
		
		fc = LogManager.getFile(filename);
		synchronized(fc)
		{
			try
			{
				fc.position(fc.size() - 4);
			}
			catch(IllegalArgumentException e)
			{
				nextpos = -1;
			}
			fc.read(sizeBuff);
			size = sizeBuff.getInt();
			nextpos = fc.size() - 4 - size;
		}
	}
	
	public boolean hasNext()
	{
		return nextpos > 0;
	}
	
	public LogRec next()
	{
		LogRec retval = null;
		try
		{
			synchronized(fc)
			{
				fc.position(nextpos);
				retval = new LogRec(fc);
				fc.position(fc.position() - 8);
				fc.read(sizeBuff);
				size = sizeBuff.getInt();
				nextpos = fc.position() - 4- size;
			}
		}
		catch(IOException e)
		{
			e.printStackTrace(System.err);
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
		synchronized(LogManager.noArchive)
		{
			LogManager.openIters--;
			
			if (LogManager.openIters == 0)
			{
				LogManager.noArchive = false;
			}
		}
	}
}
