package com.exascale;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Iterator;

public class ForwardLogIterator implements Iterator<LogRec>
{
	private String filename;
	private long nextpos;
	private ByteBuffer sizeBuff = ByteBuffer.allocate(4);
	private FileChannel fc;
	private int size;
	
	public ForwardLogIterator(String filename) throws IOException
	{
		this.filename = filename;
		nextpos = 4;
	}
	
	public boolean hasNext()
	{
		try
		{
			return nextpos < fc.size();
		}
		catch(IOException e)
		{
			e.printStackTrace(System.err);
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
			fc.position(nextpos - 4);
			fc.read(sizeBuff);
			size = sizeBuff.getInt();
			nextpos = fc.position() + size;
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
	}
}
