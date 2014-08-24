package com.exascale.logging;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.managers.LogManager;

public class ForwardLogIterator implements Iterator<LogRec>
{
	private long nextpos;
	private final FileChannel fc;

	public ForwardLogIterator(String filename) throws IOException
	{
		nextpos = 4;
		fc = LogManager.getFile(filename);
	}

	public void close()
	{
	}

	@Override
	public boolean hasNext()
	{
		try
		{
			return nextpos < fc.size();
		}
		catch (final IOException e)
		{
			HRDBMSWorker.logger.error("Error getting file size in ForwardLogIterator.hasNext().", e);
			return false;
		}
	}

	@Override
	public LogRec next()
	{
		LogRec retval = null;
		try
		{
			fc.position(nextpos);
			retval = new LogRec(fc);
			nextpos = fc.position() + 8;
		}
		catch (final IOException e)
		{
			HRDBMSWorker.logger.error("Error reading log record in ForwardLogIterator.", e);
			return null;
		}

		return retval;
	}

	@Override
	public void remove()
	{
		throw new UnsupportedOperationException();
	}
}
