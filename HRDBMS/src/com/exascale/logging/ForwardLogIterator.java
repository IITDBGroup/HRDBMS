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
	long fcSize;

	public ForwardLogIterator(final String filename) throws IOException
	{
		nextpos = 4;
		fc = LogManager.getFile(filename);
		fcSize = fc.size();
	}

	public void close()
	{
	}

	@Override
	public boolean hasNext()
	{

		return nextpos < fcSize;
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
