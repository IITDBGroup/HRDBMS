package com.exascale.logging;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.managers.ResourceManager;

public class ArchiveIterator implements Iterator<LogRec>
{
	private long nextpos;
	private final ByteBuffer sizeBuff = ByteBuffer.allocate(4);
	private FileChannel fc;
	private int size;
	private String fn;
	private final ArrayList<String> files;
	private int index = 0;
	private RandomAccessFile raf;

	public ArchiveIterator(String filename) throws IOException
	{
		this.fn = filename;
		String name = HRDBMSWorker.getHParms().getProperty("archive_dir");
		if (!name.endsWith("/"))
		{
			name += "/";
		}

		name += (fn.substring(fn.lastIndexOf('/') + 1) + "*.archive");
		this.fn = name;

		// find all archive files
		final ArrayList<Path> files = new ArrayList<Path>();
		int split = fn.lastIndexOf('/');
		String dir = fn.substring(0, split);
		String relative = fn.substring(split + 1);
		String firstPart = relative.substring(0, relative.indexOf('*'));
		File dirFile = new File(dir);
		File[] files3 = dirFile.listFiles();
		for (File f : files3)
		{
			String fStr = f.getName();
			if (fStr.startsWith(firstPart) && fStr.endsWith(".archive"))
			{
				files.add(f.toPath());
			}
		}

		ArrayList<String> files2 = new ArrayList<String>();
		for (Path file : files)
		{
			files2.add(file.toAbsolutePath().toString());
		}

		Collections.sort(files2);
		Collections.reverse(files2);

		this.files = files2;

		while (true)
		{
			try
			{
				raf = new RandomAccessFile(this.files.get(index), "r");
				break;
			}
			catch (FileNotFoundException e)
			{
				ResourceManager.panic = true;
				try
				{
					Thread.sleep(Integer.parseInt(HRDBMSWorker.getHParms().getProperty("rm_sleep_time_ms")) / 2);
				}
				catch (Exception f)
				{
				}
			}
		}

		fc = raf.getChannel();
		synchronized (fc)
		{
			try
			{
				fc.position(fc.size() - 4); // trailing log rec size
				sizeBuff.position(0);
				fc.read(sizeBuff);
				sizeBuff.position(0);
				size = sizeBuff.getInt();
				nextpos = fc.size() - 4 - size;
			}
			catch (final IllegalArgumentException e)
			{
				nextpos = -1;
			}
		}
	}

	@Override
	public boolean hasNext()
	{
		return nextpos > 0 || index < (files.size() - 1);
	}

	@Override
	public LogRec next()
	{
		if (nextpos <= 0)
		{
			try
			{
				// move to next file
				index++;
				fc.close();
				raf.close();

				while (true)
				{
					try
					{
						raf = new RandomAccessFile(this.files.get(index), "r");
						break;
					}
					catch (FileNotFoundException e)
					{
						ResourceManager.panic = true;
						try
						{
							Thread.sleep(Integer.parseInt(HRDBMSWorker.getHParms().getProperty("rm_sleep_time_ms")) / 2);
						}
						catch (Exception f)
						{
						}
					}
				}

				fc = raf.getChannel();
				synchronized (fc)
				{
					try
					{
						fc.position(fc.size() - 4); // trailing log rec size
						sizeBuff.position(0);
						fc.read(sizeBuff);
						sizeBuff.position(0);
						size = sizeBuff.getInt();
						nextpos = fc.size() - 4 - size;
					}
					catch (final IllegalArgumentException e)
					{
						nextpos = -1;
					}
				}
			}
			catch (Exception e)
			{
				return null;
			}
		}

		LogRec retval = null;
		try
		{
			synchronized (fc)
			{
				fc.position(nextpos);
				retval = new LogRec(fc);
				try
				{
					fc.position(nextpos - 8);
					sizeBuff.position(0);
					fc.read(sizeBuff);
					sizeBuff.position(0);
					size = sizeBuff.getInt();
					nextpos = fc.position() - 4 - size;
				}
				catch (final IllegalArgumentException e)
				{
					nextpos = -1;
				}
			}
		}
		catch (final IOException e)
		{
			HRDBMSWorker.logger.error("Exception occurred in LogIterator.next().", e);
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
