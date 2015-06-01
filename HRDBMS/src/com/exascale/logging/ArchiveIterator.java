package com.exascale.logging;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import com.exascale.managers.HRDBMSWorker;

public class ArchiveIterator implements Iterator<LogRec>
{
	private long nextpos;
	private final ByteBuffer sizeBuff = ByteBuffer.allocate(4);
	private FileChannel fc;
	private int size;
	private String fn;
	private ArrayList<String> files;
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
		final PathMatcher matcher = FileSystems.getDefault().getPathMatcher("glob:" + relative);
		Files.walkFileTree(Paths.get(dir), new SimpleFileVisitor<Path>() {
			@Override
			public FileVisitResult visitFile(Path file, java.nio.file.attribute.BasicFileAttributes attrs) throws IOException
			{
				if (matcher.matches(file.getFileName()))
				{
					files.add(file);
				}
				return FileVisitResult.CONTINUE;
			}

			@Override
			public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException
			{
				return FileVisitResult.CONTINUE;
			}
		});

		ArrayList<String> files2 = new ArrayList<String>();
		for (Path file : files)
		{
			files2.add(file.toAbsolutePath().toString());
		}

		Collections.sort(files2);
		Collections.reverse(files2);

		this.files = files2;

		raf = new RandomAccessFile(this.files.get(index), "r");
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

				raf = new RandomAccessFile(this.files.get(index), "r");
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
