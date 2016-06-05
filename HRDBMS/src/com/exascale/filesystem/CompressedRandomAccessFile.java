package com.exascale.filesystem;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;

public class CompressedRandomAccessFile
{
	private final File file;
	private int suffix = -1;

	public CompressedRandomAccessFile(File file, String access) throws Exception
	{
		if (!access.equals("rw"))
		{
			throw new Exception("Unsupported access mode: " + access);
		}

		this.file = file;
	}

	public CompressedRandomAccessFile(File file, String access, int suffix) throws Exception
	{
		if (!access.equals("rw"))
		{
			throw new Exception("Unsupported access mode: " + access);
		}

		this.file = file;
		this.suffix = suffix;
	}

	public FileChannel getChannel() throws IOException
	{
		if (suffix == -1)
		{
			return new SparseCompressedFileChannel2(file);
		}
		else
		{
			return new SparseCompressedFileChannel2(file, suffix);
		}
	}
}
