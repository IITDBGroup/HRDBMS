package com.exascale.filesystem;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

public class CompressedRandomAccessFile
{
	private File file;
	
	public CompressedRandomAccessFile(File file, String access) throws Exception
	{
		if (!access.equals("rw"))
		{
			throw new Exception("Unsupported access mode: " + access);
		}
		
		this.file = file;
	}
	
	public FileChannel getChannel() throws IOException
	{
		return new CompressedFileChannel(file);
	}
}
