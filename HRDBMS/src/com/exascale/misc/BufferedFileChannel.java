package com.exascale.misc;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

/**
 * Puts a buffer in front of a {@link ReadableByteChannel} so that even small
 * reads, byte/int/long will be fast.
 */
public class BufferedFileChannel extends FileChannel
{
	private final FileChannel source;
	private byte[] intermediaryBuffer = new byte[1024 * 64];
	private int intermediaryBufferSize;
	private int intermediaryBufferPosition;

	public BufferedFileChannel(final FileChannel source) throws IOException
	{
		this.source = source;
		intermediaryBufferPosition = 0;
		intermediaryBufferSize = 0;
	}

	public BufferedFileChannel(final FileChannel source, final int size) throws IOException
	{
		this.source = source;
		intermediaryBufferPosition = 0;
		intermediaryBufferSize = 0;
		intermediaryBuffer = new byte[size];
	}

	@Override
	public void force(final boolean metaData) throws IOException
	{
		source.force(metaData);
	}

	public FileChannel getSource()
	{
		return source;
	}

	@Override
	public FileLock lock(final long position, final long size, final boolean shared) throws IOException
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public MappedByteBuffer map(final MapMode mode, final long position, final long size) throws IOException
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public long position() throws IOException
	{
		return source.position() - intermediaryBufferSize + intermediaryBufferPosition;
	}

	@Override
	public FileChannel position(final long newPosition) throws IOException
	{
		final long bufferEndPosition = source.position();
		final long bufferStartPosition = bufferEndPosition - intermediaryBufferSize;
		if (newPosition >= bufferStartPosition && newPosition <= bufferEndPosition)
		{
			// Only an optimization
			final long diff = newPosition - position();
			intermediaryBufferPosition += diff;
		}
		else
		{
			source.position(newPosition);
			fillUpIntermediaryBuffer();
		}
		return this;
	}

	@Override
	public int read(final ByteBuffer dst) throws IOException
	{
		int read = 0;
		while (read < dst.limit())
		{
			read += readAsMuchAsPossibleFromIntermediaryBuffer(dst);
			if (read < dst.limit())
			{
				if (fillUpIntermediaryBuffer() == -1)
				{
					break;
				}
			}
		}
		final int retval = (read == 0 && dst.limit() > 0 ? -1 : read);
		if (retval == -1)
		{
			intermediaryBuffer = null;
		}

		return retval;
	}

	@Override
	public int read(final ByteBuffer dst, final long position) throws IOException
	{
		throw new UnsupportedOperationException();
	}

	public int read(final ByteBuffer dst, final Object lock) throws IOException
	{
		int read = 0;
		while (read < dst.limit())
		{
			read += readAsMuchAsPossibleFromIntermediaryBuffer(dst);
			if (read < dst.limit())
			{
				if (fillUpIntermediaryBuffer(lock) == -1)
				{
					break;
				}
			}
		}
		final int retval = (read == 0 && dst.limit() > 0 ? -1 : read);
		if (retval == -1)
		{
			intermediaryBuffer = null;
		}

		return retval;
	}

	@Override
	public long read(final ByteBuffer[] dsts, final int offset, final int length) throws IOException
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public long size() throws IOException
	{
		return source.size();
	}

	@Override
	public long transferFrom(final ReadableByteChannel src, final long position, final long count) throws IOException
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public long transferTo(final long position, final long count, final WritableByteChannel target) throws IOException
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public FileChannel truncate(final long size) throws IOException
	{
		source.truncate(size);
		return this;
	}

	@Override
	public FileLock tryLock(final long position, final long size, final boolean shared) throws IOException
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public int write(final ByteBuffer src) throws IOException
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public int write(final ByteBuffer src, final long position) throws IOException
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public long write(final ByteBuffer[] srcs, final int offset, final int length) throws IOException
	{
		throw new UnsupportedOperationException();
	}

	private int fillUpIntermediaryBuffer() throws IOException
	{
		final int result = source.read(ByteBuffer.wrap(intermediaryBuffer));
		intermediaryBufferPosition = 0;
		intermediaryBufferSize = result == -1 ? 0 : result;
		return result;
	}

	private int fillUpIntermediaryBuffer(final Object lock) throws IOException
	{
		int result = 0;
		synchronized (lock)
		{
			result = source.read(ByteBuffer.wrap(intermediaryBuffer));
		}
		intermediaryBufferPosition = 0;
		intermediaryBufferSize = result == -1 ? 0 : result;
		return result;
	}

	private int readAsMuchAsPossibleFromIntermediaryBuffer(final ByteBuffer dst)
	{
		final int howMuchToRead = Math.min(dst.remaining(), remainingInIntermediaryBuffer());
		dst.put(intermediaryBuffer, intermediaryBufferPosition, howMuchToRead);
		intermediaryBufferPosition += howMuchToRead;
		return howMuchToRead;
	}

	private int remainingInIntermediaryBuffer()
	{
		return intermediaryBufferSize - intermediaryBufferPosition;
	}

	@Override
	protected void implCloseChannel() throws IOException
	{
		source.close();
	}
}