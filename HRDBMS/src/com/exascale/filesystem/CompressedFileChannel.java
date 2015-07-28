package com.exascale.filesystem;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import net.jpountz.lz4.LZ4SafeDecompressor;
import com.exascale.managers.BufferManager;
import com.exascale.managers.FileManager;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.managers.ResourceManager;
import com.exascale.misc.ScalableStampedRWLock;
import com.exascale.misc.ScalableStampedReentrantRWLock;
import com.exascale.threads.HRDBMSThread;

public class CompressedFileChannel extends FileChannel
{
	private static LZ4Factory factory;
	public static final int MAX_FCS = 1024;
	static
	{
		factory = LZ4Factory.nativeInstance();
	}
	private String fn;
	private ConcurrentHashMap<Integer, FileChannel> fcs = new ConcurrentHashMap<Integer, FileChannel>(MAX_FCS, 0.75f, 6 * ResourceManager.cpus);
	private ConcurrentHashMap<Integer, Boolean> modded = new ConcurrentHashMap<Integer, Boolean>(MAX_FCS, 0.75f, 6 * ResourceManager.cpus);
	private int length; // in 128k pages
	private int bufferedBlock = -1;
	private byte[] buffer;
	private int bufferSize = 0;
	private long pos = 0;
	private final ScalableStampedReentrantRWLock lock = new ScalableStampedReentrantRWLock();
	private final ScalableStampedRWLock bufferLock = new ScalableStampedRWLock();
	private final HashMap<Integer, Integer>[] inUse = new HashMap[32];
	private final AtomicBoolean trimInProgress = new AtomicBoolean(false);
	private AtomicInteger ai = new AtomicInteger(0);

	private final ConcurrentHashMap<Integer, Integer> writeLocks = new ConcurrentHashMap<Integer, Integer>(MAX_FCS, 0.75f, ResourceManager.cpus);

	public CompressedFileChannel(File file) throws IOException
	{
		int i = 0;
		while (i < 32)
		{
			inUse[i] = new HashMap<Integer, Integer>();
			i++;
		}

		this.fn = file.getAbsolutePath();
		int high = -1;
		int split = this.fn.lastIndexOf('/');
		String dir = this.fn.substring(0, split);
		String relative = this.fn.substring(split + 1);

		final ArrayList<Path> files = new ArrayList<Path>();
		final PathMatcher matcher = FileSystems.getDefault().getPathMatcher("glob:" + relative + ".*");
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

		for (Path file2 : files)
		{
			String s = file2.toString().substring(file2.toString().lastIndexOf('.') + 1);
			int suffix = Integer.parseInt(s);
			if (suffix > high)
			{
				high = suffix;
			}
		}

		if (high == -1)
		{
			length = 0;
			// HRDBMSWorker.logger.debug("Opened " + fn + " with length =  0");
			return;
		}

		length = high + 1; // length in 3 page blocks
		LZ4SafeDecompressor decomp = factory.safeDecompressor();
		FileChannel fc = getFC(high);
		ByteBuffer bb = ByteBuffer.allocate((int)fc.size());
		fc.read(bb, 0);
		byte[] target = new byte[128 * 1024 * 3]; // max 3 pages
		int bytes = decomp.decompress(bb.array(), 0, (int)fc.size(), target, 0, 128 * 1024 * 3);
		length *= 3;
		if (bytes == 128 * 1024)
		{
			length -= 2;
		}
		else if (bytes == 128 * 1024 * 2)
		{
			length -= 1;
		}

		// HRDBMSWorker.logger.debug("Opened " + fn + " with length = " +
		// length);
	}

	public CompressedFileChannel(File file, int suffix) throws IOException
	{
		int i = 0;
		while (i < 32)
		{
			inUse[i] = new HashMap<Integer, Integer>();
			i++;
		}

		this.fn = file.getAbsolutePath();
		int high = suffix;

		if (high == -1)
		{
			length = 0;
			// HRDBMSWorker.logger.debug("Opened " + fn + " with length =  0");
			return;
		}

		length = high + 1; // length in 3 page blocks
		LZ4SafeDecompressor decomp = factory.safeDecompressor();
		FileChannel fc = getFC(high);
		ByteBuffer bb = ByteBuffer.allocate((int)fc.size());
		fc.read(bb, 0);
		byte[] target = new byte[128 * 1024 * 3]; // max 3 pages
		int bytes = decomp.decompress(bb.array(), 0, (int)fc.size(), target, 0, 128 * 1024 * 3);
		length *= 3;
		if (bytes == 128 * 1024)
		{
			length -= 2;
		}
		else if (bytes == 128 * 1024 * 2)
		{
			length -= 1;
		}
	}

	public void copyFromFC(CompressedFileChannel source) throws Exception
	{
		lock.writeLock().lock();
		try
		{
			truncate(0);
			BufferManager.invalidateFile(fn);
			long size = source.size();
			long offset = 0;
			ByteBuffer bb = ByteBuffer.allocate(128 * 1024);
			while (offset < size)
			{
				bb.position(0);
				source.read(bb, offset);
				bb.position(0);
				write(bb, offset);
				offset += bb.capacity();
			}

			force(false);
			FileManager.numBlocks.put(fn, (int)(offset / (128 * 1024)));
			FileManager.removeFile(source.fn);
			BufferManager.invalidateFile(source.fn);
		}
		catch (Exception e)
		{
			lock.writeLock().unlock();
			HRDBMSWorker.logger.debug("", e);
			throw e;
		}
		lock.writeLock().unlock();
	}

	@Override
	public void force(boolean arg0) throws IOException
	{
		lock.writeLock().lock();
		try
		{
			for (Map.Entry entry : modded.entrySet())
			{
				if (((Boolean)entry.getValue()))
				{
					int block = (Integer)entry.getKey();
					FileChannel fc = getFC(block);
					try
					{
						fc.force(false);
					}
					catch (ClosedChannelException e)
					{
					}

					modded.remove(entry.getKey());
				}
			}
		}
		catch (Exception e)
		{
			lock.writeLock().unlock();
			throw e;
		}

		lock.writeLock().unlock();
	}

	@Override
	public FileLock lock(long arg0, long arg1, boolean arg2) throws IOException
	{
		throw new IOException("Unsupported operation");
	}

	@Override
	public MappedByteBuffer map(MapMode arg0, long arg1, long arg2) throws IOException
	{
		throw new IOException("Unsupported operation");
	}

	@Override
	public long position() throws IOException
	{
		throw new IOException("Unsupported operation");
	}

	@Override
	public FileChannel position(long arg0) throws IOException
	{
		throw new IOException("Unsupported operation");
	}

	@Override
	public int read(ByteBuffer arg0) throws IOException
	{
		throw new IOException("Unsupported operation");
	}

	@Override
	public int read(ByteBuffer arg0, long arg1) throws IOException
	{
		// if (arg0.capacity() != 128 * 1024)
		// {
		// throw new IOException("Invalid read length of " + arg0.capacity());
		// }

		boolean closed = false;
		int page = (int)(arg1 >> 17);
		int offset = (int)(arg1 & (128 * 1024 - 1));
		if (offset != 0)
		{
			closed = true;
			HRDBMSWorker.logger.debug("Invalid read of offset " + offset);
			throw new IOException("Invalid read offset of " + offset);
		}
		int block = page / 3;
		lock.readLock().lock();
		try
		{
			while (writeLocks.putIfAbsent(block, block) != null)
			{
				Thread.yield();
			}
			offset += ((page % 3) * 128 * 1024);

			// bufferLock.readLock().lock();
			if (bufferLock.readLock().tryLock())
			{
				if (bufferedBlock == block)
				{
					System.arraycopy(buffer, offset, arg0.array(), 0, arg0.capacity());
					bufferLock.readLock().unlock();
					writeLocks.remove(block);
					lock.readLock().unlock();
					return arg0.capacity();
				}
				bufferLock.readLock().unlock();
			}

			int actualBlocks = length / 3;
			if (length % 3 != 0)
			{
				actualBlocks++;
			}
			int highFileNum = actualBlocks - 1;
			if (highFileNum == block)
			{
				// may be partial block
				LZ4SafeDecompressor decomp = factory.safeDecompressor();
				int mod = block & 31;
				synchronized (inUse[mod])
				{
					Integer count = inUse[mod].get(block);
					if (count == null)
					{
						inUse[mod].put(block, 1);
					}
					else
					{
						inUse[mod].put(block, count + 1);
					}
				}
				FileChannel fc = getFC(block);
				ByteBuffer bb = ByteBuffer.allocate((int)fc.size());
				while (fc.read(bb, 0) <= 0)
				{}
				int size = (int)fc.size();
				synchronized (inUse[mod])
				{
					Integer count = inUse[mod].get(block);
					if (count == 1)
					{
						inUse[mod].remove(block);
					}
					else
					{
						inUse[mod].put(block, count - 1);
					}
				}
				byte[] target = new byte[128 * 1024 * 3]; // max 3 pages
				int bytes = decomp.decompress(bb.array(), 0, size, target, 0, 128 * 1024 * 3);
				System.arraycopy(target, offset, arg0.array(), 0, arg0.capacity());
				if (bytes == 3 * 128 * 1024)
				{
					if (bufferLock.writeLock().tryLock())
					{
						bufferedBlock = block;
						buffer = target;
						bufferSize = 3;
						bufferLock.writeLock().unlock();
					}
				}
			}
			else
			{
				// full block
				LZ4FastDecompressor decomp = factory.fastDecompressor();
				int mod = block & 31;
				synchronized (inUse[mod])
				{
					Integer count = inUse[mod].get(block);
					if (count == null)
					{
						inUse[mod].put(block, 1);
					}
					else
					{
						inUse[mod].put(block, count + 1);
					}
				}
				FileChannel fc = getFC(block);
				ByteBuffer bb = ByteBuffer.allocate((int)(fc.size()));
				while (fc.read(bb, 0) <= 0)
				{}
				synchronized (inUse[mod])
				{
					Integer count = inUse[mod].get(block);
					if (count == 1)
					{
						inUse[mod].remove(block);
					}
					else
					{
						inUse[mod].put(block, count - 1);
					}
				}
				byte[] target = new byte[128 * 1024 * 3]; // 3 pages
				try
				{
					decomp.decompress(bb.array(), target);
				}
				catch (Exception e)
				{
					HRDBMSWorker.logger.debug("Block = " + block);
					HRDBMSWorker.logger.debug("FC = " + fc);
					HRDBMSWorker.logger.debug("", e);
					// HRDBMSWorker.logger.debug("FC.size() = " + fc.size());
					writeLocks.remove(block);
					lock.readLock().unlock();
					closed = true;
					throw e;
				}
				System.arraycopy(target, offset, arg0.array(), 0, arg0.capacity());
				if (bufferLock.writeLock().tryLock())
				{
					bufferedBlock = block;
					buffer = target;
					bufferSize = 3;
					bufferLock.writeLock().unlock();
				}
			}
		}
		catch (Exception e)
		{
			if (!closed)
			{
				writeLocks.remove(block);
				lock.readLock().unlock();
			}

			HRDBMSWorker.logger.debug("", e);
			throw e;
		}

		writeLocks.remove(block);
		lock.readLock().unlock();
		return arg0.capacity();
	}

	@Override
	public long read(ByteBuffer[] arg0, int arg1, int arg2) throws IOException
	{
		throw new IOException("Unsupported operation");
	}

	@Override
	public long size() throws IOException
	{
		lock.readLock().lock();
		long retval = length * 128L * 1024L;
		lock.readLock().unlock();
		return retval;
	}

	@Override
	public long transferFrom(ReadableByteChannel arg0, long arg1, long arg2) throws IOException
	{
		throw new IOException("Unsupported operation");
	}

	@Override
	public long transferTo(long arg0, long arg1, WritableByteChannel arg2) throws IOException
	{
		throw new IOException("Unsupported operation");
	}

	@Override
	public FileChannel truncate(long arg0) throws IOException
	{
		lock.writeLock().lock();
		try
		{
			pos = arg0;
			int desiredPages = (int)(arg0 / (128 * 1024));
			int desiredBlocks = desiredPages / 3;

			int actualBlocks = length / 3;
			if (desiredPages % 3 != 0)
			{
				desiredBlocks++;
			}
			if (length % 3 != 0)
			{
				actualBlocks++;
			}

			int desiredHighFileNum = desiredBlocks - 1;
			int actualHighFileNum = actualBlocks - 1;
			deleteFiles(desiredHighFileNum);

			if (desiredPages % 3 != 0)
			{
				// partial last block
				FileChannel fc = getFC(actualHighFileNum);
				LZ4SafeDecompressor decomp = factory.safeDecompressor();
				ByteBuffer bb = ByteBuffer.allocate((int)fc.size());
				fc.read(bb, 0);
				byte[] target = new byte[128 * 1024 * 3]; // max 3 pages
				decomp.decompress(bb.array(), 0, (int)fc.size(), target, 0, 128 * 1024 * 3);
				byte[] trunced = new byte[(desiredPages % 3) * 128 * 1024];
				System.arraycopy(target, 0, trunced, 0, trunced.length);
				fc.truncate(0);
				// LZ4Compressor comp = factory.highCompressor();
				LZ4Compressor comp = factory.fastCompressor();
				byte[] comped = comp.compress(trunced);
				bb = ByteBuffer.wrap(comped);
				fc.write(bb, 0);
				fc.force(false);

				if (bufferedBlock == actualHighFileNum)
				{
					bufferedBlock = -1;
				}
			}

			length = (int)(arg0 / (128 * 1024));
		}
		catch (Exception e)
		{
			lock.writeLock().unlock();
			HRDBMSWorker.logger.debug("", e);
			throw e;
		}
		lock.writeLock().unlock();
		return this;
	}

	@Override
	public FileLock tryLock(long arg0, long arg1, boolean arg2) throws IOException
	{
		throw new IOException("Unsupported operation");
	}

	@Override
	public int write(ByteBuffer arg0) throws IOException
	{
		// lock.writeLock().lock();
		try
		{
			write(arg0, pos);
			pos += arg0.capacity();
		}
		catch (Exception e)
		{
			// lock.writeLock().unlock();
			HRDBMSWorker.logger.debug("", e);
			throw e;
		}
		// lock.writeLock().unlock();
		return arg0.capacity();
	}

	@Override
	public int write(ByteBuffer arg0, long arg1) throws IOException
	{
		// multi-page write
		if (arg0.capacity() > (128 * 1024))
		{
			return multiPageWrite(arg0, arg1);
		}

		// read if not buffered
		boolean closed = false;
		int page = (int)(arg1 >> 17);
		int offset = (int)(arg1 & (128 * 1024 - 1));
		int block = page / 3;

		// HRDBMSWorker.logger.debug("Trying to write to page = " + page +
		// " with current length = " + length); //DEBUG
		offset += ((page % 3) * 128 * 1024);
		lock.readLock().lock();
		int actualBlocks = length / 3;
		if (length % 3 != 0)
		{
			actualBlocks++;
		}
		int highFileNum = actualBlocks - 1;
		if (block < highFileNum)
		{
			// lock.readLock().lock();
			while (writeLocks.putIfAbsent(block, block) != null)
			{
				Thread.yield();
			}
			// lock.writeLock().unlock();
			bufferLock.readLock().lock();
			if (bufferedBlock == block)
			{
				// HRDBMSWorker.logger.debug("The block was buffered"); //DEBUG
				System.arraycopy(arg0.array(), 0, buffer, offset, arg0.capacity());

				// compress it
				// LZ4Compressor comp = factory.highCompressor();
				LZ4Compressor comp = factory.fastCompressor();
				// write it
				int mod = block & 31;
				synchronized (inUse[mod])
				{
					Integer count = inUse[mod].get(block);
					if (count == null)
					{
						inUse[mod].put(block, 1);
					}
					else
					{
						inUse[mod].put(block, count + 1);
					}
				}
				FileChannel fc = getFC(block);
				fc.truncate(0);
				byte[] data = comp.compress(buffer);
				bufferLock.readLock().unlock();
				ByteBuffer bb = ByteBuffer.wrap(data);
				fc.write(bb, 0);
				synchronized (inUse[mod])
				{
					Integer count = inUse[mod].get(block);
					if (count == 1)
					{
						inUse[mod].remove(block);
					}
					else
					{
						inUse[mod].put(block, count - 1);
					}
				}
				// mark modded
				modded.put(block, true);
				// if (modded.size() > 1024)
				// {
				// flushModded();
				// }
				writeLocks.remove(block);
				lock.readLock().unlock();
				return arg0.capacity();
			}
			else
			{
				bufferLock.readLock().unlock();
				// full block
				// HRDBMSWorker.logger.debug("The write is in the middle of an existing full block");
				// //DEBUG
				LZ4FastDecompressor decomp = factory.fastDecompressor();
				int mod = block & 31;
				synchronized (inUse[mod])
				{
					Integer count = inUse[mod].get(block);
					if (count == null)
					{
						inUse[mod].put(block, 1);
					}
					else
					{
						inUse[mod].put(block, count + 1);
					}
				}
				FileChannel fc = getFC(block);
				ByteBuffer bb = ByteBuffer.allocate((int)(fc.size()));
				fc.read(bb, 0);
				byte[] target = new byte[128 * 1024 * 3]; // 3 pages
				try
				{
					decomp.decompress(bb.array(), target);
				}
				catch (Exception e)
				{
					HRDBMSWorker.logger.debug("", e);
					HRDBMSWorker.logger.debug("We tried to read a full block from " + fn + "." + block + ", which contained " + fc.size() + " bytes");
					HRDBMSWorker.logger.debug("Length is currently " + length);
					HRDBMSWorker.logger.debug("HighFileNum = " + highFileNum);
					writeLocks.remove(block);
					lock.readLock().unlock();
					closed = true;
					throw e;
				}
				System.arraycopy(arg0.array(), 0, target, offset, arg0.capacity());
				if (bufferLock.writeLock().tryLock())
				{
					bufferedBlock = block;
					buffer = target;
					bufferSize = 3;
					bufferLock.writeLock().unlock();
				}
				// compress it
				// LZ4Compressor comp = factory.highCompressor();
				LZ4Compressor comp = factory.fastCompressor();
				// write it
				fc.truncate(0);
				byte[] data = comp.compress(target);
				bb = ByteBuffer.wrap(data);
				fc.write(bb, 0);
				synchronized (inUse[mod])
				{
					Integer count = inUse[mod].get(block);
					if (count == 1)
					{
						inUse[mod].remove(block);
					}
					else
					{
						inUse[mod].put(block, count - 1);
					}
				}
				// mark modded
				modded.put(block, true);
				// if (modded.size() > 1024)
				// {
				// flushModded();
				// }
				writeLocks.remove(block);
				lock.readLock().unlock();
				return arg0.capacity();
			}
		}

		try
		{
			lock.readLock().unlock();
			lock.writeLock().lock();
			actualBlocks = length / 3;
			if (length % 3 != 0)
			{
				actualBlocks++;
			}
			highFileNum = actualBlocks - 1;
			if (block == bufferedBlock)
			{
				// HRDBMSWorker.logger.debug("The block was buffered"); //DEBUG
				System.arraycopy(arg0.array(), 0, buffer, offset, arg0.capacity());
				if (offset == 128 * 1024 && bufferSize < 2)
				{
					bufferSize = 2;
					length += 1;
					// HRDBMSWorker.logger.debug("The buffered block was extended by 1 page");
					// //DEBUG
				}
				else if (offset == 2 * 128 * 1024 && bufferSize < 3)
				{
					length += (3 - bufferSize);
					// HRDBMSWorker.logger.debug("The buffered block was extended by "
					// + (3 - bufferSize) + " pages"); //DEBUG
					bufferSize = 3;
				}

				// if it's a full block
				if (bufferSize == 3)
				{
					// compress it
					// LZ4Compressor comp = factory.highCompressor();
					LZ4Compressor comp = factory.fastCompressor();
					// write it
					FileChannel fc = getFC(block);
					fc.truncate(0);
					byte[] data = comp.compress(buffer);
					ByteBuffer bb = ByteBuffer.wrap(data);
					fc.write(bb, 0);
					// mark modded
					modded.put(block, true);
					// if (modded.size() > 1024)
					// {
					// flushModded();
					// }
					lock.writeLock().unlock();
					return arg0.capacity();
				}
				else
				{
					// figure out what of it is valid
					byte[] valid = new byte[128 * 1024 * bufferSize];
					System.arraycopy(buffer, 0, valid, 0, valid.length);

					// compress it
					// LZ4Compressor comp = factory.highCompressor();
					LZ4Compressor comp = factory.fastCompressor();
					// write it
					FileChannel fc = getFC(block);
					fc.truncate(0);
					byte[] data = comp.compress(valid);
					ByteBuffer bb = ByteBuffer.wrap(data);
					fc.write(bb, 0);
					// mark modded
					modded.put(block, true);
					// if (modded.size() > 1024)
					// {
					// flushModded();
					// }
					lock.writeLock().unlock();
					return arg0.capacity();
				}
			}

			if (block > highFileNum)
			{
				if (highFileNum >= 0)
				{
					// if last block is not full, fill it out
					// HRDBMSWorker.logger.debug("We have a write that is past the last block of the file");
					// //DEBUG
					FileChannel fc3 = getFC(highFileNum);
					LZ4SafeDecompressor decomp = factory.safeDecompressor();
					ByteBuffer bb2 = ByteBuffer.allocate((int)fc3.size());
					fc3.read(bb2, 0);
					byte[] target = new byte[128 * 1024 * 3]; // max 3 pages
					int bytes = -1;
					try
					{
						bytes = decomp.decompress(bb2.array(), 0, (int)fc3.size(), target, 0, 128 * 1024 * 3);
					}
					catch (Exception e)
					{
						HRDBMSWorker.logger.debug("", e);
						HRDBMSWorker.logger.debug("We tried to read from " + fn + "." + highFileNum + ", which contained " + fc3.size() + " bytes");
						HRDBMSWorker.logger.debug("Length is currently " + length);
						HRDBMSWorker.logger.debug("HighFileNum = " + highFileNum);
						lock.writeLock().unlock();
						closed = true;
						throw e;
					}

					if (bytes < (3 * 128 * 1024))
					{
						// HRDBMSWorker.logger.debug("The last block of the file needs to be filled by adding "
						// + ((3 * 128 * 1024 - bytes) / (128 * 1024)) +
						// " pages");
						// //DEBUG
						byte[] fullBlock = new byte[3 * 128 * 1024];
						System.arraycopy(target, 0, fullBlock, 0, target.length);

						length += ((3 * 128 * 1024 - bytes) / (128 * 1024));
						fc3.truncate(0);
						// LZ4Compressor comp = factory.highCompressor();
						LZ4Compressor comp = factory.fastCompressor();
						byte[] data = comp.compress(fullBlock);
						ByteBuffer bb = ByteBuffer.wrap(data);
						fc3.write(bb, 0);
						if (bufferedBlock == highFileNum)
						{
							buffer = fullBlock;
							bufferSize = 3;
						}
						modded.put(highFileNum, true);
						// if (modded.size() > 1024)
						// {
						// flushModded();
						// }
					}
				}

				// new block
				// LZ4Compressor comp = factory.highCompressor();
				LZ4Compressor comp = factory.fastCompressor();
				if (block > highFileNum + 1)
				{
					byte[] blank = new byte[3 * 128 * 1024];
					// write it
					byte[] data = comp.compress(blank);
					while (block > highFileNum + 1)
					{
						// HRDBMSWorker.logger.debug("Need to write a new empty block. BlockNum = "
						// + highFileNum + 1); //DEBUG
						FileChannel fc2 = getFC(highFileNum + 1);
						ByteBuffer bb = ByteBuffer.wrap(data);
						fc2.write(bb, 0);
						// mark modded
						modded.put(highFileNum + 1, true);
						// if (modded.size() > 1024)
						// {
						// flushModded();
						// }

						highFileNum++;
						length += 3;
					}
				}

				// need to write to new block (1 of the 3 slots)
				if (offset == 0)
				{
					byte[] data = comp.compress(arg0.array());
					ByteBuffer bb = ByteBuffer.wrap(data);
					FileChannel fc = getFC(block);
					fc.write(bb, 0);
					modded.put(block, true);
					// if (modded.size() > 1024)
					// {
					// flushModded();
					// }

					// HRDBMSWorker.logger.debug("Writing a new block with only 1 page");
					// //DEBUG
					length++;
				}
				else if (offset == 128 * 1024)
				{
					byte[] temp = new byte[2 * 128 * 1024];
					System.arraycopy(arg0.array(), 0, temp, 128 * 1024, 128 * 1024);
					byte[] data = comp.compress(temp);
					ByteBuffer bb = ByteBuffer.wrap(data);
					FileChannel fc = getFC(block);
					fc.write(bb, 0);
					modded.put(block, true);
					// if (modded.size() > 1024)
					// {
					// flushModded();
					// }

					// HRDBMSWorker.logger.debug("Writing a new block with only 2 pages");
					// //DEBUG
					length += 2;
				}
				else
				{
					byte[] temp = new byte[3 * 128 * 1024];
					System.arraycopy(arg0.array(), 0, temp, 2 * 128 * 1024, 128 * 1024);
					byte[] data = comp.compress(temp);
					ByteBuffer bb = ByteBuffer.wrap(data);
					FileChannel fc = getFC(block);
					fc.write(bb, 0);
					modded.put(block, true);
					// if (modded.size() > 1024)
					// {
					// flushModded();
					// }

					// HRDBMSWorker.logger.debug("Writing a full new block");
					// //DEBUG
					length += 3;
				}

				lock.writeLock().unlock();
				return arg0.capacity();
			}
			else
			{
				// may be partial block
				// HRDBMSWorker.logger.debug("The write is taking place on the last block");
				// //DEBUG
				LZ4SafeDecompressor decomp = factory.safeDecompressor();
				FileChannel fc = getFC(block);
				ByteBuffer bb = ByteBuffer.allocate((int)fc.size());
				fc.read(bb, 0);
				byte[] target = new byte[128 * 1024 * 3]; // max 3 pages
				int bytes = decomp.decompress(bb.array(), 0, (int)fc.size(), target, 0, 128 * 1024 * 3);
				System.arraycopy(arg0.array(), 0, target, offset, arg0.capacity());
				bufferedBlock = block;
				buffer = target;
				bufferSize = Math.max(bytes / (128 * 1024), (offset + 128 * 1024) / (128 * 1024));
				if (bufferSize != (bytes / (128 * 1024)))
				{
					length += (bufferSize - (bytes / (128 * 1024)));
					// HRDBMSWorker.logger.debug("The last block is being extended by "
					// + (bufferSize - (bytes / (128 * 1024))) + " pages");
					// //DEBUG
				}
				byte[] valid = new byte[128 * 1024 * bufferSize];
				System.arraycopy(buffer, 0, valid, 0, valid.length);

				// compress it
				// LZ4Compressor comp = factory.highCompressor();
				LZ4Compressor comp = factory.fastCompressor();
				// write it
				fc.truncate(0);
				byte[] data = comp.compress(valid);
				bb = ByteBuffer.wrap(data);
				fc.write(bb, 0);
				// mark modded
				modded.put(block, true);
				// if (modded.size() > 1024)
				// {
				// flushModded();
				// }
				lock.writeLock().unlock();
				return arg0.capacity();
			}
		}
		catch (Exception e)
		{
			HRDBMSWorker.logger.debug("", e);
			if (!closed)
			{
				lock.writeLock().unlock();
			}

			throw e;
		}
	}

	@Override
	public long write(ByteBuffer[] arg0, int arg1, int arg2) throws IOException
	{
		throw new IOException("Unsupported operation");
	}

	private void deleteFiles(int highFile) throws IOException
	{
		final ArrayList<Path> files = new ArrayList<Path>();
		int split = this.fn.lastIndexOf('/');
		String dir = this.fn.substring(0, split);
		String relative = this.fn.substring(split + 1);
		final PathMatcher matcher = FileSystems.getDefault().getPathMatcher("glob:" + relative + ".*");
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

		for (Path file : files)
		{
			String s = file.toString().substring(file.toString().lastIndexOf('.') + 1);
			int suffix = Integer.parseInt(s);

			if (suffix > highFile)
			{
				if (bufferedBlock == suffix)
				{
					bufferedBlock = -1;
					buffer = null;
				}

				FileChannel fc = fcs.get(suffix);
				if (fc != null)
				{
					fc.close();
					fcs.remove(suffix);
				}

				if (!new File(fn + "." + suffix).delete())
				{
					throw new IOException("Failed to delete file " + fn + "." + suffix + " during truncate() operation");
				}
			}
		}
	}

	private FileChannel getFC(int suffix) throws IOException
	{
		FileChannel retval = fcs.get(suffix);
		if (retval != null)
		{
			return retval;
		}

		RandomAccessFile raf = new RandomAccessFile(fn + "." + suffix, "rw");
		retval = raf.getChannel();
		if (fcs.size() > MAX_FCS)
		{
			if (trimInProgress.compareAndSet(false, true))
			{
				new TrimFCSThread(suffix, retval).start();
				return retval;
			}
			else
			{
				fcs.put(suffix, retval);
				return retval;
			}
		}
		else
		{
			fcs.put(suffix, retval);
			return retval;
		}
	}

	/*
	 * private void flushModded() throws IOException { for (Map.Entry entry :
	 * modded.entrySet()) { if ((Boolean)entry.getValue()) { FileChannel fc =
	 * getFC((Integer)entry.getKey()); try { fc.force(false); }
	 * catch(ClosedChannelException e) {} modded.remove(entry.getKey()); } } }
	 */

	private int multiPageWrite(ByteBuffer input, long offset) throws IOException
	{
		try
		{
			int pages = input.capacity() / (128 * 1024);
			int i = 0;
			byte[] data = new byte[128 * 1024];
			ByteBuffer bb;
			while (i < pages)
			{
				System.arraycopy(input.array(), i * 128 * 1024, data, 0, 128 * 1024);
				bb = ByteBuffer.wrap(data);
				write(bb, offset + i * 128 * 1024);
				i++;
			}
		}
		catch (Exception e)
		{
			HRDBMSWorker.logger.debug("", e);
			throw e;
		}

		return input.capacity();
	}

	private void trimFCS() throws IOException
	{
		lock.readLock().lock();
		Iterator<Map.Entry<Integer, FileChannel>> iterator = fcs.entrySet().iterator();
		while (fcs.size() > MAX_FCS && iterator.hasNext())
		{
			Map.Entry entry = iterator.next();
			int block = (Integer)entry.getKey();
			while (writeLocks.putIfAbsent(block, block) != null)
			{
				Thread.yield();
			}
			try
			{
				int mod = block & 31;
				synchronized (inUse[mod])
				{
					if (inUse[mod].containsKey(block))
					{
						writeLocks.remove(block);
						continue;
					}

					FileChannel fc = (FileChannel)entry.getValue();
					Boolean needsFlush = modded.get(entry.getKey());
					if (needsFlush != null && needsFlush)
					{
						//fc.force(false);
						modded.remove(entry.getKey());
					}
					fcs.remove(entry.getKey());
					fc.close();
				}
				writeLocks.remove(block);
			}
			catch (Exception e)
			{
				try
				{
					synchronized (inUse)
					{
						FileChannel fc = (FileChannel)entry.getValue();
						Boolean needsFlush = modded.get(entry.getKey());
						if (needsFlush != null && needsFlush)
						{
							fc.close();
							fcs.remove(entry.getKey());
							modded.remove(entry.getKey());
						}
					}
					writeLocks.remove(block);
				}
				catch (Exception f)
				{
					writeLocks.remove(block);
				}
			}
		}
		lock.readLock().unlock();
	}

	@Override
	protected void implCloseChannel() throws IOException
	{
		lock.writeLock().lock();
		try
		{
			for (Map.Entry entry : modded.entrySet())
			{
				if (((Boolean)entry.getValue()))
				{
					int block = (Integer)entry.getKey();
					FileChannel fc = getFC(block);
					try
					{
						fc.force(false);
					}
					catch (ClosedChannelException e)
					{
					}
				}
			}

			for (FileChannel fc : fcs.values())
			{
				fc.close();
			}

			modded = null;
			fcs = null;
		}
		catch (Exception e)
		{
			HRDBMSWorker.logger.debug("", e);
			lock.writeLock().unlock();
			throw e;
		}
		lock.writeLock().unlock();
	}

	private class TrimFCSThread extends HRDBMSThread
	{
		private final int suffix;
		private final FileChannel retval;

		public TrimFCSThread(int suffix, FileChannel retval)
		{
			this.suffix = suffix;
			this.retval = retval;
		}

		@Override
		public void run()
		{
			try
			{
				trimFCS();
				trimInProgress.set(false);
				fcs.put(suffix, retval);
			}
			catch (Exception e)
			{
				HRDBMSWorker.logger.debug("", e);
			}
		}
	}
}
