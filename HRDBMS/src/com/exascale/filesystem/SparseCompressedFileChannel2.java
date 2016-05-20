package com.exascale.filesystem;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
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

public class SparseCompressedFileChannel2 extends FileChannel
{
	private static LZ4Factory factory;
	private static long SLOT_SIZE;
	private static ArrayBlockingQueue<ByteBuffer> cache = new ArrayBlockingQueue<ByteBuffer>(2048);
	private static ArrayBlockingQueue<ByteBuffer> cache2 = new ArrayBlockingQueue<ByteBuffer>(2048);

	static
	{
		factory = LZ4Factory.nativeInstance();
		LZ4Compressor comp = factory.fastCompressor();
		int slot = comp.maxCompressedLength(Page.BLOCK_SIZE);
		if (slot % 4096 != 0)
		{
			slot += (4096 - (slot % 4096));
		}

		// SLOT_SIZE = 512 * 1024;
		SLOT_SIZE = slot;

		int i = 0;
		while (i < Runtime.getRuntime().availableProcessors())
		{
			try
			{
				cache.put(ByteBuffer.allocate((int)SLOT_SIZE));
				cache2.put(ByteBuffer.allocate((int)SLOT_SIZE * 3));
			}
			catch (InterruptedException e)
			{
			}
			i++;
		}
	}

	public String fn;
	private FileChannel theFC;
	private volatile int length; // in 128k pages
	private long pos = 0;
	private final ScalableStampedReentrantRWLock lock = new ScalableStampedReentrantRWLock();
	private final ConcurrentHashMap<Integer, Integer> writeLocks = new ConcurrentHashMap<Integer, Integer>();

	private static ByteBuffer allocateByteBuffer(int size)
	{
		if (size == SLOT_SIZE)
		{
			ByteBuffer retval = cache.poll();
			if (retval != null)
			{
				retval.position(0);
				return retval;
			}

			retval = cache2.poll();
			if (retval != null)
			{
				retval.position(0);
				retval.limit(size);
				return retval;
			}
			
			return ByteBuffer.allocate((int)SLOT_SIZE);
		}
		else
		{
			ByteBuffer retval = cache2.poll();
			if (retval != null)
			{
				if (retval.capacity() >= size)
				{
					retval.position(0);
					retval.limit(size);
					return retval;
				}
			}
			
			return ByteBuffer.allocate(size);
		}
	}

	private static void deallocateByteBuffer(ByteBuffer bb)
	{
		if (bb.capacity() == SLOT_SIZE)
		{
			cache.offer(bb);
		}
		else
		{
			bb.limit(bb.capacity());
			cache2.offer(bb);
		}
	}

	public SparseCompressedFileChannel2(File file) throws IOException
	{
		this.fn = file.getAbsolutePath();
		int high = -1;
		int split = this.fn.lastIndexOf('/');
		String dir = this.fn.substring(0, split);
		String relative = this.fn.substring(split + 1);

		final ArrayList<Path> files = new ArrayList<Path>();
		File dirFile = new File(dir);
		File[] files3 = dirFile.listFiles();
		for (File f : files3)
		{
			if (f.getName().startsWith(relative + "."))
			{
				files.add(f.toPath());
			}
		}

		for (Path file2 : files)
		{
			String s = file2.toString().substring(file2.toString().lastIndexOf('.') + 1);
			int suffix = Integer.parseInt(s);
			if (suffix > high)
			{
				high = suffix;
			}
		}

		File theFile = new File(this.fn + ".0");

		if (high == -1)
		{
			length = 0;
			// HRDBMSWorker.logger.debug("Opened " + fn + " with length = 0");
			RandomAccessFile raf = new RandomAccessFile(this.fn + ".0","rw");
			theFC = raf.getChannel();
			//theFC = FileChannel.open(theFile.toPath(), StandardOpenOption.SPARSE, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
			return;
		}

		length = (int)(theFile.length() / SLOT_SIZE);
		if (theFile.length() % SLOT_SIZE != 0)
		{
			length++;
		}
		//theFC = FileChannel.open(theFile.toPath(), StandardOpenOption.SPARSE, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
		RandomAccessFile raf = new RandomAccessFile(this.fn + ".0","rw");
		theFC = raf.getChannel();
	}

	public SparseCompressedFileChannel2(File file, int suffix) throws IOException
	{
		this(file);
	}

	public void copyFromFC(SparseCompressedFileChannel2 source) throws Exception
	{
		lock.writeLock().lock();
		try
		{
			truncate(0);
			BufferManager.invalidateFile(fn);
			long size = source.size();
			long offset = 0;
			ByteBuffer bb = ByteBuffer.allocate(Page.BLOCK_SIZE);
			while (offset < size)
			{
				bb.position(0);
				source.read(bb, offset);
				bb.position(0);
				write(bb, offset);
				offset += bb.capacity();
			}

			force(false);
			FileManager.numBlocks.put(fn, (int)(offset / Page.BLOCK_SIZE));
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
		theFC.force(false);
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
		try
		{
			boolean closed = false;
			int page = (int)(arg1 / Page.BLOCK_SIZE);
			lock.readLock().lock();
			try
			{
				while (writeLocks.putIfAbsent(page, page) != null)
				{
					lock.readLock().unlock();
					LockSupport.parkNanos(500);
					lock.readLock().lock();
				}

				{
					// full block
					// LZ4SafeDecompressor decomp = factory.safeDecompressor();
					LZ4FastDecompressor decomp = factory.fastDecompressor();
					// int mod = block & 31;
					FileChannel fc = theFC;

					ByteBuffer bb = allocateByteBuffer((int)SLOT_SIZE);
					fc.read(bb, page * SLOT_SIZE);
					// bb.position(bb.array().length - (int)SLOT_SIZE);
					// int size = bb.getInt();

					// byte[] target = new byte[128 * 1024];
					try
					{
						decomp.decompress(bb.array(), 0, arg0.array(), 0, Page.BLOCK_SIZE);
						deallocateByteBuffer(bb);
					}
					catch (Exception e)
					{
						// HRDBMSWorker.logger.debug("Block = " + block);
						HRDBMSWorker.logger.debug("FC = " + fc);
						HRDBMSWorker.logger.debug("", e);
						// HRDBMSWorker.logger.debug("FC.size() = " + fc.size());
						writeLocks.remove(page);
						lock.readLock().unlock();
						closed = true;
						throw e;
					}
				}
			}
			catch (Exception e)
			{
				if (!closed)
				{
					writeLocks.remove(page);
					lock.readLock().unlock();
				}

				HRDBMSWorker.logger.debug("", e);
				throw e;
			}

			writeLocks.remove(page);
			lock.readLock().unlock();
			return arg0.capacity();
		}
		catch (Throwable e)
		{
			HRDBMSWorker.logger.error("", e);
			throw new IOException(e);
		}
	}
	
	public int read(ByteBuffer[] bbs, long arg1) throws IOException
	{
		try
		{
			boolean closed = false;
			int page = (int)(arg1 / Page.BLOCK_SIZE);
			lock.readLock().lock();
			try
			{
				int i = 0;
				int num = bbs.length;
				while (i < num)
				{
					while (writeLocks.putIfAbsent(page + i, page + i) != null)
					{
						LockSupport.parkNanos(500);
					}
					
					i++;
				}

				{
					// full block
					// LZ4SafeDecompressor decomp = factory.safeDecompressor();
					LZ4FastDecompressor decomp = factory.fastDecompressor();
					// int mod = block & 31;
					FileChannel fc = theFC;

					ByteBuffer bb = allocateByteBuffer((int)SLOT_SIZE * num);
					fc.read(bb, page * SLOT_SIZE);
					// bb.position(bb.array().length - (int)SLOT_SIZE);
					// int size = bb.getInt();

					// byte[] target = new byte[128 * 1024];
					try
					{
						i = 0;
						while (i < num)
						{
							decomp.decompress(bb.array(), i * (int)SLOT_SIZE, bbs[i].array(), 0, Page.BLOCK_SIZE);
							i++;
						}
						deallocateByteBuffer(bb);
					}
					catch (Exception e)
					{
						// HRDBMSWorker.logger.debug("Block = " + block);
						HRDBMSWorker.logger.debug("FC = " + fc);
						HRDBMSWorker.logger.debug("", e);
						// HRDBMSWorker.logger.debug("FC.size() = " + fc.size());
						i = 0;
						while (i < num)
						{
							writeLocks.remove(page + i);
							i++;
						}
						lock.readLock().unlock();
						closed = true;
						throw e;
					}
				}
			}
			catch (Exception e)
			{
				if (!closed)
				{
					writeLocks.remove(page);
					lock.readLock().unlock();
				}

				HRDBMSWorker.logger.debug("", e);
				throw e;
			}

			int i = 0;
			while (i < bbs.length)
			{
				writeLocks.remove(page + i);
				i++;
			}
			lock.readLock().unlock();
			return bbs[0].capacity();
		}
		catch (Throwable e)
		{
			HRDBMSWorker.logger.error("", e);
			throw new IOException(e);
		}
	}

	public int read(ByteBuffer arg0, long arg1, ArrayList<Integer> cols, int layoutSize) throws IOException
	{
		try
		{
			boolean closed = false;
			int page = (int)(arg1 / Page.BLOCK_SIZE);

			lock.readLock().lock();
			try
			{
				while (writeLocks.putIfAbsent(page, page) != null)
				{
					lock.readLock().unlock();
					LockSupport.parkNanos(500);
					lock.readLock().lock();
				}

				{
					// full block
					// LZ4SafeDecompressor decomp = factory.safeDecompressor();
					LZ4FastDecompressor decomp = factory.fastDecompressor();
					// int mod = block & 31;
					FileChannel fc = theFC;

					ByteBuffer bb = allocateByteBuffer((int)SLOT_SIZE);
					fc.read(bb, page * SLOT_SIZE);
					// bb.position(bb.array().length - (int)SLOT_SIZE);
					// int size = bb.getInt();

					// byte[] target = new byte[128 * 1024 * 3]; // 3 pages
					try
					{
						decomp.decompress(bb.array(), 0, arg0.array(), 0, Page.BLOCK_SIZE);
						deallocateByteBuffer(bb);
					}
					catch (Exception e)
					{
						// HRDBMSWorker.logger.debug("Block = " + block);
						HRDBMSWorker.logger.debug("FC = " + fc);
						HRDBMSWorker.logger.debug("", e);
						// HRDBMSWorker.logger.debug("FC.size() = " + fc.size());
						writeLocks.remove(page);
						lock.readLock().unlock();
						closed = true;
						throw e;
					}
				}
			}
			catch (Exception e)
			{
				if (!closed)
				{
					writeLocks.remove(page);
					lock.readLock().unlock();
				}

				HRDBMSWorker.logger.debug("", e);
				throw e;
			}

			writeLocks.remove(page);
			lock.readLock().unlock();
			return arg0.capacity();
		}
		catch (Throwable e)
		{
			HRDBMSWorker.logger.error("", e);
			throw new IOException(e);
		}
	}

	@Override
	public long read(ByteBuffer[] arg0, int arg1, int arg2) throws IOException
	{
		throw new IOException("Unsupported operation");
	}

	public int read3(ByteBuffer bb, ByteBuffer bb2, ByteBuffer bb3, long arg1) throws IOException
	{
		read(bb, arg1);
		read(bb2, arg1 + Page.BLOCK_SIZE);
		read(bb3, arg1 + Page.BLOCK_SIZE * 2);
		return bb.capacity();
	}

	public int read3(ByteBuffer bb, ByteBuffer bb2, ByteBuffer bb3, long arg1, ArrayList<Integer> cols, int layoutSize) throws IOException
	{
		read(bb, arg1, cols, layoutSize);
		read(bb2, arg1 + Page.BLOCK_SIZE, cols, layoutSize);
		read(bb3, arg1 + Page.BLOCK_SIZE * 2, cols, layoutSize);
		return bb.capacity();
	}

	@Override
	public long size() throws IOException
	{
		lock.readLock().lock();
		long retval = length * 1L * Page.BLOCK_SIZE;
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
			int desiredPages = (int)(arg0 / Page.BLOCK_SIZE);
			theFC.truncate(desiredPages * SLOT_SIZE);
			length = desiredPages;
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
		if (arg0.capacity() > Page.BLOCK_SIZE)
		{
			return multiPageWrite(arg0, arg1);
		}

		boolean closed = false;
		int page = (int)(arg1 / Page.BLOCK_SIZE);

		lock.readLock().lock();

		if (page < length)
		{
			// lock.readLock().lock();
			while (true)
			{
				if (writeLocks.putIfAbsent(page, page) != null)
				{
					LockSupport.parkNanos(500);
					continue;
				}

				break;
			}
			// lock.writeLock().unlock();

			{
				// compress it
				// LZ4Compressor comp = factory.highCompressor();
				LZ4Compressor comp = factory.fastCompressor();
				// write it
				// fc.truncate(0);
				// int oldSize = (int)fc.size();
				byte[] data = comp.compress(arg0.array());

				ByteBuffer bb = ByteBuffer.wrap(data);
				FileChannel fc = theFC;
				fc.write(bb, page * SLOT_SIZE);

				writeLocks.remove(page);
				lock.readLock().unlock();
				return arg0.capacity();
			}
		}

		try
		{
			lock.readLock().unlock();
			lock.writeLock().lock();

			{
				// need to write to new block
				{
					LZ4Compressor comp = factory.fastCompressor();
					byte[] data = comp.compress(arg0.array());

					ByteBuffer bb = ByteBuffer.wrap(data);
					FileChannel fc = theFC;
					// Process p = Runtime.getRuntime().exec("dd if=/dev/zero
					// of=" + this.fn + ".0 bs=" + SLOT_SIZE + " count=0 seek="
					// + (page+1));
					// while (true)
					// {
					// try
					// {
					// p.waitFor();
					// break;
					// }
					// catch(InterruptedException e)
					// {}
					// }

					fc.write(bb, page * SLOT_SIZE);
					length = page + 1;
				}

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

	private int multiPageWrite(ByteBuffer input, long offset) throws IOException
	{
		try
		{
			int pages = input.capacity() / Page.BLOCK_SIZE;
			int i = 0;
			byte[] data = new byte[Page.BLOCK_SIZE];
			ByteBuffer bb;
			while (i < pages)
			{
				System.arraycopy(input.array(), i * Page.BLOCK_SIZE, data, 0, Page.BLOCK_SIZE);
				bb = ByteBuffer.wrap(data);
				write(bb, offset + i * Page.BLOCK_SIZE);
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

	@Override
	protected void implCloseChannel() throws IOException
	{
		theFC.force(false);
		theFC.close();
		theFC = null;
	}
}
