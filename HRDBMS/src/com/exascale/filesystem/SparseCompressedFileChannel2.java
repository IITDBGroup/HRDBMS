package com.exascale.filesystem;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.LockSupport;
import com.exascale.managers.BufferManager;
import com.exascale.managers.FileManager;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.misc.ScalableStampedReentrantRWLock;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

public class SparseCompressedFileChannel2 extends FileChannel
{
	private static LZ4Factory factory;
	private static long SLOT_SIZE;
	private static ArrayBlockingQueue<ByteBuffer> cache = new ArrayBlockingQueue<ByteBuffer>(2048);
	private static ArrayBlockingQueue<ByteBuffer> cache2 = new ArrayBlockingQueue<ByteBuffer>(2048);

	static
	{
		factory = LZ4Factory.nativeInstance();
		final LZ4Compressor comp = factory.fastCompressor();
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
			catch (final InterruptedException e)
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

	public SparseCompressedFileChannel2(final File file) throws IOException
	{
		this.fn = file.getAbsolutePath();
		int high = -1;
		final int split = this.fn.lastIndexOf('/');
		final String dir = this.fn.substring(0, split);
		final String relative = this.fn.substring(split + 1);

		final List<Path> files = new ArrayList<Path>();
		final File dirFile = new File(dir);
		final File[] files3 = dirFile.listFiles();
		for (final File f : files3)
		{
			if (f.getName().startsWith(relative + "."))
			{
				files.add(f.toPath());
			}
		}

		for (final Path file2 : files)
		{
			final String s = file2.toString().substring(file2.toString().lastIndexOf('.') + 1);
			final int suffix = Integer.parseInt(s);
			if (suffix > high)
			{
				high = suffix;
			}
		}

		final File theFile = new File(this.fn + ".0");

		if (high == -1)
		{
			length = 0;
			// HRDBMSWorker.logger.debug("Opened " + fn + " with length = 0");
			final RandomAccessFile raf = new RandomAccessFile(this.fn + ".0", "rw");
			theFC = raf.getChannel();
			// theFC = FileChannel.open(theFile.toPath(),
			// StandardOpenOption.SPARSE, StandardOpenOption.CREATE,
			// StandardOpenOption.READ, StandardOpenOption.WRITE);
			return;
		}

		length = (int)(theFile.length() / SLOT_SIZE);
		if (theFile.length() % SLOT_SIZE != 0)
		{
			length++;
		}
		// theFC = FileChannel.open(theFile.toPath(), StandardOpenOption.SPARSE,
		// StandardOpenOption.CREATE, StandardOpenOption.READ,
		// StandardOpenOption.WRITE);
		final RandomAccessFile raf = new RandomAccessFile(this.fn + ".0", "rw");
		theFC = raf.getChannel();
	}

	public SparseCompressedFileChannel2(final File file, final int suffix) throws IOException
	{
		this(file);
	}

	private static ByteBuffer allocateByteBuffer(final int size)
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
			final ByteBuffer retval = cache2.poll();
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

	private static void deallocateByteBuffer(final ByteBuffer bb)
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

	public void copyFromFC(final SparseCompressedFileChannel2 source) throws Exception
	{
		lock.writeLock().lock();
		try
		{
			truncate(0);
			BufferManager.invalidateFile(fn);
			final long size = source.size();
			long offset = 0;
			final ByteBuffer bb = ByteBuffer.allocate(Page.BLOCK_SIZE);
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
		catch (final Exception e)
		{
			lock.writeLock().unlock();
			HRDBMSWorker.logger.debug("", e);
			throw e;
		}
		lock.writeLock().unlock();
	}

	@Override
	public void force(final boolean arg0) throws IOException
	{
		theFC.force(false);
	}

	@Override
	public FileLock lock(final long arg0, final long arg1, final boolean arg2) throws IOException
	{
		throw new IOException("Unsupported operation");
	}

	@Override
	public MappedByteBuffer map(final MapMode arg0, final long arg1, final long arg2) throws IOException
	{
		throw new IOException("Unsupported operation");
	}

	@Override
	public long position() throws IOException
	{
		throw new IOException("Unsupported operation");
	}

	@Override
	public FileChannel position(final long arg0) throws IOException
	{
		throw new IOException("Unsupported operation");
	}

	@Override
	public int read(final ByteBuffer arg0) throws IOException
	{
		throw new IOException("Unsupported operation");
	}

	@Override
	public int read(final ByteBuffer arg0, final long arg1) throws IOException
	{
		try
		{
			boolean closed = false;
			final int page = (int)(arg1 / Page.BLOCK_SIZE);
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
					final LZ4FastDecompressor decomp = factory.fastDecompressor();
					// int mod = block & 31;
					final FileChannel fc = theFC;

					final ByteBuffer bb = allocateByteBuffer((int)SLOT_SIZE);
					fc.read(bb, page * SLOT_SIZE);
					// bb.position(bb.array().length - (int)SLOT_SIZE);
					// int size = bb.getInt();

					// byte[] target = new byte[128 * 1024];
					try
					{
						decomp.decompress(bb.array(), 0, arg0.array(), 0, Page.BLOCK_SIZE);
						deallocateByteBuffer(bb);
					}
					catch (final Exception e)
					{
						// HRDBMSWorker.logger.debug("Block = " + block);
						HRDBMSWorker.logger.debug("FC = " + fc);
						HRDBMSWorker.logger.debug("", e);
						// HRDBMSWorker.logger.debug("FC.size() = " +
						// fc.size());
						writeLocks.remove(page);
						lock.readLock().unlock();
						closed = true;
						throw e;
					}
				}
			}
			catch (final Exception e)
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
		catch (final Throwable e)
		{
			HRDBMSWorker.logger.error("", e);
			throw new IOException(e);
		}
	}

	public int read(final ByteBuffer arg0, final long arg1, final List<Integer> cols, final int layoutSize) throws IOException
	{
		try
		{
			boolean closed = false;
			final int page = (int)(arg1 / Page.BLOCK_SIZE);

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
					final LZ4FastDecompressor decomp = factory.fastDecompressor();
					// int mod = block & 31;
					final FileChannel fc = theFC;

					final ByteBuffer bb = allocateByteBuffer((int)SLOT_SIZE);
					fc.read(bb, page * SLOT_SIZE);
					// bb.position(bb.array().length - (int)SLOT_SIZE);
					// int size = bb.getInt();

					// byte[] target = new byte[128 * 1024 * 3]; // 3 pages
					try
					{
						decomp.decompress(bb.array(), 0, arg0.array(), 0, Page.BLOCK_SIZE);
						deallocateByteBuffer(bb);
					}
					catch (final Exception e)
					{
						// HRDBMSWorker.logger.debug("Block = " + block);
						HRDBMSWorker.logger.debug("FC = " + fc);
						HRDBMSWorker.logger.debug("", e);
						// HRDBMSWorker.logger.debug("FC.size() = " +
						// fc.size());
						writeLocks.remove(page);
						lock.readLock().unlock();
						closed = true;
						throw e;
					}
				}
			}
			catch (final Exception e)
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
		catch (final Throwable e)
		{
			HRDBMSWorker.logger.error("", e);
			throw new IOException(e);
		}
	}

	@Override
	public long read(final ByteBuffer[] arg0, final int arg1, final int arg2) throws IOException
	{
		throw new IOException("Unsupported operation");
	}

	public int read(final ByteBuffer[] bbs, final long arg1) throws IOException
	{
		Exception ex = null;
		final int page = (int)(arg1 / Page.BLOCK_SIZE);
		try
		{
			lock.readLock().lock();
			int i = 0;
			final int num = bbs.length;
			while (i < num)
			{
				while (writeLocks.putIfAbsent(page + i, page + i) != null)
				{
					LockSupport.parkNanos(500);
				}

				i++;
			}

			final LZ4FastDecompressor decomp = factory.fastDecompressor();
			final FileChannel fc = theFC;

			final ByteBuffer bb = allocateByteBuffer((int)SLOT_SIZE * num);
			fc.read(bb, page * SLOT_SIZE);

			i = 0;
			while (i < num)
			{
				decomp.decompress(bb.array(), i * (int)SLOT_SIZE, bbs[i].array(), 0, Page.BLOCK_SIZE);
				i++;
			}
			deallocateByteBuffer(bb);
		}
		catch (final Exception e)
		{
			ex = e;
		}

		int i = 0;
		while (i < bbs.length)
		{
			writeLocks.remove(page + i);
			i++;
		}
		lock.readLock().unlock();

		if (ex != null)
		{
			throw new IOException(ex);
		}

		return bbs[0].capacity();
	}

	public int read3(final ByteBuffer bb, final ByteBuffer bb2, final ByteBuffer bb3, final long arg1) throws IOException
	{
		read(bb, arg1);
		read(bb2, arg1 + Page.BLOCK_SIZE);
		read(bb3, arg1 + Page.BLOCK_SIZE * 2);
		return bb.capacity();
	}

	public int read3(final ByteBuffer bb, final ByteBuffer bb2, final ByteBuffer bb3, final long arg1, final List<Integer> cols, final int layoutSize) throws IOException
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
		final long retval = length * 1L * Page.BLOCK_SIZE;
		lock.readLock().unlock();
		return retval;
	}

	@Override
	public long transferFrom(final ReadableByteChannel arg0, final long arg1, final long arg2) throws IOException
	{
		throw new IOException("Unsupported operation");
	}

	@Override
	public long transferTo(final long arg0, final long arg1, final WritableByteChannel arg2) throws IOException
	{
		throw new IOException("Unsupported operation");
	}

	@Override
	public FileChannel truncate(final long arg0) throws IOException
	{
		lock.writeLock().lock();
		try
		{
			pos = arg0;
			final int desiredPages = (int)(arg0 / Page.BLOCK_SIZE);
			theFC.truncate(desiredPages * SLOT_SIZE);
			length = desiredPages;
		}
		catch (final Exception e)
		{
			lock.writeLock().unlock();
			HRDBMSWorker.logger.debug("", e);
			throw e;
		}
		lock.writeLock().unlock();
		return this;
	}

	@Override
	public FileLock tryLock(final long arg0, final long arg1, final boolean arg2) throws IOException
	{
		throw new IOException("Unsupported operation");
	}

	@Override
	public int write(final ByteBuffer arg0) throws IOException
	{
		// lock.writeLock().lock();
		try
		{
			write(arg0, pos);
			pos += arg0.capacity();
		}
		catch (final Exception e)
		{
			// lock.writeLock().unlock();
			HRDBMSWorker.logger.debug("", e);
			throw e;
		}
		// lock.writeLock().unlock();
		return arg0.capacity();
	}

	@Override
	public int write(final ByteBuffer arg0, final long arg1) throws IOException
	{
		// multi-page write
		if (arg0.capacity() > Page.BLOCK_SIZE)
		{
			return multiPageWrite(arg0, arg1);
		}

		final boolean closed = false;
		final int page = (int)(arg1 / Page.BLOCK_SIZE);

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
				final LZ4Compressor comp = factory.fastCompressor();
				// write it
				// fc.truncate(0);
				// int oldSize = (int)fc.size();
				final byte[] data = comp.compress(arg0.array());

				final ByteBuffer bb = ByteBuffer.wrap(data);
				final FileChannel fc = theFC;
				try
				{
					fc.write(bb, page * SLOT_SIZE);
				}
				catch(IllegalArgumentException ex)
				{
					HRDBMSWorker.logger.debug("Page = " + page);
					HRDBMSWorker.logger.debug("Arg1 = " + arg1);
					HRDBMSWorker.logger.debug("SLOT_SIZE = " + SLOT_SIZE);
					throw ex;
				}

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
					final LZ4Compressor comp = factory.fastCompressor();
					final byte[] data = comp.compress(arg0.array());

					final ByteBuffer bb = ByteBuffer.wrap(data);
					final FileChannel fc = theFC;
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
		catch (final Exception e)
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
	public long write(final ByteBuffer[] arg0, final int arg1, final int arg2) throws IOException
	{
		throw new IOException("Unsupported operation");
	}

	private int multiPageWrite(final ByteBuffer input, final long offset) throws IOException
	{
		try
		{
			final int pages = input.capacity() / Page.BLOCK_SIZE;
			int i = 0;
			final byte[] data = new byte[Page.BLOCK_SIZE];
			ByteBuffer bb;
			while (i < pages)
			{
				System.arraycopy(input.array(), i * Page.BLOCK_SIZE, data, 0, Page.BLOCK_SIZE);
				bb = ByteBuffer.wrap(data);
				write(bb, offset + i * Page.BLOCK_SIZE);
				i++;
			}
		}
		catch (final Exception e)
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
