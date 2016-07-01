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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
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

public class CompressedFileChannel extends FileChannel
{
	private static LZ4Factory factory;
	private static long MAX_CACHE_SIZE;
	private static int MAX_SPAN;
	static
	{
		factory = LZ4Factory.nativeInstance();
		MAX_CACHE_SIZE = Long.parseLong(HRDBMSWorker.getHParms().getProperty("max_cfc_cache_size"));
		MAX_SPAN = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("prefetch_request_size")) / 3 + 1;
	}

	public volatile int MAX_FCS;
	public String fn;
	public ConcurrentSkipListMap<Integer, FileChannel> fcs = new ConcurrentSkipListMap<Integer, FileChannel>();
	private ConcurrentHashMap<Integer, Boolean> modded = new ConcurrentHashMap<Integer, Boolean>();
	private int length; // in 128k pages
	private int bufferedBlock = -1;
	private byte[] buffer;
	private int bufferSize = 0;
	private long pos = 0;
	private final ScalableStampedReentrantRWLock lock = new ScalableStampedReentrantRWLock();
	private final ScalableStampedRWLock bufferLock = new ScalableStampedRWLock();
	private final HashMap<Integer, Integer>[] inUse = new HashMap[32];
	public final AtomicBoolean trimInProgress = new AtomicBoolean(false);
	public final AtomicBoolean oaInProgress = new AtomicBoolean(false);
	private final TreeMap<Integer, ArrayList<ByteBuffer>> cache = new TreeMap<Integer, ArrayList<ByteBuffer>>();
	private long cacheSize = 0;
	private final IdentityHashMap<ByteBuffer, ByteBuffer> sliceToActual = new IdentityHashMap<ByteBuffer, ByteBuffer>();
	private final ConcurrentHashMap<Integer, Integer> writeLocks = new ConcurrentHashMap<Integer, Integer>(MAX_FCS, 0.75f, ResourceManager.cpus);
	public AtomicLong accesses = new AtomicLong(0);
	public AtomicInteger fcs_size = new AtomicInteger(0);
	private final AtomicInteger lastRequest = new AtomicInteger(0);
	public AtomicBoolean didRead3 = new AtomicBoolean(false);
	public AtomicBoolean aoOK = new AtomicBoolean(true);
	public ConcurrentHashMap<Integer, Integer> aoBlocks = new ConcurrentHashMap<Integer, Integer>();

	public CompressedFileChannel(File file) throws IOException
	{
		MAX_FCS = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("num_fcs_per_cfc"));
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

		if (high == -1)
		{
			length = 0;
			// HRDBMSWorker.logger.debug("Opened " + fn + " with length =  0");
			return;
		}

		length = high + 1; // length in 3 page blocks
		LZ4SafeDecompressor decomp = factory.safeDecompressor();
		FileChannel fc = getFC(high, 1);
		ByteBuffer bb = allocateByteBuffer((int)fc.size());
		fc.read(bb, 0);
		byte[] target = new byte[128 * 1024 * 3]; // max 3 pages
		int bytes = decomp.decompress(bb.array(), bb.array().length - (int)fc.size(), (int)fc.size(), target, 0, 128 * 1024 * 3);
		deallocateByteBuffer(bb);
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
		MAX_FCS = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("num_fcs_per_cfc"));
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
		FileChannel fc = getFC(high, 1);
		ByteBuffer bb = allocateByteBuffer((int)fc.size());
		fc.read(bb, 0);
		byte[] target = new byte[128 * 1024 * 3]; // max 3 pages
		int bytes = decomp.decompress(bb.array(), bb.array().length - (int)fc.size(), (int)fc.size(), target, 0, 128 * 1024 * 3);
		deallocateByteBuffer(bb);
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
			ByteBuffer bb = allocateByteBuffer(128 * 1024);
			while (offset < size)
			{
				bb.position(0);
				source.read(bb, offset);
				bb.position(0);
				write(bb, offset);
				offset += bb.capacity();
			}
			deallocateByteBuffer(bb);

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
					FileChannel fc = getFC(block, 1);
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
		boolean closed = false;
		int page = (int)(arg1 >> 17);
		int offset = 0;
		int block = page / 3;
		lock.readLock().lock();
		try
		{
			while (writeLocks.putIfAbsent(block, block) != null)
			{
				lock.readLock().unlock();
				LockSupport.parkNanos(500);
				lock.readLock().lock();
			}

			offset += ((page % 3) << 17);

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
				FileChannel fc;
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

				fc = getFC(block, 1);
				ByteBuffer bb = allocateByteBuffer((int)fc.size());
				fc.read(bb, 0);
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
				int bytes = decomp.decompress(bb.array(), bb.array().length - size, size, target, 0, 128 * 1024 * 3);
				deallocateByteBuffer(bb);
				System.arraycopy(target, offset, arg0.array(), 0, arg0.capacity());

				if (bufferLock.writeLock().tryLock())
				{
					bufferedBlock = block;
					buffer = target;
					bufferSize = bytes >> 17;
				bufferLock.writeLock().unlock();
				}
			}
			else
			{
				// full block
				LZ4FastDecompressor decomp = factory.fastDecompressor();
				int mod = block & 31;
				FileChannel fc;
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

				fc = getFC(block, 1);

				ByteBuffer bb = allocateByteBuffer((int)(fc.size()));
				fc.read(bb, 0);
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
					decomp.decompress(bb.array(), bb.array().length - (int)fc.size(), target, 0, target.length);
					deallocateByteBuffer(bb);
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

	public int read(ByteBuffer arg0, long arg1, ArrayList<Integer> cols, int layoutSize) throws IOException
	{
		boolean closed = false;
		int page = (int)(arg1 >> 17);
		int offset = 0;
		int block = page / 3;
		lock.readLock().lock();
		try
		{
			while (writeLocks.putIfAbsent(block, block) != null)
			{
				lock.readLock().unlock();
				LockSupport.parkNanos(500);
				lock.readLock().lock();
			}

			offset += ((page % 3) << 17);

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
				FileChannel fc;
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

				fc = getFC(block, 1, cols, layoutSize);
				ByteBuffer bb = allocateByteBuffer((int)fc.size());
				fc.read(bb, 0);
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
				int bytes = decomp.decompress(bb.array(), bb.array().length - size, size, target, 0, 128 * 1024 * 3);
				deallocateByteBuffer(bb);
				System.arraycopy(target, offset, arg0.array(), 0, arg0.capacity());

				if (bufferLock.writeLock().tryLock())
				{
					bufferedBlock = block;
					buffer = target;
					bufferSize = bytes >> 17;
				bufferLock.writeLock().unlock();
				}
			}
			else
			{
				// full block
				LZ4FastDecompressor decomp = factory.fastDecompressor();
				int mod = block & 31;
				FileChannel fc;
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

				fc = getFC(block, 1, cols, layoutSize);

				ByteBuffer bb = allocateByteBuffer((int)(fc.size()));
				fc.read(bb, 0);
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
					decomp.decompress(bb.array(), bb.array().length - (int)fc.size(), target, 0, target.length);
					deallocateByteBuffer(bb);
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

	public int read3(ByteBuffer bb, ByteBuffer bb2, ByteBuffer bb3, long arg1) throws IOException
	{
		didRead3.set(true);
		boolean closed = false;
		int page = (int)(arg1 >> 17);
		int block = page / 3;
		lock.readLock().lock();
		try
		{
			while (writeLocks.putIfAbsent(block, block) != null)
			{
				lock.readLock().unlock();
				LockSupport.parkNanos(500);
				lock.readLock().lock();
			}

			// bufferLock.readLock().lock();

			if (bufferLock.readLock().tryLock())
			{
				if (bufferedBlock == block)
				{
					System.arraycopy(buffer, 0, bb.array(), 0, bb.capacity());
					System.arraycopy(buffer, 128 * 1024, bb2.array(), 0, bb2.capacity());
					System.arraycopy(buffer, 128 * 2 * 1024, bb3.array(), 0, bb3.capacity());
					bufferLock.readLock().unlock();
					writeLocks.remove(block);
					lock.readLock().unlock();
					return bb.capacity();
				}
				bufferLock.readLock().unlock();
			}

			{
				// full block
				LZ4FastDecompressor decomp = factory.fastDecompressor();
				int mod = block & 31;
				FileChannel fc;
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

				fc = getFC(block, 3);

				ByteBuffer bb4 = allocateByteBuffer((int)(fc.size()));
				fc.read(bb4, 0);
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
					decomp.decompress(bb4.array(), bb4.array().length - (int)fc.size(), target, 0, target.length);
					deallocateByteBuffer(bb4);
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

				System.arraycopy(target, 0, bb.array(), 0, bb.capacity());
				System.arraycopy(target, 128 * 1024, bb2.array(), 0, bb2.capacity());
				System.arraycopy(target, 128 * 2 * 1024, bb3.array(), 0, bb3.capacity());

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
		return bb.capacity();
	}

	public int read3(ByteBuffer bb, ByteBuffer bb2, ByteBuffer bb3, long arg1, ArrayList<Integer> cols, int layoutSize) throws IOException
	{
		didRead3.set(true);
		boolean closed = false;
		int page = (int)(arg1 >> 17);
		int block = page / 3;
		lock.readLock().lock();
		try
		{
			while (writeLocks.putIfAbsent(block, block) != null)
			{
				lock.readLock().unlock();
				LockSupport.parkNanos(500);
				lock.readLock().lock();
			}

			// bufferLock.readLock().lock();

			if (bufferLock.readLock().tryLock())
			{
				if (bufferedBlock == block)
				{
					System.arraycopy(buffer, 0, bb.array(), 0, bb.capacity());
					System.arraycopy(buffer, 128 * 1024, bb2.array(), 0, bb2.capacity());
					System.arraycopy(buffer, 128 * 2 * 1024, bb3.array(), 0, bb3.capacity());
					bufferLock.readLock().unlock();
					writeLocks.remove(block);
					lock.readLock().unlock();
					return bb.capacity();
				}
				bufferLock.readLock().unlock();
			}

			{
				// full block
				LZ4FastDecompressor decomp = factory.fastDecompressor();
				int mod = block & 31;
				FileChannel fc;
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

				fc = getFC(block, 3, cols, layoutSize);

				ByteBuffer bb4 = allocateByteBuffer((int)(fc.size()));
				fc.read(bb4, 0);	
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
					decomp.decompress(bb4.array(), bb4.array().length - (int)fc.size(), target, 0, target.length);
					deallocateByteBuffer(bb4);
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

				System.arraycopy(target, 0, bb.array(), 0, bb.capacity());
				System.arraycopy(target, 128 * 1024, bb2.array(), 0, bb2.capacity());
				System.arraycopy(target, 128 * 2 * 1024, bb3.array(), 0, bb3.capacity());

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
		return bb.capacity();
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
				FileChannel fc = getFC(actualHighFileNum, 1);
				LZ4SafeDecompressor decomp = factory.safeDecompressor();
				ByteBuffer bb = allocateByteBuffer((int)fc.size());
				fc.read(bb, 0);
				byte[] target = new byte[128 * 1024 * 3]; // max 3 pages
				decomp.decompress(bb.array(), bb.array().length - (int)fc.size(), (int)fc.size(), target, 0, 128 * 1024 * 3);
				deallocateByteBuffer(bb);
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
			while (true)
			{
				if (writeLocks.putIfAbsent(block, block) != null)
				{
					LockSupport.parkNanos(500);
					continue;
				}

				break;
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
				FileChannel fc;
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

				fc = getFC(block, 1);

				// fc.truncate(0);
				int oldSize = (int)fc.size();
				byte[] data = comp.compress(buffer);
				bufferLock.readLock().unlock();
				ByteBuffer bb = ByteBuffer.wrap(data);
				fc.write(bb, 0);
				int newSize = data.length;
				if (newSize < oldSize)
				{
					fc.truncate(newSize);
				}
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
				FileChannel fc;
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

				fc = getFC(block, 1);

				ByteBuffer bb = allocateByteBuffer((int)(fc.size()));
				fc.read(bb, 0);
				byte[] target = new byte[128 * 1024 * 3]; // 3 pages
				try
				{
					decomp.decompress(bb.array(), bb.array().length - (int)fc.size(), target, 0, target.length);
					deallocateByteBuffer(bb);
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
				// fc.truncate(0);
				int oldSize = (int)fc.size();
				byte[] data = comp.compress(target);
				bb = ByteBuffer.wrap(data);
				fc.write(bb, 0);
				int newSize = data.length;
				if (newSize < oldSize)
				{
					fc.truncate(newSize);
				}

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
					FileChannel fc = getFC(block, 1);
					// fc.truncate(0);
					int oldSize = (int)fc.size();
					byte[] data = comp.compress(buffer);
					ByteBuffer bb = ByteBuffer.wrap(data);
					fc.write(bb, 0);
					int newSize = data.length;
					if (newSize < oldSize)
					{
						fc.truncate(newSize);
					}
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
					FileChannel fc = getFC(block, 1);
					// fc.truncate(0);
					int oldSize = (int)fc.size();
					byte[] data = comp.compress(valid);
					ByteBuffer bb = ByteBuffer.wrap(data);
					fc.write(bb, 0);
					int newSize = data.length;
					if (newSize < oldSize)
					{
						fc.truncate(newSize);
					}
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
					FileChannel fc3 = getFC(highFileNum, 1);
					LZ4SafeDecompressor decomp = factory.safeDecompressor();
					ByteBuffer bb2 = allocateByteBuffer((int)fc3.size());
					fc3.read(bb2, 0);
					byte[] target = new byte[128 * 1024 * 3]; // max 3 pages
					int bytes = -1;
					try
					{
						bytes = decomp.decompress(bb2.array(), bb2.array().length - (int)fc3.size(), (int)fc3.size(), target, 0, 128 * 1024 * 3);
						deallocateByteBuffer(bb2);
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
						// fc3.truncate(0);
						int oldSize = (int)fc3.size();
						// LZ4Compressor comp = factory.highCompressor();
						LZ4Compressor comp = factory.fastCompressor();
						byte[] data = comp.compress(fullBlock);
						ByteBuffer bb = ByteBuffer.wrap(data);
						fc3.write(bb, 0);
						int newSize = data.length;
						if (newSize < oldSize)
						{
							fc3.truncate(newSize);
						}
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
						FileChannel fc2 = getFC(highFileNum + 1, 1);
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
					FileChannel fc = getFC(block, 1);
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
					byte[] temp2 = new byte[2 * 128 * 1024];
					System.arraycopy(arg0.array(), 0, temp2, 128 * 1024, 128 * 1024);
					byte[] data = comp.compress(temp2);
					ByteBuffer bb = ByteBuffer.wrap(data);
					FileChannel fc = getFC(block, 1);
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
					byte[] temp2 = new byte[3 * 128 * 1024];
					System.arraycopy(arg0.array(), 0, temp2, 2 * 128 * 1024, 128 * 1024);
					byte[] data = comp.compress(temp2);
					ByteBuffer bb = ByteBuffer.wrap(data);
					FileChannel fc = getFC(block, 1);
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
				FileChannel fc = getFC(block, 1);
				ByteBuffer bb = allocateByteBuffer((int)fc.size());
				fc.read(bb, 0);
				byte[] target = new byte[128 * 1024 * 3]; // max 3 pages
				int bytes = decomp.decompress(bb.array(), bb.array().length - (int)fc.size(), (int)fc.size(), target, 0, 128 * 1024 * 3);
				deallocateByteBuffer(bb);
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
				System.arraycopy(target, 0, valid, 0, valid.length);

				// compress it
				// LZ4Compressor comp = factory.highCompressor();
				LZ4Compressor comp = factory.fastCompressor();
				// write it
				// fc.truncate(0);
				int oldSize = (int)fc.size();
				byte[] data = comp.compress(valid);
				bb = ByteBuffer.wrap(data);
				fc.write(bb, 0);
				int newSize = data.length;
				if (newSize < oldSize)
				{
					fc.truncate(newSize);
				}
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

	private ByteBuffer allocateByteBuffer(int size)
	{
		synchronized (cache)
		{
			for (Map.Entry entry : cache.entrySet())
			{
				if (size <= (Integer)entry.getKey())
				{
					ArrayList<ByteBuffer> bbs = (ArrayList<ByteBuffer>)entry.getValue();
					ByteBuffer bb = bbs.remove(bbs.size() - 1);
					if (bbs.size() == 0)
					{
						cache.remove(entry.getKey());
					}

					bb.position(bb.capacity() - size);
					ByteBuffer retval = bb.slice();
					sliceToActual.put(retval, bb);
					return retval;
				}
			}

			ByteBuffer retval = ByteBuffer.allocate(size);
			sliceToActual.put(retval, retval);
			cacheSize += size;

			if (cacheSize > MAX_CACHE_SIZE)
			{
				for (Map.Entry entry : ((TreeMap<Integer, ByteBuffer>)cache.clone()).entrySet())
				{
					ArrayList<ByteBuffer> bbs = cache.get(entry.getKey());
					boolean didSomething = false;
					while (cacheSize > MAX_CACHE_SIZE)
					{
						didSomething = true;
						bbs.remove(bbs.size() - 1);
						cacheSize -= (Integer)entry.getKey();
						if (bbs.size() == 0)
						{
							cache.remove(entry.getKey());
							break;
						}
					}

					if (!didSomething)
					{
						break;
					}
				}
			}

			return retval;
		}
	}

	private void deallocateByteBuffer(ByteBuffer bb)
	{
		synchronized (cache)
		{
			ByteBuffer actual = sliceToActual.get(bb);
			sliceToActual.remove(bb);

			ArrayList<ByteBuffer> bbs = cache.get(actual.capacity());
			if (bbs == null)
			{
				bbs = new ArrayList<ByteBuffer>();
				bbs.add(actual);
				cache.put(actual.capacity(), bbs);
			}
			else
			{
				bbs.add(actual);
			}
		}
	}

	private void deleteFiles(int highFile) throws IOException
	{
		final ArrayList<Path> files = new ArrayList<Path>();
		int split = this.fn.lastIndexOf('/');
		String dir = this.fn.substring(0, split);
		String relative = this.fn.substring(split + 1);
		File dirFile = new File(dir);
		File[] files2 = dirFile.listFiles();
		for (File f : files2)
		{
			if (f.getName().startsWith(relative + "."))
			{
				files.add(f.toPath());
			}
		}

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
					fcs_size.decrementAndGet();
				}

				if (!new File(fn + "." + suffix).delete())
				{
					throw new IOException("Failed to delete file " + fn + "." + suffix + " during truncate() operation");
				}
			}
		}
	}

	private FileChannel getFC(int suffix, boolean register) throws IOException
	{
		FileChannel retval = fcs.get(suffix);
		if (retval != null)
		{
			return retval;
		}

		RandomAccessFile raf = null;
		while (true)
		{
			try
			{
				raf = new RandomAccessFile(fn + "." + suffix, "rw");
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
		retval = raf.getChannel();
		FileChannel retval2 = fcs.putIfAbsent(suffix, retval);
		if (retval2 == null)
		{
			fcs_size.incrementAndGet();
		}
		else
		{
			retval.close();
			retval = retval2;
		}

		return retval;
	}

	private FileChannel getFC(int suffix, int counts) throws IOException
	{
		int last = lastRequest.get();
		lastRequest.set(suffix);

		if (suffix - last > MAX_SPAN || suffix - last < -MAX_SPAN)
		{
			aoOK.set(false);
		}

		accesses.getAndAdd(counts);
		FileChannel retval = fcs.get(suffix);
		if (retval != null)
		{
			return retval;
		}

		RandomAccessFile raf = null;
		while (true)
		{
			try
			{
				raf = new RandomAccessFile(fn + "." + suffix, "rw");
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
		retval = raf.getChannel();
		FileChannel retval2 = fcs.putIfAbsent(suffix, retval);
		if (retval2 == null)
		{
			fcs_size.incrementAndGet();
		}
		else
		{
			retval.close();
			retval = retval2;
		}

		return retval;
	}

	private FileChannel getFC(int suffix, int counts, ArrayList<Integer> cols, int layoutSize) throws IOException
	{
		aoBlocks.remove(suffix);
		int page1 = suffix * 3;
		int page2 = suffix * 3 + 1;
		int page3 = suffix * 3 + 2;
		int col1 = page1 % layoutSize - 1;
		int col2 = page2 % layoutSize - 1;
		int col3 = page3 % layoutSize - 1;
		boolean b1 = cols.contains(col1);
		boolean b2 = cols.contains(col2);
		boolean b3 = cols.contains(col3);
		int count = 0;
		if (b1)
		{
			count++;
		}

		if (b2)
		{
			count++;
		}

		if (b3)
		{
			count++;
		}

		int MAX = ResourceManager.MAX_FCS - aoBlocks.size();
		if (count == 0)
		{
			count = 1;
		}
		int per = MAX / count;
		long z = CompressedFileChannel.this.size();
		z = z / 128;
		z = z / 1024;
		z--;
		z = z / 3;

		if (b1)
		{
			int i = 500 / cols.size();
			int j = 0;
			while (j < per && aoBlocks.size() < ResourceManager.MAX_FCS)
			{
				int page = page1 + layoutSize * i;
				int block = page / 3;
				if (block <= z)
				{
					aoBlocks.put(block, block);
				}
				else
				{
					break;
				}
				i++;
				j++;
			}
		}

		if (b2)
		{
			int i = 500 / cols.size();
			int j = 0;
			while (j < per && aoBlocks.size() < ResourceManager.MAX_FCS)
			{
				int page = page2 + layoutSize * i;
				int block = page / 3;
				if (block <= z)
				{
					aoBlocks.put(block, block);
				}
				else
				{
					break;
				}
				i++;
				j++;
			}
		}

		if (b3)
		{
			int i = 500 / cols.size();
			int j = 0;
			while (j < per && aoBlocks.size() < ResourceManager.MAX_FCS)
			{
				int page = page3 + layoutSize * i;
				int block = page / 3;
				if (block <= z)
				{
					aoBlocks.put(block, block);
				}
				else
				{
					break;
				}
				i++;
				j++;
			}
		}

		return getFC(suffix, counts);
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

	private void trimFCS(int suffix) throws IOException
	{
		// HRDBMSWorker.logger.debug("Entering trim for " + fn + " with size " +
		// fcs.size());
		// HRDBMSWorker.logger.debug("Starting trim thread");
		Iterator<Map.Entry<Integer, FileChannel>> iterator = fcs.entrySet().iterator();
		while (fcs_size.get() > MAX_FCS && iterator.hasNext())
		{
			Map.Entry entry = iterator.next();
			int block = (Integer)entry.getKey();
			if (suffix != -1 && block == suffix)
			{
				continue;
			}
			lock.readLock().lock();
			if (writeLocks.putIfAbsent(block, block) != null)
			{
				lock.readLock().unlock();
				continue;
			}

			try
			{
				int mod = block & 31;
				synchronized (inUse[mod])
				{
					if (inUse[mod].containsKey(block))
					{
						// if (inUse[mod].size() > MAX_FCS)
						// {
						// HRDBMSWorker.logger.debug("InUse[mod] = " +
						// inUse[mod]);
						// }

						writeLocks.remove(block);
						lock.readLock().unlock();
						continue;
					}

					// HRDBMSWorker.logger.debug("Trim got inUse lock");
					FileChannel fc = (FileChannel)entry.getValue();
					fc.close();
					fcs.remove(entry.getKey());
					fcs_size.decrementAndGet();
					Boolean needsFlush = modded.get(entry.getKey());
					if (needsFlush != null && needsFlush)
					{
						// fc.force(false);
						modded.remove(entry.getKey());
					}
				}
				writeLocks.remove(block);
			}
			catch (Exception e)
			{
				HRDBMSWorker.logger.debug("TRIM EXCEPTION", e);
				writeLocks.remove(block);
			}

			lock.readLock().unlock();
		}

		// HRDBMSWorker.logger.debug("Trim thread trimmed " + trimmed +
		// " entries, " + writeLocked + " were write locked, " + inUseLocked +
		// " were in use locked for a total of " + iterations + " iteration, " +
		// (trimmed + writeLocked + inUseLocked) +
		// " of which were accounted for " + ", initial size was " + size +
		// ", estimated initial size was " + estSize + ", target size was " +
		// MAX_FCS);
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
					FileChannel fc = getFC(block, 1);
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
			fcs_size.set(0);
		}
		catch (Exception e)
		{
			HRDBMSWorker.logger.debug("", e);
			lock.writeLock().unlock();
			throw e;
		}
		lock.writeLock().unlock();
	}

	public class OpenAheadThread extends HRDBMSThread
	{
		private boolean colTable = false;

		public OpenAheadThread()
		{
		}

		public OpenAheadThread(boolean colTable)
		{
			this.colTable = colTable;
		}

		@Override
		public void run()
		{
			if (!colTable)
			{
				try
				{
					int i = 0;
					int j = lastRequest.get() + 500;
					long z = CompressedFileChannel.this.size();
					z = z / 128;
					z = z / 1024;
					z--;
					z = z / 3;
					while (i < ResourceManager.MAX_FCS && j <= z)
					{
						CompressedFileChannel.this.getFC(j++, false);
						i++;
					}
				}
				catch (Exception e)
				{
					HRDBMSWorker.logger.debug("", e);
				}

				oaInProgress.set(false);
			}
			else
			{
				try
				{
					int i = 0;

					for (Integer block : aoBlocks.keySet())
					{
						CompressedFileChannel.this.getFC(block, false);
						aoBlocks.remove(block);
						i++;

						if (i == ResourceManager.MAX_FCS)
						{
							break;
						}
					}
				}
				catch (Exception e)
				{
					HRDBMSWorker.logger.debug("", e);
				}

				oaInProgress.set(false);
			}
		}
	}

	public class TrimFCSThread extends HRDBMSThread
	{
		private final int suffix;

		public TrimFCSThread()
		{
			suffix = -1;
		}

		public TrimFCSThread(int suffix, FileChannel retval)
		{
			this.suffix = suffix;
		}

		@Override
		public void run()
		{
			try
			{
				trimFCS(suffix);
				trimInProgress.set(false);
				// if (suffix != -1)
				// {
				// fcs.put(suffix, retval);
				// fcs_size.incrementAndGet();
				// }
			}
			catch (Exception e)
			{
				HRDBMSWorker.logger.debug("", e);
			}
		}
	}
}
