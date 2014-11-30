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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import com.exascale.managers.BufferManager;
import com.exascale.managers.FileManager;
import com.exascale.managers.HRDBMSWorker;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import net.jpountz.lz4.LZ4SafeDecompressor;

public class CompressedFileChannel extends FileChannel
{
	private String fn;
	private LinkedHashMap<Integer, FileChannel> fcs = new LinkedHashMap<Integer, FileChannel>();
	private ConcurrentHashMap<Integer, Boolean> modded = new ConcurrentHashMap<Integer, Boolean>();
	private int length; //in 128k pages
	private int bufferedBlock = -1;
	private byte[] buffer;
	private int bufferSize;
	private static LZ4Factory factory;
	private long pos = 0;
	
	static
	{
		factory = LZ4Factory.nativeInstance();
	}
	
	public CompressedFileChannel(File file) throws IOException
	{
		this.fn = file.getAbsolutePath();
		int low = 0;
		File f = new File(fn + "." + low);
		if (!f.exists())
		{
			length = 0;
			return;
		}
		
		int high = 44739242;
		int mid = (high - low) / 2;
		while (high != low)
		{
			if (high - low == 1)
			{
				f = new File(fn + "." + high);
				if (f.exists())
				{
					low = high;
				}
				else
				{
					high = low;
				}
			}
			else
			{
				f = new File(fn + "." + mid);
				if (f.exists())
				{
					low = mid;
				}
				else
				{
					high = mid - 1;
				}
			}
			
			mid = ((high - low) / 2) + low;
		}
		
		length = high + 1; //length in 3 page blocks
		LZ4SafeDecompressor decomp = factory.safeDecompressor();
		FileChannel fc = getFC(low);
		ByteBuffer bb = ByteBuffer.allocate((int)fc.size());
		fc.read(bb, 0);
		byte[] target = new byte[128 * 1024 * 3]; //max 3 pages
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

	@Override
	public synchronized void force(boolean arg0) throws IOException
	{
		for (Map.Entry entry : modded.entrySet())
		{
			if (((Boolean)entry.getValue()))
			{
				FileChannel fc = getFC((Integer)entry.getKey());
				try
				{
					fc.force(false);
				}
				catch(ClosedChannelException e) {}
				modded.remove((Integer)entry.getKey());
			}
		}
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
	public synchronized int read(ByteBuffer arg0, long arg1) throws IOException
	{
		if (arg0.capacity() != 128 * 1024)
		{
			throw new IOException("Invalid read length of " + arg0.capacity());
		}
		
		int page = (int)(arg1 / (128 * 1024));
		int offset = (int)(arg1 % (128 * 1024));
		if (offset != 0)
		{
			throw new IOException("Invalid read offset of " + offset);
		}
		int block = page / 3;
		offset += ((page % 3) * 128 * 1024);
		if (block == bufferedBlock)
		{
			System.arraycopy(buffer, offset, arg0.array(), 0, arg0.capacity());
			return arg0.capacity();
		}
		int actualBlocks = length / 3;
		if (length % 3 != 0)
		{
			actualBlocks++;
		}
		int highFileNum = actualBlocks - 1;
		if (highFileNum == block)
		{
			//may be partial block
			LZ4SafeDecompressor decomp = factory.safeDecompressor();
			FileChannel fc = getFC(block);
			ByteBuffer bb = ByteBuffer.allocate((int)fc.size());
			fc.read(bb, 0);
			byte[] target = new byte[128 * 1024 * 3]; //max 3 pages
			int bytes = decomp.decompress(bb.array(), 0, (int)fc.size(), target, 0, 128 * 1024 * 3);
			System.arraycopy(target, offset, arg0.array(), 0, arg0.capacity());
			bufferedBlock = block;
			buffer = target;
			bufferSize = bytes / (128 * 1024);
		}
		else
		{
			//full block
			LZ4FastDecompressor decomp = factory.fastDecompressor();
			FileChannel fc = getFC(block);
			ByteBuffer bb = ByteBuffer.allocate((int)(fc.size()));
			fc.read(bb, 0);
			byte[] target = new byte[128 * 1024 * 3]; //3 pages
			decomp.decompress(bb.array(), target);
			System.arraycopy(target, offset, arg0.array(), 0, arg0.capacity());
			bufferedBlock = block;
			buffer = target;
			bufferSize = 3;
		}
		
		return arg0.capacity();
	}

	@Override
	public long read(ByteBuffer[] arg0, int arg1, int arg2) throws IOException
	{
		throw new IOException("Unsupported operation");
	}

	@Override
	public synchronized long size() throws IOException
	{
		return length * 128L * 1024L;
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
	public synchronized FileChannel truncate(long arg0) throws IOException
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
		
		while (actualHighFileNum > desiredHighFileNum)
		{
			deleteFile(actualHighFileNum);
			actualHighFileNum--;
		}
		
		if (desiredPages % 3 != 0)
		{
			//partial last block
			FileChannel fc = getFC(actualHighFileNum);
			LZ4SafeDecompressor decomp = factory.safeDecompressor();
			ByteBuffer bb = ByteBuffer.allocate((int)fc.size());
			fc.read(bb, 0);
			byte[] target = new byte[128 * 1024 * 3]; //max 3 pages
			int bytes = decomp.decompress(bb.array(), 0, (int)fc.size(), target, 0, 128 * 1024 * 3);
			byte[] trunced = new byte[(desiredPages % 3) * 128 * 1024];
			System.arraycopy(target, 0, trunced, 0, trunced.length);
			fc.truncate(0);
			LZ4Compressor comp = factory.highCompressor();
			byte[] comped = comp.compress(trunced);
			bb = ByteBuffer.wrap(comped);
			fc.write(bb, 0);
			fc.force(false);
			
			if (bufferedBlock == actualHighFileNum)
			{
				bufferSize = desiredPages % 3;
			}
		}
		
		length = (int)(arg0 / (128 * 1024));
		return this;
	}

	@Override
	public FileLock tryLock(long arg0, long arg1, boolean arg2) throws IOException
	{
		throw new IOException("Unsupported operation");
	}

	@Override
	public synchronized int write(ByteBuffer arg0) throws IOException
	{
		write(arg0, pos);
		pos += arg0.capacity();
		return arg0.capacity();
	}
	
	private synchronized int multiPageWrite(ByteBuffer input, long offset) throws IOException
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
		
		return input.capacity();
	}

	@Override
	public synchronized int write(ByteBuffer arg0, long arg1) throws IOException
	{
		//multi-page write
		if (arg0.capacity() > (128 * 1024))
		{
			return multiPageWrite(arg0, arg1);
		}
		
		//read if not buffered
		int page = (int)(arg1 / (128 * 1024));
		int offset = (int)(arg1 % (128 * 1024));
		int block = page / 3;
		
		//HRDBMSWorker.logger.debug("Trying to write to page = " + page + " with current length = " + length); //DEBUG
		offset += ((page % 3) * 128 * 1024);
		if (block == bufferedBlock)
		{
			//HRDBMSWorker.logger.debug("The block was buffered"); //DEBUG
			System.arraycopy(arg0.array(), 0, buffer, offset, arg0.capacity());
			if (offset == 128 * 1024 && bufferSize < 2)
			{
				bufferSize = 2;
				length += 1;
				//HRDBMSWorker.logger.debug("The buffered block was extended by 1 page"); //DEBUG
			}
			else if (offset == 2 * 128 * 1024 && bufferSize < 3)
			{
				length += (3 - bufferSize);
				//HRDBMSWorker.logger.debug("The buffered block was extended by " + (3 - bufferSize) + " pages"); //DEBUG
				bufferSize = 3;
			}
			
			//if it's a full block
			if (bufferSize == 3)
			{
				//compress it
				LZ4Compressor comp = factory.highCompressor();
				//write it
				FileChannel fc = getFC(block);
				fc.truncate(0);
				byte[] data = comp.compress(buffer);
				ByteBuffer bb = ByteBuffer.wrap(data);
				fc.write(bb, 0);
				//mark modded
				modded.put(block, true);
				//if (modded.size() > 1024)
				//{
				//	flushModded();
				//}
				return arg0.capacity();
			}
			else
			{
				//figure out what of it is valid
				byte[] valid = new byte[128 * 1024 * bufferSize];
				System.arraycopy(buffer, 0, valid, 0, valid.length);
				
				//compress it
				LZ4Compressor comp = factory.highCompressor();
				//write it
				FileChannel fc = getFC(block);
				fc.truncate(0);
				byte[] data = comp.compress(valid);
				ByteBuffer bb = ByteBuffer.wrap(data);
				fc.write(bb, 0);
				//mark modded
				modded.put(block, true);
				//if (modded.size() > 1024)
				//{
				//	flushModded();
				//}
				return arg0.capacity();
			}
		}
		
		int actualBlocks = length / 3;
		if (length % 3 != 0)
		{
			actualBlocks++;
		}
		int highFileNum = actualBlocks - 1;
		if (block > highFileNum)
		{
			if (highFileNum >= 0)
			{
				//if last block is not full, fill it out
				//HRDBMSWorker.logger.debug("We have a write that is past the last block of the file"); //DEBUG
				FileChannel fc3 = getFC(highFileNum);
				LZ4SafeDecompressor decomp = factory.safeDecompressor();
				ByteBuffer bb2 = ByteBuffer.allocate((int)fc3.size());
				fc3.read(bb2, 0);
				byte[] target = new byte[128 * 1024 * 3]; //max 3 pages
				int bytes = -1;
				try
				{
					bytes = decomp.decompress(bb2.array(), 0, (int)fc3.size(), target, 0, 128 * 1024 * 3);
				}
				catch(Exception e)
				{
					HRDBMSWorker.logger.debug("", e);
					HRDBMSWorker.logger.debug("We tried to read from " + fn + "." + highFileNum + ", which contained " + fc3.size() + " bytes");
					HRDBMSWorker.logger.debug("Length is currently " + length);
					HRDBMSWorker.logger.debug("HighFileNum = " + highFileNum);
					throw e;
				}
				
				if (bytes < (3 * 128 * 1024))
				{
					//HRDBMSWorker.logger.debug("The last block of the file needs to be filled by adding " + ((3 * 128 * 1024 - bytes) / (128 * 1024)) + " pages"); //DEBUG
					byte[] fullBlock = new byte[3 * 128 * 1024];
					System.arraycopy(target, 0, fullBlock, 0, target.length);
					
					length += ((3 * 128 * 1024 - bytes) / (128 * 1024));
					fc3.truncate(0);
					LZ4Compressor comp = factory.highCompressor();
					byte[] data = comp.compress(fullBlock);
					ByteBuffer bb = ByteBuffer.wrap(data);
					fc3.write(bb, 0);
					if (bufferedBlock == highFileNum)
					{
						buffer = fullBlock;
						bufferSize = 3;
					}
					modded.put(highFileNum, true);
					//if (modded.size() > 1024)
					//{
					//	flushModded();
					//}
				}
			}
			
			//new block
			LZ4Compressor comp = factory.highCompressor();
			if (block > highFileNum + 1)
			{
				byte[] blank = new byte[3 * 128 * 1024];
				//write it
				byte[] data = comp.compress(blank);
				while (block > highFileNum + 1)
				{
					//HRDBMSWorker.logger.debug("Need to write a new empty block. BlockNum = " + highFileNum + 1); //DEBUG
					FileChannel fc2 = getFC(highFileNum + 1);
					ByteBuffer bb = ByteBuffer.wrap(data);
					fc2.write(bb, 0);
					//mark modded
					modded.put(highFileNum + 1, true);
					//if (modded.size() > 1024)
					//{
					//	flushModded();
					//}
					
					highFileNum++;
					length += 3;
				}
			}
			
			//need to write to new block (1 of the 3 slots)
			if (offset == 0)
			{
				byte[] data = comp.compress(arg0.array());
				ByteBuffer bb = ByteBuffer.wrap(data);
				FileChannel fc = getFC(block);
				fc.write(bb, 0);
				modded.put(block, true);
				//if (modded.size() > 1024)
				//{
				//	flushModded();
				//}
				
				//HRDBMSWorker.logger.debug("Writing a new block with only 1 page"); //DEBUG
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
				//if (modded.size() > 1024)
				//{
				//	flushModded();
				//}
				
				//HRDBMSWorker.logger.debug("Writing a new block with only 2 pages"); //DEBUG
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
				//if (modded.size() > 1024)
				//{
				//	flushModded();
				//}
				
				//HRDBMSWorker.logger.debug("Writing a full new block"); //DEBUG
				length += 3;
			}
			
			return arg0.capacity();
		}
		else if (highFileNum == block)
		{
			//may be partial block
			//HRDBMSWorker.logger.debug("The write is taking place on the last block"); //DEBUG
			LZ4SafeDecompressor decomp = factory.safeDecompressor();
			FileChannel fc = getFC(block);
			ByteBuffer bb = ByteBuffer.allocate((int)fc.size());
			fc.read(bb, 0);
			byte[] target = new byte[128 * 1024 * 3]; //max 3 pages
			int bytes = decomp.decompress(bb.array(), 0, (int)fc.size(), target, 0, 128 * 1024 * 3);
			System.arraycopy(arg0.array(), 0, target, offset, arg0.capacity());
			bufferedBlock = block;
			buffer = target;
			bufferSize = Math.max(bytes / (128 * 1024), (offset + 128 * 1024) / (128 * 1024));
			if (bufferSize != (bytes / (128 * 1024)))
			{
				length += (bufferSize - (bytes / (128 * 1024)));
				//HRDBMSWorker.logger.debug("The last block is being extended by " + (bufferSize - (bytes / (128 * 1024))) + " pages"); //DEBUG
			}
			byte[] valid = new byte[128 * 1024 * bufferSize];
			System.arraycopy(buffer, 0, valid, 0, valid.length);
			
			//compress it
			LZ4Compressor comp = factory.highCompressor();
			//write it
			fc.truncate(0);
			byte[] data = comp.compress(valid);
			bb = ByteBuffer.wrap(data);
			fc.write(bb, 0);
			//mark modded
			modded.put(block, true);
			//if (modded.size() > 1024)
			//{
			//	flushModded();
			//}
			return arg0.capacity();
		}
		else
		{
			//full block
			//HRDBMSWorker.logger.debug("The write is in the middle of an existing full block"); //DEBUG
			LZ4FastDecompressor decomp = factory.fastDecompressor();
			FileChannel fc = getFC(block);
			ByteBuffer bb = ByteBuffer.allocate((int)(fc.size()));
			fc.read(bb, 0);
			byte[] target = new byte[128 * 1024 * 3]; //3 pages
			try
			{
				decomp.decompress(bb.array(), target);
			}
			catch(Exception e)
			{
				HRDBMSWorker.logger.debug("", e);
				HRDBMSWorker.logger.debug("We tried to read a full block from " + fn + "." + block + ", which contained " + fc.size() + " bytes");
				HRDBMSWorker.logger.debug("Length is currently " + length);
				HRDBMSWorker.logger.debug("HighFileNum = " + highFileNum);
				throw e;
			}
			System.arraycopy(arg0.array(), 0, target, offset, arg0.capacity());
			bufferedBlock = block;
			buffer = target;
			bufferSize = 3;
			//compress it
			LZ4Compressor comp = factory.highCompressor();
			//write it
			fc.truncate(0);
			byte[] data = comp.compress(buffer);
			bb = ByteBuffer.wrap(data);
			fc.write(bb, 0);
			//mark modded
			modded.put(block, true);
			//if (modded.size() > 1024)
			//{
			//	flushModded();
			//}
			return arg0.capacity();
		}
	}

	@Override
	public long write(ByteBuffer[] arg0, int arg1, int arg2) throws IOException
	{
		throw new IOException("Unsupported operation");
	}

	@Override
	protected synchronized void implCloseChannel() throws IOException
	{
		for (Map.Entry entry : modded.entrySet())
		{
			if (((Boolean)entry.getValue()))
			{
				FileChannel fc = getFC((Integer)entry.getKey());
				try
				{
					fc.force(false);
				}
				catch(ClosedChannelException e) {}
			}
		}
			
		for (FileChannel fc : fcs.values())
		{
			fc.close();
		}
		
		modded = null;
		fcs = null;
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
		if (fcs.size() > 1024)
		{
			trimFCS();
		}
		
		fcs.put(suffix, retval);
		return retval;
	}
	
	private void flushModded() throws IOException
	{
		for (Map.Entry entry : modded.entrySet())
		{
			if ((Boolean)entry.getValue())
			{
				FileChannel fc = getFC((Integer)entry.getKey());
				try
				{
					fc.force(false);
				}
				catch(ClosedChannelException e) {}
				modded.remove(entry.getKey());
			}
		}
	}
	
	private void deleteFile(int suffix) throws IOException
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
	
	private void trimFCS() throws IOException
	{
		while (fcs.size() > 1024)
		{
			Map.Entry entry = fcs.entrySet().iterator().next();
			FileChannel fc = (FileChannel)entry.getValue();
			Boolean needsFlush = modded.get(entry.getKey());
			if (needsFlush != null && needsFlush)
			{
				fc.force(false);
				modded.remove(entry.getKey());
			}
			fcs.remove(entry.getKey());
			fc.close();
		}
	}
	
	public synchronized void copyFromFC(CompressedFileChannel source) throws Exception
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
}
