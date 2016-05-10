package com.exascale.threads;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import com.exascale.filesystem.Block;
import com.exascale.filesystem.CompressedFileChannel;
import com.exascale.filesystem.Page;
import com.exascale.filesystem.SparseCompressedFileChannel2;
import com.exascale.managers.FileManager;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.tables.Schema;
import com.exascale.tables.Transaction;

public class ReadThread extends HRDBMSThread
{
	private Page p;
	private Block b;
	private ByteBuffer bb;
	private boolean ok = true;
	private Schema schema;
	private ConcurrentHashMap<Integer, Schema> schemaMap;
	private Transaction tx;
	private ArrayList<Integer> fetchPos;
	private ArrayList<Integer> cols;
	private int layoutSize;
	private int rank = -1;
	private int rankSize = -1;
	private ArrayList<ReadThread> subThreads;
	private boolean consecutive = false;
	private int num = -1;
	private ArrayList<Integer> indexes;
	private Page[] bp;
	
	public ReadThread(ArrayList<ReadThread> subThreads)
	{
		this.subThreads = subThreads;
	}
	
	public ReadThread(Page p, int num, ArrayList<Integer> indexes, Page[] bp, int rank, int rankSize)
	{
		this.p = p;
		this.num = num;
		this.indexes = indexes;
		this.bp = bp;
		this.consecutive = true;
		this.rank = rank;
		this.rankSize = rankSize;
	}

	public ReadThread(Page p, Block b, ByteBuffer bb)
	{
		this.description = "Read thread for buffer Manager";
		this.setWait(false);
		this.p = p;
		this.b = b;
		this.bb = bb;
	}

	public ReadThread(Page p, Block b, ByteBuffer bb, ArrayList<Integer> cols, int layoutSize)
	{
		this.description = "Read thread for buffer Manager";
		this.setWait(false);
		this.p = p;
		this.b = b;
		this.bb = bb;
		this.cols = cols;
		this.layoutSize = layoutSize;
	}

	public ReadThread(Page p, Block b, ByteBuffer bb, Schema schema, ConcurrentHashMap<Integer, Schema> schemaMap, Transaction tx, ArrayList<Integer> fetchPos)
	{
		this.description = "Read thread for buffer Manager";
		this.setWait(false);
		this.p = p;
		this.b = b;
		this.bb = bb;
		this.schema = schema;
		this.schemaMap = schemaMap;
		this.tx = tx;
		this.fetchPos = fetchPos;
	}
	
	public void setRank(int rank)
	{
		this.rank = rank;
	}
	
	public void setRankSize(int rankSize)
	{
		this.rankSize = rankSize;
	}

	public boolean getOK()
	{
		return ok;
	}

	@Override
	public void run()
	{	
		try
		{
			if (subThreads != null)
			{
				for (ReadThread thread : subThreads)
				{
					thread.join();
				}
				
				this.terminate();
				return;
			}
			
			if (consecutive)
			{
				if (rank > 0 && rankSize > 1)
				{
					try
					{
						double pos = 1.0 - (((rank-1) * 1.0) / ((rankSize-1) * 1.0));
						int pri = (int)(pos * (Thread.MAX_PRIORITY - Thread.NORM_PRIORITY) + Thread.NORM_PRIORITY);
						Thread.currentThread().setPriority(pri);
					}
					catch(Exception f)
					{
						HRDBMSWorker.logger.debug("Error setting priority: Rank is " + rank + " RankSize is " + rankSize);
						throw f;
					}
				}
				
				b = p.block();
				bb = p.buffer();
				final FileChannel fc = FileManager.getFile(b.fileName());
				ByteBuffer[] bbs = new ByteBuffer[num];
				bbs[0] = bb;
				int i = 1;
				while (i < num)
				{
					bbs[i] = bp[indexes.get(i)].buffer();
					i++;
				}
				
				((SparseCompressedFileChannel2)fc).read(bbs, ((long)b.number()) * bb.capacity());
				
				i = 0;
				while (i < num)
				{
					bp[indexes.get(i)].setReady();
					i++;
				}
				
				this.terminate();
				return;
			}
			
			if (rank > 0 && rankSize > 1)
			{
				try
				{
					double pos = 1.0 - (((rank-1) * 1.0) / ((rankSize-1) * 1.0));
					int pri = (int)(pos * (Thread.MAX_PRIORITY - Thread.NORM_PRIORITY) + Thread.NORM_PRIORITY);
					Thread.currentThread().setPriority(pri);
				}
				catch(Exception f)
				{
					HRDBMSWorker.logger.debug("Error setting priority: Rank is " + rank + " RankSize is " + rankSize);
					throw f;
				}
			}
			
			bb.clear();
			bb.position(0);

			final FileChannel fc = FileManager.getFile(b.fileName());
			// if (b.number() * bb.capacity() >= fc.size())
			// {
			// HRDBMSWorker.logger.debug("Tried to read from " + b.fileName() +
			// " at block = " + b.number() +
			// " but it was past the range of the file");
			// ok = false;
			// }

			if (cols != null)
			{
				if (FileManager.SCFC)
				{
					((SparseCompressedFileChannel2)fc).read(bb, ((long)b.number()) * bb.capacity(), cols, layoutSize);
				}
				else
				{
					((CompressedFileChannel)fc).read(bb, ((long)b.number()) * bb.capacity(), cols, layoutSize);
				}
			}
			else
			{
				fc.read(bb, ((long)b.number()) * bb.capacity());
			}
			p.setReady();

			if (schema != null)
			{
				synchronized (schema)
				{
					tx.read2(p.block(), schema, p);
				}

				schemaMap.put(p.block().number(), schema);
				schema.prepRowIter(fetchPos);
			}
			this.terminate();
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.warn("I/O error occurred trying to read file: " + b.fileName() + ":" + b.number(), e);
			ok = false;
			this.terminate();
			return;
		}
		return;
	}
}