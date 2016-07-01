package com.exascale.threads;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import com.exascale.filesystem.Block;
import com.exascale.filesystem.Page;
import com.exascale.filesystem.SparseCompressedFileChannel2;
import com.exascale.managers.FileManager;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.tables.Schema;
import com.exascale.tables.Transaction;

public class Read3Thread extends HRDBMSThread
{
	private Page p;
	private Page p2;
	private Page p3;
	private Block b;
	private ByteBuffer bb;
	private ByteBuffer bb2;
	private ByteBuffer bb3;
	private boolean ok = true;
	private Schema schema1;
	private Schema schema2;
	private Schema schema3;
	private ConcurrentHashMap<Integer, Schema> schemaMap;
	private Transaction tx;
	private ArrayList<Integer> fetchPos;
	private ArrayList<ReadThread> rThreads;
	private ArrayList<Integer> cols;
	private int layoutSize;

	public Read3Thread(ArrayList<ReadThread> rThreads)
	{
		this.rThreads = rThreads;
	}

	public Read3Thread(Page p, Page p2, Page p3, Block b, ByteBuffer bb, ByteBuffer bb2, ByteBuffer bb3)
	{
		this.description = "Read thread for buffer Manager";
		this.setWait(false);
		this.p = p;
		this.p2 = p2;
		this.p3 = p3;
		this.b = b;
		this.bb = bb;
		this.bb2 = bb2;
		this.bb3 = bb3;
	}

	public Read3Thread(Page p, Page p2, Page p3, Block b, ByteBuffer bb, ByteBuffer bb2, ByteBuffer bb3, ArrayList<Integer> cols, int layoutSize)
	{
		this.description = "Read thread for buffer Manager";
		this.setWait(false);
		this.p = p;
		this.p2 = p2;
		this.p3 = p3;
		this.b = b;
		this.bb = bb;
		this.bb2 = bb2;
		this.bb3 = bb3;
		this.cols = cols;
		this.layoutSize = layoutSize;
	}

	public Read3Thread(Page p, Page p2, Page p3, Block b, ByteBuffer bb, ByteBuffer bb2, ByteBuffer bb3, Schema schema1, Schema schema2, Schema schema3, ConcurrentHashMap<Integer, Schema> schemaMap, Transaction tx, ArrayList<Integer> fetchPos)
	{
		this.description = "Read thread for buffer Manager";
		this.setWait(false);
		this.p = p;
		this.p2 = p2;
		this.p3 = p3;
		this.b = b;
		this.bb = bb;
		this.bb2 = bb2;
		this.bb3 = bb3;
		this.schema1 = schema1;
		this.schema2 = schema2;
		this.schema3 = schema3;
		this.schemaMap = schemaMap;
		this.tx = tx;
		this.fetchPos = fetchPos;
	}

	public boolean getOK()
	{
		return ok;
	}

	@Override
	public void run()
	{
		// HRDBMSWorker.logger.debug("Read3Thread starting");
		try
		{
			if (rThreads != null)
			{
				for (ReadThread thread : rThreads)
				{
					thread.join();
				}

				return;
			}

			// if (rank > 0 && rankSize > 1)
			// {
			// double pos = 1.0 - (((rank-1) * 1.0) / ((rankSize-1) * 1.0));
			// int pri = (int)(pos * (Thread.MAX_PRIORITY -
			// Thread.NORM_PRIORITY) + Thread.NORM_PRIORITY);
			// Thread.currentThread().setPriority(pri);
			// }

			bb.clear();
			bb2.clear();
			bb3.clear();
			bb.position(0);
			bb2.position(0);
			bb3.position(0);

			final FileChannel fc = FileManager.getFile(b.fileName());
			// if (b.number() * bb.capacity() >= fc.size())
			// {
			// HRDBMSWorker.logger.debug("Tried to read from " + b.fileName() +
			// " at block = " + b.number() +
			// " but it was past the range of the file");
			// ok = false;
			// }

			if (cols == null)
			{
				((SparseCompressedFileChannel2)fc).read3(bb, bb2, bb3, ((long)b.number()) * bb.capacity());
			}
			else
			{
				((SparseCompressedFileChannel2)fc).read3(bb, bb2, bb3, ((long)b.number()) * bb.capacity(), cols, layoutSize);
			}
			p.setReady();
			p2.setReady();
			p3.setReady();

			if (schema1 != null)
			{
				synchronized (schema1)
				{
					tx.read2(p.block(), schema1, p);
				}

				schemaMap.put(p.block().number(), schema1);
				schema1.prepRowIter(fetchPos);

				synchronized (schema2)
				{
					tx.read2(p2.block(), schema2, p2);
				}

				schemaMap.put(p2.block().number(), schema2);
				schema2.prepRowIter(fetchPos);

				synchronized (schema3)
				{
					tx.read2(p3.block(), schema3, p3);
				}

				schemaMap.put(p3.block().number(), schema3);
				schema3.prepRowIter(fetchPos);
				// HRDBMSWorker.logger.debug("Long Read3Thread ending");
			}
			else
			{
				// HRDBMSWorker.logger.debug("Short Read3Thread ending");
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

	public void setRank(int rank)
	{
	}

	public void setRankSize(int rankSize)
	{
	}
}