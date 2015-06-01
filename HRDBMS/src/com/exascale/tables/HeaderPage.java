package com.exascale.tables;

import java.nio.ByteBuffer;
import com.exascale.filesystem.Page;
import com.exascale.logging.InsertLogRec;
import com.exascale.managers.LockManager;
import com.exascale.managers.LogManager;

public class HeaderPage
{
	private static final int ROW_HEADER_ENTRIES_PER_PAGE = Page.BLOCK_SIZE >> 2; // largest
	// free
	// size
	private static final int COL_HEADER_ENTRIES_PER_PAGE = Page.BLOCK_SIZE >> 3; // largest
	private int device = -1;
	private int node = -1;
	// free
	// size
	// and
	// column
	// id
	private int blockNum;
	private int off;
	private final Page p;

	public HeaderPage(Page p, int type)
	{
		this.p = p;
		off = 0;
		if (p.block().number() == 0)
		{
			node = p.getInt(0); // first page contains node and device number
			device = p.getInt(4);
		}

		if (type == Schema.TYPE_ROW)
		{
			if (p.block().number() == 0)
			{
				blockNum = Schema.HEADER_SIZE + 1;
				off = (Schema.HEADER_SIZE + 3) * 4;
			}
			else
			{
				blockNum = p.block().number() * ROW_HEADER_ENTRIES_PER_PAGE - 2;
			}
		}
		else
		{
			if (p.block().number() == 0)
			{
				blockNum = Schema.HEADER_SIZE + 1;
				off = (Schema.HEADER_SIZE + 2) << 3;
			}
			else
			{
				blockNum = p.block().number() * COL_HEADER_ENTRIES_PER_PAGE - 1;
			}
		}
	}

	public int findPage(int data)
	{
		int offset = off;
		int num = blockNum;
		// HRDBMSWorker.logger.debug("Looking for a page with space.  We need "
		// + data + " bytes");
		while (offset < Page.BLOCK_SIZE)
		{
			int space = p.getInt(offset);
			// HRDBMSWorker.logger.debug("Page " + num + " has " + space +
			// " free bytes according to @" + offset);
			if (data == -1)
			{
				return -1;
			}

			if (data <= space)
			{
				return num;
			}

			if (space == -1)
			{
				return 0;
			}

			offset += 4;
			num++;
		}

		return -1;
	}

	public int findPage(int data, int after)
	{
		int offset = off;
		int num = blockNum;
		// HRDBMSWorker.logger.debug("Looking for a page with space.  We need "
		// + data + " bytes");
		while (num < after)
		{
			offset += 4;
			num++;
		}
		while (offset < Page.BLOCK_SIZE)
		{
			int space = p.getInt(offset);
			// HRDBMSWorker.logger.debug("Page " + num + " has " + space +
			// " free bytes according to @" + offset);
			if (data == -1)
			{
				return -1;
			}

			if (data <= space)
			{
				return num;
			}

			if (space == -1)
			{
				return 0;
			}

			offset += 4;
			num++;
		}

		return -1;
	}

	public int findPage(int data, int colID, int colMarker)
	{
		int offset = off;
		int num = blockNum;
		while (offset < Page.BLOCK_SIZE)
		{
			final int col = p.getInt(offset);
			int space = p.getInt(offset + 4);

			if (colID == -1)
			{
				return -1;
			}

			if (col == colID)
			{
				if (data <= space)
				{
					return num;
				}
			}

			if (space == -1)
			{
				return 0;
			}

			offset += 8;
			num++;

		}

		return -1;
	}

	public int findPage(int data, int colID, int colMarker, int after)
	{
		int offset = off;
		int num = blockNum;
		while (num < after)
		{
			offset += 8;
			num++;
		}
		while (offset < Page.BLOCK_SIZE)
		{
			final int col = p.getInt(offset);
			int space = p.getInt(offset + 4);

			if (colID == -1)
			{
				return -1;
			}

			if (col == colID)
			{
				if (data <= space)
				{
					return num;
				}
			}

			if (space == -1)
			{
				return 0;
			}

			offset += 8;
			num++;

		}

		return -1;
	}

	public int getDeviceNumber()
	{
		return device;
	}

	public int getNodeNumber()
	{
		return node;
	}

	public void updateColNum(int entryNum, int colID, Transaction tx) throws Exception
	{
		LockManager.xLock(p.block(), tx.number());
		final byte[] before = ByteBuffer.allocate(4).putInt(p.getInt(entryNum << 3)).array();
		final byte[] after = ByteBuffer.allocate(4).putInt(colID).array();
		final InsertLogRec rec = tx.insert(before, after, entryNum << 3, p.block());
		p.write(entryNum << 3, after, tx.number(), rec.lsn());
	}

	public void updateColNumNoLog(int entryNum, int colID, Transaction tx) throws Exception
	{
		LockManager.xLock(p.block(), tx.number());
		final byte[] after = ByteBuffer.allocate(4).putInt(colID).array();
		long lsn = LogManager.getLSN();
		p.write(entryNum << 3, after, tx.number(), lsn);
	}

	public void updateSize(int entryNum, int size, Transaction tx) throws Exception
	{
		LockManager.xLock(p.block(), tx.number());
		final byte[] before = ByteBuffer.allocate(4).putInt(p.getInt(entryNum << 2)).array();
		final byte[] after = ByteBuffer.allocate(4).putInt(size).array();
		// LogManager.write(new InsertLogRec(txnum, p.block(), entryNum * 4,
		// before, after));
		// p.write(entryNum * 4, ByteBuffer.allocate(4).putInt(size).array(),
		// txnum, LogManager.getLSN());
		final InsertLogRec rec = tx.insert(before, after, entryNum << 2, p.block());
		p.write(entryNum << 2, after, tx.number(), rec.lsn());
	}

	public void updateSize(int entryNum, int size, Transaction tx, int colMarker) throws Exception
	{
		LockManager.xLock(p.block(), tx.number());
		final byte[] before = ByteBuffer.allocate(4).putInt(p.getInt((entryNum << 3) + 4)).array();
		final byte[] after = ByteBuffer.allocate(4).putInt(size).array();
		// LogManager.write(new InsertLogRec(txnum, p.block(), entryNum * 8 + 4,
		// before, after));
		// p.write(entryNum * 8 + 4,
		// ByteBuffer.allocate(4).putInt(size).array(), txnum,
		// LogManager.getLSN());
		final InsertLogRec rec = tx.insert(before, after, (entryNum << 3) + 4, p.block());
		p.write((entryNum << 3) + 4, after, tx.number(), rec.lsn());
	}

	public void updateSizeNoLog(int entryNum, int size, Transaction tx) throws Exception
	{
		LockManager.xLock(p.block(), tx.number());
		final byte[] after = ByteBuffer.allocate(4).putInt(size).array();
		long lsn = LogManager.getLSN();
		p.write(entryNum << 2, after, tx.number(), lsn);
	}

	public void updateSizeNoLog(int entryNum, int size, Transaction tx, int colMarker) throws Exception
	{
		LockManager.xLock(p.block(), tx.number());
		final byte[] after = ByteBuffer.allocate(4).putInt(size).array();
		long lsn = LogManager.getLSN();
		p.write((entryNum << 3) + 4, after, tx.number(), lsn);
	}
}