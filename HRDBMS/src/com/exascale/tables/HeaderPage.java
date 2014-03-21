package com.exascale.tables;

import java.nio.ByteBuffer;
import com.exascale.filesystem.Page;
import com.exascale.logging.InsertLogRec;

public class HeaderPage
{
	private int device = -1;
	private int node = -1;
	private static final int ROW_HEADER_ENTRIES_PER_PAGE = Page.BLOCK_SIZE / 4; // largest
																				// free
																				// size
	private static final int COL_HEADER_ENTRIES_PER_PAGE = Page.BLOCK_SIZE / 8; // largest
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
			off = 8;
		}

		if (type == Schema.TYPE_ROW)
		{
			blockNum = p.block().number() * ROW_HEADER_ENTRIES_PER_PAGE - 2;
		}
		else
		{
			blockNum = p.block().number() * COL_HEADER_ENTRIES_PER_PAGE - 1;
		}
	}

	public int findPage(int data)
	{
		int offset = off;
		int num = blockNum;
		while (off < Page.BLOCK_SIZE)
		{
			if (data == -1)
			{
				return -1;
			}

			if (data <= p.getInt(offset))
			{
				return num;
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
		while (off < Page.BLOCK_SIZE)
		{
			final int col = p.getInt(offset);

			if (colID == -1)
			{
				return -1;
			}

			if (col == colID)
			{
				if (data <= p.getInt(offset + 4))
				{
					return num;
				}
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

	public void updateColNum(int entryNum, int colID, Transaction tx)
	{
		final byte[] before = ByteBuffer.allocate(4).putInt(p.getInt(entryNum * 8)).array();
		final byte[] after = ByteBuffer.allocate(4).putInt(colID).array();
		final InsertLogRec rec = tx.insert(before, after, entryNum * 8, p.block());
		p.write(entryNum * 8, after, tx.number(), rec.lsn());
	}

	public void updateSize(int entryNum, int size, Transaction tx)
	{
		final byte[] before = ByteBuffer.allocate(4).putInt(p.getInt(entryNum * 4)).array();
		final byte[] after = ByteBuffer.allocate(4).putInt(size).array();
		// LogManager.write(new InsertLogRec(txnum, p.block(), entryNum * 4,
		// before, after));
		// p.write(entryNum * 4, ByteBuffer.allocate(4).putInt(size).array(),
		// txnum, LogManager.getLSN());
		final InsertLogRec rec = tx.insert(before, after, entryNum * 4, p.block());
		p.write(entryNum * 4, after, tx.number(), rec.lsn());
	}

	public void updateSize(int entryNum, int size, Transaction tx, int colMarker)
	{
		final byte[] before = ByteBuffer.allocate(4).putInt(p.getInt(entryNum * 8 + 4)).array();
		final byte[] after = ByteBuffer.allocate(4).putInt(size).array();
		// LogManager.write(new InsertLogRec(txnum, p.block(), entryNum * 8 + 4,
		// before, after));
		// p.write(entryNum * 8 + 4,
		// ByteBuffer.allocate(4).putInt(size).array(), txnum,
		// LogManager.getLSN());
		final InsertLogRec rec = tx.insert(before, after, entryNum * 8 + 4, p.block());
		p.write(entryNum * 8 + 4, after, tx.number(), rec.lsn());
	}
}