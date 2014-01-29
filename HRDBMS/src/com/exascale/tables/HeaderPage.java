package com.exascale.tables;

import java.nio.ByteBuffer;

import com.exascale.filesystem.Page;
import com.exascale.logging.InsertLogRec;
import com.exascale.managers.LogManager;

public class HeaderPage 
{
	protected int device = -1;
	protected int node = -1;
	protected int type;
	protected static final int ROW_HEADER_ENTRIES_PER_PAGE = Page.BLOCK_SIZE / 4; //largest free size
	protected static final int COL_HEADER_ENTRIES_PER_PAGE = Page.BLOCK_SIZE / 8; //largest free size and column id
	protected int blockNum;
	protected int off;
	protected Page p;
	
	public HeaderPage(Page p, int type)
	{
		this.type = type;
		this.p = p;
		off = 0;
		if (p.block().number() == 0)
		{
			node = p.getInt(0); //first page contains node and device number
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
	
	public int getNodeNumber()
	{
		return node;
	}
	
	public int getDeviceNumber()
	{
		return device;
	}
	
	public void updateColNum(int entryNum, int colID, Transaction tx)
	{	
		byte[] before = ByteBuffer.allocate(4).putInt(p.getInt(entryNum * 8)).array();
		byte[] after = ByteBuffer.allocate(4).putInt(colID).array();
		InsertLogRec rec = tx.insert(before, after, entryNum * 8, p.block());
		p.write(entryNum * 8, after, tx.number(), rec.lsn());
	}
	
	public void updateSize(int entryNum, int size, Transaction tx, int colMarker)
	{
		byte[] before = ByteBuffer.allocate(4).putInt(p.getInt(entryNum * 8 + 4)).array();
		byte[] after = ByteBuffer.allocate(4).putInt(size).array();
		//LogManager.write(new InsertLogRec(txnum, p.block(), entryNum * 8 + 4, before, after));
		//p.write(entryNum * 8 + 4, ByteBuffer.allocate(4).putInt(size).array(), txnum, LogManager.getLSN());
		InsertLogRec rec = tx.insert(before, after, entryNum * 8 + 4, p.block());
		p.write(entryNum * 8 + 4, after, tx.number(), rec.lsn());
	}
	
	public int findPage(int data, int colID, int colMarker)
	{
		int offset = off;
		int num = blockNum;
		while (off < Page.BLOCK_SIZE)
		{
			int col = p.getInt(offset);
			
			if (colID == -1)
			{
				return -1;
			}
			
			if (col == colID)
			{
				if (data <= p.getInt(offset+4))
				{
					return num;
				}
			}
			
			offset += 8;
			num++;  
			
		}
		
		return -1;
	}
	
	public void updateSize(int entryNum, int size, Transaction tx)
	{
		byte[] before = ByteBuffer.allocate(4).putInt(p.getInt(entryNum * 4)).array();
		byte[] after = ByteBuffer.allocate(4).putInt(size).array();
		//LogManager.write(new InsertLogRec(txnum, p.block(), entryNum * 4, before, after));
		//p.write(entryNum * 4, ByteBuffer.allocate(4).putInt(size).array(), txnum, LogManager.getLSN());
		InsertLogRec rec = tx.insert(before, after, entryNum * 4, p.block());
		p.write(entryNum * 4, after, tx.number(), rec.lsn());
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
}