package com.exascale.tables;

import java.nio.ByteBuffer;

import com.exascale.filesystem.Page;
import com.exascale.managers.LogManager;

public class HeaderPage 
{
	private int device = -1;
	private int node = -1;
	private int type;
	private static final int ROW_HEADER_ENTRIES_PER_PAGE = Page.BLOCK_SIZE / 4;
	private static final int COL_HEADER_ENTRIES_PER_PAGE = Page.BLOCK_SIZE / 8;
	private int blockNum;
	private int off;
	private Page p;
	
	public HeaderPage(Page p, int type)
	{
		this.type = type;
		this.p = p;
		off = 0;
		if (p.block().number() == 0)
		{
			node = p.getInt(0);
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
	
	public void updateColNum(int entryNum, int colID, long txnum)
	{
		p.write(entryNum * 8, ByteBuffer.allocate(4).putInt(colID).array(), txnum, LogManager.getLSN());
	}
	
	public void updateSize(int entryNum, int size, long txnum, int colMarker)
	{
		p.write(entryNum * 8 + 4, ByteBuffer.allocate(4).putInt(size).array(), txnum, LogManager.getLSN());
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
	
	public void updateSize(int entryNum, int size, long txnum)
	{
		p.write(entryNum * 4, ByteBuffer.allocate(4).putInt(size).array(), txnum, LogManager.getLSN());
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