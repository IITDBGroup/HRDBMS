package com.exascale.filesystem;

import java.util.StringTokenizer;

public class Block 
{
	protected String filename;
	protected int blknum;
	
	public Block(String filename, int number)
	{
		this.filename = filename;
		this.blknum = number;
	}
	
	public String fileName()
	{
		return filename;
	}
	
	public int number()
	{
		return blknum;
	}
	
	public boolean equals(Object obj)
	{
		if (obj instanceof Block)
		{
			Block blk = (Block)obj;
			return filename.equals(blk.filename) && blknum == blk.blknum;
		}
		
		return false;
	}
	
	public int compareTo(Block b)
	{
		if (b == null)
		{
			return 1;
		}
		
		if (!filename.equals(b.filename))
		{
			return filename.compareTo(b.filename);
		}
		else
		{
			if (blknum < b.blknum)
			{
				return -1;
			}
			else if (blknum == b.blknum)
			{
				return 0;
			}
			else
			{
				return 1;
			}
		}
	}
	
	public String toString()
	{
		return filename + "~" + blknum;
	}
	
	public int hashCode()
	{
		return toString().hashCode();
	}
	
	public Block(String str)
	{
		StringTokenizer tokens = new StringTokenizer(str, "~", false);
		filename = tokens.nextToken();
		blknum = Integer.parseInt(tokens.nextToken());
	}
}
