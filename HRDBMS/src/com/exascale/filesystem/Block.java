package com.exascale.filesystem;

import java.util.StringTokenizer;

public class Block
{
	private final String filename;
	private final int blknum;

	public Block(String str)
	{
		final StringTokenizer tokens = new StringTokenizer(str, "~", false);
		filename = tokens.nextToken();
		blknum = Integer.parseInt(tokens.nextToken());
	}

	public Block(String filename, int number)
	{
		this.filename = filename;
		this.blknum = number;
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

	@Override
	public boolean equals(Object obj)
	{
		if (obj instanceof Block)
		{
			final Block blk = (Block)obj;
			return filename.equals(blk.filename) && blknum == blk.blknum;
		}

		return false;
	}

	public String fileName()
	{
		return filename;
	}

	@Override
	public int hashCode()
	{
		return toString().hashCode();
	}

	public int number()
	{
		return blknum;
	}

	@Override
	public String toString()
	{
		return filename + "~" + blknum;
	}
}
