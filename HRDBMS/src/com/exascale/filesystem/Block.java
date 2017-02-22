package com.exascale.filesystem;

import java.io.Serializable;
import java.util.StringTokenizer;

public class Block implements Serializable
{
	private final String filename;
	private final int blknum;
	private transient volatile String string;

	public Block(final String str)
	{
		final StringTokenizer tokens = new StringTokenizer(str, "~", false);
		filename = tokens.nextToken();
		blknum = Integer.parseInt(tokens.nextToken());
	}

	public Block(final String filename, final int number)
	{
		this.filename = filename;
		this.blknum = number;
	}

	public int compareTo(final Block b)
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
	public boolean equals(final Object obj)
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
		int hash = 17;
		hash = hash * 23 + filename.hashCode();
		hash = hash * 23 + blknum;
		return hash;
	}

	public int hashCode2()
	{
		int hash = 17;
		hash = hash * 23 + (blknum / 3);
		hash = hash * 23 + filename.hashCode();
		return hash;
	}

	public int number()
	{
		return blknum;
	}

	@Override
	public String toString()
	{
		if (string == null)
		{
			string = filename + "~" + blknum;
		}

		return string;
	}
}
