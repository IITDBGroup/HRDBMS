package com.exascale.filesystem;

import java.io.Serializable;

public class RID implements Serializable, Comparable
{
	private int node = -1;
	private int dev = -1;
	private int block = -1;
	private int rec = -1;

	public RID(final int node, final int dev, final int block, final int rec)
	{
		this.node = node;
		this.dev = dev;
		this.block = block;
		this.rec = rec;
	}

	@Override
	public int compareTo(final Object r)
	{
		if (!(r instanceof RID))
		{
			return -1;
		}

		final RID rhs = (RID)r;
		if (node < rhs.node)
		{
			return -1;
		}
		else if (node > rhs.node)
		{
			return 1;
		}
		else if (dev < rhs.dev)
		{
			return -1;
		}
		else if (dev > rhs.dev)
		{
			return 1;
		}
		else if (block < rhs.block)
		{
			return -1;
		}
		else if (block > rhs.block)
		{
			return 1;
		}
		else if (rec < rhs.rec)
		{
			return -1;
		}
		else if (rec > rhs.rec)
		{
			return 1;
		}
		else
		{
			return 0;
		}
	}

	@Override
	public boolean equals(final Object obj)
	{
		if (obj == null)
		{
			return false;
		}

		if (obj instanceof RID)
		{
			final RID rid = (RID)obj;
			return node == rid.node && dev == rid.dev && block == rid.block && rec == rid.rec;
		}

		return false;
	}

	public int getBlockNum()
	{
		return block;
	}

	public int getDevice()
	{
		return dev;
	}

	public int getNode()
	{
		return node;
	}

	public int getRecNum()
	{
		return rec;
	}

	public String getRID()
	{
		return "" + node + dev + block + rec;
	}

	@Override
	public int hashCode()
	{
		return Integer.reverse(node) + Integer.reverse(dev) + Integer.reverse(block) + Integer.reverse(rec);
	}

	public void setBlock(final int block)
	{
		this.block = block;
	}

	public void setDevice(final int dev)
	{
		this.dev = dev;
	}

	public void setNode(final int node)
	{
		this.node = node;
	}

	public void setRecord(final int rec)
	{
		this.rec = rec;
	}

	@Override
	public String toString()
	{
		return node + ":" + dev + ":" + block + ":" + rec;
	}
}
