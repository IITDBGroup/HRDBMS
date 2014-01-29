package com.exascale.filesystem;

public class RID 
{
	protected int node = -1;
	protected int dev = -1;
	protected int block = -1;
	protected int rec = -1;
	
	public void setNode(int node)
	{
		this.node = node;
	}
	
	public void setDevice(int dev)
	{
		this.dev = dev;
	}
	
	public void setBlock(int block)
	{
		this.block = block;
	}
	
	public void setRecord(int rec)
	{
		this.rec = rec;
	}
	
	public int getNode()
	{
		return node;
	}
	
	public int getDevice()
	{
		return dev;
	}
	
	public int getBlockNum()
	{
		return block;
	}
	
	public int getRecNum()
	{
		return rec;
	}
	
	public RID(int node, int dev, int block, int rec)
	{
		this.node = node;
		this.dev = dev;
		this.block = block;
		this.rec = rec;
	}
	
	public String getRID()
	{
		return "" + node + dev + block + rec;
	}
	
	public boolean equals(Object obj)
	{
		if (obj == null)
		{
			return false;
		}
		
		if (obj instanceof RID)
		{
			RID rid = (RID)obj;
			return node == rid.node && dev == rid.dev && block == rid.block && rec == rid.rec;
		}
		
		return false;
	}
	
	public int compareTo(RID rhs)
	{
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
}
