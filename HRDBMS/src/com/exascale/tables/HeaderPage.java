package com.exascale.tables;

import java.util.ArrayList;
import com.exascale.filesystem.Page;

public class HeaderPage
{
	private int device = -1;
	private int node = -1;
	private final Page p;

	public HeaderPage(Page p, int type)
	{
		this.p = p;
		if (p.block().number() == 0)
		{
			node = p.getInt(0); // first page contains node and device number
			device = p.getInt(4);
		}
	}

	public int getDeviceNumber()
	{
		return device;
	}

	public ArrayList<Byte> getHeaderBytes(int size)
	{
		ArrayList<Byte> retval = new ArrayList<Byte>(size);
		int pos = 8;
		int i = 0;
		while (i < size)
		{
			retval.add(p.get(pos++));
			i++;
		}

		return retval;
	}
	
	public ArrayList<Integer> getColOrder()
	{
		int pos = 26220;
		int size = p.getInt(pos);
		pos += 4;
		ArrayList<Integer> retval = new ArrayList<Integer>();
		int i = 0;
		while (i < size)
		{
			retval.add(p.getInt(pos));
			pos += 4;
			i++;
		}
		
		return retval;
	}

	public int getNodeNumber()
	{
		return node;
	}
}