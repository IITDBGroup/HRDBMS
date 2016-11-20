package com.exascale.tables;

import java.util.ArrayList;
import com.exascale.filesystem.Page;
import com.exascale.managers.HRDBMSWorker;

public class HeaderPage
{
	private int device = -1;
	private int node = -1;
	private final Page p;

	public HeaderPage(final Page p, final int type)
	{
		this.p = p;
		if (p.block().number() == 0)
		{
			node = p.getInt(0); // first page contains node and device number
			device = p.getInt(4);
		}
	}

	public ArrayList<Integer> getClustering()
	{
		final int pageSize = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("page_size"));
		if (pageSize < 252 * 1024)
		{
			return new ArrayList<Integer>();
		}

		int pos = 131072;
		final int size = p.getInt(pos);
		pos += 4;
		final ArrayList<Integer> retval = new ArrayList<Integer>(size);
		int i = 0;
		while (i < size)
		{
			retval.add(p.getInt(pos) - 1);
			pos += 4;
			i++;
		}

		return retval;
	}

	public ArrayList<Integer> getColOrder()
	{
		int pos = 26220;
		final int size = p.getInt(pos);
		pos += 4;
		final ArrayList<Integer> retval = new ArrayList<Integer>(size);
		int i = 0;
		while (i < size)
		{
			retval.add(p.getInt(pos));
			pos += 4;
			i++;
		}

		return retval;
	}

	public int getDeviceNumber()
	{
		return device;
	}

	public ArrayList<Byte> getHeaderBytes(final int size)
	{
		final ArrayList<Byte> retval = new ArrayList<Byte>(size);
		int pos = 8;
		int i = 0;
		while (i < size)
		{
			retval.add(p.get(pos++));
			i++;
		}

		return retval;
	}

	public int getNodeNumber()
	{
		return node;
	}
}