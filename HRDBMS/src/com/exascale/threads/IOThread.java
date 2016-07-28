package com.exascale.threads;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import com.exascale.filesystem.Block;
import com.exascale.managers.BufferManager;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.tables.Schema;
import com.exascale.tables.Transaction;

public class IOThread extends HRDBMSThread
{
	private final Block[] bs;
	private long txnum;
	private Schema[] schemas;
	private int schemaIndex;
	private ConcurrentHashMap<Integer, Schema> schemaMap;
	private Transaction tx;
	private ArrayList<Integer> fetchPos;

	public IOThread(Block b, long txnum)
	{
		this.setWait(false);
		this.description = ("I/O for " + b);
		this.bs = new Block[1];
		bs[0] = b;
		this.txnum = txnum;
	}

	public IOThread(Block[] bs, long txnum)
	{
		this.setWait(false);
		this.description = ("I/O for multiple blocks");
		this.bs = bs;
		this.txnum = txnum;
	}

	public IOThread(Block[] bs, Transaction tx, Schema[] schemas, int schemaIndex, ConcurrentHashMap<Integer, Schema> schemaMap, ArrayList<Integer> fetchPos)
	{
		this.setWait(false);
		this.description = ("I/O for multiple blocks");
		this.bs = bs;
		this.schemas = schemas;
		this.schemaIndex = schemaIndex;
		this.schemaMap = schemaMap;
		this.tx = tx;
		this.fetchPos = fetchPos;
	}

	@Override
	public void run()
	{
		try
		{
			for (final Block b : bs)
			{
				if (schemas == null)
				{
					BufferManager.pin(b, txnum);
				}
				else
				{
					try
					{
						BufferManager.pin(b, tx, schemas[schemaIndex++], schemaMap, fetchPos);
					}
					catch (Exception e)
					{
						HRDBMSWorker.logger.debug("", e);
					}
				}
			}
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.error("Error occurred in an I/O thread.", e);
		}

		//this.terminate();
		return;
	}
}
