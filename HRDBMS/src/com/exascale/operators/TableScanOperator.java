package com.exascale.operators;

import java.io.File;
import java.util.HashMap;
import java.util.Vector;
import java.util.concurrent.LinkedBlockingQueue;

import com.exascale.exceptions.LockAbortException;
import com.exascale.filesystem.Block;
import com.exascale.filesystem.Page;
import com.exascale.managers.FileManager;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.tables.DataType;
import com.exascale.tables.Plan;
import com.exascale.tables.QueueEndMarker;
import com.exascale.tables.Schema;
import com.exascale.tables.Schema.FieldValue;
import com.exascale.tables.Schema.Row;
import com.exascale.tables.Schema.RowIterator;
import com.exascale.tables.Transaction;
import com.exascale.threads.HRDBMSThread;

public class TableScanOperator implements Operator
{
	private String fn;
	private LinkedBlockingQueue q = new LinkedBlockingQueue();
	private Object o;
	private boolean isException = false;
	private long cost;
	private int[] devices;
	private boolean allDevices;
	private int numThreads = 0;
	private HashMap<Integer, DataType> types;
	private boolean specificDevice = false;
	private Plan plan;
	
	public TableScanOperator(String schema, String table, HashMap<Integer, DataType> types)
	{
		//ALL DEVICES
		this.fn = (schema + "." + table + ".tbl");
		allDevices = true;
		this.types = types;
	}
	
	public TableScanOperator(String schema, String table, HashMap<Integer, DataType> types, int[] devices)
	{
		this.fn = (schema + "." + table + ".tbl");
		allDevices = false;
		this.devices = devices;
		this.types = types;
	}
	
	public TableScanOperator(String fn, HashMap<Integer, DataType> types)
	{
		this.fn = fn;
		allDevices = false;
		this.types = types;
		specificDevice = true;
	}
	
	public void setPlan(Plan p)
	{
		plan = p;
	}
	
	public void setCost(long cost)
	{
		this.cost = cost;
	}
	
	public void addChild(Operator child)
	{
		throw new IllegalArgumentException("A TableScanOperator cannot have any children.");
	}
	
	public void start(Transaction tx)
	{
		File[] dirs = FileManager.getDirs();
		if (allDevices)
		{
			for (File dir : dirs)
			{
				String path = dir.getAbsolutePath();
				if (!path.endsWith("/"))
				{
					path += "/";
				}
				HRDBMSWorker.addThread(new TableScanThread(tx, path + fn, types));
				numThreads++;
			}
		}
		else
		{
			if (specificDevice)
			{
				HRDBMSWorker.addThread(new TableScanThread(tx, fn, types));
				numThreads++;
			}
			for (int devNum : devices)
			{
				String path = dirs[devNum].getAbsolutePath();
				if (!path.endsWith("/"))
				{
					path += "/";
				}
				HRDBMSWorker.addThread(new TableScanThread(tx, path + fn, types));
				numThreads++;
			}
		}
	}
	
	public boolean next()
	{
		while (true)
		{
			try
			{
				o = q.take();
				if (o instanceof QueueEndMarker)
				{
					numThreads--;
					if (numThreads == 0)
					{
						return false;
					}
					else
					{
						continue;
					}
				}
				break;
			}
			catch(InterruptedException e) {}
		}
		
		if (o instanceof Exception)
		{
			isException = true;
		}
		
		return true;
	}
	
	public void close()
	{}
	
	public Object getVal(int index)
	{
		if (isException)
		{
			return o;
		}
		
		return ((Vector<Object>)o).get(index - 1);
	}
	
	public int getNumCols()
	{
		return types.size();
	}
	
	public long cost()
	{
		return cost;
	}
	
	private class TableScanThread extends HRDBMSThread
	{
		private String fn;
		private int[] colIDs;
		private Transaction tx;
		HashMap<Integer, DataType> types;
		
		public TableScanThread(Transaction tx, String fn, HashMap<Integer, DataType> types)
		{
			this.fn = fn;
			this.colIDs = colIDs;
			this.tx = tx;
			this.types = types;
		}
		
		public void run()
		{
			int numBlocks = (int)(new File(fn).length() / Page.BLOCK_SIZE) - 4096;
			int reqSize = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("pages_per_request"));
			Block[] blocks;
			int nextBlock = 0;
			int numRemainingFetch;
			int numRemainingRead;
			int reading = 4096;
			int numUntilNextFetch = 0;
			
			if (numBlocks <= reqSize)
			{
				blocks = new Block[numBlocks];
				int i = 0;
				while (i < numBlocks)
				{
					blocks[i] = new Block(fn, 4096 + i);
					i++;
				}
				
				numRemainingFetch = 0;
				numRemainingRead = numBlocks;
			}
			else
			{
				blocks = new Block[reqSize];
				int i = 0;
				while (i < reqSize)
				{
					blocks[i] = new Block(fn, 4096 + i);
					i++;
				}
				
				nextBlock = 4096 + i;
				numRemainingFetch = numBlocks - reqSize;
				numRemainingRead = numBlocks;
				numUntilNextFetch = (reqSize * Integer.parseInt(HRDBMSWorker.getHParms().getProperty("block_request_threshold"))) / 100;
			}
			
			tx.requestPages(blocks);
			Schema schema = new Schema(types);
			while (numRemainingRead > 0)
			{
				try
				{
					tx.read(new Block(fn, reading), schema);
				}
				catch(Exception e)
				{
					while (true)
					{
						try
						{
							q.put(e);
							break;
						}
						catch(InterruptedException f)
						{}
					}
					
					this.terminate();
					return;
				}
				
				reading++;
				numRemainingRead--;
				numUntilNextFetch--;
				
				if (numUntilNextFetch == 0 && numRemainingFetch > 0)
				{
					if (numRemainingFetch <= reqSize)
					{
						blocks = new Block[numRemainingFetch];
						int i = 0;
						while (i < numRemainingFetch)
						{
							blocks[i] = new Block(fn, nextBlock);
							i++;
							nextBlock++;
						}
						
						numRemainingFetch = 0;
						numRemainingRead += numRemainingFetch;
					}
					else
					{
						int i = 0;
						while (i < reqSize)
						{
							blocks[i] = new Block(fn, nextBlock);
							nextBlock++;
							i++;
						}
						
						numRemainingFetch -= reqSize;
						numRemainingRead += reqSize;
						numUntilNextFetch = reqSize;
					}
				}
				
				//process page just read
				RowIterator iter = schema.rowIterator();
				while(iter.hasNext())
				{
					Row row = iter.next();
					Vector<FieldValue> data = new Vector<FieldValue>();
					for (int colID : types.keySet())
					{
						FieldValue fv = null;
						try
						{
							fv = row.getCol(colID);
						}
						catch(Exception e)
						{
							while (true)
							{
								try
								{
									q.put(e);
									break;
								}
								catch(InterruptedException f)
								{}
							}
							
							this.terminate();
							return;
						}
						if (fv.exists())
						{
							data.add(fv);
						}
						else
						{
							break;
						}
					}
					
					if (data.size() > 0)
					{
						while (true)
						{
							try
							{
								q.put(data);
								break;
							}
							catch(InterruptedException e)
							{}
						}
					}
				}
			}
			
			while (true)
			{
				try
				{
					q.put(new QueueEndMarker());
					break;
				}
				catch(InterruptedException f)
				{}
			}
		}
	}
}
