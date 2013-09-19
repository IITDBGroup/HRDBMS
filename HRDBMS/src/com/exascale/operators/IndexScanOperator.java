package com.exascale.operators;

import java.io.File;
import java.util.HashMap;
import java.util.Vector;
import java.util.concurrent.LinkedBlockingQueue;

import com.exascale.exceptions.LockAbortException;
import com.exascale.filesystem.Block;
import com.exascale.filesystem.Page;
import com.exascale.filesystem.RID;
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

public class IndexScanOperator implements Operator
{
	private String fn;
	private LinkedBlockingQueue q = new LinkedBlockingQueue();
	private Object o;
	private long cost;
	private int[] devices;
	private boolean allDevices;
	private int numThreads = 0;
	private DataType[] types;
	private boolean specificDevice = false;
	private String[] params;
	private Plan plan;
	
	public IndexScanOperator(String schema, String table, String index, DataType[] types, String... params)
	{
		//ALL DEVICES
		this.fn = (schema + "." + table + "." + index + ".index");
		allDevices = true;
		this.types = types;
		this.params = params;
	}
	
	public IndexScanOperator(String schema, String table, String index, DataType[] types, int[] devices, String... params)
	{
		this.fn = (schema + "." + table + "." + index + ".index");
		allDevices = false;
		this.devices = devices;
		this.types = types;
		this.params = params;
	}
	
	public IndexScanOperator(String fn, DataType[] types, String... params)
	{
		this.fn = fn;
		allDevices = false;
		this.types = types;
		specificDevice = true;
		this.params = params;
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
		throw new IllegalArgumentException("An IndexScanOperator cannot have any children.");
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
				HRDBMSWorker.addThread(new IndexScanThread(tx, path + fn, types, params));
				numThreads++;
			}
		}
		else
		{
			if (specificDevice)
			{
				HRDBMSWorker.addThread(new IndexScanThread(tx, fn, types, params));
				numThreads++;
			}
			for (int devNum : devices)
			{
				String path = dirs[devNum].getAbsolutePath();
				if (!path.endsWith("/"))
				{
					path += "/";
				}
				HRDBMSWorker.addThread(new IndexScanThread(tx, path + fn, types, params));
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
		
		return true;
	}
	
	public void close()
	{}
	
	public Object getVal(int index)
	{
		return o;
	}
	
	public int getNumCols()
	{
		return 1;
	}
	
	public long cost()
	{
		return cost;
	}
	
	private class IndexScanThread extends HRDBMSThread
	{
		private String fn;
		private String[] params;
		private Transaction tx;
		DataType[] types;
		
		public IndexScanThread(Transaction tx, String fn, DataType[] types, String[] params)
		{
			this.fn = fn;
			this.tx = tx;
			this.types = types;
			this.params = params;
		}
		
		public void run()
		{
			Index i = new Index(fn);
			handleParameters();
			Vector<RID> rids = i.getRIDs(tx, types, params);
			
			for (RID rid : rids)
			{
				while (true)
				{
					try
					{
						q.put(rid);
						break;
					}
					catch(InterruptedException e)
					{}
				}
			}
			
			while (true)
			{
				try
				{
					q.put(new QueueEndMarker());
					break;
				}
				catch(InterruptedException e)
				{}
			}
		}
		
		private void handleParameters()
		{
			int i = 0;
			while (i < params.length)
			{
				String param = params[i];
				if (param.startsWith("@"))
				{
					try
					{
						int parmNum = Integer.parseInt(param.substring(1));
						params[i] = plan.getArgument(parmNum);
					}
					catch(NumberFormatException e)
					{}
				}
			}
		}
	}
}
