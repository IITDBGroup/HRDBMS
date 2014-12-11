package com.exascale.optimizer;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.locks.LockSupport;
import com.exascale.exceptions.LockAbortException;
import com.exascale.filesystem.Block;
import com.exascale.filesystem.Page;
import com.exascale.filesystem.RID;
import com.exascale.logging.DeleteLogRec;
import com.exascale.logging.InsertLogRec;
import com.exascale.managers.FileManager;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.managers.LockManager;
import com.exascale.managers.LogManager;
import com.exascale.managers.ResourceManager;
import com.exascale.misc.ArrayListLong;
import com.exascale.misc.BufferedLinkedBlockingQueue;
import com.exascale.misc.DataEndMarker;
import com.exascale.misc.DateParser;
import com.exascale.misc.FastStringTokenizer;
import com.exascale.misc.MyDate;
import com.exascale.misc.RowComparator;
import com.exascale.misc.Utils;
import com.exascale.tables.Schema.FieldValue;
import com.exascale.tables.Transaction;
import com.exascale.threads.ThreadPoolThread;

public final class Index implements Serializable
{
	private String fileName;

	private ArrayList<String> keys;
	private ArrayList<String> types;
	private ArrayList<Boolean> orders;
	private Filter f;
	private ArrayList<Filter> secondary = new ArrayList<Filter>();
	private boolean positioned = false;
	private ArrayListLong ridList = new ArrayListLong();
	private String col;
	private String op;
	private ArrayList<Filter> terminates = new ArrayList<Filter>();
	private IndexRecord line;
	// private MySimpleDateFormat sdf = new MySimpleDateFormat("yyyy-MM-dd");
	private HashMap<String, Integer> cols2Pos = new HashMap<String, Integer>();
	private int count = 0;
	private boolean indexOnly = false;
	private ArrayList<Integer> fetches = new ArrayList<Integer>();
	private ArrayList<String> fetchTypes = new ArrayList<String>();
	private ArrayList<Object> row;
	private volatile boolean delayed = false;
	private ArrayList<Filter> delayedConditions;
	private HashMap<String, String> renames;
	private BufferedLinkedBlockingQueue queue;
	private IndexWriterThread iwt;
	private volatile Thread lastThread = null;
	private long offset = 13;
	private Transaction tx;
	private volatile Boolean isUniqueVar = null;
	private HashMap<Block, Page> myPages = new HashMap<Block, Page>();

	public Index(String fileName, ArrayList<String> keys, ArrayList<String> types, ArrayList<Boolean> orders)
	{
		this.fileName = fileName;
		this.keys = keys;
		this.types = types;
		this.orders = orders;
		int i = 0;
		for (final String key : keys)
		{
			cols2Pos.put(key, i);
			i++;
		}
	}
	
	public boolean isUnique() throws Exception
	{
		if (isUniqueVar == null)
		{
			Block b = new Block(fileName, 0);
			LockManager.sLock(b, tx.number());
			tx.requestPage(b);
			Page p = null;
			try
			{
				p = tx.getPage(b);
			}
			catch(Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				throw e;
			}
		
			boolean val = p.get(4) == 1;
			isUniqueVar = new Boolean(val);
		}
		
		return isUniqueVar;
	}
	
	public ArrayList<String> getKeys()
	{
		return keys;
	}
	
	public ArrayList<String> getTypes()
	{
		return types;
	}
	
	public ArrayList<Boolean> getOrders()
	{
		return orders;
	}
	
	private boolean reallyViolatesUniqueConstraint(FieldValue[] keys) throws Exception
	{
		IndexRecord line2 = line;
		while (!line2.isNull() && line2.keysMatch(keys))
		{
			if (!line2.isTombstone())
			{
				return true;
			}
			
			line2 = line2.nextRecord();
		}
		
		return false;
	}
	
	public void insert(FieldValue[] keys, RID rid) throws Exception
	{
		seek(13);
		this.setEqualsPosMulti(keys);
		if (line.isLeaf() && line.keysMatch(keys))
		{
			if (isUnique() && reallyViolatesUniqueConstraint(keys))
			{
				HRDBMSWorker.logger.debug("Unique constraint violation for keys: " + keys);
				throw new Exception("Violates unique constraint - " + fileName);
			}
		}
		if (line.isLeaf())
		{
			line = line.getUp(true);
		}
		if (line.containsRoom())
		{
			BlockAndOffset blockAndOffset = line.writeNewLeaf(keys, rid);
			line.addDown(keys, blockAndOffset);
		}
		else
		{
			line.split();
			seek(13);
			this.setEqualsPosMulti(keys);
			line = line.getUp(true);
			BlockAndOffset blockAndOffset = line.writeNewLeaf(keys, rid);
			line.addDown(keys, blockAndOffset);
		}
	}
	
	public void insertNoLog(FieldValue[] keys, RID rid) throws Exception
	{
		seek(13);
		this.setEqualsPosMulti(keys);
		if (line.isLeaf() && line.keysMatch(keys))
		{
			if (isUnique() && reallyViolatesUniqueConstraint(keys))
			{
				HRDBMSWorker.logger.debug("Unique constraint violation for keys: " + keys);
				throw new Exception("Violates unique constraint");
			}
		}
		
		if (line.isLeaf())
		{
			line = line.getUp(true);
		}
		if (line.containsRoom())
		{
			BlockAndOffset blockAndOffset = line.writeNewLeafNoLog(keys, rid);
			line.addDownNoLog(keys, blockAndOffset);
		}
		else
		{
			line.splitNoLog();
			seek(13);
			this.setEqualsPosMulti(keys);
			line = line.getUp(true);
			BlockAndOffset blockAndOffset = line.writeNewLeafNoLog(keys, rid);
			line.addDownNoLog(keys, blockAndOffset);
		}
	}
	
	public void delete(FieldValue[] keys, RID rid) throws Exception
	{
		seek(13);
		this.setEqualsPosMulti(keys);
		if (!line.isLeaf())
		{
			return;
		}
		while (!line.getRid().equals(rid) || line.isTombstone())
		{
			line = line.nextRecord(true);
			
			if (line.isNull())
			{
				return;
			}
			
			if (!line.keysMatch(keys))
			{
				return;
			}
		}
		
		line.markTombstone();
	}
	
	public RID get(FieldValue[] keys) throws Exception
	{
		seek(13);
		this.setEqualsPosMulti(keys);
		if (!line.isLeaf())
		{
			return null;
		}
		
		if (!line.keysMatch(keys))
		{
			return null;
		}

		if (line.isTombstone())
		{
			while (true)
			{
				line = line.nextRecord(true);
			
				if (line.isNull())
				{
					return null;
				}
			
				if (!line.keysMatch(keys))
				{
					return null;
				}
			
				if (!line.isTombstone)
				{
					break;
				}
			}
		}
		
		return line.getRid();
	}
	
	public void massDelete() throws Exception
	{
		seek(13);
		line = new IndexRecord(fileName, offset, tx, true);
		if (line.p.getInt(line.off + 5) == 0)
		{
			return;
		}
		while (!line.isLeaf())
		{
			line = line.getDown(0, true);
		}
		
		while (!line.isNull())
		{
			if (!line.isTombstone())
			{
				line.markTombstone();
			}
			
			line = line.nextRecord(true);
		}
	}
	
	public void massDeleteNoLog() throws Exception
	{
		seek(13);
		line = new IndexRecord(fileName, offset, tx, true);
		if (line.p.getInt(line.off + 5) == 0)
		{
			return;
		}
		while (!line.isLeaf())
		{
			line = line.getDown(0, true);
		}
		
		while (!line.isNull())
		{
			if (!line.isTombstone())
			{
				line.markTombstoneNoLog();
			}
			
			line = line.nextRecord(true);
		}
	}
	
	private static final class BlockAndOffset
	{
		private int block;
		private int offset;
		
		public BlockAndOffset(int block, int offset)
		{
			this.block = block;
			this.offset = offset;
		}
		
		public int block()
		{
			return block;
		}
		
		public int offset()
		{
			return offset;
		}
	}
	
	private final void setEqualsPosMulti(FieldValue[] vals) throws Exception
	{
		//HRDBMSWorker.logger.debug("Searching " + fileName + " for " + search);
		line = new IndexRecord(fileName, offset, tx, true);
		if (line.p.getInt(line.off + 5) == 0)
		{
			return;
		}
		
		while (!line.isLeaf())
		{
			//HRDBMSWorker.logger.debug("Line is " + line);
			if (line.p.getInt(line.off + 5) == 0)
			{
				return;
			}
			
			int i = 0;
			Iterator it = line.internalIterator(types);
			while (it.hasNext())
			{
				ArrayList<Object> k = (ArrayList<Object>)it.next();
				//HRDBMSWorker.logger.debug("Saw internal key: " + k);
				//if (((Comparable)val).compareTo(key) < 1)
				if (compare(vals, k) < 1)
				{
					line = line.getDown(i, true);
					break;
				}
				else
				{
					if (!it.hasNext())
					{
						line = line.getDown(i, true);
						break;
					}
				}
				
				i++;
			}
		}
	}
	
	private final void setEqualsPosMulti(FieldValue[] vals, boolean xLock) throws Exception
	{
		if (xLock)
		{
			setEqualsPosMulti(vals);
			return;
		}
		
		//HRDBMSWorker.logger.debug("Searching " + fileName + " for " + search);
		line = new IndexRecord(fileName, offset, tx);
		if (line.p.getInt(line.off + 5) == 0)
		{
			return;
		}
		
		while (!line.isLeaf())
		{
			//HRDBMSWorker.logger.debug("Line is " + line);
			if (line.p.getInt(line.off + 5) == 0)
			{
				return;
			}
			
			int i = 0;
			Iterator it = line.internalIterator(types);
			while (it.hasNext())
			{
				ArrayList<Object> k = (ArrayList<Object>)it.next();
				//HRDBMSWorker.logger.debug("Saw internal key: " + k);
				//if (((Comparable)val).compareTo(key) < 1)
				if (compare(vals, k) < 1)
				{
					line = line.getDown(i);
					break;
				}
				else
				{
					if (!it.hasNext())
					{
						line = line.getDown(i);
						break;
					}
				}
				
				i++;
			}
		}
	}
	
	private final int compare(FieldValue[] vals, ArrayList<Object> r)
	{
		RowComparator rc = new RowComparator(orders, types);
		ArrayList<Object> lhs = new ArrayList<Object>();
		int i = 0;
		for (FieldValue val : vals)
		{
			lhs.add(val.getValue());
			i++;
		}
		
		return rc.compare(lhs, r);
	}
	
	private final int compare(ArrayList<Object> vals, ArrayList<Object> r)
	{
		RowComparator rc = new RowComparator(orders, types);
		return rc.compare(vals, r);
	}
	
	public void setTransaction(Transaction tx)
	{
		this.tx = tx;
	}
	
	private void seek(int off)
	{
		offset = off;
	}

	public void addSecondaryFilter(Filter filter)
	{
		secondary.add(filter);
	}

	@Override
	public synchronized Index clone()
	{
		final Index retval = new Index(fileName, keys, types, orders);
		if (f != null)
		{
			retval.f = this.f.clone();
		}
		else
		{
			retval.f = null;
		}
		if (secondary != null)
		{
			retval.secondary = deepClone(secondary);
		}
		else
		{
			retval.secondary = null;
		}
		retval.indexOnly = indexOnly;
		if (fetches != null)
		{
			retval.fetches = (ArrayList<Integer>)fetches.clone();
		}
		else
		{
			retval.fetches = null;
		}
		if (fetchTypes != null)
		{
			retval.fetchTypes = (ArrayList<String>)fetchTypes.clone();
		}
		else
		{
			retval.fetchTypes = null;
		}
		retval.delayed = delayed;
		if (renames != null)
		{
			retval.renames = (HashMap<String, String>)renames.clone();
		}
		else
		{
			retval.renames = null;
		}
		if (delayedConditions != null)
		{
			retval.delayedConditions = deepClone(delayedConditions);
			retval.delayed = true;
		}
		else
		{
			retval.delayedConditions = null;
		}

		if (retval.delayedConditions != null)
		{
			for (final Filter filter : retval.delayedConditions)
			{
				if (retval.f != null && filter.equals(retval.f))
				{
					retval.f = null;
				}
				else
				{
					retval.secondary.remove(filter);
				}
			}
			retval.delayedConditions = null;
		}

		return retval;
	}

	public void close()
	{
		if (queue != null)
		{
			queue.close();
		}
		
		keys = null;
		types = null;
		orders = null;
		fetches = null;
		secondary = null;
		fetchTypes = null;
		iwt = null;
		ridList = null;
		terminates = null;row = null;
		myPages = null;
		line = null;
		delayedConditions = null;
		renames = null;
		cols2Pos = null;
	}

	public boolean contains(String col)
	{
		return keys.contains(col);
	}

	public ArrayList<String> getCols()
	{
		return keys;
	}

	public Filter getCondition()
	{
		return f;
	}

	public String getFileName()
	{
		return fileName;
	}

	public Filter getFilter()
	{
		return f;
	}

	public ArrayList<String> getReferencedCols()
	{
		final ArrayList<String> retval = new ArrayList<String>();
		if (f.leftIsColumn())
		{
			retval.add(f.leftColumn());
		}

		if (f.rightIsColumn())
		{
			if (!retval.contains(f.rightColumn()))
			{
				retval.add(f.rightColumn());
			}
		}

		for (final Filter filter : secondary)
		{
			if (filter.leftIsColumn())
			{
				if (!retval.contains(filter.leftColumn()))
				{
					retval.add(filter.leftColumn());
				}
			}

			if (filter.rightIsColumn())
			{
				if (!retval.contains(filter.rightColumn()))
				{
					retval.add(filter.rightColumn());
				}
			}
		}

		return retval;
	}

	public ArrayList<Filter> getSecondary()
	{
		return secondary;
	}

	public Object next() throws Exception
	{
		if (lastThread == null)
		{
			synchronized (this)
			{
				if (lastThread == null)
				{
					lastThread = Thread.currentThread();
				}
			}
		}

		if (Thread.currentThread() != lastThread)
		{
			Exception e = new Exception();
			HRDBMSWorker.logger.error("More than 1 thread in Index.next()", e);
		}

		while (delayed)
		{
			LockSupport.parkNanos(75000);
		}

		if (!positioned)
		{
			queue = new BufferedLinkedBlockingQueue(ResourceManager.QUEUE_SIZE);
			try
			{
				line = new IndexRecord(fileName, offset, tx);
				if (line.p.getInt(line.off + 5) == 0)
				{
					queue.put(new DataEndMarker());
					positioned = true;
				}
				else
				{
					setStartingPos();
					positioned = true;
					iwt = new IndexWriterThread(keys);
					iwt.start();
				}
			}
			catch(Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				throw e;
			}
		}

		Object o;
		o = queue.take();

		if (o instanceof DataEndMarker)
		{
			o = queue.peek();
			if (o == null)
			{
				queue.put(new DataEndMarker());
				return new DataEndMarker();
			}
			else
			{
				queue.put(new DataEndMarker());
				return o;
			}
		}
		
		if (o instanceof Exception)
		{
			throw (Exception)o;
		}
		return o;

	}

	public void open(int device, MetaData meta) throws Exception
	{
		try
		{
			fileName = meta.getDevicePath(device) + fileName;
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.error("", e);
			throw e;
		}

		// if (delayed)
		// {
		// System.out.println("Index is opened delayed");
		// }
	}
	
	public void open()
	{
	}

	public synchronized void reset() throws Exception
	{
		lastThread = null;

		try
		{
			seek(13);
			if (iwt != null)
			{
				iwt.join();
			}
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.error("", e);
			throw e;
		}

		delayed = true;
		positioned = false;
		terminates = new ArrayList<Filter>();

		if (delayedConditions != null)
		{
			for (final Filter filter : delayedConditions)
			{
				if (f != null && filter.equals(f))
				{
					f = null;
				}
				else
				{
					secondary.remove(filter);
				}
			}
			delayedConditions = null;
		}
	}

	public void runDelayed()
	{
		delayed = true;
	}

	public void setCondition(Filter f)
	{
		this.f = f;
	}

	public synchronized void setDelayedConditions(ArrayList<Filter> filters)
	{
		this.delayedConditions = filters;
		for (final Filter filter : filters)
		{
			if (renames != null)
			{
				if (filter.leftIsColumn() && renames.containsKey(filter.leftColumn()))
				{
					filter.updateLeftColumn(renames.get(filter.leftColumn()));
				}

				if (filter.rightIsColumn() && renames.containsKey(filter.rightColumn()))
				{
					filter.updateRightColumn(renames.get(filter.rightColumn()));
				}
			}
			if (f == null)
			{
				f = filter;
			}
			else
			{
				secondary.add(filter);
			}
		}

		delayed = false;
		// System.out.println("Starting index scan after delay");
	}

	public ArrayList<String> setIndexOnly(ArrayList<String> references, ArrayList<String> types)
	{
		indexOnly = true;
		final ArrayList<String> retval = new ArrayList<String>(keys.size());
		int i = 0;
		for (final String col : keys)
		{
			if (references.contains(col))
			{
				fetches.add(i);
				fetchTypes.add(types.get(references.indexOf(col)));
				retval.add(col);
			}

			i++;
		}

		return retval;
	}

	public void setRenames(HashMap<String, String> renames)
	{
		this.renames = renames;
	}

	public boolean startsWith(String col)
	{
		if (keys.get(0).equals(col))
		{
			return true;
		}

		return false;
	}

	@Override
	public String toString()
	{
		return super.toString() + ": " + keys.toString() + "f = " + f + "; secondary = " + secondary;
	}

	private Object calculateSecondaryStarting() throws Exception
	{
		for (final Filter filter : secondary)
		{
			String col2 = null;
			String op2 = filter.op();
			Object val = null;
			if (filter.leftIsColumn())
			{
				col2 = filter.leftColumn();
				val = filter.rightLiteral();
				filter.rightOrig();
			}
			else
			{
				col2 = filter.rightColumn();
				val = filter.leftLiteral();
				filter.leftOrig();
				if (op2.equals("L"))
				{
					op2 = "G";
				}
				else if (op2.equals("LE"))
				{
					op2 = "GE";
				}
				else if (op2.equals("G"))
				{
					op2 = "L";
				}
				else if (op2.equals("GE"))
				{
					op2 = "LE";
				}
			}

			if (keys.get(0).equals(col2) && (!filter.leftIsColumn() || !filter.rightIsColumn()))
			{
				try
				{
					if (op2.equals("L"))
					{
						if (orders.get(0))
						{
							// setFirstPosition();
							// terminate = new Filter(col, "GE", orig);
						}
						else
						{
							return val;
						}
					}
					else if (op2.equals("LE"))
					{
						if (orders.get(0))
						{
							// setFirstPosition();
							// terminate = new Filter(col, "G", orig);
						}
						else
						{
							return val;
						}
					}
					else if (op2.equals("G"))
					{
						if (orders.get(0))
						{
							return val;
						}
						else
						{
							// setFirstPosition();
							// terminate = new Filter(col, "LE", orig);
						}
					}
					else if (op2.equals("GE"))
					{
						if (orders.get(0))
						{
							return val;
						}
						else
						{
							// setFirstPosition();
							// terminate = new Filter(col, "L", orig);
						}
					}
					else if (op2.equals("E"))
					{
						return val;
					}
					else if (op2.equals("NE"))
					{
						// setFirstPosition();
					}
					else if (op2.equals("LI"))
					{
						final String prefix = getPrefix(val);
						if (prefix.length() > 0)
						{
							if (orders.get(0))
							{
								return prefix;
							}
							else
							{
								return nextGT(prefix);
							}
						}
						else
						{
							// setFirstPosition();
						}
					}
					else if (op2.equals("NL"))
					{
						// setFirstPosition();
					}
					else
					{
						HRDBMSWorker.logger.error("Unknown operator in Index: " + op);
						throw new Exception("Unknown operator in Index: " + op);
					}
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.error("", e);
					throw e;
				}
			}
			else
			{
				// setFirstPosition();
			}
		}

		return null;
	}

	private void calculateSecondaryTerminations() throws Exception
	{
		for (final Filter filter : secondary)
		{
			String col2 = null;
			String op2 = filter.op();
			Object val = null;
			String orig = null;
			if (filter.leftIsColumn())
			{
				col2 = filter.leftColumn();
				val = filter.rightLiteral();
				orig = filter.rightOrig();
			}
			else
			{
				col2 = filter.rightColumn();
				val = filter.leftLiteral();
				orig = filter.leftOrig();
				if (op2.equals("L"))
				{
					op2 = "G";
				}
				else if (op2.equals("LE"))
				{
					op2 = "GE";
				}
				else if (op2.equals("G"))
				{
					op2 = "L";
				}
				else if (op2.equals("GE"))
				{
					op2 = "LE";
				}
			}

			if (keys.get(0).equals(col2) && (!filter.leftIsColumn() || !filter.rightIsColumn()))
			{
				try
				{
					if (op2.equals("L"))
					{
						if (orders.get(0))
						{
							terminates.add(new Filter(col2, "GE", orig));
						}
						else
						{
							// setEqualsPos(val);
							// doFirst = false;
						}
					}
					else if (op2.equals("LE"))
					{
						if (orders.get(0))
						{
							// setFirstPosition();
							terminates.add(new Filter(col2, "G", orig));
						}
						else
						{
							// setEqualsPos(val);
							// doFirst = false;
						}
					}
					else if (op2.equals("G"))
					{
						if (orders.get(0))
						{
							// setEqualsPos(val);
							// doFirst = false;
						}
						else
						{
							// setFirstPosition();
							terminates.add(new Filter(col2, "LE", orig));
						}
					}
					else if (op2.equals("GE"))
					{
						if (orders.get(0))
						{
							// setEqualsPos(val);
							// doFirst = false;
						}
						else
						{
							// setFirstPosition();
							terminates.add(new Filter(col2, "L", orig));
						}
					}
					else if (op2.equals("E"))
					{
						// setEqualsPos(val);
						// doFirst = false;
						terminates.add(new Filter(col2, "NE", orig));
					}
					else if (op2.equals("NE"))
					{
						// setFirstPosition();
					}
					else if (op2.equals("LI"))
					{
						final String prefix = getPrefix(val);
						if (prefix.length() > 0)
						{
							if (orders.get(0))
							{
								// setEqualsPos(prefix);
								// doFirst = false;
								// equalVal = prefix;
								terminates.add(new Filter(col2, "GE", "'" + nextGT(prefix) + "'"));
							}
							else
							{
								// setEqualsPos(nextGT(prefix));
								// doFirst = false;
								// equalVal = nextGT(prefix);
								terminates.add(new Filter(col2, "L", "'" + prefix + "'"));
							}
						}
						else
						{
							// setFirstPosition();
						}
					}
					else if (op.equals("NL"))
					{
						// setFirstPosition();
					}
					else
					{
						HRDBMSWorker.logger.error("Unknown operator in Index: " + op);
						throw new Exception("Unknown operator in Index: " + op);
					}
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.error("", e);
					throw e;
				}
			}
			else
			{
				// setFirstPosition();
			}
		}
	}

	private boolean currentKeySatisfies() throws Exception
	{
		if (line.isNull())
		{
			return false;
		}
		
		if (line.isTombstone())
		{
			return false;
		}

		final ArrayList<Object> row = line.getKeys(types);

		try
		{
			if (!f.passes(row, cols2Pos))
			{
				// System.out.println("Filter " + f + " returns false");
				return false;
			}
			else
			{
				// System.out.println("Filter " + f + " returns true");
			}

			for (final Filter filter : secondary)
			{
				if (!filter.passes(row, cols2Pos))
				{
					// System.out.println("Filter " + filter +
					// " returns false");
					return false;
				}
				else
				{
					// System.out.println("Filter " + filter +
					// " returns false");
				}
			}

			return true;
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.error("", e);
			throw e;
		}
	}

	private ArrayList<Filter> deepClone(ArrayList<Filter> in)
	{
		final ArrayList<Filter> out = new ArrayList<Filter>();
		for (final Filter f : in)
		{
			out.add(f.clone());
		}

		return out;
	}

	private Object getObject(String val, String type) throws Exception
	{
		if (type.equals("INT"))
		{
			return Utils.parseLong(val);
		}
		else if (type.equals("FLOAT"))
		{
			return Utils.parseDouble(val);
		}
		else if (type.equals("CHAR"))
		{
			return val;
		}
		else if (type.equals("LONG"))
		{
			return Utils.parseLong(val);
		}
		else if (type.equals("DATE"))
		{
			try
			{
				return DateParser.parse(val);
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				throw e;
			}
		}

		return null;
	}

	private String getPrefix(Object x)
	{
		return ((String)x).substring(0, ((String)x).indexOf("%"));
	}

	private long getPartialRid()
	{
		return line.getPartialRid();
	}

	private boolean marksEnd() throws Exception
	{
		if (line.isNull())
		{
			return true;
		}

		if (terminates.size() == 0)
		{
			return false;
		}

		final ArrayList<Object> row = line.getKeys(types);

		try
		{
			for (final Filter terminate : terminates)
			{
				if (terminate.passes(row, cols2Pos))
				{
					return true;
				}
			}
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.error("", e);
			throw e;
		}

		return false;
	}

	private String nextGT(String prefix)
	{
		String retval = prefix.substring(0, prefix.length() - 1);
		char x = prefix.charAt(prefix.length() - 1);
		x++;
		retval += x;
		return retval;
	}
	
	private final void setEqualsPos(Object val) throws Exception
	{
		line = new IndexRecord(fileName, offset, tx);
		
		while (!line.isLeaf())
		{
			int i = 0;
			Iterator it = line.internalIterator(types);
			while (it.hasNext())
			{
				ArrayList<Object> k = (ArrayList<Object>)it.next();
				Object key = k.get(keys.indexOf(col));
				if (val instanceof Long && key instanceof Integer)
				{
					key = ((Integer)key).longValue();
				}
				if (val instanceof Integer && key instanceof Long)
				{
					val = ((Integer)val).longValue();
				}
				if (((Comparable)val).compareTo(key) < 1)
				{
					line = line.getDown(i);
					break;
				}
				else if (!it.hasNext())
				{
					line = line.getDown(i);
					break;
				}
				
				i++;
			}
		}
	}

	/*
	private final void setEqualsPos(Object val)
	{
		int count = 0;
		try
		{
			line = in.readLine();
			count++;
			long oldOff = -1;
			int i = -1;
			FastStringTokenizer tokens = null;
			while (true)
			{
				while (!line.equals("\u0000"))
				{
					tokens = new FastStringTokenizer(line, "|", false);
					i = 0;
					// System.out.println("Col = " + col);
					while (i < keys.indexOf(col))
					{
						tokens.nextToken();
						i++;
					}
					final String keyVal = tokens.nextToken();
					i++;

					final String type = types.get(keys.indexOf(col));
					Object key = null;
					try
					{
						key = getObject(keyVal, type);
					}
					catch (final Exception e)
					{
						System.out.println("Error parsing value in Index.");
						System.out.println("Line is " + line);
						System.out.println("File is " + in);
						System.exit(1);
					}
					if (orders.get(0))
					{
						if (key instanceof Double)
						{
							if (val instanceof Long)
							{
								val = ((Long)val).doubleValue();
							}
							else if (val instanceof Integer)
							{
								val = ((Integer)val).doubleValue();
							}
						}
						else if (key instanceof Long)
						{
							if (val instanceof Integer)
							{
								val = ((Integer)val).longValue();
							}
						}

						if (((Comparable)key).compareTo(val) < 0)
						{
							while (i < keys.size())
							{
								tokens.nextToken();
								i++;
							}

							try
							{
								oldOff = Utils.parseLong(tokens.nextToken());
							}
							catch (final Exception e)
							{
								HRDBMSWorker.logger.error("", e);
								System.out.println("Ran out of tokens reading line " + line);
								System.exit(1);
							}
						}
						else if (((Comparable)key).compareTo(val) == 0)
						{
							if (keys.size() == 1)
							{
								while (i < keys.size())
								{
									tokens.nextToken();
									i++;
								}

								oldOff = Utils.parseLong(tokens.nextToken());
							}
							break;
						}
						else
						{
							break;
						}
					}
					else
					{
						if (((Comparable)key).compareTo(val) > 0)
						{
							while (i < keys.size())
							{
								tokens.nextToken();
								i++;
							}

							oldOff = Utils.parseLong(tokens.nextToken());
						}
						else if (((Comparable)key).compareTo(val) == 0)
						{
							if (keys.size() == 1)
							{
								while (i < keys.size())
								{
									tokens.nextToken();
									i++;
								}

								oldOff = Utils.parseLong(tokens.nextToken());
							}
							break;
						}
						else
						{
							break;
						}
					}

					line = in.readLine();
					count++;
				}

				if (oldOff == -1)
				{
					while (i < keys.size())
					{
						tokens.nextToken();
						i++;
					}

					oldOff = Utils.parseLong(tokens.nextToken());
				}

				if (oldOff >= 0)
				{
					break;
				}
				in.seek(-1 * oldOff);
				line = in.readLine();
				oldOff = -1;
				count++;
				if (count > 500)
				{
					System.out.println(count + " rows have been read trying to find starting position based on " + val + " in index " + fileName);
				}
			}
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.error("", e);
			System.exit(1);
		}

		// System.out.println("First line is " + line);
	}
	*/
	
	private final void setFirstPosition() throws Exception
	{
		line = new IndexRecord(fileName, offset, tx);
		while (!line.isLeaf())
		{
			line = line.getDown(0);
		}
	}
	
	/*
	private final void setFirstPosition()
	{
		try
		{
			line = in.readLine();
			long off = -1;
			final FastStringTokenizer tokens = new FastStringTokenizer("", "|", false);
			while (true)
			{
				tokens.reuse(line, "|", false);
				int i = 0;
				while (i < keys.size())
				{
					tokens.nextToken();
					i++;
				}
				off = Utils.parseLong(tokens.nextToken());
				if (off >= 0)
				{
					break;
				}
				in.seek(-1 * off);
				line = in.readLine();
			}
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.error("", e);
			System.exit(1);
		}
	}
*/

	private final void setStartingPos() throws Exception
	{
		boolean doFirst = true;
		Object equalVal = null;
		col = null;
		op = f.op();
		Object val = null;
		String orig = null;
		if (f.leftIsColumn())
		{
			col = f.leftColumn();
			val = f.rightLiteral();
			orig = f.rightOrig();
		}
		else
		{
			col = f.rightColumn();
			val = f.leftLiteral();
			orig = f.leftOrig();
			switchOp();
		}

		if (keys.get(0).equals(col) && (!f.leftIsColumn() || !f.rightIsColumn()))
		{
			try
			{
				if (op.equals("L"))
				{
					if (orders.get(0))
					{
						// setFirstPosition();
						terminates.add(new Filter(col, "GE", orig));
					}
					else
					{
						// setEqualsPos(val);
						doFirst = false;
					}
				}
				else if (op.equals("LE"))
				{
					if (orders.get(0))
					{
						// setFirstPosition();
						terminates.add(new Filter(col, "G", orig));
					}
					else
					{
						// setEqualsPos(val);
						doFirst = false;
					}
				}
				else if (op.equals("G"))
				{
					if (orders.get(0))
					{
						// setEqualsPos(val);
						doFirst = false;
					}
					else
					{
						// setFirstPosition();
						terminates.add(new Filter(col, "LE", orig));
					}
				}
				else if (op.equals("GE"))
				{
					if (orders.get(0))
					{
						// setEqualsPos(val);
						doFirst = false;
					}
					else
					{
						// setFirstPosition();
						terminates.add(new Filter(col, "L", orig));
					}
				}
				else if (op.equals("E"))
				{
					// setEqualsPos(val);
					doFirst = false;
					terminates.add(new Filter(col, "NE", orig));
				}
				else if (op.equals("NE"))
				{
					// setFirstPosition();
				}
				else if (op.equals("LI"))
				{
					final String prefix = getPrefix(val);
					if (prefix.length() > 0)
					{
						if (orders.get(0))
						{
							// setEqualsPos(prefix);
							doFirst = false;
							equalVal = prefix;
							terminates.add(new Filter(col, "GE", "'" + nextGT(prefix) + "'"));
						}
						else
						{
							// setEqualsPos(nextGT(prefix));
							doFirst = false;
							equalVal = nextGT(prefix);
							terminates.add(new Filter(col, "L", "'" + prefix + "'"));
						}
					}
					else
					{
						// setFirstPosition();
					}
				}
				else if (op.equals("NL"))
				{
					// setFirstPosition();
				}
				else
				{
					HRDBMSWorker.logger.error("Unknown operator in Index: " + op);
					throw new Exception("Unknown operator in Index: " + op);
				}
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				throw e;
			}
		}
		else
		{
			// setFirstPosition();
			col = keys.get(0);
		}

		if (!doFirst)
		{
			if (equalVal == null)
			{
				this.setEqualsPos(val);
				// System.out.println("Starting with " + val);
			}
			else
			{
				this.setEqualsPos(equalVal);
				// System.out.println("Starting with " + equalVal);
			}

			calculateSecondaryTerminations();
			// System.out.println("Terminating conditions are " + terminates);
		}
		else
		{
			final Object val2 = calculateSecondaryStarting();
			if (val2 == null)
			{
				this.setFirstPosition();
				// System.out.println("Starting at beginning of index");
			}
			else
			{
				this.setEqualsPos(val2);
				// System.out.println("Starting with " + val2);
			}

			calculateSecondaryTerminations();
			// System.out.println("Terminating conditions are " + terminates);
		}
	}

	private void switchOp()
	{
		if (op.equals("E") || op.equals("NE") || op.equals("LI") || op.equals("NL"))
		{
			return;
		}

		if (op.equals("L"))
		{
			op = "G";
			return;
		}

		if (op.equals("LE"))
		{
			op = "GE";
			return;
		}

		if (op.equals("G"))
		{
			op = "L";
			return;
		}

		if (op.equals("GE"))
		{
			op = "LE";
			return;
		}
	}

	private final class IndexWriterThread extends ThreadPoolThread
	{
		private final ArrayList<String> keys;

		public IndexWriterThread(ArrayList<String> keys)
		{
			this.keys = keys;
		}

		@Override
		public final void run()
		{
			while (true)
			{
				if (!indexOnly)
				{
					try
					{
						while (!currentKeySatisfies())
						{
							if (marksEnd())
							{
								queue.put(new DataEndMarker());
								return;
							}

							line = line.nextRecord();
						}

						final ArrayList<Object> al = new ArrayList<Object>(1);
						al.add(getPartialRid());
						queue.put(al);

						line = line.nextRecord();
					}
					catch (final Exception e)
					{
						HRDBMSWorker.logger.error("", e);
						try
						{
							queue.put(e); 
						}
						catch(Exception f)
						{}
						return;
					}
				}
				else
				{
					row = new ArrayList<Object>(fetches.size());
					try
					{
						while (!currentKeySatisfies())
						{
							if (marksEnd())
							{
								queue.put(new DataEndMarker());
								return;
							}

							line = line.nextRecord();
						}

						ridList.add(getPartialRid());
						ArrayList<Object> keys = line.getKeys(types);
						for (final int pos : fetches)
						{
							final Object val = keys.get(pos);
							row.add(val);
						}

						line = line.nextRecord();
					}
					catch (final Exception e)
					{
						HRDBMSWorker.logger.error("", e);
						try
						{
							queue.put(e);
						}
						catch(Exception f)
						{}
						return;
					}
					try
					{
						queue.put(row.clone());
					}
					catch(Exception e)
					{
						HRDBMSWorker.logger.error("", e);
						try
						{
							queue.put(e);
						}
						catch(Exception f)
						{}
						return;
					}
				}
			}
		}
	}
	
	private class IndexRecord
	{
		private String file;
		private Transaction tx;
		private Page p;
		private Block b;
		private boolean isNull = false;
		private int off;
		//private int prevBlock, prevOff, nextBlock, nextOff, upBlock, upOff, node, device, blockNum, recNum;
		private boolean isLeaf = false;;
		private int keyOff;
		//private int numKeys;
		private boolean isTombstone = false;
		
		public String toString()
		{
			return "IsNull = " + isNull + "; IsLeaf = " + isLeaf + "; Block = " + b + ";";
		}
		
		public IndexRecord(String file, long offset, Transaction tx) throws Exception
		{
			this.file = file;
			this.tx = tx;
			b = new Block(file, (int)(offset / Page.BLOCK_SIZE));
			off = (int)(offset % Page.BLOCK_SIZE);
			keyOff = off + 9+128*8+8;
			LockManager.sLock(b, tx.number());
			tx.requestPage(b);
			try
			{
				p = tx.getPage(b);
			}
			catch(Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				throw e;
			}
			
			isLeaf = (p.get(off+0) == 1);
			if (p.get(off+0) == 2)
			{
				isLeaf = true;
				isTombstone = true;
			}
		}
		
		public IndexRecord(String file, long offset, Transaction tx, boolean x) throws Exception
		{
			this.file = file;
			this.tx = tx;
			b = new Block(file, (int)(offset / Page.BLOCK_SIZE));
			p = myPages.get(b);
			off = (int)(offset % Page.BLOCK_SIZE);
			keyOff = off + 9+128*8+8;
			
			if (p == null)
			{
				LockManager.xLock(b, tx.number());
				tx.requestPage(b);
				try
				{
					p = tx.getPage(b);
				}
				catch(Exception e)
				{
					HRDBMSWorker.logger.error("", e);
					throw e;
				}
				
				myPages.put(b, p);
			}
			
			isLeaf = (p.get(off+0) == 1);
			if (p.get(off+0) == 2)
			{
				isLeaf = true;
				isTombstone = true;
			}
		}
		
		public boolean containsRoom()
		{
			int numKeys = p.getInt(off + 5);
			return numKeys < 128;
		}
		
		public boolean keysMatch(FieldValue[] keys) throws Exception
		{
			ArrayList<Object> r = getKeys(types);
			ArrayList<Object> lhs = new ArrayList<Object>();

			int i = 0;
			for (FieldValue val : keys)
			{
				lhs.add(val.getValue());
				i++;
			}
			
			RowComparator rc = new RowComparator(orders, types);
			return rc.compare(lhs, r) == 0;
		}
		
		public RID getRid()
		{
			return new RID(p.getInt(off+25), p.getInt(off+29), p.getInt(off+33), p.getInt(off+37));
		}
		
		public IndexRecord getDown(int num) throws Exception
		{
			int block = p.getInt(off+9+num*8);
			int o = p.getInt(off+13+num*8);
			if (o < 0)
			{
				HRDBMSWorker.logger.debug("Error trying to get down pointer #" + num + " from index record " + this);
			}
			return new IndexRecord(file, block, o, tx);
		}
		
		public IndexRecord getDown(int num, boolean x) throws Exception
		{
			if (!x)
			{
				return getDown(num);
			}
			
			int block = p.getInt(off+9+num*8);
			int o = p.getInt(off+13+num*8);
			return new IndexRecord(file, block, o, tx, true);
		}
		
		public void markTombstone() throws Exception
		{
			byte[] before = new byte[1];
			byte[] after = new byte[1];
			before[0] = p.get(off+0);
			after[0] = (byte)2;
			DeleteLogRec rec = tx.delete(before, after, off+0, p.block());
			p.write(off+0, after, tx.number(), rec.lsn());
		}
		
		public void markTombstoneNoLog() throws Exception
		{
			byte[] after = new byte[1];
			after[0] = (byte)2;
			long lsn = LogManager.getLSN();
			p.write(off+0, after, tx.number(), lsn);
		}
		
		private int getPageFreeBytes()
		{
			int firstFree = p.getInt(5);
			int lastFree = p.getInt(9);
			return lastFree - firstFree + 1;
		}
		
		private byte[] genKeyBytes(FieldValue[] keys) throws Exception
		{
			int size = keys.length;
			int i = 0;
			for (String type : types)
			{
				if (type.equals("INT"))
				{
					size += 4;
				}
				else if (type.equals("LONG"))
				{
					size += 8;
				}
				else if (type.equals("FLOAT"))
				{
					size += 8;
				}
				else if (type.equals("DATE"))
				{
					size += 8;
				}
				else if (type.equals("CHAR"))
				{
					size += 4;
					String val = (String)keys[i].getValue();
					size += val.getBytes("UTF-8").length;
				}
				else
				{
					HRDBMSWorker.logger.error("Unknown type in IndexRecord.genKeyBytes(): " + type);
					throw new Exception("Unknown type in IndexRecord.genKeyBytes(): " + type);
				}
				
				i++;
			}
			
			ByteBuffer bb = ByteBuffer.allocate(size);
			i = 0;
			bb.position(0);
			for (String type : types)
			{
				bb.put((byte)0); //not null;
				if (type.equals("INT"))
				{
					bb.putInt(((Integer)keys[i].getValue()));
				}
				else if (type.equals("LONG"))
				{
					bb.putLong(((Long)keys[i].getValue()));
				}
				else if (type.equals("FLOAT"))
				{
					bb.putDouble(((Double)keys[i].getValue()));
				}
				else if (type.equals("DATE"))
				{
					bb.putLong(((MyDate)keys[i].getValue()).getTime());
				}
				else if (type.equals("CHAR"))
				{
					String val = (String)keys[i].getValue();
					byte[] bytes = val.getBytes("UTF-8");
					bb.putInt(bytes.length);
					try
					{
						bb.put(bytes);
					}
					catch(Exception e)
					{
						HRDBMSWorker.logger.error("", e);
						throw e;
					}
				}
				
				i++;
			}
			
			return bb.array();
		}
		
		private void updateParentDownKey(byte[] keyBytes) throws Exception
		{
			IndexRecord up = getInternalUp(true);
			if (!up.isNull())
			{
				byte[] highKey = up.getDownKey(this.getBlockAndOffset());
				ArrayList<Object> keyBytesRow = convertBytesToRow(keyBytes);
				ArrayList<Object> highKeyRow = convertBytesToRow(highKey);
				RowComparator rc = new RowComparator(orders, types);
				
				if (rc.compare(keyBytesRow, highKeyRow) > 0)
				{
					up.replaceDownKey(this.getBlockAndOffset(), keyBytes);
					up = getInternalUp(true);
					up.updateParentDownKey(keyBytes);
				}
			}
		}
		
		private void updateParentDownKeyNoLog(byte[] keyBytes) throws Exception
		{
			IndexRecord up = getInternalUp(true);
			if (!up.isNull())
			{
				byte[] highKey = up.getDownKey(this.getBlockAndOffset());
				ArrayList<Object> keyBytesRow = convertBytesToRow(keyBytes);
				ArrayList<Object> highKeyRow = convertBytesToRow(highKey);
				RowComparator rc = new RowComparator(orders, types);
				
				if (rc.compare(keyBytesRow, highKeyRow) > 0)
				{
					up.replaceDownKeyNoLog(this.getBlockAndOffset(), keyBytes);
					up = getInternalUp(true);
					up.updateParentDownKeyNoLog(keyBytes);
				}
			}
		}
		
		public void addDown(FieldValue[] keys, BlockAndOffset bao) throws Exception
		{	
			ArrayList<Object> lhs = new ArrayList<Object>();
			int i = 0;
			for (FieldValue val : keys)
			{
				lhs.add(val.getValue());
				i++;
			}
			
			//generate key bytes
			byte[] keyBytes = genKeyBytes(keys);
			
			//can we just add this to the existing record?
			int freeBytes = this.getPageFreeBytes();
			
			if (keyBytes.length <= freeBytes)
			{
				updateParentDownKey(keyBytes);
				//increment numKeys by 1
				byte[] before = new byte[4];
				p.get(off+5, before);
				ByteBuffer after = ByteBuffer.allocate(4);
				after.position(0);
				after.putInt(p.getInt(off + 5)+1);
				InsertLogRec rec = tx.insert(before,  after.array(), off+5, p.block());
				p.write(off+5, after.array(), tx.number(), rec.lsn());
				
				//insert new keyBytes
				int offset = off+1041; //first key bytes
				i = 0;
				while (i < p.getInt(off + 5)-1)
				{
					int j = 0;
					ArrayList<Object> rhs = new ArrayList<Object>();
					int start = offset;
					for (String type : types)
					{
						offset++; //skip null marker
						if (type.equals("INT"))
						{
							rhs.add(p.getInt(offset));
							offset += 4;
						}
						else if (type.equals("LONG"))
						{
							rhs.add(p.getLong(offset));
							offset += 8;
						}
						else if (type.equals("FLOAT"))
						{
							rhs.add(p.getDouble(offset));
							offset += 8;
						}
						else if (type.equals("DATE"))
						{
							rhs.add(new MyDate(p.getLong(offset)));
							offset += 8;
						}
						else if (type.equals("CHAR"))
						{
							int length = p.getInt(offset);
							offset += 4;
							byte[] bytes = new byte[length];
							p.get(offset, bytes);
							offset += length;
							rhs.add(new String(bytes, "UTF-8"));
						}
						else
						{
							HRDBMSWorker.logger.error("Unknown data type in IndexRecord.addDown(): " + type);
							throw new Exception("Unknown data type in IndexRecord.addDown(): " + type);
						}
						
						j++;
					}
					
					RowComparator rc = new RowComparator(orders, types);
					if (rc.compare(lhs, rhs) < 0)
					{
						//it goes here
						//figure out number of bytes to move from start
						int firstFree = p.getInt(5);
						int num2Move = firstFree - start;
						byte[] toMove = new byte[num2Move];
						byte[] bef = new byte[num2Move];
						p.get(start, toMove);
						p.get(start+keyBytes.length, bef);
						rec = tx.insert(bef, toMove, start+keyBytes.length, p.block());
						p.write(start+keyBytes.length, toMove, tx.number(), rec.lsn());
						bef = new byte[keyBytes.length];
						p.get(start, bef);
						rec = tx.insert(bef, keyBytes, start, p.block());
						p.write(start, keyBytes, tx.number(), rec.lsn());
						break;
					}
					
					i++;
				}
				
				if (i == p.getInt(off + 5)-1)
				{
					byte[] bef = new byte[keyBytes.length];
					p.get(offset, bef);
					rec = tx.insert(bef, keyBytes, offset, p.block());
					p.write(offset, keyBytes, tx.number(), rec.lsn());
				}
				
				//update free space
				int firstFree = p.getInt(5);
				firstFree += keyBytes.length;
				byte[] bef = new byte[4];
				p.get(5, bef);
				ByteBuffer aft = ByteBuffer.allocate(4);
				aft.position(0);
				aft.putInt(firstFree);
				rec = tx.insert(bef, aft.array(), 5, p.block());
				p.write(5, aft.array(), tx.number(), rec.lsn());
				firstFree -= 13;
				p.get(14, bef);
				aft.position(0);
				aft.putInt(firstFree);
				rec = tx.insert(bef, aft.array(), 14, p.block());
				p.write(14, aft.array(), tx.number(), rec.lsn());
				
				//insert new down pointer
				if (i == p.getInt(off + 5)-1)
				{
					int downOffset = off+9 + 8 * (p.getInt(off + 5)-1);
					bef = new byte[8];
					p.get(downOffset, bef);
					aft = ByteBuffer.allocate(8);
					aft.position(0);
					aft.putInt(bao.block());
					aft.putInt(bao.offset());
					rec = tx.insert(bef, aft.array(), downOffset, p.block());
					p.write(downOffset, aft.array(), tx.number(), rec.lsn());
				}
				else
				{
					int downOffset = off+9 + 8 * i;
					int num2Move = (127-i)*8;
					bef = new byte[num2Move];
					byte[] a = new byte[num2Move];
					p.get(downOffset+8, bef);
					p.get(downOffset, a);
					rec = tx.insert(bef, a, downOffset+8, p.block());
					p.write(downOffset+8, a, tx.number(), rec.lsn());
					bef = new byte[8];
					p.get(downOffset, bef);
					aft = ByteBuffer.allocate(8);
					aft.position(0);
					aft.putInt(bao.block());
					aft.putInt(bao.offset());
					rec = tx.insert(bef, aft.array(), downOffset, p.block());
					p.write(downOffset, aft.array(), tx.number(), rec.lsn());
				}
				
				//attach previous and next and up
				if (i == p.getInt(off + 5)-1) 
				{
					int pb;
					int po;
					IndexRecord pr;
					
					if (i == 0)
					{
						pr = new IndexRecord();
						pr.isNull = true;
					}
					else
					{
						pb = p.getInt(off+9 + (p.getInt(off + 5)-2) * 8);
						po = p.getInt(off+13 + (p.getInt(off + 5)-2) * 8);
						pr = new IndexRecord(file, pb, po, tx, true);
					}
					IndexRecord nr;
					if (pr.isNull())
					{
						nr = pr;
					}
					else
					{
						nr = pr.nextRecord(true);
					}
					IndexRecord curr = new IndexRecord(file, bao.block(), bao.offset(), tx, true);
					if (!pr.isNull())
					{
						pr.setNext(curr);
						curr.setPrev(pr);
					}
					else
					{
						curr.setPrev(new BlockAndOffset(0, 0));
					}
					if (!nr.isNull())
					{
						curr.setNext(nr);
						nr.setPrev(curr);
					}
					else
					{
						curr.setNext(new BlockAndOffset(0, 0));
					}
						
					curr.setUp(this);
				}
				else
				{
					int nb = p.getInt(off+9 + (i+1) * 8);
					int no = p.getInt(off+13 + (i+1) * 8);
					IndexRecord nr = new IndexRecord(file, nb, no, tx, true);
					IndexRecord pr = nr.prevRecord(true);
					IndexRecord curr = new IndexRecord(file, bao.block(), bao.offset(), tx, true);
					nr.setPrev(curr);
					curr.setNext(nr);
					if (!pr.isNull())
					{
						pr.setNext(curr);
						curr.setPrev(pr);
					}
					else
					{
						curr.setPrev(new BlockAndOffset(0, 0));
					}
					
					curr.setUp(this);
				}
			}
			else
			{
				if (p.getInt(9) != Page.BLOCK_SIZE - 1 && p.block().number() != 0)
				{
					//no room to extend internal record with a new key
					ByteBuffer bb = ByteBuffer.allocate(Page.BLOCK_SIZE);
					i = 0;
					while (i < Page.BLOCK_SIZE)
					{
						bb.put((byte)2);
						i++;
					}
				
					int newNum = FileManager.addNewBlock(file, bb, tx);
					Block bl = new Block(file, newNum);
					LockManager.xLock(bl, tx.number());
					tx.requestPage(bl);
					Page p2 = null;
					try
					{
						p2 = tx.getPage(bl);
					}
					catch(Exception e)
					{
						HRDBMSWorker.logger.error("", e);
						throw e;
					}
				
					//move current index record to start of new page
					int numBytes = p.getInt(5);
					byte[] bytes = new byte[numBytes];
					p.get(0, bytes);
					byte[] bef = new byte[numBytes];
					p2.get(0, bef);
					InsertLogRec rec = tx.insert(bef, bytes, 0, p2.block());
					p2.write(0, bytes, tx.number(), rec.lsn());
					bef = new byte[4];
					p2.get(9, bef);
					ByteBuffer aft = ByteBuffer.allocate(4);
					aft.position(0);
					aft.putInt(Page.BLOCK_SIZE-1);
					rec = tx.insert(bef, aft.array(), 9, p2.block());
					p2.write(9, aft.array(), tx.number(), rec.lsn());
				
					//redirect pointer from parent
					IndexRecord parent = this.getInternalUp(true);
					IndexRecord newInternal = new IndexRecord(file, bl.number(), 13, tx, true, p2);
					newInternal.setChildUpPointers();
				
					parent.replaceDownPointer(this.getBlockAndOffset(), newInternal.getBlockAndOffset());
					newInternal.addDown(keys, bao);
				}
				else
				{
					split();
					seek(13);
					setEqualsPosMulti(keys);
					
					if (line.isLeaf())
					{
						line = line.getUp(true);
					}
					
					line.addDown(keys, bao);
				}
			}
		}
		
		public void addDownNoLog(FieldValue[] keys, BlockAndOffset bao) throws Exception
		{	
			ArrayList<Object> lhs = new ArrayList<Object>();
			int i = 0;
			for (FieldValue val : keys)
			{
				lhs.add(val.getValue());
				i++;
			}
			
			//generate key bytes
			byte[] keyBytes = genKeyBytes(keys);
			
			//can we just add this to the existing record?
			int freeBytes = this.getPageFreeBytes();
			
			if (keyBytes.length <= freeBytes)
			{
				updateParentDownKeyNoLog(keyBytes);
				//increment numKeys by 1
				ByteBuffer after = ByteBuffer.allocate(4);
				after.position(0);
				after.putInt(p.getInt(off + 5)+1);
				long lsn = LogManager.getLSN();
				p.write(off+5, after.array(), tx.number(), lsn);
				
				//insert new keyBytes
				int offset = off+1041; //first key bytes
				i = 0;
				while (i < p.getInt(off + 5)-1)
				{
					int j = 0;
					ArrayList<Object> rhs = new ArrayList<Object>();
					int start = offset;
					for (String type : types)
					{
						offset++; //skip null marker
						if (type.equals("INT"))
						{
							rhs.add(p.getInt(offset));
							offset += 4;
						}
						else if (type.equals("LONG"))
						{
							rhs.add(p.getLong(offset));
							offset += 8;
						}
						else if (type.equals("FLOAT"))
						{
							rhs.add(p.getDouble(offset));
							offset += 8;
						}
						else if (type.equals("DATE"))
						{
							rhs.add(new MyDate(p.getLong(offset)));
							offset += 8;
						}
						else if (type.equals("CHAR"))
						{
							int length = p.getInt(offset);
							offset += 4;
							byte[] bytes = new byte[length];
							p.get(offset, bytes);
							offset += length;
							rhs.add(new String(bytes, "UTF-8"));
						}
						else
						{
							HRDBMSWorker.logger.error("Unknown data type in IndexRecord.addDown(): " + type);
							throw new Exception("Unknown data type in IndexRecord.addDown(): " + type);
						}
						
						j++;
					}
					
					RowComparator rc = new RowComparator(orders, types);
					if (rc.compare(lhs, rhs) < 0)
					{
						//it goes here
						//figure out number of bytes to move from start
						int firstFree = p.getInt(5);
						int num2Move = firstFree - start;
						byte[] toMove = new byte[num2Move];
						p.get(start, toMove);
						lsn = LogManager.getLSN();
						p.write(start+keyBytes.length, toMove, tx.number(), lsn);
						lsn = LogManager.getLSN();
						p.write(start, keyBytes, tx.number(), lsn);
						break;
					}
					
					i++;
				}
				
				if (i == p.getInt(off + 5)-1)
				{
					lsn = LogManager.getLSN();
					p.write(offset, keyBytes, tx.number(), lsn);
				}
				
				//update free space
				int firstFree = p.getInt(5);
				firstFree += keyBytes.length;
				ByteBuffer aft = ByteBuffer.allocate(4);
				aft.position(0);
				aft.putInt(firstFree);
				lsn = LogManager.getLSN();
				p.write(5, aft.array(), tx.number(), lsn);
				firstFree -= 13;
				aft.position(0);
				aft.putInt(firstFree);
				lsn = LogManager.getLSN();
				p.write(14, aft.array(), tx.number(), lsn);
				
				//insert new down pointer
				if (i == p.getInt(off + 5)-1)
				{
					int downOffset = off+9 + 8 * (p.getInt(off + 5)-1);
					aft = ByteBuffer.allocate(8);
					aft.position(0);
					aft.putInt(bao.block());
					aft.putInt(bao.offset());
					lsn = LogManager.getLSN();
					p.write(downOffset, aft.array(), tx.number(), lsn);
				}
				else
				{
					int downOffset = off+9 + 8 * i;
					int num2Move = (127-i)*8;
					byte[] a = new byte[num2Move];
					p.get(downOffset, a);
					lsn = LogManager.getLSN();
					p.write(downOffset+8, a, tx.number(), lsn);
					aft = ByteBuffer.allocate(8);
					aft.position(0);
					aft.putInt(bao.block());
					aft.putInt(bao.offset());
					lsn = LogManager.getLSN();
					p.write(downOffset, aft.array(), tx.number(), lsn);
				}
				
				//attach previous and next and up
				if (i == p.getInt(off + 5)-1) 
				{
					int pb;
					int po;
					IndexRecord pr;
					
					if (i == 0)
					{
						pr = new IndexRecord();
						pr.isNull = true;
					}
					else
					{
						pb = p.getInt(off+9 + (p.getInt(off + 5)-2) * 8);
						po = p.getInt(off+13 + (p.getInt(off + 5)-2) * 8);
						pr = new IndexRecord(file, pb, po, tx, true);
					}
					IndexRecord nr;
					if (pr.isNull())
					{
						nr = pr;
					}
					else
					{
						nr = pr.nextRecord(true);
					}
					IndexRecord curr = new IndexRecord(file, bao.block(), bao.offset(), tx, true);
					if (!pr.isNull())
					{
						pr.setNextNoLog(curr);
						curr.setPrevNoLog(pr);
					}
					else
					{
						curr.setPrevNoLog(new BlockAndOffset(0, 0));
					}
					if (!nr.isNull())
					{
						curr.setNextNoLog(nr);
						nr.setPrevNoLog(curr);
					}
					else
					{
						curr.setNextNoLog(new BlockAndOffset(0, 0));
					}
						
					curr.setUpNoLog(this);
				}
				else
				{
					int nb = p.getInt(off+9 + (i+1) * 8);
					int no = p.getInt(off+13 + (i+1) * 8);
					IndexRecord nr = new IndexRecord(file, nb, no, tx, true);
					IndexRecord pr = nr.prevRecord(true);
					IndexRecord curr = new IndexRecord(file, bao.block(), bao.offset(), tx, true);
					nr.setPrevNoLog(curr);
					curr.setNextNoLog(nr);
					if (!pr.isNull())
					{
						pr.setNextNoLog(curr);
						curr.setPrevNoLog(pr);
					}
					else
					{
						curr.setPrevNoLog(new BlockAndOffset(0, 0));
					}
					
					curr.setUpNoLog(this);
				}
			}
			else
			{
				if (p.getInt(9) != Page.BLOCK_SIZE - 1 && p.block().number() != 0)
				{
					//no room to extend internal record with a new key
					ByteBuffer bb = ByteBuffer.allocate(Page.BLOCK_SIZE);
					i = 0;
					while (i < Page.BLOCK_SIZE)
					{
						bb.put((byte)2);
						i++;
					}
				
					int newNum = FileManager.addNewBlockNoLog(file, bb, tx);
					Block bl = new Block(file, newNum);
					LockManager.xLock(bl, tx.number());
					tx.requestPage(bl);
					Page p2 = null;
					try
					{
						p2 = tx.getPage(bl);
					}
					catch(Exception e)
					{
						HRDBMSWorker.logger.error("", e);
						throw e;
					}
				
					//move current index record to start of new page
					int numBytes = p.getInt(5);
					byte[] bytes = new byte[numBytes];
					p.get(0, bytes);
					long lsn = LogManager.getLSN();
					p2.write(0, bytes, tx.number(), lsn);
					ByteBuffer aft = ByteBuffer.allocate(4);
					aft.position(0);
					aft.putInt(Page.BLOCK_SIZE-1);
					lsn = LogManager.getLSN();
					p2.write(9, aft.array(), tx.number(), lsn);
				
					//redirect pointer from parent
					IndexRecord parent = this.getInternalUp(true);
					IndexRecord newInternal = new IndexRecord(file, bl.number(), 13, tx, true, p2);
					newInternal.setChildUpPointersNoLog();
				
					parent.replaceDownPointerNoLog(this.getBlockAndOffset(), newInternal.getBlockAndOffset());
					newInternal.addDownNoLog(keys, bao);
				}
				else
				{
					splitNoLog();
					seek(13);
					setEqualsPosMulti(keys);
					
					if (line.isLeaf())
					{
						line = line.getUp(true);
					}
					
					line.addDownNoLog(keys, bao);
				}
			}
		}
		
		private BlockAndOffset getBlockAndOffset()
		{
			return new BlockAndOffset(b.number(), off);
		}
		
		private void replaceDownPointer(BlockAndOffset oldPtr, BlockAndOffset newPtr) throws Exception
		{
			int o = off + 9;
			while (true)
			{
				int bNum = p.getInt(o);
				int oNum = p.getInt(o+4);
				
				if (bNum == oldPtr.block() && oNum == oldPtr.offset())
				{
					//replace it
					byte[] before = new byte[8];
					p.get(o, before);
					ByteBuffer after = ByteBuffer.allocate(8);
					after.position(0);
					after.putInt(newPtr.block());
					after.putInt(newPtr.offset());
					InsertLogRec rec = tx.insert(before, after.array(), o, p.block());
					p.write(o, after.array(), tx.number(), rec.lsn());
					return;
				}
				
				o += 8;
			}
		}
		
		private void replaceDownPointerNoLog(BlockAndOffset oldPtr, BlockAndOffset newPtr) throws Exception
		{
			int o = off + 9;
			while (true)
			{
				int bNum = p.getInt(o);
				int oNum = p.getInt(o+4);
				
				if (bNum == oldPtr.block() && oNum == oldPtr.offset())
				{
					//replace it
					ByteBuffer after = ByteBuffer.allocate(8);
					after.position(0);
					after.putInt(newPtr.block());
					after.putInt(newPtr.offset());
					long lsn = LogManager.getLSN();
					p.write(o, after.array(), tx.number(), lsn);
					return;
				}
				
				o += 8;
			}
		}
		
		public void setNext(BlockAndOffset bao) throws Exception
		{
			byte[] before = new byte[8];
			p.get(off+9, before);
			ByteBuffer after = ByteBuffer.allocate(8);
			after.position(0);
			after.putInt(bao.block());
			after.putInt(bao.offset());
			after.position(0);
			InsertLogRec rec = tx.insert(before, after.array(), off+9, p.block());
			p.write(off+9, after.array(), tx.number(), rec.lsn());
		}
		
		public void setNextNoLog(BlockAndOffset bao) throws Exception
		{
			ByteBuffer after = ByteBuffer.allocate(8);
			after.position(0);
			after.putInt(bao.block());
			after.putInt(bao.offset());
			after.position(0);
			long lsn = LogManager.getLSN();
			p.write(off+9, after.array(), tx.number(), lsn);
		}
		
		public void setUp(BlockAndOffset bao) throws Exception
		{
			byte[] before = new byte[8];
			p.get(off+17, before);
			ByteBuffer after = ByteBuffer.allocate(8);
			after.position(0);
			after.putInt(bao.block);
			after.putInt(bao.offset);
			after.position(0);
			InsertLogRec rec = tx.insert(before, after.array(), off+17, p.block());
			p.write(off+17, after.array(), tx.number(), rec.lsn());
		}
		
		public void setUp(IndexRecord ir) throws Exception
		{
			int block = ir.b.number();
			int o = ir.off;
			setUp(new BlockAndOffset(block, o));
		}
		
		public void setUpNoLog(BlockAndOffset bao) throws Exception
		{
			ByteBuffer after = ByteBuffer.allocate(8);
			after.position(0);
			after.putInt(bao.block);
			after.putInt(bao.offset);
			after.position(0);
			long lsn = LogManager.getLSN();
			p.write(off+17, after.array(), tx.number(), lsn);
		}
		
		public void setUpNoLog(IndexRecord ir) throws Exception
		{
			int block = ir.b.number();
			int o = ir.off;
			setUpNoLog(new BlockAndOffset(block, o));
		}
		
		public void setUpInternal(BlockAndOffset bao) throws Exception
		{
			byte[] before = new byte[8];
			p.get(off+9+128*8, before);
			ByteBuffer after = ByteBuffer.allocate(8);
			after.position(0);
			after.putInt(bao.block);
			after.putInt(bao.offset);
			after.position(0);
			InsertLogRec rec = tx.insert(before, after.array(), off+9+128*8, p.block());
			p.write(off+9+128*8, after.array(), tx.number(), rec.lsn());
		}
		
		public void setUpInternal(IndexRecord ir) throws Exception
		{
			int block = ir.b.number();
			int o = ir.off;
			setUpInternal(new BlockAndOffset(block, o));
		}
		
		public void setUpInternalNoLog(BlockAndOffset bao) throws Exception
		{
			ByteBuffer after = ByteBuffer.allocate(8);
			after.position(0);
			after.putInt(bao.block);
			after.putInt(bao.offset);
			after.position(0);
			long lsn = LogManager.getLSN();
			p.write(off+9+128*8, after.array(), tx.number(), lsn);
		}
		
		public void setUpInternalNoLog(IndexRecord ir) throws Exception
		{
			int block = ir.b.number();
			int o = ir.off;
			setUpInternalNoLog(new BlockAndOffset(block, o));
		}
		
		public void setNext(IndexRecord ir) throws Exception
		{
			int block = ir.b.number();
			int o = ir.off;
			setNext(new BlockAndOffset(block, o));
		}
		
		public void setNextNoLog(IndexRecord ir) throws Exception
		{
			int block = ir.b.number();
			int o = ir.off;
			setNextNoLog(new BlockAndOffset(block, o));
		}
		
		public void splitNoLog() throws Exception
		{
			//create a new internal record that has high half of keys and down pointers
			ByteBuffer bb = ByteBuffer.allocate(Page.BLOCK_SIZE);
			int i = 0;
			while (i < Page.BLOCK_SIZE)
			{
				bb.asLongBuffer().put(0x0202020202020202L);
				i += 8;
			}
			
			int newNum = FileManager.addNewBlockNoLog(file, bb, tx);
			Block bl = new Block(file, newNum);
			LockManager.xLock(bl, tx.number());
			tx.requestPage(bl);
			Page p2 = null;
			try
			{
				p2 = tx.getPage(bl);
			}
			catch(Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				throw e;
			}
			
			int numBytes = p.getInt(5);
			byte[] bytes = new byte[numBytes];
			p.get(0, bytes);
			long lsn = LogManager.getLSN();
			p2.write(0, bytes, tx.number(), lsn);
			
			//move down pointers
			int nks = p.getInt(off+5);
			int lowKeys = nks / 2;
			int highKeys = nks - lowKeys;
			byte[] after = new byte[highKeys*8];
			p.get(off+9+lowKeys*8, after);
			byte[] before = new byte[highKeys*8];
			lsn = LogManager.getLSN();
			p2.write(22, after, tx.number(), lsn);
			Arrays.fill(before, (byte)0);
			lsn = LogManager.getLSN();
			p.write(off+9+lowKeys*8, before, tx.number(), lsn);
			
			before = new byte[lowKeys*8];
			Arrays.fill(before, (byte)0);
			lsn = LogManager.getLSN();
			p2.write(off+9+highKeys*8, before, tx.number(), lsn);
			
			i = 0;
			int o = off+9+128*8+8;
			int p1HKS = -1;
			while (i < lowKeys)
			{
				if (i == lowKeys - 1)
				{
					p1HKS = o;
				}
				for (String type : types)
				{
					o++; //null indicator
					
					if (type.equals("INT"))
					{
						o += 4;
					}
					else if (type.equals("LONG"))
					{
						o += 8;
					}
					else if (type.equals("FLOAT"))
					{
						o += 8;
					}
					else if (type.equals("DATE"))
					{
						o += 8;
					}
					else if (type.equals("CHAR"))
					{
						o += p.getInt(o);
						o += 4;
					}
					else
					{
						HRDBMSWorker.logger.error("Unknown type in IndexRecord.split(): " + type);
						throw new Exception("Unknown type in IndexRecord.split(): " + type);
					}
				}
				
				i++;
			}
			
			byte[] p1HighKey = new byte[o - p1HKS];
			p.get(p1HKS, p1HighKey);
			//we are now positioned at the start of the keys that have to get moved from the current page to the new one
			int start = o;
			i = 0;
			int p2HKS = -1;
			while (i < highKeys)
			{
				if (i == highKeys - 1)
				{
					p2HKS = o;
				}
				for (String type : types)
				{
					o++; //null indicator
					
					if (type.equals("INT"))
					{
						o += 4;
					}
					else if (type.equals("LONG"))
					{
						o += 8;
					}
					else if (type.equals("FLOAT"))
					{
						o += 8;
					}
					else if (type.equals("DATE"))
					{
						o += 8;
					}
					else if (type.equals("CHAR"))
					{
						o += p.getInt(o);
						o += 4;
					}
					else
					{
						HRDBMSWorker.logger.error("Unknown type in IndexRecord.split(): " + type);
						throw new Exception("Unknown type in IndexRecord.split(): " + type);
					}
				}
				
				i++;
			}
			
			byte[] p2HighKey = new byte[o - p2HKS];
			p.get(p2HKS, p2HighKey);
			int end = o;
			int length = end - start;
			byte[] oldPageKeys = new byte[length];
			p.get(start, oldPageKeys);
			byte[] blank = new byte[length];
			Arrays.fill(blank, (byte)0);
			lsn = LogManager.getLSN();
			p.write(start, blank, tx.number(), lsn);
			lsn = LogManager.getLSN();
			p2.write(30+128*8, oldPageKeys, tx.number(), lsn); //blank high keys in p2?
			
			int firstFreeByte = p.getInt(5);
			firstFreeByte -= length;
			ByteBuffer a1 = ByteBuffer.allocate(4);
			a1.position(0);
			a1.putInt(firstFreeByte);
			lsn = LogManager.getLSN();
			p.write(5, a1.array(), tx.number(), lsn);
			
			firstFreeByte = p.getInt(14);
			firstFreeByte -= length;
			a1 = ByteBuffer.allocate(8);
			a1.position(0);
			a1.putInt(firstFreeByte);
			a1.putInt(lowKeys);
			lsn = LogManager.getLSN();
			p.write(14, a1.array(), tx.number(), lsn);
			
			a1 = ByteBuffer.allocate(17);
			a1.position(0);
			a1.putInt(30+128*8+length);
			a1.putInt(Page.BLOCK_SIZE-1);
			a1.put((byte)0);
			a1.putInt(17+128*8+length);
			a1.putInt(highKeys);
			lsn = LogManager.getLSN();
			p2.write(5, a1.array(), tx.number(), lsn);
			
			IndexRecord newRec = new IndexRecord(file, p2.block().number(), 13, tx, true, p2);
			newRec.setChildUpPointersNoLog();
			
			if (b.number() == 0)
			{
				//we are splitting the root
				//move the root to a new page
				bb = ByteBuffer.allocate(Page.BLOCK_SIZE);
				i = 0;
				while (i < Page.BLOCK_SIZE)
				{
					bb.put((byte)2);
					i++;
				}
				
				newNum = FileManager.addNewBlockNoLog(file, bb, tx);
				Block bl2 = new Block(file, newNum);
				LockManager.xLock(bl2, tx.number());
				tx.requestPage(bl2);
				Page p3 = null;
				try
				{
					p3 = tx.getPage(bl2);
				}
				catch(Exception e)
				{
					HRDBMSWorker.logger.error("", e);
					throw e;
				}
				
				firstFreeByte = p.getInt(5);
				byte[] a1a = new byte[firstFreeByte];
				p.get(0, a1a);
				lsn = LogManager.getLSN();
				p3.write(0, a1a, tx.number(), lsn);
				a1 = ByteBuffer.allocate(4);
				a1.position(0);
				a1.putInt(Page.BLOCK_SIZE-1);
				lsn = LogManager.getLSN();
				p3.write(9, a1.array(), tx.number(), lsn);
				
				newRec = new IndexRecord(file, p3.block().number(), 13, tx, true, p3);
				newRec.setChildUpPointersNoLog();
				
				//write new root 
				a1 = ByteBuffer.allocate(25+128*8 + p1HighKey.length + p2HighKey.length);
				a1.position(0);
				a1.putInt(30+128*8 + p1HighKey.length + p2HighKey.length);
				a1.putInt(p.getInt(9));
				a1.put((byte)0);
				a1.putInt(17+128*8 + p1HighKey.length + p2HighKey.length);
				a1.putInt(2);
				a1.putInt(bl2.number());
				a1.putInt(13);
				a1.putInt(bl.number());
				a1.putInt(13);
				i = 2;
				while (i < 129)
				{
					a1.putLong(0);
					i++;
				}
				a1.put(p1HighKey);
				a1.put(p2HighKey);
				lsn = LogManager.getLSN();
				p.write(5, a1.array(), tx.number(), lsn);
				
				newRec = new IndexRecord(file, 0, 13, tx, true, p);
				newRec.setChildUpPointersNoLog();
			}
			else
			{
				IndexRecord up = getInternalUp(true);
				p2HighKey = up.getDownKey(this.getBlockAndOffset());
				up.replaceDownKeyNoLog(this.getBlockAndOffset(), p1HighKey);
				up = getInternalUp(true);
				up.addInternalDownNoLog(new BlockAndOffset(bl.number(), 13), p2HighKey);
			}
		}
		
		public void split() throws Exception
		{
			//create a new internal record that has high half of keys and down pointers
			ByteBuffer bb = ByteBuffer.allocate(Page.BLOCK_SIZE);
			int i = 0;
			while (i < Page.BLOCK_SIZE)
			{
				bb.put((byte)2);
				i++;
			}
			
			int newNum = FileManager.addNewBlock(file, bb, tx);
			Block bl = new Block(file, newNum);
			LockManager.xLock(bl, tx.number());
			tx.requestPage(bl);
			Page p2 = null;
			try
			{
				p2 = tx.getPage(bl);
			}
			catch(Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				throw e;
			}
			
			int numBytes = p.getInt(5);
			byte[] bytes = new byte[numBytes];
			p.get(0, bytes);
			byte[] bef = new byte[numBytes];
			p2.get(0, bef);
			InsertLogRec rec = tx.insert(bef, bytes, 0, p2.block());
			p2.write(0, bytes, tx.number(), rec.lsn());
			
			//move down pointers
			int nks = p.getInt(off+5);
			int lowKeys = nks / 2;
			int highKeys = nks - lowKeys;
			byte[] after = new byte[highKeys*8];
			p.get(off+9+lowKeys*8, after);
			byte[] before = new byte[highKeys*8];
			p2.get(22, before);
			rec = tx.insert(before, after, 22, p2.block());
			p2.write(22, after, tx.number(), rec.lsn());
			Arrays.fill(before, (byte)0);
			rec = tx.insert(after, before, off+9+lowKeys*8, p.block());
			p.write(off+9+lowKeys*8, before, tx.number(), rec.lsn());
			
			after = new byte[lowKeys*8];
			before = new byte[lowKeys*8];
			Arrays.fill(before, (byte)0);
			p2.get(off+9+highKeys*8, after);
			rec = tx.insert(after, before, off+9+highKeys*8, p2.block());
			p2.write(off+9+highKeys*8, before, tx.number(), rec.lsn());
			
			i = 0;
			int o = off+9+128*8+8;
			int p1HKS = -1;
			while (i < lowKeys)
			{
				if (i == lowKeys - 1)
				{
					p1HKS = o;
				}
				for (String type : types)
				{
					o++; //null indicator
					
					if (type.equals("INT"))
					{
						o += 4;
					}
					else if (type.equals("LONG"))
					{
						o += 8;
					}
					else if (type.equals("FLOAT"))
					{
						o += 8;
					}
					else if (type.equals("DATE"))
					{
						o += 8;
					}
					else if (type.equals("CHAR"))
					{
						o += p.getInt(o);
						o += 4;
					}
					else
					{
						HRDBMSWorker.logger.error("Unknown type in IndexRecord.split(): " + type);
						throw new Exception("Unknown type in IndexRecord.split(): " + type);
					}
				}
				
				i++;
			}
			
			byte[] p1HighKey = new byte[o - p1HKS];
			p.get(p1HKS, p1HighKey);
			//we are now positioned at the start of the keys that have to get moved from the current page to the new one
			int start = o;
			i = 0;
			int p2HKS = -1;
			while (i < highKeys)
			{
				if (i == highKeys - 1)
				{
					p2HKS = o;
				}
				for (String type : types)
				{
					o++; //null indicator
					
					if (type.equals("INT"))
					{
						o += 4;
					}
					else if (type.equals("LONG"))
					{
						o += 8;
					}
					else if (type.equals("FLOAT"))
					{
						o += 8;
					}
					else if (type.equals("DATE"))
					{
						o += 8;
					}
					else if (type.equals("CHAR"))
					{
						o += p.getInt(o);
						o += 4;
					}
					else
					{
						HRDBMSWorker.logger.error("Unknown type in IndexRecord.split(): " + type);
						throw new Exception("Unknown type in IndexRecord.split(): " + type);
					}
				}
				
				i++;
			}
			
			byte[] p2HighKey = new byte[o - p2HKS];
			p.get(p2HKS, p2HighKey);
			int end = o;
			int length = end - start;
			byte[] oldPageKeys = new byte[length];
			p.get(start, oldPageKeys);
			byte[] blank = new byte[length];
			Arrays.fill(blank, (byte)0);
			rec = tx.insert(oldPageKeys, blank, start, p.block());
			p.write(start, blank, tx.number(), rec.lsn());
			byte[] newPage = new byte[length];
			p2.get(30+128*8, newPage);
			rec = tx.insert(newPage, oldPageKeys, 30+128*8, p2.block());
			p2.write(30+128*8, oldPageKeys, tx.number(), rec.lsn());
			
			int firstFreeByte = p.getInt(5);
			ByteBuffer b1 = ByteBuffer.allocate(4);
			b1.position(0);
			b1.putInt(firstFreeByte);
			firstFreeByte -= length;
			ByteBuffer a1 = ByteBuffer.allocate(4);
			a1.position(0);
			a1.putInt(firstFreeByte);
			rec = tx.insert(b1.array(), a1.array(), 5, p.block());
			p.write(5, a1.array(), tx.number(), rec.lsn());
			
			firstFreeByte = p.getInt(14);
			b1 = ByteBuffer.allocate(8);
			b1.position(0);
			b1.putInt(firstFreeByte);
			b1.putInt(p.getInt(18));
			firstFreeByte -= length;
			a1 = ByteBuffer.allocate(8);
			a1.position(0);
			a1.putInt(firstFreeByte);
			a1.putInt(lowKeys);
			rec = tx.insert(b1.array(), a1.array(), 14, p.block());
			p.write(14, a1.array(), tx.number(), rec.lsn());
			
			byte[] b1a = new byte[17];
			p2.get(5, b1a);
			a1 = ByteBuffer.allocate(17);
			a1.position(0);
			a1.putInt(30+128*8+length);
			a1.putInt(Page.BLOCK_SIZE-1);
			a1.put((byte)0);
			a1.putInt(17+128*8+length);
			a1.putInt(highKeys);
			rec = tx.insert(b1a, a1.array(), 5, p2.block());
			p2.write(5, a1.array(), tx.number(), rec.lsn());
			
			IndexRecord newRec = new IndexRecord(file, p2.block().number(), 13, tx, true, p2); 
			newRec.setChildUpPointers();
			
			if (b.number() == 0)
			{
				//we are splitting the root
				//move the root to a new page
				bb = ByteBuffer.allocate(Page.BLOCK_SIZE);
				i = 0;
				while (i < Page.BLOCK_SIZE)
				{
					bb.put((byte)2);
					i++;
				}
				
				newNum = FileManager.addNewBlock(file, bb, tx);
				Block bl2 = new Block(file, newNum);
				LockManager.xLock(bl2, tx.number());
				tx.requestPage(bl2);
				Page p3 = null;
				try
				{
					p3 = tx.getPage(bl2);
				}
				catch(Exception e)
				{
					HRDBMSWorker.logger.error("", e);
					throw e;
				}
				
				firstFreeByte = p.getInt(5);
				byte[] a1a = new byte[firstFreeByte];
				p.get(0, a1a);
				b1a = new byte[firstFreeByte];
				p3.get(0, b1a);
				rec = tx.insert(b1a, a1a, 0, p3.block());
				p3.write(0, a1a, tx.number(), rec.lsn());
				b1a = new byte[4];
				p3.get(9, b1a);
				a1 = ByteBuffer.allocate(4);
				a1.position(0);
				a1.putInt(Page.BLOCK_SIZE-1);
				rec = tx.insert(b1a, a1.array(), 9, p3.block());
				p3.write(9, a1.array(), tx.number(), rec.lsn());
				
				newRec = new IndexRecord(file, p3.block().number(), 13, tx, true, p3);
				newRec.setChildUpPointers();
				
				//write new root 
				b1a = new byte[25+128*8 + p1HighKey.length + p2HighKey.length]; 
				p.get(5, b1a);
				a1 = ByteBuffer.allocate(25+128*8 + p1HighKey.length + p2HighKey.length);
				a1.position(0);
				a1.putInt(30+128*8 + p1HighKey.length + p2HighKey.length);
				a1.putInt(p.getInt(9));
				a1.put((byte)0);
				a1.putInt(17+128*8 + p1HighKey.length + p2HighKey.length);
				a1.putInt(2);
				a1.putInt(bl2.number());
				a1.putInt(13);
				a1.putInt(bl.number());
				a1.putInt(13);
				i = 2;
				while (i < 129)
				{
					a1.putLong(0);
					i++;
				}
				a1.put(p1HighKey);
				a1.put(p2HighKey);
				rec = tx.insert(b1a, a1.array(), 5, p.block());
				p.write(5, a1.array(), tx.number(), rec.lsn());
				
				newRec = new IndexRecord(file, 0, 13, tx, true, p);
				newRec.setChildUpPointers();
			}
			else
			{
				IndexRecord up = getInternalUp(true);
				p2HighKey = up.getDownKey(this.getBlockAndOffset());
				up.replaceDownKey(this.getBlockAndOffset(), p1HighKey);
				up = getInternalUp(true);
				up.addInternalDown(new BlockAndOffset(bl.number(), 13), p2HighKey);
			}
		}
		
		private void setChildUpPointers() throws Exception
		{
			int o = 22;
			int i = 0;
			int nks = p.getInt(18);
			while (i < nks)
			{
				int block = p.getInt(o);
				o += 4;
				int offset = p.getInt(o);
				o += 4;
				IndexRecord rec = new IndexRecord(file, block, offset, tx, true);
				if (rec.isLeaf())
				{
					rec.setUp(this);
				}
				else
				{
					rec.setUpInternal(this);
				}
				i++;
			}
		}
		
		private void setChildUpPointersNoLog() throws Exception
		{
			int o = 22;
			int i = 0;
			int nks = p.getInt(18);
			while (i < nks)
			{
				int block = p.getInt(o);
				o += 4;
				int offset = p.getInt(o);
				o += 4;
				IndexRecord rec = new IndexRecord(file, block, offset, tx, true);
				if (rec.isLeaf())
				{
					rec.setUpNoLog(this);
				}
				else
				{
					rec.setUpInternalNoLog(this);
				}
				i++;
			}
		}
		
		private void addInternalDown(BlockAndOffset bao, byte[] keyBytes) throws Exception
		{
			if (this.containsRoom())
			{
				int available = p.getInt(9) - p.getInt(5) + 1;
				if (keyBytes.length <= available)
				{
					//update record in place
					RowComparator rc = new RowComparator(orders, types);
					ArrayList<Object> newKey = new ArrayList<Object>();
					ByteBuffer bb = ByteBuffer.wrap(keyBytes);
					int o = 0;
					int j = 0;
					for (String type : types)
					{
						o++;
						if (type.equals("INT"))
						{
							newKey.add(bb.getInt(o));
							o += 4;
						}
						else if (type.equals("LONG"))
						{
							newKey.add(bb.getLong(o));
							o+= 8;
						}
						else if (type.equals("FLOAT"))
						{
							newKey.add(bb.getDouble(o));
							o += 8;
						}
						else if (type.equals("DATE"))
						{
							newKey.add(new MyDate(bb.getLong(o)));
							o += 8;
						}
						else if (type.equals("CHAR"))
						{
							int length = bb.getInt(o);
							o += 4;
							byte[] bytes = new byte[length];
							//bb.get(o, bytes);
							bb.get(bytes, o, bytes.length);
							o += length;
							try
							{
								newKey.add(new String(bytes, "UTF-8"));
							}
							catch(Exception e)
							{
								HRDBMSWorker.logger.error("", e);
								throw e;
							}
						}
						else
						{
							HRDBMSWorker.logger.error("Unknown type in IndexRecord.addInternalDown(): " + type);
							throw new Exception("Unknown type in IndexRecord.addInternalDown(): " + type);
						}
						
						j++;
					}
					int i = 0;
					int numKeys = p.getInt(off+5);
					o = off+9+129*8;
					int goesHere = -1;
					while (i < numKeys)
					{
						ArrayList<Object> row = new ArrayList<Object>();
						goesHere = o;
						j = 0;
						for (String type : types)
						{
							o++;
							if (type.equals("INT"))
							{
								row.add(p.getInt(o));
								o += 4;
							}
							else if (type.equals("LONG"))
							{
								row.add(p.getLong(o));
								o+= 8;
							}
							else if (type.equals("FLOAT"))
							{
								row.add(p.getDouble(o));
								o += 8;
							}
							else if (type.equals("DATE"))
							{
								row.add(new MyDate(p.getLong(o)));
								o += 8;
							}
							else if (type.equals("CHAR"))
							{
								int length = p.getInt(o);
								o += 4;
								byte[] bytes = new byte[length];
								p.get(o, bytes);
								o += length;
								try
								{
									row.add(new String(bytes, "UTF-8"));
								}
								catch(Exception e)
								{
									HRDBMSWorker.logger.error("", e);
									throw e;
								}
							}
							else
							{
								HRDBMSWorker.logger.error("Unknown type in IndexRecord.addInternalDown(): " + type);
								throw new Exception("Unknown type in IndexRecord.addInternalDown(): " + type);
							}
							
							j++;
						}
						
						if (rc.compare(newKey, row) < 0)
						{
							//the new key goes in position i
							break;
						}
						
						i++;
					}
					
					if (i == numKeys)
					{
						goesHere = o;
					}
					
					//key goes in position i @ offset goesHere
					int firstFree = p.getInt(5);
					ByteBuffer bef = ByteBuffer.allocate(4);
					bef.position(0);
					bef.putInt(firstFree);
					ByteBuffer aft = ByteBuffer.allocate(4);
					aft.position(0);
					aft.putInt(firstFree+keyBytes.length);
					InsertLogRec rec = tx.insert(bef.array(), aft.array(), 5, p.block());
					p.write(5, aft.array(), tx.number(), rec.lsn());
					
					byte[] ba = new byte[8];
					p.get(14, ba);
					aft = ByteBuffer.allocate(8);
					aft.position(0);
					aft.putInt(firstFree+keyBytes.length-13);
					aft.putInt(numKeys+1);
					rec = tx.insert(ba, aft.array(), 14, p.block());
					p.write(14, aft.array(), tx.number(), rec.lsn());
					
					int toMove = numKeys * 8 - i * 8;
					if (toMove > 0)
					{
						ba = new byte[toMove];
						p.get(off+9+(i+1)*8, ba);
						byte[] aa = new byte[toMove];
						p.get(off+9+i*8, aa);
						rec = tx.insert(ba, aa, off+9+(i+1)*8, p.block());
						p.write(off+9+(i+1)*8, aa, tx.number(), rec.lsn());
					}
						
					ba = new byte[8];
					p.get(off+9+i*8, ba);
					aft = ByteBuffer.allocate(8);
					aft.position(0);
					aft.putInt(bao.block());
					aft.putInt(bao.offset());
					rec = tx.insert(ba, aft.array(), off+9+i*8, p.block());
					p.write(off+9+i*8, aft.array(), tx.number(), rec.lsn());
					
					if (i == numKeys)
					{
						//nothing to move
						byte[] before = new byte[keyBytes.length];
						p.get(goesHere, before);
						rec = tx.insert(before, keyBytes, goesHere, p.block());
						p.write(goesHere, keyBytes, tx.number(), rec.lsn());
					}
					else
					{
						j = i;
						o = goesHere;
						while (j < numKeys)
						{
							for (String type : types)
							{
								o++;
								if (type.equals("INT"))
								{
									o += 4;
								}
								else if (type.equals("LONG"))
								{
									o += 8;
								}
								else if (type.equals("FLOAT"))
								{
									o += 8;
								}
								else if (type.equals("DATE"))
								{
									o += 8;
								}
								else if (type.equals("CHAR"))
								{
									int length = p.getInt(o);
									o += (4 + length);
								}
								else
								{
									HRDBMSWorker.logger.error("Unknown type in IndexRecord.addInternalDown(): " + type);
									throw new Exception("Unknown type in IndexRecord.addInternalDown(): " + type);
								}			
							}
							
							j++;
						}
						
						//move data from goesHere <-> o by keyBytes.length bytes
						toMove = o - goesHere;
						byte[] before = new byte[toMove];
						p.get(goesHere + keyBytes.length, before);
						byte[] after = new byte[toMove];
						p.get(goesHere, after);
						rec = tx.insert(before, after, goesHere+keyBytes.length, p.block());
						p.write(goesHere+keyBytes.length, after, tx.number(), rec.lsn());
						
						before = new byte[keyBytes.length];
						p.get(goesHere, before);
						rec = tx.insert(before, keyBytes, goesHere, p.block());
						p.write(goesHere, keyBytes, tx.number(), rec.lsn());
					}
					
					IndexRecord down = new IndexRecord(file, bao.block(), bao.offset(), tx, true);
					if (down.isLeaf())
					{
						down.setUp(this);
					}
					else
					{
						down.setUpInternal(this);
					}
				}
				else
				{
					//check if this is the root
					if (b.number() == 0)
					{
						//split and add and fix up and possibly reset down key in root
						split();
						int lstart = 22 + 129 * 8;
						int o = lstart;
						for (String type : types)
						{
							o++;
							if (type.equals("INT"))
							{
								o += 4;
							}
							else if (type.equals("LONG"))
							{
								o += 8;
							}
							else if (type.equals("FLOAT"))
							{
								o += 8;
							}
							else if (type.equals("DATE"))
							{
								o += 8;
							}
							else if (type.equals("CHAR"))
							{
								int length = p.getInt(o);
								o += (4 + length);
							}
							else
							{
								HRDBMSWorker.logger.error("Unknown type in IndexRecord.addInternalDown(): " + type);
								throw new Exception("Unknown type in IndexRecord.addInternalDown(): " + type);
							}
						}
						
						int rstart = o;
						for (String type : types)
						{
							o++;
							if (type.equals("INT"))
							{
								o += 4;
							}
							else if (type.equals("LONG"))
							{
								o += 8;
							}
							else if (type.equals("FLOAT"))
							{
								o += 8;
							}
							else if (type.equals("DATE"))
							{
								o += 8;
							}
							else if (type.equals("CHAR"))
							{
								int length = p.getInt(o);
								o += (4 + length);
							}
							else
							{
								HRDBMSWorker.logger.error("Unknown type in IndexRecord.addInternalDown(): " + type);
								throw new Exception("Unknown type in IndexRecord.addInternalDown(): " + type);
							}
						}
						
						byte[] lKey = new byte[rstart-lstart];
						p.get(lstart, lKey);
						byte[] rKey = new byte[o-rstart];
						p.get(rstart, rKey);
						ArrayList<Object> leftRow = this.convertBytesToRow(lKey);
						ArrayList<Object> row = convertBytesToRow(keyBytes);
						RowComparator rc = new RowComparator(orders, types);
						
						if (rc.compare(leftRow, row) <= 0)
						{
							//add and fix up
							IndexRecord child = this.getDown(0);
							child.addInternalDown(bao, keyBytes);
						}
						else
						{
							ArrayList<Object> rightRow = convertBytesToRow(rKey);
							
							if (rc.compare(row, rightRow) <= 0)
							{
								//add and fix up
								IndexRecord child = this.getDown(1);
								child.addInternalDown(bao, keyBytes);
							}
							else
							{
								//add and fix up
								IndexRecord child = this.getDown(1);
								child.addInternalDown(bao, keyBytes);
								child.updateParentDownKey(keyBytes);
							}
						}
						
						return;
					}
					
					//otherwise move record and then update in place
					if (p.getInt(9) != Page.BLOCK_SIZE - 1)
					{
						ByteBuffer bb = ByteBuffer.allocate(Page.BLOCK_SIZE);
						int i = 0;
						while (i < Page.BLOCK_SIZE)
						{
							bb.put((byte)2);
							i++;
						}
					
						int newNum = FileManager.addNewBlock(file, bb, tx);
						Block bl = new Block(file, newNum);
						LockManager.xLock(bl, tx.number());
						tx.requestPage(bl);
						Page p2 = null;
						try
						{
							p2 = tx.getPage(bl);
						}
						catch(Exception e)
						{
							HRDBMSWorker.logger.error("", e);
							throw e;
						}
					
						int length = p.getInt(5);
						byte[] before = new byte[length];
						p2.get(0, before);
						byte[] after = new byte[length];
						p.get(0, after);
						InsertLogRec rec = tx.insert(before, after, 0, p2.block());
						p2.write(0, after, tx.number(), rec.lsn());
					
						before = new byte[4];
						p2.get(9, before);
						ByteBuffer aft = ByteBuffer.allocate(4);
						aft.position(0);
						aft.putInt(Page.BLOCK_SIZE-1);
						rec = tx.insert(before, aft.array(), 9, p2.block());
						p2.write(9, aft.array(), tx.number(), rec.lsn());
					
						IndexRecord parent = getInternalUp(true);
						parent.replaceDownPointer(this.getBlockAndOffset(), new BlockAndOffset(bl.number(), 13));
						IndexRecord moved = new IndexRecord(file, bl.number(), 13, tx, true, p2);
						moved.setChildUpPointers();
						moved.addInternalDown(bao, keyBytes);
					}
					else
					{
						split();
						byte[] myHigh = getHighKey();
						ArrayList<Object> myHighRow = convertBytesToRow(myHigh);
						ArrayList<Object> row = convertBytesToRow(keyBytes);
						
						RowComparator rc = new RowComparator(orders, types);
						if (rc.compare(row, myHighRow) <= 0)
						{
							addInternalDown(bao, keyBytes);
						}
						else
						{
							IndexRecord rec = getNextSibling();
							rec.addInternalDown(bao, keyBytes);
						}
					}
				}
			}
			else
			{
				//if this is the root
				if (b.number() == 0)
				{
					//split and add and fix up and possibly reset down key in root
					split();
					int lstart = 22 + 129 * 8;
					int o = lstart;
					for (String type : types)
					{
						o++;
						if (type.equals("INT"))
						{
							o += 4;
						}
						else if (type.equals("LONG"))
						{
							o += 8;
						}
						else if (type.equals("FLOAT"))
						{
							o += 8;
						}
						else if (type.equals("DATE"))
						{
							o += 8;
						}
						else if (type.equals("CHAR"))
						{
							int length = p.getInt(o);
							o += (4 + length);
						}
						else
						{
							HRDBMSWorker.logger.error("Unknown type in IndexRecord.addInternalDown(): " + type);
							throw new Exception("Unknown type in IndexRecord.addInternalDown(): " + type);
						}
					}
					
					int rstart = o;
					for (String type : types)
					{
						o++;
						if (type.equals("INT"))
						{
							o += 4;
						}
						else if (type.equals("LONG"))
						{
							o += 8;
						}
						else if (type.equals("FLOAT"))
						{
							o += 8;
						}
						else if (type.equals("DATE"))
						{
							o += 8;
						}
						else if (type.equals("CHAR"))
						{
							int length = p.getInt(o);
							o += (4 + length);
						}
						else
						{
							HRDBMSWorker.logger.error("Unknown type in IndexRecord.addInternalDown(): " + type);
							throw new Exception("Unknown type in IndexRecord.addInternalDown(): " + type);
						}
					}
					
					byte[] lKey = new byte[rstart-lstart];
					p.get(lstart, lKey);
					byte[] rKey = new byte[o-rstart];
					p.get(rstart, rKey);
					ArrayList<Object> leftRow = this.convertBytesToRow(lKey);
					ArrayList<Object> row = convertBytesToRow(keyBytes);
					RowComparator rc = new RowComparator(orders, types);
					
					if (rc.compare(row, leftRow) <= 0)
					{
						//add and fix up
						IndexRecord child = this.getDown(0);
						child.addInternalDown(bao, keyBytes);
					}
					else
					{
						ArrayList<Object> rightRow = convertBytesToRow(rKey);
						
						if (rc.compare(row, rightRow) <= 0)
						{
							//add and fix up
							IndexRecord child = this.getDown(1);
							child.addInternalDown(bao, keyBytes);
						}
						else
						{
							//add and fix up
							IndexRecord child = this.getDown(1);
							child.addInternalDown(bao, keyBytes);
							child.updateParentDownKey(keyBytes);
						}
					}
				}
				else
				{
					split();
					byte[] myHigh = getHighKey();
					ArrayList<Object> myHighRow = convertBytesToRow(myHigh);
					
					ArrayList<Object> row = convertBytesToRow(keyBytes);
					
					RowComparator rc = new RowComparator(orders, types);
					if (rc.compare(row, myHighRow) <= 0)
					{
						addInternalDown(bao, keyBytes);
					}
					else
					{
						IndexRecord rec = getNextSibling();
						rec.addInternalDown(bao, keyBytes);
					}
				}
			}
		}
		
		private void addInternalDownNoLog(BlockAndOffset bao, byte[] keyBytes) throws Exception
		{
			if (this.containsRoom())
			{
				int available = p.getInt(9) - p.getInt(5) + 1;
				if (keyBytes.length <= available)
				{
					//update record in place
					RowComparator rc = new RowComparator(orders, types);
					ArrayList<Object> newKey = new ArrayList<Object>();
					ByteBuffer bb = ByteBuffer.wrap(keyBytes);
					int o = 0;
					int j = 0;
					for (String type : types)
					{
						o++;
						if (type.equals("INT"))
						{
							newKey.add(bb.getInt(o));
							o += 4;
						}
						else if (type.equals("LONG"))
						{
							newKey.add(bb.getLong(o));
							o+= 8;
						}
						else if (type.equals("FLOAT"))
						{
							newKey.add(bb.getDouble(o));
							o += 8;
						}
						else if (type.equals("DATE"))
						{
							newKey.add(new MyDate(bb.getLong(o)));
							o += 8;
						}
						else if (type.equals("CHAR"))
						{
							int length = bb.getInt(o);
							o += 4;
							byte[] bytes = new byte[length];
							//bb.get(o, bytes);
							bb.get(bytes, o, bytes.length);
							o += length;
							try
							{
								newKey.add(new String(bytes, "UTF-8"));
							}
							catch(Exception e)
							{
								HRDBMSWorker.logger.error("", e);
								throw e;
							}
						}
						else
						{
							HRDBMSWorker.logger.error("Unknown type in IndexRecord.addInternalDown(): " + type);
							throw new Exception("Unknown type in IndexRecord.addInternalDown(): " + type);
						}
						
						j++;
					}
					int i = 0;
					int numKeys = p.getInt(off+5);
					o = off+9+129*8;
					int goesHere = -1;
					while (i < numKeys)
					{
						ArrayList<Object> row = new ArrayList<Object>();
						goesHere = o;
						j = 0;
						for (String type : types)
						{
							o++;
							if (type.equals("INT"))
							{
								row.add(p.getInt(o));
								o += 4;
							}
							else if (type.equals("LONG"))
							{
								row.add(p.getLong(o));
								o+= 8;
							}
							else if (type.equals("FLOAT"))
							{
								row.add(p.getDouble(o));
								o += 8;
							}
							else if (type.equals("DATE"))
							{
								row.add(new MyDate(p.getLong(o)));
								o += 8;
							}
							else if (type.equals("CHAR"))
							{
								int length = p.getInt(o);
								o += 4;
								byte[] bytes = new byte[length];
								p.get(o, bytes);
								o += length;
								try
								{
									row.add(new String(bytes, "UTF-8"));
								}
								catch(Exception e)
								{
									HRDBMSWorker.logger.error("", e);
									throw e;
								}
							}
							else
							{
								HRDBMSWorker.logger.error("Unknown type in IndexRecord.addInternalDown(): " + type);
								throw new Exception("Unknown type in IndexRecord.addInternalDown(): " + type);
							}
							
							j++;
						}
						
						if (rc.compare(newKey, row) < 0)
						{
							//the new key goes in position i
							break;
						}
						
						i++;
					}
					
					if (i == numKeys)
					{
						goesHere = o;
					}
					
					//key goes in position i @ offset goesHere
					int firstFree = p.getInt(5);
					ByteBuffer aft = ByteBuffer.allocate(4);
					aft.position(0);
					aft.putInt(firstFree+keyBytes.length);
					long lsn = LogManager.getLSN();
					p.write(5, aft.array(), tx.number(), lsn);
					
					aft = ByteBuffer.allocate(8);
					aft.position(0);
					aft.putInt(firstFree+keyBytes.length-13);
					aft.putInt(p.getInt(18)+1);
					lsn = LogManager.getLSN();
					p.write(14, aft.array(), tx.number(), lsn);
					
					int toMove = numKeys * 8 - i * 8;
					if (toMove > 0)
					{
						byte[] aa = new byte[toMove];
						p.get(off+9+i*8, aa);
						lsn = LogManager.getLSN();
						p.write(off+9+(i+1)*8, aa, tx.number(), lsn);
					}
						
					aft = ByteBuffer.allocate(8);
					aft.position(0);
					aft.putInt(bao.block());
					aft.putInt(bao.offset());
					lsn = LogManager.getLSN();
					p.write(off+9+i*8, aft.array(), tx.number(), lsn);
					
					if (i == numKeys)
					{
						//nothing to move
						lsn = LogManager.getLSN();
						p.write(goesHere, keyBytes, tx.number(), lsn);
					}
					else
					{
						j = i;
						o = goesHere;
						while (j < numKeys)
						{
							for (String type : types)
							{
								o++;
								if (type.equals("INT"))
								{
									o += 4;
								}
								else if (type.equals("LONG"))
								{
									o += 8;
								}
								else if (type.equals("FLOAT"))
								{
									o += 8;
								}
								else if (type.equals("DATE"))
								{
									o += 8;
								}
								else if (type.equals("CHAR"))
								{
									int length = p.getInt(o);
									o += (4 + length);
								}
								else
								{
									HRDBMSWorker.logger.error("Unknown type in IndexRecord.addInternalDown(): " + type);
									throw new Exception("Unknown type in IndexRecord.addInternalDown(): " + type);
								}			
							}
							
							j++;
						}
						
						//move data from goesHere <-> o by keyBytes.length bytes
						toMove = o - goesHere;
						byte[] after = new byte[toMove];
						p.get(goesHere, after);
						lsn = LogManager.getLSN();
						p.write(goesHere+keyBytes.length, after, tx.number(), lsn);
						
						lsn = LogManager.getLSN();
						p.write(goesHere, keyBytes, tx.number(), lsn);
					}
					
					IndexRecord down = new IndexRecord(file, bao.block(), bao.offset(), tx, true);
					if (down.isLeaf())
					{
						down.setUpNoLog(this);
					}
					else
					{
						down.setUpInternalNoLog(this);
					}
				}
				else
				{
					//check if this is the root
					if (b.number() == 0)
					{
						//split and add and fix up and possibly reset down key in root
						splitNoLog();
						int lstart = 22 + 129 * 8;
						int o = lstart;
						for (String type : types)
						{
							o++;
							if (type.equals("INT"))
							{
								o += 4;
							}
							else if (type.equals("LONG"))
							{
								o += 8;
							}
							else if (type.equals("FLOAT"))
							{
								o += 8;
							}
							else if (type.equals("DATE"))
							{
								o += 8;
							}
							else if (type.equals("CHAR"))
							{
								int length = p.getInt(o);
								o += (4 + length);
							}
							else
							{
								HRDBMSWorker.logger.error("Unknown type in IndexRecord.addInternalDown(): " + type);
								throw new Exception("Unknown type in IndexRecord.addInternalDown(): " + type);
							}
						}
						
						int rstart = o;
						for (String type : types)
						{
							o++;
							if (type.equals("INT"))
							{
								o += 4;
							}
							else if (type.equals("LONG"))
							{
								o += 8;
							}
							else if (type.equals("FLOAT"))
							{
								o += 8;
							}
							else if (type.equals("DATE"))
							{
								o += 8;
							}
							else if (type.equals("CHAR"))
							{
								int length = p.getInt(o);
								o += (4 + length);
							}
							else
							{
								HRDBMSWorker.logger.error("Unknown type in IndexRecord.addInternalDown(): " + type);
								throw new Exception("Unknown type in IndexRecord.addInternalDown(): " + type);
							}
						}
						
						byte[] lKey = new byte[rstart-lstart];
						p.get(lstart, lKey);
						byte[] rKey = new byte[o-rstart];
						p.get(rstart, rKey);
						ArrayList<Object> leftRow = this.convertBytesToRow(lKey);
						ArrayList<Object> row = convertBytesToRow(keyBytes);
						RowComparator rc = new RowComparator(orders, types);
						
						if (rc.compare(leftRow, row) <= 0)
						{
							//add and fix up
							IndexRecord child = this.getDown(0);
							child.addInternalDownNoLog(bao, keyBytes);
						}
						else
						{
							ArrayList<Object> rightRow = convertBytesToRow(rKey);
							
							if (rc.compare(row, rightRow) <= 0)
							{
								//add and fix up
								IndexRecord child = this.getDown(1);
								child.addInternalDownNoLog(bao, keyBytes);
							}
							else
							{
								//add and fix up
								IndexRecord child = this.getDown(1);
								child.addInternalDownNoLog(bao, keyBytes);
								child.updateParentDownKeyNoLog(keyBytes);
							}
						}
						
						return;
					}
					
					//otherwise move record and then update in place
					if (p.getInt(9) != Page.BLOCK_SIZE - 1)
					{
						ByteBuffer bb = ByteBuffer.allocate(Page.BLOCK_SIZE);
						int i = 0;
						while (i < Page.BLOCK_SIZE)
						{
							bb.put((byte)2);
							i++;
						}
					
						int newNum = FileManager.addNewBlockNoLog(file, bb, tx);
						Block bl = new Block(file, newNum);
						LockManager.xLock(bl, tx.number());
						tx.requestPage(bl);
						Page p2 = null;
						try
						{
							p2 = tx.getPage(bl);
						}
						catch(Exception e)
						{
							HRDBMSWorker.logger.error("", e);
							throw e;
						}
					
						int length = p.getInt(5);
						byte[] after = new byte[length];
						p.get(0, after);
						long lsn = LogManager.getLSN();
						p2.write(0, after, tx.number(), lsn);
					
						ByteBuffer aft = ByteBuffer.allocate(4);
						aft.position(0);
						aft.putInt(Page.BLOCK_SIZE-1);
						lsn = LogManager.getLSN();
						p2.write(9, aft.array(), tx.number(), lsn);
					
						IndexRecord parent = getInternalUp(true);
						parent.replaceDownPointerNoLog(this.getBlockAndOffset(), new BlockAndOffset(bl.number(), 13));
						IndexRecord moved = new IndexRecord(file, bl.number(), 13, tx, true, p2);
						moved.setChildUpPointersNoLog();
						moved.addInternalDownNoLog(bao, keyBytes);
					}
					else
					{
						splitNoLog();
						byte[] myHigh = getHighKey();
						ArrayList<Object> myHighRow = convertBytesToRow(myHigh);
						ArrayList<Object> row = convertBytesToRow(keyBytes);
						
						RowComparator rc = new RowComparator(orders, types);
						if (rc.compare(row, myHighRow) <= 0)
						{
							addInternalDownNoLog(bao, keyBytes);
						}
						else
						{
							IndexRecord rec = getNextSibling();
							rec.addInternalDownNoLog(bao, keyBytes);
						}
					}
				}
			}
			else
			{
				//if this is the root
				if (b.number() == 0)
				{
					//split and add and fix up and possibly reset down key in root
					splitNoLog();
					int lstart = 22 + 129 * 8;
					int o = lstart;
					for (String type : types)
					{
						o++;
						if (type.equals("INT"))
						{
							o += 4;
						}
						else if (type.equals("LONG"))
						{
							o += 8;
						}
						else if (type.equals("FLOAT"))
						{
							o += 8;
						}
						else if (type.equals("DATE"))
						{
							o += 8;
						}
						else if (type.equals("CHAR"))
						{
							int length = p.getInt(o);
							o += (4 + length);
						}
						else
						{
							HRDBMSWorker.logger.error("Unknown type in IndexRecord.addInternalDown(): " + type);
							throw new Exception("Unknown type in IndexRecord.addInternalDown(): " + type);
						}
					}
					
					int rstart = o;
					for (String type : types)
					{
						o++;
						if (type.equals("INT"))
						{
							o += 4;
						}
						else if (type.equals("LONG"))
						{
							o += 8;
						}
						else if (type.equals("FLOAT"))
						{
							o += 8;
						}
						else if (type.equals("DATE"))
						{
							o += 8;
						}
						else if (type.equals("CHAR"))
						{
							int length = p.getInt(o);
							o += (4 + length);
						}
						else
						{
							HRDBMSWorker.logger.error("Unknown type in IndexRecord.addInternalDown(): " + type);
							throw new Exception("Unknown type in IndexRecord.addInternalDown(): " + type);
						}
					}
					
					byte[] lKey = new byte[rstart-lstart];
					p.get(lstart, lKey);
					byte[] rKey = new byte[o-rstart];
					p.get(rstart, rKey);
					ArrayList<Object> leftRow = this.convertBytesToRow(lKey);
					ArrayList<Object> row = convertBytesToRow(keyBytes);
					RowComparator rc = new RowComparator(orders, types);
					
					if (rc.compare(row, leftRow) <= 0)
					{
						//add and fix up
						IndexRecord child = this.getDown(0);
						child.addInternalDownNoLog(bao, keyBytes);
					}
					else
					{
						ArrayList<Object> rightRow = convertBytesToRow(rKey);
						
						if (rc.compare(row, rightRow) <= 0)
						{
							//add and fix up
							IndexRecord child = this.getDown(1);
							child.addInternalDownNoLog(bao, keyBytes);
						}
						else
						{
							//add and fix up
							IndexRecord child = this.getDown(1);
							child.addInternalDownNoLog(bao, keyBytes);
							child.updateParentDownKeyNoLog(keyBytes);
						}
					}
				}
				else
				{
					splitNoLog();
					byte[] myHigh = getHighKey();
					ArrayList<Object> myHighRow = convertBytesToRow(myHigh);
					
					ArrayList<Object> row = convertBytesToRow(keyBytes);
					
					RowComparator rc = new RowComparator(orders, types);
					if (rc.compare(row, myHighRow) <= 0)
					{
						addInternalDownNoLog(bao, keyBytes);
					}
					else
					{
						IndexRecord rec = getNextSibling();
						rec.addInternalDownNoLog(bao, keyBytes);
					}
				}
			}
		}
		
		private IndexRecord getNextSibling() throws Exception
		{
			IndexRecord up = getInternalUp(true);
			return up.getKeyAfter(new BlockAndOffset(this.b.number(), this.off));
		}
		
		private IndexRecord getKeyAfter(BlockAndOffset bao) throws Exception
		{
			int o = 22;
			int i = 0;
			while (true)
			{
				int block = p.getInt(o);
				o += 4;
				int offset = p.getInt(o);
				o += 4;
				
				if (block == bao.block() && offset == bao.offset())
				{
					return new IndexRecord(file, p.getInt(o), p.getInt(o+4), tx, true);
				}
			}
		}
		
		private IndexRecord getDownMatchingKey(byte[] keyBytes) throws Exception
		{
			int o = off+9+129*8;
			int i = 0;
			while (true)
			{
				int start = o;
				for (String type : types)
				{
					o++;
					if (type.equals("INT"))
					{
						o += 4;
					}
					else if (type.equals("LONG"))
					{
						o += 8;
					}
					else if (type.equals("FLOAT"))
					{
						o += 8;
					}
					else if (type.equals("DATE"))
					{
						o += 8;
					}
					else if (type.equals("CHAR"))
					{
						int length = p.getInt(o);
						o += (4+length);
					}
					else
					{
						HRDBMSWorker.logger.error("Unknown type in IndexRecord.getDownMatchingKey(): " + type);
						throw new Exception("Unknown type in IndexRecord.getDownMatchingKey(): " + type);
					}
				}
				
				int length = o - start;
				if (length == keyBytes.length)
				{
					byte[] lhs = new byte[length];
					p.get(start, lhs);
					if (Arrays.equals(lhs, keyBytes))
					{
						//found the match
						return this.getDown(i, true);
					}
				}
				
				i++;
			}
		}
		
		private byte[] getHighKey() throws Exception
		{
			int nks = p.getInt(off+5);
			int o = off+9+129*8;
			int i = 0;
			int p1HKS = -1;
			while (i < nks)
			{
				if (i == nks - 1)
				{
					p1HKS = o;
				}
				for (String type : types)
				{
					o++; //null indicator
					
					if (type.equals("INT"))
					{
						o += 4;
					}
					else if (type.equals("LONG"))
					{
						o += 8;
					}
					else if (type.equals("FLOAT"))
					{
						o += 8;
					}
					else if (type.equals("DATE"))
					{
						o += 8;
					}
					else if (type.equals("CHAR"))
					{
						o += p.getInt(o);
						o += 4;
					}
					else
					{
						HRDBMSWorker.logger.error("Unknown type in IndexRecord.getHighKey(): " + type);
						throw new Exception("Unknown type in IndexRecord.getHighKey(): " + type);
					}
				}
				
				i++;
			}
			
			byte[] p1HighKey = new byte[o - p1HKS];
			p.get(p1HKS, p1HighKey);
			return p1HighKey;
		}
		
		private IndexRecord getRecordAfter(BlockAndOffset bao) throws Exception
		{
			int o = off+9;
			while (true)
			{
				if (p.getInt(o) == bao.block() && p.getInt(o+4) == bao.offset())
				{
					return new IndexRecord(file, p.getInt(o+8), p.getInt(o+12), tx, true);
				}
				
				o += 8;
			}
		}
		
		private ArrayList<Object> convertBytesToRow(byte[] keyBytes) throws Exception
		{
			ByteBuffer bb = ByteBuffer.wrap(keyBytes);
			int o = 0;
			ArrayList<Object> retval = new ArrayList<Object>(types.size());
			for (String type : types)
			{
				o++;
				if (type.equals("INT"))
				{
					retval.add(bb.getInt(o));
					o += 4;
				}
				else if (type.equals("LONG"))
				{
					retval.add(bb.getLong(o));
					o += 8;
				}
				else if (type.equals("FLOAT"))
				{
					retval.add(bb.getDouble(o));
					o += 8;
				}
				else if (type.equals("DATE"))
				{
					retval.add(new MyDate(bb.getLong(o)));
					o += 8;
				}
				else if (type.equals("CHAR"))
				{
					try
					{
						int length = bb.getInt(o);
						o += 4;
						byte[] bytes = new byte[length];
						bb.get(bytes, o, length);
						o += length;
						retval.add(new String(bytes, "UTF-8"));
					}
					catch(Exception e)
					{
						HRDBMSWorker.logger.error("Key bytes are: ");
						for (byte b : keyBytes)
						{
							HRDBMSWorker.logger.error("" + b);
						}
						HRDBMSWorker.logger.error("", e);
						throw e;
					}
				}
			}
			
			return retval;
		}
		
		private byte[] getKeyBytesByIndex(int index) throws Exception
		{
			int o = off+9;
			int i = 0;
			while (i < index)
			{
				for (String type : types)
				{
					o++;
					if (type.equals("INT"))
					{
						o += 4;
					}
					else if (type.equals("LONG"))
					{
						o += 8;
					}
					else if (type.equals("FLOAT"))
					{
						o += 8;
					}
					else if (type.equals("DATE"))
					{
						o += 8;
					}
					else if (type.equals("CHAR"))
					{
						int length = p.getInt(o);
						o += (4+length);
					}
					else
					{
						HRDBMSWorker.logger.error("Unknown type in IndexRecord.getKeyBytesByIndex(): " + type);
						throw new Exception("Unknown type in IndexRecord.getKeyBytesByIndex(): " + type);
					}
				}
				
				i++;
			}
			
			int start = o;
			for (String type : types)
			{
				o++;
				if (type.equals("INT"))
				{
					o += 4;
				}
				else if (type.equals("LONG"))
				{
					o += 8;
				}
				else if (type.equals("FLOAT"))
				{
					o += 8;
				}
				else if (type.equals("DATE"))
				{
					o += 8;
				}
				else if (type.equals("CHAR"))
				{
					int length = p.getInt(o);
					o += (4+length);
				}
				else
				{
					HRDBMSWorker.logger.error("Unknown type in IndexRecord.getKeyBytesByIndex(): " + type);
					throw new Exception("Unknown type in IndexRecord.getKeyBytesByIndex(): " + type);
				}
			}
			
			byte[] retval = new byte[o-start];
			p.get(start, retval);
			return retval;
		}
		
		private byte[] getDownKey(BlockAndOffset bao) throws Exception
		{
			int o = off+9;
			int i = 0;
			while (true)
			{
				if (p.getInt(o) == bao.block() && p.getInt(o+4) == bao.offset())
				{
					//found the match
					break;
				}
				
				o += 8;
				i++;
			}
			
			o = off+9+129*8;
			int j = 0;
			int start = -1;
			while (j <= i)
			{
				if (j == i)
				{
					start = o;
				}
				for (String type : types)
				{
					o++;
					if (type.equals("INT"))
					{
						o += 4;
					}
					else if (type.equals("LONG"))
					{
						o += 8;
					}
					else if (type.equals("DATE"))
					{
						o += 8;
					}
					else if (type.equals("FLOAT"))
					{
						o += 8;
					}
					else if (type.equals("CHAR"))
					{
						int length = p.getInt(o);
						o += (4 + length);
					}
					else
					{
						HRDBMSWorker.logger.error("Unknown type in IndexRecord.replaceDownKey(): " + type);
						throw new Exception("Unknown type in IndexRecord.replaceDownKey(): " + type);
					}
				}
				
				j++;
			}
			
			int currentLength = o - start;
			byte[] retval = new byte[currentLength];
			p.get(start, retval);
			return retval;
		}
		
		private void replaceDownKey(BlockAndOffset bao, byte[] keyBytes) throws Exception
		{
			int o = off+9;
			int i = 0;
			while (true)
			{
				if (p.getInt(o) == bao.block() && p.getInt(o+4) == bao.offset())
				{
					//found the match
					break;
				}
				
				o += 8;
				i++;
			}
			
			o = off+9+129*8;
			int j = 0;
			int start = -1;
			while (j <= i)
			{
				if (j == i)
				{
					start = o;
				}
				for (String type : types)
				{
					o++;
					if (type.equals("INT"))
					{
						o += 4;
					}
					else if (type.equals("LONG"))
					{
						o += 8;
					}
					else if (type.equals("DATE"))
					{
						o += 8;
					}
					else if (type.equals("FLOAT"))
					{
						o += 8;
					}
					else if (type.equals("CHAR"))
					{
						int length = p.getInt(o);
						o += (4 + length);
					}
					else
					{
						HRDBMSWorker.logger.error("Unknown type in IndexRecord.replaceDownKey(): " + type);
						throw new Exception("Unknown type in IndexRecord.replaceDownKey(): " + type);
					}
				}
				
				j++;
			}
			
			//positioned at start of keyBytes after the one that needs to change
			//start contains position of start of keyBytes that needs to change
			int currentLength = o - start;
			if (currentLength == keyBytes.length)
			{
				//lay directly over it
				byte[] before = new byte[currentLength];
				p.get(start, before);
				InsertLogRec rec = tx.insert(before, keyBytes, start, p.block());
				p.write(start, keyBytes, tx.number(), rec.lsn());
			}
			else if (keyBytes.length < currentLength)
			{
				//put new keyBytes in place
				byte[] before = new byte[keyBytes.length];
				p.get(start, before);
				InsertLogRec rec = tx.insert(before, keyBytes, start, p.block());
				p.write(start, keyBytes, tx.number(), rec.lsn());
				
				int moveForward = currentLength - keyBytes.length;
				start = o;
				int numKeys = p.getInt(off+5);
				while (j < numKeys)
				{
					for (String type : types)
					{
						o++;
						if (type.equals("INT"))
						{
							o += 4;
						}
						else if (type.equals("LONG"))
						{
							o += 8;
						}
						else if (type.equals("DATE"))
						{
							o += 8;
						}
						else if (type.equals("FLOAT"))
						{
							o += 8;
						}
						else if (type.equals("CHAR"))
						{
							int length = p.getInt(o);
							o += (4 + length);
						}
						else
						{
							HRDBMSWorker.logger.error("Unknown type in IndexRecord.replaceDownKey(): " + type);
							throw new Exception("Unknown type in IndexRecord.replaceDownKey(): " + type);
						}
					}
					
					j++;
				}
				
				int toMove = o - start;
				if (toMove > 0)
				{
					before = new byte[toMove];
					p.get(start-moveForward, before);
					byte[] after = new byte[toMove];
					p.get(start, after);
					rec = tx.insert(before, after, start-moveForward, p.block());
					p.write(start-moveForward, after, tx.number(), rec.lsn());
				}
					
				int firstFree = p.getInt(5);
				ByteBuffer bef = ByteBuffer.allocate(4);
				bef.position(0);
				bef.putInt(firstFree);
				ByteBuffer a = ByteBuffer.allocate(4);
				a.position(0);
				a.putInt(firstFree-moveForward);
				rec = tx.insert(bef.array(), a.array(), 5, p.block());
				p.write(5, a.array(), tx.number(), rec.lsn());
				bef.position(0);
				bef.putInt(p.getInt(14));
				a.position(0);
				a.putInt(firstFree-moveForward-13);
				rec = tx.insert(bef.array(), a.array(), 14, p.block());
				p.write(14, a.array(), tx.number(), rec.lsn());
			}
			else
			{
				//we need to expand this record
				int expand = keyBytes.length - currentLength;
				//do we have this much room available?
				int available = p.getInt(9) - p.getInt(5) + 1;
				
				if (expand <= available)
				{
					//we have room
					//make room for new keyBytes
					int start2 = o;
					int numKeys = p.getInt(off+5);
					while (j < numKeys)
					{
						for (String type : types)
						{
							o++;
							if (type.equals("INT"))
							{
								o += 4;
							}
							else if (type.equals("LONG"))
							{
								o += 8;
							}
							else if (type.equals("DATE"))
							{
								o += 8;
							}
							else if (type.equals("FLOAT"))
							{
								o += 8;
							}
							else if (type.equals("CHAR"))
							{
								int length = p.getInt(o);
								o += (4 + length);
							}
							else
							{
								HRDBMSWorker.logger.error("Unknown type in IndexRecord.replaceDownKey(): " + type);
								throw new Exception("Unknown type in IndexRecord.replaceDownKey(): " + type);
							}
						}
						
						j++;
					}
					
					int toMove = o - start2;
					if (toMove > 0)
					{
						byte[] before = new byte[toMove];
						p.get(start2+expand, before);
						byte[] after = new byte[toMove];
						p.get(start2, after);
						InsertLogRec rec = tx.insert(before, after, start2+expand, p.block());
						p.write(start2+expand, after, tx.number(), rec.lsn());
					}
					
					//put new keyBytes in place
					byte[] before = new byte[keyBytes.length];
					p.get(start, before);
					InsertLogRec rec = tx.insert(before, keyBytes, start, p.block());
					p.write(start, keyBytes, tx.number(), rec.lsn());
						
					int firstFree = p.getInt(5);
					ByteBuffer bef = ByteBuffer.allocate(4);
					bef.position(0);
					bef.putInt(firstFree);
					ByteBuffer a = ByteBuffer.allocate(4);
					a.position(0);
					a.putInt(firstFree+expand);
					rec = tx.insert(bef.array(), a.array(), 5, p.block());
					p.write(5, a.array(), tx.number(), rec.lsn());
					bef.position(0);
					bef.putInt(p.getInt(14));
					a.position(0);
					a.putInt(firstFree+expand-13);
					rec = tx.insert(bef.array(), a.array(), 14, p.block());
					p.write(14, a.array(), tx.number(), rec.lsn());
				}
				else
				{
					//we need to move this internal node to a new page, update the parent down pointer, and recall method
					if (p.getInt(9) != Page.BLOCK_SIZE - 1)
					{
						ByteBuffer bb = ByteBuffer.allocate(Page.BLOCK_SIZE);
						i = 0;
						while (i < Page.BLOCK_SIZE)
						{
							bb.put((byte)2);
							i++;
						}
					
						int newNum = FileManager.addNewBlock(file, bb, tx);
						Block bl = new Block(file, newNum);
						LockManager.xLock(bl, tx.number());
						tx.requestPage(bl);
						Page p2 = null;
						try
						{
							p2 = tx.getPage(bl);
						}
						catch(Exception e)
						{
							HRDBMSWorker.logger.error("", e);
							throw e;
						}
					
						int length = p.getInt(5);
						byte[] before = new byte[length];
						p2.get(0, before);
						byte[] after = new byte[length];
						p.get(0, after);
						InsertLogRec rec = tx.insert(before, after, 0, p2.block());
						p2.write(0, after, tx.number(), rec.lsn());
					
						before = new byte[4];
						p2.get(9, before);
						ByteBuffer a = ByteBuffer.allocate(4);
						a.position(0);
						a.putInt(Page.BLOCK_SIZE-1);
						rec = tx.insert(before, a.array(), 9, p2.block());
						p2.write(9, a.array(), tx.number(), rec.lsn());
					
						IndexRecord parent = getInternalUp(true);
						parent.replaceDownPointer(this.getBlockAndOffset(), new BlockAndOffset(bl.number(), 13));
						parent = getInternalUp(true);
						parent.replaceDownKey(bao, keyBytes);
						IndexRecord newRec = new IndexRecord(file, p2.block().number(), 13, tx, true, p2);
						newRec.setChildUpPointers();
					}
					else
					{
						//need to split first
						split();
						IndexRecord line2 = new IndexRecord(fileName, 0, 13, tx, true);
						ArrayList<Object> row = convertBytesToRow(keyBytes);

						while (line2.b.number() != bao.block() || line2.off != bao.offset())
						{
							i = 0;
							Iterator it = line2.internalIterator(types);
							while (it.hasNext())
							{
								ArrayList<Object> k = (ArrayList<Object>)it.next();
								//HRDBMSWorker.logger.debug("Saw internal key: " + k);
								//if (((Comparable)val).compareTo(key) < 1)
								if (compare(row, k) < 1)
								{
									line2 = line2.getDown(i);
									break;
								}
								else
								{
									if (!it.hasNext())
									{
										line2 = line2.getDown(i);
										break;
									}
								}
								
								i++;
							}
						}
						
						line2.getInternalUp(true).replaceDownKey(bao, keyBytes);
					}
				}
			}
		}
		
		private void replaceDownKeyNoLog(BlockAndOffset bao, byte[] keyBytes) throws Exception
		{
			int o = off+9;
			int i = 0;
			while (true)
			{
				if (p.getInt(o) == bao.block() && p.getInt(o+4) == bao.offset())
				{
					//found the match
					break;
				}
				
				o += 8;
				i++;
			}
			
			o = off+9+129*8;
			int j = 0;
			int start = -1;
			while (j <= i)
			{
				if (j == i)
				{
					start = o;
				}
				for (String type : types)
				{
					o++;
					if (type.equals("INT"))
					{
						o += 4;
					}
					else if (type.equals("LONG"))
					{
						o += 8;
					}
					else if (type.equals("DATE"))
					{
						o += 8;
					}
					else if (type.equals("FLOAT"))
					{
						o += 8;
					}
					else if (type.equals("CHAR"))
					{
						int length = p.getInt(o);
						o += (4 + length);
					}
					else
					{
						HRDBMSWorker.logger.error("Unknown type in IndexRecord.replaceDownKey(): " + type);
						throw new Exception("Unknown type in IndexRecord.replaceDownKey(): " + type);
					}
				}
				
				j++;
			}
			
			//positioned at start of keyBytes after the one that needs to change
			//start contains position of start of keyBytes that needs to change
			int currentLength = o - start;
			if (currentLength == keyBytes.length)
			{
				//lay directly over it
				long lsn = LogManager.getLSN();
				p.write(start, keyBytes, tx.number(), lsn);
			}
			else if (keyBytes.length < currentLength)
			{
				//put new keyBytes in place
				long lsn = LogManager.getLSN();
				p.write(start, keyBytes, tx.number(), lsn);
				
				int moveForward = currentLength - keyBytes.length;
				start = o;
				int numKeys = p.getInt(off+5);
				while (j < numKeys)
				{
					for (String type : types)
					{
						o++;
						if (type.equals("INT"))
						{
							o += 4;
						}
						else if (type.equals("LONG"))
						{
							o += 8;
						}
						else if (type.equals("DATE"))
						{
							o += 8;
						}
						else if (type.equals("FLOAT"))
						{
							o += 8;
						}
						else if (type.equals("CHAR"))
						{
							int length = p.getInt(o);
							o += (4 + length);
						}
						else
						{
							HRDBMSWorker.logger.error("Unknown type in IndexRecord.replaceDownKey(): " + type);
							throw new Exception("Unknown type in IndexRecord.replaceDownKey(): " + type);
						}
					}
					
					j++;
				}
				
				int toMove = o - start;
				if (toMove > 0)
				{
					byte[] after = new byte[toMove];
					p.get(start, after);
					lsn = LogManager.getLSN();
					p.write(start-moveForward, after, tx.number(), lsn);
				}
					
				int firstFree = p.getInt(5);
				ByteBuffer a = ByteBuffer.allocate(4);
				a.position(0);
				a.putInt(firstFree-moveForward);
				lsn = LogManager.getLSN();
				p.write(5, a.array(), tx.number(), lsn);
				a.position(0);
				a.putInt(firstFree-moveForward-13);
				lsn = LogManager.getLSN();
				p.write(14, a.array(), tx.number(), lsn);
			}
			else
			{
				//we need to expand this record
				int expand = keyBytes.length - currentLength;
				//do we have this much room available?
				int available = p.getInt(9) - p.getInt(5) + 1;
				
				if (expand <= available)
				{
					//we have room
					//make room for new keyBytes
					int start2 = o;
					int numKeys = p.getInt(off+5);
					while (j < numKeys)
					{
						for (String type : types)
						{
							o++;
							if (type.equals("INT"))
							{
								o += 4;
							}
							else if (type.equals("LONG"))
							{
								o += 8;
							}
							else if (type.equals("DATE"))
							{
								o += 8;
							}
							else if (type.equals("FLOAT"))
							{
								o += 8;
							}
							else if (type.equals("CHAR"))
							{
								int length = p.getInt(o);
								o += (4 + length);
							}
							else
							{
								HRDBMSWorker.logger.error("Unknown type in IndexRecord.replaceDownKey(): " + type);
								throw new Exception("Unknown type in IndexRecord.replaceDownKey(): " + type);
							}
						}
						
						j++;
					}
					
					int toMove = o - start2;
					if (toMove > 0)
					{
						byte[] after = new byte[toMove];
						p.get(start2, after);
						long lsn = LogManager.getLSN();
						p.write(start2+expand, after, tx.number(), lsn);
					}
					
					//put new keyBytes in place
					long lsn = LogManager.getLSN();
					p.write(start, keyBytes, tx.number(), lsn);
						
					int firstFree = p.getInt(5);
					ByteBuffer a = ByteBuffer.allocate(4);
					a.position(0);
					a.putInt(firstFree+expand);
					lsn = LogManager.getLSN();
					p.write(5, a.array(), tx.number(), lsn);
					a.position(0);
					a.putInt(firstFree+expand-13);
					lsn = LogManager.getLSN();
					p.write(14, a.array(), tx.number(), lsn);
				}
				else
				{
					//we need to move this internal node to a new page, update the parent down pointer, and recall method
					if (p.getInt(9) != Page.BLOCK_SIZE - 1)
					{
						ByteBuffer bb = ByteBuffer.allocate(Page.BLOCK_SIZE);
						i = 0;
						while (i < Page.BLOCK_SIZE)
						{
							bb.put((byte)2);
							i++;
						}
					
						int newNum = FileManager.addNewBlockNoLog(file, bb, tx);
						Block bl = new Block(file, newNum);
						LockManager.xLock(bl, tx.number());
						tx.requestPage(bl);
						Page p2 = null;
						try
						{
							p2 = tx.getPage(bl);
						}
						catch(Exception e)
						{
							HRDBMSWorker.logger.error("", e);
							throw e;
						}
					
						int length = p.getInt(5);
						byte[] after = new byte[length];
						p.get(0, after);
						long lsn = LogManager.getLSN();
						p2.write(0, after, tx.number(), lsn);
					
						ByteBuffer a = ByteBuffer.allocate(4);
						a.position(0);
						a.putInt(Page.BLOCK_SIZE-1);
						lsn = LogManager.getLSN();
						p2.write(9, a.array(), tx.number(), lsn);
					
						IndexRecord parent = getInternalUp(true);
						parent.replaceDownPointerNoLog(this.getBlockAndOffset(), new BlockAndOffset(bl.number(), 13));
						parent = getInternalUp(true);
						parent.replaceDownKeyNoLog(bao, keyBytes);
						IndexRecord newRec = new IndexRecord(file, p2.block().number(), 13, tx, true, p2);
						newRec.setChildUpPointersNoLog();
					}
					else
					{
						//need to split first
						splitNoLog();
						IndexRecord line2 = new IndexRecord(fileName, 0, 13, tx, true);
						ArrayList<Object> row = convertBytesToRow(keyBytes);

						while (line2.b.number() != bao.block() || line2.off != bao.offset())
						{
							i = 0;
							Iterator it = line2.internalIterator(types);
							while (it.hasNext())
							{
								ArrayList<Object> k = (ArrayList<Object>)it.next();
								//HRDBMSWorker.logger.debug("Saw internal key: " + k);
								//if (((Comparable)val).compareTo(key) < 1)
								if (compare(row, k) < 1)
								{
									line2 = line2.getDown(i);
									break;
								}
								else
								{
									if (!it.hasNext())
									{
										line2 = line2.getDown(i);
										break;
									}
								}
								
								i++;
							}
						}
						
						line2.getInternalUp(true).replaceDownKeyNoLog(bao, keyBytes);
					}
				}
			}
		}
		
		public BlockAndOffset writeNewLeafNoLog(FieldValue[] keys, RID rid) throws Exception
		{
			int firstFree = p.getInt(5);
			int lastFree = p.getInt(9);
			byte[] keyBytes = this.genKeyBytes(keys);
			int length = 41 + keyBytes.length;
			int freeLength = lastFree - firstFree + 1;
			
			if (length + keyBytes.length <= freeLength && b.number() != 0) 
			{
				//do it on this page
				int leafOff = lastFree - length + 1;
				ByteBuffer after = ByteBuffer.allocate(length);
				after.position(0);
				after.put((byte)1); //leaf
				after.putLong(0); //prev pointer filled in later
				after.putLong(0); //next pointer filled in later
				after.putLong(0); //up pointer filled in later
				after.putInt(rid.getNode());
				after.putInt(rid.getDevice());
				after.putInt(rid.getBlockNum());
				after.putInt(rid.getRecNum());
				after.put(keyBytes);
				long lsn = LogManager.getLSN();
				p.write(leafOff, after.array(), tx.number(), lsn);
				after = ByteBuffer.allocate(4);
				after.position(0);
				after.putInt(leafOff - 1);
				lsn = LogManager.getLSN();
				p.write(9, after.array(), tx.number(), lsn);
				return new BlockAndOffset(b.number(), leafOff);
			}
			
			LockManager.sLock(new Block(file, -1), tx.number());
			int i = 1;
			int numBlocks = FileManager.numBlocks.get(file);
			while (i < numBlocks)
			{
				Block bl = new Block(file, i);
				LockManager.xLock(bl, tx.number());
				tx.requestPage(bl);
				Page p2 = null;
				try
				{
					p2 = tx.getPage(bl);
				}
				catch(Exception e)
				{
					HRDBMSWorker.logger.error("", e);
					throw e;
				}
				
				firstFree = p2.getInt(5);
				lastFree = p2.getInt(9);
				freeLength = lastFree - firstFree + 1;
				
				if (length + keyBytes.length <= freeLength)
				{
					//do it on this page
					IndexRecord dummy = new IndexRecord(file, bl.number(), 13, tx, true, p2);
					return dummy.writeNewLeafNoLog(keys, rid);
				}
				
				i++;
			}
			
			//need to add new page
			ByteBuffer bb = ByteBuffer.allocate(Page.BLOCK_SIZE);
			i = 0;
			while (i < Page.BLOCK_SIZE)
			{
				bb.put((byte)2);
				i++;
			}
			
			int newNum = FileManager.addNewBlockNoLog(file, bb, tx);
			Block bl = new Block(file, newNum);
			LockManager.xLock(bl, tx.number());
			tx.requestPage(bl);
			Page p2 = null;
			try
			{
				p2 = tx.getPage(bl);
			}
			catch(Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				throw e;
			}
			
			//copy first 5 bytes
			byte[] bytes = new byte[5];
			p.get(0, bytes);
			long lsn = LogManager.getLSN();
			p2.write(0, bytes, tx.number(), lsn);
			
			//fill in remainder of header and dummy internal node
			ByteBuffer aft = ByteBuffer.allocate(25+128*8);
			aft.position(0);
			aft.putInt(30+128*8);
			aft.putInt(Page.BLOCK_SIZE-1);
			aft.put((byte)0); //internal
			aft.putInt(17+128*8);
			i = 0;
			while (i < 128)
			{
				aft.putLong(0); //dummy down pointers
				i++;
			}
			
			aft.putLong(0); //dummy up pointer
			
			lsn = LogManager.getLSN();
			p2.write(5, aft.array(), tx.number(), lsn);
			
			IndexRecord dummy = new IndexRecord(file, bl.number(), 13, tx, true, p2);
			return dummy.writeNewLeafNoLog(keys, rid);
		}
		
		public BlockAndOffset writeNewLeaf(FieldValue[] keys, RID rid) throws Exception
		{
			int firstFree = p.getInt(5);
			int lastFree = p.getInt(9);
			byte[] keyBytes = this.genKeyBytes(keys);
			int length = 41 + keyBytes.length;
			int freeLength = lastFree - firstFree + 1;
			
			if (length + keyBytes.length <= freeLength && b.number() != 0) 
			{
				//do it on this page
				int leafOff = lastFree - length + 1;
				byte[] before = new byte[length];
				p.get(leafOff, before);
				ByteBuffer after = ByteBuffer.allocate(length);
				after.position(0);
				after.put((byte)1); //leaf
				after.putLong(0); //prev pointer filled in later
				after.putLong(0); //next pointer filled in later
				after.putLong(0); //up pointer filled in later
				after.putInt(rid.getNode());
				after.putInt(rid.getDevice());
				after.putInt(rid.getBlockNum());
				after.putInt(rid.getRecNum());
				after.put(keyBytes);
				InsertLogRec rec = tx.insert(before, after.array(), leafOff, p.block());
				p.write(leafOff, after.array(), tx.number(), rec.lsn());
				before = new byte[4];
				p.get(9, before);
				after = ByteBuffer.allocate(4);
				after.position(0);
				after.putInt(leafOff - 1);
				rec = tx.insert(before,  after.array(), 9, p.block());
				p.write(9, after.array(), tx.number(), rec.lsn());
				return new BlockAndOffset(b.number(), leafOff);
			}
			
			LockManager.sLock(new Block(file, -1), tx.number());
			int i = 1;
			int numBlocks = FileManager.numBlocks.get(file);
			while (i < numBlocks)
			{
				Block bl = new Block(file, i);
				LockManager.xLock(bl, tx.number());
				tx.requestPage(bl);
				Page p2 = null;
				try
				{
					p2 = tx.getPage(bl);
				}
				catch(Exception e)
				{
					HRDBMSWorker.logger.error("", e);
					throw e;
				}
				
				firstFree = p2.getInt(5);
				lastFree = p2.getInt(9);
				freeLength = lastFree - firstFree + 1;
				
				if (length + keyBytes.length <= freeLength)
				{
					//do it on this page
					IndexRecord dummy = new IndexRecord(file, bl.number(), 13, tx, true, p2);
					return dummy.writeNewLeaf(keys, rid);
				}
				
				i++;
			}
			
			//need to add new page
			ByteBuffer bb = ByteBuffer.allocate(Page.BLOCK_SIZE);
			i = 0;
			while (i < Page.BLOCK_SIZE)
			{
				bb.put((byte)2);
				i++;
			}
			
			int newNum = FileManager.addNewBlock(file, bb, tx);
			Block bl = new Block(file, newNum);
			LockManager.xLock(bl, tx.number());
			tx.requestPage(bl);
			Page p2 = null;
			try
			{
				p2 = tx.getPage(bl);
			}
			catch(Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				throw e;
			}
			
			//copy first 5 bytes
			byte[] bytes = new byte[5];
			p.get(0, bytes);
			byte[] bef = new byte[5];
			p2.get(0, bef);
			InsertLogRec rec = tx.insert(bef, bytes, 0, p2.block());
			p2.write(0, bytes, tx.number(), rec.lsn());
			
			//fill in remainder of header and dummy internal node
			bef = new byte[25+128*8];
			p2.get(5, bef);
			ByteBuffer aft = ByteBuffer.allocate(25+128*8);
			aft.position(0);
			aft.putInt(30+128*8);
			aft.putInt(Page.BLOCK_SIZE-1);
			aft.put((byte)0); //internal
			aft.putInt(17+128*8);
			i = 0;
			while (i < 128)
			{
				aft.putLong(0); //dummy down pointers
				i++;
			}
			
			aft.putLong(0); //dummy up pointer
			
			rec = tx.insert(bef, aft.array(), 5, p2.block());
			p2.write(5, aft.array(), tx.number(), rec.lsn());
			
			IndexRecord dummy = new IndexRecord(file, bl.number(), 13, tx, true, p2);
			return dummy.writeNewLeaf(keys, rid);
		}
		
		public void setPrev(IndexRecord ir) throws Exception
		{
			int block = ir.b.number();
			int o = ir.off;
			setPrev(new BlockAndOffset(block, o));
		}
		
		public void setPrevNoLog(IndexRecord ir) throws Exception
		{
			int block = ir.b.number();
			int o = ir.off;
			setPrevNoLog(new BlockAndOffset(block, o));
		}
		
		public void setPrev(BlockAndOffset bao) throws Exception
		{
			byte[] before = new byte[8];
			p.get(off+1, before);
			ByteBuffer after = ByteBuffer.allocate(8);
			after.position(0);
			after.putInt(bao.block());
			after.putInt(bao.offset());
			after.position(0);
			InsertLogRec rec = tx.insert(before, after.array(), off+1, p.block());
			p.write(off+1, after.array(), tx.number(), rec.lsn());
		}
		
		public void setPrevNoLog(BlockAndOffset bao) throws Exception
		{
			ByteBuffer after = ByteBuffer.allocate(8);
			after.position(0);
			after.putInt(bao.block());
			after.putInt(bao.offset());
			after.position(0);
			long lsn = LogManager.getLSN();
			p.write(off+1, after.array(), tx.number(), lsn);
		}
		
		/*private void insertAfter(BlockAndOffset bao) throws Exception
		{
			IndexRecord next = new IndexRecord(file, p.getInt(off+9), p.getInt(off+13), tx, true);
			this.setNext(bao);
			if (!next.isNull())
			{
				next.setPrev(bao);
			}
		}
		
		private void insertAfterNoLog(BlockAndOffset bao) throws Exception
		{
			IndexRecord next = new IndexRecord(file, p.getInt(off+9), p.getInt(off+13), tx, true);
			this.setNextNoLog(bao);
			if (!next.isNull())
			{
				next.setPrevNoLog(bao);
			}
		}*/
		
		public IndexRecord(String file, int block, int offset, Transaction tx) throws Exception
		{
			this.file = file;
			this.tx = tx;
			b = new Block(file, block);
			off = offset;
			keyOff = off + 9+128*8+8;
			LockManager.sLock(b, tx.number());
			tx.requestPage(b);
			try
			{
				p = tx.getPage(b);
			}
			catch(Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				throw e;
			}
			
			isLeaf = (p.get(off+0) == 1);
			if (p.get(off+0) == 2)
			{
				isLeaf = true;
				isTombstone = true;
			}
		}
		
		//public IndexRecord(String file, int block, int offset, Transaction tx, Page p) throws Exception
		//{
		//	this.file = file;
		//	this.tx = tx;
		//	this.p = p;
		//	this.b = p.block();
		//	off = offset;
		//	keyOff = off + 9+128*8+8;
		//	LockManager.sLock(b, tx.number());
		//	isLeaf = (p.get(off+0) == 1);
		//	if (p.get(off+0) == 2)
		//	{
		//		isLeaf = true;
		//		isTombstone = true;
		//	}
		//}
		
		public IndexRecord(String file, int block, int offset, Transaction tx, boolean x) throws Exception
		{
			this.file = file;
			this.tx = tx;
			b = new Block(file, block);
			p = myPages.get(b);
			off = offset;
			keyOff = off + 9+128*8+8;
			
			if (p == null)
			{
				LockManager.xLock(b, tx.number());
				tx.requestPage(b);
				try
				{
					p = tx.getPage(b);
				}
				catch(Exception e)
				{
					HRDBMSWorker.logger.error("", e);
					throw e;
				}
				
				myPages.put(b, p);
			}
			
			isLeaf = (p.get(off+0) == 1);
			if (p.get(off+0) == 2)
			{
				isLeaf = true;
				isTombstone = true;
			}
		}
		
		public IndexRecord(String file, int block, int offset, Transaction tx, boolean x, Page p) throws Exception
		{
			this.file = file;
			this.tx = tx;
			this.p = p;
			if (p == null)
			{
				throw new Exception("NULL page in IndexRecord Constructor");
			}
			this.b = p.block();
			off = offset;
			keyOff = off + 9+128*8+8;
			if (myPages.get(b) == null)
			{
				LockManager.xLock(b, tx.number());
				myPages.put(b, p);
			}
			isLeaf = (p.get(off+0) == 1);
			if (p.get(off+0) == 2)
			{
				isLeaf = true;
				isTombstone = true;
			}
		}
		
		private IndexRecord() 
		{}
		
		public ArrayList<Object> getKeys(ArrayList<String> types) throws Exception
		{
			int o = off+41;
			ArrayList<Object> retval = new ArrayList<Object>(types.size());
			//TODO null
			for (String type : types)
			{
				o++; // skip null indicator
				if (type.equals("INT"))
				{
					try
					{
						retval.add(p.getInt(o));
					}
					catch(Exception e)
					{
						HRDBMSWorker.logger.debug("", e);
						HRDBMSWorker.logger.debug("Retval = " + retval);
						HRDBMSWorker.logger.debug("p = " + p);
						HRDBMSWorker.logger.debug("o = " + o);
						throw e;
					}
					o += 4;
				}
				else if (type.equals("FLOAT"))
				{
					retval.add(p.getDouble(o));
					o += 8;
				}
				else if (type.equals("CHAR"))
				{
					int length = p.getInt(o);
					o += 4;
					byte[] buff = new byte[length];
					try
					{
						p.get(o, buff);
					}
					catch(Exception e)
					{
						HRDBMSWorker.logger.debug("Trying to read string of length " + buff.length + " in index " + this.file);
						throw e;
					}
					try
					{
						retval.add(new String(buff, "UTF-8"));
					}
					catch(Exception e)
					{
						HRDBMSWorker.logger.error("", e);
						throw e;
					}
					o += length;
				}
				else if (type.equals("LONG"))
				{
					retval.add(p.getLong(o));
					o += 8;
				}
				else if (type.equals("DATE"))
				{
					retval.add(new MyDate(p.getLong(o)));
					o += 8;
				}
				else
				{
					HRDBMSWorker.logger.error("Unknown type in IndexRecord.getKeys(): " + type);
					throw new Exception("Unknown type in IndexRecord.getKeys(): " + type);
				}
			}
			
			return retval;
		}
		
		public boolean isNull()
		{
			return isNull;
		}
		
		public boolean isTombstone()
		{
			return isTombstone;
		}
		
		public long getPartialRid()
		{
			return (((long)p.getInt(off+33)) << 32) + p.getInt(off+37);
		}
		
		public IndexRecord nextRecord() throws Exception 
		{
			if (p.getInt(off+9) == 0 && p.getInt(off+13) == 0)
			{
				IndexRecord retval = new IndexRecord();
				retval.isNull = true;
				return retval;
			}
			
			return new IndexRecord(file, p.getInt(off+9), p.getInt(off+13), tx);
		}
		
		public IndexRecord prevRecord() throws Exception
		{
			if (p.getInt(off+1) == 0 && p.getInt(off+5) == 0)
			{
				IndexRecord retval = new IndexRecord();
				retval.isNull = true;
				return retval;
			}
			
			return new IndexRecord(file, p.getInt(off+1), p.getInt(off+5), tx);
		}
		
		public IndexRecord prevRecord(boolean x) throws Exception
		{
			if (!x)
			{
				return prevRecord();
			}
			
			if (p.getInt(off+1) == 0 && p.getInt(off+5) == 0)
			{
				IndexRecord retval = new IndexRecord();
				retval.isNull = true;
				return retval;
			}
			
			return new IndexRecord(file, p.getInt(off+1), p.getInt(off+5), tx, true);
		}
		
		public IndexRecord nextRecord(boolean x) throws Exception
		{
			if (!x)
			{
				return nextRecord();
			}
			
			if (p.getInt(off+9) == 0 && p.getInt(off+13) == 0)
			{
				IndexRecord retval = new IndexRecord();
				retval.isNull = true;
				return retval;
			}
			
			return new IndexRecord(file, p.getInt(off+9), p.getInt(off+13), tx, true);
		}
		
		//public IndexRecord getUp() throws Exception
		//{
		//	if (p.getInt(off+17) == 0 && p.getInt(off+21) == 0)
		//	{
		//		IndexRecord retval = new IndexRecord();
		//		retval.isNull = true;
		//		return retval;
		//	}
		//	
		//	return new IndexRecord(file, p.getInt(off+17), p.getInt(off+21), tx);
		//}
		
		public IndexRecord getUp(boolean x) throws Exception
		{
			if (p.getInt(off+17) == 0 && p.getInt(off+21) == 0)
			{
				IndexRecord retval = new IndexRecord();
				retval.isNull = true;
				return retval;
			}
			
			if (x)
			{
				return new IndexRecord(file, p.getInt(off+17), p.getInt(off+21), tx, true);
			}
			else
			{
				return new IndexRecord(file, p.getInt(off+17), p.getInt(off+21), tx);
			}
		}
		
		public IndexRecord getInternalUp(boolean x) throws Exception
		{
			if (p.getInt(off+9+128*8) == 0 && p.getInt(off+13+128*8) == 0)
			{
				IndexRecord retval = new IndexRecord();
				retval.isNull = true;
				return retval;
			}
			
			if (x)
			{
				return new IndexRecord(file, p.getInt(off+9+128*8), p.getInt(off+13+128*8), tx, true);
			}
			else
			{
				return new IndexRecord(file, p.getInt(off+9+128*8), p.getInt(off+13+128*8), tx);
			}
		}
		
		public boolean isLeaf()
		{
			return isLeaf;
		}
		
		private ArrayList<Object> getInternalKeys(ArrayList<String> types) throws Exception
		{
			try
			{
				ArrayList<Object> retval = new ArrayList<Object>(types.size());
				for (String type : types)
				{
					keyOff++; // skip null indicator
					if (type.equals("INT"))
					{
						retval.add(p.getInt(keyOff));
						keyOff += 4;
					}
					else if (type.equals("FLOAT"))
					{
						retval.add(p.getDouble(keyOff));
						keyOff += 8;
					}
					else if (type.equals("CHAR"))
					{
						int length = p.getInt(keyOff);
						keyOff += 4;
						byte[] buff = new byte[length];
						p.get(keyOff, buff);
						try
						{
							retval.add(new String(buff, "UTF-8"));
						}
						catch(Exception e)
						{
							HRDBMSWorker.logger.error("", e);
							throw e;
						}
						keyOff += length;
					}
					else if (type.equals("LONG"))
					{
						retval.add(p.getLong(keyOff));
						keyOff += 8;
					}
					else if (type.equals("DATE"))
					{
						retval.add(new MyDate(p.getLong(keyOff)));
						keyOff += 8;
					}
					else
					{
						HRDBMSWorker.logger.error("Unknown type in IndexRecord.getInternalKeys(): " + type);
						throw new Exception("Unknown type in IndexRecord.getInternalKeys(): " + type);
					}
				}
			
				return retval;
			}
			catch(Exception e)
			{
				HRDBMSWorker.logger.debug("IndexRecord with error has offset = " + off);
				throw e;
			}
		}
		
		public InternalIterator internalIterator(ArrayList<String> types)
		{
			return new InternalIterator(types, p.getInt(off+5));
		}
		
		private class InternalIterator implements Iterator
		{
			private ArrayList<String> types;
			private int numKeys;
			private int index = 0;
			
			public InternalIterator(ArrayList<String> types, int numKeys)
			{
				this.types = types;
				this.numKeys = numKeys;
			}
			
			@Override
			public boolean hasNext()
			{
				return index < numKeys;
			}

			@Override
			public Object next()
			{
				try
				{
					index++;
					return getInternalKeys(types);
				}
				catch(Exception e)
				{
					return null;
				}
			}

			@Override
			public void remove()
			{
			}
		}
	}
}
