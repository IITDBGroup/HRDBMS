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
	private final String fileName;

	private final ArrayList<String> keys;
	private final ArrayList<String> types;
	private final ArrayList<Boolean> orders;
	private Filter f;
	private ArrayList<Filter> secondary = new ArrayList<Filter>();
	private String in;
	private boolean positioned = false;
	private final ArrayListLong ridList = new ArrayListLong();
	private String col;
	private String op;
	private ArrayList<Filter> terminates = new ArrayList<Filter>();
	private IndexRecord line;
	// private MySimpleDateFormat sdf = new MySimpleDateFormat("yyyy-MM-dd");
	private final HashMap<String, Integer> cols2Pos = new HashMap<String, Integer>();
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
	
	private boolean isUnique() throws LockAbortException
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
			System.exit(1);
		}
		
		return p.get(4) == 1;
	}
	
	private boolean reallyViolatesUniqueConstraint(FieldValue[] keys) throws LockAbortException
	{
		IndexRecord line2 = line;
		while (line2.keysMatch(keys))
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
		this.setEqualsPosMulti(keys);
		if (line.keysMatch(keys))
		{
			if (isUnique() && reallyViolatesUniqueConstraint(keys))
			{
				throw new Exception("Violates unique constraint");
			}
			else
			{
				BlockAndOffset blockAndOffset = line.writeNewLeaf(keys,rid);
				line.insertAfter(blockAndOffset);
				return;
			}
		}
		line = line.getUp(true);
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
	
	public void update(FieldValue[] keys, RID oldRid, RID newRid) throws Exception
	{
		this.setEqualsPosMulti(keys);
		while (!line.getRid().equals(oldRid) || line.isTombstone())
		{
			line = line.nextRecord();
			
			if (!line.keysMatch(keys))
			{
				throw new Exception("Record could not be found for update");
			}
		}
		
		line.updateRid(newRid);
	}
	
	public void delete(FieldValue[] keys, RID rid) throws Exception
	{
		this.setEqualsPosMulti(keys);
		while (!line.getRid().equals(rid) || line.isTombstone())
		{
			line = line.nextRecord();
			
			if (!line.keysMatch(keys))
			{
				throw new Exception("Record could not be found for deletion");
			}
		}
		
		line.markTombstone();
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
	
	private final void setEqualsPosMulti(FieldValue[] vals) throws LockAbortException
	{
		line = new IndexRecord(fileName, offset, tx);
		
		while (!line.isLeaf())
		{
			int i = 0;
			Iterator it = line.internalIterator(types);
			while (it.hasNext())
			{
				ArrayList<Object> k = (ArrayList<Object>)it.next();
				//if (((Comparable)val).compareTo(key) < 1)
				if (compare(vals, k) < 1)
				{
					line = line.getDown(i);
					break;
				}
			}
		}
	}
	
	private final int compare(FieldValue[] vals, ArrayList<Object> rhs)
	{
		RowComparator rc = new RowComparator(orders, types);
		ArrayList<Object> lhs = new ArrayList<Object>(vals.length);
		for (FieldValue val : vals)
		{
			lhs.add(val.getValue());
		}
		
		return rc.compare(lhs,  rhs);
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

	public Object next() 
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
				setStartingPos();
			}
			catch(Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				System.exit(1);
			}
			positioned = true;
			iwt = new IndexWriterThread(keys);
			iwt.start();
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
		return o;

	}

	public void open(int device, MetaData meta)
	{
		try
		{
			in = meta.getDevicePath(device) + fileName;
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.error("", e);
			System.exit(1);
		}

		// if (delayed)
		// {
		// System.out.println("Index is opened delayed");
		// }
	}

	public synchronized void reset()
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
			System.exit(1);
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

	private Object calculateSecondaryStarting()
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
						System.exit(1);
					}
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.error("", e);
					System.exit(1);
				}
			}
			else
			{
				// setFirstPosition();
			}
		}

		return null;
	}

	private void calculateSecondaryTerminations()
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
						System.exit(1);
					}
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.error("", e);
					System.exit(1);
				}
			}
			else
			{
				// setFirstPosition();
			}
		}
	}

	private boolean currentKeySatisfies()
	{
		if (line.isNull())
		{
			return false;
		}
		
		if (line.isTombstone())
		{
			return false;
		}

		final int length = types.size();
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
			System.exit(1);
		}

		return false;
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

	private Object getObject(String val, String type)
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
				System.exit(1);
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

	private boolean marksEnd()
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
			System.exit(1);
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
	
	private final void setEqualsPos(Object val) throws LockAbortException
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
				if (((Comparable)val).compareTo(key) < 1)
				{
					line = line.getDown(i);
					break;
				}
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
	
	private final void setFirstPosition() throws LockAbortException
	{
		line = new IndexRecord(in, offset, tx);
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

	private final void setStartingPos() throws LockAbortException
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
					System.exit(1);
				}
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				System.exit(1);
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
			final FastStringTokenizer tokens = new FastStringTokenizer("", "|", false);
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
						System.exit(1);
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
						System.exit(1);
					}
					queue.put(row.clone());
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
		private int prevBlock, prevOff, nextBlock, nextOff, upBlock, upOff, node, device, blockNum, recNum;
		private boolean isLeaf = false;;
		private int keyOff = 9+128*8+8;
		private int numKeys;
		private boolean isTombstone = false;
		
		public IndexRecord(String file, long offset, Transaction tx) throws LockAbortException
		{
			this.file = file;
			this.tx = tx;
			b = new Block(file, (int)(offset / Page.BLOCK_SIZE));
			off = (int)(offset % Page.BLOCK_SIZE);
			LockManager.sLock(b, tx.number());
			tx.requestPage(b);
			try
			{
				p = tx.getPage(b);
			}
			catch(Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				System.exit(1);
			}
			
			isLeaf = (p.get(off+0) == 1);
			if (p.get(off+0) == 2)
			{
				isLeaf = true;
				isTombstone = true;
			}
			
			if (isLeaf)
			{
				prevBlock = p.getInt(off+1);
				prevOff = p.getInt(off+5);
				nextBlock = p.getInt(off+9);
				nextOff = p.getInt(off+13);
				upBlock = p.getInt(off+17);
				upOff = p.getInt(off+21);
				node = p.getInt(off+25);
				device = p.getInt(off+29);
				blockNum = p.getInt(off+33);
				recNum = p.getInt(off+37);
			}
			else
			{
				numKeys = p.getInt(off+5);
			}
		}
		
		public boolean containsRoom()
		{
			return numKeys < 128;
		}
		
		public boolean keysMatch(FieldValue[] keys)
		{
			ArrayList<Object> rhs = getKeys(types);
			ArrayList<Object> lhs = new ArrayList<Object>(keys.length);
			for (FieldValue val : keys)
			{
				lhs.add(val.getValue());
			}
			
			RowComparator rc = new RowComparator(orders, types);
			return rc.compare(lhs, rhs) == 0;
		}
		
		public RID getRid()
		{
			return new RID(node, device, blockNum, recNum);
		}
		
		public IndexRecord getDown(int num) throws LockAbortException
		{
			int block = p.getInt(off+9+num*8);
			int o = p.getInt(off+13+num*8);
			return new IndexRecord(file, block, o, tx);
		}
		
		public IndexRecord getDown(int num, boolean x) throws LockAbortException
		{
			if (!x)
			{
				return getDown(num);
			}
			
			int block = p.getInt(off+9+num*8);
			int o = p.getInt(off+13+num*8);
			return new IndexRecord(file, block, o, tx, true);
		}
		
		public void markTombstone()
		{
			byte[] before = new byte[1];
			byte[] after = new byte[1];
			before[0] = p.get(off+0);
			after[0] = (byte)2;
			DeleteLogRec rec = tx.delete(before, after, off+0, b);
			p.write(off+0, after, tx.number(), rec.lsn());
		}
		
		private int getPageFreeBytes()
		{
			int firstFree = p.getInt(5);
			int lastFree = p.getInt(9);
			return lastFree - firstFree + 1;
		}
		
		private byte[] genKeyBytes(FieldValue[] keys)
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
					size += val.length();
				}
				else
				{
					HRDBMSWorker.logger.error("Unknown type in IndexRecord.genKeyBytes(): " + type);
					System.exit(1);
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
					bb.putInt(val.length());
					try
					{
						bb.put(val.getBytes("UTF-8"));
					}
					catch(Exception e)
					{
						HRDBMSWorker.logger.error("", e);
						System.exit(1);
					}
				}
			}
			
			return bb.array();
		}
		
		public void addDown(FieldValue[] keys, BlockAndOffset bao) throws Exception
		{	
			ArrayList<Object> lhs = new ArrayList<Object>(keys.length);
			for (FieldValue val : keys)
			{
				lhs.add(val.getValue());
			}
			
			//generate key bytes
			byte[] keyBytes = genKeyBytes(keys);
			
			//can we just add this to the existing record?
			int freeBytes = line.getPageFreeBytes();
			
			if (keyBytes.length <= freeBytes)
			{
				//increment numKeys by 1
				byte[] before = new byte[4];
				p.get(off+5, before);
				ByteBuffer after = ByteBuffer.allocate(4);
				after.position(0);
				after.putInt(numKeys+1);
				InsertLogRec rec = tx.insert(before,  after.array(), off+5, b);
				p.write(off+5, after.array(), tx.number(), rec.lsn());
				
				//insert new keyBytes
				int offset = off+1033; //first key bytes
				int i = 0;
				while (i < numKeys)
				{
					ArrayList<Object> rhs = new ArrayList<Object>(types.size());
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
							System.exit(1);
						}
					}
					
					RowComparator rc = new RowComparator(orders, types);
					if (rc.compare(lhs, rhs) < 0)
					{
						//it goes here
						//figure out number of bytes to move from start
						int firstFree = p.getInt(off+5);
						int num2Move = firstFree - start;
						byte[] toMove = new byte[num2Move];
						byte[] bef = new byte[num2Move];
						p.get(start, toMove);
						p.get(start+keyBytes.length, bef);
						rec = tx.insert(bef, toMove, start+keyBytes.length, b);
						p.write(start+keyBytes.length, toMove, tx.number(), rec.lsn());
						bef = new byte[keyBytes.length];
						p.get(start, bef);
						rec = tx.insert(before, keyBytes, start, b);
						p.write(start, keyBytes, tx.number(), rec.lsn());
						break;
					}
					
					i++;
				}
				
				if (i == numKeys)
				{
					byte[] bef = new byte[keyBytes.length];
					p.get(offset, bef);
					rec = tx.insert(before, keyBytes, offset, b);
					p.write(offset, keyBytes, tx.number(), rec.lsn());
				}
				
				//update free space
				int firstFree = p.getInt(1);
				firstFree += keyBytes.length;
				byte[] bef = new byte[4];
				p.get(5, bef);
				ByteBuffer aft = ByteBuffer.allocate(4);
				aft.position(0);
				aft.putInt(firstFree);
				rec = tx.insert(bef, aft.array(), 5, b);
				p.write(5, aft.array(), tx.number(), rec.lsn());
				firstFree -= 13;
				p.get(14, bef);
				aft.position(0);
				aft.putInt(firstFree);
				rec = tx.insert(bef, aft.array(), 14, b);
				p.write(14, aft.array(), tx.number(), rec.lsn());
				
				//insert new down pointer
				if (i == numKeys)
				{
					int downOffset = off+9 + 8 * numKeys;
					bef = new byte[8];
					p.get(downOffset, bef);
					aft = ByteBuffer.allocate(8);
					aft.position(0);
					aft.putInt(bao.block());
					aft.putInt(bao.offset());
					rec = tx.insert(bef, aft.array(), downOffset, b);
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
					rec = tx.insert(before, a, downOffset+8, b);
					p.write(downOffset+8, a, tx.number(), rec.lsn());
					bef = new byte[8];
					p.get(downOffset, bef);
					aft = ByteBuffer.allocate(8);
					aft.position(0);
					aft.putInt(bao.block());
					aft.putInt(bao.offset());
					rec = tx.insert(before, aft.array(), downOffset, b);
					p.write(downOffset, aft.array(), tx.number(), rec.lsn());
				}
				
				//attach previous and next and up
				if (i == numKeys)
				{
					int pb = p.getInt(off+9 + (numKeys-1) * 8);
					int po = p.getInt(off+13 + (numKeys-1) * 8);
					IndexRecord pr = new IndexRecord(file, pb, po, tx, true);
					IndexRecord nr = pr.nextRecord(true);
					IndexRecord curr = new IndexRecord(file, bao.block(), bao.offset(), tx, true);
					pr.setNext(curr);
					curr.setPrev(pr);
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
				//no room to extend internal record with a new key, could be root record
				//TODO get length lock
				ByteBuffer bb = ByteBuffer.allocate(Page.BLOCK_SIZE);
				int i = 0;
				while (i < Page.BLOCK_SIZE)
				{
					bb.put((byte)2);
					i++;
				}
				
				FileManager.addNewBlock(file, bb);
				Block bl = new Block(file, (int)((new File(file)).length() / Page.BLOCK_SIZE - 1));
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
					System.exit(1);
				}
				
				//move current index record to start of new page
				int numBytes = p.getInt(5);
				byte[] bytes = new byte[numBytes];
				p.get(0, bytes);
				byte[] bef = new byte[numBytes];
				p2.get(0, bef);
				InsertLogRec rec = tx.insert(bef, bytes, 0, bl);
				p2.write(0, bytes, tx.number(), rec.lsn());
				bef = new byte[4];
				p2.get(9, bef);
				ByteBuffer aft = ByteBuffer.allocate(4);
				aft.position(0);
				aft.putInt(Page.BLOCK_SIZE-1);
				rec = tx.insert(bef, aft.array(), 9, bl);
				p2.write(9, aft.array(), tx.number(), rec.lsn());
				
				//redirect pointer from parent
				IndexRecord parent = this.getUp(true);
				IndexRecord newInternal = new IndexRecord(file, bl.number(), 13, tx, true);
				if (parent.isNull())
				{
					//this was root node that needed to be moved
					throw new Exception("Index key too large to fit root node in 1 page");
				}
				parent.replaceDownPointer(this.getBlockAndOffset(), newInternal.getBlockAndOffset());
				newInternal.addDown(keys, bao);
			}
		}
		
		private BlockAndOffset getBlockAndOffset()
		{
			return new BlockAndOffset(b.number(), off);
		}
		
		private void replaceDownPointer(BlockAndOffset oldPtr, BlockAndOffset newPtr)
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
					InsertLogRec rec = tx.insert(before, after.array(), o, b);
					p.write(o, after.array(), tx.number(), rec.lsn());
					return;
				}
				
				o += 8;
			}
		}
		
		public void setNext(BlockAndOffset bao)
		{
			byte[] before = new byte[8];
			p.get(off+9, before);
			ByteBuffer after = ByteBuffer.allocate(8);
			after.position(0);
			after.putInt(bao.block());
			after.putInt(bao.offset());
			after.position(0);
			InsertLogRec rec = tx.insert(before, after.array(), off+9, b);
			p.write(off+9, after.array(), tx.number(), rec.lsn());
		}
		
		public void setUp(BlockAndOffset bao)
		{
			byte[] before = new byte[8];
			p.get(off+17, before);
			ByteBuffer after = ByteBuffer.allocate(8);
			after.position(0);
			after.putInt(bao.block);
			after.putInt(bao.offset);
			after.position(0);
			InsertLogRec rec = tx.insert(before, after.array(), off+17, b);
			p.write(off+17, after.array(), tx.number(), rec.lsn());
		}
		
		public void setUp(IndexRecord ir)
		{
			int block = ir.blockNum;
			int o = ir.off;
			setUp(new BlockAndOffset(block, o));
		}
		
		public void setNext(IndexRecord ir)
		{
			int block = ir.blockNum;
			int o = ir.off;
			setNext(new BlockAndOffset(block, o));
		}
		
		public void split() throws Exception
		{
			//create a new internal record that has high half of keys and down pointers
			//TODO get length lock
			ByteBuffer bb = ByteBuffer.allocate(Page.BLOCK_SIZE);
			int i = 0;
			while (i < Page.BLOCK_SIZE)
			{
				bb.put((byte)2);
				i++;
			}
			
			FileManager.addNewBlock(file, bb);
			Block bl = new Block(file, (int)((new File(file)).length() / Page.BLOCK_SIZE - 1));
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
				System.exit(1);
			}
			
			int numBytes = p.getInt(5);
			byte[] bytes = new byte[numBytes];
			p.get(0, bytes);
			byte[] bef = new byte[numBytes];
			p2.get(0, bef);
			InsertLogRec rec = tx.insert(bef, bytes, 0, bl);
			p2.write(0, bytes, tx.number(), rec.lsn());
			
			//move down pointers
			byte[] after = new byte[64*8];
			p.get(off+9+64*8, after);
			byte[] before = new byte[64*8];
			p2.get(22, before);
			rec = tx.insert(before, after, 22, bl);
			p2.write(22, after, tx.number(), rec.lsn());
			Arrays.fill(before, (byte)0);
			rec = tx.insert(after, before, off+9+64*8, b);
			p.write(off+9+64*8, before, tx.number(), rec.lsn());
			p2.get(off+9+64*8, after);
			rec = tx.insert(after, before, off+9+64*8, bl);
			p2.write(off+9+64*8, before, tx.number(), rec.lsn());
			
			i = 0;
			int o = off+9+128*8+8;
			int p1HKS = -1;
			while (i < 64)
			{
				if (i == 63)
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
						System.exit(1);
					}
				}
				
				i++;
			}
			
			byte[] p1HighKey = new byte[o - p1HKS];
			p.get(p1HKS, p1HighKey);
			//we are now positioned at the start of the 64 keys that have to get moved from the current page to the new one
			int start = o;
			i = 0;
			int p2HKS = -1;
			while (i < 64)
			{
				if (i == 63)
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
						System.exit(1);
					}
				}
				
				i++;
			}
			
			byte[] p2HighKey = new byte[o-p2HKS];
			p.get(p2HKS, p2HighKey);
			int end = o;
			int length = end - start;
			byte[] oldPageKeys = new byte[length];
			p.get(start, oldPageKeys);
			byte[] blank = new byte[length];
			Arrays.fill(blank, (byte)0);
			rec = tx.insert(oldPageKeys, blank, start, b);
			p.write(start, blank, tx.number(), rec.lsn());
			byte[] newPage = new byte[length];
			p2.get(30+128*8, newPage);
			rec = tx.insert(newPage, oldPageKeys, 30+128*8, bl);
			p2.write(30+128*8, oldPageKeys, tx.number(), rec.lsn());
			
			int firstFreeByte = p.getInt(5);
			ByteBuffer b1 = ByteBuffer.allocate(4);
			b1.position(0);
			b1.putInt(firstFreeByte);
			firstFreeByte -= length;
			ByteBuffer a1 = ByteBuffer.allocate(4);
			a1.position(0);
			a1.putInt(firstFreeByte);
			rec = tx.insert(b1.array(), a1.array(), 5, b);
			p.write(5, a1.array(), tx.number(), rec.lsn());
			
			firstFreeByte = p.getInt(14);
			b1 = ByteBuffer.allocate(8);
			b1.position(0);
			b1.putInt(firstFreeByte);
			b1.putInt(128);
			firstFreeByte -= length;
			a1 = ByteBuffer.allocate(8);
			a1.position(0);
			a1.putInt(firstFreeByte);
			a1.putInt(64);
			rec = tx.insert(b1.array(), a1.array(), 14, b);
			p.write(14, a1.array(), tx.number(), rec.lsn());
			
			byte[] b1a = new byte[17];
			p2.get(5, b1a);
			a1 = ByteBuffer.allocate(17);
			a1.position(0);
			a1.putInt(30+128*8+length);
			a1.putInt(Page.BLOCK_SIZE-1);
			a1.put((byte)0);
			a1.putInt(17+128*8+length);
			a1.putInt(64);
			rec = tx.insert(b1a, a1.array(), 5, bl);
			p2.write(5, a1.array(), tx.number(), rec.lsn());
			
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
				
				FileManager.addNewBlock(file, bb);
				Block bl2 = new Block(file, (int)((new File(file)).length() / Page.BLOCK_SIZE - 1));
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
					System.exit(1);
				}
				
				firstFreeByte = p3.getInt(5);
				byte[] a1a = new byte[firstFreeByte];
				p.get(0, a1a);
				b1a = new byte[firstFreeByte];
				p3.get(0, b1a);
				rec = tx.insert(b1a, a1a, 0, bl2);
				p3.write(0, a1a, tx.number(), rec.lsn());
				b1a = new byte[4];
				p3.get(9, b1a);
				a1 = ByteBuffer.allocate(4);
				a1.position(0);
				a1.putInt(Page.BLOCK_SIZE-1);
				rec = tx.insert(b1a, a1.array(), 9, bl2);
				p3.write(9, a1.array(), tx.number(), rec.lsn());
				
				//write new root
				b1a = new byte[25+128*8 + p1HighKey.length + p2HighKey.length];
				p.get(5, b1a);
				a1 = ByteBuffer.allocate(25+128*8);
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
				rec = tx.insert(b1a, a1.array(), 5, b);
				p.write(5, a1.array(), tx.number(), rec.lsn());
			}
			else
			{
				IndexRecord up = getUp(true);
				up.replaceDownKey(this.getBlockAndOffset(), p1HighKey);
				up.addInternalDown(new BlockAndOffset(bl.number(), 13), p2HighKey);
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
					ArrayList<Object> newKey = new ArrayList<Object>(types.size());
					ByteBuffer bb = ByteBuffer.wrap(keyBytes);
					int o = 0;
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
								System.exit(1);
							}
						}
						else
						{
							HRDBMSWorker.logger.error("Unknown type in IndexRecord.addInternalDown(): " + type);
							System.exit(1);
						}
					}
					int i = 0;
					int numKeys = p.getInt(off+5);
					o = off+9+129*8;
					int goesHere = -1;
					while (i < numKeys)
					{
						ArrayList<Object> row = new ArrayList<Object>(types.size());
						goesHere = o;
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
									System.exit(1);
								}
							}
							else
							{
								HRDBMSWorker.logger.error("Unknown type in IndexRecord.addInternalDown(): " + type);
								System.exit(1);
							}
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
					
					//key goes in position i
					int firstFree = p.getInt(5);
					ByteBuffer bef = ByteBuffer.allocate(4);
					bef.position(0);
					bef.putInt(firstFree);
					ByteBuffer aft = ByteBuffer.allocate(4);
					aft.position(0);
					aft.putInt(firstFree+keyBytes.length);
					InsertLogRec rec = tx.insert(bef.array(), aft.array(), 5, b);
					p.write(5, aft.array(), tx.number(), rec.lsn());
					
					byte[] ba = new byte[8];
					p.get(14, ba);
					aft = ByteBuffer.allocate(8);
					aft.position(0);
					aft.putInt(firstFree+keyBytes.length-13);
					aft.putInt(p.getInt(18)+1);
					
					int toMove = 128*8 - (i+1)*128;
					if (toMove > 0)
					{
						ba = new byte[toMove];
						p.get(off+9+(i+1)*8, ba);
						byte[] aa = new byte[toMove];
						p.get(off+9+i*8, aa);
						rec = tx.insert(ba, aa, off+9+(i+1)*8, b);
						p.write(off+9+(i+1)*8, aa, tx.number(), rec.lsn());
					}
						
					ba = new byte[8];
					p.get(off+9+i*8, ba);
					aft = ByteBuffer.allocate(8);
					aft.position(0);
					aft.putInt(bao.block());
					aft.putInt(bao.offset());
					rec = tx.insert(ba, aft.array(), off+9+i*8, b);
					p.write(off+9+i*8, aft.array(), tx.number(), rec.lsn());
					
					if (i == numKeys)
					{
						//nothing to move
						byte[] before = new byte[keyBytes.length];
						p.get(goesHere, before);
						rec = tx.insert(before, keyBytes, goesHere, b);
						p.write(goesHere, keyBytes, tx.number(), rec.lsn());
					}
					else
					{
						int j = i;
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
									System.exit(1);
								}
								
							}
						}
						
						//move data from goesHere <-> o by keyBytes.length bytes
						toMove = o - goesHere;
						byte[] before = new byte[toMove];
						p.get(goesHere + keyBytes.length, before);
						byte[] after = new byte[toMove];
						p.get(goesHere, after);
						rec = tx.insert(before, after, goesHere+keyBytes.length, b);
						p.write(goesHere+keyBytes.length, after, tx.number(), rec.lsn());
						
						before = new byte[keyBytes.length];
						p.get(goesHere, before);
						rec = tx.insert(before, keyBytes, goesHere, b);
						p.write(goesHere, keyBytes, tx.number(), rec.lsn());
					}
				}
				else
				{
					//check if this is the root, if so - exception
					if (b.number() == 0)
					{
						throw new Exception("Index key is too large - root node does not fit in a page");
					}
					
					//otherwise move record and then update in place
					//TODO get length lock
					ByteBuffer bb = ByteBuffer.allocate(Page.BLOCK_SIZE);
					int i = 0;
					while (i < Page.BLOCK_SIZE)
					{
						bb.put((byte)2);
						i++;
					}
					
					FileManager.addNewBlock(file, bb);
					Block bl = new Block(file, (int)((new File(file)).length() / Page.BLOCK_SIZE - 1));
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
						System.exit(1);
					}
					
					int length = p.getInt(5);
					byte[] before = new byte[length];
					p2.get(0, before);
					byte[] after = new byte[length];
					p.get(0, after);
					InsertLogRec rec = tx.insert(before, after, 0, bl);
					p2.write(0, after, tx.number(), rec.lsn());
					
					before = new byte[4];
					p2.get(9, before);
					ByteBuffer aft = ByteBuffer.allocate(4);
					aft.position(0);
					aft.putInt(Page.BLOCK_SIZE-1);
					rec = tx.insert(before, aft.array(), 9, bl);
					p2.write(9, aft.array(), tx.number(), rec.lsn());
					IndexRecord parent = getUp(true);
					parent.replaceDownPointer(this.getBlockAndOffset(), new BlockAndOffset(bl.number(), 13));
					IndexRecord moved = new IndexRecord(file, bl.number(), 13, tx, true);
					moved.addInternalDown(bao, keyBytes);
				}
			}
			else
			{
				//if this is the root, figure out what left and right high keys will be
				if (b.number() == 0)
				{
					byte[] down = this.getKeyBytesByIndex(63);
					ArrayList<Object> key = convertBytesToRow(keyBytes);
					ArrayList<Object> downRow = convertBytesToRow(down);
					
					RowComparator rc = new RowComparator(orders, types);
					if (rc.compare(key, downRow) > 0)
					{
						down = this.getKeyBytesByIndex(127);
					}
						
					//call split
					split();
					
					//get correct one of 2 new internal records resulting from split
					IndexRecord root = new IndexRecord(file, 0, 13, tx, true);
					IndexRecord correct = root.getDownMatchingKey(down);
					
					//call addInternalDown on that one
					correct.addInternalDown(bao, keyBytes);
				}
				else
				{
					//if this is not the root
					//call split
					split();
					//after split the left node will be the same as the original
					//the right node will be the next down pointer that the parent has
					//figure out which one this down ptr goes into
					//call addInternalDown on that one
					IndexRecord left = new IndexRecord(file, b.number(), 13, tx, true);
					byte[] down = left.getKeyBytesByIndex(63);
					ArrayList<Object> key = convertBytesToRow(keyBytes);
					ArrayList<Object> downRow = convertBytesToRow(down);
					
					RowComparator rc = new RowComparator(orders, types);
					if (rc.compare(key, downRow) < 0)
					{
						left.addInternalDown(bao, keyBytes);
					}
					else
					{
						IndexRecord parent = left.getUp(true);
						IndexRecord right = parent.getRecordAfter(left.getBlockAndOffset());
						right.addInternalDown(bao, keyBytes);
					}
				}
			}
		}
		
		private IndexRecord getDownMatchingKey(byte[] keyBytes) throws LockAbortException
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
						System.exit(1);
					}
				}
				
				int length = o - start;
				if (length == keyBytes.length)
				{
					byte[] lhs = new byte[length];
					p.get(start, lhs);
					if (lhs.equals(keyBytes))
					{
						//found the match
						return this.getDown(i, true);
					}
				}
				
				i++;
			}
		}
		
		private IndexRecord getRecordAfter(BlockAndOffset bao) throws LockAbortException
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
		
		private ArrayList<Object> convertBytesToRow(byte[] keyBytes)
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
					int length = bb.getInt(o);
					o += 4;
					byte[] bytes = new byte[length];
					bb.get(bytes, o, length);
					o += length;
					try
					{
						retval.add(new String(bytes, "UTF-8"));
					}
					catch(Exception e)
					{
						HRDBMSWorker.logger.error("", e);
						System.exit(1);
					}
				}
			}
			
			return retval;
		}
		
		private byte[] getKeyBytesByIndex(int index)
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
						System.exit(1);
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
					System.exit(1);
				}
			}
			
			byte[] retval = new byte[o-start];
			p.get(start, retval);
			return retval;
		}
		
		private void replaceDownKey(BlockAndOffset bao, byte[] keyBytes) throws IOException, LockAbortException
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
						System.exit(1);
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
				InsertLogRec rec = tx.insert(before, keyBytes, start, b);
				p.write(start, keyBytes, tx.number(), rec.lsn());
			}
			else if (keyBytes.length < currentLength)
			{
				//put new keyBytes in place
				byte[] before = new byte[keyBytes.length];
				p.get(start, before);
				InsertLogRec rec = tx.insert(before, keyBytes, start, b);
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
							System.exit(1);
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
					rec = tx.insert(before, after, start-moveForward, b);
					p.write(start-moveForward, after, tx.number(), rec.lsn());
				}
					
				int firstFree = p.getInt(5);
				ByteBuffer bef = ByteBuffer.allocate(4);
				bef.position(0);
				bef.putInt(firstFree);
				ByteBuffer a = ByteBuffer.allocate(4);
				a.position(0);
				a.putInt(firstFree-moveForward);
				rec = tx.insert(bef.array(), a.array(), 5, b);
				p.write(5, a.array(), tx.number(), rec.lsn());
				bef.position(0);
				bef.putInt(firstFree-13);
				a.position(0);
				a.putInt(firstFree-moveForward-13);
				rec = tx.insert(bef.array(), a.array(), 14, b);
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
								System.exit(1);
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
						InsertLogRec rec = tx.insert(before, after, start2+expand, b);
						p.write(start2+expand, after, tx.number(), rec.lsn());
					}
					
					//put new keyBytes in place
					byte[] before = new byte[keyBytes.length];
					p.get(start, before);
					InsertLogRec rec = tx.insert(before, keyBytes, start, b);
					p.write(start, keyBytes, tx.number(), rec.lsn());
						
					int firstFree = p.getInt(5);
					ByteBuffer bef = ByteBuffer.allocate(4);
					bef.position(0);
					bef.putInt(firstFree);
					ByteBuffer a = ByteBuffer.allocate(4);
					a.position(0);
					a.putInt(firstFree+expand);
					rec = tx.insert(bef.array(), a.array(), 5, b);
					p.write(5, a.array(), tx.number(), rec.lsn());
					bef.position(0);
					bef.putInt(firstFree-13);
					a.position(0);
					a.putInt(firstFree+expand-13);
					rec = tx.insert(bef.array(), a.array(), 14, b);
					p.write(14, a.array(), tx.number(), rec.lsn());
				}
				else
				{
					//we need to move this internal node to a new page, update the parent down pointer, and recall method
					//TODO get length lock
					ByteBuffer bb = ByteBuffer.allocate(Page.BLOCK_SIZE);
					i = 0;
					while (i < Page.BLOCK_SIZE)
					{
						bb.put((byte)2);
						i++;
					}
					
					FileManager.addNewBlock(file, bb);
					Block bl = new Block(file, (int)((new File(file)).length() / Page.BLOCK_SIZE - 1));
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
						System.exit(1);
					}
					
					int length = p.getInt(5);
					byte[] before = new byte[length];
					p2.get(0, before);
					byte[] after = new byte[length];
					p.get(0, after);
					InsertLogRec rec = tx.insert(before, after, 0, bl);
					p2.write(0, after, tx.number(), rec.lsn());
					
					before = new byte[4];
					p2.get(9, before);
					ByteBuffer a = ByteBuffer.allocate(4);
					a.position(0);
					a.putInt(Page.BLOCK_SIZE-1);
					rec = tx.insert(before, a.array(), 9, bl);
					p2.write(9, a.array(), tx.number(), rec.lsn());
					
					IndexRecord parent = getUp(true);
					parent.replaceDownPointer(this.getBlockAndOffset(), new BlockAndOffset(bl.number(), 13));
					parent.replaceDownKey(bao, keyBytes);
				}
			}
		}
		
		public BlockAndOffset writeNewLeaf(FieldValue[] keys, RID rid) throws IOException, LockAbortException
		{
			int firstFree = p.getInt(5);
			int lastFree = p.getInt(9);
			byte[] keyBytes = this.genKeyBytes(keys);
			int length = 41 + keyBytes.length;
			int freeLength = lastFree - firstFree + 1;
			
			if (length <= freeLength && b.number() != 0)
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
				InsertLogRec rec = tx.insert(before, after.array(), leafOff, b);
				p.write(leafOff, after.array(), tx.number(), rec.lsn());
				return new BlockAndOffset(b.number(), leafOff);
			}
			
			//TODO obtain length lock
			int i = 1;
			int numBlocks = (int)((new File(file)).length() / Page.BLOCK_SIZE);
			while (i < numBlocks)
			{
				Block bl = new Block(file, (int)((new File(file)).length() / Page.BLOCK_SIZE - 1));
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
					System.exit(1);
				}
				
				firstFree = p2.getInt(5);
				lastFree = p2.getInt(9);
				freeLength = lastFree - firstFree + 1;
				
				if (length <= freeLength)
				{
					//do it on this page
					IndexRecord dummy = new IndexRecord(file, bl.number(), 13, tx, true);
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
			
			FileManager.addNewBlock(file, bb);
			Block bl = new Block(file, (int)((new File(file)).length() / Page.BLOCK_SIZE - 1));
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
				System.exit(1);
			}
			
			//copy first 5 bytes
			byte[] bytes = new byte[5];
			p.get(0, bytes);
			byte[] bef = new byte[5];
			p2.get(0, bef);
			InsertLogRec rec = tx.insert(bef, bytes, 0, bl);
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
			
			IndexRecord dummy = new IndexRecord(file, bl.number(), 13, tx, true);
			return dummy.writeNewLeaf(keys, rid);
		}
		
		public void setPrev(IndexRecord ir)
		{
			int block = ir.blockNum;
			int o = ir.off;
			setPrev(new BlockAndOffset(block, o));
		}
		
		public void setPrev(BlockAndOffset bao)
		{
			byte[] before = new byte[8];
			p.get(off+1, before);
			ByteBuffer after = ByteBuffer.allocate(8);
			after.position(0);
			after.putInt(bao.block());
			after.putInt(bao.offset());
			after.position(0);
			InsertLogRec rec = tx.insert(before, after.array(), off+1, b);
			p.write(off+1, after.array(), tx.number(), rec.lsn());
		}
		
		private void insertAfter(BlockAndOffset bao) throws LockAbortException
		{
			IndexRecord next = new IndexRecord(file, nextBlock, nextOff, tx, true);
			this.setNext(bao);
			if (!next.isNull())
			{
				next.setPrev(bao);
			}
		}
		
		public void updateRid(RID newRid)
		{
			byte[] before = new byte[16];
			ByteBuffer after = ByteBuffer.allocate(16);
			p.get(off+25, before);
			after.position(0);
			after.putInt(newRid.getNode());
			after.putInt(newRid.getDevice());
			after.putInt(newRid.getBlockNum());
			after.putInt(newRid.getRecNum());
			after.position(0);
			InsertLogRec rec = tx.insert(before, after.array(), off+25, b);
			p.write(off+25, after.array(), tx.number(), rec.lsn());
		}
		
		public IndexRecord(String file, int block, int offset, Transaction tx) throws LockAbortException
		{
			this.file = file;
			this.tx = tx;
			b = new Block(file, block);
			off = offset;
			LockManager.sLock(b, tx.number());
			tx.requestPage(b);
			try
			{
				p = tx.getPage(b);
			}
			catch(Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				System.exit(1);
			}
			
			isLeaf = (p.get(off+0) == 1);
			if (p.get(off+0) == 2)
			{
				isLeaf = true;
				isTombstone = true;
			}
			
			if (isLeaf)
			{
				prevBlock = p.getInt(off+1);
				prevOff = p.getInt(off+5);
				nextBlock = p.getInt(off+9);
				nextOff = p.getInt(off+13);
				upBlock = p.getInt(off+17);
				upOff = p.getInt(off+21);
				node = p.getInt(off+25);
				device = p.getInt(off+29);
				blockNum = p.getInt(off+33);
				recNum = p.getInt(off+37);
			}
			else
			{
				numKeys = p.getInt(off+5);
			}
		}
		
		public IndexRecord(String file, int block, int offset, Transaction tx, boolean x) throws LockAbortException
		{
			this.file = file;
			this.tx = tx;
			b = new Block(file, block);
			off = offset;
			LockManager.xLock(b, tx.number());
			tx.requestPage(b);
			try
			{
				p = tx.getPage(b);
			}
			catch(Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				System.exit(1);
			}
			
			isLeaf = (p.get(off+0) == 1);
			if (p.get(off+0) == 2)
			{
				isLeaf = true;
				isTombstone = true;
			}
			
			if (isLeaf)
			{
				prevBlock = p.getInt(off+1);
				prevOff = p.getInt(off+5);
				nextBlock = p.getInt(off+9);
				nextOff = p.getInt(off+13);
				upBlock = p.getInt(off+17);
				upOff = p.getInt(off+21);
				node = p.getInt(off+25);
				device = p.getInt(off+29);
				blockNum = p.getInt(off+33);
				recNum = p.getInt(off+37);
			}
			else
			{
				numKeys = p.getInt(off+5);
			}
		}
		
		private IndexRecord()
		{}
		
		public ArrayList<Object> getKeys(ArrayList<String> types)
		{
			int o = off+41;
			ArrayList<Object> retval = new ArrayList<Object>(types.size());
			//TODO null
			for (String type : types)
			{
				o++; // skip null indicator
				if (type.equals("INT"))
				{
					retval.add(p.getInt(o));
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
						retval.add(new String(buff, "UTF-8"));
					}
					catch(Exception e)
					{
						HRDBMSWorker.logger.error("", e);
						System.exit(1);
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
					System.exit(1);
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
			return (((long)blockNum) << 32) + recNum;
		}
		
		public IndexRecord nextRecord() throws LockAbortException
		{
			if (nextBlock == 0 && nextOff == 0)
			{
				IndexRecord retval = new IndexRecord();
				retval.isNull = true;
				return retval;
			}
			
			return new IndexRecord(file, nextBlock, nextOff, tx);
		}
		
		public IndexRecord prevRecord() throws LockAbortException
		{
			if (prevBlock == 0 && prevOff == 0)
			{
				IndexRecord retval = new IndexRecord();
				retval.isNull = true;
				return retval;
			}
			
			return new IndexRecord(file, prevBlock, prevOff, tx);
		}
		
		public IndexRecord prevRecord(boolean x) throws LockAbortException
		{
			if (!x)
			{
				return prevRecord();
			}
			
			if (prevBlock == 0 && prevOff == 0)
			{
				IndexRecord retval = new IndexRecord();
				retval.isNull = true;
				return retval;
			}
			
			return new IndexRecord(file, prevBlock, prevOff, tx, true);
		}
		
		public IndexRecord nextRecord(boolean x) throws LockAbortException
		{
			if (!x)
			{
				return nextRecord();
			}
			
			if (nextBlock == 0 && nextOff == 0)
			{
				IndexRecord retval = new IndexRecord();
				retval.isNull = true;
				return retval;
			}
			
			return new IndexRecord(file, nextBlock, nextOff, tx, true);
		}
		
		public IndexRecord getUp() throws LockAbortException
		{
			if (upBlock == 0 && upOff == 0)
			{
				IndexRecord retval = new IndexRecord();
				retval.isNull = true;
				return retval;
			}
			
			return new IndexRecord(file, upBlock, upOff, tx);
		}
		
		public IndexRecord getUp(boolean x) throws LockAbortException
		{
			if (upBlock == 0 && upOff == 0)
			{
				IndexRecord retval = new IndexRecord();
				retval.isNull = true;
				return retval;
			}
			
			if (x)
			{
				return new IndexRecord(file, upBlock, upOff, tx, true);
			}
			else
			{
				return new IndexRecord(file, upBlock, upOff, tx);
			}
		}
		
		public boolean isLeaf()
		{
			return isLeaf;
		}
		
		private ArrayList<Object> getInternalKeys(ArrayList<String> types)
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
					try
					{
						retval.add(new String(buff, "UTF-8"));
					}
					catch(Exception e)
					{
						HRDBMSWorker.logger.error("", e);
						System.exit(1);
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
					System.exit(1);
				}
			}
			
			return retval;
		}
		
		public InternalIterator internalIterator(ArrayList<String> types)
		{
			return new InternalIterator(types, numKeys);
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
				return getInternalKeys(types);
			}

			@Override
			public void remove()
			{
			}
		}
	}
}
