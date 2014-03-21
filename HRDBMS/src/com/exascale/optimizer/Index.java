package com.exascale.optimizer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.locks.LockSupport;
import com.exascale.filesystem.Block;
import com.exascale.filesystem.Page;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.managers.LockManager;
import com.exascale.managers.ResourceManager;
import com.exascale.misc.ArrayListLong;
import com.exascale.misc.BufferedLinkedBlockingQueue;
import com.exascale.misc.DataEndMarker;
import com.exascale.misc.DateParser;
import com.exascale.misc.FastStringTokenizer;
import com.exascale.misc.MyDate;
import com.exascale.misc.Utils;
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
			setStartingPos();
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
	
	private final void setEqualsPos(Object val)
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
	
	private final void setFirstPosition()
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

	private final void setStartingPos()
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
		private boolean isLeaf;
		private int keyOff = 9+128*8+8;
		private int numKeys;
		
		public IndexRecord(String file, long offset, Transaction tx)
		{
			this.file = file;
			this.tx = tx;
			b = new Block(file, (int)(offset / Page.BLOCK_SIZE));
			off = (int)(offset % Page.BLOCK_SIZE);
			if (tx.level == Transaction.ISOLATION_RR || tx.level == Transaction.ISOLATION_CS)
			{
				try
				{
					LockManager.sLock(b, tx.number());
				}
				catch(Exception e)
				{
					HRDBMSWorker.logger.error("", e);
					System.exit(1);
				}
			}
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
			
			if (tx.level == Transaction.ISOLATION_CS)
			{
				LockManager.unlockSLock(b, tx.number());
			}
			
			isLeaf = (p.get(off+0) == 1);
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
		
		public IndexRecord getDown(int num)
		{
			int block = p.getInt(off+9+num*8);
			int o = p.getInt(off+13+num*8);
			return new IndexRecord(file, block, o, tx);
		}
		
		public IndexRecord(String file, int block, int offset, Transaction tx)
		{
			this.file = file;
			this.tx = tx;
			b = new Block(file, block);
			off = offset;
			if (tx.level == Transaction.ISOLATION_RR || tx.level == Transaction.ISOLATION_CS)
			{
				try
				{
					LockManager.sLock(b, tx.number());
				}
				catch(Exception e)
				{
					HRDBMSWorker.logger.error("", e);
					System.exit(1);
				}
			}
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
			
			if (tx.level == Transaction.ISOLATION_CS)
			{
				LockManager.unlockSLock(b, tx.number());
			}
			
			isLeaf = (p.get(off+0) == 1);
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
		
		public long getPartialRid()
		{
			return (((long)blockNum) << 32) + recNum;
		}
		
		public IndexRecord nextRecord()
		{
			if (nextBlock == 0 && nextOff == 0)
			{
				IndexRecord retval = new IndexRecord();
				retval.isNull = true;
				return retval;
			}
			
			return new IndexRecord(file, nextBlock, nextOff, tx);
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
