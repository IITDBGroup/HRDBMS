package com.exascale.optimizer;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.locks.LockSupport;
import com.exascale.filesystem.Block;
import com.exascale.filesystem.Page;
import com.exascale.filesystem.RID;
import com.exascale.logging.InsertLogRec;
import com.exascale.managers.FileManager;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.managers.LockManager;
import com.exascale.managers.LogManager;
import com.exascale.managers.ResourceManager;
import com.exascale.misc.ArrayListLong;
import com.exascale.misc.BufferedLinkedBlockingQueue;
import com.exascale.misc.DataEndMarker;
import com.exascale.misc.MyDate;
import com.exascale.misc.RowComparator;
import com.exascale.tables.Schema;
import com.exascale.tables.Schema.FieldValue;
import com.exascale.tables.Transaction;
import com.exascale.threads.ThreadPoolThread;

public final class Index implements Serializable
{
	private static Charset cs = StandardCharsets.UTF_8;

	private static long soffset;
	private static sun.misc.Unsafe unsafe;
	private static int PREFETCH_REQUEST_SIZE;
	private static int PAGES_IN_ADVANCE;

	static
	{
		try
		{
			final Field f = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
			f.setAccessible(true);
			unsafe = (sun.misc.Unsafe)f.get(null);
			final Field fieldToUpdate = String.class.getDeclaredField("value");
			// get unsafe offset to this field
			soffset = unsafe.objectFieldOffset(fieldToUpdate);
			PREFETCH_REQUEST_SIZE = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("prefetch_request_size")); // 80
			PAGES_IN_ADVANCE = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("pages_in_advance")); // 40
		}
		catch (Exception e)
		{
			unsafe = null;
		}
	}
	private static final double promoRate = 1.0d / Math.exp(1.0d);
	private String fileName;
	private ArrayList<String> keys;
	private ArrayList<String> types;
	private ArrayList<Boolean> orders;
	private Filter f;
	private ArrayList<Filter> secondary = new ArrayList<Filter>();
	private boolean positioned = false;
	private transient ArrayListLong ridList;
	private transient String col;
	private transient String op;
	private transient ArrayList<Filter> terminates;
	private transient IndexRecord line;
	// private MySimpleDateFormat sdf = new MySimpleDateFormat("yyyy-MM-dd");
	private HashMap<String, Integer> cols2Pos = new HashMap<String, Integer>();
	private boolean indexOnly = false;
	private ArrayList<Integer> fetches = new ArrayList<Integer>();
	private ArrayList<String> fetchTypes = new ArrayList<String>();
	private transient ArrayList<Object> row;
	private volatile boolean delayed = false;
	private ArrayList<Filter> delayedConditions;
	private HashMap<String, String> renames;
	private transient BufferedLinkedBlockingQueue queue;
	private transient IndexWriterThread iwt;
	private transient volatile Thread lastThread = null;
	private long offset = 9;
	private Transaction tx;
	private volatile Boolean isUniqueVar = null;
	public HashMap<Block, Page> myPages = new HashMap<Block, Page>();
	private CharsetDecoder cd = cs.newDecoder();
	private CharsetEncoder ce = cs.newEncoder();
	private transient Random random;
	private transient byte[] typesBytes;

	private RowComparator rc;

	public HashMap<Long, ArrayList<Object>> cache = new HashMap<Long, ArrayList<Object>>();

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

		rc = new RowComparator(orders, types);
	}

	public static Index deserialize(InputStream in, HashMap<Long, Object> prev) throws Exception
	{
		Index value = (Index)unsafe.allocateInstance(Index.class);
		int type = OperatorUtils.getType(in);
		if (type == 0)
		{
			return (Index)OperatorUtils.readReference(in, prev);
		}

		if (type != 64)
		{
			throw new Exception("Corrupted stream. Expected type 64 but received " + type);
		}

		prev.put(OperatorUtils.readLong(in), value);
		value.fileName = OperatorUtils.readString(in, prev);
		value.keys = OperatorUtils.deserializeALS(in, prev);
		value.types = OperatorUtils.deserializeALS(in, prev);
		value.orders = OperatorUtils.deserializeALB(in, prev);
		value.f = OperatorUtils.deserializeFilter(in, prev);
		value.secondary = OperatorUtils.deserializeALF(in, prev);
		value.positioned = OperatorUtils.readBool(in);
		value.cols2Pos = OperatorUtils.deserializeStringIntHM(in, prev);
		value.indexOnly = OperatorUtils.readBool(in);
		value.fetches = OperatorUtils.deserializeALI(in, prev);
		value.fetchTypes = OperatorUtils.deserializeALS(in, prev);
		value.delayed = OperatorUtils.readBool(in);
		value.delayedConditions = OperatorUtils.deserializeALF(in, prev);
		value.renames = OperatorUtils.deserializeStringHM(in, prev);
		value.offset = OperatorUtils.readLong(in);
		value.tx = new Transaction(OperatorUtils.readLong(in));
		value.isUniqueVar = OperatorUtils.readBoolClass(in, prev);
		value.myPages = new HashMap<Block, Page>();
		value.rc = new RowComparator(value.orders, value.types);
		value.cache = new HashMap<Long, ArrayList<Object>>();
		value.cd = cs.newDecoder();
		value.ce = cs.newEncoder();
		return value;
	}

	public static Index deserializeKnown(InputStream in, HashMap<Long, Object> prev) throws Exception
	{
		Index value = (Index)unsafe.allocateInstance(Index.class);
		prev.put(OperatorUtils.readLong(in), value);
		value.fileName = OperatorUtils.readString(in, prev);
		value.keys = OperatorUtils.deserializeALS(in, prev);
		value.types = OperatorUtils.deserializeALS(in, prev);
		value.orders = OperatorUtils.deserializeALB(in, prev);
		value.f = OperatorUtils.deserializeFilter(in, prev);
		value.secondary = OperatorUtils.deserializeALF(in, prev);
		value.positioned = OperatorUtils.readBool(in);
		value.cols2Pos = OperatorUtils.deserializeStringIntHM(in, prev);
		value.indexOnly = OperatorUtils.readBool(in);
		value.fetches = OperatorUtils.deserializeALI(in, prev);
		value.fetchTypes = OperatorUtils.deserializeALS(in, prev);
		value.delayed = OperatorUtils.readBool(in);
		value.delayedConditions = OperatorUtils.deserializeALF(in, prev);
		value.renames = OperatorUtils.deserializeStringHM(in, prev);
		value.offset = OperatorUtils.readLong(in);
		value.tx = new Transaction(OperatorUtils.readLong(in));
		value.isUniqueVar = OperatorUtils.readBoolClass(in, prev);
		value.myPages = new HashMap<Block, Page>();
		value.rc = new RowComparator(value.orders, value.types);
		value.cache = new HashMap<Long, ArrayList<Object>>();
		value.cd = cs.newDecoder();
		value.ce = cs.newEncoder();
		return value;
	}

	// public HashSet<Block> changedBlocks = new HashSet<Block>();
	// private static long MAX_PAGES;

	// static
	// {
	// MAX_PAGES =
	// (Long.parseLong(HRDBMSWorker.getHParms().getProperty("bp_pages")) /
	// MetaData.getNumDevices()) / 15;
	// }

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

		retval.tx = tx;
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
		terminates = null;
		row = null;
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

	public void delete(ArrayList<Object> keys, RID rid) throws Exception
	{
		this.setEqualsPosMulti(keys, true);
		IndexRecord rec = line;
		while (!rec.isNull())
		{
			if (rec.ridsMatch(rid))
			{
				rec.markTombstone();
				return;
			}

			rec = rec.next(true);
		}

		throw new Exception("Unable to locate record for deletion");
	}

	public void delete(FieldValue[] keys, RID rid) throws Exception
	{
		delete(fvaToAlo(keys), rid);
	}

	@Override
	public boolean equals(Object obj)
	{
		if (obj == null || !(obj instanceof Index))
		{
			return false;
		}

		Index rhs = (Index)obj;
		boolean retval = keys.equals(rhs.keys) && types.equals(rhs.types) && orders.equals(rhs.orders);
		if (!retval)
		{
			return false;
		}

		if (!fileName.startsWith("/") && !rhs.fileName.startsWith("/"))
		{
			return fileName.equals(rhs.fileName);
		}

		if (fileName.startsWith("/") && rhs.fileName.startsWith("/"))
		{
			return fileName.equals(rhs.fileName);
		}

		String l = null;
		String r = null;
		if (fileName.contains("/"))
		{
			l = fileName.substring(fileName.lastIndexOf('/') + 1);
		}
		else
		{
			l = fileName;
		}

		if (rhs.fileName.contains("/"))
		{
			r = rhs.fileName.substring(rhs.fileName.lastIndexOf('/') + 1);
		}
		else
		{
			r = rhs.fileName;
		}

		return l.equals(r);
	}

	public ArrayList<Object> fvaToAlo(FieldValue[] fva)
	{
		ArrayList<Object> retval = new ArrayList<Object>(fva.length);
		for (FieldValue fv : fva)
		{
			retval.add(fv.getValue());
		}

		return retval;
	}

	public IndexRecord get(ArrayList<Object> keys) throws Exception
	{
		setEqualsPosMulti(keys);
		IndexRecord rec = line;
		if (rec.keysMatch(keys))
		{
			return rec;
		}
		else
		{
			return null;
		}
	}

	public IndexRecord get(FieldValue[] keys) throws Exception
	{
		return get(fvaToAlo(keys));
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

	public ArrayList<String> getKeys()
	{
		return keys;
	}

	public ArrayList<Boolean> getOrders()
	{
		return orders;
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

	public ArrayList<String> getTypes()
	{
		return types;
	}

	public void insert(ArrayList<Object> keys, RID rid) throws Exception
	{
		setEqualsPosMulti(keys, true);
		IndexRecord rec = line;
		int cmp = rec.compareTo(keys);
		if (!rec.isTombstone() && cmp == 0)
		{
			if (isUnique())
			{
				throw new Exception("Unique constraint violation");
			}
		}

		IndexRecord newRec = writeNewLeaf(keys, rid);
		if (cmp >= 0)
		{
			// put to the left of rec
			IndexRecord left = rec.prev(true);
			left.setNext(newRec);
			rec.setPrev(newRec);
			newRec.setPrev(left);
			newRec.setNext(rec);
		}
		else
		{
			// put to the right of rec @ end of list
			rec.setNext(newRec);
			newRec.setPrev(rec);
		}

		while (random.nextDouble() < promoRate)
		{
			newRec = promote(newRec);
		}
	}

	public void insert(FieldValue[] keys, RID rid) throws Exception
	{
		ArrayList<Object> alo = fvaToAlo(keys);
		insert(alo, rid);
	}

	public void insertNoLog(ArrayList<Object> keys, RID rid) throws Exception
	{
		// DEBUG
		// if (keys.size() == 0)
		// {
		// Exception e = new Exception("Empty key inserted into index");
		// HRDBMSWorker.logger.debug("Types has size " + types.size());
		// HRDBMSWorker.logger.debug("", e);
		// throw e;
		// }
		// DEBUG

		// for (Object o : keys)
		// {
		// if (o instanceof Integer && (Integer)o < 0)
		// {
		// HRDBMSWorker.logger.debug("Negative integer when writing to index");
		// }
		// else if (o instanceof Long && (Long)o < 0)
		// {
		// HRDBMSWorker.logger.debug("Negative long when writing to index");
		// }
		// }

		setEqualsPosMulti(keys, true);
		IndexRecord rec = line;
		int cmp = rec.compareTo(keys);
		if (!rec.isTombstone() && cmp == 0)
		{
			if (isUnique())
			{
				HRDBMSWorker.logger.debug("Unique constraint violation");
				rec.compareTo(keys, true);
				HRDBMSWorker.logger.debug("New RID = " + rid);
				HRDBMSWorker.logger.debug("Old RID = " + rec.getRid());
				throw new Exception("Unique constraint violation");
			}
		}

		IndexRecord newRec = writeNewLeafNoLog(keys, rid);
		if (cmp >= 0)
		{
			// put to the left of rec
			IndexRecord left = rec.prev(true);
			left.setNextNoLog(newRec);
			rec.setPrevNoLog(newRec);
			newRec.setPrevNoLog(left);
			newRec.setNextNoLog(rec);
		}
		else
		{
			// put to the right of rec @ end of list
			rec.setNextNoLog(newRec);
			newRec.setPrevNoLog(rec);
		}

		while (random.nextDouble() < promoRate)
		{
			newRec = promoteNoLog(newRec);
		}
	}

	public void insertNoLog(FieldValue[] keys, RID rid) throws Exception
	{
		insertNoLog(fvaToAlo(keys), rid);
	}

	public boolean isAvailable(Page p, int size)
	{
		int free = p.getInt(5);
		int remaining = Page.BLOCK_SIZE - free;
		return size <= remaining;
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
			catch (Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				throw e;
			}

			boolean val = p.get(4) == 1;
			isUniqueVar = val;
		}

		return isUniqueVar;
	}

	public void massDelete() throws Exception
	{
		setFirstPosition(true);
		IndexRecord rec = line;
		while (!rec.isNull())
		{
			if (rec.isLeaf())
			{
				rec.markTombstone();
				rec = rec.next(true);
			}
		}
	}

	public void massDeleteNoLog() throws Exception
	{
		setFirstPosition(true);
		IndexRecord rec = line;
		while (!rec.isNull())
		{
			if (rec.isLeaf())
			{
				rec.markTombstoneNoLog();
				rec = rec.next(true);
			}
		}
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
			LockSupport.parkNanos(500);
		}

		if (!positioned)
		{
			queue = new BufferedLinkedBlockingQueue(ResourceManager.QUEUE_SIZE);
			ridList = new ArrayListLong();
			terminates = new ArrayList<Filter>();
			try
			{
				setStartingPos();
				// DEBUG
				// HRDBMSWorker.logger.debug("Positioning for " + f);
				// HRDBMSWorker.logger.debug("Secondary is " + secondary);
				// HRDBMSWorker.logger.debug("Positioned on " + line);
				// if (!line.isStart())
				// {
				// HRDBMSWorker.logger.debug("With keys " +
				// line.getKeys(types));
				// }
				// DEBUG
				positioned = true;
				iwt = new IndexWriterThread(keys);
				iwt.start();
			}
			catch (Exception e)
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

	public void open()
	{
		random = new Random(System.currentTimeMillis());
		typesBytes = new byte[types.size()];
		int i = 0;
		for (String type : types)
		{
			if (type.equals("INT"))
			{
				typesBytes[i] = 0;
			}
			else if (type.equals("FLOAT"))
			{
				typesBytes[i] = 1;
			}
			else if (type.equals("CHAR"))
			{
				typesBytes[i] = 2;
			}
			else if (type.equals("LONG"))
			{
				typesBytes[i] = 3;
			}
			else if (type.equals("DATE"))
			{
				typesBytes[i] = 4;
			}
			else
			{
				typesBytes[i] = 5;
			}

			i++;
		}
	}

	public void open(int device, MetaData meta) throws Exception
	{
		try
		{
			if (!fileName.startsWith("/"))
			{
				fileName = meta.getDevicePath(device) + fileName;
			}
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
		random = new Random(System.currentTimeMillis());

		typesBytes = new byte[types.size()];
		int i = 0;
		for (String type : types)
		{
			if (type.equals("INT"))
			{
				typesBytes[i] = 0;
			}
			else if (type.equals("FLOAT"))
			{
				typesBytes[i] = 1;
			}
			else if (type.equals("CHAR"))
			{
				typesBytes[i] = 2;
			}
			else if (type.equals("LONG"))
			{
				typesBytes[i] = 3;
			}
			else if (type.equals("DATE"))
			{
				typesBytes[i] = 4;
			}
			else
			{
				typesBytes[i] = 5;
			}

			i++;
		}
	}

	public void replace(ArrayList<Object> keys, RID oldRid, RID newRid) throws Exception
	{
		setEqualsPosMulti(keys, true);
		IndexRecord rec = line;
		while (!rec.isNull() && rec.keysMatch(keys))
		{
			if (rec.ridsMatch(oldRid))
			{
				rec.replaceRid(newRid);
				return;
			}

			rec = rec.next(true);
		}

		throw new Exception("Unable to find record to update RID");
	}

	public synchronized void reset() throws Exception
	{
		lastThread = null;

		try
		{
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

	public void scan(CNFFilter filter, boolean sample, int get, int skip, BufferedLinkedBlockingQueue queue, String[] fetchP2C, TreeMap<Integer, String> finalP2C, Transaction tx) throws Exception
	{
		LockManager.sLock(new Block(fileName, -1), tx.number());
		// FileManager.getFile(fileName);
		Integer numBlocks = FileManager.numBlocks.get(fileName);
		if (numBlocks == null)
		{
			FileManager.getFile(fileName);
			numBlocks = FileManager.numBlocks.get(fileName);
		}

		if (numBlocks == 0)
		{
			throw new Exception("Unable to open file " + fileName);
		}

		ArrayList<Integer> fetchPos = new ArrayList<Integer>();
		ArrayList<Integer> keepPos = new ArrayList<Integer>();
		for (String s : fetchP2C)
		{
			int indx = keys.indexOf(s);
			fetchPos.add(indx);
		}

		for (String s : finalP2C.values())
		{
			int indx = keys.indexOf(s);
			indx = fetchPos.indexOf(indx);
			keepPos.add(indx);
		}

		int onPage = 0;
		int lastRequested = -1;
		ArrayList<Object> row = new ArrayList<Object>(fetchPos.size());
		int get2 = get;
		int skip2 = skip;
		int get3 = get;
		int skip3 = skip;
		while (onPage < numBlocks)
		{
			if ((!sample && lastRequested - onPage < PAGES_IN_ADVANCE) || (sample && lastRequested - onPage < PAGES_IN_ADVANCE * skip))
			{
				if (!sample)
				{
					Block[] toRequest = new Block[lastRequested + PREFETCH_REQUEST_SIZE < numBlocks ? PREFETCH_REQUEST_SIZE : numBlocks - lastRequested - 1];
					int i = 0;
					final int length = toRequest.length;
					while (i < length)
					{
						toRequest[i] = new Block(fileName, lastRequested + i + 1);
						i++;
					}
					tx.requestPages(toRequest);
					lastRequested += toRequest.length;
				}
				else
				{
					ArrayList<Block> toRequest = new ArrayList<Block>();
					int i = 0;
					int length = lastRequested + PREFETCH_REQUEST_SIZE < numBlocks ? PREFETCH_REQUEST_SIZE : numBlocks - lastRequested - 1;
					while (i < length)
					{
						if (skip3 == 0)
						{
							get3 = get - 1;
							skip3 = skip;
						}
						else if (get3 == 0)
						{
							skip3--;
							i++;
							continue;
						}
						else
						{
							get3--;
						}

						toRequest.add(new Block(fileName, lastRequested + i + 1));
						i++;
					}

					if (toRequest.size() > 0)
					{
						Block[] toRequest2 = new Block[toRequest.size()];
						int j = 0;
						for (Block b : toRequest)
						{
							toRequest2[j] = b;
							j++;
						}
						tx.requestPages(toRequest2);
					}

					lastRequested += length;
				}
			}

			if (sample && skip2 == 0)
			{
				get2 = get - 1;
				skip2 = skip;
			}
			else if (sample && get2 == 0)
			{
				skip2--;
				onPage++;
				continue;
			}
			else if (sample)
			{
				get2--;
			}

			Block b = new Block(fileName, onPage++);
			Page p = tx.getPage(b);
			LockManager.sLock(b, tx.number());
			int firstFree = p.getInt(5);
			int offset = 0;
			if (b.number() == 0)
			{
				offset = 17;
			}
			else
			{
				offset = 9;
			}

			outer: while (offset < firstFree)
			{
				row.clear();
				IndexRecord rec = new IndexRecord(fileName, b.number(), offset, tx, p);
				if (rec.isLeaf() && !rec.isStart() && !rec.isTombstone())
				{
					ArrayList<Object> r = rec.getKeysAndScroll(types);
					for (int pos : fetchPos)
					{
						row.add(r.get(pos));
					}

					if (filter.passes(row))
					{
						ArrayList<Object> r2 = new ArrayList<Object>();
						for (int pos : keepPos)
						{
							r2.add(row.get(pos));
						}

						queue.put(r2);
					}

					offset += rec.getScroll();
				}
				else if (rec.isTombstone())
				{
					rec.getKeysAndScroll(types);
					offset += rec.getScroll();
				}
				else if (rec.isStart())
				{
					offset += 33;
				}
				else
				{
					rec.getKeysAndScroll(types);
					offset += rec.getScroll();
				}
			}

			tx.unpin(p);
		}
	}

	public void serialize(OutputStream out, IdentityHashMap<Object, Long> prev) throws Exception
	{
		Long id = prev.get(this);
		if (id != null)
		{
			OperatorUtils.serializeReference(id, out);
			return;
		}

		OperatorUtils.writeType(64, out);
		prev.put(this, OperatorUtils.writeID(out));
		OperatorUtils.writeString(fileName, out, prev);
		OperatorUtils.serializeALS(keys, out, prev);
		OperatorUtils.serializeALS(types, out, prev);
		OperatorUtils.serializeALB(orders, out, prev);
		OperatorUtils.serializeFilter(f, out, prev);
		OperatorUtils.serializeALF(secondary, out, prev);
		OperatorUtils.writeBool(positioned, out);
		OperatorUtils.serializeStringIntHM(cols2Pos, out, prev);
		OperatorUtils.writeBool(indexOnly, out);
		OperatorUtils.serializeALI(fetches, out, prev);
		OperatorUtils.serializeALS(fetchTypes, out, prev);
		OperatorUtils.writeBool(delayed, out);
		OperatorUtils.serializeALF(delayedConditions, out, prev);
		OperatorUtils.serializeStringHM(renames, out, prev);
		OperatorUtils.writeLong(offset, out);
		OperatorUtils.writeLong(tx.number(), out); // Notice type
		OperatorUtils.writeBoolClass(isUniqueVar, out, prev);
		// recreate myPages
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

	public void setTransaction(Transaction tx)
	{
		this.tx = tx;
		// HRDBMSWorker.logger.debug(this + " just had its transaction set to "
		// + tx);
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

	public IndexRecord writeNewInternal(IndexRecord rec) throws Exception
	{
		Block b = new Block(fileName, -1);
		LockManager.xLock(b, tx.number());
		int last = FileManager.numBlocks.get(fileName);
		b = new Block(fileName, last - 1);
		LockManager.xLock(b, tx.number());
		tx.requestPage(b);
		Page p = tx.getPage(b);
		byte[] keyBytes = rec.keyBytes();
		if (isAvailable(p, 33 + keyBytes.length))
		{
			ByteBuffer bb = ByteBuffer.allocate(33 + keyBytes.length);
			bb.put((byte)0);
			bb.putLong(0);
			bb.putLong(0);
			bb.putLong(0);
			bb.putLong(0);
			bb.put(keyBytes);
			int free = p.getInt(5);
			byte[] before = new byte[33 + keyBytes.length];
			p.get(free, before);
			InsertLogRec rec2 = tx.insert(before, bb.array(), free, p.block());
			p.write(free, bb.array(), tx.number(), rec2.lsn());
			before = new byte[4];
			p.get(5, before);
			bb = ByteBuffer.allocate(4);
			bb.putInt(free + 33 + keyBytes.length);
			rec2 = tx.insert(before, bb.array(), 5, p.block());
			p.write(5, bb.array(), tx.number(), rec2.lsn());
			return new IndexRecord(p, free, keyBytes, tx);
		}
		else
		{
			p = addNewBlock();
			ByteBuffer bb = ByteBuffer.allocate(33 + keyBytes.length);
			bb.put((byte)0);
			bb.putLong(0);
			bb.putLong(0);
			bb.putLong(0);
			bb.putLong(0);
			bb.put(keyBytes);
			int free = p.getInt(5);
			byte[] before = new byte[33 + keyBytes.length];
			p.get(free, before);
			InsertLogRec rec2 = tx.insert(before, bb.array(), free, p.block());
			p.write(free, bb.array(), tx.number(), rec2.lsn());
			before = new byte[4];
			p.get(5, before);
			bb = ByteBuffer.allocate(4);
			bb.putInt(free + 33 + keyBytes.length);
			rec2 = tx.insert(before, bb.array(), 5, p.block());
			p.write(5, bb.array(), tx.number(), rec2.lsn());
			return new IndexRecord(p, free, keyBytes, tx);
		}
	}

	public IndexRecord writeNewInternalNoLog(IndexRecord rec) throws Exception
	{
		Block b = new Block(fileName, -1);
		LockManager.xLock(b, tx.number());
		int last = FileManager.numBlocks.get(fileName);
		b = new Block(fileName, last - 1);
		Page p = myPages.get(b);
		if (p == null)
		{
			LockManager.xLock(b, tx.number());
			tx.requestPage(b);
			p = tx.getPage(b);
			myPages.put(b, p);
		}
		byte[] keyBytes = rec.keyBytes();
		if (isAvailable(p, 33 + keyBytes.length))
		{
			ByteBuffer bb = ByteBuffer.allocate(33 + keyBytes.length);
			bb.put((byte)0);
			bb.putLong(0);
			bb.putLong(0);
			bb.putLong(0);
			bb.putLong(0);
			bb.put(keyBytes);
			int free = p.getInt(5);
			p.write(free, bb.array(), tx.number(), LogManager.getLSN());
			bb = ByteBuffer.allocate(4);
			bb.putInt(free + 33 + keyBytes.length);
			p.write(5, bb.array(), tx.number(), LogManager.getLSN());
			return new IndexRecord(p, free, keyBytes, tx);
		}
		else
		{
			p = addNewBlockNoLog();
			ByteBuffer bb = ByteBuffer.allocate(33 + keyBytes.length);
			bb.put((byte)0);
			bb.putLong(0);
			bb.putLong(0);
			bb.putLong(0);
			bb.putLong(0);
			bb.put(keyBytes);
			int free = p.getInt(5);
			p.write(free, bb.array(), tx.number(), LogManager.getLSN());
			bb = ByteBuffer.allocate(4);
			bb.putInt(free + 33 + keyBytes.length);
			p.write(5, bb.array(), tx.number(), LogManager.getLSN());
			return new IndexRecord(p, free, keyBytes, tx);
		}
	}

	public IndexRecord writeNewLeaf(ArrayList<Object> keys, RID rid) throws Exception
	{
		Block b = new Block(fileName, -1);
		LockManager.xLock(b, tx.number());
		int last = FileManager.numBlocks.get(fileName);
		b = new Block(fileName, last - 1);
		LockManager.xLock(b, tx.number());
		tx.requestPage(b);
		Page p = tx.getPage(b);
		byte[] keyBytes = genKeyBytes(keys);
		if (isAvailable(p, 41 + keyBytes.length))
		{
			ByteBuffer bb = ByteBuffer.allocate(41 + keyBytes.length);
			bb.put((byte)1);
			bb.putLong(0);
			bb.putLong(0);
			bb.putLong(0);
			// bb.putLong(0); NO down pointer
			bb.putInt(rid.getNode());
			bb.putInt(rid.getDevice());
			bb.putInt(rid.getBlockNum());
			bb.putInt(rid.getRecNum());
			bb.put(keyBytes);
			int free = p.getInt(5);
			byte[] before = new byte[41 + keyBytes.length];
			p.get(free, before);
			InsertLogRec rec = tx.insert(before, bb.array(), free, p.block());
			p.write(free, bb.array(), tx.number(), rec.lsn());
			before = new byte[4];
			p.get(5, before);
			bb = ByteBuffer.allocate(4);
			bb.putInt(free + 41 + keyBytes.length);
			rec = tx.insert(before, bb.array(), 5, p.block());
			p.write(5, bb.array(), tx.number(), rec.lsn());
			return new IndexRecord(p, free, keyBytes, tx);
		}
		else
		{
			p = addNewBlock();
			ByteBuffer bb = ByteBuffer.allocate(41 + keyBytes.length);
			bb.put((byte)1);
			bb.putLong(0);
			bb.putLong(0);
			bb.putLong(0);
			// bb.putLong(0); No down
			bb.putInt(rid.getNode());
			bb.putInt(rid.getDevice());
			bb.putInt(rid.getBlockNum());
			bb.putInt(rid.getRecNum());
			bb.put(keyBytes);
			int free = p.getInt(5);
			byte[] before = new byte[41 + keyBytes.length];
			p.get(free, before);
			InsertLogRec rec = tx.insert(before, bb.array(), free, p.block());
			p.write(free, bb.array(), tx.number(), rec.lsn());
			before = new byte[4];
			p.get(5, before);
			bb = ByteBuffer.allocate(4);
			bb.putInt(free + 41 + keyBytes.length);
			rec = tx.insert(before, bb.array(), 5, p.block());
			p.write(5, bb.array(), tx.number(), rec.lsn());
			return new IndexRecord(p, free, keyBytes, tx);
		}
	}

	public IndexRecord writeNewLeafNoLog(ArrayList<Object> keys, RID rid) throws Exception
	{
		Block b = new Block(fileName, -1);
		// LockManager.xLock(b, tx.number());
		int last = FileManager.numBlocks.get(fileName);
		b = new Block(fileName, last - 1);
		Page p = myPages.get(b);
		if (p == null)
		{
			LockManager.xLock(b, tx.number());
			tx.requestPage(b);
			p = tx.getPage(b);
			myPages.put(b, p);
		}
		byte[] keyBytes = genKeyBytes(keys);
		if (isAvailable(p, 41 + keyBytes.length))
		{
			ByteBuffer bb = ByteBuffer.allocate(41 + keyBytes.length);
			bb.put((byte)1);
			bb.putLong(0);
			bb.putLong(0);
			bb.putLong(0);
			// bb.putLong(0); No down
			bb.putInt(rid.getNode());
			bb.putInt(rid.getDevice());
			bb.putInt(rid.getBlockNum());
			bb.putInt(rid.getRecNum());
			bb.put(keyBytes);
			int free = p.getInt(5);
			p.write(free, bb.array(), tx.number(), LogManager.getLSN());
			bb = ByteBuffer.allocate(4);
			bb.putInt(free + 41 + keyBytes.length);
			p.write(5, bb.array(), tx.number(), LogManager.getLSN());
			return new IndexRecord(p, free, keyBytes, tx);
		}
		else
		{
			p = addNewBlockNoLog();
			ByteBuffer bb = ByteBuffer.allocate(41 + keyBytes.length);
			bb.put((byte)1);
			bb.putLong(0);
			bb.putLong(0);
			bb.putLong(0);
			// bb.putLong(0); no down
			bb.putInt(rid.getNode());
			bb.putInt(rid.getDevice());
			bb.putInt(rid.getBlockNum());
			bb.putInt(rid.getRecNum());
			bb.put(keyBytes);
			int free = p.getInt(5);
			p.write(free, bb.array(), tx.number(), LogManager.getLSN());
			bb = ByteBuffer.allocate(4);
			bb.putInt(free + 41 + keyBytes.length);
			p.write(5, bb.array(), tx.number(), LogManager.getLSN());
			return new IndexRecord(p, free, keyBytes, tx);
		}
	}

	private Page addNewBlock() throws Exception
	{
		final ByteBuffer oldBuff = ByteBuffer.allocate(Page.BLOCK_SIZE);
		oldBuff.position(0);
		int i = 0;
		while (i < Page.BLOCK_SIZE)
		{
			oldBuff.putLong(144680345676153346L);
			i += 8;
		}
		final int newBlockNum = FileManager.addNewBlock(fileName, oldBuff, tx);

		ByteBuffer buff = ByteBuffer.allocate(Page.BLOCK_SIZE);
		buff.position(0);
		buff.putInt(types.size());
		if (isUnique())
		{
			buff.put((byte)1);
		}
		else
		{
			buff.put((byte)0);
		}
		buff.putInt(9);

		Block bl = new Block(fileName, newBlockNum);
		LockManager.xLock(bl, tx.number());
		tx.requestPage(bl);
		Page p2 = null;
		try
		{
			p2 = tx.getPage(bl);
		}
		catch (Exception e)
		{
			HRDBMSWorker.logger.error("", e);
			throw e;
		}

		InsertLogRec rec = tx.insert(oldBuff.array(), buff.array(), 0, bl);
		p2.write(0, buff.array(), tx.number(), rec.lsn());

		return p2;
	}

	private Page addNewBlockNoLog() throws Exception
	{
		final ByteBuffer oldBuff = ByteBuffer.allocate(Page.BLOCK_SIZE);
		oldBuff.position(0);
		int i = 0;
		while (i < Page.BLOCK_SIZE)
		{
			oldBuff.putLong(144680345676153346L);
			i += 8;
		}
		final int newBlockNum = FileManager.addNewBlockNoLog(fileName, oldBuff, tx);

		ByteBuffer buff = ByteBuffer.allocate(Page.BLOCK_SIZE);
		buff.position(0);
		buff.putInt(types.size());
		if (isUnique())
		{
			buff.put((byte)1);
		}
		else
		{
			buff.put((byte)0);
		}
		buff.putInt(9);

		Block bl = new Block(fileName, newBlockNum);
		LockManager.xLock(bl, tx.number());
		tx.requestPage(bl);
		Page p2 = null;
		try
		{
			p2 = tx.getPage(bl);
			myPages.put(bl, p2);
		}
		catch (Exception e)
		{
			HRDBMSWorker.logger.error("", e);
			throw e;
		}

		p2.write(0, buff.array(), tx.number(), LogManager.getLSN());

		return p2;
	}

	private FieldValue[] aloToFieldValues(ArrayList<Object> row)
	{
		FieldValue[] retval = new FieldValue[row.size()];
		int i = 0;
		int z = 0;
		final int limit = row.size();
		// for (Object o : row)
		while (z < limit)
		{
			Object o = row.get(z++);
			if (o instanceof Integer)
			{
				retval[i] = new Schema.IntegerFV((Integer)o);
			}
			else if (o instanceof Long)
			{
				retval[i] = new Schema.BigintFV((Long)o);
			}
			else if (o instanceof Double)
			{
				retval[i] = new Schema.DoubleFV((Double)o);
			}
			else if (o instanceof MyDate)
			{
				retval[i] = new Schema.DateFV((MyDate)o);
			}
			else if (o instanceof String)
			{
				retval[i] = new Schema.VarcharFV((String)o);
			}

			i++;
		}

		return retval;
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
			}
			else
			{
				col2 = filter.rightColumn();
				val = filter.leftLiteral();
				
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
	}

	private final int compare(ArrayList<Object> vals, ArrayList<Object> r)
	{
		return rc.compare(vals, r);
	}

	private final int compare(ArrayList<Object> vals, ArrayList<Object> r, boolean debug)
	{
		return rc.compare(vals, r, true);
	}

	private IndexRecord createNewLevel(IndexRecord rec) throws Exception
	{
		Block b = new Block(fileName, -1);
		LockManager.xLock(b, tx.number());
		int last = FileManager.numBlocks.get(fileName);
		b = new Block(fileName, last - 1);
		LockManager.xLock(b, tx.number());
		tx.requestPage(b);
		Page p = tx.getPage(b);
		Block b2 = new Block(fileName, 0);
		LockManager.xLock(b2, tx.number());
		tx.requestPage(b2);
		Page p2 = tx.getPage(b2);
		int headBlock = p2.getInt(9);
		int headOff = p2.getInt(13);
		byte[] keyBytes = rec.keyBytes();
		if (isAvailable(p, 66 + keyBytes.length))
		{
			int free = p.getInt(5);
			ByteBuffer bb = ByteBuffer.allocate(66 + keyBytes.length);
			bb.put((byte)3);
			bb.putLong(0);
			bb.putInt(p.block().number());
			bb.putInt(free + 33);
			bb.putLong(0);
			bb.putInt(headBlock);
			bb.putInt(headOff);
			bb.put((byte)0);
			bb.putInt(p.block().number());
			bb.putInt(free);
			bb.putLong(0);
			bb.putLong(0);
			bb.putInt(rec.b.number());
			bb.putInt(rec.off);
			bb.put(keyBytes);
			byte[] before = new byte[66 + keyBytes.length];
			p.get(free, before);
			InsertLogRec rec2 = tx.insert(before, bb.array(), free, p.block());
			p.write(free, bb.array(), tx.number(), rec2.lsn());
			before = new byte[4];
			p.get(5, before);
			bb = ByteBuffer.allocate(4);
			bb.putInt(free + 66 + keyBytes.length);
			rec2 = tx.insert(before, bb.array(), 5, p.block());
			p.write(5, bb.array(), tx.number(), rec2.lsn());
			bb = ByteBuffer.allocate(8);
			bb.putInt(p.block().number());
			bb.putInt(free);
			before = new byte[8];
			p2.get(9, before);
			rec2 = tx.insert(before, bb.array(), 9, p2.block());
			p2.write(9, bb.array(), tx.number(), rec2.lsn());
			IndexRecord retval = new IndexRecord(p, free + 33, keyBytes, tx);
			IndexRecord oldHead = new IndexRecord(fileName, headBlock, headOff, tx, true);
			IndexRecord newHead = new IndexRecord(p, free, null, tx);
			oldHead.setUp(newHead);
			rec.setUp(retval);
			return retval;
		}
		else
		{
			p = addNewBlock();
			int free = p.getInt(5);
			ByteBuffer bb = ByteBuffer.allocate(66 + keyBytes.length);
			bb.put((byte)3);
			bb.putLong(0);
			bb.putInt(p.block().number());
			bb.putInt(free + 33);
			bb.putLong(0);
			bb.putInt(headBlock);
			bb.putInt(headOff);
			bb.put((byte)0);
			bb.putInt(p.block().number());
			bb.putInt(free);
			bb.putLong(0);
			bb.putLong(0);
			bb.putInt(rec.b.number());
			bb.putInt(rec.off);
			bb.put(keyBytes);
			byte[] before = new byte[66 + keyBytes.length];
			p.get(free, before);
			InsertLogRec rec2 = tx.insert(before, bb.array(), free, p.block());
			p.write(free, bb.array(), tx.number(), rec2.lsn());
			before = new byte[4];
			p.get(5, before);
			bb = ByteBuffer.allocate(4);
			bb.putInt(free + 66 + keyBytes.length);
			rec2 = tx.insert(before, bb.array(), 5, p.block());
			p.write(5, bb.array(), tx.number(), rec2.lsn());
			bb = ByteBuffer.allocate(8);
			bb.putInt(p.block().number());
			bb.putInt(free);
			before = new byte[8];
			p2.get(9, before);
			rec2 = tx.insert(before, bb.array(), 9, p2.block());
			p2.write(9, bb.array(), tx.number(), rec2.lsn());
			IndexRecord retval = new IndexRecord(p, free + 33, keyBytes, tx);
			IndexRecord oldHead = new IndexRecord(fileName, headBlock, headOff, tx, true);
			IndexRecord newHead = new IndexRecord(p, free, null, tx);
			oldHead.setUp(newHead);
			rec.setUp(retval);
			return retval;
		}
	}

	private IndexRecord createNewLevelNoLog(IndexRecord rec) throws Exception
	{
		Block b = new Block(fileName, -1);
		LockManager.xLock(b, tx.number());
		int last = FileManager.numBlocks.get(fileName);
		b = new Block(fileName, last - 1);
		Page p = myPages.get(b);
		if (p == null)
		{
			LockManager.xLock(b, tx.number());
			tx.requestPage(b);
			p = tx.getPage(b);
			myPages.put(b, p);
		}
		Block b2 = new Block(fileName, 0);
		Page p2 = myPages.get(b2);
		if (p2 == null)
		{
			LockManager.xLock(b2, tx.number());
			tx.requestPage(b2);
			p2 = tx.getPage(b2);
			myPages.put(b2, p2);
		}
		int headBlock = p2.getInt(9);
		int headOff = p2.getInt(13);
		byte[] keyBytes = rec.keyBytes();
		if (isAvailable(p, 66 + keyBytes.length))
		{
			int free = p.getInt(5);
			ByteBuffer bb = ByteBuffer.allocate(66 + keyBytes.length);
			bb.put((byte)3);
			bb.putLong(0);
			bb.putInt(p.block().number());
			bb.putInt(free + 33);
			bb.putLong(0);
			bb.putInt(headBlock);
			bb.putInt(headOff);
			bb.put((byte)0);
			bb.putInt(p.block().number());
			bb.putInt(free);
			bb.putLong(0);
			bb.putLong(0);
			bb.putInt(rec.b.number());
			bb.putInt(rec.off);
			bb.put(keyBytes);
			p.write(free, bb.array(), tx.number(), LogManager.getLSN());
			bb = ByteBuffer.allocate(4);
			bb.putInt(free + 66 + keyBytes.length);
			p.write(5, bb.array(), tx.number(), LogManager.getLSN());
			bb = ByteBuffer.allocate(8);
			bb.putInt(p.block().number());
			bb.putInt(free);
			p2.write(9, bb.array(), tx.number(), LogManager.getLSN());
			IndexRecord retval = new IndexRecord(p, free + 33, keyBytes, tx);
			IndexRecord oldHead = new IndexRecord(fileName, headBlock, headOff, tx, true);
			IndexRecord newHead = new IndexRecord(p, free, null, tx);
			oldHead.setUpNoLog(newHead);
			rec.setUpNoLog(retval);
			return retval;
		}
		else
		{
			p = addNewBlockNoLog();
			int free = p.getInt(5);
			ByteBuffer bb = ByteBuffer.allocate(66 + keyBytes.length);
			bb.put((byte)3);
			bb.putLong(0);
			bb.putInt(p.block().number());
			bb.putInt(free + 33);
			bb.putLong(0);
			bb.putInt(headBlock);
			bb.putInt(headOff);
			bb.put((byte)0);
			bb.putInt(p.block().number());
			bb.putInt(free);
			bb.putLong(0);
			bb.putLong(0);
			bb.putInt(rec.b.number());
			bb.putInt(rec.off);
			bb.put(keyBytes);
			p.write(free, bb.array(), tx.number(), LogManager.getLSN());
			bb = ByteBuffer.allocate(4);
			bb.putInt(free + 66 + keyBytes.length);
			p.write(5, bb.array(), tx.number(), LogManager.getLSN());
			bb = ByteBuffer.allocate(8);
			bb.putInt(p.block().number());
			bb.putInt(free);
			p2.write(9, bb.array(), tx.number(), LogManager.getLSN());
			IndexRecord retval = new IndexRecord(p, free + 33, keyBytes, tx);
			IndexRecord oldHead = new IndexRecord(fileName, headBlock, headOff, tx, true);
			IndexRecord newHead = new IndexRecord(p, free, null, tx);
			oldHead.setUpNoLog(newHead);
			rec.setUpNoLog(retval);
			return retval;
		}
	}

	private boolean currentKeySatisfies() throws Exception
	{
		if (line.isStart())
		{
			return false;
		}

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

			int z = 0;
			final int limit = secondary.size();
			// for (final Filter filter : secondary)
			while (z < limit)
			{
				final Filter filter = secondary.get(z++);
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

	private byte[] genKeyBytes(ArrayList<Object> keys2) throws Exception
	{
		FieldValue[] keys = aloToFieldValues(keys2);
		int size = keys.length;
		int i = 0;
		ArrayList<byte[]> bytes = new ArrayList<byte[]>();
		for (byte type : typesBytes)
		{
			if (type == 0)
			{
				size += 4;
			}
			else if (type == 3)
			{
				size += 8;
			}
			else if (type == 1)
			{
				size += 8;
			}
			else if (type == 4)
			{
				size += 4;
			}
			else if (type == 2)
			{
				size += 4;
				String val = (String)keys[i].getValue();
				// byte[] data = val.getBytes("UTF-8");
				byte[] ba = new byte[val.length() << 2];
				char[] value = (char[])unsafe.getObject(val, soffset);
				int blen = ((sun.nio.cs.ArrayEncoder)ce).encode(value, 0, value.length, ba);
				byte[] data = Arrays.copyOf(ba, blen);
				size += data.length;
				bytes.add(data);
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
		int j = 0;
		for (byte type : typesBytes)
		{
			bb.put((byte)0); // not null;
			if (type == 0)
			{
				bb.putInt(((Integer)keys[i].getValue()));
			}
			else if (type == 3)
			{
				final long temp = ((Long)keys[i].getValue());
				// if (temp < 0)
				// {
				// HRDBMSWorker.logger.debug("Writing negative long in genKeyBytes");
				// }
				bb.putLong(temp);
			}
			else if (type == 1)
			{
				bb.putDouble(((Double)keys[i].getValue()));
			}
			else if (type == 4)
			{
				bb.putInt(((MyDate)keys[i].getValue()).getTime());
			}
			else if (type == 2)
			{
				byte[] data = bytes.get(j++);
				bb.putInt(data.length);
				try
				{
					bb.put(data);
				}
				catch (Exception e)
				{
					HRDBMSWorker.logger.error("", e);
					throw e;
				}
			}

			i++;
		}

		return bb.array();
	}

	private long getPartialRid()
	{
		return line.getPartialRid();
	}

	private String getPrefix(Object x)
	{
		return ((String)x).substring(0, ((String)x).indexOf("%"));
	}

	private boolean marksEnd() throws Exception
	{
		if (line.isNull())
		{
			return true;
		}

		if (line.isStart())
		{
			return false;
		}

		if (terminates.size() == 0)
		{
			return false;
		}

		final ArrayList<Object> row = line.getKeys(types);

		try
		{
			int z = 0;
			final int limit = terminates.size();
			// for (final Filter terminate : terminates)
			while (z < limit)
			{
				final Filter terminate = terminates.get(z++);
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

	private IndexRecord promote(IndexRecord rec) throws Exception
	{
		IndexRecord next = rec.prev(true);
		while (!next.isStart())
		{
			IndexRecord up = next.getUp(true);
			if (!up.isNull())
			{
				return up.insertInternalToRight(rec);
			}

			next = next.prev(true);
		}

		IndexRecord up = next.getUp(true);
		if (!up.isNull())
		{
			return up.insertInternalToRight(rec);
		}
		else
		{
			return createNewLevel(rec);
		}
	}

	private IndexRecord promoteNoLog(IndexRecord rec) throws Exception
	{
		IndexRecord next = rec.prev(true);
		while (!next.isStart())
		{
			IndexRecord up = next.getUp(true);
			if (!up.isNull())
			{
				return up.insertInternalToRightNoLog(rec);
			}

			next = next.prev(true);
		}

		IndexRecord up = next.getUp(true);
		if (!up.isNull())
		{
			return up.insertInternalToRightNoLog(rec);
		}
		else
		{
			return createNewLevelNoLog(rec);
		}
	}

	private final void setEqualsPos(Object val) throws Exception
	{
		Block b = new Block(fileName, 0);
		tx.requestPage(b);
		Page p = tx.getPage(b);
		// DEBUG
		// RandomAccessFile dump = new RandomAccessFile("/tmp/dump", "rw");
		// FileChannel fc = dump.getChannel();
		// p.buffer().position(0);
		// fc.write(p.buffer());
		// fc.close();
		// dump.close();
		// DEBUG
		int headBlock = p.getInt(9);
		int headOff = p.getInt(13);
		line = new IndexRecord(fileName, headBlock, headOff, tx);
		// DEBUG
		// HRDBMSWorker.logger.debug("Doing partial positioning for " + val);
		// DEBUG
		IndexRecord prev = null;

		while (true)
		{
			// line is start rec
			int cmp = 0;
			if (!line.isStart())
			{
				ArrayList<Object> k = line.getKeys(types);
				// DEBUG
				// HRDBMSWorker.logger.debug("Key is " + k);
				// DEBUG
				Object key = k.get(keys.indexOf(col));
				// DEBUG
				// HRDBMSWorker.logger.debug("Pulled out " + key);
				// DEBUG

				if (val instanceof Long && key instanceof Integer)
				{
					key = ((Integer)key).longValue();
				}
				if (val instanceof Integer && key instanceof Long)
				{
					val = ((Integer)val).longValue();
				}
				cmp = ((Comparable)val).compareTo(key);
				// DEBUG
				// HRDBMSWorker.logger.debug("Comparing " + val + " to " + key);
				// HRDBMSWorker.logger.debug("Result is " + cmp);
				// DEBUG

				if (cmp > 0)
				{
					IndexRecord next = line.next();
					if (next.isNull())
					{
						IndexRecord down = line.getDown();
						if (down.isNull())
						{
							return;
						}
						else
						{
							line = down;
							// prev = null;
						}
					}
					else
					{
						prev = line;
						line = next;
					}
				}
				else
				{
					IndexRecord down = prev.getDown();
					if (down.isNull())
					{
						return;
					}
					else
					{
						prev = down;
						line = down.next();
					}
				}
			}
			else
			{
				IndexRecord next = line.next();
				if (next.isNull())
				{
					IndexRecord down = line.getDown();
					if (down.isNull())
					{
						return;
					}
					else
					{
						line = down;
						// prev = null;
					}
				}
				else
				{
					prev = line;
					line = next;
				}
			}
		}
	}

	private final void setEqualsPosMulti(ArrayList<Object> vals) throws Exception
	{
		Block b = new Block(fileName, 0);
		Page p = myPages.get(b);
		tx.requestPage(b);
		p = tx.getPage(b);
		int headBlock = p.getInt(9);
		int headOff = p.getInt(13);
		line = new IndexRecord(fileName, headBlock, headOff, tx);
		IndexRecord prev = null;

		while (true)
		{
			// line is start rec
			int cmp = 0;
			if (!line.isStart())
			{
				cmp = compare(vals, line.getKeys(types));
				// DEBUG
				// HRDBMSWorker.logger.debug("Comparing " + vals + " to " +
				// line.getKeys(types));
				// HRDBMSWorker.logger.debug("Result is " + cmp);
				// DEBUG
				if (cmp > 0)
				{
					IndexRecord next = line.next();
					if (next.isNull())
					{
						IndexRecord down = line.getDown();
						if (down.isNull())
						{
							return;
						}
						else
						{
							line = down;
							// prev = null;
						}
					}
					else
					{
						prev = line;
						line = next;
					}
				}
				else if (cmp == 0)
				{
					if (isUnique())
					{
						while (true)
						{
							IndexRecord down = line.getDown();
							if (down.isNull())
							{
								return;
							}
							else
							{
								line = down;
							}
						}
					}
					else
					{
						IndexRecord down = prev.getDown();
						if (down.isNull())
						{
							return;
						}
						else
						{
							prev = down;
							line = down.next();
						}
					}
				}
				else
				{
					IndexRecord down = prev.getDown();
					if (down.isNull())
					{
						return;
					}
					else
					{
						prev = down;
						line = down.next();
					}
				}
			}
			else
			{
				IndexRecord next = line.next();
				if (next.isNull())
				{
					IndexRecord down = line.getDown();
					if (down.isNull())
					{
						return;
					}
					else
					{
						line = down;
						// prev = null;
					}
				}
				else
				{
					prev = line;
					line = next;
				}
			}
		}
	}

	private final void setEqualsPosMulti(ArrayList<Object> vals, boolean xLock) throws Exception
	{
		if (!xLock)
		{
			setEqualsPosMulti(vals);
			return;
		}

		Block b = new Block(fileName, 0);
		Page p = myPages.get(b);
		if (p == null)
		{
			LockManager.xLock(b, tx.number());
			tx.requestPage(b);
			p = tx.getPage(b);
			myPages.put(b, p);
		}
		// DEBUG
		// RandomAccessFile dump = new RandomAccessFile("/tmp/dump", "rw");
		// FileChannel fc = dump.getChannel();
		// p.buffer().position(0);
		// fc.write(p.buffer());
		// fc.close();
		// dump.close();
		// DEBUG
		int headBlock = p.getInt(9);
		int headOff = p.getInt(13);
		line = new IndexRecord(fileName, headBlock, headOff, tx, true);
		IndexRecord prev = null;

		while (true)
		{
			// line is start rec
			int cmp = 0;
			if (!line.isStart())
			{
				cmp = compare(vals, line.getKeys(types));
				// DEBUG
				// HRDBMSWorker.logger.debug("Comparing " + vals + " to " +
				// line.getKeys(types));
				// HRDBMSWorker.logger.debug("Result is " + cmp);
				// DEBUG
				if (cmp > 0)
				{
					IndexRecord next = line.next(true);
					if (next.isNull())
					{
						IndexRecord down = line.getDown(true);
						if (down.isNull())
						{
							return;
						}
						else
						{
							line = down;
							// prev = null;
						}
					}
					else
					{
						prev = line;
						line = next;
					}
				}
				else if (cmp == 0)
				{
					if (isUnique())
					{
						while (true)
						{
							IndexRecord down = line.getDown(true);
							if (down.isNull())
							{
								return;
							}
							else
							{
								line = down;
							}
						}
					}
					else
					{
						IndexRecord down = prev.getDown(true);
						if (down.isNull())
						{
							return;
						}
						else
						{
							prev = down;
							line = down.next(true);
						}
					}
				}
				else
				{
					IndexRecord down = prev.getDown(true);
					if (down.isNull())
					{
						return;
					}
					else
					{
						prev = down;
						line = down.next(true);
					}
				}
			}
			else
			{
				IndexRecord next = line.next(true);
				if (next.isNull())
				{
					IndexRecord down = line.getDown(true);
					if (down.isNull())
					{
						return;
					}
					else
					{
						line = down;
						// prev = null;
					}
				}
				else
				{
					prev = line;
					line = next;
				}
			}
		}
	}

	private final void setFirstPosition() throws Exception
	{
		Block b = new Block(fileName, 0);
		tx.requestPage(b);
		Page p = tx.getPage(b);
		int headBlock = p.getInt(9);
		int headOff = p.getInt(13);
		line = new IndexRecord(fileName, headBlock, headOff, tx);

		while (!line.getDown().isNull())
		{
			line = line.getDown();
		}
	}

	private final void setFirstPosition(boolean xlock) throws Exception
	{
		Block b = new Block(fileName, 0);
		Page p = myPages.get(b);
		if (p == null)
		{
			LockManager.xLock(b, tx.number());
			tx.requestPage(b);
			p = tx.getPage(b);
			myPages.put(b, p);
		}
		int headBlock = p.getInt(9);
		int headOff = p.getInt(13);
		line = new IndexRecord(fileName, headBlock, headOff, tx, true);

		while (!line.getDown().isNull())
		{
			line = line.getDown(true);
		}
	}

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

		if (op.equals("E") && secondary.size() > 0 && (secondary.size() + 1) == keys.size())
		{
			boolean multiPos = true;
			for (Filter f2 : secondary)
			{
				if (f2.op().equals("E"))
				{
					if (f2.leftIsColumn() && !f2.rightIsColumn())
					{

					}
					else if (f2.rightIsColumn() && !f2.leftIsColumn())
					{

					}
					else
					{
						multiPos = false;
						break;
					}
				}
				else
				{
					multiPos = false;
					break;
				}
			}

			try
			{
				if (multiPos)
				{
					ArrayList<Object> pos = new ArrayList<Object>();
					ArrayList<Filter> combined = new ArrayList<Filter>();
					combined.add(f);
					combined.addAll(secondary);

					int i = 0;
					while (i < keys.size())
					{
						boolean found = false;
						for (Filter f2 : combined)
						{
							if (f2.leftIsColumn())
							{
								if (f2.leftColumn().equals(keys.get(i)))
								{
									val = f2.rightLiteral();
									orig = f2.rightOrig();
									terminates.add(new Filter(f2.leftColumn(), "NE", orig));
									if (val instanceof Integer && types.get(i).equals("LONG"))
									{
										pos.add(new Long((Integer)val));
									}
									else if (val instanceof Long && types.get(i).equals("INTEGER"))
									{
										pos.add(((Long)val).intValue());
									}
									else
									{
										pos.add(val);
									}

									found = true;
									break;
								}
							}
							else
							{
								if (f2.rightColumn().equals(keys.get(i)))
								{
									val = f2.leftLiteral();
									orig = f2.leftOrig();
									terminates.add(new Filter(f2.rightColumn(), "NE", orig));
									if (val instanceof Integer && types.get(i).equals("LONG"))
									{
										pos.add(new Long((Integer)val));
									}
									else if (val instanceof Long && types.get(i).equals("INTEGER"))
									{
										pos.add(((Long)val).intValue());
									}
									else
									{
										pos.add(val);
									}

									found = true;
									break;
								}
							}
						}

						if (!found)
						{
							throw new Exception("Trying to do multi-col position in index, but could not find column " + keys.get(i));
						}

						i++;
					}

					Index.this.setEqualsPosMulti(pos, false);
					return;
				}
			}
			catch (Exception e)
			{
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

				terminates.clear();
			}
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

	public class IndexRecord
	{
		private Transaction tx;
		private Page p;
		private Block b;
		private boolean isNull = false;
		private int off;
		private boolean isLeaf = false;;
		private boolean isTombstone = false;
		private boolean isStart = false;
		private byte[] keyBytes = null;
		private int scroll;

		public IndexRecord(Page p, int offset, byte[] key, Transaction tx) throws Exception
		{
			this.tx = tx;
			b = p.block();
			this.p = p;
			off = offset;

			byte type = p.get(off + 0);
			isLeaf = (type == 1);
			if (type == 2)
			{
				isLeaf = true;
				isTombstone = true;
			}

			if (type == 3)
			{
				isStart = true;
			}

			keyBytes = key;
		}

		public IndexRecord(String file, int block, int offset, Transaction tx) throws Exception
		{
			this.tx = tx;
			b = new Block(file, block);
			off = offset;
			LockManager.sLock(b, tx.number());
			tx.requestPage(b);
			try
			{
				p = tx.getPage(b);
			}
			catch (Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				throw e;
			}

			byte type = p.get(off + 0);
			isLeaf = (type == 1);
			if (type == 2)
			{
				isLeaf = true;
				isTombstone = true;
			}

			if (type == 3)
			{
				isStart = true;
			}
		}

		public IndexRecord(String file, int block, int offset, Transaction tx, boolean x) throws Exception
		{
			this.tx = tx;
			b = new Block(file, block);
			p = myPages.get(b);
			off = offset;

			if (p == null)
			{
				LockManager.xLock(b, tx.number());
				tx.requestPage(b);
				try
				{
					p = tx.getPage(b);
				}
				catch (Exception e)
				{
					HRDBMSWorker.logger.error("", e);
					throw e;
				}

				myPages.put(b, p);
			}

			byte type = p.get(off);
			isLeaf = (type == 1);
			if (type == 2)
			{
				isLeaf = true;
				isTombstone = true;
			}

			if (type == 3)
			{
				isStart = true;
			}
		}

		public IndexRecord(String file, int block, int offset, Transaction tx, boolean x, Page p) throws Exception
		{
			this.tx = tx;
			this.p = p;
			if (p == null)
			{
				throw new Exception("NULL page in IndexRecord Constructor");
			}
			this.b = p.block();
			off = offset;
			if (myPages.get(b) == null)
			{
				LockManager.xLock(b, tx.number());
				myPages.put(b, p);
			}

			byte type = p.get(off + 0);
			isLeaf = (type == 1);
			if (type == 2)
			{
				isLeaf = true;
				isTombstone = true;
			}

			if (type == 3)
			{
				isStart = true;
			}
		}

		public IndexRecord(String file, int block, int offset, Transaction tx, Page p) throws Exception
		{
			this.tx = tx;
			this.p = p;
			if (p == null)
			{
				throw new Exception("NULL page in IndexRecord Constructor");
			}
			this.b = p.block();
			off = offset;

			byte type = p.get(off + 0);
			isLeaf = (type == 1);
			if (type == 2)
			{
				isLeaf = true;
				isTombstone = true;
			}

			if (type == 3)
			{
				isStart = true;
			}
		}

		private IndexRecord()
		{
			isNull = true;
		}

		public int compareTo(ArrayList<Object> rhs) throws Exception
		{
			if (isStart())
			{
				return -1;
			}

			return compare(getKeys(types), rhs);
		}

		public int compareTo(ArrayList<Object> rhs, boolean debug) throws Exception
		{
			if (isStart())
			{
				return -1;
			}

			return compare(getKeys(types), rhs, true);
		}

		public int compareTo(IndexRecord rhs) throws Exception
		{
			if (isStart())
			{
				return -1;
			}

			return compare(getKeys(types), rhs.getKeys(types));
		}

		public IndexRecord getDown() throws Exception
		{
			if (isLeaf || isTombstone)
			{
				return new IndexRecord();
			}

			int block = p.getInt(off + 25);
			int offset = p.getInt(off + 29);

			if (block == 0 && offset == 0)
			{
				return new IndexRecord();
			}

			return new IndexRecord(fileName, block, offset, tx);
		}

		public IndexRecord getDown(boolean xlock) throws Exception
		{
			if (xlock)
			{
				if (isLeaf || isTombstone)
				{
					return new IndexRecord();
				}

				int block = p.getInt(off + 25);
				int offset = p.getInt(off + 29);

				if (block == 0 && offset == 0)
				{
					return new IndexRecord();
				}

				if (block != p.block().number())
				{
					return new IndexRecord(fileName, block, offset, tx, xlock);
				}
				else
				{
					return new IndexRecord(fileName, block, offset, tx, xlock, p);
				}
			}
			else
			{
				return next();
			}
		}

		public ArrayList<Object> getKeys(ArrayList<String> types) throws Exception
		{
			try
			{
				// DEBUG
				// if (types.size() == 0)
				// {
				// Exception e = new Exception("Empty types in index");
				// HRDBMSWorker.logger.debug("", e);
				// throw e;
				// }
				// DEBUG

				ArrayList<Object> retval = new ArrayList<Object>(types.size());
				int o = 0;
				if (isLeaf)
				{
					o = off + 41;
				}
				else
				{
					o = off + 33;
				}

				for (byte type : typesBytes)
				{
					o++; // null indicator
					if (type == 0)
					{
						final int temp = p.getInt(o);
						// if (temp < 0)
						// {
						// HRDBMSWorker.logger.debug("Read negativge int from index");
						// }
						retval.add(temp);
						o += 4;
					}
					else if (type == 1)
					{
						retval.add(p.getDouble(o));
						o += 8;
					}
					else if (type == 2)
					{
						int length = p.getInt(o);
						o += 4;
						char[] ca = new char[length];
						byte[] bytes = new byte[length];
						p.get(o, bytes);
						String value = (String)unsafe.allocateInstance(String.class);
						int clen = ((sun.nio.cs.ArrayDecoder)cd).decode(bytes, 0, length, ca);
						if (clen == ca.length)
						{
							unsafe.putObject(value, soffset, ca);
						}
						else
						{
							char[] v = Arrays.copyOf(ca, clen);
							unsafe.putObject(value, soffset, v);
						}
						retval.add(value);
						o += length;
					}
					else if (type == 3)
					{
						final long temp = p.getLong(o);
						// if (temp < 0)
						// {
						// HRDBMSWorker.logger.debug("Read negative long from index: "
						// + temp);
						// }
						retval.add(temp);
						o += 8;
					}
					else if (type == 4)
					{
						retval.add(new MyDate(p.getInt(o)));
						o += 4;
					}
					else
					{
						throw new Exception("Unknown type: " + type);
					}
				}

				return retval;
			}
			catch (Exception e)
			{
				HRDBMSWorker.logger.debug(toString());
				// DEBUG
				// RandomAccessFile dump = new RandomAccessFile("/tmp/dump",
				// "rw");
				// FileChannel fc = dump.getChannel();
				// p.buffer().position(0);
				// fc.write(p.buffer());
				// fc.close();
				// dump.close();
				// DEBUG
				throw e;
			}
		}

		public ArrayList<Object> getKeysAndScroll(ArrayList<String> types) throws Exception
		{
			try
			{
				ArrayList<Object> retval = new ArrayList<Object>(types.size());
				int o = 0;
				if (isLeaf)
				{
					o = off + 41;
				}
				else
				{
					o = off + 33;
				}

				for (byte type : typesBytes)
				{
					o++; // null indicator
					if (type == 0)
					{
						retval.add(p.getInt(o));
						o += 4;
					}
					else if (type == 1)
					{
						retval.add(p.getDouble(o));
						o += 8;
					}
					else if (type == 2)
					{
						int length = p.getInt(o);
						o += 4;
						char[] ca = new char[length];
						byte[] bytes = new byte[length];
						p.get(o, bytes);
						String value = (String)unsafe.allocateInstance(String.class);
						int clen = ((sun.nio.cs.ArrayDecoder)cd).decode(bytes, 0, length, ca);
						if (clen == ca.length)
						{
							unsafe.putObject(value, soffset, ca);
						}
						else
						{
							char[] v = Arrays.copyOf(ca, clen);
							unsafe.putObject(value, soffset, v);
						}
						retval.add(value);
						o += length;
					}
					else if (type == 3)
					{
						retval.add(p.getLong(o));
						o += 8;
					}
					else if (type == 4)
					{
						retval.add(new MyDate(p.getInt(o)));
						o += 4;
					}
					else
					{
						throw new Exception("Unknown type: " + type);
					}
				}

				scroll = o - off;
				return retval;
			}
			catch (Exception e)
			{
				HRDBMSWorker.logger.debug(toString());
				// DEBUG
				// RandomAccessFile dump = new RandomAccessFile("/tmp/dump",
				// "rw");
				// FileChannel fc = dump.getChannel();
				// p.buffer().position(0);
				// fc.write(p.buffer());
				// fc.close();
				// dump.close();
				// DEBUG
				throw e;
			}
		}

		public long getPartialRid()
		{
			return (((long)p.getInt(off + 33)) << 32) + p.getInt(off + 37);
		}

		public RID getRid()
		{
			return new RID(p.getInt(off + 25), p.getInt(off + 29), p.getInt(off + 33), p.getInt(off + 37));
		}

		public int getScroll()
		{
			return scroll;
		}

		public IndexRecord getUp() throws Exception
		{
			int block = p.getInt(off + 17);
			int offset = p.getInt(off + 21);

			if (block == 0 && offset == 0)
			{
				return new IndexRecord();
			}

			return new IndexRecord(fileName, block, offset, tx);
		}

		public IndexRecord getUp(boolean xlock) throws Exception
		{
			if (xlock)
			{
				int block = p.getInt(off + 17);
				int offset = p.getInt(off + 21);

				if (block == 0 && offset == 0)
				{
					return new IndexRecord();
				}

				if (block != p.block().number())
				{
					return new IndexRecord(fileName, block, offset, tx, xlock);
				}
				else
				{
					return new IndexRecord(fileName, block, offset, tx, xlock, p);
				}
			}
			else
			{
				return next();
			}
		}

		public boolean isLeaf()
		{
			return isLeaf;
		}

		public boolean isNull()
		{
			return isNull;
		}

		public boolean isStart()
		{
			return isStart;
		}

		public boolean isTombstone()
		{
			return isTombstone;
		}

		public byte[] keyBytes()
		{
			return keyBytes;
		}

		public boolean keysMatch(ArrayList<Object> keys) throws Exception
		{
			ArrayList<Object> r = getKeys(types);

			// RowComparator rc = new RowComparator(orders, types);
			return rc.compare(keys, r) == 0;
		}

		public void markTombstone() throws Exception
		{
			byte[] before = new byte[1];
			p.get(off, before);
			byte[] after = new byte[1];
			after[0] = (byte)2;
			InsertLogRec rec = tx.insert(before, after, off, p.block());
			p.write(off, after, tx.number(), rec.lsn());
		}

		public void markTombstoneNoLog() throws Exception
		{
			byte[] after = new byte[1];
			after[0] = (byte)2;
			p.write(off, after, tx.number(), LogManager.getLSN());
		}

		public IndexRecord next() throws Exception
		{
			int block = p.getInt(off + 9);
			int offset = p.getInt(off + 13);

			if (block == 0 && offset == 0)
			{
				return new IndexRecord();
			}

			return new IndexRecord(fileName, block, offset, tx);
		}

		public IndexRecord next(boolean xlock) throws Exception
		{
			if (xlock)
			{
				int block = p.getInt(off + 9);
				int offset = p.getInt(off + 13);

				if (block == 0 && offset == 0)
				{
					return new IndexRecord();
				}

				if (block != p.block().number())
				{
					return new IndexRecord(fileName, block, offset, tx, xlock);
				}
				else
				{
					return new IndexRecord(fileName, block, offset, tx, xlock, p);
				}
			}
			else
			{
				return next();
			}
		}

		public IndexRecord prev() throws Exception
		{
			int block = p.getInt(off + 1);
			int offset = p.getInt(off + 5);

			if (block == 0 && offset == 0)
			{
				return new IndexRecord();
			}

			return new IndexRecord(fileName, block, offset, tx);
		}

		public IndexRecord prev(boolean xlock) throws Exception
		{
			if (xlock)
			{
				int block = p.getInt(off + 1);
				int offset = p.getInt(off + 5);

				if (block == 0 && offset == 0)
				{
					return new IndexRecord();
				}

				if (block != p.block().number())
				{
					return new IndexRecord(fileName, block, offset, tx, xlock);
				}
				else
				{
					return new IndexRecord(fileName, block, offset, tx, xlock, p);
				}
			}
			else
			{
				return next();
			}
		}

		public void replaceRid(RID rid) throws Exception
		{
			ByteBuffer bb = ByteBuffer.allocate(16);
			bb.putInt(rid.getNode());
			bb.putInt(rid.getDevice());
			bb.putInt(rid.getBlockNum());
			bb.putInt(rid.getRecNum());
			byte[] before = new byte[16];
			p.get(off + 33, before);
			InsertLogRec rec = tx.insert(before, bb.array(), off + 33, p.block());
			p.write(off + 33, bb.array(), tx.number(), rec.lsn());
			// changedBlocks.add(p.block());
		}

		public boolean ridsMatch(RID rid)
		{
			RID myRid = new RID(p.getInt(off + 25), p.getInt(off + 29), p.getInt(off + 33), p.getInt(off + 37));
			return myRid.equals(rid);
		}

		public void setDown(IndexRecord ir) throws Exception
		{
			int block = ir.b.number();
			int o = ir.off;
			ByteBuffer after = ByteBuffer.allocate(8);
			after.putInt(block);
			after.putInt(o);
			byte[] before = new byte[8];
			p.get(off + 25, before);
			InsertLogRec rec = tx.insert(before, after.array(), off + 25, p.block());
			p.write(off + 25, after.array(), tx.number(), rec.lsn());
		}

		public void setDownNoLog(IndexRecord ir) throws Exception
		{
			int block = ir.b.number();
			int o = ir.off;
			ByteBuffer after = ByteBuffer.allocate(8);
			after.putInt(block);
			after.putInt(o);
			p.write(off + 25, after.array(), tx.number(), LogManager.getLSN());
		}

		public void setNext(IndexRecord ir) throws Exception
		{
			int block = ir.b.number();
			int o = ir.off;
			ByteBuffer after = ByteBuffer.allocate(8);
			after.putInt(block);
			after.putInt(o);
			byte[] before = new byte[8];
			p.get(off + 9, before);
			InsertLogRec rec = tx.insert(before, after.array(), off + 9, p.block());
			p.write(off + 9, after.array(), tx.number(), rec.lsn());
		}

		public void setNextNoLog(IndexRecord ir) throws Exception
		{
			int block = ir.b.number();
			int o = ir.off;
			ByteBuffer after = ByteBuffer.allocate(8);
			after.putInt(block);
			after.putInt(o);
			p.write(off + 9, after.array(), tx.number(), LogManager.getLSN());
		}

		public void setPrev(IndexRecord ir) throws Exception
		{
			int block = ir.b.number();
			int o = ir.off;
			ByteBuffer after = ByteBuffer.allocate(8);
			after.putInt(block);
			after.putInt(o);
			byte[] before = new byte[8];
			p.get(off + 1, before);
			InsertLogRec rec = tx.insert(before, after.array(), off + 1, p.block());
			p.write(off + 1, after.array(), tx.number(), rec.lsn());
		}

		public void setPrevNoLog(IndexRecord ir) throws Exception
		{
			int block = ir.b.number();
			int o = ir.off;
			ByteBuffer after = ByteBuffer.allocate(8);
			after.putInt(block);
			after.putInt(o);
			p.write(off + 1, after.array(), tx.number(), LogManager.getLSN());
		}

		public void setUp(IndexRecord ir) throws Exception
		{
			int block = ir.b.number();
			int o = ir.off;
			ByteBuffer after = ByteBuffer.allocate(8);
			after.putInt(block);
			after.putInt(o);
			byte[] before = new byte[8];
			p.get(off + 17, before);
			InsertLogRec rec = tx.insert(before, after.array(), off + 17, p.block());
			p.write(off + 17, after.array(), tx.number(), rec.lsn());
		}

		public void setUpNoLog(IndexRecord ir) throws Exception
		{
			int block = ir.b.number();
			int o = ir.off;
			ByteBuffer after = ByteBuffer.allocate(8);
			after.putInt(block);
			after.putInt(o);
			p.write(off + 17, after.array(), tx.number(), LogManager.getLSN());
		}

		@Override
		public String toString()
		{
			return "IsNull = " + isNull + "; IsLeaf = " + isLeaf + "; IsStart = " + isStart + "; Block = " + b + "; Off = " + off;
		}

		private IndexRecord insertInternalToRight(IndexRecord rec) throws Exception
		{
			IndexRecord right = next(true);
			IndexRecord newRec = writeNewInternal(rec);
			setNext(newRec);
			newRec.setPrev(this);

			if (!right.isNull())
			{
				newRec.setNext(right);
				right.setPrev(newRec);
			}

			newRec.setDown(rec);
			rec.setUp(newRec);
			return newRec;
		}

		private IndexRecord insertInternalToRightNoLog(IndexRecord rec) throws Exception
		{
			IndexRecord right = next(true);
			IndexRecord newRec = writeNewInternalNoLog(rec);
			setNextNoLog(newRec);
			newRec.setPrevNoLog(this);

			if (!right.isNull())
			{
				newRec.setNextNoLog(right);
				right.setPrevNoLog(newRec);
			}

			newRec.setDownNoLog(rec);
			rec.setUpNoLog(newRec);
			return newRec;
		}
	}

	private final class IndexWriterThread extends ThreadPoolThread
	{
		public IndexWriterThread(ArrayList<String> keys)
		{
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

							line = line.next();
							// count++;
							// if (count >= MAX_PAGES)
							// {
							// tx.checkpoint(fileName, line.b);
							// count = 0;
							// }
						}

						final ArrayList<Object> al = new ArrayList<Object>(1);
						al.add(getPartialRid());
						queue.put(al);

						line = line.next();
						// count++;
						// if (count >= MAX_PAGES)
						// {
						// tx.checkpoint(fileName, line.b);
						// count = 0;
						// }
					}
					catch (final Exception e)
					{
						HRDBMSWorker.logger.error("", e);
						try
						{
							queue.put(e);
						}
						catch (Exception f)
						{
						}
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

							line = line.next();
						}

						ridList.add(getPartialRid());
						ArrayList<Object> keys = line.getKeys(types);
						int z = 0;
						final int limit = fetches.size();
						// for (final int pos : fetches)
						while (z < limit)
						{
							final int pos = fetches.get(z++);
							final Object val = keys.get(pos);
							row.add(val);
						}

						line = line.next();
					}
					catch (final Exception e)
					{
						HRDBMSWorker.logger.error("", e);
						try
						{
							queue.put(e);
						}
						catch (Exception f)
						{
						}
						return;
					}
					try
					{
						queue.put(row.clone());
					}
					catch (Exception e)
					{
						HRDBMSWorker.logger.error("", e);
						try
						{
							queue.put(e);
						}
						catch (Exception f)
						{
						}
						return;
					}
				}
			}
		}
	}
}
