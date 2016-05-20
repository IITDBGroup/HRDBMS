package com.exascale.tables;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.LockSupport;
import com.exascale.exceptions.LockAbortException;
import com.exascale.exceptions.RecNumOverflowException;
import com.exascale.filesystem.Block;
import com.exascale.filesystem.Page;
import com.exascale.filesystem.RID;
import com.exascale.logging.InsertLogRec;
import com.exascale.logging.LogRec;
import com.exascale.managers.BufferManager;
import com.exascale.managers.FileManager;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.managers.LockManager;
import com.exascale.managers.LogManager;
import com.exascale.misc.DataEndMarker;
import com.exascale.misc.MyDate;
import com.exascale.misc.SPSCQueue;
import com.exascale.optimizer.TableScanOperator;
import com.exascale.threads.HRDBMSThread;
import com.exascale.threads.ReadThread;

public class Schema
{
	private static final int ROWS_TO_ALLOCATE = (int)(1024 * (Page.BLOCK_SIZE * 1.0) / (128.0 * 1024.0));
	private static final int ROWS_TO_ALLOCATE_COL = (int)(5500 * (Page.BLOCK_SIZE * 1.0) / (128.0 * 1024.0));
	public static final byte TYPE_ROW = 0, TYPE_COL = 1;
	private static Charset cs = StandardCharsets.UTF_8;
	private static sun.misc.Unsafe unsafe;
	private static long offset;
	static
	{
		try
		{
			final Field f = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
			f.setAccessible(true);
			unsafe = (sun.misc.Unsafe)f.get(null);
			final Field fieldToUpdate = String.class.getDeclaredField("value");
			// get unsafe offset to this field
			offset = unsafe.objectFieldOffset(fieldToUpdate);
		}
		catch (final Exception e)
		{
			unsafe = null;
		}
	}

	private volatile SPSCQueue iterCache;

	private Map<Integer, DataType> colTypes;
	private int nextRecNum;
	private int headEnd;
	private int dataStart;
	private long modTime;
	private byte blockType;
	private final int colIDListOff = 24;
	private int rowIDListOff;
	private int offArrayOff;
	private int[] colIDs;
	private int colIDListSize;
	private int rowIDListSize;
	private RID[] rowIDs;
	private int[][] offsetArray;
	private int offsetArrayRows;
	private Page p;
	private Map<Integer, Integer> colIDToIndex = new HashMap<Integer, Integer>();
	private Map<RID, Integer> rowIDToIndex = null;
	private Transaction tx;
	private volatile int myDev = -1;
	private volatile int myNode = -1;
	private boolean rowIDsSet = false;
	private final CharsetDecoder cd = cs.newDecoder();
	private int nodeNumber = -2;
	private int deviceNumber = -2;
	private TreeMap<Integer, Page> pageGroup = new TreeMap<Integer, Page>();
	private boolean addOpen = true;
	private ArrayList<Page> pPrev;
	private ConcurrentHashMap<RID, ArrayList<FieldValue>> recCache;
	private HashMap<Integer, int[][]> offsetArraySet;
	private HashMap<Integer, Map<RID, Integer>> rowIDToIndexSet;
	private HashMap<Integer, ArrayList<RID>> rowIDSet;
	private boolean fixed = false;
	private ArrayList<Byte> headerBytes;
	private ArrayList<Integer> colOrder;
	private ArrayList<RID> rowIDsAL;
	private int cachedLenLen = -1;
	private TreeSet<RID> copy = null;
	
	public Schema clone()
	{
		Schema retval = new Schema(colTypes);
		retval.nextRecNum = nextRecNum;
		retval.headEnd = headEnd;
		retval.dataStart = dataStart;
		retval.modTime = modTime;
		retval.blockType = blockType;
		retval.rowIDListOff = rowIDListOff;
		retval.offArrayOff = offArrayOff;
		retval.colIDs = colIDs;
		retval.colIDListSize = colIDListSize;
		retval.rowIDListSize = rowIDListSize;
		retval.rowIDs = rowIDs;
		retval.offsetArray = offsetArray;
		retval.offsetArrayRows = offsetArrayRows;
		retval.p = p;
		retval.colIDToIndex = colIDToIndex;
		retval.rowIDToIndex = rowIDToIndex;
		retval.tx = tx;
		retval.myDev = myDev;
		retval.myNode = myNode;
		retval.rowIDsSet = rowIDsSet;
		retval.nodeNumber = nodeNumber;
		retval.deviceNumber = deviceNumber;
		retval.pageGroup = pageGroup;
		retval.addOpen = addOpen;
		retval.pPrev = pPrev;
		retval.recCache = recCache;
		retval.offsetArraySet = offsetArraySet;
		retval.rowIDToIndexSet = rowIDToIndexSet;
		retval.rowIDSet = rowIDSet;
		retval.fixed = fixed;
		retval.headerBytes = headerBytes;
		retval.colOrder = colOrder;
		retval.rowIDsAL = rowIDsAL;
		retval.cachedLenLen = cachedLenLen;
		retval.copy = copy;
		return retval;
	}
	
	public void makeNull()
	{
		colIDs = null;
		rowIDs = null;
		offsetArray = null;
		p = null;
		colIDToIndex = null;
		rowIDToIndex = null;
		tx = null;
		pageGroup = null;
		pPrev = null;
		recCache = null;
		offsetArraySet = null;
		rowIDToIndexSet = null;
		rowIDSet = null;
		rowIDsAL = null;
		copy = null;
	}

	public Schema(Map<Integer, DataType> map)
	{
		colTypes = map;
	}

	public Schema(Map<Integer, DataType> map, int nodeNumber, int deviceNumber)
	{
		colTypes = map;
		if (nodeNumber < 0)
		{
			nodeNumber = -1;
		}
		this.nodeNumber = nodeNumber;
		this.deviceNumber = deviceNumber;
	}

	private static void putMedium(ByteBuffer bb, int val)
	{
		bb.put((byte)((val & 0xff0000) >> 16));
		bb.put((byte)((val & 0xff00) >> 8));
		bb.put((byte)(val & 0xff));
	}

	public void add(int colNum, Page p)
	{
		if (!fixed)
		{
			HashMap<Integer, DataType> newMap = new HashMap<Integer, DataType>();
			for (Map.Entry entry : colTypes.entrySet())
			{
				int key = (int)entry.getKey();
				DataType type = (DataType)entry.getValue();
				if (type.getType() == DataType.VARCHAR)
				{
					type = new DataType(DataType.CVARCHAR, type.getLength(), type.getScale());
				}

				newMap.put(key, type);
			}

			colTypes = newMap;
			fixed = true;
		}

		if (!addOpen)
		{
			pPrev = new ArrayList<Page>(pageGroup.values());
			pageGroup.clear();
			addOpen = true;
		}

		pageGroup.put(colNum, p);
	}

	public void close() throws Exception
	{
		final byte[] after = new byte[30 + (rowIDListSize * 12) + (colIDListSize * 3) + (3 * rowIDListSize * colIDListSize)];

		// copy(after,0,
		// ByteBuffer.allocate(4).putInt(this.nextRecNum).array()); //nextRecNum
		ByteBuffer x = ByteBuffer.allocate(3);
		x.position(0);
		if (nextRecNum == -1)
		{
			nextRecNum = p.getMedium(1);
		}
		// x.putInt(this.nextRecNum);
		putMedium(x, this.nextRecNum);
		System.arraycopy(x.array(), 0, after, 0, 3);
		// copy(after,4, ByteBuffer.allocate(4).putInt(before.length -
		// 1).array()); //headEnd
		x.position(0);
		putMedium(x, after.length - 1);
		System.arraycopy(x.array(), 0, after, 3, 3);
		// copy(after,8, ByteBuffer.allocate(4).putInt(this.dataStart).array());
		// //dataStart
		x.position(0);
		if (dataStart == -1)
		{
			dataStart = p.getMedium(7);
		}
		putMedium(x, this.dataStart);
		System.arraycopy(x.array(), 0, after, 6, 3);
		// copy(after,12, ByteBuffer.allocate(8).putLong(this.modTime).array());
		// //modTime
		if (modTime == -1)
		{
			modTime = p.getLong(10);
		}
		System.arraycopy(ByteBuffer.allocate(8).putLong(this.modTime).array(), 0, after, 9, 8);
		// copy(after,32,
		// ByteBuffer.allocate(4).putInt(freeSpaceListEntries).array());
		x.position(0);
		putMedium(x, 27 + colIDListSize * 3);
		System.arraycopy(x.array(), 0, after, 17, 3);
		// copy(after, i, ByteBuffer.allocate(4).putInt(colIDListSize).array());
		x.position(0);
		putMedium(x, 30 + colIDListSize * 3 + 12 * rowIDListSize);
		System.arraycopy(x.array(), 0, after, 20, 3);
		x.position(0);
		putMedium(x, colIDListSize);
		System.arraycopy(x.array(), 0, after, 23, 3);
		int i = 26;

		for (final int id : colIDs)
		{
			// copy(after, i, ByteBuffer.allocate(4).putInt(id).array());
			x.position(0);
			putMedium(x, id);
			System.arraycopy(x.array(), 0, after, i, 3);
			i += 3;
		}

		x.position(0);
		putMedium(x, rowIDListSize);
		System.arraycopy(x.array(), 0, after, i, 3);
		i += 3;

		int a = 0;
		if (!rowIDsSet)
		{
			setRowIDs();
		}
		for (final RID id : rowIDs)
		{
			if (a >= rowIDListSize)
			{
				break;
			}
			// copy(after, i,
			// ByteBuffer.allocate(4).putInt(id.getNode()).array());
			x.position(0);
			putMedium(x, id.getNode());
			System.arraycopy(x.array(), 0, after, i, 3);
			// copy(after, i+4,
			// ByteBuffer.allocate(4).putInt(id.getDevice()).array());
			x.position(0);
			putMedium(x, id.getDevice());
			System.arraycopy(x.array(), 0, after, i + 3, 3);
			// copy(after, i+8,
			// ByteBuffer.allocate(4).putInt(id.getBlockNum()).array());
			x.position(0);
			putMedium(x, id.getBlockNum());
			System.arraycopy(x.array(), 0, after, i + 6, 3);
			// copy(after, i+12,
			// ByteBuffer.allocate(4).putInt(id.getRecNum()).array());
			x.position(0);
			putMedium(x, id.getRecNum());
			System.arraycopy(x.array(), 0, after, i + 9, 3);
			i += 12;
		}

		int k = 0;
		while (k < offsetArrayRows)
		{
			int[] row = offsetArray[k];
			ByteBuffer temp = ByteBuffer.allocate(row.length * 3);
			int z = 0;
			while (z < row.length)
			{
				putMedium(temp, row[z++]);
			}
			System.arraycopy(temp.array(), 0, after, i, 3 * row.length);
			i += (3 * row.length);
			k++;
		}

		long lsn = LogManager.getLSN();
		p.write(1, after, tx.number(), lsn);
	}

	public ColIterator colIterator()
	{
		return new ColIterator();
	}

	public Iterator<ArrayList<FieldValue>> colTableIterator()
	{
		return recCache.values().iterator();
	}

	public Iterator<Entry<RID, ArrayList<FieldValue>>> colTableIteratorWithRIDs()
	{
		return recCache.entrySet().iterator();
	}

	public void deleteRow(RID id) throws LockAbortException, Exception
	{
		//TableScanOperator.noResults.remove(p.block());
		byte[] before;
		byte[] after;
		int off;
		int index = -1;
		if (rowIDToIndex == null)
		{
			rowIDToIndex = new HashMap<RID, Integer>();
			int i = 0;
			if (!rowIDsSet)
			{
				setRowIDs();
			}
			for (final RID rid : rowIDs)
			{
				if (i >= rowIDListSize)
				{
					break;
				}
				rowIDToIndex.put(rid, i);
				i++;
			}
		}
		try
		{
			index = rowIDToIndex.get(id);
		}
		catch (Exception e)
		{
			HRDBMSWorker.logger.debug("Looking for rid = " + id + " in " + rowIDToIndex, e);
			throw e;
		}
		off = offArrayOff + index * (colIDListSize * 3);
		before = new byte[colIDListSize * 3];
		p.get(off, before);
		after = new byte[colIDListSize * 3];
		int i = 0;
		final int l1 = after.length;
		while (i < l1)
		{
			after[i] = (byte)0xFF;
			i++;
		}

		i = 0;
		while (i < colIDListSize)
		{
			offsetArray[index][i] = -1;
			i++;
		}

		LogRec rec = tx.delete(before, after, off, p.block()); // write log rec
		p.write(off, after, tx.number(), rec.lsn());

		// modification timestamp
		off = 10;
		before = new byte[8];
		after = new byte[8];
		p.get(10, before);
		modTime = System.currentTimeMillis();
		after = ByteBuffer.allocate(8).putLong(modTime).array();
		rec = tx.delete(before, after, off, p.block());
		p.write(off, after, tx.number(), rec.lsn());
	}

	public void deleteRowColTable(RID id) throws LockAbortException, Exception
	{
		// TableScanOperator.noResults.remove(p.block());
		for (Map.Entry entry : pageGroup.entrySet())
		{
			int colNum = (int)entry.getKey();
			this.p = (Page)entry.getValue();
			byte[] before;
			byte[] after;
			int off;
			int index = -1;
			rowIDToIndex = rowIDToIndexSet.get(colNum);
			try
			{
				index = rowIDToIndex.get(id);
			}
			catch (Exception e)
			{
				HRDBMSWorker.logger.debug("Looking for rid = " + id + " in " + rowIDToIndex, e);
				throw e;
			}
			offsetArray = offsetArraySet.get(colNum);
			offArrayOff = 4 + (rowIDToIndex.size() * 3);
			off = offArrayOff + (index * 3);
			before = new byte[3];
			p.get(off, before);
			after = new byte[3];
			int i = 0;
			final int l1 = after.length;
			while (i < l1)
			{
				after[i] = (byte)0xFF;
				i++;
			}

			offsetArray[index][0] = -1;

			LogRec rec = tx.delete(before, after, off, p.block()); // write log
																	// rec
			p.write(off, after, tx.number(), rec.lsn());
		}
	}

	public void deleteRowColTableNoLog(RID id) throws LockAbortException, Exception
	{
		// TableScanOperator.noResults.remove(p.block());
		for (Map.Entry entry : pageGroup.entrySet())
		{
			int colNum = (int)entry.getKey();
			this.p = (Page)entry.getValue();
			byte[] after;
			int off;
			int index = -1;
			rowIDToIndex = rowIDToIndexSet.get(colNum);
			try
			{
				index = rowIDToIndex.get(id);
			}
			catch (Exception e)
			{
				HRDBMSWorker.logger.debug("Looking for rid = " + id + " in " + rowIDToIndex, e);
				throw e;
			}
			offsetArray = offsetArraySet.get(colNum);
			offArrayOff = 4 + (rowIDToIndex.size() * 3);
			off = offArrayOff + (index * 3);
			after = new byte[3];
			int i = 0;
			final int l1 = after.length;
			while (i < l1)
			{
				after[i] = (byte)0xFF;
				i++;
			}

			offsetArray[index][0] = -1;
			p.write(off, after, tx.number(), LogManager.getLSN());
		}
	}

	public void deleteRowNoLog(RID id) throws LockAbortException, Exception
	{
		//TableScanOperator.noResults.remove(p.block());
		byte[] after;
		int off;
		if (rowIDToIndex == null)
		{
			rowIDToIndex = new HashMap<RID, Integer>();
			int i = 0;
			if (!rowIDsSet)
			{
				setRowIDs();
			}
			for (final RID rid : rowIDs)
			{
				if (i >= rowIDListSize)
				{
					break;
				}
				rowIDToIndex.put(rid, i);
				i++;
			}
		}
		final int index = rowIDToIndex.get(id);
		off = offArrayOff + index * (colIDListSize * 3);
		after = new byte[colIDListSize * 3];
		int i = 0;
		final int l2 = after.length;
		while (i < l2)
		{
			after[i] = (byte)0xFF;
			i++;
		}

		i = 0;
		while (i < colIDListSize)
		{
			offsetArray[index][i] = -1;
			i++;
		}

		long lsn = LogManager.getLSN();
		p.write(off, after, tx.number(), lsn);

		// modification timestamp
		off = 10;
		after = new byte[8];
		modTime = System.currentTimeMillis();
		after = ByteBuffer.allocate(8).putLong(modTime).array();
		lsn = LogManager.getLSN();
		p.write(off, after, tx.number(), lsn);
	}

	public void dummyRead(Transaction tx, Page p) throws Exception
	{
		if (this.p != null)
		{
			new UnpinThread(this.p, tx.number()).run();
		}
		this.p = p;
		this.tx = tx;
		blockType = p.get(0);
	}

	public Col getCol(int id)
	{
		return new Col(colIDToIndex.get(id));
	}

	public int[] getColIDs()
	{
		return colIDs;
	}

	public FieldValue getField(int rowIndex, int colIndex) throws Exception
	{
		DataType dt = null;
		int type;

		try
		{
			dt = colTypes.get(colIDs[colIndex]);
			type = dt.getType();
		}
		catch (Exception e)
		{
			HRDBMSWorker.logger.warn("Unable to retrieve field", e);
			HRDBMSWorker.logger.warn("We are on row " + rowIndex);
			HRDBMSWorker.logger.warn("Looking for colIndex " + colIndex);
			HRDBMSWorker.logger.warn("The colIDs array is ");
			int i = 0;
			while (i < colIDs.length)
			{
				HRDBMSWorker.logger.warn("" + colIDs[i]);
				i++;
			}

			HRDBMSWorker.logger.warn("ColTypes is " + colTypes);
			throw e;
		}

		if (type == DataType.BIGINT)
		{
			return new BigintFV(rowIndex, colIndex, this);
		}

		if (type == DataType.BINARY)
		{
			return new BinaryFV(rowIndex, colIndex, dt.getLength(), this);
		}

		if (type == DataType.DATE)
			// TODO || type == DataType.TIME || type == DataType.TIMESTAMP)
		{
			return new DateFV(rowIndex, colIndex, this);
		}

		if (type == DataType.DECIMAL)
		{
			return new DecimalFV(rowIndex, colIndex, dt.getLength(), dt.getScale(), this);
		}

		if (type == DataType.DOUBLE)
		{
			return new DoubleFV(rowIndex, colIndex, this);
		}

		if (type == DataType.FLOAT)
		{
			return new FloatFV(rowIndex, colIndex, this);
		}

		if (type == DataType.INTEGER)
		{
			return new IntegerFV(rowIndex, colIndex, this);
		}

		if (type == DataType.SMALLINT)
		{
			return new SmallintFV(rowIndex, colIndex, this);
		}

		if (type == DataType.VARBINARY)
		{
			return new VarbinaryFV(rowIndex, colIndex, this);
		}

		if (type == DataType.VARCHAR)
		{
			return new VarcharFV(rowIndex, colIndex, this);
		}

		if (type == DataType.CVARCHAR)
		{
			CVarcharFV retval = new CVarcharFV(rowIndex, colIndex, this);
			
			//if (p.block().number() == 2)
			//{
			//	HRDBMSWorker.logger.debug("Returned value is: " + retval.value);
			//}
		
			return retval;
		}

		HRDBMSWorker.logger.error("Unknown data type in Schema.getField()");
		return null;
	}

	public Page getPage()
	{
		return p;
	}

	public Row getRow(RID id)
	{
		try
		{
			if (rowIDToIndex == null)
			{
				rowIDToIndex = new HashMap<RID, Integer>();
				int i = 0;
				if (!rowIDsSet)
				{
					setRowIDs();
				}
				for (final RID rid : rowIDs)
				{
					if (i >= rowIDListSize)
					{
						break;
					}
					rowIDToIndex.put(rid, i);
					i++;
				}
			}
			Row row = new Row(rowIDToIndex.get(id));
			return row;
		}
		catch (Exception e)
		{
			HRDBMSWorker.logger.debug("", e);
			HRDBMSWorker.logger.debug("Failed to find a row by RID");
			HRDBMSWorker.logger.debug("We were looking for " + id);
			HRDBMSWorker.logger.debug("The page contains " + rowIDToIndex);
			return null;
		}
	}

	public ArrayList<FieldValue> getRowForColTable(RID rid) throws Exception
	{
		ArrayList<FieldValue> alfv = new ArrayList<FieldValue>();

		for (Map.Entry entry2 : pageGroup.entrySet())
		{
			int colNum = (int)entry2.getKey();
			this.p = (Page)entry2.getValue();
			cachedLenLen = -1;
			rowIDToIndex = rowIDToIndexSet.get(colNum);
			offsetArray = offsetArraySet.get(colNum);
			int index = rowIDToIndex.get(rid);
			Map<Integer, DataType> colTypesBackup = colTypes;
			colTypes = new HashMap<Integer, DataType>();
			colTypes.put(0, colTypesBackup.get(colNum));
			Row row2 = new Row(index);
			FieldValue fv = row2.getCol(0);
			colTypes = colTypesBackup;
			if (fv.exists())
			{
				// TODO - handle nulls
				alfv.add(fv);
			}
			else
			{
				return null;
			}
		}

		return alfv;
	}

	public RID insertRow(FieldValue[] vals) throws LockAbortException, UnsupportedEncodingException, IOException, RecNumOverflowException, Exception
	{
		int length = 0;
		for (final FieldValue val : vals)
		{
			length += val.size();
		}

		final Integer off = hasEnoughSpace(headerGrowthPerRow(), length);
		if (off != null)
		{
			TableScanOperator.noResults.remove(p.block());
			int node;
			if (nodeNumber == -2)
			{
				node = getNodeNumber(); // from 1st header page
			}
			else
			{
				node = nodeNumber;
			}

			int dev;
			if (deviceNumber == -2)
			{
				dev = getDeviceNumber(); // from 1st header page
			}
			else
			{
				dev = deviceNumber;
			}

			if (nextRecNum == -1)
			{
				nextRecNum = p.getMedium(1);
			}
			final RID rid = new RID(node, dev, p.block().number(), nextRecNum);
			final byte[] before = new byte[length];
			p.get(off, before);
			final byte[] after = new byte[length];
			int i = 0;
			int index = 0;
			while (index < colIDListSize)
			{
				i += vals[index].write(after, i);
				index++;
			}

			LogRec rec = tx.insert(before, after, off, p.block());
			p.write(off, after, tx.number(), rec.lsn());
			updateHeader(rid, vals, nextRecNum, off, length); // update
			// nextRecNum,
			// freeSpace,
			// nullArray,
			// rowIDList,
			// and
			// offsetArray
			// internal
			// and
			// external
			return rid;
		}
		else
		{
			final Block b = addNewBlock(p.block().fileName(), colIDs);
			tx.requestPage(b);
			final Schema newPage = new Schema(this.colTypes, nodeNumber, deviceNumber);
			tx.read(b, newPage, true);
			RID rid = newPage.insertRow(vals);
			newPage.close();
			return rid;
		}
	}

	public RID insertRowAppend(FieldValue[] vals) throws LockAbortException, UnsupportedEncodingException, IOException, RecNumOverflowException, Exception
	{
		int length = 0;
		for (final FieldValue val : vals)
		{
			length += val.size();
		}

		final Integer off = hasEnoughSpace(headerGrowthPerRow(), length);
		if (off != null)
		{
			TableScanOperator.noResults.remove(p.block());
			int node;
			if (nodeNumber == -2)
			{
				node = getNodeNumber(); // from 1st header page
			}
			else
			{
				node = nodeNumber;
			}

			int dev;
			if (deviceNumber == -2)
			{
				dev = getDeviceNumber(); // from 1st header page
			}
			else
			{
				dev = deviceNumber;
			}

			if (nextRecNum == -1)
			{
				nextRecNum = p.getMedium(1);
			}
			final RID rid = new RID(node, dev, p.block().number(), nextRecNum);
			final byte[] after = new byte[length];
			int i = 0;
			int index = 0;
			while (index < colIDListSize)
			{
				i += vals[index].write(after, i);
				index++;
			}

			long lsn = LogManager.getLSN();
			p.write(off, after, tx.number(), lsn);
			updateHeaderNoLog(rid, vals, nextRecNum, off, length); // update
			// nextRecNum,
			// freeSpace,
			// nullArray,
			// rowIDList,
			// and
			// offsetArray
			// internal
			// and
			// external
			return rid;
		}
		else
		{
			final Block b = addNewBlockNoLog(p.block().fileName(), colIDs);
			tx.requestPage(b);
			final Schema newPage = new Schema(this.colTypes, nodeNumber, deviceNumber);
			tx.read(b, newPage, true);
			RID rid = newPage.insertRowAppend(vals);
			newPage.close();
			return rid;
		}
	}

	public RID insertRowColTable(FieldValue[] vals) throws Exception
	{
		final ArrayList<Integer> offs = hasEnoughSpace(vals);
		if (offs != null)
		{
			TableScanOperator.noResults.remove(p.block());
			int node;
			if (nodeNumber == -2)
			{
				node = getNodeNumber(); // from 1st header page
			}
			else
			{
				node = nodeNumber;
			}

			int dev;
			if (deviceNumber == -2)
			{
				dev = getDeviceNumber(); // from 1st header page
			}
			else
			{
				dev = deviceNumber;
			}

			RID rid = null;
			if (Transaction.reorder)
			{
				ArrayList<Integer> colOrder = this.getColOrder();
				int first = colOrder.get(0);
				rowIDsAL = rowIDSet.get(first);
				this.p = pageGroup.get(first);
				cachedLenLen = -1;
				nextRecNum = 0;

				if (rowIDsAL.size() > 0)
				{
					RID last = rowIDsAL.get(rowIDsAL.size() - 1);
					nextRecNum = last.getRecNum() + 1;
				}

				rid = new RID(node, dev, p.block().number(), nextRecNum);
				rowIDsAL = rowIDSet.get(0);
				this.p = pageGroup.get(0);
				cachedLenLen = -1;
			}
			else
			{
				rowIDsAL = rowIDSet.get(0);
				this.p = pageGroup.get(0);
				cachedLenLen = -1;
				nextRecNum = 0;

				if (rowIDsAL.size() > 0)
				{
					RID last = rowIDsAL.get(rowIDsAL.size() - 1);
					nextRecNum = last.getRecNum() + 1;
				}

				rid = new RID(node, dev, p.block().number(), nextRecNum);
			}
			
			int length = vals[0].size();
			int off = offs.get(0);
			byte[] before = new byte[length];
			p.get(off, before);
			byte[] after = new byte[length];

			if (vals[0] instanceof CVarcharFV)
			{
				((CVarcharFV)vals[0]).write(after, 0, this);
			}
			else
			{
				vals[0].write(after, 0);
			}

			LogRec rec = tx.insert(before, after, off, p.block());
			p.write(off, after, tx.number(), rec.lsn());

			int offArrayLen = rowIDsAL.size() * 3;
			after = new byte[offArrayLen];
			int offArrayOff = 4 + (rowIDsAL.size() * 3);
			p.get(offArrayOff, after);
			before = new byte[offArrayLen];
			p.get(offArrayOff + 3, before);
			rec = tx.insert(before, after, offArrayOff + 3, p.block());
			p.write(offArrayOff + 3, after, tx.number(), rec.lsn());
			before = new byte[3];
			off = 7 + (rowIDsAL.size() * 6);
			p.get(off, before);
			after = new byte[3];
			ByteBuffer bb = ByteBuffer.wrap(after);
			bb.position(0);
			putMedium(bb, offs.get(0));
			rec = tx.insert(before, after, off, p.block());
			p.write(off, after, tx.number(), rec.lsn());

			before = new byte[3];
			off = 4 + (rowIDsAL.size() * 3);
			p.get(off, before);
			after = new byte[3];
			bb = ByteBuffer.wrap(after);
			bb.position(0);
			putMedium(bb, rid.getRecNum());
			//bb.putShort((short)rid.getRecNum());
			// System.arraycopy(after, 0, after, 9, 3);
			// bb.position(0);
			// putMedium(bb, rid.getBlockNum());
			// System.arraycopy(after, 0, after, 6, 3);
			// bb.position(0);
			// putMedium(bb, rid.getDevice());
			// System.arraycopy(after, 0, after, 3, 3);
			// bb.position(0);
			// putMedium(bb, rid.getNode());
			rec = tx.insert(before, after, off, p.block());
			p.write(off, after, tx.number(), rec.lsn());

			before = new byte[3];
			after = new byte[3];
			off = 1;
			p.get(off, before);
			bb = ByteBuffer.wrap(after);
			bb.position(0);
			putMedium(bb, rowIDsAL.size() + 1);
			rec = tx.insert(before, after, off, p.block());
			p.write(off, after, tx.number(), rec.lsn());
			rowIDsAL.add(rid);
			offsetArrayRows++;
			rowIDListSize++;
			offsetArray = offsetArraySet.get(0);
			if (offsetArray.length == rowIDsAL.size() - 1)
			{
				int[][] newOA = new int[offsetArray.length * 2][1];
				System.arraycopy(offsetArray, 0, newOA, 0, offsetArray.length);
				newOA[rowIDsAL.size() - 1][0] = offs.get(0);
				offsetArraySet.put(0, newOA);
			}
			else
			{
				offsetArray[rowIDsAL.size() - 1][0] = offs.get(0);
			}

			rowIDToIndex = rowIDToIndexSet.get(0);
			rowIDToIndex.put(rid, rowIDsAL.size() - 1);

			int pos = 0;
			for (FieldValue fv : vals)
			{
				if (pos == 0)
				{
					pos++;
					continue;
				}

				this.p = pageGroup.get(pos);
				cachedLenLen = -1;
				rowIDsAL = rowIDSet.get(pos);
				length = vals[pos].size();
				off = offs.get(pos);
				before = new byte[length];
				p.get(off, before);
				after = new byte[length];

				if (vals[pos] instanceof CVarcharFV)
				{
					((CVarcharFV)vals[pos]).write(after, 0, this);
				}
				else
				{
					vals[pos].write(after, 0);
				}

				rec = tx.insert(before, after, off, p.block());
				p.write(off, after, tx.number(), rec.lsn());

				offArrayLen = rowIDsAL.size() * 3;
				after = new byte[offArrayLen];
				offArrayOff = 4 + (rowIDsAL.size() * 3);
				p.get(offArrayOff, after);
				before = new byte[offArrayLen];
				p.get(offArrayOff + 3, before);
				rec = tx.insert(before, after, offArrayOff + 3, p.block());
				p.write(offArrayOff + 3, after, tx.number(), rec.lsn());
				before = new byte[3];
				off = 7 + (rowIDsAL.size() * 6);
				p.get(off, before);
				after = new byte[3];
				bb = ByteBuffer.wrap(after);
				bb.position(0);
				putMedium(bb, offs.get(pos));
				//o = (short)((offs.get(pos) >>> 1) & 0xffff);
				//bb.putShort(o);
				rec = tx.insert(before, after, off, p.block());
				p.write(off, after, tx.number(), rec.lsn());

				before = new byte[3];
				off = 4 + (rowIDsAL.size() * 3);
				p.get(off, before);
				after = new byte[3];
				bb = ByteBuffer.wrap(after);
				bb.position(0);
				putMedium(bb, rid.getRecNum());
				//bb.putShort((short)rid.getRecNum());
				// System.arraycopy(after, 0, after, 9, 3);
				// bb.position(0);
				// putMedium(bb, rid.getBlockNum());
				// System.arraycopy(after, 0, after, 6, 3);
				// bb.position(0);
				// putMedium(bb, rid.getDevice());
				// System.arraycopy(after, 0, after, 3, 3);
				// bb.position(0);
				// putMedium(bb, rid.getNode());
				rec = tx.insert(before, after, off, p.block());
				p.write(off, after, tx.number(), rec.lsn());

				before = new byte[3];
				after = new byte[3];
				off = 1;
				p.get(off, before);
				bb = ByteBuffer.wrap(after);
				bb.position(0);
				putMedium(bb, rowIDsAL.size() + 1);
				rec = tx.insert(before, after, off, p.block());
				p.write(off, after, tx.number(), rec.lsn());
				rowIDsAL.add(rid);
				offsetArray = offsetArraySet.get(pos);
				if (offsetArray.length == rowIDsAL.size() - 1)
				{
					int[][] newOA = new int[offsetArray.length * 2][1];
					System.arraycopy(offsetArray, 0, newOA, 0, offsetArray.length);
					newOA[rowIDsAL.size() - 1][0] = offs.get(pos);
					offsetArraySet.put(pos, newOA);
				}
				else
				{
					offsetArray[rowIDsAL.size() - 1][0] = offs.get(pos);
				}

				rowIDToIndex = rowIDToIndexSet.get(pos);
				rowIDToIndex.put(rid, rowIDsAL.size() - 1);
				pos++;
			}
			return rid;
		}
		else
		{
			LockManager.xLock(new Block(p.block().fileName(), -1), tx.number());
			// allocate new set
			final Block b = addNewBlockColTable(p.block().fileName());
			tx.requestPage(b, new ArrayList<Integer>(pageGroup.keySet()));
			final Schema newPage = new Schema(this.colTypes, nodeNumber, deviceNumber);
			tx.read(b, newPage, new ArrayList<Integer>(pageGroup.keySet()), false, true);
			RID rid = newPage.insertRowColTable(vals);
			return rid;
		}
	}

	public RID insertRowColTableAppend(FieldValue[] vals) throws Exception
	{
		final ArrayList<Integer> offs = hasEnoughSpace(vals);
		if (offs != null)
		{
			TableScanOperator.noResults.remove(p.block());
			int node;
			if (nodeNumber == -2)
			{
				node = getNodeNumber(); // from 1st header page
			}
			else
			{
				node = nodeNumber;
			}

			int dev;
			if (deviceNumber == -2)
			{
				dev = getDeviceNumber(); // from 1st header page
			}
			else
			{
				dev = deviceNumber;
			}

			RID rid = null;
			if (Transaction.reorder)
			{
				ArrayList<Integer> colOrder = this.getColOrder();
				int first = colOrder.get(0);
				rowIDsAL = rowIDSet.get(first);
				this.p = pageGroup.get(first);
				cachedLenLen = -1;
				nextRecNum = 0;

				if (rowIDsAL.size() > 0)
				{
					RID last = rowIDsAL.get(rowIDsAL.size() - 1);
					nextRecNum = last.getRecNum() + 1;
				}

				rid = new RID(node, dev, p.block().number(), nextRecNum);
				rowIDsAL = rowIDSet.get(0);
				this.p = pageGroup.get(0);
				cachedLenLen = -1;
			}
			else
			{
				rowIDsAL = rowIDSet.get(0);
				this.p = pageGroup.get(0);
				cachedLenLen = -1;
				nextRecNum = 0;

				if (rowIDsAL.size() > 0)
				{
					RID last = rowIDsAL.get(rowIDsAL.size() - 1);
					nextRecNum = last.getRecNum() + 1;
				}

				rid = new RID(node, dev, p.block().number(), nextRecNum);
			}
			
			int length = vals[0].size();
			int off = offs.get(0);
			byte[] after = new byte[length];

			if (vals[0] instanceof CVarcharFV)
			{
				((CVarcharFV)vals[0]).write(after, 0, this);
			}
			else
			{
				vals[0].write(after, 0);
			}
			p.write(off, after, tx.number(), LogManager.getLSN());
			//
			//if (p.block().number() == 3)
			//{
			//	HRDBMSWorker.logger.debug(p.block().fileName() + ": Wrote " + after.length + " bytes at offset " + off);
			//}
			//

			int offArrayLen = rowIDsAL.size() * 3;
			int offArrayOff = 4 + (rowIDsAL.size() * 3);
			p.writeShift(offArrayOff, offArrayOff + 3, offArrayLen, tx.number(), LogManager.getLSN());
			off = 7 + (rowIDsAL.size() * 6);
			after = new byte[3];
			ByteBuffer bb = ByteBuffer.wrap(after);
			bb.position(0);
			putMedium(bb, offs.get(0));
			//short o = (short)((offs.get(0) >>> 1) & 0xffff);
			//bb.putShort(o);
			p.write(off, after, tx.number(), LogManager.getLSN());

			off = 4 + (rowIDsAL.size() * 3);
			after = new byte[3];
			bb = ByteBuffer.wrap(after);
			bb.position(0);
			putMedium(bb, rid.getRecNum());
			//bb.putShort((short)rid.getRecNum());
			// System.arraycopy(after, 0, after, 9, 3);
			// bb.position(0);
			// putMedium(bb, rid.getBlockNum());
			// System.arraycopy(after, 0, after, 6, 3);
			// bb.position(0);
			// putMedium(bb, rid.getDevice());
			// System.arraycopy(after, 0, after, 3, 3);
			// bb.position(0);
			// putMedium(bb, rid.getNode());
			p.write(off, after, tx.number(), LogManager.getLSN());

			after = new byte[3];
			off = 1;
			bb = ByteBuffer.wrap(after);
			bb.position(0);
			putMedium(bb, rowIDsAL.size() + 1);
			p.write(off, after, tx.number(), LogManager.getLSN());
			rowIDsAL.add(rid);
			offsetArrayRows++;
			rowIDListSize++;
			offsetArray = offsetArraySet.get(0);
			if (offsetArray.length == rowIDsAL.size() - 1)
			{
				int[][] newOA = new int[offsetArray.length * 2][1];
				System.arraycopy(offsetArray, 0, newOA, 0, offsetArray.length);
				newOA[rowIDsAL.size() - 1][0] = offs.get(0);
				offsetArraySet.put(0, newOA);
			}
			else
			{
				offsetArray[rowIDsAL.size() - 1][0] = offs.get(0);
			}

			rowIDToIndex = rowIDToIndexSet.get(0);
			rowIDToIndex.put(rid, rowIDsAL.size() - 1);

			int pos = 0;
			for (FieldValue fv : vals)
			{
				if (pos == 0)
				{
					pos++;
					continue;
				}

				this.p = pageGroup.get(pos);
				cachedLenLen = -1;
				rowIDsAL = rowIDSet.get(pos);
				length = vals[pos].size();
				off = offs.get(pos);
				after = new byte[length];

				if (vals[pos] instanceof CVarcharFV)
				{
					((CVarcharFV)vals[pos]).write(after, 0, this);
				}
				else
				{
					vals[pos].write(after, 0);
				}
				p.write(off, after, tx.number(), LogManager.getLSN());
				//
				//if (p.block().number() == 3)
				//{
				//	HRDBMSWorker.logger.debug(p.block().fileName() + ": Wrote " + after.length + " bytes at offset " + off);
				//}
				//

				offArrayLen = rowIDsAL.size() * 3;
				offArrayOff = 4 + (rowIDsAL.size() * 3);
				p.writeShift(offArrayOff, offArrayOff + 3, offArrayLen, tx.number(), LogManager.getLSN());
				off = 7 + (rowIDsAL.size() * 6);
				after = new byte[3];
				bb = ByteBuffer.wrap(after);
				bb.position(0);
				putMedium(bb, offs.get(pos));
				//o = (short)((offs.get(pos) >>> 1) & 0xffff);
				//bb.putShort(o);
				p.write(off, after, tx.number(), LogManager.getLSN());

				off = 4 + (rowIDsAL.size() * 3);
				after = new byte[3];
				bb = ByteBuffer.wrap(after);
				bb.position(0);
				putMedium(bb, rid.getRecNum());
				//bb.putShort((short)rid.getRecNum());
				// System.arraycopy(after, 0, after, 9, 3);
				// bb.position(0);
				// putMedium(bb, rid.getBlockNum());
				// System.arraycopy(after, 0, after, 6, 3);
				// bb.position(0);
				// putMedium(bb, rid.getDevice());
				// System.arraycopy(after, 0, after, 3, 3);
				// bb.position(0);
				// putMedium(bb, rid.getNode());
				p.write(off, after, tx.number(), LogManager.getLSN());

				after = new byte[3];
				off = 1;
				bb = ByteBuffer.wrap(after);
				bb.position(0);
				putMedium(bb, rowIDsAL.size() + 1);
				p.write(off, after, tx.number(), LogManager.getLSN());
				rowIDsAL.add(rid);
				offsetArray = offsetArraySet.get(pos);
				if (offsetArray.length == rowIDsAL.size() - 1)
				{
					int[][] newOA = new int[offsetArray.length * 2][1];
					System.arraycopy(offsetArray, 0, newOA, 0, offsetArray.length);
					newOA[rowIDsAL.size() - 1][0] = offs.get(pos);
					offsetArraySet.put(pos, newOA);
				}
				else
				{
					offsetArray[rowIDsAL.size() - 1][0] = offs.get(pos);
				}

				rowIDToIndex = rowIDToIndexSet.get(pos);
				rowIDToIndex.put(rid, rowIDsAL.size() - 1);
				pos++;
			}

			return rid;
		}
		else
		{
			LockManager.xLock(new Block(p.block().fileName(), -1), tx.number());
			// allocate new set
			final Block b = addNewBlockColTableNoLog(p.block().fileName());
			tx.requestPage(b, new ArrayList<Integer>(pageGroup.keySet()));
			final Schema newPage = new Schema(this.colTypes, nodeNumber, deviceNumber);
			tx.read(b, newPage, new ArrayList<Integer>(pageGroup.keySet()), false, true);
			RID rid = newPage.insertRowColTableAppend(vals);
			return rid;
		}
	}

	public void massDelete() throws Exception
	{
		// TableScanOperator.noResults.remove(p.block());
		for (Map.Entry entry : pageGroup.entrySet())
		{
			int colNum = (int)entry.getKey();
			this.p = (Page)entry.getValue();
			byte[] before;
			byte[] after;
			rowIDToIndex = rowIDToIndexSet.get(colNum);
			offsetArray = offsetArraySet.get(colNum);
			offArrayOff = 4 + (rowIDToIndex.size() * 3);
			int offArrayLen = (rowIDToIndex.size() * 3);
			before = new byte[offArrayLen];
			p.get(offArrayOff, before);
			after = new byte[offArrayLen];
			int i = 0;
			final int l1 = after.length;
			while (i < l1)
			{
				after[i] = (byte)0xFF;
				i++;
			}

			i = 0;
			while (i < rowIDToIndex.size())
			{
				offsetArray[i][0] = -1;
				i++;
			}

			LogRec rec = tx.delete(before, after, offArrayOff, p.block()); // write
																			// log
																			// rec
			p.write(offArrayOff, after, tx.number(), rec.lsn());
		}
	}

	public void massDeleteNoLog() throws Exception
	{
		// TableScanOperator.noResults.remove(p.block());
		for (Map.Entry entry : pageGroup.entrySet())
		{
			int colNum = (int)entry.getKey();
			this.p = (Page)entry.getValue();
			byte[] after;
			rowIDToIndex = rowIDToIndexSet.get(colNum);
			offsetArray = offsetArraySet.get(colNum);
			offArrayOff = 4 + (rowIDToIndex.size() * 3);
			int offArrayLen = rowIDToIndex.size() * 3;
			after = new byte[offArrayLen];
			int i = 0;
			final int l1 = after.length;
			while (i < l1)
			{
				after[i] = (byte)0xFF;
				i++;
			}

			i = 0;
			while (i < rowIDToIndex.size())
			{
				offsetArray[i][0] = -1;
				i++;
			}

			p.write(offArrayOff, after, tx.number(), LogManager.getLSN());
		}
	}

	public void prepRowIter(ArrayList<Integer> fetchPos) throws Exception
	{
		try
		{
			iterCache = new SPSCQueue(1024);
			RowIterator iter = new RowIterator(false);
			while (iter.hasNext())
			{
				Row row = iter.next();
				row.getAllCols(fetchPos);
				iterCache.put(row);
			}

			iterCache.put(new DataEndMarker());
		}
		catch (Exception e)
		{
			HRDBMSWorker.logger.debug("", e);
		}
	}

	public void read(Transaction tx) throws Exception
	{
		addOpen = false;
		offsetArraySet = new HashMap<Integer, int[][]>();
		rowIDToIndexSet = new HashMap<Integer, Map<RID, Integer>>();
		rowIDSet = new HashMap<Integer, ArrayList<RID>>();
		// recCache = new HashMap<RID, ArrayList<FieldValue>>();
		try
		{
			rowIDsSet = false;
			if (this.pPrev != null)
			{
				for (Page pp : pPrev)
				{
					new UnpinThread(pp, tx.number()).run();
				}

				pPrev = null;
			}

			this.tx = tx;
			nextRecNum = -1;
			headEnd = -1;
			dataStart = -1;
			modTime = -1;
			colIDToIndex.clear();
			colIDToIndex.put(0, 0);
			colIDs = new int[1];
			colIDs[0] = 0;

			// blockType = p.get(0);
			// rowIDListOff = p.getMedium(18);
			// offArrayOff = p.getMedium(21);

			// int pos = colIDListOff;
			// colIDListSize = p.getMedium(pos);
			// pos += 3;
			// int i = 0;
			// colIDs = new int[colIDListSize];

			// p.get(pos, colIDs);
			// pos += (4 * colIDListSize);
			// int z = 0;
			// while (z < colIDListSize)
			// {
			// colIDs[z++] = p.getMedium(pos);
			// pos += 3;
			// }

			/*
			 * while (i < colIDListSize) { colIDs[i] = p.getInt(pos); i++; pos
			 * += 4; }
			 */

			// i = 0;
			// for (final int colID : colIDs)
			// {
			// colIDToIndex.put(colID, i);
			// i++;
			// }

			for (Map.Entry entry2 : pageGroup.entrySet())
			{
				int colNum = (int)entry2.getKey();
				this.p = (Page)entry2.getValue();
				cachedLenLen = -1;

				if (myNode == -1 || myNode == -2)
				{
					getNodeNumber();
					getDeviceNumber();
				}

				int pos = 1;
				rowIDListOff = 1;
				rowIDListSize = p.getMedium(pos);
				pos += 3;
				rowIDToIndex = new LinkedHashMap<RID, Integer>((int)(rowIDListSize  * 1.35));
				rowIDs = null;
				int i = 0;
				setRowIDsCT(colNum);
				for (final RID rid : rowIDsAL)
				{
					if (i >= rowIDListSize)
					{
						break;
					}
					rowIDToIndex.put(rid, i);
					i++;
				}
				rowIDSet.put(colNum, rowIDsAL);
				rowIDToIndexSet.put(colNum, rowIDToIndex);
				pos += (3 * rowIDListSize);

				// i = 0;
				// for (final RID rid : rowIDs)
				// {
				// rowIDToIndex.put(rid, i);
				// i++;
				// }

				int y = 1;
				while (y * ROWS_TO_ALLOCATE_COL < rowIDListSize)
				{
					y++;
				}

				if (offsetArray != null && offsetArray.length >= (y * ROWS_TO_ALLOCATE_COL))
				{
				}
				else
				{
					offsetArray = new int[y * ROWS_TO_ALLOCATE_COL][1];
				}
				offsetArrayRows = rowIDListSize;
				int row = 0;
				while (row < rowIDListSize)
				{
					offsetArray[row][0] = p.getMedium(pos);
					if (offsetArray[row][0] == 0x00ffffff)
					{
						offsetArray[row][0] = -1;
					}
					pos += 3;
					row++;
				}

				offsetArraySet.put(colNum, offsetArray);
				offsetArray = null;
			}
		}
		catch (Exception e)
		{
			HRDBMSWorker.logger.debug("", e);
			throw e;
		}
	}

	public void read(Transaction tx, boolean forIter) throws Exception
	{
		offsetArraySet = null;
		rowIDSet = null;
		rowIDToIndexSet = null;
		addOpen = false;

		try
		{
			rowIDsSet = false;
			if (this.pPrev != null)
			{
				for (Page pp : pPrev)
				{
					new UnpinThread(pp, tx.number()).run();
				}

				pPrev = null;
			}

			this.tx = tx;
			nextRecNum = -1;
			headEnd = -1;
			dataStart = -1;
			modTime = -1;
			colIDToIndex.clear();
			colIDToIndex.put(0, 0);
			colIDs = new int[1];
			colIDs[0] = 0;

			// blockType = p.get(0);
			// rowIDListOff = p.getMedium(18);
			// offArrayOff = p.getMedium(21);

			// int pos = colIDListOff;
			// colIDListSize = p.getMedium(pos);
			// pos += 3;
			// int i = 0;
			// colIDs = new int[colIDListSize];

			// p.get(pos, colIDs);
			// pos += (4 * colIDListSize);
			// int z = 0;
			// while (z < colIDListSize)
			// {
			// colIDs[z++] = p.getMedium(pos);
			// pos += 3;
			// }

			/*
			 * while (i < colIDListSize) { colIDs[i] = p.getInt(pos); i++; pos
			 * += 4; }
			 */

			// i = 0;
			// for (final int colID : colIDs)
			// {
			// colIDToIndex.put(colID, i);
			// i++;
			// }

			boolean first = true;

			int colPos = 0;
			ArrayList<ReadThread> threads = new ArrayList<ReadThread>();
			
			recCache = new ConcurrentHashMap<RID, ArrayList<FieldValue>>((int)(rowIDListSize * 1.35));
			
			for (Map.Entry entry2 : pageGroup.entrySet())
			{
				if (colPos == 0)
				{
					this.p = (Page)entry2.getValue();
					if (myNode == -1 || myNode == -2)
					{
						getNodeNumber();
						getDeviceNumber();
					}
				}
				
				int colNum = (int)entry2.getKey();
				//this.p = (Page)entry2.getValue();
				ReadThread thread = new ReadThread((Page)entry2.getValue(), colPos, colNum, this);
				thread.start();
				threads.add(thread);
				
				//if (colPos == 0)
				//{
				//	Thread.sleep(100);
				//}
				colPos++;
			}
			
			for (ReadThread thread : threads)
			{
				thread.join();
				if (!thread.getOK())
				{
					throw thread.getExcetpion();
				}
			}
			
			int x = Schema.ReadThread.master;
		}
		catch (Exception e)
		{
			HRDBMSWorker.logger.debug("", e);
			throw e;
		}
	}
	
	private static class ReadThread extends HRDBMSThread
	{
		private Page page;
		private int colPos;
		private int colNum;
		private boolean ok = true;
		private Exception e;
		public static volatile int master = 0;
		private Schema schema;
		
		public ReadThread(Page page, int colPos, int colNum, Schema schema)
		{
			this.page = page;
			this.colPos = colPos;
			this.colNum = colNum;
			this.schema = schema;
		}
		
		public boolean getOK()
		{
			return ok;
		}
		
		public Exception getExcetpion()
		{
			return e;
		}
		
		public void run()
		{
			try
			{
				Schema s = schema.clone();
				s.cachedLenLen = -1;

				int pos = 1;
				s.rowIDListOff = 1;
				s.p = page;
				s.rowIDListSize = s.p.getMedium(pos);
				
				pos += 3;
				s.rowIDToIndex = new LinkedHashMap<RID, Integer>((int)(s.rowIDListSize  * 1.35));
				int i = 0;
				s.setRowIDsCT(colNum);
				for (final RID rid : s.rowIDsAL)
				{
					if (i >= s.rowIDListSize)
					{
						break;
					}
					s.rowIDToIndex.put(rid, i);
					i++;
				}
				pos += (3 * s.rowIDListSize);

				// i = 0;
				// for (final RID rid : rowIDs)
				// {
				// rowIDToIndex.put(rid, i);
				// i++;
				// }

				s.offsetArray = new int[s.rowIDListSize][1];
				s.offsetArrayRows = s.rowIDListSize;
				int row = 0;
				while (row < s.rowIDListSize)
				{
					s.offsetArray[row][0] = s.p.getMedium(pos);
					if (s.offsetArray[row][0] == 0x00ffffff)
					{
						s.offsetArray[row][0] = -1;
					}
					pos += 3;
					row++;
				}

				Map<Integer, DataType> colTypesBackup = s.colTypes;
				s.colTypes = new HashMap<Integer, DataType>();
				s.colTypes.put(0, colTypesBackup.get(colNum));
				
				DataType dt = s.colTypes.get(s.colIDs[0]);

				for (Map.Entry entry : s.rowIDToIndex.entrySet())
				{
					RID rid = (RID)entry.getKey();
					int index = (int)entry.getValue();

					Row row2 = s.new Row(index);
					try
					{
						FieldValue fv = row2.getCol(0, dt);

						try
						{
							ArrayList<FieldValue> alfv = s.recCache.get(rid);
							if (alfv == null)
							{
								alfv = new ArrayList<FieldValue>(s.pageGroup.size());
								int j = 0;
								while (j < s.pageGroup.size())
								{
									alfv.add(null);
									j++;
								}
								
								ArrayList<FieldValue> alfv2 = s.recCache.putIfAbsent(rid, alfv);
								if (alfv2 != null)
								{
									alfv = alfv2;
								}
							}
							
							//synchronized(alfv)
							//{
								alfv.set(colPos, fv);
							//}
						}
						catch(Exception e)
						{
							HRDBMSWorker.logger.debug("Error find rid = " + rid + " in " + s.recCache + " during col table read");
							
							HRDBMSWorker.logger.debug("Page group info follows:");
							for (Page p2 : s.pageGroup.values())
							{
								HRDBMSWorker.logger.debug("Block: " + p2.block() + " Pinned: " + p2.isPinned() + " Ready: " + p2.isReady());
							}
							throw e;
						}
					}
					catch(Exception f)
					{
						String string = "Probable error reading column data, offset array is [";
						for (int[] array : s.offsetArray)
						{
							string += (array[0] + ",");
						}
						
						string += "]";
						
						HRDBMSWorker.logger.debug(string, f);
						throw f;
					}
				}

				//s.colTypes = colTypesBackup;
				s.makeNull();
				master = colPos;
			}
			catch (Exception e)
			{
				ok = false;
				this.e = e;
				HRDBMSWorker.logger.debug("", e);
			}
		}
	}

	public void read(Transaction tx, Page p) throws Exception
	{
		try
		{
			rowIDsSet = false;
			myDev = -1;
			myNode = -1;
			if (this.p != null)
			{
				new UnpinThread(this.p, tx.number()).run();
			}
			this.p = p;
			this.tx = tx;
			nextRecNum = -1;
			headEnd = -1;
			dataStart = -1;
			modTime = -1;

			blockType = p.get(0);
			rowIDListOff = p.getMedium(18);
			offArrayOff = p.getMedium(21);

			int pos = colIDListOff;
			colIDListSize = p.getMedium(pos);
			pos += 3;
			int i = 0;
			colIDs = new int[colIDListSize];

			// p.get(pos, colIDs);
			// pos += (4 * colIDListSize);
			int z = 0;
			while (z < colIDListSize)
			{
				colIDs[z++] = p.getMedium(pos);
				pos += 3;
			}

			/*
			 * while (i < colIDListSize) { colIDs[i] = p.getInt(pos); i++; pos
			 * += 4; }
			 */

			i = 0;
			for (final int colID : colIDs)
			{
				colIDToIndex.put(colID, i);
				i++;
			}

			rowIDListSize = p.getMedium(pos);
			pos += 3;
			pos += (12 * rowIDListSize);

			// i = 0;
			// for (final RID rid : rowIDs)
			// {
			// rowIDToIndex.put(rid, i);
			// i++;
			// }
			rowIDToIndex = null;

			int y = 1;
			while (y * ROWS_TO_ALLOCATE < rowIDListSize)
			{
				y++;
			}

			if (offsetArray != null && offsetArray.length >= (y * ROWS_TO_ALLOCATE))
			{
			}
			else
			{
				offsetArray = new int[y * ROWS_TO_ALLOCATE][colIDListSize];
			}
			offsetArrayRows = rowIDListSize;
			int row = 0;
			while (row < rowIDListSize)
			{
				z = 0;
				while (z < colIDListSize)
				{
					offsetArray[row][z++] = p.getMedium(pos);
					pos += 3;
				}
				// p.get(pos, offsetArray[row]);
				// pos += (4 * colIDListSize);
				row++;
			}
		}
		catch (Exception e)
		{
			HRDBMSWorker.logger.debug("", e);
			throw e;
		}
	}

	public RowIterator rowIterator()
	{
		return new RowIterator(true);
	}

	public RowIterator rowIterator(boolean flag)
	{
		if (flag)
		{
			while (iterCache == null)
			{
				LockSupport.parkNanos(500);
			}

			return new RowIterator(true);
		}
		else
		{
			return new RowIterator(false);
		}
	}

	private Block addNewBlock(String fn, int[] colIDs) throws IOException, LockAbortException, Exception
	{
		final ByteBuffer oldBuff = ByteBuffer.allocate(Page.BLOCK_SIZE);
		oldBuff.position(0);
		int i = 0;
		while (i < Page.BLOCK_SIZE)
		{
			oldBuff.putLong(-1);
			i += 8;
		}
		final int newBlockNum = FileManager.addNewBlock(fn, oldBuff, tx);

		ByteBuffer buff = ByteBuffer.allocate(Page.BLOCK_SIZE);
		buff.position(0);
		buff.put(blockType);
		putMedium(buff, 0); // nextRecNum
		putMedium(buff, 29 + 3 * colIDs.length); // headEnd
		putMedium(buff, Page.BLOCK_SIZE); // dataStart
		buff.putLong(System.currentTimeMillis()); // modTime
		putMedium(buff, 27 + (3 * colIDs.length)); // rowIDListOff
		putMedium(buff, 30 + (3 * colIDs.length)); // offArrayOff
		putMedium(buff, colIDs.length); // colIDListSize - start of colIDs

		for (final int id : colIDs)
		{
			putMedium(buff, id); // colIDs
		}

		putMedium(buff, 0); // rowIDListSize - start of rowIDs
		// offset array start

		Block bl = new Block(fn, newBlockNum);
		LockManager.xLock(bl, tx.number());
		//TableScanOperator.noResults.remove(p.block());
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

		return bl;
	}

	private Block addNewBlockColTable(String fn) throws Exception
	{
		final ByteBuffer oldBuff = ByteBuffer.allocate(Page.BLOCK_SIZE);
		oldBuff.position(0);
		int i = 0;
		while (i < Page.BLOCK_SIZE)
		{
			oldBuff.putLong(-1);
			i += 8;
		}

		ByteBuffer buff = ByteBuffer.allocate(Page.BLOCK_SIZE);
		buff.position(0);
		ArrayList<Byte> hb = getHeaderBytes();
		buff.put((byte)1);
		putMedium(buff, 0); // RID list length

		int newBlockNum = FileManager.addNewBlock(fn, oldBuff, tx);
		Block bl = new Block(fn, newBlockNum);
		LockManager.xLock(bl, tx.number());

		int pos = 1;
		while (pos < pageGroup.size())
		{
			newBlockNum = FileManager.addNewBlock(fn, oldBuff, tx);
			Block bl2 = new Block(fn, newBlockNum);
			LockManager.xLock(bl2, tx.number());
			pos++;
		}

		tx.requestPage(bl, new ArrayList<Integer>(pageGroup.keySet()));

		Page[] p2 = null;
		try
		{
			p2 = tx.getPage(bl, new ArrayList<Integer>(pageGroup.keySet()));
		}
		catch (Exception e)
		{
			HRDBMSWorker.logger.error("", e);
			throw e;
		}

		int z = 0;
		for (Page pp : p2)
		{
			buff.position(0);
			buff.put(hb.get(z++));
			InsertLogRec rec = tx.insert(oldBuff.array(), buff.array(), 0, pp.block());
			pp.write(0, buff.array(), tx.number(), rec.lsn());
		}

		return bl;
	}

	private Block addNewBlockColTableNoLog(String fn) throws Exception
	{
		final ByteBuffer oldBuff = ByteBuffer.allocate(Page.BLOCK_SIZE);
		oldBuff.position(0);
		int i = 0;
		while (i < Page.BLOCK_SIZE)
		{
			oldBuff.putLong(-1);
			i += 8;
		}

		ByteBuffer buff = ByteBuffer.allocate(Page.BLOCK_SIZE);
		buff.position(0);
		ArrayList<Byte> hb = getHeaderBytes();
		buff.put((byte)1);
		putMedium(buff, 0); // RID list length

		int newBlockNum = FileManager.addNewBlockNoLog(fn, oldBuff, tx);
		Block bl = new Block(fn, newBlockNum);
		LockManager.xLock(bl, tx.number());

		int pos = 1;
		while (pos < pageGroup.size())
		{
			newBlockNum = FileManager.addNewBlockNoLog(fn, oldBuff, tx);
			Block bl2 = new Block(fn, newBlockNum);
			LockManager.xLock(bl2, tx.number());
			pos++;
		}

		tx.requestPage(bl, new ArrayList<Integer>(pageGroup.keySet()));

		Page[] p2 = null;
		try
		{
			p2 = tx.getPage(bl, new ArrayList<Integer>(pageGroup.keySet()));
		}
		catch (Exception e)
		{
			HRDBMSWorker.logger.error("", e);
			throw e;
		}

		int z = 0;
		for (Page pp : p2)
		{
			buff.position(0);
			buff.put(hb.get(z++));
			// InsertLogRec rec = tx.insert(oldBuff.array(), buff.array(), 0,
			// pp.block());
			pp.write(0, buff.array(), tx.number(), LogManager.getLSN());
		}

		return bl;
	}

	private Block addNewBlockNoLog(String fn, int[] colIDs) throws IOException, LockAbortException, Exception
	{
		final ByteBuffer oldBuff = ByteBuffer.allocate(Page.BLOCK_SIZE);
		oldBuff.position(0);
		int i = 0;
		while (i < Page.BLOCK_SIZE)
		{
			oldBuff.putLong(-1);
			i += 8;
		}
		final int newBlockNum = FileManager.addNewBlockNoLog(fn, oldBuff, tx);

		ByteBuffer buff = ByteBuffer.allocate(Page.BLOCK_SIZE);
		buff.position(0);
		buff.put(blockType);
		putMedium(buff, 0); // nextRecNum
		putMedium(buff, 29 + 3 * colIDs.length); // headEnd
		putMedium(buff, Page.BLOCK_SIZE); // dataStart
		buff.putLong(System.currentTimeMillis()); // modTime
		putMedium(buff, 27 + (3 * colIDs.length)); // rowIDListOff
		putMedium(buff, 30 + (3 * colIDs.length)); // offArrayOff
		putMedium(buff, colIDs.length); // colIDListSize - start of colIDs

		for (final int id : colIDs)
		{
			putMedium(buff, id); // colIDs
		}

		putMedium(buff, 0); // rowIDListSize - start of rowIDs
		// offset array start

		Block bl = new Block(fn, newBlockNum);
		LockManager.xLock(bl, tx.number());
		//TableScanOperator.noResults.remove(p.block());
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

		p2.write(0, buff.array(), tx.number(), LogManager.getLSN());

		return bl;
	}

	private int findNextFreeRecNum(int justUsed) throws Exception
	{
		if (!rowIDsSet)
		{
			setRowIDs();
		}

		int candidate = justUsed + 1;

		if (!copy.contains(new RID(0, 0, 0, candidate)))
		{
			return candidate;
		}

		int i = -1;
		for (final RID rid : copy)
		{
			if (rid.getRecNum() != i + 1)
			{
				return i + 1;
			}

			i++;
		}

		if (i == Integer.MAX_VALUE)
		{
			throw new RecNumOverflowException();
		}

		return i + 1;
	}

	private int getDeviceNumber() throws LockAbortException, Exception
	{
		if (myDev == -1)
		{
			final Block b = new Block(p.block().fileName(), 0);
			tx.requestPage(b);

			final HeaderPage hp = tx.readHeaderPage(b, blockType);
			myDev = hp.getDeviceNumber();
		}

		return myDev;
	}

	private ArrayList<Byte> getHeaderBytes() throws Exception
	{
		if (headerBytes == null)
		{
			final Block b = new Block(p.block().fileName(), 0);
			tx.requestPage(b);

			final HeaderPage hp = tx.readHeaderPage(b, blockType);
			headerBytes = hp.getHeaderBytes(colTypes.size());
		}

		return headerBytes;
	}
	
	private ArrayList<Integer> getColOrder() throws Exception
	{
		if (colOrder == null)
		{
			final Block b = new Block(p.block().fileName(), 0);
			tx.requestPage(b);

			final HeaderPage hp = tx.readHeaderPage(b, blockType);
			colOrder = hp.getColOrder();
		}
		
		return colOrder;
	}

	private int getNodeNumber() throws LockAbortException, Exception
	{
		if (myNode == -1)
		{
			final Block b = new Block(p.block().fileName(), 0);
			tx.requestPage(b);

			final HeaderPage hp = tx.readHeaderPage(b, blockType);
			myNode = hp.getNodeNumber();
		}

		return myNode;
	}

	private ArrayList<Integer> hasEnoughSpace(FieldValue[] vals) throws Exception
	{
		ArrayList<Integer> retval = new ArrayList<Integer>(vals.length);
		int pos = 0;
		while (pos < pageGroup.size())
		{
			int dataStart = Page.BLOCK_SIZE;
			rowIDsAL = rowIDSet.get(pos);
			offsetArray = offsetArraySet.get(pos);
			int i = rowIDsAL.size() - 1;
			while (i >= 0)
			{
				int offset = offsetArray[i][0];
				if (offset == 0 || offset == -1)
				{
					i--;
					continue;
				}
				else
				{
					dataStart = offset;
					break;
				}
			}

			this.p = pageGroup.get(pos);
			cachedLenLen = -1;
			if (vals[pos] instanceof VarcharFV)
			{
				vals[pos] = new CVarcharFV((VarcharFV)vals[pos], this);
			}
			dataStart -= vals[pos].size();
			
			int headEnd = 9 + (rowIDsAL.size() * 6);

			if (headEnd >= dataStart)
			{
				return null;
			}

			retval.add(dataStart);
			pos++;
		}

		return retval;
	}

	private Integer hasEnoughSpace(int head, int data)
	{
		if (dataStart == -1)
		{
			dataStart = p.getMedium(7);
		}

		if (headEnd == -1)
		{
			headEnd = p.getMedium(4);
		}

		int start = dataStart - data;
		int end = headEnd + head;

		if (end < start)
		{
			return start;
		}

		return null;
	}

	private int headerGrowthPerRow()
	{
		return 12 + colIDListSize * 3;
	}

	private void setRowIDs() throws Exception
	{
		copy = new TreeSet<RID>(new RIDComparator());
		int i = 0;
		int pos = rowIDListOff + 3;
		if (rowIDs != null && rowIDs.length >= rowIDListSize)
		{
		}
		else
		{
			rowIDs = new RID[rowIDListSize];
		}
		int[] rid = new int[4];
		while (i < rowIDListSize)
		{
			int z = 0;
			while (z < 4)
			{
				rid[z++] = p.getMedium(pos);
				pos += 3;
			}

			rowIDs[i] = new RID(rid[0], rid[1], rid[2], rid[3]);
			copy.add(rowIDs[i]);
			i++;
		}

		rowIDsSet = true;
	}

	private void setRowIDsCT(int colNum) throws Exception
	{
		//copy = new TreeSet<RID>(new RIDComparator());
		int i = 0;
		int pos = rowIDListOff + 3;
		rowIDsAL = new ArrayList<RID>(rowIDListSize);
		int pnum = p.block().number();
		String pfn = p.block().fileName();
		Block pblk = p.block();
		int mColNum = -1;
		HashMap<Integer, Integer> map = null;
		if (Transaction.reorder)
		{
			map = tx.colMap.get(pfn);
			if (map == null)
			{
				map = tx.getColOrder(pblk);
				tx.colMap.put(pfn, map);
			}
			mColNum = map.get(colNum);
		}

		while (i < rowIDListSize)
		{
			int id = p.getMedium(pos);
			pos += 3;
			if (!Transaction.reorder)
			{
				rowIDsAL.add(new RID(myNode, myDev, pnum - colNum, id));
			}
			else
			{	
				rowIDsAL.add(new RID(myNode, myDev, pnum - mColNum, id));
			}
			//copy.add(rowIDsAL.get(i));
			i++;
		}

		rowIDsSet = true;
	}

	private void updateHeader(RID rid, FieldValue[] vals, int nextRecNum, int off, int length) throws RecNumOverflowException, LockAbortException, Exception
	{
		if (dataStart == -1)
		{
			dataStart = p.getMedium(7);
		}
		if (off < dataStart)
		{
			this.dataStart = off;
		}
		this.modTime = System.currentTimeMillis();
		updateRowIDs(rid);
		this.nextRecNum = findNextFreeRecNum(nextRecNum);
		updateOffsets(vals, off);

		final byte[] before = new byte[30 + (rowIDListSize * 12) + (colIDListSize * 3) + (3 * rowIDListSize * colIDListSize)];
		p.get(1, before);
		final byte[] after = new byte[before.length];

		// copy(after,0,
				// ByteBuffer.allocate(4).putInt(this.nextRecNum).array()); //nextRecNum
		ByteBuffer x = ByteBuffer.allocate(3);
		x.position(0);
		putMedium(x, this.nextRecNum);
		System.arraycopy(x.array(), 0, after, 0, 3);
		// copy(after,4, ByteBuffer.allocate(4).putInt(before.length -
		// 1).array()); //headEnd
		x.position(0);
		putMedium(x, before.length - 1);
		System.arraycopy(x.array(), 0, after, 3, 3);
		headEnd = before.length - 1;
		// copy(after,8, ByteBuffer.allocate(4).putInt(this.dataStart).array());
		// //dataStart
		x.position(0);
		putMedium(x, this.dataStart);
		System.arraycopy(x.array(), 0, after, 6, 3);
		// copy(after,12, ByteBuffer.allocate(8).putLong(this.modTime).array());
		// //modTime

		System.arraycopy(ByteBuffer.allocate(8).putLong(this.modTime).array(), 0, after, 9, 8);

		x.position(0);
		putMedium(x, 27 + colIDListSize * 3);
		System.arraycopy(x.array(), 0, after, 17, 3);
		// copy(after, i, ByteBuffer.allocate(4).putInt(colIDListSize).array());
		x.position(0);
		putMedium(x, 30 + colIDListSize * 3 + 12 * rowIDListSize);
		System.arraycopy(x.array(), 0, after, 20, 3);
		x.position(0);
		putMedium(x, colIDListSize);
		System.arraycopy(x.array(), 0, after, 23, 3);
		int i = 26;

		for (final int id : colIDs)
		{
			// copy(after, i, ByteBuffer.allocate(4).putInt(id).array());
			x.position(0);
			putMedium(x, id);
			System.arraycopy(x.array(), 0, after, i, 3);
			i += 3;
		}

		x.position(0);
		putMedium(x, rowIDListSize);
		System.arraycopy(x.array(), 0, after, i, 3);
		i += 3;

		int a = 0;
		if (!rowIDsSet)
		{
			setRowIDs();
		}
		for (final RID id : rowIDs)
		{
			if (a >= rowIDListSize)
			{
				break;
			}
			// copy(after, i,
			// ByteBuffer.allocate(4).putInt(id.getNode()).array());
			x.position(0);
			putMedium(x, id.getNode());
			System.arraycopy(x.array(), 0, after, i, 3);
			// copy(after, i+4,
			// ByteBuffer.allocate(4).putInt(id.getDevice()).array());
			x.position(0);
			putMedium(x, id.getDevice());
			System.arraycopy(x.array(), 0, after, i + 3, 3);
			// copy(after, i+8,
			// ByteBuffer.allocate(4).putInt(id.getBlockNum()).array());
			x.position(0);
			putMedium(x, id.getBlockNum());
			System.arraycopy(x.array(), 0, after, i + 6, 3);
			// copy(after, i+12,
			// ByteBuffer.allocate(4).putInt(id.getRecNum()).array());
			x.position(0);
			putMedium(x, id.getRecNum());
			System.arraycopy(x.array(), 0, after, i + 9, 3);
			i += 12;
		}

		int k = 0;
		while (k < offsetArrayRows)
		{
			int[] row = offsetArray[k];
			ByteBuffer temp = ByteBuffer.allocate(row.length * 3);
			int z = 0;
			while (z < row.length)
			{
				putMedium(temp, row[z++]);
			}
			System.arraycopy(temp.array(), 0, after, i, 3 * row.length);
			i += (3 * row.length);
			k++;
		}

		final InsertLogRec rec = tx.insert(before, after, 1, p.block());
		p.write(1, after, tx.number(), rec.lsn());
	}

	private void updateHeaderNoLog(RID rid, FieldValue[] vals, int nextRecNum, int off, int length) throws RecNumOverflowException, LockAbortException, Exception
	{
		if (dataStart == -1)
		{
			dataStart = p.getMedium(7);
		}
		if (off < dataStart)
		{
			this.dataStart = off;
		}
		this.modTime = System.currentTimeMillis();
		updateRowIDs(rid);
		this.nextRecNum = findNextFreeRecNum(nextRecNum);
		updateOffsets(vals, off);
		headEnd = 29 + (rowIDListSize * 12) + (colIDListSize * 3) + (3 * rowIDListSize * colIDListSize);
	}

	private void updateOffsets(FieldValue[] vals, int off)
	{
		if (rowIDListSize <= offsetArray.length)
		{
			int j = 0;
			final int i = offsetArrayRows;
			for (final FieldValue fv : vals)
			{
				offsetArray[i][j] = off;
				j++;
				off += fv.size();
			}

			offsetArrayRows++;
			return;
		}
		else
		{
			final int[][] newOA = new int[offsetArray.length + ROWS_TO_ALLOCATE][colIDs.length];
			System.arraycopy(offsetArray, 0, newOA, 0, offsetArray.length);

			int j = 0;
			final int i = offsetArray.length;
			for (final FieldValue fv : vals)
			{
				newOA[i][j] = off;
				j++;
				off += fv.size();
			}

			offsetArray = newOA;
			offsetArrayRows++;
		}
	}

	private void updateRowIDs(RID id) throws Exception
	{
		if (!rowIDsSet)
		{
			setRowIDs();
		}
		if (rowIDListSize + 1 > rowIDs.length)
		{
			final RID[] newRI = new RID[rowIDListSize + 1];
			// int i = 0;
			// while (i < rowIDs.length)
			// {
			// newRI[i] = rowIDs[i];
			// i++;
			// }
			System.arraycopy(rowIDs, 0, newRI, 0, rowIDListSize);

			final int i = rowIDListSize;
			newRI[i] = id;
			rowIDs = newRI;
		}
		else
		{
			rowIDs[rowIDListSize] = id;
		}
		rowIDListSize++;
		copy.add(id);
	}

	public static class BigintFV extends FieldValue
	{
		private long value;

		public BigintFV(int row, int col, Schema s)
		{
			super(row, col, s);

			if (!isNull && exists)
			{
				value = s.p.getLong(off);
			}
		}

		public BigintFV(Long val)
		{
			this.exists = true;
			if (val == null)
			{
				this.isNull = true;
			}
			else
			{
				this.value = val;
			}
		}

		@Override
		public Long getValue()
		{
			return value;
		}

		@Override
		public int size()
		{
			if (!isNull && exists)
			{
				return 8;
			}

			return 0;
		}

		@Override
		public String toString()
		{
			return "" + value;
		}

		@Override
		public int write(byte[] buff, int off)
		{
			// if (value < 0)
			// {
			// HRDBMSWorker.logger.debug("Writing negative long in schema");
			// }
			if (!isNull && exists)
			{
				final byte[] val = ByteBuffer.allocate(8).putLong(value).array();
				// int i = 0;
				// while (i < 8)
				// {
				// buff[off + i] = val[i];
				// i++;
				// }
				System.arraycopy(val, 0, buff, off, 8);

				return 8;
			}

			return 0;
		}
	}

	public static class BinaryFV extends FieldValue
	{
		private byte[] value;

		public BinaryFV(int row, int col, int len, Schema s) throws Exception
		{
			super(row, col, s);

			if (!isNull && exists)
			{
				value = new byte[len];
				s.p.get(off, value);
			}
		}

		@Override
		public byte[] getValue()
		{
			return value;
		}

		@Override
		public int size()
		{
			if (!isNull && exists)
			{
				return value.length;
			}

			return 0;
		}

		@Override
		public String toString()
		{
			return "BinaryFV";
		}

		@Override
		public int write(byte[] buff, int off)
		{
			if (!isNull && exists)
			{
				// int i = 0;
				// final int length = value.length;
				// while (i < length)
				// {
				// buff[off + i] = value[i];
				// i++;
				// }
				System.arraycopy(value, 0, buff, off, value.length);

				return value.length;
			}

			return 0;
		}
	}

	public static class CVarcharFV extends FieldValue
	{
		public static boolean compress;
		private final static int NUM_SYM = 668;
		// private static HashMap<Integer, Integer> freq = new HashMap<Integer,
		// Integer>();
		// private static HashMap<Integer, HuffmanNode> treeParts = new
		// HashMap<Integer, HuffmanNode>();
		// private static HuffmanNode tree;
		private final static int[] encode = new int[NUM_SYM];
		private final static int[] encodeLength = new int[NUM_SYM];
		private final static int[] masks = { 0, 0x01, 0x03, 0x07, 0x0f, 0x1f, 0x3f, 0x7f, 0xff, 0x01ff, 0x03ff, 0x07ff, 0x0fff, 0x1fff, 0x3fff, 0x7fff, 0xffff, 0x01ffff, 0x03ffff, 0x07ffff, 0x0fffff, 0x1fffff, 0x3fffff, 0x7fffff, 0xffffff, 0x01ffffff, 0x03ffffff, 0x07ffffff, 0x0fffffff, 0x1fffffff, 0x3fffffff, 0x7fffffff, 0xffffffff };
		//private final static long[] masks2 = { 0, 0x01, 0x03, 0x07, 0x0f, 0x1f, 0x3f, 0x7f, 0xff, 0x01ff, 0x03ff, 0x07ff, 0x0fff, 0x1fff, 0x3fff, 0x7fff, 0xffff, 0x01ffff, 0x03ffff, 0x07ffff, 0x0fffff, 0x1fffff, 0x3fffff, 0x7fffff, 0xffffff, 0x01ffffff, 0x03ffffff, 0x07ffffff, 0x0fffffff, 0x1fffffff, 0x3fffffff, 0x7fffffff, 0xffffffffL, 0x01ffffffffL, 0x03ffffffffL, 0x07ffffffffL, 0x0fffffffffL, 0x1fffffffffL, 0x3fffffffffL, 0x7fffffffffL, 0xffffffffffL, 0x01ffffffffffL, 0x03ffffffffffL, 0x07ffffffffffL, 0x0fffffffffffL, 0x1fffffffffffL, 0x3fffffffffffL, 0x7fffffffffffL, 0xffffffffffffL, 0x01ffffffffffffL, 0x03ffffffffffffL, 0x07ffffffffffffL, 0x0fffffffffffffL, 0x1fffffffffffffL, 0x3fffffffffffffL, 0x7fffffffffffffL, 0xffffffffffffffL, 0x01ffffffffffffffL, 0x03ffffffffffffffL, 0x07ffffffffffffffL, 0x0fffffffffffffffL, 0x1fffffffffffffffL, 0x3fffffffffffffffL, 0x7fffffffffffffffL, 0xffffffffffffffffL };
		private final static Code[] decode3 = new Code[16777216];
		private final static int[][] codeExtended = new int[256][256];
		private final static int[][] codeExtended2 = new int[NUM_SYM - 256][256];
		private final static int[][] codeExtended3 = new int[NUM_SYM - 256][256];
		static
		{
			HRDBMSWorker.logger.debug("CVarcharFV start init");
			compress = HRDBMSWorker.getHParms().getProperty("enable_cvarchar_compression").equals("true");
			codeExtended['t']['h'] = 256;
			codeExtended['T']['H'] = 257;
			codeExtended['t']['H'] = 258;
			codeExtended['T']['h'] = 259;
			codeExtended['h']['e'] = 260;
			codeExtended['H']['E'] = 261;
			codeExtended['h']['E'] = 262;
			codeExtended['H']['e'] = 263;
			codeExtended['i']['n'] = 272;
			codeExtended['I']['N'] = 273;
			codeExtended['I']['n'] = 274;
			codeExtended['i']['N'] = 275;
			codeExtended['e']['r'] = 276;
			codeExtended['E']['R'] = 277;
			codeExtended['E']['r'] = 278;
			codeExtended['e']['R'] = 279;
			codeExtended['a']['n'] = 280;
			codeExtended['A']['N'] = 281;
			codeExtended['A']['n'] = 282;
			codeExtended['a']['N'] = 283;
			codeExtended['r']['e'] = 284;
			codeExtended['R']['E'] = 285;
			codeExtended['R']['e'] = 286;
			codeExtended['r']['E'] = 287;
			codeExtended['o']['n'] = 288;
			codeExtended['O']['N'] = 289;
			codeExtended['O']['n'] = 290;
			codeExtended['o']['N'] = 291;
			codeExtended['a']['t'] = 292;
			codeExtended['A']['T'] = 293;
			codeExtended['A']['t'] = 294;
			codeExtended['a']['T'] = 295;
			codeExtended['e']['n'] = 296;
			codeExtended['E']['N'] = 297;
			codeExtended['E']['n'] = 298;
			codeExtended['e']['N'] = 299;
			codeExtended['n']['d'] = 300;
			codeExtended['N']['D'] = 301;
			codeExtended['N']['d'] = 302;
			codeExtended['n']['D'] = 303;
			codeExtended['t']['i'] = 304;
			codeExtended['T']['I'] = 305;
			codeExtended['T']['i'] = 306;
			codeExtended['t']['I'] = 307;
			codeExtended['e']['s'] = 308;
			codeExtended['E']['S'] = 309;
			codeExtended['E']['s'] = 310;
			codeExtended['e']['S'] = 311;
			codeExtended['o']['r'] = 312;
			codeExtended['O']['R'] = 313;
			codeExtended['O']['r'] = 314;
			codeExtended['o']['R'] = 315;
			codeExtended['t']['e'] = 316;
			codeExtended['T']['E'] = 317;
			codeExtended['T']['e'] = 318;
			codeExtended['t']['E'] = 319;
			codeExtended['o']['f'] = 320;
			codeExtended['O']['F'] = 321;
			codeExtended['O']['f'] = 322;
			codeExtended['o']['F'] = 323;
			codeExtended['e']['d'] = 324;
			codeExtended['E']['D'] = 325;
			codeExtended['E']['d'] = 326;
			codeExtended['e']['D'] = 327;
			codeExtended['i']['s'] = 328;
			codeExtended['I']['S'] = 329;
			codeExtended['I']['s'] = 330;
			codeExtended['i']['S'] = 331;
			codeExtended['i']['t'] = 332;
			codeExtended['I']['T'] = 333;
			codeExtended['I']['t'] = 334;
			codeExtended['i']['T'] = 335;
			codeExtended['a']['l'] = 336;
			codeExtended['A']['L'] = 337;
			codeExtended['A']['l'] = 338;
			codeExtended['a']['L'] = 339;
			codeExtended['a']['r'] = 340;
			codeExtended['A']['R'] = 341;
			codeExtended['A']['r'] = 342;
			codeExtended['a']['R'] = 343;
			codeExtended['s']['t'] = 344;
			codeExtended['S']['T'] = 345;
			codeExtended['S']['t'] = 346;
			codeExtended['s']['T'] = 347;
			codeExtended['t']['o'] = 348;
			codeExtended['T']['O'] = 349;
			codeExtended['T']['o'] = 350;
			codeExtended['t']['O'] = 351;
			codeExtended['n']['t'] = 352;
			codeExtended['N']['T'] = 353;
			codeExtended['N']['t'] = 354;
			codeExtended['n']['T'] = 355;
			codeExtended['n']['g'] = 356;
			codeExtended['N']['G'] = 357;
			codeExtended['N']['g'] = 358;
			codeExtended['n']['G'] = 359;
			codeExtended['s']['e'] = 368;
			codeExtended['S']['E'] = 369;
			codeExtended['S']['e'] = 370;
			codeExtended['s']['E'] = 371;
			codeExtended['h']['a'] = 372;
			codeExtended['H']['A'] = 373;
			codeExtended['H']['a'] = 374;
			codeExtended['h']['A'] = 375;
			codeExtended['a']['s'] = 376;
			codeExtended['A']['S'] = 377;
			codeExtended['A']['s'] = 378;
			codeExtended['a']['S'] = 379;
			codeExtended['o']['u'] = 380;
			codeExtended['O']['U'] = 381;
			codeExtended['O']['u'] = 382;
			codeExtended['o']['U'] = 383;
			codeExtended['i']['o'] = 384;
			codeExtended['I']['O'] = 385;
			codeExtended['I']['o'] = 386;
			codeExtended['i']['O'] = 387;
			codeExtended['l']['e'] = 388;
			codeExtended['L']['E'] = 389;
			codeExtended['L']['e'] = 390;
			codeExtended['l']['E'] = 391;
			codeExtended['v']['e'] = 392;
			codeExtended['V']['E'] = 393;
			codeExtended['V']['e'] = 394;
			codeExtended['v']['E'] = 395;
			codeExtended['c']['o'] = 396;
			codeExtended['C']['O'] = 397;
			codeExtended['C']['o'] = 398;
			codeExtended['c']['O'] = 399;
			codeExtended['m']['e'] = 400;
			codeExtended['M']['E'] = 401;
			codeExtended['M']['e'] = 402;
			codeExtended['m']['E'] = 403;
			codeExtended['d']['e'] = 404;
			codeExtended['D']['E'] = 405;
			codeExtended['D']['e'] = 406;
			codeExtended['d']['E'] = 407;
			codeExtended['h']['i'] = 408;
			codeExtended['H']['I'] = 409;
			codeExtended['H']['i'] = 410;
			codeExtended['h']['I'] = 411;
			codeExtended['r']['i'] = 412;
			codeExtended['R']['I'] = 413;
			codeExtended['R']['i'] = 414;
			codeExtended['r']['I'] = 415;
			codeExtended['r']['o'] = 416;
			codeExtended['R']['O'] = 417;
			codeExtended['R']['o'] = 418;
			codeExtended['r']['O'] = 419;
			codeExtended['i']['c'] = 420;
			codeExtended['I']['C'] = 421;
			codeExtended['I']['c'] = 422;
			codeExtended['i']['C'] = 423;
			codeExtended['n']['e'] = 440;
			codeExtended['N']['E'] = 441;
			codeExtended['N']['e'] = 442;
			codeExtended['n']['E'] = 443;
			codeExtended['e']['a'] = 444;
			codeExtended['E']['A'] = 445;
			codeExtended['E']['a'] = 446;
			codeExtended['e']['A'] = 447;
			codeExtended['r']['a'] = 448;
			codeExtended['R']['A'] = 449;
			codeExtended['R']['a'] = 450;
			codeExtended['r']['A'] = 451;
			codeExtended['c']['e'] = 452;
			codeExtended['C']['E'] = 453;
			codeExtended['C']['e'] = 454;
			codeExtended['c']['E'] = 455;
			codeExtended['l']['i'] = 456;
			codeExtended['L']['I'] = 457;
			codeExtended['L']['i'] = 458;
			codeExtended['l']['I'] = 459;
			codeExtended['c']['h'] = 460;
			codeExtended['C']['H'] = 461;
			codeExtended['C']['h'] = 462;
			codeExtended['c']['H'] = 463;
			codeExtended['l']['l'] = 464;
			codeExtended['L']['L'] = 465;
			codeExtended['L']['l'] = 466;
			codeExtended['l']['L'] = 467;
			codeExtended['b']['e'] = 468;
			codeExtended['B']['E'] = 469;
			codeExtended['B']['e'] = 470;
			codeExtended['b']['E'] = 471;
			codeExtended['m']['a'] = 472;
			codeExtended['M']['A'] = 473;
			codeExtended['M']['a'] = 474;
			codeExtended['m']['A'] = 475;
			codeExtended['s']['i'] = 476;
			codeExtended['S']['I'] = 477;
			codeExtended['S']['i'] = 478;
			codeExtended['s']['I'] = 479;
			codeExtended['o']['m'] = 480;
			codeExtended['O']['M'] = 481;
			codeExtended['O']['m'] = 482;
			codeExtended['o']['M'] = 483;
			codeExtended['u']['r'] = 484;
			codeExtended['U']['R'] = 485;
			codeExtended['U']['r'] = 486;
			codeExtended['u']['R'] = 487;
			codeExtended['c']['a'] = 488;
			codeExtended['C']['A'] = 489;
			codeExtended['C']['a'] = 490;
			codeExtended['c']['A'] = 491;
			codeExtended['e']['l'] = 492;
			codeExtended['E']['L'] = 493;
			codeExtended['E']['l'] = 494;
			codeExtended['e']['L'] = 495;
			codeExtended['t']['a'] = 496;
			codeExtended['T']['A'] = 497;
			codeExtended['T']['a'] = 498;
			codeExtended['t']['A'] = 499;
			codeExtended['l']['a'] = 500;
			codeExtended['L']['A'] = 501;
			codeExtended['L']['a'] = 502;
			codeExtended['l']['A'] = 503;
			codeExtended['n']['s'] = 504;
			codeExtended['N']['S'] = 505;
			codeExtended['N']['s'] = 506;
			codeExtended['n']['S'] = 507;
			codeExtended['d']['i'] = 508;
			codeExtended['D']['I'] = 509;
			codeExtended['D']['i'] = 510;
			codeExtended['d']['I'] = 511;
			codeExtended['f']['o'] = 512;
			codeExtended['F']['O'] = 513;
			codeExtended['F']['o'] = 514;
			codeExtended['f']['O'] = 515;
			codeExtended['h']['o'] = 516;
			codeExtended['H']['O'] = 517;
			codeExtended['H']['o'] = 518;
			codeExtended['h']['O'] = 519;
			codeExtended['p']['e'] = 520;
			codeExtended['P']['E'] = 521;
			codeExtended['P']['e'] = 522;
			codeExtended['p']['E'] = 523;
			codeExtended['e']['c'] = 524;
			codeExtended['E']['C'] = 525;
			codeExtended['E']['c'] = 526;
			codeExtended['e']['C'] = 527;
			codeExtended['p']['r'] = 528;
			codeExtended['P']['R'] = 529;
			codeExtended['P']['r'] = 530;
			codeExtended['p']['R'] = 531;
			codeExtended['n']['o'] = 532;
			codeExtended['N']['O'] = 533;
			codeExtended['N']['o'] = 534;
			codeExtended['n']['O'] = 535;
			codeExtended['c']['t'] = 536;
			codeExtended['C']['T'] = 537;
			codeExtended['C']['t'] = 538;
			codeExtended['c']['T'] = 539;
			codeExtended['u']['s'] = 540;
			codeExtended['U']['S'] = 541;
			codeExtended['U']['s'] = 542;
			codeExtended['u']['S'] = 543;
			codeExtended['a']['c'] = 544;
			codeExtended['A']['C'] = 545;
			codeExtended['A']['c'] = 546;
			codeExtended['a']['C'] = 547;
			codeExtended['o']['t'] = 548;
			codeExtended['O']['T'] = 549;
			codeExtended['O']['t'] = 550;
			codeExtended['o']['T'] = 551;
			codeExtended['i']['l'] = 552;
			codeExtended['I']['L'] = 553;
			codeExtended['I']['l'] = 554;
			codeExtended['i']['L'] = 555;
			codeExtended['t']['r'] = 556;
			codeExtended['T']['R'] = 557;
			codeExtended['T']['r'] = 558;
			codeExtended['t']['R'] = 559;
			codeExtended['l']['y'] = 560;
			codeExtended['L']['Y'] = 561;
			codeExtended['L']['y'] = 562;
			codeExtended['l']['Y'] = 563;
			codeExtended['n']['c'] = 564;
			codeExtended['N']['C'] = 565;
			codeExtended['N']['c'] = 566;
			codeExtended['n']['C'] = 567;
			codeExtended['e']['t'] = 568;
			codeExtended['E']['T'] = 569;
			codeExtended['E']['t'] = 570;
			codeExtended['e']['T'] = 571;
			codeExtended['u']['t'] = 572;
			codeExtended['U']['T'] = 573;
			codeExtended['U']['t'] = 574;
			codeExtended['u']['T'] = 575;
			codeExtended['s']['s'] = 576;
			codeExtended['S']['S'] = 577;
			codeExtended['S']['s'] = 578;
			codeExtended['s']['S'] = 579;
			codeExtended['s']['o'] = 580;
			codeExtended['S']['O'] = 581;
			codeExtended['S']['o'] = 582;
			codeExtended['s']['O'] = 583;
			codeExtended['r']['s'] = 584;
			codeExtended['R']['S'] = 585;
			codeExtended['R']['s'] = 586;
			codeExtended['r']['S'] = 587;
			codeExtended['u']['n'] = 588;
			codeExtended['U']['N'] = 589;
			codeExtended['U']['n'] = 590;
			codeExtended['u']['N'] = 591;
			codeExtended['l']['o'] = 592;
			codeExtended['L']['O'] = 593;
			codeExtended['L']['o'] = 594;
			codeExtended['l']['O'] = 595;
			codeExtended['w']['a'] = 596;
			codeExtended['W']['A'] = 597;
			codeExtended['W']['a'] = 598;
			codeExtended['w']['A'] = 599;
			codeExtended['g']['e'] = 600;
			codeExtended['G']['E'] = 601;
			codeExtended['G']['e'] = 602;
			codeExtended['g']['E'] = 603;
			codeExtended['i']['e'] = 604;
			codeExtended['I']['E'] = 605;
			codeExtended['I']['e'] = 606;
			codeExtended['i']['E'] = 607;
			codeExtended['w']['h'] = 608;
			codeExtended['W']['H'] = 609;
			codeExtended['W']['h'] = 610;
			codeExtended['w']['H'] = 611;

			codeExtended2[1]['E'] = 264;
			codeExtended2[0]['e'] = 265;
			codeExtended2[3]['e'] = 266;
			codeExtended2[2]['e'] = 267;
			codeExtended2[0]['E'] = 268;
			codeExtended2[1]['e'] = 269;
			codeExtended2[2]['E'] = 270;
			codeExtended2[3]['E'] = 271;
			codeExtended2[24]['d'] = 360;
			codeExtended2[25]['D'] = 361;
			codeExtended2[26]['d'] = 362;
			codeExtended2[27]['d'] = 363;
			codeExtended2[24]['D'] = 364;
			codeExtended2[25]['d'] = 365;
			codeExtended2[27]['D'] = 366;
			codeExtended2[26]['D'] = 367;
			codeExtended2[16]['g'] = 424;
			codeExtended2[17]['G'] = 425;
			codeExtended2[18]['g'] = 426;
			codeExtended2[19]['g'] = 427;
			codeExtended2[16]['G'] = 428;
			codeExtended2[17]['g'] = 429;
			codeExtended2[19]['G'] = 430;
			codeExtended2[18]['G'] = 431;
			codeExtended2[128]['n'] = 432;
			codeExtended2[129]['N'] = 433;
			codeExtended2[130]['n'] = 434;
			codeExtended2[131]['n'] = 435;
			codeExtended2[128]['N'] = 436;
			codeExtended2[129]['n'] = 437;
			codeExtended2[131]['N'] = 438;
			codeExtended2[130]['N'] = 439;
			codeExtended2[48]['o'] = 612;
			codeExtended2[49]['O'] = 613;
			codeExtended2[50]['o'] = 614;
			codeExtended2[51]['o'] = 615;
			codeExtended2[48]['O'] = 616;
			codeExtended2[49]['o'] = 617;
			codeExtended2[50]['O'] = 618;
			codeExtended2[51]['O'] = 619;
			codeExtended2[40]['t'] = 620;
			codeExtended2[41]['T'] = 621;
			codeExtended2[42]['t'] = 622;
			codeExtended2[43]['t'] = 623;
			codeExtended2[40]['T'] = 624;
			codeExtended2[41]['t'] = 625;
			codeExtended2[42]['T'] = 626;
			codeExtended2[43]['T'] = 627;
			codeExtended2[36]['i'] = 628;
			codeExtended2[37]['I'] = 629;
			codeExtended2[38]['i'] = 630;
			codeExtended2[39]['i'] = 631;
			codeExtended2[36]['I'] = 632;
			codeExtended2[37]['i'] = 633;
			codeExtended2[38]['I'] = 634;
			codeExtended2[39]['I'] = 635;
			codeExtended2[256]['r'] = 636;
			codeExtended2[257]['R'] = 637;
			codeExtended2[258]['r'] = 638;
			codeExtended2[259]['r'] = 639;
			codeExtended2[256]['R'] = 640;
			codeExtended2[257]['r'] = 641;
			codeExtended2[258]['R'] = 642;
			codeExtended2[259]['R'] = 643;
			codeExtended2[4]['r'] = 644;
			codeExtended2[5]['R'] = 645;
			codeExtended2[7]['r'] = 646;
			codeExtended2[6]['r'] = 647;
			codeExtended2[4]['R'] = 648;
			codeExtended2[5]['r'] = 649;
			codeExtended2[7]['R'] = 650;
			codeExtended2[6]['R'] = 651;

			codeExtended3[356]['n'] = 652;
			codeExtended3[357]['N'] = 653;
			codeExtended3[358]['n'] = 654;
			codeExtended3[359]['n'] = 655;
			codeExtended3[360]['n'] = 656;
			codeExtended3[356]['N'] = 657;
			codeExtended3[361]['n'] = 658;
			codeExtended3[362]['n'] = 659;
			codeExtended3[358]['N'] = 660;
			codeExtended3[363]['n'] = 661;
			codeExtended3[359]['N'] = 662;
			codeExtended3[360]['N'] = 663;
			codeExtended3[357]['n'] = 664;
			codeExtended3[361]['N'] = 665;
			codeExtended3[362]['N'] = 666;
			codeExtended3[363]['N'] = 667;

			// 1 - non ascii
			// 2 - ascii non char
			// 3 - ascii shouldn't be used
			// 4 - ascii normal but unlikely in DB

			/*
			 * freq.put(0, 100000); freq.put(1, 2); freq.put(2, 2); freq.put(3, 2);
			 * freq.put(4, 2); freq.put(5, 2); freq.put(6, 2); freq.put(7, 2);
			 * freq.put(8, 2); freq.put(9, 3); freq.put(10, 2); freq.put(11, 2);
			 * freq.put(12, 2); freq.put(13, 2); freq.put(14, 2); freq.put(15, 2);
			 * freq.put(16, 2); freq.put(17, 2); freq.put(18, 2); freq.put(19, 2);
			 * freq.put(20, 2); freq.put(21, 2); freq.put(22, 2); freq.put(23, 2);
			 * freq.put(24, 2); freq.put(25, 2); freq.put(26, 2); freq.put(27, 2);
			 * freq.put(28, 2); freq.put(29, 2); freq.put(30, 2); freq.put(31, 2);
			 * freq.put(32, 407934); freq.put(33, 170); freq.put(34, 4);
			 * freq.put(35, 425); freq.put(36, 1333); freq.put(37, 380);
			 * freq.put(38, 536); freq.put(39, 4); freq.put(40, 5176); freq.put(41,
			 * 5307); freq.put(42, 1493); freq.put(43, 511); freq.put(44, 17546);
			 * freq.put(45, 32638); freq.put(46, 35940); freq.put(47, 3681);
			 * freq.put(48, 50000); freq.put(49, 50000); freq.put(50, 50000);
			 * freq.put(51, 50000); freq.put(52, 50000); freq.put(53, 50000);
			 * freq.put(54, 50000); freq.put(55, 50000); freq.put(56, 50000);
			 * freq.put(57, 50000); freq.put(58, 10347); freq.put(59, 2884);
			 * freq.put(60, 2911); freq.put(61, 540); freq.put(62, 2952);
			 * freq.put(63, 3503); freq.put(64, 173); freq.put(65, 130731);
			 * freq.put(66, 29367); freq.put(67, 59494); freq.put(68, 67066);
			 * freq.put(69, 210175); freq.put(70, 35981); freq.put(71, 41523);
			 * freq.put(72, 70732); freq.put(73, 124119); freq.put(74, 6163);
			 * freq.put(75, 17680); freq.put(76, 79926); freq.put(77, 47446);
			 * freq.put(78, 123062); freq.put(79, 141497); freq.put(80, 43002);
			 * freq.put(81, 2525); freq.put(82, 107187); freq.put(83, 113326);
			 * freq.put(84, 159271); freq.put(85, 51835); freq.put(86, 22228);
			 * freq.put(87, 36979); freq.put(88, 5450); freq.put(89, 27646);
			 * freq.put(90, 1597); freq.put(91, 205); freq.put(92, 37); freq.put(93,
			 * 210); freq.put(94, 8); freq.put(95, 2755); freq.put(96, 4);
			 * freq.put(97, 130731); freq.put(98, 29367); freq.put(99, 59494);
			 * freq.put(100, 67066); freq.put(101, 210175); freq.put(102, 35981);
			 * freq.put(103, 41523); freq.put(104, 70732); freq.put(105, 124119);
			 * freq.put(106, 6163); freq.put(107, 17680); freq.put(108, 79926);
			 * freq.put(109, 47446); freq.put(110, 123062); freq.put(111, 141497);
			 * freq.put(112, 43002); freq.put(113, 2525); freq.put(114, 107187);
			 * freq.put(115, 113326); freq.put(116, 159271); freq.put(117, 51835);
			 * freq.put(118, 22228); freq.put(119, 36979); freq.put(120, 5450);
			 * freq.put(121, 27646); freq.put(122, 1597); freq.put(123, 62);
			 * freq.put(124, 16); freq.put(125, 61); freq.put(126, 8); freq.put(127,
			 * 2); int i = 128; while (i < 256) { freq.put(i++, 1); }
			 * 
			 * freq.put(256, 47342); //th freq.put(257, 47342); //TH freq.put(258,
			 * 47342); //tH freq.put(259, 47342); //Th freq.put(260, 40933); //he
			 * freq.put(261, 40933); //HE freq.put(262, 40933); //hE freq.put(263,
			 * 40933); //He freq.put(264, 32681); //THE freq.put(265, 32681); //the
			 * freq.put(266, 32681); //The freq.put(267, 32681); //tHe freq.put(268,
			 * 32681); //thE freq.put(269, 32681); //THe freq.put(270, 32681); //tHE
			 * freq.put(271, 32681); //ThE freq.put(272, 32386); //in freq.put(273,
			 * 32386); //IN freq.put(274, 32386); //In freq.put(275, 32386); //iN
			 * freq.put(276, 27267); //er freq.put(277, 27267); //ER freq.put(278,
			 * 27267); //Er freq.put(279, 27267); //eR freq.put(280, 26427); //an
			 * freq.put(281, 26427); //AN freq.put(282, 26427); //An freq.put(283,
			 * 26427); //aN freq.put(284, 24686); //re freq.put(285, 24686); //RE
			 * freq.put(286, 24686); //Re freq.put(287, 24686); //rE freq.put(288,
			 * 23404); //on freq.put(289, 23404); //ON freq.put(290, 23404); //On
			 * freq.put(291, 23404); //oN freq.put(292, 19792); //at freq.put(293,
			 * 19792); //AT freq.put(294, 19792); //At freq.put(295, 19792); //aT
			 * freq.put(296, 19359); //en freq.put(297, 19359); //EN freq.put(298,
			 * 19359); //En freq.put(299, 19359); //eN freq.put(300, 18002); //nd
			 * freq.put(301, 18002); //ND freq.put(302, 18002); //Nd freq.put(303,
			 * 18002); //nD freq.put(304, 17873); //ti freq.put(305, 17873); //TI
			 * freq.put(306, 17873); //Ti freq.put(307, 17873); //tI freq.put(308,
			 * 17830); //es freq.put(309, 17830); //ES freq.put(310, 17830); //Es
			 * freq.put(311, 17830); //eS freq.put(312, 16994); //or freq.put(313,
			 * 16994); //OR freq.put(314, 16994); //Or freq.put(315, 16994); //oR
			 * freq.put(316, 16040); //te freq.put(317, 16040); //TE freq.put(318,
			 * 16040); //Te freq.put(319, 16040); //tE freq.put(320, 15642); //of
			 * freq.put(321, 15642); //OF freq.put(322, 15642); //Of freq.put(323,
			 * 15642); //oF freq.put(324, 15550); //ed freq.put(325, 15550); //ED
			 * freq.put(326, 15550); //Ed freq.put(327, 15550); //eD freq.put(328,
			 * 15022); //is freq.put(329, 15022); //IS freq.put(330, 15022); //Is
			 * freq.put(331, 15022); //iS freq.put(332, 14953); //it freq.put(333,
			 * 14953); //IT freq.put(334, 14953); //It freq.put(335, 14953); //iT
			 * freq.put(336, 14476); //al freq.put(337, 14476); //AL freq.put(338,
			 * 14476); //Al freq.put(339, 14476); //aL freq.put(340, 14309); //ar
			 * freq.put(341, 14309); //AR freq.put(342, 14309); //Ar freq.put(343,
			 * 14309); //aR freq.put(344, 14024); //st freq.put(345, 14024); //ST
			 * freq.put(346, 14024); //St freq.put(347, 14024); //sT freq.put(348,
			 * 13862); //to freq.put(349, 13862); //TO freq.put(350, 13862); //To
			 * freq.put(351, 13862); //tO freq.put(352, 13861); //nt freq.put(353,
			 * 13861); //NT freq.put(354, 13861); //Nt freq.put(355, 13861); //nT
			 * freq.put(356, 12687); //ng freq.put(357, 12687); //NG freq.put(358,
			 * 12687); //Ng freq.put(359, 12687); //nG freq.put(360, 12496); //and
			 * freq.put(361, 12496); //AND freq.put(362, 12496); //And freq.put(363,
			 * 12496); //aNd freq.put(364, 12496); //anD freq.put(365, 12496); //ANd
			 * freq.put(366, 12496); //aND freq.put(367, 12496); //AnD freq.put(368,
			 * 12408); //se freq.put(369, 12408); //SE freq.put(370, 12408); //Se
			 * freq.put(371, 12408); //sE freq.put(372, 12324); //ha freq.put(373,
			 * 12324); //HA freq.put(374, 12324); //Ha freq.put(375, 12324); //hA
			 * freq.put(376, 11596); //as freq.put(377, 11596); //AS freq.put(378,
			 * 11596); //As freq.put(379, 11596); //aS freq.put(380, 11582); //ou
			 * freq.put(381, 11582); //OU freq.put(382, 11582); //Ou freq.put(383,
			 * 11582); //oU freq.put(384, 11115); //io freq.put(385, 11115); //IO
			 * freq.put(386, 11115); //Io freq.put(387, 11115); //iO freq.put(388,
			 * 11039); //le freq.put(389, 11039); //LE freq.put(390, 11039); //Le
			 * freq.put(391, 11039); //lE freq.put(392, 10986); //ve freq.put(393,
			 * 10986); //VE freq.put(394, 10986); //Ve freq.put(395, 10986); //vE
			 * freq.put(396, 10568); //co freq.put(397, 10568); //CO freq.put(398,
			 * 10568); //Co freq.put(399, 10568); //cO freq.put(400, 10557); //me
			 * freq.put(401, 10557); //ME freq.put(402, 10557); //Me freq.put(403,
			 * 10557); //mE freq.put(404, 10181); //de freq.put(405, 10181); //DE
			 * freq.put(406, 10181); //De freq.put(407, 10181); //dE freq.put(408,
			 * 10160); //hi freq.put(409, 10160); //HI freq.put(410, 10160); //Hi
			 * freq.put(411, 10160); //hI freq.put(412, 9686); //ri freq.put(413,
			 * 9686); //RI freq.put(414, 9686); //Ri freq.put(415, 9686); //rI
			 * freq.put(416, 9674); //ro freq.put(417, 9674); //RO freq.put(418,
			 * 9674); //Ro freq.put(419, 9674); //rO freq.put(420, 9301); //ic
			 * freq.put(421, 9301); //IC freq.put(422, 9301); //Ic freq.put(423,
			 * 9301); //iC freq.put(424, 10051); //ing freq.put(425, 10051); //ING
			 * freq.put(426, 10051); //Ing freq.put(427, 10051); //iNg freq.put(428,
			 * 10051); //inG freq.put(429, 10051); //INg freq.put(430, 10051); //iNG
			 * freq.put(431, 10051); //InG freq.put(432, 9654); //ion freq.put(433,
			 * 9654); //ION freq.put(434, 9654); //Ion freq.put(435, 9654); //iOn
			 * freq.put(436, 9654); //ioN freq.put(437, 9654); //IOn freq.put(438,
			 * 9654); //iON freq.put(439, 9654); //IoN freq.put(440, 9208); //ne
			 * freq.put(441, 9208); //NE freq.put(442, 9208); //Ne freq.put(443,
			 * 9208); //nE freq.put(444, 9161); //ea freq.put(445, 9161); //EA
			 * freq.put(446, 9161); //Ea freq.put(447, 9161); //eA freq.put(448,
			 * 9127); //ra freq.put(449, 9127); //RA freq.put(450, 9127); //Ra
			 * freq.put(451, 9127); //rA freq.put(452, 8672); //ce freq.put(453,
			 * 8672); //CE freq.put(454, 8672); //Ce freq.put(455, 8672); //cE
			 * freq.put(456, 8311); //li freq.put(457, 8311); //LI freq.put(458,
			 * 8311); //Li freq.put(459, 8311); //lI freq.put(460, 7957); //ch
			 * freq.put(461, 7957); //CH freq.put(462, 7957); //Ch freq.put(463,
			 * 7957); //cH freq.put(464, 7675); //ll freq.put(465, 7675); //LL
			 * freq.put(466, 7675); //Ll freq.put(467, 7675); //lL freq.put(468,
			 * 7671); //be freq.put(469, 7671); //BE freq.put(470, 7671); //Be
			 * freq.put(471, 7671); //bE freq.put(472, 7525); //ma freq.put(473,
			 * 7525); //MA freq.put(474, 7525); //Ma freq.put(475, 7525); //mA
			 * freq.put(476, 7322); //si freq.put(477, 7322); //SI freq.put(478,
			 * 7322); //Si freq.put(479, 7322); //sI freq.put(480, 7272); //om
			 * freq.put(481, 7272); //OM freq.put(482, 7272); //Om freq.put(483,
			 * 7272); //oM freq.put(484, 7225); //ur freq.put(485, 7225); //UR
			 * freq.put(486, 7225); //Ur freq.put(487, 7225); //uR freq.put(488,
			 * 7164); //ca freq.put(489, 7164); //CA freq.put(490, 7164); //Ca
			 * freq.put(491, 7164); //cA freq.put(492, 7059); //el freq.put(493,
			 * 7059); //EL freq.put(494, 7059); //El freq.put(495, 7059); //eL
			 * freq.put(496, 7054); //ta freq.put(497, 7054); //TA freq.put(498,
			 * 7054); //Ta freq.put(499, 7054); //tA freq.put(500, 7022); //la
			 * freq.put(501, 7022); //LA freq.put(502, 7022); //La freq.put(503,
			 * 7022); //lA freq.put(504, 6775); //ns freq.put(505, 6775); //NS
			 * freq.put(506, 6775); //Ns freq.put(507, 6775); //nS freq.put(508,
			 * 6562); //di freq.put(509, 6562); //DI freq.put(510, 6562); //Di
			 * freq.put(511, 6562); //dI freq.put(512, 6493); //fo freq.put(513,
			 * 6493); //FO freq.put(514, 6493); //Fo freq.put(515, 6493); //fO
			 * freq.put(516, 6455); //ho freq.put(517, 6455); //HO freq.put(518,
			 * 6455); //Ho freq.put(519, 6455); //hO freq.put(520, 6363); //pe
			 * freq.put(521, 6363); //PE Alex wrote "pe" and other lines in this
			 * area while saying pee-pee freq.put(522, 6363); //Pe freq.put(523,
			 * 6363); //pE freq.put(524, 6353); //ec freq.put(525, 6353); //EC
			 * freq.put(526, 6353); //Ec freq.put(527, 6353); //eC freq.put(528,
			 * 6316); //pr freq.put(529, 6316); //PR freq.put(530, 6316); //Pr
			 * freq.put(531, 6316); //pR freq.put(532, 6184); //no freq.put(533,
			 * 6184); //NO freq.put(534, 6184); //No freq.put(535, 6184); //nO
			 * freq.put(536, 6136); //ct freq.put(537, 6136); //CT freq.put(538,
			 * 6136); //Ct freq.put(539, 6136); //cT freq.put(540, 6047); //us
			 * freq.put(541, 6047); //US freq.put(542, 6047); //Us freq.put(543,
			 * 6047); //uS freq.put(544, 5961); //ac freq.put(545, 5961); //AC
			 * freq.put(546, 5961); //Ac freq.put(547, 5961); //aC freq.put(548,
			 * 5885); //ot freq.put(549, 5885); //OT freq.put(550, 5885); //Ot
			 * freq.put(551, 5885); //oT freq.put(552, 5744); //il freq.put(553,
			 * 5744); //IL freq.put(554, 5744); //Il freq.put(555, 5744); //iL
			 * freq.put(556, 5668); //tr freq.put(557, 5668); //TR freq.put(558,
			 * 5668); //Tr freq.put(559, 5668); //tR freq.put(560, 5658); //ly
			 * freq.put(561, 5658); //LY freq.put(562, 5658); //Ly freq.put(563,
			 * 5658); //lY freq.put(564, 5534); //nc freq.put(565, 5534); //NC
			 * freq.put(566, 5534); //Nc freq.put(567, 5534); //nC freq.put(568,
			 * 5492); //et freq.put(569, 5492); //ET freq.put(570, 5492); //Et
			 * freq.put(571, 5492); //eT freq.put(572, 5393); //ut freq.put(573,
			 * 5393); //UT freq.put(574, 5393); //Ut freq.put(575, 5393); //uT
			 * freq.put(576, 5392); //ss freq.put(577, 5392); //SS freq.put(578,
			 * 5392); //Ss freq.put(579, 5392); //sS freq.put(580, 5294); //so
			 * freq.put(581, 5294); //SO freq.put(582, 5294); //So freq.put(583,
			 * 5294); //sO freq.put(584, 5278); //rs freq.put(585, 5278); //RS
			 * freq.put(586, 5278); //Rs freq.put(587, 5278); //rS freq.put(588,
			 * 5250); //un freq.put(589, 5250); //UN freq.put(590, 5250); //Un
			 * freq.put(591, 5250); //uN freq.put(592, 5150); //lo freq.put(593,
			 * 5150); //LO freq.put(594, 5150); //Lo freq.put(595, 5150); //lO
			 * freq.put(596, 5129); //wa freq.put(597, 5129); //WA freq.put(598,
			 * 5129); //Wa freq.put(599, 5129); //wA freq.put(600, 5127); //ge
			 * freq.put(601, 5127); //GE freq.put(602, 5127); //Ge freq.put(603,
			 * 5127); //gE freq.put(604, 5120); //ie freq.put(605, 5120); //IE
			 * freq.put(606, 5120); //Ie freq.put(607, 5120); //iE freq.put(608,
			 * 5042); //wh freq.put(609, 5042); //WH freq.put(610, 5042); //Wh
			 * freq.put(611, 5042); //wH freq.put(612, 7941); //tio freq.put(613,
			 * 7941); //TIO freq.put(614, 7941); //Tio freq.put(615, 7941); //tIo
			 * freq.put(616, 7941); //tiO freq.put(617, 7941); //TIo freq.put(618,
			 * 7941); //TiO freq.put(619, 7941); //tIO freq.put(620, 7324); //ent
			 * freq.put(621, 7324); //ENT freq.put(622, 7324); //Ent freq.put(623,
			 * 7324); //eNt freq.put(634, 7324); //enT freq.put(625, 7324); //ENt
			 * freq.put(626, 7324); //EnT freq.put(627, 7324); //eNT freq.put(628,
			 * 5560); //ati freq.put(629, 5560); //ATI freq.put(630, 5560); //Ati
			 * freq.put(631, 5560); //aTi freq.put(632, 5560); //atI freq.put(633,
			 * 5560); //ATi freq.put(634, 5560); //AtI freq.put(635, 5560); //aTI
			 * freq.put(636, 5359); //for freq.put(637, 5359); //FOR freq.put(638,
			 * 5359); //For freq.put(639, 5359); //fOr freq.put(640, 5359); //foR
			 * freq.put(641, 5359); //FOr freq.put(642, 5359); //FoR freq.put(643,
			 * 5359); //fOR freq.put(644, 5156); //her freq.put(645, 5156); //HER
			 * freq.put(646, 5156); //Her freq.put(647, 5156); //hEr freq.put(648,
			 * 5156); //heR freq.put(649, 5156); //HEr freq.put(650, 5156); //HeR
			 * freq.put(651, 5156); //hER freq.put(652, 7868); //tion freq.put(653,
			 * 7868); //TION freq.put(654, 7868); //Tion freq.put(655, 7868); //tIon
			 * freq.put(656, 7868); //tiOn freq.put(657, 7868); //tioN freq.put(658,
			 * 7868); //TIon freq.put(659, 7868); //TiOn freq.put(660, 7868); //TioN
			 * freq.put(661, 7868); //tIOn freq.put(662, 7868); //tIoN freq.put(663,
			 * 7868); //tiON freq.put(664, 7868); //TIOn freq.put(665, 7868); //TIoN
			 * freq.put(666, 7868); //TiON freq.put(667, 7868); //tION
			 * 
			 * buildTree(); for (int key : freq.keySet()) { tree =
			 * treeParts.get(key); }
			 * 
			 * TreeMap<Integer, String> codes = new TreeMap<Integer, String>();
			 * traverse(tree, codes, ""); for (Entry entry : codes.entrySet()) {
			 * encodeLength[(Integer)entry.getKey()] =
			 * ((String)entry.getValue()).length(); encode[(Integer)entry.getKey()]
			 * = Integer.parseInt((String)entry.getValue(), 2); }
			 * 
			 * freq.clear(); treeParts.clear(); long start1 =
			 * System.currentTimeMillis(); buildDecode1(); long end1 =
			 * System.currentTimeMillis(); System.out.println("buildDecode1() took "
			 * + (((end1 - start1) * 1.0) / 1000.0) + "s"); long start2 =
			 * System.currentTimeMillis(); buildDecode2(); long end2 =
			 * System.currentTimeMillis(); System.out.println("buildDecode2() took "
			 * + (((end2 - start2) * 1.0) / 1000.0) + "s"); buildDecode3(); tree =
			 * null; long end = System.currentTimeMillis();
			 * System.out.println("Init took " + (((end - start) * 1.0) / 1000.0) +
			 * "s");
			 * 
			 * i = 0; int maxLength = 0; while (i < NUM_SYM) {
			 * System.out.println("encode[" + i + "] = " + encode[i]);
			 * System.out.println("encodeLength[" + i + "] = " + encodeLength[i]);
			 * if (encodeLength[i] > maxLength) { maxLength = encodeLength[i]; }
			 * i++; }
			 * 
			 * System.out.println("MAXLENGTH = " + maxLength); try {
			 * writeDataFile(); } catch(Exception e) { e.printStackTrace(); }
			 */

			try
			{
				readDataFile();
			}
			catch (Exception e)
			{
				HRDBMSWorker.logger.debug("", e);
				System.exit(1);
			}

			HRDBMSWorker.logger.debug("CVarcharFV end init");
		}
		private String value;
		private byte[] bytes = null;

		private int size;

		public CVarcharFV()
		{
		}

		public CVarcharFV(int row, int col, Schema s) throws Exception
		{
			super(row, col, s);

			if (!isNull && exists)
			{
				int lenlen = s.cachedLenLen;
				if (lenlen == -1)
				{
					s.cachedLenLen = lenlen = s.p.get(0);
				}
				int len = 0;
				if (lenlen == 1)
				{
					len = s.p.get(off) & 0xff;
				}
				else if (lenlen == 2)
				{
					len = s.p.getShort(off) & 0xffff;
				}
				else
				{
					len = (s.p.getInt(off - 1) & 0x00ffffff);
				}
				
				//if (s.p.block().number() == 2)
				//{
				//	HRDBMSWorker.logger.debug("Reading row - offset was " + off + " lenlen was " + lenlen + " len was " + len); 
				//}
				
				if (len == 0)
				{
					value = new String();
				}
				else
				{
					if (!compress)
					{
						char[] ca = new char[len];
						final byte[] temp = new byte[len];
						bytes = temp;
						size = lenlen + len;
						s.p.get(off + lenlen, temp);
						try
						{
							// value = new String(temp, "UTF-8");
							value = (String)unsafe.allocateInstance(String.class);
							int clen = ((sun.nio.cs.ArrayDecoder)s.cd).decode(temp, 0, len, ca);
							if (clen == ca.length)
							{
								unsafe.putObject(value, Schema.offset, ca);
							}
							else
							{
								char[] v = Arrays.copyOf(ca, clen);
								unsafe.putObject(value, Schema.offset, v);
							}
						}
						catch (final Exception e)
						{
							throw e;
						}
					}
					else
					{
						try
						{
							//HRDBMSWorker.logger.debug("Entering...");
							byte[] temp = new byte[len];
							size = lenlen + len;
							s.p.get(off + lenlen, temp);
							
							byte[] temp2 = new byte[len << 1];
							len = decompress(temp, len, temp2);
							if (len < 0)
							{
								Exception e = new Exception("Length of " + len + " returned by decompress");
								throw e;
							}

							char[] ca = new char[len];
							// bytes = temp2; //Do we need this? I don't think
							// so...

							// value =
							// (String)unsafe.allocateInstance(String.class);
							int clen = ((sun.nio.cs.ArrayDecoder)s.cd).decode(temp2, 0, len, ca);
							if (clen == ca.length)
							{
								value = new String(ca);
							}
							else
							{
								char[] v = Arrays.copyOf(ca, clen);
								value = new String(v);
							}
							
							//HRDBMSWorker.logger.debug("Compressed length was " + (size - lenlen) + " uncomp length was " + len + " string is: " + value);
						}
						catch (final Throwable e)
						{
							HRDBMSWorker.logger.debug("", e);
							throw e;
						}
					}
				}
			}
		}

		public CVarcharFV(VarcharFV fv, Schema s) throws Exception
		{
			this.exists = true;
			this.isNull = fv.isNull;
			if (!compress)
			{
				this.bytes = fv.bytes;
				int lenlen = s.cachedLenLen;
				if (lenlen == -1)
				{
					s.cachedLenLen = lenlen = s.p.get(0);
				}
				size = lenlen + bytes.length;
			}
			else
			{
				try
				{
					byte[] temp = new byte[fv.bytes.length * 3 + 1];
					int len = compress(fv.bytes, fv.bytes.length, temp);
					if (len < 0)
					{
						Exception e = new Exception("Length of " + len + " returned by compress");
						throw e;
					}
					bytes = Arrays.copyOf(temp, len);
					int lenlen = s.cachedLenLen;
					if (lenlen == -1)
					{
						s.cachedLenLen = lenlen = s.p.get(0);
					}
					size = lenlen + bytes.length;
				}
				catch (Exception e)
				{
					HRDBMSWorker.logger.debug("", e);
					throw e;
				}
			}
		}

		public static int compress(byte[] in, int inLen, byte[] out)
		{
			int retval = 0;
			int i = 0;
			short x = 32;
			int value = 0;
			int remainder = 0;
			int remLen = 0;
			ByteBuffer bb = ByteBuffer.wrap(out);
			int coded = 0;
			int length = 0;
			while (i <= inLen || remLen != 0)
			{
				if (remLen != 0)
				{
					coded = remainder;
					remainder = 0;
					length = remLen;
					remLen = 0;
				}
				else
				{
					int code = 0;
					if (i < inLen)
					{
						int temp = in[i++] & 0xff;
						int temp2 = 0;
						if (i < inLen)
						{
							temp2 = in[i] & 0xff;
							int temp3 = codeExtended[temp][temp2];
							if (temp3 != 0)
							{
								int temp4 = 0;
								if (i + 1 < inLen)
								{
									temp4 = in[i + 1] & 0xff;
									int temp5 = codeExtended2[temp3 - 256][temp4];
									if (temp5 != 0)
									{
										int temp6 = 0;
										if (i + 2 < inLen)
										{
											temp6 = in[i + 2] & 0xff;
											int temp7 = codeExtended3[temp5 - 256][temp6];
											if (temp7 != 0)
											{
												i += 3;
												code = temp7;
											}
											else
											{
												i += 2;
												code = temp5;
											}
										}
										else
										{
											i += 2;
											code = temp5;
										}
									}
									else
									{
										i++;
										code = temp3;
									}
								}
								else
								{
									i++;
									code = temp3;
								}
							}
							else
							{
								code = temp;
							}
						}
						else
						{
							code = temp;
						}
					}
					else
					{
						i++;
					}
					coded = encode[code];
					length = encodeLength[code];
				}

				if (x - length >= 0)
				{
					value |= (coded << (x - length));
					x -= length;
				}
				else
				{
					value |= (coded >>> (length - x));
					remLen = length - x;
					remainder = coded & masks[remLen];
					x = 0;
				}

				if (x == 0)
				{
					bb.putInt(value);
					retval += 4;
					x = 32;
					value = 0;
				}
			}

			if (x == 32)
			{
				return retval;
			}

			if (x >= 24)
			{
				bb.put((byte)((value >>> 24)));
				retval++;
			}
			else if (x >= 16)
			{
				bb.putShort((short)((value >>> 16)));
				retval += 2;
			}
			else if (x >= 8)
			{
				bb.putShort((short)((value >>> 16)));
				bb.put((byte)((value >>> 8)));
				retval += 3;
			}
			else
			{
				bb.putInt(value);
				retval += 4;
			}

			return retval;
		}

		public static int decompress(byte[] in, int inLen, byte[] out) throws Exception
		{
			int i = 0;
			int o = 0;
			ByteBuffer bb = ByteBuffer.wrap(in);
			int remainder = 0;
			int remLen = 0;
			Code code = null;
			int value = 0;

			while (true)
			{
				int shift = 0;
				int shiftLen = 0;

				if (remLen >= 24)
				{
					shiftLen = remLen - 24;
					shift = remainder & masks[shiftLen];
					remainder = (remainder >>> (remLen - 24));
					value = 0;
				}
				else 
				{
					final int free = 24 - remLen;
					value = (remainder << (free));
					int free2 = (inLen - i) << 3;
					
					if (free2 > free)
					{
						free2 = free;
					}
					
					if (free2 > 16)
					{
						shift = ((bb.getShort(i) & 0xffff) << 8) | (bb.get(i+2) & 0xff);
						i += 3;
						remainder = (shift >>> remLen);
						shiftLen = remLen;
						shift = shift & masks[shiftLen];
					}
					else if (free2 > 8)
					{
						remainder = bb.getShort(i) & 0xffff;
						i += 2;
						if (free >= 16)
						{
							remainder = (remainder << (8 - remLen));
						}
						else
						{
							shiftLen = 16 - free;
							shift = remainder & masks[shiftLen];
							remainder = (remainder >>> shiftLen);
						}
					}
					else if (free2 > 0)
					{
						remainder = bb.get(i++) & 0xff;
						if (free >= 8)
						{
							remainder = (remainder << (16 - remLen));
						}
						else
						{
							shiftLen = 8 - free;
							shift = remainder & masks[shiftLen];
							remainder = (remainder >>> shiftLen);
						}
					}
					else
					{
						remainder = 0;
					}
				}
				
				value |= remainder;
				code = decode3[value];

				if (i < inLen)
				{
					System.arraycopy(code.bytes, 0, out, o, code.bytes.length);
					o += code.bytes.length;
				}
				else
				{
					for (byte b : code.bytes)
					{
						if (b == 0)
						{
							return o;
						}

						out[o++] = b;
					}
				}

				remLen = 24 - code.used;
				
				//if (code.used == 0)
				//{
				//	HRDBMSWorker.logger.debug("CODE.USED = ZERO for value " + value);
				//}
				
				remainder = value & masks[remLen];
				remainder = ((remainder << shiftLen) | shift);
				remLen += shiftLen;
			}
		}
		
		private static Code readCode(ByteBuffer bb, InputStream in) throws Exception
		{
			int size = 0;
			byte[] bytes = null;
			short used = 0;
			if (bb.remaining() > 0)
			{
				size = bb.get();
			}
			else
			{
				in.read(bb.array());
				bb.position(0);
				size = bb.get();
			}

			bytes = new byte[size];
			if (bb.remaining() >= size)
			{
				bb.get(bytes);
			}
			else if (bb.remaining() == 0)
			{
				in.read(bb.array());
				bb.position(0);
				bb.get(bytes);
			}
			else
			{
				int i = 0;
				while (i < size)
				{
					if (bb.remaining() > 0)
					{
						bytes[i++] = bb.get();
					}
					else
					{
						in.read(bb.array());
						bb.position(0);
						bytes[i++] = bb.get();
					}
				}
			}

			if (bb.remaining() > 0)
			{
				used = bb.get();
			}
			else
			{
				in.read(bb.array());
				bb.position(0);
				used = bb.get();
			}

			Code code = new Code();
			code.bytes = bytes;
			code.used = used;
			return code;
		}
		
		private static void readDataFile() throws Exception
		{
			//InputStream in = CVarcharFV.class.getResourceAsStream("/com/exascale/huffman/huffman.dat");
			InputStream in = new FileInputStream("huffman.dat");
			ByteBuffer bb = ByteBuffer.allocate(NUM_SYM * 4);
			in.read(bb.array());
			int i = 0;
			while (i < NUM_SYM)
			{
				encode[i] = bb.getInt();
				i++;
			}

			bb.position(0);
			in.read(bb.array());
			i = 0;
			while (i < NUM_SYM)
			{
				encodeLength[i] = bb.getInt();
				i++;
			}

			bb = ByteBuffer.allocate(8 * 1024 * 1024);
			in.read(bb.array());

			i = 0;
			while (i < 16777216)
			{
				decode3[i] = readCode(bb, in);
				i++;
			}

			in.close();
		}

		@Override
		public String getValue()
		{
			return value;
		}

		@Override
		public int size()
		{
			if (!isNull && exists)
			{
				return size;
			}

			return 0;
		}

		@Override
		public String toString()
		{
			return value;
		}

		public int write(byte[] buff, int off, Schema s) throws UnsupportedEncodingException
		{
			if (!isNull && exists)
			{
				final byte[] stringBytes = bytes;
				final byte[] val = ByteBuffer.allocate(4).putInt(stringBytes.length).array();
				// int i = 0;
				// while (i < 4)
				// {
				// buff[off + i] = val[i];
				// i++;
				// }
				int lenlen = s.cachedLenLen;
				if (lenlen == -1)
				{
					s.cachedLenLen = lenlen = s.p.get(0);
				}
				int start = 4 - lenlen;

				System.arraycopy(val, start, buff, off, lenlen);
				
				//if (s.p.block().number() == 2)
				//{
				//	HRDBMSWorker.logger.debug("Writing length = " + stringBytes.length + " byte = " + (buff[off] & 0xff) + " buffer = " + val[0] + " " + val[1] + " " + val[2] + " " + val[3] + " LENLEN = " + lenlen); //DEBUG
				//}

				// i = 0;
				// final int length = stringBytes.length;
				// while (i < length)
				// {
				// buff[off + 4 + i] = stringBytes[i];
				// i++;
				// }
				try
				{
					System.arraycopy(stringBytes, 0, buff, off + lenlen, stringBytes.length);
				}
				catch(Exception e)
				{
					HRDBMSWorker.logger.debug("Offset is " + off + " lenlen is " + lenlen + " bytes length is " + stringBytes.length + " buffer size is " + buff.length);
					throw e;
				}

				return size;
			}

			return 0;
		}

		private static class Code
		{
			public byte[] bytes;
			public short used;
		}
	}

	public static class DateFV extends FieldValue
	{
		private MyDate value;

		public DateFV(int row, int col, Schema s)
		{
			super(row, col, s);

			if (!isNull && exists)
			{
				value = new MyDate(s.p.getInt(off - 1) & 0x00ffffff);
			}
		}

		public DateFV(MyDate val)
		{
			this.exists = true;
			if (val == null)
			{
				this.isNull = true;
			}
			else
			{
				this.value = val;
			}
		}

		@Override
		public MyDate getValue()
		{
			return value;
		}

		@Override
		public int size()
		{
			if (!isNull && exists)
			{
				return 3;
			}

			return 0;
		}

		@Override
		public String toString()
		{
			return "" + value;
		}

		@Override
		public int write(byte[] buff, int off)
		{
			if (!isNull && exists)
			{
				final byte[] val = ByteBuffer.allocate(4).putInt(value.getTime()).array();
				// int i = 0;
				// while (i < 8)
				// {
				// buff[off + i] = val[i];
				// i++;
				// }
				System.arraycopy(val, 1, buff, off, 3);

				return 3;
			}

			return 0;
		}
	}

	public static class DecimalFV extends FieldValue
	{
		private BigDecimal value;
		private final int length;
		private byte[] bytes;

		public DecimalFV(int row, int col, int length, int scale, Schema s) throws Exception
		{
			super(row, col, s);
			this.length = length;

			if (!isNull && exists)
			{
				final int numBytes = (int)(Math.ceil(((length + 1) * 1.0) / 2.0));
				bytes = new byte[numBytes];
				s.p.get(off, bytes);
				final boolean positive = (bytes[0] & 0xF0) == 0;
				BigInteger temp = BigInteger.valueOf(bytes[0] & 0x0F);
				int i = 1;
				while (i < numBytes)
				{
					temp = temp.multiply(BigInteger.valueOf(100));
					temp = temp.add(BigInteger.valueOf(((bytes[i] & 0xF0) >> 4) * 10 + (bytes[i] & 0x0F)));
					i++;
				}

				if (!positive)
				{
					temp = temp.negate();
				}

				value = new BigDecimal(temp, scale);
			}
		}

		@Override
		public BigDecimal getValue()
		{
			return value;
		}

		@Override
		public int size()
		{
			if (!isNull && exists)
			{
				return (int)(Math.ceil(((length + 1) * 1.0) / 2.0));
			}

			return 0;
		}

		@Override
		public String toString()
		{
			return "" + value;
		}

		@Override
		public int write(byte[] buff, int off)
		{
			if (!isNull && exists)
			{
				// int i = 0;
				// final int length = bytes.length;
				// while (i < length)
				// {
				// buff[off + i] = bytes[i];
				// i++;
				// }
				System.arraycopy(bytes, 0, buff, off, bytes.length);

				return bytes.length;
			}

			return 0;
		}
	}

	public static class DoubleFV extends FieldValue
	{
		private double value;

		public DoubleFV(Double val)
		{
			this.exists = true;
			if (val == null)
			{
				this.isNull = true;
			}
			else
			{
				this.value = val;
			}
		}

		public DoubleFV(int row, int col, Schema s)
		{
			super(row, col, s);

			if (!isNull && exists)
			{
				value = s.p.getDouble(off);
			}
		}

		@Override
		public Double getValue()
		{
			return value;
		}

		@Override
		public int size()
		{
			if (!isNull && exists)
			{
				return 8;
			}

			return 0;
		}

		@Override
		public String toString()
		{
			return "" + value;
		}

		@Override
		public int write(byte[] buff, int off)
		{
			if (!isNull && exists)
			{
				final byte[] val = ByteBuffer.allocate(8).putDouble(value).array();
				// int i = 0;
				// while (i < 8)
				// {
				// buff[off + i] = val[i];
				// i++;
				// }
				System.arraycopy(val, 0, buff, off, 8);

				return 8;
			}

			return 0;
		}
	}

	public static abstract class FieldValue
	{
		protected boolean isNull;
		protected boolean exists;
		protected int off;

		public FieldValue()
		{
		}

		public FieldValue(int row, int col, Schema s)
		{
			off = s.offsetArray[row][col];
			isNull = (off == 0);
			exists = (off != 0xffffff && off != -1);
		}

		public boolean exists()
		{
			return exists;
		}

		public abstract Object getValue();

		public boolean isNull()
		{
			return isNull;
		}

		public int size()
		{
			return 0;
		}

		public int write(byte[] buff, int off) throws UnsupportedEncodingException
		{
			return 0;
		}
	}

	public static class FloatFV extends FieldValue
	{
		private float value;

		public FloatFV(int row, int col, Schema s)
		{
			super(row, col, s);

			if (!isNull && exists)
			{
				value = s.p.getFloat(off);
			}
		}

		@Override
		public Float getValue()
		{
			return value;
		}

		@Override
		public int size()
		{
			if (!isNull && exists)
			{
				return 4;
			}

			return 0;
		}

		@Override
		public String toString()
		{
			return "" + value;
		}

		@Override
		public int write(byte[] buff, int off)
		{
			if (!isNull && exists)
			{
				final byte[] val = ByteBuffer.allocate(4).putFloat(value).array();
				// int i = 0;
				// while (i < 4)
				// {
				// buff[off + i] = val[i];
				// i++;
				// }
				System.arraycopy(val, 0, buff, off, 4);

				return 4;
			}

			return 0;
		}
	}

	public static class IntegerFV extends FieldValue
	{
		private int value;

		public IntegerFV(int row, int col, Schema s)
		{
			super(row, col, s);

			if (!isNull && exists)
			{
				value = s.p.getInt(off);
			}
		}

		public IntegerFV(Integer val)
		{
			this.exists = true;
			if (val == null)
			{
				this.isNull = true;
			}
			else
			{
				this.value = val;
			}
		}

		@Override
		public Integer getValue()
		{
			return value;
		}

		@Override
		public int size()
		{
			if (!isNull && exists)
			{
				return 4;
			}

			return 0;
		}

		@Override
		public String toString()
		{
			return "" + value;
		}

		@Override
		public int write(byte[] buff, int off)
		{
			// if (value < 0)
			// {
			// HRDBMSWorker.logger.debug("Writing negative int in schema");
			// }
			if (!isNull && exists)
			{
				final byte[] val = ByteBuffer.allocate(4).putInt(value).array();
				// int i = 0;
				// while (i < 4)
				// {
				// buff[off + i] = val[i];
				// i++;
				// }
				System.arraycopy(val, 0, buff, off, 4);

				return 4;
			}

			return 0;
		}
	}

	public class Row
	{
		private final int index;
		private FieldValue[] cache;

		public Row(int index)
		{
			this.index = index;
		}

		public FieldValue[] getAllCols() throws Exception
		{
			int i = 0;
			final FieldValue[] retval = new FieldValue[colIDListSize];
			while (i < colIDListSize)
			{
				retval[i] = getField(index, i);
				i++;
			}

			cache = retval;
			return retval;
		}

		public FieldValue[] getAllCols(ArrayList<Integer> fetchPos) throws Exception
		{
			try
			{
				int i = 0;
				final FieldValue[] retval = new FieldValue[colIDListSize];
				// while (i < colIDListSize)
				// {
				// retval[i] = getField(index, i);
				// i++;
				// }

				int z = 0;
				final int limit = fetchPos.size();
				while (z < limit)
				{
					int pos = fetchPos.get(z++);
					i = colIDToIndex.get(pos);
					if (pos != i)
					{
						HRDBMSWorker.logger.debug("ColID " + pos + " is in index position " + i);
						HRDBMSWorker.logger.debug("ColIDListSize is " + colIDListSize);
						HRDBMSWorker.logger.debug("Block = " + p.block());
					}
					retval[i] = getField(index, i);
				}

				cache = retval;
				return retval;
			}
			catch (Exception e)
			{
				HRDBMSWorker.logger.debug("", e);
				throw e;
			}
		}

		public FieldValue getCol(int id) throws Exception
		{
			// return getField(index, colIDToIndex.get(id));
			int rowIndex = index;
			// int colIndex = colIDToIndex.get(id);
			int colIndex = id;

			if (cache != null)
			{
				return cache[colIndex];
			}

			DataType dt = null;
			int type;

			try
			{
				dt = colTypes.get(colIDs[colIndex]);
				type = dt.getType();
			}
			catch (Exception e)
			{
				HRDBMSWorker.logger.warn("Unable to retrieve field", e);
				HRDBMSWorker.logger.warn("We are on row " + rowIndex);
				HRDBMSWorker.logger.warn("Looking for colIndex " + colIndex);
				HRDBMSWorker.logger.warn("The colIDs array is ");
				int i = 0;
				while (i < colIDs.length)
				{
					HRDBMSWorker.logger.warn("" + colIDs[i]);
					i++;
				}

				HRDBMSWorker.logger.warn("ColTypes is " + colTypes);
				throw e;
			}

			if (type == DataType.BIGINT)
			{
				return new BigintFV(rowIndex, colIndex, Schema.this);
			}

			if (type == DataType.BINARY)
			{
				return new BinaryFV(rowIndex, colIndex, dt.getLength(), Schema.this);
			}

			if (type == DataType.DATE)
				// TODO || type == DataType.TIME || type == DataType.TIMESTAMP)
			{
				return new DateFV(rowIndex, colIndex, Schema.this);
			}

			if (type == DataType.DECIMAL)
			{
				return new DecimalFV(rowIndex, colIndex, dt.getLength(), dt.getScale(), Schema.this);
			}

			if (type == DataType.DOUBLE)
			{
				return new DoubleFV(rowIndex, colIndex, Schema.this);
			}

			if (type == DataType.FLOAT)
			{
				return new FloatFV(rowIndex, colIndex, Schema.this);
			}

			if (type == DataType.INTEGER)
			{
				return new IntegerFV(rowIndex, colIndex, Schema.this);
			}

			if (type == DataType.SMALLINT)
			{
				return new SmallintFV(rowIndex, colIndex, Schema.this);
			}

			if (type == DataType.VARBINARY)
			{
				return new VarbinaryFV(rowIndex, colIndex, Schema.this);
			}

			if (type == DataType.VARCHAR)
			{
				return new VarcharFV(rowIndex, colIndex, Schema.this);
			}

			if (type == DataType.CVARCHAR)
			{
				CVarcharFV retval = new CVarcharFV(rowIndex, colIndex, Schema.this);

				//if (p.block().number() == 2)
				//{
				//	HRDBMSWorker.logger.debug("Returned value is: " + retval.value);
				//}

				return retval;
			}

			HRDBMSWorker.logger.error("Unknown data type in Schema.getField()");
			return null;
		}
		
		public FieldValue getCol(int id, DataType dt) throws Exception
		{
			// return getField(index, colIDToIndex.get(id));
			int rowIndex = index;
			// int colIndex = colIDToIndex.get(id);
			int colIndex = id;

			if (cache != null)
			{
				return cache[colIndex];
			}

			int type;

			try
			{
				type = dt.getType();
			}
			catch (Exception e)
			{
				HRDBMSWorker.logger.warn("Unable to retrieve field", e);
				HRDBMSWorker.logger.warn("We are on row " + rowIndex);
				HRDBMSWorker.logger.warn("Looking for colIndex " + colIndex);
				HRDBMSWorker.logger.warn("The colIDs array is ");
				int i = 0;
				while (i < colIDs.length)
				{
					HRDBMSWorker.logger.warn("" + colIDs[i]);
					i++;
				}

				HRDBMSWorker.logger.warn("ColTypes is " + colTypes);
				throw e;
			}

			if (type == DataType.BIGINT)
			{
				return new BigintFV(rowIndex, colIndex, Schema.this);
			}

			if (type == DataType.BINARY)
			{
				return new BinaryFV(rowIndex, colIndex, dt.getLength(), Schema.this);
			}

			if (type == DataType.DATE)
				// TODO || type == DataType.TIME || type == DataType.TIMESTAMP)
			{
				return new DateFV(rowIndex, colIndex, Schema.this);
			}

			if (type == DataType.DECIMAL)
			{
				return new DecimalFV(rowIndex, colIndex, dt.getLength(), dt.getScale(), Schema.this);
			}

			if (type == DataType.DOUBLE)
			{
				return new DoubleFV(rowIndex, colIndex, Schema.this);
			}

			if (type == DataType.FLOAT)
			{
				return new FloatFV(rowIndex, colIndex, Schema.this);
			}

			if (type == DataType.INTEGER)
			{
				return new IntegerFV(rowIndex, colIndex, Schema.this);
			}

			if (type == DataType.SMALLINT)
			{
				return new SmallintFV(rowIndex, colIndex, Schema.this);
			}

			if (type == DataType.VARBINARY)
			{
				return new VarbinaryFV(rowIndex, colIndex, Schema.this);
			}

			if (type == DataType.VARCHAR)
			{
				return new VarcharFV(rowIndex, colIndex, Schema.this);
			}

			if (type == DataType.CVARCHAR)
			{
				CVarcharFV retval = new CVarcharFV(rowIndex, colIndex, Schema.this);

				//if (p.block().number() == 2)
				//{
				//	HRDBMSWorker.logger.debug("Returned value is: " + retval.value);
				//}

				return retval;
			}

			HRDBMSWorker.logger.error("Unknown data type in Schema.getField()");
			return null;
		}

		public RID getRID() throws Exception
		{
			if (!rowIDsSet)
			{
				setRowIDs();
			}
			return rowIDs[index];
		}
	}

	public class RowIterator implements Iterator<Row>
	{
		private int rowIndex = 0;
		private Row currentRow;
		private final boolean useCache;

		public RowIterator(boolean useCache)
		{
			this.useCache = useCache;
		}

		@Override
		public boolean hasNext()
		{
			if (iterCache != null && useCache)
			{
				try
				{
					Object o = iterCache.take();
					if (o instanceof DataEndMarker)
					{
						iterCache = null;
						return false;
					}
					else
					{
						currentRow = (Row)o;
					}

					return true;
				}
				catch (Exception e)
				{
					HRDBMSWorker.logger.debug("", e);
					currentRow = null;
					return true;
				}
			}
			if (rowIndex < rowIDListSize)
			{
				return true;
			}
			else
			{
				return false;
			}
		}

		@Override
		public Row next()
		{
			if (iterCache != null && useCache)
			{
				return currentRow;
			}

			rowIndex++;
			currentRow = new Row(rowIndex - 1);
			return currentRow;
		}

		@Override
		public void remove()
		{
			throw new UnsupportedOperationException();
		}
	}

	public static class SmallintFV extends FieldValue
	{
		private short value;

		public SmallintFV(int row, int col, Schema s)
		{
			super(row, col, s);

			if (!isNull && exists)
			{
				value = s.p.getShort(off);
			}
		}

		@Override
		public Short getValue()
		{
			return value;
		}

		@Override
		public int size()
		{
			if (!isNull && exists)
			{
				return 2;
			}

			return 0;
		}

		@Override
		public String toString()
		{
			return "" + value;
		}

		@Override
		public int write(byte[] buff, int off)
		{
			if (!isNull && exists)
			{
				final byte[] val = ByteBuffer.allocate(2).putShort(value).array();
				// int i = 0;
				// while (i < 2)
				// {
				// buff[off + i] = val[i];
				// i++;
				// }
				System.arraycopy(val, 0, buff, off, 2);

				return 2;
			}

			return 0;
		}
	}

	public static class VarbinaryFV extends FieldValue
	{
		private byte[] value;

		public VarbinaryFV(int row, int col, Schema s) throws Exception
		{
			super(row, col, s);

			if (!isNull && exists)
			{
				final int len = s.p.getInt(off);
				value = new byte[len];
				s.p.get(off + 4, value);
			}
		}

		@Override
		public byte[] getValue()
		{
			return value;
		}

		@Override
		public int size()
		{
			if (!isNull && exists)
			{
				return 4 + value.length;
			}

			return 0;
		}

		@Override
		public String toString()
		{
			return "VarbinaryFV";
		}

		@Override
		public int write(byte[] buff, int off)
		{
			if (!isNull && exists)
			{
				final byte[] val = ByteBuffer.allocate(4).putInt(value.length).array();
				// int i = 0;
				// while (i < 4)
				// {
				// buff[off + i] = val[i];
				// i++;
				// }
				System.arraycopy(val, 0, buff, off, 4);

				// i = 0;
				// final int length = value.length;
				// while (i < length)
				// {
				// buff[off + 4 + i] = value[i];
				// i++;
				// }
				System.arraycopy(value, 0, buff, off + 4, value.length);

				return 4 + value.length;
			}

			return 0;
		}
	}

	public static class VarcharFV extends FieldValue
	{
		private String value;
		public byte[] bytes = null;
		private int size;

		public VarcharFV(int row, int col, Schema s) throws Exception
		{
			super(row, col, s);

			if (!isNull && exists)
			{
				final int len = (s.p.getInt(off - 1) & 0x00ffffff);
				if (len == 0)
				{
					value = new String();
				}
				else
				{
					char[] ca = new char[len];
					final byte[] temp = new byte[len];
					bytes = temp;
					size = 3 + len;
					s.p.get(off + 3, temp);
					try
					{
						// value = new String(temp, "UTF-8");
						value = (String)unsafe.allocateInstance(String.class);
						int clen = ((sun.nio.cs.ArrayDecoder)s.cd).decode(temp, 0, len, ca);
						if (clen == ca.length)
						{
							unsafe.putObject(value, Schema.offset, ca);
						}
						else
						{
							char[] v = Arrays.copyOf(ca, clen);
							unsafe.putObject(value, Schema.offset, v);
						}
					}
					catch (final Exception e)
					{
						throw e;
					}
				}
			}
		}

		public VarcharFV(String val)
		{
			this.exists = true;
			if (val == null)
			{
				this.isNull = true;
				size = 0;
			}
			else
			{
				this.value = val;
				try
				{
					bytes = val.getBytes(StandardCharsets.UTF_8);
					size = 3 + bytes.length;
				}
				catch (Exception e)
				{
				}
			}
		}

		@Override
		public String getValue()
		{
			return value;
		}

		@Override
		public int size()
		{
			if (!isNull && exists)
			{
				return size;
			}

			return 0;
		}

		@Override
		public String toString()
		{
			return value;
		}

		@Override
		public int write(byte[] buff, int off) throws UnsupportedEncodingException
		{
			if (!isNull && exists)
			{
				final byte[] stringBytes = bytes;
				final byte[] val = ByteBuffer.allocate(4).putInt(stringBytes.length).array();
				// int i = 0;
				// while (i < 4)
				// {
				// buff[off + i] = val[i];
				// i++;
				// }
				System.arraycopy(val, 1, buff, off, 3);

				// i = 0;
				// final int length = stringBytes.length;
				// while (i < length)
				// {
				// buff[off + 4 + i] = stringBytes[i];
				// i++;
				// }
				System.arraycopy(stringBytes, 0, buff, off + 3, stringBytes.length);

				return size;
			}

			return 0;
		}
	}

	private static class RIDComparator implements Comparator
	{
		@Override
		public int compare(Object o1, Object o2)
		{
			RID lhs = (RID)o1;
			RID rhs = (RID)o2;
			return Integer.compare(lhs.getRecNum(), rhs.getRecNum());
		}
	}

	private class UnpinThread extends HRDBMSThread
	{
		private final Page p;
		private final long txnum;

		public UnpinThread(Page p, long txnum)
		{
			this.p = p;
			this.txnum = txnum;
		}

		@Override
		public void run()
		{
			BufferManager.unpin(p, txnum);
		}
	}

	class Col
	{
		private final int index;

		public Col(int index)
		{
			this.index = index;
		}

		public FieldValue[] getAllRows() throws Exception
		{
			int i = 0;
			final FieldValue[] retval = new FieldValue[rowIDListSize];
			while (i < rowIDListSize)
			{
				retval[i] = getField(i, index);
				i++;
			}

			return retval;
		}

		public int getColID()
		{
			return colIDs[index];
		}

		public FieldValue getRow(RID id) throws Exception
		{
			if (rowIDToIndex == null)
			{
				rowIDToIndex = new HashMap<RID, Integer>();
				int i = 0;
				if (!rowIDsSet)
				{
					setRowIDs();
				}
				for (final RID rid : rowIDs)
				{
					if (i >= rowIDListSize)
					{
						break;
					}
					rowIDToIndex.put(rid, i);
					i++;
				}
			}
			return getField(rowIDToIndex.get(id), index);
		}
	}

	class ColIterator implements Iterator<Col>
	{
		private int colIndex = 0;
		private Col currentCol;

		@Override
		public boolean hasNext()
		{
			if (colIndex < colIDListSize)
			{
				return true;
			}
			else
			{
				return false;
			}
		}

		@Override
		public Col next()
		{
			colIndex++;
			currentCol = new Col(colIndex - 1);
			return currentCol;
		}

		@Override
		public void remove()
		{
			throw new UnsupportedOperationException();
		}
	}
}