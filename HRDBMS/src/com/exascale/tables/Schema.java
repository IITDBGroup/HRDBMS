package com.exascale.tables;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import com.exascale.exceptions.LockAbortException;
import com.exascale.exceptions.RecNumOverflowException;
import com.exascale.filesystem.Block;
import com.exascale.filesystem.Page;
import com.exascale.filesystem.RID;
import com.exascale.logging.InsertLogRec;
import com.exascale.logging.LogRec;
import com.exascale.managers.FileManager;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.managers.LockManager;
import com.exascale.misc.MyDate;

public class Schema
{
	public static final byte TYPE_ROW = 0, TYPE_COL = 1;

	public static final int HEADER_SIZE = 4096;

	private final Map<Integer, DataType> colTypes;

	private int nextRecNum;

	private int headEnd;

	private int dataStart;

	private long modTime;

	private byte blockType;

	private int freeSpaceListEntries;

	private int nullArrayOff;

	private int colIDListOff;

	private int rowIDListOff;

	private int offArrayOff;

	private FreeSpace[] freeSpace;

	private int[] colIDs;
	private int colIDListSize;
	private int rowIDListSize;
	private RID[] rowIDs;
	private byte[][] nullArray;
	private int[][] offsetArray;
	private Page p;
	private Map<Integer, Integer> colIDToIndex = new HashMap<Integer, Integer>();
	private Map<RID, Integer> rowIDToIndex = new HashMap<RID, Integer>();
	private Transaction tx;

	public Schema(Map<Integer, DataType> map)
	{
		colTypes = map;
	}

	public ColIterator colIterator()
	{
		return new ColIterator();
	}

	public void deleteRow(RID id) throws LockAbortException, Exception
	{
		final int level = tx.getIsolationLevel();
		if (level == Transaction.ISOLATION_CS || level == Transaction.ISOLATION_UR)
		{
			tx.setIsolationLevel(Transaction.ISOLATION_RR);
			tx.read(p.block(), this);
			tx.setIsolationLevel(level);
		}

		LockManager.xLock(p.block(), tx.number());
		byte[] before;
		byte[] after;
		int off;
		final int index = rowIDToIndex.get(id);
		off = offArrayOff + index * colIDListSize * 4;
		before = new byte[colIDListSize * 4];
		p.get(off, before);
		after = new byte[colIDListSize * 4];
		int i = 0;
		while (i < after.length)
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

		if (id.getRecNum() < nextRecNum)
		{
			nextRecNum = id.getRecNum();
			off = 1;
			before = new byte[4];
			after = new byte[4];
			p.get(1, before);
			after = ByteBuffer.allocate(4).putInt(id.getRecNum()).array();
			rec = tx.delete(before, after, off, p.block()); // write log rec
			p.write(off, after, tx.number(), rec.lsn());
		}

		// modification timestamp
		off = 13;
		before = new byte[8];
		after = new byte[8];
		p.get(13, before);
		modTime = System.currentTimeMillis();
		after = ByteBuffer.allocate(8).putLong(modTime).array();
		rec = tx.delete(before, after, off, p.block());
		p.write(off, after, tx.number(), rec.lsn());
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
		catch(Exception e)
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

		HRDBMSWorker.logger.error("Unknown data type in Schema.getField()");
		return null;
	}

	public RID[] getRIDS()
	{
		return rowIDs;
	}

	public Row getRow(RID id)
	{
		try
		{
			Row row = new Row(rowIDToIndex.get(id));
			return row;
		}
		catch(Exception e)
		{
			HRDBMSWorker.logger.debug("", e);
			HRDBMSWorker.logger.debug("Failed to find a row by RID");
			HRDBMSWorker.logger.debug("We were looking for " + id);
			HRDBMSWorker.logger.debug("The page contains " + rowIDToIndex);
			return null;
		}
	}

	public RID insertRow(FieldValue[] vals) throws LockAbortException, UnsupportedEncodingException, IOException, RecNumOverflowException, Exception
	{
		int length = 0;
		for (final FieldValue val : vals)
		{
			length += val.size();
		}

		int level = tx.getIsolationLevel();
		if (level == Transaction.ISOLATION_CS || level == Transaction.ISOLATION_UR)
		{
			tx.setIsolationLevel(Transaction.ISOLATION_RR);
			tx.read(p.block(), this);
			tx.setIsolationLevel(level);
		}

		LockManager.xLock(p.block(), tx.number());

		final Integer off = hasEnoughSpace(headerGrowthPerRow(), length);
		if (off != null)
		{
			final int node = getNodeNumber(); // from 1st header page
			final int dev = getDeviceNumber(); // from 1st header page
			final RID rid = new RID(node, dev, p.block().number(), nextRecNum);
			final byte[] after = new byte[length];
			int i = 0;
			int index = 0;
			while (index < colIDListSize)
			{
				i += vals[index].write(after, i);
				index++;
			}

			final byte[] before = new byte[length];
			p.get(off, before);
			final LogRec rec = tx.insert(before, after, off, p.block()); // write
																			// log
																			// rec
			p.write(off, after, tx.number(), rec.lsn());
			updateHeader(rid, vals, nextRecNum, off, length); // update
																// nextRecNum,
																// freeSpace,
																// nullArray,
																// rowIDList,
																// and
																// offsetArray
																// internal and
																// external
			return rid;
		}
		else
		{
			int blockNum = findPage(length);

			while (blockNum != -1)
			{
				final Block b = new Block(p.block().fileName(), blockNum);
				tx.requestPage(b);
				final Schema newPage = new Schema(this.colTypes);

				level = tx.getIsolationLevel();
				if (level == Transaction.ISOLATION_CS || level == Transaction.ISOLATION_UR)
				{
					tx.setIsolationLevel(Transaction.ISOLATION_RR);
					tx.read(b, newPage);
					tx.setIsolationLevel(level);
				}
				else
				{
					tx.read(b, newPage);
				}

				LockManager.xLock(b, tx.number());
				if (newPage.hasEnoughSpace(headerGrowthPerRow(), length) != null)
				{
					return newPage.insertRow(vals);
				}

				blockNum = findPage(blockNum, length); // find blocks after
														// this num
			}

			final Block b = addNewBlock(p.block().fileName(), colIDs);
			tx.requestPage(b);
			final Schema newPage = new Schema(this.colTypes);
			level = tx.getIsolationLevel();
			if (level == Transaction.ISOLATION_CS || level == Transaction.ISOLATION_UR)
			{
				tx.setIsolationLevel(Transaction.ISOLATION_RR);
				tx.read(b, newPage);
				tx.setIsolationLevel(level);
			}
			else
			{
				tx.read(b, newPage);
			}

			return newPage.insertRow(vals);
		}
	}

	public RID[] insertRowForColTable(FieldValue[] vals) throws LockAbortException, IOException, RecNumOverflowException, Exception
	{
		// relies on colID always starting from 0 and being sequential
		// type TYPE_COL
		int count = 0;
		final RID[] retval = new RID[vals.length];

		final int level = tx.getIsolationLevel();
		boolean changedLevel = false;
		if (level == Transaction.ISOLATION_CS || level == Transaction.ISOLATION_UR)
		{
			tx.setIsolationLevel(Transaction.ISOLATION_RR);
			tx.read(p.block(), this);
			changedLevel = true;
		}

		LockManager.xLock(p.block(), tx.number());

		while (count < vals.length)
		{
			final int length = vals[count].size();

			if (count == colIDs[0] && hasEnoughSpace(headerGrowthPerRow(), length) != null)
			{
				// this page has this column
				final FieldValue[] value = new FieldValue[1];
				value[0] = vals[count];
				retval[count] = this.insertRow(value);
			}
			else
			{
				int blockNum = findPage(length, count, 0);

				while (blockNum != -1)
				{
					final Block b = new Block(p.block().fileName(), blockNum);
					tx.requestPage(b);
					final Schema newPage = new Schema(this.colTypes);
					tx.read(b, newPage);
					LockManager.xLock(b, tx.number());

					if (newPage.hasEnoughSpace(headerGrowthPerRow(), length) != null)
					{
						final FieldValue[] value = new FieldValue[1];
						value[0] = vals[count];
						retval[count] = newPage.insertRow(value);
					}

					blockNum = findPage(blockNum, length, count, 0); // find
																		// blocks
																		// before
																		// this
																		// num
				}

				final int[] cols = new int[1];
				cols[0] = count;
				final Block b = addNewBlock(p.block().fileName(), cols);
				tx.requestPage(b);
				final Schema newPage = new Schema(this.colTypes);
				tx.read(b, newPage);

				final FieldValue[] value = new FieldValue[1];
				value[0] = vals[count];
				retval[count] = newPage.insertRow(value);
			}

			count++;
		}

		if (changedLevel)
		{
			tx.setIsolationLevel(level);
		}

		return retval;
	}

	public void read(Transaction tx, Page p)
	{
		this.p = p;
		this.tx = tx;

		blockType = p.get(0);
		nextRecNum = p.getInt(1);
		headEnd = p.getInt(5);
		dataStart = p.getInt(9);
		modTime = p.getLong(13);
		nullArrayOff = p.getInt(21);
		colIDListOff = p.getInt(25);
		rowIDListOff = p.getInt(29);
		offArrayOff = p.getInt(33);

		freeSpaceListEntries = p.getInt(37);
		int pos = 41;
		int i = 0;
		try
		{
			freeSpace = new FreeSpace[freeSpaceListEntries];
		}
		catch(Exception e)
		{
			HRDBMSWorker.logger.error("", e);
			HRDBMSWorker.logger.error("Page = " + p.block().fileName() + ":" + p.block().number());
			HRDBMSWorker.logger.error("freeSpaceListEntries = " + freeSpaceListEntries);
		}
		while (i < freeSpaceListEntries)
		{
			final int start = p.getInt(pos);
			final int end = p.getInt(pos + 4);
			freeSpace[i] = new FreeSpace(start, end);
			i++;
			pos += 8;
		}

		colIDListSize = p.getInt(pos);
		pos += 4;
		i = 0;
		colIDs = new int[colIDListSize];
		while (i < colIDListSize)
		{
			colIDs[i] = p.getInt(pos);
			i++;
			pos += 4;
		}

		i = 0;
		for (final int colID : colIDs)
		{
			colIDToIndex.put(colID, i);
			i++;
		}

		rowIDListSize = p.getInt(pos);
		pos += 4;
		i = 0;
		rowIDs = new RID[rowIDListSize];
		while (i < rowIDListSize)
		{
			final int node = p.getInt(pos);
			final int dev = p.getInt(pos + 4);
			final int block = p.getInt(pos + 8);
			final int rec = p.getInt(pos + 12);
			rowIDs[i] = new RID(node, dev, block, rec);
			i++;
			pos += 16;
		}

		i = 0;
		for (final RID rid : rowIDs)
		{
			rowIDToIndex.put(rid, i);
			i++;
		}

		nullArray = new byte[rowIDListSize][colIDListSize];
		int row = 0;
		while (row < rowIDListSize)
		{
			int col = 0;
			while (col < colIDListSize)
			{
				nullArray[row][col] = p.get(pos);
				pos++;
				col++;
			}

			row++;
		}

		offsetArray = new int[rowIDListSize][colIDListSize];
		row = 0;
		while (row < rowIDListSize)
		{
			int col = 0;
			while (col < colIDListSize)
			{
				offsetArray[row][col] = p.getInt(pos);
				pos += 4;
				col++;
			}

			row++;
		}
	}

	public RowIterator rowIterator()
	{
		return new RowIterator();
	}

	public RIDChange updateRow(RID id, int startColID, FieldValue[] vals) throws Exception
	{
		if (blockType == TYPE_ROW)
		{
			final int level = tx.getIsolationLevel();
			if (level == Transaction.ISOLATION_CS || level == Transaction.ISOLATION_UR)
			{
				tx.setIsolationLevel(Transaction.ISOLATION_RR);
				tx.read(p.block(), this);
				tx.setIsolationLevel(level);
			}

			LockManager.xLock(p.block(), tx.number());
			final Row row = this.getRow(id);
			final FieldValue[] values = row.getAllCols();
			this.deleteRow(id);
			// int i = colIDToIndex.get(startColID);
			// int j = 0;
			// while (i < colIDListSize)
			// {
			// values[i] = vals[j];
			// i++;
			// j++;
			// }
			System.arraycopy(vals, 0, values, colIDToIndex.get(startColID), colIDListSize);

			final RID newRID = this.insertRow(values);

			if (newRID.equals(id))
			{
				return null;
			}
			else
			{
				return new RIDChange(id, newRID);
			}
		}
		else
		{
			throw new UnsupportedOperationException();
		}
	}

	public RIDChange updateRowForColTable(RID id, FieldValue val) throws LockAbortException, IOException, RecNumOverflowException, Exception
	{
		RID newRID;
		this.deleteRow(id);

		final int length = val.size();

		if (hasEnoughSpace(headerGrowthPerRow(), length) != null)
		{
			final FieldValue[] value = new FieldValue[1];
			value[0] = val;
			newRID = this.insertRow(value);
		}
		else
		{
			int blockNum = findPage(length, colIDs[0], 0);

			while (blockNum != -1)
			{
				final Block b = new Block(p.block().fileName(), blockNum);
				tx.requestPage(b);
				final Schema newPage = new Schema(this.colTypes);
				final int level = tx.getIsolationLevel();
				if (level == Transaction.ISOLATION_CS || level == Transaction.ISOLATION_UR)
				{
					tx.setIsolationLevel(Transaction.ISOLATION_RR);
					tx.read(b, newPage);
					tx.setIsolationLevel(level);
				}
				else
				{
					tx.read(b, newPage);
				}

				LockManager.xLock(b, tx.number());

				if (newPage.hasEnoughSpace(headerGrowthPerRow(), length) != null)
				{
					final FieldValue[] value = new FieldValue[1];
					value[0] = val;
					newRID = newPage.insertRow(value);
				}

				blockNum = findPage(blockNum, length, colIDs[0], 0); // find
																		// blocks
																		// after
																		// this
																		// num
			}

			final Block b = addNewBlock(p.block().fileName(), colIDs);
			tx.requestPage(b);
			final Schema newPage = new Schema(this.colTypes);

			final int level = tx.getIsolationLevel();
			if (level == Transaction.ISOLATION_CS || level == Transaction.ISOLATION_UR)
			{
				tx.setIsolationLevel(Transaction.ISOLATION_RR);
				tx.read(b, newPage);
				tx.setIsolationLevel(level);
			}
			else
			{
				tx.read(b, newPage);
			}

			final FieldValue[] value = new FieldValue[1];
			value[0] = val;
			newRID = newPage.insertRow(value);
		}

		if (newRID.equals(id))
		{
			return null;
		}
		else
		{
			return new RIDChange(id, newRID);
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
		buff.putInt(0); // nextRecNum
		buff.putInt(52 + (4 * colIDs.length)); // headEnd
		buff.putInt(Page.BLOCK_SIZE); // dataStart
		buff.putLong(System.currentTimeMillis()); // modTime
		buff.putInt(-1); // nullArrayOff
		buff.putInt(49); // colIDListOff
		buff.putInt(53 + (4 * colIDs.length)); // rowIDListOff
		buff.putInt(-1); // offArrayOff
		buff.putInt(1); // freeSpaceListEntries
		buff.putInt(53 + (4 * colIDs.length)); // free space start = headEnd + 1
		buff.putInt(Page.BLOCK_SIZE - 1); // free space end
		buff.putInt(colIDs.length); // colIDListSize - start of colIDs

		for (final int id : colIDs)
		{
			buff.putInt(id); // colIDs
		}

		buff.putInt(0); // rowIDListSize - start of rowIDs
		// null Array start
		// offset array start
		
		Block bl = new Block(fn, newBlockNum);
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
		
		InsertLogRec rec = tx.insert(oldBuff.array(), buff.array(), 0, bl);
		p2.write(0, buff.array(), tx.number(), rec.lsn());

		addNewBlockToHeader(newBlockNum, colIDs[0]);
		return bl;
	}

	// we just added a new block to the table, now register it in the header
	private void addNewBlockToHeader(int newBlockNum, int colID) throws LockAbortException, Exception
	{
		int pageNum;
		int entryNum;
		if (blockType == TYPE_ROW)
		{
			pageNum = (newBlockNum + 2) / (Page.BLOCK_SIZE / 4);
			entryNum = (newBlockNum + 2) % (Page.BLOCK_SIZE / 4);
		}
		else
		{
			pageNum = (newBlockNum + 1) / (Page.BLOCK_SIZE / 8);
			entryNum = (newBlockNum + 1) / (Page.BLOCK_SIZE / 8);
		}

		final Block blkd = new Block(p.block().fileName(), newBlockNum);
		final Block blkh = new Block(p.block().fileName(), pageNum);
		tx.requestPage(blkh);
		final Schema newSchema = new Schema(this.colTypes);
		tx.read(blkd, newSchema);
		final int size = newSchema.getSizeLargestFS();
		final HeaderPage hp = tx.forceReadHeaderPage(blkh, blockType);

		if (blockType == TYPE_ROW)
		{
			hp.updateSize(entryNum, size, tx);
		}
		else
		{
			hp.updateSize(entryNum, size, tx, 0);
			hp.updateColNum(entryNum, colID, tx);
		}
	}

	private void againUpdateFreeList(int headSize)
	{
		if (freeSpace[0].getStart() < headSize)
		{
			if (freeSpace[0].getEnd() >= headSize)
			{
				freeSpace[0].setStart(headSize);
				return;
			}

			final FreeSpace[] newFS = new FreeSpace[freeSpace.length - 1];
			// int i = 0;
			// while (i < newFS.length)
			// {
			// newFS[i] = freeSpace[i+1];
			// i++;
			// }
			System.arraycopy(freeSpace, 1, newFS, 0, newFS.length);

			freeSpace = newFS;
			freeSpaceListEntries--;
		}
	}

	private void deleteFSAt(int loc)
	{
		final FreeSpace[] newFS = new FreeSpace[freeSpace.length - 1];
		int i = 0;
		while (i < loc)
		{
			newFS[i] = freeSpace[i];
			i++;
		}

		while (i < newFS.length)
		{
			newFS[i] = freeSpace[i + 1];
			i++;
		}

		freeSpace = newFS;
		freeSpaceListEntries--;
	}

	private int findNextFreeRecNum(int justUsed) throws RecNumOverflowException
	{
		int candidate = justUsed + 1;
		final RID[] copy = rowIDs.clone();
		Arrays.sort(copy);

		int i = 0;
		for (final RID rid : copy)
		{
			if (rid.getRecNum() > justUsed)
			{
				break;
			}

			i++;
		}

		final RID[] copy2 = new RID[copy.length - i];
		// int j = 0;
		// while (j < copy2.length)
		// {
		// copy2[j] = copy[i];
		// i++;
		// j++;
		// }
		System.arraycopy(copy, i, copy2, 0, copy2.length);

		for (final RID rid : copy2)
		{
			final int num = rid.getRecNum();
			if (num != candidate)
			{
				return candidate;
			}
			else
			{
				if (candidate == Integer.MAX_VALUE)
				{
					throw new RecNumOverflowException();
				}

				candidate = num + 1;
			}
		}

		return candidate;
	}

	private int findPage(int data) throws LockAbortException, Exception
	{
		Block blk = null;
		int i = 0;
		final String fn = p.block().fileName();
		int block = -1;
		while (i < HEADER_SIZE && block == -1)
		{
			blk = new Block(fn, i);
			tx.requestPage(blk);
			final HeaderPage hp = tx.readHeaderPage(blk, blockType);

			if (hp == null)
			{
				return -1;
			}

			block = hp.findPage(data);
			if (block == 0)
			{
				return -1;
			}
			i++;
		}

		return block;
	}

	private int findPage(int after, int data) throws LockAbortException, Exception
	{
		after = after + 1;
		int i = (after + 2) / (Page.BLOCK_SIZE / 4);
		int block = -1;
		while (i < HEADER_SIZE && block == -1)
		{
			final HeaderPage hp = tx.readHeaderPage(new Block(p.block().fileName(), i), blockType);
			if (hp == null)
			{
				return -1;
			}
			block = hp.findPage(data, after);
			if (block == 0)
			{
				return -1;
			}
			i++;
		}

		return block;
	}

	private int findPage(int data, int colID, int colMarker) throws LockAbortException, Exception
	{
		Block blk = null;
		int i = 0;
		final String fn = p.block().fileName();
		int block = -1;
		while (i < HEADER_SIZE && block == -1)
		{
			blk = new Block(fn, i);
			tx.requestPage(blk);
			final HeaderPage hp = tx.readHeaderPage(blk, blockType);

			if (hp == null)
			{
				return -1;
			}
			block = hp.findPage(data, colID, colMarker);
			if (block == 0)
			{
				return -1;
			}
			i++;
		}

		return block;
	}

	private int findPage(int after, int data, int colID, int colMarker) throws LockAbortException, Exception
	{
		after = after + 1;
		int i = (after + 1) / (Page.BLOCK_SIZE / 8);
		int block = -1;
		while (i < HEADER_SIZE && block == -1)
		{
			final HeaderPage hp = tx.readHeaderPage(new Block(p.block().fileName(), i), blockType);
			if (hp == null)
			{
				return -1;
			}
			block = hp.findPage(data, colID, colMarker, after);
			if (block == 0)
			{
				return -1;
			}
			i++;
		}

		return block;
	}

	private int getDeviceNumber() throws LockAbortException, Exception
	{
		final Block b = new Block(p.block().fileName(), 0);
		tx.requestPage(b);

		final HeaderPage hp = tx.readHeaderPage(b, blockType);
		return hp.getDeviceNumber();
	}

	private int getNodeNumber() throws LockAbortException, Exception
	{
		final Block b = new Block(p.block().fileName(), 0);
		tx.requestPage(b);

		final HeaderPage hp = tx.readHeaderPage(b, blockType);
		return hp.getNodeNumber();
	}

	private int getSizeLargestFS()
	{
		int largest = 0;
		for (final FreeSpace fs : freeSpace)
		{
			final int size = fs.getEnd() - fs.getStart() + 1;

			if (size > largest)
			{
				largest = size;
			}
		}

		return largest;
	}

	private Integer hasEnoughSpace(int head, int data)
	{
		if (freeSpace.length == 0)
		{
			return null;
		}

		final FreeSpace fs = freeSpace[0];

		if (fs.getStart() == headEnd + 1)
		{
			if (fs.getEnd() - fs.getStart() + 1 >= head)
			{
				// we have enough room to extend header
				final int end = fs.getStart() + head - 1;
				int dfsx = freeSpace.length - 1;

				while (dfsx >= 0)
				{
					final FreeSpace dfs = freeSpace[dfsx];

					if (dfs.getEnd() - dfs.getStart() + 1 >= data)
					{
						// we have enough room for data
						final int start = dfs.getEnd() - data + 1;
						if (start > end)
						{
							// as long as the header space and data space don't
							// overlap
							return start;
						}
					}

					dfsx--;
				}
			}
		}

		return null;
	}

	private int headerGrowthPerRow()
	{
		return 24 + colIDListSize * 5;
	}

	private void insertFSAfter(int after, FreeSpace fs)
	{
		final FreeSpace[] newFS = new FreeSpace[freeSpace.length + 1];
		int i = 0;
		while (i <= after)
		{
			newFS[i] = freeSpace[i];
			i++;
		}

		newFS[i] = fs;
		i++;

		while (i < newFS.length)
		{
			newFS[i] = freeSpace[i - 1];
			i++;
		}

		freeSpace = newFS;
		freeSpaceListEntries++;
	}

	private void updateFreeSpace(int off, int length) throws LockAbortException, Exception
	{
		final int oldMax = getSizeLargestFS();
		int i = 0;
		for (final FreeSpace fs : freeSpace)
		{
			if (off >= fs.getStart() && off <= fs.getEnd())
			{
				// in this freespace
				if (off == fs.getStart())
				{
					if (off + length - 1 == fs.getEnd())
					{
						// this freespace gets deleted
						deleteFSAt(i);
						break;
					}
					else
					{
						fs.setStart(fs.getStart() + length);
						break;
					}
				}

				if (off + length - 1 == fs.getEnd())
				{
					fs.setEnd(off - 1);
					break;
				}

				final int end = fs.getEnd();
				fs.setEnd(off - 1);
				insertFSAfter(i, new FreeSpace(off + length, end));
				break;
			}
			i++;
		}

		final int newMax = getSizeLargestFS();

		if (oldMax != newMax)
		{
			updateFSInHeader(newMax);
		}
	}

	private void updateFSInHeader(int size) throws LockAbortException, Exception
	{
		int pageNum;
		int entryNum;
		if (blockType == TYPE_ROW)
		{
			pageNum = (p.block().number() + 2) / (Page.BLOCK_SIZE / 4);
			entryNum = (p.block().number() + 2) % (Page.BLOCK_SIZE / 4);
		}
		else
		{
			pageNum = (p.block().number() + 1) / (Page.BLOCK_SIZE / 8);
			entryNum = (p.block().number() + 1) / (Page.BLOCK_SIZE / 8);
		}

		final Block blk = new Block(p.block().fileName(), pageNum);
		tx.requestPage(blk);
		final HeaderPage hp = tx.forceReadHeaderPage(blk, blockType);

		if (blockType == TYPE_ROW)
		{
			hp.updateSize(entryNum, size, tx);
		}
		else
		{
			hp.updateSize(entryNum, size, tx, 0);
		}
	}

	private void updateHeader(RID rid, FieldValue[] vals, int nextRecNum, int off, int length) throws RecNumOverflowException, LockAbortException, Exception
	{
		this.nextRecNum = findNextFreeRecNum(nextRecNum);
		updateFreeSpace(off, length);
		if (off < dataStart)
		{
			this.dataStart = off;
		}
		this.modTime = System.currentTimeMillis();
		updateNullArray(vals);
		updateRowIDs(rid);
		updateOffsets(vals, off);
		againUpdateFreeList(48 + (rowIDListSize * 16) + (freeSpaceListEntries * 8) + (colIDListSize * 4) + (5 * rowIDListSize * colIDListSize));

		final byte[] before = new byte[48 + (rowIDListSize * 16) + (freeSpaceListEntries * 8) + (colIDListSize * 4) + (5 * rowIDListSize * colIDListSize)];
		p.get(1, before);
		final byte[] after = new byte[before.length];

		// copy(after,0,
		// ByteBuffer.allocate(4).putInt(this.nextRecNum).array()); //nextRecNum
		System.arraycopy(ByteBuffer.allocate(4).putInt(this.nextRecNum).array(), 0, after, 0, 4);
		// copy(after,4, ByteBuffer.allocate(4).putInt(before.length -
		// 1).array()); //headEnd
		System.arraycopy(ByteBuffer.allocate(4).putInt(before.length-1).array(), 0, after, 4, 4);
		headEnd = before.length - 1;
		// copy(after,8, ByteBuffer.allocate(4).putInt(this.dataStart).array());
		// //dataStart
		System.arraycopy(ByteBuffer.allocate(4).putInt(this.dataStart).array(), 0, after, 8, 4);
		// copy(after,12, ByteBuffer.allocate(8).putLong(this.modTime).array());
		// //modTime
		System.arraycopy(ByteBuffer.allocate(8).putLong(this.modTime).array(), 0, after, 12, 8);
		// copy(after,32,
		// ByteBuffer.allocate(4).putInt(freeSpaceListEntries).array());
		System.arraycopy(ByteBuffer.allocate(4).putInt(freeSpaceListEntries).array(), 0, after, 36, 4);

		int i = 40;
		for (final FreeSpace fs : freeSpace)
		{
			// copy(after, i,
			// ByteBuffer.allocate(4).putInt(fs.getStart()).array());
			System.arraycopy(ByteBuffer.allocate(4).putInt(fs.getStart()).array(), 0, after, i, 4);
			// copy(after, i+4,
			// ByteBuffer.allocate(4).putInt(fs.getEnd()).array());
			System.arraycopy(ByteBuffer.allocate(4).putInt(fs.getEnd()).array(), 0, after, i + 4, 4);
			i += 8;
		}

		// copy(after, 20, ByteBuffer.allocate(4).putInt(i).array());
		System.arraycopy(ByteBuffer.allocate(4).putInt(i+1).array(), 0, after, 24, 4); 
		// copy(after, i, ByteBuffer.allocate(4).putInt(colIDListSize).array());
		System.arraycopy(ByteBuffer.allocate(4).putInt(colIDListSize).array(), 0, after, i, 4);
		i += 4;

		for (final int id : colIDs)
		{
			// copy(after, i, ByteBuffer.allocate(4).putInt(id).array());
			System.arraycopy(ByteBuffer.allocate(4).putInt(id).array(), 0, after, i, 4);
			i += 4;
		}

		// copy(after, 24, ByteBuffer.allocate(4).putInt(i).array());
		System.arraycopy(ByteBuffer.allocate(4).putInt(i+1).array(), 0, after, 28, 4);
		// copy(after, i, ByteBuffer.allocate(4).putInt(rowIDListSize).array());
		System.arraycopy(ByteBuffer.allocate(4).putInt(rowIDListSize).array(), 0, after, i, 4);
		i += 4;

		for (final RID id : rowIDs)
		{
			// copy(after, i,
			// ByteBuffer.allocate(4).putInt(id.getNode()).array());
			System.arraycopy(ByteBuffer.allocate(4).putInt(id.getNode()).array(), 0, after, i, 4);
			// copy(after, i+4,
			// ByteBuffer.allocate(4).putInt(id.getDevice()).array());
			System.arraycopy(ByteBuffer.allocate(4).putInt(id.getDevice()).array(), 0, after, i + 4, 4);
			// copy(after, i+8,
			// ByteBuffer.allocate(4).putInt(id.getBlockNum()).array());
			System.arraycopy(ByteBuffer.allocate(4).putInt(id.getBlockNum()).array(), 0, after, i + 8, 4);
			// copy(after, i+12,
			// ByteBuffer.allocate(4).putInt(id.getRecNum()).array());
			System.arraycopy(ByteBuffer.allocate(4).putInt(id.getRecNum()).array(), 0, after, i + 12, 4);
			i += 16;
		}
		
		System.arraycopy(ByteBuffer.allocate(4).putInt(i+1).array(), 0, after, 20, 4); 

		for (final byte[] row : nullArray)
		{
			for (final byte col : row)
			{
				after[i] = col;
				i++;
			}
		}

		// copy(after, 28, ByteBuffer.allocate(4).putInt(i).array());
		System.arraycopy(ByteBuffer.allocate(4).putInt(i+1).array(), 0, after, 32, 4);

		for (final int[] row : offsetArray)
		{
			for (final int col : row)
			{
				// copy(after, i, ByteBuffer.allocate(4).putInt(col).array());
				System.arraycopy(ByteBuffer.allocate(4).putInt(col).array(), 0, after, i, 4);
				i += 4;
			}
		}

		final InsertLogRec rec = tx.insert(before, after, 1, p.block());
		p.write(1, after, tx.number(), rec.lsn());
	}

	private void updateNullArray(FieldValue[] vals)
	{
		byte[][] newNA;
		newNA = new byte[nullArray.length + 1][colIDs.length];
		// int i = 0;
		// while (i < nullArray.length)
		// {
		// newNA[i] = nullArray[i];
		// i++;
		// }
		System.arraycopy(nullArray, 0, newNA, 0, nullArray.length);

		int j = 0;
		final int i = nullArray.length;
		for (final FieldValue fv : vals)
		{
			if (fv.isNull())
			{
				newNA[i][j] = 1;
			}
			else
			{
				newNA[i][j] = 0;
			}
			j++;
		}
		nullArray = newNA;
	}

	private void updateOffsets(FieldValue[] vals, int off)
	{
		final int[][] newOA = new int[rowIDs.length][colIDs.length];
		// int i = 0;
		// while (i < offsetArray.length)
		// {
		// newOA[i] = offsetArray[i];
		// i++;
		// }
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
	}

	private void updateRowIDs(RID id)
	{
		final RID[] newRI = new RID[rowIDs.length + 1];
		// int i = 0;
		// while (i < rowIDs.length)
		// {
		// newRI[i] = rowIDs[i];
		// i++;
		// }
		System.arraycopy(rowIDs, 0, newRI, 0, rowIDs.length);

		final int i = rowIDs.length;
		newRI[i] = id;
		rowIDs = newRI;
		rowIDListSize++;
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
		public int write(byte[] buff, int off)
		{
			if (!isNull && exists)
			{
				final byte[] val = ByteBuffer.allocate(8).putLong(value).array();
				int i = 0;
				while (i < 8)
				{
					buff[off + i] = val[i];
					i++;
				}

				return size();
			}

			return 0;
		}
		
		public String toString()
		{
			return "" + value;
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
		
		public String toString()
		{
			return "" + value; //FIXME
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
		public int write(byte[] buff, int off)
		{
			if (!isNull && exists)
			{
				int i = 0;
				while (i < value.length)
				{
					buff[off + i] = value[i];
					i++;
				}

				return size();
			}

			return 0;
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
				value = new MyDate(s.p.getLong(off));
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
		
		public String toString()
		{
			return "" + value;
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
				return 8;
			}

			return 0;
		}

		@Override
		public int write(byte[] buff, int off)
		{
			if (!isNull && exists)
			{
				final byte[] val = ByteBuffer.allocate(8).putLong(value.getTime()).array();
				int i = 0;
				while (i < 8)
				{
					buff[off + i] = val[i];
					i++;
				}

				return size();
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
		
		public String toString()
		{
			return "" + value;
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
		public int write(byte[] buff, int off)
		{
			if (!isNull && exists)
			{
				int i = 0;
				while (i < bytes.length)
				{
					buff[off + i] = bytes[i];
					i++;
				}

				return size();
			}

			return 0;
		}
	}

	public static class DoubleFV extends FieldValue
	{
		private double value;

		public DoubleFV(int row, int col, Schema s)
		{
			super(row, col, s);

			if (!isNull && exists)
			{
				value = s.p.getDouble(off);
			}
		}
		
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
		
		public String toString()
		{
			return "" + value;
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
		public int write(byte[] buff, int off)
		{
			if (!isNull && exists)
			{
				final byte[] val = ByteBuffer.allocate(8).putDouble(value).array();
				int i = 0;
				while (i < 8)
				{
					buff[off + i] = val[i];
					i++;
				}

				return size();
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
			isNull = (s.nullArray[row][col] != 0);
			off = s.offsetArray[row][col];
			exists = (off != -1);
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
		
		public String toString()
		{
			return "" + value;
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
		public int write(byte[] buff, int off)
		{
			if (!isNull && exists)
			{
				final byte[] val = ByteBuffer.allocate(4).putFloat(value).array();
				int i = 0;
				while (i < 4)
				{
					buff[off + i] = val[i];
					i++;
				}

				return size();
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
		
		public String toString()
		{
			return "" + value;
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
		public int write(byte[] buff, int off)
		{
			if (!isNull && exists)
			{
				final byte[] val = ByteBuffer.allocate(4).putInt(value).array();
				int i = 0;
				while (i < 4)
				{
					buff[off + i] = val[i];
					i++;
				}

				return size();
			}

			return 0;
		}
	}

	public class Row
	{
		private final int index;

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

			return retval;
		}

		public FieldValue getCol(int id) throws Exception
		{
			return getField(index, colIDToIndex.get(id));
		}

		public RID getRID()
		{
			return rowIDs[index];
		}
	}

	public class RowIterator implements Iterator<Row>
	{
		private int rowIndex = 0;
		private Row currentRow;

		@Override
		public boolean hasNext()
		{
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
		
		public String toString()
		{
			return "" + value;
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
		public int write(byte[] buff, int off)
		{
			if (!isNull && exists)
			{
				final byte[] val = ByteBuffer.allocate(2).putShort(value).array();
				int i = 0;
				while (i < 2)
				{
					buff[off + i] = val[i];
					i++;
				}

				return size();
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
		
		public String toString()
		{
			return "" + value; //FIXME
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
		public int write(byte[] buff, int off)
		{
			if (!isNull && exists)
			{
				final byte[] val = ByteBuffer.allocate(4).putInt(value.length).array();
				int i = 0;
				while (i < 4)
				{
					buff[off + i] = val[i];
					i++;
				}

				i = 0;
				while (i < value.length)
				{
					buff[off + 4 + i] = value[i];
					i++;
				}

				return size();
			}

			return 0;
		}
	}

	public static class VarcharFV extends FieldValue
	{
		private String value;
		private int size;

		public VarcharFV(int row, int col, Schema s) throws Exception
		{
			super(row, col, s);

			if (!isNull && exists)
			{
				final int len = s.p.getInt(off);
				final byte[] temp = new byte[len];
				size = 4 + len;
				s.p.get(off + 4, temp);
				try
				{
					value = new String(temp, "UTF-8");
				}
				catch (final Exception e)
				{
					throw e;
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
					size = 4 + val.getBytes("UTF-8").length;
				}
				catch(Exception e)
				{}
			}
		}
		
		public String toString()
		{
			return "" + value;
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
		public int write(byte[] buff, int off) throws UnsupportedEncodingException
		{
			if (!isNull && exists)
			{
				final byte[] stringBytes = value.getBytes("UTF-8");
				final byte[] val = ByteBuffer.allocate(4).putInt(stringBytes.length).array();
				int i = 0;
				while (i < 4)
				{
					buff[off + i] = val[i];
					i++;
				}

				i = 0;
				while (i < stringBytes.length)
				{
					buff[off + 4 + i] = stringBytes[i];
					i++;
				}

				return size();
			}

			return 0;
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