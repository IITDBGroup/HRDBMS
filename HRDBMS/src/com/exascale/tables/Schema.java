package com.exascale;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;

public class Schema 
{
	public static final byte TYPE_ROW = 0, TYPE_COL = 1;
	public static final int HEADER_SIZE = 4096;
	private Map<Integer, DataType> colTypes;
	private int nextRecNum;
	private int headEnd;
	private int dataStart;
	private long modTime;
	private byte blockType;
	private int freeSpaceListEntries;
	private int nullArrayOff;
	private int colIDListOff;
	private int rowIDListOff;
	private int ColIDListOff;
	private int offArrayOff;
	private FreeSpace[] freeSpace;
	private int[] colIDs;
	private int colIDListSize;
	private int rowIDListSize;
	private RID[] rowIDs;
	private byte[][] nullArray;
	private int[][] offsetArray;
	private Page p;
	private Map<Integer, Integer> colIDToIndex;
	private Map<RID, Integer> rowIDToIndex;
	private Transaction tx;
	
	private String getTableName()
	{
		String retval = p.block().fileName();
		retval = retval.substring(retval.lastIndexOf("/") + 1);
		retval = retval.substring(0, retval.indexOf(".", retval.indexOf(".")));
		return retval;
	}
	
	private int headerGrowthPerRow()
	{
		return 24 + colIDListSize * 5;
	}
	
	private Integer hasEnoughSpace(int head, int data)
	{
		if (freeSpace.length == 0)
		{
			return null;
		}
		
		FreeSpace fs = freeSpace[0];
		
		if (fs.getStart() == headEnd + 1)
		{
			if (fs.getEnd() - fs.getStart() + 1 >= head)
			{
				int end = fs.getStart() + head - 1;
				int dfsx = freeSpace.length - 1;
				
				while (dfsx >= 0)
				{
					FreeSpace dfs = freeSpace[dfsx];
					
					if (dfs.getEnd() - dfs.getStart() + 1 >= data)
					{
						int start = dfs.getEnd() - data + 1;
						if (start > end)
						{
							return start;
						}
					}
					
					dfsx--;
				}
			}
		}
		
		return null;
	}
	
	public Schema(Map<Integer, DataType> map)
	{
		colTypes = map;
	}
	
	public RID[] getRIDS()
	{
		return rowIDs;
	}
	
	public int[] getColIDs()
	{
		return colIDs;
	}
	
	private Block addNewBlock(String fn, int[] colIDs) throws IOException, LockAbortException
	{
		ByteBuffer buff = ByteBuffer.allocate(Page.BLOCK_SIZE);
		buff.position(0);
		buff.put(blockType);
		buff.putInt(0); //nextRecNum
		buff.putInt(52 + (4 * colIDs.length)); //headEnd
		buff.putInt(Page.BLOCK_SIZE); //dataStart
		buff.putLong(System.currentTimeMillis()); //modTime
		buff.putInt(-1); //nullArrayOff
		buff.putInt(49); //colIDListOff
		buff.putInt(53 + (4 * colIDs.length)); //rowIDListOff
		buff.putInt(-1); //offArrayOff
		buff.putInt(1); //freeSpaceListEntries
		buff.putInt(53 + (4 * colIDs.length)); //free space start = headEnd + 1
		buff.putInt(Page.BLOCK_SIZE - 1); //free space end
		buff.putInt(colIDs.length); //colIDListSize - start of colIDs
		
		for (int id : colIDs)
		{
			buff.putInt(id); //colIDs
		}
		
		buff.putInt(0); //rowIDListSize - start of rowIDs
		//null Array start
		//offset array start
		
		int newBlockNum = FileManager.addNewBlock(fn, buff);
		addNewBlockToHeader(newBlockNum);
		alertXAManagerToAddNewBlock(newBlockNum, getNodeNumber(), getDeviceNumber(), getTableName());
		return new Block(fn, newBlockNum);
	}
	
	private void addNewBlockToHeader(int newBlockNum) throws LockAbortException
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
		
		
		Block blkd = new Block(p.block().fileName(), newBlockNum);
		Block blkh = new Block(p.block().fileName(), pageNum);
		tx.requestPage(blkh);
		Schema newSchema = new Schema(this.colTypes);
		tx.read(blkd, newSchema);
		int size = newSchema.getSizeLargestFS();
		HeaderPage hp = tx.readHeaderPage(blkh, blockType);
		
		if (blockType == TYPE_ROW)
		{
			hp.updateSize(entryNum, size, tx.number());
		}
		else
		{
			hp.updateSize(entryNum, size, tx.number(), 0);
			hp.updateColNum(entryNum, colIDs[0], tx.number());
		}
	}
	
	private void updateFreeSpace(int off, int length) throws LockAbortException
	{
		int oldMax = getSizeLargestFS();
		int i = 0;
		for (FreeSpace fs : freeSpace)
		{
			if (off >= fs.getStart() && off <= fs.getEnd())
			{
				//in this freespace
				if (off == fs.getStart())
				{
					if (off + length - 1 == fs.getEnd())
					{
						//this freespace gets deleted
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
					fs.setEnd(fs.getStart() - 1);
					break;
				}
				
				int end = fs.getEnd();
				fs.setEnd(off - 1);
				insertFSAfter(i, new FreeSpace(off+length, end));
				break;
			}
			i++;
		}
		
		int newMax = getSizeLargestFS();
		
		if (oldMax != newMax)
		{
			updateFSInHeader(newMax);
		}
	}
	
	private int getSizeLargestFS()
	{
		int largest = 0;
		for (FreeSpace fs : freeSpace)
		{
			int size = fs.getEnd() - fs.getStart() + 1;
			
			if (size > largest)
			{
				largest = size;
			}
		}
		
		return largest;
	}
	
	private void deleteFSAt(int loc)
	{
		FreeSpace[] newFS = new FreeSpace[freeSpace.length - 1];
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
	
	private void insertFSAfter(int after, FreeSpace fs)
	{
		FreeSpace[] newFS = new FreeSpace[freeSpace.length + 1];
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
	
	private void copy(byte[] target, int off, byte[] source)
	{
		int i = 0;
		int j = off;
		while (i < source.length)
		{
			target[j] = source[i];
			i++;
			j++;
		}
	}
	
	private void updateHeader(RID rid, FieldValue[] vals, int nextRecNum, int off, int length) throws RecNumOverflowException, LockAbortException
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
		
		byte[] before = new byte[48 + (rowIDListSize * 16) + (freeSpaceListEntries * 8) + (colIDListSize * 4) + (5 * rowIDListSize * colIDListSize)];
		p.get(1, before);
		byte[] after = new byte[before.length];

		copy(after,0, ByteBuffer.allocate(4).putInt(this.nextRecNum).array()); //nextRecNum
		copy(after,4, ByteBuffer.allocate(4).putInt(before.length - 1).array()); //headEnd
		copy(after,8, ByteBuffer.allocate(4).putInt(this.dataStart).array()); //dataStart
		copy(after,12, ByteBuffer.allocate(8).putLong(this.modTime).array()); //modTime
		
		copy(after,32, ByteBuffer.allocate(4).putInt(freeSpaceListEntries).array()); 
		
		int i = 36;
		for (FreeSpace fs : freeSpace)
		{
			copy(after, i, ByteBuffer.allocate(4).putInt(fs.getStart()).array());
			copy(after, i+4, ByteBuffer.allocate(4).putInt(fs.getEnd()).array());
			i += 8;
		}
		
		copy(after, 20, ByteBuffer.allocate(4).putInt(i).array());
		copy(after, i, ByteBuffer.allocate(4).putInt(colIDListSize).array());
		i += 4;
		
		for (int id : colIDs)
		{
			copy(after, i, ByteBuffer.allocate(4).putInt(id).array());
			i += 4;
		}
		
		copy(after, 24, ByteBuffer.allocate(4).putInt(i).array());
		copy(after, i, ByteBuffer.allocate(4).putInt(rowIDListSize).array());
		i += 4;
		
		for (RID id : rowIDs)
		{
			copy(after, i, ByteBuffer.allocate(4).putInt(id.getNode()).array());
			copy(after, i+4, ByteBuffer.allocate(4).putInt(id.getDevice()).array());
			copy(after, i+8, ByteBuffer.allocate(4).putInt(id.getBlockNum()).array());
			copy(after, i+12, ByteBuffer.allocate(4).putInt(id.getRecNum()).array());
			i += 16;
		}
		
		copy(after, 16, ByteBuffer.allocate(4).putInt(i).array());
		
		for (byte[] row : nullArray)
		{
			for (byte col : row)
			{
				after[i] = col;
				i++;
			}
		}
		
		copy(after, 28, ByteBuffer.allocate(4).putInt(i).array());
		
		for (int[] row : offsetArray)
		{
			for (int col : row)
			{
				copy(after, i, ByteBuffer.allocate(4).putInt(col).array());
				i += 4;
			}
		}
		
		InsertLogRec rec = tx.insert(before, after, 1, p.block());
		p.write(1, after, tx.number(), rec.lsn());
	}
	
	private void againUpdateFreeList(int headSize)
	{
		if (freeSpace[0].getStart() < headSize)
		{
			if (freeSpace[0].getEnd() <= headSize)
			{
				freeSpace[0].setStart(headSize);
				return;
			}
			
			FreeSpace[] newFS = new FreeSpace[freeSpace.length - 1];
			int i = 0;
			while (i < newFS.length)
			{
				newFS[i] = freeSpace[i+1];
				i++;
			}
			
			freeSpace = newFS;
			freeSpaceListEntries--;
		}
	}
	
	private void updateOffsets(FieldValue[] vals, int off)
	{
		int[][] newOA = new int[rowIDs.length][colIDs.length];
		int i = 0;
		while (i < offsetArray.length)
		{
			newOA[i] = offsetArray[i];
			i++;
		}
		
		int j = 0;
		for (FieldValue fv : vals)
		{
			newOA[i][j] = off;
			j++;
			off += fv.size();
		}
		
		offsetArray = newOA;
	}
	
	private void updateNullArray(FieldValue[] vals)
	{
		byte[][] newNA;
		newNA = new byte[nullArray.length + 1][colIDs.length];
		int i = 0;
		while (i < nullArray.length)
		{
			newNA[i] = nullArray[i];
			i++;
		}
		
		int j = 0;
		for (FieldValue fv : vals)
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
	
	private void updateRowIDs(RID id)
	{
		RID[] newRI = new RID[rowIDs.length + 1];
		int i = 0;
		while (i < rowIDs.length)
		{
			newRI[i] = rowIDs[i];
			i++;
		}
		
		newRI[i] = id;
		rowIDs = newRI;
		rowIDListSize++;
	}
	
	private int findNextFreeRecNum(int justUsed) throws RecNumOverflowException
	{
		int candidate = justUsed+1;
		RID[] copy = rowIDs.clone();
		Arrays.sort(copy);
		
		int i = 0;
		for (RID rid : copy)
		{
			if (rid.getRecNum() > justUsed)
			{
				break;
			}
			
			i++;
		}
		
		RID[] copy2 = new RID[copy.length - i];
		int j = 0;
		while (j < copy2.length)
		{
			copy2[j] = copy[i];
			i++;
			j++;
		}
		
		for (RID rid : copy2)
		{
			int num = rid.getRecNum();
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
		freeSpace = new FreeSpace[freeSpaceListEntries];
		while (i < freeSpaceListEntries)
		{
			int start = p.getInt(pos);
			int end = p.getInt(pos+4);
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
		for (int colID : colIDs)
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
			int node = p.getInt(pos);
			int dev = p.getInt(pos+4);
			int block = p.getInt(pos+8);
			int rec = p.getInt(pos+12);
			rowIDs[i] = new RID(node, dev, block, rec);
			i++;
			pos += 16;
		}
		
		i = 0;
		for (RID rid : rowIDs)
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
	
	public ColIterator colIterator()
	{
		return new ColIterator();
	}
	
	class RowIterator implements Iterator<Row>
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
	
	class Row
	{
		private int index;
		
		public Row(int index)
		{
			this.index = index;
		}
		
		public RID getRID()
		{
			return rowIDs[index];
		}
		
		public FieldValue getCol(int id) throws Exception
		{
			return getField(index, colIDToIndex.get(id));
		}
		
		public FieldValue[] getAllCols() throws Exception
		{
			int i = 0;
			FieldValue[] retval = new FieldValue[colIDListSize];
			while (i < colIDListSize)
			{
				retval[i] = getField(index, i);
				i++;
			}
			
			return retval;
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
	
	class Col
	{
		private int index;
		
		public Col(int index)
		{
			this.index = index;
		}
		
		public int getColID()
		{
			return colIDs[index];
		}
		
		public FieldValue getRow(RID id) throws Exception
		{
			return getField(rowIDToIndex.get(id), index);
		}
		
		public FieldValue[] getAllRows() throws Exception
		{
			int i = 0;
			FieldValue[] retval = new FieldValue[rowIDListSize];
			while (i < rowIDListSize)
			{
				retval[i] = getField(i, index);
				i++;
			}
			
			return retval;
		}
	}
	
	public Row getRow(RID id)
	{
		return new Row(rowIDToIndex.get(id));
	}
	
	public Col getCol(int id)
	{
		return new Col(colIDToIndex.get(id));
	}
	
	public FieldValue getField(int rowIndex, int colIndex) throws Exception
	{
		DataType dt = colTypes.get(colIDs[colIndex]);
		int type = dt.getType();
		
		if (type == DataType.BIGINT)
		{
			return new BigintFV(rowIndex, colIndex);
		}
		
		if (type == DataType.BINARY)
		{
			return new BinaryFV(rowIndex, colIndex, dt.getLength());
		}
		
		if (type == DataType.DATE || type == DataType.TIME || type == DataType.TIMESTAMP)
		{
			return new DateTimeFV(rowIndex, colIndex);
		}
		
		if (type == DataType.DECIMAL)
		{
			return new DecimalFV(rowIndex, colIndex, dt.getLength(), dt.getScale());
		}
		
		if (type == DataType.DOUBLE)
		{
			return new DoubleFV(rowIndex, colIndex);
		}
		
		if (type == DataType.FLOAT)
		{
			return new FloatFV(rowIndex, colIndex);
		}
		
		if (type == DataType.INTEGER)
		{
			return new IntegerFV(rowIndex, colIndex);
		}
		
		if (type == DataType.SMALLINT)
		{
			return new SmallintFV(rowIndex, colIndex);
		}
		
		if (type == DataType.VARBINARY)
		{
			return new VarbinaryFV(rowIndex, colIndex);
		}
		
		if (type == DataType.VARCHAR)
		{
			return new VarcharFV(rowIndex, colIndex);
		}
		
		System.err.println("Unknown data type in Schema.getField()");
		return null;
	}
	
	public abstract class FieldValue 
	{
		protected boolean isNull;
		protected boolean exists;
		protected int off;
		
		public FieldValue(int row, int col)
		{
			isNull = (nullArray[row][col] != 0);
			off = offsetArray[row][col];
			exists = (off != -1);
		}
		
		public FieldValue()
		{}
		
		public boolean isNull()
		{
			return isNull;
		}
		
		public boolean exists()
		{
			return exists;
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
	
	public class BigintFV extends FieldValue
	{
		private long value;
		
		public BigintFV(int row, int col)
		{
			super(row, col);
			
			if (!isNull && exists)
			{
				value = p.getLong(off); 
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
		
		public int size()
		{
			if (!isNull && exists)
			{
				return 8;
			}
			
			return 0;
		}
		
		public long getValue()
		{
			return value;
		}
		
		public int write(byte[] buff, int off)
		{
			if (!isNull && exists)
			{
				byte[] val = ByteBuffer.allocate(8).putLong(value).array();
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
	
	public class BinaryFV extends FieldValue
	{
		private byte[] value;
		
		public BinaryFV(int row, int col, int len)
		{
			super(row, col);
			
			if (!isNull && exists)
			{
				value = new byte[len];
				p.get(off, value);
			}
		}
		
		public int size()
		{
			if (!isNull && exists)
			{
				return value.length;
			}
			
			return 0;
		}
		
		public byte[] getValue()
		{
			return value;
		}
		
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
	
	public class DateTimeFV extends FieldValue
	{
		private Date value;
		
		public DateTimeFV(int row, int col)
		{
			super(row, col);
			
			if (!isNull && exists)
			{
				value = new Date(p.getLong(off));
			}
		}
		
		public int size()
		{
			if (!isNull && exists)
			{
				return 8;
			}
			
			return 0;
		}
		
		public Date getValue()
		{
			return value;
		}
		
		public int write(byte[] buff, int off)
		{
			if (!isNull && exists)
			{
				byte[] val = ByteBuffer.allocate(8).putLong(value.getTime()).array();
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
	
	public class DecimalFV extends FieldValue
	{
		private BigDecimal value;
		private int length;
		private byte[] bytes;
		
		public DecimalFV(int row, int col, int length, int scale)
		{
			super(row, col);
			this.length = length;
			
			if (!isNull && exists)
			{
				int numBytes = (int)(Math.ceil(((length+1)*1.0) / 2.0));
				bytes = new byte[numBytes];
				p.get(off, bytes);
				boolean positive = (bytes[0] & 0xF0) == 0;
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
		
		public int size()
		{
			if (!isNull && exists)
			{
				return (int)(Math.ceil(((length+1)*1.0) / 2.0));
			}
			
			return 0;
		}
		
		public BigDecimal getValue()
		{
			return value;
		}
		
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
	
	public class DoubleFV extends FieldValue
	{
		private double value;
		
		public DoubleFV(int row, int col)
		{
			super(row, col);
			
			if (!isNull && exists)
			{
				value = p.getDouble(off); 
			}
		}
		
		public int size()
		{
			if (!isNull && exists)
			{
				return 8;
			}
			
			return 0;
		}
		
		public double getValue()
		{
			return value;
		}
		
		public int write(byte[] buff, int off)
		{
			if (!isNull && exists)
			{
				byte[] val = ByteBuffer.allocate(8).putDouble(value).array();
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
	
	public class FloatFV extends FieldValue
	{
		private float value;
		
		public FloatFV(int row, int col)
		{
			super(row, col);
			
			if (!isNull && exists)
			{
				value = p.getFloat(off); 
			}
		}
		
		public int size()
		{
			if (!isNull && exists)
			{
				return 4;
			}
			
			return 0;
		}
		
		public float getValue()
		{
			return value;
		}
		
		public int write(byte[] buff, int off)
		{
			if (!isNull && exists)
			{
				byte[] val = ByteBuffer.allocate(4).putFloat(value).array();
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
	
	public class IntegerFV extends FieldValue
	{
		private int value;
		
		public IntegerFV(int row, int col)
		{
			super(row, col);
			
			if (!isNull && exists)
			{
				value = p.getInt(off); 
			}
		}
		
		public int size()
		{
			if (!isNull && exists)
			{
				return 4;
			}
			
			return 0;
		}
		
		public int getValue()
		{
			return value;
		}
		
		public int write(byte[] buff, int off)
		{
			if (!isNull && exists)
			{
				byte[] val = ByteBuffer.allocate(4).putInt(value).array();
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
	
	public class SmallintFV extends FieldValue
	{
		private short value;
		
		public SmallintFV(int row, int col)
		{
			super(row, col);
			
			if (!isNull && exists)
			{
				value = p.getShort(off); 
			}
		}
		
		public int size()
		{
			if (!isNull && exists)
			{
				return 2;
			}
			
			return 0;
		}
		
		public short getValue()
		{
			return value;
		}
		
		public int write(byte[] buff, int off)
		{
			if (!isNull && exists)
			{
				byte[] val = ByteBuffer.allocate(2).putShort(value).array();
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
	
	public class VarbinaryFV extends FieldValue
	{
		private byte[] value;
		
		public VarbinaryFV(int row, int col)
		{
			super(row, col);
			
			if (!isNull && exists)
			{
				int len = p.getInt(off);
				value = new byte[len];
				p.get(off+4, value);
			}
		}
		
		public int size()
		{
			if (!isNull && exists)
			{
				return 4 + value.length;
			}
			
			return 0;
		}
		
		public byte[] getValue()
		{
			return value;
		}
		
		public int write(byte[] buff, int off)
		{
			if (!isNull && exists)
			{
				byte[] val = ByteBuffer.allocate(4).putInt(value.length).array();
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
	
	public class VarcharFV extends FieldValue
	{
		private String value;
		private int size;
		
		public VarcharFV(int row, int col) throws Exception
		{
			super(row, col);
			
			if (!isNull && exists)
			{
				int len = p.getInt(off);
				byte[] temp = new byte[len];
				size = 4 + len;
				p.get(off + 4, temp);
				try
				{
					value = new String(temp, "UTF-8");
				}
				catch(Exception e)
				{
					throw e;
				}
			}
		}
		
		public int size()
		{
			if (!isNull && exists)
			{
				return size;
			}
			
			return 0;
		}
		
		public String getValue()
		{
			return value;
		}
		
		public int write(byte[] buff, int off) throws UnsupportedEncodingException
		{
			if (!isNull && exists)
			{
				byte[] stringBytes = value.getBytes("UTF-8");
				byte[] val = ByteBuffer.allocate(4).putInt(stringBytes.length).array();
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
	
	public void deleteRow(RID id) throws LockAbortException
	{
		int level = tx.getIsolationLevel();
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
		int index = rowIDToIndex.get(id);
		off = offArrayOff + index * colIDListSize * 4;
		before = new byte[colIDListSize * 4];
		p.get(off, before);
		after = new byte[colIDListSize * 4];
		int i = 0;
		while (i < after.length)
		{
			offsetArray[index][i] = -1;
			after[i] = (byte)0xFF;
			i++;
		}
		
		LogRec rec = tx.delete(before, after, off, p.block()); //write log rec
		p.write(off, after, tx.number(), rec.lsn());
		
		if (id.getRecNum() < nextRecNum)
		{
			nextRecNum = id.getRecNum();
			off = 1;
			before = new byte[4];
			after = new byte[4];
			p.get(1, before);
			after = ByteBuffer.allocate(4).putInt(id.getRecNum()).array();
			rec = tx.delete(before, after, off, p.block()); //write log rec
			p.write(off, after, tx.number(), rec.lsn());
		}
		
		//modification timestamp
		off = 13;
		before = new byte[8];
		after = new byte[8];
		p.get(13, before);
		modTime = System.currentTimeMillis();
		after = ByteBuffer.allocate(8).putLong(modTime).array();
		rec = tx.delete(before, after, off, p.block());
		p.write(off, after, tx.number(), rec.lsn());
	}	
	
	public RIDChange updateRow(RID id, int startColID, FieldValue[] vals) throws Exception
	{
		if (blockType == TYPE_ROW)
		{
			int level = tx.getIsolationLevel();
			if (level == Transaction.ISOLATION_CS || level == Transaction.ISOLATION_UR)
			{
				tx.setIsolationLevel(Transaction.ISOLATION_RR);
				tx.read(p.block(), this);
				tx.setIsolationLevel(level);
			}
			
			LockManager.xLock(p.block(), tx.number());
			Row row = this.getRow(id);
			FieldValue[] values = row.getAllCols();
			this.deleteRow(id);
			int i = colIDToIndex.get(startColID);
			int j = 0;
			while (i < colIDListSize)
			{
				values[i] = vals[j];
				i++;
				j++;
			}
		
			RID newRID = this.insertRow(values);
		
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
	
	private void updateFSInHeader(int size) throws LockAbortException
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
		
		Block blk = new Block(p.block().fileName(), pageNum);
		tx.requestPage(blk);
		HeaderPage hp = tx.readHeaderPage(blk, blockType);
		
		if (blockType == TYPE_ROW)
		{
			hp.updateSize(entryNum, size, tx.number());
		}
		else
		{
			hp.updateSize(entryNum, size, tx.number(), 0);
		}
	}
	
	private int findPage(int data, int colID, int colMarker) throws LockAbortException
	{
		Block[] header = new Block[HEADER_SIZE];
		int i = 0;
		String fn = p.block().fileName();
		while (i < HEADER_SIZE)
		{
			header[i] = new Block(fn, i);
			i++;
		}
		tx.requestPages(header);
		
		i = 0;
		int block = -1;
		while (i < HEADER_SIZE && block == -1)
		{
			HeaderPage hp = tx.readHeaderPage(header[i], blockType);
			
			if (hp == null)
			{
				return -1;
			}
			block = hp.findPage(data, colID, colMarker);
		}
		
		return block;
	}
	
	private int findPage(int data) throws LockAbortException
	{
		Block[] header = new Block[HEADER_SIZE];
		int i = 0;
		String fn = p.block().fileName();
		while (i < HEADER_SIZE)
		{
			header[i] = new Block(fn, i);
			i++;
		}
		tx.requestPages(header);
		
		i = 0;
		int block = -1;
		while (i < HEADER_SIZE && block == -1)
		{
			HeaderPage hp = tx.readHeaderPage(header[i], blockType);
			
			if (hp == null)
			{
				return -1;
			}
			
			block = hp.findPage(data);
		}
		
		return block;
	}
	
	private int getNodeNumber() throws LockAbortException
	{
		Block b = new Block(p.block().fileName(), 0);
		tx.requestPage(b);
		
		HeaderPage hp = tx.readHeaderPage(b, blockType);
		return hp.getNodeNumber();
	}
	
	private int getDeviceNumber() throws LockAbortException
	{
		Block b = new Block(p.block().fileName(), 0);
		tx.requestPage(b);
		
		HeaderPage hp = tx.readHeaderPage(b, blockType);
		return hp.getDeviceNumber();
	}
	
	private int findPage(int after, int data, int colID, int colMarker) throws LockAbortException
	{
		int i = after + 1;
		int block = -1;
		while (i < HEADER_SIZE && block == -1)
		{
			HeaderPage hp = tx.readHeaderPage(new Block(p.block().fileName(), i), blockType);
			block = hp.findPage(data, colID, colMarker);
		}
		
		return block;
	}
	
	private int findPage(int after, int data) throws LockAbortException
	{
		int i = after + 1;
		int block = -1;
		while (i < HEADER_SIZE && block == -1)
		{
			HeaderPage hp = tx.readHeaderPage(new Block(p.block().fileName(), i), blockType);
			block = hp.findPage(data);
		}
		
		return block;
	}
	
	public RIDChange updateRowForColTable(RID id, FieldValue val) throws LockAbortException, IOException, RecNumOverflowException
	{
		RID newRID;
		this.deleteRow(id);
		
		int length = val.size();
		
		if (hasEnoughSpace(headerGrowthPerRow(), length) != null)
		{
			FieldValue[] value = new FieldValue[1];
			value[0] = val;
			newRID = this.insertRow(value);
		}
		else
		{
			int blockNum = findPage(length, colIDs[0], 0);
				
			while (blockNum != -1)
			{
				Block b = new Block(p.block().fileName(), blockNum);
				tx.requestPage(b);
				Schema newPage = new Schema(this.colTypes);
				int level = tx.getIsolationLevel();
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
					FieldValue[] value = new FieldValue[1];
					value[0] = val;
					newRID = newPage.insertRow(value);
				}
			
				blockNum = findPage(blockNum, length, colIDs[0], 0); //find blocks after this num
			}
			
			Block b = addNewBlock(p.block().fileName(), colIDs);
			tx.requestPage(b);
			Schema newPage = new Schema(this.colTypes);
			
			int level = tx.getIsolationLevel();
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
			
			FieldValue[] value = new FieldValue[1];
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
	
	public RID insertRow(FieldValue[] vals) throws LockAbortException, UnsupportedEncodingException, IOException, RecNumOverflowException
	{
			int length = 0;
			for (FieldValue val : vals)
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
		
			Integer off = hasEnoughSpace(headerGrowthPerRow(), length);
			if (off != null)
			{
				int node = getNodeNumber(); //from 1st header page
				int dev = getDeviceNumber(); //from 1st header page
				RID rid = new RID(node, dev, p.block().number(), nextRecNum);
				byte[] after = new byte[length];
				int i = 0;
				int index = 0;
				while (index < colIDListSize)
				{
					i += vals[index].write(after, i);
					index++;
				}
			
				byte[] before = new byte[length];
				p.buffer().position(off);
				p.buffer().get(before);
				LogRec rec = tx.insert(before, after, off, p.block()); //write log rec
				p.write(off, after, tx.number(), rec.lsn());			
				updateHeader(rid, vals, nextRecNum, off, length); //update nextRecNum, freeSpace, nullArray, rowIDList, and offsetArray internal and external
				return rid;
			}	
			else
			{
				int blockNum = findPage(length);
				
				while (blockNum != -1)
				{
					Block b = new Block(p.block().fileName(), blockNum);
					tx.requestPage(b);
					Schema newPage = new Schema(this.colTypes);
					
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
			
					blockNum = findPage(blockNum, length); //find blocks before this num
				}
			
				Block b = addNewBlock(p.block().fileName(), colIDs);
				tx.requestPage(b);
				Schema newPage = new Schema(this.colTypes);
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
	
	public RID[] insertRowForColTable(FieldValue[] vals) throws LockAbortException, IOException, RecNumOverflowException
	{
		//relies on colID always starting from 0 and being sequential
		//type TYPE_COL
		int count = 0;
		RID[] retval = new RID[vals.length];
		
		int level = tx.getIsolationLevel();
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
			int length = vals[count].size();
			
			if (count == colIDs[0] && hasEnoughSpace(headerGrowthPerRow(), length) != null)
			{
				//this page has this column
				FieldValue[] value = new FieldValue[1];
				value[0] = vals[count];
				retval[count] = this.insertRow(value);
			}
			else
			{
				int blockNum = findPage(length, count, 0);
					
				while (blockNum != -1)
				{
					Block b = new Block(p.block().fileName(), blockNum);
					tx.requestPage(b);
					Schema newPage = new Schema(this.colTypes);
					tx.read(b, newPage);
					LockManager.xLock(b, tx.number());
			
					if (newPage.hasEnoughSpace(headerGrowthPerRow(), length) != null)
					{
						FieldValue[] value = new FieldValue[1];
						value[0] = vals[count];
						retval[count] = newPage.insertRow(value);
					}
				
					blockNum = findPage(blockNum, length, count, 0); //find blocks before this num
				}
				
				int[] cols = new int[1];
				cols[0] = count;
				Block b = addNewBlock(p.block().fileName(), cols);
				tx.requestPage(b);
				Schema newPage = new Schema(this.colTypes);
				tx.read(b, newPage);
				
				FieldValue[] value = new FieldValue[1];
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
}