package com.exascale.logging;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashSet;

import com.exascale.filesystem.Block;
import com.exascale.managers.HRDBMSWorker;

public class LogRec 
{
	protected int type;
	protected long txnum;
	protected long lsn;
	protected long timestamp;
	protected ByteBuffer buffer;
	
	public static final int NQCHECK = 0, START = 1, COMMIT = 2, ROLLB = 3, INSERT = 4,  DELETE = 5;
	
	protected LogRec(int type, long txnum, ByteBuffer buffer)
	{
		this.type = type;
		this.txnum = txnum;
		this.buffer = buffer;
		buffer.position(0);
		buffer.putInt(type);
		buffer.putLong(txnum);
		this.setTimeStamp(System.currentTimeMillis());
	}
	
	public LogRec(FileChannel fc) throws IOException
	{
		fc.position(fc.position() - 4); //leading size
		ByteBuffer size = ByteBuffer.allocate(4);
		size.position(0);
		fc.read(size);
		size.position(0);
		int sizeVal = size.getInt();
		ByteBuffer rec = ByteBuffer.allocate(sizeVal);
		rec.position(0);
		fc.read(rec);
		buffer = rec;
		buffer.position(0);
		type = buffer.getInt();
		txnum = buffer.getLong();
		lsn = buffer.getLong();
		timestamp = buffer.getLong();
	}
	
	public LogRec rebuild()
	{
		if (type == INSERT)
		{
			buffer.position(28);
			byte[] bytes = new byte[buffer.getInt()];
			buffer.get(bytes);
			Block block;
			try
			{
				block = new Block(new String(bytes, "UTF-8"));
			}
			catch(Exception e)
			{
				HRDBMSWorker.logger.error("Error converting bytes to UTF-8 string in LogRec.rebuild().", e);
				return null;
			}
			int imageSize = buffer.getInt();
			bytes = new byte[imageSize];
			buffer.get(bytes);
			byte[] after = new byte[imageSize];
			buffer.get(after);
			int off = buffer.getInt();
			InsertLogRec retval = new InsertLogRec(txnum, block, off, bytes, after);
			retval.setLSN(lsn);
			retval.setTimeStamp(timestamp);
			return retval;
		}
		
		if (type == DELETE)
		{
			buffer.position(28);
			byte[] bytes = new byte[buffer.getInt()];
			buffer.get(bytes);
			Block block;
			try
			{
				block = new Block(new String(bytes, "UTF-8"));
			}
			catch(Exception e)
			{
				HRDBMSWorker.logger.error("Error converting bytes to UTF-8 string in LogRec.rebuild().", e);
				return null;
			}
			int imageSize = buffer.getInt();
			bytes = new byte[imageSize];
			buffer.get(bytes);
			byte[] after = new byte[imageSize];
			buffer.get(after);
			int off = buffer.getInt();
			DeleteLogRec retval = new DeleteLogRec(txnum, block, off, bytes, after);
			retval.setLSN(lsn);
			retval.setTimeStamp(timestamp);
			return retval;
		}
		
		if (type == NQCHECK)
		{
			buffer.position(28);
			int numTx = buffer.getInt();
			
			int i = 0;
			HashSet<Long> txs = new HashSet<Long>();
			while (i < numTx)
			{
				txs.add(buffer.getLong());
				i++;
			}
			
			NQCheckLogRec retval = new NQCheckLogRec(txs);
			retval.setLSN(lsn);
			retval.setTimeStamp(timestamp);
			return retval;
		}
		
		if (type == START)
		{
			return new StartLogRec(txnum);
		}
		
		if (type == COMMIT)
		{
			return new CommitLogRec(txnum);
		}
		
		if (type == ROLLB)
		{
			return new RollbackLogRec(txnum);
		}
		
		return this;
	}
	
	public int type()
	{
		return type;
	}
	
	public long lsn()
	{
		return lsn;
	}
	
	public void setTimeStamp(long time)
	{
		this.timestamp = time;
		buffer.position(20);
		buffer.putLong(time);
	}
	
	public void setLSN(long lsn)
	{
		this.lsn = lsn;
		buffer.position(12);
		buffer.putLong(lsn);
	}
	
	public int size()
	{
		return buffer.capacity();
	}
	
	public ByteBuffer buffer()
	{
		return buffer;
	}
	
	public void undo()
	{}
	
	public void redo()
	{}
	
	public long txnum()
	{
		return txnum;
	}
}
  