package com.exascale.logging;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashSet;
import com.exascale.filesystem.Block;
import com.exascale.managers.HRDBMSWorker;

public class LogRec
{
	private final int type;
	private final long txnum;
	private long lsn;
	private long timestamp;
	protected final ByteBuffer buffer;

	public static final int NQCHECK = 0, START = 1, COMMIT = 2, ROLLB = 3, INSERT = 4, DELETE = 5, PREPARE = 6, READY = 7, NOTREADY = 8, XACOMMIT = 9, XAABORT = 10, EXTEND = 11;

	public LogRec(FileChannel fc) throws IOException
	{
		fc.position(fc.position() - 4); // leading size
		final ByteBuffer size = ByteBuffer.allocate(4);
		size.position(0);
		fc.read(size);
		size.position(0);
		final int sizeVal = size.getInt();
		final ByteBuffer rec = ByteBuffer.allocate(sizeVal);
		rec.position(0);
		fc.read(rec);
		buffer = rec;
		buffer.position(0);
		type = buffer.getInt();
		txnum = buffer.getLong();
		lsn = buffer.getLong();
		timestamp = buffer.getLong();
	}

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

	public ByteBuffer buffer()
	{
		return buffer;
	}

	public long lsn()
	{
		return lsn;
	}

	public LogRec rebuild() throws Exception
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
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("Error converting bytes to UTF-8 string in LogRec.rebuild().", e);
				return null;
			}
			final int imageSize = buffer.getInt();
			bytes = new byte[imageSize];
			buffer.get(bytes);
			final byte[] after = new byte[imageSize];
			buffer.get(after);
			final int off = buffer.getInt();
			final InsertLogRec retval = new InsertLogRec(txnum, block, off, bytes, after);
			retval.setLSN(lsn);
			retval.setTimeStamp(timestamp);
			return retval;
		}
		
		if (type == EXTEND)
		{
			buffer.position(28);
			byte[] bytes = new byte[buffer.getInt()];
			buffer.get(bytes);
			Block block;
			try
			{
				block = new Block(new String(bytes, "UTF-8"));
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("Error converting bytes to UTF-8 string in LogRec.rebuild().", e);
				return null;
			}
			
			ExtendLogRec retval = new ExtendLogRec(txnum, block);
			retval.setLSN(lsn);
			retval.setTimeStamp(timestamp);
			return retval;
		}
		
		if (type == XACOMMIT)
		{
			buffer.position(28);
			int size = buffer.getInt();
			ArrayList<Integer> retval = new ArrayList<Integer>(size);
			while (size > 0)
			{
				retval.add(buffer.getInt());
				size--;
			}
			
			return new XACommitLogRec(txnum, retval);
		}
		
		if (type == XAABORT)
		{
			buffer.position(28);
			int size = buffer.getInt();
			ArrayList<Integer> retval = new ArrayList<Integer>(size);
			while (size > 0)
			{
				retval.add(buffer.getInt());
				size--;
			}
			
			return new XAAbortLogRec(txnum, retval);
		}
		
		if (type == PREPARE)
		{
			buffer.position(28);
			int size = buffer.getInt();
			ArrayList<Integer> retval = new ArrayList<Integer>(size);
			while (size > 0)
			{
				retval.add(buffer.getInt());
				size--;
			}
			
			return new PrepareLogRec(txnum, retval);
		}
		
		if (type == READY)
		{
			buffer.position(28);
			int length = buffer.getInt();
			byte[] data = new byte[length];
			buffer.get(data);
			try
			{
				return new ReadyLogRec(txnum, new String(data, "UTF-8"));
			}
			catch(Exception e)
			{}
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
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("Error converting bytes to UTF-8 string in LogRec.rebuild().", e);
				return null;
			}
			final int imageSize = buffer.getInt();
			bytes = new byte[imageSize];
			buffer.get(bytes);
			final byte[] after = new byte[imageSize];
			buffer.get(after);
			final int off = buffer.getInt();
			final DeleteLogRec retval = new DeleteLogRec(txnum, block, off, bytes, after);
			retval.setLSN(lsn);
			retval.setTimeStamp(timestamp);
			return retval;
		}

		if (type == NQCHECK)
		{
			buffer.position(28);
			final int numTx = buffer.getInt();

			int i = 0;
			final HashSet<Long> txs = new HashSet<Long>();
			while (i < numTx)
			{
				txs.add(buffer.getLong());
				i++;
			}

			final NQCheckLogRec retval = new NQCheckLogRec(txs);
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

	public void redo()
	{
	}

	public void setLSN(long lsn)
	{
		this.lsn = lsn;
		buffer.position(12);
		buffer.putLong(lsn);
	}

	public void setTimeStamp(long time)
	{
		this.timestamp = time;
		buffer.position(20);
		buffer.putLong(time);
	}

	public int size()
	{
		return buffer.capacity();
	}

	public long txnum()
	{
		return txnum;
	}

	public int type()
	{
		return type;
	}

	public void undo()
	{
	}
}
