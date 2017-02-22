package com.exascale.logging;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import com.exascale.filesystem.Block;
import com.exascale.managers.HRDBMSWorker;

public class LogRec implements Comparable<LogRec>
{
	public static final int NQCHECK = 0, START = 1, COMMIT = 2, ROLLB = 3, INSERT = 4, DELETE = 5, PREPARE = 6, READY = 7, NOTREADY = 8, XACOMMIT = 9, XAABORT = 10, EXTEND = 11, TRUNCATE = 12;
	private final int type;
	private final long txnum;
	private long lsn;
	private long timestamp;

	protected final ByteBuffer buffer;

	public LogRec(final FileChannel fc) throws IOException
	{
		final long orig = fc.position();
		fc.position(orig - 4); // leading size
		final ByteBuffer size = ByteBuffer.allocate(4);
		size.position(0);
		fc.read(size);
		size.position(0);
		final int sizeVal = size.getInt();
		if (sizeVal < 28)
		{
			throw new IOException("Too short log rec of length " + sizeVal + " original position = " + orig);
		}
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

	public LogRec(final FileChannel fc, final boolean partial) throws IOException
	{
		// fc.position(fc.position() - 4); // leading size
		// final ByteBuffer size = ByteBuffer.allocate(4);
		// size.position(0);
		// fc.read(size);
		// size.position(0);
		// final int sizeVal = size.getInt();
		final ByteBuffer rec = ByteBuffer.allocate(12);
		rec.position(0);
		fc.read(rec);
		buffer = rec;
		buffer.position(0);
		type = buffer.getInt();
		txnum = buffer.getLong();
	}

	protected LogRec(final int type, final long txnum, final ByteBuffer buffer)
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

	@Override
	public int compareTo(final LogRec arg0)
	{
		if (lsn < arg0.lsn)
		{
			return -1;
		}
		else if (lsn > arg0.lsn)
		{
			return 1;
		}
		else
		{
			return 0;
		}
	}

	@Override
	public boolean equals(final Object rhs)
	{
		if (rhs == null)
		{
			return false;
		}

		if (rhs instanceof LogRec)
		{
			return lsn == ((LogRec)rhs).lsn;
		}

		return false;
	}

	public int getEnd()
	{
		if (this instanceof InsertLogRec)
		{
			return ((InsertLogRec)this).getEnd();
		}

		if (this instanceof DeleteLogRec)
		{
			return ((DeleteLogRec)this).getEnd();
		}

		return -1;
	}

	public int getOffset()
	{
		if (this instanceof InsertLogRec)
		{
			return ((InsertLogRec)this).getOffset();
		}

		if (this instanceof DeleteLogRec)
		{
			return ((DeleteLogRec)this).getOffset();
		}

		return -1;
	}

	public long getTimeStamp()
	{
		return timestamp;
	}

	@Override
	public int hashCode()
	{
		return new Long(lsn).hashCode();
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
			final char[] bytes2 = new char[buffer.getInt()];
			buffer.asCharBuffer().get(bytes2);
			final Block block = new Block(new String(bytes2));
			buffer.position(32 + (bytes2.length << 1));
			final int imageSize = buffer.getInt();
			final byte[] bytes = new byte[imageSize];
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
			final byte[] bytes = new byte[buffer.getInt()];
			buffer.get(bytes);
			Block block;
			try
			{
				block = new Block(new String(bytes, StandardCharsets.UTF_8));
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("Error converting bytes to UTF-8 string in LogRec.rebuild().", e);
				return null;
			}

			final ExtendLogRec retval = new ExtendLogRec(txnum, block);
			retval.setLSN(lsn);
			retval.setTimeStamp(timestamp);
			return retval;
		}

		if (type == TRUNCATE)
		{
			buffer.position(28);
			final byte[] bytes = new byte[buffer.getInt()];
			buffer.get(bytes);
			Block block;
			try
			{
				block = new Block(new String(bytes, StandardCharsets.UTF_8));
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("Error converting bytes to UTF-8 string in LogRec.rebuild().", e);
				return null;
			}

			final TruncateLogRec retval = new TruncateLogRec(txnum, block);
			retval.setLSN(lsn);
			retval.setTimeStamp(timestamp);
			return retval;
		}

		if (type == XACOMMIT)
		{
			buffer.position(28);
			int size = buffer.getInt();
			final ArrayList<Integer> retval = new ArrayList<Integer>(size);
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
			final ArrayList<Integer> retval = new ArrayList<Integer>(size);
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
			final ArrayList<Integer> retval = new ArrayList<Integer>(size);
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
			final int length = buffer.getInt();
			final byte[] data = new byte[length];
			buffer.get(data);
			try
			{
				return new ReadyLogRec(txnum, new String(data, StandardCharsets.UTF_8));
			}
			catch (final Exception e)
			{
			}
		}

		if (type == DELETE)
		{
			buffer.position(28);
			byte[] bytes = new byte[buffer.getInt()];
			buffer.get(bytes);
			Block block;
			try
			{
				block = new Block(new String(bytes, StandardCharsets.UTF_8));
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

	public void redo() throws Exception
	{
	}

	public void setLSN(final long lsn)
	{
		this.lsn = lsn;
		buffer.position(12);
		buffer.putLong(lsn);
	}

	public void setTimeStamp(final long time)
	{
		this.timestamp = time;
		buffer.putLong(20, time);
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

	public void undo() throws Exception
	{
	}
}
