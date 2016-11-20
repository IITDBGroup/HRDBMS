package com.exascale.client;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Field;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;

public class HRDBMSStatement implements Statement
{
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
	protected boolean closed = false;
	private final HRDBMSConnection conn;
	private HRDBMSResultSet result;
	private int updateCount = -1;
	private int fetchSize = 30000;
	protected ArrayList<Object> parms = new ArrayList<Object>();
	private final ArrayList<String> batch = new ArrayList<String>();

	private final CharsetEncoder ce = cs.newEncoder();

	public HRDBMSStatement(final HRDBMSConnection conn)
	{
		this.conn = conn;
	}

	public HRDBMSStatement(final HRDBMSConnection conn, final int type, final int concur) throws SQLFeatureNotSupportedException
	{
		this.conn = conn;

		if (concur != ResultSet.CONCUR_READ_ONLY)
		{
			throw new SQLFeatureNotSupportedException();
		}

		if (type != ResultSet.TYPE_FORWARD_ONLY)
		{
			throw new SQLFeatureNotSupportedException();
		}
	}

	public HRDBMSStatement(final HRDBMSConnection conn, final int type, final int concur, final int hold) throws SQLFeatureNotSupportedException
	{
		this.conn = conn;

		if (concur != ResultSet.CONCUR_READ_ONLY)
		{
			throw new SQLFeatureNotSupportedException();
		}

		if (type != ResultSet.TYPE_FORWARD_ONLY)
		{
			throw new SQLFeatureNotSupportedException();
		}

		if (type != ResultSet.CLOSE_CURSORS_AT_COMMIT)
		{
			throw new SQLFeatureNotSupportedException();
		}
	}

	private static int bytesToInt(final byte[] val)
	{
		final int ret = java.nio.ByteBuffer.wrap(val).getInt();
		return ret;
	}

	private static byte[] intToBytes(final int val)
	{
		final byte[] buff = new byte[4];
		buff[0] = (byte)(val >> 24);
		buff[1] = (byte)((val & 0x00FF0000) >> 16);
		buff[2] = (byte)((val & 0x0000FF00) >> 8);
		buff[3] = (byte)((val & 0x000000FF));
		return buff;
	}

	@Override
	public void addBatch(String sql) throws SQLException
	{
		if (closed)
		{
			throw new SQLException("addBatch() called on a closed connection");
		}

		sql = setParms(sql);

		if (sql.toUpperCase().startsWith("WITH") || sql.toUpperCase().startsWith("SELECT"))
		{
			throw new SQLException("A SELECT statement cannot be added to a batch");
		}

		batch.add(sql);
	}

	@Override
	public void cancel() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void clearBatch() throws SQLException
	{
		if (closed)
		{
			throw new SQLException("clearBatch() called on a closed statement");
		}

		batch.clear();
	}

	@Override
	public void clearWarnings() throws SQLException
	{
		if (closed)
		{
			throw new SQLException("clearWarnings() called on a closed statement");
		}

		conn.firstWarning = null;
	}

	@Override
	public void close() throws SQLException
	{
		if (this.result != null)
		{
			result.close();
		}

		closed = true;
	}

	@Override
	public void closeOnCompletion() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean execute(String sql) throws SQLException
	{
		if (sql.toUpperCase().startsWith("SELECT") || sql.toUpperCase().startsWith("WITH"))
		{
			try
			{
				if (conn.rs != null && !conn.rs.isClosed())
				{
					throw new SQLException("A connection can only have 1 open result set at a time");
				}

				sql = setParms(sql);
				final byte[] command = "EXECUTEQ        ".getBytes(StandardCharsets.UTF_8);
				final byte[] statement = sql.getBytes(StandardCharsets.UTF_8);
				final byte[] length = intToBytes(statement.length);
				conn.out.write(command);
				conn.out.write(length);
				conn.out.write(statement);
				conn.out.flush();
				if (!conn.openTransaction)
				{
					conn.openTransaction = true;
				}
				try
				{
					this.getConfirmation();
					try
					{
						result = conn.rs = new HRDBMSResultSet(conn, fetchSize, this);
					}
					catch (final Exception g)
					{
						throw new SQLException("Failed to obtain result set metadata from the server");
					}
					this.updateCount = -1;
				}
				catch (Exception e)
				{
					if (e instanceof SQLException)
					{
						throw e;
					}
					e = receiveException();
					conn.rollback();
					if (result != null)
					{
						result.close();
					}
					throw new SQLException(e.getMessage());
				}

				return true;
			}
			catch (final Exception f)
			{
				if (f instanceof SQLException)
				{
					throw (SQLException)f;
				}

				conn.rollback();
				if (result != null)
				{
					result.close();
				}
				throw new SQLException("An error occurred while sending the request to the server.  The transaction will be rolled back.");
			}
		}
		else
		{
			try
			{
				if (conn.rs != null && !conn.rs.isClosed())
				{
					throw new SQLException("You must close the previous result set before executing a new statement");
				}

				sql = setParms(sql);
				final byte[] command = "EXECUTEU        ".getBytes(StandardCharsets.UTF_8);
				final byte[] statement = sql.getBytes(StandardCharsets.UTF_8);
				final byte[] length = intToBytes(statement.length);
				conn.out.write(command);
				conn.out.write(length);
				conn.out.write(statement);
				conn.out.flush();
				if (!conn.openTransaction)
				{
					conn.openTransaction = true;
				}
				conn.txIsReadOnly = false;

				if (!conn.delayDML)
				{
					try
					{
						this.getConfirmation();
						int retval;
						try
						{
							retval = receiveInt();
						}
						catch (final Exception g)
						{
							throw new SQLException("Failed to retrieve response from server");
						}
						this.updateCount = retval;
						if (conn.autoCommit)
						{
							conn.commit();
						}
						return false;
					}
					catch (Exception e)
					{
						if (e instanceof SQLException)
						{
							throw e;
						}
						e = receiveException();
						conn.rollback();
						if (result != null)
						{
							result.close();
						}
						throw new SQLException(e.getMessage());
					}
				}

				return false;
			}
			catch (final Exception f)
			{
				if (f instanceof SQLException)
				{
					throw (SQLException)f;
				}

				conn.rollback();
				if (result != null)
				{
					result.close();
				}
				throw new SQLException("An error occurred while sending the request to the server.  The transaction will be rolled back.");
			}
		}
	}

	@Override
	public boolean execute(final String sql, final int autoGeneratedKeys) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean execute(final String sql, final int[] columnIndexes) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean execute(final String sql, final String[] columnNames) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int[] executeBatch() throws SQLException
	{
		if (closed)
		{
			throw new SQLException("executeBatch() called on closed statement");
		}

		boolean wasAutoCommit = false;
		if (conn.autoCommit)
		{
			conn.autoCommit = false;
			wasAutoCommit = true;
		}

		final int[] retval = new int[batch.size()];
		final int i = 0;
		for (final String sql : batch)
		{
			try
			{
				retval[i] = executeUpdate(sql);
			}
			catch (final Exception e)
			{
				throw new BatchUpdateException(e);
			}
		}

		if (wasAutoCommit)
		{
			conn.autoCommit = true;
			conn.commit();
		}

		return retval;
	}

	@Override
	public ResultSet executeQuery(String sql) throws SQLException
	{
		try
		{
			if (conn.rs != null && !conn.rs.isClosed())
			{
				throw new SQLException("A connection can only have 1 open result set at a time");
			}

			sql = setParms(sql);
			final byte[] command = "EXECUTEQ        ".getBytes(StandardCharsets.UTF_8);
			final byte[] statement = sql.getBytes(StandardCharsets.UTF_8);
			final byte[] length = intToBytes(statement.length);
			conn.out.write(command);
			conn.out.write(length);
			conn.out.write(statement);
			conn.out.flush();
			if (!conn.openTransaction)
			{
				conn.openTransaction = true;
			}
			try
			{
				this.getConfirmation();
				try
				{
					result = conn.rs = new HRDBMSResultSet(conn, fetchSize, this);
				}
				catch (final Exception g)
				{
					throw new SQLException("Failed to obtain result set metadata from the server");
				}
				this.updateCount = -1;
			}
			catch (Exception e)
			{
				if (e instanceof SQLException)
				{
					throw e;
				}
				e = receiveException();
				conn.rollback();
				if (result != null)
				{
					result.close();
				}
				throw new SQLException(e.getMessage());
			}

			return result;
		}
		catch (final Exception f)
		{
			if (f instanceof SQLException)
			{
				throw (SQLException)f;
			}

			conn.rollback();
			if (result != null)
			{
				result.close();
			}
			throw new SQLException("An error occurred while sending the request to the server.  The transaction will be rolled back.");
		}
	}

	@Override
	public int executeUpdate(String sql) throws SQLException
	{
		try
		{
			if (conn.rs != null && !conn.rs.isClosed())
			{
				throw new SQLException("You must close the previous result set before executing a new statement");
			}

			sql = setParms(sql);
			final byte[] command = "EXECUTEU        ".getBytes(StandardCharsets.UTF_8);
			final byte[] statement = sql.getBytes(StandardCharsets.UTF_8);
			final byte[] length = intToBytes(statement.length);
			conn.out.write(command);
			conn.out.write(length);
			conn.out.write(statement);
			conn.out.flush();
			if (!conn.openTransaction)
			{
				conn.openTransaction = true;
			}
			conn.txIsReadOnly = false;

			if (!conn.delayDML)
			{
				try
				{
					this.getConfirmation();
					int retval;
					try
					{
						retval = receiveInt();
					}
					catch (final Exception g)
					{
						throw new SQLException("Failed to retrieve response from server");
					}
					this.updateCount = retval;
					if (conn.autoCommit)
					{
						conn.commit();
					}
					return retval;
				}
				catch (Exception e)
				{
					if (e instanceof SQLException)
					{
						throw e;
					}
					e = receiveException();
					conn.rollback();
					if (result != null)
					{
						result.close();
					}
					throw new SQLException(e.getMessage());
				}
			}

			return -1;
		}
		catch (final Exception f)
		{
			if (f instanceof SQLException)
			{
				throw (SQLException)f;
			}

			conn.rollback();
			if (result != null)
			{
				result.close();
			}
			throw new SQLException("An error occurred while sending the request to the server.  The transaction will be rolled back.");
		}
	}

	@Override
	public int executeUpdate(final String sql, final int autoGeneratedKeys) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int executeUpdate(final String sql, final int[] columnIndexes) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int executeUpdate(final String sql, final String[] columnNames) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	public Object get(final String name, final Object key) throws SQLException
	{
		try
		{
			final byte[] command = "GET             ".getBytes(StandardCharsets.UTF_8);
			final byte[] from = stringToBytes(name);
			conn.out.write(command);
			conn.out.write(from);
			final ObjectOutputStream objOut = new ObjectOutputStream(conn.out);
			objOut.writeObject(key);
			objOut.flush();
			conn.out.flush();

			try
			{
				this.getConfirmation();
				final ObjectInputStream objIn = new ObjectInputStream(conn.in);
				return objIn.readObject();
			}
			catch (Exception e)
			{
				e.printStackTrace(); // DEBUG
				if (e instanceof SQLException)
				{
					throw e;
				}
				e = receiveException();
				throw new SQLException(e.getMessage());
			}
		}
		catch (final Exception f)
		{
			if (f instanceof SQLException)
			{
				throw (SQLException)f;
			}

			throw new SQLException("An error occurred while sending the request to the server.  The transaction will be rolled back.");
		}
	}

	@Override
	public Connection getConnection() throws SQLException
	{
		if (closed)
		{
			throw new SQLException("getConnection() called on a closed statement");
		}

		return conn;
	}

	@Override
	public int getFetchDirection() throws SQLException
	{
		if (closed)
		{
			throw new SQLException("getFetchDirection() called on a closed statement");
		}

		return ResultSet.FETCH_FORWARD;
	}

	@Override
	public int getFetchSize() throws SQLException
	{
		if (closed)
		{
			throw new SQLException("getFetchSize() called on a closed statement");
		}

		return fetchSize;
	}

	@Override
	public ResultSet getGeneratedKeys() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getMaxFieldSize() throws SQLException
	{
		if (closed)
		{
			throw new SQLException("getMaxFieldSize() called on closed statement");
		}
		return 0;
	}

	@Override
	public int getMaxRows() throws SQLException
	{
		if (closed)
		{
			throw new SQLException("getMaxRows() called on closed statement");
		}
		return 0;
	}

	@Override
	public boolean getMoreResults() throws SQLException
	{
		if (closed)
		{
			throw new SQLException("getMoreResults() called on a closed statement");
		}

		if (result != null)
		{
			result.close();
			result = null;
		}

		return false;
	}

	@Override
	public boolean getMoreResults(final int current) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getQueryTimeout() throws SQLException
	{
		if (closed)
		{
			throw new SQLException("getQueryTimeout() called on closed statement");
		}
		return 0;
	}

	@Override
	public ResultSet getResultSet() throws SQLException
	{
		if (closed)
		{
			throw new SQLException("getResultSet() called on a closed statement");
		}

		return result;
	}

	@Override
	public int getResultSetConcurrency() throws SQLException
	{
		if (closed)
		{
			throw new SQLException("getResultSetConcurrency() called on a closed statement");
		}

		return ResultSet.CONCUR_READ_ONLY;
	}

	@Override
	public int getResultSetHoldability() throws SQLException
	{
		if (closed)
		{
			throw new SQLException("getResultSetHoldability() called on a closed statement");
		}

		return ResultSet.CLOSE_CURSORS_AT_COMMIT;
	}

	@Override
	public int getResultSetType() throws SQLException
	{
		if (closed)
		{
			throw new SQLException("getResultSetType() called on a closed statement");
		}

		return ResultSet.TYPE_FORWARD_ONLY;
	}

	@Override
	public int getUpdateCount() throws SQLException
	{
		if (closed)
		{
			throw new SQLException("getUpdateCount() called on a closed statement");
		}

		return updateCount;
	}

	@Override
	public SQLWarning getWarnings() throws SQLException
	{
		if (closed)
		{
			throw new SQLException("getWarnings() called on a closed statement");
		}

		return conn.getWarnings();
	}

	@Override
	public boolean isClosed() throws SQLException
	{
		return closed;
	}

	@Override
	public boolean isCloseOnCompletion() throws SQLException
	{
		if (closed)
		{
			throw new SQLException("isCloseOnCompletion() called on a closed statement");
		}

		return false;
	}

	@Override
	public boolean isPoolable() throws SQLException
	{
		if (closed)
		{
			throw new SQLException("isPoolable() called on a closed statement");
		}

		return true;
	}

	@Override
	public boolean isWrapperFor(final Class<?> iface) throws SQLException
	{
		return false;
	}

	public void put(final String name, final Object key, final Object value) throws SQLException
	{
		try
		{
			final byte[] command = "PUT             ".getBytes(StandardCharsets.UTF_8);
			final byte[] from = stringToBytes(name);
			conn.out.write(command);
			conn.out.write(from);
			final ObjectOutputStream objOut = new ObjectOutputStream(conn.out);
			objOut.writeObject(key);
			objOut.writeObject(value);
			objOut.flush();
			conn.out.flush();

			try
			{
				this.getConfirmation();
			}
			catch (Exception e)
			{
				if (e instanceof SQLException)
				{
					throw e;
				}
				e = receiveException();
				throw new SQLException(e.getMessage());
			}
		}
		catch (final Exception f)
		{
			if (f instanceof SQLException)
			{
				throw (SQLException)f;
			}

			throw new SQLException("An error occurred while sending the request to the server.  The transaction will be rolled back.");
		}
	}

	public void remove(final String name, final Object key) throws SQLException
	{
		try
		{
			final byte[] command = "REMOVE          ".getBytes(StandardCharsets.UTF_8);
			final byte[] from = stringToBytes(name);
			conn.out.write(command);
			conn.out.write(from);
			final ObjectOutputStream objOut = new ObjectOutputStream(conn.out);
			objOut.writeObject(key);
			objOut.flush();
			conn.out.flush();

			try
			{
				this.getConfirmation();
			}
			catch (Exception e)
			{
				e.printStackTrace(); // DEBUG
				if (e instanceof SQLException)
				{
					throw e;
				}
				e = receiveException();
				throw new SQLException(e.getMessage());
			}
		}
		catch (final Exception f)
		{
			if (f instanceof SQLException)
			{
				throw (SQLException)f;
			}

			throw new SQLException("An error occurred while sending the request to the server.  The transaction will be rolled back.");
		}
	}

	@Override
	public void setCursorName(final String name) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setEscapeProcessing(final boolean enable) throws SQLException
	{
		throw new SQLException("setEscapeProcessing() is not supported");
	}

	@Override
	public void setFetchDirection(final int direction) throws SQLException
	{
		if (closed)
		{
			throw new SQLException("setFetchDirection() called on a closed statement");
		}
	}

	@Override
	public void setFetchSize(final int rows) throws SQLException
	{
		if (closed)
		{
			throw new SQLException("setFetchSize() called on a closed statement");
		}

		if (rows <= 0)
		{
			throw new SQLException("Fetch size must be positive()");
		}

		fetchSize = rows;
	}

	@Override
	public void setMaxFieldSize(final int max) throws SQLException
	{
		throw new SQLException("setMaxFieldSize() is not supported");
	}

	@Override
	public void setMaxRows(final int max) throws SQLException
	{
		throw new SQLException("setMaxRows() is not supported");
	}

	@Override
	public void setPoolable(final boolean poolable) throws SQLException
	{
		if (closed)
		{
			throw new SQLException("setPoolable() called on a closed statement");
		}
	}

	@Override
	public void setQueryTimeout(final int seconds) throws SQLException
	{
		throw new SQLException("setQueryTimeout() is not supported");
	}

	@Override
	public <T> T unwrap(final Class<T> iface) throws SQLException
	{
		throw new SQLException("Unwrap() is not supported.");
	}

	private void getConfirmation() throws Exception
	{
		final byte[] inMsg = new byte[2];

		int count = 0;
		while (count < 2)
		{
			try
			{
				final int temp = conn.in.read(inMsg, count, 2 - count);
				if (temp == -1)
				{
					throw new Exception();
				}
				else
				{
					count += temp;
				}
			}
			catch (final Exception e)
			{
				throw new Exception();
			}
		}

		final String inStr = new String(inMsg, StandardCharsets.UTF_8);
		if (!inStr.equals("OK"))
		{
			throw new Exception();
		}
	}

	private void read(final byte[] bytes) throws Exception
	{
		int count = 0;
		final int size = bytes.length;
		while (count < size)
		{
			final int temp = conn.in.read(bytes, count, bytes.length - count);
			if (temp == -1)
			{
				throw new Exception();
			}
			else
			{
				count += temp;
			}
		}

		return;
	}

	private Exception receiveException() throws Exception
	{
		try
		{
			final byte[] length = new byte[4];
			read(length);
			final int len = bytesToInt(length);
			final byte[] text = new byte[len];
			read(text);
			final String txt = new String(text, StandardCharsets.UTF_8);
			return new Exception(txt);
		}
		catch (final Throwable e)
		{
			throw new Exception("An error occurred on the server, but an exception occurred while trying to retrieve the error message.  A rollback has been performed.");
		}
	}

	private int receiveInt() throws Exception
	{
		int count = 4;
		final byte[] data = new byte[4];
		while (count > 0)
		{
			final int temp = conn.in.read(data, 4 - count, count);
			if (temp == -1)
			{
				throw new Exception("Early EOF reading from socket");
			}

			count -= temp;
		}

		return bytesToInt(data);
	}

	private String setParms(final String in)
	{
		String out = "";
		int x = 0;
		int i = 0;
		boolean quoted = false;
		int quoteType = 0;
		final int size = in.length();
		while (i < size)
		{
			if ((in.charAt(i) != '\'' && in.charAt(i) != '"') || (in.charAt(i) == '\'' && quoteType == 2) || (in.charAt(i) == '"' && quoteType == 1))
			{
				if (!quoted)
				{
					if (in.charAt(i) == '?')
					{
						final Object parm = parms.get(x);
						if (parm == null)
						{
							out += "NULL";
						}
						else if (parm instanceof String)
						{
							out += ("'" + parm + "'");
						}
						else if (parm instanceof Date)
						{
							final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
							out += ("DATE(" + sdf.format((Date)parm) + ")");
						}
						else
						{
							out += parm;
						}

						x++;
					}
					else
					{
						out += in.charAt(i);
					}
				}
				else
				{
					out += in.charAt(i);
				}
			}
			else
			{
				if (quoteType == 0)
				{
					if (in.charAt(i) == '\'' && ((i + 1) == in.length() || in.charAt(i + 1) != '\''))
					{
						quoteType = 1;
						quoted = true;
						out += '\'';
					}
					else if (in.charAt(i) == '"' && ((i + 1) == in.length() || in.charAt(i + 1) != '"'))
					{
						quoteType = 2;
						quoted = true;
						out += '"';
					}
					else
					{
						out += in.charAt(i);
						out += in.charAt(i + 1);
						i++;
					}
				}
				else if (quoteType == 1)
				{
					if (in.charAt(i) == '\'' && ((i + 1) == in.length() || in.charAt(i + 1) != '\''))
					{
						quoteType = 0;
						quoted = false;
						out += '\'';
					}
					else if (in.charAt(i) == '"')
					{
						out += '"';
					}
					else
					{
						out += "\'\'";
						i++;
					}
				}
				else
				{
					if (in.charAt(i) == '"' && ((i + 1) == in.length() || in.charAt(i + 1) != '"'))
					{
						quoteType = 0;
						quoted = false;
						out += '"';
					}
					else if (in.charAt(i) == '\'')
					{
						out += '\'';
					}
					else
					{
						out += "\"\"";
						i++;
					}
				}
			}

			i++;
		}

		return out;
	}

	private byte[] stringToBytes(final String string)
	{
		byte[] data = null;
		try
		{
			// data = string.getBytes(StandardCharsets.UTF_8);
			final byte[] ba = new byte[string.length() << 2];
			final char[] value = (char[])unsafe.getObject(string, offset);
			final int blen = ((sun.nio.cs.ArrayEncoder)ce).encode(value, 0, value.length, ba);
			data = Arrays.copyOf(ba, blen);
		}
		catch (final Exception e)
		{
		}
		final byte[] len = intToBytes(data.length);
		final byte[] retval = new byte[data.length + len.length];
		System.arraycopy(len, 0, retval, 0, len.length);
		System.arraycopy(data, 0, retval, len.length, data.length);
		return retval;
	}
}
