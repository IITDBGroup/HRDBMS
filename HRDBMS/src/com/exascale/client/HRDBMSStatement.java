package com.exascale.client;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import com.exascale.managers.HRDBMSWorker;

public class HRDBMSStatement implements Statement
{
	protected boolean closed = false;
	private HRDBMSConnection conn;
	private HRDBMSResultSet result;
	private int updateCount = -1;
	private int fetchSize = 100;
	protected ArrayList<Object> parms = new ArrayList<Object>();
	private ArrayList<String> batch = new ArrayList<String>();
	
	public HRDBMSStatement(HRDBMSConnection conn)
	{
		this.conn = conn;
	}
	
	public HRDBMSStatement(HRDBMSConnection conn, int type, int concur) throws SQLFeatureNotSupportedException
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
	
	public HRDBMSStatement(HRDBMSConnection conn, int type, int concur, int hold) throws SQLFeatureNotSupportedException
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
	
	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException
	{
		throw new SQLException("Unwrap() is not supported.");
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException
	{
		return false;
	}
	
	private String setParms(String in)
	{
		String out = "";
		int x = 0;
		int i = 0;
		boolean quoted = false;
		int quoteType = 0;
		while (i < in.length())
		{
			if ((in.charAt(i) != '\'' && in.charAt(i) != '"') || (in.charAt(i) == '\'' && quoteType == 2) || (in.charAt(i) == '"' && quoteType == 1))
			{
				if (!quoted)
				{
					if (in.charAt(i) == '?')
					{
						Object parm = parms.get(x);
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
							SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
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
			byte[] command = "EXECUTEQ".getBytes("UTF-8");
			byte[] statement = sql.getBytes("UTF-8");
			byte[] length = intToBytes(statement.length);
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
				catch(Exception g)
				{
					throw new SQLException("Failed to obtain result set metadata from the server");
				}
				this.updateCount = -1;
			}
			catch(Exception e)
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
		catch(Exception f)
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
	
	private int bytesToInt(byte[] val)
	{
		final int ret = java.nio.ByteBuffer.wrap(val).getInt();
		return ret;
	}
	
	private Exception receiveException() throws Exception
	{
		try
		{
			byte[] length = new byte[4];
			read(length);
			int len = bytesToInt(length);
			byte[] text = new byte[len];
			read(text);
			String txt = new String(text, "UTF-8");
			return new Exception(txt);
		}
		catch(Exception e)
		{
			throw new Exception("An error occurred on the server, but an exception occurred while trying to retrieve the error message.  A rollback has been performed.");
		}
	}
	
	private void read(byte[] bytes) throws Exception
	{
		int count = 0;
		while (count < bytes.length)
		{
			int temp = conn.in.read(bytes, count, bytes.length - count);
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
	
	private void getConfirmation() throws Exception
	{
		byte[] inMsg = new byte[2];
		
		int count = 0;
		while (count < 2)
		{
			try
			{
				int temp = conn.in.read(inMsg, count, 2 - count);
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
		
		String inStr = new String(inMsg, "UTF-8");
		if (!inStr.equals("OK"))
		{
			throw new Exception();
		}
	}
	
	private static byte[] intToBytes(int val)
	{
		final byte[] buff = new byte[4];
		buff[0] = (byte)(val >> 24);
		buff[1] = (byte)((val & 0x00FF0000) >> 16);
		buff[2] = (byte)((val & 0x0000FF00) >> 8);
		buff[3] = (byte)((val & 0x000000FF));
		return buff;
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
			byte[] command = "EXECUTEU".getBytes("UTF-8");
			byte[] statement = sql.getBytes("UTF-8");
			byte[] length = intToBytes(statement.length);
			conn.out.write(command);
			conn.out.write(length);
			conn.out.write(statement);
			conn.out.flush();
			if (!conn.openTransaction)
			{
				conn.openTransaction = true;
			}
			conn.txIsReadOnly = false;
			try
			{
				this.getConfirmation();
				int retval;
				try
				{
					retval = receiveInt();
				}
				catch(Exception g)
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
			catch(Exception e)
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
		catch(Exception f)
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
	public void close() throws SQLException
	{
		if (this.result != null)
		{
			result.close();
		}
		
		closed = true;
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
	public void setMaxFieldSize(int max) throws SQLException
	{
		throw new SQLException("setMaxFieldSize() is not supported");
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
	public void setMaxRows(int max) throws SQLException
	{
		throw new SQLException("setMaxRows() is not supported");
	}

	@Override
	public void setEscapeProcessing(boolean enable) throws SQLException
	{
		throw new SQLException("setEscapeProcessing() is not supported");
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
	public void setQueryTimeout(int seconds) throws SQLException
	{
		throw new SQLException("setQueryTimeout() is not supported");
	}

	@Override
	public void cancel() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
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
	public void clearWarnings() throws SQLException
	{
		if (closed)
		{
			throw new SQLException("clearWarnings() called on a closed statement");
		}
		
		conn.firstWarning = null;
	}

	@Override
	public void setCursorName(String name) throws SQLException
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
				byte[] command = "EXECUTEQ".getBytes("UTF-8");
				byte[] statement = sql.getBytes("UTF-8");
				byte[] length = intToBytes(statement.length);
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
					catch(Exception g)
					{
						throw new SQLException("Failed to obtain result set metadata from the server");
					}
					this.updateCount = -1;
				}
				catch(Exception e)
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
			catch(Exception f)
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
				byte[] command = "EXECUTEU".getBytes("UTF-8");
				byte[] statement = sql.getBytes("UTF-8");
				byte[] length = intToBytes(statement.length);
				conn.out.write(command);
				conn.out.write(length);
				conn.out.write(statement);
				conn.out.flush();
				if (!conn.openTransaction)
				{
					conn.openTransaction = true;
				}
				conn.txIsReadOnly = false;
				try
				{
					this.getConfirmation();
					int retval;
					try
					{
						retval = receiveInt();
					}
					catch(Exception g)
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
				catch(Exception e)
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
			catch(Exception f)
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
	public ResultSet getResultSet() throws SQLException
	{
		if (closed)
		{
			throw new SQLException("getResultSet() called on a closed statement");
		}
		
		return result;
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
	public void setFetchDirection(int direction) throws SQLException
	{
		if (closed)
		{
			throw new SQLException("setFetchDirection() called on a closed statement");
		}
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
	public void setFetchSize(int rows) throws SQLException
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
	public int getFetchSize() throws SQLException
	{
		if (closed)
		{
			throw new SQLException("getFetchSize() called on a closed statement");
		}
		
		return fetchSize;
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
	public int getResultSetType() throws SQLException
	{
		if (closed)
		{
			throw new SQLException("getResultSetType() called on a closed statement");
		}
		
		return ResultSet.TYPE_FORWARD_ONLY;
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
	public void clearBatch() throws SQLException
	{
		if (closed)
		{
			throw new SQLException("clearBatch() called on a closed statement");
		}
		
		batch.clear();
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
		
		int[] retval = new int[batch.size()];
		int i = 0;
		for (String sql : batch)
		{
			try
			{
				retval[i] = executeUpdate(sql);
			}
			catch(Exception e)
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
	public Connection getConnection() throws SQLException
	{
		if (closed)
		{
			throw new SQLException("getConnection() called on a closed statement");
		}
		
		return conn;
	}

	@Override
	public boolean getMoreResults(int current) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public ResultSet getGeneratedKeys() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int executeUpdate(String sql, int[] columnIndexes) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int executeUpdate(String sql, String[] columnNames) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean execute(String sql, int autoGeneratedKeys) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean execute(String sql, int[] columnIndexes) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean execute(String sql, String[] columnNames) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
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
	public boolean isClosed() throws SQLException
	{
		return closed;
	}

	@Override
	public void setPoolable(boolean poolable) throws SQLException
	{
		if (closed)
		{
			throw new SQLException("setPoolable() called on a closed statement");
		}
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
	public void closeOnCompletion() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
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

	private int receiveInt() throws Exception
	{
		int count = 4;
		byte[] data = new byte[4];
		while (count > 0)
		{
			int temp = conn.in.read(data, 4-count, count);
			if (temp == -1)
			{
				throw new Exception("Early EOF reading from socket");
			}
				
			count -= temp;
		}
		
		return bytesToInt(data);
	}
}
