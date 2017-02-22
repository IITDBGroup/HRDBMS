package com.exascale.client;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

public class HRDBMSConnection implements Connection
{
	protected BufferedInputStream in;
	protected BufferedOutputStream out;
	protected boolean autoCommit = true;
	private boolean closed = false;
	protected SQLWarning firstWarning = null;
	protected boolean openTransaction = false;
	private boolean readOnly = false;
	private int isolationLevel = Connection.TRANSACTION_READ_COMMITTED;
	private Socket sock;
	protected HRDBMSResultSet rs;
	protected boolean txIsReadOnly = true;
	protected boolean delayDML = false;
	int portNum;

	public HRDBMSConnection(final Socket sock, final String user, final String pwd, final int portNum, final String force) throws Exception
	{
		this.sock = sock;
		this.portNum = portNum;
		in = new BufferedInputStream(sock.getInputStream());
		out = new BufferedOutputStream(sock.getOutputStream());
		try
		{
			if (force.equals("TRUE"))
			{
				clientHandshake2();
			}
			else
			{
				clientHandshake();
			}
		}
		catch (final Exception e)
		{
			e.printStackTrace();
			throw new SQLException("The handshake between the client and the server failed!");
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
	public void abort(final Executor executor) throws SQLException
	{
		if (executor == null)
		{
			throw new SQLException("Abort() was called with a null executor");
		}

		if (closed)
		{
			return;
		}

		this.close();
	}

	@Override
	public void clearWarnings() throws SQLException
	{
		if (closed)
		{
			throw new SQLException("clearWarnings() called on closed connection");
		}

		firstWarning = null;
	}

	@Override
	public void close() throws SQLException
	{
		if (closed)
		{
			return;
		}

		closed = true;
		sendClose();
		try
		{
			in.close();
			out.close();
			sock.close();
		}
		catch (final Exception e)
		{
		}
	}

	@Override
	public void commit() throws SQLException
	{
		if (closed)
		{
			throw new SQLException("Invalid call to commit()");
		}

		sendCommit();

		if (rs != null)
		{
			rs.close();
		}

		txIsReadOnly = true;
	}

	@Override
	public Array createArrayOf(final String arg0, final Object[] arg1) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Blob createBlob() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Clob createClob() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public NClob createNClob() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public SQLXML createSQLXML() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Statement createStatement() throws SQLException
	{
		if (closed)
		{
			throw new SQLException("createStatement() called on closed connection");
		}

		return new HRDBMSStatement(this);
	}

	@Override
	public Statement createStatement(final int arg0, final int arg1) throws SQLException
	{
		if (closed)
		{
			throw new SQLException("createStatement() called on closed connection");
		}

		return new HRDBMSStatement(this, arg0, arg1);
	}

	@Override
	public Statement createStatement(final int arg0, final int arg1, final int arg2) throws SQLException
	{
		if (closed)
		{
			throw new SQLException("createStatement() called on closed connection");
		}

		return new HRDBMSStatement(this, arg0, arg1, arg2);
	}

	@Override
	public Struct createStruct(final String arg0, final Object[] arg1) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	public void doPacing() throws SQLException
	{
		if (closed)
		{
			throw new SQLException("doPacing() called on closed connection");
		}

		if (!delayDML)
		{
			throw new SQLException("Can't do pacing without delayed DML responses");
		}

		sendPacing();
	}

	@Override
	public boolean getAutoCommit() throws SQLException
	{
		if (closed)
		{
			throw new SQLException("getAutoCommit() called on closed connection");
		}
		return autoCommit;
	}

	@Override
	public String getCatalog() throws SQLException
	{
		if (closed)
		{
			throw new SQLException("getCatalog() called on closed connection");
		}
		return "HRDBMS";
	}

	@Override
	public Properties getClientInfo() throws SQLException
	{
		if (closed)
		{
			throw new SQLException("getClientInfo() called on closed connection");
		}

		return new Properties();
	}

	@Override
	public String getClientInfo(final String arg0) throws SQLException
	{
		if (closed)
		{
			throw new SQLException("getClientInfo() called on closed connection");
		}

		return null;
	}

	@Override
	public int getHoldability() throws SQLException
	{
		if (closed)
		{
			throw new SQLException("getHoldability() called on closed connection");
		}

		return ResultSet.CLOSE_CURSORS_AT_COMMIT;
	}

	@Override
	public DatabaseMetaData getMetaData() throws SQLException
	{
		if (closed)
		{
			throw new SQLException("getMetaData() called on closed connection");
		}

		return new HRDBMSDatabaseMetaData(this);
	}

	@Override
	public int getNetworkTimeout() throws SQLException
	{
		return 0;
	}

	@Override
	public String getSchema() throws SQLException
	{
		return getSchemaFromServer();
	}

	@Override
	public int getTransactionIsolation() throws SQLException
	{
		if (closed)
		{
			throw new SQLException("getTransactionIsolation() called on closed connection");
		}

		return isolationLevel;
	}

	@Override
	public Map<String, Class<?>> getTypeMap() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public SQLWarning getWarnings() throws SQLException
	{
		if (closed)
		{
			throw new SQLException("getWarnings() called on closed connection");
		}

		return firstWarning;
	}

	@Override
	public boolean isClosed() throws SQLException
	{
		return closed;
	}

	@Override
	public boolean isReadOnly() throws SQLException
	{
		if (closed)
		{
			throw new SQLException("isReadOnly() called on closed connection");
		}

		return readOnly;
	}

	@Override
	public boolean isValid(final int arg0) throws SQLException
	{
		if (arg0 < 0)
		{
			throw new SQLException("isValid() was called with a negative timeout");
		}

		if (closed)
		{
			return false;
		}

		return testConnection();
	}

	@Override
	public boolean isWrapperFor(final Class<?> iface) throws SQLException
	{
		return false;
	}

	@Override
	public String nativeSQL(final String arg0) throws SQLException
	{
		if (closed)
		{
			throw new SQLException("nativeSQL() called on closed connection");
		}

		return arg0;
	}

	@Override
	public CallableStatement prepareCall(final String arg0) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public CallableStatement prepareCall(final String arg0, final int arg1, final int arg2) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public CallableStatement prepareCall(final String arg0, final int arg1, final int arg2, final int arg3) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public PreparedStatement prepareStatement(final String arg0) throws SQLException
	{
		if (closed)
		{
			throw new SQLException("prepareStatement() called on closed connection");
		}

		return new HRDBMSPreparedStatement(this, arg0);
	}

	@Override
	public PreparedStatement prepareStatement(final String arg0, final int arg1) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public PreparedStatement prepareStatement(final String arg0, final int arg1, final int arg2) throws SQLException
	{
		if (closed)
		{
			throw new SQLException("prepareStatement() called on closed connection");
		}

		return new HRDBMSPreparedStatement(this, arg0, arg1, arg2);
	}

	@Override
	public PreparedStatement prepareStatement(final String arg0, final int arg1, final int arg2, final int arg3) throws SQLException
	{
		if (closed)
		{
			throw new SQLException("prepareStatement() called on closed connection");
		}

		return new HRDBMSPreparedStatement(this, arg0, arg1, arg2, arg3);
	}

	@Override
	public PreparedStatement prepareStatement(final String arg0, final int[] arg1) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public PreparedStatement prepareStatement(final String arg0, final String[] arg1) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void releaseSavepoint(final Savepoint arg0) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();

	}

	@Override
	public void rollback() throws SQLException
	{
		if (autoCommit || closed)
		{
			throw new SQLException("Invalid call to rollback()");
		}

		sendRollback();
		if (rs != null)
		{
			rs.close();
		}

		txIsReadOnly = true;
	}

	@Override
	public void rollback(final Savepoint arg0) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	public void saveDMLResponseTillCommit() throws SQLException
	{
		if (closed)
		{
			throw new SQLException("saveDMLResponseTillCommit() called on closed connection");
		}

		if (autoCommit)
		{
			throw new SQLException("Can't delay DML responses with auto-commit turned on");
		}

		delayDML = true;
		sendDelayDML();
	}

	@Override
	public void setAutoCommit(final boolean arg0) throws SQLException
	{
		if (autoCommit == arg0)
		{
			return;
		}

		if (closed)
		{
			throw new SQLException("setAutoCommit() called on closed connection");
		}

		if (openTransaction)
		{
			sendCommit();
			if (rs != null)
			{
				rs.close();
			}
		}

		autoCommit = arg0;
	}

	@Override
	public void setCatalog(final String arg0) throws SQLException
	{
	}

	@Override
	public void setClientInfo(final Properties arg0) throws SQLClientInfoException
	{
	}

	@Override
	public void setClientInfo(final String arg0, final String arg1) throws SQLClientInfoException
	{
	}

	@Override
	public void setHoldability(final int arg0) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setNetworkTimeout(final Executor executor, final int milliseconds) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setReadOnly(final boolean arg0) throws SQLException
	{
		if (closed || openTransaction)
		{
			throw new SQLException("Invalid call to setReadOnly()");
		}

		readOnly = arg0;
	}

	@Override
	public Savepoint setSavepoint() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Savepoint setSavepoint(final String arg0) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setSchema(final String schema) throws SQLException
	{
		if (closed)
		{
			throw new SQLException("setTransactionIsolation() called on closed connection");
		}

		sendSetSchema(schema);
	}

	@Override
	public void setTransactionIsolation(final int arg0) throws SQLException
	{
		if (closed)
		{
			throw new SQLException("setTransactionIsolation() called on closed connection");
		}

		if (arg0 == Connection.TRANSACTION_READ_UNCOMMITTED || arg0 == Connection.TRANSACTION_READ_COMMITTED || arg0 == Connection.TRANSACTION_REPEATABLE_READ || arg0 == Connection.TRANSACTION_SERIALIZABLE)
		{
			isolationLevel = arg0;
			sendIsolationLevel(arg0);
		}
		else
		{
			throw new SQLException("Invalid isolation level in call to setTransactionIsolation()");
		}
	}

	@Override
	public void setTypeMap(final Map<String, Class<?>> arg0) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public <T> T unwrap(final Class<T> iface) throws SQLException
	{
		throw new SQLException(this + " is not a wrapper.");
	}

	private void clientHandshake() throws Exception
	{
		final byte[] outMsg = "CLIENT          ".getBytes(StandardCharsets.UTF_8);
		outMsg[8] = 0;
		outMsg[9] = 0;
		outMsg[10] = 0;
		outMsg[11] = 0;
		outMsg[12] = 0;
		outMsg[13] = 0;
		outMsg[14] = 0;
		outMsg[15] = 0;
		out.write(outMsg);
		out.flush();
		byte[] inMsg = new byte[2];

		int count = 0;
		while (count < 2)
		{
			try
			{
				final int temp = in.read(inMsg, count, 2 - count);
				if (temp == -1)
				{
					throw new Exception();
				}

				count += temp;
			}
			catch (final Exception e)
			{
				throw new Exception();
			}
		}

		final String inStr = new String(inMsg, StandardCharsets.UTF_8);
		if (inStr.equals("OK"))
		{
		}
		else if (inStr.equals("RD"))
		{
			// workload balancing redirect
			count = 0;
			inMsg = new byte[4];
			while (count < 4)
			{
				try
				{
					final int temp = in.read(inMsg, count, 4 - count);
					if (temp == -1)
					{
						throw new Exception();
					}

					count += temp;
				}
				catch (final Exception e)
				{
					throw new Exception();
				}
			}

			final int length = bytesToInt(inMsg);

			count = 0;
			inMsg = new byte[length];
			while (count < length)
			{
				try
				{
					final int temp = in.read(inMsg, count, length - count);
					if (temp == -1)
					{
						throw new Exception();
					}

					count += temp;
				}
				catch (final Exception e)
				{
					throw new Exception();
				}
			}

			final String newHost = new String(inMsg, StandardCharsets.UTF_8);
			in.close();
			out.close();
			sock.close();

			this.sock = new Socket();
			this.sock.setReceiveBufferSize(4194304);
			this.sock.setSendBufferSize(4194304);
			this.sock.connect(new InetSocketAddress(newHost, portNum));
			in = new BufferedInputStream(sock.getInputStream());
			out = new BufferedOutputStream(sock.getOutputStream());
			try
			{
				clientHandshake2();
			}
			catch (final Exception e)
			{
				e.printStackTrace();
				throw new SQLException("The handshake between the client and the server failed!");
			}
		}
		else
		{
			throw new Exception();
		}
	}

	private void clientHandshake2() throws Exception
	{
		final byte[] outMsg = "CLIENT2         ".getBytes(StandardCharsets.UTF_8);
		outMsg[8] = 0;
		outMsg[9] = 0;
		outMsg[10] = 0;
		outMsg[11] = 0;
		outMsg[12] = 0;
		outMsg[13] = 0;
		outMsg[14] = 0;
		outMsg[15] = 0;
		out.write(outMsg);
		out.flush();
		getConfirmation();
	}

	private void getConfirmation() throws Exception
	{
		final byte[] inMsg = new byte[2];

		int count = 0;
		while (count < 2)
		{
			try
			{
				final int temp = in.read(inMsg, count, 2 - count);
				if (temp == -1)
				{
					throw new Exception();
				}

				count += temp;
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

	private String getSchemaFromServer() throws SQLException
	{
		try
		{
			final byte[] outMsg = "GETSCHMA        ".getBytes(StandardCharsets.UTF_8);
			outMsg[8] = 0;
			outMsg[9] = 0;
			outMsg[10] = 0;
			outMsg[11] = 0;
			outMsg[12] = 0;
			outMsg[13] = 0;
			outMsg[14] = 0;
			outMsg[15] = 0;
			out.write(outMsg);
			out.flush();
			return getString();
		}
		catch (final Exception e)
		{
			throw new SQLException("GetSchema() failed.  Reason: " + e.getMessage());
		}
	}

	private String getString() throws Exception
	{
		byte[] inMsg = new byte[4];

		int count = 0;
		while (count < 4)
		{
			try
			{
				final int temp = in.read(inMsg, count, 4 - count);
				if (temp == -1)
				{
					throw new Exception();
				}

				count += temp;
			}
			catch (final Exception e)
			{
				throw new Exception();
			}
		}

		final int length = bytesToInt(inMsg);

		inMsg = new byte[length];
		count = 0;
		while (count < length)
		{
			try
			{
				final int temp = in.read(inMsg, count, length - count);
				if (temp == -1)
				{
					throw new Exception();
				}

				count += temp;
			}
			catch (final Exception e)
			{
				throw new Exception();
			}
		}

		final String inStr = new String(inMsg, StandardCharsets.UTF_8);
		return inStr;
	}

	private void sendClose()
	{
		try
		{
			final byte[] outMsg = "CLOSE           ".getBytes(StandardCharsets.UTF_8);
			outMsg[8] = 0;
			outMsg[9] = 0;
			outMsg[10] = 0;
			outMsg[11] = 0;
			outMsg[12] = 0;
			outMsg[13] = 0;
			outMsg[14] = 0;
			outMsg[15] = 0;
			out.write(outMsg);
			out.flush();
		}
		catch (final Exception e)
		{
		}
		this.openTransaction = false;
	}

	private void sendCommit() throws SQLException
	{
		try
		{
			final byte[] outMsg = "COMMIT          ".getBytes(StandardCharsets.UTF_8);
			outMsg[8] = 0;
			outMsg[9] = 0;
			outMsg[10] = 0;
			outMsg[11] = 0;
			outMsg[12] = 0;
			outMsg[13] = 0;
			outMsg[14] = 0;
			outMsg[15] = 0;
			out.write(outMsg);
			out.flush();
			getConfirmation();
		}
		catch (final Exception e)
		{
			sendRollback();
			this.openTransaction = false;
			throw new SQLException("Commit failed! A rollback was performed.");
		}

		this.openTransaction = false;
	}

	private void sendDelayDML() throws SQLException
	{
		try
		{
			final byte[] outMsg = "DELAYDML        ".getBytes(StandardCharsets.UTF_8);
			outMsg[8] = 0;
			outMsg[9] = 0;
			outMsg[10] = 0;
			outMsg[11] = 0;
			outMsg[12] = 0;
			outMsg[13] = 0;
			outMsg[14] = 0;
			outMsg[15] = 0;
			out.write(outMsg);
			out.flush();
			getConfirmation();
		}
		catch (final Exception e)
		{
			throw new SQLException("Error sending request to server!");
		}
	}

	private void sendIsolationLevel(final int level)
	{
		try
		{
			final byte[] outMsg = "ISOLATIO            ".getBytes(StandardCharsets.UTF_8);
			outMsg[8] = 0;
			outMsg[9] = 0;
			outMsg[10] = 0;
			outMsg[11] = 0;
			outMsg[12] = 0;
			outMsg[13] = 0;
			outMsg[14] = 0;
			outMsg[15] = 0;
			// append integer
			final byte[] bInt = intToBytes(level);
			outMsg[16] = bInt[0];
			outMsg[17] = bInt[1];
			outMsg[18] = bInt[2];
			outMsg[19] = bInt[3];
			out.write(outMsg);
			out.flush();
		}
		catch (final Exception e)
		{
		}
	}

	private void sendPacing() throws SQLException
	{
		try
		{
			final byte[] outMsg = "PACING          ".getBytes(StandardCharsets.UTF_8);
			outMsg[8] = 0;
			outMsg[9] = 0;
			outMsg[10] = 0;
			outMsg[11] = 0;
			outMsg[12] = 0;
			outMsg[13] = 0;
			outMsg[14] = 0;
			outMsg[15] = 0;
			out.write(outMsg);
			out.flush();
			getConfirmation();
		}
		catch (final Exception e)
		{
			throw new SQLException("Error occurred as part of asynchronous transaction. The transacstion needs to be rolled back.", e);
		}
	}

	private void sendRollback() throws SQLException
	{
		try
		{
			final byte[] outMsg = "ROLLBACK        ".getBytes(StandardCharsets.UTF_8);
			outMsg[8] = 0;
			outMsg[9] = 0;
			outMsg[10] = 0;
			outMsg[11] = 0;
			outMsg[12] = 0;
			outMsg[13] = 0;
			outMsg[14] = 0;
			outMsg[15] = 0;
			out.write(outMsg);
			out.flush();
			getConfirmation();
			this.openTransaction = false;
		}
		catch (final Exception e)
		{
			this.openTransaction = false;
			closed = true;
			try
			{
				in.close();
				out.close();
				sock.close();
			}
			catch (final Exception f)
			{
			}

			throw new SQLException("Rollback failed!  Disconnecting from the server to force a rollback.");
		}
	}

	private void sendSetSchema(final String schema) throws SQLException
	{
		try
		{
			final byte[] outMsg = "SETSCHMA        ".getBytes(StandardCharsets.UTF_8);
			outMsg[8] = 0;
			outMsg[9] = 0;
			outMsg[10] = 0;
			outMsg[11] = 0;
			outMsg[12] = 0;
			outMsg[13] = 0;
			outMsg[14] = 0;
			outMsg[15] = 0;
			final byte[] string = schema.getBytes(StandardCharsets.UTF_8);
			final byte[] length = intToBytes(string.length);
			final byte[] outMsg2 = new byte[20 + string.length];
			System.arraycopy(outMsg, 0, outMsg2, 0, 16);
			System.arraycopy(length, 0, outMsg2, 16, 4);
			System.arraycopy(string, 0, outMsg2, 20, string.length);
			out.write(outMsg2);
			out.flush();
			getConfirmation();
		}
		catch (final Exception e)
		{
			throw new SQLException("SetSchema() failed.  Reason: " + e.getMessage());
		}
	}

	private boolean testConnection()
	{
		try
		{
			final byte[] outMsg = "TEST            ".getBytes(StandardCharsets.UTF_8);
			outMsg[8] = 0;
			outMsg[9] = 0;
			outMsg[10] = 0;
			outMsg[11] = 0;
			outMsg[12] = 0;
			outMsg[13] = 0;
			outMsg[14] = 0;
			outMsg[15] = 0;
			out.write(outMsg);
			out.flush();
			getConfirmation();
		}
		catch (final Exception e)
		{
			return false;
		}

		return true;
	}
}