package com.exascale.client;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.net.Socket;
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
import com.exascale.compression.CompressedSocket;
import com.exascale.managers.HRDBMSWorker;

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
	int portNum;

	public HRDBMSConnection(Socket sock, String user, String pwd, int portNum) throws Exception
	{
		this.sock = sock;
		this.portNum = portNum;
		in = new BufferedInputStream(sock.getInputStream());
		out = new BufferedOutputStream(sock.getOutputStream());
		try
		{
			clientHandshake();
		}
		catch(Exception e)
		{
			e.printStackTrace();
			throw new SQLException("The handshake between the client and the server failed!");
		}
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
		if (closed || autoCommit)
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
	public Array createArrayOf(String arg0, Object[] arg1) throws SQLException
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
	public Statement createStatement(int arg0, int arg1) throws SQLException
	{
		if (closed)
		{
			throw new SQLException("createStatement() called on closed connection");
		}

		return new HRDBMSStatement(this, arg0, arg1);
	}

	@Override
	public Statement createStatement(int arg0, int arg1, int arg2) throws SQLException
	{
		if (closed)
		{
			throw new SQLException("createStatement() called on closed connection");
		}

		return new HRDBMSStatement(this, arg0, arg1, arg2);
	}

	@Override
	public Struct createStruct(String arg0, Object[] arg1) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
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
	public String getClientInfo(String arg0) throws SQLException
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
	public boolean isValid(int arg0) throws SQLException
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
	public boolean isWrapperFor(Class<?> iface) throws SQLException
	{
		return false;
	}

	@Override
	public String nativeSQL(String arg0) throws SQLException
	{
		if (closed)
		{
			throw new SQLException("nativeSQL() called on closed connection");
		}

		return arg0;
	}

	@Override
	public CallableStatement prepareCall(String arg0) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public CallableStatement prepareCall(String arg0, int arg1, int arg2) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public CallableStatement prepareCall(String arg0, int arg1, int arg2, int arg3) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public PreparedStatement prepareStatement(String arg0) throws SQLException
	{
		if (closed)
		{
			throw new SQLException("prepareStatement() called on closed connection");
		}

		return new HRDBMSPreparedStatement(this, arg0);
	}

	@Override
	public PreparedStatement prepareStatement(String arg0, int arg1) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public PreparedStatement prepareStatement(String arg0, int arg1, int arg2) throws SQLException
	{
		if (closed)
		{
			throw new SQLException("prepareStatement() called on closed connection");
		}

		return new HRDBMSPreparedStatement(this, arg0, arg1, arg2);
	}

	@Override
	public PreparedStatement prepareStatement(String arg0, int arg1, int arg2, int arg3) throws SQLException
	{
		if (closed)
		{
			throw new SQLException("prepareStatement() called on closed connection");
		}

		return new HRDBMSPreparedStatement(this, arg0, arg1, arg2, arg3);
	}

	@Override
	public PreparedStatement prepareStatement(String arg0, int[] arg1) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public PreparedStatement prepareStatement(String arg0, String[] arg1) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void releaseSavepoint(Savepoint arg0) throws SQLException
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
	public void rollback(Savepoint arg0) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setAutoCommit(boolean arg0) throws SQLException
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
	public void setCatalog(String arg0) throws SQLException
	{
	}

	@Override
	public void setClientInfo(Properties arg0) throws SQLClientInfoException
	{
	}

	@Override
	public void setClientInfo(String arg0, String arg1) throws SQLClientInfoException
	{
	}

	@Override
	public void setHoldability(int arg0) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setReadOnly(boolean arg0) throws SQLException
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
	public Savepoint setSavepoint(String arg0) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setTransactionIsolation(int arg0) throws SQLException
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
	public void setTypeMap(Map<String, Class<?>> arg0) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException
	{
		throw new SQLException(this + " is not a wrapper.");
	}

	@Override
	public void setSchema(String schema) throws SQLException
	{
		if (closed)
		{
			throw new SQLException("setTransactionIsolation() called on closed connection");
		}
		
		sendSetSchema(schema);
	}

	@Override
	public String getSchema() throws SQLException
	{
		return getSchemaFromServer();
	}

	@Override
	public void abort(Executor executor) throws SQLException
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
	public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getNetworkTimeout() throws SQLException
	{
		return 0;
	}
	
	private void sendClose()
	{
		try
		{
			byte[] outMsg = "CLOSE           ".getBytes("UTF-8");
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
		catch(Exception e)
		{}
		this.openTransaction = false;
	}
	
	private void sendCommit() throws SQLException
	{
		try
		{
			byte[] outMsg = "COMMIT          ".getBytes("UTF-8");
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
		catch(Exception e)
		{
			sendRollback();
			this.openTransaction = false;
			throw new SQLException("Commit failed! A rollback was performed.");
		}
		
		this.openTransaction = false;
	}
	
	private void sendRollback() throws SQLException
	{
		try
		{
			byte[] outMsg = "ROLLBACK        ".getBytes("UTF-8");
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
		catch(Exception e)
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
	
	private void clientHandshake() throws Exception
	{
		byte[] outMsg = "CLIENT          ".getBytes("UTF-8");
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
				int temp = in.read(inMsg, count, 2 - count);
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
		if (inStr.equals("OK"))
		{}
		else if (inStr.equals("RD"))
		{
			//workload balancing redirect
			count = 0;
			inMsg = new byte[4];
			while (count < 4)
			{
				try
				{
					int temp = in.read(inMsg, count, 4 - count);
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
			
			int length = bytesToInt(inMsg);
			
			count = 0;
			inMsg = new byte[length];
			while (count < length)
			{
				try
				{
					int temp = in.read(inMsg, count, length - count);
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
			
			String newHost = new String(inMsg, "UTF-8");
			in.close();
			out.close();
			sock.close();
			
			this.sock = new CompressedSocket(newHost, portNum);
			in = new BufferedInputStream(sock.getInputStream());
			out = new BufferedOutputStream(sock.getOutputStream());
			try
			{
				clientHandshake2();
			}
			catch(Exception e)
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
		byte[] outMsg = "CLIENT2         ".getBytes("UTF-8");
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
		byte[] inMsg = new byte[2];
		
		int count = 0;
		while (count < 2)
		{
			try
			{
				int temp = in.read(inMsg, count, 2 - count);
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
	
	private void sendIsolationLevel(int level)
	{
		try
		{
			byte[] outMsg = "ISOLATIO            ".getBytes("UTF-8");
			outMsg[8] = 0;
			outMsg[9] = 0;
			outMsg[10] = 0;
			outMsg[11] = 0;
			outMsg[12] = 0;
			outMsg[13] = 0;
			outMsg[14] = 0;
			outMsg[15] = 0;
			//append integer
			byte[] bInt = intToBytes(level);
			outMsg[16] = bInt[0];
			outMsg[17] = bInt[1];
			outMsg[18] = bInt[2];
			outMsg[19] = bInt[3];
			out.write(outMsg);
			out.flush();
		}
		catch(Exception e)
		{	
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
	
	private boolean testConnection()
	{
		try
		{
			byte[] outMsg = "TEST            ".getBytes("UTF-8");
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
		catch(Exception e)
		{	
			return false;
		}
		
		return true;
	}
	
	private void sendSetSchema(String schema) throws SQLException
	{
		try
		{
			byte[] outMsg = "SETSCHMA        ".getBytes("UTF-8");
			outMsg[8] = 0;
			outMsg[9] = 0;
			outMsg[10] = 0;
			outMsg[11] = 0;
			outMsg[12] = 0;
			outMsg[13] = 0;
			outMsg[14] = 0;
			outMsg[15] = 0;
			byte[] string = schema.getBytes("UTF-8");
			byte[] length = intToBytes(string.length);
			byte[] outMsg2 = new byte[20+string.length];
			System.arraycopy(outMsg, 0, outMsg2, 0, 16);
			System.arraycopy(length, 0, outMsg2, 16, 4);
			System.arraycopy(string, 0, outMsg2, 20, string.length);
			out.write(outMsg2);
			out.flush();
			getConfirmation();
		}
		catch(Exception e)
		{	
			throw new SQLException("SetSchema() failed.  Reason: " + e.getMessage());
		}
	}
	
	private String getSchemaFromServer() throws SQLException
	{
		try
		{
			byte[] outMsg = "GETSCHMA        ".getBytes("UTF-8");
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
		catch(Exception e)
		{	
			throw new SQLException("GetSchema() failed.  Reason: " + e.getMessage());
		}
	}
	
	private int bytesToInt(byte[] val)
	{
		final int ret = java.nio.ByteBuffer.wrap(val).getInt();
		return ret;
	}
	
	private String getString() throws Exception
	{
		byte[] inMsg = new byte[4];
		
		int count = 0;
		while (count < 4)
		{
			try
			{
				int temp = in.read(inMsg, count, 4 - count);
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
		
		int length = this.bytesToInt(inMsg);
		
		inMsg = new byte[length];
		count = 0;
		while (count < length)
		{
			try
			{
				int temp = in.read(inMsg, count, length - count);
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
		return inStr;
	}
}