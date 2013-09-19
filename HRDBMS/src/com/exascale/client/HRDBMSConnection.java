package com.exascale.client;
import java.io.DataInputStream;
import java.io.DataOutputStream;
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

public class HRDBMSConnection implements Connection
{
	private DataInputStream in;
	private DataOutputStream out;
	private Socket sock;
	private String user;
	private String pwd;
	private String db;
	private boolean autoCommit = true;
	private boolean closed = false;
	private SQLWarning firstWarning = null;
	private boolean openTransaction = false;
	private boolean readOnly = false;
	private int isolationLevel = Connection.TRANSACTION_READ_COMMITTED;
	
	public HRDBMSConnection(Socket sock, String user, String pwd, String db) throws Exception
	{
		this.sock = sock;
		this.user = user;
		this.pwd = pwd;
		this.db = db;
		in = new DataInputStream(sock.getInputStream());
		out = new DataOutputStream(sock.getOutputStream());
		clientHandshake();
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		return false;
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		throw new SQLException(this + " is not a wrapper.");
	}

	@Override
	public void clearWarnings() throws SQLException {
		if (closed)
		{
			throw new SQLException("clearWarnings() called on closed connection");
		}
		
		firstWarning = null;
	}

	@Override
	public void close() throws SQLException {
		if (closed)
		{
			return;
		}
		
		closed = true;
		in.close();
		out.close();
	}

	@Override
	public void commit() throws SQLException {
		if (closed || autoCommit)
		{
			throw new SQLException("Invalid call to commit()");
		}
		
		sendCommit();
	}

	@Override
	public Array createArrayOf(String arg0, Object[] arg1) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Blob createBlob() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Clob createClob() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public NClob createNClob() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public SQLXML createSQLXML() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Statement createStatement() throws SQLException {
		if (closed)
		{
			throw new SQLException("createStatement() called on closed connection");
		}
		
		return new HRDBMSStatement(this);
	}

	@Override
	public Statement createStatement(int arg0, int arg1) throws SQLException {
		if (closed)
		{
			throw new SQLException("createStatement() called on closed connection");
		}
		
		return new HRDBMSStatement(this, arg0, arg1);
	}

	@Override
	public Statement createStatement(int arg0, int arg1, int arg2)
			throws SQLException {
		if (closed)
		{
			throw new SQLException("createStatement() called on closed connection");
		}
		
		return new HRDBMSStatement(this, arg0, arg1, arg2);
	}

	@Override
	public Struct createStruct(String arg0, Object[] arg1) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean getAutoCommit() throws SQLException {
		if (closed)
		{
			throw new SQLException("getAutoCommit() called on closed connection");
		}
		return autoCommit;
	}

	@Override
	public String getCatalog() throws SQLException {
		if (closed)
		{
			throw new SQLException("getCatalog() called on closed connection");
		}
		return db;
	}

	@Override
	public Properties getClientInfo() throws SQLException {
		if (closed)
		{
			throw new SQLException("getClientInfo() called on closed connection");
		}
		
		return new Properties();
	}

	@Override
	public String getClientInfo(String arg0) throws SQLException {
		if (closed)
		{
			throw new SQLException("getClientInfo() called on closed connection");
		}
		
		return null;
	}

	@Override
	public int getHoldability() throws SQLException {
		if (closed)
		{
			throw new SQLException("getHoldability() called on closed connection");
		}
		
		return ResultSet.CLOSE_CURSORS_AT_COMMIT;
	}

	@Override
	public DatabaseMetaData getMetaData() throws SQLException {
		if (closed)
		{
			throw new SQLException("getHoldability() called on closed connection");
		}
		
		return new HRDBMSDatabaseMetaData();
	}

	@Override
	public int getTransactionIsolation() throws SQLException {
		if (closed)
		{
			throw new SQLException("getHoldability() called on closed connection");
		}
		
		return isolationLevel;
	}

	@Override
	public Map<String, Class<?>> getTypeMap() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		if (closed)
		{
			throw new SQLException("getWarnings() called on closed connection");
		}
		
		return firstWarning;
	}

	@Override
	public boolean isClosed() throws SQLException {
		return closed;
	}

	@Override
	public boolean isReadOnly() throws SQLException {
		if (closed)
		{
			throw new SQLException("getHoldability() called on closed connection");
		}
		
		return readOnly;
	}

	@Override
	public boolean isValid(int arg0) throws SQLException {
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
	public String nativeSQL(String arg0) throws SQLException {
		if (closed)
		{
			throw new SQLException("nativeSQL() called on closed connection");
		}
		
		return arg0;
	}

	@Override
	public CallableStatement prepareCall(String arg0) throws SQLException {
		if (closed)
		{
			throw new SQLException("prepareCall() called on closed connection");
		}
		
		return new HRDBMSCallableStatement(this, arg0);
	}

	@Override
	public CallableStatement prepareCall(String arg0, int arg1, int arg2)
			throws SQLException {
		if (closed)
		{
			throw new SQLException("prepareCall() called on closed connection");
		}
		
		return new HRDBMSCallableStatement(this, arg0, arg1, arg2);
	}

	@Override
	public CallableStatement prepareCall(String arg0, int arg1, int arg2,
			int arg3) throws SQLException {
		if (closed)
		{
			throw new SQLException("prepareCall() called on closed connection");
		}
		
		return new HRDBMSCallableStatement(this, arg0, arg1, arg2, arg3);
	}

	@Override
	public PreparedStatement prepareStatement(String arg0) throws SQLException {
		if (closed)
		{
			throw new SQLException("prepareStatement() called on closed connection");
		}
		
		return new HRDBMSPreparedStatement(this, arg0);
	}

	@Override
	public PreparedStatement prepareStatement(String arg0, int arg1)
			throws SQLException {
		if (closed)
		{
			throw new SQLException("prepareStatement() called on closed connection");
		}
		
		return new HRDBMSPreparedStatement(this, arg0, arg1);
	}

	@Override
	public PreparedStatement prepareStatement(String arg0, int[] arg1)
			throws SQLException {
		if (closed)
		{
			throw new SQLException("prepareStatement() called on closed connection");
		}
		
		return new HRDBMSPreparedStatement(this, arg0, arg1);
	}

	@Override
	public PreparedStatement prepareStatement(String arg0, String[] arg1)
			throws SQLException {
		if (closed)
		{
			throw new SQLException("prepareStatement() called on closed connection");
		}
		
		return new HRDBMSPreparedStatement(this, arg0, arg1);
	}

	@Override
	public PreparedStatement prepareStatement(String arg0, int arg1, int arg2)
			throws SQLException {
		if (closed)
		{
			throw new SQLException("prepareStatement() called on closed connection");
		}
		
		return new HRDBMSPreparedStatement(this, arg0, arg1, arg2);
	}

	@Override
	public PreparedStatement prepareStatement(String arg0, int arg1, int arg2,
			int arg3) throws SQLException {
		if (closed)
		{
			throw new SQLException("prepareStatement() called on closed connection");
		}
		
		return new HRDBMSPreparedStatement(this, arg0, arg1, arg2, arg3);
	}

	@Override
	public void releaseSavepoint(Savepoint arg0) throws SQLException {
		throw new SQLFeatureNotSupportedException();
		
	}

	@Override
	public void rollback() throws SQLException {
		if (autoCommit || closed)
		{
			throw new SQLException("Invalid call to rollback()");
		}
		
		sendRollback();
	}

	@Override
	public void rollback(Savepoint arg0) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setAutoCommit(boolean arg0) throws SQLException {
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
		}
		
		autoCommit = arg0;
	}

	@Override
	public void setCatalog(String arg0) throws SQLException {
	}

	@Override
	public void setClientInfo(Properties arg0) throws SQLClientInfoException {
		throw new SQLClientInfoException();
	}

	@Override
	public void setClientInfo(String arg0, String arg1)
			throws SQLClientInfoException {
		throw new SQLClientInfoException();
	}

	@Override
	public void setHoldability(int arg0) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setReadOnly(boolean arg0) throws SQLException {
		if (closed || openTransaction)
		{
			throw new SQLException("Invalid call to setReadOnly()");
		}
		
		readOnly = arg0;
	}

	@Override
	public Savepoint setSavepoint() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Savepoint setSavepoint(String arg0) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setTransactionIsolation(int arg0) throws SQLException {
		if (closed)
		{
			throw new SQLException("setTransactionIsolation() called on closed connection");
		}
		
		if (arg0 == Connection.TRANSACTION_READ_UNCOMMITTED || arg0 == Connection.TRANSACTION_READ_COMMITTED || arg0 == Connection.TRANSACTION_REPEATABLE_READ || arg0 == Connection.TRANSACTION_SERIALIZABLE)
		{
			isolationLevel = arg0;
		}
		else
		{
			throw new SQLException("Invalid isolation level in call to setTransactionIsolation()");
		}
	}

	@Override
	public void setTypeMap(Map<String, Class<?>> arg0) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}
}