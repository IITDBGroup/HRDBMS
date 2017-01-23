package com.exascale.client;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;

public class HRDBMSPreparedStatement extends HRDBMSStatement implements PreparedStatement
{
	private final String sql;

	public HRDBMSPreparedStatement(HRDBMSConnection conn, String sql)
	{
		super(conn);
		this.sql = sql;
	}

	public HRDBMSPreparedStatement(HRDBMSConnection conn, String sql, int arg1, int arg2) throws SQLFeatureNotSupportedException
	{
		super(conn, arg1, arg2);
		this.sql = sql;
	}

	public HRDBMSPreparedStatement(HRDBMSConnection conn, String sql, int arg1, int arg2, int arg3) throws SQLFeatureNotSupportedException
	{
		super(conn, arg1, arg2, arg3);
		this.sql = sql;
	}

	@Override
	public void addBatch() throws SQLException
	{
		addBatch(sql);
	}

	@Override
	public void clearParameters() throws SQLException
	{
		if (closed)
		{
			throw new SQLException("clearParameters() called on a closed statement");
		}

		parms.clear();
	}

	@Override
	public boolean execute() throws SQLException
	{
		return execute(sql);
	}

	@Override
	public ResultSet executeQuery() throws SQLException
	{
		return executeQuery(sql);
	}

	@Override
	public int executeUpdate() throws SQLException
	{
		return executeUpdate(sql);
	}

	@Override
	public ResultSetMetaData getMetaData() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public ParameterMetaData getParameterMetaData() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setArray(int parameterIndex, Array x) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException
	{
		if (closed)
		{
			throw new SQLException("setBigDecimal() called on a closed statement");
		}

		while (parameterIndex > parms.size())
		{
			parms.add(null);
		}

		parms.add(parameterIndex, x);
	}

	@Override
	public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setBlob(int parameterIndex, Blob x) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setBoolean(int parameterIndex, boolean x) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setByte(int parameterIndex, byte x) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setBytes(int parameterIndex, byte[] x) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setClob(int parameterIndex, Clob x) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setClob(int parameterIndex, Reader reader) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setClob(int parameterIndex, Reader reader, long length) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setDate(int parameterIndex, Date x) throws SQLException
	{
		if (closed)
		{
			throw new SQLException("setDate() called on a closed statement");
		}

		while (parameterIndex > parms.size())
		{
			parms.add(null);
		}

		parms.add(parameterIndex, x);
	}

	@Override
	public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setDouble(int parameterIndex, double x) throws SQLException
	{
		if (closed)
		{
			throw new SQLException("setDouble() called on a closed statement");
		}

		while (parameterIndex > parms.size())
		{
			parms.add(null);
		}

		parms.add(parameterIndex, x);
	}

	@Override
	public void setFloat(int parameterIndex, float x) throws SQLException
	{
		if (closed)
		{
			throw new SQLException("setFloat() called on a closed statement");
		}

		while (parameterIndex > parms.size())
		{
			parms.add(null);
		}

		parms.add(parameterIndex, x);
	}

	@Override
	public void setInt(int parameterIndex, int x) throws SQLException
	{
		if (closed)
		{
			throw new SQLException("setInt() called on a closed statement");
		}

		while (parameterIndex > parms.size())
		{
			parms.add(null);
		}

		parms.add(parameterIndex, x);
	}

	@Override
	public void setLong(int parameterIndex, long x) throws SQLException
	{
		if (closed)
		{
			throw new SQLException("setLong() called on a closed statement");
		}

		while (parameterIndex > parms.size())
		{
			parms.add(null);
		}

		parms.add(parameterIndex, x);
	}

	@Override
	public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setNClob(int parameterIndex, NClob value) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setNClob(int parameterIndex, Reader reader) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setNString(int parameterIndex, String value) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setNull(int parameterIndex, int sqlType) throws SQLException
	{
		if (closed)
		{
			throw new SQLException("setNull() called on a closed statement");
		}

		while (parameterIndex > parms.size())
		{
			parms.add(null);
		}

		parms.add(parameterIndex, null);
	}

	@Override
	public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setObject(int parameterIndex, Object x) throws SQLException
	{
		if (closed)
		{
			throw new SQLException("setObject() called on a closed statement");
		}

		while (parameterIndex > parms.size())
		{
			parms.add(null);
		}

		parms.add(parameterIndex, x);
	}

	@Override
	public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setRef(int parameterIndex, Ref x) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setRowId(int parameterIndex, RowId x) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setShort(int parameterIndex, short x) throws SQLException
	{
		if (closed)
		{
			throw new SQLException("setShort() called on a closed statement");
		}

		while (parameterIndex > parms.size())
		{
			parms.add(null);
		}

		parms.add(parameterIndex, x);
	}

	@Override
	public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setString(int parameterIndex, String x) throws SQLException
	{
		if (closed)
		{
			throw new SQLException("setString() called on a closed statement");
		}

		while (parameterIndex > parms.size())
		{
			parms.add(null);
		}

		parms.add(parameterIndex, x);
	}

	@Override
	public void setTime(int parameterIndex, Time x) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setURL(int parameterIndex, URL x) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}
}
