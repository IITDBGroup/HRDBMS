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

	public HRDBMSPreparedStatement(final HRDBMSConnection conn, final String sql)
	{
		super(conn);
		this.sql = sql;
	}

	public HRDBMSPreparedStatement(final HRDBMSConnection conn, final String sql, final int arg1, final int arg2) throws SQLFeatureNotSupportedException
	{
		super(conn, arg1, arg2);
		this.sql = sql;
	}

	public HRDBMSPreparedStatement(final HRDBMSConnection conn, final String sql, final int arg1, final int arg2, final int arg3) throws SQLFeatureNotSupportedException
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
	public void setArray(final int parameterIndex, final Array x) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setAsciiStream(final int parameterIndex, final InputStream x) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setAsciiStream(final int parameterIndex, final InputStream x, final int length) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setAsciiStream(final int parameterIndex, final InputStream x, final long length) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setBigDecimal(final int parameterIndex, final BigDecimal x) throws SQLException
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
	public void setBinaryStream(final int parameterIndex, final InputStream x) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setBinaryStream(final int parameterIndex, final InputStream x, final int length) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setBinaryStream(final int parameterIndex, final InputStream x, final long length) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setBlob(final int parameterIndex, final Blob x) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setBlob(final int parameterIndex, final InputStream inputStream) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setBlob(final int parameterIndex, final InputStream inputStream, final long length) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setBoolean(final int parameterIndex, final boolean x) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setByte(final int parameterIndex, final byte x) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setBytes(final int parameterIndex, final byte[] x) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setCharacterStream(final int parameterIndex, final Reader reader) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setCharacterStream(final int parameterIndex, final Reader reader, final int length) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setCharacterStream(final int parameterIndex, final Reader reader, final long length) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setClob(final int parameterIndex, final Clob x) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setClob(final int parameterIndex, final Reader reader) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setClob(final int parameterIndex, final Reader reader, final long length) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setDate(final int parameterIndex, final Date x) throws SQLException
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
	public void setDate(final int parameterIndex, final Date x, final Calendar cal) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setDouble(final int parameterIndex, final double x) throws SQLException
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
	public void setFloat(final int parameterIndex, final float x) throws SQLException
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
	public void setInt(final int parameterIndex, final int x) throws SQLException
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
	public void setLong(final int parameterIndex, final long x) throws SQLException
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
	public void setNCharacterStream(final int parameterIndex, final Reader value) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setNCharacterStream(final int parameterIndex, final Reader value, final long length) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setNClob(final int parameterIndex, final NClob value) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setNClob(final int parameterIndex, final Reader reader) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setNClob(final int parameterIndex, final Reader reader, final long length) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setNString(final int parameterIndex, final String value) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setNull(final int parameterIndex, final int sqlType) throws SQLException
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
	public void setNull(final int parameterIndex, final int sqlType, final String typeName) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setObject(final int parameterIndex, final Object x) throws SQLException
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
	public void setObject(final int parameterIndex, final Object x, final int targetSqlType) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setObject(final int parameterIndex, final Object x, final int targetSqlType, final int scaleOrLength) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setRef(final int parameterIndex, final Ref x) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setRowId(final int parameterIndex, final RowId x) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setShort(final int parameterIndex, final short x) throws SQLException
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
	public void setSQLXML(final int parameterIndex, final SQLXML xmlObject) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setString(final int parameterIndex, final String x) throws SQLException
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
	public void setTime(final int parameterIndex, final Time x) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setTime(final int parameterIndex, final Time x, final Calendar cal) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setTimestamp(final int parameterIndex, final Timestamp x) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setTimestamp(final int parameterIndex, final Timestamp x, final Calendar cal) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setUnicodeStream(final int parameterIndex, final InputStream x, final int length) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setURL(final int parameterIndex, final URL x) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}
}
