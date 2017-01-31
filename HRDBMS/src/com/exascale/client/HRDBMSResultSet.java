package com.exascale.client;

import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.Reader;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.misc.DataEndMarker;
import com.exascale.misc.MyDate;

public class HRDBMSResultSet implements ResultSet
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
	private final ArrayList<Object> rs = new ArrayList<Object>();
	private int firstRowIs = 0;
	private int position = -1;
	private boolean closed = false;
	private final HRDBMSConnection conn;
	private int fetchSize;
	private boolean wasNull = false;
	private HashMap<String, Integer> cols2Pos;
	private TreeMap<Integer, String> pos2Cols;
	private HashMap<String, String> cols2Types;
	private final HRDBMSStatement stmt;

	private final CharsetDecoder cd = cs.newDecoder();

	public HRDBMSResultSet(final HRDBMSConnection conn, final int fetchSize, final HRDBMSStatement stmt) throws Exception
	{
		this.conn = conn;
		this.fetchSize = fetchSize;
		this.stmt = stmt;
		requestMetaData();
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
	public boolean absolute(final int row) throws SQLException
	{
		throw new SQLException("Result set is TYPE_FORWARD_ONLY");
	}

	@Override
	public void afterLast() throws SQLException
	{
		throw new SQLException("Result set is TYPE_FORWARD_ONLY");
	}

	@Override
	public void beforeFirst() throws SQLException
	{
		throw new SQLException("Result set is TYPE_FORWARD_ONLY");
	}

	@Override
	public void cancelRowUpdates() throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void clearWarnings() throws SQLException
	{
		if (closed)
		{
			throw new SQLException("clearWarnings() called on a closed result set");
		}

	}

	@Override
	public void close() throws SQLException
	{
		if (closed)
		{
			return;
		}

		try
		{
			closed = true;
			if (conn.autoCommit)
			{
				conn.commit();
			}
			sendCloseRS();
		}
		catch (final Exception e)
		{
			throw new SQLException(e.getMessage());
		}
	}

	@Override
	public void deleteRow() throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public int findColumn(final String columnLabel) throws SQLException
	{
		if (closed)
		{
			throw new SQLException("findColumn() called on a closed result set");
		}

		final Integer pos = cols2Pos.get(columnLabel.toUpperCase());
		if (pos == null)
		{
			throw new SQLException("The column does not exist in the result set");
		}

		return pos;
	}

	@Override
	public boolean first() throws SQLException
	{
		throw new SQLException("Result set is TYPE_FORWARD_ONLY");
	}

	@Override
	public Array getArray(final int columnIndex) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Array getArray(final String columnLabel) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public InputStream getAsciiStream(final int columnIndex) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public InputStream getAsciiStream(final String columnLabel) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public BigDecimal getBigDecimal(final int columnIndex) throws SQLException
	{
		wasNull = false;

		if (closed)
		{
			throw new SQLException("Cannot fetch a value from a closed result set");
		}

		final Object row = rs.get(position - firstRowIs);
		if (row instanceof DataEndMarker)
		{
			throw new SQLException("Past end of result set");
		}

		final ArrayList<Object> alo = (ArrayList<Object>)row;

		if (columnIndex < 1 || columnIndex > alo.size())
		{
			throw new SQLException("Column index is out of bounds");
		}

		final Object col = alo.get(columnIndex - 1);

		if (col == null)
		{
			wasNull = true;
			return null;
		}

		if (!(col instanceof Integer || col instanceof Short || col instanceof Long || col instanceof Float || col instanceof Double || col instanceof BigDecimal))
		{
			throw new SQLException("The column is not numeric");
		}

		final Number num = (Number)col;
		return new BigDecimal(num.doubleValue());
	}

	@Override
	public BigDecimal getBigDecimal(final int columnIndex, final int scale) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public BigDecimal getBigDecimal(final String columnLabel) throws SQLException
	{
		wasNull = false;

		if (closed)
		{
			throw new SQLException("Cannot fetch a value from a closed result set");
		}

		final Integer columnIndex = cols2Pos.get(columnLabel.toUpperCase());
		if (columnIndex == null)
		{
			throw new SQLException("The column requested does not exist in the result set");
		}

		final Object row = rs.get(position - firstRowIs);
		if (row instanceof DataEndMarker)
		{
			throw new SQLException("Past end of result set");
		}

		final ArrayList<Object> alo = (ArrayList<Object>)row;

		if (columnIndex < 1 || columnIndex > alo.size())
		{
			throw new SQLException("Column index is out of bounds");
		}

		final Object col = alo.get(columnIndex - 1);

		if (col == null)
		{
			wasNull = true;
			return null;
		}

		if (!(col instanceof Integer || col instanceof Short || col instanceof Long || col instanceof Float || col instanceof Double || col instanceof BigDecimal))
		{
			throw new SQLException("The column is not numeric");
		}

		final Number num = (Number)col;
		return new BigDecimal(num.doubleValue());
	}

	@Override
	public BigDecimal getBigDecimal(final String columnLabel, final int scale) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public InputStream getBinaryStream(final int columnIndex) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public InputStream getBinaryStream(final String columnLabel) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Blob getBlob(final int columnIndex) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Blob getBlob(final String columnLabel) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean getBoolean(final int columnIndex) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean getBoolean(final String columnLabel) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public byte getByte(final int columnIndex) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public byte getByte(final String columnLabel) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public byte[] getBytes(final int columnIndex) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public byte[] getBytes(final String columnLabel) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Reader getCharacterStream(final int columnIndex) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Reader getCharacterStream(final String columnLabel) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Clob getClob(final int columnIndex) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Clob getClob(final String columnLabel) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getConcurrency() throws SQLException
	{
		if (closed)
		{
			throw new SQLException("getConcurrency() called on closed result set");
		}

		return ResultSet.CONCUR_READ_ONLY;
	}

	@Override
	public String getCursorName() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Date getDate(final int columnIndex) throws SQLException
	{
		wasNull = false;

		if (closed)
		{
			throw new SQLException("Cannot fetch a value from a closed result set");
		}

		final Object row = rs.get(position - firstRowIs);
		if (row instanceof DataEndMarker)
		{
			throw new SQLException("Past end of result set");
		}

		final ArrayList<Object> alo = (ArrayList<Object>)row;

		if (columnIndex < 1 || columnIndex > alo.size())
		{
			throw new SQLException("Column index is out of bounds");
		}

		final Object col = alo.get(columnIndex - 1);

		if (col == null)
		{
			wasNull = true;
			return null;
		}

		if (!(col instanceof Date))
		{
			throw new SQLException("The column is not a date");
		}

		return (Date)col;
	}

	@Override
	public Date getDate(final int columnIndex, final Calendar cal) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Date getDate(final String columnLabel) throws SQLException
	{
		wasNull = false;

		if (closed)
		{
			throw new SQLException("Cannot fetch a value from a closed result set");
		}

		final Integer columnIndex = cols2Pos.get(columnLabel.toUpperCase());
		if (columnIndex == null)
		{
			throw new SQLException("The column requested does not exist in the result set");
		}

		final Object row = rs.get(position - firstRowIs);
		if (row instanceof DataEndMarker)
		{
			throw new SQLException("Past end of result set");
		}

		final ArrayList<Object> alo = (ArrayList<Object>)row;

		if (columnIndex < 1 || columnIndex > alo.size())
		{
			throw new SQLException("Column index is out of bounds");
		}

		final Object col = alo.get(columnIndex - 1);

		if (col == null)
		{
			wasNull = true;
			return null;
		}

		if (!(col instanceof Date))
		{
			throw new SQLException("The column is not a date");
		}

		return (Date)col;
	}

	@Override
	public Date getDate(final String columnLabel, final Calendar cal) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public double getDouble(final int columnIndex) throws SQLException
	{
		wasNull = false;

		if (closed)
		{
			throw new SQLException("Cannot fetch a value from a closed result set");
		}

		final Object row = rs.get(position - firstRowIs);
		if (row instanceof DataEndMarker)
		{
			throw new SQLException("Past end of result set");
		}

		final ArrayList<Object> alo = (ArrayList<Object>)row;

		if (columnIndex < 1 || columnIndex > alo.size())
		{
			throw new SQLException("Column index is out of bounds");
		}

		final Object col = alo.get(columnIndex - 1);

		if (col == null)
		{
			wasNull = true;
			return 0;
		}
		if (!(col instanceof Integer || col instanceof Short || col instanceof Long || col instanceof Float || col instanceof Double || col instanceof BigDecimal))
		{
			throw new SQLException("The column is not numeric");
		}

		final Number num = (Number)col;
		return num.doubleValue();
	}

	@Override
	public double getDouble(final String columnLabel) throws SQLException
	{
		wasNull = false;

		if (closed)
		{
			throw new SQLException("Cannot fetch a value from a closed result set");
		}

		final Integer columnIndex = cols2Pos.get(columnLabel.toUpperCase());
		if (columnIndex == null)
		{
			throw new SQLException("The column requested does not exist in the result set");
		}

		final Object row = rs.get(position - firstRowIs);
		if (row instanceof DataEndMarker)
		{
			throw new SQLException("Past end of result set");
		}

		final ArrayList<Object> alo = (ArrayList<Object>)row;

		if (columnIndex < 1 || columnIndex > alo.size())
		{
			throw new SQLException("Column index is out of bounds");
		}

		final Object col = alo.get(columnIndex - 1);

		if (col == null)
		{
			wasNull = true;
			return 0;
		}

		if (!(col instanceof Integer || col instanceof Short || col instanceof Long || col instanceof Float || col instanceof Double || col instanceof BigDecimal))
		{
			throw new SQLException("The column is not numeric");
		}

		final Number num = (Number)col;
		return num.doubleValue();
	}

	@Override
	public int getFetchDirection() throws SQLException
	{
		if (closed)
		{
			throw new SQLException("getFetchDirection() called on closed result set");
		}

		return ResultSet.FETCH_FORWARD;
	}

	@Override
	public int getFetchSize() throws SQLException
	{
		if (closed)
		{
			throw new SQLException("getFetchSize() called on closed result set");
		}

		return fetchSize;
	}

	@Override
	public float getFloat(final int columnIndex) throws SQLException
	{
		wasNull = false;

		if (closed)
		{
			throw new SQLException("Cannot fetch a value from a closed result set");
		}

		final Object row = rs.get(position - firstRowIs);
		if (row instanceof DataEndMarker)
		{
			throw new SQLException("Past end of result set");
		}

		final ArrayList<Object> alo = (ArrayList<Object>)row;

		if (columnIndex < 1 || columnIndex > alo.size())
		{
			throw new SQLException("Column index is out of bounds");
		}

		final Object col = alo.get(columnIndex - 1);

		if (col == null)
		{
			wasNull = true;
			return 0;
		}

		if (!(col instanceof Integer || col instanceof Short || col instanceof Long || col instanceof Float || col instanceof Double || col instanceof BigDecimal))
		{
			throw new SQLException("The column is not numeric");
		}

		final Number num = (Number)col;
		return num.floatValue();
	}

	@Override
	public float getFloat(final String columnLabel) throws SQLException
	{
		wasNull = false;

		if (closed)
		{
			throw new SQLException("Cannot fetch a value from a closed result set");
		}

		final Integer columnIndex = cols2Pos.get(columnLabel.toUpperCase());
		if (columnIndex == null)
		{
			throw new SQLException("The column requested does not exist in the result set");
		}

		final Object row = rs.get(position - firstRowIs);
		if (row instanceof DataEndMarker)
		{
			throw new SQLException("Past end of result set");
		}

		final ArrayList<Object> alo = (ArrayList<Object>)row;

		if (columnIndex < 1 || columnIndex > alo.size())
		{
			throw new SQLException("Column index is out of bounds");
		}

		final Object col = alo.get(columnIndex - 1);

		if (col == null)
		{
			wasNull = true;
			return 0;
		}

		if (!(col instanceof Integer || col instanceof Short || col instanceof Long || col instanceof Float || col instanceof Double || col instanceof BigDecimal))
		{
			throw new SQLException("The column is not numeric");
		}

		final Number num = (Number)col;
		return num.floatValue();
	}

	@Override
	public int getHoldability() throws SQLException
	{
		if (closed)
		{
			throw new SQLException("getHoldability() called on closed result set");
		}

		return ResultSet.CLOSE_CURSORS_AT_COMMIT;
	}

	@Override
	public int getInt(final int columnIndex) throws SQLException
	{
		wasNull = false;

		if (closed)
		{
			throw new SQLException("Cannot fetch a value from a closed result set");
		}

		final Object row = rs.get(position - firstRowIs);
		if (row instanceof DataEndMarker)
		{
			throw new SQLException("Past end of result set");
		}

		final ArrayList<Object> alo = (ArrayList<Object>)row;

		if (columnIndex < 1 || columnIndex > alo.size())
		{
			throw new SQLException("Column index is out of bounds");
		}

		final Object col = alo.get(columnIndex - 1);

		if (col == null)
		{
			wasNull = true;
			return 0;
		}
		if (!(col instanceof Integer || col instanceof Short || col instanceof Long))
		{
			throw new SQLException("The column cannot be converted to an integer");
		}

		if (col instanceof Long)
		{
			if ((Long)col > Integer.MAX_VALUE || (Long)col < Integer.MIN_VALUE)
			{
				throw new SQLException("The column cannot be converted to an integer");
			}
		}
		final Number num = (Number)col;
		return num.intValue();
	}

	@Override
	public int getInt(final String columnLabel) throws SQLException
	{
		wasNull = false;

		if (closed)
		{
			throw new SQLException("Cannot fetch a value from a closed result set");
		}

		final Integer columnIndex = cols2Pos.get(columnLabel.toUpperCase());
		if (columnIndex == null)
		{
			throw new SQLException("The column requested does not exist in the result set");
		}

		final Object row = rs.get(position - firstRowIs);
		if (row instanceof DataEndMarker)
		{
			throw new SQLException("Past end of result set");
		}

		final ArrayList<Object> alo = (ArrayList<Object>)row;

		if (columnIndex < 1 || columnIndex > alo.size())
		{
			throw new SQLException("Column index is out of bounds");
		}

		final Object col = alo.get(columnIndex - 1);

		if (col == null)
		{
			wasNull = true;
			return 0;
		}

		if (!(col instanceof Integer || col instanceof Short || col instanceof Long))
		{
			throw new SQLException("The column cannot be converted to an integer");
		}

		if (col instanceof Long)
		{
			if ((Long)col > Integer.MAX_VALUE || (Long)col < Integer.MIN_VALUE)
			{
				throw new SQLException("The column cannot be converted to an integer");
			}
		}

		final Number num = (Number)col;
		return num.intValue();
	}

	@Override
	public long getLong(final int columnIndex) throws SQLException
	{
		wasNull = false;

		if (closed)
		{
			throw new SQLException("Cannot fetch a value from a closed result set");
		}

		final Object row = rs.get(position - firstRowIs);
		if (row instanceof DataEndMarker)
		{
			throw new SQLException("Past end of result set");
		}

		final ArrayList<Object> alo = (ArrayList<Object>)row;

		if (columnIndex < 1 || columnIndex > alo.size())
		{
			throw new SQLException("Column index is out of bounds");
		}

		final Object col = alo.get(columnIndex - 1);

		if (col == null)
		{
			wasNull = true;
			return 0;
		}

		if (!(col instanceof Integer || col instanceof Short || col instanceof Long))
		{
			throw new SQLException("The column cannot be converted to a long");
		}

		final Number num = (Number)col;
		return num.longValue();
	}

	@Override
	public long getLong(final String columnLabel) throws SQLException
	{
		wasNull = false;

		if (closed)
		{
			throw new SQLException("Cannot fetch a value from a closed result set");
		}

		final Integer columnIndex = cols2Pos.get(columnLabel.toUpperCase());
		if (columnIndex == null)
		{
			throw new SQLException("The column requested does not exist in the result set");
		}

		final Object row = rs.get(position - firstRowIs);
		if (row instanceof DataEndMarker)
		{
			throw new SQLException("Past end of result set");
		}

		final ArrayList<Object> alo = (ArrayList<Object>)row;

		if (columnIndex < 1 || columnIndex > alo.size())
		{
			throw new SQLException("Column index is out of bounds");
		}

		final Object col = alo.get(columnIndex - 1);

		if (col == null)
		{
			wasNull = true;
			return 0;
		}

		if (!(col instanceof Integer || col instanceof Short || col instanceof Long))
		{
			throw new SQLException("The column cannot be converted to a long");
		}

		final Number num = (Number)col;
		return num.longValue();
	}

	@Override
	public ResultSetMetaData getMetaData() throws SQLException
	{
		if (closed)
		{
			throw new SQLException("getMetaData() called on a closed result set");
		}

		return new HRDBMSResultSetMetaData(cols2Pos, pos2Cols, cols2Types);
	}

	@Override
	public Reader getNCharacterStream(final int columnIndex) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Reader getNCharacterStream(final String columnLabel) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public NClob getNClob(final int columnIndex) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public NClob getNClob(final String columnLabel) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public String getNString(final int columnIndex) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public String getNString(final String columnLabel) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Object getObject(final int columnIndex) throws SQLException
	{
		wasNull = false;

		if (closed)
		{
			throw new SQLException("Cannot fetch a value from a closed result set");
		}

		final Object row = rs.get(position - firstRowIs);
		if (row instanceof DataEndMarker)
		{
			throw new SQLException("Past end of result set");
		}

		final ArrayList<Object> alo = (ArrayList<Object>)row;

		if (columnIndex < 1 || columnIndex > alo.size())
		{
			throw new SQLException("Column index is out of bounds");
		}

		final Object col = alo.get(columnIndex - 1);
		if (col == null)
		{
			wasNull = true;
		}

		return col;
	}

	@Override
	public <T> T getObject(final int columnIndex, final Class<T> type) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Object getObject(final int columnIndex, final Map<String, Class<?>> map) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Object getObject(final String columnLabel) throws SQLException
	{
		wasNull = false;

		if (closed)
		{
			throw new SQLException("Cannot fetch a value from a closed result set");
		}

		final Integer columnIndex = cols2Pos.get(columnLabel.toUpperCase());
		if (columnIndex == null)
		{
			throw new SQLException("The column requested does not exist in the result set");
		}

		final Object row = rs.get(position - firstRowIs);
		if (row instanceof DataEndMarker)
		{
			throw new SQLException("Past end of result set");
		}

		final ArrayList<Object> alo = (ArrayList<Object>)row;

		if (columnIndex < 1 || columnIndex > alo.size())
		{
			throw new SQLException("Column index is out of bounds");
		}

		final Object col = alo.get(columnIndex - 1);
		if (col == null)
		{
			wasNull = true;
		}

		return col;
	}

	@Override
	public <T> T getObject(final String columnLabel, final Class<T> type) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Object getObject(final String columnLabel, final Map<String, Class<?>> map) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Ref getRef(final int columnIndex) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Ref getRef(final String columnLabel) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getRow() throws SQLException
	{
		if (closed)
		{
			throw new SQLException("getRow() called on closed result set");
		}

		if (position == -1)
		{
			return 0;
		}

		final Object row = rs.get(position - firstRowIs);
		if (row instanceof DataEndMarker)
		{
			return 0;
		}

		return position + 1;
	}

	@Override
	public RowId getRowId(final int columnIndex) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public RowId getRowId(final String columnLabel) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public short getShort(final int columnIndex) throws SQLException
	{
		wasNull = false;

		if (closed)
		{
			throw new SQLException("Cannot fetch a value from a closed result set");
		}

		final Object row = rs.get(position - firstRowIs);
		if (row instanceof DataEndMarker)
		{
			throw new SQLException("Past end of result set");
		}

		final ArrayList<Object> alo = (ArrayList<Object>)row;

		if (columnIndex < 1 || columnIndex > alo.size())
		{
			throw new SQLException("Column index is out of bounds");
		}

		final Object col = alo.get(columnIndex - 1);

		if (col == null)
		{
			wasNull = true;
			return 0;
		}

		if (!(col instanceof Short) && !(col instanceof Integer) && !(col instanceof Long))
		{
			throw new SQLException("The column cannot be converted to a short");
		}

		if (col instanceof Short)
		{
			return (short)col;
		}

		if (col instanceof Integer)
		{
			if ((Integer)col > Short.MAX_VALUE || (Integer)col < Short.MIN_VALUE)
			{
				throw new SQLException("The column cannot be converted to a short");
			}

			return ((Integer)col).shortValue();
		}

		if ((Long)col > Short.MAX_VALUE || (Long)col < Short.MIN_VALUE)
		{
			throw new SQLException("The column cannot be converted to a short");
		}

		return ((Integer)col).shortValue();
	}

	@Override
	public short getShort(final String columnLabel) throws SQLException
	{
		wasNull = false;

		if (closed)
		{
			throw new SQLException("Cannot fetch a value from a closed result set");
		}

		final Integer columnIndex = cols2Pos.get(columnLabel.toUpperCase());
		if (columnIndex == null)
		{
			throw new SQLException("The column requested does not exist in the result set");
		}

		final Object row = rs.get(position - firstRowIs);
		if (row instanceof DataEndMarker)
		{
			throw new SQLException("Past end of result set");
		}

		final ArrayList<Object> alo = (ArrayList<Object>)row;

		if (columnIndex < 1 || columnIndex > alo.size())
		{
			throw new SQLException("Column index is out of bounds");
		}

		final Object col = alo.get(columnIndex - 1);

		if (col == null)
		{
			wasNull = true;
			return 0;
		}

		if (!(col instanceof Short) && !(col instanceof Integer) && !(col instanceof Long))
		{
			throw new SQLException("The column cannot be converted to a short");
		}

		if (col instanceof Short)
		{
			return (short)col;
		}

		if (col instanceof Integer)
		{
			if ((Integer)col > Short.MAX_VALUE || (Integer)col < Short.MIN_VALUE)
			{
				throw new SQLException("The column cannot be converted to a short");
			}

			return ((Integer)col).shortValue();
		}

		if ((Long)col > Short.MAX_VALUE || (Long)col < Short.MIN_VALUE)
		{
			throw new SQLException("The column cannot be converted to a short");
		}

		return ((Integer)col).shortValue();
	}

	@Override
	public SQLXML getSQLXML(final int columnIndex) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public SQLXML getSQLXML(final String columnLabel) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Statement getStatement() throws SQLException
	{
		if (closed)
		{
			throw new SQLException("getStatement() called on a closed result set");
		}

		return stmt;
	}

	@Override
	public String getString(final int columnIndex) throws SQLException
	{
		wasNull = false;

		if (closed)
		{
			throw new SQLException("Cannot fetch a value from a closed result set");
		}

		final Object row = rs.get(position - firstRowIs);
		if (row instanceof DataEndMarker)
		{
			throw new SQLException("Past end of result set");
		}

		final ArrayList<Object> alo = (ArrayList<Object>)row;

		if (columnIndex < 1 || columnIndex > alo.size())
		{
			throw new SQLException("Column index is out of bounds");
		}

		final Object col = alo.get(columnIndex - 1);

		if (col == null)
		{
			wasNull = true;
			return null;
		}

		if (!(col instanceof String))
		{
			throw new SQLException("The column is not a character data type");
		}

		return (String)col;
	}

	@Override
	public String getString(final String columnLabel) throws SQLException
	{
		wasNull = false;

		if (closed)
		{
			throw new SQLException("Cannot fetch a value from a closed result set");
		}

		final Integer columnIndex = cols2Pos.get(columnLabel.toUpperCase());
		if (columnIndex == null)
		{
			throw new SQLException("The column requested does not exist in the result set");
		}

		final Object row = rs.get(position - firstRowIs);
		if (row instanceof DataEndMarker)
		{
			throw new SQLException("Past end of result set");
		}

		final ArrayList<Object> alo = (ArrayList<Object>)row;

		if (columnIndex < 1 || columnIndex > alo.size())
		{
			throw new SQLException("Column index is out of bounds");
		}

		final Object col = alo.get(columnIndex - 1);

		if (col == null)
		{
			wasNull = true;
			return null;
		}

		if (!(col instanceof String))
		{
			throw new SQLException("The column is not a character data type");
		}

		return (String)col;
	}

	@Override
	public Time getTime(final int columnIndex) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Time getTime(final int columnIndex, final Calendar cal) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Time getTime(final String columnLabel) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Time getTime(final String columnLabel, final Calendar cal) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Timestamp getTimestamp(final int columnIndex) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Timestamp getTimestamp(final int columnIndex, final Calendar cal) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Timestamp getTimestamp(final String columnLabel) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Timestamp getTimestamp(final String columnLabel, final Calendar cal) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getType() throws SQLException
	{
		if (closed)
		{
			throw new SQLException("getType() called on closed result set");
		}

		return ResultSet.TYPE_FORWARD_ONLY;
	}

	@Override
	public InputStream getUnicodeStream(final int columnIndex) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public InputStream getUnicodeStream(final String columnLabel) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public URL getURL(final int columnIndex) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public URL getURL(final String columnLabel) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public SQLWarning getWarnings() throws SQLException
	{
		if (closed)
		{
			throw new SQLException("getWarnings() called on a closed result set");
		}

		return null;
	}

	@Override
	public void insertRow() throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public boolean isAfterLast() throws SQLException
	{
		if (closed)
		{
			throw new SQLException("isAfterLast() called on closed result set");
		}

		if (position == 0 && rs.size() == 0)
		{
			getMoreData();
		}

		final Object row = rs.get(rs.size() - 1);
		if (!(row instanceof DataEndMarker))
		{
			return false;
		}

		if (position < firstRowIs + rs.size() - 1)
		{
			return false;
		}

		if (position > 0)
		{
			return true;
		}

		return false;
	}

	@Override
	public boolean isBeforeFirst() throws SQLException
	{
		if (closed)
		{
			throw new SQLException("isBeforeFirst() called on closed result set");
		}

		if (position != -1)
		{
			return false;
		}

		if (rs.size() == 0)
		{
			getMoreData();
		}

		final Object row = rs.get(0);
		if (row instanceof DataEndMarker)
		{
			return false;
		}

		return true;
	}

	@Override
	public boolean isClosed() throws SQLException
	{
		return closed;
	}

	@Override
	public boolean isFirst() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean isLast() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean isWrapperFor(final Class<?> iface) throws SQLException
	{
		return false;
	}

	@Override
	public boolean last() throws SQLException
	{
		throw new SQLException("Result set is TYPE_FORWARD_ONLY");
	}

	@Override
	public void moveToCurrentRow() throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void moveToInsertRow() throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public boolean next() throws SQLException
	{
		if (closed)
		{
			throw new SQLException("next() called on a closed result set");
		}

		position++;

		if (firstRowIs - 1 + rs.size() < position)
		{
			if (rs.size() > 0)
			{
				final Object row = rs.get(rs.size() - 1);
				if (row instanceof DataEndMarker)
				{
					return false;
				}
			}

			// call to get more data
			getMoreData();
			firstRowIs = position;
		}

		final Object row = rs.get(position - firstRowIs);
		if (row instanceof DataEndMarker)
		{
			return false;
		}
		else
		{
			return true;
		}
	}

	@Override
	public boolean previous() throws SQLException
	{
		throw new SQLException("Result set is TYPE_FORWARD_ONLY");
	}

	@Override
	public void refreshRow() throws SQLException
	{
		throw new SQLException("Result set is TYPE_FORWARD_ONLY");
	}

	@Override
	public boolean relative(final int rows) throws SQLException
	{
		throw new SQLException("Result set is TYPE_FORWARD_ONLY");
	}

	@Override
	public boolean rowDeleted() throws SQLException
	{
		if (closed)
		{
			throw new SQLException("rowDeleted() called on closed result set");
		}

		return false;
	}

	@Override
	public boolean rowInserted() throws SQLException
	{
		if (closed)
		{
			throw new SQLException("rowInserted() called on closed result set");
		}

		return false;
	}

	@Override
	public boolean rowUpdated() throws SQLException
	{
		if (closed)
		{
			throw new SQLException("rowUpdated() called on closed result set");
		}

		return false;
	}

	@Override
	public void setFetchDirection(final int direction) throws SQLException
	{
		if (closed)
		{
			throw new SQLException("setFetchDirection() called on closed result set");
		}

		if (direction != ResultSet.FETCH_FORWARD)
		{
			throw new SQLException("Result set is TYPE_FORWARD_ONLY");
		}
	}

	@Override
	public void setFetchSize(final int rows) throws SQLException
	{
		if (closed)
		{
			throw new SQLException("setFetchSize() called on a closed result set");
		}

		if (rows <= 0)
		{
			throw new SQLException("Fetch size must be positive()");
		}

		fetchSize = rows;
	}

	@Override
	public <T> T unwrap(final Class<T> iface) throws SQLException
	{
		throw new SQLException("Unwrap() is not supported.");
	}

	@Override
	public void updateArray(final int columnIndex, final Array x) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateArray(final String columnLabel, final Array x) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateAsciiStream(final int columnIndex, final InputStream x) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateAsciiStream(final int columnIndex, final InputStream x, final int length) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateAsciiStream(final int columnIndex, final InputStream x, final long length) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateAsciiStream(final String columnLabel, final InputStream x) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateAsciiStream(final String columnLabel, final InputStream x, final int length) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateAsciiStream(final String columnLabel, final InputStream x, final long length) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateBigDecimal(final int columnIndex, final BigDecimal x) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateBigDecimal(final String columnLabel, final BigDecimal x) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateBinaryStream(final int columnIndex, final InputStream x) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateBinaryStream(final int columnIndex, final InputStream x, final int length) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateBinaryStream(final int columnIndex, final InputStream x, final long length) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateBinaryStream(final String columnLabel, final InputStream x) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateBinaryStream(final String columnLabel, final InputStream x, final int length) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateBinaryStream(final String columnLabel, final InputStream x, final long length) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateBlob(final int columnIndex, final Blob x) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateBlob(final int columnIndex, final InputStream inputStream) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateBlob(final int columnIndex, final InputStream inputStream, final long length) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateBlob(final String columnLabel, final Blob x) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateBlob(final String columnLabel, final InputStream inputStream) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateBlob(final String columnLabel, final InputStream inputStream, final long length) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateBoolean(final int columnIndex, final boolean x) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateBoolean(final String columnLabel, final boolean x) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateByte(final int columnIndex, final byte x) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateByte(final String columnLabel, final byte x) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateBytes(final int columnIndex, final byte[] x) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateBytes(final String columnLabel, final byte[] x) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateCharacterStream(final int columnIndex, final Reader x) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateCharacterStream(final int columnIndex, final Reader x, final int length) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateCharacterStream(final int columnIndex, final Reader x, final long length) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateCharacterStream(final String columnLabel, final Reader reader) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateCharacterStream(final String columnLabel, final Reader reader, final int length) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateCharacterStream(final String columnLabel, final Reader reader, final long length) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateClob(final int columnIndex, final Clob x) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateClob(final int columnIndex, final Reader reader) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateClob(final int columnIndex, final Reader reader, final long length) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateClob(final String columnLabel, final Clob x) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateClob(final String columnLabel, final Reader reader) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateClob(final String columnLabel, final Reader reader, final long length) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateDate(final int columnIndex, final Date x) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateDate(final String columnLabel, final Date x) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateDouble(final int columnIndex, final double x) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateDouble(final String columnLabel, final double x) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateFloat(final int columnIndex, final float x) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateFloat(final String columnLabel, final float x) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateInt(final int columnIndex, final int x) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateInt(final String columnLabel, final int x) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateLong(final int columnIndex, final long x) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateLong(final String columnLabel, final long x) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateNCharacterStream(final int columnIndex, final Reader x) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateNCharacterStream(final int columnIndex, final Reader x, final long length) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateNCharacterStream(final String columnLabel, final Reader reader) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateNCharacterStream(final String columnLabel, final Reader reader, final long length) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateNClob(final int columnIndex, final NClob nClob) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateNClob(final int columnIndex, final Reader reader) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateNClob(final int columnIndex, final Reader reader, final long length) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateNClob(final String columnLabel, final NClob nClob) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateNClob(final String columnLabel, final Reader reader) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateNClob(final String columnLabel, final Reader reader, final long length) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateNString(final int columnIndex, final String nString) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateNString(final String columnLabel, final String nString) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateNull(final int columnIndex) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateNull(final String columnLabel) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateObject(final int columnIndex, final Object x) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateObject(final int columnIndex, final Object x, final int scaleOrLength) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateObject(final String columnLabel, final Object x) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateObject(final String columnLabel, final Object x, final int scaleOrLength) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateRef(final int columnIndex, final Ref x) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");

	}

	@Override
	public void updateRef(final String columnLabel, final Ref x) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateRow() throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateRowId(final int columnIndex, final RowId x) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateRowId(final String columnLabel, final RowId x) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateShort(final int columnIndex, final short x) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateShort(final String columnLabel, final short x) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateSQLXML(final int columnIndex, final SQLXML xmlObject) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateSQLXML(final String columnLabel, final SQLXML xmlObject) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateString(final int columnIndex, final String x) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateString(final String columnLabel, final String x) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateTime(final int columnIndex, final Time x) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateTime(final String columnLabel, final Time x) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateTimestamp(final int columnIndex, final Timestamp x) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateTimestamp(final String columnLabel, final Timestamp x) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public boolean wasNull() throws SQLException
	{
		if (closed)
		{
			throw new SQLException("wasNull() called on a closed result set");
		}

		return wasNull;
	}

	private final Object fromBytes(final byte[] val) throws Exception
	{
		final ByteBuffer bb = ByteBuffer.wrap(val);
		final int numFields = bb.getInt();

		if (numFields == 0)
		{
			return new ArrayList<Object>();
		}

		bb.position(bb.position() + numFields);
		final byte[] bytes = bb.array();
		if (bytes[4] == 5)
		{
			return new DataEndMarker();
		}
		final ArrayList<Object> retval = new ArrayList<Object>(numFields);
		int i = 0;
		while (i < numFields)
		{
			if (bytes[i + 4] == 0)
			{
				// long
				final Long o = bb.getLong();
				retval.add(o);
			}
			else if (bytes[i + 4] == 1)
			{
				// integer
				final Integer o = bb.getInt();
				retval.add(o);
			}
			else if (bytes[i + 4] == 2)
			{
				// double
				final Double o = bb.getDouble();
				retval.add(o);
			}
			else if (bytes[i + 4] == 3)
			{
				// date
				final MyDate o = new MyDate(bb.getInt());
				retval.add(o);
			}
			else if (bytes[i + 4] == 4)
			{
				// string
				final int length = bb.getInt();
				final byte[] temp = new byte[length];
				final char[] ca = new char[length];
				bb.get(temp);
				try
				{
					// final String o = new String(temp,
					// StandardCharsets.UTF_8);
					final String value = (String)unsafe.allocateInstance(String.class);
					final int clen = ((sun.nio.cs.ArrayDecoder)cd).decode(temp, 0, length, ca);
					if (clen == ca.length)
					{
						unsafe.putObject(value, offset, ca);
					}
					else
					{
						final char[] v = Arrays.copyOf(ca, clen);
						unsafe.putObject(value, offset, v);
					}
					retval.add(value);
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.error("", e);
					throw e;
				}
			}
			// else if (bytes[i + 4] == 6)
			// {
			// // AtomicLong
			// final long o = bb.getLong();
			// retval.add(new AtomicLong(o));
			// }
			// else if (bytes[i + 4] == 7)
			// {
			// // AtomicDouble
			// final double o = bb.getDouble();
			// retval.add(new AtomicBigDecimal(new BigDecimalReplacement(o)));
			// }
			else if (bytes[i + 4] == 8)
			{
				// Empty ArrayList
				retval.add(new ArrayList<Object>());
			}
			else if (bytes[i + 4] == 9)
			{
				retval.add(null);
			}
			else
			{
				HRDBMSWorker.logger.error("Unknown type in fromBytes()");
				throw new Exception("Unknown type in fromBytes()");
			}

			i++;
		}

		return retval;
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

	private void getMoreData() throws SQLException
	{
		try
		{
			final byte[] outMsg = "NEXT            ".getBytes(StandardCharsets.UTF_8);
			outMsg[8] = 0;
			outMsg[9] = 0;
			outMsg[10] = 0;
			outMsg[11] = 0;
			outMsg[12] = 0;
			outMsg[13] = 0;
			outMsg[14] = 0;
			outMsg[15] = 0;
			conn.out.write(outMsg);
			byte[] num = intToBytes(fetchSize);
			conn.out.write(num);
			conn.out.flush();
			try
			{
				getConfirmation();
			}
			catch (Exception e)
			{
				e = receiveException();
				throw new SQLException(e.getMessage());
			}

			this.rs.clear();
			int i = fetchSize;
			while (i > 0)
			{
				num = new byte[4];
				read(num);
				final int size = bytesToInt(num);
				final byte[] data = new byte[size];
				read(data);
				final Object obj = fromBytes(data);
				rs.add(obj);
				i--;

				if (obj instanceof DataEndMarker)
				{
					break;
				}
			}
		}
		catch (final Exception e)
		{
			if (e instanceof SQLException)
			{
				throw (SQLException)e;
			}

			throw new SQLException(e.getMessage());
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
		catch (final Exception e)
		{
			throw new Exception("An error occurred on the server, but an exception occurred while trying to retrieve the error message.  A rollback has been performed.");
		}
	}

	private void requestMetaData() throws Exception
	{
		final byte[] outMsg = "RSMETA          ".getBytes(StandardCharsets.UTF_8);
		outMsg[8] = 0;
		outMsg[9] = 0;
		outMsg[10] = 0;
		outMsg[11] = 0;
		outMsg[12] = 0;
		outMsg[13] = 0;
		outMsg[14] = 0;
		outMsg[15] = 0;
		conn.out.write(outMsg);
		conn.out.flush();

		final ObjectInputStream in = new ObjectInputStream(conn.in);
		cols2Pos = (HashMap<String, Integer>)in.readObject();
		pos2Cols = (TreeMap<Integer, String>)in.readObject();
		cols2Types = (HashMap<String, String>)in.readObject();
	}

	private void sendCloseRS() throws Exception
	{
		final byte[] outMsg = "CLOSERS         ".getBytes(StandardCharsets.UTF_8);
		outMsg[8] = 0;
		outMsg[9] = 0;
		outMsg[10] = 0;
		outMsg[11] = 0;
		outMsg[12] = 0;
		outMsg[13] = 0;
		outMsg[14] = 0;
		outMsg[15] = 0;
		conn.out.write(outMsg);
		conn.out.flush();
	}

}
