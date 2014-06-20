package com.exascale.client;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.ByteBuffer;
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
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.misc.AtomicDouble;
import com.exascale.misc.DataEndMarker;
import com.exascale.misc.MyDate;
import java.io.ObjectInputStream;

public class HRDBMSResultSet implements ResultSet
{
	private ArrayList<Object> rs = new ArrayList<Object>();
	private int firstRowIs = 0;
	private int position = -1;
	private boolean closed = false;
	private HRDBMSConnection conn;
	private int fetchSize;
	private boolean wasNull = false;
	private HashMap<String, Integer> cols2Pos;
	private TreeMap<Integer, String> pos2Cols;
	private HashMap<String, String> cols2Types;
	private HRDBMSStatement stmt;
	
	public HRDBMSResultSet(HRDBMSConnection conn, int fetchSize, HRDBMSStatement stmt) throws Exception
	{
		this.conn = conn;
		this.fetchSize = fetchSize;
		this.stmt = stmt;
		requestMetaData();
	}
	
	private void requestMetaData() throws Exception
	{
		byte[] outMsg = "RSMETA          ".getBytes("UTF-8");
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
			
		ObjectInputStream in = new ObjectInputStream(conn.in);
		cols2Pos = (HashMap<String, Integer>)in.readObject();
		pos2Cols = (TreeMap<Integer, String>)in.readObject();
		cols2Types = (HashMap<String, String>)in.readObject();
		in.close();
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
	
	private void getMoreData() throws SQLException
	{
		try
		{
			byte[] outMsg = "NEXT            ".getBytes("UTF-8");
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
			catch(Exception e)
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
				int size = bytesToInt(num);
				byte[] data = new byte[size];
				Object obj = fromBytes(data);
				rs.add(obj);
				i--;
			
				if (obj instanceof DataEndMarker)
				{
					break;
				}
			}
		}
		catch(Exception e)
		{
			if (e instanceof SQLException)
			{
				throw (SQLException)e;
			}
			
			throw new SQLException(e.getMessage());
		}
	}
	
	private int bytesToInt(byte[] val)
	{
		final int ret = java.nio.ByteBuffer.wrap(val).getInt();
		return ret;
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
	
	private final Object fromBytes(byte[] val) throws Exception
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
				final MyDate o = new MyDate(bb.getLong());
				retval.add(o);
			}
			else if (bytes[i + 4] == 4)
			{
				// string
				final int length = bb.getInt();
				final byte[] temp = new byte[length];
				bb.get(temp);
				try
				{
					final String o = new String(temp, "UTF-8");
					retval.add(o);
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.error("", e);
					throw e;
				}
			}
			else if (bytes[i + 4] == 6)
			{
				// AtomicLong
				final long o = bb.getLong();
				retval.add(new AtomicLong(o));
			}
			else if (bytes[i + 4] == 7)
			{
				// AtomicDouble
				final double o = bb.getDouble();
				retval.add(new AtomicDouble(o));
			}
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
	public boolean next() throws SQLException
	{
		if (closed)
		{
			throw new SQLException("next() called on a closed result set");
		}
		
		position++;
		
		if (firstRowIs - 1 + rs.size() < position)
		{
			Object row = rs.get(rs.size()-1);
			if (row instanceof DataEndMarker)
			{
				return false;
			}
			
			//call to get more data
			getMoreData();
			firstRowIs = position;
		}
		
		Object row = rs.get(position - firstRowIs);
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
	public void close() throws SQLException
	{
		if (closed)
		{
			return;
		}
		
		try
		{
			closed = true;
			if (conn.autoCommit || conn.txIsReadOnly)
			{
				conn.commit();
			}
			sendCloseRS();
		}
		catch(Exception e)
		{
			throw new SQLException(e.getMessage());
		}
	}
	
	private void sendCloseRS() throws Exception
	{
		byte[] outMsg = "CLOSERS         ".getBytes("UTF-8");
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

	@Override
	public boolean wasNull() throws SQLException
	{
		if (closed)
		{
			throw new SQLException("wasNull() called on a closed result set");
		}
		
		return wasNull;
	}

	@Override
	public String getString(int columnIndex) throws SQLException
	{
		wasNull = false;
		
		if (closed)
		{
			throw new SQLException("Cannot fetch a value from a closed result set");
		}
		
		Object row = rs.get(position - firstRowIs);
		if (row instanceof DataEndMarker)
		{
			throw new SQLException("Past end of result set");
		}
		
		ArrayList<Object> alo = (ArrayList<Object>)row;
		
		if (columnIndex < 1 || columnIndex > alo.size())
		{
			throw new SQLException("Column index is out of bounds");
		}
		
		Object col = alo.get(columnIndex-1);
		
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
	public boolean getBoolean(int columnIndex) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public byte getByte(int columnIndex) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public short getShort(int columnIndex) throws SQLException
	{
		wasNull = false;
		
		if (closed)
		{
			throw new SQLException("Cannot fetch a value from a closed result set");
		}
		
		Object row = rs.get(position - firstRowIs);
		if (row instanceof DataEndMarker)
		{
			throw new SQLException("Past end of result set");
		}
		
		ArrayList<Object> alo = (ArrayList<Object>)row;
		
		if (columnIndex < 1 || columnIndex > alo.size())
		{
			throw new SQLException("Column index is out of bounds");
		}
		
		Object col = alo.get(columnIndex-1);
		
		if (col == null)
		{
			wasNull = true;
			return 0;
		}
		
		if (!(col instanceof Short))
		{
			throw new SQLException("The column cannot be converted to a short");
		}
		
		return (short)col;	
	}

	@Override
	public int getInt(int columnIndex) throws SQLException
	{
		wasNull = false;
		
		if (closed)
		{
			throw new SQLException("Cannot fetch a value from a closed result set");
		}
		
		Object row = rs.get(position - firstRowIs);
		if (row instanceof DataEndMarker)
		{
			throw new SQLException("Past end of result set");
		}
		
		ArrayList<Object> alo = (ArrayList<Object>)row;
		
		if (columnIndex < 1 || columnIndex > alo.size())
		{
			throw new SQLException("Column index is out of bounds");
		}
		
		Object col = alo.get(columnIndex-1);
		
		if (col == null)
		{
			wasNull = true;
			return 0;
		}
		if (!(col instanceof Integer || col instanceof Short))
		{
			throw new SQLException("The column cannot be converted to an integer");
		}
		
		Number num = (Number)col;
		return num.intValue();
	}

	@Override
	public long getLong(int columnIndex) throws SQLException
	{
		wasNull = false;
		
		if (closed)
		{
			throw new SQLException("Cannot fetch a value from a closed result set");
		}
		
		Object row = rs.get(position - firstRowIs);
		if (row instanceof DataEndMarker)
		{
			throw new SQLException("Past end of result set");
		}
		
		ArrayList<Object> alo = (ArrayList<Object>)row;
		
		if (columnIndex < 1 || columnIndex > alo.size())
		{
			throw new SQLException("Column index is out of bounds");
		}
		
		Object col = alo.get(columnIndex-1);
		
		if (col == null)
		{
			wasNull = true;
			return 0;
		}
		
		if (!(col instanceof Integer || col instanceof Short || col instanceof Long))
		{
			throw new SQLException("The column cannot be converted to a long");
		}
		
		Number num = (Number)col;
		return num.longValue();
	}

	@Override
	public float getFloat(int columnIndex) throws SQLException
	{
		wasNull = false;
		
		if (closed)
		{
			throw new SQLException("Cannot fetch a value from a closed result set");
		}
		
		Object row = rs.get(position - firstRowIs);
		if (row instanceof DataEndMarker)
		{
			throw new SQLException("Past end of result set");
		}
		
		ArrayList<Object> alo = (ArrayList<Object>)row;
		
		if (columnIndex < 1 || columnIndex > alo.size())
		{
			throw new SQLException("Column index is out of bounds");
		}
		
		Object col = alo.get(columnIndex-1);
		
		if (col == null)
		{
			wasNull = true;
			return 0;
		}
		
		if (!(col instanceof Integer || col instanceof Short || col instanceof Long || col instanceof Float || col instanceof Double || col instanceof BigDecimal))
		{
			throw new SQLException("The column is not numeric");
		}
		
		Number num = (Number)col;
		return num.floatValue();
	}

	@Override
	public double getDouble(int columnIndex) throws SQLException
	{
		wasNull = false;
		
		if (closed)
		{
			throw new SQLException("Cannot fetch a value from a closed result set");
		}
		
		Object row = rs.get(position - firstRowIs);
		if (row instanceof DataEndMarker)
		{
			throw new SQLException("Past end of result set");
		}
		
		ArrayList<Object> alo = (ArrayList<Object>)row;
		
		if (columnIndex < 1 || columnIndex > alo.size())
		{
			throw new SQLException("Column index is out of bounds");
		}
		
		Object col = alo.get(columnIndex-1);
		
		if (col == null)
		{
			wasNull = true;
			return 0;
		}
		if (!(col instanceof Integer || col instanceof Short || col instanceof Long || col instanceof Float || col instanceof Double || col instanceof BigDecimal))
		{
			throw new SQLException("The column is not numeric");
		}
		
		Number num = (Number)col;
		return num.doubleValue();
	}

	@Override
	public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public byte[] getBytes(int columnIndex) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Date getDate(int columnIndex) throws SQLException
	{
		wasNull = false;
		
		if (closed)
		{
			throw new SQLException("Cannot fetch a value from a closed result set");
		}
		
		Object row = rs.get(position - firstRowIs);
		if (row instanceof DataEndMarker)
		{
			throw new SQLException("Past end of result set");
		}
		
		ArrayList<Object> alo = (ArrayList<Object>)row;
		
		if (columnIndex < 1 || columnIndex > alo.size())
		{
			throw new SQLException("Column index is out of bounds");
		}
		
		Object col = alo.get(columnIndex-1);
		
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
	public Time getTime(int columnIndex) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Timestamp getTimestamp(int columnIndex) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public InputStream getAsciiStream(int columnIndex) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public InputStream getUnicodeStream(int columnIndex) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public InputStream getBinaryStream(int columnIndex) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public String getString(String columnLabel) throws SQLException
	{
		wasNull = false;
		
		if (closed)
		{
			throw new SQLException("Cannot fetch a value from a closed result set");
		}
		
		Integer columnIndex = cols2Pos.get(columnLabel.toUpperCase());
		if (columnIndex == null)
		{
			throw new SQLException("The column requested does not exist in the result set");
		}
		
		Object row = rs.get(position - firstRowIs);
		if (row instanceof DataEndMarker)
		{
			throw new SQLException("Past end of result set");
		}
		
		ArrayList<Object> alo = (ArrayList<Object>)row;
		
		if (columnIndex < 1 || columnIndex > alo.size())
		{
			throw new SQLException("Column index is out of bounds");
		}
		
		Object col = alo.get(columnIndex-1);
		
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
	public boolean getBoolean(String columnLabel) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public byte getByte(String columnLabel) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public short getShort(String columnLabel) throws SQLException
	{
		wasNull = false;
		
		if (closed)
		{
			throw new SQLException("Cannot fetch a value from a closed result set");
		}
		
		Integer columnIndex = cols2Pos.get(columnLabel.toUpperCase());
		if (columnIndex == null)
		{
			throw new SQLException("The column requested does not exist in the result set");
		}
		
		Object row = rs.get(position - firstRowIs);
		if (row instanceof DataEndMarker)
		{
			throw new SQLException("Past end of result set");
		}
		
		ArrayList<Object> alo = (ArrayList<Object>)row;
		
		if (columnIndex < 1 || columnIndex > alo.size())
		{
			throw new SQLException("Column index is out of bounds");
		}
		
		Object col = alo.get(columnIndex-1);
		
		if (col == null)
		{
			wasNull = true;
			return 0;
		}
		
		if (!(col instanceof Short))
		{
			throw new SQLException("The column cannot be converted to a short");
		}
		
		return (short)col;
	}

	@Override
	public int getInt(String columnLabel) throws SQLException
	{
		wasNull = false;
		
		if (closed)
		{
			throw new SQLException("Cannot fetch a value from a closed result set");
		}
		
		Integer columnIndex = cols2Pos.get(columnLabel.toUpperCase());
		if (columnIndex == null)
		{
			throw new SQLException("The column requested does not exist in the result set");
		}
		
		Object row = rs.get(position - firstRowIs);
		if (row instanceof DataEndMarker)
		{
			throw new SQLException("Past end of result set");
		}
		
		ArrayList<Object> alo = (ArrayList<Object>)row;
		
		if (columnIndex < 1 || columnIndex > alo.size())
		{
			throw new SQLException("Column index is out of bounds");
		}
		
		Object col = alo.get(columnIndex-1);
		
		if (col == null)
		{
			wasNull = true;
			return 0;
		}
		
		if (!(col instanceof Integer || col instanceof Short))
		{
			throw new SQLException("The column cannot be converted to an integer");
		}
		
		Number num = (Number)col;
		return num.intValue();
	}

	@Override
	public long getLong(String columnLabel) throws SQLException
	{
		wasNull = false;
		
		if (closed)
		{
			throw new SQLException("Cannot fetch a value from a closed result set");
		}
		
		Integer columnIndex = cols2Pos.get(columnLabel.toUpperCase());
		if (columnIndex == null)
		{
			throw new SQLException("The column requested does not exist in the result set");
		}
		
		Object row = rs.get(position - firstRowIs);
		if (row instanceof DataEndMarker)
		{
			throw new SQLException("Past end of result set");
		}
		
		ArrayList<Object> alo = (ArrayList<Object>)row;
		
		if (columnIndex < 1 || columnIndex > alo.size())
		{
			throw new SQLException("Column index is out of bounds");
		}
		
		Object col = alo.get(columnIndex-1);
		
		if (col == null)
		{
			wasNull = true;
			return 0;
		}
		
		if (!(col instanceof Integer || col instanceof Short || col instanceof Long))
		{
			throw new SQLException("The column cannot be converted to a long");
		}
		
		Number num = (Number)col;
		return num.longValue();
	}

	@Override
	public float getFloat(String columnLabel) throws SQLException
	{
		wasNull = false;
		
		if (closed)
		{
			throw new SQLException("Cannot fetch a value from a closed result set");
		}
		
		Integer columnIndex = cols2Pos.get(columnLabel.toUpperCase());
		if (columnIndex == null)
		{
			throw new SQLException("The column requested does not exist in the result set");
		}
		
		Object row = rs.get(position - firstRowIs);
		if (row instanceof DataEndMarker)
		{
			throw new SQLException("Past end of result set");
		}
		
		ArrayList<Object> alo = (ArrayList<Object>)row;
		
		if (columnIndex < 1 || columnIndex > alo.size())
		{
			throw new SQLException("Column index is out of bounds");
		}
		
		Object col = alo.get(columnIndex-1);
		
		if (col == null)
		{
			wasNull = true;
			return 0;
		}
		
		if (!(col instanceof Integer || col instanceof Short || col instanceof Long || col instanceof Float || col instanceof Double || col instanceof BigDecimal))
		{
			throw new SQLException("The column is not numeric");
		}
		
		Number num = (Number)col;
		return num.floatValue();
	}

	@Override
	public double getDouble(String columnLabel) throws SQLException
	{
		wasNull = false;
		
		if (closed)
		{
			throw new SQLException("Cannot fetch a value from a closed result set");
		}
		
		Integer columnIndex = cols2Pos.get(columnLabel.toUpperCase());
		if (columnIndex == null)
		{
			throw new SQLException("The column requested does not exist in the result set");
		}
		
		Object row = rs.get(position - firstRowIs);
		if (row instanceof DataEndMarker)
		{
			throw new SQLException("Past end of result set");
		}
		
		ArrayList<Object> alo = (ArrayList<Object>)row;
		
		if (columnIndex < 1 || columnIndex > alo.size())
		{
			throw new SQLException("Column index is out of bounds");
		}
		
		Object col = alo.get(columnIndex-1);
		
		if (col == null)
		{
			wasNull = true;
			return 0;
		}
		
		if (!(col instanceof Integer || col instanceof Short || col instanceof Long || col instanceof Float || col instanceof Double || col instanceof BigDecimal))
		{
			throw new SQLException("The column is not numeric");
		}
		
		Number num = (Number)col;
		return num.doubleValue();
	}

	@Override
	public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public byte[] getBytes(String columnLabel) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Date getDate(String columnLabel) throws SQLException
	{
		wasNull = false;
		
		if (closed)
		{
			throw new SQLException("Cannot fetch a value from a closed result set");
		}
		
		Integer columnIndex = cols2Pos.get(columnLabel.toUpperCase());
		if (columnIndex == null)
		{
			throw new SQLException("The column requested does not exist in the result set");
		}
		
		Object row = rs.get(position - firstRowIs);
		if (row instanceof DataEndMarker)
		{
			throw new SQLException("Past end of result set");
		}
		
		ArrayList<Object> alo = (ArrayList<Object>)row;
		
		if (columnIndex < 1 || columnIndex > alo.size())
		{
			throw new SQLException("Column index is out of bounds");
		}
		
		Object col = alo.get(columnIndex-1);
		
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
	public Time getTime(String columnLabel) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Timestamp getTimestamp(String columnLabel) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public InputStream getAsciiStream(String columnLabel) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public InputStream getUnicodeStream(String columnLabel) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public InputStream getBinaryStream(String columnLabel) throws SQLException
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
	public void clearWarnings() throws SQLException
	{
		if (closed)
		{
			throw new SQLException("clearWarnings() called on a closed result set");
		}
		
	}

	@Override
	public String getCursorName() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
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
	public Object getObject(int columnIndex) throws SQLException
	{
		wasNull = false;
		
		if (closed)
		{
			throw new SQLException("Cannot fetch a value from a closed result set");
		}
		
		Object row = rs.get(position - firstRowIs);
		if (row instanceof DataEndMarker)
		{
			throw new SQLException("Past end of result set");
		}
		
		ArrayList<Object> alo = (ArrayList<Object>)row;
		
		if (columnIndex < 1 || columnIndex > alo.size())
		{
			throw new SQLException("Column index is out of bounds");
		}
		
		Object col = alo.get(columnIndex-1);
		if (col == null)
		{
			wasNull = true;
		}
		
		return col;
	}

	@Override
	public Object getObject(String columnLabel) throws SQLException
	{
		wasNull = false;
		
		if (closed)
		{
			throw new SQLException("Cannot fetch a value from a closed result set");
		}
		
		Integer columnIndex = cols2Pos.get(columnLabel.toUpperCase());
		if (columnIndex == null)
		{
			throw new SQLException("The column requested does not exist in the result set");
		}
		
		Object row = rs.get(position - firstRowIs);
		if (row instanceof DataEndMarker)
		{
			throw new SQLException("Past end of result set");
		}
		
		ArrayList<Object> alo = (ArrayList<Object>)row;
		
		if (columnIndex < 1 || columnIndex > alo.size())
		{
			throw new SQLException("Column index is out of bounds");
		}
		
		Object col = alo.get(columnIndex-1);
		if (col == null)
		{
			wasNull = true;
		}
		
		return col;
	}

	@Override
	public int findColumn(String columnLabel) throws SQLException
	{
		if (closed)
		{
			throw new SQLException("findColumn() called on a closed result set");
		}
		
		Integer pos = cols2Pos.get(columnLabel.toUpperCase());
		if (pos == null)
		{
			throw new SQLException("The column does not exist in the result set");
		}
		
		return pos;
	}

	@Override
	public Reader getCharacterStream(int columnIndex) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Reader getCharacterStream(String columnLabel) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public BigDecimal getBigDecimal(int columnIndex) throws SQLException
	{
		wasNull = false;
		
		if (closed)
		{
			throw new SQLException("Cannot fetch a value from a closed result set");
		}
		
		Object row = rs.get(position - firstRowIs);
		if (row instanceof DataEndMarker)
		{
			throw new SQLException("Past end of result set");
		}
		
		ArrayList<Object> alo = (ArrayList<Object>)row;
		
		if (columnIndex < 1 || columnIndex > alo.size())
		{
			throw new SQLException("Column index is out of bounds");
		}
		
		Object col = alo.get(columnIndex-1);
		
		if (col == null)
		{
			wasNull = true;
			return null;
		}
		
		if (!(col instanceof Integer || col instanceof Short || col instanceof Long || col instanceof Float || col instanceof Double || col instanceof BigDecimal))
		{
			throw new SQLException("The column is not numeric");
		}
		
		Number num = (Number)col;
		return new BigDecimal(num.doubleValue());
	}

	@Override
	public BigDecimal getBigDecimal(String columnLabel) throws SQLException
	{
		wasNull = false;
		
		if (closed)
		{
			throw new SQLException("Cannot fetch a value from a closed result set");
		}
		
		Integer columnIndex = cols2Pos.get(columnLabel.toUpperCase());
		if (columnIndex == null)
		{
			throw new SQLException("The column requested does not exist in the result set");
		}
		
		Object row = rs.get(position - firstRowIs);
		if (row instanceof DataEndMarker)
		{
			throw new SQLException("Past end of result set");
		}
		
		ArrayList<Object> alo = (ArrayList<Object>)row;
		
		if (columnIndex < 1 || columnIndex > alo.size())
		{
			throw new SQLException("Column index is out of bounds");
		}
		
		Object col = alo.get(columnIndex-1);
		
		if (col == null)
		{
			wasNull = true;
			return null;
		}
		
		if (!(col instanceof Integer || col instanceof Short || col instanceof Long || col instanceof Float || col instanceof Double || col instanceof BigDecimal))
		{
			throw new SQLException("The column is not numeric");
		}
		
		Number num = (Number)col;
		return new BigDecimal(num.doubleValue());
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
		
		Object row = rs.get(0);
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
		
		Object row = rs.get(rs.size()-1);
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
		else
		{
			return false;
		}
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
	public void beforeFirst() throws SQLException
	{
		throw new SQLException("Result set is TYPE_FORWARD_ONLY");
	}

	@Override
	public void afterLast() throws SQLException
	{
		throw new SQLException("Result set is TYPE_FORWARD_ONLY");
	}

	@Override
	public boolean first() throws SQLException
	{
		throw new SQLException("Result set is TYPE_FORWARD_ONLY");
	}

	@Override
	public boolean last() throws SQLException
	{
		throw new SQLException("Result set is TYPE_FORWARD_ONLY");
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
		
		Object row = rs.get(position - firstRowIs);
		if (row instanceof DataEndMarker)
		{
			return 0;
		}
		else
		{
			return position+1;
		}
	}

	@Override
	public boolean absolute(int row) throws SQLException
	{
		throw new SQLException("Result set is TYPE_FORWARD_ONLY");
	}

	@Override
	public boolean relative(int rows) throws SQLException
	{
		throw new SQLException("Result set is TYPE_FORWARD_ONLY");
	}

	@Override
	public boolean previous() throws SQLException
	{
		throw new SQLException("Result set is TYPE_FORWARD_ONLY");
	}

	@Override
	public void setFetchDirection(int direction) throws SQLException
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
	public int getFetchDirection() throws SQLException
	{
		if (closed)
		{
			throw new SQLException("getFetchDirection() called on closed result set");
		}
		
		return ResultSet.FETCH_FORWARD;
	}

	@Override
	public void setFetchSize(int rows) throws SQLException
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
	public int getFetchSize() throws SQLException
	{
		if (closed)
		{
			throw new SQLException("getFetchSize() called on closed result set");
		}
		
		return fetchSize;
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
	public int getConcurrency() throws SQLException
	{
		if (closed)
		{
			throw new SQLException("getConcurrency() called on closed result set");
		}
		
		return ResultSet.CONCUR_READ_ONLY;
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
	public boolean rowInserted() throws SQLException
	{
		if (closed)
		{
			throw new SQLException("rowInserted() called on closed result set");
		}
		
		return false;
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
	public void updateNull(int columnIndex) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateBoolean(int columnIndex, boolean x) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateByte(int columnIndex, byte x) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateShort(int columnIndex, short x) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateInt(int columnIndex, int x) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateLong(int columnIndex, long x) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateFloat(int columnIndex, float x) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateDouble(int columnIndex, double x) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateString(int columnIndex, String x) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateBytes(int columnIndex, byte[] x) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateDate(int columnIndex, Date x) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateTime(int columnIndex, Time x) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateObject(int columnIndex, Object x) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateNull(String columnLabel) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateBoolean(String columnLabel, boolean x) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateByte(String columnLabel, byte x) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateShort(String columnLabel, short x) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateInt(String columnLabel, int x) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateLong(String columnLabel, long x) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateFloat(String columnLabel, float x) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateDouble(String columnLabel, double x) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateString(String columnLabel, String x) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateBytes(String columnLabel, byte[] x) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateDate(String columnLabel, Date x) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateTime(String columnLabel, Time x) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateObject(String columnLabel, Object x) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void insertRow() throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateRow() throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void deleteRow() throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void refreshRow() throws SQLException
	{
		throw new SQLException("Result set is TYPE_FORWARD_ONLY");
	}

	@Override
	public void cancelRowUpdates() throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void moveToInsertRow() throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void moveToCurrentRow() throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
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
	public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Ref getRef(int columnIndex) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Blob getBlob(int columnIndex) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Clob getClob(int columnIndex) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Array getArray(int columnIndex) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Ref getRef(String columnLabel) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Blob getBlob(String columnLabel) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Clob getClob(String columnLabel) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Array getArray(String columnLabel) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Date getDate(int columnIndex, Calendar cal) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Date getDate(String columnLabel, Calendar cal) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Time getTime(int columnIndex, Calendar cal) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Time getTime(String columnLabel, Calendar cal) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public URL getURL(int columnIndex) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public URL getURL(String columnLabel) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateRef(int columnIndex, Ref x) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
		
	}

	@Override
	public void updateRef(String columnLabel, Ref x) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateBlob(int columnIndex, Blob x) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateBlob(String columnLabel, Blob x) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateClob(int columnIndex, Clob x) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateClob(String columnLabel, Clob x) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateArray(int columnIndex, Array x) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateArray(String columnLabel, Array x) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public RowId getRowId(int columnIndex) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public RowId getRowId(String columnLabel) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateRowId(int columnIndex, RowId x) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateRowId(String columnLabel, RowId x) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
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
	public boolean isClosed() throws SQLException
	{
		return closed;
	}

	@Override
	public void updateNString(int columnIndex, String nString) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateNString(String columnLabel, String nString) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateNClob(int columnIndex, NClob nClob) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateNClob(String columnLabel, NClob nClob) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public NClob getNClob(int columnIndex) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public NClob getNClob(String columnLabel) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public SQLXML getSQLXML(int columnIndex) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public SQLXML getSQLXML(String columnLabel) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public String getNString(int columnIndex) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public String getNString(String columnLabel) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Reader getNCharacterStream(int columnIndex) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Reader getNCharacterStream(String columnLabel) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateClob(int columnIndex, Reader reader, long length) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateClob(String columnLabel, Reader reader, long length) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateCharacterStream(int columnIndex, Reader x) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateClob(int columnIndex, Reader reader) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateClob(String columnLabel, Reader reader) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateNClob(int columnIndex, Reader reader) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public void updateNClob(String columnLabel, Reader reader) throws SQLException
	{
		throw new SQLException("Result set is CONCUR_READ_ONLY");
	}

	@Override
	public <T> T getObject(int columnIndex, Class<T> type) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public <T> T getObject(String columnLabel, Class<T> type) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

}
