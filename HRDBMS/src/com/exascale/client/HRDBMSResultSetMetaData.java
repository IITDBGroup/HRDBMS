package com.exascale.client;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.TreeMap;

public class HRDBMSResultSetMetaData implements ResultSetMetaData
{
	private final HashMap<String, Integer> cols2Pos;
	private final TreeMap<Integer, String> pos2Cols;
	private final HashMap<String, String> cols2Types;

	public HRDBMSResultSetMetaData(final HashMap<String, Integer> cols2Pos, final TreeMap<Integer, String> pos2Cols, final HashMap<String, String> cols2Types)
	{
		this.cols2Pos = cols2Pos;
		this.pos2Cols = pos2Cols;
		this.cols2Types = cols2Types;
	}

	@Override
	public String getCatalogName(final int column) throws SQLException
	{
		return "";
	}

	@Override
	public String getColumnClassName(final int column) throws SQLException
	{
		final String name = pos2Cols.get(column - 1);
		final String type = cols2Types.get(name);

		if (type.equals("CHAR"))
		{
			return "java.lang.String";
		}
		else if (type.equals("INT"))
		{
			return "java.lang.Integer";
		}
		else if (type.equals("FLOAT"))
		{
			return "java.lang.Double";
		}
		else if (type.equals("LONG"))
		{
			return "java.lang.Long";
		}
		else if (type.equals("DATE"))
		{
			return "java.util.Date";
		}
		else
		{
			throw new SQLException("Unknown data type for column");
		}
	}

	@Override
	public int getColumnCount() throws SQLException
	{
		return cols2Pos.size();
	}

	@Override
	public int getColumnDisplaySize(final int column) throws SQLException
	{
		final String name = pos2Cols.get(column - 1);
		final int nameLen = name.length();

		if (nameLen < 16)
		{
			return 16;
		}
		else
		{
			return nameLen;
		}
	}

	@Override
	public String getColumnLabel(final int column) throws SQLException
	{
		return pos2Cols.get(column - 1);
	}

	@Override
	public String getColumnName(final int column) throws SQLException
	{
		return pos2Cols.get(column - 1);
	}

	@Override
	public int getColumnType(final int column) throws SQLException
	{
		final String name = pos2Cols.get(column - 1);
		final String type = cols2Types.get(name);

		if (type.equals("CHAR"))
		{
			return java.sql.Types.VARCHAR;
		}
		else if (type.equals("INT"))
		{
			return java.sql.Types.INTEGER;
		}
		else if (type.equals("FLOAT"))
		{
			return java.sql.Types.DOUBLE;
		}
		else if (type.equals("LONG"))
		{
			return java.sql.Types.BIGINT;
		}
		else if (type.equals("DATE"))
		{
			return java.sql.Types.DATE;
		}
		else
		{
			throw new SQLException("Unknown data type for column");
		}
	}

	@Override
	public String getColumnTypeName(final int column) throws SQLException
	{
		return cols2Types.get(pos2Cols.get(column - 1));
	}

	@Override
	public int getPrecision(final int column) throws SQLException
	{
		return 0;
	}

	@Override
	public int getScale(final int column) throws SQLException
	{
		return 0;
	}

	@Override
	public String getSchemaName(final int column) throws SQLException
	{
		return "";
	}

	@Override
	public String getTableName(final int column) throws SQLException
	{
		return "";
	}

	@Override
	public boolean isAutoIncrement(final int column) throws SQLException
	{
		return false;
	}

	@Override
	public boolean isCaseSensitive(final int column) throws SQLException
	{
		return false;
	}

	@Override
	public boolean isCurrency(final int column) throws SQLException
	{
		return false;
	}

	@Override
	public boolean isDefinitelyWritable(final int column) throws SQLException
	{
		return false;
	}

	@Override
	public int isNullable(final int column) throws SQLException
	{
		return ResultSetMetaData.columnNullableUnknown;
	}

	@Override
	public boolean isReadOnly(final int column) throws SQLException
	{
		return false;
	}

	@Override
	public boolean isSearchable(final int column) throws SQLException
	{
		return true;
	}

	@Override
	public boolean isSigned(final int column) throws SQLException
	{
		return true;
	}

	@Override
	public boolean isWrapperFor(final Class<?> iface) throws SQLException
	{
		return false;
	}

	@Override
	public boolean isWritable(final int column) throws SQLException
	{
		return true;
	}

	@Override
	public <T> T unwrap(final Class<T> iface) throws SQLException
	{
		throw new SQLException("Unwrap() is not supported.");
	}

}
