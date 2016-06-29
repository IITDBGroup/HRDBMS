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

	public HRDBMSResultSetMetaData(HashMap<String, Integer> cols2Pos, TreeMap<Integer, String> pos2Cols, HashMap<String, String> cols2Types)
	{
		this.cols2Pos = cols2Pos;
		this.pos2Cols = pos2Cols;
		this.cols2Types = cols2Types;
	}

	@Override
	public String getCatalogName(int column) throws SQLException
	{
		return "";
	}

	@Override
	public String getColumnClassName(int column) throws SQLException
	{
		String name = pos2Cols.get(column - 1);
		String type = cols2Types.get(name);

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
	public int getColumnDisplaySize(int column) throws SQLException
	{
		String name = pos2Cols.get(column - 1);
		int nameLen = name.length();

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
	public String getColumnLabel(int column) throws SQLException
	{
		return pos2Cols.get(column - 1);
	}

	@Override
	public String getColumnName(int column) throws SQLException
	{
		return pos2Cols.get(column - 1);
	}

	@Override
	public int getColumnType(int column) throws SQLException
	{
		String name = pos2Cols.get(column - 1);
		String type = cols2Types.get(name);

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
	public String getColumnTypeName(int column) throws SQLException
	{
		return cols2Types.get(pos2Cols.get(column - 1));
	}

	@Override
	public int getPrecision(int column) throws SQLException
	{
		return 0;
	}

	@Override
	public int getScale(int column) throws SQLException
	{
		return 0;
	}

	@Override
	public String getSchemaName(int column) throws SQLException
	{
		return "";
	}

	@Override
	public String getTableName(int column) throws SQLException
	{
		return "";
	}

	@Override
	public boolean isAutoIncrement(int column) throws SQLException
	{
		return false;
	}

	@Override
	public boolean isCaseSensitive(int column) throws SQLException
	{
		return false;
	}

	@Override
	public boolean isCurrency(int column) throws SQLException
	{
		return false;
	}

	@Override
	public boolean isDefinitelyWritable(int column) throws SQLException
	{
		return false;
	}

	@Override
	public int isNullable(int column) throws SQLException
	{
		return ResultSetMetaData.columnNullableUnknown;
	}

	@Override
	public boolean isReadOnly(int column) throws SQLException
	{
		return false;
	}

	@Override
	public boolean isSearchable(int column) throws SQLException
	{
		return true;
	}

	@Override
	public boolean isSigned(int column) throws SQLException
	{
		return true;
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException
	{
		return false;
	}

	@Override
	public boolean isWritable(int column) throws SQLException
	{
		return true;
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException
	{
		throw new SQLException("Unwrap() is not supported.");
	}

}
