package com.exascale.optimizer.load;

import com.exascale.optimizer.SQLStatement;
import com.exascale.optimizer.TableName;

public class Load extends SQLStatement {
	private final TableName table, extTable;
	private final boolean replace;
	private final String delimiter;
	private final String glob;

	public Load(final TableName table, final boolean replace, final String delimiter, final String glob,
				final TableName extTable) {
		this.table = table;
		this.replace = replace;
		this.delimiter = delimiter;
		this.glob = glob;
		this.extTable = extTable;
	}

	public String getDelimiter()
	{
		return delimiter;
	}
	public String getGlob()
	{
		return glob;
	}
	public TableName getTable()
	{
		return table;
	}
	public boolean isReplace()
	{
		return replace;
	}
	public TableName getExtTable() { return extTable; }
}
