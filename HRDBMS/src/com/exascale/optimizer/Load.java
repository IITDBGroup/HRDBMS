package com.exascale.optimizer;

public class Load extends SQLStatement
{
	private final TableName table;
	private final boolean replace;
	private final String delimiter;
	private final String glob;

	public Load(final TableName table, final boolean replace, final String delimiter, final String glob)
	{
		this.table = table;
		this.replace = replace;
		this.delimiter = delimiter;
		this.glob = glob;
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
}
