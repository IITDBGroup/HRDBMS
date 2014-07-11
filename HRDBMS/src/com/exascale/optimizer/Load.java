package com.exascale.optimizer;

public class Load extends SQLStatement
{
	private TableName table;
	private boolean replace;
	private String delimiter;
	private String glob;
	
	public Load(TableName table, boolean replace, String delimiter, String glob)
	{
		this.table = table;
		this.replace = replace;
		this.delimiter = delimiter;
		this.glob = glob;
	}
	
	public TableName getTable()
	{
		return table;
	}
	
	public boolean isReplace()
	{
		return replace;
	}
	
	public String getDelimiter()
	{
		return delimiter;
	}
	
	public String getGlob()
	{
		return glob;
	}
}
