package com.exascale.optimizer;

import java.util.ArrayList;

public class CreateIndex extends SQLStatement
{
	private TableName index;
	private TableName table;
	private ArrayList<IndexDef> cols;
	private boolean unique;
	
	public CreateIndex(TableName index, TableName table, ArrayList<IndexDef> cols, boolean unique)
	{
		this.index = index;
		this.table = table;
		this.cols = cols;
		this.unique = unique;
	}
	
	public TableName getIndex()
	{
		return index;
	}
	
	public TableName getTable()
	{
		return table;
	}
	
	public ArrayList<IndexDef> getCols()
	{
		return cols;
	}
	
	public boolean getUnique()
	{
		return unique;
	}
}
