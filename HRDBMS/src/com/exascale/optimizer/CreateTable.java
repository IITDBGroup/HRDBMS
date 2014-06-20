package com.exascale.optimizer;

import java.util.ArrayList;

public class CreateTable extends SQLStatement
{
	private TableName table;
	private ArrayList<ColDef> cols;
	private PrimaryKey pk;
	
	public CreateTable(TableName table, ArrayList<ColDef> cols, PrimaryKey pk)
	{
		this.table = table;
		this.cols = cols;
		this.pk = pk;
	}
	
	public TableName getTable()
	{
		return table;
	}
	
	public ArrayList<ColDef> getCols()
	{
		return cols;
	}
	
	public PrimaryKey getPK()
	{
		return pk;
	}
}
