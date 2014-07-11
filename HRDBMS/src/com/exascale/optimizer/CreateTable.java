package com.exascale.optimizer;

import java.util.ArrayList;

public class CreateTable extends SQLStatement
{
	private TableName table;
	private ArrayList<ColDef> cols;
	private PrimaryKey pk;
	private String nodeGroupExp;
	private String nodeExp;
	private String deviceExp;
	
	public CreateTable(TableName table, ArrayList<ColDef> cols, PrimaryKey pk, String nodeGroupExp, String nodeExp, String deviceExp)
	{
		this.table = table;
		this.cols = cols;
		this.pk = pk;
		this.nodeGroupExp = nodeGroupExp;
		this.nodeExp = nodeExp;
		this.deviceExp = deviceExp;
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
	
	public String getNodeGroupExp()
	{
		return nodeGroupExp;
	}
	
	public String getNodeExp()
	{
		return nodeExp;
	}
	
	public String getDeviceExp()
	{
		return deviceExp;
	}
}
