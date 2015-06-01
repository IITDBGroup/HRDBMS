package com.exascale.optimizer;

import java.util.ArrayList;

public class CreateTable extends SQLStatement
{
	private final TableName table;
	private final ArrayList<ColDef> cols;
	private final PrimaryKey pk;
	private final String nodeGroupExp;
	private final String nodeExp;
	private final String deviceExp;

	public CreateTable(TableName table, ArrayList<ColDef> cols, PrimaryKey pk, String nodeGroupExp, String nodeExp, String deviceExp)
	{
		this.table = table;
		this.cols = cols;
		this.pk = pk;
		this.nodeGroupExp = nodeGroupExp;
		this.nodeExp = nodeExp;
		this.deviceExp = deviceExp;
	}

	public ArrayList<ColDef> getCols()
	{
		return cols;
	}

	public String getDeviceExp()
	{
		return deviceExp;
	}

	public String getNodeExp()
	{
		return nodeExp;
	}

	public String getNodeGroupExp()
	{
		return nodeGroupExp;
	}

	public PrimaryKey getPK()
	{
		return pk;
	}

	public TableName getTable()
	{
		return table;
	}
}
