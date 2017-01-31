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
	private final int type;
	private ArrayList<Integer> colOrder;
	private ArrayList<Integer> organization;

	public CreateTable(final TableName table, final ArrayList<ColDef> cols, final PrimaryKey pk, final String nodeGroupExp, final String nodeExp, final String deviceExp, final int type)
	{
		this.table = table;
		this.cols = cols;
		this.pk = pk;
		this.nodeGroupExp = nodeGroupExp;
		this.nodeExp = nodeExp;
		this.deviceExp = deviceExp;
		this.type = type;
	}

	public CreateTable(final TableName table, final ArrayList<ColDef> cols, final PrimaryKey pk, final String nodeGroupExp, final String nodeExp, final String deviceExp, final int type, final ArrayList<Integer> colOrder)
	{
		this.table = table;
		this.cols = cols;
		this.pk = pk;
		this.nodeGroupExp = nodeGroupExp;
		this.nodeExp = nodeExp;
		this.deviceExp = deviceExp;
		this.type = type;
		this.colOrder = colOrder;
	}

	public ArrayList<Integer> getColOrder()
	{
		return colOrder;
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

	public ArrayList<Integer> getOrganization()
	{
		return organization;
	}

	public PrimaryKey getPK()
	{
		return pk;
	}

	public TableName getTable()
	{
		return table;
	}

	public int getType()
	{
		return type;
	}

	public void setOrganization(final ArrayList<Integer> organization)
	{
		this.organization = organization;
	}
}
