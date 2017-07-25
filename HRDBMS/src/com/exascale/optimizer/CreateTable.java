package com.exascale.optimizer;

import java.util.ArrayList;
import java.util.List;

public class CreateTable extends SQLStatement
{
	private final TableName table;
	private final List<ColDef> cols;
	private final PrimaryKey pk;
	private final String nodeGroupExp;
	private final String nodeExp;
	private final String deviceExp;
	private final int type;
	private List<Integer> colOrder;
	private List<Integer> organization;

	public CreateTable(final TableName table, final List<ColDef> cols, final PrimaryKey pk, final String nodeGroupExp, final String nodeExp, final String deviceExp, final int type)
	{
		this.table = table;
		this.cols = cols;
		this.pk = pk;
		this.nodeGroupExp = nodeGroupExp;
		this.nodeExp = nodeExp;
		this.deviceExp = deviceExp;
		this.type = type;
	}

	public CreateTable(final TableName table, final List<ColDef> cols, final PrimaryKey pk, final String nodeGroupExp, final String nodeExp, final String deviceExp, final int type, final List<Integer> colOrder)
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

	public List<Integer> getColOrder()
	{
		return colOrder;
	}

	public List<ColDef> getCols()
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

	public List<Integer> getOrganization()
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

	public void setOrganization(final List<Integer> organization)
	{
		this.organization = organization;
	}
}
