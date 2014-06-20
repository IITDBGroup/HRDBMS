package com.exascale.optimizer;

import java.util.ArrayList;

public class FullSelect
{
	private SubSelect sub;
	private FullSelect full;
	private ArrayList<ConnectedSelect> connected;
	private OrderBy orderBy;
	private FetchFirst fetchFirst;
	private ArrayList<Column> cols;
	
	public FullSelect(SubSelect sub, FullSelect full, ArrayList<ConnectedSelect> connected, OrderBy orderBy, FetchFirst fetchFirst)
	{
		this.sub = sub;
		this.full = full;
		this.connected = connected;
		this.orderBy = orderBy;
		this.fetchFirst = fetchFirst;
	}
	
	public SubSelect getSubSelect()
	{
		return sub;
	}
	
	public FullSelect getFullSelect()
	{
		return full;
	}
	
	public OrderBy getOrderBy()
	{
		return orderBy;
	}
	
	public FetchFirst getFetchFirst()
	{
		return fetchFirst;
	}
	
	public ArrayList<ConnectedSelect> getConnected()
	{
		return connected;
	}
	
	public void addCols(ArrayList<Column> cols)
	{
		this.cols = cols;
	}
	
	public ArrayList<Column> getCols()
	{
		return cols;
	}
}
