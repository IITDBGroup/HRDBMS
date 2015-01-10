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
	
	public FullSelect clone()
	{
		SubSelect subClone = null;
		FullSelect fullClone = null;
		ArrayList<ConnectedSelect> connectedClone = null;
		OrderBy orderByClone = null;
		FetchFirst fetchFirstClone = null;
		
		if (sub != null)
		{
			subClone = sub.clone();
		}
		
		if (full != null)
		{
			fullClone = full.clone();
		}
		
		if (connected != null)
		{
			connectedClone = new ArrayList<ConnectedSelect>();
			for (ConnectedSelect c : connected)
			{
				connectedClone.add(c.clone());
			}
		}
		
		if (orderBy != null)
		{
			orderByClone = orderBy.clone();
		}
		
		if (fetchFirst != null)
		{
			fetchFirstClone = fetchFirst.clone();
		}
		
		FullSelect retval = new FullSelect(subClone, fullClone, connectedClone, orderByClone, fetchFirstClone);
		
		if (cols != null)
		{
			retval.cols = new ArrayList<Column>();
			for (Column c : cols)
			{
				retval.cols.add(c.clone());
			}
		}
		
		return retval;
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
