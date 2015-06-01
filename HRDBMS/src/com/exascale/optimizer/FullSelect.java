package com.exascale.optimizer;

import java.util.ArrayList;

public class FullSelect
{
	private final SubSelect sub;
	private final FullSelect full;
	private final ArrayList<ConnectedSelect> connected;
	private final OrderBy orderBy;
	private final FetchFirst fetchFirst;
	private ArrayList<Column> cols;

	public FullSelect(SubSelect sub, FullSelect full, ArrayList<ConnectedSelect> connected, OrderBy orderBy, FetchFirst fetchFirst)
	{
		this.sub = sub;
		this.full = full;
		this.connected = connected;
		this.orderBy = orderBy;
		this.fetchFirst = fetchFirst;
	}

	public void addCols(ArrayList<Column> cols)
	{
		this.cols = cols;
	}

	@Override
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

	public ArrayList<Column> getCols()
	{
		return cols;
	}

	public ArrayList<ConnectedSelect> getConnected()
	{
		return connected;
	}

	public FetchFirst getFetchFirst()
	{
		return fetchFirst;
	}

	public FullSelect getFullSelect()
	{
		return full;
	}

	public OrderBy getOrderBy()
	{
		return orderBy;
	}

	public SubSelect getSubSelect()
	{
		return sub;
	}
}
