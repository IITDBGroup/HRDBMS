package com.exascale.optimizer;

import java.util.ArrayList;
import java.util.List;

public class FullSelect
{
	private final SubSelect sub;
	private final FullSelect full;
	private final List<ConnectedSelect> connected;
	private final OrderBy orderBy;
	private final FetchFirst fetchFirst;
	private List<Column> cols;

	public FullSelect(final SubSelect sub, final FullSelect full, final List<ConnectedSelect> connected, final OrderBy orderBy, final FetchFirst fetchFirst)
	{
		this.sub = sub;
		this.full = full;
		this.connected = connected;
		this.orderBy = orderBy;
		this.fetchFirst = fetchFirst;
	}

	public void addCols(final List<Column> cols)
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
			for (final ConnectedSelect c : connected)
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

		final FullSelect retval = new FullSelect(subClone, fullClone, connectedClone, orderByClone, fetchFirstClone);

		if (cols != null)
		{
			retval.cols = new ArrayList<Column>();
			for (final Column c : cols)
			{
				retval.cols.add(c.clone());
			}
		}

		return retval;
	}

	public List<Column> getCols()
	{
		return cols;
	}

	public List<ConnectedSelect> getConnected()
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
