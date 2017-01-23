package com.exascale.optimizer;

import java.util.ArrayList;

public class SearchCondition
{
	private SearchClause search;
	private ArrayList<ConnectedSearchClause> connected;

	public SearchCondition(SearchClause search, ArrayList<ConnectedSearchClause> connected)
	{
		this.search = search;
		this.connected = connected;
	}

	@Override
	public SearchCondition clone()
	{
		ArrayList<ConnectedSearchClause> c = new ArrayList<ConnectedSearchClause>();
		if (connected != null)
		{
			for (ConnectedSearchClause csc : connected)
			{
				c.add(csc.clone());
			}
		}

		return new SearchCondition(search.clone(), c);
	}

	@Override
	public boolean equals(Object o)
	{
		if (!(o instanceof SearchCondition))
		{
			return false;
		}

		SearchCondition rhs = (SearchCondition)o;
		if (search.equals(rhs.search))
		{
			if (connected == null || connected.size() == 0)
			{
				if (rhs.connected == null || rhs.connected.size() == 0)
				{
					return true;
				}
				else
				{
					return false;
				}
			}
			else
			{
				if (rhs.connected == null || rhs.connected.size() == 0)
				{
					return false;
				}

				if (connected.equals(rhs.connected))
				{
					return true;
				}
				else
				{
					return false;
				}
			}
		}
		else
		{
			return false;
		}
	}

	public SearchClause getClause()
	{
		return search;
	}

	public ArrayList<ConnectedSearchClause> getConnected()
	{
		return connected;
	}

	public void setClause(SearchClause search)
	{
		this.search = search;
	}

	public void setConnected(ArrayList<ConnectedSearchClause> connected)
	{
		this.connected = connected;
	}
}
