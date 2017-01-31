package com.exascale.optimizer;

import java.util.ArrayList;

public class SearchCondition
{
	private SearchClause search;
	private ArrayList<ConnectedSearchClause> connected;

	public SearchCondition(final SearchClause search, final ArrayList<ConnectedSearchClause> connected)
	{
		this.search = search;
		this.connected = connected;
	}
	
	public String toString()
	{
		String retval = "(" + search.toString();
		if (connected != null)
		{
			for (ConnectedSearchClause csc : connected)
			{
				retval += csc.toString();
			}
		}
		
		retval += ")";
		return retval;
	}

	@Override
	public SearchCondition clone()
	{
		final ArrayList<ConnectedSearchClause> c = new ArrayList<ConnectedSearchClause>();
		if (connected != null)
		{
			for (final ConnectedSearchClause csc : connected)
			{
				c.add(csc.clone());
			}
		}

		return new SearchCondition(search.clone(), c);
	}

	@Override
	public boolean equals(final Object o)
	{
		if (!(o instanceof SearchCondition))
		{
			return false;
		}

		final SearchCondition rhs = (SearchCondition)o;
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

	public void setClause(final SearchClause search)
	{
		this.search = search;
	}

	public void setConnected(final ArrayList<ConnectedSearchClause> connected)
	{
		this.connected = connected;
	}
}
