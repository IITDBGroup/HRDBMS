package com.exascale.optimizer;

public class ConnectedSearchClause
{
	private boolean and;
	private SearchClause search;

	public ConnectedSearchClause(SearchClause search, boolean and)
	{
		this.search = search;
		this.and = and;
	}

	@Override
	public ConnectedSearchClause clone()
	{
		return new ConnectedSearchClause(search.clone(), and);
	}

	@Override
	public boolean equals(Object o)
	{
		if (!(o instanceof ConnectedSearchClause))
		{
			return false;
		}

		ConnectedSearchClause rhs = (ConnectedSearchClause)o;
		return and == rhs.and && search.equals(rhs.search);
	}

	public SearchClause getSearch()
	{
		return search;
	}

	@Override
	public int hashCode()
	{
		int hash = 23;
		hash = hash * 31 + (and ? 1 : 0);
		hash = hash * 31 + search.hashCode();
		return hash;
	}

	public boolean isAnd()
	{
		return and;
	}

	public void setAnd(boolean and)
	{
		this.and = and;
	}

	public void setSearch(SearchClause search)
	{
		this.search = search;
	}
}
