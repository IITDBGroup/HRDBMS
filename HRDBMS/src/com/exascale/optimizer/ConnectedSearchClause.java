package com.exascale.optimizer;

public class ConnectedSearchClause
{
	private boolean and;
	private SearchClause search;
	
	public ConnectedSearchClause clone()
	{
		return new ConnectedSearchClause(search.clone(), and);
	}
	
	public ConnectedSearchClause(SearchClause search, boolean and)
	{
		this.search = search;
		this.and = and;
	}
	
	public SearchClause getSearch()
	{
		return search;
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
