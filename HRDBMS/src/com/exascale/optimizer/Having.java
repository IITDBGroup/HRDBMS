package com.exascale.optimizer;

public class Having
{
	private SearchCondition search;
	
	public Having(SearchCondition search)
	{
		this.search = search;
	}
	
	public SearchCondition getSearch()
	{
		return search;
	}
}
