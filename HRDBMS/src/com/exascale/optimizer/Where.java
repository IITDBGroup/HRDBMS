package com.exascale.optimizer;

public class Where
{
	private final SearchCondition search;

	public Where(SearchCondition search)
	{
		this.search = search;
	}

	@Override
	public Where clone()
	{
		return new Where(search.clone());
	}

	public SearchCondition getSearch()
	{
		return search;
	}
}
