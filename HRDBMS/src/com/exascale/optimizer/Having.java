package com.exascale.optimizer;

public class Having
{
	private final SearchCondition search;

	public Having(SearchCondition search)
	{
		this.search = search;
	}

	@Override
	public Having clone()
	{
		return new Having(search.clone());
	}

	public SearchCondition getSearch()
	{
		return search;
	}
}
