package com.exascale.optimizer;

public class SearchClause
{
	private boolean negated;
	private Predicate predicate;
	private SearchCondition condition;
	
	public SearchClause clone()
	{
		if (predicate != null)
		{
			return new SearchClause(predicate.clone(), negated);
		}
		else
		{
			return new SearchClause(condition.clone(), negated);
		}
	}
	
	public SearchClause(Predicate predicate, boolean negated)
	{
		this.predicate = predicate;
		this.negated = negated;
	}
	
	public SearchClause(SearchCondition condition, boolean negated)
	{
		this.condition = condition;
		this.negated = negated;
	}
	
	public boolean getNegated()
	{
		return negated;
	}
	
	public Predicate getPredicate()
	{
		return predicate;
	}
	
	public SearchCondition getSearch()
	{
		return condition;
	}
	
	public void setNegated(boolean negated)
	{
		this.negated = negated;
	}
}
