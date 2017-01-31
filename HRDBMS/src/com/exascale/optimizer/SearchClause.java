package com.exascale.optimizer;

public class SearchClause
{
	private boolean negated;
	private Predicate predicate;
	private SearchCondition condition;

	public SearchClause(final Predicate predicate, final boolean negated)
	{
		this.predicate = predicate;
		this.negated = negated;
	}

	public SearchClause(final SearchCondition condition, final boolean negated)
	{
		this.condition = condition;
		this.negated = negated;
	}
	
	public String toString()
	{
		String retval = " ";
		if (negated)
		{
			retval += "NOT ";
		}
		
		if (predicate != null)
		{
			retval += predicate.toString();
		}
		else
		{
			retval += condition.toString();
		}
		
		return retval;
	}

	@Override
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

	@Override
	public boolean equals(final Object o)
	{
		if (!(o instanceof SearchClause))
		{
			return false;
		}

		final SearchClause rhs = (SearchClause)o;
		if (negated != rhs.negated)
		{
			return false;
		}

		if (predicate == null)
		{
			if (rhs.predicate != null)
			{
				return false;
			}
		}
		else
		{
			if (!predicate.equals(rhs.predicate))
			{
				return false;
			}
		}

		if (condition == null)
		{
			if (rhs.condition != null)
			{
				return false;
			}
		}
		else
		{
			if (!condition.equals(rhs.condition))
			{
				return false;
			}
		}

		return true;
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

	@Override
	public int hashCode()
	{
		int hash = 23;
		hash = hash * 31 + (negated ? 1 : 0);
		hash = hash * 31 + (predicate != null ? predicate.hashCode() : 1);
		hash = hash * 31 + (condition != null ? condition.hashCode() : 1);
		return hash;
	}

	public void setNegated(final boolean negated)
	{
		this.negated = negated;
	}
}
