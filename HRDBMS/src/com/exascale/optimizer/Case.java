package com.exascale.optimizer;

public class Case
{
	private final SearchCondition cond;
	private final Expression result;

	public Case(final SearchCondition cond, final Expression result)
	{
		this.cond = cond;
		this.result = result;
	}

	@Override
	public Case clone()
	{
		return new Case(cond.clone(), result.clone());
	}

	@Override
	public boolean equals(final Object o)
	{
		if (!(o instanceof Case))
		{
			return false;
		}

		final Case rhs = (Case)o;

		return cond.equals(rhs.cond) && result.equals(rhs.result);
	}

	public SearchCondition getCondition()
	{
		return cond;
	}

	public Expression getResult()
	{
		return result;
	}

	@Override
	public int hashCode()
	{
		int hash = 23;
		hash = hash * 31 + cond.hashCode();
		hash = hash * 31 + result.hashCode();
		return hash;
	}
}
