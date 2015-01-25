package com.exascale.optimizer;

public class Case
{
	private SearchCondition cond;
	private Expression result;
	
	public Case(SearchCondition cond, Expression result)
	{
		this.cond = cond;
		this.result = result;
	}
	
	public SearchCondition getCondition()
	{
		return cond;
	}
	
	public Expression getResult()
	{
		return result;
	}
	
	public Case clone()
	{
		return new Case(cond.clone(), result.clone());
	}
	
	public boolean equals(Object o)
	{
		if (!(o instanceof Case))
		{
			return false;
		}
		
		Case rhs = (Case)o;
		
		return cond.equals(rhs.cond) && result.equals(rhs.result);
	}
}
