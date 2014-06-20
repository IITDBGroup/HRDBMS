package com.exascale.optimizer;

public class Predicate
{
	private Expression lhs;
	private String op;
	private Expression rhs;
	
	protected Predicate()
	{}
	
	public Predicate(Expression lhs, String op, Expression rhs)
	{
		this.lhs = lhs;
		this.op = op;
		this.rhs = rhs;
	}
	
	public Expression getLHS()
	{
		return lhs;
	}
	
	public String getOp()
	{
		return op;
	}
	
	public Expression getRHS()
	{
		return rhs;
	}
}
