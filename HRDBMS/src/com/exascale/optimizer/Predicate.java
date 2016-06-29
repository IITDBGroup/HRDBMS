package com.exascale.optimizer;

public class Predicate
{
	private Expression lhs;
	private String op;
	private Expression rhs;

	public Predicate(Expression lhs, String op, Expression rhs)
	{
		this.lhs = lhs;
		this.op = op;
		this.rhs = rhs;
	}

	protected Predicate()
	{
	}

	@Override
	public Predicate clone()
	{
		Expression lClone = null;
		Expression rClone = null;

		if (lhs != null)
		{
			lClone = lhs.clone();
		}

		if (rhs != null)
		{
			rClone = rhs.clone();
		}

		return new Predicate(lClone, op, rClone);
	}

	@Override
	public boolean equals(Object o)
	{
		if (!(o instanceof Predicate))
		{
			return false;
		}

		Predicate rhs2 = (Predicate)o;
		return lhs.equals(rhs2.lhs) && op.equals(rhs2.op) && rhs.equals(rhs2.rhs);
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

	@Override
	public int hashCode()
	{
		int hash = 23;
		hash = hash * 31 + lhs.hashCode();
		hash = hash * 31 + op.hashCode();
		hash = hash * 31 + rhs.hashCode();
		return hash;
	}
}
