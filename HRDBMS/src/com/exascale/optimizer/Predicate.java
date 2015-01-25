package com.exascale.optimizer;

import com.exascale.exceptions.ParseException;
import com.exascale.managers.HRDBMSWorker;

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
	
	public boolean equals(Object o)
	{
		if (!(o instanceof Predicate))
		{
			return false;
		}
		
		Predicate rhs2 = (Predicate)o;
		return lhs.equals(rhs2.lhs) && op.equals(rhs2.op) && rhs.equals(rhs2.rhs);
	}
	
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
