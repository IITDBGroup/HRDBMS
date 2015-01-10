package com.exascale.optimizer;

import java.util.ArrayList;

public class Function
{
	private String name;
	private ArrayList<Expression> args;
	boolean distinct = false;
	
	public Function(String name, ArrayList<Expression> args)
	{
		this.name = name;
		this.args = args;
	}
	
	public Function(String name, ArrayList<Expression> args, boolean distinct)
	{
		this.name = name;
		this.args = args;
		this.distinct = distinct;
	}
	
	public Function clone()
	{
		if (args == null)
		{
			return new Function(name, args, distinct);
		}
		
		ArrayList<Expression> newArgs = new ArrayList<Expression>();
		for (Expression e : args)
		{
			newArgs.add(e.clone());
		}
		
		return new Function(name, newArgs, distinct);
	}
	
	public String getName()
	{
		return name;
	}
	
	public ArrayList<Expression> getArgs()
	{
		return args;
	}
	
	public boolean getDistinct()
	{
		return distinct;
	}
	
	public boolean equals(Object rhs)
	{
		if (!(rhs instanceof Function))
		{
			return false;
		}
		
		Function r = (Function)rhs;
		return name.equals(r.name) && args.equals(r.args) && distinct == r.distinct;
	}
}
