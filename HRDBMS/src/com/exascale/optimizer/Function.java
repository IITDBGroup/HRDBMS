package com.exascale.optimizer;

import java.util.ArrayList;

public class Function
{
	private final String name;
	private final ArrayList<Expression> args;
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

	@Override
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

	@Override
	public boolean equals(Object rhs)
	{
		if (!(rhs instanceof Function))
		{
			return false;
		}

		Function r = (Function)rhs;
		return name.equals(r.name) && args.equals(r.args) && distinct == r.distinct;
	}

	public ArrayList<Expression> getArgs()
	{
		return args;
	}

	public boolean getDistinct()
	{
		return distinct;
	}

	public String getName()
	{
		return name;
	}

	@Override
	public int hashCode()
	{
		int hash = 23;
		hash = hash * 31 + name.hashCode();
		hash = hash * 31 + args.hashCode();
		hash = hash * 31 + (distinct ? 1 : 0);
		return hash;
	}
}
