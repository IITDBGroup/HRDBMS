package com.exascale.optimizer;

import java.util.ArrayList;
import java.util.List;

public class Function
{
	private final String name;
	private final List<Expression> args;
	boolean distinct = false;

	public Function(final String name, final List<Expression> args)
	{
		this.name = name;
		this.args = args;
	}

	public Function(final String name, final List<Expression> args, final boolean distinct)
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

		final List<Expression> newArgs = new ArrayList<Expression>();
		for (final Expression e : args)
		{
			newArgs.add(e.clone());
		}

		return new Function(name, newArgs, distinct);
	}

	@Override
	public boolean equals(final Object rhs)
	{
		if (!(rhs instanceof Function))
		{
			return false;
		}

		final Function r = (Function)rhs;
		return name.equals(r.name) && args.equals(r.args) && distinct == r.distinct;
	}

	public List<Expression> getArgs()
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
