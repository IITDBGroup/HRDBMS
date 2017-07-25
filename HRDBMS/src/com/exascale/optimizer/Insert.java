package com.exascale.optimizer;

import java.util.ArrayList;
import java.util.List;

public class Insert extends SQLStatement
{
	private final TableName table;
	private List<Expression> exps;
	private List<List<Expression>> mExps;
	private boolean fromSelect = false;
	private FullSelect select;

	public Insert(final TableName table, final List<List<Expression>> mExps, final boolean multi)
	{
		this.table = table;
		this.mExps = mExps;
	}

	public Insert(final TableName table, final List<Expression> exps)
	{
		this.table = table;
		this.exps = exps;
	}

	public Insert(final TableName table, final FullSelect select)
	{
		this.table = table;
		this.select = select;
		fromSelect = true;
	}

	public boolean fromSelect()
	{
		return fromSelect;
	}

	public List<Expression> getExpressions()
	{
		return exps;
	}

	public List<List<Expression>> getMultiExpressions()
	{
		return mExps;
	}

	public FullSelect getSelect()
	{
		return select;
	}

	public TableName getTable()
	{
		return table;
	}

	public boolean isMulti()
	{
		return (mExps != null);
	}
}
