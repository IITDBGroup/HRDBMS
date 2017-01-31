package com.exascale.optimizer;

import java.util.ArrayList;

public class Insert extends SQLStatement
{
	private final TableName table;
	private ArrayList<Expression> exps;
	private ArrayList<ArrayList<Expression>> mExps;
	private boolean fromSelect = false;
	private FullSelect select;

	public Insert(final TableName table, final ArrayList<ArrayList<Expression>> mExps, final boolean multi)
	{
		this.table = table;
		this.mExps = mExps;
	}

	public Insert(final TableName table, final ArrayList<Expression> exps)
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

	public ArrayList<Expression> getExpressions()
	{
		return exps;
	}

	public ArrayList<ArrayList<Expression>> getMultiExpressions()
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
