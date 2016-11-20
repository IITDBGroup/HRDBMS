package com.exascale.optimizer;

import java.util.ArrayList;

public class Update extends SQLStatement
{
	private final TableName table;
	private ArrayList<Column> cols;
	private Expression exp;
	private Where where;
	private final boolean multi;
	private ArrayList<ArrayList<Column>> cols2;
	private ArrayList<Expression> exps2;
	private ArrayList<Where> wheres2;

	public Update(final TableName table, final ArrayList<ArrayList<Column>> cols2, final ArrayList<Expression> exps2, final ArrayList<Where> wheres2)
	{
		this.table = table;
		this.cols2 = cols2;
		this.exps2 = exps2;
		this.wheres2 = wheres2;
		this.multi = true;
	}

	public Update(final TableName table, final ArrayList<Column> cols, final Expression exp, final Where where)
	{
		this.table = table;
		this.cols = cols;
		this.exp = exp;
		this.where = where;
		this.multi = false;
	}

	public ArrayList<Column> getCols()
	{
		return cols;
	}

	public ArrayList<ArrayList<Column>> getCols2()
	{
		return cols2;
	}

	public Expression getExpression()
	{
		return exp;
	}

	public ArrayList<Expression> getExps2()
	{
		return exps2;
	}

	public TableName getTable()
	{
		return table;
	}

	public Where getWhere()
	{
		return where;
	}

	public ArrayList<Where> getWheres2()
	{
		return wheres2;
	}

	public boolean isMulti()
	{
		return multi;
	}
}
