package com.exascale.optimizer;

import java.util.ArrayList;
import java.util.List;

public class Update extends SQLStatement
{
	private final TableName table;
	private List<Column> cols;
	private Expression exp;
	private Where where;
	private final boolean multi;
	private List<List<Column>> cols2;
	private List<Expression> exps2;
	private List<Where> wheres2;

	public Update(final TableName table, final List<List<Column>> cols2, final List<Expression> exps2, final List<Where> wheres2)
	{
		this.table = table;
		this.cols2 = cols2;
		this.exps2 = exps2;
		this.wheres2 = wheres2;
		this.multi = true;
	}

	public Update(final TableName table, final List<Column> cols, final Expression exp, final Where where)
	{
		this.table = table;
		this.cols = cols;
		this.exp = exp;
		this.where = where;
		this.multi = false;
	}

	public List<Column> getCols()
	{
		return cols;
	}

	public List<List<Column>> getCols2()
	{
		return cols2;
	}

	public Expression getExpression()
	{
		return exp;
	}

	public List<Expression> getExps2()
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

	public List<Where> getWheres2()
	{
		return wheres2;
	}

	public boolean isMulti()
	{
		return multi;
	}
}
