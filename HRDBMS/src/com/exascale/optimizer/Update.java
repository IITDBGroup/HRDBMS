package com.exascale.optimizer;

import java.util.ArrayList;

public class Update extends SQLStatement
{
	private final TableName table;
	private final ArrayList<Column> cols;
	private final Expression exp;
	private final Where where;

	public Update(TableName table, ArrayList<Column> cols, Expression exp, Where where)
	{
		this.table = table;
		this.cols = cols;
		this.exp = exp;
		this.where = where;
	}

	public ArrayList<Column> getCols()
	{
		return cols;
	}

	public Expression getExpression()
	{
		return exp;
	}

	public TableName getTable()
	{
		return table;
	}

	public Where getWhere()
	{
		return where;
	}
}
