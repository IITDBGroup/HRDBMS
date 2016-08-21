package com.exascale.optimizer;

import java.util.ArrayList;

public class Update extends SQLStatement
{
	private final TableName table;
	private ArrayList<Column> cols;
	private Expression exp;
	private Where where;
	private boolean multi;
	private ArrayList<ArrayList<Column>> cols2;
	private ArrayList<Expression> exps2;
	private ArrayList<Where> wheres2;

	public Update(TableName table, ArrayList<Column> cols, Expression exp, Where where)
	{
		this.table = table;
		this.cols = cols;
		this.exp = exp;
		this.where = where;
		this.multi = false;
	}
	
	public Update(TableName table, ArrayList<ArrayList<Column>> cols2, ArrayList<Expression> exps2, ArrayList<Where> wheres2)
	{
		this.table = table;
		this.cols2 = cols2;
		this.exps2 = exps2;
		this.wheres2 = wheres2;
		this.multi = true;
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
	
	public boolean isMulti()
	{
		return multi;
	}
	
	public ArrayList<ArrayList<Column>> getCols2()
	{
		return cols2;
	}
	
	public ArrayList<Expression> getExps2()
	{
		return exps2;
	}
	
	public ArrayList<Where> getWheres2()
	{
		return wheres2;
	}
}
