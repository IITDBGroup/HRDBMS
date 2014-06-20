package com.exascale.optimizer;

import java.util.ArrayList;

public class Update extends SQLStatement
{
	private TableName table;
	private ArrayList<Column> cols;
	private Expression exp;
	private Where where;
	
	public Update(TableName table, ArrayList<Column> cols, Expression exp, Where where)
	{
		this.table = table;
		this.cols = cols;
		this.exp = exp;
		this.where = where;
	}
	
	public TableName getTable()
	{
		return table;
	}
	
	public ArrayList<Column> getCols()
	{
		return cols;
	}
	
	public Expression getExpression()
	{
		return exp;
	}
	
	public Where getWhere()
	{
		return where;
	}
}
