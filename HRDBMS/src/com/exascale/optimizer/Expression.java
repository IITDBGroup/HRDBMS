package com.exascale.optimizer;

import java.util.ArrayList;

public class Expression
{
	private Literal literal;
	private boolean isLiteral;
	private Column column;
	private boolean isColumn;
	private boolean isCountStar;
	private Function function;
	private boolean isFunction;
	private boolean isSelect;
	private Expression lhs;
	private String op;
	private Expression rhs;
	private SubSelect select;
	private ArrayList<Expression> list;
	private boolean isList;
	
	public boolean equals(Object o)
	{
		if (o == null || !(o instanceof Expression))
		{
			return false;
		}
		
		Expression rhs = (Expression)o;
		
		if (literal == null)
		{
			if (rhs.literal != null)
			{
				return false;
			}
		}
		else
		{
			if (!literal.equals(rhs.literal))
			{
				return false;
			}
		}
		
		if (rhs.isLiteral != isLiteral)
		{
			return false;
		}
		
		if (column == null)
		{
			if (rhs.column != null)
			{
				return false;
			}
		}
		else
		{
			if (!column.equals(rhs.column))
			{
				return false;
			}
		}
		
		if (rhs.isColumn != isColumn)
		{
			return false;
		}
		
		if (rhs.isCountStar != isCountStar)
		{
			return false;
		}
		
		if (function == null)
		{
			if (rhs.function != null)
			{
				return false;
			}
		}
		else
		{
			if (!function.equals(rhs.function))
			{
				return false;
			}
		}
		
		if (rhs.isFunction != isFunction)
		{
			return false;
		}
		
		if (rhs.isSelect != isSelect)
		{
			return false;
		}
		
		if (lhs == null)
		{
			if (rhs.lhs != null)
			{
				return false;
			}
		}
		else
		{
			if (!lhs.equals(rhs.lhs))
			{
				return false;
			}
		}
		
		if (this.rhs == null)
		{
			if (rhs.rhs != null)
			{
				return false;
			}
		}
		else
		{
			if (!this.rhs.equals(rhs.rhs))
			{
				return false;
			}
		}
		
		if (op == null)
		{
			if (rhs.op != null)
			{
				return false;
			}
		}
		else
		{
			if (!op.equals(rhs.op))
			{
				return false;
			}
		}
		
		if (list == null)
		{
			if (rhs.list != null)
			{
				return false;
			}
		}
		else
		{
			if (!list.equals(rhs.list))
			{
				return false;
			}
		}
		
		if (rhs.isList != isList)
		{
			return false;
		}
		
		if (select != null || rhs.select != null)
		{
			return false;
		}
		
		return true;
	}

	public Expression(Literal literal)
	{
		this.literal = literal;
		isLiteral = true;
		isColumn = false;
		isCountStar = false;
		isFunction = false;
		isSelect = false;
		isList = false;
	}
	
	public Expression(Column column)
	{
		this.column = column;
		isLiteral = false;
		isColumn = true;
		isCountStar = false;
		isFunction = false;
		isSelect = false;
		isList = false;
	}
	
	public Expression()
	{
		isLiteral = false;
		isColumn = false;
		isCountStar = true;
		isFunction = false;
		isSelect = false;
		isList = false;
	}
	
	public Expression(Function function)
	{
		this.function = function;
		isLiteral = false;
		isColumn = false;
		isCountStar = false;
		isFunction = true;
		isSelect = false;
		isList = false;
	}
	
	public Expression(Expression lhs, String op, Expression rhs)
	{
		this.lhs = lhs;
		this.op = op;
		this.rhs = rhs;
		isLiteral = false;
		isColumn = false;
		isCountStar = false;
		isFunction = false;
		isSelect = false;
		isList = false;
	}
	
	public Expression(SubSelect select)
	{
		this.select = select;
		isLiteral = false;
		isColumn = false;
		isCountStar = false;
		isFunction = false;
		isSelect = true;
		isList = false;
	}
	
	public Expression(ArrayList<Expression> list)
	{
		this.list = list;
		isLiteral = false;
		isColumn = false;
		isCountStar = false;
		isFunction = false;
		isSelect = false;
		isList = true;
	}
	
	public boolean isExpression()
	{
		return isLiteral == false && isColumn == false && isCountStar == false && isFunction == false && isSelect == false && isList == false;
	}
	
	public boolean isLiteral()
	{
		return isLiteral;
	}
	
	public Literal getLiteral()
	{
		return literal;
	}
	
	public boolean isColumn()
	{
		return isColumn;
	}
	
	public Column getColumn()
	{
		return column;
	}
	
	public boolean isCountStar()
	{
		return isCountStar;
	}
	
	public boolean isFunction()
	{
		return isFunction;
	}
	
	public boolean isList()
	{
		return isList;
	}
	
	public boolean isSelect()
	{
		return isSelect;
	}
	
	public Function getFunction()
	{
		return function;
	}
	
	public Expression getLHS()
	{
		return lhs;
	}
	
	public Expression getRHS()
	{
		return rhs;
	}
	
	public String getOp()
	{
		return op;
	}
	
	public SubSelect getSelect()
	{
		return select;
	}
	
	public ArrayList<Expression> getList()
	{
		return list;
	}
}
