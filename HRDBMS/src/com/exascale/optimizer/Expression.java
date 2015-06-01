package com.exascale.optimizer;

import java.util.ArrayList;

public class Expression
{
	private Literal literal;
	private final boolean isLiteral;
	private Column column;
	private final boolean isColumn;
	private final boolean isCountStar;
	private Function function;
	private final boolean isFunction;
	private final boolean isSelect;
	private Expression lhs;
	private String op;
	private Expression rhs;
	private SubSelect select;
	private ArrayList<Expression> list;
	private final boolean isList;
	private ArrayList<Case> cases;
	private Expression defaultResult;
	private final boolean isCase;

	public Expression()
	{
		isLiteral = false;
		isColumn = false;
		isCountStar = true;
		isFunction = false;
		isSelect = false;
		isList = false;
		isCase = false;
	}

	public Expression(ArrayList<Case> cases, Expression defaultResult)
	{
		this.cases = cases;
		this.defaultResult = defaultResult;
		isLiteral = false;
		isColumn = false;
		isCountStar = false;
		isFunction = false;
		isSelect = false;
		isList = false;
		isCase = true;
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
		isCase = false;
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
		isCase = false;
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
		isCase = false;
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
		isCase = false;
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
		isCase = false;
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
		isCase = false;
	}

	@Override
	public Expression clone()
	{
		if (isLiteral)
		{
			return new Expression(literal.clone());
		}

		if (isColumn)
		{
			return new Expression(column.clone());
		}

		if (isCountStar)
		{
			return new Expression();
		}

		if (isFunction)
		{
			return new Expression(function.clone());
		}

		if (isSelect)
		{
			return new Expression(select.clone());
		}

		if (isList)
		{
			ArrayList<Expression> newList = new ArrayList<Expression>();
			for (Expression e : list)
			{
				newList.add(e.clone());
			}

			return new Expression(newList);
		}

		if (isCase)
		{
			ArrayList<Case> newCases = new ArrayList<Case>();
			for (Case c : cases)
			{
				newCases.add(c.clone());
			}

			return new Expression(newCases, defaultResult.clone());
		}

		return new Expression(lhs.clone(), op, rhs.clone());
	}

	@Override
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

		if (cases == null)
		{
			if (rhs.cases != null)
			{
				return false;
			}
		}
		else
		{
			if (!cases.equals(rhs.cases))
			{
				return false;
			}
		}

		if (defaultResult == null)
		{
			if (rhs.defaultResult != null)
			{
				return false;
			}
		}
		else
		{
			if (!defaultResult.equals(rhs.defaultResult))
			{
				return false;
			}
		}

		if (isCase != rhs.isCase)
		{
			return false;
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

	public ArrayList<Case> getCases()
	{
		return cases;
	}

	public Column getColumn()
	{
		return column;
	}

	public Expression getDefault()
	{
		return defaultResult;
	}

	public Function getFunction()
	{
		return function;
	}

	public Expression getLHS()
	{
		return lhs;
	}

	public ArrayList<Expression> getList()
	{
		return list;
	}

	public Literal getLiteral()
	{
		return literal;
	}

	public String getOp()
	{
		return op;
	}

	public Expression getRHS()
	{
		return rhs;
	}

	public SubSelect getSelect()
	{
		return select;
	}

	public boolean isCase()
	{
		return isCase;
	}

	public boolean isColumn()
	{
		return isColumn;
	}

	public boolean isCountStar()
	{
		return isCountStar;
	}

	public boolean isExpression()
	{
		return isLiteral == false && isColumn == false && isCountStar == false && isFunction == false && isSelect == false && isList == false && isCase == false;
	}

	public boolean isFunction()
	{
		return isFunction;
	}

	public boolean isList()
	{
		return isList;
	}

	public boolean isLiteral()
	{
		return isLiteral;
	}

	public boolean isSelect()
	{
		return isSelect;
	}
}
