package com.exascale.optimizer;

import java.util.*;

public abstract class JoinOperator implements Operator
{
	public static JoinOperator manufactureJoin(final JoinOperator prod, final SelectOperator select, final MetaData meta) throws Exception
	{
		final List<Filter> filters = select.getFilter();
		if (prod instanceof ProductOperator)
		{
			if (filters.size() == 1)
			{
				final Filter filter = filters.get(0);
				if (filter.op().equals("E"))
				{
					// hash join
					return new HashJoinOperator(filter.leftColumn(), filter.rightColumn(), meta);
				}
			}

			return new NestedLoopJoinOperator(filters, meta);
		}
		else
		{
			if (prod instanceof HashJoinOperator)
			{
				if (filters.size() == 1)
				{
					final Filter filter = filters.get(0);
					if (filter.op().equals("E"))
					{
						// hash join
						prod.addJoinCondition(filter.leftColumn(), filter.rightColumn());
						return prod;
					}
				}

				final NestedLoopJoinOperator retval = new NestedLoopJoinOperator(prod);
				retval.addJoinCondition(filters);
				return retval;
			}
			else
			{
				prod.addJoinCondition(filters);
				return prod;
			}
		}
	}

	public abstract void addJoinCondition(List<Filter> filters);

	public abstract void addJoinCondition(String left, String right) throws Exception;

	@Override
	public abstract JoinOperator clone();

	public abstract Set<Map<Filter, Filter>> getHSHMFilter();

	public abstract boolean getIndexAccess();

	public abstract List<String> getJoinForChild(Operator op);
}
