package com.exascale.optimizer.testing;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Vector;

public abstract class JoinOperator implements Operator
{
	public abstract void addJoinCondition(Vector<Filter> filters);
	public abstract void addJoinCondition(String left, String right);
	public abstract HashSet<HashMap<Filter, Filter>> getHSHMFilter();
	public static JoinOperator manufactureJoin(JoinOperator prod, SelectOperator select, MetaData meta)
	{
		Vector<Filter> filters = select.getFilter();
		if (prod instanceof ProductOperator)
		{
			if (filters.size() == 1)
			{
				Filter filter = filters.get(0);
				if (filter.op().equals("E"))
				{
					//hash join
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
					Filter filter = filters.get(0);
					if (filter.op().equals("E"))
					{
						//hash join
						prod.addJoinCondition(filter.leftColumn(), filter.rightColumn());
						return prod;
					}
				}
				
				NestedLoopJoinOperator retval = new NestedLoopJoinOperator(prod);
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
}
