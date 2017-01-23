package com.exascale.optimizer;

public class SubSelect
{
	private final SelectClause select;
	private final FromClause from;
	private Where where;
	private GroupBy groupBy;
	private final Having having;
	private final OrderBy orderBy;
	private final FetchFirst fetchFirst;

	public SubSelect(SelectClause select, FromClause from, Where where, GroupBy groupBy, Having having, OrderBy orderBy, FetchFirst fetchFirst)
	{
		this.select = select;
		this.from = from;
		this.where = where;
		this.groupBy = groupBy;
		this.having = having;
		this.orderBy = orderBy;
		this.fetchFirst = fetchFirst;
	}

	@Override
	public SubSelect clone()
	{
		SelectClause selectClone = null;
		FromClause fromClone = null;
		Where whereClone = null;
		GroupBy groupByClone = null;
		Having havingClone = null;
		OrderBy orderByClone = null;
		FetchFirst fetchFirstClone = null;
		if (select != null)
		{
			selectClone = select.clone();
		}

		if (from != null)
		{
			fromClone = from.clone();
		}

		if (where != null)
		{
			whereClone = where.clone();
		}

		if (groupBy != null)
		{
			groupByClone = groupBy.clone();
		}

		if (having != null)
		{
			havingClone = having.clone();
		}

		if (orderBy != null)
		{
			orderByClone = orderBy.clone();
		}

		if (fetchFirst != null)
		{
			fetchFirstClone = fetchFirst.clone();
		}

		return new SubSelect(selectClone, fromClone, whereClone, groupByClone, havingClone, orderByClone, fetchFirstClone);
	}

	public FetchFirst getFetchFirst()
	{
		return fetchFirst;
	}

	public FromClause getFrom()
	{
		return from;
	}

	public GroupBy getGroupBy()
	{
		return groupBy;
	}

	public Having getHaving()
	{
		return having;
	}

	public OrderBy getOrderBy()
	{
		return orderBy;
	}

	public SelectClause getSelect()
	{
		return select;
	}

	public Where getWhere()
	{
		return where;
	}

	public void setGroupBy(GroupBy groupBy)
	{
		this.groupBy = groupBy;
	}

	public void setWhere(Where where)
	{
		this.where = where;
	}
}
