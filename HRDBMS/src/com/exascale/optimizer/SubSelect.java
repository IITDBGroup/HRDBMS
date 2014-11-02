package com.exascale.optimizer;

public class SubSelect
{
	private SelectClause select;
	private FromClause from;
	private Where where;
	private GroupBy groupBy;
	private Having having;
	private OrderBy orderBy;
	private FetchFirst fetchFirst;
	
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
	
	public void setWhere(Where where)
	{
		this.where = where;
	}
	
	public void setGroupBy(GroupBy groupBy)
	{
		this.groupBy = groupBy;
	}
	
	public FromClause getFrom()
	{
		return from;
	}
	
	public Where getWhere()
	{
		return where;
	}
	
	public OrderBy getOrderBy()
	{
		return orderBy;
	}
	
	public FetchFirst getFetchFirst()
	{
		return fetchFirst;
	}
	
	public Having getHaving()
	{
		return having;
	}
	
	public SelectClause getSelect()
	{
		return select;
	}
	
	public GroupBy getGroupBy()
	{
		return groupBy;
	}
}
