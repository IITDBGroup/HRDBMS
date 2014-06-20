package com.exascale.optimizer;

public class TableReference
{
	private SingleTable table;
	private boolean isSingleTable;
	private TableReference lhs;
	private String op;
	private TableReference rhs;
	private SearchCondition search;
	private boolean isSelect;
	private FullSelect select;
	private String alias;
	
	public TableReference(SingleTable table)
	{
		this.table = table;
		isSingleTable = true;
		isSelect = false;
		alias = table.getAlias();
	}
	
	public TableReference(TableReference lhs, String op, TableReference rhs, SearchCondition search, String alias)
	{
		this.lhs = lhs;
		this.op = op;
		this.rhs = rhs;
		this.search = search;
		isSingleTable = false;
		isSelect = false;
		this.alias = alias;
	}
	
	public TableReference(FullSelect select)
	{
		this.select = select;
		isSingleTable = false;
		isSelect = true;
	}
	
	public TableReference(FullSelect select, String alias)
	{
		this.select = select;
		isSingleTable = false;
		isSelect = true;
		this.alias = alias;
	}
	
	public boolean isSingleTable()
	{
		return isSingleTable;
	}
	
	public SingleTable getSingleTable()
	{
		return table;
	}
	
	public boolean isSelect()
	{
		return isSelect;
	}
	
	public FullSelect getSelect()
	{
		return select;
	}
	
	public TableReference getLHS()
	{
		return lhs;
	}
	
	public TableReference getRHS()
	{
		return rhs;
	}
	
	public void removeSingleTable()
	{
		isSingleTable = false;
		table = null;
	}
	
	public void addSelect(FullSelect select)
	{
		isSelect = true;
		this.select = select;
	}
	
	public void setAlias(String alias)
	{
		this.alias = alias;
	}
	
	public String getAlias()
	{
		return alias;
	}
	
	public String getOp()
	{
		return op;
	}
	
	public SearchCondition getSearch()
	{
		return search;
	}
}
