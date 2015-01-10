package com.exascale.optimizer;

import java.util.ArrayList;

public class FromClause
{
	private ArrayList<TableReference> tables;
	
	public FromClause(ArrayList<TableReference> tables)
	{
		this.tables = tables;
	}
	
	public ArrayList<TableReference> getTables()
	{
		return tables;
	}
	
	public FromClause clone()
	{
		ArrayList<TableReference> newTables = new ArrayList<TableReference>();
		for (TableReference t : tables)
		{
			newTables.add(t.clone());
		}
		
		return new FromClause(newTables);
	}
}
