package com.exascale.optimizer;

import java.util.ArrayList;

public class FromClause
{
	private final ArrayList<TableReference> tables;

	public FromClause(final ArrayList<TableReference> tables)
	{
		this.tables = tables;
	}

	@Override
	public FromClause clone()
	{
		final ArrayList<TableReference> newTables = new ArrayList<TableReference>();
		for (final TableReference t : tables)
		{
			newTables.add(t.clone());
		}

		return new FromClause(newTables);
	}

	public ArrayList<TableReference> getTables()
	{
		return tables;
	}
}
