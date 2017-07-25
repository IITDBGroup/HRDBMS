package com.exascale.optimizer;

import java.util.ArrayList;
import java.util.List;

public class FromClause
{
	private final List<TableReference> tables;

	public FromClause(final List<TableReference> tables)
	{
		this.tables = tables;
	}

	@Override
	public FromClause clone()
	{
		final List<TableReference> newTables = new ArrayList<TableReference>();
		for (final TableReference t : tables)
		{
			newTables.add(t.clone());
		}

		return new FromClause(newTables);
	}

	public List<TableReference> getTables()
	{
		return tables;
	}
}
