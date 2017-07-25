package com.exascale.optimizer;

import java.util.ArrayList;
import java.util.List;

public class Select extends SQLStatement
{
	private final List<CTE> ctes;
	private final FullSelect select;

	public Select(final List<CTE> ctes, final FullSelect select)
	{
		this.ctes = ctes;
		this.select = select;
	}

	/** Returns a list of common table expressions associated with this select */
	public List<CTE> getCTEs()
	{
		return ctes;
	}

	public FullSelect getFullSelect()
	{
		return select;
	}
}
