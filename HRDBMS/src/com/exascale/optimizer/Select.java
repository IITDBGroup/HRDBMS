package com.exascale.optimizer;

import java.util.ArrayList;

public class Select extends SQLStatement
{
	private final ArrayList<CTE> ctes;
	private final FullSelect select;

	public Select(final ArrayList<CTE> ctes, final FullSelect select)
	{
		this.ctes = ctes;
		this.select = select;
	}

	/** Returns a list of common table expressions associated with this select */
	public ArrayList<CTE> getCTEs()
	{
		return ctes;
	}

	public FullSelect getFullSelect()
	{
		return select;
	}
}
