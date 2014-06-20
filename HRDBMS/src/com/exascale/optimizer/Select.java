package com.exascale.optimizer;

import java.util.ArrayList;

public class Select extends SQLStatement
{
	private ArrayList<CTE> ctes;
	private FullSelect select;
	
	public Select(ArrayList<CTE> ctes, FullSelect select)
	{
		this.ctes = ctes;
		this.select = select;
	}
	
	public ArrayList<CTE> getCTEs()
	{
		return ctes;
	}
	
	public FullSelect getFullSelect()
	{
		return select;
	}
}
