package com.exascale.optimizer;

import java.util.ArrayList;

public class SelectClause
{
	private final boolean selectAll;
	private final boolean selectStar;
	private final ArrayList<SelectListEntry> selectList;

	public SelectClause(boolean selectAll, boolean selectStar, ArrayList<SelectListEntry> selectList)
	{
		this.selectAll = selectAll;
		this.selectStar = selectStar;
		this.selectList = selectList;
	}

	@Override
	public SelectClause clone()
	{
		ArrayList<SelectListEntry> newList = new ArrayList<SelectListEntry>();
		if (selectList != null)
		{
			for (SelectListEntry entry : selectList)
			{
				newList.add(entry.clone());
			}
		}
		else
		{
			newList = null;
		}

		return new SelectClause(selectAll, selectStar, selectList);
	}

	public ArrayList<SelectListEntry> getSelectList()
	{
		return selectList;
	}

	public boolean isSelectAll()
	{
		return selectAll;
	}

	public boolean isSelectStar()
	{
		return selectStar;
	}
}
