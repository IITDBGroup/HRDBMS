package com.exascale.optimizer;

import java.util.ArrayList;
import java.util.List;

public class SelectClause
{
	private final boolean selectAll;
	private final boolean selectStar;
	private final List<SelectListEntry> selectList;

	public SelectClause(final boolean selectAll, final boolean selectStar, final List<SelectListEntry> selectList)
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
			for (final SelectListEntry entry : selectList)
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

	public List<SelectListEntry> getSelectList()
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
