package com.exascale.optimizer;

import java.util.ArrayList;

public class SelectClause
{
	private boolean selectAll;
	private boolean selectStar;
	private ArrayList<SelectListEntry> selectList;
	
	public SelectClause(boolean selectAll, boolean selectStar, ArrayList<SelectListEntry> selectList)
	{
		this.selectAll = selectAll;
		this.selectStar = selectStar;
		this.selectList = selectList;
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
