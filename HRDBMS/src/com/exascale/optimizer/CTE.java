package com.exascale.optimizer;

import java.util.ArrayList;

public class CTE
{
	private String name;
	private ArrayList<Column> cols;
	private FullSelect select;
	
	public CTE(String name, ArrayList<Column> cols, FullSelect select)
	{
		this.name = name;
		this.cols = cols;
		this.select = select;
	}
	
	public String getName()
	{
		return name;
	}
	
	public ArrayList<Column> getCols()
	{
		return cols;
	}
	
	public FullSelect getSelect()
	{
		return select;
	}
}
