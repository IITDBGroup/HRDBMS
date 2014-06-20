package com.exascale.optimizer;

import java.util.ArrayList;

public class GroupBy
{
	private ArrayList<Column> cols;
	
	public GroupBy(ArrayList<Column> cols)
	{
		this.cols = cols;
	}
	
	public ArrayList<Column> getCols()
	{
		return cols;
	}
}
