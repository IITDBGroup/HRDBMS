package com.exascale.optimizer;

import java.util.ArrayList;

public class PrimaryKey
{
	private ArrayList<Column> cols;
	
	public PrimaryKey(ArrayList<Column> cols)
	{
		this.cols = cols;
	}
	
	public ArrayList<Column> getCols()
	{
		return cols;
	}
}
