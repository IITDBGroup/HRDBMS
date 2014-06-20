package com.exascale.optimizer;

public class DropView extends SQLStatement
{
	private TableName view;
	
	public DropView(TableName view)
	{
		this.view = view;
	}
	
	public TableName getView()
	{
		return view;
	}
}
