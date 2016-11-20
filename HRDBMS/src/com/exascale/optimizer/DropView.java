package com.exascale.optimizer;

public class DropView extends SQLStatement
{
	private final TableName view;

	public DropView(final TableName view)
	{
		this.view = view;
	}

	public TableName getView()
	{
		return view;
	}
}
