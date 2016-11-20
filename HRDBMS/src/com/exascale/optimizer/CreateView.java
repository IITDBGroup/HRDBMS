package com.exascale.optimizer;

public class CreateView extends SQLStatement
{
	private final TableName view;
	private final FullSelect select;
	private final String text;

	public CreateView(final TableName view, final FullSelect select, final String text)
	{
		this.view = view;
		this.select = select;
		this.text = text;
	}

	public FullSelect getSelect()
	{
		return select;
	}

	public String getText()
	{
		return text;
	}

	public TableName getView()
	{
		return view;
	}
}
