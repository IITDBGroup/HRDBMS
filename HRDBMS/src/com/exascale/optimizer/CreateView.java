package com.exascale.optimizer;

public class CreateView extends SQLStatement
{
	private TableName view;
	private FullSelect select;
	private String text;
	
	public CreateView(TableName view, FullSelect select, String text)
	{
		this.view = view;
		this.select = select;
		this.text = text;
	}
	
	public TableName getView()
	{
		return view;
	}
	
	public FullSelect getSelect()
	{
		return select;
	}
	
	public String getText()
	{
		return text;
	}
}
