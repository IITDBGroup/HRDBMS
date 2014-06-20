package com.exascale.optimizer;

import java.util.ArrayList;

public class SearchCondition
{
	private SearchClause search;
	private ArrayList<ConnectedSearchClause> connected;
	
	public SearchCondition clone()
	{
		ArrayList<ConnectedSearchClause> c = new ArrayList<ConnectedSearchClause>();
		if (connected != null)
		{
			for (ConnectedSearchClause csc : connected)
			{
				c.add(csc.clone());
			}
		}
		
		return new SearchCondition(search.clone(), c);
	}
	
	public SearchCondition(SearchClause search, ArrayList<ConnectedSearchClause> connected)
	{
		this.search = search;
		this.connected = connected;
	}
	
	public SearchClause getClause()
	{
		return search;
	}
	
	public void setClause(SearchClause search)
	{
		this.search = search;
	}
	
	public ArrayList<ConnectedSearchClause> getConnected()
	{
		return connected;
	}
	
	public void setConnected(ArrayList<ConnectedSearchClause> connected)
	{
		this.connected = connected;
	}
}
