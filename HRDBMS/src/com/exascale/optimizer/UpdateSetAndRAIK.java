package com.exascale.optimizer;

import java.util.ArrayList;

public class UpdateSetAndRAIK
{
	private ArrayList<Object> updateSet;
	private RIDAndIndexKeys raik;
	
	public UpdateSetAndRAIK(ArrayList<Object> updateSet, RIDAndIndexKeys raik)
	{
		this.updateSet = updateSet;
		this.raik = raik;
	}
	
	public ArrayList<Object> getUpdateSet()
	{
		return updateSet;
	}
	
	public RIDAndIndexKeys getRAIK()
	{
		return raik;
	}
}
