package com.exascale.optimizer;

public class ConnectedSelect
{
	private String combo;
	private SubSelect sub;
	private FullSelect full;
	
	public ConnectedSelect(SubSelect sub, String combo)
	{
		this.combo = combo;
		this.sub = sub;
	}
	
	public ConnectedSelect(FullSelect full, String combo)
	{
		this.combo = combo;
		this.full = full;
	}
	
	public FullSelect getFull()
	{
		return full;
	}
	
	public SubSelect getSub()
	{
		return sub;
	}
	
	public String getCombo()
	{
		return combo;
	}
}
