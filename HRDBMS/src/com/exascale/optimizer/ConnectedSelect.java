package com.exascale.optimizer;

public class ConnectedSelect
{
	private final String combo;
	private SubSelect sub;
	private FullSelect full;

	public ConnectedSelect(FullSelect full, String combo)
	{
		this.combo = combo;
		this.full = full;
	}

	public ConnectedSelect(SubSelect sub, String combo)
	{
		this.combo = combo;
		this.sub = sub;
	}

	@Override
	public ConnectedSelect clone()
	{
		if (sub != null)
		{
			return new ConnectedSelect(sub.clone(), combo);
		}

		return new ConnectedSelect(full.clone(), combo);
	}

	public String getCombo()
	{
		return combo;
	}

	public FullSelect getFull()
	{
		return full;
	}

	public SubSelect getSub()
	{
		return sub;
	}
}
