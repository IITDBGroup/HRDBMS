package com.exascale.optimizer;

public class ConnectedSelect
{
	private final String combo;
	private SubSelect sub;
	private FullSelect full;

	public ConnectedSelect(final FullSelect full, final String combo)
	{
		this.combo = combo;
		this.full = full;
	}

	public ConnectedSelect(final SubSelect sub, final String combo)
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
