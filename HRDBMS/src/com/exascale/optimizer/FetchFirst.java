package com.exascale.optimizer;

public class FetchFirst
{
	private final long num;

	public FetchFirst(final long num)
	{
		this.num = num;
	}

	@Override
	public FetchFirst clone()
	{
		return new FetchFirst(num);
	}

	public long getNumber()
	{
		return num;
	}
}
