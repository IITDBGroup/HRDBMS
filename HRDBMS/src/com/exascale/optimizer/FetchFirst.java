package com.exascale.optimizer;

public class FetchFirst
{
	private long num;
	
	public FetchFirst(long num)
	{
		this.num = num;
	}
	
	public long getNumber()
	{
		return num;
	}
	
	public FetchFirst clone()
	{
		return new FetchFirst(num);
	}
}
