package com.exascale;

public interface Operator 
{
	public void beforeFirst();
	public void afterLast();
	public boolean next();
	public boolean previous();
	public void close();
	public long cost();
}
