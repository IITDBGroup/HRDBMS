package com.exascale.operators;

import com.exascale.tables.Plan;
import com.exascale.tables.Transaction;

public interface Operator 
{
	public boolean next();
	public void close();
	public long cost();
	public Object getVal(int index);
	public int getNumCols();
	public void start(Transaction tx);
	public void addChild(Operator child);
	public void setCost(long cost);
	public void setPlan(Plan p);
	public Object getRow();
}
