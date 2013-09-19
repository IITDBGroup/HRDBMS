package com.exascale.operators;

import com.exascale.tables.Plan;
import com.exascale.tables.Schema.FieldValue;
import com.exascale.tables.Transaction;
import com.exascale.tables.Schema.BigintFV;

public class CountOperator implements Operator
{
	private boolean next = true;
	private Operator child;
	private long cost;
	private Plan plan;
	
	public CountOperator()
	{
		//NO GROUP BY counts non null first column
	}
	
	//TODO count different column
	
	public void setPlan(Plan p)
	{
		plan = p;
	}
	
	public boolean next()
	{
		boolean oldNext = next;
		next = false;
		return oldNext;
	}
	
	public void close()
	{
		child.close();
	}
	
	public long cost()
	{
		return cost;
	}
	
	public Object getVal(int index)
	{
		long i = 0;
		while (child.next())
		{
			Object o = child.getVal(1);
			if (o instanceof Exception)
			{
				return o;
			}
			
			if (!((FieldValue)o).isNull())
			{
				i++;
			}
		}
		
		return new BigintFV(new Long(1));
	}
	
	public int getNumCols()
	{
		return 1;
	}
	
	public void start(Transaction tx)
	{
		child.start(tx);
	}
	
	public void addChild(Operator child)
	{
		this.child = child;
	}
	
	public void setCost(long cost)
	{
		this.cost = cost;
	}
}
