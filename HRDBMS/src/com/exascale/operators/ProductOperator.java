package com.exascale.operators;

import com.exascale.managers.HRDBMSWorker;
import com.exascale.tables.Plan;
import com.exascale.tables.Transaction;

public class ProductOperator implements Operator
{
	private Operator lchild, rchild;
	private long cost;
	private Plan plan;
	private DiskBackedVector rvector = null;
	private int rVectorPos = 0;
	
	public boolean next()
	{
		if (rvector == null)
		{
			HRDBMSWorker.addThread(new ProductFillRVector());
			rVectorPos = 0;
			return lchild.next();
		}
		
		rVectorPos++;
		if (rVectorPos < rvector.size())
		{
			return true;
		}
		
		while (!rvector.complete())
		{
			if (rVectorPos < rvector.size())
			{
				return true;
			}
		}
		
		rVectorPos = 0;
		return lchild.next();
	}
	
	public void close()
	{
		lchild.close();
		rchild.close();
	}
	
	public long cost()
	{
		return cost;
	}
	
	public Object getVal(int index)
	{
		if (index <= lchild.getNumCols())
		{
			return lchild.getVal(index);
		}
		else
		{
			Object o = rvector.get(rVectorPos);
			if (o instanceof Exception)
			{
				return o;
			}
			
			((Vector)o).get(index - lchild.getNumCols());
		}
	}
	
	public int getNumCols()
	{
		return lchild.getNumCols() + rchild.getNumCols();
	}
	
	public void start(Transaction tx)
	{
		lchild.start(tx);
		rchild.start(tx);
	}
	
	public void addChild(Operator child)
	{
		if (lchild == null)
		{
			lchild = child;
		}
		
		if (rchild == null)
		{
			rchild = child;
		}
		
		throw new IllegalArgumentException("ProductOperator can only have 2 children.");
	}
	
	public void setCost(long cost)
	{
		this.cost = cost;
	}
	
	public void setPlan(Plan p)
	{
		plan = p;
	}
}
