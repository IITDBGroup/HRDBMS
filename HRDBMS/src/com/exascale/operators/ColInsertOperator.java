package com.exascale.operators;

import com.exascale.tables.Plan;
import com.exascale.tables.Transaction;

public class ColInsertOperator implements Operator
{

	@Override
	public boolean next() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public long cost() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Object getVal(int index) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getNumCols() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void start(Transaction tx) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void addChild(Operator child) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setCost(long cost) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setPlan(Plan p) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Object getRow() {
		// TODO Auto-generated method stub
		return null;
	}

}
