package com.exascale.optimizer.testing;

import java.util.ArrayList;

public class NullCNFFilter extends CNFFilter
{
	public boolean passes(ArrayList<Object> row)
	{
		return true;
	}
	
	public boolean passes(ArrayList<Object> lRow, ArrayList<Object> rRow)
	{
		return true;
	}
}
