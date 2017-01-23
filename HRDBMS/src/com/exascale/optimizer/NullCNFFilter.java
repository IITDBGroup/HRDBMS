package com.exascale.optimizer;

import java.util.ArrayList;

public final class NullCNFFilter extends CNFFilter
{
	@Override
	public boolean passes(ArrayList<Object> row)
	{
		return true;
	}

	@Override
	public boolean passes(ArrayList<Object> lRow, ArrayList<Object> rRow)
	{
		return true;
	}
}
