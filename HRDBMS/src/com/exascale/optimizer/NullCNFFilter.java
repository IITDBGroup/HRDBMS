package com.exascale.optimizer;

import java.util.ArrayList;

public final class NullCNFFilter extends CNFFilter
{
	@Override
	public boolean passes(final ArrayList<Object> row)
	{
		return true;
	}

	@Override
	public boolean passes(final ArrayList<Object> lRow, final ArrayList<Object> rRow)
	{
		return true;
	}
}
