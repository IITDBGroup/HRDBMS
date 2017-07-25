package com.exascale.optimizer;

import java.util.ArrayList;
import java.util.List;

public final class NullCNFFilter extends CNFFilter
{
	@Override
	public boolean passes(final List<Object> row)
	{
		return true;
	}

	@Override
	public boolean passes(final List<Object> lRow, final List<Object> rRow)
	{
		return true;
	}
}
