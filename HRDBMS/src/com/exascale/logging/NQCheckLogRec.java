package com.exascale.logging;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

public class NQCheckLogRec extends LogRec
{
	private final Set<Long> txs;

	public NQCheckLogRec(final Set<Long> txs)
	{
		super(LogRec.NQCHECK, -1, ByteBuffer.allocate(32 + 8 * txs.size()));
		this.txs = txs;
		this.buffer().position(28);
		this.buffer().putInt(txs.size());

		for (final Long txnum : txs)
		{
			this.buffer().putLong(txnum);
		}
	}

	public Set<Long> getOpenTxs()
	{
		return txs;
	}
}
