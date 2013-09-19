package com.exascale.logging;

import java.nio.ByteBuffer;
import java.util.HashSet;

public class NQCheckLogRec extends LogRec 
{
	private HashSet<Long> txs;
	
	public NQCheckLogRec(HashSet<Long> txs)
	{
		super(LogRec.NQCHECK, -1, ByteBuffer.allocate(32 + 8 * txs.size()));
		this.txs = txs;
		this.buffer().position(28);
		this.buffer().putInt(txs.size());
		
		for (Long txnum : txs)
		{
			this.buffer().putLong(txnum);
		}
	}
	
	public HashSet<Long> getOpenTxs()
	{
		return txs;
	}
}
