package com.exascale.logging;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;

public class NQCheckLogRec extends LogRec 
{
	protected ConcurrentHashMap<Long, Long> txs;
	
	public NQCheckLogRec(ConcurrentHashMap<Long, Long> txs)
	{
		super(LogRec.NQCHECK, -1, ByteBuffer.allocate(32 + 8 * txs.size()));
		this.txs = txs;
		this.buffer().position(28);
		this.buffer().putInt(txs.size());
		
		for (Long txnum : txs.keySet())
		{
			this.buffer().putLong(txnum);
		}
	}
	
	public HashSet<Long> getOpenTxs()
	{
		return new HashSet<Long>(txs.keySet());
	}
}
