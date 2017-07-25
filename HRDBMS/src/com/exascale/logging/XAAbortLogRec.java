package com.exascale.logging;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class XAAbortLogRec extends LogRec
{
	public XAAbortLogRec(final long txnum, final List<Integer> nodes)
	{
		super(LogRec.XAABORT, txnum, ByteBuffer.allocate(((nodes.size() + 1) << 2) + 28));
		this.buffer().position(28);
		this.buffer().putInt(nodes.size());
		for (final Integer i : nodes)
		{
			this.buffer().putInt(i);
		}
	}

	public List<Integer> getNodes()
	{
		buffer.position(28);
		int size = buffer.getInt();
		final List<Integer> retval = new ArrayList<Integer>(size);
		while (size > 0)
		{
			retval.add(buffer.getInt());
			size--;
		}

		return retval;
	}
}
