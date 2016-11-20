package com.exascale.logging;

import java.nio.ByteBuffer;
import java.util.ArrayList;

public class PrepareLogRec extends LogRec
{
	public PrepareLogRec(final long txnum, final ArrayList<Integer> nodes)
	{
		super(LogRec.PREPARE, txnum, ByteBuffer.allocate(((nodes.size() + 1) << 2) + 28));
		this.buffer().position(28);
		this.buffer().putInt(nodes.size());
		for (final Integer i : nodes)
		{
			this.buffer().putInt(i);
		}
	}

	public ArrayList<Integer> getNodes()
	{
		buffer.position(28);
		int size = buffer.getInt();
		final ArrayList<Integer> retval = new ArrayList<Integer>(size);
		while (size > 0)
		{
			retval.add(buffer.getInt());
			size--;
		}

		return retval;
	}
}
