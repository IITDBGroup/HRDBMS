package com.exascale.misc;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class BloomFilter
{
	private byte[][] bits;
	private ConcurrentHashMap<Long, List<Object>> cache;

	public BloomFilter()
	{
		bits = new byte[32][];
		int i = 0;
		while (i < 32)
		{
			bits[i] = new byte[8 * 1024 * 1024];
			i++;
		}
	}

	public BloomFilter(final boolean flag)
	{
		cache = new ConcurrentHashMap<Long, List<Object>>(4 * 1024 * 1024, 1.0f);
	}

	public void add(final long hash)
	{
		final long offset = hash & 0x7FFFFFFFl;
		final int bytePos = (int)(offset >> 3);
		final int bitPos = (int)(offset & 0x07);
		final int x = bytePos >>> 23;
		final int y = bytePos & 0x7fffff;
		final byte val = (byte)(1 << bitPos);
		synchronized (bits[x])
		{
			bits[x][y] |= val;
		}
		return;
	}

	public void add(final long hash, final List<Object> val)
	{
		cache.put(hash & 0x3FFFFFl, val);
	}

	public boolean passes(final long hash)
	{
		final long offset = hash & 0x7FFFFFFFl;
		final int bytePos = (int)(offset >> 3);
		final int bitPos = (int)(offset & 0x07);
		return (bits[bytePos >>> 23][bytePos & 0x7fffff] & ((byte)(1 << bitPos))) != 0;
	}

	public boolean passes(final long hash, final List<Object> val)
	{
		final List<Object> o = cache.get(hash & 0x3FFFFFl);
		return !val.equals(o);
	}
}