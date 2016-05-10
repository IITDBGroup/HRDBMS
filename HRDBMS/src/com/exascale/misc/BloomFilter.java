package com.exascale.misc;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

public class BloomFilter
{
	private byte[][] bits;
	private ConcurrentHashMap<Long, ArrayList<Object>> cache;

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

	public BloomFilter(boolean flag)
	{
		cache = new ConcurrentHashMap<Long, ArrayList<Object>>(4 * 1024 * 1024, 1.0f);
	}

	public synchronized void add(long hash)
	{
		long offset = hash & 0x7FFFFFFFl;
		int bytePos = (int)(offset >> 3);
		int bitPos = (int)(offset & 0x07);
		bits[bytePos >>> 23][bytePos & 0x7fffff] |= (byte)(1 << bitPos);
		return;
	}

	public void add(long hash, ArrayList<Object> val)
	{
		cache.put(hash & 0x3FFFFFl, val);
	}

	public boolean passes(long hash)
	{
		long offset = hash & 0x7FFFFFFFl;
		int bytePos = (int)(offset >> 3);
		int bitPos = (int)(offset & 0x07);
		return (bits[bytePos >>> 23][bytePos & 0x7fffff] & ((byte)(1 << bitPos))) != 0;
	}

	public boolean passes(long hash, ArrayList<Object> val)
	{
		ArrayList<Object> o = cache.get(hash & 0x3FFFFFl);
		return !val.equals(o);
	}
}
