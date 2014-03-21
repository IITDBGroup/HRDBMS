package com.exascale.misc;

import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

public class MultiHashMap<K, V>
{
	private final ConcurrentHashMap<K, Vector<V>> map;

	public MultiHashMap()
	{
		map = new ConcurrentHashMap<K, Vector<V>>();
	}

	public Vector<V> get(K key)
	{
		return map.get(key);
	}

	public boolean multiContains(K key, V val)
	{
		final Vector<V> vector = map.get(key);

		if (vector == null)
		{
			return false;
		}

		if (vector.contains(val))
		{
			return true;
		}

		return false;
	}

	public synchronized void multiPut(K key, V val)
	{
		if (map.containsKey(key))
		{
			final Vector<V> vector = map.get(key);

			if (!vector.contains(val))
			{
				vector.add(val);
			}
		}
		else
		{
			final Vector<V> vector = new Vector<V>();
			vector.add(val);
			map.put(key, vector);
		}
	}

	public synchronized void multiRemove(K key, V val)
	{
		if (map.containsKey(key))
		{
			final Vector<V> vector = map.get(key);
			vector.remove(val);

			if (vector.size() == 0)
			{
				map.remove(vector);
			}
		}
	}
}
