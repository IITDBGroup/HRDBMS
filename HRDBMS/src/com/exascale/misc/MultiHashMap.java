package com.exascale.misc;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import com.exascale.managers.ResourceManager;

public class MultiHashMap<K, V> implements Serializable
{
	private final ConcurrentHashMap<K, ConcurrentHashMap<V, V>> map;
	private final AtomicInteger size = new AtomicInteger(0);

	public MultiHashMap()
	{
		map = new ConcurrentHashMap<K, ConcurrentHashMap<V, V>>(16, 0.75f, 6 * ResourceManager.cpus);
	}

	public synchronized void clear()
	{
		map.clear();
		size.set(0);
	}

	public Set<V> get(K key)
	{
		ConcurrentHashMap<V, V> retval = map.get(key);
		if (retval == null)
		{
			return new HashSet<V>();
		}
		else
		{
			return retval.keySet();
		}
	}

	public Set<K> getKeySet()
	{
		return map.keySet();
	}

	public boolean multiContains(K key, V val)
	{
		final ConcurrentHashMap<V, V> vector = map.get(key);

		if (vector == null)
		{
			return false;
		}

		if (vector.containsKey(val))
		{
			return true;
		}

		return false;
	}

	public synchronized void multiPut(K key, V val)
	{
		if (map.containsKey(key))
		{
			final ConcurrentHashMap<V, V> vector = map.get(key);
			vector.put(val, val);
		}
		else
		{
			final ConcurrentHashMap<V, V> vector = new ConcurrentHashMap<V, V>(16, 0.75f, 6 * ResourceManager.cpus);
			vector.put(val, val);
			map.put(key, vector);
		}

		size.getAndIncrement();
	}

	public synchronized V multiRemove(K key)
	{
		V retval = null;
		if (map.containsKey(key))
		{
			final ConcurrentHashMap<V, V> vector = map.get(key);
			for (V k : vector.keySet())
			{
				retval = k;
				vector.remove(k);
				break;
			}

			if (vector.size() == 0)
			{
				map.remove(vector);
			}

			size.getAndDecrement();
		}

		return retval;
	}

	public synchronized void multiRemove(K key, V val)
	{
		if (map.containsKey(key))
		{
			final ConcurrentHashMap<V, V> vector = map.get(key);
			vector.remove(val);

			if (vector.size() == 0)
			{
				map.remove(vector);
			}

			size.getAndDecrement();
		}
	}

	public void remove(K key)
	{
		map.remove(key);
	}

	public int size()
	{
		return map.size();
	}

	public int totalSize()
	{
		return size.get();
	}
}
