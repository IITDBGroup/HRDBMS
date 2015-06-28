package com.exascale.misc;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import com.exascale.managers.ResourceManager;

public class LOMultiHashMap<K, V>
{
	private final ConcurrentHashMap<K, ConcurrentHashMap<V, V>> map;
	private final AtomicInteger size = new AtomicInteger(0);

	public LOMultiHashMap()
	{
		map = new ConcurrentHashMap<K, ConcurrentHashMap<V, V>>(16, 0.75f, 6 * ResourceManager.cpus);
	}

	public void clear()
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

	public void multiPut(K key, V val)
	{
		if (map.containsKey(key))
		{
			final ConcurrentHashMap<V, V> vector = map.get(key);
			vector.put(val, val);
		}
		else
		{
			ConcurrentHashMap<V, V> vector = new ConcurrentHashMap<V, V>(16, 0.75f, 6 * ResourceManager.cpus);
			vector.put(val, val);
			if (map.putIfAbsent(key, vector) != null)
			{
				vector = map.get(key);
				vector.put(val, val);
			}
		}

		size.getAndIncrement();
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
