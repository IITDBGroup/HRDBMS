package com.exascale.misc;

import java.util.List;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import com.exascale.managers.ResourceManager;

public class LOMultiHashMap<K, V>
{
	private final ConcurrentHashMap<K, Vector<V>> map;
	private final AtomicInteger size = new AtomicInteger(0);

	public LOMultiHashMap()
	{
		map = new ConcurrentHashMap<K, Vector<V>>(16, 0.75f, 6 * ResourceManager.cpus);
	}

	public void clear()
	{
		map.clear();
		size.set(0);
	}

	public List<V> get(final K key)
	{
		final Vector<V> retval = map.get(key);
		if (retval == null)
		{
			return new Vector<V>();
		}
		else
		{
			return retval;
		}
	}

	public Set<K> getKeySet()
	{
		return map.keySet();
	}

	public void multiPut(final K key, final V val)
	{
		Vector<V> vector = map.get(key);
		if (vector != null)
		{
			vector.add(val);
		}
		else
		{
			vector = new Vector<V>();
			vector.add(val);
			if (map.putIfAbsent(key, vector) != null)
			{
				vector = map.get(key);
				vector.add(val);
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
