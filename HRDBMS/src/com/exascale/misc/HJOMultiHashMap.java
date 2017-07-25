package com.exascale.misc;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import com.exascale.managers.ResourceManager;

public class HJOMultiHashMap<K, V>
{
	private final ConcurrentHashMap<K, List<V>> map;
	private final AtomicInteger size = new AtomicInteger(0);

	public HJOMultiHashMap()
	{
		map = new ConcurrentHashMap<K, List<V>>(16, 0.75f, 6 * ResourceManager.cpus);
	}

	public void clear()
	{
		map.clear();
		size.set(0);
	}

	public List<V> get(final K key)
	{
		final List<V> retval = map.get(key);
		if (retval == null)
		{
			return new ArrayList<V>();
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
		if (map.containsKey(key))
		{
			final List<V> vector = map.get(key);
			vector.add(val);
		}
		else
		{
			List<V> vector = new ArrayList<V>();
			vector.add(val);
			if (map.putIfAbsent(key, vector) != null)
			{
				vector = map.get(key);
				vector.add(val);
			}
		}

		size.getAndIncrement();
	}

	public void multiRemove(final K key)
	{
		final List<V> removed = map.remove(key);
		if (removed != null)
		{
			size.addAndGet(-removed.size());
		}
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
