package com.exascale.misc;

import java.util.ArrayList;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import com.exascale.managers.ResourceManager;

public class VHJOMultiHashMap<K, V>
{
	private final ConcurrentHashMap<K, Vector<V>> map;
	private final AtomicInteger size = new AtomicInteger(0);

	public VHJOMultiHashMap()
	{
		map = new ConcurrentHashMap<K, Vector<V>>(16, 0.75f, 6 * ResourceManager.cpus);
	}

	public void clear()
	{
		map.clear();
		size.set(0);
	}

	public List<V> get(K key)
	{
		Vector<V> retval = map.get(key);
		if (retval == null)
		{
			return new Vector<V>();
		}
		else
		{
			return retval;
		}
	}
	
	public void multiRemove(K key)
	{
		Vector<V> removed = map.remove(key);
		if (removed != null)
		{
			size.addAndGet(-removed.size());
		}
	}

	public void multiPut(K key, V val)
	{
		if (map.containsKey(key))
		{
			final Vector<V> vector = map.get(key);
			vector.add(val);
		}
		else
		{
			Vector<V> vector = new Vector<V>();
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