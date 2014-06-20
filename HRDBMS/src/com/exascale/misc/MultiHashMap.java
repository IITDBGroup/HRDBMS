package com.exascale.misc;

import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class MultiHashMap<K, V>
{
	private final ConcurrentHashMap<K, Vector<V>> map;
	private AtomicInteger size = new AtomicInteger(0);

	public MultiHashMap()
	{
		map = new ConcurrentHashMap<K, Vector<V>>();
	}
	
	public int size()
	{
		return map.size();
	}
	
	public int totalSize()
	{
		return size.get();
	}
	
	public Set<K> getKeySet()
	{
		return map.keySet();
	}
	
	public void clear()
	{
		map.clear();
	}

	public Vector<V> get(K key)
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
		
		size.getAndIncrement();
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
			
			size.getAndDecrement();
		}
	}
	
	public void remove(K key)
	{
		map.remove(key);
	}
}
