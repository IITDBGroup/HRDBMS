package com.exascale.misc;

import java.util.HashMap;
import java.util.Vector;

public class MultiHashMap<K, V>
{	
	private HashMap<K,Vector<V>> map;
	
	public MultiHashMap()
	{
		map = new HashMap<K,Vector<V>>();
	}
	
	public void multiPut(K key, V val)
	{
		if (map.containsKey(key))
		{
			Vector<V> vector = map.get(key);
			
			if (!vector.contains(val))
			{
				vector.add(val);
			}
		}
		else
		{
			Vector<V> vector = new Vector<V>();
			vector.add(val);
			map.put(key, vector);
		}
	}
	
	public void multiRemove(K key, V val)
	{
		if (map.containsKey(key))
		{
			Vector<V> vector = map.get(key);
			vector.remove(val);
			
			if (vector.size() == 0)
			{
				map.remove(vector);
			}
		}
	}
	
	public Vector<V> get(K key)
	{
		return map.get(key);
	}
	
	public boolean multiContains(K key, V val)
	{
		Vector<V> vector = map.get(key);
		
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
}
