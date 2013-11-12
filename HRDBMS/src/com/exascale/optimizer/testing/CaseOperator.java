package com.exascale.optimizer.testing;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.exascale.optimizer.testing.ResourceManager.DiskBackedHashMap;

public class CaseOperator implements Operator, Serializable
{
	private MetaData meta;
	private Vector<HashSet<HashMap<Filter, Filter>>> filters;
	private Vector<Object> results;
	private Operator child = null;
	private Operator parent;
	private HashMap<String, String> cols2Types;
	private HashMap<String, Integer> cols2Pos;
	private TreeMap<Integer, String> pos2Col;
	private String name;
	private String type;
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	private Vector<String> origResults;
	private ArrayList<String> references = new ArrayList<String>();
	
	public CaseOperator( Vector<HashSet<HashMap<Filter, Filter>>> filters, Vector<String> results, String name, String type, MetaData meta)
	{
		this.filters = filters;
		this.name = name;
		this.type = type;
		this.meta = meta;
		this.origResults = results;
		
		this.results = new Vector<Object>();
		
		for (HashSet<HashMap<Filter, Filter>> filter : filters)
		{
			for (HashMap<Filter, Filter> f : filter)
			{
				for (Filter f2 : f.keySet())
				{
					if (f2.leftIsColumn())
					{
						references.add(f2.leftColumn());
					}
					
					if (f2.rightIsColumn())
					{
						references.add(f2.rightColumn());
					}
				}
			}
		}
		
		for (String val1 : results)
		{
			if (val1.startsWith("DATE('"))
			{
				String temp = val1.substring(6);
				StringTokenizer tokens = new StringTokenizer(temp, "'", false);
				temp = tokens.nextToken();
			
				try 
				{
					this.results.add(sdf.parse(temp));
				} 
				catch (ParseException e) 
				{
					e.printStackTrace();
					System.exit(1);
				}
			}
			else if (val1.startsWith("'"))
			{
				val1 = val1.substring(1);
				val1 = val1.substring(0, val1.length() - 1);
				this.results.add(val1);
			}
			else if ((val1.charAt(0) >= '0' && val1.charAt(0) <= '9') || val1.charAt(0) == '-')
			{
				if (val1.contains("."))
				{
					this.results.add(Double.parseDouble(val1));
				}
				else
				{
					this.results.add(Long.parseLong(val1));
				}
			}
			else
			{
				references.add(val1);
				this.results.add("\u0000" + val1);
			}
		}
	}
	
	public ArrayList<String> getReferences()
	{
		return references;
	}
	
	public String getOutputCol()
	{
		return name;
	}
	
	public ArrayList<Operator> children()
	{
		ArrayList<Operator> retval = new ArrayList<Operator>();
		retval.add(child);
		return retval;
	}
	
	public MetaData getMeta()
	{
		return meta;
	}
	
	public Operator parent()
	{
		return parent;
	}
	
	public String toString()
	{
		return "CaseOperator";
	}
	
	public void add(Operator op) throws Exception 
	{
		if (child == null)
		{
			child = op;
			child.registerParent(this);
			cols2Types = (HashMap<String, String>)child.getCols2Types().clone();
			cols2Types.put(name, type);
			cols2Pos = (HashMap<String, Integer>)child.getCols2Pos().clone();
			cols2Pos.put(name, cols2Pos.size());
			pos2Col = (TreeMap<Integer, String>)child.getPos2Col().clone();
			pos2Col.put(pos2Col.size(), name);
		}
		else
		{
			throw new Exception("CaseOperator only supports 1 child.");
		}
	}
	
	public void removeChild(Operator op)
	{
		if (op == child)
		{
			child = null;
			op.removeParent(this);
		}
	}
	
	public void removeParent(Operator op)
	{
		parent = null;
	}
	
	@Override
	public void start() throws Exception 
	{
		child.start();
	}

	@Override
	public Object next(Operator op) throws Exception 
	{
		Object o = child.next(this);
		
		if (! (o instanceof DataEndMarker))
		{
			int i = 0;
			for (HashSet<HashMap<Filter, Filter>> aCase : filters)
			{
				if (passesCase((ArrayList<Object>)o, cols2Pos, aCase))
				{
					Object obj = results.get(i);
					if (obj instanceof String && ((String)obj).startsWith("\u0000"))
					{
						((ArrayList<Object>)o).add(((ArrayList<Object>)o).get(cols2Pos.get(((String)obj).substring(1))));
						return o;
					}
					else
					{
						((ArrayList<Object>)o).add(obj);
						return o;
					}
				}
				i++;
			}
			
			Object obj = results.get(i);
			if (obj instanceof String && ((String)obj).startsWith("\u0000"))
			{
				//column lookup
				((ArrayList<Object>)o).add(((ArrayList<Object>)o).get(cols2Pos.get(((String)obj).substring(1))));
				return o;
			}
			else
			{
				((ArrayList<Object>)o).add(obj);
				return o;
			}
		}
		
		return o;
	}

	@Override
	public void close() throws Exception 
	{
		child.close();
	}

	public void registerParent(Operator op) throws Exception
	{
		if (parent == null)
		{
			parent = op;
		}
		else
		{
			throw new Exception("CaseOperator only supports 1 parent.");
		}
	}

	@Override
	public HashMap<String, String> getCols2Types() {
		return cols2Types;
	}

	@Override
	public HashMap<String, Integer> getCols2Pos() {
		return cols2Pos;
	}

	@Override
	public TreeMap<Integer, String> getPos2Col() {
		return pos2Col;
	}
	
	private boolean passesCase(ArrayList<Object> row, HashMap<String, Integer> cols2Pos, HashSet<HashMap<Filter, Filter>> ands)
	{
		for (HashMap<Filter, Filter> ors : ands)
		{
			if (!passesOredCondition(row, cols2Pos, ors))
			{
				return false;
			}
		}
		
		return true;
	}
	
	private boolean passesOredCondition(ArrayList<Object> row, HashMap<String, Integer> cols2Pos, HashMap<Filter, Filter> filters)
	{
		try
		{
			for (Map.Entry entry : filters.entrySet())
			{
				if (((Filter)entry.getKey()).passes(row, cols2Pos))
				{
					return true;
				}
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
			System.exit(1);
		}
		
		return false;
	}
}
