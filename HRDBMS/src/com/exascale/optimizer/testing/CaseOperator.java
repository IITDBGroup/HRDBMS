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
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.exascale.optimizer.testing.ResourceManager.DiskBackedHashMap;

public final class CaseOperator implements Operator, Serializable
{
	protected MetaData meta;
	protected ArrayList<HashSet<HashMap<Filter, Filter>>> filters;
	protected ArrayList<Object> results;
	protected Operator child = null;
	protected Operator parent;
	protected HashMap<String, String> cols2Types;
	protected HashMap<String, Integer> cols2Pos;
	protected TreeMap<Integer, String> pos2Col;
	protected String name;
	protected String type;
	//protected SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	protected ArrayList<String> origResults;
	protected ArrayList<String> references = new ArrayList<String>();
	protected int node;
	
	public void reset()
	{
		child.reset();
	}
	
	public void setChildPos(int pos)
	{
	}
	
	public int getChildPos()
	{
		return 0;
	}
	
	public CaseOperator clone()
	{
		CaseOperator retval = new CaseOperator(filters, origResults, name, type, meta);
		retval.node = node;
		return retval;
	}
	
	public CaseOperator( ArrayList<HashSet<HashMap<Filter, Filter>>> filters, ArrayList<String> results, String name, String type, MetaData meta)
	{
		this.filters = filters;
		this.name = name;
		this.type = type;
		this.meta = meta;
		this.origResults = results;
		
		this.results = new ArrayList<Object>(results.size());
		
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
				//FastStringTokenizer tokens = new FastStringTokenizer(temp, "'", false);
				FastStringTokenizer tokens = new FastStringTokenizer(temp, "'", false);
				temp = tokens.nextToken();
				this.results.add(DateParser.parse(temp));
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
					this.results.add(Utils.parseDouble(val1));
				}
				else
				{
					this.results.add(Utils.parseLong(val1));
				}
			}
			else
			{
				references.add(val1);
				this.results.add("\u0000" + val1);
			}
		}
	}
	
	public int getNode()
	{
		return node;
	}
	
	public void setNode(int node)
	{
		this.node = node;
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
		ArrayList<Operator> retval = new ArrayList<Operator>(1);
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
			if (child.getCols2Types() != null)
			{
				cols2Types = (HashMap<String, String>)child.getCols2Types().clone();
				cols2Types.put(name, type);
				cols2Pos = (HashMap<String, Integer>)child.getCols2Pos().clone();
				cols2Pos.put(name, cols2Pos.size());
				pos2Col = (TreeMap<Integer, String>)child.getPos2Col().clone();
				pos2Col.put(pos2Col.size(), name);
			}
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
	
	public void nextAll(Operator op) throws Exception
	{
		child.nextAll(op);
		Object o = next(op);
		while (!(o instanceof DataEndMarker))
		{
			o = next(op);
		}
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
	
	protected boolean passesCase(ArrayList<Object> row, HashMap<String, Integer> cols2Pos, HashSet<HashMap<Filter, Filter>> ands)
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
	
	protected boolean passesOredCondition(ArrayList<Object> row, HashMap<String, Integer> cols2Pos, HashMap<Filter, Filter> filters)
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
