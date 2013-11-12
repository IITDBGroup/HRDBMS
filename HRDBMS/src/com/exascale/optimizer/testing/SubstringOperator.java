package com.exascale.optimizer.testing;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Stack;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import com.exascale.optimizer.testing.ResourceManager.DiskBackedHashMap;

public class SubstringOperator implements Operator, Serializable
{
	private Operator child;
	private Operator parent;
	private HashMap<String, String> cols2Types;
	private HashMap<String, Integer> cols2Pos;
	private TreeMap<Integer, String> pos2Col;
	private String col;
	private String name;
	private int colPos;
	private MetaData meta;
	private int start;
	private int end;
	
	public SubstringOperator(String col, int start, int end, String name, MetaData meta)
	{
		this.col = col;
		this.meta = meta;
		this.name = name;
		this.start = start;
		this.end = end;
	}
	
	public String getOutputCol()
	{
		return name;
	}
	
	public ArrayList<String> getReferences()
	{
		ArrayList<String> retval = new ArrayList<String>();
		retval.add(col);
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
	
	public ArrayList<Operator> children()
	{
		ArrayList<Operator> retval = new ArrayList<Operator>();
		retval.add(child);
		return retval;
	}
	
	public String toString()
	{
		return "SubstringOperator";
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
		if (o instanceof DataEndMarker)
		{
			return o;
		}
		
		ArrayList<Object> row = (ArrayList<Object>)o;
		String field = (String)row.get(colPos);
		row.add(field.substring(start, end));
		return row;
	}

	@Override
	public void close() throws Exception 
	{
		child.close();
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
	public void add(Operator op) throws Exception 
	{
		if (child == null)
		{
			child = op;
			child.registerParent(this);
			cols2Types = (HashMap<String, String>)child.getCols2Types().clone();
			cols2Types.put(name, "CHAR");
			cols2Pos = (HashMap<String, Integer>)child.getCols2Pos().clone();
			cols2Pos.put(name, cols2Pos.size());
			pos2Col = (TreeMap<Integer, String>)child.getPos2Col().clone();
			pos2Col.put(pos2Col.size(), name);
			colPos = cols2Pos.get(col);
		}
		else
		{
			throw new Exception("SubstringOperator only supports 1 child.");
		}
	}

	@Override
	public void registerParent(Operator op) throws Exception
	{
		if (parent == null)
		{
			parent = op;
		}
		else
		{
			throw new Exception("SubstringOperator only supports 1 parent.");
		}
	}

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
}
