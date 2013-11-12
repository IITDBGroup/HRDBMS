package com.exascale.optimizer.testing;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Stack;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import com.exascale.optimizer.testing.ResourceManager.DiskBackedHashMap;

public class ExtendOperator implements Operator, Serializable
{
	private Operator child;
	private Operator parent;
	private HashMap<String, String> cols2Types;
	private HashMap<String, Integer> cols2Pos;
	private TreeMap<Integer, String> pos2Col;
	private String prefix;
	private MetaData meta;
	private String name;
	private boolean startDone = false;
	private boolean closeDone = false;
	
	public ExtendOperator(String prefix, String name, MetaData meta)
	{
		this.prefix = prefix;
		this.meta = meta;
		this.name = name;
	}
	
	public ArrayList<String> getReferences()
	{
		ArrayList<String> retval = new ArrayList<String>();
		StringTokenizer tokens = new StringTokenizer(prefix, ",", false);
		while (tokens.hasMoreTokens())
		{
			String temp = tokens.nextToken();
			if (Character.isLetter(temp.charAt(0)) || (temp.charAt(0) == '_'))
			{
				retval.add(temp);
			}
		}
		
		return retval;
	}
	
	public String getOutputCol()
	{
		return name;
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
		return "ExtendOperator";
	}
	
	@Override
	public synchronized void start() throws Exception 
	{
		if (!startDone)
		{
			startDone = true;
			child.start();
		}
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
		row.add(parsePrefixDouble(prefix, row));
		return row;
	}

	@Override
	public synchronized void close() throws Exception 
	{
		if (!closeDone)
		{
			closeDone = true;
			child.close();
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
	public void add(Operator op) throws Exception 
	{
		if (child == null)
		{
			child = op;
			child.registerParent(this);
			cols2Types = (HashMap<String, String>)child.getCols2Types().clone();
			cols2Types.put(name, "FLOAT");
			cols2Pos = (HashMap<String, Integer>)child.getCols2Pos().clone();
			cols2Pos.put(name, cols2Pos.size());
			pos2Col = (TreeMap<Integer, String>)child.getPos2Col().clone();
			pos2Col.put(pos2Col.size(), name);
		}
		else
		{
			throw new Exception("ExtendOperator only supports 1 child.");
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
			throw new Exception("ExtendOperator only supports 1 parent.");
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

	private Double parsePrefixDouble(String prefix, ArrayList<Object> row)
	{
		Stack<String> parseStack = new Stack<String>();
		Stack<Object> execStack = new Stack<Object>();
		StringTokenizer tokens = new StringTokenizer(prefix, ",", false);
		while (tokens.hasMoreTokens())
		{
			parseStack.push(tokens.nextToken());
		}
		
		while (parseStack.size() > 0)
		{
			String temp = parseStack.pop();
			if (temp.equals("*"))
			{
				Double lhs = (Double)execStack.pop();
				Double rhs = (Double)execStack.pop();
				execStack.push(lhs * rhs);
				
			}
			else if (temp.equals("-"))
			{
				Double lhs = (Double)execStack.pop();
				Double rhs = (Double)execStack.pop();
				execStack.push(lhs - rhs);
			}
			else if (temp.equals("+"))
			{
				Double lhs = (Double)execStack.pop();
				Double rhs = (Double)execStack.pop();
				execStack.push(lhs + rhs);
			}
			else if (temp.equals("/"))
			{
				Double lhs = (Double)execStack.pop();
				Double rhs = (Double)execStack.pop();
				execStack.push(lhs / rhs);
			}
			else
			{
				try
				{
					if (Character.isLetter(temp.charAt(0)) || (temp.charAt(0) == '_'))
					{
						Object field = row.get(cols2Pos.get(temp));
						if (field instanceof Long)
						{
							execStack.push(new Double(((Long)field).longValue()));
						}
						else if (field instanceof Integer)
						{
							execStack.push(new Double(((Integer)field).intValue()));
						}
						else if (field instanceof Double)
						{
							execStack.push(field);
						}
					}
					else
					{
						double d = Double.parseDouble(temp);
						execStack.push(d);
					}
				}
				catch(Exception e)
				{
					e.printStackTrace();
					System.exit(1);
				}
			}
		}
		
		return (Double)execStack.pop();
	}
}
