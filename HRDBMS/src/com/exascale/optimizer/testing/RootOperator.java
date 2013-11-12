package com.exascale.optimizer.testing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeMap;

public class RootOperator implements Operator
{
	private Operator child;
	private HashMap<String, String> cols2Types;
	private HashMap<String, Integer> cols2Pos;
	private TreeMap<Integer, String> pos2Col;
	private HashMap<String, Double> generated;
	
	public RootOperator()
	{
	}
	
	public ArrayList<String> getReferences()
	{
		ArrayList<String> retval = new ArrayList<String>();
		return retval;
	}
	
	public RootOperator(HashMap<String, Double> generated)
	{
		this.generated = generated;
	}
	
	public HashMap<String, Double> getGenerated()
	{
		return generated;
	}
	
	public ArrayList<Operator> children()
	{
		ArrayList<Operator> retval = new ArrayList<Operator>();
		retval.add(child);
		return retval;
	}
	
	public MetaData getMeta()
	{
		return null;
	}
	
	public Operator parent()
	{
		return null;
	}
	
	public long bufferSize()
	{
		return 0;
	}
	
	public String toString()
	{
		return "RootOperator";
	}
	
	public void add(Operator op) throws Exception
	{
		if (child == null)
		{
			child = op;
			op.registerParent(this);
			cols2Types = child.getCols2Types();
			cols2Pos = child.getCols2Pos();
			pos2Col = child.getPos2Col();
		}
		else
		{
			throw new Exception("SelectOperator only supports 1 child.");
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
		//parent = null;
	}
	
	public void start() throws Exception 
	{
		child.start();
	}
	
	public Object next() throws Exception
	{
		return child.next(this);
	}
	
	public ArrayList<Object> next(Operator op) throws Exception
	{
		throw new Exception("Cannot call next(op) on RootOperator");
	}
	
	public void close() throws Exception 
	{
		child.close();
	}

	public void registerParent(Operator op) throws Exception
	{
		throw new Exception("A RootOperator cannot have parents!");
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
}
