package com.exascale.optimizer.testing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeMap;

public class RootOperator implements Operator
{
	protected Operator child;
	protected HashMap<String, String> cols2Types;
	protected HashMap<String, Integer> cols2Pos;
	protected TreeMap<Integer, String> pos2Col;
	protected HashMap<String, Double> generated;
	protected int node;
	protected MetaData meta;
	
	public void setChildPos(int pos)
	{
	}
	
	public void reset()
	{
		child.reset();
	}
	
	public int getChildPos()
	{
		return 0;
	}
	
	public RootOperator(MetaData meta)
	{
		this.meta = meta;
	}
	
	public RootOperator clone()
	{
		return new RootOperator(meta);
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
		ArrayList<String> retval = new ArrayList<String>(0);
		return retval;
	}
	
	public RootOperator(HashMap<String, Double> generated, MetaData meta)
	{
		this.generated = generated;
		this.meta = meta;
	}
	
	public HashMap<String, Double> getGenerated()
	{
		return generated;
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
	
	public void nextAll(Operator op) throws Exception
	{
		child.nextAll(op);
		Object o = next(op);
		while (!(o instanceof DataEndMarker))
		{
			o = next(op);
		}
	}
	
	public Object next() throws Exception
	{
		return child.next(this);
	}
	
	public Object next(Operator op) throws Exception
	{
		return child.next(this);
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
