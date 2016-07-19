package com.exascale.optimizer.externalTable;

import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.TreeMap;
import com.exascale.optimizer.MetaData;
import com.exascale.optimizer.Operator;
import com.exascale.tables.Plan;

public class ExternalTableType implements Operator
{

	@Override
	public void add(Operator op) throws Exception
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public ArrayList<Operator> children()
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Operator clone()
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void close() throws Exception
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public int getChildPos()
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public HashMap<String, Integer> getCols2Pos()
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public HashMap<String, String> getCols2Types()
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public MetaData getMeta()
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getNode()
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public TreeMap<Integer, String> getPos2Col()
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ArrayList<String> getReferences()
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object next(Operator op) throws Exception
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void nextAll(Operator op) throws Exception
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public long numRecsReceived()
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Operator parent()
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean receivedDEM()
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void registerParent(Operator op) throws Exception
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void removeChild(Operator op)
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void removeParent(Operator op)
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void reset() throws Exception
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void serialize(OutputStream out, IdentityHashMap<Object, Long> prev) throws Exception
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setChildPos(int pos)
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setNode(int node)
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setPlan(Plan p)
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void start() throws Exception
	{
		// TODO Auto-generated method stub
		
	}

}
