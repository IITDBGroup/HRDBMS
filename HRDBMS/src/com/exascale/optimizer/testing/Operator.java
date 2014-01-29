package com.exascale.optimizer.testing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeMap;

public interface Operator
{
	public void start() throws Exception;
	public Object next(Operator op) throws Exception;
	public void nextAll(Operator op) throws Exception;
	public void close() throws Exception;
	public void add(Operator op) throws Exception;
	public void registerParent(Operator op) throws Exception;
	public HashMap<String, String> getCols2Types();
	public HashMap<String, Integer> getCols2Pos();
	public TreeMap<Integer, String> getPos2Col();
	public ArrayList<Operator> children();
	public Operator parent();
	public void removeChild(Operator op);
	public void removeParent(Operator op);
	public MetaData getMeta();
	public ArrayList<String> getReferences();
	public Operator clone();
	public void setNode(int node);
	public int getNode();
	public void setChildPos(int pos);
	public int getChildPos();
	public void reset();
}
