package com.exascale.optimizer;

import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.TreeMap;
import com.exascale.tables.Plan;

public interface Operator
{
	/** Add the passed operator as a child of this operator */
	public void add(Operator op) throws Exception;

	public ArrayList<Operator> children();

	public Operator clone();

	public void close() throws Exception;

	public int getChildPos();

	public HashMap<String, Integer> getCols2Pos();

	public HashMap<String, String> getCols2Types();

	public MetaData getMeta();

	public int getNode();

	public TreeMap<Integer, String> getPos2Col();

	public ArrayList<String> getReferences();

	public Object next(Operator op) throws Exception;

	public void nextAll(Operator op) throws Exception;

	public long numRecsReceived();

	public Operator parent();

	public boolean receivedDEM();

	public void registerParent(Operator op) throws Exception;

	public void removeChild(Operator op);

	public void removeParent(Operator op);

	public void reset() throws Exception;

	public void serialize(OutputStream out, IdentityHashMap<Object, Long> prev) throws Exception;

	public void setChildPos(int pos);

	public void setNode(int node);

	public void setPlan(Plan p);

	public void start() throws Exception;
}
