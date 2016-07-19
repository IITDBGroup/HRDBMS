package com.exascale.optimizer.externalTable;

import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.TreeMap;
import com.exascale.optimizer.MetaData;
import com.exascale.tables.Plan;

public interface HTTPCsvExternal
{
	public void add(HTTPCsvExternal httpCsvExtrnl) throws Exception;

	public ArrayList<HTTPCsvExternal> children();

	public HTTPCsvExternal clone();

	public void close() throws Exception;

	public int getChildPos();

	public HashMap<String, Integer> getCols2Pos();

	public HashMap<String, String> getCols2Types();

	public MetaData getMeta();

	public int getNode();

	public TreeMap<Integer, String> getPos2Col();

	public ArrayList<String> getReferences();

	public Object next(HTTPCsvExternal httpCsvExtrnl) throws Exception;

	public void nextAll(HTTPCsvExternal httpCsvExtrnl) throws Exception;

	public long numRecsReceived();

	public HTTPCsvExternal parent();

	public boolean receivedDEM();

	public void registerParent(HTTPCsvExternal httpCsvExtrnl) throws Exception;

	public void removeChild(HTTPCsvExternal httpCsvExtrnl);

	public void removeParent(HTTPCsvExternal httpCsvExtrnl);

	public void reset() throws Exception;

	public void serialize(OutputStream out, IdentityHashMap<Object, Long> prev) throws Exception;

	public void setChildPos(int pos);

	public void setNode(int node);

	public void setPlan(Plan p);

	public void start() throws Exception;
}