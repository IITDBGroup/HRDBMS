package com.exascale.optimizer;

import java.io.Serializable;
import java.util.ArrayList;
import com.exascale.filesystem.RID;

public class RIDAndIndexKeys implements Serializable
{
	private RID rid;
	private ArrayList<ArrayList<Object>> indexKeys;
	
	public RIDAndIndexKeys(RID rid, ArrayList<ArrayList<Object>> indexKeys)
	{
		this.rid = rid;
		this.indexKeys = indexKeys;
	}
	
	public RID getRID()
	{
		return rid;
	}
	
	public ArrayList<ArrayList<Object>> getIndexKeys()
	{
		return indexKeys;
	}
}
