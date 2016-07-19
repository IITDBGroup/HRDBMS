package com.exascale.optimizer.externalTable;

import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Properties;
import java.util.TreeMap;
import com.exascale.optimizer.MetaData;
import com.exascale.tables.Plan;

public class HTTPCsvExternal implements ExternalTableType
{

	public static String HTTP_ADDRESS_KEY = "HttpAddress";
	
	// fields 
	private int pos;
	private List<String[]> rows;
	
	//chache
	
	@Override
	public void initialize(Properties params)
	{
		rows = new ArrayList<String[]> ();
		// TODO get HTTP Address
		// TODO read CSV file content and cache in rows field
	}

	@Override
	public String[] next()
	{
		// TODO return rows[pos] and pos++ (check that still tuples avialable)
		return null;
	}

	@Override
	public void reset()
	{
		// TODO pos = 0
		
	}

	@Override
	public boolean hasNext()
	{
		// TODO Auto-generated method stub
		return false;
	}
	

}