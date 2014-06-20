package com.exascale.optimizer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeMap;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.misc.DateParser;
import com.exascale.misc.FastStringTokenizer;
import com.exascale.misc.MyDate;
import com.exascale.misc.Utils;
import com.exascale.tables.Transaction;
import com.exascale.threads.ConnectionWorker;

//TODO columns are qualified by table

public final class MetaData implements Serializable
{
	private final boolean LOCAL = false;

	private ArrayList<ArrayList<Object>> cards = null;
	private ArrayList<ArrayList<Object>> dists = null;
	private final Boolean cardsLock = false;
	private final Boolean distsLock = false;
	private final HashMap<Filter, Double> lCache = new HashMap<Filter, Double>();
	private static final HashMap<ConnectionWorker, String> defaultSchemas = new HashMap<ConnectionWorker, String>();
	private ConnectionWorker connection = null;
	
	public MetaData(ConnectionWorker connection)
	{
		this.connection = connection;
	}
	
	public MetaData()
	{
		
	}
	
	public static int myCoordNum()
	{
		return 0;
	}
	
	public static void setDefaultSchema(ConnectionWorker conn, String schema)
	{
		defaultSchemas.put(conn, schema);
	}
	
	public static void removeDefaultSchema(ConnectionWorker conn)
	{
		defaultSchemas.remove(conn);
	}

	public HashMap<String, Double> generateCard(Operator op)
	{
		final HashMap<Operator, ArrayList<String>> tables = new HashMap<Operator, ArrayList<String>>();
		final HashMap<Operator, ArrayList<ArrayList<Filter>>> filters = new HashMap<Operator, ArrayList<ArrayList<Filter>>>();
		final HashMap<Operator, HashMap<String, Double>> retval = new HashMap<Operator, HashMap<String, Double>>();
		final ArrayList<Operator> leaves = getLeaves(op);
		final ArrayList<Operator> queued = new ArrayList<Operator>(leaves.size());
		for (final Operator leaf : leaves)
		{
			final Operator o = doWork(leaf, tables, filters, retval);
			if (o != null)
			{
				queued.add(o);
			}
		}

		while (queued.size() > 1)
		{
			final Operator o = queued.get(0);
			if (queued.indexOf(o) != queued.lastIndexOf(o))
			{
				queued.remove(queued.lastIndexOf(o));
				final Operator o2 = doWork(o, tables, filters, retval);
				queued.add(o2);
				queued.remove(0);
			}
			else
			{
				queued.add(o);
				queued.remove(0);
			}
		}

		return retval.get(queued.get(0));
	}

	public Index getBestCompoundIndex(HashSet<String> cols, String schema, String table)
	{
		if (cols.size() <= 1)
		{
			return null;
		}

		final ArrayList<Index> indexes = this.getIndexesForTable(schema, table);
		int maxCount = 1;
		Index maxIndex = null;
		for (final Index index : indexes)
		{
			int count = 0;
			for (final String col : index.getCols())
			{
				if (cols.contains(col))
				{
					count++;
				}
			}

			if (count > maxCount)
			{
				maxCount = count;
				maxIndex = index;
			}
		}

		return maxIndex;
	}
	
	public String getCurrentSchema()
	{
		if (connection == null)
		{
			return null;
		}
		
		String retval = defaultSchemas.get(connection);
		if (retval == null)
		{
			//TODO return userid
			return "HRDBMS";
		}
		
		return retval;
	}
	
	public String getMyHostName()
	{
		return "";
	}
	
	public static boolean verifyInsert(String schema, String table, Operator op)
	{
		//TODO verify that the number of cols and data types from op match an insert into table
		return true;
	}
	
	public static ArrayList<Integer> getNodesForTable(String schema, String table)
	{
		return new ArrayList<Integer>();
	}
	
	public static ArrayList<String> getIndexFileNamesForTable(String schema, String table)
	{
		return new ArrayList<String>();
	}
	
	public static ArrayList<ArrayList<String>> getKeys(ArrayList<String> indexes)
	{
		return new ArrayList<ArrayList<String>>();)
	}
	
	public static ArrayList<ArrayList<String>> getTypes(ArrayList<String> indexes)
	{
		return new ArrayList<ArrayList<String>>();
	}
	
	public static ArrayList<ArrayList<Boolean>> getOrders(ArrayList<String> indexes)
	{
		return new ArrayList<ArrayList<Boolean>>();
	}
	
	public static ArrayList<String> getIndexColsForTable(String schema, String table)
	{
		return new ArrayList<String>();
	}
	
	public static ArrayList<String> getColsFromIndexFileName(String index)
	{
		return new ArrayList<String>();
	}
	
	public static int determineNode(String schema, String table, ArrayList<Object> row)
	{
		return 0;
	}
	
	public static int determineDevice(ArrayList<Object> row, PartitionMetaData partMeta)
	{
		return 0;
	}
	
	public static int getLengthForCharCol(String schema, String table, String col)
	{
		return 0;
	}
	
	public static void createView(String schema, String table, String text, Transaction tx) throws Exception
	{
		
	}
	
	public static void dropView(String schema, String table, Transaction tx) throws Exception
	{
		
	}
	
	public static boolean verifyIndexExistence(String schema, String index)
	{
		return true;
	}
	
	public static boolean verifyColExistence(String schema, String table, String col)
	{
		return true;
	}
	
	public static void createIndex(String schema, String table, String index, ArrayList<IndexDef> defs, boolean unique, Transaction tx) throws Exception
	{
		
	}
	
	public static void dropIndex(String schema, String index, Transaction tx) throws Exception
	{
		
	}
	
	public static void dropTable(String schema, String table, Transaction tx) throws Exception
	{
		
	}
	
	public static void createTable(String schema, String table, ArrayList<ColDef> defs, ArrayList<String> pks, Transaction tx) throws Exception
	{
		
	}
	
	public static ArrayList<Integer> getAllTableNodes(String schema, String table)
	{
		return new ArrayList<Integer>();
	}
	
	public static boolean verifyUpdate(String schema, String tbl, ArrayList<Column> cols, ArrayList<String> buildList, Operator op) throws Exception
	{
		//verify that all columns are 1 part - parseException
		//get data types for cols on this table
		//make sure that selecting the buildList cols from op in that order satisfies updates for cols on this table
		return true;
	}
	
	public boolean verifyViewExistence(String schema, String name)
	{
		return false;
	}
	
	public String getViewSQL(String schema, String name)
	{
		return "";
	}
	
	public boolean verifyTableExistence(String schema, String name)
	{
		if (!schema.equals("TPCH"))
		{
			return false;
		}
		
		if (name.equals("LINEITEM") || name.equals("CUSTOMER") || name.equals("ORDERS") || name.equals("PART") || name.equals("SUPPLIER") || name.equals("PARTSUPP") || name.equals("NATION") || name.equals("REGION"))
		{
			return true;
		}
		
		return false;
	}

	public long getCard(String col, HashMap<String, Double> generated)
	{
		try
		{
			if (cards == null)
			{
				synchronized (cardsLock)
				{
					if (cards == null)
					{
						final ArrayList<ArrayList<Object>> cardsTemp = new ArrayList<ArrayList<Object>>();
						final BufferedReader in = new BufferedReader(new FileReader(new File("card.tbl")));
						String line = in.readLine();
						while (line != null)
						{
							final ArrayList<Object> cols = new ArrayList<Object>(2);
							final FastStringTokenizer tokens = new FastStringTokenizer(line, "|", false);
							cols.add(tokens.nextToken());
							cols.add(Utils.parseLong(tokens.nextToken()));
							cardsTemp.add(cols);
							line = in.readLine();
						}
						in.close();
						cards = cardsTemp;
					}
				}
			}

			for (final ArrayList<Object> line : cards)
			{
				if (col.equals(line.get(0)))
				{
					return (Long)line.get(1);
				}
			}
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.error("", e);
			System.exit(1);
		}

		if (generated.containsKey(col))
		{
			return (generated.get(col).longValue());
		}

		// System.out.println("Can't find cardinality for " + col);
		// Thread.dumpStack();
		return 1000000;
	}

	public long getCard(String col, RootOperator op)
	{
		return getCard(col, op.getGenerated());
	}

	public long getColgroupCard(ArrayList<String> cols, HashMap<String, Double> generated)
	{
		// TODO should check gathered colgroup stats
		double card = 1;
		for (final String col : cols)
		{
			card *= this.getCard(col, generated);
		}

		return (long)card;
	}

	public long getColgroupCard(ArrayList<String> cols, RootOperator op)
	{
		return getColgroupCard(cols, op.getGenerated());
	}

	public HashMap<String, Integer> getCols2PosForTable(String schema, String name) throws Exception
	{
		final HashMap<String, Integer> retval = new HashMap<String, Integer>();
		if (name.equals("LINEITEM"))
		{
			retval.put("L_ORDERKEY", 0);
			retval.put("L_PARTKEY", 1);
			retval.put("L_SUPPKEY", 2);
			retval.put("L_LINENUMBER", 3);
			retval.put("L_QUANTITY", 4);
			retval.put("L_EXTENDEDPRICE", 5);
			retval.put("L_DISCOUNT", 6);
			retval.put("L_TAX", 7);
			retval.put("L_RETURNFLAG", 8);
			retval.put("L_LINESTATUS", 9);
			retval.put("L_SHIPDATE", 10);
			retval.put("L_COMMITDATE", 11);
			retval.put("L_RECEIPTDATE", 12);
			retval.put("L_SHIPINSTRUCT", 13);
			retval.put("L_SHIPMODE", 14);
			retval.put("L_COMMENT", 15);
		}
		else if (name.equals("CUSTOMER"))
		{
			retval.put("C_CUSTKEY", 0);
			retval.put("C_NAME", 1);
			retval.put("C_ADDRESS", 2);
			retval.put("C_NATIONKEY", 3);
			retval.put("C_PHONE", 4);
			retval.put("C_ACCTBAL", 5);
			retval.put("C_MKTSEGMENT", 6);
			retval.put("C_COMMENT", 7);
		}
		else if (name.equals("ORDERS"))
		{
			retval.put("O_ORDERKEY", 0);
			retval.put("O_CUSTKEY", 1);
			retval.put("O_ORDERSTATUS", 2);
			retval.put("O_TOTALPRICE", 3);
			retval.put("O_ORDERDATE", 4);
			retval.put("O_ORDERPRIORITY", 5);
			retval.put("O_CLERK", 6);
			retval.put("O_SHIPPRIORITY", 7);
			retval.put("O_COMMENT", 8);
		}
		else if (name.equals("SUPPLIER"))
		{
			retval.put("S_SUPPKEY", 0);
			retval.put("S_NAME", 1);
			retval.put("S_ADDRESS", 2);
			retval.put("S_NATIONKEY", 3);
			retval.put("S_PHONE", 4);
			retval.put("S_ACCTBAL", 5);
			retval.put("S_COMMENT", 6);
		}
		else if (name.equals("NATION"))
		{
			retval.put("N_NATIONKEY", 0);
			retval.put("N_NAME", 1);
			retval.put("N_REGIONKEY", 2);
			retval.put("N_COMMENT", 3);
		}
		else if (name.equals("REGION"))
		{
			retval.put("R_REGIONKEY", 0);
			retval.put("R_NAME", 1);
			retval.put("R_COMMENT", 2);
		}
		else if (name.equals("PART"))
		{
			retval.put("P_PARTKEY", 0);
			retval.put("P_NAME", 1);
			retval.put("P_MFGR", 2);
			retval.put("P_BRAND", 3);
			retval.put("P_TYPE", 4);
			retval.put("P_SIZE", 5);
			retval.put("P_CONTAINER", 6);
			retval.put("P_RETAILPRICE", 7);
			retval.put("P_COMMENT", 8);
		}
		else if (name.equals("PARTSUPP"))
		{
			retval.put("PS_PARTKEY", 0);
			retval.put("PS_SUPPKEY", 1);
			retval.put("PS_AVAILQTY", 2);
			retval.put("PS_SUPPLYCOST", 3);
			retval.put("PS_COMMENT", 4);
		}
		else
		{
			throw new Exception("Unknown table.");
		}

		return retval;
	}

	public HashMap<String, String> getCols2TypesForTable(String schema, String name) throws Exception
	{
		final HashMap<String, String> retval = new HashMap<String, String>();
		if (name.equals("LINEITEM"))
		{
			retval.put("L_ORDERKEY", "INT");
			retval.put("L_PARTKEY", "INT");
			retval.put("L_SUPPKEY", "INT");
			retval.put("L_LINENUMBER", "INT");
			retval.put("L_QUANTITY", "FLOAT");
			retval.put("L_EXTENDEDPRICE", "FLOAT");
			retval.put("L_DISCOUNT", "FLOAT");
			retval.put("L_TAX", "FLOAT");
			retval.put("L_RETURNFLAG", "CHAR");
			retval.put("L_LINESTATUS", "CHAR");
			retval.put("L_SHIPDATE", "DATE");
			retval.put("L_COMMITDATE", "DATE");
			retval.put("L_RECEIPTDATE", "DATE");
			retval.put("L_SHIPINSTRUCT", "CHAR");
			retval.put("L_SHIPMODE", "CHAR");
			retval.put("L_COMMENT", "CHAR");
		}
		else if (name.equals("CUSTOMER"))
		{
			retval.put("C_CUSTKEY", "INT");
			retval.put("C_NAME", "CHAR");
			retval.put("C_ADDRESS", "CHAR");
			retval.put("C_NATIONKEY", "INT");
			retval.put("C_PHONE", "CHAR");
			retval.put("C_ACCTBAL", "FLOAT");
			retval.put("C_MKTSEGMENT", "CHAR");
			retval.put("C_COMMENT", "CHAR");
		}
		else if (name.equals("ORDERS"))
		{
			retval.put("O_ORDERKEY", "INT");
			retval.put("O_CUSTKEY", "INT");
			retval.put("O_ORDERSTATUS", "CHAR");
			retval.put("O_TOTALPRICE", "FLOAT");
			retval.put("O_ORDERDATE", "DATE");
			retval.put("O_ORDERPRIORITY", "CHAR");
			retval.put("O_CLERK", "CHAR");
			retval.put("O_SHIPPRIORITY", "INT");
			retval.put("O_COMMENT", "CHAR");
		}
		else if (name.equals("SUPPLIER"))
		{
			retval.put("S_SUPPKEY", "INT");
			retval.put("S_NAME", "CHAR");
			retval.put("S_ADDRESS", "CHAR");
			retval.put("S_NATIONKEY", "INT");
			retval.put("S_PHONE", "CHAR");
			retval.put("S_ACCTBAL", "FLOAT");
			retval.put("S_COMMENT", "CHAR");
		}
		else if (name.equals("NATION"))
		{
			retval.put("N_NATIONKEY", "INT");
			retval.put("N_NAME", "CHAR");
			retval.put("N_REGIONKEY", "INT");
			retval.put("N_COMMENT", "CHAR");
		}
		else if (name.equals("REGION"))
		{
			retval.put("R_REGIONKEY", "INT");
			retval.put("R_NAME", "CHAR");
			retval.put("R_COMMENT", "CHAR");
		}
		else if (name.equals("PART"))
		{
			retval.put("P_PARTKEY", "INT");
			retval.put("P_NAME", "CHAR");
			retval.put("P_MFGR", "CHAR");
			retval.put("P_BRAND", "CHAR");
			retval.put("P_TYPE", "CHAR");
			retval.put("P_SIZE", "INT");
			retval.put("P_CONTAINER", "CHAR");
			retval.put("P_RETAILPRICE", "FLOAT");
			retval.put("P_COMMENT", "CHAR");
		}
		else if (name.equals("PARTSUPP"))
		{
			retval.put("PS_PARTKEY", "INT");
			retval.put("PS_SUPPKEY", "INT");
			retval.put("PS_AVAILQTY", "INT");
			retval.put("PS_SUPPLYCOST", "FLOAT");
			retval.put("PS_COMMENT", "CHAR");
		}
		else
		{
			throw new Exception("Unknown table.");
		}

		return retval;
	}

	public String getDevicePath(int num)
	{
		if (num == 0)
		{
			return "/mnt/ssd/";
		}

		// if (num == 1)
		// {
		// return "/data2/";
		// }
		//
		// if (num == 2)
		// {
		// return "/data3/";
		// }
		//
		// if (num == 3)
		// {
		// return "/data4/";
		// }
		//
		// if (num == 4)
		// {
		// return "/data5/";
		// }
		//
		// if (num == 5)
		// {
		// return "/data6/";
		// }

		return null;
	}

	public String getHostNameForNode(int node)
	{
		if (node == -1)
		{
			return "54.186.68.29";
		}

		if (node == 0)
		{
			return "172.31.6.176";
		}

		if (node == 1)
		{
			return "172.31.6.20";
		}

		if (node == 2)
		{
			return "172.31.10.16";
		}

		if (node == 3)
		{
			return "172.31.14.253";
		}

		if (node == 4)
		{
			return "172.31.0.234";
		}

		if (node == 5)
		{
			return "172.31.0.235";
		}

		if (node == 6)
		{
			return "172.31.0.236";
		}

		if (node == 7)
		{
			return "172.31.0.237";
		}

		if (node == 8)
		{
			return "172.31.13.210";
		}

		if (node == 9)
		{
			return "172.31.13.211";
		}
		if (node == 10)
		{
			return "172.31.13.212";
		}
		if (node == 11)
		{
			return "172.31.13.213";
		}
		if (node == 12)
		{
			return "172.31.13.214";
		}
		if (node == 13)
		{
			return "172.31.13.215";
		}
		if (node == 14)
		{
			return "172.31.13.216";
		}
		if (node == 15)
		{
			return "172.31.13.217";
		}

		// if (node == -1)
		// {
		// return "192.168.1.3";
		// }
		//
		// if (node == 0)
		// {
		// return "192.168.1.3";
		// }
		/*
		 * if (node == -1) { return "hec-01"; }
		 * 
		 * if (node == 0) { return "hec-02"; }
		 * 
		 * if (node == 1) { return "hec-03"; }
		 * 
		 * if (node == 2) { return "hec-04"; }
		 * 
		 * if (node == 3) { return "hec-06"; }
		 * 
		 * if (node == 4) { return "hec-07"; }
		 * 
		 * if (node == 5) { return "hec-08"; }
		 * 
		 * if (node == 6) { return "hec-09"; }
		 * 
		 * if (node == 7) { return "hec-10"; }
		 * 
		 * if (node == 8) { return "hec-11"; }
		 * 
		 * if (node == 9) { return "hec-13"; }
		 * 
		 * if (node == 10) { return "hec-14"; }
		 * 
		 * if (node == 11) { return "hec-16"; }
		 * 
		 * if (node == 12) { return "hec-17"; }
		 * 
		 * if (node == 13) { return "hec-18"; }
		 * 
		 * if (node == 14) { return "hec-19"; }
		 * 
		 * if (node == 15) { return "hec-20"; }
		 * 
		 * if (node == 16) { return "hec-23"; }
		 * 
		 * if (node == 17) { return "hec-24"; }
		 * 
		 * if (node == 18) { return "hec-26"; }
		 * 
		 * if (node == 19) { return "hec-27"; }
		 * 
		 * if (node == 20) { return "hec-29"; }
		 * 
		 * 
		 * if (node == 21) { return "hec-55"; }
		 * 
		 * if (node == 22) { return "hec-56"; }
		 * 
		 * if (node == 23) { return "hec-58"; }
		 * 
		 * if (node == 24) { return "hec-59"; }
		 * 
		 * if (node == 25) { return "hec-61"; }
		 * 
		 * if (node == 26) { return "hec-63"; }
		 * 
		 * if (node == 27) { return "hec-32"; }
		 * 
		 * if (node == 28) { return "hec-34"; }
		 * 
		 * if (node == 29) { return "hec-35"; }
		 * 
		 * if (node == 30) { return "hec-36"; }
		 * 
		 * if (node == 31) { return "hec-37"; }
		 * 
		 * if (node == 32) { return "hec-38"; }
		 * 
		 * if (node == 33) { return "hec-39"; }
		 * 
		 * if (node == 34) { return "hec-41"; }
		 * 
		 * if (node == 35) { return "hec-43"; }
		 * 
		 * if (node == 36) { return "hec-45"; }
		 * 
		 * if (node == 37) { return "hec-46"; }
		 * 
		 * if (node == 38) { return "hec-47"; }
		 * 
		 * if (node == 39) { return "hec-49"; }
		 */

		// if (node == 48)
		// {
		// return "hec-51";
		// }

		return null;
	}

	public ArrayList<Index> getIndexesForTable(String schema, String table)
	{
		final ArrayList<Index> retval = new ArrayList<Index>();
		if (table.equals("LINEITEM"))
		{
			ArrayList<String> keys = new ArrayList<String>();
			keys.add("L_SHIPDATE");
			keys.add("L_EXTENDEDPRICE");
			keys.add("L_QUANTITY");
			keys.add("L_DISCOUNT");
			keys.add("L_SUPPKEY");
			ArrayList<String> types = new ArrayList<String>();
			types.add("DATE");
			types.add("FLOAT");
			types.add("FLOAT");
			types.add("FLOAT");
			types.add("INT");
			ArrayList<Boolean> orders = new ArrayList<Boolean>();
			orders.add(true);
			orders.add(true);
			orders.add(true);
			orders.add(true);
			orders.add(true);
			retval.add(new Index("xl_shipdate.indx", keys, types, orders));

			keys = new ArrayList<String>();
			keys.add("L_SHIPMODE");
			keys.add("L_RECEIPTDATE");
			types = new ArrayList<String>();
			types.add("INT");
			types.add("DATE");
			orders = new ArrayList<Boolean>();
			orders.add(true);
			orders.add(true);
			retval.add(new Index("xl_receiptdate.indx", keys, types, orders));

			keys = new ArrayList<String>();
			keys.add("L_SHIPINSTRUCT");
			keys.add("L_SHIPMODE");
			keys.add("L_QUANTITY");
			types = new ArrayList<String>();
			types.add("CHAR");
			types.add("CHAR");
			types.add("FLOAT");
			orders = new ArrayList<Boolean>();
			orders.add(true);
			orders.add(true);
			orders.add(true);
			retval.add(new Index("xl_shipmode.indx", keys, types, orders));

			keys = new ArrayList<String>();
			keys.add("L_ORDERKEY");
			keys.add("L_SUPPKEY");
			types = new ArrayList<String>();
			types.add("INT");
			types.add("INT");
			orders = new ArrayList<Boolean>();
			orders.add(true);
			orders.add(true);
			retval.add(new Index("xl_orderkey.indx", keys, types, orders));

			keys = new ArrayList<String>();
			keys.add("L_PARTKEY");
			types = new ArrayList<String>();
			types.add("INT");
			orders = new ArrayList<Boolean>();
			orders.add(true);
			retval.add(new Index("xl_partkey.indx", keys, types, orders));
		}

		if (table.equals("PART"))
		{
			ArrayList<String> keys = new ArrayList<String>();
			keys.add("P_SIZE");
			ArrayList<String> types = new ArrayList<String>();
			types.add("INT");
			ArrayList<Boolean> orders = new ArrayList<Boolean>();
			orders.add(true);
			retval.add(new Index("xp_size.indx", keys, types, orders));

			keys = new ArrayList<String>();
			keys.add("P_TYPE");
			keys.add("P_SIZE");
			types = new ArrayList<String>();
			types.add("CHAR");
			types.add("INT");
			orders = new ArrayList<Boolean>();
			orders.add(true);
			orders.add(true);
			retval.add(new Index("xp_type.indx", keys, types, orders));

			keys = new ArrayList<String>();
			keys.add("P_NAME");
			types = new ArrayList<String>();
			types.add("CHAR");
			orders = new ArrayList<Boolean>();
			orders.add(true);
			retval.add(new Index("xp_name.indx", keys, types, orders));

			keys = new ArrayList<String>();
			keys.add("P_BRAND");
			keys.add("P_CONTAINER");
			keys.add("P_SIZE");
			types = new ArrayList<String>();
			types.add("CHAR");
			types.add("CHAR");
			types.add("INT");
			orders = new ArrayList<Boolean>();
			orders.add(true);
			orders.add(true);
			orders.add(true);
			retval.add(new Index("xp_container.indx", keys, types, orders));
		}

		if (table.equals("CUSTOMER"))
		{
			final ArrayList<String> keys = new ArrayList<String>();
			keys.add("C_MKTSEGMENT");
			final ArrayList<String> types = new ArrayList<String>();
			types.add("CHAR");
			final ArrayList<Boolean> orders = new ArrayList<Boolean>();
			orders.add(true);
			retval.add(new Index("xc_mktsegment.indx", keys, types, orders));
		}

		if (table.equals("ORDERS"))
		{
			ArrayList<String> keys = new ArrayList<String>();
			keys.add("O_ORDERDATE");
			ArrayList<String> types = new ArrayList<String>();
			types.add("DATE");
			ArrayList<Boolean> orders = new ArrayList<Boolean>();
			orders.add(true);
			retval.add(new Index("xo_orderdate.indx", keys, types, orders));

			keys = new ArrayList<String>();
			keys.add("O_CUSTKEY");
			types = new ArrayList<String>();
			types.add("INT");
			orders = new ArrayList<Boolean>();
			orders.add(true);
			retval.add(new Index("xo_custkey.indx", keys, types, orders));
		}

		if (table.equals("SUPPLIER"))
		{
			final ArrayList<String> keys = new ArrayList<String>();
			keys.add("S_COMMENT");
			final ArrayList<String> types = new ArrayList<String>();
			types.add("CHAR");
			final ArrayList<Boolean> orders = new ArrayList<Boolean>();
			orders.add(true);
			retval.add(new Index("xs_comment.indx", keys, types, orders));

			// keys = new ArrayList<String>();
			// keys.add("S_SUPPKEY");
			// types = new ArrayList<String>();
			// types.add("INT");
			// orders = new ArrayList<Boolean>();
			// orders.add(true);
			// retval.add(new Index("xs_suppkey.indx", keys, types, orders));
		}

		if (table.equals("PARTSUPP"))
		{
			final ArrayList<String> keys = new ArrayList<String>();
			keys.add("PS_PARTKEY");
			keys.add("PS_SUPPKEY");
			final ArrayList<String> types = new ArrayList<String>();
			types.add("INT");
			types.add("INT");
			final ArrayList<Boolean> orders = new ArrayList<Boolean>();
			orders.add(true);
			orders.add(true);
			retval.add(new Index("xps_partkey.indx", keys, types, orders));
		}

		return retval;
	}

	public int getNumDevices()
	{
		return 1;
	}

	public int getNumNodes()
	{
		return 16;
	}

	public PartitionMetaData getPartMeta(String schema, String table)
	{
		return new PartitionMetaData(schema, table);
	}

	public TreeMap<Integer, String> getPos2ColForTable(String schema, String name) throws Exception
	{
		final TreeMap<Integer, String> retval = new TreeMap<Integer, String>();
		if (name.equals("LINEITEM"))
		{
			retval.put(0, "L_ORDERKEY");
			retval.put(1, "L_PARTKEY");
			retval.put(2, "L_SUPPKEY");
			retval.put(3, "L_LINENUMBER");
			retval.put(4, "L_QUANTITY");
			retval.put(5, "L_EXTENDEDPRICE");
			retval.put(6, "L_DISCOUNT");
			retval.put(7, "L_TAX");
			retval.put(8, "L_RETURNFLAG");
			retval.put(9, "L_LINESTATUS");
			retval.put(10, "L_SHIPDATE");
			retval.put(11, "L_COMMITDATE");
			retval.put(12, "L_RECEIPTDATE");
			retval.put(13, "L_SHIPINSTRUCT");
			retval.put(14, "L_SHIPMODE");
			retval.put(15, "L_COMMENT");
		}
		else if (name.equals("CUSTOMER"))
		{
			retval.put(0, "C_CUSTKEY");
			retval.put(1, "C_NAME");
			retval.put(2, "C_ADDRESS");
			retval.put(3, "C_NATIONKEY");
			retval.put(4, "C_PHONE");
			retval.put(5, "C_ACCTBAL");
			retval.put(6, "C_MKTSEGMENT");
			retval.put(7, "C_COMMENT");
		}
		else if (name.equals("ORDERS"))
		{
			retval.put(0, "O_ORDERKEY");
			retval.put(1, "O_CUSTKEY");
			retval.put(2, "O_ORDERSTATUS");
			retval.put(3, "O_TOTALPRICE");
			retval.put(4, "O_ORDERDATE");
			retval.put(5, "O_ORDERPRIORITY");
			retval.put(6, "O_CLERK");
			retval.put(7, "O_SHIPPRIORITY");
			retval.put(8, "O_COMMENT");
		}
		else if (name.equals("SUPPLIER"))
		{
			retval.put(0, "S_SUPPKEY");
			retval.put(1, "S_NAME");
			retval.put(2, "S_ADDRESS");
			retval.put(3, "S_NATIONKEY");
			retval.put(4, "S_PHONE");
			retval.put(5, "S_ACCTBAL");
			retval.put(6, "S_COMMENT");
		}
		else if (name.equals("NATION"))
		{
			retval.put(0, "N_NATIONKEY");
			retval.put(1, "N_NAME");
			retval.put(2, "N_REGIONKEY");
			retval.put(3, "N_COMMENT");
		}
		else if (name.equals("REGION"))
		{
			retval.put(0, "R_REGIONKEY");
			retval.put(1, "R_NAME");
			retval.put(2, "R_COMMENT");
		}
		else if (name.equals("PART"))
		{
			retval.put(0, "P_PARTKEY");
			retval.put(1, "P_NAME");
			retval.put(2, "P_MFGR");
			retval.put(3, "P_BRAND");
			retval.put(4, "P_TYPE");
			retval.put(5, "P_SIZE");
			retval.put(6, "P_CONTAINER");
			retval.put(7, "P_RETAILPRICE");
			retval.put(8, "P_COMMENT");
		}
		else if (name.equals("PARTSUPP"))
		{
			retval.put(0, "PS_PARTKEY");
			retval.put(1, "PS_SUPPKEY");
			retval.put(2, "PS_AVAILQTY");
			retval.put(3, "PS_SUPPLYCOST");
			retval.put(4, "PS_COMMENT");
		}
		else
		{
			throw new Exception("Unknown table.");
		}

		return retval;
	}

	public long getTableCard(String schema, String table)
	{
		if (table.equals("SUPPLIER"))
		{
			return 10000 * 4;
		}

		if (table.equals("PART"))
		{
			return 200000 * 4;
		}

		if (table.equals("PARTSUPP"))
		{
			return 800000 * 4;
		}

		if (table.equals("CUSTOMER"))
		{
			return 150000 * 4;
		}

		if (table.equals("ORDERS"))
		{
			return 1500000 * 4;
		}

		if (table.equals("LINEITEM"))
		{
			return 6001215 * 4;
		}

		if (table.equals("NATION"))
		{
			return 25;
		}

		if (table.equals("REGION"))
		{
			return 5;
		}

		HRDBMSWorker.logger.error("Unknown table in getTableCard()");
		System.exit(1);
		return 0;
	}

	public String getTableForCol(String col) //TODO must return schema.table - must accept qualified or unqualified col
	{
		if (col.startsWith("L_"))
		{
			return "LINEITEM";
		}

		if (col.startsWith("P_"))
		{
			return "PART";
		}

		if (col.startsWith("PS_"))
		{
			return "PARTSUPP";
		}

		if (col.startsWith("S_"))
		{
			return "SUPPLIER";
		}

		if (col.startsWith("C_"))
		{
			return "CUSTOMER";
		}

		if (col.startsWith("O_"))
		{
			return "ORDERS";
		}

		if (col.startsWith("N_"))
		{
			return "NATION";
		}

		if (col.startsWith("R_"))
		{
			return "REGION";
		}

		return null;
	}

	public double likelihood(ArrayList<Filter> filters)
	{
		double sum = 0;

		for (final Filter filter : filters)
		{
			sum += likelihood(filter);
		}

		if (sum > 1)
		{
			sum = 1;
		}

		return sum;
	}

	public double likelihood(ArrayList<Filter> filters, HashMap<String, Double> generated)
	{
		double sum = 0;

		for (final Filter filter : filters)
		{
			sum += likelihood(filter, generated);
		}

		if (sum > 1)
		{
			sum = 1;
		}

		return sum;
	}

	public double likelihood(ArrayList<Filter> filters, RootOperator op)
	{
		return likelihood(filters, op.getGenerated());
	}

	public double likelihood(Filter filter)
	{
		long leftCard = 1;
		long rightCard = 1;
		final HashMap<String, Double> generated = new HashMap<String, Double>();

		if (filter instanceof ConstantFilter)
		{
			final double retval = ((ConstantFilter)filter).getLikelihood();
			if (retval < 0)
			{
				Exception e = new Exception();
				HRDBMSWorker.logger.error("ERROR: likelihood(" + filter + ")" + " returned " + retval, e);
				System.exit(1);
			}
		}

		if (filter.alwaysTrue())
		{
			return 1;
		}

		if (filter.alwaysFalse())
		{
			return 0;
		}

		if (filter.leftIsColumn())
		{
			leftCard = getCard(filter.leftColumn(), generated);
		}

		if (filter.rightIsColumn())
		{
			// figure out number of possible values for right side
			rightCard = getCard(filter.rightColumn(), generated);
		}

		final String op = filter.op();

		if (op.equals("E"))
		{
			final double retval = 1.0 / bigger(leftCard, rightCard);
			if (retval < 0)
			{
				Exception e = new Exception();
				HRDBMSWorker.logger.error("ERROR: likelihood(" + filter + ")" + " returned " + retval, e);
				System.exit(1);
			}

			return retval;
		}

		if (op.equals("NE"))
		{
			final double retval = 1.0 - 1.0 / bigger(leftCard, rightCard);
			if (retval < 0)
			{
				Exception e = new Exception();
				HRDBMSWorker.logger.error("ERROR: likelihood(" + filter + ")" + " returned " + retval, e);
				System.exit(1);
			}

			return retval;
		}

		if (op.equals("L") || op.equals("LE"))
		{
			if (filter.leftIsColumn() && filter.rightIsColumn())
			{
				return 0.5;
			}

			if (filter.leftIsColumn())
			{
				if (filter.rightIsNumber())
				{
					final double right = filter.getRightNumber();
					final String left = filter.leftColumn();
					final double retval = percentBelow(left, right);
					if (retval < 0)
					{
						Exception e = new Exception();
						HRDBMSWorker.logger.error("ERROR: likelihood(" + filter + ")" + " returned " + retval, e);
						System.exit(1);
					}

					return retval;
				}
				else if (filter.rightIsDate())
				{
					final MyDate right = filter.getRightDate();
					final String left = filter.leftColumn();
					final double retval = percentBelow(left, right);
					if (retval < 0)
					{
						Exception e = new Exception();
						HRDBMSWorker.logger.error("ERROR: likelihood(" + filter + ")" + " returned " + retval, e);
						System.exit(1);
					}

					return retval;
				}
				else
				{
					// string
					final String right = filter.getRightString();
					final String left = filter.leftColumn();
					final double retval = percentBelow(left, right);
					if (retval < 0)
					{
						Exception e = new Exception();
						HRDBMSWorker.logger.error("ERROR: likelihood(" + filter + ")" + " returned " + retval, e);
						System.exit(1);
					}

					return retval;
				}
			}
			else
			{
				if (filter.leftIsNumber())
				{
					final double left = filter.getLeftNumber();
					final String right = filter.rightColumn();
					final double retval = percentAbove(right, left);
					if (retval < 0)
					{
						Exception e = new Exception();
						HRDBMSWorker.logger.error("ERROR: likelihood(" + filter + ")" + " returned " + retval, e);
						System.exit(1);
					}

					return retval;
				}
				else if (filter.leftIsDate())
				{
					final MyDate left = filter.getLeftDate();
					final String right = filter.rightColumn();
					final double retval = percentAbove(right, left);
					if (retval < 0)
					{
						Exception e = new Exception();
						HRDBMSWorker.logger.error("ERROR: likelihood(" + filter + ")" + " returned " + retval, e);
						System.exit(1);
					}

					return retval;
				}
				else
				{
					// string
					final String left = filter.getLeftString();
					final String right = filter.rightColumn();
					final double retval = percentAbove(right, left);
					if (retval < 0)
					{
						Exception e = new Exception();
						HRDBMSWorker.logger.error("ERROR: likelihood(" + filter + ")" + " returned " + retval, e);
						System.exit(1);
					}

					return retval;
				}
			}
		}

		if (op.equals("G") || op.equals("GE"))
		{
			if (filter.leftIsColumn() && filter.rightIsColumn())
			{
				return 0.5;
			}

			if (filter.leftIsColumn())
			{
				if (filter.rightIsNumber())
				{
					final double right = filter.getRightNumber();
					final String left = filter.leftColumn();
					final double retval = percentAbove(left, right);
					if (retval < 0)
					{
						Exception e = new Exception();
						HRDBMSWorker.logger.error("ERROR: likelihood(" + filter + ")" + " returned " + retval, e);
						System.exit(1);
					}

					return retval;
				}
				else if (filter.rightIsDate())
				{
					final MyDate right = filter.getRightDate();
					final String left = filter.leftColumn();
					final double retval = percentAbove(left, right);
					if (retval < 0)
					{
						Exception e = new Exception();
						HRDBMSWorker.logger.error("ERROR: likelihood(" + filter + ")" + " returned " + retval, e);
						System.exit(1);
					}

					return retval;
				}
				else
				{
					// string
					final String right = filter.getRightString();
					final String left = filter.leftColumn();
					final double retval = percentAbove(left, right);
					if (retval < 0)
					{
						Exception e = new Exception();
						HRDBMSWorker.logger.error("ERROR: likelihood(" + filter + ")" + " returned " + retval, e);
						System.exit(1);
					}

					return retval;
				}
			}
			else
			{
				if (filter.leftIsNumber())
				{
					final double left = filter.getLeftNumber();
					final String right = filter.rightColumn();
					final double retval = percentBelow(right, left);
					if (retval < 0)
					{
						Exception e = new Exception();
						HRDBMSWorker.logger.error("ERROR: likelihood(" + filter + ")" + " returned " + retval, e);
						System.exit(1);
					}

					return retval;
				}
				else if (filter.leftIsDate())
				{
					final MyDate left = filter.getLeftDate();
					final String right = filter.rightColumn();
					final double retval = percentBelow(right, left);
					if (retval < 0)
					{
						Exception e = new Exception();
						HRDBMSWorker.logger.error("ERROR: likelihood(" + filter + ")" + " returned " + retval, e);
						System.exit(1);
					}

					return retval;
				}
				else
				{
					// string
					final String left = filter.getLeftString();
					final String right = filter.rightColumn();
					final double retval = percentBelow(right, left);
					if (retval < 0)
					{
						Exception e = new Exception();
						HRDBMSWorker.logger.error("ERROR: likelihood(" + filter + ")" + " returned " + retval, e);
						System.exit(1);
					}

					return retval;
				}
			}
		}

		if (op.equals("LI"))
		{
			return 0.25;
		}

		if (op.equals("NL"))
		{
			return 0.75;
		}

		HRDBMSWorker.logger.error("Unknown operator in likelihood()");
		System.exit(1);
		return 0;
	}

	// likelihood of a row directly out of the table passing this test
	public double likelihood(Filter filter, HashMap<String, Double> generated)
	{
		final Double r = lCache.get(filter);
		if (r != null)
		{
			return r;
		}

		long leftCard = 1;
		long rightCard = 1;

		if (filter instanceof ConstantFilter)
		{
			final double retval = ((ConstantFilter)filter).getLikelihood();
			if (retval < 0)
			{
				Exception e = new Exception();
				HRDBMSWorker.logger.error("ERROR: likelihood(" + filter + ")" + " returned " + retval, e);
				System.exit(1);
			}
			lCache.put(filter, retval);
			return retval;
		}

		if (filter.alwaysTrue())
		{
			lCache.put(filter, 1.0);
			return 1;
		}

		if (filter.alwaysFalse())
		{
			lCache.put(filter, 0.0);
			return 0;
		}

		if (filter.leftIsColumn())
		{
			leftCard = getCard(filter.leftColumn(), generated);
		}

		if (filter.rightIsColumn())
		{
			// figure out number of possible values for right side
			rightCard = getCard(filter.rightColumn(), generated);
		}

		final String op = filter.op();

		if (op.equals("E"))
		{
			final double retval = 1.0 / bigger(leftCard, rightCard);
			if (retval < 0)
			{
				Exception e = new Exception();
				HRDBMSWorker.logger.error("ERROR: likelihood(" + filter + ")" + " returned " + retval, e);
				System.exit(1);
			}
			lCache.put(filter, retval);
			return retval;
		}

		if (op.equals("NE"))
		{
			final double retval = 1.0 - 1.0 / bigger(leftCard, rightCard);
			if (retval < 0)
			{
				Exception e = new Exception();
				HRDBMSWorker.logger.error("ERROR: likelihood(" + filter + ")" + " returned " + retval, e);
				System.exit(1);
			}
			lCache.put(filter, retval);
			return retval;
		}

		if (op.equals("L") || op.equals("LE"))
		{
			if (filter.leftIsColumn() && filter.rightIsColumn())
			{
				lCache.put(filter, 0.5);
				return 0.5;
			}

			if (filter.leftIsColumn())
			{
				if (filter.rightIsNumber())
				{
					final double right = filter.getRightNumber();
					final String left = filter.leftColumn();
					final double retval = percentBelow(left, right);
					if (retval < 0)
					{
						Exception e = new Exception();
						HRDBMSWorker.logger.error("ERROR: likelihood(" + filter + ")" + " returned " + retval, e);
						System.exit(1);
					}
					lCache.put(filter, retval);
					return retval;
				}
				else if (filter.rightIsDate())
				{
					final MyDate right = filter.getRightDate();
					final String left = filter.leftColumn();
					final double retval = percentBelow(left, right);
					if (retval < 0)
					{
						Exception e = new Exception();
						HRDBMSWorker.logger.error("ERROR: likelihood(" + filter + ")" + " returned " + retval, e);
						System.exit(1);
					}
					lCache.put(filter, retval);
					return retval;
				}
				else
				{
					// string
					final String right = filter.getRightString();
					final String left = filter.leftColumn();
					final double retval = percentBelow(left, right);
					if (retval < 0)
					{
						Exception e = new Exception();
						HRDBMSWorker.logger.error("ERROR: likelihood(" + filter + ")" + " returned " + retval, e);
						System.exit(1);
					}
					lCache.put(filter, retval);
					return retval;
				}
			}
			else
			{
				if (filter.leftIsNumber())
				{
					final double left = filter.getLeftNumber();
					final String right = filter.rightColumn();
					final double retval = percentAbove(right, left);
					if (retval < 0)
					{
						Exception e = new Exception();
						HRDBMSWorker.logger.error("ERROR: likelihood(" + filter + ")" + " returned " + retval, e);
						System.exit(1);
					}
					lCache.put(filter, retval);
					return retval;
				}
				else if (filter.leftIsDate())
				{
					final MyDate left = filter.getLeftDate();
					final String right = filter.rightColumn();
					final double retval = percentAbove(right, left);
					if (retval < 0)
					{
						Exception e = new Exception();
						HRDBMSWorker.logger.error("ERROR: likelihood(" + filter + ")" + " returned " + retval, e);
						System.exit(1);
					}
					lCache.put(filter, retval);
					return retval;
				}
				else
				{
					// string
					final String left = filter.getLeftString();
					final String right = filter.rightColumn();
					final double retval = percentAbove(right, left);
					if (retval < 0)
					{
						Exception e = new Exception();
						HRDBMSWorker.logger.error("ERROR: likelihood(" + filter + ")" + " returned " + retval, e);
						System.exit(1);
					}
					lCache.put(filter, retval);
					return retval;
				}
			}
		}

		if (op.equals("G") || op.equals("GE"))
		{
			if (filter.leftIsColumn() && filter.rightIsColumn())
			{
				lCache.put(filter, 0.5);
				return 0.5;
			}

			if (filter.leftIsColumn())
			{
				if (filter.rightIsNumber())
				{
					final double right = filter.getRightNumber();
					final String left = filter.leftColumn();
					final double retval = percentAbove(left, right);
					if (retval < 0)
					{
						Exception e = new Exception();
						HRDBMSWorker.logger.error("ERROR: likelihood(" + filter + ")" + " returned " + retval, e);
						System.exit(1);
					}
					lCache.put(filter, retval);
					return retval;
				}
				else if (filter.rightIsDate())
				{
					final MyDate right = filter.getRightDate();
					final String left = filter.leftColumn();
					final double retval = percentAbove(left, right);
					if (retval < 0)
					{
						Exception e = new Exception();
						HRDBMSWorker.logger.error("ERROR: likelihood(" + filter + ")" + " returned " + retval, e);
						System.exit(1);
					}
					lCache.put(filter, retval);
					return retval;
				}
				else
				{
					// string
					final String right = filter.getRightString();
					final String left = filter.leftColumn();
					final double retval = percentAbove(left, right);
					if (retval < 0)
					{
						Exception e = new Exception();
						HRDBMSWorker.logger.error("ERROR: likelihood(" + filter + ")" + " returned " + retval, e);
						System.exit(1);
					}
					lCache.put(filter, retval);
					return retval;
				}
			}
			else
			{
				if (filter.leftIsNumber())
				{
					final double left = filter.getLeftNumber();
					final String right = filter.rightColumn();
					final double retval = percentBelow(right, left);
					if (retval < 0)
					{
						Exception e = new Exception();
						HRDBMSWorker.logger.error("ERROR: likelihood(" + filter + ")" + " returned " + retval, e);
						System.exit(1);
					}
					lCache.put(filter, retval);
					return retval;
				}
				else if (filter.leftIsDate())
				{
					final MyDate left = filter.getLeftDate();
					final String right = filter.rightColumn();
					final double retval = percentBelow(right, left);
					if (retval < 0)
					{
						Exception e = new Exception();
						HRDBMSWorker.logger.error("ERROR: likelihood(" + filter + ")" + " returned " + retval, e);
						System.exit(1);
					}
					lCache.put(filter, retval);
					return retval;
				}
				else
				{
					// string
					final String left = filter.getLeftString();
					final String right = filter.rightColumn();
					final double retval = percentBelow(right, left);
					if (retval < 0)
					{
						Exception e = new Exception();
						HRDBMSWorker.logger.error("ERROR: likelihood(" + filter + ")" + " returned " + retval, e);
						System.exit(1);
					}
					lCache.put(filter, retval);
					return retval;
				}
			}
		}

		if (op.equals("LI"))
		{
			lCache.put(filter, 0.25);
			return 0.25;
		}

		if (op.equals("NL"))
		{
			lCache.put(filter, 0.75);
			return 0.75;
		}

		HRDBMSWorker.logger.error("Unknown operator in likelihood()");
		System.exit(1);
		return 0;
	}

	public double likelihood(Filter filter, RootOperator op)
	{
		return likelihood(filter, op.getGenerated());
	}

	public double likelihood(HashSet<HashMap<Filter, Filter>> hshm, HashMap<String, Double> generated)
	{
		final ArrayList<Double> ands = new ArrayList<Double>(hshm.size());
		for (final HashMap<Filter, Filter> ored : hshm)
		{
			double sum = 0;

			for (final Filter filter : ored.keySet())
			{
				sum += likelihood(filter, generated);
			}

			if (sum > 1)
			{
				sum = 1;
			}

			ands.add(sum);
		}

		double retval = 1;
		for (final double x : ands)
		{
			retval *= x;
		}

		return retval;
	}

	public double likelihood(HashSet<HashMap<Filter, Filter>> hshm, RootOperator op)
	{
		return likelihood(hshm, op.getGenerated());
	}

	private long bigger(long x, long y)
	{
		if (x >= y)
		{
			return x;
		}

		return y;
	}

	private ArrayList<Object> convertRangeStringToObject(String set, String schema, String table, String rangeCol)
	{
		// TODO
		return null;
	}

	private Operator doWork(Operator op, HashMap<Operator, ArrayList<String>> tables, HashMap<Operator, ArrayList<ArrayList<Filter>>> filters, HashMap<Operator, HashMap<String, Double>> retvals)
	{
		ArrayList<String> t;
		ArrayList<ArrayList<Filter>> f;
		HashMap<String, Double> r;

		if (op instanceof TableScanOperator)
		{
			t = new ArrayList<String>();
			f = new ArrayList<ArrayList<Filter>>();
			r = new HashMap<String, Double>();
		}
		else
		{
			t = tables.get(op);
			f = filters.get(op);
			r = retvals.get(op);
		}

		while (true)
		{
			if (op instanceof TableScanOperator)
			{
				t.add(((TableScanOperator)op).getSchema() + "." + ((TableScanOperator)op).getTable());
				// System.out.println("Op is TableScanOperator");
				// System.out.println("Table list is " + t);
			}
			else if (op instanceof SelectOperator)
			{
				final ArrayList<Filter> filter = new ArrayList<Filter>(((SelectOperator)op).getFilter());
				f.add(filter);
				// System.out.println("Op is SelectOperator");
				// System.out.println("Filter list is " + f);
			}
			else if (op instanceof SemiJoinOperator)
			{
				final HashSet<HashMap<Filter, Filter>> hshm = ((SemiJoinOperator)op).getHSHM();
				for (final HashMap<Filter, Filter> ored : hshm)
				{
					final ArrayList<Filter> filter = new ArrayList<Filter>(ored.keySet());
					f.add(filter);
				}
			}
			else if (op instanceof AntiJoinOperator)
			{
				final HashSet<HashMap<Filter, Filter>> hshm = ((AntiJoinOperator)op).getHSHM();
				final ArrayList<Filter> al = new ArrayList<Filter>();
				al.add(new ConstantFilter(1 - this.likelihood(hshm, r)));
				f.add(al);
			}
			else if (op instanceof RootOperator)
			{
				return null;
			}
			else if (op instanceof MultiOperator)
			{
				// System.out.println("Op is MultiOperator");
				for (final String col : ((MultiOperator)op).getOutputCols())
				{
					// System.out.println("Output col: " + col);
					double card;
					final ArrayList<String> keys = ((MultiOperator)op).getKeys();
					if (keys.size() == 1)
					{
						card = this.getCard(keys.get(0), r);
					}
					else if (keys.size() == 0)
					{
						card = 1;
					}
					else
					{
						card = this.getColgroupCard(keys, r);
					}

					for (final ArrayList<Filter> filter : f)
					{
						if (references(filter, new ArrayList(keys)))
						{
							card *= this.likelihood(filter, r);
						}
					}

					r.put(col, card);
				}

				// System.out.println("Generated is " + r);
			}
			else if (op instanceof YearOperator)
			{
				double card = 1;
				for (final String table : t)
				{
					final FastStringTokenizer tokens = new FastStringTokenizer(table, ".", false);
					final String schema = tokens.nextToken();
					final String table2 = tokens.nextToken();
					card *= this.getTableCard(schema, table2);
				}

				for (final ArrayList<Filter> filter : f)
				{
					card *= this.likelihood(filter, r);
				}

				r.put(((YearOperator)op).getOutputCol(), card);
				// System.out.println("Operator is YearOperator");
				// System.out.println("Generated is " + r);
			}
			else if (op instanceof SubstringOperator)
			{
				double card = 1;
				for (final String table : t)
				{
					final FastStringTokenizer tokens = new FastStringTokenizer(table, ".", false);
					final String schema = tokens.nextToken();
					final String table2 = tokens.nextToken();
					card *= this.getTableCard(schema, table2);
				}

				for (final ArrayList<Filter> filter : f)
				{
					card *= this.likelihood(filter, r);
				}

				r.put(((SubstringOperator)op).getOutputCol(), card);
				// System.out.println("Operator is SubstringOperator");
				// System.out.println("Generated is " + r);
			}
			else if (op instanceof RenameOperator)
			{
				for (final Map.Entry entry : ((RenameOperator)op).getRenameMap().entrySet())
				{
					double card = this.getCard((String)entry.getKey(), r);
					for (final ArrayList<Filter> filter : f)
					{
						final ArrayList<String> keys = new ArrayList<String>(1);
						keys.add((String)entry.getKey());
						if (references(filter, keys))
						{
							card *= this.likelihood(filter, r);
						}
					}

					r.put((String)entry.getValue(), card);
				}

				// System.out.println("Operator is RenameOperator");
				// System.out.println("Generated is " + r);
			}
			else if (op instanceof ExtendOperator)
			{
				double card = 1;
				for (final String table : t)
				{
					final FastStringTokenizer tokens = new FastStringTokenizer(table, ".", false);
					final String schema = tokens.nextToken();
					final String table2 = tokens.nextToken();
					card *= this.getTableCard(schema, table2);
				}

				for (final ArrayList<Filter> filter : f)
				{
					card *= this.likelihood(filter, r);
				}

				r.put(((ExtendOperator)op).getOutputCol(), card);
				// System.out.println("Operator is ExtendOperator");
				// System.out.println("Generated is " + r);
			}
			else if (op instanceof CaseOperator)
			{
				double card = 1;
				for (final String table : t)
				{
					final FastStringTokenizer tokens = new FastStringTokenizer(table, ".", false);
					final String schema = tokens.nextToken();
					final String table2 = tokens.nextToken();
					card *= this.getTableCard(schema, table2);
				}

				for (final ArrayList<Filter> filter : f)
				{
					card *= this.likelihood(filter, r);
				}

				r.put(((CaseOperator)op).getOutputCol(), card);
				// System.out.println("Operator is CaseOperator");
				// System.out.println("Generated is " + r);
			}

			final Operator oldOp = op;
			if (op instanceof TableScanOperator)
			{
				op = ((TableScanOperator)op).parents().get(0);
			}
			else
			{
				op = op.parent();
				if (op == null)
				{
					if (!tables.containsKey(oldOp))
					{
						tables.put(oldOp, t);
						filters.put(oldOp, f);
						retvals.put(oldOp, r);
						return oldOp;
					}
					else
					{
						tables.get(oldOp).addAll(t);
						filters.get(oldOp).addAll(f);
						retvals.get(oldOp).putAll(r);
						return oldOp;
					}
				}
			}

			if (op.children().size() > 1)
			{
				if (op instanceof SemiJoinOperator || op instanceof AntiJoinOperator)
				{
					if (oldOp.equals(op.children().get(1)))
					{
						t = new ArrayList<String>();
						t.add("SYSIBM.SYSDUMMY");
					}
				}
				if (!tables.containsKey(op))
				{
					tables.put(op, t);
					filters.put(op, f);
					retvals.put(op, r);
					return op;
				}
				else
				{
					tables.get(op).addAll(t);
					filters.get(op).addAll(f);
					retvals.get(op).putAll(r);
				}
				return op;
			}
		}
	}

	private ArrayList<MyDate> getDateQuartiles(String col)
	{
		// SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

		try
		{
			if (dists == null)
			{
				synchronized (distsLock)
				{
					if (dists == null)
					{
						final ArrayList<ArrayList<Object>> distsTemp = new ArrayList<ArrayList<Object>>();
						final BufferedReader in = new BufferedReader(new FileReader(new File("dist.tbl")));
						String line = in.readLine();
						while (line != null)
						{
							final ArrayList<Object> cols = new ArrayList<Object>(6);
							final FastStringTokenizer tokens = new FastStringTokenizer(line, "|", false);
							cols.add(tokens.nextToken());
							cols.add(tokens.nextToken());
							cols.add(tokens.nextToken());
							cols.add(tokens.nextToken());
							cols.add(tokens.nextToken());
							cols.add(tokens.nextToken());
							distsTemp.add(cols);
							line = in.readLine();
						}
						in.close();
						dists = distsTemp;
					}
				}
			}

			for (final ArrayList<Object> line : dists)
			{
				final String column = (String)line.get(0);
				if (col.equals(column))
				{
					final ArrayList<MyDate> retval = new ArrayList<MyDate>(5);
					Object l1 = line.get(1);
					if (l1 instanceof String)
					{
						synchronized (distsLock)
						{
							l1 = line.get(1);
							if (l1 instanceof String)
							{
								String l2, l3, l4, l5;
								l2 = (String)line.get(2);
								l3 = (String)line.get(3);
								l4 = (String)line.get(4);
								l5 = (String)line.get(5);
								line.clear();
								line.add(column);
								line.add(DateParser.parse((String)l1));
								line.add(DateParser.parse(l2));
								line.add(DateParser.parse(l3));
								line.add(DateParser.parse(l4));
								line.add(DateParser.parse(l5));
							}
						}
					}
					retval.add((MyDate)line.get(1));
					retval.add((MyDate)line.get(2));
					retval.add((MyDate)line.get(3));
					retval.add((MyDate)line.get(4));
					retval.add((MyDate)line.get(5));
					return retval;
				}
			}

			return null;
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.error("", e);
			System.exit(1);
			return null;
		}
	}

	private String getDeviceExpression(String schema, String table)
	{
		if (table.equals("NATION"))
		{
			return "ALL,HASH,{N_NATIONKEY}";
		}

		if (table.equals("REGION"))
		{
			return "ALL,HASH,{R_REGIONKEY}";
		}

		if (table.equals("CUSTOMER"))
		{
			return "ALL,HASH,{C_NAME}";
		}

		if (table.equals("ORDERS"))
		{
			return "ALL,HASH,{O_COMMENT}";
		}

		if (table.equals("LINEITEM"))
		{
			return "ALL,HASH,{L_COMMENT}";
		}

		if (table.equals("SUPPLIER"))
		{
			return "ALL,HASH,{S_NAME}";
		}

		if (table.equals("PART"))
		{
			return "ALL,HASH,{P_NAME}";
		}

		if (table.equals("PARTSUPP"))
		{
			return "ALL,HASH,{PS_COMMENT}";
		}

		return null;
	}

	private ArrayList<Double> getDoubleQuartiles(String col)
	{
		try
		{
			if (dists == null)
			{
				synchronized (distsLock)
				{
					if (dists == null)
					{
						final ArrayList<ArrayList<Object>> distsTemp = new ArrayList<ArrayList<Object>>();
						final BufferedReader in = new BufferedReader(new FileReader(new File("dist.tbl")));
						String line = in.readLine();
						while (line != null)
						{
							final ArrayList<Object> cols = new ArrayList<Object>(6);
							final FastStringTokenizer tokens = new FastStringTokenizer(line, "|", false);
							cols.add(tokens.nextToken());
							cols.add(tokens.nextToken());
							cols.add(tokens.nextToken());
							cols.add(tokens.nextToken());
							cols.add(tokens.nextToken());
							cols.add(tokens.nextToken());
							distsTemp.add(cols);
							line = in.readLine();
						}
						in.close();

						dists = distsTemp;
					}
				}
			}

			for (final ArrayList<Object> line : dists)
			{
				final String column = (String)line.get(0);
				if (col.equals(column))
				{
					final ArrayList<Double> retval = new ArrayList<Double>(5);
					Object l1 = line.get(1);
					if (l1 instanceof String)
					{
						synchronized (distsLock)
						{
							l1 = line.get(1);
							if (l1 instanceof String)
							{
								String l2, l3, l4, l5;
								l2 = (String)line.get(2);
								l3 = (String)line.get(3);
								l4 = (String)line.get(4);
								l5 = (String)line.get(5);
								line.clear();
								line.add(column);
								line.add(Utils.parseDouble((String)l1));
								line.add(Utils.parseDouble(l2));
								line.add(Utils.parseDouble(l3));
								line.add(Utils.parseDouble(l4));
								line.add(Utils.parseDouble(l5));
							}
						}
					}
					retval.add((Double)line.get(1));
					retval.add((Double)line.get(2));
					retval.add((Double)line.get(3));
					retval.add((Double)line.get(4));
					retval.add((Double)line.get(5));
					return retval;
				}
			}

			return null;
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.error("", e);
			System.exit(1);
			return null;
		}
	}

	private ArrayList<Operator> getLeaves(Operator op)
	{
		if (op.children().size() == 0)
		{
			final ArrayList<Operator> retval = new ArrayList<Operator>(1);
			retval.add(op);
			return retval;
		}

		final ArrayList<Operator> retval = new ArrayList<Operator>();
		for (final Operator o : op.children())
		{
			retval.addAll(getLeaves(o));
		}

		return retval;
	}

	private String getNodeExpression(String schema, String table)
	{
		if (LOCAL)
		{
			return "{0}";
		}

		if (table.equals("NATION") || table.equals("REGION"))
		{
			return "ANY";
		}

		if (table.equals("CUSTOMER"))
		{
			return "ALL,HASH,{C_CUSTKEY}";
		}

		if (table.equals("ORDERS"))
		{
			return "ALL,HASH,{O_ORDERKEY}";
		}

		if (table.equals("LINEITEM"))
		{
			return "ALL,HASH,{L_ORDERKEY}";
		}

		if (table.equals("SUPPLIER"))
		{
			return "ALL,HASH,{S_SUPPKEY}";
		}

		if (table.equals("PART"))
		{
			return "ALL,HASH,{P_PARTKEY}";
		}

		if (table.equals("PARTSUPP"))
		{
			return "ALL,HASH,{PS_PARTKEY}";
		}

		return null;
	}

	private String getNodeGroupExpression(String schema, String table)
	{
		return "NONE";
	}

	private ArrayList<Integer> getNodeListForGroup(int group)
	{
		// TODO
		return null;
	}

	private ArrayList<String> getStringQuartiles(String col)
	{
		try
		{
			if (dists == null)
			{
				synchronized (distsLock)
				{
					if (dists == null)
					{
						final ArrayList<ArrayList<Object>> distsTemp = new ArrayList<ArrayList<Object>>();
						final BufferedReader in = new BufferedReader(new FileReader(new File("dist.tbl")));
						String line = in.readLine();
						while (line != null)
						{
							final ArrayList<Object> cols = new ArrayList<Object>(6);
							final FastStringTokenizer tokens = new FastStringTokenizer(line, "|", false);
							cols.add(tokens.nextToken());
							cols.add(tokens.nextToken());
							cols.add(tokens.nextToken());
							cols.add(tokens.nextToken());
							cols.add(tokens.nextToken());
							cols.add(tokens.nextToken());
							distsTemp.add(cols);
							line = in.readLine();
						}
						in.close();
						dists = distsTemp;
					}
				}
			}

			for (final ArrayList<Object> line : dists)
			{
				final String column = (String)line.get(0);
				if (col.equals(column))
				{
					final ArrayList<String> retval = new ArrayList<String>(5);
					retval.add((String)line.get(1));
					retval.add((String)line.get(2));
					retval.add((String)line.get(3));
					retval.add((String)line.get(4));
					retval.add((String)line.get(5));
					return retval;
				}
			}

			return null;
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.error("", e);
			System.exit(1);
			return null;
		}
	}

	private double percentAbove(String col, double val)
	{
		final ArrayList<Double> quartiles = getDoubleQuartiles(col);
		// System.out.println("In percentAbove with col = " + col +
		// " and val = " + val);
		// System.out.println("Quartiles are " + quartiles);
		if (quartiles == null)
		{
			return 0.5;
		}

		if (quartiles.get(0) >= val)
		{
			return 1;
		}

		if (quartiles.get(1) >= val)
		{
			return 0.75 + 0.25 * ((quartiles.get(1) - val) / (quartiles.get(1) - quartiles.get(0)));
		}

		if (quartiles.get(2) >= val)
		{
			return 0.5 + 0.25 * ((quartiles.get(2) - val) / (quartiles.get(2) - quartiles.get(1)));
		}

		if (quartiles.get(3) >= val)
		{
			return 0.25 + 0.25 * ((quartiles.get(3) - val) / (quartiles.get(3) - quartiles.get(2)));
		}

		if (quartiles.get(4) >= val)
		{
			return 0.25 * ((quartiles.get(4) - val) / (quartiles.get(4) - quartiles.get(3)));
		}

		return 0;
	}

	private double percentAbove(String col, MyDate val)
	{
		final ArrayList<MyDate> quartiles = getDateQuartiles(col);
		if (quartiles == null)
		{
			return 0.5;
		}

		if (quartiles.get(0).compareTo(val) > -1)
		{
			return 1;
		}

		if (quartiles.get(1).compareTo(val) > -1)
		{
			return 0.75 + 0.25 * ((quartiles.get(1).getTime() - val.getTime()) / (quartiles.get(1).getTime() - quartiles.get(0).getTime()));
		}

		if (quartiles.get(2).compareTo(val) > -1)
		{
			return 0.5 + 0.25 * ((quartiles.get(2).getTime() - val.getTime()) / (quartiles.get(2).getTime() - quartiles.get(1).getTime()));
		}

		if (quartiles.get(3).compareTo(val) > -1)
		{
			return 0.25 + 0.25 * ((quartiles.get(3).getTime() - val.getTime()) / (quartiles.get(3).getTime() - quartiles.get(2).getTime()));
		}

		if (quartiles.get(4).compareTo(val) > -1)
		{
			return 0.25 * ((quartiles.get(4).getTime() - val.getTime()) / (quartiles.get(4).getTime() - quartiles.get(3).getTime()));
		}

		return 0;
	}

	private double percentAbove(String col, String val)
	{
		final ArrayList<String> quartiles = getStringQuartiles(col);
		if (quartiles == null)
		{
			return 0.5;
		}

		if (quartiles.get(0).compareTo(val) > -1)
		{
			return 1;
		}

		if (quartiles.get(1).compareTo(val) > -1)
		{
			return 0.75 + 0.25 * ((stringToLong(quartiles.get(1)) - stringToLong(val)) / (stringToLong(quartiles.get(1)) - stringToLong(quartiles.get(0))));
		}

		if (quartiles.get(2).compareTo(val) > -1)
		{
			return 0.5 + 0.25 * ((stringToLong(quartiles.get(2)) - stringToLong(val)) / (stringToLong(quartiles.get(2)) - stringToLong(quartiles.get(1))));
		}

		if (quartiles.get(3).compareTo(val) > -1)
		{
			return 0.25 + 0.25 * ((stringToLong(quartiles.get(3)) - stringToLong(val)) / (stringToLong(quartiles.get(3)) - stringToLong(quartiles.get(2))));
		}

		if (quartiles.get(4).compareTo(val) > -1)
		{
			return 0.25 * ((stringToLong(quartiles.get(4)) - stringToLong(val)) / (stringToLong(quartiles.get(4)) - stringToLong(quartiles.get(3))));
		}

		return 0;
	}

	private double percentBelow(String col, double val)
	{
		final ArrayList<Double> quartiles = getDoubleQuartiles(col);
		if (quartiles == null)
		{
			return 0.5;
		}

		if (quartiles.get(4) <= val)
		{
			return 1;
		}

		if (quartiles.get(3) <= val)
		{
			return 0.75 + 0.25 * ((val - quartiles.get(3)) / (quartiles.get(4) - quartiles.get(3)));
		}

		if (quartiles.get(2) <= val)
		{
			return 0.5 + 0.25 * ((val - quartiles.get(2)) / (quartiles.get(3) - quartiles.get(2)));
		}

		if (quartiles.get(1) <= val)
		{
			return 0.25 + 0.25 * ((val - quartiles.get(1)) / (quartiles.get(2) - quartiles.get(1)));
		}

		if (quartiles.get(0) <= val)
		{
			return 0.25 * ((val - quartiles.get(0)) / (quartiles.get(1) - quartiles.get(0)));
		}

		return 0;
	}

	private double percentBelow(String col, MyDate val)
	{
		final ArrayList<MyDate> quartiles = getDateQuartiles(col);
		if (quartiles == null)
		{
			return 0.5;
		}

		if (quartiles.get(4).compareTo(val) < 1)
		{
			return 1;
		}

		if (quartiles.get(3).compareTo(val) < 1)
		{
			return 0.75 + 0.25 * ((val.getTime() - quartiles.get(3).getTime()) / (quartiles.get(4).getTime() - quartiles.get(3).getTime()));
		}

		if (quartiles.get(2).compareTo(val) < 1)
		{
			return 0.5 + 0.25 * ((val.getTime() - quartiles.get(2).getTime()) / (quartiles.get(3).getTime() - quartiles.get(2).getTime()));
		}

		if (quartiles.get(1).compareTo(val) < 1)
		{
			return 0.25 + 0.25 * ((val.getTime() - quartiles.get(1).getTime()) / (quartiles.get(2).getTime() - quartiles.get(1).getTime()));
		}

		if (quartiles.get(0).compareTo(val) < 1)
		{
			return 0.25 * ((val.getTime() - quartiles.get(0).getTime()) / (quartiles.get(1).getTime() - quartiles.get(0).getTime()));
		}

		return 0;
	}

	private double percentBelow(String col, String val)
	{
		final ArrayList<String> quartiles = getStringQuartiles(col);
		if (quartiles == null)
		{
			return 0.5;
		}

		if (quartiles.get(4).compareTo(val) < 1)
		{
			return 1;
		}

		if (quartiles.get(3).compareTo(val) < 1)
		{
			return 0.75 + 0.25 * ((stringToLong(val) - stringToLong(quartiles.get(3))) / (stringToLong(quartiles.get(4)) - stringToLong(quartiles.get(3))));
		}

		if (quartiles.get(2).compareTo(val) < 1)
		{
			return 0.5 + 0.25 * ((stringToLong(val) - stringToLong(quartiles.get(2))) / (stringToLong(quartiles.get(3)) - stringToLong(quartiles.get(2))));
		}

		if (quartiles.get(1).compareTo(val) < 1)
		{
			return 0.25 + 0.25 * ((stringToLong(val) - stringToLong(quartiles.get(1))) / (stringToLong(quartiles.get(2)) - stringToLong(quartiles.get(1))));
		}

		if (quartiles.get(0).compareTo(val) < 1)
		{
			return 0.25 * ((stringToLong(val) - stringToLong(quartiles.get(0))) / (stringToLong(quartiles.get(1)) - stringToLong(quartiles.get(0))));
		}

		return 0;
	}

	private boolean references(ArrayList<Filter> filters, ArrayList<String> cols)
	{
		for (final Filter filter : filters)
		{
			if (filter.leftIsColumn())
			{
				if (cols.contains(filter.leftColumn()))
				{
					return true;
				}
			}

			if (filter.rightIsColumn())
			{
				if (cols.contains(filter.rightColumn()))
				{
					return true;
				}
			}
		}

		return false;
	}

	private long stringToLong(String val)
	{
		int i = 0;
		long retval = 0;
		while (i < 15 && i < val.length())
		{
			final int point = val.charAt(i) & 0x0000000F;
			retval += (((long)point) << (56 - (i * 4)));
			i++;
		}

		return retval;
	}

	public final class PartitionMetaData implements Serializable
	{
		private static final int NODEGROUP_NONE = -3;
		private static final int NODE_ANY = -2;
		private static final int NODE_ALL = -1;
		private static final int DEVICE_ALL = -1;
		private ArrayList<Integer> nodeGroupSet;
		private ArrayList<String> nodeGroupHash;
		private ArrayList<Object> nodeGroupRange;
		private int numNodeGroups;
		private String nodeGroupRangeCol;
		private HashMap<Integer, ArrayList<Integer>> nodeGroupHashMap;
		private ArrayList<Integer> nodeSet;
		private int numNodes;
		private ArrayList<String> nodeHash;
		private ArrayList<Object> nodeRange;
		private String nodeRangeCol;
		private int numDevices;
		private ArrayList<Integer> deviceSet;
		private ArrayList<String> deviceHash;
		private ArrayList<Object> deviceRange;
		private String deviceRangeCol;
		private final String schema;
		private final String table;

		public PartitionMetaData(String schema, String table)
		{
			this.schema = schema;
			this.table = table;
			final String ngExp = getNodeGroupExpression(schema, table);
			final String nExp = getNodeExpression(schema, table);
			final String dExp = getDeviceExpression(schema, table);
			setNGData(ngExp);
			setNData(nExp);
			setDData(dExp);
		}

		public boolean allDevices()
		{
			return deviceSet.get(0) == DEVICE_ALL;
		}

		public boolean allNodes()
		{
			return nodeSet.get(0) == NODE_ALL;
		}

		public boolean anyNode()
		{
			return nodeSet.get(0) == NODE_ANY;
		}

		public boolean deviceIsHash()
		{
			return deviceHash != null;
		}

		public ArrayList<Integer> deviceSet()
		{
			return deviceSet;
		}

		public ArrayList<String> getDeviceHash()
		{
			return deviceHash;
		}

		public String getDeviceRangeCol()
		{
			return deviceRangeCol;
		}

		public ArrayList<Object> getDeviceRanges()
		{
			return deviceRange;
		}

		public ArrayList<String> getNodeGroupHash()
		{
			return nodeGroupHash;
		}

		public HashMap<Integer, ArrayList<Integer>> getNodeGroupHashMap()
		{
			return nodeGroupHashMap;
		}

		public String getNodeGroupRangeCol()
		{
			return nodeGroupRangeCol;
		}

		public ArrayList<Object> getNodeGroupRanges()
		{
			return nodeGroupRange;
		}

		public ArrayList<String> getNodeHash()
		{
			return nodeHash;
		}

		public String getNodeRangeCol()
		{
			return nodeRangeCol;
		}

		public ArrayList<Object> getNodeRanges()
		{
			return nodeRange;
		}

		public int getNumDevices()
		{
			return numDevices;
		}

		public int getNumNodeGroups()
		{
			return numNodeGroups;
		}

		public int getNumNodes()
		{
			return numNodes;
		}

		public int getSingleDevice()
		{
			if (deviceSet.get(0) == DEVICE_ALL)
			{
				return 0;
			}

			return deviceSet.get(0);
		}

		public int getSingleNode()
		{
			return nodeSet.get(0);
		}

		public int getSingleNodeGroup()
		{
			return nodeGroupSet.get(0);
		}

		public boolean isSingleDeviceSet()
		{
			return numDevices == 1;
		}

		public boolean isSingleNodeGroupSet()
		{
			return numNodeGroups == 1;
		}

		public boolean isSingleNodeSet()
		{
			return numNodes == 1;
		}

		public boolean nodeGroupIsHash()
		{
			return nodeGroupHash != null;
		}

		public ArrayList<Integer> nodeGroupSet()
		{
			return nodeGroupSet;
		}

		public boolean nodeIsHash()
		{
			return nodeHash != null;
		}

		public ArrayList<Integer> nodeSet()
		{
			return nodeSet;
		}

		public boolean noNodeGroupSet()
		{
			return nodeGroupSet.get(0) == NODEGROUP_NONE;
		}

		private void setDData(String exp)
		{
			final FastStringTokenizer tokens = new FastStringTokenizer(exp, ",", false);
			final String first = tokens.nextToken();

			if (first.equals("ALL"))
			{
				deviceSet = new ArrayList<Integer>(1);
				deviceSet.add(DEVICE_ALL);
				numDevices = MetaData.this.getNumDevices();
			}
			else
			{
				String set = first.substring(1);
				set = set.substring(0, set.length() - 1);
				final FastStringTokenizer tokens2 = new FastStringTokenizer(set, "|", false);
				deviceSet = new ArrayList<Integer>(tokens2.allTokens().length);
				numDevices = 0;
				while (tokens2.hasMoreTokens())
				{
					deviceSet.add(Utils.parseInt(tokens2.nextToken()));
					numDevices++;
				}
			}

			if (numDevices == 1)
			{
				return;
			}

			final String type = tokens.nextToken();
			if (type.equals("HASH"))
			{
				String set = tokens.nextToken().substring(1);
				set = set.substring(0, set.length() - 1);
				final FastStringTokenizer tokens2 = new FastStringTokenizer(set, "|", false);
				deviceHash = new ArrayList<String>(tokens2.allTokens().length);
				while (tokens2.hasMoreTokens())
				{
					deviceHash.add(tokens2.nextToken());
				}
			}
			else
			{
				deviceRangeCol = tokens.nextToken();
				String set = tokens.nextToken().substring(1);
				set = set.substring(0, set.length() - 1);
				deviceRange = convertRangeStringToObject(set, schema, table, deviceRangeCol);
			}
		}

		private void setNData(String exp)
		{
			final FastStringTokenizer tokens = new FastStringTokenizer(exp, ",", false);
			final String first = tokens.nextToken();

			if (first.equals("ANY"))
			{
				nodeSet = new ArrayList<Integer>(1);
				nodeSet.add(NODE_ANY);
				numNodes = 1;
				return;
			}

			if (first.equals("ALL"))
			{
				nodeSet = new ArrayList<Integer>(1);
				nodeSet.add(NODE_ALL);
				numNodes = MetaData.this.getNumNodes();
			}
			else
			{
				String set = first.substring(1);
				set = set.substring(0, set.length() - 1);
				final FastStringTokenizer tokens2 = new FastStringTokenizer(set, "|", false);
				nodeSet = new ArrayList<Integer>(tokens2.allTokens().length);
				numNodes = 0;
				while (tokens2.hasMoreTokens())
				{
					nodeSet.add(Utils.parseInt(tokens2.nextToken()));
					numNodes++;
				}
			}

			if (numNodes == 1)
			{
				return;
			}

			final String type = tokens.nextToken();
			if (type.equals("HASH"))
			{
				String set = tokens.nextToken().substring(1);
				set = set.substring(0, set.length() - 1);
				final FastStringTokenizer tokens2 = new FastStringTokenizer(set, "|", false);
				nodeHash = new ArrayList<String>(tokens2.allTokens().length);
				while (tokens2.hasMoreTokens())
				{
					nodeHash.add(tokens2.nextToken());
				}
			}
			else
			{
				nodeRangeCol = tokens.nextToken();
				String set = tokens.nextToken().substring(1);
				set = set.substring(0, set.length() - 1);
				nodeRange = convertRangeStringToObject(set, schema, table, nodeRangeCol);
			}
		}

		private void setNGData(String exp)
		{
			if (exp.equals("NONE"))
			{
				nodeGroupSet = new ArrayList<Integer>(1);
				nodeGroupSet.add(NODEGROUP_NONE);
				return;
			}

			final FastStringTokenizer tokens = new FastStringTokenizer(exp, ",", false);
			String set = tokens.nextToken().substring(1);
			set = set.substring(0, set.length() - 1);
			FastStringTokenizer tokens2 = new FastStringTokenizer(set, "|", false);
			nodeGroupSet = new ArrayList<Integer>(tokens2.allTokens().length);
			numNodeGroups = 0;
			while (tokens2.hasMoreTokens())
			{
				nodeGroupSet.add(Utils.parseInt(tokens2.nextToken()));
				numNodeGroups++;
			}

			if (numNodeGroups == 1)
			{
				return;
			}

			nodeGroupHashMap = new HashMap<Integer, ArrayList<Integer>>();
			for (final int groupID : nodeGroupSet)
			{
				nodeGroupHashMap.put(groupID, getNodeListForGroup(groupID));
			}

			final String type = tokens.nextToken();
			if (type.equals("HASH"))
			{
				set = tokens.nextToken().substring(1);
				set = set.substring(0, set.length() - 1);
				tokens2 = new FastStringTokenizer(set, "|", false);
				nodeGroupHash = new ArrayList<String>(tokens2.allTokens().length);
				while (tokens2.hasMoreTokens())
				{
					nodeGroupHash.add(tokens2.nextToken());
				}
			}
			else
			{
				nodeGroupRangeCol = tokens.nextToken();
				set = tokens.nextToken().substring(1);
				set = set.substring(0, set.length() - 1);
				nodeGroupRange = convertRangeStringToObject(set, schema, table, nodeGroupRangeCol);
			}
		}
	}
}
