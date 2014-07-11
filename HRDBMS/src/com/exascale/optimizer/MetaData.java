package com.exascale.optimizer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeMap;
import com.exascale.exceptions.ParseException;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.managers.PlanCacheManager;
import com.exascale.managers.XAManager;
import com.exascale.misc.DataEndMarker;
import com.exascale.misc.DateParser;
import com.exascale.misc.FastStringTokenizer;
import com.exascale.misc.MurmurHash;
import com.exascale.misc.MyDate;
import com.exascale.misc.Utils;
import com.exascale.tables.Transaction;
import com.exascale.threads.ConnectionWorker;
import com.exascale.threads.HRDBMSThread;
import com.exascale.threads.XAWorker;

public final class MetaData implements Serializable
{
	private static final HashMap<ConnectionWorker, String> defaultSchemas = new HashMap<ConnectionWorker, String>();
	private static int myNode;
	private static HashMap<Integer, String> nodeTable = new HashMap<Integer, String>();
	private ConnectionWorker connection = null;
	
	static
	{
		try
		{
			final BufferedReader nodes = new BufferedReader(new FileReader(new File("nodes.cfg")));
			String line = nodes.readLine();
			int workerID = 0;
			int coordID = -2;
			while (line != null)
			{
				final StringTokenizer tokens = new StringTokenizer(line, ",", false);
				final String host = tokens.nextToken().trim();
				String type = tokens.nextToken().trim().toUpperCase();
				if (isMyIP(host))
				{
					if (HRDBMSWorker.type == HRDBMSWorker.TYPE_WORKER)
					{
						myNode = workerID;
						nodeTable.put(myNode, host);
						workerID++;
					}
					else
					{
						myNode = coordID;
						nodeTable.put(myNode, host);
						coordID--;
					}
				}
				else
				{
					if (type.equals("W"))
					{
						nodeTable.put(workerID, host);
						workerID++;
					}
					else
					{
						nodeTable.put(coordID, host);
						coordID--;
					}
				}
			}
		}
		catch(Exception e)
		{
			HRDBMSWorker.logger.fatal("Error during static metadata initialization", e);
			System.exit(1);
		}
	}
	
	public static int myNodeNum()
	{
		return myNode;
	}
	
	public static boolean isMyIP(String host) throws Exception
	{
		return isThisMyIpAddress(InetAddress.getByName(host));
	}
	
	public static boolean isThisMyIpAddress(InetAddress addr) {
	    // Check if the address is a valid special local or loop back
	    if (addr.isAnyLocalAddress() || addr.isLoopbackAddress())
	        return true;

	    // Check if the address is defined on any interface
	    try {
	        return NetworkInterface.getByInetAddress(addr) != null;
	    } catch (SocketException e) {
	        return false;
	    }
	}
	
	public MetaData(ConnectionWorker connection)
	{
		this.connection = connection;
	}
	
	public MetaData()
	{
		
	}
	
	public static int myCoordNum() throws Exception
	{
		if (HRDBMSWorker.type == HRDBMSWorker.TYPE_WORKER)
		{
			throw new Exception("Not a coordinator");
		}
		
		return myNode;
	}
	
	public static void setDefaultSchema(ConnectionWorker conn, String schema)
	{
		defaultSchemas.put(conn, schema);
	}
	
	public static void removeDefaultSchema(ConnectionWorker conn)
	{
		defaultSchemas.remove(conn);
	}

	public HashMap<String, Double> generateCard(Operator op, Transaction tx, Operator tree) throws Exception
	{
		final HashMap<Operator, ArrayList<String>> tables = new HashMap<Operator, ArrayList<String>>();
		final HashMap<Operator, ArrayList<ArrayList<Filter>>> filters = new HashMap<Operator, ArrayList<ArrayList<Filter>>>();
		final HashMap<Operator, HashMap<String, Double>> retval = new HashMap<Operator, HashMap<String, Double>>();
		final ArrayList<Operator> leaves = getLeaves(op);
		final ArrayList<Operator> queued = new ArrayList<Operator>(leaves.size());
		for (final Operator leaf : leaves)
		{
			final Operator o = doWork(leaf, tables, filters, retval, tx, tree);
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
				final Operator o2 = doWork(o, tables, filters, retval, tx, tree);
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

	public Index getBestCompoundIndex(HashSet<String> cols, String schema, String table, Transaction tx) throws Exception
	{
		if (cols.size() <= 1)
		{
			return null;
		}

		final ArrayList<Index> indexes = this.getIndexesForTable(schema, table, tx);
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
	
	public String getMyHostName(Transaction tx) throws Exception
	{
		return PlanCacheManager.getHostLookup().setParms(myNode).execute(tx);
	}
	
	public static boolean verifyInsert(String schema, String table, Operator op, Transaction tx) throws Exception
	{
		TreeMap<Integer, String> catalogPos2Col = MetaData.getPos2ColForTable(schema, table, tx);
		HashMap<String, String> catalogCols2Types = new MetaData().getCols2TypesForTable(schema, table, tx);
		HashMap<Integer, String> catalogPos2Types = new HashMap<Integer, String>();
		int i = 0;
		while (i < catalogPos2Col.size())
		{
			catalogPos2Types.put(i, catalogCols2Types.get(catalogPos2Col.get(i)));
			i++;
		}
		
		HashMap<Integer, String> opPos2Types = new HashMap<Integer, String>();
		i = 0;
		while (i < op.getPos2Col().size())
		{
			opPos2Types.put(i, op.getCols2Types().get(op.getPos2Col().get(i)));
			i++;
		}
		
		if (catalogPos2Types.size() != opPos2Types.size())
		{
			return false;
		}
		
		i = 0;
		while (i < opPos2Types.size())
		{
			String opType = opPos2Types.get(i);
			String catalogType = catalogPos2Types.get(i);
			
			if (opType.equals("CHAR") && !opType.equals(catalogType))
			{
				return false;
			}
			else if (opType.equals("DATE") && !opType.equals(catalogType))
			{
				return false;
			}
			else if (opType.equals("INT") || opType.equals("LONG") || opType.equals("FLOAT"))
			{
				if (catalogType.equals("CHAR") || catalogType.equals("DATE"))
				{
					return false;
				}
			}
			i++;
		}
		
		return true;
	}
	
	public static ArrayList<Integer> getDevicesForTable(String schema, String table, Transaction tx) throws Exception
	{
		PartitionMetaData pmeta = new MetaData().new PartitionMetaData(schema, table, tx);
		if (pmeta.allDevices())
		{
			ArrayList<Integer> retval = new ArrayList<Integer>();
			int i = 0;
			while (i < getNumDevices())
			{
				retval.add(i);
				i++;
			}
			
			return retval;
		}
		else
		{
			return pmeta.deviceSet();
		}
	}
	
	public static ArrayList<Integer> getNodesForTable(String schema, String table, Transaction tx) throws Exception
	{
		Object r = PlanCacheManager.getPartitioning().setParms(schema, table).execute(tx);
		if (r instanceof DataEndMarker)
		{
			throw new Exception("Table not found");
		}
		
		ArrayList<Object> row = (ArrayList<Object>)r;
		String nodeGroupExp = (String)row.get(0);
		String nodeExp = (String)row.get(1);
		
		if (nodeGroupExp.equals("NONE"))
		{
			if (nodeExp.startsWith("ALL") || nodeExp.startsWith("ANY"))
			{
				ArrayList<Object> rows = PlanCacheManager.getWorkerNodes().setParms().execute(tx);
				ArrayList<Integer> retval = new ArrayList<Integer>();
				for (Object o : rows)
				{
					if (o instanceof ArrayList)
					{
						retval.add((Integer)((ArrayList)o).get(0));
					}
				}
				
				return retval;
			}
			
			StringTokenizer tokens1 = new StringTokenizer(nodeExp, ",", false);
			String nodeSet = tokens1.nextToken();
			StringTokenizer tokens2 = new StringTokenizer(nodeSet, "|{}", false);
			HashSet<Integer> retval = new HashSet<Integer>();
			while (tokens2.hasMoreTokens())
			{
				int node = Integer.parseInt(tokens2.nextToken());
				retval.add(node);
			}
			
			return new ArrayList<Integer>(retval);
		}
		
		StringTokenizer tokens1 = new StringTokenizer(nodeGroupExp, ",", false);
		String nodeGroupSet = tokens1.nextToken();
		StringTokenizer tokens2 = new StringTokenizer(nodeGroupSet, "|{}", false);
		HashSet<Integer> retval = new HashSet<Integer>();
		while (tokens2.hasMoreTokens())
		{
			int node = Integer.parseInt(tokens2.nextToken());
			retval.add(node);
		}
		
		return new ArrayList<Integer>(retval);
	}
	
	public static ArrayList<String> getIndexFileNamesForTable(String schema, String table, Transaction tx) throws Exception
	{
		ArrayList<Object> rs = PlanCacheManager.getIndexes().setParms(schema, table).execute(tx);
		ArrayList<String> retval = new ArrayList<String>();
		
		for (Object o : rs)
		{
			ArrayList<Object> row = (ArrayList<Object>)o;
			retval.add(schema + "." + row.get(0) + ".indx");
		}
		
		return retval;
	}
	
	public static ArrayList<ArrayList<String>> getKeys(ArrayList<String> indexes, Transaction tx) throws Exception
	{
		ArrayList<ArrayList<String>> retval = new ArrayList<ArrayList<String>>();
		for (String index : indexes)
		{
			String schema = index.substring(0, index.indexOf('.'));
			String name = index.substring(schema.length()+1, index.indexOf('.', schema.length() + 1));
			ArrayList<Object> rs = PlanCacheManager.getKeys().setParms(schema, name).execute(tx);
			//tabname, colname
			ArrayList<String> retRow = new ArrayList<String>();
			for (Object o : rs)
			{
				ArrayList<Object> row = (ArrayList<Object>)o;
				retRow.add(row.get(0) + "." + row.get(1));
			}
			retval.add(retRow);
		}
		
		return retval;
	}
	
	public static ArrayList<ArrayList<String>> getTypes(ArrayList<String> indexes, Transaction tx) throws Exception
	{
		ArrayList<ArrayList<String>> retval = new ArrayList<ArrayList<String>>();
		for (String index : indexes)
		{
			String schema = index.substring(0, index.indexOf('.'));
			String name = index.substring(schema.length()+1, index.indexOf('.', schema.length() + 1));
			ArrayList<Object> rs = PlanCacheManager.getTypes().setParms(schema, name).execute(tx);
			ArrayList<String> retRow = new ArrayList<String>();
			for (Object o : rs)
			{
				ArrayList<Object> row = (ArrayList<Object>)o;
				String type = (String)row.get(0);
				if (type.equals("BIGINT"))
				{
					type = "LONG";
				}
				else if (type.equals("VARCHAR"))
				{
					type = "CHAR";
				}
				else if (type.equals("DOUBLE"))
				{
					type = "FLOAT";
				}
				retRow.add(type);
			}
			retval.add(retRow);
		}
		
		return retval;
	}
	
	public static ArrayList<ArrayList<Boolean>> getOrders(ArrayList<String> indexes, Transaction tx) throws Exception
	{
		ArrayList<ArrayList<Boolean>> retval = new ArrayList<ArrayList<Boolean>>();
		for (String index : indexes)
		{
			String schema = index.substring(0, index.indexOf('.'));
			String name = index.substring(schema.length()+1, index.indexOf('.', schema.length() + 1));
			ArrayList<Object> rs = PlanCacheManager.getOrders().setParms(schema, name).execute(tx);
			ArrayList<Boolean> retRow = new ArrayList<Boolean>();
			for (Object o : rs)
			{
				ArrayList<Object> row = (ArrayList<Object>)o;
				String ord = (String)row.get(0);
				boolean order = true;
				if (!ord.equals("A"))
				{
					order = false;
				}
				retRow.add(order);
			}
			retval.add(retRow);
		}
		
		return retval;
	}
	
	public static ArrayList<String> getIndexColsForTable(String schema, String table, Transaction tx) throws Exception
	{
		ArrayList<Object> rs = PlanCacheManager.getIndexColsForTable().setParms(schema, table).execute(tx);
		ArrayList<String> retRow = new ArrayList<String>();
		for (Object o : rs)
		{
			ArrayList<Object> row = (ArrayList<Object>)o;
			retRow.add(table + "." + row.get(0));
		}
		
		return retRow;
	}
	
	public static ArrayList<String> getColsFromIndexFileName(String index, Transaction tx) throws Exception
	{
		String schema = index.substring(0, index.indexOf('.'));
		String name = index.substring(schema.length()+1, index.indexOf('.', schema.length() + 1));
		ArrayList<Object> rs = PlanCacheManager.getKeys().setParms(schema, name).execute(tx);
		//tabname, colname
		ArrayList<String> retRow = new ArrayList<String>();
		for (Object o : rs)
		{
			ArrayList<Object> row = (ArrayList<Object>)o;
			retRow.add(row.get(0) + "." + row.get(1));
		}
		
		return retRow;
	}
	
	public static ArrayList<Integer> determineNode(String schema, String table, ArrayList<Object> row, Transaction tx) throws Exception
	{
		PartitionMetaData pmeta = new MetaData().new PartitionMetaData(schema, table, tx);
		HashMap<String, Integer> cols2Pos = MetaData.getCols2PosForTable(schema, table, tx);
		if (pmeta.noNodeGroupSet())
		{
			if (pmeta.anyNode())
			{
				ArrayList<Object> rows = PlanCacheManager.getWorkerNodes().setParms().execute(tx);
				ArrayList<Integer> retval = new ArrayList<Integer>();
				for (Object o : rows)
				{
					if (o instanceof ArrayList)
					{
						retval.add((Integer)((ArrayList)o).get(0));
					}
				}
				
				return retval;
			}
			else if (pmeta.isSingleNodeSet())
			{
				if (pmeta.getSingleNode() == -1)
				{
					ArrayList<Object> rows = PlanCacheManager.getCoordNodes().setParms().execute(tx);
					ArrayList<Integer> retval = new ArrayList<Integer>();
					for (Object o : rows)
					{
						if (o instanceof ArrayList)
						{
							retval.add((Integer)((ArrayList)o).get(0));
						}
					}
					
					return retval;
				}
				else
				{
					ArrayList<Integer> retval = new ArrayList<Integer>();
					retval.add(pmeta.getSingleNode());
					return retval;
				}
			}
			else if (pmeta.nodeIsHash())
			{
				ArrayList<String> nodeHash = pmeta.getNodeHash();
				ArrayList<Object> partial = new ArrayList<Object>();
				for (String col : nodeHash)
				{
					partial.add(row.get(cols2Pos.get(table + "." + col)));
				}
				
				long hash = 0x0EFFFFFFFFFFFFFFL & hash(partial);
				if (pmeta.allNodes())
				{
					ArrayList<Integer> retval = new ArrayList<Integer>();
					retval.add((int)(hash % new MetaData().getNumNodes(tx))); //LOOKOUT if we ever do dynamic # nodes
					return retval;
				}
				else
				{
					ArrayList<Integer> retval = new ArrayList<Integer>();
					retval.add(pmeta.nodeSet().get((int)(hash % pmeta.nodeSet().size())));
					return retval;
				}
			}
			else
			{
				//range
				String col = table + "." + pmeta.getNodeRangeCol();
				Object obj = row.get(cols2Pos.get(col));
				ArrayList<Object> ranges = pmeta.getNodeRanges();
				int i = 0;
				while (i < ranges.size())
				{
					if (((Comparable)obj).compareTo((Comparable)ranges.get(i)) <= 0)
					{
						if (pmeta.allNodes())
						{
							ArrayList<Integer> retval = new ArrayList<Integer>();
							retval.add(i);
							return retval;
						}
						else
						{
							ArrayList<Integer> retval = new ArrayList<Integer>();
							retval.add(pmeta.nodeSet().get(i));
							return retval;
						}
					}
					
					i++;
				}
				
				if (pmeta.allNodes())
				{
					ArrayList<Integer> retval = new ArrayList<Integer>();
					retval.add(i);
					return retval;
				}
				else
				{
					ArrayList<Integer> retval = new ArrayList<Integer>();
					retval.add(pmeta.nodeSet().get(i));
					return retval;
				}
			}
		}
		else
		{
			ArrayList<Integer> ngSet = null;
			if (pmeta.nodeGroupIsHash())
			{
				ArrayList<String> nodeGroupHash = pmeta.getNodeGroupHash();
				ArrayList<Object> partial = new ArrayList<Object>();
				for (String col : nodeGroupHash)
				{
					partial.add(row.get(cols2Pos.get(table + "." + col)));
				}
				
				long hash = 0x0EFFFFFFFFFFFFFFL & hash(partial);
				ngSet = pmeta.getNodeGroupHashMap().get((int)(hash % pmeta.getNodeGroupHashMap().size()));
			}
			else
			{
				String col = table + "." + pmeta.getNodeGroupRangeCol();
				Object obj = row.get(cols2Pos.get(col));
				ArrayList<Object> ranges = pmeta.getNodeGroupRanges();
				int i = 0;
				while (i < ranges.size())
				{
					if (((Comparable)obj).compareTo((Comparable)ranges.get(i)) <= 0)
					{
						ngSet = pmeta.getNodeGroupHashMap().get(i);
						break;
					}
					
					i++;
				}
				
				if (ngSet == null)
				{
					ngSet = pmeta.getNodeGroupHashMap().get(i);
				}
			}
			
			if (pmeta.anyNode())
			{
				return ngSet;
			}
			else if (pmeta.nodeIsHash())
			{
				ArrayList<String> nodeHash = pmeta.getNodeHash();
				ArrayList<Object> partial = new ArrayList<Object>();
				for (String col : nodeHash)
				{
					partial.add(row.get(cols2Pos.get(table + "." + col)));
				}
				
				long hash = 0x0EFFFFFFFFFFFFFFL & hash(partial);
				if (pmeta.allNodes())
				{
					ArrayList<Integer> retval = new ArrayList<Integer>();
					retval.add(ngSet.get((int)(hash % ngSet.size())));
					return retval;
				}
				else
				{
					ArrayList<Integer> retval = new ArrayList<Integer>();
					retval.add(ngSet.get(pmeta.nodeSet().get((int)(hash % pmeta.nodeSet().size()))));
					return retval;
				}
			}
			else
			{
				//range
				String col = table + "." + pmeta.getNodeRangeCol();
				Object obj = row.get(cols2Pos.get(col));
				ArrayList<Object> ranges = pmeta.getNodeRanges();
				int i = 0;
				while (i < ranges.size())
				{
					if (((Comparable)obj).compareTo((Comparable)ranges.get(i)) <= 0)
					{
						if (pmeta.allNodes())
						{
							ArrayList<Integer> retval = new ArrayList<Integer>();
							retval.add(ngSet.get(i));
							return retval;
						}
						else
						{
							ArrayList<Integer> retval = new ArrayList<Integer>();
							retval.add(ngSet.get(pmeta.nodeSet().get(i)));
							return retval;
						}
					}
					
					i++;
				}
				
				if (pmeta.allNodes())
				{
					ArrayList<Integer> retval = new ArrayList<Integer>();
					retval.add(ngSet.get(i));
					return retval;
				}
				else
				{
					ArrayList<Integer> retval = new ArrayList<Integer>();
					retval.add(ngSet.get(pmeta.nodeSet().get(i)));
					return retval;
				}
			}
		}
	}
	
	private static long hash(Object key)
	{
		long eHash;
		if (key == null)
		{
			eHash = 0;
		}
		else
		{
			eHash = MurmurHash.hash64(key.toString());
		}

		return eHash;
	}
	
	public static int determineDevice(ArrayList<Object> row, PartitionMetaData pmeta, HashMap<String, Integer> cols2Pos)
	{
		String schema = pmeta.getTable();
		String table = pmeta.getTable();
		if (pmeta.deviceIsHash())
		{
			ArrayList<String> devHash = pmeta.getDeviceHash();
			ArrayList<Object> partial = new ArrayList<Object>();
			for (String col : devHash)
			{
				partial.add(row.get(cols2Pos.get(table + "." + col)));
			}
			
			long hash = 0x0EFFFFFFFFFFFFFFL & hash(partial);
			if (pmeta.allDevices())
			{
				return (int)(hash % getNumDevices());
			}
			else
			{
				return pmeta.deviceSet().get((int)(hash % pmeta.deviceSet().size()));
			}
		}
		else
		{
			String col = table + "." + pmeta.getDeviceRangeCol();
			Object obj = row.get(cols2Pos.get(col));
			ArrayList<Object> ranges = pmeta.getDeviceRanges();
			int i = 0;
			while (i < ranges.size())
			{
				if (((Comparable)obj).compareTo((Comparable)ranges.get(i)) <= 0)
				{
					if (pmeta.allDevices())
					{
						return i;
					}
					else
					{
						return pmeta.deviceSet().get(i);
					}
				}
				
				i++;
			}
			
			if (pmeta.allNodes())
			{
				return i;
			}
			else
			{
				return pmeta.deviceSet().get(i);
			}
		}
	}
	
	public static int determineDevice(String schema, String table, ArrayList<Object> row, PartitionMetaData pmeta, Transaction tx) throws Exception
	{
		HashMap<String, Integer> cols2Pos = MetaData.getCols2PosForTable(schema, table, tx);
		if (pmeta.deviceIsHash())
		{
			ArrayList<String> devHash = pmeta.getDeviceHash();
			ArrayList<Object> partial = new ArrayList<Object>();
			for (String col : devHash)
			{
				partial.add(row.get(cols2Pos.get(table + "." + col)));
			}
			
			long hash = 0x0EFFFFFFFFFFFFFFL & hash(partial);
			if (pmeta.allDevices())
			{
				return (int)(hash % getNumDevices());
			}
			else
			{
				return pmeta.deviceSet().get((int)(hash % pmeta.deviceSet().size()));
			}
		}
		else
		{
			String col = table + "." + pmeta.getDeviceRangeCol();
			Object obj = row.get(cols2Pos.get(col));
			ArrayList<Object> ranges = pmeta.getDeviceRanges();
			int i = 0;
			while (i < ranges.size())
			{
				if (((Comparable)obj).compareTo((Comparable)ranges.get(i)) <= 0)
				{
					if (pmeta.allDevices())
					{
						return i;
					}
					else
					{
						return pmeta.deviceSet().get(i);
					}
				}
				
				i++;
			}
			
			if (pmeta.allNodes())
			{
				return i;
			}
			else
			{
				return pmeta.deviceSet().get(i);
			}
		}
	}
	
	public static int getLengthForCharCol(String schema, String table, String col, Transaction tx) throws Exception
	{
		return PlanCacheManager.getLength().setParms(schema, table, col).execute(tx);
	}
	
	public static void createView(String schema, String table, String text, Transaction tx) throws Exception
	{
		int id = PlanCacheManager.getNextViewID().setParms().execute(tx);
		PlanCacheManager.getInsertView().setParms(id, schema, table, text).execute(tx);
	}
	
	public static void dropView(String schema, String table, Transaction tx) throws Exception
	{
		PlanCacheManager.getDeleteView().setParms(schema, table).execute(tx);
	}
	
	public static boolean verifyIndexExistence(String schema, String index, Transaction tx) throws Exception
	{
		Object o = PlanCacheManager.getVerifyIndexExist().setParms(schema, index).execute(tx);
		if (o instanceof DataEndMarker)
		{
			return false;
		}
		
		return true;
	}
	
	public static boolean verifyColExistence(String schema, String table, String col, Transaction tx) throws Exception
	{
		HashMap<String, Integer> cols2Pos = getCols2PosForTable(schema, table, tx);
		return cols2Pos.containsKey(table + "." + col);
	}
	
	public static void createIndex(String schema, String table, String index, ArrayList<IndexDef> defs, boolean unique, Transaction tx) throws Exception
	{
		int tableID = PlanCacheManager.getTableID().setParms(schema, table).execute(tx);
		int id = PlanCacheManager.getNextIndexID().setParms(tableID).execute(tx);
		PlanCacheManager.getInsertIndex().setParms(id, index, tableID, unique).execute(tx);
		HashMap<String, Integer> cols2Pos = getCols2PosForTable(schema, table, tx);
		int pos = 0;
		for (IndexDef def : defs)
		{
			String col = def.getCol().getColumn();
			int colID = cols2Pos.get(table + "." + col);
			PlanCacheManager.getInsertIndexCol().setParms(id, tableID, colID, pos, def.isAsc()).execute(tx);
			pos++;
		}
		
		buildIndex(schema, index, table, defs.size(), unique, tx);
		populateIndex(schema, index, table, tx);
	}
	
	public static void dropIndex(String schema, String index, Transaction tx) throws Exception
	{
		PlanCacheManager.invalidate();
		ArrayList<Object> row = PlanCacheManager.getTableAndIndexID().setParms(schema, index).execute(tx);
		int tableID = (Integer)row.get(0);
		int indexID = (Integer)row.get(1);
		PlanCacheManager.getDeleteIndex().setParms(tableID, indexID).execute(tx);
		PlanCacheManager.getDeleteIndexCol().setParms(tableID, indexID).execute(tx);
		PlanCacheManager.getDeleteIndexStats().setParms(tableID, indexID).execute(tx);
		PlanCacheManager.invalidate();
	}
	
	public static void dropTable(String schema, String table, Transaction tx) throws Exception
	{
		PlanCacheManager.invalidate();
		int id = PlanCacheManager.getTableID().setParms(schema, table).execute(tx);
		PlanCacheManager.getDeleteTable().setParms(schema, table).execute(tx);
		PlanCacheManager.getDeleteCols().setParms(id).execute(tx);
		PlanCacheManager.getMultiDeleteIndexes().setParms(id).execute(tx);
		PlanCacheManager.getMultiDeleteIndexCols().setParms(id).execute(tx);
		PlanCacheManager.getDeleteTableStats().setParms(id).execute(tx);
		PlanCacheManager.getDeleteColStats().setParms(id).execute(tx);
		PlanCacheManager.getDeleteColDist().setParms(id).execute(tx);
		PlanCacheManager.getDeletePartitioning().setParms(id).execute(tx);
		PlanCacheManager.getMultiDeleteIndexStats().setParms(id).execute(tx);
		PlanCacheManager.invalidate();
	}
	
	public static void createTable(String schema, String table, ArrayList<ColDef> defs, ArrayList<String> pks, Transaction tx, String nodeGroupExp, String nodeExp, String deviceExp) throws Exception
	{
		//validate expressions
		PartitionMetaData pmeta = new MetaData().new PartitionMetaData(schema, table, nodeGroupExp, nodeExp, deviceExp, tx);
		//tables
		//cols
		//indexes
		//indexcols
		//partitioning
		int tableID = PlanCacheManager.getNextTableID().setParms().execute(tx);
		PlanCacheManager.getInsertTable().setParms(tableID, schema, table, "R").execute(tx);
		int colID = 0;
		for (ColDef def : defs)
		{
			//INT, LONG, FLOAT, DATE, CHAR(x)
			//COLID, TABLEID, NAME, TYPE, LENGTH, SCALE, PKPOS, NULLABLE
			String name = def.getCol().getColumn();
			String type = def.getType();
			int length = 0;
			int scale = 0;
			if (type.equals("LONG"))
			{
				type = "BIGINT";
				length = 8;
			}
			else if (type.equals("FLOAT"))
			{
				type = "DOUBLE";
				length = 8;
			}
			else if (type.equals("INT"))
			{
				length = 4;
			}
			else if (type.equals("DATE"))
			{
				length = 8;
			}
			else if (type.startsWith("CHAR"))
			{
				length = Integer.parseInt(type.substring(5, type.length()-1));
				type = "VARCHAR";
				type = "VARCHAR";
			}
			
			int pkpos = -1;
			if (pks.contains(name))
			{
				pkpos = pks.indexOf(name);
			}
			
			PlanCacheManager.getInsertCol().setParms(colID, tableID, name, type, length, scale, pkpos, def.isNullable()).execute(tx);
			colID++;
		}
		
		PlanCacheManager.getInsertPartition().setParms(tableID, nodeGroupExp, nodeExp, deviceExp).execute(tx);
		int indexID = PlanCacheManager.getNextIndexID().setParms(tableID).execute(tx);
		PlanCacheManager.getInsertIndex().setParms(indexID, "PK"+table, tableID, true).execute(tx);
		HashMap<String, Integer> cols2Pos = getCols2PosForTable(schema, table, tx);
		int pos = 0;
		for (String col : pks)
		{
			colID = cols2Pos.get(table + "." + col);
			PlanCacheManager.getInsertIndexCol().setParms(indexID, tableID, colID, pos, true).execute(tx);
			pos++;
		}
		
		buildTable(schema, table, defs.size(), tx);
		buildIndex(schema, "PK"+table, table, pks.size(), true, tx);
	}
	
	private static void buildTable(String schema, String table, int numCols, Transaction tx) throws Exception
	{
		String fn = schema + "." + table + ".tbl";
		ArrayList<Integer> nodes = MetaData.getNodesForTable(schema, table, tx);
		ArrayList<Integer> devices = MetaData.getDevicesForTable(schema, table, tx);
		ArrayList<Socket> sockets = new ArrayList<Socket>();
		int max = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("max_neighbor_nodes"));
		
		for (int node : nodes)
		{
			Socket sock;
			String hostname = new MetaData().getHostNameForNode(node, tx);
			sock = new Socket(hostname, Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
			OutputStream out = sock.getOutputStream();
			byte[] outMsg = "NEWTABLE        ".getBytes("UTF-8");
			outMsg[8] = 0;
			outMsg[9] = 0;
			outMsg[10] = 0;
			outMsg[11] = 0;
			outMsg[12] = 0;
			outMsg[13] = 0;
			outMsg[14] = 0;
			outMsg[15] = 0;
			out.write(outMsg);
			out.write(intToBytes(numCols));
			out.write(stringToBytes(fn));
			ObjectOutputStream objOut = new ObjectOutputStream(out);
			objOut.writeObject(devices);
			objOut.flush();
			out.flush();
			objOut.close();
			sockets.add(sock);
			
			if (sockets.size() >= max)
			{
				sock = sockets.get(0);
				out = sock.getOutputStream();
				getConfirmation(sock);
				out.close();
				sock.close();
				sockets.remove(0);
			}
		}
		
		for (Socket sock : sockets)
		{
			OutputStream out = sock.getOutputStream();
			getConfirmation(sock);
			out.close();
			sock.close();
		}
	}
	
	private static void buildIndex(String schema, String index, String table, int numCols, boolean unique, Transaction tx) throws Exception
	{
		String fn = schema + "." + index + ".indx";
		ArrayList<Integer> nodes = MetaData.getNodesForTable(schema, table, tx);
		ArrayList<Integer> devices = MetaData.getDevicesForTable(schema, table, tx);
		ArrayList<Socket> sockets = new ArrayList<Socket>();
		int max = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("max_neighbor_nodes"));
		
		for (int node : nodes)
		{
			Socket sock;
			String hostname = new MetaData().getHostNameForNode(node, tx);
			sock = new Socket(hostname, Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
			OutputStream out = sock.getOutputStream();
			byte[] outMsg = "NEWINDEX        ".getBytes("UTF-8");
			outMsg[8] = 0;
			outMsg[9] = 0;
			outMsg[10] = 0;
			outMsg[11] = 0;
			outMsg[12] = 0;
			outMsg[13] = 0;
			outMsg[14] = 0;
			outMsg[15] = 0;
			out.write(outMsg);
			out.write(intToBytes(numCols));
			if (unique)
			{
				out.write(intToBytes(1));
			}
			else
			{
				out.write(intToBytes(0));
			}
			out.write(stringToBytes(fn));
			ObjectOutputStream objOut = new ObjectOutputStream(out);
			objOut.writeObject(devices);
			objOut.flush();
			out.flush();
			objOut.close();
			sockets.add(sock);
			
			if (sockets.size() >= max)
			{
				sock = sockets.get(0);
				out = sock.getOutputStream();
				getConfirmation(sock);
				out.close();
				sock.close();
				sockets.remove(0);
			}
		}
		
		for (Socket sock : sockets)
		{
			OutputStream out = sock.getOutputStream();
			getConfirmation(sock);
			out.close();
			sock.close();
		}
	}
	
	private static void populateIndex(String schema, String index, String table, Transaction tx) throws Exception
	{
		String iFn = schema + "." + index + ".indx";
		String tFn = schema + "." + table + ".tbl";
		ArrayList<Integer> nodes = MetaData.getNodesForTable(schema, table, tx);
		ArrayList<Integer> devices = MetaData.getDevicesForTable(schema, table, tx);
		ArrayList<Object> rs = PlanCacheManager.getKeys().setParms(schema, index).execute(tx);
		//tabname, colname
		ArrayList<String> keys = new ArrayList<String>();
		for (Object o : rs)
		{
			ArrayList<Object> row = (ArrayList<Object>)o;
			keys.add(row.get(0) + "." + row.get(1));
		}
		
		rs = PlanCacheManager.getTypes().setParms(schema, index).execute(tx);
		ArrayList<String> types = new ArrayList<String>();
		for (Object o : rs)
		{
			ArrayList<Object> row = (ArrayList<Object>)o;
			String type = (String)row.get(0);
			if (type.equals("BIGINT"))
			{
				type = "LONG";
			}
			else if (type.equals("VARCHAR"))
			{
				type = "CHAR";
			}
			else if (type.equals("DOUBLE"))
			{
				type = "FLOAT";
			}
			types.add(type);
		}
		
		rs = PlanCacheManager.getOrders().setParms(schema, index).execute(tx);
		ArrayList<Boolean> orders = new ArrayList<Boolean>();
		for (Object o : rs)
		{
			ArrayList<Object> row = (ArrayList<Object>)o;
			String ord = (String)row.get(0);
			boolean order = true;
			if (!ord.equals("A"))
			{
				order = false;
			}
			orders.add(order);
		}
		
		ArrayList<Integer> poses = new ArrayList<Integer>();
		HashMap<String, Integer> cols2Pos = getCols2PosForTable(schema, table, tx);
		for (String col : keys)
		{
			poses.add(cols2Pos.get(col));
		}
		
		ArrayList<Socket> sockets = new ArrayList<Socket>();
		int max = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("max_neighbor_nodes"));
		
		TreeMap<Integer, String> pos2Col = getPos2ColForTable(schema, table, tx);
		HashMap<String, String> cols2Types = getCols2TypesForTable(schema, table, tx);
		
		for (int node : nodes)
		{
			Socket sock;
			String hostname = new MetaData().getHostNameForNode(node, tx);
			sock = new Socket(hostname, Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
			OutputStream out = sock.getOutputStream();
			byte[] outMsg = "POPINDEX        ".getBytes("UTF-8");
			outMsg[8] = 0;
			outMsg[9] = 0;
			outMsg[10] = 0;
			outMsg[11] = 0;
			outMsg[12] = 0;
			outMsg[13] = 0;
			outMsg[14] = 0;
			outMsg[15] = 0;
			out.write(outMsg);
			out.write(stringToBytes(iFn));
			out.write(stringToBytes(tFn));
			ObjectOutputStream objOut = new ObjectOutputStream(out);
			objOut.writeObject(devices);
			objOut.writeObject(keys);
			objOut.writeObject(types);
			objOut.writeObject(orders);
			objOut.writeObject(poses);
			objOut.writeObject(pos2Col);
			objOut.writeObject(cols2Types);
			objOut.flush();
			out.flush();
			objOut.close();
			sockets.add(sock);
			
			if (sockets.size() >= max)
			{
				sock = sockets.get(0);
				out = sock.getOutputStream();
				getConfirmation(sock);
				out.close();
				sock.close();
				sockets.remove(0);
			}
		}
		
		for (Socket sock : sockets)
		{
			OutputStream out = sock.getOutputStream();
			getConfirmation(sock);
			out.close();
			sock.close();
		}
	}
	
	private static void getConfirmation(Socket sock) throws Exception
	{
		InputStream in = sock.getInputStream();
		byte[] inMsg = new byte[2];
		
		int count = 0;
		while (count < 2)
		{
			try
			{
				int temp = in.read(inMsg, count, 2 - count);
				if (temp == -1)
				{
					in.close();
					throw new Exception();
				}
				else
				{
					count += temp;
				}
			}
			catch (final Exception e)
			{
				in.close();
				throw new Exception();
			}
		}
		
		String inStr = new String(inMsg, "UTF-8");
		if (!inStr.equals("OK"))
		{
			in.close();
			throw new Exception();
		}
		
		try
		{
			in.close();
		}
		catch(Exception e)
		{}
	}

	private static byte[] intToBytes(int val)
	{
		final byte[] buff = new byte[4];
		buff[0] = (byte)(val >> 24);
		buff[1] = (byte)((val & 0x00FF0000) >> 16);
		buff[2] = (byte)((val & 0x0000FF00) >> 8);
		buff[3] = (byte)((val & 0x000000FF));
		return buff;
	}

	private static byte[] stringToBytes(String string)
	{
		byte[] data = null;
		try
		{
			data = string.getBytes("UTF-8");
		}
		catch(Exception e)
		{}
		byte[] len = intToBytes(data.length);
		byte[] retval = new byte[data.length + len.length];
		System.arraycopy(len, 0, retval, 0, len.length);
		System.arraycopy(data, 0, retval, len.length, data.length);
		return retval;
	}
		
	public static boolean verifyUpdate(String schema, String tbl, ArrayList<Column> cols, ArrayList<String> buildList, Operator op, Transaction tx) throws Exception
	{
		//verify that all columns are 1 part - parseException
		for (Column col : cols)
		{
			if (col.getTable() != null)
			{
				throw new ParseException("Two part column names are not allowed in a SET clause");
			}
		}
		//get data types for cols on this table
		//make sure that selecting the buildList cols from op in that order satisfies updates for cols on this table
		TreeMap<Integer, String> catalogPos2Col = MetaData.getPos2ColForTable(schema, tbl, tx);
		HashMap<String, String> catalogCols2Types = new MetaData().getCols2TypesForTable(schema, tbl, tx);
		HashMap<Integer, String> catalogPos2Types = new HashMap<Integer, String>();
		int i = 0;
		while (i < catalogPos2Col.size())
		{
			catalogPos2Types.put(i, catalogCols2Types.get(catalogPos2Col.get(i)));
			i++;
		}
		
		HashMap<Integer, String> opPos2Types = new HashMap<Integer, String>();
		i = 0;
		while (i < buildList.size())
		{
			opPos2Types.put(i, op.getCols2Types().get(buildList.get(i)));
			i++;
		}
		
		if (catalogPos2Types.size() != opPos2Types.size())
		{
			return false;
		}
		
		i = 0;
		while (i < opPos2Types.size())
		{
			String opType = opPos2Types.get(i);
			String catalogType = catalogPos2Types.get(i);
			
			if (opType.equals("CHAR") && !opType.equals(catalogType))
			{
				return false;
			}
			else if (opType.equals("DATE") && !opType.equals(catalogType))
			{
				return false;
			}
			else if (opType.equals("INT") || opType.equals("LONG") || opType.equals("FLOAT"))
			{
				if (catalogType.equals("CHAR") || catalogType.equals("DATE"))
				{
					return false;
				}
			}
			i++;
		}
		
		return true;
	}
	
	public boolean verifyViewExistence(String schema, String name, Transaction tx) throws Exception
	{
		Object o = PlanCacheManager.getVerifyViewExist().setParms(schema, name).execute(tx);
		if (o instanceof DataEndMarker)
		{
			return false;
		}
		
		return true;
	}
	
	public String getViewSQL(String schema, String name, Transaction tx) throws Exception
	{
		return PlanCacheManager.getViewSQL().setParms(schema, name).execute(tx);
	}
	
	public boolean verifyTableExistence(String schema, String name, Transaction tx) throws Exception
	{
		Object o = PlanCacheManager.getVerifyTableExist().setParms(schema, name).execute(tx);
		if (o instanceof DataEndMarker)
		{
			return false;
		}
		
		return true;
	}

	public long getCard(String schema, String table, String col, HashMap<String, Double> generated, Transaction tx)
	{
		try
		{
			long card = PlanCacheManager.getColCard().setParms(schema, table, col).execute(tx);
			return card;
		}
		catch(Exception e)
		{}

		if (generated.containsKey(col))
		{
			return (generated.get(col).longValue());
		}

		HRDBMSWorker.logger.warn("Can't find cardinality for " + schema + "." + table + "."  + col);
		return 1000000;
	}
	
	public long getCard(String col, HashMap<String, Double> generated, Transaction tx, Operator tree) throws Exception
	{
		String c = col.substring(col.indexOf('.') + 1);
		String ST = getTableForCol(col, tree);
		String schema = ST.substring(0, ST.indexOf('.'));
		String table = ST.substring(ST.indexOf('.') + 1);
		try
		{
			long card = PlanCacheManager.getColCard().setParms(schema, table, c).execute(tx);
			return card;
		}
		catch(Exception e)
		{}

		if (generated.containsKey(col))
		{
			return (generated.get(col).longValue());
		}

		HRDBMSWorker.logger.warn("Can't find cardinality for " + schema + "." + table + "."  + col);
		return 1000000;
	}

	public long getCard(String schema, String table, String col, RootOperator op, Transaction tx)
	{
		return getCard(schema, table, col, op.getGenerated(), tx);
	}
	
	public long getCard(String col, RootOperator op, Transaction tx, Operator tree) throws Exception
	{
		return getCard(col, op.getGenerated(), tx, tree);
	}

	public long getColgroupCard(ArrayList<String> schemas, ArrayList<String> tables, ArrayList<String> cols, HashMap<String, Double> generated, Transaction tx) throws Exception
	{
		boolean oneSchema = true;
		boolean oneTable = true;
		String theSchema = null;
		String theTable = null;
		
		int i = 0;
		for (String schema : schemas)
		{
			if (theSchema == null)
			{
				theSchema = schema;
			}
			else
			{
				if (!schema.equals(theSchema))
				{
					oneSchema = false;
					break;
				}
			}
			
			String table = tables.get(i);
			if (theTable == null)
			{
				theTable = table;
			}
			else
			{
				if (!table.equals(theTable))
				{
					oneTable = false;
					break;
				}
			}
		}
		
		if (oneSchema && oneTable)
		{
			int tableID = PlanCacheManager.getTableID().setParms(theSchema, theTable).execute(tx);
			ArrayList<Object> rs = PlanCacheManager.getIndexIDsForTable().setParms(theSchema, theTable).execute(tx);
			for (Object r : rs)
			{
				ArrayList<Object> row = (ArrayList<Object>)r;
				int count = PlanCacheManager.getIndexColCount().setParms(tableID, (Integer)row.get(0)).execute(tx);
				if (count == schemas.size())
				{
					boolean hasAllCols = true;
					HashMap<String, Integer> cols2Pos = getCols2PosForTable(theSchema, theTable, tx);
					for (String col : cols)
					{
						int colID = cols2Pos.get(theTable + "." + col);
						Object o = PlanCacheManager.getCheckIndexForCol().setParms(tableID, (Integer)row.get(0), colID).execute(tx);
						if (o instanceof DataEndMarker)
						{
							hasAllCols = false;
							break;
						}
					}
					
					if (hasAllCols)
					{
						Object o = PlanCacheManager.getIndexCard().setParms(tableID, (Integer)row.get(0)).execute(tx);
						if (o instanceof DataEndMarker)
						{
							continue;
						}
						else
						{
							ArrayList<Object> cardRow = (ArrayList<Object>)o;
							return (Long)cardRow.get(0);
						}
					}
				}
			}
		}
		
		double card = 1;
		i = 0;
		for (final String col : cols)
		{
			card *= this.getCard(schemas.get(i), tables.get(i), col, generated, tx);
			i++;
		}

		return (long)card;
	}
	
	public long getColgroupCard(ArrayList<String> cs, HashMap<String, Double> generated, Transaction tx, Operator tree) throws Exception
	{
		ArrayList<String> schemas = new ArrayList<String>();
		ArrayList<String> tables = new ArrayList<String>();
		ArrayList<String> cols = new ArrayList<String>();
		for (String c : cs)
		{
			cols.add(c.substring(c.indexOf('.') + 1));
			String ST = getTableForCol(c, tree);
			schemas.add(ST.substring(0, ST.indexOf('.')));
			tables.add(ST.substring(ST.indexOf('.') + 1));
		}
		boolean oneSchema = true;
		boolean oneTable = true;
		String theSchema = null;
		String theTable = null;
		
		int i = 0;
		for (String schema : schemas)
		{
			if (theSchema == null)
			{
				theSchema = schema;
			}
			else
			{
				if (!schema.equals(theSchema))
				{
					oneSchema = false;
					break;
				}
			}
			
			String table = tables.get(i);
			if (theTable == null)
			{
				theTable = table;
			}
			else
			{
				if (!table.equals(theTable))
				{
					oneTable = false;
					break;
				}
			}
		}
		
		if (oneSchema && oneTable)
		{
			int tableID = PlanCacheManager.getTableID().setParms(theSchema, theTable).execute(tx);
			ArrayList<Object> rs = PlanCacheManager.getIndexIDsForTable().setParms(theSchema, theTable).execute(tx);
			for (Object r : rs)
			{
				ArrayList<Object> row = (ArrayList<Object>)r;
				int count = PlanCacheManager.getIndexColCount().setParms(tableID, (Integer)row.get(0)).execute(tx);
				if (count == schemas.size())
				{
					boolean hasAllCols = true;
					HashMap<String, Integer> cols2Pos = getCols2PosForTable(theSchema, theTable, tx);
					for (String col : cols)
					{
						int colID = cols2Pos.get(theTable + "." + col);
						Object o = PlanCacheManager.getCheckIndexForCol().setParms(tableID, (Integer)row.get(0), colID).execute(tx);
						if (o instanceof DataEndMarker)
						{
							hasAllCols = false;
							break;
						}
					}
					
					if (hasAllCols)
					{
						Object o = PlanCacheManager.getIndexCard().setParms(tableID, (Integer)row.get(0)).execute(tx);
						if (o instanceof DataEndMarker)
						{
							continue;
						}
						else
						{
							ArrayList<Object> cardRow = (ArrayList<Object>)o;
							return (Long)cardRow.get(0);
						}
					}
				}
			}
		}
		
		double card = 1;
		i = 0;
		for (final String col : cols)
		{
			card *= this.getCard(schemas.get(i), tables.get(i), col, generated, tx);
			i++;
		}

		return (long)card;
	}

	public long getColgroupCard(ArrayList<String> schemas, ArrayList<String> tables, ArrayList<String> cols, RootOperator op, Transaction tx) throws Exception
	{
		return getColgroupCard(schemas, tables, cols, op.getGenerated(), tx);
	}
	
	public long getColgroupCard(ArrayList<String> cols, RootOperator op, Transaction tx, Operator tree) throws Exception
	{
		return getColgroupCard(cols, op.getGenerated(), tx, tree);
	}

	public static HashMap<String, Integer> getCols2PosForTable(String schema, String name, Transaction tx) throws Exception
	{
		final HashMap<String, Integer> retval = new HashMap<String, Integer>();
		ArrayList<Object> rs = PlanCacheManager.getCols2PosForTable().setParms(schema, name).execute(tx);
		for (Object r : rs)
		{
			if (r instanceof DataEndMarker)
			{}
			else
			{
				ArrayList<Object> row = (ArrayList<Object>)r;
				retval.put(name + "." + (String)row.get(0), (Integer)row.get(1));
			}
		}

		return retval;
	}

	public static HashMap<String, String> getCols2TypesForTable(String schema, String name, Transaction tx) throws Exception
	{
		final HashMap<String, String> retval = new HashMap<String, String>();
		ArrayList<Object> rs = PlanCacheManager.getCols2Types().setParms(schema, name).execute(tx);
		for (Object r : rs)
		{
			ArrayList<Object> row = (ArrayList<Object>)r;
			String col = name + "." + (String)row.get(0);
			String type = (String)row.get(1);
			if (type.equals("BIGINT"))
			{
				type = "LONG";
			}
			else if (type.equals("DOUBLE"))
			{
				type = "FLOAT";
			}
			else if (type.equals("VARCHAR"))
			{
				type = "CHAR";
			}
			
			retval.put(col, type);
		}

		return retval;
	}

	public String getDevicePath(int num)
	{
		String deviceList = HRDBMSWorker.getHParms().getProperty("data_directories");
		FastStringTokenizer tokens = new FastStringTokenizer(deviceList, ",", false);
		tokens.setIndex(num);
		String path = tokens.nextToken();
		if (!path.endsWith("/"))
		{
			path += "/";
		}
		
		return path;
	}

	public String getHostNameForNode(int node, Transaction tx) throws Exception
	{
		return PlanCacheManager.getHostLookup().setParms(node).execute(tx);
	}
	
	public String getHostNameForNode(int node)
	{
		return nodeTable.get(node);
	}

	public ArrayList<Index> getIndexesForTable(String schema, String table, Transaction tx) throws Exception
	{
		final ArrayList<Index> retval = new ArrayList<Index>();
		ArrayList<Object> rs = PlanCacheManager.getIndexes().setParms(schema, table).execute(tx);
		for (Object o : rs)
		{
			ArrayList<Object> row = (ArrayList<Object>)o;
			ArrayList<Object> rs2 = PlanCacheManager.getKeys().setParms(schema, (String)row.get(1)).execute(tx);
			//tabname, colname
			ArrayList<String> keys = new ArrayList<String>();
			for (Object o2 : rs2)
			{
				ArrayList<Object> row2 = (ArrayList<Object>)o2;
				keys.add(row.get(0) + "." + row.get(1));
			}
			
			rs2 = PlanCacheManager.getTypes().setParms(schema, (String)row.get(1)).execute(tx);
			ArrayList<String> types = new ArrayList<String>();
			for (Object o2 : rs2)
			{
				ArrayList<Object> row2 = (ArrayList<Object>)o2;
				String type = (String)row.get(0);
				if (type.equals("BIGINT"))
				{
					type = "LONG";
				}
				else if (type.equals("VARCHAR"))
				{
					type = "CHAR";
				}
				else if (type.equals("DOUBLE"))
				{
					type = "FLOAT";
				}
				types.add(type);
			}
			
			rs2 = PlanCacheManager.getOrders().setParms(schema, (String)row.get(1)).execute(tx);
			ArrayList<Boolean> orders = new ArrayList<Boolean>();
			for (Object o2 : rs2)
			{
				ArrayList<Object> row2 = (ArrayList<Object>)o2;
				String ord = (String)row.get(0);
				boolean order = true;
				if (!ord.equals("A"))
				{
					order = false;
				}
				orders.add(order);
			}
			
			retval.add(new Index(schema + "." + row.get(0) + ".indx", keys, types, orders));
		}
		
		return retval;
	}

	public static int getNumDevices()
	{
		String dirList = HRDBMSWorker.getHParms().getProperty("data_directories");
		FastStringTokenizer tokens = new FastStringTokenizer(dirList, ",", false);
		return tokens.allTokens().length;
	}

	public int getNumNodes(Transaction tx) throws Exception
	{
		return PlanCacheManager.getCountWorkerNodes().setParms().execute(tx);
	}

	public PartitionMetaData getPartMeta(String schema, String table, Transaction tx) throws Exception
	{
		return new PartitionMetaData(schema, table, tx);
	}

	public static TreeMap<Integer, String> getPos2ColForTable(String schema, String name, Transaction tx) throws Exception
	{
		final TreeMap<Integer, String> retval = new TreeMap<Integer, String>();
		ArrayList<Object> rs = PlanCacheManager.getCols2PosForTable().setParms(schema, name).execute(tx);
		for (Object r : rs)
		{
			if (r instanceof DataEndMarker)
			{}
			else
			{
				ArrayList<Object> row = (ArrayList<Object>)r;
				retval.put((Integer)row.get(1), name + "." + (String)row.get(0));
			}
		}

		return retval;
	}

	public long getTableCard(String schema, String table, Transaction tx)
	{
		try
		{
			long card = PlanCacheManager.getTableCard().setParms(schema, table).execute(tx);
			return card;
		}
		catch(Exception e)
		{
			HRDBMSWorker.logger.warn("No table card found for table " + schema + "." + table);
			return 1000000;
		}
	}
	
	private boolean isQualified(String col)
	{
		if (col.contains(".") && !col.startsWith("."))
		{
			return true;
		}
		
		return false;
	}
	
	private  ArrayList<TableScanOperator> getTables(Operator op)
	{
		ArrayList<TableScanOperator> retval = new ArrayList<TableScanOperator>();
		if (op instanceof TableScanOperator)
		{
			retval.add((TableScanOperator)op);
			return retval;
		}
		else
		{
			for (Operator o : op.children())
			{
				retval.addAll(getTables(o));
			}
			
			return retval;
		}
	}
	
	private String getQual(String col)
	{
		return col.substring(0, col.indexOf('.'));
	}
	
	private String getCol(String col)
	{
		if (!col.contains("."))
		{
			return col;
		}
		else
		{
			return col.substring(col.indexOf('.') + 1);
		}
	}
	
	private boolean schemaIs(TableScanOperator t, String q)
	{
		return t.getSchema().equals(q);
	}
	
	private boolean aliasIs(TableScanOperator t, String q)
	{
		return t.getAlias().equals(q);
	}
	
	private boolean containsCol(Operator op, String c)
	{
		Set<String> set = op.getCols2Pos().keySet();
		for (String s : set)
		{
			if (s.contains("."))
			{
				s = s.substring(s.indexOf('.') + 1);
				if (s.equals(c))
				{
					return true;
				}
			}
		}
		
		return false;
	}

	public String getTableForCol(String col, Operator tree) throws Exception//must return schema.table - must accept qualified or unqualified col
	{
		Operator op = tree;
		ArrayList<TableScanOperator> tables = getTables(tree);
		if (isQualified(col))
		{
			String qual = getQual(col);
			String c = getCol(col);
			String retSchema = null;
			String retTable = null;
			for (TableScanOperator t : tables)
			{
				if (containsCol(t, c))
				{
					if (schemaIs(t, qual) || aliasIs(t, qual))
					{
						if (retSchema == null)
						{
							retSchema = t.getSchema();
							retTable = t.getTable();
						}
						else
						{
							if (!retSchema.equals(t.getSchema()) || !retTable.equals(t.getTable()))
							{
								throw new Exception("Ambiguous column " + col);
							}
						}
					}
				}
			}
			
			if (retSchema != null)
			{
				return retSchema + "." + retTable;
			}
			else
			{
				throw new Exception("Column not found when trying to figure out table " + col);
			}
		}
		else
		{
			String c = getCol(col);
			String retSchema = null;
			String retTable = null;
			for (TableScanOperator t : tables)
			{
				if (containsCol(t, c))
				{
					if (retSchema == null)
					{
						retSchema = t.getSchema();
						retTable = t.getTable();
					}
					else
					{
						if (!retSchema.equals(t.getSchema()) || !retTable.equals(t.getTable()))
						{
							throw new Exception("Ambiguous column " + col);
						}
					}
				}
			}
			
			if (retSchema != null)
			{
				return retSchema + "." + retTable;
			}
			else
			{
				return ".";
			}
		}
	}

	public double likelihood(ArrayList<Filter> filters, Transaction tx, Operator tree) throws Exception
	{
		double sum = 0;

		for (final Filter filter : filters)
		{
			sum += likelihood(filter, tx, tree);
		}

		if (sum > 1)
		{
			sum = 1;
		}

		return sum;
	}

	public double likelihood(ArrayList<Filter> filters, HashMap<String, Double> generated, Transaction tx, Operator tree) throws Exception
	{
		double sum = 0;

		for (final Filter filter : filters)
		{
			sum += likelihood(filter, generated, tx, tree);
		}

		if (sum > 1)
		{
			sum = 1;
		}

		return sum;
	}

	public double likelihood(ArrayList<Filter> filters, RootOperator op, Transaction tx, Operator tree) throws Exception
	{
		return likelihood(filters, op.getGenerated(), tx, tree);
	}

	public double likelihood(Filter filter, Transaction tx, Operator tree) throws Exception
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
			String left = filter.leftColumn();
			String leftST = getTableForCol(left, tree);
			String leftSchema = leftST.substring(0, leftST.indexOf('.'));
			String leftTable = leftST.substring(leftST.indexOf('.') + 1);
			String leftCol = left.substring(left.indexOf('.') + 1);
			leftCard = getCard(leftSchema, leftTable, leftCol, generated, tx);
		}

		if (filter.rightIsColumn())
		{
			// figure out number of possible values for right side
			String right = filter.rightColumn();
			String rightST = getTableForCol(right, tree);
			String rightSchema = rightST.substring(0, rightST.indexOf('.'));
			String rightTable = rightST.substring(rightST.indexOf('.') + 1);
			String rightCol = right.substring(right.indexOf('.') + 1);
			rightCard = getCard(rightSchema, rightTable, rightCol, generated, tx);
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
					final double retval = percentBelow(left, right, tx, tree);
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
					final double retval = percentBelow(left, right, tx, tree);
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
					final double retval = percentBelow(left, right, tx, tree);
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
					final double retval = percentAbove(right, left, tx, tree);
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
					final double retval = percentAbove(right, left, tx, tree);
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
					final double retval = percentAbove(right, left, tx, tree);
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
					final double retval = percentAbove(left, right, tx, tree);
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
					final double retval = percentAbove(left, right, tx, tree);
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
					final double retval = percentAbove(left, right, tx, tree);
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
					final double retval = percentBelow(right, left, tx, tree);
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
					final double retval = percentBelow(right, left, tx, tree);
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
					final double retval = percentBelow(right, left, tx, tree);
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
	public double likelihood(Filter filter, HashMap<String, Double> generated, Transaction tx, Operator tree) throws Exception
	{
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
			return retval;
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
			String left = filter.leftColumn();
			String leftST = getTableForCol(left, tree);
			String leftSchema = leftST.substring(0, leftST.indexOf('.'));
			String leftTable = leftST.substring(leftST.indexOf('.') + 1);
			String leftCol = left.substring(left.indexOf('.') + 1);
			leftCard = getCard(leftSchema, leftTable, leftCol, generated, tx);
		}

		if (filter.rightIsColumn())
		{
			// figure out number of possible values for right side
			String right = filter.rightColumn();
			String rightST = getTableForCol(right, tree);
			String rightSchema = rightST.substring(0, rightST.indexOf('.'));
			String rightTable = rightST.substring(rightST.indexOf('.') + 1);
			String rightCol = right.substring(right.indexOf('.') + 1);
			rightCard = getCard(rightSchema, rightTable, rightCol, generated, tx);
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
					final double retval = percentBelow(left, right, tx, tree);
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
					final double retval = percentBelow(left, right, tx, tree);
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
					final double retval = percentBelow(left, right, tx, tree);
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
					final double retval = percentAbove(right, left, tx, tree);
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
					final double retval = percentAbove(right, left, tx, tree);
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
					final double retval = percentAbove(right, left, tx, tree);
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
					final double retval = percentAbove(left, right, tx, tree);
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
					final double retval = percentAbove(left, right, tx, tree);
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
					final double retval = percentAbove(left, right, tx, tree);
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
					final double retval = percentBelow(right, left, tx, tree);
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
					final double retval = percentBelow(right, left, tx, tree);
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
					final double retval = percentBelow(right, left, tx, tree);
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

	public double likelihood(Filter filter, RootOperator op, Transaction tx, Operator tree) throws Exception
	{
		return likelihood(filter, op.getGenerated(), tx, tree);
	}

	public double likelihood(HashSet<HashMap<Filter, Filter>> hshm, HashMap<String, Double> generated, Transaction tx, Operator tree) throws Exception
	{
		final ArrayList<Double> ands = new ArrayList<Double>(hshm.size());
		for (final HashMap<Filter, Filter> ored : hshm)
		{
			double sum = 0;

			for (final Filter filter : ored.keySet())
			{
				sum += likelihood(filter, generated, tx, tree);
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

	public double likelihood(HashSet<HashMap<Filter, Filter>> hshm, RootOperator op, Transaction tx, Operator tree) throws Exception
	{
		return likelihood(hshm, op.getGenerated(), tx, tree);
	}

	private long bigger(long x, long y)
	{
		if (x >= y)
		{
			return x;
		}

		return y;
	}

	private ArrayList<Object> convertRangeStringToObject(String set, String schema, String table, String rangeCol, Transaction tx) throws Exception
	{
		String type = PlanCacheManager.getColType().setParms(schema, table, rangeCol).execute(tx);
		ArrayList<Object> retval = new ArrayList<Object>();
		StringTokenizer tokens = new StringTokenizer(set, "{}|");
		while (tokens.hasMoreTokens())
		{
			String token = tokens.nextToken();
			if (type.equals("INT"))
			{
				retval.add(Integer.parseInt(token));
			}
			else if (type.equals("BIGINT"))
			{
				retval.add(Long.parseLong(token));
			}
			else if (type.equals("DOUBLE"))
			{
				retval.add(Utils.parseDouble(token));
			}
			else if (type.equals("VARCHAR"))
			{
				retval.add(token);
			}
			else if (type.equals("DATE"))
			{
				int year = Integer.parseInt(token.substring(0, 4));
				int month = Integer.parseInt(token.substring(5, 7));
				int day = Integer.parseInt(token.substring(8, 10));
				retval.add(new MyDate(year, month, day));
			}
		}
		
		return retval;
	}

	private Operator doWork(Operator op, HashMap<Operator, ArrayList<String>> tables, HashMap<Operator, ArrayList<ArrayList<Filter>>> filters, HashMap<Operator, HashMap<String, Double>> retvals, Transaction tx, Operator tree) throws Exception
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
				al.add(new ConstantFilter(1 - this.likelihood(hshm, r, tx, tree)));
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
						card = this.getCard(keys.get(0), r, tx, tree);
					}
					else if (keys.size() == 0)
					{
						card = 1;
					}
					else
					{
						card = this.getColgroupCard(keys, r, tx, tree);
					}

					for (final ArrayList<Filter> filter : f)
					{
						if (references(filter, new ArrayList(keys)))
						{
							card *= this.likelihood(filter, r, tx, tree);
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
					card *= this.getTableCard(schema, table2, tx);
				}

				for (final ArrayList<Filter> filter : f)
				{
					card *= this.likelihood(filter, r, tx, tree);
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
					card *= this.getTableCard(schema, table2, tx);
				}

				for (final ArrayList<Filter> filter : f)
				{
					card *= this.likelihood(filter, r, tx, tree);
				}

				r.put(((SubstringOperator)op).getOutputCol(), card);
				// System.out.println("Operator is SubstringOperator");
				// System.out.println("Generated is " + r);
			}
			else if (op instanceof RenameOperator)
			{
				for (final Map.Entry entry : ((RenameOperator)op).getRenameMap().entrySet())
				{
					double card = this.getCard((String)entry.getKey(), r, tx, tree);
					for (final ArrayList<Filter> filter : f)
					{
						final ArrayList<String> keys = new ArrayList<String>(1);
						keys.add((String)entry.getKey());
						if (references(filter, keys))
						{
							card *= this.likelihood(filter, r, tx, tree);
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
					card *= this.getTableCard(schema, table2, tx);
				}

				for (final ArrayList<Filter> filter : f)
				{
					card *= this.likelihood(filter, r, tx, tree);
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
					card *= this.getTableCard(schema, table2, tx);
				}

				for (final ArrayList<Filter> filter : f)
				{
					card *= this.likelihood(filter, r, tx, tree);
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

	private ArrayList<MyDate> getDateQuartiles(String col, Transaction tx, Operator tree) throws Exception
	{
		// SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		String c = col.substring(col.indexOf('.') + 1);
		String ST = getTableForCol(col, tree);
		String schema = ST.substring(0, ST.indexOf('.'));
		String table = ST.substring(ST.indexOf('.') + 1);
		ArrayList<MyDate> retval = new ArrayList<MyDate>(5);
		Object o = PlanCacheManager.getDist().setParms(schema, table, c).execute(tx);
		if (o instanceof DataEndMarker)
		{

			retval.add(new MyDate(1960, 1, 1));
			retval.add(new MyDate(2000, 1, 1));
			retval.add(new MyDate(2020, 1, 1));
			retval.add(new MyDate(2040, 1, 1));
			retval.add(new MyDate(2060, 1, 1));
		}
		else
		{
			for (Object obj : (ArrayList<Object>)o)
			{
				String val = (String)obj;
				int year = Integer.parseInt(val.substring(0, 4));
				int month = Integer.parseInt(val.substring(5, 7));
				int day = Integer.parseInt(val.substring(8, 10));
				retval.add(new MyDate(year, month, day));
			}
		}
		
		return retval;
	}

	private String getDeviceExpression(String schema, String table, Transaction tx) throws Exception
	{
		ArrayList<Object> row = PlanCacheManager.getPartitioning().setParms(schema, table).execute(tx);
		return (String)row.get(2);
	}

	private ArrayList<Double> getDoubleQuartiles(String col, Transaction tx, Operator tree) throws Exception
	{
		String c = col.substring(col.indexOf('.') + 1);
		String ST = getTableForCol(col, tree);
		String schema = ST.substring(0, ST.indexOf('.'));
		String table = ST.substring(ST.indexOf('.') + 1);
		ArrayList<Double> retval = new ArrayList<Double>(5);
		Object o = PlanCacheManager.getDist().setParms(schema, table, c).execute(tx);
		if (o instanceof DataEndMarker)
		{

			retval.add(Double.MIN_VALUE);
			retval.add(Double.MIN_VALUE / 2);
			retval.add(0D);
			retval.add(Double.MAX_VALUE / 2);
			retval.add(Double.MAX_VALUE);
		}
		else
		{
			for (Object obj : (ArrayList<Object>)o)
			{
				String val = (String)obj;
				retval.add(Utils.parseDouble(val));
			}
		}
		
		return retval;
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

	private String getNodeExpression(String schema, String table, Transaction tx) throws Exception
	{
		ArrayList<Object> row = PlanCacheManager.getPartitioning().setParms(schema, table).execute(tx);
		return (String)row.get(1);
	}

	private String getNodeGroupExpression(String schema, String table, Transaction tx) throws Exception
	{
		ArrayList<Object> row = PlanCacheManager.getPartitioning().setParms(schema, table).execute(tx);
		return (String)row.get(0);
	}

	private ArrayList<String> getStringQuartiles(String col, Transaction tx, Operator tree) throws Exception
	{
		String c = col.substring(col.indexOf('.') + 1);
		String ST = getTableForCol(col, tree);
		String schema = ST.substring(0, ST.indexOf('.'));
		String table = ST.substring(ST.indexOf('.') + 1);
		ArrayList<String> retval = new ArrayList<String>(5);
		Object o = PlanCacheManager.getDist().setParms(schema, table, c).execute(tx);
		if (o instanceof DataEndMarker)
		{

			retval.add("A");
			retval.add("N");
			retval.add("Z");
			retval.add("n");
			retval.add("z");
		}
		else
		{
			for (Object obj : (ArrayList<Object>)o)
			{
				String val = (String)obj;
				retval.add(val);
			}
		}
		
		return retval;
	}

	private double percentAbove(String col, double val, Transaction tx, Operator tree) throws Exception
	{
		final ArrayList<Double> quartiles = getDoubleQuartiles(col, tx, tree);
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

	private double percentAbove(String col, MyDate val, Transaction tx, Operator tree) throws Exception
	{
		final ArrayList<MyDate> quartiles = getDateQuartiles(col, tx, tree);
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

	private double percentAbove(String col, String val, Transaction tx, Operator tree) throws Exception
	{
		final ArrayList<String> quartiles = getStringQuartiles(col, tx, tree);
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

	private double percentBelow(String col, double val, Transaction tx, Operator tree) throws Exception
	{
		final ArrayList<Double> quartiles = getDoubleQuartiles(col, tx, tree);
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

	private double percentBelow(String col, MyDate val, Transaction tx, Operator tree) throws Exception
	{
		final ArrayList<MyDate> quartiles = getDateQuartiles(col, tx, tree);
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

	private double percentBelow(String col, String val, Transaction tx, Operator tree) throws Exception
	{
		final ArrayList<String> quartiles = getStringQuartiles(col, tx, tree);
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
	
	private static class TableStatsThread extends HRDBMSThread
	{
		private String schema;
		private String table;
		private int tableID;
		private Transaction tx;
		private boolean ok = true;
		
		public TableStatsThread(String schema, String table, int tableID, Transaction tx)
		{
			this.schema = schema;
			this.table = table;
			this.tableID = tableID;
			this.tx = tx;
		}
		
		public void run()
		{
			try
			{
				String sql = "INSERT INTO SYS.TABLESTATS SELECT " + tableID + ", COUNT(*) FROM " + schema + "." + table;
				XAWorker worker = XAManager.executeAuthorizedUpdate(sql, tx);
				worker.start();
				worker.join();
			}
			catch(Exception e)
			{
				ok = false;
			}
		}
		
		public boolean getOK()
		{
			return ok;
		}
	}
	
	private static class ColStatsThread extends HRDBMSThread
	{
		private String schema;
		private String table;
		private String col;
		private int tableID;
		private int colID;
		private Transaction tx;
		private boolean ok = true;
		
		public ColStatsThread(String schema, String table, String col, int tableID, int colID, Transaction tx)
		{
			this.schema = schema;
			this.table = table;
			this.col = col;
			this.tableID = tableID;
			this.colID = colID;
			this.tx = tx;
		}
		
		public void run()
		{
			try
			{
				String sql = "INSERT INTO SYS.COLSTATS SELECT " + tableID + "," + colID + ", COUNT(DISTINCT " + col + ") FROM " + schema + "." + table;
				XAWorker worker = XAManager.executeAuthorizedUpdate(sql, tx);
				worker.start();
				worker.join();
			}
			catch(Exception e)
			{
				ok = false;
			}
		}
		
		public boolean getOK()
		{
			return ok;
		}
	}
	
	private static class IndexStatsThread extends HRDBMSThread
	{
		private String schema;
		private String table;
		private ArrayList<String> keys;
		private int tableID;
		private int indexID;
		private Transaction tx;
		private boolean ok = true;
		
		public IndexStatsThread(String schema, String table, ArrayList<String> keys, int tableID, int indexID, Transaction tx)
		{
			this.schema = schema;
			this.table = table;
			this.keys = keys;
			this.tableID = tableID;
			this.indexID = indexID;
			this.tx = tx;
		}
		
		public void run()
		{
			try
			{
				String sql = "INSERT INTO SYS.INDEXSTATS SELECT " + tableID + "," + indexID + ", SELECT COUNT(*) FROM (SELECT DISTINCT " + keys.get(0);
				int i = 1;
				while (i < keys.size())
				{
					sql += (", " + keys.get(i));
					i++;
				}
				sql += (" FROM " + schema + "." + table + ")");
				XAWorker worker = XAManager.executeAuthorizedUpdate(sql, tx);
				worker.start();
				worker.join();
			}
			catch(Exception e)
			{
				ok = false;
			}
		}
		
		public boolean getOK()
		{
			return ok;
		}
	}
	
	private static class ColDistThread extends HRDBMSThread
	{
		private String schema;
		private String table;
		private String col;
		private int tableID;
		private int colID;
		private long card;
		private Transaction tx;
		private boolean ok = true;
		
		public ColDistThread(String schema, String table, String col, int tableID, int colID, long card, Transaction tx)
		{
			this.schema = schema;
			this.table = table;
			this.col = col;
			this.tableID = tableID;
			this.colID = colID;
			this.card = card;
			this.tx = tx;
		}
		
		public void run()
		{
			try
			{
				String type = PlanCacheManager.getColType().setParms(schema, table, col).execute(tx);
				String sql = "SELECT " + col + " FROM " + schema + "." + table + " ORDER BY " + col + " ASC";
				XAWorker worker = XAManager.executeQuery(sql, tx, null);
				worker.start();
				ArrayList<Object> cmd = new ArrayList<Object>();
				cmd.add("NEXT");
				cmd.add(1000000);
				worker.in.put(cmd);
				Object o = worker.out.take();
				if (o instanceof DataEndMarker)
				{
					throw new Exception("No data in table");
				}
				ArrayList<Object> row = (ArrayList<Object>)o;
				String low = null;
				String q1 = null;
				String q2 = null;
				String q3 = null;
				String high = null;
				if (type.equals("INT"))
				{
					low = Integer.toString((Integer)row.get(0));
				}
				else if (type.equals("BIGINT"))
				{
					low = Long.toString((Long)row.get(0));
				}
				else if (type.equals("VARCHAR"))
				{
					low = (String)row.get(0);
				}
				else if (type.equals("DOUBLE"))
				{
					low = Double.toString((Double)row.get(0));
				}
				else if (type.equals("DATE"))
				{
					low = ((MyDate)row.get(0)).format();
				}
				
				int i = 1;
				Object o2 = o;
				while (i < card / 4 && !(o2 instanceof DataEndMarker))
				{
					o = o2;
					o2 = worker.out.take();
					i++;
				}
				
				if (o2 instanceof DataEndMarker)
				{
					row = (ArrayList<Object>)o;
					if (type.equals("INT"))
					{
						high = q3 = q2 = q1 = Integer.toString((Integer)row.get(0));
					}
					else if (type.equals("BIGINT"))
					{
						high = q3 = q2 = q1 = Long.toString((Long)row.get(0));
					}
					else if (type.equals("VARCHAR"))
					{
						high = q3 = q2 = q1 = (String)row.get(0);
					}
					else if (type.equals("DOUBLE"))
					{
						high = q3 = q2 = q1 = Double.toString((Double)row.get(0));
					}
					else if (type.equals("DATE"))
					{
						high = q3 = q2 = q1 = ((MyDate)row.get(0)).format();
					}
					
					sql = "INSERT INTO SYS.COLDIST VALUES(" + tableID + ", " + colID + ", '" + low + "', '" + q1 + "', '" + q2 + "', '" + q3 + "', '" + high + "')";
					worker = XAManager.executeAuthorizedUpdate(sql, tx);
					worker.start();
					worker.join();
					return;
				}
				else
				{
					row = (ArrayList<Object>)o2;
					if (type.equals("INT"))
					{
						q1 = Integer.toString((Integer)row.get(0));
					}
					else if (type.equals("BIGINT"))
					{
						q1 = Long.toString((Long)row.get(0));
					}
					else if (type.equals("VARCHAR"))
					{
						q1 = (String)row.get(0);
					}
					else if (type.equals("DOUBLE"))
					{
						q1 = Double.toString((Double)row.get(0));
					}
					else if (type.equals("DATE"))
					{
						q1 = ((MyDate)row.get(0)).format();
					}
				}
				
				while (i < card / 2 && !(o2 instanceof DataEndMarker))
				{
					o = o2;
					o2 = worker.out.take();
					i++;
				}
				
				if (o2 instanceof DataEndMarker)
				{
					row = (ArrayList<Object>)o;
					if (type.equals("INT"))
					{
						high = q3 = q2 = Integer.toString((Integer)row.get(0));
					}
					else if (type.equals("BIGINT"))
					{
						high = q3 = q2 = Long.toString((Long)row.get(0));
					}
					else if (type.equals("VARCHAR"))
					{
						high = q3 = q2 = (String)row.get(0);
					}
					else if (type.equals("DOUBLE"))
					{
						high = q3 = q2 = Double.toString((Double)row.get(0));
					}
					else if (type.equals("DATE"))
					{
						high = q3 = q2 = ((MyDate)row.get(0)).format();
					}
					
					sql = "INSERT INTO SYS.COLDIST VALUES(" + tableID + ", " + colID + ", '" + low + "', '" + q1 + "', '" + q2 + "', '" + q3 + "', '" + high + "')";
					worker = XAManager.executeAuthorizedUpdate(sql, tx);
					worker.start();
					worker.join();
					return;
				}
				else
				{
					row = (ArrayList<Object>)o2;
					if (type.equals("INT"))
					{
						q2 = Integer.toString((Integer)row.get(0));
					}
					else if (type.equals("BIGINT"))
					{
						q2 = Long.toString((Long)row.get(0));
					}
					else if (type.equals("VARCHAR"))
					{
						q2 = (String)row.get(0);
					}
					else if (type.equals("DOUBLE"))
					{
						q2 = Double.toString((Double)row.get(0));
					}
					else if (type.equals("DATE"))
					{
						q2 = ((MyDate)row.get(0)).format();
					}
				}
				
				while (i < card * 3 / 4 && !(o2 instanceof DataEndMarker))
				{
					o = o2;
					o2 = worker.out.take();
					i++;
				}
				
				if (o2 instanceof DataEndMarker)
				{
					row = (ArrayList<Object>)o;
					if (type.equals("INT"))
					{
						high = q3 = Integer.toString((Integer)row.get(0));
					}
					else if (type.equals("BIGINT"))
					{
						high = q3 = Long.toString((Long)row.get(0));
					}
					else if (type.equals("VARCHAR"))
					{
						high = q3 = (String)row.get(0);
					}
					else if (type.equals("DOUBLE"))
					{
						high = q3 = Double.toString((Double)row.get(0));
					}
					else if (type.equals("DATE"))
					{
						high = q3 = ((MyDate)row.get(0)).format();
					}
					
					sql = "INSERT INTO SYS.COLDIST VALUES(" + tableID + ", " + colID + ", '" + low + "', '" + q1 + "', '" + q2 + "', '" + q3 + "', '" + high + "')";
					worker = XAManager.executeAuthorizedUpdate(sql, tx);
					worker.start();
					worker.join();
					return;
				}
				else
				{
					row = (ArrayList<Object>)o2;
					if (type.equals("INT"))
					{
						q3 = Integer.toString((Integer)row.get(0));
					}
					else if (type.equals("BIGINT"))
					{
						q3 = Long.toString((Long)row.get(0));
					}
					else if (type.equals("VARCHAR"))
					{
						q3 = (String)row.get(0);
					}
					else if (type.equals("DOUBLE"))
					{
						q3 = Double.toString((Double)row.get(0));
					}
					else if (type.equals("DATE"))
					{
						q3 = ((MyDate)row.get(0)).format();
					}
				}
				
				while (!(o2 instanceof DataEndMarker))
				{
					o = o2;
					o2 = worker.out.take();
					i++;
				}
				
				row = (ArrayList<Object>)o;
				if (type.equals("INT"))
				{
					high = Integer.toString((Integer)row.get(0));
				}
				else if (type.equals("BIGINT"))
				{
					high = Long.toString((Long)row.get(0));
				}
				else if (type.equals("VARCHAR"))
				{
					high = (String)row.get(0);
				}
				else if (type.equals("DOUBLE"))
				{
					high = Double.toString((Double)row.get(0));
				}
				else if (type.equals("DATE"))
				{
					high = ((MyDate)row.get(0)).format();
				}
					
				sql = "INSERT INTO SYS.COLDIST VALUES(" + tableID + ", " + colID + ", '" + low + "', '" + q1 + "', '" + q2 + "', '" + q3 + "', '" + high + "')";
				worker = XAManager.executeAuthorizedUpdate(sql, tx);
				worker.start();
				worker.join();
			}
			catch(Exception e)
			{
				ok = false;
			}
		}
		
		public boolean getOK()
		{
			return ok;
		}
	}
	
	public static void runstats(String schema, String table, Transaction tx) throws Exception
	{
		Transaction tTx = new Transaction(Transaction.ISOLATION_UR);
		int tableID = PlanCacheManager.getTableID().setParms(schema, table).execute(tx);
		TableStatsThread tThread = new TableStatsThread(schema, table, tableID, tTx);
		tThread.start();
		TreeMap<Integer, String> pos2Col = getPos2ColForTable(schema, table, tx);
		ArrayList<ColStatsThread> cThreads = new ArrayList<ColStatsThread>();
		ArrayList<Transaction> cTxs = new ArrayList<Transaction>();
		int i = 0;
		while (i < pos2Col.size())
		{
			Transaction ctx = new Transaction(Transaction.ISOLATION_UR);
			cTxs.add(ctx);
			cThreads.add(new ColStatsThread(schema, table, pos2Col.get(i), tableID, i, ctx));
			i++;
		}
		
		for (ColStatsThread thread : cThreads)
		{
			thread.start();
		}
		
		//indexes
		ArrayList<IndexStatsThread> iThreads = new ArrayList<IndexStatsThread>();
		ArrayList<Transaction> iTxs = new ArrayList<Transaction>();
		ArrayList<Object> rs = PlanCacheManager.getIndexIDsForTable().setParms(schema, table).execute(tx);
		for (Object o : rs)
		{
			if (o instanceof DataEndMarker)
			{
				continue;
			}
			
			ArrayList<Object> row = (ArrayList<Object>)o;
			int indexID = (Integer)row.get(0);
			ArrayList<Object> rs2 = PlanCacheManager.getKeysByID().setParms(tableID, indexID).execute(tx);
			ArrayList<String> keys = new ArrayList<String>();
			for (Object o2 : rs2)
			{
				if (o2 instanceof DataEndMarker)
				{
					continue;
				}
				
				keys.add((String)((ArrayList<Object>)o2).get(0));
			}
			
			Transaction itx = new Transaction(Transaction.ISOLATION_UR);
			iThreads.add(new IndexStatsThread(schema, table, keys, tableID, indexID, itx));
			iTxs.add(itx);
		}
		
		for (IndexStatsThread thread : iThreads)
		{
			thread.start();
		}
		
		tThread.join();
		boolean allOK = true;
		if (!tThread.getOK())
		{
			allOK = false;
		}
		
		ArrayList<Transaction> cdTxs = new ArrayList<Transaction>();
		if (allOK)
		{
			ArrayList<ColDistThread> cdThreads = new ArrayList<ColDistThread>();
			i = 0;
			long card = PlanCacheManager.getTableCard().setParms(schema, table).execute(tx);
			while (i < pos2Col.size())
			{
				Transaction ctx = new Transaction(Transaction.ISOLATION_UR);
				cdTxs.add(ctx);
				cdThreads.add(new ColDistThread(schema, table, pos2Col.get(i), tableID, i, card, ctx));
				i++;
			}
			
			for (ColDistThread thread : cdThreads)
			{
				thread.start();
			}
			
			for (ColDistThread thread : cdThreads)
			{
				thread.join();
				if (!thread.getOK())
				{
					allOK = false;
				}
			}
		}
		
		for (ColStatsThread thread : cThreads)
		{
			thread.join();
			if (!thread.getOK())
			{
				allOK = false;
			}
		}
		
		for (IndexStatsThread thread : iThreads)
		{
			thread.join();
			if (!thread.getOK())
			{
				allOK = false;
			}
		} 
		

			
		if (allOK)
		{
			tTx.commit();
			for (Transaction ctx : cTxs)
			{
				ctx.commit();
			}
			for (Transaction itx : iTxs)
			{
				itx.commit();
			}
			for (Transaction cdtx : cdTxs)
			{
				cdtx.commit();
			}
			
			return;
		}
		else
		{
			tTx.rollback();
			for (Transaction ctx : cTxs)
			{
				ctx.rollback();
			}
			for (Transaction itx : iTxs)
			{
				itx.rollback();
			}
			for (Transaction cdtx : cdTxs)
			{
				cdtx.rollback();
			}
			
			throw new Exception("An error occurred during runstats processing");
		}
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
		private String schema;
		private String table;
		private Transaction tx;

		public PartitionMetaData(String schema, String table, Transaction tx) throws Exception
		{
			this.schema = schema;
			this.table = table;
			this.tx = tx;
			ArrayList<Object> row = PlanCacheManager.getPartitioning().setParms(schema,  table).execute(tx);
			final String ngExp = (String)row.get(0);
			final String nExp = (String)row.get(1);
			final String dExp = (String)row.get(2);
			setNGData(ngExp);
			setNData(nExp);
			setDData(dExp);
		}
		
		public PartitionMetaData(String schema, String table, String ngExp, String nExp, String dExp, Transaction tx) throws Exception
		{
			this.schema = schema;
			this.table = table;
			this.tx = tx;
			setNGData(ngExp);
			setNData(nExp);
			setDData(dExp);
		}
		
		public String getSchema()
		{
			return schema;
		}
		
		public String getTable()
		{
			return table;
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

		private void setDData(String exp) throws Exception
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
				deviceRange = convertRangeStringToObject(set, schema, table, deviceRangeCol, tx);
			}
		}

		private void setNData(String exp) throws Exception
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
				numNodes = MetaData.this.getNumNodes(tx);
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
				nodeRange = convertRangeStringToObject(set, schema, table, nodeRangeCol, tx);
			}
		}

		private void setNGData(String exp) throws Exception
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
			StringTokenizer tokens2 = new StringTokenizer(set, "{}", false);
			nodeGroupSet = new ArrayList<Integer>();
			numNodeGroups = 0;
			int setNum = 0;
			nodeGroupHashMap = new HashMap<Integer, ArrayList<Integer>>();
			while (tokens2.hasMoreTokens())
			{
				String nodesInGroup = tokens2.nextToken();
				nodeGroupSet.add(setNum);
				numNodeGroups++;
				
				ArrayList<Integer> nodeListForGroup = new ArrayList<Integer>();
				FastStringTokenizer tokens3 = new FastStringTokenizer(nodesInGroup, "|", false);
				while (tokens3.hasMoreTokens())
				{
					String token = tokens3.nextToken();
					nodeListForGroup.add(Integer.parseInt(token));
				}
				nodeGroupHashMap.put(setNum, nodeListForGroup);
				setNum++;
			}

			if (numNodeGroups == 1)
			{
				return;
			}

			final String type = tokens.nextToken();
			if (type.equals("HASH"))
			{
				set = tokens.nextToken().substring(1);
				set = set.substring(0, set.length() - 1);
				tokens2 = new StringTokenizer(set, "|", false);
				nodeGroupHash = new ArrayList<String>();
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
				nodeGroupRange = convertRangeStringToObject(set, schema, table, nodeGroupRangeCol, tx);
			}
		}
	}
}
