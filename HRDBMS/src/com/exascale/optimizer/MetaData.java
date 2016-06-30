package com.exascale.optimizer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.Socket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeMap;
import com.exascale.exceptions.ParseException;
import com.exascale.filesystem.Block;
import com.exascale.managers.BufferManager;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.managers.LockManager;
import com.exascale.managers.PlanCacheManager;
import com.exascale.managers.XAManager;
import com.exascale.misc.DataEndMarker;
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
	public static int numWorkerNodes = 0;
	private static ArrayList<Integer> coords = new ArrayList<Integer>();
	private static ArrayList<Integer> workers = new ArrayList<Integer>();
	private static HashMap<String, ArrayList<Object>> getPartitioningCache = new HashMap<String, ArrayList<Object>>();
	private static HashMap<String, ArrayList<Object>> getIndexesCache = new HashMap<String, ArrayList<Object>>();
	private static HashMap<String, ArrayList<Object>> getUniqueIndexesCache = new HashMap<String, ArrayList<Object>>();
	private static HashMap<String, ArrayList<Object>> getKeysCache = new HashMap<String, ArrayList<Object>>();
	private static HashMap<String, ArrayList<Object>> getTypesCache = new HashMap<String, ArrayList<Object>>();
	private static HashMap<String, ArrayList<Object>> getOrdersCache = new HashMap<String, ArrayList<Object>>();
	private static HashMap<String, ArrayList<Object>> getIndexColsForTableCache = new HashMap<String, ArrayList<Object>>();
	private static HashMap<String, Integer> getLengthCache = new HashMap<String, Integer>();
	private static HashMap<String, Integer> getTableIDCache = new HashMap<String, Integer>();
	private static HashMap<String, Long> getColCardCache = new HashMap<String, Long>();
	private static HashMap<String, ArrayList<Object>> getIndexIDsForTableCache = new HashMap<String, ArrayList<Object>>();
	private static HashMap<String, ArrayList<Object>> getCols2PosForTableCache = new HashMap<String, ArrayList<Object>>();
	private static HashMap<String, ArrayList<Object>> getCols2TypesCache = new HashMap<String, ArrayList<Object>>();
	private static HashMap<String, ArrayList<Object>> getUniqueCache = new HashMap<String, ArrayList<Object>>();
	private static HashMap<String, Long> getTableCardCache = new HashMap<String, Long>();
	private static HashMap<String, String> getColTypeCache = new HashMap<String, String>();
	private static HashMap<String, Object> getDistCache = new HashMap<String, Object>();
	private static int numDevices;

	static
	{
		try
		{
			String dirList = HRDBMSWorker.getHParms().getProperty("data_directories");
			FastStringTokenizer tokens2 = new FastStringTokenizer(dirList, ",", false);
			numDevices = tokens2.allTokens().length;
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
						numWorkerNodes++;
						workers.add(myNode);
					}
					else
					{
						myNode = coordID;
						nodeTable.put(myNode, host);
						coordID--;
						coords.add(myNode);
					}
				}
				else
				{
					if (type.equals("W"))
					{
						nodeTable.put(workerID, host);
						workerID++;
						numWorkerNodes++;
						workers.add(workerID - 1);
					}
					else
					{
						nodeTable.put(coordID, host);
						coordID--;
						coords.add(coordID + 1);
					}
				}

				line = nodes.readLine();
			}

			nodes.close();
		}
		catch (Exception e)
		{
			// HRDBMSWorker.logger.fatal("Error during static metadata
			// initialization",
			// e);
			// System.exit(1);
		}
	}

	private transient ConnectionWorker connection = null;
	private final HashMap<ColAndTree, String> gtfcCache = new HashMap<ColAndTree, String>();

	public MetaData()
	{

	}

	public MetaData(ConnectionWorker connection)
	{
		this.connection = connection;
	}

	public MetaData(String x) throws Exception
	{
		try
		{
			if (nodeTable.size() == 0)
			{
				synchronized (nodeTable)
				{
					if (nodeTable.size() == 0)
					{
						final BufferedReader nodes = new BufferedReader(new FileReader(new File(x + "nodes.cfg")));
						String line = nodes.readLine();
						int workerID = 0;
						while (line != null)
						{
							final StringTokenizer tokens = new StringTokenizer(line, ",", false);
							final String host = tokens.nextToken().trim();
							String type = tokens.nextToken().trim().toUpperCase();
							if (isMyIP(host))
							{
								myNode = workerID;
								nodeTable.put(myNode, host);
								workerID++;
							}
							else
							{
								if (type.equals("W"))
								{
									nodeTable.put(workerID, host);
									workerID++;
								}
							}

							line = nodes.readLine();
						}

						nodes.close();
					}
				}
			}
		}
		catch (Exception e)
		{
			throw e;
		}
	}

	public static TreeMap<Integer, String> cols2PosFlip(HashMap<String, Integer> cols2Pos)
	{
		final TreeMap<Integer, String> retval = new TreeMap<Integer, String>();

		for (Map.Entry entry : cols2Pos.entrySet())
		{
			retval.put((Integer)entry.getValue(), (String)entry.getKey());
		}

		return retval;
	}

	public static void createView(String schema, String table, String text, Transaction tx) throws Exception
	{
		int id = PlanCacheManager.getNextViewID().setParms().execute(tx);
		PlanCacheManager.getInsertView().setParms(id, schema, table, text).execute(tx);
	}

	public static int determineDevice(ArrayList<Object> row, PartitionMetaData pmeta, HashMap<String, Integer> cols2Pos) throws Exception
	{
		pmeta.getTable();
		String table = pmeta.getTable();
		if (pmeta.isSingleDeviceSet())
		{
			return pmeta.getSingleDevice();
		}
		else if (pmeta.deviceIsHash())
		{
			ArrayList<String> devHash = pmeta.getDeviceHash();
			ArrayList<Object> partial = new ArrayList<Object>();
			final int z = devHash.size();
			int i = 0;
			while (i < z)
			{
				String col = devHash.get(i++);
				try
				{
					partial.add(row.get(cols2Pos.get(table + "." + col)));
				}
				catch (Exception e)
				{
					HRDBMSWorker.logger.debug("Looking for " + table + "." + col);
					HRDBMSWorker.logger.debug("Cols2Pos is " + cols2Pos);
					HRDBMSWorker.logger.debug("Row is " + row);
				}
			}

			long hash = 0x7FFFFFFFFFFFFFFFL & hash(partial);
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
			final int size = ranges.size();
			while (i < size)
			{
				if (((Comparable)obj).compareTo(ranges.get(i)) <= 0)
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

	public static ArrayList<Integer> determineNode(String schema, String table, ArrayList<Object> row, Transaction tx, PartitionMetaData pmeta, HashMap<String, Integer> cols2Pos, int numNodes) throws Exception
	{
		if (pmeta.noNodeGroupSet())
		{
			if (pmeta.anyNode())
			{
				ArrayList<Integer> retval = getWorkerNodes();
				return retval;
			}
			else if (pmeta.isSingleNodeSet())
			{
				if (pmeta.getSingleNode() == -1)
				{
					ArrayList<Integer> retval = getCoordNodes();
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
				final int z = nodeHash.size();
				int i = 0;
				while (i < z)
				{
					String col = nodeHash.get(i++);
					partial.add(row.get(cols2Pos.get(table + "." + col)));
				}

				long hash = 0x7FFFFFFFFFFFFFFFL & hash(partial);
				if (pmeta.allNodes())
				{
					ArrayList<Integer> retval = new ArrayList<Integer>();
					retval.add((int)(hash % numNodes)); // LOOKOUT if we ever do
					// dynamic # nodes
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
				// range
				String col = table + "." + pmeta.getNodeRangeCol();
				Object obj = row.get(cols2Pos.get(col));
				ArrayList<Object> ranges = pmeta.getNodeRanges();
				int i = 0;
				final int size = ranges.size();
				while (i < size)
				{
					if (((Comparable)obj).compareTo(ranges.get(i)) <= 0)
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
			if (pmeta.isSingleNodeGroupSet())
			{
				ngSet = pmeta.getNodeGroupHashMap().get(pmeta.getSingleNodeGroup());
			}
			else if (pmeta.nodeGroupIsHash())
			{
				ArrayList<String> nodeGroupHash = pmeta.getNodeGroupHash();
				ArrayList<Object> partial = new ArrayList<Object>();
				final int z = nodeGroupHash.size();
				int i = 0;
				while (i < z)
				{
					String col = nodeGroupHash.get(i++);
					partial.add(row.get(cols2Pos.get(table + "." + col)));
				}

				long hash = 0x7FFFFFFFFFFFFFFFL & hash(partial);
				ngSet = pmeta.getNodeGroupHashMap().get((int)(hash % pmeta.getNodeGroupHashMap().size()));
			}
			else
			{
				String col = table + "." + pmeta.getNodeGroupRangeCol();
				Object obj = row.get(cols2Pos.get(col));
				ArrayList<Object> ranges = pmeta.getNodeGroupRanges();
				int i = 0;
				final int size = ranges.size();
				while (i < size)
				{
					if (((Comparable)obj).compareTo(ranges.get(i)) <= 0)
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
			else if (pmeta.isSingleNodeSet())
			{
				ArrayList<Integer> retval = new ArrayList<Integer>();
				retval.add(ngSet.get(pmeta.getSingleNode()));
				return retval;
			}
			else if (pmeta.nodeIsHash())
			{
				ArrayList<String> nodeHash = pmeta.getNodeHash();
				ArrayList<Object> partial = new ArrayList<Object>();
				final int z = nodeHash.size();
				int i = 0;
				while (i < z)
				{
					String col = nodeHash.get(i++);
					partial.add(row.get(cols2Pos.get(table + "." + col)));
				}

				long hash = 0x7FFFFFFFFFFFFFFFL & hash(partial);
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
				// range
				String col = table + "." + pmeta.getNodeRangeCol();
				Object obj = row.get(cols2Pos.get(col));
				ArrayList<Object> ranges = pmeta.getNodeRanges();
				int i = 0;
				final int size = ranges.size();
				while (i < size)
				{
					if (((Comparable)obj).compareTo(ranges.get(i)) <= 0)
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

	public static ArrayList<Integer> determineNodeNoLookups(String schema, String table, ArrayList<Object> row, PartitionMetaData pmeta, HashMap<String, Integer> cols2Pos, int numNodes, ArrayList<Integer> workerNodes, ArrayList<Integer> coordNodes) throws Exception
	{
		if (pmeta.noNodeGroupSet())
		{
			if (pmeta.anyNode())
			{
				return workerNodes;
			}
			else if (pmeta.isSingleNodeSet())
			{
				if (pmeta.getSingleNode() == -1)
				{
					return coordNodes;
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

				long hash = 0x7FFFFFFFFFFFFFFFL & hash(partial);
				if (pmeta.allNodes())
				{
					ArrayList<Integer> retval = new ArrayList<Integer>();
					retval.add((int)(hash % numNodes)); // LOOKOUT if we ever do
					// dynamic # nodes
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
				// range
				String col = table + "." + pmeta.getNodeRangeCol();
				Object obj = row.get(cols2Pos.get(col));
				ArrayList<Object> ranges = pmeta.getNodeRanges();
				int i = 0;
				final int size = ranges.size();
				while (i < size)
				{
					if (((Comparable)obj).compareTo(ranges.get(i)) <= 0)
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
			if (pmeta.isSingleNodeGroupSet())
			{
				ngSet = pmeta.getNodeGroupHashMap().get(pmeta.getSingleNodeGroup());
			}
			else if (pmeta.nodeGroupIsHash())
			{
				ArrayList<String> nodeGroupHash = pmeta.getNodeGroupHash();
				ArrayList<Object> partial = new ArrayList<Object>();
				for (String col : nodeGroupHash)
				{
					partial.add(row.get(cols2Pos.get(table + "." + col)));
				}

				long hash = 0x7FFFFFFFFFFFFFFFL & hash(partial);
				ngSet = pmeta.getNodeGroupHashMap().get((int)(hash % pmeta.getNodeGroupHashMap().size()));
			}
			else
			{
				String col = table + "." + pmeta.getNodeGroupRangeCol();
				Object obj = row.get(cols2Pos.get(col));
				ArrayList<Object> ranges = pmeta.getNodeGroupRanges();
				int i = 0;
				final int size = ranges.size();
				while (i < size)
				{
					if (((Comparable)obj).compareTo(ranges.get(i)) <= 0)
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
			else if (pmeta.isSingleNodeSet())
			{
				ArrayList<Integer> retval = new ArrayList<Integer>();
				retval.add(ngSet.get(pmeta.getSingleNode()));
				return retval;
			}
			else if (pmeta.nodeIsHash())
			{
				ArrayList<String> nodeHash = pmeta.getNodeHash();
				ArrayList<Object> partial = new ArrayList<Object>();
				for (String col : nodeHash)
				{
					partial.add(row.get(cols2Pos.get(table + "." + col)));
				}

				long hash = 0x7FFFFFFFFFFFFFFFL & hash(partial);
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
				// range
				String col = table + "." + pmeta.getNodeRangeCol();
				Object obj = row.get(cols2Pos.get(col));
				ArrayList<Object> ranges = pmeta.getNodeRanges();
				int i = 0;
				final int size = ranges.size();
				while (i < size)
				{
					if (((Comparable)obj).compareTo(ranges.get(i)) <= 0)
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

	public static void dropIndex(String schema, String index, Transaction tx) throws Exception
	{
		PlanCacheManager.invalidate();
		ArrayList<Object> row = PlanCacheManager.getTableAndIndexID().setParms(schema, index).execute(tx);
		int tableID = (Integer)row.get(0);
		int indexID = (Integer)row.get(1);
		PlanCacheManager.getDeleteIndex().setParms(tableID, indexID).execute(tx);
		PlanCacheManager.getDeleteIndexCol().setParms(tableID, indexID).execute(tx);
		PlanCacheManager.getDeleteIndexStats().setParms(tableID, indexID).execute(tx);
		BufferManager.invalidateFile(schema + "." + index + ".indx");
		PlanCacheManager.invalidate();
		getPartitioningCache.clear();
		getIndexesCache.clear();
		getKeysCache.clear();
		getTypesCache.clear();
		getOrdersCache.clear();
		getIndexColsForTableCache.clear();
		getLengthCache.clear();
		getTableIDCache.clear();
		getColCardCache.clear();
		getIndexIDsForTableCache.clear();
		getCols2PosForTableCache.clear();
		getCols2TypesCache.clear();
		getUniqueCache.clear();
		getTableCardCache.clear();
		getColTypeCache.clear();
		getDistCache.clear();
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
		BufferManager.invalidateFile(schema + "." + table + ".tbl");
		PlanCacheManager.invalidate();
		getPartitioningCache.clear();
		getIndexesCache.clear();
		getKeysCache.clear();
		getTypesCache.clear();
		getOrdersCache.clear();
		getIndexColsForTableCache.clear();
		getLengthCache.clear();
		getTableIDCache.clear();
		getColCardCache.clear();
		getIndexIDsForTableCache.clear();
		getCols2PosForTableCache.clear();
		getCols2TypesCache.clear();
		getUniqueCache.clear();
		getTableCardCache.clear();
		getColTypeCache.clear();
		getDistCache.clear();
	}

	public static void dropView(String schema, String table, Transaction tx) throws Exception
	{
		PlanCacheManager.getDeleteView().setParms(schema, table).execute(tx);
	}

	public static ArrayList<String> getColsFromIndexFileName(String index, Transaction tx, ArrayList<ArrayList<String>> keys, ArrayList<String> indexes) throws Exception
	{
		int i = 0;
		for (String indx : indexes)
		{
			if (indx.equals(index))
			{
				return keys.get(i);
			}

			i++;
		}

		return null;
	}

	public static HashMap<Integer, String> getCoordMap()
	{
		HashMap<Integer, String> retval = new HashMap<Integer, String>();
		for (Map.Entry entry : nodeTable.entrySet())
		{
			int node = (Integer)entry.getKey();
			String host = (String)entry.getValue();
			if (node < 0)
			{
				retval.put(node, host);
			}
		}

		return retval;
	}

	public static ArrayList<Integer> getCoordNodes()
	{
		return (ArrayList<Integer>)coords.clone();
	}

	public static ArrayList<Integer> getDevicesForTable(String schema, String table, Transaction tx) throws Exception
	{
		PartitionMetaData pmeta = new MetaData().new PartitionMetaData(schema, table, tx);
		if (pmeta.allDevices())
		{
			ArrayList<Integer> retval = new ArrayList<Integer>();
			int i = 0;
			final int num = getNumDevices();
			while (i < num)
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

	public static int getNumDevices()
	{
		return numDevices;
	}

	public static ArrayList<Integer> getWorkerNodes()
	{
		return (ArrayList<Integer>)workers.clone();
	}

	public static boolean isMyIP(String host) throws Exception
	{
		return isThisMyIpAddress(InetAddress.getByName(host));
	}

	public static boolean isThisMyIpAddress(InetAddress addr)
	{
		// Check if the address is a valid special local or loop back
		if (addr.isAnyLocalAddress() || addr.isLoopbackAddress())
		{
			return true;
		}

		// Check if the address is defined on any interface
		try
		{
			return NetworkInterface.getByInetAddress(addr) != null;
		}
		catch (SocketException e)
		{
			return false;
		}
	}

	public static int myCoordNum() throws Exception
	{
		if (HRDBMSWorker.type == HRDBMSWorker.TYPE_WORKER)
		{
			throw new Exception("Not a coordinator");
		}

		return myNode;
	}

	public static int myNodeNum()
	{
		return myNode;
	}

	public static void removeDefaultSchema(ConnectionWorker conn)
	{
		defaultSchemas.remove(conn);
	}

	public static void setDefaultSchema(ConnectionWorker conn, String schema)
	{
		defaultSchemas.put(conn, schema);
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

	private static ArrayList<Object> convertToHosts(ArrayList<Object> tree, Transaction tx) throws Exception
	{
		ArrayList<Object> retval = new ArrayList<Object>();
		int i = 0;
		final int size = tree.size();
		while (i < size)
		{
			Object obj = tree.get(i);
			if (obj instanceof Integer)
			{
				retval.add(new MetaData().getHostNameForNode((Integer)obj, tx));
			}
			else
			{
				retval.add(convertToHosts((ArrayList<Object>)obj, tx));
			}

			i++;
		}

		return retval;
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

		String inStr = new String(inMsg, StandardCharsets.UTF_8);
		if (!inStr.equals("OK"))
		{
			in.close();
			throw new Exception();
		}

		try
		{
			in.close();
		}
		catch (Exception e)
		{
		}
	}

	private static long hash(Object key) throws Exception
	{
		long eHash;
		if (key == null)
		{
			eHash = 0;
		}
		else
		{
			if (key instanceof ArrayList)
			{
				byte[] data = toBytes(key);
				eHash = MurmurHash.hash64(data, data.length);
			}
			else
			{
				byte[] data = key.toString().getBytes(StandardCharsets.UTF_8);
				eHash = MurmurHash.hash64(data, data.length);
			}
		}

		return eHash;
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

	private static byte[] longToBytes(long val)
	{
		final byte[] buff = new byte[8];
		buff[0] = (byte)(val >> 56);
		buff[1] = (byte)((val & 0x00FF000000000000L) >> 48);
		buff[2] = (byte)((val & 0x0000FF0000000000L) >> 40);
		buff[3] = (byte)((val & 0x000000FF00000000L) >> 32);
		buff[4] = (byte)((val & 0x00000000FF000000L) >> 24);
		buff[5] = (byte)((val & 0x0000000000FF0000L) >> 16);
		buff[6] = (byte)((val & 0x000000000000FF00L) >> 8);
		buff[7] = (byte)((val & 0x00000000000000FFL));
		return buff;
	}

	private static ArrayList<Object> makeTree(ArrayList<Integer> nodes)
	{
		int max = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("max_neighbor_nodes"));
		if (nodes.size() <= max)
		{
			ArrayList<Object> retval = new ArrayList<Object>(nodes);
			return retval;
		}

		ArrayList<Object> retval = new ArrayList<Object>();
		int i = 0;
		while (i < max)
		{
			retval.add(nodes.get(i));
			i++;
		}

		int remaining = nodes.size() - i;
		int perNode = remaining / max + 1;

		int j = 0;
		final int size = nodes.size();
		while (i < size)
		{
			int first = (Integer)retval.get(j);
			retval.remove(j);
			ArrayList<Integer> list = new ArrayList<Integer>(perNode + 1);
			list.add(first);
			int k = 0;
			while (k < perNode && i < size)
			{
				list.add(nodes.get(i));
				i++;
				k++;
			}

			retval.add(j, list);
			j++;
		}

		if (((ArrayList<Integer>)retval.get(0)).size() <= max)
		{
			return retval;
		}

		// more than 2 tier
		i = 0;
		while (i < retval.size())
		{
			ArrayList<Integer> list = (ArrayList<Integer>)retval.remove(i);
			retval.add(i, makeTree(list));
			i++;
		}

		return retval;
	}

	private static byte[] stringToBytes(String string)
	{
		byte[] data = null;
		try
		{
			data = string.getBytes(StandardCharsets.UTF_8);
		}
		catch (Exception e)
		{
		}
		byte[] len = intToBytes(data.length);
		byte[] retval = new byte[data.length + len.length];
		System.arraycopy(len, 0, retval, 0, len.length);
		System.arraycopy(data, 0, retval, len.length, data.length);
		return retval;
	}

	private static final byte[] toBytes(Object v) throws Exception
	{
		ArrayList<byte[]> bytes = null;
		ArrayList<Object> val;
		if (v instanceof ArrayList)
		{
			val = (ArrayList<Object>)v;
		}
		else
		{
			final byte[] retval = new byte[9];
			retval[0] = 0;
			retval[1] = 0;
			retval[2] = 0;
			retval[3] = 5;
			retval[4] = 0;
			retval[5] = 0;
			retval[6] = 0;
			retval[7] = 1;
			retval[8] = 5;
			return retval;
		}

		int size = val.size() + 8;
		final byte[] header = new byte[size];
		int i = 8;
		int z = 0;
		int limit = val.size();
		// for (final Object o : val)
		while (z < limit)
		{
			Object o = val.get(z++);
			if (o instanceof Long)
			{
				header[i] = (byte)0;
				size += 8;
			}
			else if (o instanceof Integer)
			{
				header[i] = (byte)1;
				size += 4;
			}
			else if (o instanceof Double)
			{
				header[i] = (byte)2;
				size += 8;
			}
			else if (o instanceof MyDate)
			{
				header[i] = (byte)3;
				size += 4;
			}
			else if (o instanceof String)
			{
				header[i] = (byte)4;
				byte[] b = ((String)o).getBytes(StandardCharsets.UTF_8);
				size += (4 + b.length);

				if (bytes == null)
				{
					bytes = new ArrayList<byte[]>();
					bytes.add(b);
				}
				else
				{
					bytes.add(b);
				}
			}
			// else if (o instanceof AtomicLong)
			// {
			// header[i] = (byte)6;
			// size += 8;
			// }
			// else if (o instanceof AtomicBigDecimal)
			// {
			// header[i] = (byte)7;
			// size += 8;
			// }
			else if (o instanceof ArrayList)
			{
				if (((ArrayList)o).size() != 0)
				{
					Exception e = new Exception("Non-zero size ArrayList in toBytes()");
					HRDBMSWorker.logger.error("Non-zero size ArrayList in toBytes()", e);
					throw e;
				}
				header[i] = (byte)8;
			}
			else
			{
				HRDBMSWorker.logger.error("Unknown type " + o.getClass() + " in toBytes()");
				HRDBMSWorker.logger.error(o);
				throw new Exception("Unknown type " + o.getClass() + " in toBytes()");
			}

			i++;
		}

		final byte[] retval = new byte[size];
		// System.out.println("In toBytes(), row has " + val.size() +
		// " columns, object occupies " + size + " bytes");
		System.arraycopy(header, 0, retval, 0, header.length);
		i = 8;
		final ByteBuffer retvalBB = ByteBuffer.wrap(retval);
		retvalBB.putInt(size - 4);
		retvalBB.putInt(val.size());
		retvalBB.position(header.length);
		int x = 0;
		z = 0;
		limit = val.size();
		// for (final Object o : val)
		while (z < limit)
		{
			Object o = val.get(z++);
			if (retval[i] == 0)
			{
				retvalBB.putLong((Long)o);
			}
			else if (retval[i] == 1)
			{
				retvalBB.putInt((Integer)o);
			}
			else if (retval[i] == 2)
			{
				retvalBB.putDouble((Double)o);
			}
			else if (retval[i] == 3)
			{
				retvalBB.putInt(((MyDate)o).getTime());
			}
			else if (retval[i] == 4)
			{
				byte[] temp = bytes.get(x);
				x++;
				retvalBB.putInt(temp.length);
				retvalBB.put(temp);
			}
			// else if (retval[i] == 6)
			// {
			// retvalBB.putLong(((AtomicLong)o).get());
			// }
			// else if (retval[i] == 7)
			// {
			// retvalBB.putDouble(((AtomicBigDecimal)o).get().doubleValue());
			// }
			else if (retval[i] == 8)
			{
			}

			i++;
		}

		return retval;
	}

	public void cluster(String schema, String table, Transaction tx, TreeMap<Integer, String> pos2Col, HashMap<String, String> cols2Types, int type) throws Exception
	{
		ArrayList<Integer> nodes = getWorkerNodes();
		ArrayList<Object> tree = makeTree(nodes);
		ArrayList<Socket> sockets = new ArrayList<Socket>();

		int max = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("max_neighbor_nodes"));

		for (Object o : tree)
		{
			ArrayList<Object> list;
			if (o instanceof Integer)
			{
				list = new ArrayList<Object>(1);
				list.add(o);
			}
			else
			{
				list = (ArrayList<Object>)o;
			}

			Object obj = list.get(0);
			while (obj instanceof ArrayList)
			{
				obj = ((ArrayList)obj).get(0);
			}

			Socket sock;
			String hostname = new MetaData().getHostNameForNode((Integer)obj, tx);
			// sock = new Socket(hostname,
			// Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
			sock = new Socket();
			sock.setReceiveBufferSize(4194304);
			sock.setSendBufferSize(4194304);
			sock.connect(new InetSocketAddress(hostname, Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number"))));
			OutputStream out = sock.getOutputStream();
			byte[] outMsg = "CLUSTER         ".getBytes(StandardCharsets.UTF_8);
			outMsg[8] = 0;
			outMsg[9] = 0;
			outMsg[10] = 0;
			outMsg[11] = 0;
			outMsg[12] = 0;
			outMsg[13] = 0;
			outMsg[14] = 0;
			outMsg[15] = 0;
			out.write(outMsg);
			out.write(longToBytes(tx.number()));
			out.write(stringToBytes(schema));
			out.write(stringToBytes(table));
			out.write(intToBytes(type));
			ObjectOutputStream objOut = new ObjectOutputStream(out);
			objOut.writeObject(convertToHosts(list, tx));
			objOut.writeObject(pos2Col);
			objOut.writeObject(cols2Types);
			objOut.flush();
			out.flush();
			sockets.add(sock);

			if (sockets.size() >= max)
			{
				sock = sockets.get(0);
				out = sock.getOutputStream();
				getConfirmation(sock);
				objOut.close();
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

	public void createIndex(String schema, String table, String index, ArrayList<IndexDef> defs, boolean unique, Transaction tx) throws Exception
	{
		Integer tableID = getTableIDCache.get(schema + "." + table);
		if (tableID == null)
		{
			tableID = PlanCacheManager.getTableID().setParms(schema, table).execute(tx);
			getTableIDCache.put(schema + "." + table, tableID);
		}
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
		populateIndex(schema, index, table, tx, cols2Pos);
	}

	public void createTable(String schema, String table, ArrayList<ColDef> defs, ArrayList<String> pks, Transaction tx, String nodeGroupExp, String nodeExp, String deviceExp, int tType, ArrayList<Integer> colOrder, ArrayList<Integer> organization) throws Exception
	{
		// validate expressions
		HashMap<String, String> cols2Types = new HashMap<String, String>();
		for (ColDef def : defs)
		{
			String name = def.getCol().getColumn();
			String type = def.getType();
			if (type.equals("LONG"))
			{
				type = "BIGINT";
			}
			else if (type.equals("FLOAT"))
			{
				type = "DOUBLE";
			}
			else if (type.startsWith("CHAR"))
			{
				type = "VARCHAR";
			}

			cols2Types.put(name, type);
		}
		new MetaData().new PartitionMetaData(schema, table, nodeGroupExp, nodeExp, deviceExp, tx, cols2Types);
		// tables
		// cols
		// indexes
		// indexcols
		// partitioning
		int tableID = PlanCacheManager.getNextTableID().setParms().execute(tx);
		String typeFlag = "R";
		if (tType == 1)
		{
			typeFlag = "C";
		}

		PlanCacheManager.getInsertTable().setParms(tableID, schema, table, typeFlag).execute(tx);
		int colID = 0;
		for (ColDef def : defs)
		{
			// INT, LONG, FLOAT, DATE, CHAR(x)
			// COLID, TABLEID, NAME, TYPE, LENGTH, SCALE, PKPOS, NULLABLE
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
				length = Integer.parseInt(type.substring(5, type.length() - 1));
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
		int indexID = -1;
		if (pks != null && pks.size() != 0)
		{
			indexID = PlanCacheManager.getNextIndexID().setParms(tableID).execute(tx);
			PlanCacheManager.getInsertIndex().setParms(indexID, "PK" + table, tableID, true).execute(tx);

			HashMap<String, Integer> cols2Pos = getCols2PosForTable(schema, table, tx);
			int pos = 0;
			for (String col : pks)
			{
				colID = cols2Pos.get(table + "." + col);
				PlanCacheManager.getInsertIndexCol().setParms(indexID, tableID, colID, pos, true).execute(tx);
				pos++;
			}
		}

		buildTable(schema, table, defs.size(), tx, tType, defs, colOrder, organization);

		if (pks != null && pks.size() != 0)
		{
			buildIndex(schema, "PK" + table, table, pks.size(), true, tx);
		}
	}

	public int determineDevice(String schema, String table, ArrayList<Object> row, PartitionMetaData pmeta, Transaction tx) throws Exception
	{
		HashMap<String, Integer> cols2Pos = getCols2PosForTable(schema, table, tx);
		if (pmeta.deviceIsHash())
		{
			ArrayList<String> devHash = pmeta.getDeviceHash();
			ArrayList<Object> partial = new ArrayList<Object>();
			for (String col : devHash)
			{
				partial.add(row.get(cols2Pos.get(table + "." + col)));
			}

			long hash = 0x7FFFFFFFFFFFFFFFL & hash(partial);
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
			final int size = ranges.size();
			while (i < size)
			{
				if (((Comparable)obj).compareTo(ranges.get(i)) <= 0)
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
			for (final String col : index.getKeys())
			{
				if (cols.contains(col))
				{
					count++;
				}
				else
				{
					break;
				}
			}

			if (count > maxCount)
			{
				maxCount = count;
				maxIndex = index;
			}
		}

		if (maxCount == cols.size())
		{
			for (Index index : indexes)
			{
				int count = 0;
				for (String col : index.getKeys())
				{
					if (cols.contains(col))
					{
						count++;
					}
					else
					{
						break;
					}
				}

				if (count == maxCount && index.getKeys().size() == maxCount)
				{
					return index;
				}
			}
		}

		return maxIndex;
	}

	public long getCard(String col, HashMap<String, Double> generated, Transaction tx, Operator tree) throws Exception
	{
		String c = col.substring(col.indexOf('.') + 1);
		String ST = getTableForCol(col, tree);

		try
		{
			String schema = ST.substring(0, ST.indexOf('.'));
			String table = ST.substring(ST.indexOf('.') + 1);
			Long card = getColCardCache.get(schema + "." + table + "." + c);
			if (card == null)
			{
				card = PlanCacheManager.getColCard().setParms(schema, table, c).execute(tx);
				getColCardCache.put(schema + "." + table + "." + c, card);
			}
			return card;
		}
		catch (Exception e)
		{
		}

		if (generated.containsKey(col))
		{
			return (generated.get(col).longValue());
		}

		// HRDBMSWorker.logger.warn("Can't find cardinality for " + schema + "."
		// + table + "." + col);
		return 1000000;
	}

	public long getCard(String col, RootOperator op, Transaction tx, Operator tree) throws Exception
	{
		return getCard(col, op.getGenerated(), tx, tree);
	}

	public long getCard(String schema, String table, String col, HashMap<String, Double> generated, Transaction tx)
	{
		try
		{
			Long card = getColCardCache.get(schema + "." + table + "." + col);
			if (card == null)
			{
				card = PlanCacheManager.getColCard().setParms(schema, table, col).execute(tx);
				getColCardCache.put(schema + "." + table + "." + col, card);
			}
			return card;
		}
		catch (Exception e)
		{
			// HRDBMSWorker.logger.debug("", e);
		}

		if (generated.containsKey(col))
		{
			return (generated.get(col).longValue());
		}

		// HRDBMSWorker.logger.warn("Can't find cardinality for " + schema + "."
		// + table + "." + col);
		return 1000000;
	}

	public long getCard(String schema, String table, String col, RootOperator op, Transaction tx)
	{
		return getCard(schema, table, col, op.getGenerated(), tx);
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
			Integer tableID = getTableIDCache.get(theSchema + "." + theTable);
			if (tableID == null)
			{
				tableID = PlanCacheManager.getTableID().setParms(theSchema, theTable).execute(tx);
				getTableIDCache.put(theSchema + "." + theTable, tableID);
			}
			ArrayList<Object> rs = getIndexIDsForTableCache.get(theSchema + "." + theTable);
			if (rs == null)
			{
				rs = PlanCacheManager.getIndexIDsForTable().setParms(theSchema, theTable).execute(tx);
				getIndexIDsForTableCache.put(theSchema + "." + theTable, rs);
			}
			for (Object r : rs)
			{
				if (!(r instanceof DataEndMarker))
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

	public long getColgroupCard(ArrayList<String> cs, HashMap<String, Double> generated, Transaction tx, Operator tree) throws Exception
	{
		if (cs.size() == 0)
		{
			return 1;
		}

		ArrayList<String> schemas = new ArrayList<String>();
		ArrayList<String> tables = new ArrayList<String>();
		ArrayList<String> cols = new ArrayList<String>();
		for (String c : cs)
		{
			cols.add(c.substring(c.indexOf('.') + 1));
			String ST = getTableForCol(c, tree);
			if (ST == null)
			{
				int i = 0;
				long card = 1;
				final int size = cs.size();
				while (i < size)
				{
					long old = card;
					card *= 1000000;
					if (card < 0)
					{
						return old;
					}
					i++;
				}

				return card;
			}
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

		Integer tableID = -1;
		if (oneSchema && oneTable && theSchema != null && theTable != null)
		{
			try
			{
				tableID = getTableIDCache.get(theSchema + "." + theTable);
				if (tableID == null)
				{
					tableID = PlanCacheManager.getTableID().setParms(theSchema, theTable).execute(tx);
					getTableIDCache.put(theSchema + "." + theTable, tableID);
				}
			}
			catch (Exception e)
			{
				oneSchema = false;
			}
		}

		if (oneSchema && oneTable && theSchema != null && theTable != null)
		{
			ArrayList<Object> rs = getIndexIDsForTableCache.get(theSchema + "." + theTable);
			if (rs == null)
			{
				rs = PlanCacheManager.getIndexIDsForTable().setParms(theSchema, theTable).execute(tx);
				getIndexIDsForTableCache.put(theSchema + "." + theTable, rs);
			}
			for (Object r : rs)
			{
				if (!(r instanceof DataEndMarker))
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

	public long getColgroupCard(ArrayList<String> cols, RootOperator op, Transaction tx, Operator tree) throws Exception
	{
		return getColgroupCard(cols, op.getGenerated(), tx, tree);
	}

	public HashMap<String, Integer> getCols2PosForTable(String schema, String name, Transaction tx) throws Exception
	{
		final HashMap<String, Integer> retval = new HashMap<String, Integer>();
		ArrayList<Object> rs = getCols2PosForTableCache.get(schema + "." + name);
		if (rs == null)
		{
			rs = PlanCacheManager.getCols2PosForTable().setParms(schema, name).execute(tx);
			getCols2PosForTableCache.put(schema + "." + name, rs);
		}

		// DEBUG
		// if (rs.size() < 2)
		// {
		// HRDBMSWorker.logger.debug("Unable to get cols2Pos info for " + schema
		// + "." + name);
		// Block b = new Block("/data1/SYS.PKTABLES.indx", 0);
		// tx.requestPage(b);
		// Page p = tx.getPage(b);
		// RandomAccessFile raf = new RandomAccessFile("/tmp/dump", "rw");
		// FileChannel fc = raf.getChannel();
		// p.buffer().position(0);
		// fc.write(p.buffer());
		// fc.close();
		// raf.close();
		// System.exit(-1);
		// }
		// DEBUG

		for (Object r : rs)
		{
			if (r instanceof DataEndMarker)
			{
			}
			else
			{
				ArrayList<Object> row = (ArrayList<Object>)r;
				retval.put(name + "." + (String)row.get(0), (Integer)row.get(1));
			}
		}

		return retval;
	}

	public HashMap<String, String> getCols2TypesForTable(String schema, String name, Transaction tx) throws Exception
	{
		final HashMap<String, String> retval = new HashMap<String, String>();
		ArrayList<Object> rs = getCols2TypesCache.get(schema + "." + name);
		if (rs == null)
		{
			rs = PlanCacheManager.getCols2Types().setParms(schema, name).execute(tx);
			getCols2TypesCache.put(schema + "." + name, rs);
		}
		for (Object r : rs)
		{
			if (!(r instanceof DataEndMarker))
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
		}

		return retval;
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
			// TODO return userid
			return "HRDBMS";
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

	public String getHostNameForNode(int node)
	{
		return nodeTable.get(node);
	}

	public String getHostNameForNode(int node, Transaction tx) throws Exception
	{
		if (node == -1)
		{
			return getMyHostName(tx);
		}
		else
		{
			return nodeTable.get(node);
		}
	}

	public ArrayList<String> getIndexColsForTable(String schema, String table, Transaction tx) throws Exception
	{
		ArrayList<Object> rs = getIndexColsForTableCache.get(schema + "." + table);
		if (rs == null)
		{
			rs = PlanCacheManager.getIndexColsForTable().setParms(schema, table).execute(tx);
			getIndexColsForTableCache.put(schema + "." + table, rs);
		}
		ArrayList<String> retRow = new ArrayList<String>();
		for (Object o : rs)
		{
			if (!(o instanceof DataEndMarker))
			{
				ArrayList<Object> row = (ArrayList<Object>)o;
				retRow.add(table + "." + row.get(0));
			}
		}

		return retRow;
	}

	public ArrayList<Index> getIndexesForTable(String schema, String table, Transaction tx) throws Exception
	{
		final ArrayList<Index> retval = new ArrayList<Index>();
		ArrayList<Object> rs = getIndexesCache.get(schema + "." + table);
		if (rs == null)
		{
			rs = PlanCacheManager.getIndexes().setParms(schema, table).execute(tx);
			getIndexesCache.put(schema + "." + table, rs);
		}
		for (Object o : rs)
		{
			if (!(o instanceof DataEndMarker))
			{
				ArrayList<Object> row = (ArrayList<Object>)o;
				ArrayList<Object> rs2 = getKeysCache.get(schema + "." + row.get(0));
				if (rs2 == null)
				{
					rs2 = PlanCacheManager.getKeys().setParms(schema, (String)row.get(0)).execute(tx);
					getKeysCache.put(schema + "." + row.get(0), rs2);
				}
				// tabname, colname
				ArrayList<String> keys = new ArrayList<String>();
				for (Object o2 : rs2)
				{
					ArrayList<Object> row2 = (ArrayList<Object>)o2;
					keys.add(row2.get(0) + "." + row2.get(1));
				}

				rs2 = getTypesCache.get(schema + "." + row.get(0));
				if (rs2 == null)
				{
					rs2 = PlanCacheManager.getTypes().setParms(schema, (String)row.get(0)).execute(tx);
					getTypesCache.put(schema + "." + row.get(0), rs2);
				}
				ArrayList<String> types = new ArrayList<String>();
				for (Object o2 : rs2)
				{
					ArrayList<Object> row2 = (ArrayList<Object>)o2;
					String type = (String)row2.get(0);
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

				rs2 = getOrdersCache.get(schema + "." + row.get(0));
				if (rs2 == null)
				{
					rs2 = PlanCacheManager.getOrders().setParms(schema, (String)row.get(0)).execute(tx);
					getOrdersCache.put(schema + "." + row.get(0), rs2);
				}
				ArrayList<Boolean> orders = new ArrayList<Boolean>();
				for (Object o2 : rs2)
				{
					if (!(o2 instanceof DataEndMarker))
					{
						ArrayList<Object> row2 = (ArrayList<Object>)o2;
						String ord = (String)row2.get(0);
						boolean order = true;
						if (!ord.equals("A"))
						{
							order = false;
						}
						orders.add(order);
					}
				}

				retval.add(new Index(schema + "." + row.get(0) + ".indx", keys, types, orders));
			}
		}

		return retval;
	}

	public ArrayList<String> getIndexFileNamesForTable(String schema, String table, Transaction tx) throws Exception
	{
		ArrayList<Object> rs = getIndexesCache.get(schema + "." + table);
		if (rs == null)
		{
			rs = PlanCacheManager.getIndexes().setParms(schema, table).execute(tx);
			getIndexesCache.put(schema + "." + table, rs);
		}

		ArrayList<String> retval = new ArrayList<String>();

		for (Object o : rs)
		{
			if (!(o instanceof DataEndMarker))
			{
				ArrayList<Object> row = (ArrayList<Object>)o;
				retval.add(schema + "." + row.get(0) + ".indx");
			}
		}

		return retval;
	}

	public ArrayList<ArrayList<String>> getKeys(ArrayList<String> indexes, Transaction tx) throws Exception
	{
		ArrayList<ArrayList<String>> retval = new ArrayList<ArrayList<String>>();
		for (String index : indexes)
		{
			String schema = index.substring(0, index.indexOf('.'));
			String name = index.substring(schema.length() + 1, index.indexOf('.', schema.length() + 1));
			ArrayList<Object> rs = getKeysCache.get(schema + "." + name);
			if (rs == null)
			{
				rs = PlanCacheManager.getKeys().setParms(schema, name).execute(tx);
				getKeysCache.put(schema + "." + name, rs);
			}
			// tabname, colname
			ArrayList<String> retRow = new ArrayList<String>();
			for (Object o : rs)
			{
				if (!(o instanceof DataEndMarker))
				{
					ArrayList<Object> row = (ArrayList<Object>)o;
					retRow.add(row.get(0) + "." + row.get(1));
				}
			}
			retval.add(retRow);
		}

		return retval;
	}

	public int getLengthForCharCol(String schema, String table, String col, Transaction tx) throws Exception
	{
		if (col.contains("."))
		{
			col = col.substring(col.indexOf('.') + 1);
		}

		Integer retval = getLengthCache.get(schema + "." + table + "." + col);
		if (retval == null)
		{
			retval = PlanCacheManager.getLength().setParms(schema, table, col).execute(tx);
			getLengthCache.put(schema + "." + table + "." + col, retval);
		}
		return retval;
	}

	public String getMyHostName(Transaction tx) throws Exception
	{
		return nodeTable.get(myNode);
	}

	public ArrayList<Integer> getNodesForTable(String schema, String table, Transaction tx) throws Exception
	{
		ArrayList<Object> r = getPartitioningCache.get(schema + "." + table);
		if (r == null)
		{
			r = PlanCacheManager.getPartitioning().setParms(schema, table).execute(tx);
			getPartitioningCache.put(schema + "." + table, r);
		}

		ArrayList<Object> row = r;
		String nodeGroupExp = (String)row.get(0);
		String nodeExp = (String)row.get(1);

		if (nodeGroupExp.equals("NONE"))
		{
			if (nodeExp.startsWith("ALL") || nodeExp.startsWith("ANY"))
			{
				ArrayList<Integer> retval = getWorkerNodes();
				return retval;
			}

			StringTokenizer tokens1 = new StringTokenizer(nodeExp, ",", false);
			String nodeSet = tokens1.nextToken();
			StringTokenizer tokens2 = new StringTokenizer(nodeSet, "|{}", false);
			HashSet<Integer> retval = new HashSet<Integer>();
			while (tokens2.hasMoreTokens())
			{
				int node = Integer.parseInt(tokens2.nextToken());
				if (node == -1)
				{
					ArrayList<Integer> coords = getCoordNodes();
					retval.addAll(coords);
				}
				else
				{
					retval.add(node);
				}
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

	public ArrayList<ArrayList<Boolean>> getOrders(ArrayList<String> indexes, Transaction tx) throws Exception
	{
		ArrayList<ArrayList<Boolean>> retval = new ArrayList<ArrayList<Boolean>>();
		for (String index : indexes)
		{
			String schema = index.substring(0, index.indexOf('.'));
			String name = index.substring(schema.length() + 1, index.indexOf('.', schema.length() + 1));
			ArrayList<Object> rs = getOrdersCache.get(schema + "." + name);
			if (rs == null)
			{
				rs = PlanCacheManager.getOrders().setParms(schema, name).execute(tx);
				getOrdersCache.put(schema + "." + name, rs);
			}
			ArrayList<Boolean> retRow = new ArrayList<Boolean>();
			for (Object o : rs)
			{
				if (!(o instanceof DataEndMarker))
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
			}
			retval.add(retRow);
		}

		return retval;
	}

	public PartitionMetaData getPartMeta(String schema, String table, Transaction tx) throws Exception
	{
		return new PartitionMetaData(schema, table, tx);
	}

	public TreeMap<Integer, String> getPos2ColForTable(String schema, String name, Transaction tx) throws Exception
	{
		final TreeMap<Integer, String> retval = new TreeMap<Integer, String>();
		ArrayList<Object> rs = getCols2PosForTableCache.get(schema + "." + name);
		if (rs == null)
		{
			rs = PlanCacheManager.getCols2PosForTable().setParms(schema, name).execute(tx);
			getCols2PosForTableCache.put(schema + "." + name, rs);
		}
		for (Object r : rs)
		{
			if (r instanceof DataEndMarker)
			{
			}
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
			Long card = getTableCardCache.get(schema + "." + table);
			if (card == null)
			{
				card = PlanCacheManager.getTableCard().setParms(schema, table).execute(tx);
				getTableCardCache.put(schema + "." + table, card);
			}
			return card;
		}
		catch (Exception e)
		{
			// HRDBMSWorker.logger.warn("No table card found for table " +
			// schema + "." + table);
			return 1000000;
		}
	}

	public String getTableForCol(String col, Operator tree) throws Exception// must
	// return
	// schema.table
	// -
	// must
	// accept
	// qualified
	// or
	// unqualified
	// col
	{
		ColAndTree cat = new ColAndTree(col, tree);
		String s = gtfcCache.get(cat);
		if (s != null)
		{
			return s;
		}

		ArrayList<TableScanOperator> tables = new ArrayList<TableScanOperator>();
		getTables(tree, new HashSet<Operator>(), tables);
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
					if (tableIs(t, qual) || aliasIs(t, qual))
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
				String retval = retSchema + "." + retTable;
				gtfcCache.put(cat, retval);
				return retval;
			}
			else
			{
				return null;
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
				String retval = retSchema + "." + retTable;
				gtfcCache.put(cat, retval);
				return retval;
			}
			else
			{
				return null;
			}
		}
	}

	public int getTypeForTable(String schema, String table, Transaction tx) throws Exception
	{
		return PlanCacheManager.getTableType().setParms(schema, table).execute(tx);
	}

	public ArrayList<ArrayList<String>> getTypes(ArrayList<String> indexes, Transaction tx) throws Exception
	{
		ArrayList<ArrayList<String>> retval = new ArrayList<ArrayList<String>>();
		for (String index : indexes)
		{
			String schema = index.substring(0, index.indexOf('.'));
			String name = index.substring(schema.length() + 1, index.indexOf('.', schema.length() + 1));
			ArrayList<Object> rs = getTypesCache.get(schema + "." + name);
			if (rs == null)
			{
				rs = PlanCacheManager.getTypes().setParms(schema, name).execute(tx);
				getTypesCache.put(schema + "." + name, rs);
			}
			ArrayList<String> retRow = new ArrayList<String>();
			for (Object o : rs)
			{
				if (!(o instanceof DataEndMarker))
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
			}
			retval.add(retRow);
		}

		return retval;
	}

	public ArrayList<Boolean> getUnique(String schema, String table, Transaction tx) throws Exception
	{
		ArrayList<Boolean> retval = new ArrayList<Boolean>();
		ArrayList<Object> rs = getUniqueCache.get(schema + "." + table);
		if (rs == null)
		{
			rs = PlanCacheManager.getUnique().setParms(schema, table).execute(tx);
			getUniqueCache.put(schema + "." + table, rs);
		}
		for (Object o : rs)
		{
			if (!(o instanceof DataEndMarker))
			{
				ArrayList<Object> row = (ArrayList<Object>)o;
				boolean unique = (((String)row.get(1)).equals("Y"));
				retval.add(unique);
			}
		}

		return retval;
	}

	public ArrayList<Index> getUniqueIndexesForTable(String schema, String table, Transaction tx) throws Exception
	{
		final ArrayList<Index> retval = new ArrayList<Index>();
		ArrayList<Object> rs = getUniqueIndexesCache.get(schema + "." + table);
		if (rs == null)
		{
			rs = PlanCacheManager.getUniqueIndexes().setParms(schema, table).execute(tx);
			getUniqueIndexesCache.put(schema + "." + table, rs);
		}
		for (Object o : rs)
		{
			if (!(o instanceof DataEndMarker))
			{
				ArrayList<Object> row = (ArrayList<Object>)o;
				ArrayList<Object> rs2 = getKeysCache.get(schema + "." + row.get(0));
				if (rs2 == null)
				{
					rs2 = PlanCacheManager.getKeys().setParms(schema, (String)row.get(0)).execute(tx);
					getKeysCache.put(schema + "." + row.get(0), rs2);
				}
				// tabname, colname
				ArrayList<String> keys = new ArrayList<String>();
				for (Object o2 : rs2)
				{
					ArrayList<Object> row2 = (ArrayList<Object>)o2;
					keys.add(row2.get(0) + "." + row2.get(1));
				}

				rs2 = getTypesCache.get(schema + "." + row.get(0));
				if (rs2 == null)
				{
					rs2 = PlanCacheManager.getTypes().setParms(schema, (String)row.get(0)).execute(tx);
					getTypesCache.put(schema + "." + row.get(0), rs2);
				}
				ArrayList<String> types = new ArrayList<String>();
				for (Object o2 : rs2)
				{
					ArrayList<Object> row2 = (ArrayList<Object>)o2;
					String type = (String)row2.get(0);
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

				rs2 = getOrdersCache.get(schema + "." + row.get(0));
				if (rs2 == null)
				{
					rs2 = PlanCacheManager.getOrders().setParms(schema, (String)row.get(0)).execute(tx);
					getOrdersCache.put(schema + "." + row.get(0), rs2);
				}
				ArrayList<Boolean> orders = new ArrayList<Boolean>();
				for (Object o2 : rs2)
				{
					if (!(o2 instanceof DataEndMarker))
					{
						ArrayList<Object> row2 = (ArrayList<Object>)o2;
						String ord = (String)row2.get(0);
						boolean order = true;
						if (!ord.equals("A"))
						{
							order = false;
						}
						orders.add(order);
					}
				}

				retval.add(new Index(schema + "." + row.get(0) + ".indx", keys, types, orders));
			}
		}

		return retval;
	}

	public String getViewSQL(String schema, String name, Transaction tx) throws Exception
	{
		return PlanCacheManager.getViewSQL().setParms(schema, name).execute(tx);
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

	// public static int getNumNodes(Transaction tx) throws Exception
	// {
	// return PlanCacheManager.getCountWorkerNodes().setParms().execute(tx);
	// }

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

	// likelihood of a row directly out of the table passing this test
	public double likelihood(Filter filter, HashMap<String, Double> generated, Transaction tx, Operator tree) throws Exception
	{
		long leftCard = 1;
		long rightCard = 1;

		if (filter instanceof ConstantFilter)
		{
			final double retval = ((ConstantFilter)filter).getLikelihood();
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
			if (leftST == null)
			{
				leftCard = 1000000;
			}
			else
			{
				String leftSchema = leftST.substring(0, leftST.indexOf('.'));
				String leftTable = leftST.substring(leftST.indexOf('.') + 1);
				String leftCol = left.substring(left.indexOf('.') + 1);
				leftCard = getCard(leftSchema, leftTable, leftCol, generated, tx);
			}
		}

		if (filter.rightIsColumn())
		{
			// figure out number of possible values for right side
			String right = filter.rightColumn();
			String rightST = getTableForCol(right, tree);
			if (rightST == null)
			{
				rightCard = 1000000;
			}
			else
			{
				String rightSchema = rightST.substring(0, rightST.indexOf('.'));
				String rightTable = rightST.substring(rightST.indexOf('.') + 1);
				String rightCol = right.substring(right.indexOf('.') + 1);
				rightCard = getCard(rightSchema, rightTable, rightCol, generated, tx);
			}
		}

		final String op = filter.op();

		if (op.equals("E"))
		{
			if (filter.leftIsColumn() && filter.rightIsColumn())
			{
				final double retval = 1.0 / smaller(leftCard, rightCard);
				return retval;
			}

			final double retval = 1.0 / bigger(leftCard, rightCard);
			return retval;
		}

		if (op.equals("NE"))
		{
			if (filter.leftIsColumn() && filter.rightIsColumn())
			{
				final double retval = 1.0 - (1.0 / smaller(leftCard, rightCard));
				return retval;
			}

			final double retval = 1.0 - 1.0 / bigger(leftCard, rightCard);
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
					return retval;
				}
				else if (filter.rightIsDate())
				{
					final MyDate right = filter.getRightDate();
					final String left = filter.leftColumn();
					final double retval = percentBelow(left, right, tx, tree);
					return retval;
				}
				else
				{
					// string
					final String right = filter.getRightString();
					final String left = filter.leftColumn();
					final double retval = percentBelow(left, right, tx, tree);
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
					return retval;
				}
				else if (filter.leftIsDate())
				{
					final MyDate left = filter.getLeftDate();
					final String right = filter.rightColumn();
					final double retval = percentAbove(right, left, tx, tree);
					return retval;
				}
				else
				{
					// string
					final String left = filter.getLeftString();
					final String right = filter.rightColumn();
					final double retval = percentAbove(right, left, tx, tree);
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
					return retval;
				}
				else if (filter.rightIsDate())
				{
					final MyDate right = filter.getRightDate();
					final String left = filter.leftColumn();
					final double retval = percentAbove(left, right, tx, tree);
					return retval;
				}
				else
				{
					// string
					final String right = filter.getRightString();
					final String left = filter.leftColumn();
					final double retval = percentAbove(left, right, tx, tree);
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
					return retval;
				}
				else if (filter.leftIsDate())
				{
					final MyDate left = filter.getLeftDate();
					final String right = filter.rightColumn();
					final double retval = percentBelow(right, left, tx, tree);
					return retval;
				}
				else
				{
					// string
					final String left = filter.getLeftString();
					final String right = filter.rightColumn();
					final double retval = percentBelow(right, left, tx, tree);
					return retval;
				}
			}
		}

		if (op.equals("LI"))
		{
			return 0.05;
		}

		if (op.equals("NL"))
		{
			return 0.95;
		}

		throw new Exception("Unknown operator in likelihood()");
	}

	public double likelihood(Filter filter, RootOperator op, Transaction tx, Operator tree) throws Exception
	{
		return likelihood(filter, op.getGenerated(), tx, tree);
	}

	public double likelihood(Filter filter, Transaction tx, Operator tree) throws Exception
	{
		long leftCard = 1;
		long rightCard = 1;
		final HashMap<String, Double> generated = new HashMap<String, Double>();

		if (filter instanceof ConstantFilter)
		{
			((ConstantFilter)filter).getLikelihood();
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
			if (leftST == null)
			{
				leftCard = 1000000;
			}
			else
			{
				String leftSchema = leftST.substring(0, leftST.indexOf('.'));
				String leftTable = leftST.substring(leftST.indexOf('.') + 1);
				String leftCol = left.substring(left.indexOf('.') + 1);
				leftCard = getCard(leftSchema, leftTable, leftCol, generated, tx);
			}
		}

		if (filter.rightIsColumn())
		{
			// figure out number of possible values for right side
			String right = filter.rightColumn();
			String rightST = getTableForCol(right, tree);
			if (rightST == null)
			{
				rightCard = 1000000;
			}
			else
			{
				String rightSchema = rightST.substring(0, rightST.indexOf('.'));
				String rightTable = rightST.substring(rightST.indexOf('.') + 1);
				String rightCol = right.substring(right.indexOf('.') + 1);
				rightCard = getCard(rightSchema, rightTable, rightCol, generated, tx);
			}
		}

		final String op = filter.op();

		if (op.equals("E"))
		{
			if (filter.leftIsColumn() && filter.rightIsColumn())
			{
				final double retval = 1.0 / smaller(leftCard, rightCard);
				return retval;
			}

			final double retval = 1.0 / bigger(leftCard, rightCard);
			return retval;
		}

		if (op.equals("NE"))
		{
			if (filter.leftIsColumn() && filter.rightIsColumn())
			{
				final double retval = 1.0 - (1.0 / smaller(leftCard, rightCard));
				return retval;
			}

			final double retval = 1.0 - 1.0 / bigger(leftCard, rightCard);
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
					return retval;
				}
				else if (filter.rightIsDate())
				{
					final MyDate right = filter.getRightDate();
					final String left = filter.leftColumn();
					final double retval = percentBelow(left, right, tx, tree);
					return retval;
				}
				else
				{
					// string
					final String right = filter.getRightString();
					final String left = filter.leftColumn();
					final double retval = percentBelow(left, right, tx, tree);
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
					return retval;
				}
				else if (filter.leftIsDate())
				{
					final MyDate left = filter.getLeftDate();
					final String right = filter.rightColumn();
					final double retval = percentAbove(right, left, tx, tree);
					return retval;
				}
				else
				{
					// string
					final String left = filter.getLeftString();
					final String right = filter.rightColumn();
					final double retval = percentAbove(right, left, tx, tree);
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
					return retval;
				}
				else if (filter.rightIsDate())
				{
					final MyDate right = filter.getRightDate();
					final String left = filter.leftColumn();
					final double retval = percentAbove(left, right, tx, tree);
					return retval;
				}
				else
				{
					// string
					final String right = filter.getRightString();
					final String left = filter.leftColumn();
					final double retval = percentAbove(left, right, tx, tree);
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
					return retval;
				}
				else if (filter.leftIsDate())
				{
					final MyDate left = filter.getLeftDate();
					final String right = filter.rightColumn();
					final double retval = percentBelow(right, left, tx, tree);
					return retval;
				}
				else
				{
					// string
					final String left = filter.getLeftString();
					final String right = filter.rightColumn();
					final double retval = percentBelow(right, left, tx, tree);
					return retval;
				}
			}
		}

		if (op.equals("LI"))
		{
			return 0.05;
		}

		if (op.equals("NL"))
		{
			return 0.95;
		}

		throw new Exception("Unknown operator in likelihood()");
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

	public void populateIndex(String schema, String index, String table, Transaction tx, HashMap<String, Integer> cols2Pos) throws Exception
	{
		String iFn = schema + "." + index + ".indx";
		String tFn = schema + "." + table + ".tbl";
		ArrayList<Integer> nodes = getNodesForTable(schema, table, tx);
		ArrayList<Integer> devices = MetaData.getDevicesForTable(schema, table, tx);
		ArrayList<Object> rs = getKeysCache.get(schema + "." + index);
		if (rs == null)
		{
			rs = PlanCacheManager.getKeys().setParms(schema, index).execute(tx);
			getKeysCache.put(schema + "." + index, rs);
		}
		// tabname, colname
		ArrayList<String> keys = new ArrayList<String>();
		for (Object o : rs)
		{
			if (!(o instanceof DataEndMarker))
			{
				ArrayList<Object> row = (ArrayList<Object>)o;
				keys.add(row.get(0) + "." + row.get(1));
			}
		}

		rs = getTypesCache.get(schema + "." + index);
		if (rs == null)
		{
			rs = PlanCacheManager.getTypes().setParms(schema, index).execute(tx);
			getTypesCache.put(schema + "." + index, rs);
		}
		ArrayList<String> types = new ArrayList<String>();
		for (Object o : rs)
		{
			if (!(o instanceof DataEndMarker))
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
		}

		rs = getOrdersCache.get(schema + "." + index);
		if (rs == null)
		{
			rs = PlanCacheManager.getOrders().setParms(schema, index).execute(tx);
			getOrdersCache.put(schema + "." + index, rs);
		}
		ArrayList<Boolean> orders = new ArrayList<Boolean>();
		for (Object o : rs)
		{
			if (!(o instanceof DataEndMarker))
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
		}

		ArrayList<Integer> poses = new ArrayList<Integer>();
		for (String col : keys)
		{
			poses.add(cols2Pos.get(col));
		}

		ArrayList<Object> tree = makeTree(nodes);
		ArrayList<Socket> sockets = new ArrayList<Socket>();
		int max = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("max_neighbor_nodes"));

		TreeMap<Integer, String> pos2Col = cols2PosFlip(cols2Pos);
		HashMap<String, String> cols2Types = getCols2TypesForTable(schema, table, tx);
		int type = getTypeForTable(schema, table, tx);

		for (Object o : tree)
		{
			ArrayList<Object> list;
			if (o instanceof Integer)
			{
				list = new ArrayList<Object>(1);
				list.add(o);
			}
			else
			{
				list = (ArrayList<Object>)o;
			}

			Object obj = list.get(0);
			while (obj instanceof ArrayList)
			{
				obj = ((ArrayList)obj).get(0);
			}

			Socket sock;
			String hostname = new MetaData().getHostNameForNode((Integer)obj, tx);
			// sock = new Socket(hostname,
			// Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
			sock = new Socket();
			sock.setReceiveBufferSize(4194304);
			sock.setSendBufferSize(4194304);
			sock.connect(new InetSocketAddress(hostname, Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number"))));
			OutputStream out = sock.getOutputStream();
			byte[] outMsg = "POPINDEX        ".getBytes(StandardCharsets.UTF_8);
			outMsg[8] = 0;
			outMsg[9] = 0;
			outMsg[10] = 0;
			outMsg[11] = 0;
			outMsg[12] = 0;
			outMsg[13] = 0;
			outMsg[14] = 0;
			outMsg[15] = 0;
			out.write(outMsg);
			out.write(longToBytes(tx.number()));
			out.write(stringToBytes(iFn));
			out.write(stringToBytes(tFn));
			out.write(intToBytes(type));
			ObjectOutputStream objOut = new ObjectOutputStream(out);
			objOut.writeObject(devices);
			objOut.writeObject(keys);
			objOut.writeObject(types);
			objOut.writeObject(orders);
			objOut.writeObject(poses);
			objOut.writeObject(pos2Col);
			objOut.writeObject(cols2Types);
			objOut.writeObject(convertToHosts(list, tx));
			objOut.flush();
			out.flush();
			sockets.add(sock);

			if (sockets.size() >= max)
			{
				sock = sockets.get(0);
				out = sock.getOutputStream();
				getConfirmation(sock);
				objOut.close();
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

	public void runstats(String schema, String table, Transaction tx) throws Exception
	{
		Transaction tTx = new Transaction(Transaction.ISOLATION_UR);
		int tableID = PlanCacheManager.getTableID().setParms(schema, table).execute(tTx);
		TableStatsThread tThread = new TableStatsThread(schema, table, tableID, tTx);
		TreeMap<Integer, String> pos2Col = getPos2ColForTable(schema, table, tTx);
		ArrayList<RunstatsThread> threads = new ArrayList<RunstatsThread>();
		ArrayList<Transaction> cTxs = new ArrayList<Transaction>();

		tThread.start();
		tThread.join();
		boolean allOK = true;
		if (!tThread.getOK())
		{
			allOK = false;
		}

		ArrayList<Transaction> cdTxs = new ArrayList<Transaction>();
		// if (allOK)
		// {
		int i = 0;
		final int size = pos2Col.size();
		while (i < size)
		{
			Transaction ctx = new Transaction(Transaction.ISOLATION_UR);
			cTxs.add(ctx);
			i++;
		}

		long card = PlanCacheManager.getTableCard().setParms(schema, table).execute(cTxs.get(0));

		// indexes
		ArrayList<Transaction> iTxs = new ArrayList<Transaction>();
		ArrayList<Object> rs = PlanCacheManager.getIndexIDsForTable().setParms(schema, table).execute(tTx);
		HashMap<String, Integer> colToIndex = new HashMap<String, Integer>();
		int iThreads = 0;
		for (Object o : rs)
		{
			if (o instanceof DataEndMarker)
			{
				continue;
			}
			ArrayList<Object> row = (ArrayList<Object>)o;
			int indexID = (Integer)row.get(0);
			ArrayList<Object> rs2 = PlanCacheManager.getKeysByID().setParms(tableID, indexID).execute(tTx);
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
			threads.add(new IndexStatsThread(schema, table, keys, tableID, indexID, itx, card));
			if (keys.size() == 0)
			{
				colToIndex.put(keys.get(0), indexID);
			}
			iThreads++;
			iTxs.add(itx);
		}

		i = 0;
		while (i < size)
		{
			Integer indexID = colToIndex.get(pos2Col.get(i));
			if (indexID != null)
			{
				threads.add(new ColStatsThread(schema, table, pos2Col.get(i), tableID, i, cTxs.get(i), card, indexID));
			}
			else
			{
				threads.add(new ColStatsThread(schema, table, pos2Col.get(i), tableID, i, cTxs.get(i), card, -1));
			}
			i++;
		}

		i = 0;
		int j = iThreads;
		while (i < size)
		{
			Transaction ctx = new Transaction(Transaction.ISOLATION_UR);
			cdTxs.add(ctx);
			String col = pos2Col.get(i);
			if (col.contains("."))
			{
				col = col.substring(col.indexOf('.') + 1);
			}
			threads.add(j, new ColDistThread(schema, table, col, tableID, i, card, ctx));
			i++;
			j += 2;
		}

		ArrayList<RunstatsThread> submitted = new ArrayList<RunstatsThread>();
		HRDBMSWorker.logger.debug("THREADS:" + threads);
		for (RunstatsThread thread : threads)
		{
			thread.start();
			submitted.add(thread);
			if (submitted.size() == 8)
			{
				while (true)
				{
					RunstatsThread thread2 = null;
					int x = 0;
					for (RunstatsThread thread3 : submitted)
					{
						if (thread3.isDone())
						{
							thread2 = thread3;
							break;
						}

						x++;
					}

					if (thread2 != null)
					{
						thread2.join();
						if (!thread2.getOK())
						{
							allOK = false;
						}

						submitted.remove(x);
						break;
					}
					else
					{
						Thread.sleep(100);
					}
				}
			}
		}

		for (RunstatsThread thread : submitted)
		{
			thread.join();
			if (!thread.getOK())
			{
				allOK = false;
			}
		}
		// }

		getPartitioningCache.clear();
		getIndexesCache.clear();
		getKeysCache.clear();
		getTypesCache.clear();
		getOrdersCache.clear();
		getIndexColsForTableCache.clear();
		getLengthCache.clear();
		getTableIDCache.clear();
		getColCardCache.clear();
		getIndexIDsForTableCache.clear();
		getCols2PosForTableCache.clear();
		getCols2TypesCache.clear();
		getUniqueCache.clear();
		getTableCardCache.clear();
		getColTypeCache.clear();
		getDistCache.clear();

		if (allOK)
		{
			PlanCacheManager.invalidate();
			return;
		}
		else
		{
			throw new Exception("An error occurred during runstats processing");
		}
	}

	public boolean verifyColExistence(String schema, String table, String col, Transaction tx) throws Exception
	{
		HashMap<String, Integer> cols2Pos = getCols2PosForTable(schema, table, tx);
		return cols2Pos.containsKey(table + "." + col);
	}

	public boolean verifyInsert(String schema, String table, Operator op, Transaction tx) throws Exception
	{
		TreeMap<Integer, String> catalogPos2Col = getPos2ColForTable(schema, table, tx);
		HashMap<String, String> catalogCols2Types = new MetaData().getCols2TypesForTable(schema, table, tx);
		HashMap<Integer, String> catalogPos2Types = new HashMap<Integer, String>();
		int i = 0;
		final int s1 = catalogPos2Col.size();
		while (i < s1)
		{
			catalogPos2Types.put(i, catalogCols2Types.get(catalogPos2Col.get(i)));
			i++;
		}

		HashMap<Integer, String> opPos2Types = new HashMap<Integer, String>();
		i = 0;
		final int s2 = op.getPos2Col().size();
		while (i < s2)
		{
			opPos2Types.put(i, op.getCols2Types().get(op.getPos2Col().get(i)));
			i++;
		}

		if (catalogPos2Types.size() != opPos2Types.size())
		{
			return false;
		}

		i = 0;
		final int s3 = opPos2Types.size();
		while (i < s3)
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

	public boolean verifyTableExistence(String schema, String name, Transaction tx) throws Exception
	{
		Object o = PlanCacheManager.getVerifyTableExist().setParms(schema, name).execute(tx);
		if (o instanceof DataEndMarker)
		{
			return false;
		}

		return true;
	}

	public boolean verifyUpdate(String schema, String tbl, ArrayList<Column> cols, ArrayList<String> buildList, Operator op, Transaction tx) throws Exception
	{
		// verify that all columns are 1 part - parseException
		for (Column col : cols)
		{
			if (col.getTable() != null)
			{
				throw new ParseException("Two part column names are not allowed in a SET clause");
			}
		}
		// get data types for cols on this table
		// make sure that selecting the buildList cols from op in that order
		// satisfies updates for cols on this table
		TreeMap<Integer, String> catalogPos2Col = getPos2ColForTable(schema, tbl, tx);
		HashMap<String, String> catalogCols2Types = new MetaData().getCols2TypesForTable(schema, tbl, tx);
		HashMap<Integer, String> catalogPos2Types = new HashMap<Integer, String>();
		int i = 0;
		final int s1 = catalogPos2Col.size();
		while (i < s1)
		{
			catalogPos2Types.put(i, catalogCols2Types.get(catalogPos2Col.get(i)));
			i++;
		}

		HashMap<Integer, String> opPos2Types = new HashMap<Integer, String>();
		i = 0;
		final int s2 = catalogPos2Col.size();
		while (i < s2)
		{
			String col = catalogPos2Col.get(i);
			boolean contains = false;
			int index = -1;
			String col1 = col.substring(col.indexOf('.') + 1);
			int j = 0;
			for (Column col2 : cols)
			{
				if (col2.getColumn().equals(col1))
				{
					contains = true;
					index = j;
				}

				j++;
			}

			if (!contains)
			{
				opPos2Types.put(i, catalogPos2Types.get(i));
			}
			else
			{
				String toGet = buildList.get(index);
				String type = op.getCols2Types().get(toGet);
				if (type != null)
				{
					opPos2Types.put(i, type);
				}
				else
				{
					if (toGet.contains("."))
					{
						toGet = toGet.substring(toGet.indexOf('.') + 1);
					}

					for (Map.Entry entry : op.getCols2Types().entrySet())
					{
						String temp = (String)entry.getKey();
						if (temp.contains("."))
						{
							temp = temp.substring(temp.indexOf('.') + 1);
						}

						if (temp.equals(toGet))
						{
							opPos2Types.put(i, (String)entry.getValue());
							break;
						}
					}
				}
			}

			i++;
		}

		if (catalogPos2Types.size() != opPos2Types.size())
		{
			return false;
		}

		i = 0;
		final int s3 = opPos2Types.size();
		while (i < s3)
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

	private boolean aliasIs(TableScanOperator t, String q)
	{
		return t.getAlias().equals(q);
	}

	private long bigger(long x, long y)
	{
		if (x >= y)
		{
			if (x <= 0)
			{
				return 1;
			}
			else
			{
				return x;
			}
		}

		if (y <= 0)
		{
			return 1;
		}

		return y;
	}

	private void buildIndex(String schema, String index, String table, int numCols, boolean unique, Transaction tx) throws Exception
	{
		String fn = schema + "." + index + ".indx";
		ArrayList<Integer> nodes = getNodesForTable(schema, table, tx);
		ArrayList<Integer> devices = MetaData.getDevicesForTable(schema, table, tx);
		ArrayList<Object> tree = makeTree(nodes);
		ArrayList<Socket> sockets = new ArrayList<Socket>();
		int max = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("max_neighbor_nodes"));

		for (Object o : tree)
		{
			ArrayList<Object> list;
			if (o instanceof Integer)
			{
				list = new ArrayList<Object>(1);
				list.add(o);
			}
			else
			{
				list = (ArrayList<Object>)o;
			}

			Object obj = list.get(0);
			while (obj instanceof ArrayList)
			{
				obj = ((ArrayList)obj).get(0);
			}

			Socket sock;
			String hostname = new MetaData().getHostNameForNode((Integer)obj, tx);
			// sock = new Socket(hostname,
			// Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
			sock = new Socket();
			sock.setReceiveBufferSize(4194304);
			sock.setSendBufferSize(4194304);
			sock.connect(new InetSocketAddress(hostname, Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number"))));
			OutputStream out = sock.getOutputStream();
			byte[] outMsg = "NEWINDEX        ".getBytes(StandardCharsets.UTF_8);
			outMsg[8] = 0;
			outMsg[9] = 0;
			outMsg[10] = 0;
			outMsg[11] = 0;
			outMsg[12] = 0;
			outMsg[13] = 0;
			outMsg[14] = 0;
			outMsg[15] = 0;
			out.write(outMsg);
			out.write(longToBytes(tx.number()));
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
			objOut.writeObject(convertToHosts(list, tx));
			objOut.flush();
			out.flush();
			sockets.add(sock);

			if (sockets.size() >= max)
			{
				sock = sockets.get(0);
				out = sock.getOutputStream();
				getConfirmation(sock);
				objOut.close();
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

	private void buildTable(String schema, String table, int numCols, Transaction tx, int tType, ArrayList<ColDef> defs, ArrayList<Integer> colOrder, ArrayList<Integer> organization) throws Exception
	{
		if (tType != 0 && colOrder == null)
		{
			colOrder = new ArrayList<Integer>();
			int i = 1;
			while (i <= numCols)
			{
				colOrder.add(i++);
			}
		}

		if (tType != 0 && organization == null)
		{
			organization = new ArrayList<Integer>();
		}

		String fn = schema + "." + table + ".tbl";
		ArrayList<Integer> nodes = getNodesForTable(schema, table, tx);
		ArrayList<Integer> devices = MetaData.getDevicesForTable(schema, table, tx);
		ArrayList<Object> tree = makeTree(nodes);
		ArrayList<Socket> sockets = new ArrayList<Socket>();
		int max = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("max_neighbor_nodes"));

		for (Object o : tree)
		{
			ArrayList<Object> list;
			if (o instanceof Integer)
			{
				list = new ArrayList<Object>(1);
				list.add(o);
			}
			else
			{
				list = (ArrayList<Object>)o;
			}

			Object obj = list.get(0);
			while (obj instanceof ArrayList)
			{
				obj = ((ArrayList)obj).get(0);
			}

			Socket sock;
			String hostname = new MetaData().getHostNameForNode((Integer)obj, tx);
			// sock = new Socket(hostname,
			// Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
			sock = new Socket();
			sock.setReceiveBufferSize(4194304);
			sock.setSendBufferSize(4194304);
			sock.connect(new InetSocketAddress(hostname, Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number"))));
			OutputStream out = sock.getOutputStream();
			byte[] outMsg = "NEWTABLE        ".getBytes(StandardCharsets.UTF_8);
			outMsg[8] = 0;
			outMsg[9] = 0;
			outMsg[10] = 0;
			outMsg[11] = 0;
			outMsg[12] = 0;
			outMsg[13] = 0;
			outMsg[14] = 0;
			outMsg[15] = 0;
			out.write(outMsg);
			out.write(longToBytes(tx.number()));
			out.write(intToBytes(numCols));
			out.write(stringToBytes(fn));
			out.write(intToBytes(tType));
			ObjectOutputStream objOut = new ObjectOutputStream(out);
			objOut.writeObject(devices);
			objOut.writeObject(convertToHosts(list, tx));
			objOut.writeObject(defs);
			if (tType != 0)
			{
				objOut.writeObject(colOrder);
				objOut.writeObject(organization);
				HRDBMSWorker.logger.debug("Sending message to create table with organization: " + organization);
			}
			objOut.flush();
			out.flush();
			sockets.add(sock);

			if (sockets.size() >= max)
			{
				sock = sockets.get(0);
				out = sock.getOutputStream();
				getConfirmation(sock);
				objOut.close();
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

	private boolean containsCol(TableScanOperator op, String c)
	{
		Set<String> set = op.getTableCols2Pos().keySet();
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

	private ArrayList<Object> convertRangeStringToObject(String set, String schema, String table, String rangeCol, Transaction tx) throws Exception
	{
		String type = getColTypeCache.get(schema + "." + table + "." + rangeCol);
		if (type == null)
		{
			type = PlanCacheManager.getColType().setParms(schema, table, rangeCol).execute(tx);
			getColTypeCache.put(schema + "." + table + "." + rangeCol, type);
		}
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
				retval.add(Double.parseDouble(token));
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

	private ArrayList<Object> convertRangeStringToObject(String set, String schema, String table, String rangeCol, Transaction tx, String type) throws Exception
	{
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
				retval.add(Double.parseDouble(token));
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

	private void defaultDate(ArrayList<MyDate> retval)
	{
		retval.add(new MyDate(1960, 1, 1));
		retval.add(new MyDate(2000, 1, 1));
		retval.add(new MyDate(2020, 1, 1));
		retval.add(new MyDate(2040, 1, 1));
		retval.add(new MyDate(2060, 1, 1));
	}

	private void defaultDouble(ArrayList<Double> retval)
	{
		retval.add(Double.MIN_VALUE);
		retval.add(Double.MIN_VALUE / 2);
		retval.add(0D);
		retval.add(Double.MAX_VALUE / 2);
		retval.add(Double.MAX_VALUE);
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

	private ArrayList<MyDate> getDateQuartiles(String col, Transaction tx, Operator tree) throws Exception
	{
		// SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		ArrayList<MyDate> retval = new ArrayList<MyDate>(5);
		String c = col.substring(col.indexOf('.') + 1);
		String ST = getTableForCol(col, tree);
		if (ST == null)
		{
			defaultDate(retval);
			return retval;
		}
		String schema = ST.substring(0, ST.indexOf('.'));
		String table = ST.substring(ST.indexOf('.') + 1);
		Object o = getDistCache.get(schema + "." + table + "." + c);
		if (o == null)
		{
			o = PlanCacheManager.getDist().setParms(schema, table, c).execute(tx);
			getDistCache.put(schema + "." + table + "." + c, o);
		}
		if (o instanceof DataEndMarker)
		{
			defaultDate(retval);
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

	private ArrayList<Double> getDoubleQuartiles(String col, Transaction tx, Operator tree) throws Exception
	{
		ArrayList<Double> retval = new ArrayList<Double>(5);
		String c = col.substring(col.indexOf('.') + 1);
		String ST = getTableForCol(col, tree);
		if (ST == null)
		{
			defaultDouble(retval);
			return retval;
		}
		String schema = ST.substring(0, ST.indexOf('.'));
		String table = ST.substring(ST.indexOf('.') + 1);
		Object o = getDistCache.get(schema + "." + table + "." + c);
		if (o == null)
		{
			o = PlanCacheManager.getDist().setParms(schema, table, c).execute(tx);
			getDistCache.put(schema + "." + table + "." + c, o);
		}
		if (o instanceof DataEndMarker)
		{
			defaultDouble(retval);
		}
		else
		{
			for (Object obj : (ArrayList<Object>)o)
			{
				String val = (String)obj;
				retval.add(Double.parseDouble(val));
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

	private String getQual(String col)
	{
		return col.substring(0, col.indexOf('.'));
	}

	private ArrayList<String> getStringQuartiles(String col, Transaction tx, Operator tree) throws Exception
	{
		ArrayList<String> retval = new ArrayList<String>(5);
		String c = col.substring(col.indexOf('.') + 1);
		String ST = getTableForCol(col, tree);
		if (ST == null)
		{
			retval.add("A");
			retval.add("N");
			retval.add("Z");
			retval.add("n");
			retval.add("z");
			return retval;
		}
		String schema = ST.substring(0, ST.indexOf('.'));
		String table = ST.substring(ST.indexOf('.') + 1);
		Object o = getDistCache.get(schema + "." + table + "." + c);
		if (o == null)
		{
			o = PlanCacheManager.getDist().setParms(schema, table, c).execute(tx);
			getDistCache.put(schema + "." + table + "." + c, o);
		}
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

	private void getTables(Operator op, HashSet<Operator> touched, ArrayList<TableScanOperator> tables)
	{
		if (touched.contains(op))
		{
			return;
		}

		touched.add(op);
		if (op instanceof TableScanOperator)
		{
			tables.add((TableScanOperator)op);
			return;
		}
		else
		{
			for (Operator o : op.children())
			{
				getTables(o, touched, tables);
			}

			return;
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
			return 0.75 + 0.25 * ((stringToDouble(quartiles.get(1)) - stringToDouble(val)) / (stringToDouble(quartiles.get(1)) - stringToDouble(quartiles.get(0))));
		}

		if (quartiles.get(2).compareTo(val) > -1)
		{
			return 0.5 + 0.25 * ((stringToDouble(quartiles.get(2)) - stringToDouble(val)) / (stringToDouble(quartiles.get(2)) - stringToDouble(quartiles.get(1))));
		}

		if (quartiles.get(3).compareTo(val) > -1)
		{
			return 0.25 + 0.25 * ((stringToDouble(quartiles.get(3)) - stringToDouble(val)) / (stringToDouble(quartiles.get(3)) - stringToDouble(quartiles.get(2))));
		}

		if (quartiles.get(4).compareTo(val) > -1)
		{
			return 0.25 * ((stringToDouble(quartiles.get(4)) - stringToDouble(val)) / (stringToDouble(quartiles.get(4)) - stringToDouble(quartiles.get(3))));
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
			return 0.75 + 0.25 * ((stringToDouble(val) - stringToDouble(quartiles.get(3))) / (stringToDouble(quartiles.get(4)) - stringToDouble(quartiles.get(3))));
		}

		if (quartiles.get(2).compareTo(val) < 1)
		{
			return 0.5 + 0.25 * ((stringToDouble(val) - stringToDouble(quartiles.get(2))) / (stringToDouble(quartiles.get(3)) - stringToDouble(quartiles.get(2))));
		}

		if (quartiles.get(1).compareTo(val) < 1)
		{
			return 0.25 + 0.25 * ((stringToDouble(val) - stringToDouble(quartiles.get(1))) / (stringToDouble(quartiles.get(2)) - stringToDouble(quartiles.get(1))));
		}

		if (quartiles.get(0).compareTo(val) < 1)
		{
			return 0.25 * ((stringToDouble(val) - stringToDouble(quartiles.get(0))) / (stringToDouble(quartiles.get(1)) - stringToDouble(quartiles.get(0))));
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

	private long smaller(long x, long y)
	{
		if (x <= y)
		{
			if (x <= 0)
			{
				return 1;
			}
			else
			{
				return x;
			}
		}

		if (y <= 0)
		{
			return 1;
		}

		return y;
	}

	private double stringToDouble(String val)
	{
		int i = 0;
		double retval = 0;
		while (i < 16 && i < val.length())
		{
			final int point = val.charAt(i);
			retval += (point * Math.pow(2.0, 240 - (i << 4)));
			i++;
		}

		return retval;
	}

	private boolean tableIs(TableScanOperator t, String q)
	{
		return t.getTable().equals(q);
	}

	public final class PartitionMetaData implements Serializable
	{
		private static final int NODEGROUP_NONE = -3;
		private static final int NODE_ANY = -2;
		public static final int NODE_ALL = -1;
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
		private final Transaction tx;
		private final String ngExp, nExp, dExp;

		public PartitionMetaData(String schema, String table, String ngExp, String nExp, String dExp, Transaction tx, HashMap<String, String> cols2Types) throws Exception
		{
			this.schema = schema;
			this.table = table;
			this.tx = tx;
			this.ngExp = ngExp;
			this.nExp = nExp;
			this.dExp = dExp;
			setNGData2(ngExp, cols2Types);
			setNData2(nExp, cols2Types);
			setDData2(dExp, cols2Types);
		}

		public PartitionMetaData(String schema, String table, Transaction tx) throws Exception
		{
			this.schema = schema;
			this.table = table;
			this.tx = tx;
			ArrayList<Object> row = getPartitioningCache.get(schema + "." + table);
			if (row == null)
			{
				row = PlanCacheManager.getPartitioning().setParms(schema, table).execute(tx);
				getPartitioningCache.put(schema + "." + table, row);
			}
			ngExp = (String)row.get(0);
			nExp = (String)row.get(1);
			dExp = (String)row.get(2);
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

		public String getSchema()
		{
			return schema;
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

		public String getTable()
		{
			return table;
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

		private void otherNG(String exp) throws Exception
		{
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
			else if (type.equals("RANGE"))
			{
				nodeGroupRangeCol = tokens.nextToken();
				set = tokens.nextToken().substring(1);
				set = set.substring(0, set.length() - 1);
				nodeGroupRange = convertRangeStringToObject(set, schema, table, nodeGroupRangeCol, tx);
			}
			else
			{
				throw new Exception("Node group type was not range or hash");
			}
		}

		private void setDData(String exp) throws Exception
		{
			final FastStringTokenizer tokens = new FastStringTokenizer(exp, ",", false);
			final String first = tokens.nextToken();

			if (first.equals("ALL"))
			{
				deviceSet = new ArrayList<Integer>(1);
				deviceSet.add(DEVICE_ALL);
				numDevices = MetaData.getNumDevices();
			}
			else
			{
				setDNotAll(first);
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
				setDNotHash(tokens);
			}
		}

		private void setDData2(String exp, HashMap<String, String> cols2Types) throws Exception
		{
			final FastStringTokenizer tokens = new FastStringTokenizer(exp, ",", false);
			final String first = tokens.nextToken();

			if (first.equals("ALL"))
			{
				deviceSet = new ArrayList<Integer>(1);
				deviceSet.add(DEVICE_ALL);
				numDevices = MetaData.getNumDevices();
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
					int device = Integer.parseInt(tokens2.nextToken());
					if (device >= MetaData.getNumDevices())
					{
						throw new Exception("Invalid device number: " + device);
					}
					deviceSet.add(device);
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
			else if (type.equals("RANGE"))
			{
				deviceRangeCol = tokens.nextToken();
				String set = tokens.nextToken().substring(1);
				set = set.substring(0, set.length() - 1);
				String type2 = cols2Types.get(deviceRangeCol);
				if (type2 == null)
				{
					throw new Exception("Device range column does not exist");
				}
				deviceRange = convertRangeStringToObject(set, schema, table, deviceRangeCol, tx, type2);
				if (deviceRange.size() != numDevices - 1)
				{
					throw new Exception("Wrong number of device ranges");
				}
			}
			else
			{
				throw new Exception("Device type is not hash or range");
			}
		}

		private void setDNotAll(String first)
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

		private void setDNotHash(FastStringTokenizer tokens) throws Exception
		{
			deviceRangeCol = tokens.nextToken();
			String set = tokens.nextToken().substring(1);
			set = set.substring(0, set.length() - 1);
			deviceRange = convertRangeStringToObject(set, schema, table, deviceRangeCol, tx);
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
				numNodes = MetaData.numWorkerNodes;
			}
			else
			{
				setNNotAA(exp, first);
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
				setNNotHash(tokens);
			}
		}

		private void setNData2(String exp, HashMap<String, String> cols2Types) throws Exception
		{
			final FastStringTokenizer tokens = new FastStringTokenizer(exp, ",", false);
			final String first = tokens.nextToken();

			if (first.equals("ANY"))
			{
				nodeSet = new ArrayList<Integer>(1);
				nodeSet.add(NODE_ANY);
				numNodes = 1;
				if (nodeGroupSet.get(0) != PartitionMetaData.NODEGROUP_NONE)
				{
					throw new Exception("Can't use nodegroups with a table using ANY node partitioning");
				}
				return;
			}

			if (first.equals("ALL"))
			{
				nodeSet = new ArrayList<Integer>(1);
				nodeSet.add(NODE_ALL);
				numNodes = MetaData.numWorkerNodes;
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
					int node = Integer.parseInt(tokens2.nextToken());
					if (node >= MetaData.numWorkerNodes)
					{
						throw new Exception("Invalid node number: " + node);
					}
					nodeSet.add(node);
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
					String col = tokens2.nextToken();
					if (!cols2Types.containsKey(col))
					{
						throw new Exception("Hash column " + col + " does not exist!");
					}
					nodeHash.add(col);
				}
			}
			else if (type.equals("RANGE"))
			{
				nodeRangeCol = tokens.nextToken();
				String type2 = cols2Types.get(nodeRangeCol);
				if (type2 == null)
				{
					throw new Exception("Node range column does not exist");
				}
				String set = tokens.nextToken().substring(1);
				set = set.substring(0, set.length() - 1);
				nodeRange = convertRangeStringToObject(set, schema, table, nodeRangeCol, tx, type2);
				if (nodeRange.size() != numNodes - 1)
				{
					throw new Exception("Wrong number of node ranges");
				}
			}
			else
			{
				throw new Exception("Node type is not hash or range");
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

			otherNG(exp);
		}

		private void setNGData2(String exp, HashMap<String, String> cols2Types) throws Exception
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
			int expectedNumNodesInGroup = -1;
			while (tokens2.hasMoreTokens())
			{
				String nodesInGroup = tokens2.nextToken();
				int nodeCount = 0;
				nodeGroupSet.add(setNum);
				numNodeGroups++;

				ArrayList<Integer> nodeListForGroup = new ArrayList<Integer>();
				FastStringTokenizer tokens3 = new FastStringTokenizer(nodesInGroup, "|", false);
				while (tokens3.hasMoreTokens())
				{
					String token = tokens3.nextToken();
					int node = Integer.parseInt(token);
					if (node >= MetaData.numWorkerNodes)
					{
						throw new Exception("Invalid node number: " + node);
					}
					nodeListForGroup.add(node);
					nodeCount++;
				}
				nodeGroupHashMap.put(setNum, nodeListForGroup);
				setNum++;
				if (expectedNumNodesInGroup == -1)
				{
					expectedNumNodesInGroup = nodeCount;
				}
				else if (expectedNumNodesInGroup != nodeCount)
				{
					throw new Exception("Expected " + expectedNumNodesInGroup + " nodes in node group but found " + nodeCount + " nodes");
				}
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
					String col = tokens2.nextToken();
					if (!cols2Types.containsKey(col))
					{
						throw new Exception("Hash column " + col + " does not exist");
					}
					nodeGroupHash.add(col);
				}
			}
			else if (type.equals("RANGE"))
			{
				nodeGroupRangeCol = tokens.nextToken();
				String type2 = cols2Types.get(nodeGroupRangeCol);
				if (type2 == null)
				{
					throw new Exception("Node group range column does not exist");
				}
				set = tokens.nextToken().substring(1);
				set = set.substring(0, set.length() - 1);
				nodeGroupRange = convertRangeStringToObject(set, schema, table, nodeGroupRangeCol, tx, type2);
				if (nodeGroupRange.size() != numNodeGroups - 1)
				{
					throw new Exception("Wrong number of node group ranges");
				}
			}
			else
			{
				throw new Exception("Node group type was not range or hash");
			}
		}

		private void setNNotAA(String exp, String first)
		{
			String set = first.substring(1);
			set = set.substring(0, set.length() - 1);
			final FastStringTokenizer tokens2 = new FastStringTokenizer(set, "|", false);
			nodeSet = new ArrayList<Integer>(tokens2.allTokens().length);
			numNodes = 0;
			while (tokens2.hasMoreTokens())
			{
				nodeSet.add(Integer.parseInt(tokens2.nextToken()));
				numNodes++;
			}
		}

		private void setNNotHash(FastStringTokenizer tokens) throws Exception
		{
			nodeRangeCol = tokens.nextToken();
			String set = tokens.nextToken().substring(1);
			set = set.substring(0, set.length() - 1);
			nodeRange = convertRangeStringToObject(set, schema, table, nodeRangeCol, tx);
		}
	}

	private class ColAndTree
	{
		private final String col;
		private final Operator tree;

		public ColAndTree(String col, Operator tree)
		{
			this.col = col;
			this.tree = tree;
		}

		@Override
		public boolean equals(Object r)
		{
			ColAndTree rhs = (ColAndTree)r;
			return col.equals(rhs.col) && tree == rhs.tree;
		}

		@Override
		public int hashCode()
		{
			int hash = 23;
			hash = hash * 31 + col.hashCode();
			hash = hash * 31 + tree.hashCode();
			return hash;
		}
	}

	private static class ColDistThread extends RunstatsThread
	{
		private final String schema;
		private final String table;
		private final String col;
		private final int tableID;
		private final int colID;
		private final long card;
		private Transaction tx;
		private boolean ok = true;
		private long p;
		private final long totalCard;

		public ColDistThread(String schema, String table, String col, int tableID, int colID, long card, Transaction tx)
		{
			this.schema = schema;
			this.table = table;
			this.col = col;
			this.tableID = tableID;
			this.colID = colID;
			this.card = card;
			this.tx = tx;
			this.totalCard = card;
		}

		@Override
		public boolean getOK()
		{
			return ok;
		}

		@Override
		public void run()
		{
			try
			{
				p = 100000000l / totalCard;
				if (p < 5)
				{
					p = 5;
				}

				tx.setIsolationLevel(Transaction.ISOLATION_UR);
				String type = PlanCacheManager.getColType().setParms(schema, table, col).execute(tx);
				String sql = "SELECT " + col + " FROM " + schema + "." + table + " ORDER BY " + col + " ASC";
				XAWorker worker = null;
				if (totalCard > 1000000)
				{
					worker = XAManager.executeQuery(sql, tx, null, p);
				}
				else
				{
					worker = XAManager.executeQuery(sql, tx, null);
				}
				worker.start();
				int x = 0;
				ArrayList<Object> cmd = new ArrayList<Object>();
				cmd.add("NEXT");
				cmd.add(100000);
				worker.in.put(cmd);
				Object o = worker.out.take();
				x++;
				if (o instanceof DataEndMarker)
				{
					throw new Exception("No data in table: " + sql);
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
				while (((totalCard <= 1000000 && i < (card >> 2)) || (totalCard > 1000000 && i < ((card >> 2) * (p / 100.0d)))) && !(o2 instanceof DataEndMarker))
				{
					o = o2;
					o2 = worker.out.take();
					x++;
					if (x == 100000)
					{
						cmd = new ArrayList<Object>();
						cmd.add("NEXT");
						cmd.add(100000);
						worker.in.put(cmd);
						x = 0;
					}
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

					cmd = new ArrayList<Object>(1);
					cmd.add("CLOSE");
					worker.in.put(cmd);
					int updateCount = -1;
					Random random = new Random();
					while (updateCount == -1)
					{
						Object obj = PlanCacheManager.getDist().setParms(schema, table, col).execute(tx);
						if (obj instanceof DataEndMarker)
						{
							sql = "INSERT INTO SYS.COLDIST VALUES(" + tableID + ", " + colID + ", '" + low + "', '" + q1 + "', '" + q2 + "', '" + q3 + "', '" + high + "')";
							worker = XAManager.executeAuthorizedUpdate(sql, tx);
							worker.start();
							worker.join();
							updateCount = worker.getUpdateCount();
							if (updateCount == -1)
							{
								XAManager.rollback(tx);
								tx = new Transaction(Transaction.ISOLATION_UR);
							}
							else
							{
								XAManager.commit(tx);
								return;
							}

							Thread.sleep(random.nextInt(60000));
						}
						else
						{
							sql = "UPDATE SYS.COLDIST SET (LOW, Q1, Q2, Q3, HIGH) = ('" + low + "', '" + q1 + "', '" + q2 + "', '" + q3 + "', '" + high + "') WHERE TABLEID = " + tableID + " AND COLID = " + colID;
							worker = XAManager.executeAuthorizedUpdate(sql, tx);
							worker.start();
							worker.join();
							updateCount = worker.getUpdateCount();
							if (updateCount == -1)
							{
								XAManager.rollback(tx);
								// tx = new
								// Transaction(Transaction.ISOLATION_UR);
								return;
							}
							else if (updateCount == 1)
							{
								XAManager.commit(tx);
								return;
							}
							else
							{
								XAManager.rollback(tx);
								HRDBMSWorker.logger.debug("The SQL statement: '" + sql + "' updated " + updateCount + " rows");
								ok = false;
								return;
							}

							// Thread.sleep(random.nextInt(60000));
						}
					}
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

				while (((totalCard <= 1000000 && i < (card >> 1)) || (totalCard > 1000000 && i < ((card >> 1) * (p / 100.0d)))) && !(o2 instanceof DataEndMarker))
				{
					o = o2;
					o2 = worker.out.take();
					x++;
					if (x == 100000)
					{
						cmd = new ArrayList<Object>();
						cmd.add("NEXT");
						cmd.add(100000);
						worker.in.put(cmd);
						x = 0;
					}
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

					cmd = new ArrayList<Object>(1);
					cmd.add("CLOSE");
					worker.in.put(cmd);
					int updateCount = -1;
					Random random = new Random();
					while (updateCount == -1)
					{
						Object obj = PlanCacheManager.getDist().setParms(schema, table, col).execute(tx);
						if (obj instanceof DataEndMarker)
						{
							sql = "INSERT INTO SYS.COLDIST VALUES(" + tableID + ", " + colID + ", '" + low + "', '" + q1 + "', '" + q2 + "', '" + q3 + "', '" + high + "')";
							worker = XAManager.executeAuthorizedUpdate(sql, tx);
							worker.start();
							worker.join();
							updateCount = worker.getUpdateCount();
							if (updateCount == -1)
							{
								XAManager.rollback(tx);
								tx = new Transaction(Transaction.ISOLATION_UR);
							}
							else
							{
								XAManager.commit(tx);
								return;
							}

							Thread.sleep(random.nextInt(60000));
						}
						else
						{
							sql = "UPDATE SYS.COLDIST SET (LOW, Q1, Q2, Q3, HIGH) = ('" + low + "', '" + q1 + "', '" + q2 + "', '" + q3 + "', '" + high + "') WHERE TABLEID = " + tableID + " AND COLID = " + colID;
							worker = XAManager.executeAuthorizedUpdate(sql, tx);
							worker.start();
							worker.join();
							updateCount = worker.getUpdateCount();
							if (updateCount == -1)
							{
								XAManager.rollback(tx);
								// tx = new
								// Transaction(Transaction.ISOLATION_UR);
								return;
							}
							else if (updateCount == 1)
							{
								XAManager.commit(tx);
								return;
							}
							else
							{
								XAManager.rollback(tx);
								HRDBMSWorker.logger.debug("The SQL statement: '" + sql + "' updated " + updateCount + " rows");
								ok = false;
								return;
							}

							// Thread.sleep(random.nextInt(60000));
						}
					}
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

				while (((totalCard <= 1000000 && i < (card * 3 / 4)) || (totalCard > 1000000 && i < ((card * 3 / 4) * (p / 100.0d)))) && !(o2 instanceof DataEndMarker))
				{
					o = o2;
					o2 = worker.out.take();
					x++;
					if (x == 100000)
					{
						cmd = new ArrayList<Object>();
						cmd.add("NEXT");
						cmd.add(100000);
						worker.in.put(cmd);
						x = 0;
					}
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

					cmd = new ArrayList<Object>(1);
					cmd.add("CLOSE");
					worker.in.put(cmd);
					int updateCount = -1;
					Random random = new Random();
					while (updateCount == -1)
					{
						Object obj = PlanCacheManager.getDist().setParms(schema, table, col).execute(tx);
						if (obj instanceof DataEndMarker)
						{
							sql = "INSERT INTO SYS.COLDIST VALUES(" + tableID + ", " + colID + ", '" + low + "', '" + q1 + "', '" + q2 + "', '" + q3 + "', '" + high + "')";
							worker = XAManager.executeAuthorizedUpdate(sql, tx);
							worker.start();
							worker.join();
							updateCount = worker.getUpdateCount();
							if (updateCount == -1)
							{
								XAManager.rollback(tx);
								tx = new Transaction(Transaction.ISOLATION_UR);
							}
							else
							{
								XAManager.commit(tx);
								return;
							}

							Thread.sleep(random.nextInt(60000));
						}
						else
						{
							sql = "UPDATE SYS.COLDIST SET (LOW, Q1, Q2, Q3, HIGH) = ('" + low + "', '" + q1 + "', '" + q2 + "', '" + q3 + "', '" + high + "') WHERE TABLEID = " + tableID + " AND COLID = " + colID;
							worker = XAManager.executeAuthorizedUpdate(sql, tx);
							worker.start();
							worker.join();
							updateCount = worker.getUpdateCount();
							if (updateCount == -1)
							{
								XAManager.rollback(tx);
								// tx = new
								// Transaction(Transaction.ISOLATION_UR);
								return;
							}
							else if (updateCount == 1)
							{
								XAManager.commit(tx);
								return;
							}
							else
							{
								XAManager.rollback(tx);
								HRDBMSWorker.logger.debug("The SQL statement: '" + sql + "' updated " + updateCount + " rows");
								ok = false;
								return;
							}

							// Thread.sleep(random.nextInt(60000));
						}
					}
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
					x++;
					if (x == 100000)
					{
						cmd = new ArrayList<Object>();
						cmd.add("NEXT");
						cmd.add(100000);
						worker.in.put(cmd);
						x = 0;
					}
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

				cmd = new ArrayList<Object>(1);
				cmd.add("CLOSE");
				worker.in.put(cmd);
				int updateCount = -1;
				Random random = new Random();
				while (updateCount == -1)
				{
					Object obj = PlanCacheManager.getDist().setParms(schema, table, col).execute(tx);
					if (obj instanceof DataEndMarker)
					{
						sql = "INSERT INTO SYS.COLDIST VALUES(" + tableID + ", " + colID + ", '" + low + "', '" + q1 + "', '" + q2 + "', '" + q3 + "', '" + high + "')";
						worker = XAManager.executeAuthorizedUpdate(sql, tx);
						worker.start();
						worker.join();
						updateCount = worker.getUpdateCount();
						if (updateCount == -1)
						{
							XAManager.rollback(tx);
							tx = new Transaction(Transaction.ISOLATION_UR);
						}
						else
						{
							XAManager.commit(tx);
							return;
						}

						Thread.sleep(random.nextInt(60000));
					}
					else
					{
						sql = "UPDATE SYS.COLDIST SET (LOW, Q1, Q2, Q3, HIGH) = ('" + low + "', '" + q1 + "', '" + q2 + "', '" + q3 + "', '" + high + "') WHERE TABLEID = " + tableID + " AND COLID = " + colID;
						worker = XAManager.executeAuthorizedUpdate(sql, tx);
						worker.start();
						worker.join();
						updateCount = worker.getUpdateCount();
						if (updateCount == -1)
						{
							XAManager.rollback(tx);
							// tx = new Transaction(Transaction.ISOLATION_UR);
							return;
						}
						else if (updateCount == 1)
						{
							XAManager.commit(tx);
							return;
						}
						else
						{
							XAManager.rollback(tx);
							HRDBMSWorker.logger.debug("The SQL statement: '" + sql + "' updated " + updateCount + " rows");
							ok = false;
							return;
						}

						// Thread.sleep(random.nextInt(60000));
					}
				}
			}
			catch (Exception e)
			{
				ok = false;
				HRDBMSWorker.logger.debug("", e);
				try
				{
					XAManager.rollback(tx);
				}
				catch (Exception f)
				{
					// BLACKLIST?
					HRDBMSWorker.logger.debug("", f);
				}
			}
		}
	}

	private static class ColStatsThread extends RunstatsThread
	{
		private final String schema;
		private final String table;
		private final String col;
		private final int tableID;
		private final int colID;
		private Transaction tx;
		private boolean ok = true;
		private final long totalCard;
		private long p;
		private final int indexID;

		public ColStatsThread(String schema, String table, String col, int tableID, int colID, Transaction tx, long totalCard, int indexID)
		{
			this.schema = schema;
			this.table = table;
			this.col = col;
			this.tableID = tableID;
			this.colID = colID;
			this.tx = tx;
			this.totalCard = totalCard;
			this.indexID = indexID;
		}

		@Override
		public boolean getOK()
		{
			return ok;
		}

		@Override
		public void run()
		{
			try
			{
				String sql;
				String unique = "N";

				if (indexID != -1)
				{
					sql = "SELECT * FROM SYS.INDEXES WHERE TABLEID = " + tableID + " AND INDEXID = " + indexID;
					XAWorker worker = null;
					worker = XAManager.executeQuery(sql, tx, null);
					worker.start();
					ArrayList<Object> cmd = new ArrayList<Object>(2);
					cmd.add("NEXT");
					cmd.add(1);
					worker.in.put(cmd);

					Object obj2 = null;
					while (true)
					{
						try
						{
							obj2 = worker.out.take();
							break;
						}
						catch (InterruptedException e)
						{
						}
					}

					if (obj2 instanceof Exception)
					{
						cmd = new ArrayList<Object>(1);
						cmd.add("CLOSE");
						worker.in.put(cmd);
						ok = false;
						return;
					}

					cmd = new ArrayList<Object>(1);
					cmd.add("CLOSE");
					worker.in.put(cmd);

					if (obj2 instanceof DataEndMarker)
					{
						ok = false;
						return;
					}

					unique = (String)((ArrayList<Object>)obj2).get(3);
				}

				long card = -1;
				if (unique.equals("Y"))
				{
					card = totalCard;
				}
				else
				{
					p = 100000000l / totalCard;
					if (p < 5)
					{
						p = 5;
					}

					tx.setIsolationLevel(Transaction.ISOLATION_UR);
					sql = "SELECT COUNT(DISTINCT " + col + ") FROM " + schema + "." + table;
					XAWorker worker = null;
					if (totalCard > 1000000)
					{
						worker = XAManager.executeQuery(sql, tx, null, p); // sample
					}
					else
					{
						worker = XAManager.executeQuery(sql, tx, null);
					}
					worker.start();
					ArrayList<Object> cmd = new ArrayList<Object>(2);
					cmd.add("NEXT");
					cmd.add(1);
					worker.in.put(cmd);

					Object obj = null;
					while (true)
					{
						try
						{
							obj = worker.out.take();
							break;
						}
						catch (InterruptedException e)
						{
						}
					}

					if (obj instanceof Exception)
					{
						cmd = new ArrayList<Object>(1);
						cmd.add("CLOSE");
						worker.in.put(cmd);
						ok = false;
						return;
					}

					cmd = new ArrayList<Object>(1);
					cmd.add("CLOSE");
					worker.in.put(cmd);

					if (obj instanceof DataEndMarker)
					{
						ok = false;
						return;
					}

					card = (Long)((ArrayList<Object>)obj).get(0);
					if (totalCard > 1000000)
					{
						card = (long)(card + ((100.0d * card * card * (100.0d - p)) / (totalCard * p * p)));
						if (card > totalCard)
						{
							card = totalCard;
						}
					}
				}
				int updateCount = -1;
				Random random = new Random();
				while (updateCount == -1)
				{
					try
					{
						PlanCacheManager.getColCard().setParms(schema, table, col).execute(tx);
					}
					catch (Exception e)
					{
						sql = "INSERT INTO SYS.COLSTATS VALUES(" + tableID + "," + colID + "," + card + ")";
						XAWorker worker = XAManager.executeAuthorizedUpdate(sql, tx);
						worker.start();
						worker.join();
						updateCount = worker.getUpdateCount();
						if (updateCount == -1)
						{
							XAManager.rollback(tx);
							tx = new Transaction(Transaction.ISOLATION_UR);
						}
						else
						{
							XAManager.commit(tx);
							return;
						}

						Thread.sleep(random.nextInt(60000));
						continue;
					}

					sql = "UPDATE SYS.COLSTATS SET CARD = " + card + " WHERE TABLEID = " + tableID + " AND COLID = " + colID;
					XAWorker worker = XAManager.executeAuthorizedUpdate(sql, tx);
					worker.start();
					worker.join();
					updateCount = worker.getUpdateCount();
					if (updateCount == -1)
					{
						XAManager.rollback(tx);
						// tx = new Transaction(Transaction.ISOLATION_UR);
						return;
					}
					else if (updateCount == 1)
					{
						XAManager.commit(tx);
						return;
					}
					else
					{
						XAManager.rollback(tx);
						HRDBMSWorker.logger.debug("The SQL statement: '" + sql + "' updated " + updateCount + " rows");
						ok = false;
						return;
					}
				}

				Thread.sleep(random.nextInt(60000));
			}
			catch (Exception e)
			{
				ok = false;
				HRDBMSWorker.logger.debug("", e);
				try
				{
					XAManager.rollback(tx);
				}
				catch (Exception f)
				{
					// BLACKLIST?
					HRDBMSWorker.logger.debug("", f);
				}
			}
		}
	}

	private static class IndexStatsThread extends RunstatsThread
	{
		private final String schema;
		private final String table;
		private final ArrayList<String> keys;
		private final int tableID;
		private final int indexID;
		private Transaction tx;
		private boolean ok = true;
		private final long totalCard;
		private long p;

		public IndexStatsThread(String schema, String table, ArrayList<String> keys, int tableID, int indexID, Transaction tx, long totalCard)
		{
			this.schema = schema;
			this.table = table;
			this.keys = keys;
			this.tableID = tableID;
			this.indexID = indexID;
			this.tx = tx;
			this.totalCard = totalCard;
		}

		@Override
		public boolean getOK()
		{
			return ok;
		}

		@Override
		public void run()
		{
			try
			{
				String sql = "SELECT * FROM SYS.INDEXES WHERE TABLEID = " + tableID + " AND INDEXID = " + indexID;
				XAWorker worker = null;
				worker = XAManager.executeQuery(sql, tx, null);
				worker.start();
				ArrayList<Object> cmd = new ArrayList<Object>(2);
				cmd.add("NEXT");
				cmd.add(1);
				worker.in.put(cmd);

				Object obj2 = null;
				while (true)
				{
					try
					{
						obj2 = worker.out.take();
						break;
					}
					catch (InterruptedException e)
					{
					}
				}

				if (obj2 instanceof Exception)
				{
					cmd = new ArrayList<Object>(1);
					cmd.add("CLOSE");
					worker.in.put(cmd);
					ok = false;
					return;
				}

				cmd = new ArrayList<Object>(1);
				cmd.add("CLOSE");
				worker.in.put(cmd);

				if (obj2 instanceof DataEndMarker)
				{
					ok = false;
					return;
				}

				String unique = (String)((ArrayList<Object>)obj2).get(3);
				long card = -1;
				if (unique.equals("Y"))
				{
					card = totalCard;
				}
				else
				{
					p = 100000000l / totalCard;
					if (p < 5)
					{
						p = 5;
					}
					tx.setIsolationLevel(Transaction.ISOLATION_UR);

					if (keys.size() > 1)
					{
						sql = "SELECT COUNT(*) FROM (SELECT DISTINCT " + keys.get(0);
						int i = 1;
						final int size = keys.size();
						while (i < size)
						{
							sql += (", " + keys.get(i));
							i++;
						}
						sql += (" FROM " + schema + "." + table + ")");
					}
					else
					{
						sql = "SELECT COUNT(DISTINCT " + keys.get(0) + ") FROM " + schema + "." + table;
					}

					if (totalCard > 1000000)
					{
						worker = XAManager.executeQuery(sql, tx, null, p);
					}
					else
					{
						worker = XAManager.executeQuery(sql, tx, null);
					}
					worker.start();
					cmd = new ArrayList<Object>(2);
					cmd.add("NEXT");
					cmd.add(1);
					worker.in.put(cmd);

					obj2 = null;
					while (true)
					{
						try
						{
							obj2 = worker.out.take();
							break;
						}
						catch (InterruptedException e)
						{
						}
					}

					if (obj2 instanceof Exception)
					{
						cmd = new ArrayList<Object>(1);
						cmd.add("CLOSE");
						worker.in.put(cmd);
						ok = false;
						return;
					}

					cmd = new ArrayList<Object>(1);
					cmd.add("CLOSE");
					worker.in.put(cmd);

					if (obj2 instanceof DataEndMarker)
					{
						ok = false;
						return;
					}

					card = (Long)((ArrayList<Object>)obj2).get(0);
					if (totalCard > 1000000)
					{
						card = (long)(card + ((100.0d * card * card * (100.0d - p)) / (totalCard * p * p)));
						if (card > totalCard)
						{
							card = totalCard;
						}
					}
				}
				int updateCount = -1;
				Random random = new Random();
				while (updateCount == -1)
				{
					LockManager.xLock(new Block("/data1/SYS.INDEXSTATS.tbl", 4097), tx.number());
					LockManager.xLock(new Block("/data1/SYS.PKINDEXSTATS.indx", 0), tx.number());
					Object obj = PlanCacheManager.getIndexCard().setParms(tableID, indexID).execute(tx);
					if (obj instanceof DataEndMarker)
					{
						sql = "INSERT INTO SYS.INDEXSTATS VALUES(" + tableID + "," + indexID + "," + card + ")";
						worker = XAManager.executeAuthorizedUpdate(sql, tx);
						worker.start();
						worker.join();
						updateCount = worker.getUpdateCount();
						if (updateCount == -1)
						{
							XAManager.rollback(tx);
							tx = new Transaction(Transaction.ISOLATION_UR);
						}
						else
						{
							XAManager.commit(tx);
							return;
						}

						Thread.sleep(random.nextInt(60000));
					}
					else
					{
						sql = "UPDATE SYS.INDEXSTATS SET NUMDISTINCT = " + card + " WHERE TABLEID = " + tableID + " AND INDEXID = " + indexID;
						worker = XAManager.executeAuthorizedUpdate(sql, tx);
						worker.start();
						worker.join();
						updateCount = worker.getUpdateCount();
						if (updateCount == -1)
						{
							XAManager.rollback(tx);
							// tx = new Transaction(Transaction.ISOLATION_UR);
							return;
						}
						else if (updateCount == 1)
						{
							XAManager.commit(tx);
							return;
						}
						else
						{
							XAManager.rollback(tx);
							HRDBMSWorker.logger.debug("The SQL statement: '" + sql + "' updated " + updateCount + " rows");
							ok = false;
							return;
						}

						// Thread.sleep(random.nextInt(60000));
					}
				}
			}
			catch (Exception e)
			{
				ok = false;
				HRDBMSWorker.logger.debug("", e);
				try
				{
					XAManager.rollback(tx);
				}
				catch (Exception f)
				{
					// BLACKLIST?
					HRDBMSWorker.logger.debug("", f);
				}
			}
		}
	}

	private static abstract class RunstatsThread extends HRDBMSThread
	{
		public abstract boolean getOK();
	}

	private static class TableStatsThread extends HRDBMSThread
	{
		private final String schema;
		private final String table;
		private final int tableID;
		private Transaction tx;
		private boolean ok = true;

		public TableStatsThread(String schema, String table, int tableID, Transaction tx)
		{
			this.schema = schema;
			this.table = table;
			this.tableID = tableID;
			this.tx = tx;
		}

		public boolean getOK()
		{
			return ok;
		}

		@Override
		public void run()
		{
			try
			{
				tx.setIsolationLevel(Transaction.ISOLATION_UR);
				String sql = "SELECT COUNT(*) FROM " + schema + "." + table;
				XAWorker worker = XAManager.executeQuery(sql, tx, null);
				worker.start();
				ArrayList<Object> cmd = new ArrayList<Object>(2);
				cmd.add("NEXT");
				cmd.add(1);
				worker.in.put(cmd);

				Object obj = null;
				while (true)
				{
					try
					{
						obj = worker.out.take();
						break;
					}
					catch (InterruptedException e)
					{
					}
				}

				if (obj instanceof Exception)
				{
					cmd = new ArrayList<Object>(1);
					cmd.add("CLOSE");
					worker.in.put(cmd);
					ok = false;
					return;
				}

				cmd = new ArrayList<Object>(1);
				cmd.add("CLOSE");
				worker.in.put(cmd);

				if (obj instanceof DataEndMarker)
				{
					ok = false;
					return;
				}

				long card = (Long)((ArrayList<Object>)obj).get(0);
				Random random = new Random();
				int updateCount = -1;
				while (updateCount == -1)
				{
					try
					{
						PlanCacheManager.getTableCard().setParms(schema, table).execute(tx);
					}
					catch (Exception e)
					{
						sql = "INSERT INTO SYS.TABLESTATS VALUES(" + tableID + "," + card + ")";
						worker = XAManager.executeAuthorizedUpdate(sql, tx);
						worker.start();
						worker.join();
						updateCount = worker.getUpdateCount();
						if (updateCount == -1)
						{
							XAManager.rollback(tx);
							tx = new Transaction(Transaction.ISOLATION_UR);
						}
						else
						{
							XAManager.commit(tx);
							return;
						}

						Thread.sleep(random.nextInt(60000));
						continue;
					}

					sql = "UPDATE SYS.TABLESTATS SET CARD = " + card + " WHERE TABLEID = " + tableID;
					worker = XAManager.executeAuthorizedUpdate(sql, tx);
					worker.start();
					worker.join();
					updateCount = worker.getUpdateCount();
					if (updateCount == -1)
					{
						XAManager.rollback(tx);
						// tx = new Transaction(Transaction.ISOLATION_UR);
						return;
					}
					else if (updateCount == 1)
					{
						XAManager.commit(tx);
						return;
					}
					else
					{
						XAManager.rollback(tx);
						HRDBMSWorker.logger.debug("The SQL statement: '" + sql + "' updated " + updateCount + " rows");
						ok = false;
						return;
					}

					// Thread.sleep(random.nextInt(60000));
				}
			}
			catch (Exception e)
			{
				ok = false;
				HRDBMSWorker.logger.debug("", e);
				try
				{
					XAManager.rollback(tx);
				}
				catch (Exception f)
				{
					// BLACKLIST?
					HRDBMSWorker.logger.debug("", f);
				}
			}
		}
	}
}
