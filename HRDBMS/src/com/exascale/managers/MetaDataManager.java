package com.exascale.managers;

import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.BlockingQueue;
import com.exascale.optimizer.testing.*;
import com.exascale.tables.DataType;
import com.exascale.tables.InternalResultSet;
import com.exascale.tables.Plan;
import com.exascale.tables.Transaction;
import com.exascale.threads.HRDBMSThread;

public class MetaDataManager extends HRDBMSThread
{
	public MetaDataManager()
	{
		HRDBMSWorker.logger.info("Starting initialization of Metadata Manager.");
		description = "MetaData Manager";
		this.setWait(true);
	}

	public void run()
	{
		if (!FileManager.sysTablesExists())
		{
			try
			{
				HRDBMSWorker.logger.info("About to start initial catalog creation.");
				FileManager.createCatalog();
			}
			catch(Exception e)
			{
				HRDBMSWorker.logger.error("Exception occurred during initial catalog creation/synchronization.", e);
				System.exit(1);
			}
			
			HRDBMSWorker.logger.info("Metadata Manager initialization complete.");
		}
		
		this.terminate();
		return;
	}
	
	/*protected static InternalResultSet catalogQuery(Transaction tx, String sql, Object... args)
	{
		Plan p = PlanCacheManager.checkPlanCache(sql);
		p.setArgs(args);
		BufferedLinkedBlockingQueue q = XAManager.runWithRS(p, tx);
		Vector<Vector<Object>> data = new Vector<Vector<Object>>();
		Object o = q.take();
		while (!(o instanceof DataEndMarker))
		{
			if (o instanceof Exception)
			{
				throw (Exception)o;
			}
			Vector<Object> v = (Vector<Object>)o;
			data.add(v);
			o = q.take();
		}
		
		return new InternalResultSet(data);
	}
	
	public static void createTable(Transaction tx, String schema, String name, String[] colNames, DataType[] types, int[] keyCols, String[] isSequence, Long[] nextSeqValue, Integer[] seqInc, String[] generated, byte[][] defaults, String[] nullable, int[] clusterKeys, String groupExp, String nodeExp, String devExp, String type)
	{
		int iso = tx.getIsolationLevel();
		tx.setIsolationLevel(Transaction.ISOLATION_RR);
		
		InternalResultSet check = catalogQuery(tx, "SELECT COUNT(*) FROM SYS.TABLES WHERE SCHEMA = ? AND TABNAME = ?", schema, name);
		check.next();
		if (check.getInt(1) != 0)
		{
			tx.setIsolationLevel(iso);
			throw new Exception("Table already exists!");
		}
		
		HashMap<Integer, Vector<Integer>> activeNodes = calculateActiveNodesAndDevs(groupExp, nodeExp, devExp);
		Vector<CreateTableFileThread> threads = new Vector<CreateTableFileThread>();

		int threadCount = 0;
		for (int nodeID : activeNodes.keySet())
		{
			InternalResultSet nResult = catalogQuery(tx, "SELECT HOSTNAME, RACK, STATE FROM SYS.NODES A, SYS.NODESTATE B WHERE A.NODEID = ? AND A.NODEID = B.NODE", nodeID);
			if (nResult.getString(3).equals("I"))
			{
				addReplay(new CreateTableFileThread(nodeID, nResult.getString(1), schema + "." + name + ".tbl", colNames.length, type));
			}
			else
			{
				threads.add(HRDBMSWorker.addThread(new CreateTableFileThread(nodeID, nResult.getString(1), schema + "." + name + ".tbl", colNames.length, type)));
				threadCount++;
			}
			
			InternalResultSet backups = catalogQuery(tx, "SELECT SECOND, THIRD FROM SYS.BACKUPS WHERE FIRST = ?", nodeID);
			backups.next();
			if (backups.getInt(1) != null)
			{
				int node = backups.getInt(1); 
				InternalResultSet irs = catalogQuery(tx, "SELECT HOSTNAME FROM SYS.NODES WHERE NODEID = ?", node);
				irs.next();
				String nodeHost = irs.getString(1);
				InternalResultSet nodeOK = catalogQuery(tx, "SELECT STATE FROM SYS.NODESTATE WHERE NODE = ?", node);
				nodeOK.next();
				if (nodeOK.getString(1).equals("I"))
				{
					addReplay(new CreateTableFileThread(node, nodeHost, schema + "." + name + "." + nodeID + ".tbl", colNames.length, type));
				}
				else
				{
					threads.add(HRDBMSWorker.addThread(new CreateTableFileThread(node, nodeHost, schema + "." + name + "." + nodeID + ".tbl", colNames.length, type)));
					threadCount++;
				}
				
				if (backups.getInt(2) != null)
				{
					node = backups.getInt(2);
					irs = catalogQuery(tx, "SELECT HOSTNAME FROM SYS.NODES WHERE NODEID = ?", node);
					irs.next();
					nodeHost = irs.getString(1);
					nodeOK = catalogQuery(tx, "SELECT STATE FROM SYS.NODESTATE WHERE NODE = ?", node);
					nodeOK.next();
					if (nodeOK.getString(1).equals("I"))
					{
						addReplay(new CreateTableFileThread(node, nodeHost, schema + "." + name + "." + nodeID + ".tbl", colNames.length, type));
					}
					else
					{
						threads.add(HRDBMSWorker.addThread(new CreateTableFileThread(node, nodeHost, schema + "." + name + "." + nodeID + ".tbl", colNames.length, type)));
						threadCount++;
					}
				}
			}
		}
		
		try
		{
			Vector<Long> failed = HRDBMSWorker.waitOnThreads(threads, Thread.currentThread(), HRDBMSWorker.get().getProperty("create_table_timeout"));
			for (Long threadNum : failed)
			{
				HRDBMSThread thread = HRDBMSWorker.getThreadList().get(threadNum);
				addReplay((ReplayThread)thread);
				updateAllCoords(tx, "UPDATE SYS.NODESTATE SET STATE = 'I' WHERE NODE = ?", ((ReplayThread)thread).getNode());
				HRDBMSWorker.addThread(new BlacklistThread((ReplayThread)thread).getNode());
			}
		
			if (threadCount == 0 || failed.size() == threadCount())
			{
				tx.setIsolationLevel(iso);
				throw new Exception("All worker nodes are unavailable from this coordinator!");
			}
		
			int tableID = insertAllCoordsReturnSeq(tx, "INSERT INTO SYS.TABLES(SCHEMA, TABNAME, NUMCOLS, NUMKEYCOLS, TYPE) VALUES (?, ?, ?, ?, ?)", schema, name, colNames.length, keyCols.length, type);
		
			if (!(colNames.length == types.length && types.length == isSequence.length && isSequence.length == nextSeqValue.length && nextSeqValue.length == seqInc.length && seqInc.length == generated.length && generated.length == defaults.length && defaults.length == nullable.length))
			{
				tx.setIsolationLevel(iso);
				throw new IllegalArgumentException("Array sizes do not match on call to createTable");
			}
		
			int i = 0;
			for (String colName : colNames)
			{
				int j = 0;
				Integer pkPos = null;
				Integer clusterPos = null;
				for (int colID : keyCols)
				{
					if (colID == i)
					{
						pkPos = j;
					}
					j++;
				}
			
				j = 0;
				for (int colID : clusterKeys)
				{
					if (colID == i)
					{
						clusterPos = j;
					}
					j++;
				}
			
				insertAllCoords(tx, "INSERT INTO SYS.COLUMNS(COLID, TABLEID, COLNAME, COLTYPE, LENGTH, SCALE, IDENTITY, NEXTVAL, INCREMENT, DEFAULT, NULLABLE, PKPOSITION, CLUSTERPOS, GENERATED) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", i, tableID, colName, types[i].getTypeString(), types[i].getLength(), types[i].getScale(), isSequence[i], nextSeqValue[i], seqInc[i], defaults[i], nullable[i], pkPos, clusterPos, generated[i]);
				i++;
			}
			
			for (Map.Entry<Integer, Vector<Integer>> entry : activeNodes.entrySet())
			{
				int nodeID = entry.getKey();
				for (int devID : entry.getValue())
				{
					insertAllCoords(tx, "INSERT INTO SYS.TABLESTATS(TABLEID, NODEID, DEVID, CARD, PAGES, AVGROWLEN) VALUES (?, ?, ?, ?, ?, ?)", tableID, nodeID, devID, 0, 4097, 0);
					int colID = 0;
					for (String colName : colNames)
					{
						insertAllCoords(tx, "INSERT INTO SYS.COLSTATS(TABLEID, NODEID, DEVID, COLID, CARD, AVGCOLLEN, NUMNULLS) VALUES (?, ?, ?, ?, ?, ?, ?)", tableID, nodeID, devID, colID, 0, 0, 0);
						colID++;
					}
				}
			}
		
			int level;
			if (groupExp != null)
			{
				level = 1;
			}
			else if (nodeExp != null)
			{
				level = 2;
			}
			else
			{
				level = 0;
			}
		
			insertAllCoords(tx, "INSERT INTO SYS.PARTITIONING(TABLEID, LEVEL, GROUPEXP, NODEEXP, DEVEXP) VALUES (?, ?, ?, ?, ?)", tableID, level, groupExp, nodeExp, devExp);
		}
		catch(Exception e)
		{
			tx.setIsolationLevel(iso);
			throw e;
		}
		
		tx.setIsolationLevel(iso);
	}
	
	public static void dropTable(Transaction tx, String schema, String name)
	{
		int iso = tx.getIsolationLevel();
		tx.setIsolationLevel(Transaction.ISOLATION_RR);
		
		InternalResultSet check = catalogQuery(tx, "SELECT TABLEID FROM SYS.TABLES WHERE SCHEMA = ? AND TABNAME = ?", schema, name);
		if (!check.next())
		{
			tx.setIsolationLevel(iso);
			throw new Exception("Table does not exist!");
		}
		
		try
		{
			deleteAllCoords(tx, "DELETE FROM SYS.TABLES WHERE TABLEID = ?", check.getInt(1));
			deleteAllCoords(tx, "DELETE FROM SYS.COLUMNS WHERE TABLEID = ?", check.getInt(1));
			deleteAllCoords(tx, "DELETE FROM SYS.COLGROUPS WHERE TABLEID = ?", check.getInt(1));
			deleteAllCoords(tx, "DELETE FROM SYS.TABLESTATS WHERE TABLEID = ?", check.getInt(1));
			deleteAllCoords(tx, "DELETE FROM SYS.COLGROUPSTATS WHERE TABLEID = ?", check.getInt(1));
			deleteAllCoords(tx, "DELETE FROM SYS.PARTITIONING WHERE TABLEID = ?", check.getInt(1));
			deleteAllCoords(tx, "DELETE FROM SYS.COLSTATS WHERE TABLEID = ?", check.getInt(1));
			deleteAllCoords(tx, "DELETE FROM SYS.COLDIST WHERE TABLEID = ?", check.getInt(1));
		}
		catch(Exception e)
		{
			tx.setIsolationLevel(iso);
			throw e;
		}
		
		tx.setIsolationLevel(iso);
	}
	
	public static void createColGroup(Transaction tx, String schema, String tabName, int[] colIDs, String groupName)
	{
		int iso = tx.getIsolationLevel();
		tx.setIsolationLevel(Transaction.ISOLATION_RR);
		
		InternalResultSet irs = catalogQuery(tx, "SELECT TABLEID FROM SYS.TABLES WHERE SCHEMA = ? AND TABNAME = ?", schema, tabName);
		if (!irs.next())
		{
			tx.setIsolationLevel(iso);
			throw new Exception("Table does not exist!");
		}
		
		int tableID = irs.getInt(1);
		
		irs = catalogQuery(tx, "SELECT COUNT(*) FROM SYS.COLGROUPS WHERE TABLEID = ? AND COLGROUPNAME = ?", tableID, groupName);
		irs.next();
		if (irs.getInt(1) != 0)
		{
			tx.setIsolationLevel(iso);
			throw new Exception("A colgroup with that name already exists on this table!");
		}
		
		irs = catalogQuery(tx, "SELECT COUNT(DISTINCT COLGROUPNAME) FROM SYS.COLGROUPS WHERE TABLEID = ?", tableID);
		irs.next();
		int colgroupID = irs.getInt(1);
		
		try
		{
			for (int colID : colIDs)
			{
				insertAllCoords(tx, "INSERT INTO SYS.COLGROUPS(COLGROUPID, TABLEID, COLID, COLGROUPNAME) VALUES (?, ?, ?, ?)", colgroupID, tableID, colID, groupName);
			}
		}
		catch(Exception e)
		{
			tx.setIsolationLevel(iso);
			throw e;
		}
		
		tx.setIsolationLevel(iso);
	}
	
	public static void dropColGroup(Transaction tx, String schema, String tabName, String groupName)
	{
		int iso = tx.getIsolationLevel();
		tx.setIsolationLevel(Transaction.ISOLATION_RR);
		
		InternalResultSet check = catalogQuery(tx, "SELECT TABLEID FROM SYS.TABLES WHERE SCHEMA = ? AND TABNAME = ?", schema, tabName);
		if (!check.next())
		{
			tx.setIsolationLevel(iso);
			throw new Exception("Table does not exist!");
		}
		
		InternalResultSet irs = catalogQuery(tx, "SELECT DISTINCT COLGROUPID FROM SYS.COLGROUPS WHERE TABLEID = ? AND COLGROUPNAME = ?", check.getInt(1), groupName);
		
		if (!irs.next())
		{
			tx.setIsolationLevel(iso);
			throw new Exception("Colgroup does not exist!");
		}
		
		int colgroupID = irs.getInt(1);
		
		try
		{
			deleteAllCoords(tx, "DELETE FROM SYS.COLGROUPS WHERE TABLEID = ? AND COLGROUPNAME = ?", check.getInt(1), groupName);
			deleteAllCoords(tx, "DELETE FROM SYS.COLGROUPSTATS WHERE TABLEID = ? AND COLGROUPID = ?", check.getInt(1), colgroupID);
		}
		catch(Exception e)
		{
			tx.setIsolationLevel(iso);
			throw e;
		}
		
		tx.setIsolationLevel(iso);
	}
	
	public static void createNodeGroup(Transaction tx, String name, int[] nodes)
	{
		int iso = tx.getIsolationLevel();
		tx.setIsolationLevel(Transaction.ISOLATION_RR);
		
		InternalResultSet irs = catalogQuery(tx, "SELECT COUNT(*) FROM SYS.NODEGROUPS WHERE NAME = ?", name);
		irs.next();
		if (irs.getInt(1) != 0)
		{
			tx.setIsolationLevel(iso);
			throw new Exception("Nodegroup already exists!");
		}
		
		try
		{
			for (int node : nodes)
			{
				insertAllCoords(tx, "INSERT INTO SYS.NODEGROUPS(NAME, NODEID) VALUES (?, ?)", name, node);
			}
		}
		catch(Exception e)
		{
			tx.setIsolationLevel(iso);
			throw e;
		}
		
		tx.setIsolationLevel(iso);
	}

	public static void dropNodeGroup(Transaction tx, String name)
	{
		int iso = tx.getIsolationLevel();
		tx.setIsolationLevel(Transaction.ISOLATION_RR);
		
		InternalResultSet irs = catalogQuery(tx, "SELECT COUNT(*) FROM SYS.NODEGROUPS WHERE NAME = ?", name);
		irs.next();
		if (irs.getInt(1) == 0)
		{
			tx.setIsolationLevel(iso);
			throw new Exception("Nodegroup does not exist!");
		}
		
		try
		{
			deleteAllCoords("DELETE FROM SYS.NODEGROUPS WHERE NAME = ?", name);
		}
		catch(Exception e)
		{
			tx.setIsolationLevel(iso);
			throw e;
		}
		
		tx.setIsolationLevel(iso);
	}
	
	public static void createView(Transaction tx, String schema, String name, String text)
	{
		int iso = tx.getIsolationLevel();
		tx.setIsolationLevel(Transaction.ISOLATION_RR);
		
		InternalResultSet irs = catalogQuery(tx, "SELECT COUNT(*) FROM SYS.VIEWS WHERE SCHEMA = ? AND NAME = ?", schema, name);
		irs.next();
		if (irs.getInt(1) != 0)
		{
			tx.setIsolationLevel(iso);
			throw new Exception("View already exists!");
		}
		
		try
		{
			insertAllCoords(tx, "INSERT INTO SYS.VIEWS(SCHEMA, NAME, TEXT) VALUES (?, ?, ?)", schema, name, text);
		}
		catch(Exception e)
		{
			tx.setIsolationLevel(iso);
			throw e;
		}
		
		tx.setIsolationLevel(iso);
	}
	
	public static void dropView(Transaction tx, String schema, String name)
	{
		int iso = tx.getIsolationLevel();
		tx.setIsolationLevel(Transaction.ISOLATION_RR);
		
		InternalResultSet irs = catalogQuery(tx, "SELECT COUNT(*) FROM SYS.VIEWS WHERE SCHEMA = ? AND NAME = ?", schema, name);
		irs.next();
		if (irs.getInt(1) == 0)
		{
			tx.setIsolationLevel(iso);
			throw new Exception("View does not exist!");
		}
		
		try
		{
			deleteAllCoords(tx, "DELETE FROM SYS.VIEWS WHERE SCHEMA = ? AND NAME = ?", schema, name);
		}
		catch(Exception e)
		{
			tx.setIsolationLevel(iso);
			throw e;
		}
		
		tx.setIsolationLevel(iso);
	}
	
	public static void createIndex(Transaction tx, String schema, String tabName, String indexName, String unique, int[] colIDs, String type, String[] order)
	{
		int iso = tx.getIsolationLevel();
		tx.setIsolationLevel(Transaction.ISOLATION_RR);
		
		InternalResultSet check = catalogQuery(tx, "SELECT TABLEID FROM SYS.TABLES WHERE SCHEMA = ? AND TABNAME = ?", schema, tabName);
		if (!check.next())
		{
			tx.setIsolationLevel(iso);
			throw new Exception("Table does not exist!");
		}
		int tableID = check.getInt(1);
		
		catalogQuery(tx, "SELECT COUNT(*) FROM SYS.INDEXES WHERE INDEXNAME = ? AND TABLEID = ?", indexName, tableID); 
		check.next();
		if (check.getInt(1) != 0)
		{
			tx.setIsolationLevel(iso);
			throw new Exception("Index already exists!");
		}
		
		Vector<CreateIndexFileThread> threads = new Vector<CreateIndexFileThread>();

		int threadCount = 0;
		for (int nodeID : activeNodes.keySet())
		{
			InternalResultSet nResult = catalogQuery(tx, "SELECT HOSTNAME, RACK, STATE FROM SYS.NODES A, SYS.NODESTATE B WHERE A.NODEID = ? AND A.NODEID = B.NODE", nodeID);
			if (nResult.getString(3).equals("I"))
			{
				addReplay(new CreateIndexFileThread(nodeID, nResult.getString(1), schema + "." + tabName + "." + indexName + ".index", colIDs.length, unique, type));
			}
			else
			{
				threads.add(HRDBMSWorker.addThread(new CreateIndexFileThread(nodeID, nResult.getString(1), schema + "." + tabName + "." + indexName + ".index", colIDs.length, unique, type)));
				threadCount++;
			}
			
			InternalResultSet backups = catalogQuery(tx, "SELECT SECOND, THIRD FROM SYS.BACKUPS WHERE FIRST = ?", nodeID);
			backups.next();
			if (backups.getInt(1) != null)
			{
				int node = backups.getInt(1); 
				InternalResultSet irs = catalogQuery(tx, "SELECT HOSTNAME FROM SYS.NODES WHERE NODEID = ?", node);
				irs.next();
				String nodeHost = irs.getString(1);
				InternalResultSet nodeOK = catalogQuery(tx, "SELECT STATE FROM SYS.NODESTATE WHERE NODE = ?", node);
				nodeOK.next();
				if (nodeOK.getString(1).equals("I"))
				{
					addReplay(new CreateIndexFileThread(node, nodeHost, schema + "." + tabName + "." + indexName + "." + nodeID + ".index", colIDs.length, unique, type));
				}
				else
				{
					threads.add(HRDBMSWorker.addThread(new CreateIndexFileThread(node, nodeHost, schema + "." + tabName + "." + indexName + "." + nodeID + ".index", colIDs.length, unique, type)));
					threadCount++;
				}
				
				if (backups.getInt(2) != null)
				{
					node = backups.getInt(2);
					irs = catalogQuery(tx, "SELECT HOSTNAME FROM SYS.NODES WHERE NODEID = ?", node);
					irs.next();
					nodeHost = irs.getString(1);
					nodeOK = catalogQuery(tx, "SELECT STATE FROM SYS.NODESTATE WHERE NODE = ?", node);
					nodeOK.next();
					if (nodeOK.getString(1).equals("I"))
					{
						addReplay(new CreateIndexFileThread(node, nodeHost, schema + "." + tabName + "." + indexName + "." + nodeID + ".index", colIDs.length, unique, type));
					}
					else
					{
						threads.add(HRDBMSWorker.addThread(new CreateIndexFileThread(node, nodeHost, schema + "." + tabName + "." + indexName + "." + nodeID + ".index", colIDs.length, unique, type)));
						threadCount++;
					}
				}
			}
		}
			
		try
		{
			Vector<Long> failed = HRDBMSWorker.waitOnThreads(threads, Thread.currentThread(), HRDBMSWorker.getHParms().getProperty("create_table_timeout"));
			for (Long threadNum : failed)
			{
				HRDBMSThread thread = HRDBMSWorker.getThreadList().get(threadNum);
				addReplay((ReplayThread)thread);
				updateAllCoords(tx, "UPDATE SYS.NODESTATE SET STATE = 'I' WHERE NODE = ?", ((ReplayThread)thread).getNode());
				HRDBMSWorker.addThread(new BlacklistThread((ReplayThread)thread).getNode());
			}
		
			if (threadCount == 0 || failed.size() == threadCount())
			{
				tx.setIsolationLevel(iso);
				throw new Exception("All worker nodes are unavailable from this coordinator!");
			}
		
			InternalResultSet high = catalogQuery(tx, "SELECT COUNT(*) FROM SYS.INDEXES WHERE TABLEID = ?", tableID);
			high.next();
			int indexID = high.getInt(1);
			insertAllCoords(tx, "INSERT INTO SYS.INDEXES(INDEXID, INDEXNAME, TABLEID, UNIQUE, NUMCOLS, TYPE) VALUES (?, ?, ?, ?, ?, ?)", indexID, tableID, unique, colIDs.length, type); 
		
			int pos = 0;
			for (int colID : colIDs)
			{
				insertAllCoords(tx, "INSERT INTO SYS.INDEXCOLS(INDEXID, TABLEID, COLID, POSITION, ORDER) VALUES (?, ?, ?, ?, ?)", indexID, tableID, colID, pos, order[pos]);
				pos++;
			}
			
			InternalResultSet count = query(tx, "SELECT COUNT(*), SYS_NODEID, SYS_DEVID FROM " + schema + "." + tabName + " GROUP BY SYS_NODEID, SYS_DEVID");
			while (count.next())
			{
				int rows = getInt(1);
				int nodeID = getInt(2);
				int devID = getInt(3);
				insertAllCoords(tx, "INSERT INTO SYS.INDEXSTATS(TABLEID, INDEXID, NLEAFS, NLEVELS, 1STKEYCARD, 2NDKEYCARD, 3RDKEYCARD, 4THKEYCARD, FULLKEYCARD, NROWS, NBLOCKS, NODEID, DEVID) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", tableID, indexID, 0, 2, null, null, null, null, null, rows, 1, nodeID, devID);
			}
		}
		catch(Exception e)
		{
			tx.setIsolationLevel(iso);
			throw e;
		}
			
		tx.setIsolationLevel(iso);;
	}
	
	public static void dropIndex(Transaction tx, String schema, String tabName, String indexName)
	{
		int iso = tx.getIsolationLevel();
		tx.setIsolationLevel(Transaction.ISOLATION_RR);
		
		InternalResultSet check = catalogQuery(tx, "SELECT TABLEID FROM SYS.TABLES WHERE SCHEMA = ? AND TABNAME = ?", schema, tabName);
		if (!check.next())
		{
			tx.setIsolationLevel(iso);
			throw new Exception("Table does not exist!");
		}
		int tableID = check.getInt(1);
		
		check = catalogQuery(tx, "SELECT INDEXID FROM SYS.INDEXES WHERE TABLEID = ? AND INDEXNAME = ?", tableID, indexName);

		if (!check.next())
		{
			tx.setIsolationLevel(iso);
			throw new Exception("Index does not exist!");
		}
		int indexID = check.getInt(1);
		
		try
		{
			deleteAllCoords(tx, "DELETE FROM SYS.INDEXES WHERE TABLEID = ? AND INDEXNAME = ?", tableID, indexName);
			deleteAllCoords(tx, "DELETE FROM SYS.INDEXCOLS WHERE TABLEID = ? AND INDEXID = ?", tableID, indexID);
			deleteAllCoords(tx, "DELETE FROM SYS.INDEXSTATS WHERE TABLEID = ? AND INDEXID = ?", tableID, indexID);
		}
		catch(Exception e)
		{
			tx.setIsolationLevel(iso);
			throw e;
		}
		
		tx.setIsolationLevel(iso);
	}
	
	public static MetaData getMetaData(Transaction tx, String schema, String name)
	{	InternalResultSet irs = catalogQuery(tx, "SELECT TABLEID FROM SYS.TABLES WHERE SCHEMA = ? AND TABNAME = ?", schema, name);
		InternalResultSet tablesRS = catalogQuery(tx, "SELECT * FROM SYS.TABLES WHERE SCHEMA = ? AND TABNAME = ?", schema, name);
		InternalResultSet columnsRS = catalogQuery(tx, "SELECT * FROM SYS.COLUMNS WHERE TABLEID = ?", tableID);
		InternalResultSet indexRS = catalogQuery(tx, "SELECT * FROM SYS.INDEXES WHERE TABLEID = ?", tableID);
		InternalResultSet indexColsRS = catalogQuery(tx, "SELECT * FROM SYS.INDEXCOLS WHERE TABLEID = ?", tableID);
		InternalResultSet colGroupsRS = catalogQuery(tx, "SELECT * FROM SYS.COLGROUPS WHERE TABLEID = ?", tableID);
		InternalResultSet tableStatsRS = catalogQuery(tx, "SELECT * FROM SYS.TABLESTATS WHERE TABLEID = ?", tableID);
		InternalResultSet colGroupStatsRS = catalogQuery(tx, "SELECT * FROM SYS.COLGROUPSTATS WHERE TABLEID = ?", tableID);
		InternalResultSet indexStatsRS = catalogQuery(tx, "SELECT * FROM SYS.INDEXSTATS WHERE TABLEID = ?", tableID);
		InternalResultSet partRS = catalogQuery(tx, "SELECT * FROM SYS.PARTITIONING WHERE TABLEID = ?", tableID);
		InternalResultSet colStatsRS = catalogQuery(tx, "SELECT * FROM SYS.COLSTATS WHERE TABLEID = ?", tableID);
		InternalResultSet colDistRS = catalogQuery(tx, "SELECT * FROM SYS.COLDIST WHERE TABLEID = ?", tableID);
		
		return new MetaData(tablesRS, columnsRS, indexRS, indexColsRS, colGroupsRS, tableStatsRS, colGroupStatsRS, indexStatsRS, partRS, colStatsRS, colDistRS);		
		//DOES NOT INCLUDE NODEGROUPS, NODES, NETWORK, DEVICES, BACKUPS, or NODESTATE
	} */
}
