package com.exascale.managers;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import com.exascale.misc.DataEndMarker;
import com.exascale.optimizer.AggregateOperator;
import com.exascale.optimizer.CountOperator;
import com.exascale.optimizer.Filter;
import com.exascale.optimizer.HashJoinOperator;
import com.exascale.optimizer.Index;
import com.exascale.optimizer.IndexOperator;
import com.exascale.optimizer.MaxOperator;
import com.exascale.optimizer.MetaData;
import com.exascale.optimizer.MultiOperator;
import com.exascale.optimizer.Operator;
import com.exascale.optimizer.ReorderOperator;
import com.exascale.optimizer.RootOperator;
import com.exascale.optimizer.SelectOperator;
import com.exascale.optimizer.SortOperator;
import com.exascale.optimizer.TableScanOperator;
import com.exascale.tables.Plan;
import com.exascale.tables.SQL;
import com.exascale.tables.Transaction;
import com.exascale.threads.XAWorker;

public class PlanCacheManager
{
	private static ConcurrentHashMap<SQL, Plan> planCache = new ConcurrentHashMap<SQL, Plan>(16, 0.75f, 6 * ResourceManager.cpus);
	// private static ReentrantReadWriteLock lock = new
	// ReentrantReadWriteLock();
	private static Integer numWorkers = null;
	private static volatile boolean addPlan = true;

	// Plans have creation timestamp and reserved flag

	static
	{
		try
		{
			// add catalog plans
			// getHostLookup
			Transaction tx = new Transaction(Transaction.ISOLATION_RR);
			MetaData meta = new MetaData();
			ArrayList<String> keys = new ArrayList<String>();
			ArrayList<String> types = new ArrayList<String>();
			ArrayList<Boolean> orders = new ArrayList<Boolean>();
			keys.add("NODES.NODEID");
			types.add("INT");
			orders.add(true);
			Index index = new Index("SYS.SKNODES.indx", keys, types, orders);
			IndexOperator iOp = new IndexOperator(index, meta);
			iOp.setNode(-1);
			iOp.setDevice(0);
			TableScanOperator tOp = new TableScanOperator("SYS", "NODES", meta, tx);
			ArrayList<Integer> devs = new ArrayList<Integer>();
			devs.add(0);
			tOp.addActiveDevices(devs);
			tOp.setChildForDevice(0, iOp);
			ArrayList<String> needed = new ArrayList<String>();
			needed.add("NODES.HOSTNAME");
			tOp.setNeededCols(needed);
			tOp.setNode(-1);
			tOp.setPhase2Done();
			tOp.add(iOp);
			RootOperator root = new RootOperator(meta);
			root.add(tOp);
			ArrayList<Operator> trees = new ArrayList<Operator>();
			trees.add(root);
			Plan p = new Plan(true, trees);
			addPlan("SELECT HOSTNAME FROM SYS.NODES WHERE NODEID = ?", p);

			// getWorkerNodes
			meta = new MetaData();
			tOp = new TableScanOperator("SYS", "NODES", meta, tx);
			devs = new ArrayList<Integer>();
			devs.add(0);
			tOp.addActiveDevices(devs);
			needed = new ArrayList<String>();
			needed.add("NODES.NODEID");
			tOp.setNeededCols(needed);
			tOp.setNode(-1);
			tOp.setPhase2Done();
			SelectOperator select = new SelectOperator(new Filter("NODES.NODEID", "GE", "0"), meta);
			select.add(tOp);
			root = new RootOperator(meta);
			root.add(select);
			trees = new ArrayList<Operator>();
			trees.add(root);
			p = new Plan(true, trees);
			addPlan("SELECT NODEID FROM SYS.NODES WHERE NODEID >= 0", p);

			// getCoordNodes
			meta = new MetaData();
			tOp = new TableScanOperator("SYS", "NODES", meta, tx);
			devs = new ArrayList<Integer>();
			devs.add(0);
			tOp.addActiveDevices(devs);
			needed = new ArrayList<String>();
			needed.add("NODES.NODEID");
			tOp.setNeededCols(needed);
			tOp.setNode(-1);
			tOp.setPhase2Done();
			select = new SelectOperator(new Filter("NODES.NODEID", "L", "-1"), meta);
			select.add(tOp);
			root = new RootOperator(meta);
			root.add(select);
			trees = new ArrayList<Operator>();
			trees.add(root);
			p = new Plan(true, trees);
			addPlan("SELECT NODEID FROM SYS.NODES WHERE NODEID < -1", p);

			// getCountWorkerNodes
			meta = new MetaData();
			tOp = new TableScanOperator("SYS", "NODES", meta, tx);
			devs = new ArrayList<Integer>();
			devs.add(0);
			tOp.addActiveDevices(devs);
			needed = new ArrayList<String>();
			needed.add("NODES.NODEID");
			tOp.setNeededCols(needed);
			tOp.setNode(-1);
			tOp.setPhase2Done();
			select = new SelectOperator(new Filter("NODES.NODEID", "GE", "0"), meta);
			select.add(tOp);
			ArrayList<AggregateOperator> ops = new ArrayList<AggregateOperator>();
			ops.add(new CountOperator("._E1", meta));
			ArrayList<String> groupCols = new ArrayList<String>();
			MultiOperator multi = new MultiOperator(ops, groupCols, meta, false);
			multi.add(select);
			ArrayList<String> colOrder = new ArrayList<String>();
			colOrder.add("._E1");
			ReorderOperator reorder = new ReorderOperator(colOrder, meta);
			reorder.add(multi);
			root = new RootOperator(meta);
			root.add(reorder);
			trees = new ArrayList<Operator>();
			trees.add(root);
			p = new Plan(true, trees);
			addPlan("SELECT COUNT(*) FROM SYS.NODES WHERE NODEID >= 0", p);

			// getIndexes
			meta = new MetaData();
			keys = new ArrayList<String>();
			types = new ArrayList<String>();
			orders = new ArrayList<Boolean>();
			keys.add("TABLES.SCHEMA");
			keys.add("TABLES.TABNAME");
			types.add("CHAR");
			types.add("CHAR");
			orders.add(true);
			orders.add(true);
			index = new Index("SYS.PKTABLES.indx", keys, types, orders);
			iOp = new IndexOperator(index, meta);
			iOp.setNode(-1);
			iOp.setDevice(0);
			tOp = new TableScanOperator("SYS", "TABLES", meta, tx);
			devs = new ArrayList<Integer>();
			devs.add(0);
			tOp.addActiveDevices(devs);
			tOp.setChildForDevice(0, iOp);
			tOp.setAlias("A");
			needed = new ArrayList<String>();
			needed.add("A.TABLEID");
			tOp.setNeededCols(needed);
			tOp.setNode(-1);
			tOp.setPhase2Done();
			tOp.add(iOp);
			TableScanOperator tOp2 = new TableScanOperator("SYS", "INDEXES", meta, tx);
			devs = new ArrayList<Integer>();
			devs.add(0);
			tOp2.addActiveDevices(devs);
			tOp2.setAlias("B");
			needed = new ArrayList<String>();
			needed.add("B.TABLEID");
			needed.add("B.INDEXNAME");
			tOp2.setNeededCols(needed);
			tOp2.setNode(-1);
			tOp2.setPhase2Done();
			HashJoinOperator hash = new HashJoinOperator("A.TABLEID", "B.TABLEID", meta);
			hash.add(tOp);
			hash.add(tOp2);
			colOrder = new ArrayList<String>();
			colOrder.add("B.INDEXNAME");
			reorder = new ReorderOperator(colOrder, meta);
			reorder.add(hash);
			orders = new ArrayList<Boolean>();
			orders.add(true);
			SortOperator sort = new SortOperator(colOrder, orders, meta);
			sort.add(reorder);
			root = new RootOperator(meta);
			root.add(sort);
			trees = new ArrayList<Operator>();
			trees.add(root);
			p = new Plan(true, trees);
			addPlan("SELECT INDEXNAME FROM SYS.TABLES A, SYS.INDEXES B WHERE A.SCHEMA = ? AND A.TABNAME = ? AND A.TABLEID = B.TABLEID ORDER BY INDEXNAME", p);

			// getUniqueIndexes
			meta = new MetaData();
			keys = new ArrayList<String>();
			types = new ArrayList<String>();
			orders = new ArrayList<Boolean>();
			keys.add("TABLES.SCHEMA");
			keys.add("TABLES.TABNAME");
			types.add("CHAR");
			types.add("CHAR");
			orders.add(true);
			orders.add(true);
			index = new Index("SYS.PKTABLES.indx", keys, types, orders);
			iOp = new IndexOperator(index, meta);
			iOp.setNode(-1);
			iOp.setDevice(0);
			tOp = new TableScanOperator("SYS", "TABLES", meta, tx);
			devs = new ArrayList<Integer>();
			devs.add(0);
			tOp.addActiveDevices(devs);
			tOp.setChildForDevice(0, iOp);
			tOp.setAlias("A");
			needed = new ArrayList<String>();
			needed.add("A.TABLEID");
			tOp.setNeededCols(needed);
			tOp.setNode(-1);
			tOp.setPhase2Done();
			tOp.add(iOp);
			tOp2 = new TableScanOperator("SYS", "INDEXES", meta, tx);
			devs = new ArrayList<Integer>();
			devs.add(0);
			tOp2.addActiveDevices(devs);
			tOp2.setAlias("B");
			needed = new ArrayList<String>();
			needed.add("B.TABLEID");
			needed.add("B.INDEXNAME");
			needed.add("B.UNIQUE");
			tOp2.setNeededCols(needed);
			tOp2.setNode(-1);
			tOp2.setPhase2Done();
			select = new SelectOperator(new Filter("B.UNIQUE", "E", "'Y'"), meta);
			select.add(tOp2);
			hash = new HashJoinOperator("A.TABLEID", "B.TABLEID", meta);
			hash.add(tOp);
			hash.add(select);
			colOrder = new ArrayList<String>();
			colOrder.add("B.INDEXNAME");
			reorder = new ReorderOperator(colOrder, meta);
			reorder.add(hash);
			orders = new ArrayList<Boolean>();
			orders.add(true);
			sort = new SortOperator(colOrder, orders, meta);
			sort.add(reorder);
			root = new RootOperator(meta);
			root.add(sort);
			trees = new ArrayList<Operator>();
			trees.add(root);
			p = new Plan(true, trees);
			addPlan("SELECT INDEXNAME FROM SYS.TABLES A, SYS.INDEXES B WHERE A.SCHEMA = ? AND A.TABNAME = ? AND A.TABLEID = B.TABLEID AND B.UNIQUE = 'Y' ORDER BY INDEXNAME", p);

			// getIndexColsForTable
			meta = new MetaData();
			keys = new ArrayList<String>();
			types = new ArrayList<String>();
			orders = new ArrayList<Boolean>();
			keys.add("TABLES.SCHEMA");
			keys.add("TABLES.TABNAME");
			types.add("CHAR");
			types.add("CHAR");
			orders.add(true);
			orders.add(true);
			index = new Index("SYS.PKTABLES.indx", keys, types, orders);
			iOp = new IndexOperator(index, meta);
			iOp.setNode(-1);
			iOp.setDevice(0);
			tOp = new TableScanOperator("SYS", "TABLES", meta, tx);
			devs = new ArrayList<Integer>();
			devs.add(0);
			tOp.addActiveDevices(devs);
			tOp.setChildForDevice(0, iOp);
			tOp.setAlias("A");
			needed = new ArrayList<String>();
			needed.add("A.TABLEID");
			tOp.setNeededCols(needed);
			tOp.setNode(-1);
			tOp.setPhase2Done();
			tOp.add(iOp);
			tOp2 = new TableScanOperator("SYS", "INDEXCOLS", meta, tx);
			devs = new ArrayList<Integer>();
			devs.add(0);
			tOp2.addActiveDevices(devs);
			tOp2.setAlias("B");
			needed = new ArrayList<String>();
			needed.add("B.TABLEID");
			needed.add("B.COLID");
			tOp2.setNeededCols(needed);
			tOp2.setNode(-1);
			tOp2.setPhase2Done();
			hash = new HashJoinOperator("A.TABLEID", "B.TABLEID", meta);
			hash.add(tOp);
			hash.add(tOp2);
			TableScanOperator tOp3 = new TableScanOperator("SYS", "COLUMNS", meta, tx);
			devs = new ArrayList<Integer>();
			devs.add(0);
			tOp3.addActiveDevices(devs);
			tOp3.setAlias("C");
			needed = new ArrayList<String>();
			needed.add("C.TABLEID");
			needed.add("C.COLID");
			needed.add("C.COLNAME");
			tOp3.setNeededCols(needed);
			tOp3.setNode(-1);
			tOp3.setPhase2Done();
			HashJoinOperator hash2 = new HashJoinOperator("B.TABLEID", "C.TABLEID", meta);
			hash2.addFilter("B.COLID", "C.COLID");
			hash2.add(hash);
			hash2.add(tOp3);
			colOrder = new ArrayList<String>();
			colOrder.add("C.COLNAME");
			reorder = new ReorderOperator(colOrder, meta);
			reorder.add(hash2);
			root = new RootOperator(meta);
			root.add(reorder);
			trees = new ArrayList<Operator>();
			trees.add(root);
			p = new Plan(true, trees);
			addPlan("SELECT COLNAME FROM SYS.TABLES A, SYS.INDEXCOLS B, SYS.COLUMNS C WHERE A.SCHEMA = ? AND A.TABNAME = ? AND A.TABLEID = B.TABLEID AND B.TABLEID = C.TABLEID AND B.COLID = C.COLID", p);

			// getKeys
			meta = new MetaData();
			keys = new ArrayList<String>();
			types = new ArrayList<String>();
			orders = new ArrayList<Boolean>();
			keys.add("INDEXES.INDEXNAME");
			types.add("CHAR");
			orders.add(true);
			index = new Index("SYS.SKINDEXES.indx", keys, types, orders);
			iOp = new IndexOperator(index, meta);
			iOp.setNode(-1);
			iOp.setDevice(0);
			tOp = new TableScanOperator("SYS", "INDEXES", meta, tx);
			devs = new ArrayList<Integer>();
			devs.add(0);
			tOp.addActiveDevices(devs);
			tOp.setChildForDevice(0, iOp);
			tOp.setAlias("A");
			needed = new ArrayList<String>();
			needed.add("A.INDEXNAME");
			needed.add("A.INDEXID");
			needed.add("A.TABLEID");
			tOp.setNeededCols(needed);
			tOp.setNode(-1);
			tOp.setPhase2Done();
			tOp.add(iOp);
			keys = new ArrayList<String>();
			types = new ArrayList<String>();
			orders = new ArrayList<Boolean>();
			keys.add("TABLES.SCHEMA");
			keys.add("TABLES.TABNAME");
			types.add("CHAR");
			types.add("CHAR");
			orders.add(true);
			orders.add(true);
			Index index2 = new Index("SYS.PKTABLES.indx", keys, types, orders);
			IndexOperator iOp2 = new IndexOperator(index2, meta);
			iOp2.setNode(-1);
			iOp2.setDevice(0);
			tOp2 = new TableScanOperator("SYS", "TABLES", meta, tx);
			devs = new ArrayList<Integer>();
			devs.add(0);
			tOp2.addActiveDevices(devs);
			tOp2.setChildForDevice(0, iOp2);
			tOp2.setAlias("B");
			needed = new ArrayList<String>();
			needed.add("B.TABNAME");
			needed.add("B.TABLEID");
			tOp2.setNeededCols(needed);
			tOp2.setNode(-1);
			tOp2.setPhase2Done();
			tOp2.add(iOp2);
			hash = new HashJoinOperator("A.TABLEID", "B.TABLEID", meta);
			hash.add(tOp);
			hash.add(tOp2);
			tOp3 = new TableScanOperator("SYS", "INDEXCOLS", meta, tx);
			devs = new ArrayList<Integer>();
			devs.add(0);
			tOp3.addActiveDevices(devs);
			tOp3.setAlias("C");
			needed = new ArrayList<String>();
			needed.add("C.TABLEID");
			needed.add("C.INDEXID");
			needed.add("C.COLID");
			needed.add("C.POSITION");
			tOp3.setNeededCols(needed);
			tOp3.setNode(-1);
			tOp3.setPhase2Done();
			hash2 = new HashJoinOperator("A.TABLEID", "C.TABLEID", meta);
			hash2.addFilter("A.INDEXID", "C.INDEXID");
			hash2.add(hash);
			hash2.add(tOp3);
			TableScanOperator tOp4 = new TableScanOperator("SYS", "COLUMNS", meta, tx);
			devs = new ArrayList<Integer>();
			devs.add(0);
			tOp4.addActiveDevices(devs);
			tOp4.setAlias("D");
			needed = new ArrayList<String>();
			needed.add("D.COLNAME");
			needed.add("D.COLID");
			needed.add("D.TABLEID");
			tOp4.setNeededCols(needed);
			tOp4.setNode(-1);
			tOp4.setPhase2Done();
			HashJoinOperator hash3 = new HashJoinOperator("C.TABLEID", "D.TABLEID", meta);
			hash3.addFilter("C.COLID", "D.COLID");
			hash3.add(hash2);
			hash3.add(tOp4);
			ArrayList<String> sortCols = new ArrayList<String>();
			sortCols.add("C.POSITION");
			ArrayList<Boolean> sortOrders = new ArrayList<Boolean>();
			sortOrders.add(true);
			sort = new SortOperator(sortCols, sortOrders, meta);
			sort.add(hash3);
			colOrder = new ArrayList<String>();
			colOrder.add("B.TABNAME");
			colOrder.add("D.COLNAME");
			reorder = new ReorderOperator(colOrder, meta);
			reorder.add(sort);
			root = new RootOperator(meta);
			root.add(reorder);
			trees = new ArrayList<Operator>();
			trees.add(root);
			p = new Plan(true, trees);
			addPlan("SELECT TABNAME, COLNAME FROM SYS.INDEXES A, SYS.TABLES B, SYS.INDEXCOLS C, SYS.COLUMNS D WHERE A.INDEXNAME = ? AND B.SCHEMA = ? AND A.TABLEID = B.TABLEID AND A.TABLEID = C.TABLEID AND A.INDEXID = C.INDEXID AND C.TABLEID = D.TABLEID AND C.COLID = D.COLID", p);

			// getKeysByID
			meta = new MetaData();
			keys = new ArrayList<String>();
			types = new ArrayList<String>();
			orders = new ArrayList<Boolean>();
			keys.add("INDEXCOLS.TABLEID");
			keys.add("INDEXCOLS.INDEXID");
			keys.add("INDEXCOLS.COLID");
			types.add("INT");
			types.add("INT");
			types.add("INT");
			orders.add(true);
			orders.add(true);
			orders.add(true);
			index = new Index("SYS.PKINDEXCOLS.indx", keys, types, orders);
			iOp = new IndexOperator(index, meta);
			iOp.setNode(-1);
			iOp.setDevice(0);
			tOp = new TableScanOperator("SYS", "INDEXCOLS", meta, tx);
			devs = new ArrayList<Integer>();
			devs.add(0);
			tOp.addActiveDevices(devs);
			tOp.setChildForDevice(0, iOp);
			tOp.setAlias("A");
			needed = new ArrayList<String>();
			needed.add("A.TABLEID");
			needed.add("A.INDEXID");
			needed.add("A.COLID");
			needed.add("A.POSITION");
			tOp.setNeededCols(needed);
			tOp.setNode(-1);
			tOp.setPhase2Done();
			tOp.add(iOp);
			tOp2 = new TableScanOperator("SYS", "COLUMNS", meta, tx);
			devs = new ArrayList<Integer>();
			devs.add(0);
			tOp2.addActiveDevices(devs);
			tOp2.setAlias("B");
			needed = new ArrayList<String>();
			needed.add("B.COLNAME");
			needed.add("B.COLID");
			needed.add("B.TABLEID");
			tOp2.setNeededCols(needed);
			tOp2.setNode(-1);
			tOp2.setPhase2Done();
			hash = new HashJoinOperator("A.TABLEID", "B.TABLEID", meta);
			hash.addFilter("A.COLID", "B.COLID");
			hash.add(tOp);
			hash.add(tOp2);
			sortCols = new ArrayList<String>();
			sortCols.add("A.POSITION");
			sortOrders = new ArrayList<Boolean>();
			sortOrders.add(true);
			sort = new SortOperator(sortCols, sortOrders, meta);
			sort.add(hash);
			colOrder = new ArrayList<String>();
			colOrder.add("B.COLNAME");
			reorder = new ReorderOperator(colOrder, meta);
			reorder.add(sort);
			root = new RootOperator(meta);
			root.add(reorder);
			trees = new ArrayList<Operator>();
			trees.add(root);
			p = new Plan(true, trees);
			addPlan("SELECT COLNAME FROM SYS.INDEXCOLS A, SYS.COLUMNS B WHERE A.TABLEID = B.TABLEID A.COLID = B.COLID AND A.TABLEID = ? AND A.INDEXID = ?", p);

			// getTypes
			meta = new MetaData();
			keys = new ArrayList<String>();
			types = new ArrayList<String>();
			orders = new ArrayList<Boolean>();
			keys.add("INDEXES.INDEXNAME");
			types.add("CHAR");
			orders.add(true);
			index = new Index("SYS.SKINDEXES.indx", keys, types, orders);
			iOp = new IndexOperator(index, meta);
			iOp.setNode(-1);
			iOp.setDevice(0);
			tOp = new TableScanOperator("SYS", "INDEXES", meta, tx);
			devs = new ArrayList<Integer>();
			devs.add(0);
			tOp.addActiveDevices(devs);
			tOp.setChildForDevice(0, iOp);
			tOp.setAlias("A");
			needed = new ArrayList<String>();
			needed.add("A.INDEXNAME");
			needed.add("A.INDEXID");
			needed.add("A.TABLEID");
			tOp.setNeededCols(needed);
			tOp.setNode(-1);
			tOp.setPhase2Done();
			tOp.add(iOp);
			keys = new ArrayList<String>();
			types = new ArrayList<String>();
			orders = new ArrayList<Boolean>();
			keys.add("TABLES.SCHEMA");
			keys.add("TABLES.TABNAME");
			types.add("CHAR");
			types.add("CHAR");
			orders.add(true);
			orders.add(true);
			index2 = new Index("SYS.PKTABLES.indx", keys, types, orders);
			iOp2 = new IndexOperator(index2, meta);
			iOp2.setNode(-1);
			iOp2.setDevice(0);
			tOp2 = new TableScanOperator("SYS", "TABLES", meta, tx);
			devs = new ArrayList<Integer>();
			devs.add(0);
			tOp2.addActiveDevices(devs);
			tOp2.setChildForDevice(0, iOp2);
			tOp2.setAlias("B");
			needed = new ArrayList<String>();
			needed.add("B.TABLEID");
			tOp2.setNeededCols(needed);
			tOp2.setNode(-1);
			tOp2.setPhase2Done();
			tOp2.add(iOp2);
			hash = new HashJoinOperator("A.TABLEID", "B.TABLEID", meta);
			hash.add(tOp);
			hash.add(tOp2);
			tOp3 = new TableScanOperator("SYS", "INDEXCOLS", meta, tx);
			devs = new ArrayList<Integer>();
			devs.add(0);
			tOp3.addActiveDevices(devs);
			tOp3.setAlias("C");
			needed = new ArrayList<String>();
			needed.add("C.TABLEID");
			needed.add("C.INDEXID");
			needed.add("C.COLID");
			needed.add("C.POSITION");
			tOp3.setNeededCols(needed);
			tOp3.setNode(-1);
			tOp3.setPhase2Done();
			hash2 = new HashJoinOperator("A.TABLEID", "C.TABLEID", meta);
			hash2.addFilter("A.INDEXID", "C.INDEXID");
			hash2.add(hash);
			hash2.add(tOp3);
			tOp4 = new TableScanOperator("SYS", "COLUMNS", meta, tx);
			devs = new ArrayList<Integer>();
			devs.add(0);
			tOp4.addActiveDevices(devs);
			tOp4.setAlias("D");
			needed = new ArrayList<String>();
			needed.add("D.COLTYPE");
			needed.add("D.COLID");
			needed.add("D.TABLEID");
			tOp4.setNeededCols(needed);
			tOp4.setNode(-1);
			tOp4.setPhase2Done();
			hash3 = new HashJoinOperator("C.TABLEID", "D.TABLEID", meta);
			hash3.addFilter("C.COLID", "D.COLID");
			hash3.add(hash2);
			hash3.add(tOp4);
			sortCols = new ArrayList<String>();
			sortCols.add("C.POSITION");
			sortOrders = new ArrayList<Boolean>();
			sortOrders.add(true);
			sort = new SortOperator(sortCols, sortOrders, meta);
			sort.add(hash3);
			colOrder = new ArrayList<String>();
			colOrder.add("D.COLTYPE");
			reorder = new ReorderOperator(colOrder, meta);
			reorder.add(sort);
			root = new RootOperator(meta);
			root.add(reorder);
			trees = new ArrayList<Operator>();
			trees.add(root);
			p = new Plan(true, trees);
			addPlan("SELECT COLTYPE FROM SYS.INDEXES A, SYS.TABLES B, SYS.INDEXCOLS C, SYS.COLUMNS D WHERE A.INDEXNAME = ? AND B.SCHEMA = ? AND A.TABLEID = B.TABLEID AND A.TABLEID = C.TABLEID AND A.INDEXID = C.INDEXID AND C.TABLEID = D.TABLEID AND C.COLID = D.COLID", p);

			// getOrders
			meta = new MetaData();
			keys = new ArrayList<String>();
			types = new ArrayList<String>();
			orders = new ArrayList<Boolean>();
			keys.add("INDEXES.INDEXNAME");
			types.add("CHAR");
			orders.add(true);
			index = new Index("SYS.SKINDEXES.indx", keys, types, orders);
			iOp = new IndexOperator(index, meta);
			iOp.setNode(-1);
			iOp.setDevice(0);
			tOp = new TableScanOperator("SYS", "INDEXES", meta, tx);
			devs = new ArrayList<Integer>();
			devs.add(0);
			tOp.addActiveDevices(devs);
			tOp.setChildForDevice(0, iOp);
			tOp.setAlias("A");
			needed = new ArrayList<String>();
			needed.add("A.INDEXNAME");
			needed.add("A.INDEXID");
			needed.add("A.TABLEID");
			tOp.setNeededCols(needed);
			tOp.setNode(-1);
			tOp.setPhase2Done();
			tOp.add(iOp);
			keys = new ArrayList<String>();
			types = new ArrayList<String>();
			orders = new ArrayList<Boolean>();
			keys.add("TABLES.SCHEMA");
			keys.add("TABLES.TABNAME");
			types.add("CHAR");
			types.add("CHAR");
			orders.add(true);
			orders.add(true);
			index2 = new Index("SYS.PKTABLES.indx", keys, types, orders);
			iOp2 = new IndexOperator(index2, meta);
			iOp2.setNode(-1);
			iOp2.setDevice(0);
			tOp2 = new TableScanOperator("SYS", "TABLES", meta, tx);
			devs = new ArrayList<Integer>();
			devs.add(0);
			tOp2.addActiveDevices(devs);
			tOp2.setChildForDevice(0, iOp2);
			tOp2.setAlias("B");
			needed = new ArrayList<String>();
			needed.add("B.TABLEID");
			tOp2.setNeededCols(needed);
			tOp2.setNode(-1);
			tOp2.setPhase2Done();
			tOp2.add(iOp2);
			hash = new HashJoinOperator("A.TABLEID", "B.TABLEID", meta);
			hash.add(tOp);
			hash.add(tOp2);
			tOp3 = new TableScanOperator("SYS", "INDEXCOLS", meta, tx);
			devs = new ArrayList<Integer>();
			devs.add(0);
			tOp3.addActiveDevices(devs);
			tOp3.setAlias("C");
			needed = new ArrayList<String>();
			needed.add("C.TABLEID");
			needed.add("C.INDEXID");
			needed.add("C.POSITION");
			needed.add("C.ORDER");
			tOp3.setNeededCols(needed);
			tOp3.setNode(-1);
			tOp3.setPhase2Done();
			hash2 = new HashJoinOperator("A.TABLEID", "C.TABLEID", meta);
			hash2.addFilter("A.INDEXID", "C.INDEXID");
			hash2.add(hash);
			hash2.add(tOp3);
			sortCols = new ArrayList<String>();
			sortCols.add("C.POSITION");
			sortOrders = new ArrayList<Boolean>();
			sortOrders.add(true);
			sort = new SortOperator(sortCols, sortOrders, meta);
			sort.add(hash2);
			colOrder = new ArrayList<String>();
			colOrder.add("C.ORDER");
			reorder = new ReorderOperator(colOrder, meta);
			reorder.add(sort);
			root = new RootOperator(meta);
			root.add(reorder);
			trees = new ArrayList<Operator>();
			trees.add(root);
			p = new Plan(true, trees);
			addPlan("SELECT ORDER FROM SYS.INDEXES A, SYS.TABLES B, SYS.INDEXCOLS C WHERE A.INDEXNAME = ? AND B.SCHEMA = ? AND A.TABLEID = B.TABLEID AND A.TABLEID = C.TABLEID AND A.INDEXID = C.INDEXID", p);

			// getColCard
			meta = new MetaData();
			keys = new ArrayList<String>();
			types = new ArrayList<String>();
			orders = new ArrayList<Boolean>();
			keys.add("TABLES.SCHEMA");
			keys.add("TABLES.TABNAME");
			types.add("CHAR");
			types.add("CHAR");
			orders.add(true);
			orders.add(true);
			index = new Index("SYS.PKTABLES.indx", keys, types, orders);
			iOp = new IndexOperator(index, meta);
			iOp.setNode(-1);
			iOp.setDevice(0);
			tOp = new TableScanOperator("SYS", "TABLES", meta, tx);
			devs = new ArrayList<Integer>();
			devs.add(0);
			tOp.addActiveDevices(devs);
			tOp.setChildForDevice(0, iOp);
			tOp.setAlias("A");
			needed = new ArrayList<String>();
			needed.add("A.TABLEID");
			tOp.setNeededCols(needed);
			tOp.setNode(-1);
			tOp.setPhase2Done();
			tOp.add(iOp);
			tOp2 = new TableScanOperator("SYS", "COLUMNS", meta, tx);
			devs = new ArrayList<Integer>();
			devs.add(0);
			tOp2.addActiveDevices(devs);
			tOp2.setAlias("B");
			needed = new ArrayList<String>();
			needed.add("B.TABLEID");
			needed.add("B.COLNAME");
			needed.add("B.COLID");
			tOp2.setNeededCols(needed);
			tOp2.setNode(-1);
			tOp2.setPhase2Done();
			hash = new HashJoinOperator("A.TABLEID", "B.TABLEID", meta);
			hash.add(tOp);
			hash.add(tOp2);
			select = new SelectOperator(new Filter("B.COLNAME", "E", "COLNAME"), meta);
			select.add(hash);
			tOp3 = new TableScanOperator("SYS", "COLSTATS", meta, tx);
			devs = new ArrayList<Integer>();
			devs.add(0);
			tOp3.addActiveDevices(devs);
			tOp3.setAlias("C");
			needed = new ArrayList<String>();
			needed.add("C.TABLEID");
			needed.add("C.COLID");
			needed.add("C.CARD");
			tOp3.setNeededCols(needed);
			tOp3.setNode(-1);
			tOp3.setPhase2Done();
			hash2 = new HashJoinOperator("B.TABLEID", "C.TABLEID", meta);
			hash2.addFilter("B.COLID", "C.COLID");
			hash2.add(select);
			hash2.add(tOp3);
			colOrder = new ArrayList<String>();
			colOrder.add("C.CARD");
			reorder = new ReorderOperator(colOrder, meta);
			reorder.add(hash2);
			root = new RootOperator(meta);
			root.add(reorder);
			trees = new ArrayList<Operator>();
			trees.add(root);
			p = new Plan(true, trees);
			addPlan("SELECT CARD FROM SYS.TABLES A, SYS.COLUMNS B, SYS.COLSTATS C WHERE A.SCHEMA = ? AND A.TABNAME = ? AND A.TABLEID = B.TABLEID AND B.COLNAME = ? AND B.TABLEID = C.TABLEID AND B.COLID = C.COLID", p);

			// getIndexIDsForTable
			meta = new MetaData();
			keys = new ArrayList<String>();
			types = new ArrayList<String>();
			orders = new ArrayList<Boolean>();
			keys.add("TABLES.SCHEMA");
			keys.add("TABLES.TABNAME");
			types.add("CHAR");
			types.add("CHAR");
			orders.add(true);
			orders.add(true);
			index = new Index("SYS.PKTABLES.indx", keys, types, orders);
			iOp = new IndexOperator(index, meta);
			iOp.setNode(-1);
			iOp.setDevice(0);
			tOp = new TableScanOperator("SYS", "TABLES", meta, tx);
			devs = new ArrayList<Integer>();
			devs.add(0);
			tOp.addActiveDevices(devs);
			tOp.setChildForDevice(0, iOp);
			tOp.setAlias("A");
			needed = new ArrayList<String>();
			needed.add("A.TABLEID");
			tOp.setNeededCols(needed);
			tOp.setNode(-1);
			tOp.setPhase2Done();
			tOp.add(iOp);
			tOp2 = new TableScanOperator("SYS", "INDEXES", meta, tx);
			devs = new ArrayList<Integer>();
			devs.add(0);
			tOp2.addActiveDevices(devs);
			tOp2.setAlias("B");
			needed = new ArrayList<String>();
			needed.add("B.TABLEID");
			needed.add("B.INDEXID");
			tOp2.setNeededCols(needed);
			tOp2.setNode(-1);
			tOp2.setPhase2Done();
			hash = new HashJoinOperator("A.TABLEID", "B.TABLEID", meta);
			hash.add(tOp);
			hash.add(tOp2);
			colOrder = new ArrayList<String>();
			colOrder.add("B.INDEXID");
			reorder = new ReorderOperator(colOrder, meta);
			reorder.add(hash);
			root = new RootOperator(meta);
			root.add(reorder);
			trees = new ArrayList<Operator>();
			trees.add(root);
			p = new Plan(true, trees);
			addPlan("SELECT INDEXID FROM SYS.TABLES A, SYS.INDEXES B WHERE A.SCHEMA = ? AND A.TABNAME = ? AND A.TABLEID = B.TABLEID", p);

			// getPartitioning
			meta = new MetaData();
			keys = new ArrayList<String>();
			types = new ArrayList<String>();
			orders = new ArrayList<Boolean>();
			keys.add("TABLES.SCHEMA");
			keys.add("TABLES.TABNAME");
			types.add("CHAR");
			types.add("CHAR");
			orders.add(true);
			orders.add(true);
			index = new Index("SYS.PKTABLES.indx", keys, types, orders);
			iOp = new IndexOperator(index, meta);
			iOp.setNode(-1);
			iOp.setDevice(0);
			tOp = new TableScanOperator("SYS", "TABLES", meta, tx);
			devs = new ArrayList<Integer>();
			devs.add(0);
			tOp.addActiveDevices(devs);
			tOp.setChildForDevice(0, iOp);
			tOp.setAlias("A");
			needed = new ArrayList<String>();
			needed.add("A.TABLEID");
			tOp.setNeededCols(needed);
			tOp.setNode(-1);
			tOp.setPhase2Done();
			tOp.add(iOp);
			tOp2 = new TableScanOperator("SYS", "PARTITIONING", meta, tx);
			devs = new ArrayList<Integer>();
			devs.add(0);
			tOp2.addActiveDevices(devs);
			tOp2.setAlias("B");
			needed = new ArrayList<String>();
			needed.add("B.TABLEID");
			needed.add("B.GROUPEXP");
			needed.add("B.NODEEXP");
			needed.add("B.DEVICEEXP");
			tOp2.setNeededCols(needed);
			tOp2.setNode(-1);
			tOp2.setPhase2Done();
			hash = new HashJoinOperator("A.TABLEID", "B.TABLEID", meta);
			hash.add(tOp);
			hash.add(tOp2);
			colOrder = new ArrayList<String>();
			colOrder.add("B.GROUPEXP");
			colOrder.add("B.NODEEXP");
			colOrder.add("B.DEVICEEXP");
			reorder = new ReorderOperator(colOrder, meta);
			reorder.add(hash);
			root = new RootOperator(meta);
			root.add(reorder);
			trees = new ArrayList<Operator>();
			trees.add(root);
			p = new Plan(true, trees);
			addPlan("SELECT GROUPEXP, NODEEXP, DEVICEEXP FROM SYS.TABLES A, SYS.PARTITIONING B WHERE A.SCHEMA = ? AND A.TABNAME = ? AND A.TABLEID = B.TABLEID", p);

			// getIndexColCount
			meta = new MetaData();
			keys = new ArrayList<String>();
			types = new ArrayList<String>();
			orders = new ArrayList<Boolean>();
			keys.add("INDEXCOLS.TABLEID");
			keys.add("INDEXCOLS.INDEXID");
			keys.add("INDEXCOLS.COLID");
			types.add("INT");
			types.add("INT");
			types.add("INT");
			orders.add(true);
			orders.add(true);
			orders.add(true);
			index = new Index("SYS.PKINDEXCOLS.indx", keys, types, orders);
			iOp = new IndexOperator(index, meta);
			iOp.setNode(-1);
			iOp.setDevice(0);
			tOp = new TableScanOperator("SYS", "INDEXCOLS", meta, tx);
			devs = new ArrayList<Integer>();
			devs.add(0);
			tOp.addActiveDevices(devs);
			tOp.setChildForDevice(0, iOp);
			needed = new ArrayList<String>();
			needed.add("INDEXCOLS.TABLEID");
			needed.add("INDEXCOLS.INDEXID");
			needed.add("INDEXCOLS.COLID");
			tOp.setNeededCols(needed);
			tOp.setNode(-1);
			tOp.setPhase2Done();
			tOp.add(iOp);
			ops = new ArrayList<AggregateOperator>();
			ops.add(new CountOperator("._E1", meta));
			groupCols = new ArrayList<String>();
			multi = new MultiOperator(ops, groupCols, meta, false);
			multi.add(tOp);
			colOrder = new ArrayList<String>();
			colOrder.add("._E1");
			reorder = new ReorderOperator(colOrder, meta);
			reorder.add(multi);
			root = new RootOperator(meta);
			root.add(reorder);
			trees = new ArrayList<Operator>();
			trees.add(root);
			p = new Plan(true, trees);
			addPlan("SELECT COUNT(*) FROM SYS.INDEXCOLS WHERE TABLEID = ? AND INDEXID = ?", p);

			// getNextTableID
			meta = new MetaData();
			tOp = new TableScanOperator("SYS", "TABLES", meta, tx);
			devs = new ArrayList<Integer>();
			devs.add(0);
			tOp.addActiveDevices(devs);
			needed = new ArrayList<String>();
			needed.add("TABLES.TABLEID");
			tOp.setNeededCols(needed);
			tOp.setNode(-1);
			tOp.setPhase2Done();
			ops = new ArrayList<AggregateOperator>();
			ops.add(new MaxOperator("TABLES.TABLEID", "._E1", meta, true));
			groupCols = new ArrayList<String>();
			multi = new MultiOperator(ops, groupCols, meta, false);
			multi.add(tOp);
			colOrder = new ArrayList<String>();
			colOrder.add("._E1");
			reorder = new ReorderOperator(colOrder, meta);
			reorder.add(multi);
			root = new RootOperator(meta);
			root.add(reorder);
			trees = new ArrayList<Operator>();
			trees.add(root);
			p = new Plan(true, trees);
			addPlan("SELECT MAX(TABLEID) FROM SYS.TABLES", p);

			// getNextViewID
			meta = new MetaData();
			tOp = new TableScanOperator("SYS", "VIEWS", meta, tx);
			devs = new ArrayList<Integer>();
			devs.add(0);
			tOp.addActiveDevices(devs);
			needed = new ArrayList<String>();
			needed.add("VIEWS.VIEWID");
			tOp.setNeededCols(needed);
			tOp.setNode(-1);
			tOp.setPhase2Done();
			ops = new ArrayList<AggregateOperator>();
			ops.add(new MaxOperator("VIEWS.VIEWID", "._E1", meta, true));
			groupCols = new ArrayList<String>();
			multi = new MultiOperator(ops, groupCols, meta, false);
			multi.add(tOp);
			colOrder = new ArrayList<String>();
			colOrder.add("._E1");
			reorder = new ReorderOperator(colOrder, meta);
			reorder.add(multi);
			root = new RootOperator(meta);
			root.add(reorder);
			trees = new ArrayList<Operator>();
			trees.add(root);
			p = new Plan(true, trees);
			addPlan("SELECT MAX(VIEWID) FROM SYS.VIEWS", p);

			// getCheckIndexForCol
			meta = new MetaData();
			keys = new ArrayList<String>();
			types = new ArrayList<String>();
			orders = new ArrayList<Boolean>();
			keys.add("INDEXCOLS.TABLEID");
			keys.add("INDEXCOLS.INDEXID");
			keys.add("INDEXCOLS.COLID");
			types.add("INT");
			types.add("INT");
			types.add("INT");
			orders.add(true);
			orders.add(true);
			orders.add(true);
			index = new Index("SYS.PKINDEXCOLS.indx", keys, types, orders);
			iOp = new IndexOperator(index, meta);
			iOp.setNode(-1);
			iOp.setDevice(0);
			tOp = new TableScanOperator("SYS", "INDEXCOLS", meta, tx);
			devs = new ArrayList<Integer>();
			devs.add(0);
			tOp.addActiveDevices(devs);
			tOp.setChildForDevice(0, iOp);
			needed = new ArrayList<String>();
			needed.add("INDEXCOLS.TABLEID");
			needed.add("INDEXCOLS.INDEXID");
			needed.add("INDEXCOLS.COLID");
			tOp.setNeededCols(needed);
			tOp.setNode(-1);
			tOp.setPhase2Done();
			tOp.add(iOp);
			root = new RootOperator(meta);
			root.add(tOp);
			trees = new ArrayList<Operator>();
			trees.add(root);
			p = new Plan(true, trees);
			addPlan("SELECT TABLEID, INDEXID, COLID FROM SYS.INDEXCOLS WHERE TABLEID = ? AND INDEXID = ? AND COLID = ?", p);

			// getIndexCard
			meta = new MetaData();
			keys = new ArrayList<String>();
			types = new ArrayList<String>();
			orders = new ArrayList<Boolean>();
			keys.add("INDEXSTATS.TABLEID");
			keys.add("INDEXSTATS.INDEXID");
			types.add("INT");
			types.add("INT");
			orders.add(true);
			orders.add(true);
			index = new Index("SYS.PKINDEXSTATS.indx", keys, types, orders);
			iOp = new IndexOperator(index, meta);
			iOp.setNode(-1);
			iOp.setDevice(0);
			tOp = new TableScanOperator("SYS", "INDEXSTATS", meta, tx);
			devs = new ArrayList<Integer>();
			devs.add(0);
			tOp.addActiveDevices(devs);
			tOp.setChildForDevice(0, iOp);
			needed = new ArrayList<String>();
			needed.add("INDEXSTATS.NUMDISTINCT");
			tOp.setNeededCols(needed);
			tOp.setNode(-1);
			tOp.setPhase2Done();
			tOp.add(iOp);
			root = new RootOperator(meta);
			root.add(tOp);
			trees = new ArrayList<Operator>();
			trees.add(root);
			p = new Plan(true, trees);
			addPlan("SELECT NUMDISTINCT FROM SYS.INDEXSTATS WHERE TABLEID = ? AND INDEXID = ?", p);

			// getCols2Pos
			meta = new MetaData();
			keys = new ArrayList<String>();
			types = new ArrayList<String>();
			orders = new ArrayList<Boolean>();
			keys.add("TABLES.SCHEMA");
			keys.add("TABLES.TABNAME");
			types.add("CHAR");
			types.add("CHAR");
			orders.add(true);
			orders.add(true);
			index = new Index("SYS.PKTABLES.indx", keys, types, orders);
			iOp = new IndexOperator(index, meta);
			iOp.setNode(-1);
			iOp.setDevice(0);
			tOp = new TableScanOperator("SYS", "TABLES", meta, tx);
			devs = new ArrayList<Integer>();
			devs.add(0);
			tOp.addActiveDevices(devs);
			tOp.setChildForDevice(0, iOp);
			tOp.setAlias("A");
			needed = new ArrayList<String>();
			needed.add("A.TABLEID");
			tOp.setNeededCols(needed);
			tOp.setNode(-1);
			tOp.setPhase2Done();
			tOp.add(iOp);
			tOp2 = new TableScanOperator("SYS", "COLUMNS", meta, tx);
			devs = new ArrayList<Integer>();
			devs.add(0);
			tOp2.addActiveDevices(devs);
			tOp2.setAlias("B");
			needed = new ArrayList<String>();
			needed.add("B.TABLEID");
			needed.add("B.COLNAME");
			needed.add("B.COLID");
			tOp2.setNeededCols(needed);
			tOp2.setNode(-1);
			tOp2.setPhase2Done();
			hash = new HashJoinOperator("A.TABLEID", "B.TABLEID", meta);
			hash.add(tOp);
			hash.add(tOp2);
			colOrder = new ArrayList<String>();
			colOrder.add("B.COLNAME");
			colOrder.add("B.COLID");
			reorder = new ReorderOperator(colOrder, meta);
			reorder.add(hash);
			root = new RootOperator(meta);
			root.add(reorder);
			trees = new ArrayList<Operator>();
			trees.add(root);
			p = new Plan(true, trees);
			addPlan("SELECT COLNAME, COLID FROM SYS.TABLES A, SYS.COLUMNS B WHERE A.SCHEMA = ? AND B.TABNAME = ? AND A.TABLEID = B.TABLEID", p);

			// getTableCard
			meta = new MetaData();
			keys = new ArrayList<String>();
			types = new ArrayList<String>();
			orders = new ArrayList<Boolean>();
			keys.add("TABLES.SCHEMA");
			keys.add("TABLES.TABNAME");
			types.add("CHAR");
			types.add("CHAR");
			orders.add(true);
			orders.add(true);
			index = new Index("SYS.PKTABLES.indx", keys, types, orders);
			iOp = new IndexOperator(index, meta);
			iOp.setNode(-1);
			iOp.setDevice(0);
			tOp = new TableScanOperator("SYS", "TABLES", meta, tx);
			devs = new ArrayList<Integer>();
			devs.add(0);
			tOp.addActiveDevices(devs);
			tOp.setChildForDevice(0, iOp);
			tOp.setAlias("A");
			needed = new ArrayList<String>();
			needed.add("A.TABLEID");
			tOp.setNeededCols(needed);
			tOp.setNode(-1);
			tOp.setPhase2Done();
			tOp.add(iOp);
			tOp2 = new TableScanOperator("SYS", "TABLESTATS", meta, tx);
			devs = new ArrayList<Integer>();
			devs.add(0);
			tOp2.addActiveDevices(devs);
			tOp2.setAlias("B");
			needed = new ArrayList<String>();
			needed.add("B.TABLEID");
			needed.add("B.CARD");
			tOp2.setNeededCols(needed);
			tOp2.setNode(-1);
			tOp2.setPhase2Done();
			hash = new HashJoinOperator("A.TABLEID", "B.TABLEID", meta);
			hash.add(tOp);
			hash.add(tOp2);
			colOrder = new ArrayList<String>();
			colOrder.add("B.CARD");
			reorder = new ReorderOperator(colOrder, meta);
			reorder.add(hash);
			root = new RootOperator(meta);
			root.add(reorder);
			trees = new ArrayList<Operator>();
			trees.add(root);
			p = new Plan(true, trees);
			addPlan("SELECT CARD FROM SYS.TABLES A, SYS.TABLESTATS B WHERE A.SCHEMA = ? AND A.TABNAME = ? AND A.TABLEID = B.TABLEID", p);

			// getColType
			meta = new MetaData();
			keys = new ArrayList<String>();
			types = new ArrayList<String>();
			orders = new ArrayList<Boolean>();
			keys.add("TABLES.SCHEMA");
			keys.add("TABLES.TABNAME");
			types.add("CHAR");
			types.add("CHAR");
			orders.add(true);
			orders.add(true);
			index = new Index("SYS.PKTABLES.indx", keys, types, orders);
			iOp = new IndexOperator(index, meta);
			iOp.setNode(-1);
			iOp.setDevice(0);
			tOp = new TableScanOperator("SYS", "TABLES", meta, tx);
			devs = new ArrayList<Integer>();
			devs.add(0);
			tOp.addActiveDevices(devs);
			tOp.setChildForDevice(0, iOp);
			tOp.setAlias("A");
			needed = new ArrayList<String>();
			needed.add("A.TABLEID");
			tOp.setNeededCols(needed);
			tOp.setNode(-1);
			tOp.setPhase2Done();
			tOp.add(iOp);
			tOp2 = new TableScanOperator("SYS", "COLUMNS", meta, tx);
			devs = new ArrayList<Integer>();
			devs.add(0);
			tOp2.addActiveDevices(devs);
			tOp2.setAlias("B");
			needed = new ArrayList<String>();
			needed.add("B.TABLEID");
			needed.add("B.COLNAME");
			needed.add("B.COLTYPE");
			tOp2.setNeededCols(needed);
			tOp2.setNode(-1);
			tOp2.setPhase2Done();
			hash = new HashJoinOperator("A.TABLEID", "B.TABLEID", meta);
			hash.add(tOp);
			hash.add(tOp2);
			select = new SelectOperator(new Filter("B.COLNAME", "E", "COLNAME"), meta);
			select.add(hash);
			colOrder = new ArrayList<String>();
			colOrder.add("B.COLTYPE");
			reorder = new ReorderOperator(colOrder, meta);
			reorder.add(select);
			root = new RootOperator(meta);
			root.add(reorder);
			trees = new ArrayList<Operator>();
			trees.add(root);
			p = new Plan(true, trees);
			addPlan("SELECT COLTYPE FROM SYS.TABLES A, SYS.COLUMNS B WHERE A.SCHEMA = ? AND A.TABNAME = ? AND A.TABLEID = B.TABLEID AND B.COLNAME = ?", p);

			// getColDist
			meta = new MetaData();
			keys = new ArrayList<String>();
			types = new ArrayList<String>();
			orders = new ArrayList<Boolean>();
			keys.add("TABLES.SCHEMA");
			keys.add("TABLES.TABNAME");
			types.add("CHAR");
			types.add("CHAR");
			orders.add(true);
			orders.add(true);
			index = new Index("SYS.PKTABLES.indx", keys, types, orders);
			iOp = new IndexOperator(index, meta);
			iOp.setNode(-1);
			iOp.setDevice(0);
			tOp = new TableScanOperator("SYS", "TABLES", meta, tx);
			devs = new ArrayList<Integer>();
			devs.add(0);
			tOp.addActiveDevices(devs);
			tOp.setChildForDevice(0, iOp);
			tOp.setAlias("A");
			needed = new ArrayList<String>();
			needed.add("A.TABLEID");
			tOp.setNeededCols(needed);
			tOp.setNode(-1);
			tOp.setPhase2Done();
			tOp.add(iOp);
			tOp2 = new TableScanOperator("SYS", "COLUMNS", meta, tx);
			devs = new ArrayList<Integer>();
			devs.add(0);
			tOp2.addActiveDevices(devs);
			tOp2.setAlias("B");
			needed = new ArrayList<String>();
			needed.add("B.TABLEID");
			needed.add("B.COLNAME");
			needed.add("B.COLID");
			tOp2.setNeededCols(needed);
			tOp2.setNode(-1);
			tOp2.setPhase2Done();
			hash = new HashJoinOperator("A.TABLEID", "B.TABLEID", meta);
			hash.add(tOp);
			hash.add(tOp2);
			select = new SelectOperator(new Filter("B.COLNAME", "E", "COLNAME"), meta);
			select.add(hash);
			tOp3 = new TableScanOperator("SYS", "COLDIST", meta, tx);
			devs = new ArrayList<Integer>();
			devs.add(0);
			tOp3.addActiveDevices(devs);
			tOp3.setAlias("C");
			needed = new ArrayList<String>();
			needed.add("C.TABLEID");
			needed.add("C.COLID");
			needed.add("C.LOW");
			needed.add("C.Q1");
			needed.add("C.Q2");
			needed.add("C.Q3");
			needed.add("C.HIGH");
			tOp3.setNeededCols(needed);
			tOp3.setNode(-1);
			tOp3.setPhase2Done();
			hash2 = new HashJoinOperator("B.TABLEID", "C.TABLEID", meta);
			hash2.addFilter("B.COLID", "C.COLID");
			hash2.add(select);
			hash2.add(tOp3);
			colOrder = new ArrayList<String>();
			colOrder.add("C.LOW");
			colOrder.add("C.Q1");
			colOrder.add("C.Q2");
			colOrder.add("C.Q3");
			colOrder.add("C.HIGH");
			reorder = new ReorderOperator(colOrder, meta);
			reorder.add(hash2);
			root = new RootOperator(meta);
			root.add(reorder);
			trees = new ArrayList<Operator>();
			trees.add(root);
			p = new Plan(true, trees);
			addPlan("SELECT LOW, Q1, Q2, Q3, HIGH FROM SYS.TABLES A, SYS.COLUMNS B, SYS.COLDIST C WHERE A.SCHEMA = ? AND A.TABNAME = ? AND A.TABLEID = B.TABLEID AND B.COLNAME = ? AND B.TABLEID = C.TABLEID AND B.COLID = C.COLID", p);

			// getVerifyTableExist
			meta = new MetaData();
			keys = new ArrayList<String>();
			types = new ArrayList<String>();
			orders = new ArrayList<Boolean>();
			keys.add("TABLES.SCHEMA");
			keys.add("TABLES.TABNAME");
			types.add("CHAR");
			types.add("CHAR");
			orders.add(true);
			orders.add(true);
			index = new Index("SYS.PKTABLES.indx", keys, types, orders);
			iOp = new IndexOperator(index, meta);
			iOp.setNode(-1);
			iOp.setDevice(0);
			tOp = new TableScanOperator("SYS", "TABLES", meta, tx);
			devs = new ArrayList<Integer>();
			devs.add(0);
			tOp.addActiveDevices(devs);
			tOp.setChildForDevice(0, iOp);
			needed = new ArrayList<String>();
			needed.add("TABLES.TABLEID");
			tOp.setNeededCols(needed);
			tOp.setNode(-1);
			tOp.setPhase2Done();
			tOp.add(iOp);
			root = new RootOperator(meta);
			root.add(tOp);
			trees = new ArrayList<Operator>();
			trees.add(root);
			p = new Plan(true, trees);
			addPlan("SELECT TABLEID FROM SYS.TABLES WHERE SCHEMA = ? AND TABNAME = ?", p);

			// getVerifyViewExist
			meta = new MetaData();
			keys = new ArrayList<String>();
			types = new ArrayList<String>();
			orders = new ArrayList<Boolean>();
			keys.add("VIEWS.SCHEMA");
			keys.add("VIEWS.NAME");
			types.add("CHAR");
			types.add("CHAR");
			orders.add(true);
			orders.add(true);
			index = new Index("SYS.PKVIEWS.indx", keys, types, orders);
			iOp = new IndexOperator(index, meta);
			iOp.setNode(-1);
			iOp.setDevice(0);
			tOp = new TableScanOperator("SYS", "VIEWS", meta, tx);
			devs = new ArrayList<Integer>();
			devs.add(0);
			tOp.addActiveDevices(devs);
			tOp.setChildForDevice(0, iOp);
			needed = new ArrayList<String>();
			needed.add("VIEWS.TEXT");
			tOp.setNeededCols(needed);
			tOp.setNode(-1);
			tOp.setPhase2Done();
			tOp.add(iOp);
			root = new RootOperator(meta);
			root.add(tOp);
			trees = new ArrayList<Operator>();
			trees.add(root);
			p = new Plan(true, trees);
			addPlan("SELECT TEXT FROM SYS.VIEWS WHERE SCHEMA = ? AND NAME = ?", p);

			// getNextIndexID
			meta = new MetaData();
			keys = new ArrayList<String>();
			types = new ArrayList<String>();
			orders = new ArrayList<Boolean>();
			keys.add("INDEXES.TABLEID");
			keys.add("INDEXES.INDEXNAME");
			types.add("INT");
			types.add("CHAR");
			orders.add(true);
			orders.add(true);
			index = new Index("SYS.PKINDEXES.indx", keys, types, orders);
			iOp = new IndexOperator(index, meta);
			iOp.setNode(-1);
			iOp.setDevice(0);
			tOp = new TableScanOperator("SYS", "INDEXES", meta, tx);
			devs = new ArrayList<Integer>();
			devs.add(0);
			tOp.addActiveDevices(devs);
			tOp.setChildForDevice(0, iOp);
			needed = new ArrayList<String>();
			needed.add("INDEXES.INDEXID");
			tOp.setNeededCols(needed);
			tOp.setNode(-1);
			tOp.setPhase2Done();
			tOp.add(iOp);
			ops = new ArrayList<AggregateOperator>();
			ops.add(new MaxOperator("INDEXES.INDEXID", "._E1", meta, true));
			groupCols = new ArrayList<String>();
			multi = new MultiOperator(ops, groupCols, meta, false);
			multi.add(tOp);
			colOrder = new ArrayList<String>();
			colOrder.add("._E1");
			reorder = new ReorderOperator(colOrder, meta);
			reorder.add(multi);
			root = new RootOperator(meta);
			root.add(reorder);
			trees = new ArrayList<Operator>();
			trees.add(root);
			p = new Plan(true, trees);
			addPlan("SELECT MAX(INDEXID) FROM SYS.INDEXES WHERE TABLEID = ?", p);

			// getVerifyIndexExist
			meta = new MetaData();
			keys = new ArrayList<String>();
			types = new ArrayList<String>();
			orders = new ArrayList<Boolean>();
			keys.add("TABLES.SCHEMA");
			keys.add("TABLES.TABNAME");
			types.add("CHAR");
			types.add("CHAR");
			orders.add(true);
			orders.add(true);
			index = new Index("SYS.PKTABLES.indx", keys, types, orders);
			iOp = new IndexOperator(index, meta);
			iOp.setNode(-1);
			iOp.setDevice(0);
			tOp = new TableScanOperator("SYS", "TABLES", meta, tx);
			devs = new ArrayList<Integer>();
			devs.add(0);
			tOp.addActiveDevices(devs);
			tOp.setChildForDevice(0, iOp);
			tOp.setAlias("A");
			needed = new ArrayList<String>();
			needed.add("A.TABLEID");
			tOp.setNeededCols(needed);
			tOp.setNode(-1);
			tOp.setPhase2Done();
			tOp.add(iOp);
			tOp2 = new TableScanOperator("SYS", "INDEXES", meta, tx);
			devs = new ArrayList<Integer>();
			devs.add(0);
			tOp2.addActiveDevices(devs);
			tOp2.setAlias("B");
			needed = new ArrayList<String>();
			needed.add("B.TABLEID");
			needed.add("B.INDEXNAME");
			needed.add("B.INDEXID");
			tOp2.setNeededCols(needed);
			tOp2.setNode(-1);
			tOp2.setPhase2Done();
			hash = new HashJoinOperator("A.TABLEID", "B.TABLEID", meta);
			hash.add(tOp);
			hash.add(tOp2);
			select = new SelectOperator(new Filter("B.INDEXNAME", "E", "NAME"), meta);
			select.add(hash);
			colOrder = new ArrayList<String>();
			colOrder.add("B.TABLEID");
			colOrder.add("B.INDEXID");
			reorder = new ReorderOperator(colOrder, meta);
			reorder.add(select);
			root = new RootOperator(meta);
			root.add(reorder);
			trees = new ArrayList<Operator>();
			trees.add(root);
			p = new Plan(true, trees);
			addPlan("SELECT TABLEID, INDEXID FROM SYS.TABLES A, SYS.INDEXES B WHERE A.SCHEMA = ? AND A.TABLEID = B.TABLEID AND B.INDEXNAME = ?", p);

			// getCols2Types
			meta = new MetaData();
			keys = new ArrayList<String>();
			types = new ArrayList<String>();
			orders = new ArrayList<Boolean>();
			keys.add("TABLES.SCHEMA");
			keys.add("TABLES.TABNAME");
			types.add("CHAR");
			types.add("CHAR");
			orders.add(true);
			orders.add(true);
			index = new Index("SYS.PKTABLES.indx", keys, types, orders);
			iOp = new IndexOperator(index, meta);
			iOp.setNode(-1);
			iOp.setDevice(0);
			tOp = new TableScanOperator("SYS", "TABLES", meta, tx);
			devs = new ArrayList<Integer>();
			devs.add(0);
			tOp.addActiveDevices(devs);
			tOp.setChildForDevice(0, iOp);
			tOp.setAlias("A");
			needed = new ArrayList<String>();
			needed.add("A.TABLEID");
			tOp.setNeededCols(needed);
			tOp.setNode(-1);
			tOp.setPhase2Done();
			tOp.add(iOp);
			tOp2 = new TableScanOperator("SYS", "COLUMNS", meta, tx);
			devs = new ArrayList<Integer>();
			devs.add(0);
			tOp2.addActiveDevices(devs);
			tOp2.setAlias("B");
			needed = new ArrayList<String>();
			needed.add("B.TABLEID");
			needed.add("B.COLNAME");
			needed.add("B.COLTYPE");
			tOp2.setNeededCols(needed);
			tOp2.setNode(-1);
			tOp2.setPhase2Done();
			hash = new HashJoinOperator("A.TABLEID", "B.TABLEID", meta);
			hash.add(tOp);
			hash.add(tOp2);
			colOrder = new ArrayList<String>();
			colOrder.add("B.COLNAME");
			colOrder.add("B.COLTYPE");
			reorder = new ReorderOperator(colOrder, meta);
			reorder.add(hash);
			root = new RootOperator(meta);
			root.add(reorder);
			trees = new ArrayList<Operator>();
			trees.add(root);
			p = new Plan(true, trees);
			addPlan("SELECT COLNAME, COLTYPE FROM SYS.TABLES A, SYS.COLUMNS B WHERE A.SCHEMA = ? AND B.TABNAME = ? AND A.TABLEID = B.TABLEID", p);

			// getLength
			meta = new MetaData();
			keys = new ArrayList<String>();
			types = new ArrayList<String>();
			orders = new ArrayList<Boolean>();
			keys.add("TABLES.SCHEMA");
			keys.add("TABLES.TABNAME");
			types.add("CHAR");
			types.add("CHAR");
			orders.add(true);
			orders.add(true);
			index = new Index("SYS.PKTABLES.indx", keys, types, orders);
			iOp = new IndexOperator(index, meta);
			iOp.setNode(-1);
			iOp.setDevice(0);
			tOp = new TableScanOperator("SYS", "TABLES", meta, tx);
			devs = new ArrayList<Integer>();
			devs.add(0);
			tOp.addActiveDevices(devs);
			tOp.setChildForDevice(0, iOp);
			tOp.setAlias("A");
			needed = new ArrayList<String>();
			needed.add("A.TABLEID");
			tOp.setNeededCols(needed);
			tOp.setNode(-1);
			tOp.setPhase2Done();
			tOp.add(iOp);
			tOp2 = new TableScanOperator("SYS", "COLUMNS", meta, tx);
			devs = new ArrayList<Integer>();
			devs.add(0);
			tOp2.addActiveDevices(devs);
			tOp2.setAlias("B");
			needed = new ArrayList<String>();
			needed.add("B.TABLEID");
			needed.add("B.COLNAME");
			needed.add("B.LENGTH");
			tOp2.setNeededCols(needed);
			tOp2.setNode(-1);
			tOp2.setPhase2Done();
			hash = new HashJoinOperator("A.TABLEID", "B.TABLEID", meta);
			hash.add(tOp);
			hash.add(tOp2);
			select = new SelectOperator(new Filter("B.COLNAME", "E", "NAME"), meta);
			select.add(hash);
			colOrder = new ArrayList<String>();
			colOrder.add("B.LENGTH");
			reorder = new ReorderOperator(colOrder, meta);
			reorder.add(select);
			root = new RootOperator(meta);
			root.add(reorder);
			trees = new ArrayList<Operator>();
			trees.add(root);
			p = new Plan(true, trees);
			addPlan("SELECT LENGTH FROM SYS.TABLES A, SYS.COLUMNS B WHERE A.SCHEMA = ? AND B.TABNAME = ? AND A.TABLEID = B.TABLEID AND B.COLNAME = ?", p);

			// getTableType
			meta = new MetaData();
			keys = new ArrayList<String>();
			types = new ArrayList<String>();
			orders = new ArrayList<Boolean>();
			keys.add("TABLES.SCHEMA");
			keys.add("TABLES.TABNAME");
			types.add("CHAR");
			types.add("CHAR");
			orders.add(true);
			orders.add(true);
			index = new Index("SYS.PKTABLES.indx", keys, types, orders);
			iOp = new IndexOperator(index, meta);
			iOp.setNode(-1);
			iOp.setDevice(0);
			tOp = new TableScanOperator("SYS", "TABLES", meta, tx);
			devs = new ArrayList<Integer>();
			devs.add(0);
			tOp.addActiveDevices(devs);
			tOp.setChildForDevice(0, iOp);
			needed = new ArrayList<String>();
			needed.add("TABLES.TYPE");
			tOp.setNeededCols(needed);
			tOp.setNode(-1);
			tOp.setPhase2Done();
			tOp.add(iOp);
			root = new RootOperator(meta);
			root.add(tOp);
			trees = new ArrayList<Operator>();
			trees.add(root);
			p = new Plan(true, trees);
			addPlan("SELECT TYPE FROM SYS.TABLES WHERE SCHEMA = ? AND TABNAME = ?", p);

			// getUnique - get uniqueness?
			keys = new ArrayList<String>();
			types = new ArrayList<String>();
			orders = new ArrayList<Boolean>();
			keys.add("TABLES.SCHEMA");
			keys.add("TABLES.TABNAME");
			types.add("CHAR");
			types.add("CHAR");
			orders.add(true);
			orders.add(true);
			index = new Index("SYS.PKTABLES.indx", keys, types, orders);
			iOp = new IndexOperator(index, meta);
			iOp.setNode(-1);
			iOp.setDevice(0);
			tOp = new TableScanOperator("SYS", "TABLES", meta, tx);
			devs = new ArrayList<Integer>();
			devs.add(0);
			tOp.addActiveDevices(devs);
			tOp.setChildForDevice(0, iOp);
			tOp.setAlias("A");
			needed = new ArrayList<String>();
			needed.add("A.TABLEID");
			tOp.setNeededCols(needed);
			tOp.setNode(-1);
			tOp.setPhase2Done();
			tOp.add(iOp);
			tOp2 = new TableScanOperator("SYS", "INDEXES", meta, tx);
			devs = new ArrayList<Integer>();
			devs.add(0);
			tOp2.addActiveDevices(devs);
			tOp2.setAlias("B");
			needed = new ArrayList<String>();
			needed.add("B.TABLEID");
			needed.add("B.INDEXNAME");
			needed.add("B.UNIQUE");
			tOp2.setNeededCols(needed);
			tOp2.setNode(-1);
			tOp2.setPhase2Done();
			hash = new HashJoinOperator("A.TABLEID", "B.TABLEID", meta);
			hash.add(tOp);
			hash.add(tOp2);
			colOrder = new ArrayList<String>();
			colOrder.add("B.INDEXNAME");
			colOrder.add("B.UNIQUE");
			reorder = new ReorderOperator(colOrder, meta);
			reorder.add(hash);
			orders = new ArrayList<Boolean>();
			orders.add(true);
			ArrayList<String> sortOrder = new ArrayList<String>();
			sortOrder.add("B.INDEXNAME");
			sort = new SortOperator(sortOrder, orders, meta);
			sort.add(reorder);
			root = new RootOperator(meta);
			root.add(sort);
			trees = new ArrayList<Operator>();
			trees.add(root);
			p = new Plan(true, trees);
			addPlan("SELECT INDEXNAME,UNIQUE FROM SYS.TABLES A, SYS.INDEXES B WHERE A.SCHEMA = ? AND A.TABNAME = ? AND A.TABLEID = B.TABLEID ORDER BY INDEXNAME", p);
			tx.commit();
			addPlan = false;
			// HRDBMSWorker.logger.debug(planCache.toString());
		}
		catch (Exception e)
		{
			HRDBMSWorker.logger.fatal("Exception during catalog plan creation", e);
			System.exit(1);
		}
	}

	public static void addPlan(String sql, Plan p)
	{
		if (addPlan)
		{
			// lock.readLock().lock();
			try
			{
				planCache.put(new SQL(sql), p);
			}
			catch (Exception e)
			{
				// lock.readLock().unlock();
				throw e;
			}
			// lock.readLock().unlock();
		}
	}

	public static Plan checkPlanCache(String sql)
	{
		// lock.readLock().lock();
		try
		{
			final Plan plan = planCache.get(new SQL(sql));
			if (plan == null)
			{
				// lock.readLock().unlock();
				return null;
			}

			// lock.readLock().unlock();
			return new Plan(plan);
		}
		catch (Exception e)
		{
			// lock.readLock().unlock();
			throw e;
		}
	}

	public static CheckIndexForColPlan getCheckIndexForCol()
	{
		return new CheckIndexForColPlan(checkPlanCache("SELECT TABLEID, INDEXID, COLID FROM SYS.INDEXCOLS WHERE TABLEID = ? AND INDEXID = ? AND COLID = ?"));
	}

	public static ColCardPlan getColCard()
	{
		return new ColCardPlan(checkPlanCache("SELECT CARD FROM SYS.TABLES A, SYS.COLUMNS B, SYS.COLSTATS C WHERE A.SCHEMA = ? AND A.TABNAME = ? AND A.TABLEID = B.TABLEID AND B.COLNAME = ? AND B.TABLEID = C.TABLEID AND B.COLID = C.COLID"));
	}

	public static Cols2PosPlan getCols2PosForTable()
	{
		return new Cols2PosPlan(checkPlanCache("SELECT COLNAME, COLID FROM SYS.TABLES A, SYS.COLUMNS B WHERE A.SCHEMA = ? AND B.TABNAME = ? AND A.TABLEID = B.TABLEID"));
	}

	public static Cols2TypesPlan getCols2Types()
	{
		return new Cols2TypesPlan(checkPlanCache("SELECT COLNAME, COLTYPE FROM SYS.TABLES A, SYS.COLUMNS B WHERE A.SCHEMA = ? AND B.TABNAME = ? AND A.TABLEID = B.TABLEID"));
	}

	public static ColTypePlan getColType()
	{
		return new ColTypePlan(checkPlanCache("SELECT COLTYPE FROM SYS.TABLES A, SYS.COLUMNS B WHERE A.SCHEMA = ? AND A.TABNAME = ? AND A.TABLEID = B.TABLEID AND B.COLNAME = ?"));
	}

	// public static CoordNodesPlan getCoordNodes()
	// {
	// return new
	// CoordNodesPlan(checkPlanCache("SELECT NODEID FROM SYS.NODES WHERE NODEID
	// < -1"));
	// }

	// public static CountWorkerNodesPlan getCountWorkerNodes()
	// {
	// return new
	// CountWorkerNodesPlan(checkPlanCache("SELECT COUNT(*) FROM SYS.NODES WHERE
	// NODEID >= 0"));
	// }

	public static DeleteColDistPlan getDeleteColDist()
	{
		return new DeleteColDistPlan();
	}

	public static DeleteColsPlan getDeleteCols()
	{
		return new DeleteColsPlan();
	}

	public static DeleteColStatsPlan getDeleteColStats()
	{
		return new DeleteColStatsPlan();
	}

	public static DeleteIndexPlan getDeleteIndex()
	{
		return new DeleteIndexPlan();
	}

	public static DeleteIndexColPlan getDeleteIndexCol()
	{
		return new DeleteIndexColPlan();
	}

	public static DeleteIndexStatsPlan getDeleteIndexStats()
	{
		return new DeleteIndexStatsPlan();
	}

	public static DeletePartitioningPlan getDeletePartitioning()
	{
		return new DeletePartitioningPlan();
	}

	public static DeleteTablePlan getDeleteTable()
	{
		return new DeleteTablePlan();
	}

	public static DeleteTableStatsPlan getDeleteTableStats()
	{
		return new DeleteTableStatsPlan();
	}

	public static DeleteViewPlan getDeleteView()
	{
		return new DeleteViewPlan();
	}

	public static DistPlan getDist()
	{
		return new DistPlan(checkPlanCache("SELECT LOW, Q1, Q2, Q3, HIGH FROM SYS.TABLES A, SYS.COLUMNS B, SYS.COLDIST C WHERE A.SCHEMA = ? AND A.TABNAME = ? AND A.TABLEID = B.TABLEID AND B.COLNAME = ? AND B.TABLEID = C.TABLEID AND B.COLID = C.COLID"));
	}

	// public static HostLookupPlan getHostLookup()
	// {
	// return new
	// HostLookupPlan(checkPlanCache("SELECT HOSTNAME FROM SYS.NODES WHERE
	// NODEID = ?"));
	// }

	public static IndexCardPlan getIndexCard()
	{
		return new IndexCardPlan(checkPlanCache("SELECT NUMDISTINCT FROM SYS.INDEXSTATS WHERE TABLEID = ? AND INDEXID = ?"));
	}

	public static IndexColCountPlan getIndexColCount()
	{
		return new IndexColCountPlan(checkPlanCache("SELECT COUNT(*) FROM SYS.INDEXCOLS WHERE TABLEID = ? AND INDEXID = ?"));
	}

	public static IndexColsForTablePlan getIndexColsForTable()
	{
		return new IndexColsForTablePlan(checkPlanCache("SELECT COLNAME FROM SYS.TABLES A, SYS.INDEXCOLS B, SYS.COLUMNS C WHERE A.SCHEMA = ? AND A.TABNAME = ? AND A.TABLEID = B.TABLEID AND B.TABLEID = C.TABLEID AND B.COLID = C.COLID"));
	}

	public static IndexPlan getIndexes()
	{
		return new IndexPlan(checkPlanCache("SELECT INDEXNAME FROM SYS.TABLES A, SYS.INDEXES B WHERE A.SCHEMA = ? AND A.TABNAME = ? AND A.TABLEID = B.TABLEID ORDER BY INDEXNAME"));
	}

	public static IndexIDsForTablePlan getIndexIDsForTable()
	{
		return new IndexIDsForTablePlan(checkPlanCache("SELECT INDEXID FROM SYS.TABLES A, SYS.INDEXES B WHERE A.SCHEMA = ? AND A.TABNAME = ? AND A.TABLEID = B.TABLEID"));
	}

	public static InsertColPlan getInsertCol()
	{
		return new InsertColPlan();
	}

	public static InsertIndexPlan getInsertIndex()
	{
		return new InsertIndexPlan();
	}

	public static InsertIndexColPlan getInsertIndexCol()
	{
		return new InsertIndexColPlan();
	}

	public static InsertPartitionPlan getInsertPartition()
	{
		return new InsertPartitionPlan();
	}

	public static InsertTablePlan getInsertTable()
	{
		return new InsertTablePlan();
	}

	public static InsertViewPlan getInsertView()
	{
		return new InsertViewPlan();
	}

	public static KeysPlan getKeys()
	{
		return new KeysPlan(checkPlanCache("SELECT TABNAME, COLNAME FROM SYS.INDEXES A, SYS.TABLES B, SYS.INDEXCOLS C, SYS.COLUMNS D WHERE A.INDEXNAME = ? AND B.SCHEMA = ? AND A.TABLEID = B.TABLEID AND A.TABLEID = C.TABLEID AND A.INDEXID = C.INDEXID AND C.TABLEID = D.TABLEID AND C.COLID = D.COLID"));
	}

	public static KeysByIDPlan getKeysByID()
	{
		return new KeysByIDPlan(checkPlanCache("SELECT COLNAME FROM SYS.INDEXCOLS A, SYS.COLUMNS B WHERE A.TABLEID = B.TABLEID A.COLID = B.COLID AND A.TABLEID = ? AND A.INDEXID = ?"));
	}

	public static LengthPlan getLength()
	{
		return new LengthPlan(checkPlanCache("SELECT LENGTH FROM SYS.TABLES A, SYS.COLUMNS B WHERE A.SCHEMA = ? AND B.TABNAME = ? AND A.TABLEID = B.TABLEID AND B.COLNAME = ?"));
	}

	public static MultiDeleteIndexColsPlan getMultiDeleteIndexCols()
	{
		return new MultiDeleteIndexColsPlan();
	}

	public static MultiDeleteIndexesPlan getMultiDeleteIndexes()
	{
		return new MultiDeleteIndexesPlan();
	}

	public static MultiDeleteIndexStatsPlan getMultiDeleteIndexStats()
	{
		return new MultiDeleteIndexStatsPlan();
	}

	public static NextIndexIDPlan getNextIndexID()
	{
		return new NextIndexIDPlan(checkPlanCache("SELECT MAX(INDEXID) FROM SYS.INDEXES WHERE TABLEID = ?"));
	}

	public static NextTableIDPlan getNextTableID()
	{
		return new NextTableIDPlan(checkPlanCache("SELECT MAX(TABLEID) FROM SYS.TABLES"));
	}

	public static NextViewIDPlan getNextViewID()
	{
		return new NextViewIDPlan(checkPlanCache("SELECT MAX(VIEWID) FROM SYS.VIEWS"));
	}

	public static OrdersPlan getOrders()
	{
		return new OrdersPlan(checkPlanCache("SELECT ORDER FROM SYS.INDEXES A, SYS.TABLES B, SYS.INDEXCOLS C WHERE A.INDEXNAME = ? AND B.SCHEMA = ? AND A.TABLEID = B.TABLEID AND A.TABLEID = C.TABLEID AND A.INDEXID = C.INDEXID"));
	}

	public static PartitioningPlan getPartitioning()
	{
		return new PartitioningPlan(checkPlanCache("SELECT GROUPEXP, NODEEXP, DEVICEEXP FROM SYS.TABLES A, SYS.PARTITIONING B WHERE A.SCHEMA = ? AND A.TABNAME = ? AND A.TABLEID = B.TABLEID"));
	}

	public static TableAndIndexIDPlan getTableAndIndexID()
	{
		return new TableAndIndexIDPlan(checkPlanCache("SELECT TABLEID, INDEXID FROM SYS.TABLES A, SYS.INDEXES B WHERE A.SCHEMA = ? AND A.TABLEID = B.TABLEID AND B.INDEXNAME = ?"));
	}

	public static TableCardPlan getTableCard()
	{
		return new TableCardPlan(checkPlanCache("SELECT CARD FROM SYS.TABLES A, SYS.TABLESTATS B WHERE A.SCHEMA = ? AND A.TABNAME = ? AND A.TABLEID = B.TABLEID"));
	}

	public static TableIDPlan getTableID()
	{
		return new TableIDPlan(checkPlanCache("SELECT TABLEID FROM SYS.TABLES WHERE SCHEMA = ? AND TABNAME = ?"));
	}

	public static TableTypePlan getTableType()
	{
		return new TableTypePlan(checkPlanCache("SELECT TYPE FROM SYS.TABLES WHERE SCHEMA = ? AND TABNAME = ?"));
	}

	public static TypesPlan getTypes()
	{
		return new TypesPlan(checkPlanCache("SELECT COLTYPE FROM SYS.INDEXES A, SYS.TABLES B, SYS.INDEXCOLS C, SYS.COLUMNS D WHERE A.INDEXNAME = ? AND B.SCHEMA = ? AND A.TABLEID = B.TABLEID AND A.TABLEID = C.TABLEID AND A.INDEXID = C.INDEXID AND C.TABLEID = D.TABLEID AND C.COLID = D.COLID"));
	}

	public static UniquePlan getUnique()
	{
		return new UniquePlan(checkPlanCache("SELECT INDEXNAME,UNIQUE FROM SYS.TABLES A, SYS.INDEXES B WHERE A.SCHEMA = ? AND A.TABNAME = ? AND A.TABLEID = B.TABLEID ORDER BY INDEXNAME"));
	}

	public static UniqueIndexPlan getUniqueIndexes()
	{
		return new UniqueIndexPlan(checkPlanCache("SELECT INDEXNAME FROM SYS.TABLES A, SYS.INDEXES B WHERE A.SCHEMA = ? AND A.TABNAME = ? AND A.TABLEID = B.TABLEID AND B.UNIQUE = 'Y' ORDER BY INDEXNAME"));
	}

	public static VerifyIndexPlan getVerifyIndexExist()
	{
		return new VerifyIndexPlan(checkPlanCache("SELECT TABLEID, INDEXID FROM SYS.TABLES A, SYS.INDEXES B WHERE A.SCHEMA = ? AND A.TABLEID = B.TABLEID AND B.INDEXNAME = ?"));
	}

	public static VerifyTableExistPlan getVerifyTableExist()
	{
		return new VerifyTableExistPlan(checkPlanCache("SELECT TABLEID FROM SYS.TABLES WHERE SCHEMA = ? AND TABNAME = ?"));
	}

	public static VerifyViewExistPlan getVerifyViewExist()
	{
		return new VerifyViewExistPlan(checkPlanCache("SELECT TEXT FROM SYS.VIEWS WHERE SCHEMA = ? AND NAME = ?"));
	}

	public static ViewSQLPlan getViewSQL()
	{
		return new ViewSQLPlan(checkPlanCache("SELECT TEXT FROM SYS.VIEWS WHERE SCHEMA = ? AND NAME = ?"));
	}

	// public static WorkerNodesPlan getWorkerNodes()
	// {
	// return new
	// WorkerNodesPlan(checkPlanCache("SELECT NODEID FROM SYS.NODES WHERE NODEID
	// >= 0"));
	// }

	public static void invalidate()
	{
		// lock.writeLock().lock();
		for (Map.Entry entry : planCache.entrySet())
		{
			Plan p = (Plan)entry.getValue();
			if (!p.isReserved())
			{
				planCache.remove(entry.getKey());
			}
		}
		// lock.writeLock().unlock();
	}

	public static void reduce()
	{
		// lock.readLock().lock();
		double avg = 0;
		long num = -1;
		for (final Plan p : planCache.values())
		{
			long newNum;
			if (num == -1)
			{
				newNum = 1;
			}
			else
			{
				newNum = num + 1;
			}

			avg = avg / (newNum * 1.0 / num) + p.getTimeStamp() / newNum - avg;
			num = newNum;
		}

		for (final Map.Entry<SQL, Plan> entry : planCache.entrySet())
		{
			final Plan p = entry.getValue();
			if (!p.isReserved())
			{
				if (p.getTimeStamp() < (long)avg)
				{
					planCache.remove(entry.getKey());
				}
			}
		}

		// lock.readLock().unlock();
	}

	public static class CheckIndexForColPlan
	{
		private final Plan p;
		private int tableID;
		private int indexID;
		private int colID;

		public CheckIndexForColPlan(Plan p)
		{
			this.p = p;
		}

		public Object execute(Transaction tx) throws Exception
		{
			if (tableID >= 0 && tableID <= 12)
			{
				if (indexID == 0)
				{
					if (tableID == 0)
					{
						if (colID >= 1 && colID <= 2)
						{
							return new ArrayList<Object>();
						}
					}
					else if (tableID == 1)
					{
						if (colID >= 1 && colID <= 2)
						{
							return new ArrayList<Object>();
						}
					}
					else if (tableID == 2)
					{
						if (colID >= 1 && colID <= 2)
						{
							return new ArrayList<Object>();
						}
					}
					else if (tableID == 3)
					{
						if (colID >= 0 && colID <= 2)
						{
							return new ArrayList<Object>();
						}
					}
					else if (tableID == 4)
					{
						if (colID >= 1 && colID <= 2)
						{
							return new ArrayList<Object>();
						}
					}
					else if (tableID == 5)
					{
						if (colID == 0)
						{
							return new ArrayList<Object>();
						}
					}
					else if (tableID == 6)
					{
						if (colID == 1)
						{
							return new ArrayList<Object>();
						}
					}
					else if (tableID == 7)
					{
						if (colID >= 0 && colID <= 1)
						{
							return new ArrayList<Object>();
						}
					}
					else if (tableID == 8)
					{
						if (colID >= 0 && colID <= 1)
						{
							return new ArrayList<Object>();
						}
					}
					else if (tableID == 9)
					{
						if (colID == 0)
						{
							return new ArrayList<Object>();
						}
					}
					else if (tableID == 10)
					{
						if (colID == 0)
						{
							return new ArrayList<Object>();
						}
					}
					else if (tableID == 11)
					{
						if (colID == 0)
						{
							return new ArrayList<Object>();
						}
					}
					else if (tableID == 12)
					{
						if (colID >= 0 && colID <= 1)
						{
							return new ArrayList<Object>();
						}
					}
				}
				else if (indexID == 1)
				{
					if (tableID == 2)
					{
						if (colID == 1)
						{
							return new ArrayList<Object>();
						}
					}
					else if (tableID == 6)
					{
						if (colID == 0)
						{
							return new ArrayList<Object>();
						}
					}
				}

				return new DataEndMarker();
			}

			int iso = tx.getIsolationLevel();
			tx.setIsolationLevel(Transaction.ISOLATION_RR);
			XAWorker worker = XAManager.executeCatalogQuery(p, tx);
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
				tx.setIsolationLevel(iso);

				throw (Exception)obj;
			}

			cmd = new ArrayList<Object>(1);
			cmd.add("CLOSE");
			worker.in.put(cmd);
			tx.setIsolationLevel(iso);
			return obj;
		}

		public CheckIndexForColPlan setParms(int tableID, int indexID, int colID) throws Exception
		{
			this.tableID = tableID;
			this.indexID = indexID;
			this.colID = colID;
			if (tableID >= 0 && tableID <= 12)
			{
				return this;
			}

			Operator op = p.getTrees().get(0);
			while (!(op instanceof IndexOperator))
			{
				op = op.children().get(0);
			}
			IndexOperator iOp = (IndexOperator)op;
			Index index = iOp.getIndex();
			index.setCondition(new Filter("INDEXCOLS.TABLEID", "E", Integer.toString(tableID)));
			index.addSecondaryFilter(new Filter("INDEXCOLS.INDEXID", "E", Integer.toString(indexID)));
			index.addSecondaryFilter(new Filter("INDEXCOLS.COLID", "E", Integer.toString(colID)));
			return this;
		}
	}

	public static class ColCardPlan
	{
		private final Plan p;

		public ColCardPlan(Plan p)
		{
			this.p = p;
		}

		public long execute(Transaction tx) throws Exception
		{
			int iso = tx.getIsolationLevel();
			tx.setIsolationLevel(Transaction.ISOLATION_UR);
			XAWorker worker = XAManager.executeCatalogQuery(p, tx);
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

			if (obj instanceof DataEndMarker)
			{
				cmd = new ArrayList<Object>(1);
				cmd.add("CLOSE");
				worker.in.put(cmd);
				tx.setIsolationLevel(iso);

				throw new Exception("No column statistics");
			}

			if (obj instanceof Exception)
			{
				cmd = new ArrayList<Object>(1);
				cmd.add("CLOSE");
				worker.in.put(cmd);
				tx.setIsolationLevel(iso);

				throw (Exception)obj;
			}

			Long retval = (Long)((ArrayList<Object>)obj).get(0);

			cmd = new ArrayList<Object>(1);
			cmd.add("CLOSE");
			worker.in.put(cmd);
			tx.setIsolationLevel(iso);
			return retval;
		}

		public ColCardPlan setParms(String schema, String table, String col) throws Exception
		{
			if (col.contains("."))
			{
				col = col.substring(col.indexOf('.') + 1);
			}

			Operator op = p.getTrees().get(0);
			while (!(op instanceof IndexOperator))
			{
				op = op.children().get(0);
			}
			IndexOperator iOp = (IndexOperator)op;
			Index index = iOp.getIndex();
			index.setCondition(new Filter("TABLES.SCHEMA", "E", "'" + schema + "'"));
			index.addSecondaryFilter(new Filter("TABLES.TABNAME", "E", "'" + table + "'"));
			while (!(op instanceof SelectOperator))
			{
				if (op instanceof TableScanOperator)
				{
					op = ((TableScanOperator)op).firstParent();
				}
				else
				{
					op = op.parent();
				}
			}
			SelectOperator select = (SelectOperator)op;
			select.getFilter().remove(0);
			select.getFilter().add(new Filter("B.COLNAME", "E", "'" + col + "'"));
			return this;
		}
	}

	public static class Cols2PosPlan
	{
		private final Plan p;
		private String schema;
		private String table;

		public Cols2PosPlan(Plan p)
		{
			this.p = p;
		}

		public ArrayList<Object> execute(Transaction tx) throws Exception
		{
			ArrayList<Object> retval = new ArrayList<Object>();
			if (schema.equals("SYS"))
			{
				if (table.equals("TABLES"))
				{
					ArrayList<Object> row = new ArrayList<Object>();
					row.add("TABLEID");
					row.add(0);
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("SCHEMA");
					row.add(1);
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("TABNAME");
					row.add(2);
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("TYPE");
					row.add(3);
					retval.add(row);
				}
				else if (table.equals("COLUMNS"))
				{
					ArrayList<Object> row = new ArrayList<Object>();
					row.add("COLID");
					row.add(0);
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("TABLEID");
					row.add(1);
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("COLNAME");
					row.add(2);
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("COLTYPE");
					row.add(3);
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("LENGTH");
					row.add(4);
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("SCALE");
					row.add(5);
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("PKPOS");
					row.add(6);
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("NULLABLE");
					row.add(7);
					retval.add(row);
				}
				else if (table.equals("INDEXES"))
				{
					ArrayList<Object> row = new ArrayList<Object>();
					row.add("INDEXID");
					row.add(0);
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("INDEXNAME");
					row.add(1);
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("TABLEID");
					row.add(2);
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("UNIQUE");
					row.add(3);
					retval.add(row);
				}
				else if (table.equals("INDEXCOLS"))
				{
					ArrayList<Object> row = new ArrayList<Object>();
					row.add("INDEXID");
					row.add(0);
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("TABLEID");
					row.add(1);
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("COLID");
					row.add(2);
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("POSITION");
					row.add(3);
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("ORDER");
					row.add(4);
					retval.add(row);
				}
				else if (table.equals("VIEWS"))
				{
					ArrayList<Object> row = new ArrayList<Object>();
					row.add("VIEWID");
					row.add(0);
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("SCHEMA");
					row.add(1);
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("NAME");
					row.add(2);
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("TEXT");
					row.add(3);
					retval.add(row);
				}
				else if (table.equals("TABLESTATS"))
				{
					ArrayList<Object> row = new ArrayList<Object>();
					row.add("TABLEID");
					row.add(0);
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("CARD");
					row.add(1);
					retval.add(row);
				}
				else if (table.equals("COLSTATS"))
				{
					ArrayList<Object> row = new ArrayList<Object>();
					row.add("TABLEID");
					row.add(0);
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("COLID");
					row.add(1);
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("CARD");
					row.add(2);
					retval.add(row);
				}
				else if (table.equals("NODES"))
				{
					ArrayList<Object> row = new ArrayList<Object>();
					row.add("NODEID");
					row.add(0);
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("HOSTNAME");
					row.add(1);
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("TYPE");
					row.add(2);
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("RACK");
					row.add(3);
					retval.add(row);
				}
				else if (table.equals("COLDIST"))
				{
					ArrayList<Object> row = new ArrayList<Object>();
					row.add("TABLEID");
					row.add(0);
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("COLID");
					row.add(1);
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("LOW");
					row.add(2);
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("Q1");
					row.add(3);
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("Q2");
					row.add(4);
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("Q3");
					row.add(5);
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("HIGH");
					row.add(6);
					retval.add(row);
				}
				else if (table.equals("BACKUPS"))
				{
					ArrayList<Object> row = new ArrayList<Object>();
					row.add("FIRST");
					row.add(0);
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("SECOND");
					row.add(1);
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("THIRD");
					row.add(2);
					retval.add(row);
				}
				else if (table.equals("PARTITIONING"))
				{
					ArrayList<Object> row = new ArrayList<Object>();
					row.add("TABLEID");
					row.add(0);
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("GROUPEXP");
					row.add(1);
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("NODEEXP");
					row.add(2);
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("DEVICEEXP");
					row.add(3);
					retval.add(row);
				}
				else if (table.equals("NODESTATE"))
				{
					ArrayList<Object> row = new ArrayList<Object>();
					row.add("NODE");
					row.add(0);
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("STATE");
					row.add(1);
					retval.add(row);
				}
				else if (table.equals("INDEXSTATS"))
				{
					ArrayList<Object> row = new ArrayList<Object>();
					row.add("TABLEID");
					row.add(0);
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("INDEXID");
					row.add(1);
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("NUMDISTINCT");
					row.add(2);
					retval.add(row);
				}
				else
				{
					retval.add(new DataEndMarker());
				}

				return retval;
			}

			int iso = tx.getIsolationLevel();
			tx.setIsolationLevel(Transaction.ISOLATION_RR);
			XAWorker worker = XAManager.executeCatalogQuery(p, tx);
			worker.start();
			ArrayList<Object> cmd = new ArrayList<Object>(2);
			cmd.add("NEXT");
			cmd.add(1000000);
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

			while (!(obj instanceof DataEndMarker))
			{
				if (obj instanceof Exception)
				{
					cmd = new ArrayList<Object>(1);
					cmd.add("CLOSE");
					worker.in.put(cmd);
					tx.setIsolationLevel(iso);

					throw (Exception)obj;
				}

				retval.add(obj);

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
			}

			retval.add(new DataEndMarker());
			cmd = new ArrayList<Object>(1);
			cmd.add("CLOSE");
			worker.in.put(cmd);
			tx.setIsolationLevel(iso);
			return retval;
		}

		public Cols2PosPlan setParms(String schema, String table) throws Exception
		{
			this.table = table;
			this.schema = schema;
			if (schema.equals("SYS"))
			{
				return this;
			}

			Operator op = p.getTrees().get(0);
			while (!(op instanceof IndexOperator))
			{
				op = op.children().get(0);
			}
			IndexOperator iOp = (IndexOperator)op;
			Index index = iOp.getIndex();
			index.setCondition(new Filter("TABLES.SCHEMA", "E", "'" + schema + "'"));
			index.addSecondaryFilter(new Filter("TABLES.TABNAME", "E", "'" + table + "'"));
			return this;
		}
	}

	public static class Cols2TypesPlan
	{
		private final Plan p;
		private String schema;
		private String table;

		public Cols2TypesPlan(Plan p)
		{
			this.p = p;
		}

		public ArrayList<Object> execute(Transaction tx) throws Exception
		{
			ArrayList<Object> retval = new ArrayList<Object>();
			if (schema.equals("SYS"))
			{
				if (table.equals("TABLES"))
				{
					ArrayList<Object> row = new ArrayList<Object>();
					row.add("TABLEID");
					row.add("INT");
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("SCHEMA");
					row.add("VARCHAR");
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("TABNAME");
					row.add("VARCHAR");
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("TYPE");
					row.add("VARCHAR");
					retval.add(row);
				}
				else if (table.equals("COLUMNS"))
				{
					ArrayList<Object> row = new ArrayList<Object>();
					row.add("COLID");
					row.add("INT");
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("TABLEID");
					row.add("INT");
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("COLNAME");
					row.add("VARCHAR");
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("COLTYPE");
					row.add("VARCHAR");
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("LENGTH");
					row.add("INT");
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("SCALE");
					row.add("INT");
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("PKPOS");
					row.add("INT");
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("NULLABLE");
					row.add("VARCHAR");
					retval.add(row);
				}
				else if (table.equals("INDEXES"))
				{
					ArrayList<Object> row = new ArrayList<Object>();
					row.add("INDEXID");
					row.add("INT");
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("INDEXNAME");
					row.add("VARCHAR");
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("TABLEID");
					row.add("INT");
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("UNIQUE");
					row.add("VARCHAR");
					retval.add(row);
				}
				else if (table.equals("INDEXCOLS"))
				{
					ArrayList<Object> row = new ArrayList<Object>();
					row.add("INDEXID");
					row.add("INT");
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("TABLEID");
					row.add("INT");
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("COLID");
					row.add("INT");
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("POSITION");
					row.add("INT");
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("ORDER");
					row.add("VARCHAR");
					retval.add(row);
				}
				else if (table.equals("VIEWS"))
				{
					ArrayList<Object> row = new ArrayList<Object>();
					row.add("VIEWID");
					row.add("INT");
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("SCHEMA");
					row.add("VARCHAR");
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("NAME");
					row.add("VARCHAR");
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("TEXT");
					row.add("VARCHAR");
					retval.add(row);
				}
				else if (table.equals("TABLESTATS"))
				{
					ArrayList<Object> row = new ArrayList<Object>();
					row.add("TABLEID");
					row.add("INT");
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("CARD");
					row.add("BIGINT");
					retval.add(row);
				}
				else if (table.equals("COLSTATS"))
				{
					ArrayList<Object> row = new ArrayList<Object>();
					row.add("TABLEID");
					row.add("INT");
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("COLID");
					row.add("INT");
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("CARD");
					row.add("BIGINT");
					retval.add(row);
				}
				else if (table.equals("NODES"))
				{
					ArrayList<Object> row = new ArrayList<Object>();
					row.add("NODEID");
					row.add("INT");
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("HOSTNAME");
					row.add("VARCHAR");
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("TYPE");
					row.add("VARCHAR");
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("RACK");
					row.add("VARCHAR");
					retval.add(row);
				}
				else if (table.equals("COLDIST"))
				{
					ArrayList<Object> row = new ArrayList<Object>();
					row.add("TABLEID");
					row.add("INT");
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("COLID");
					row.add("INT");
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("LOW");
					row.add("VARCHAR");
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("Q1");
					row.add("VARCHAR");
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("Q2");
					row.add("VARCHAR");
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("Q3");
					row.add("VARCHAR");
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("HIGH");
					row.add("VARCHAR");
					retval.add(row);
				}
				else if (table.equals("BACKUPS"))
				{
					ArrayList<Object> row = new ArrayList<Object>();
					row.add("FIRST");
					row.add("INT");
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("SECOND");
					row.add("INT");
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("THIRD");
					row.add("INT");
					retval.add(row);
				}
				else if (table.equals("PARTITIONING"))
				{
					ArrayList<Object> row = new ArrayList<Object>();
					row.add("TABLEID");
					row.add("INT");
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("GROUPEXP");
					row.add("VARCHAR");
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("NODEEXP");
					row.add("VARCHAR");
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("DEVICEEXP");
					row.add("VARCHAR");
					retval.add(row);
				}
				else if (table.equals("NODESTATE"))
				{
					ArrayList<Object> row = new ArrayList<Object>();
					row.add("NODE");
					row.add("INT");
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("STATE");
					row.add("VARCHAR");
					retval.add(row);
				}
				else if (table.equals("INDEXSTATS"))
				{
					ArrayList<Object> row = new ArrayList<Object>();
					row.add("TABLEID");
					row.add("INT");
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("INDEXID");
					row.add("INT");
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("NUMDISTINCT");
					row.add("BIGINT");
					retval.add(row);
				}
				else
				{
					retval.add(new DataEndMarker());
				}

				return retval;
			}

			int iso = tx.getIsolationLevel();
			tx.setIsolationLevel(Transaction.ISOLATION_RR);
			XAWorker worker = XAManager.executeCatalogQuery(p, tx);
			worker.start();
			ArrayList<Object> cmd = new ArrayList<Object>(2);
			cmd.add("NEXT");
			cmd.add(1000000);
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

			while (!(obj instanceof DataEndMarker))
			{
				if (obj instanceof Exception)
				{
					cmd = new ArrayList<Object>(1);
					cmd.add("CLOSE");
					worker.in.put(cmd);
					tx.setIsolationLevel(iso);

					throw (Exception)obj;
				}

				retval.add(obj);

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
			}

			retval.add(new DataEndMarker());
			cmd = new ArrayList<Object>(1);
			cmd.add("CLOSE");
			worker.in.put(cmd);
			tx.setIsolationLevel(iso);
			return retval;
		}

		public Cols2TypesPlan setParms(String schema, String table) throws Exception
		{
			this.table = table;
			this.schema = schema;
			if (schema.equals("SYS"))
			{
				return this;
			}

			Operator op = p.getTrees().get(0);
			while (!(op instanceof IndexOperator))
			{
				op = op.children().get(0);
			}
			IndexOperator iOp = (IndexOperator)op;
			Index index = iOp.getIndex();
			index.setCondition(new Filter("TABLES.SCHEMA", "E", "'" + schema + "'"));
			index.addSecondaryFilter(new Filter("TABLES.TABNAME", "E", "'" + table + "'"));
			return this;
		}
	}

	public static class ColTypePlan
	{
		private final Plan p;
		private String schema;

		public ColTypePlan(Plan p)
		{
			this.p = p;
		}

		public String execute(Transaction tx) throws Exception
		{
			if (schema.equals("SYS"))
			{
				throw new Exception("getColType() should never be called on catalog tables");
			}
			int iso = tx.getIsolationLevel();
			tx.setIsolationLevel(Transaction.ISOLATION_RR);
			XAWorker worker = XAManager.executeCatalogQuery(p, tx);
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

			if (obj instanceof DataEndMarker)
			{
				cmd = new ArrayList<Object>(1);
				cmd.add("CLOSE");
				worker.in.put(cmd);
				tx.setIsolationLevel(iso);

				throw new Exception("Column not found");
			}

			if (obj instanceof Exception)
			{
				cmd = new ArrayList<Object>(1);
				cmd.add("CLOSE");
				worker.in.put(cmd);
				tx.setIsolationLevel(iso);

				throw (Exception)obj;
			}

			String retval = (String)((ArrayList<Object>)obj).get(0);

			cmd = new ArrayList<Object>(1);
			cmd.add("CLOSE");
			worker.in.put(cmd);
			tx.setIsolationLevel(iso);
			return retval;
		}

		public ColTypePlan setParms(String schema, String table, String col) throws Exception
		{
			if (col.contains("."))
			{
				col = col.substring(col.indexOf('.') + 1);
			}

			this.schema = schema;
			if (schema.equals("SYS"))
			{
				return this;
			}
			Operator op = p.getTrees().get(0);
			while (!(op instanceof IndexOperator))
			{
				op = op.children().get(0);
			}
			IndexOperator iOp = (IndexOperator)op;
			Index index = iOp.getIndex();
			index.setCondition(new Filter("TABLES.SCHEMA", "E", "'" + schema + "'"));
			index.addSecondaryFilter(new Filter("TABLES.TABNAME", "E", "'" + table + "'"));
			while (!(op instanceof SelectOperator))
			{
				if (op instanceof TableScanOperator)
				{
					op = ((TableScanOperator)op).firstParent();
				}
				else
				{
					op = op.parent();
				}
			}
			SelectOperator select = (SelectOperator)op;
			select.getFilter().remove(0);
			select.getFilter().add(new Filter("B.COLNAME", "E", "'" + col + "'"));
			return this;
		}
	}

	public static class CoordNodesPlan
	{
		private final Plan p;

		public CoordNodesPlan(Plan p)
		{
			this.p = p;
		}

		public ArrayList<Object> execute(Transaction tx) throws Exception
		{
			int iso = tx.getIsolationLevel();
			tx.setIsolationLevel(Transaction.ISOLATION_RR);
			XAWorker worker = XAManager.executeCatalogQuery(p, tx);
			worker.start();
			ArrayList<Object> cmd = new ArrayList<Object>(2);
			cmd.add("NEXT");
			cmd.add(1000000);
			worker.in.put(cmd);

			ArrayList<Object> retval = new ArrayList<Object>();
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
			while (!(obj instanceof DataEndMarker))
			{
				if (obj instanceof Exception)
				{
					cmd = new ArrayList<Object>(1);
					cmd.add("CLOSE");
					worker.in.put(cmd);
					tx.setIsolationLevel(iso);
					throw (Exception)obj;
				}

				retval.add(obj);

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
			}

			cmd = new ArrayList<Object>(1);
			cmd.add("CLOSE");
			worker.in.put(cmd);
			tx.setIsolationLevel(iso);
			return retval;
		}

		public CoordNodesPlan setParms()
		{
			return this;
		}
	}

	public static class CountWorkerNodesPlan
	{
		private final Plan p;

		public CountWorkerNodesPlan(Plan p)
		{
			this.p = p;
		}

		public int execute(Transaction tx) throws Exception
		{
			if (numWorkers != null)
			{
				return numWorkers;
			}

			int iso = tx.getIsolationLevel();
			tx.setIsolationLevel(Transaction.ISOLATION_RR);
			XAWorker worker = XAManager.executeCatalogQuery(p, tx);
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
				tx.setIsolationLevel(iso);
				throw (Exception)obj;
			}

			if (obj instanceof DataEndMarker)
			{
				cmd = new ArrayList<Object>(1);
				cmd.add("CLOSE");
				worker.in.put(cmd);
				tx.setIsolationLevel(iso);
				throw new Exception("No result received when querying number of worker nodes");
			}

			int retval = ((Long)((ArrayList<Object>)obj).get(0)).intValue();
			// HRDBMSWorker.logger.debug("There are " + retval +
			// " worker nodes");
			cmd = new ArrayList<Object>(1);
			cmd.add("CLOSE");
			worker.in.put(cmd);
			tx.setIsolationLevel(iso);
			numWorkers = retval;
			return retval;
		}

		public CountWorkerNodesPlan setParms()
		{
			return this;
		}
	}

	public static class DeleteColDistPlan
	{
		private int tableID;

		public void execute(Transaction tx) throws Exception
		{
			String sql = "DELETE FROM SYS.COLDIST WHERE TABLEID = " + tableID;
			XAWorker worker = XAManager.executeAuthorizedUpdate(sql, tx);
			worker.start();
			worker.join();
			int updateCount = worker.getUpdateCount();
			if (updateCount == -1)
			{
				throw worker.getException();
			}
		}

		public DeleteColDistPlan setParms(int tableID)
		{
			this.tableID = tableID;
			return this;
		}
	}

	public static class DeleteColsPlan
	{
		private int tableID;

		public void execute(Transaction tx) throws Exception
		{
			String sql = "DELETE FROM SYS.COLUMNS WHERE TABLEID = " + tableID;
			XAWorker worker = XAManager.executeAuthorizedUpdate(sql, tx);
			worker.start();
			worker.join();
			int updateCount = worker.getUpdateCount();
			if (updateCount == -1)
			{
				throw worker.getException();
			}
		}

		public DeleteColsPlan setParms(int tableID)
		{
			this.tableID = tableID;
			return this;
		}
	}

	public static class DeleteColStatsPlan
	{
		private int tableID;

		public void execute(Transaction tx) throws Exception
		{
			String sql = "DELETE FROM SYS.COLSTATS WHERE TABLEID = " + tableID;
			XAWorker worker = XAManager.executeAuthorizedUpdate(sql, tx);
			worker.start();
			worker.join();
			int updateCount = worker.getUpdateCount();
			if (updateCount == -1)
			{
				throw worker.getException();
			}
		}

		public DeleteColStatsPlan setParms(int tableID)
		{
			this.tableID = tableID;
			return this;
		}
	}

	public static class DeleteIndexColPlan
	{
		private int tableID;
		private int indexID;

		public void execute(Transaction tx) throws Exception
		{
			String sql = "DELETE FROM SYS.INDEXCOLS WHERE TABLEID = " + tableID + " AND INDEXID = " + indexID;
			XAWorker worker = XAManager.executeAuthorizedUpdate(sql, tx);
			worker.start();
			worker.join();
			int updateCount = worker.getUpdateCount();
			if (updateCount == -1)
			{
				throw worker.getException();
			}
		}

		public DeleteIndexColPlan setParms(int tableID, int indexID)
		{
			this.tableID = tableID;
			this.indexID = indexID;
			return this;
		}
	}

	public static class DeleteIndexPlan
	{
		private int tableID;
		private int indexID;

		public void execute(Transaction tx) throws Exception
		{
			String sql = "DELETE FROM SYS.INDEXES WHERE TABLEID = " + tableID + " AND INDEXID = " + indexID;
			XAWorker worker = XAManager.executeAuthorizedUpdate(sql, tx);
			worker.start();
			worker.join();
			int updateCount = worker.getUpdateCount();
			if (updateCount == -1)
			{
				throw worker.getException();
			}
		}

		public DeleteIndexPlan setParms(int tableID, int indexID)
		{
			this.tableID = tableID;
			this.indexID = indexID;
			return this;
		}
	}

	public static class DeleteIndexStatsPlan
	{
		private int tableID;
		private int indexID;

		public void execute(Transaction tx) throws Exception
		{
			String sql = "DELETE FROM SYS.INDEXSTATS WHERE TABLEID = " + tableID + " AND INDEXID = " + indexID;
			XAWorker worker = XAManager.executeAuthorizedUpdate(sql, tx);
			worker.start();
			worker.join();
			int updateCount = worker.getUpdateCount();
			if (updateCount == -1)
			{
				throw worker.getException();
			}
		}

		public DeleteIndexStatsPlan setParms(int tableID, int indexID)
		{
			this.tableID = tableID;
			this.indexID = indexID;
			return this;
		}
	}

	public static class DeletePartitioningPlan
	{
		private int tableID;

		public void execute(Transaction tx) throws Exception
		{
			String sql = "DELETE FROM SYS.PARTITIONING WHERE TABLEID = " + tableID;
			XAWorker worker = XAManager.executeAuthorizedUpdate(sql, tx);
			worker.start();
			worker.join();
			int updateCount = worker.getUpdateCount();
			if (updateCount == -1)
			{
				throw worker.getException();
			}
		}

		public DeletePartitioningPlan setParms(int tableID)
		{
			this.tableID = tableID;
			return this;
		}
	}

	public static class DeleteTablePlan
	{
		private String schema;
		private String table;

		public void execute(Transaction tx) throws Exception
		{
			String sql = "DELETE FROM SYS.TABLES WHERE SCHEMA = " + "'" + schema + "'" + " AND TABNAME = " + "'" + table + "'";
			XAWorker worker = XAManager.executeAuthorizedUpdate(sql, tx);
			worker.start();
			worker.join();
			int updateCount = worker.getUpdateCount();
			if (updateCount == -1)
			{
				throw worker.getException();
			}
		}

		public DeleteTablePlan setParms(String schema, String table)
		{
			this.schema = schema;
			this.table = table;
			return this;
		}
	}

	public static class DeleteTableStatsPlan
	{
		private int tableID;

		public void execute(Transaction tx) throws Exception
		{
			String sql = "DELETE FROM SYS.TABLESTATS WHERE TABLEID = " + tableID;
			XAWorker worker = XAManager.executeAuthorizedUpdate(sql, tx);
			worker.start();
			worker.join();
			int updateCount = worker.getUpdateCount();
			if (updateCount == -1)
			{
				throw worker.getException();
			}
		}

		public DeleteTableStatsPlan setParms(int tableID)
		{
			this.tableID = tableID;
			return this;
		}
	}

	public static class DeleteViewPlan
	{
		private String schema;
		private String table;

		public void execute(Transaction tx) throws Exception
		{
			String sql = "DELETE FROM SYS.VIEWS WHERE SCHEMA = '" + schema + "' AND NAME = '" + table + "'";
			XAWorker worker = XAManager.executeAuthorizedUpdate(sql, tx);
			worker.start();
			worker.join();
			int updateCount = worker.getUpdateCount();
			if (updateCount == -1)
			{
				throw worker.getException();
			}
		}

		public DeleteViewPlan setParms(String schema, String table)
		{
			this.schema = schema;
			this.table = table;
			return this;
		}
	}

	public static class DistPlan
	{
		private final Plan p;

		public DistPlan(Plan p)
		{
			this.p = p;
		}

		public Object execute(Transaction tx) throws Exception
		{
			int iso = tx.getIsolationLevel();
			tx.setIsolationLevel(Transaction.ISOLATION_UR);
			XAWorker worker = XAManager.executeCatalogQuery(p, tx);
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
				tx.setIsolationLevel(iso);

				throw (Exception)obj;
			}

			cmd = new ArrayList<Object>(1);
			cmd.add("CLOSE");
			worker.in.put(cmd);
			tx.setIsolationLevel(iso);
			return obj;
		}

		public DistPlan setParms(String schema, String table, String col) throws Exception
		{
			Operator op = p.getTrees().get(0);
			while (!(op instanceof IndexOperator))
			{
				op = op.children().get(0);
			}
			IndexOperator iOp = (IndexOperator)op;
			Index index = iOp.getIndex();
			index.setCondition(new Filter("TABLES.SCHEMA", "E", "'" + schema + "'"));
			index.addSecondaryFilter(new Filter("TABLES.TABNAME", "E", "'" + table + "'"));
			while (!(op instanceof SelectOperator))
			{
				if (op instanceof TableScanOperator)
				{
					op = ((TableScanOperator)op).firstParent();
				}
				else
				{
					op = op.parent();
				}
			}
			SelectOperator select = (SelectOperator)op;
			select.getFilter().remove(0);
			select.getFilter().add(new Filter("B.COLNAME", "E", "'" + col + "'"));
			return this;
		}
	}

	public static class HostLookupPlan
	{
		private final Plan p;

		public HostLookupPlan(Plan p)
		{
			this.p = p;
		}

		public String execute(Transaction tx) throws Exception
		{
			int iso = tx.getIsolationLevel();
			tx.setIsolationLevel(Transaction.ISOLATION_RR);
			XAWorker worker = XAManager.executeCatalogQuery(p, tx);
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

			if (obj instanceof DataEndMarker)
			{
				cmd = new ArrayList<Object>(1);
				cmd.add("CLOSE");
				worker.in.put(cmd);
				tx.setIsolationLevel(iso);

				throw new Exception("Node not found");
			}

			if (obj instanceof Exception)
			{
				cmd = new ArrayList<Object>(1);
				cmd.add("CLOSE");
				worker.in.put(cmd);
				tx.setIsolationLevel(iso);

				throw (Exception)obj;
			}

			String retval = (String)((ArrayList<Object>)obj).get(0);

			cmd = new ArrayList<Object>(1);
			cmd.add("CLOSE");
			worker.in.put(cmd);
			tx.setIsolationLevel(iso);
			return retval;
		}

		public HostLookupPlan setParms(int node) throws Exception
		{
			if (node == -1)
			{
				if (HRDBMSWorker.type == HRDBMSWorker.TYPE_WORKER)
				{
					throw new Exception("A worker node is trying to look up node -1");
				}

				node = MetaData.myCoordNum();
			}
			RootOperator root = (RootOperator)p.getTrees().get(0);
			IndexOperator iOp = (IndexOperator)root.children().get(0).children().get(0);
			Index index = iOp.getIndex();
			index.setCondition(new Filter("NODES.NODEID", "E", Integer.toString(node)));
			return this;
		}
	}

	public static class IndexCardPlan
	{
		private final Plan p;

		public IndexCardPlan(Plan p)
		{
			this.p = p;
		}

		public Object execute(Transaction tx) throws Exception
		{
			int iso = tx.getIsolationLevel();
			tx.setIsolationLevel(Transaction.ISOLATION_UR);
			XAWorker worker = XAManager.executeCatalogQuery(p, tx);
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
				tx.setIsolationLevel(iso);

				throw (Exception)obj;
			}

			cmd = new ArrayList<Object>(1);
			cmd.add("CLOSE");
			worker.in.put(cmd);
			tx.setIsolationLevel(iso);
			return obj;
		}

		public IndexCardPlan setParms(int tableID, int indexID) throws Exception
		{
			Operator op = p.getTrees().get(0);
			while (!(op instanceof IndexOperator))
			{
				op = op.children().get(0);
			}
			IndexOperator iOp = (IndexOperator)op;
			Index index = iOp.getIndex();
			index.setCondition(new Filter("INDEXSTATS.TABLEID", "E", Integer.toString(tableID)));
			index.addSecondaryFilter(new Filter("INDEXSTATS.INDEXID", "E", Integer.toString(indexID)));
			return this;
		}
	}

	public static class IndexColCountPlan
	{
		private final Plan p;
		private int tableID;
		private int indexID;

		public IndexColCountPlan(Plan p)
		{
			this.p = p;
		}

		public int execute(Transaction tx) throws Exception
		{
			if (tableID >= 0 && tableID <= 12)
			{
				if (indexID == 0)
				{
					if (tableID == 0)
					{
						return 2;
					}
					else if (tableID == 1)
					{
						return 2;
					}
					else if (tableID == 2)
					{
						return 2;
					}
					else if (tableID == 3)
					{
						return 3;
					}
					else if (tableID == 4)
					{
						return 2;
					}
					else if (tableID == 5)
					{
						return 1;
					}
					else if (tableID == 6)
					{
						return 1;
					}
					else if (tableID == 7)
					{
						return 2;
					}
					else if (tableID == 8)
					{
						return 2;
					}
					else if (tableID == 9)
					{
						return 1;
					}
					else if (tableID == 10)
					{
						return 1;
					}
					else if (tableID == 11)
					{
						return 1;
					}
					else if (tableID == 12)
					{
						return 2;
					}
				}
				else if (indexID == 1)
				{
					if (tableID == 2)
					{
						return 1;
					}
					else if (tableID == 6)
					{
						return 1;
					}
				}

				throw new Exception("Catalog index not found");
			}

			int iso = tx.getIsolationLevel();
			tx.setIsolationLevel(Transaction.ISOLATION_RR);
			XAWorker worker = XAManager.executeCatalogQuery(p, tx);
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

			if (obj instanceof DataEndMarker)
			{
				cmd = new ArrayList<Object>(1);
				cmd.add("CLOSE");
				worker.in.put(cmd);
				tx.setIsolationLevel(iso);

				throw new Exception("Index not found");
			}

			if (obj instanceof Exception)
			{
				cmd = new ArrayList<Object>(1);
				cmd.add("CLOSE");
				worker.in.put(cmd);
				tx.setIsolationLevel(iso);

				throw (Exception)obj;
			}

			Integer retval = ((Long)((ArrayList<Object>)obj).get(0)).intValue();

			cmd = new ArrayList<Object>(1);
			cmd.add("CLOSE");
			worker.in.put(cmd);
			tx.setIsolationLevel(iso);
			return retval;
		}

		public IndexColCountPlan setParms(int tableID, int indexID) throws Exception
		{
			this.tableID = tableID;
			this.indexID = indexID;
			if (tableID >= 0 && tableID <= 12)
			{
				return this;
			}

			Operator op = p.getTrees().get(0);
			while (!(op instanceof IndexOperator))
			{
				op = op.children().get(0);
			}
			IndexOperator iOp = (IndexOperator)op;
			Index index = iOp.getIndex();
			index.setCondition(new Filter("INDEXCOLS.TABLEID", "E", Integer.toString(tableID)));
			index.addSecondaryFilter(new Filter("INDEXCOLS.INDEXID", "E", Integer.toString(indexID)));
			return this;
		}
	}

	public static class IndexColsForTablePlan
	{
		private final Plan p;
		private String schema;
		private String name;

		public IndexColsForTablePlan(Plan p)
		{
			this.p = p;
		}

		public ArrayList<Object> execute(Transaction tx) throws Exception
		{
			if (schema.equals("SYS"))
			{
				ArrayList<Object> retval = new ArrayList<Object>();
				ArrayList<Object> row = new ArrayList<Object>();
				if (name.equals("TABLES"))
				{
					row.add("SCHEMA");
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("TABNAME");
					retval.add(row);
				}
				else if (name.equals("INDEXES"))
				{
					row.add("TABLEID");
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("INDEXNAME");
					retval.add(row);
				}
				else if (name.equals("COLUMNS"))
				{
					row.add("TABLEID");
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("COLNAME");
					retval.add(row);
				}
				else if (name.equals("INDEXCOLS"))
				{
					row.add("TABLEID");
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("INDEXID");
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("COLID");
					retval.add(row);
				}
				else if (name.equals("TABLESTATS"))
				{
					row.add("TABLEID");
					retval.add(row);
				}
				else if (name.equals("COLSTATS"))
				{
					row.add("TABLEID");
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("COLID");
					retval.add(row);
				}
				else if (name.equals("INDEXSTATS"))
				{
					row.add("TABLEID");
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("INDEXID");
					retval.add(row);
				}
				else if (name.equals("NODES"))
				{
					row.add("NODEID");
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("HOSTNAME");
					retval.add(row);
				}
				else if (name.equals("PARTITIONING"))
				{
					row.add("TABLEID");
					retval.add(row);
				}
				else if (name.equals("COLDIST"))
				{
					row.add("TABLEID");
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("COLID");
					retval.add(row);
				}
				else if (name.equals("NODESTATE"))
				{
					row.add("NODEID");
					retval.add(row);
				}
				else if (name.equals("VIEWS"))
				{
					row.add("SCHEMA");
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("NAME");
					retval.add(row);
				}
				else if (name.equals("BACKUPS"))
				{
					row.add("FIRST");
					retval.add(row);
				}
				else
				{
					throw new Exception("Index not found");
				}

				return retval;
			}

			int iso = tx.getIsolationLevel();
			tx.setIsolationLevel(Transaction.ISOLATION_RR);
			XAWorker worker = XAManager.executeCatalogQuery(p, tx);
			worker.start();
			ArrayList<Object> cmd = new ArrayList<Object>(2);
			cmd.add("NEXT");
			cmd.add(1000000);
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

			ArrayList<Object> retval = new ArrayList<Object>();
			while (!(obj instanceof DataEndMarker))
			{
				if (obj instanceof Exception)
				{
					cmd = new ArrayList<Object>(1);
					cmd.add("CLOSE");
					worker.in.put(cmd);
					tx.setIsolationLevel(iso);
					throw (Exception)obj;
				}

				retval.add(obj);

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
			}

			cmd = new ArrayList<Object>(1);
			cmd.add("CLOSE");
			worker.in.put(cmd);
			tx.setIsolationLevel(iso);
			return retval;
		}

		public IndexColsForTablePlan setParms(String schema, String name) throws Exception
		{
			this.schema = schema;
			this.name = name;
			if (schema.equals("SYS"))
			{
				return this;
			}
			Operator op = p.getTrees().get(0);
			while (op.children().size() != 0)
			{
				op = op.children().get(0);
			}

			Index index = ((IndexOperator)op).getIndex();
			index.setCondition(new Filter("TABLES.SCHEMA", "E", "'" + schema + "'"));
			index.addSecondaryFilter(new Filter("TABLES.TABNAME", "E", "'" + name + "'"));
			return this;
		}
	}

	public static class IndexIDsForTablePlan
	{
		private final Plan p;
		private String schema;
		private String name;

		public IndexIDsForTablePlan(Plan p)
		{
			this.p = p;
		}

		public ArrayList<Object> execute(Transaction tx) throws Exception
		{
			if (schema.equals("SYS"))
			{
				ArrayList<Object> retval = new ArrayList<Object>();
				ArrayList<Object> row = new ArrayList<Object>();
				if (name.equals("TABLES"))
				{
					row.add(0);
					retval.add(row);
				}
				else if (name.equals("INDEXES"))
				{
					row.add(0);
					retval.add(row);
					row = new ArrayList<Object>();
					row.add(1);
					retval.add(row);
				}
				else if (name.equals("COLUMNS"))
				{
					row.add(0);
					retval.add(row);
				}
				else if (name.equals("INDEXCOLS"))
				{
					row.add(0);
					retval.add(row);
				}
				else if (name.equals("TABLESTATS"))
				{
					row.add(0);
					retval.add(row);
				}
				else if (name.equals("COLSTATS"))
				{
					row.add(0);
					retval.add(row);
				}
				else if (name.equals("INDEXSTATS"))
				{
					row.add(0);
					retval.add(row);
				}
				else if (name.equals("NODES"))
				{
					row.add(0);
					retval.add(row);
					row = new ArrayList<Object>();
					row.add(1);
					retval.add(row);
				}
				else if (name.equals("PARTITIONING"))
				{
					row.add(0);
					retval.add(row);
				}
				else if (name.equals("COLDIST"))
				{
					row.add(0);
					retval.add(row);
				}
				else if (name.equals("NODESTATE"))
				{
					row.add(0);
					retval.add(row);
				}
				else if (name.equals("VIEWS"))
				{
					row.add(0);
					retval.add(row);
				}
				else if (name.equals("BACKUPS"))
				{
					row.add(0);
					retval.add(row);
				}
				else
				{
					throw new Exception("Table not found");
				}

				return retval;
			}

			int iso = tx.getIsolationLevel();
			tx.setIsolationLevel(Transaction.ISOLATION_RR);
			XAWorker worker = XAManager.executeCatalogQuery(p, tx);
			worker.start();
			ArrayList<Object> cmd = new ArrayList<Object>(2);
			cmd.add("NEXT");
			cmd.add(1000000);
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

			ArrayList<Object> retval = new ArrayList<Object>();
			while (!(obj instanceof DataEndMarker))
			{
				if (obj instanceof Exception)
				{
					cmd = new ArrayList<Object>(1);
					cmd.add("CLOSE");
					worker.in.put(cmd);
					tx.setIsolationLevel(iso);
					throw (Exception)obj;
				}

				retval.add(obj);

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
			}

			cmd = new ArrayList<Object>(1);
			cmd.add("CLOSE");
			worker.in.put(cmd);
			tx.setIsolationLevel(iso);
			return retval;
		}

		public IndexIDsForTablePlan setParms(String schema, String name) throws Exception
		{
			this.schema = schema;
			this.name = name;
			if (schema.equals("SYS"))
			{
				return this;
			}
			Operator op = p.getTrees().get(0);
			while (op.children().size() != 0)
			{
				op = op.children().get(0);
			}

			Index index = ((IndexOperator)op).getIndex();
			index.setCondition(new Filter("TABLES.SCHEMA", "E", "'" + schema + "'"));
			index.addSecondaryFilter(new Filter("TABLES.TABNAME", "E", "'" + name + "'"));
			return this;
		}
	}

	public static class IndexPlan
	{
		private final Plan p;
		private String schema;
		private String table;

		public IndexPlan(Plan p)
		{
			this.p = p;
		}

		public ArrayList<Object> execute(Transaction tx) throws Exception
		{
			if (schema.equals("SYS"))
			{
				ArrayList<Object> retval = new ArrayList<Object>();
				ArrayList<Object> row = new ArrayList<Object>();
				if (table.equals("TABLES"))
				{
					row.add("PKTABLES");
					retval.add(row);
				}
				else if (table.equals("INDEXES"))
				{
					row.add("PKINDEXES");
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("SKINDEXES");
					retval.add(row);
				}
				else if (table.equals("COLUMNS"))
				{
					row.add("PKCOLUMNS");
					retval.add(row);
				}
				else if (table.equals("INDEXCOLS"))
				{
					row.add("PKINDEXCOLS");
					retval.add(row);
				}
				else if (table.equals("TABLESTATS"))
				{
					row.add("PKTABLESTATS");
					retval.add(row);
				}
				else if (table.equals("COLSTATS"))
				{
					row.add("PKCOLSTATS");
					retval.add(row);
				}
				else if (table.equals("INDEXSTATS"))
				{
					row.add("PKINDEXSTATS");
					retval.add(row);
				}
				else if (table.equals("NODES"))
				{
					row.add("PKNODES");
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("SKNODES");
					retval.add(row);
				}
				else if (table.equals("PARTITIONING"))
				{
					row.add("PKPARTITIONING");
					retval.add(row);
				}
				else if (table.equals("COLDIST"))
				{
					row.add("PKCOLDIST");
					retval.add(row);
				}
				else if (table.equals("NODESTATE"))
				{
					row.add("PKNODESTATE");
					retval.add(row);
				}
				else if (table.equals("VIEWS"))
				{
					row.add("PKVIEWS");
					retval.add(row);
				}
				else if (table.equals("BACKUPS"))
				{
					row.add("PKBACKUPS");
					retval.add(row);
				}
				else
				{
					throw new Exception("Table not found");
				}

				return retval;
			}

			int iso = tx.getIsolationLevel();
			tx.setIsolationLevel(Transaction.ISOLATION_RR);
			XAWorker worker = XAManager.executeCatalogQuery(p, tx);
			worker.start();
			ArrayList<Object> cmd = new ArrayList<Object>(2);
			cmd.add("NEXT");
			cmd.add(1000000);
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

			ArrayList<Object> retval = new ArrayList<Object>();
			while (!(obj instanceof DataEndMarker))
			{
				if (obj instanceof Exception)
				{
					cmd = new ArrayList<Object>(1);
					cmd.add("CLOSE");
					worker.in.put(cmd);
					tx.setIsolationLevel(iso);
					throw (Exception)obj;
				}

				retval.add(obj);

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
			}

			cmd = new ArrayList<Object>(1);
			cmd.add("CLOSE");
			worker.in.put(cmd);
			tx.setIsolationLevel(iso);
			return retval;
		}

		public IndexPlan setParms(String schema, String table) throws Exception
		{
			this.schema = schema;
			this.table = table;
			if (schema.equals("SYS"))
			{
				return this;
			}
			RootOperator root = (RootOperator)p.getTrees().get(0);
			IndexOperator iOp = (IndexOperator)root.children().get(0).children().get(0).children().get(0).children().get(0).children().get(0);
			Index index = iOp.getIndex();
			index.setCondition(new Filter("TABLES.SCHEMA", "E", "'" + schema + "'"));
			index.addSecondaryFilter(new Filter("TABLES.TABNAME", "E", "'" + table + "'"));
			return this;
		}
	}

	public static class InsertColPlan
	{
		private int cid;
		private int tid;
		private String name;
		private String type;
		private int length;
		private int scale;
		private int pkpos;
		private boolean nullable;

		public void execute(Transaction tx) throws Exception
		{
			String nullString = null;
			if (nullable)
			{
				nullString = "'Y'";
			}
			else
			{
				nullString = "'N'";
			}
			String sql = "INSERT INTO SYS.COLUMNS VALUES(" + cid + "," + tid + ",'" + name + "','" + type + "'," + length + "," + scale + "," + pkpos + "," + nullString + ")";
			XAWorker worker = XAManager.executeAuthorizedUpdate(sql, tx);
			worker.start();
			worker.join();
			int updateCount = worker.getUpdateCount();
			if (updateCount == -1)
			{
				throw worker.getException();
			}
		}

		public InsertColPlan setParms(int cid, int tid, String name, String type, int length, int scale, int pkpos, boolean nullable)
		{
			this.cid = cid;
			this.tid = tid;
			this.name = name;
			this.type = type;
			this.length = length;
			this.scale = scale;
			this.pkpos = pkpos;
			this.nullable = nullable;
			return this;
		}
	}

	public static class InsertIndexColPlan
	{
		private int tableID;
		private int indexID;
		private int colID;
		private int pos;
		private boolean asc;

		public void execute(Transaction tx) throws Exception
		{
			String ascString = null;
			if (asc)
			{
				ascString = "'A'";
			}
			else
			{
				ascString = "'D'";
			}
			String sql = "INSERT INTO SYS.INDEXCOLS VALUES(" + indexID + "," + tableID + "," + colID + "," + pos + "," + ascString + ")";
			XAWorker worker = XAManager.executeAuthorizedUpdate(sql, tx);
			worker.start();
			worker.join();
			int updateCount = worker.getUpdateCount();
			if (updateCount == -1)
			{
				throw worker.getException();
			}
		}

		public InsertIndexColPlan setParms(int indexID, int tableID, int colID, int pos, boolean asc)
		{
			this.tableID = tableID;
			this.indexID = indexID;
			this.colID = colID;
			this.pos = pos;
			this.asc = asc;
			return this;
		}
	}

	public static class InsertIndexPlan
	{
		private int iid;
		private int tid;
		private String name;
		private boolean unique;

		public void execute(Transaction tx) throws Exception
		{
			String uString = null;
			if (unique)
			{
				uString = "'Y'";
			}
			else
			{
				uString = "'N'";
			}
			String sql = "INSERT INTO SYS.INDEXES VALUES(" + iid + ",'" + name + "'," + tid + "," + uString + ")";
			XAWorker worker = XAManager.executeAuthorizedUpdate(sql, tx);
			worker.start();
			worker.join();
			int updateCount = worker.getUpdateCount();
			if (updateCount == -1)
			{
				throw worker.getException();
			}
		}

		public InsertIndexPlan setParms(int iid, String name, int tid, boolean unique)
		{
			this.iid = iid;
			this.tid = tid;
			this.name = name;
			this.unique = unique;
			return this;
		}
	}

	public static class InsertPartitionPlan
	{
		private int id;
		private String group;
		private String node;
		private String dev;

		public void execute(Transaction tx) throws Exception
		{
			String sql = "INSERT INTO SYS.PARTITIONING VALUES(" + id + ",'" + group + "','" + node + "','" + dev + "')";
			XAWorker worker = XAManager.executeAuthorizedUpdate(sql, tx);
			worker.start();
			worker.join();
			int updateCount = worker.getUpdateCount();
			if (updateCount == -1)
			{
				throw worker.getException();
			}
		}

		public InsertPartitionPlan setParms(int id, String group, String node, String dev)
		{
			this.id = id;
			this.group = group;
			this.node = node;
			this.dev = dev;
			return this;
		}
	}

	public static class InsertTablePlan
	{
		private int tableID;
		private String schema;
		private String table;
		private String type;

		public void execute(Transaction tx) throws Exception
		{
			String sql = "INSERT INTO SYS.TABLES VALUES(" + tableID + "," + "'" + schema + "'" + "," + "'" + table + "'" + "," + "'" + type + "')";
			XAWorker worker = XAManager.executeAuthorizedUpdate(sql, tx);
			worker.start();
			worker.join();
			int updateCount = worker.getUpdateCount();
			if (updateCount == -1)
			{
				throw worker.getException();
			}
		}

		public InsertTablePlan setParms(int tableID, String schema, String table, String type)
		{
			this.tableID = tableID;
			this.schema = schema;
			this.table = table;
			this.type = type;
			return this;
		}
	}

	public static class InsertViewPlan
	{
		private int id;
		private String schema;
		private String table;
		private String text;

		public void execute(Transaction tx) throws Exception
		{
			String textCopy = text.replace("'", "\\'");
			String sql = "INSERT INTO SYS.VIEWS VALUES(" + id + ",'" + schema + "','" + table + "','" + textCopy + "')";
			// HRDBMSWorker.logger.debug(sql);
			XAWorker worker = XAManager.executeAuthorizedUpdate(sql, tx);
			worker.start();
			worker.join();
			int updateCount = worker.getUpdateCount();
			if (updateCount == -1)
			{
				throw worker.getException();
			}
		}

		public InsertViewPlan setParms(int id, String schema, String table, String text)
		{
			this.id = id;
			this.schema = schema;
			this.table = table;
			this.text = text;
			return this;
		}
	}

	public static class KeysByIDPlan
	{
		private final Plan p;
		private int tableID;
		private int indexID;

		public KeysByIDPlan(Plan p)
		{
			this.p = p;
		}

		public ArrayList<Object> execute(Transaction tx) throws Exception
		{
			if (tableID >= 0 && tableID <= 12)
			{
				ArrayList<Object> retval = new ArrayList<Object>();
				ArrayList<Object> row = new ArrayList<Object>();
				if (tableID == 0 && indexID == 0)
				{
					row.add("SCHEMA");
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("TABNAME");
					retval.add(row);
				}
				else if (tableID == 2 && indexID == 0)
				{
					row.add("TABLEID");
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("INDEXNAME");
					retval.add(row);
				}
				else if (tableID == 2 && indexID == 1)
				{
					row.add("INDEXNAME");
					retval.add(row);
				}
				else if (tableID == 1 && indexID == 0)
				{
					row.add("TABLEID");
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("COLNAME");
					retval.add(row);
				}
				else if (tableID == 3 && indexID == 0)
				{
					row.add("TABLEID");
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("INDEXID");
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("COLID");
					retval.add(row);
				}
				else if (tableID == 5 && indexID == 0)
				{
					row.add("TABLEID");
					retval.add(row);
				}
				else if (tableID == 7 && indexID == 0)
				{
					row.add("TABLEID");
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("COLID");
					retval.add(row);
				}
				else if (tableID == 12 && indexID == 0)
				{
					row.add("TABLEID");
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("INDEXID");
					retval.add(row);
				}
				else if (tableID == 6 && indexID == 0)
				{
					row.add("HOSTNAME");
					retval.add(row);
				}
				else if (tableID == 6 && indexID == 1)
				{
					row.add("NODEID");
					retval.add(row);
				}
				else if (tableID == 11 && indexID == 0)
				{
					row.add("TABLEID");
					retval.add(row);
				}
				else if (tableID == 8 && indexID == 0)
				{
					row.add("TABLEID");
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("COLID");
					retval.add(row);
				}
				else if (tableID == 10 && indexID == 0)
				{
					row.add("NODEID");
					retval.add(row);
				}
				else if (tableID == 4 && indexID == 0)
				{
					row.add("SCHEMA");
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("NAME");
					retval.add(row);
				}
				else if (tableID == 9 && indexID == 0)
				{
					row.add("FIRST");
					retval.add(row);
				}
				else
				{
					throw new Exception("Index not found");
				}

				return retval;
			}

			int iso = tx.getIsolationLevel();
			tx.setIsolationLevel(Transaction.ISOLATION_RR);
			XAWorker worker = XAManager.executeCatalogQuery(p, tx);
			worker.start();
			ArrayList<Object> cmd = new ArrayList<Object>(2);
			cmd.add("NEXT");
			cmd.add(1000000);
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

			ArrayList<Object> retval = new ArrayList<Object>();
			while (!(obj instanceof DataEndMarker))
			{
				if (obj instanceof Exception)
				{
					cmd = new ArrayList<Object>(1);
					cmd.add("CLOSE");
					worker.in.put(cmd);
					tx.setIsolationLevel(iso);
					throw (Exception)obj;
				}

				retval.add(obj);

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
			}

			cmd = new ArrayList<Object>(1);
			cmd.add("CLOSE");
			worker.in.put(cmd);
			tx.setIsolationLevel(iso);
			return retval;
		}

		public KeysByIDPlan setParms(int tableID, int indexID) throws Exception
		{
			this.tableID = tableID;
			this.indexID = indexID;
			if (tableID >= 0 && tableID <= 12)
			{
				return this;
			}
			Operator op = p.getTrees().get(0);
			while (op.children().size() != 0)
			{
				op = op.children().get(0);
			}

			Index index = ((IndexOperator)op).getIndex();
			index.setCondition(new Filter("INDEXCOLS.TABLEID", "E", Integer.toString(tableID)));
			index.addSecondaryFilter(new Filter("INDEXCOLS.INDEXID", "E", Integer.toString(indexID)));
			return this;
		}
	}

	public static class KeysPlan
	{
		private final Plan p;
		private String schema;
		private String name;

		public KeysPlan(Plan p)
		{
			this.p = p;
		}

		public ArrayList<Object> execute(Transaction tx) throws Exception
		{
			if (schema.equals("SYS"))
			{
				ArrayList<Object> retval = new ArrayList<Object>();
				ArrayList<Object> row = new ArrayList<Object>();
				if (name.equals("PKTABLES"))
				{
					row.add("TABLES");
					row.add("SCHEMA");
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("TABLES");
					row.add("TABNAME");
					retval.add(row);
				}
				else if (name.equals("PKINDEXES"))
				{
					row.add("INDEXES");
					row.add("TABLEID");
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("INDEXES");
					row.add("INDEXNAME");
					retval.add(row);
				}
				else if (name.equals("SKINDEXES"))
				{
					row.add("INDEXES");
					row.add("INDEXNAME");
					retval.add(row);
				}
				else if (name.equals("PKCOLUMNS"))
				{
					row.add("COLUMNS");
					row.add("TABLEID");
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("COLUMNS");
					row.add("COLNAME");
					retval.add(row);
				}
				else if (name.equals("PKINDEXCOLS"))
				{
					row.add("INDEXCOLS");
					row.add("TABLEID");
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("INDEXCOLS");
					row.add("INDEXID");
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("INDEXCOLS");
					row.add("COLID");
					retval.add(row);
				}
				else if (name.equals("PKTABLESTATS"))
				{
					row.add("TABLESTATS");
					row.add("TABLEID");
					retval.add(row);
				}
				else if (name.equals("PKCOLSTATS"))
				{
					row.add("COLSTATS");
					row.add("TABLEID");
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("COLSTATS");
					row.add("COLID");
					retval.add(row);
				}
				else if (name.equals("PKINDEXSTATS"))
				{
					row.add("INDEXSTATS");
					row.add("TABLEID");
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("INDEXSTATS");
					row.add("INDEXID");
					retval.add(row);
				}
				else if (name.equals("PKNODES"))
				{
					row.add("NODES");
					row.add("HOSTNAME");
					retval.add(row);
				}
				else if (name.equals("SKNODES"))
				{
					row.add("NODES");
					row.add("NODEID");
					retval.add(row);
				}
				else if (name.equals("PKPARTITIONING"))
				{
					row.add("PARTITIONING");
					row.add("TABLEID");
					retval.add(row);
				}
				else if (name.equals("PKCOLDIST"))
				{
					row.add("COLDIST");
					row.add("TABLEID");
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("COLDIST");
					row.add("COLID");
					retval.add(row);
				}
				else if (name.equals("PKNODESTATE"))
				{
					row.add("NODESTATE");
					row.add("NODEID");
					retval.add(row);
				}
				else if (name.equals("PKVIEWS"))
				{
					row.add("VIEWS");
					row.add("SCHEMA");
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("VIEWS");
					row.add("NAME");
					retval.add(row);
				}
				else if (name.equals("PKBACKUPS"))
				{
					row.add("BACKUPS");
					row.add("FIRST");
					retval.add(row);
				}
				else
				{
					throw new Exception("Index not found");
				}

				return retval;
			}

			int iso = tx.getIsolationLevel();
			tx.setIsolationLevel(Transaction.ISOLATION_RR);
			XAWorker worker = XAManager.executeCatalogQuery(p, tx);
			worker.start();
			ArrayList<Object> cmd = new ArrayList<Object>(2);
			cmd.add("NEXT");
			cmd.add(1000000);
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

			ArrayList<Object> retval = new ArrayList<Object>();
			while (!(obj instanceof DataEndMarker))
			{
				if (obj instanceof Exception)
				{
					cmd = new ArrayList<Object>(1);
					cmd.add("CLOSE");
					worker.in.put(cmd);
					tx.setIsolationLevel(iso);
					throw (Exception)obj;
				}

				retval.add(obj);

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
			}

			cmd = new ArrayList<Object>(1);
			cmd.add("CLOSE");
			worker.in.put(cmd);
			tx.setIsolationLevel(iso);
			return retval;
		}

		public KeysPlan setParms(String schema, String name) throws Exception
		{
			this.schema = schema;
			this.name = name;
			if (schema.equals("SYS"))
			{
				return this;
			}
			Operator op = p.getTrees().get(0);
			while (op.children().size() != 0)
			{
				op = op.children().get(0);
			}

			Index index = ((IndexOperator)op).getIndex();
			index.setCondition(new Filter("INDEXES.INDEXNAME", "E", "'" + name + "'"));

			while (!(op instanceof HashJoinOperator))
			{
				if (op instanceof TableScanOperator)
				{
					op = ((TableScanOperator)op).firstParent();
				}
				else
				{
					op = op.parent();
				}
			}

			op = op.children().get(1).children().get(0);
			index = ((IndexOperator)op).getIndex();
			index.setCondition(new Filter("TABLES.SCHEMA", "E", "'" + schema + "'"));
			return this;
		}
	}

	public static class LengthPlan
	{
		private final Plan p;
		private String schema;
		private String table;
		private String col;

		public LengthPlan(Plan p)
		{
			this.p = p;
		}

		public int execute(Transaction tx) throws Exception
		{
			if (schema.equals("SYS"))
			{
				if (table.equals("TABLES"))
				{
					if (col.equals("SCHEMA") || col.equals("TABNAME"))
					{
						return 128;
					}

					if (col.equals("TYPE"))
					{
						return 1;
					}
				}
				else if (table.equals("COLUMNS"))
				{
					if (col.equals("COLNAME"))
					{
						return 128;
					}

					if (col.equals("COLTYPE"))
					{
						return 16;
					}

					if (col.equals("NULLABLE"))
					{
						return 1;
					}
				}
				else if (table.equals("INDEXES"))
				{
					if (col.equals("INDEXNAME"))
					{
						return 128;
					}

					if (col.equals("UNIQUE"))
					{
						return 1;
					}
				}
				else if (table.equals("INDEXCOLS"))
				{
					if (col.equals("ORDER"))
					{
						return 1;
					}
				}
				else if (table.equals("VIEWS"))
				{
					if (col.equals("SCHEMA") || col.equals("NAME"))
					{
						return 128;
					}

					if (col.equals("TEXT"))
					{
						return 32768;
					}
				}
				else if (table.equals("NODES"))
				{
					if (col.equals("HOSTNAME") || col.equals("RACK"))
					{
						return 128;
					}

					if (col.equals("TYPE"))
					{
						return 1;
					}
				}
				else if (table.equals("COLDIST"))
				{
					if (col.equals("LOW") || col.equals("HIGH") || col.equals("Q1") || col.equals("Q2") || col.equals("Q3"))
					{
						return 4096;
					}
				}
				else if (table.equals("PARTITIONING"))
				{
					if (col.equals("GROUPEXP") || col.equals("NODEEXP") || col.equals("DEVICEEXP"))
					{
						return 8192;
					}
				}
				else if (table.equals("NODESTATE"))
				{
					if (col.equals("STATE"))
					{
						return 1;
					}
				}

				throw new Exception("Column not found");
			}

			int iso = tx.getIsolationLevel();
			tx.setIsolationLevel(Transaction.ISOLATION_RR);
			XAWorker worker = XAManager.executeCatalogQuery(p, tx);
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
				tx.setIsolationLevel(iso);

				throw (Exception)obj;
			}

			if (obj instanceof DataEndMarker)
			{
				cmd = new ArrayList<Object>(1);
				cmd.add("CLOSE");
				worker.in.put(cmd);
				tx.setIsolationLevel(iso);

				throw new Exception("Column not found");
			}

			cmd = new ArrayList<Object>(1);
			cmd.add("CLOSE");
			worker.in.put(cmd);
			tx.setIsolationLevel(iso);
			return (Integer)((ArrayList<Object>)obj).get(0);
		}

		public LengthPlan setParms(String schema, String table, String col) throws Exception
		{
			this.table = table;
			this.schema = schema;
			this.col = col;
			if (schema.equals("SYS"))
			{
				return this;
			}

			Operator op = p.getTrees().get(0);
			while (!(op instanceof IndexOperator))
			{
				op = op.children().get(0);
			}
			IndexOperator iOp = (IndexOperator)op;
			Index index = iOp.getIndex();
			index.setCondition(new Filter("TABLES.SCHEMA", "E", "'" + schema + "'"));
			index.addSecondaryFilter(new Filter("TABLES.TABNAME", "E", "'" + table + "'"));
			while (!(op instanceof SelectOperator))
			{
				if (op instanceof TableScanOperator)
				{
					op = ((TableScanOperator)op).firstParent();
				}
				else
				{
					op = op.parent();
				}
			}
			SelectOperator select = (SelectOperator)op;
			select.getFilter().remove(0);
			select.getFilter().add(new Filter("B.COLNAME", "E", "'" + col + "'"));
			return this;
		}
	}

	public static class MultiDeleteIndexColsPlan
	{
		private int tableID;

		public void execute(Transaction tx) throws Exception
		{
			String sql = "DELETE FROM SYS.INDEXCOLS WHERE TABLEID = " + tableID;
			XAWorker worker = XAManager.executeAuthorizedUpdate(sql, tx);
			worker.start();
			worker.join();
			int updateCount = worker.getUpdateCount();
			if (updateCount == -1)
			{
				throw worker.getException();
			}
		}

		public MultiDeleteIndexColsPlan setParms(int tableID)
		{
			this.tableID = tableID;
			return this;
		}
	}

	public static class MultiDeleteIndexesPlan
	{
		private int tableID;

		public void execute(Transaction tx) throws Exception
		{
			String sql = "DELETE FROM SYS.INDEXES WHERE TABLEID = " + tableID;
			XAWorker worker = XAManager.executeAuthorizedUpdate(sql, tx);
			worker.start();
			worker.join();
			int updateCount = worker.getUpdateCount();
			if (updateCount == -1)
			{
				throw worker.getException();
			}
		}

		public MultiDeleteIndexesPlan setParms(int tableID)
		{
			this.tableID = tableID;
			return this;
		}
	}

	public static class MultiDeleteIndexStatsPlan
	{
		private int tableID;

		public void execute(Transaction tx) throws Exception
		{
			String sql = "DELETE FROM SYS.INDEXSTATS WHERE TABLEID = " + tableID;
			XAWorker worker = XAManager.executeAuthorizedUpdate(sql, tx);
			worker.start();
			worker.join();
			int updateCount = worker.getUpdateCount();
			if (updateCount == -1)
			{
				throw worker.getException();
			}
		}

		public MultiDeleteIndexStatsPlan setParms(int tableID)
		{
			this.tableID = tableID;
			return this;
		}
	}

	public static class NextIndexIDPlan
	{
		private final Plan p;

		public NextIndexIDPlan(Plan p)
		{
			this.p = p;
		}

		public int execute(Transaction tx) throws Exception
		{
			int iso = tx.getIsolationLevel();
			tx.setIsolationLevel(Transaction.ISOLATION_RR);
			XAWorker worker = XAManager.executeCatalogQuery(p, tx);
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

			if (obj instanceof DataEndMarker)
			{
				cmd = new ArrayList<Object>(1);
				cmd.add("CLOSE");
				worker.in.put(cmd);
				tx.setIsolationLevel(iso);
				return 0;
			}

			if (obj instanceof Exception)
			{
				cmd = new ArrayList<Object>(1);
				cmd.add("CLOSE");
				worker.in.put(cmd);
				tx.setIsolationLevel(iso);

				throw (Exception)obj;
			}

			int retval = (Integer)((ArrayList<Object>)obj).get(0);

			cmd = new ArrayList<Object>(1);
			cmd.add("CLOSE");
			worker.in.put(cmd);
			tx.setIsolationLevel(iso);
			return retval + 1;
		}

		public NextIndexIDPlan setParms(int tableID) throws Exception
		{
			Operator op = p.getTrees().get(0);
			while (op.children().size() != 0)
			{
				op = op.children().get(0);
			}

			Index index = ((IndexOperator)op).getIndex();
			index.setCondition(new Filter("INDEXES.TABLEID", "E", Integer.toString(tableID)));
			return this;
		}
	}

	public static class NextTableIDPlan
	{
		private final Plan p;

		public NextTableIDPlan(Plan p)
		{
			this.p = p;
		}

		public int execute(Transaction tx) throws Exception
		{
			int iso = tx.getIsolationLevel();
			tx.setIsolationLevel(Transaction.ISOLATION_RR);
			XAWorker worker = XAManager.executeCatalogQuery(p, tx);
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

			if (obj instanceof DataEndMarker)
			{
				cmd = new ArrayList<Object>(1);
				cmd.add("CLOSE");
				worker.in.put(cmd);
				tx.setIsolationLevel(iso);

				throw new Exception("Unable to get next table ID");
			}

			if (obj instanceof Exception)
			{
				cmd = new ArrayList<Object>(1);
				cmd.add("CLOSE");
				worker.in.put(cmd);
				tx.setIsolationLevel(iso);

				throw (Exception)obj;
			}

			int retval = (Integer)((ArrayList<Object>)obj).get(0);

			cmd = new ArrayList<Object>(1);
			cmd.add("CLOSE");
			worker.in.put(cmd);
			tx.setIsolationLevel(iso);
			return retval + 1;
		}

		public NextTableIDPlan setParms() throws Exception
		{
			return this;
		}
	}

	public static class NextViewIDPlan
	{
		private final Plan p;

		public NextViewIDPlan(Plan p)
		{
			this.p = p;
		}

		public int execute(Transaction tx) throws Exception
		{
			int iso = tx.getIsolationLevel();
			tx.setIsolationLevel(Transaction.ISOLATION_RR);
			XAWorker worker = XAManager.executeCatalogQuery(p, tx);
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

			if (obj instanceof DataEndMarker)
			{
				cmd = new ArrayList<Object>(1);
				cmd.add("CLOSE");
				worker.in.put(cmd);
				tx.setIsolationLevel(iso);

				return 0;
			}

			if (obj instanceof Exception)
			{
				cmd = new ArrayList<Object>(1);
				cmd.add("CLOSE");
				worker.in.put(cmd);
				tx.setIsolationLevel(iso);

				throw (Exception)obj;
			}

			int retval = (Integer)((ArrayList<Object>)obj).get(0);

			cmd = new ArrayList<Object>(1);
			cmd.add("CLOSE");
			worker.in.put(cmd);
			tx.setIsolationLevel(iso);
			return retval + 1;
		}

		public NextViewIDPlan setParms() throws Exception
		{
			return this;
		}
	}

	public static class OrdersPlan
	{
		private final Plan p;
		private String schema;
		private String name;

		public OrdersPlan(Plan p)
		{
			this.p = p;
		}

		public ArrayList<Object> execute(Transaction tx) throws Exception
		{
			if (schema.equals("SYS"))
			{
				ArrayList<Object> retval = new ArrayList<Object>();
				ArrayList<Object> row = new ArrayList<Object>();
				if (name.equals("PKTABLES"))
				{
					row.add("A");
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("A");
					retval.add(row);
				}
				else if (name.equals("PKINDEXES"))
				{
					row.add("A");
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("A");
					retval.add(row);
				}
				else if (name.equals("SKINDEXES"))
				{
					row.add("A");
					retval.add(row);
				}
				else if (name.equals("PKCOLUMNS"))
				{
					row.add("A");
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("A");
					retval.add(row);
				}
				else if (name.equals("PKINDEXCOLS"))
				{
					row.add("A");
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("A");
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("A");
					retval.add(row);
				}
				else if (name.equals("PKTABLESTATS"))
				{
					row.add("A");
					retval.add(row);
				}
				else if (name.equals("PKCOLSTATS"))
				{
					row.add("A");
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("A");
					retval.add(row);
				}
				else if (name.equals("PKINDEXSTATS"))
				{
					row.add("A");
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("A");
					retval.add(row);
				}
				else if (name.equals("PKNODES"))
				{
					row.add("A");
					retval.add(row);
				}
				else if (name.equals("SKNODES"))
				{
					row.add("A");
					retval.add(row);
				}
				else if (name.equals("PKPARTITIONING"))
				{
					row.add("A");
					retval.add(row);
				}
				else if (name.equals("PKCOLDIST"))
				{
					row.add("A");
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("A");
					retval.add(row);
				}
				else if (name.equals("PKNODESTATE"))
				{
					row.add("A");
					retval.add(row);
				}
				else if (name.equals("PKVIEWS"))
				{
					row.add("A");
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("A");
					retval.add(row);
				}
				else if (name.equals("PKBACKUPS"))
				{
					row.add("A");
					retval.add(row);
				}
				else
				{
					throw new Exception("Index not found");
				}

				return retval;
			}

			int iso = tx.getIsolationLevel();
			tx.setIsolationLevel(Transaction.ISOLATION_RR);
			XAWorker worker = XAManager.executeCatalogQuery(p, tx);
			worker.start();
			ArrayList<Object> cmd = new ArrayList<Object>(2);
			cmd.add("NEXT");
			cmd.add(1000000);
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

			ArrayList<Object> retval = new ArrayList<Object>();
			while (!(obj instanceof DataEndMarker))
			{
				if (obj instanceof Exception)
				{
					cmd = new ArrayList<Object>(1);
					cmd.add("CLOSE");
					worker.in.put(cmd);
					tx.setIsolationLevel(iso);
					throw (Exception)obj;
				}

				retval.add(obj);

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
			}

			cmd = new ArrayList<Object>(1);
			cmd.add("CLOSE");
			worker.in.put(cmd);
			tx.setIsolationLevel(iso);
			return retval;
		}

		public OrdersPlan setParms(String schema, String name) throws Exception
		{
			this.schema = schema;
			this.name = name;
			if (schema.equals("SYS"))
			{
				return this;
			}
			Operator op = p.getTrees().get(0);
			while (op.children().size() != 0)
			{
				op = op.children().get(0);
			}

			Index index = ((IndexOperator)op).getIndex();
			index.setCondition(new Filter("INDEXES.INDEXNAME", "E", "'" + name + "'"));

			while (!(op instanceof HashJoinOperator))
			{
				if (op instanceof TableScanOperator)
				{
					op = ((TableScanOperator)op).firstParent();
				}
				else
				{
					op = op.parent();
				}
			}

			op = op.children().get(1).children().get(0);
			index = ((IndexOperator)op).getIndex();
			index.setCondition(new Filter("TABLES.SCHEMA", "E", "'" + schema + "'"));
			return this;
		}
	}

	public static class PartitioningPlan
	{
		private final Plan p;
		private String schema;
		private String table;

		public PartitioningPlan(Plan p)
		{
			this.p = p;
		}

		public ArrayList<Object> execute(Transaction tx) throws Exception
		{
			if (schema.equals("SYS"))
			{
				ArrayList<Object> retval = new ArrayList<Object>();
				retval.add("NONE");
				retval.add("{-1}");
				retval.add("{0}");
				return retval;
			}

			int iso = tx.getIsolationLevel();
			tx.setIsolationLevel(Transaction.ISOLATION_RR);
			XAWorker worker = XAManager.executeCatalogQuery(p, tx);
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

			if (obj instanceof DataEndMarker)
			{
				cmd = new ArrayList<Object>(1);
				cmd.add("CLOSE");
				worker.in.put(cmd);
				tx.setIsolationLevel(iso);

				throw new Exception("Table not found: " + schema + "." + table);
			}

			if (obj instanceof Exception)
			{
				cmd = new ArrayList<Object>(1);
				cmd.add("CLOSE");
				worker.in.put(cmd);
				tx.setIsolationLevel(iso);
				throw (Exception)obj;
			}

			ArrayList<Object> retval = (ArrayList<Object>)obj;

			cmd = new ArrayList<Object>(1);
			cmd.add("CLOSE");
			worker.in.put(cmd);
			tx.setIsolationLevel(iso);
			return retval;
		}

		public PartitioningPlan setParms(String schema, String table) throws Exception
		{
			this.schema = schema;
			this.table = table;
			if (schema.equals("SYS"))
			{
				return this;
			}
			RootOperator root = (RootOperator)p.getTrees().get(0);
			IndexOperator iOp = (IndexOperator)root.children().get(0).children().get(0).children().get(0).children().get(0);
			Index index = iOp.getIndex();
			index.setCondition(new Filter("TABLES.SCHEMA", "E", "'" + schema + "'"));
			index.addSecondaryFilter(new Filter("TABLES.TABNAME", "E", "'" + table + "'"));
			return this;
		}
	}

	public static class TableAndIndexIDPlan
	{
		private final Plan p;
		private String schema;
		private String name;

		public TableAndIndexIDPlan(Plan p)
		{
			this.p = p;
		}

		public ArrayList<Object> execute(Transaction tx) throws Exception
		{
			if (schema.equals("SYS"))
			{
				ArrayList<Object> retval = new ArrayList<Object>();
				if (name.equals("PKTABLES"))
				{
					retval.add(0);
					retval.add(0);
					return retval;
				}
				else if (name.equals("PKINDEXES"))
				{
					retval.add(2);
					retval.add(0);
					return retval;
				}
				else if (name.equals("SKINDEXES"))
				{
					retval.add(2);
					retval.add(1);
					return retval;
				}
				else if (name.equals("PKCOLUMNS"))
				{
					retval.add(1);
					retval.add(0);
					return retval;
				}
				else if (name.equals("PKINDEXCOLS"))
				{
					retval.add(3);
					retval.add(0);
					return retval;
				}
				else if (name.equals("PKTABLESTATS"))
				{
					retval.add(5);
					retval.add(0);
					return retval;
				}
				else if (name.equals("PKCOLSTATS"))
				{
					retval.add(7);
					retval.add(0);
					return retval;
				}
				else if (name.equals("PKINDEXSTATS"))
				{
					retval.add(12);
					retval.add(0);
					return retval;
				}
				else if (name.equals("PKNODES"))
				{
					retval.add(6);
					retval.add(0);
					return retval;
				}
				else if (name.equals("SKNODES"))
				{
					retval.add(6);
					retval.add(1);
					return retval;
				}
				else if (name.equals("PKPARTITIONING"))
				{
					retval.add(11);
					retval.add(0);
					return retval;
				}
				else if (name.equals("PKCOLDIST"))
				{
					retval.add(8);
					retval.add(0);
					return retval;
				}
				else if (name.equals("PKNODESTATE"))
				{
					retval.add(10);
					retval.add(0);
					return retval;
				}
				else if (name.equals("PKVIEWS"))
				{
					retval.add(4);
					retval.add(0);
					return retval;
				}
				else if (name.equals("PKBACKUPS"))
				{
					retval.add(9);
					retval.add(0);
					return retval;
				}

				throw new Exception("Index not found");
			}

			int iso = tx.getIsolationLevel();
			tx.setIsolationLevel(Transaction.ISOLATION_RR);
			XAWorker worker = XAManager.executeCatalogQuery(p, tx);
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
				tx.setIsolationLevel(iso);
				throw (Exception)obj;
			}

			if (obj instanceof DataEndMarker)
			{
				cmd = new ArrayList<Object>(1);
				cmd.add("CLOSE");
				worker.in.put(cmd);
				tx.setIsolationLevel(iso);
				throw new Exception("Index not found");
			}

			cmd = new ArrayList<Object>(1);
			cmd.add("CLOSE");
			worker.in.put(cmd);
			tx.setIsolationLevel(iso);
			return (ArrayList<Object>)obj;
		}

		public TableAndIndexIDPlan setParms(String schema, String name) throws Exception
		{
			this.schema = schema;
			this.name = name;
			if (schema.equals("SYS"))
			{
				return this;
			}
			Operator op = p.getTrees().get(0);
			while (!(op instanceof IndexOperator))
			{
				op = op.children().get(0);
			}
			IndexOperator iOp = (IndexOperator)op;
			Index index = iOp.getIndex();
			index.setCondition(new Filter("TABLES.SCHEMA", "E", "'" + schema + "'"));
			while (!(op instanceof SelectOperator))
			{
				if (op instanceof TableScanOperator)
				{
					op = ((TableScanOperator)op).firstParent();
				}
				else
				{
					op = op.parent();
				}
			}
			SelectOperator select = (SelectOperator)op;
			select.getFilter().remove(0);
			select.getFilter().add(new Filter("B.INDEXNAME", "E", "'" + name + "'"));
			return this;
		}
	}

	public static class TableCardPlan
	{
		private final Plan p;

		public TableCardPlan(Plan p)
		{
			this.p = p;
		}

		public long execute(Transaction tx) throws Exception
		{
			int iso = tx.getIsolationLevel();
			tx.setIsolationLevel(Transaction.ISOLATION_UR);
			XAWorker worker = XAManager.executeCatalogQuery(p, tx);
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

			if (obj instanceof DataEndMarker)
			{
				cmd = new ArrayList<Object>(1);
				cmd.add("CLOSE");
				worker.in.put(cmd);
				tx.setIsolationLevel(iso);

				throw new Exception("No table statistics");
			}

			if (obj instanceof Exception)
			{
				cmd = new ArrayList<Object>(1);
				cmd.add("CLOSE");
				worker.in.put(cmd);
				tx.setIsolationLevel(iso);

				throw (Exception)obj;
			}

			Long retval = (Long)((ArrayList<Object>)obj).get(0);

			cmd = new ArrayList<Object>(1);
			cmd.add("CLOSE");
			worker.in.put(cmd);
			tx.setIsolationLevel(iso);
			return retval;
		}

		public TableCardPlan setParms(String schema, String table) throws Exception
		{
			Operator op = p.getTrees().get(0);
			while (!(op instanceof IndexOperator))
			{
				op = op.children().get(0);
			}
			IndexOperator iOp = (IndexOperator)op;
			Index index = iOp.getIndex();
			index.setCondition(new Filter("TABLES.SCHEMA", "E", "'" + schema + "'"));
			index.addSecondaryFilter(new Filter("TABLES.TABNAME", "E", "'" + table + "'"));
			return this;
		}
	}

	public static class TableIDPlan
	{
		private final Plan p;
		private String schema;
		private String name;

		public TableIDPlan(Plan p)
		{
			this.p = p;
		}

		public int execute(Transaction tx) throws Exception
		{
			if (schema.equals("SYS"))
			{
				if (name.equals("TABLES"))
				{
					return 0;
				}
				else if (name.equals("INDEXES"))
				{
					return 2;
				}
				else if (name.equals("COLUMNS"))
				{
					return 1;
				}
				else if (name.equals("INDEXCOLS"))
				{
					return 3;
				}
				else if (name.equals("TABLESTATS"))
				{
					return 5;
				}
				else if (name.equals("COLSTATS"))
				{
					return 7;
				}
				else if (name.equals("INDEXSTATS"))
				{
					return 12;
				}
				else if (name.equals("NODES"))
				{
					return 6;
				}
				else if (name.equals("PARTITIONING"))
				{
					return 11;
				}
				else if (name.equals("COLDIST"))
				{
					return 8;
				}
				else if (name.equals("NODESTATE"))
				{
					return 10;
				}
				else if (name.equals("VIEWS"))
				{
					return 4;
				}
				else if (name.equals("BACKUPS"))
				{
					return 9;
				}

				throw new Exception("Table not found: " + schema + "." + name);
			}

			int iso = tx.getIsolationLevel();
			tx.setIsolationLevel(Transaction.ISOLATION_RR);
			XAWorker worker = XAManager.executeCatalogQuery(p, tx);
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
				tx.setIsolationLevel(iso);
				throw (Exception)obj;
			}

			if (obj instanceof DataEndMarker)
			{
				cmd = new ArrayList<Object>(1);
				cmd.add("CLOSE");
				worker.in.put(cmd);
				tx.setIsolationLevel(iso);
				throw new Exception("Table not found");
			}

			cmd = new ArrayList<Object>(1);
			cmd.add("CLOSE");
			worker.in.put(cmd);
			tx.setIsolationLevel(iso);
			return (Integer)((ArrayList<Object>)obj).get(0);
		}

		public TableIDPlan setParms(String schema, String name) throws Exception
		{
			this.schema = schema;
			this.name = name;
			if (schema.equals("SYS"))
			{
				return this;
			}
			Operator op = p.getTrees().get(0);
			while (op.children().size() != 0)
			{
				op = op.children().get(0);
			}

			Index index = ((IndexOperator)op).getIndex();
			index.setCondition(new Filter("TABLES.SCHEMA", "E", "'" + schema + "'"));
			index.addSecondaryFilter(new Filter("TABLES.TABNAME", "E", "'" + name + "'"));
			return this;
		}
	}

	public static class TableTypePlan
	{
		private final Plan p;
		private String schema;

		public TableTypePlan(Plan p)
		{
			this.p = p;
		}

		public int execute(Transaction tx) throws Exception
		{
			if (schema.equals("SYS"))
			{
				return 0;
			}

			int iso = tx.getIsolationLevel();
			tx.setIsolationLevel(Transaction.ISOLATION_RR);
			XAWorker worker = XAManager.executeCatalogQuery(p, tx);
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
				tx.setIsolationLevel(iso);
				throw (Exception)obj;
			}

			if (obj instanceof DataEndMarker)
			{
				cmd = new ArrayList<Object>(1);
				cmd.add("CLOSE");
				worker.in.put(cmd);
				tx.setIsolationLevel(iso);
				throw new Exception("Table not found");
			}

			cmd = new ArrayList<Object>(1);
			cmd.add("CLOSE");
			worker.in.put(cmd);
			tx.setIsolationLevel(iso);
			String type = (String)((ArrayList<Object>)obj).get(0);
			if (type.equals("C"))
			{
				return 1;
			}
			else
			{
				return 0;
			}
		}

		public TableTypePlan setParms(String schema, String name) throws Exception
		{
			this.schema = schema;
			if (schema.equals("SYS"))
			{
				return this;
			}
			Operator op = p.getTrees().get(0);
			while (op.children().size() != 0)
			{
				op = op.children().get(0);
			}

			Index index = ((IndexOperator)op).getIndex();
			index.setCondition(new Filter("TABLES.SCHEMA", "E", "'" + schema + "'"));
			index.addSecondaryFilter(new Filter("TABLES.TABNAME", "E", "'" + name + "'"));
			return this;
		}
	}

	public static class TypesPlan
	{
		private final Plan p;
		private String schema;
		private String name;

		public TypesPlan(Plan p)
		{
			this.p = p;
		}

		public ArrayList<Object> execute(Transaction tx) throws Exception
		{
			if (schema.equals("SYS"))
			{
				ArrayList<Object> retval = new ArrayList<Object>();
				ArrayList<Object> row = new ArrayList<Object>();
				if (name.equals("PKTABLES"))
				{
					row.add("VARCHAR");
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("VARCHAR");
					retval.add(row);
				}
				else if (name.equals("PKINDEXES"))
				{
					row.add("INT");
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("VARCHAR");
					retval.add(row);
				}
				else if (name.equals("SKINDEXES"))
				{
					row.add("VARCHAR");
					retval.add(row);
				}
				else if (name.equals("PKCOLUMNS"))
				{
					row.add("INT");
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("VARCHAR");
					retval.add(row);
				}
				else if (name.equals("PKINDEXCOLS"))
				{
					row.add("INT");
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("INT");
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("INT");
					retval.add(row);
				}
				else if (name.equals("PKTABLESTATS"))
				{
					row.add("INT");
					retval.add(row);
				}
				else if (name.equals("PKCOLSTATS"))
				{
					row.add("INT");
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("INT");
					retval.add(row);
				}
				else if (name.equals("PKINDEXSTATS"))
				{
					row.add("INT");
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("INT");
					retval.add(row);
				}
				else if (name.equals("PKNODES"))
				{
					row.add("VARCHAR");
					retval.add(row);
				}
				else if (name.equals("SKNODES"))
				{
					row.add("INT");
					retval.add(row);
				}
				else if (name.equals("PKPARTITIONING"))
				{
					row.add("INT");
					retval.add(row);
				}
				else if (name.equals("PKCOLDIST"))
				{
					row.add("INT");
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("INT");
					retval.add(row);
				}
				else if (name.equals("PKNODESTATE"))
				{
					row.add("INT");
					retval.add(row);
				}
				else if (name.equals("PKVIEWS"))
				{
					row.add("VARCHAR");
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("VARCHAR");
					retval.add(row);
				}
				else if (name.equals("PKBACKUPS"))
				{
					row.add("INT");
					retval.add(row);
				}
				else
				{
					throw new Exception("Index not found");
				}

				return retval;
			}

			int iso = tx.getIsolationLevel();
			tx.setIsolationLevel(Transaction.ISOLATION_RR);
			XAWorker worker = XAManager.executeCatalogQuery(p, tx);
			worker.start();
			ArrayList<Object> cmd = new ArrayList<Object>(2);
			cmd.add("NEXT");
			cmd.add(1000000);
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

			ArrayList<Object> retval = new ArrayList<Object>();
			while (!(obj instanceof DataEndMarker))
			{
				if (obj instanceof Exception)
				{
					cmd = new ArrayList<Object>(1);
					cmd.add("CLOSE");
					worker.in.put(cmd);
					tx.setIsolationLevel(iso);
					throw (Exception)obj;
				}

				retval.add(obj);

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
			}

			cmd = new ArrayList<Object>(1);
			cmd.add("CLOSE");
			worker.in.put(cmd);
			tx.setIsolationLevel(iso);
			return retval;
		}

		public TypesPlan setParms(String schema, String name) throws Exception
		{
			this.schema = schema;
			this.name = name;
			if (schema.equals("SYS"))
			{
				return this;
			}
			Operator op = p.getTrees().get(0);
			while (op.children().size() != 0)
			{
				op = op.children().get(0);
			}

			Index index = ((IndexOperator)op).getIndex();
			index.setCondition(new Filter("INDEXES.INDEXNAME", "E", "'" + name + "'"));

			while (!(op instanceof HashJoinOperator))
			{
				if (op instanceof TableScanOperator)
				{
					op = ((TableScanOperator)op).firstParent();
				}
				else
				{
					op = op.parent();
				}
			}

			op = op.children().get(1).children().get(0);
			index = ((IndexOperator)op).getIndex();
			index.setCondition(new Filter("TABLES.SCHEMA", "E", "'" + schema + "'"));
			return this;
		}
	}

	public static class UniqueIndexPlan
	{
		private final Plan p;
		private String schema;
		private String table;

		public UniqueIndexPlan(Plan p)
		{
			this.p = p;
		}

		public ArrayList<Object> execute(Transaction tx) throws Exception
		{
			if (schema.equals("SYS"))
			{
				ArrayList<Object> retval = new ArrayList<Object>();
				ArrayList<Object> row = new ArrayList<Object>();
				if (table.equals("TABLES"))
				{
					row.add("PKTABLES");
					retval.add(row);
				}
				else if (table.equals("INDEXES"))
				{
					row.add("PKINDEXES");
					retval.add(row);
				}
				else if (table.equals("COLUMNS"))
				{
					row.add("PKCOLUMNS");
					retval.add(row);
				}
				else if (table.equals("INDEXCOLS"))
				{
					row.add("PKINDEXCOLS");
					retval.add(row);
				}
				else if (table.equals("TABLESTATS"))
				{
					row.add("PKTABLESTATS");
					retval.add(row);
				}
				else if (table.equals("COLSTATS"))
				{
					row.add("PKCOLSTATS");
					retval.add(row);
				}
				else if (table.equals("INDEXSTATS"))
				{
					row.add("PKINDEXSTATS");
					retval.add(row);
				}
				else if (table.equals("NODES"))
				{
					row.add("PKNODES");
					retval.add(row);
				}
				else if (table.equals("PARTITIONING"))
				{
					row.add("PKPARTITIONING");
					retval.add(row);
				}
				else if (table.equals("COLDIST"))
				{
					row.add("PKCOLDIST");
					retval.add(row);
				}
				else if (table.equals("NODESTATE"))
				{
					row.add("PKNODESTATE");
					retval.add(row);
				}
				else if (table.equals("VIEWS"))
				{
					row.add("PKVIEWS");
					retval.add(row);
				}
				else if (table.equals("BACKUPS"))
				{
					row.add("PKBACKUPS");
					retval.add(row);
				}
				else
				{
					throw new Exception("Table not found");
				}

				return retval;
			}

			int iso = tx.getIsolationLevel();
			tx.setIsolationLevel(Transaction.ISOLATION_RR);
			XAWorker worker = XAManager.executeCatalogQuery(p, tx);
			worker.start();
			ArrayList<Object> cmd = new ArrayList<Object>(2);
			cmd.add("NEXT");
			cmd.add(1000000);
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

			ArrayList<Object> retval = new ArrayList<Object>();
			while (!(obj instanceof DataEndMarker))
			{
				if (obj instanceof Exception)
				{
					cmd = new ArrayList<Object>(1);
					cmd.add("CLOSE");
					worker.in.put(cmd);
					tx.setIsolationLevel(iso);
					throw (Exception)obj;
				}

				retval.add(obj);

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
			}

			cmd = new ArrayList<Object>(1);
			cmd.add("CLOSE");
			worker.in.put(cmd);
			tx.setIsolationLevel(iso);
			return retval;
		}

		public UniqueIndexPlan setParms(String schema, String table) throws Exception
		{
			this.schema = schema;
			this.table = table;
			if (schema.equals("SYS"))
			{
				return this;
			}
			RootOperator root = (RootOperator)p.getTrees().get(0);
			IndexOperator iOp = (IndexOperator)root.children().get(0).children().get(0).children().get(0).children().get(0).children().get(0);
			Index index = iOp.getIndex();
			index.setCondition(new Filter("TABLES.SCHEMA", "E", "'" + schema + "'"));
			index.addSecondaryFilter(new Filter("TABLES.TABNAME", "E", "'" + table + "'"));
			return this;
		}
	}

	public static class UniquePlan
	{
		private final Plan p;
		private String schema;
		private String table;

		public UniquePlan(Plan p)
		{
			this.p = p;
		}

		public ArrayList<Object> execute(Transaction tx) throws Exception
		{
			if (schema.equals("SYS"))
			{
				ArrayList<Object> retval = new ArrayList<Object>();
				ArrayList<Object> row = new ArrayList<Object>();
				if (table.equals("TABLES"))
				{
					row.add("PKTABLES");
					row.add(true);
					retval.add(row);
				}
				else if (table.equals("INDEXES"))
				{
					row.add("PKINDEXES");
					row.add(true);
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("SKINDEXES");
					row.add(false);
					retval.add(row);
				}
				else if (table.equals("COLUMNS"))
				{
					row.add("PKCOLUMNS");
					row.add(true);
					retval.add(row);
				}
				else if (table.equals("INDEXCOLS"))
				{
					row.add("PKINDEXCOLS");
					row.add(true);
					retval.add(row);
				}
				else if (table.equals("TABLESTATS"))
				{
					row.add("PKTABLESTATS");
					row.add(true);
					retval.add(row);
				}
				else if (table.equals("COLSTATS"))
				{
					row.add("PKCOLSTATS");
					row.add(true);
					retval.add(row);
				}
				else if (table.equals("INDEXSTATS"))
				{
					row.add("PKINDEXSTATS");
					row.add(true);
					retval.add(row);
				}
				else if (table.equals("NODES"))
				{
					row.add("PKNODES");
					row.add(true);
					retval.add(row);
					row = new ArrayList<Object>();
					row.add("SKNODES");
					row.add(true);
					retval.add(row);
				}
				else if (table.equals("PARTITIONING"))
				{
					row.add("PKPARTITIONING");
					row.add(true);
					retval.add(row);
				}
				else if (table.equals("COLDIST"))
				{
					row.add("PKCOLDIST");
					row.add(true);
					retval.add(row);
				}
				else if (table.equals("NODESTATE"))
				{
					row.add("PKNODESTATE");
					row.add(true);
					retval.add(row);
				}
				else if (table.equals("VIEWS"))
				{
					row.add("PKVIEWS");
					row.add(true);
					retval.add(row);
				}
				else if (table.equals("BACKUPS"))
				{
					row.add("PKBACKUPS");
					row.add(true);
					retval.add(row);
				}
				else
				{
					throw new Exception("Table not found");
				}

				return retval;
			}

			int iso = tx.getIsolationLevel();
			tx.setIsolationLevel(Transaction.ISOLATION_RR);
			XAWorker worker = XAManager.executeCatalogQuery(p, tx);
			worker.start();
			ArrayList<Object> cmd = new ArrayList<Object>(2);
			cmd.add("NEXT");
			cmd.add(1000000);
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

			ArrayList<Object> retval = new ArrayList<Object>();
			while (!(obj instanceof DataEndMarker))
			{
				if (obj instanceof Exception)
				{
					cmd = new ArrayList<Object>(1);
					cmd.add("CLOSE");
					worker.in.put(cmd);
					tx.setIsolationLevel(iso);
					throw (Exception)obj;
				}

				retval.add(obj);

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
			}

			cmd = new ArrayList<Object>(1);
			cmd.add("CLOSE");
			worker.in.put(cmd);
			tx.setIsolationLevel(iso);
			return retval;
		}

		public UniquePlan setParms(String schema, String table) throws Exception
		{
			this.schema = schema;
			this.table = table;
			if (schema.equals("SYS"))
			{
				return this;
			}
			RootOperator root = (RootOperator)p.getTrees().get(0);
			IndexOperator iOp = (IndexOperator)root.children().get(0).children().get(0).children().get(0).children().get(0);
			Index index = iOp.getIndex();
			index.setCondition(new Filter("TABLES.SCHEMA", "E", "'" + schema + "'"));
			index.addSecondaryFilter(new Filter("TABLES.TABNAME", "E", "'" + table + "'"));
			return this;
		}
	}

	public static class VerifyIndexPlan
	{
		private final Plan p;
		private String schema;
		private String name;

		public VerifyIndexPlan(Plan p)
		{
			this.p = p;
		}

		public Object execute(Transaction tx) throws Exception
		{
			if (schema.equals("SYS"))
			{
				ArrayList<Object> retval = new ArrayList<Object>();
				if (name.equals("PKTABLES"))
				{
					return retval;
				}
				else if (name.equals("PKINDEXES"))
				{
					return retval;
				}
				else if (name.equals("SKINDEXES"))
				{
					return retval;
				}
				else if (name.equals("PKCOLUMNS"))
				{
					return retval;
				}
				else if (name.equals("PKINDEXCOLS"))
				{
					return retval;
				}
				else if (name.equals("PKTABLESTATS"))
				{
					return retval;
				}
				else if (name.equals("PKCOLSTATS"))
				{
					return retval;
				}
				else if (name.equals("PKINDEXSTATS"))
				{
					return retval;
				}
				else if (name.equals("PKNODES"))
				{
					return retval;
				}
				else if (name.equals("SKNODES"))
				{
					return retval;
				}
				else if (name.equals("PKPARTITIONING"))
				{
					return retval;
				}
				else if (name.equals("PKCOLDIST"))
				{
					return retval;
				}
				else if (name.equals("PKNODESTATE"))
				{
					return retval;
				}
				else if (name.equals("PKVIEWS"))
				{
					return retval;
				}
				else if (name.equals("PKBACKUPS"))
				{
					return retval;
				}

				return new DataEndMarker();
			}

			int iso = tx.getIsolationLevel();
			tx.setIsolationLevel(Transaction.ISOLATION_RR);
			XAWorker worker = XAManager.executeCatalogQuery(p, tx);
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
				tx.setIsolationLevel(iso);
				throw (Exception)obj;
			}

			cmd = new ArrayList<Object>(1);
			cmd.add("CLOSE");
			worker.in.put(cmd);
			tx.setIsolationLevel(iso);
			return obj;
		}

		public VerifyIndexPlan setParms(String schema, String name) throws Exception
		{
			this.schema = schema;
			this.name = name;
			if (schema.equals("SYS"))
			{
				return this;
			}
			Operator op = p.getTrees().get(0);
			while (!(op instanceof IndexOperator))
			{
				op = op.children().get(0);
			}
			IndexOperator iOp = (IndexOperator)op;
			Index index = iOp.getIndex();
			index.setCondition(new Filter("TABLES.SCHEMA", "E", "'" + schema + "'"));
			while (!(op instanceof SelectOperator))
			{
				if (op instanceof TableScanOperator)
				{
					op = ((TableScanOperator)op).firstParent();
				}
				else
				{
					op = op.parent();
				}
			}
			SelectOperator select = (SelectOperator)op;
			select.getFilter().remove(0);
			select.getFilter().add(new Filter("B.INDEXNAME", "E", "'" + name + "'"));
			return this;
		}
	}

	public static class VerifyTableExistPlan
	{
		private final Plan p;
		private String schema;
		private String name;

		public VerifyTableExistPlan(Plan p)
		{
			this.p = p;
		}

		public Object execute(Transaction tx) throws Exception
		{
			if (schema.equals("SYS"))
			{
				ArrayList<Object> retval = new ArrayList<Object>();
				if (name.equals("TABLES"))
				{
					return retval;
				}
				else if (name.equals("INDEXES"))
				{
					return retval;
				}
				else if (name.equals("COLUMNS"))
				{
					return retval;
				}
				else if (name.equals("INDEXCOLS"))
				{
					return retval;
				}
				else if (name.equals("TABLESTATS"))
				{
					return retval;
				}
				else if (name.equals("COLSTATS"))
				{
					return retval;
				}
				else if (name.equals("INDEXSTATS"))
				{
					return retval;
				}
				else if (name.equals("NODES"))
				{
					return retval;
				}
				else if (name.equals("PARTITIONING"))
				{
					return retval;
				}
				else if (name.equals("COLDIST"))
				{
					return retval;
				}
				else if (name.equals("NODESTATE"))
				{
					return retval;
				}
				else if (name.equals("VIEWS"))
				{
					return retval;
				}
				else if (name.equals("BACKUPS"))
				{
					return retval;
				}

				return new DataEndMarker();
			}

			int iso = tx.getIsolationLevel();
			tx.setIsolationLevel(Transaction.ISOLATION_RR);
			XAWorker worker = XAManager.executeCatalogQuery(p, tx);
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
				tx.setIsolationLevel(iso);
				throw (Exception)obj;
			}

			cmd = new ArrayList<Object>(1);
			cmd.add("CLOSE");
			worker.in.put(cmd);
			tx.setIsolationLevel(iso);
			return obj;
		}

		public VerifyTableExistPlan setParms(String schema, String name) throws Exception
		{
			this.schema = schema;
			this.name = name;
			if (schema.equals("SYS"))
			{
				return this;
			}
			Operator op = p.getTrees().get(0);
			while (op.children().size() != 0)
			{
				op = op.children().get(0);
			}

			Index index = ((IndexOperator)op).getIndex();
			index.setCondition(new Filter("TABLES.SCHEMA", "E", "'" + schema + "'"));
			index.addSecondaryFilter(new Filter("TABLES.TABNAME", "E", "'" + name + "'"));
			return this;
		}
	}

	public static class VerifyViewExistPlan
	{
		private final Plan p;

		public VerifyViewExistPlan(Plan p)
		{
			this.p = p;
		}

		public Object execute(Transaction tx) throws Exception
		{
			int iso = tx.getIsolationLevel();
			tx.setIsolationLevel(Transaction.ISOLATION_RR);
			XAWorker worker = XAManager.executeCatalogQuery(p, tx);
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
				tx.setIsolationLevel(iso);
				throw (Exception)obj;
			}

			cmd = new ArrayList<Object>(1);
			cmd.add("CLOSE");
			worker.in.put(cmd);
			tx.setIsolationLevel(iso);
			return obj;
		}

		public VerifyViewExistPlan setParms(String schema, String name) throws Exception
		{
			Operator op = p.getTrees().get(0);
			while (op.children().size() != 0)
			{
				op = op.children().get(0);
			}

			Index index = ((IndexOperator)op).getIndex();
			index.setCondition(new Filter("VIEWS.SCHEMA", "E", "'" + schema + "'"));
			index.addSecondaryFilter(new Filter("VIEWS.NAME", "E", "'" + name + "'"));
			return this;
		}
	}

	public static class ViewSQLPlan
	{
		private final Plan p;

		public ViewSQLPlan(Plan p)
		{
			this.p = p;
		}

		public String execute(Transaction tx) throws Exception
		{
			int iso = tx.getIsolationLevel();
			tx.setIsolationLevel(Transaction.ISOLATION_RR);
			XAWorker worker = XAManager.executeCatalogQuery(p, tx);
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
				tx.setIsolationLevel(iso);
				throw (Exception)obj;
			}

			if (obj instanceof DataEndMarker)
			{
				cmd = new ArrayList<Object>(1);
				cmd.add("CLOSE");
				worker.in.put(cmd);
				tx.setIsolationLevel(iso);
				throw new Exception("View not found");
			}

			cmd = new ArrayList<Object>(1);
			cmd.add("CLOSE");
			worker.in.put(cmd);
			tx.setIsolationLevel(iso);
			return (String)((ArrayList<Object>)obj).get(0);
		}

		public ViewSQLPlan setParms(String schema, String name) throws Exception
		{
			Operator op = p.getTrees().get(0);
			while (op.children().size() != 0)
			{
				op = op.children().get(0);
			}

			Index index = ((IndexOperator)op).getIndex();
			index.setCondition(new Filter("VIEWS.SCHEMA", "E", "'" + schema + "'"));
			index.addSecondaryFilter(new Filter("VIEWS.NAME", "E", "'" + name + "'"));
			return this;
		}
	}

	public static class WorkerNodesPlan
	{
		private final Plan p;

		public WorkerNodesPlan(Plan p)
		{
			this.p = p;
		}

		public ArrayList<Object> execute(Transaction tx) throws Exception
		{
			int iso = tx.getIsolationLevel();
			tx.setIsolationLevel(Transaction.ISOLATION_RR);
			XAWorker worker = XAManager.executeCatalogQuery(p, tx);
			worker.start();
			ArrayList<Object> cmd = new ArrayList<Object>(2);
			cmd.add("NEXT");
			cmd.add(1000000000);
			worker.in.put(cmd);

			ArrayList<Object> retval = new ArrayList<Object>();
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
			while (!(obj instanceof DataEndMarker))
			{
				if (obj instanceof Exception)
				{
					cmd = new ArrayList<Object>(1);
					cmd.add("CLOSE");
					worker.in.put(cmd);
					tx.setIsolationLevel(iso);
					throw (Exception)obj;
				}

				retval.add(obj);

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
			}

			cmd = new ArrayList<Object>(1);
			cmd.add("CLOSE");
			worker.in.put(cmd);
			tx.setIsolationLevel(iso);
			return retval;
		}

		public WorkerNodesPlan setParms()
		{
			return this;
		}
	}
}
