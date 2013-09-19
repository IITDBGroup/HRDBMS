package com.exascale.managers;

import java.util.HashMap;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.TreeNode;

import com.exascale.tables.DataType;
import com.exascale.tables.Plan;
import com.exascale.tables.SQL;

public class PlanCacheManager 
{
	private static ConcurrentHashMap<SQL, Plan> planCache = new ConcurrentHashMap<SQL, Plan>();
	//Plans have creation timestamp and reserved flag

	public PlanCacheManager()
	{
		String catalogDir = HRDBMSWorker.getHParms().getProperty("catalog_dir");
		if (!catalogDir.endsWith("/"))
		{
			catalogDir += "/";
		}
		
		String sql = "SELECT COUNT(*) FROM SYS.TABLES WHERE SCHEMA = ? AND TABNAME = ?";
		DefaultMutableTreeNode root = new DefaultMutableTreeNode(new CountOperator());
		root.add(new DefaultMutableTreeNode(new IndexScanOperator(catalogDir + "SYS.TABLES.PKTABLES.index", "@1", "@2"));
		Vector<TreeNode> treeList = new Vector<TreeNode>();
		treeList.add(root);
		DataType[] args = new DataType[2];
		args[0] = new DataType(DataType.VARCHAR, 128, 0);
		args[1] = new DataType(DataType.VARCHAR, 128, 0);
		planCache.put(new SQL(sql), new Plan(true, treeList, args));
		
		sql = "SELECT HOSTNAME, RACK, STATE FROM SYS.NODES A, SYS.NODESTATE B WHERE A.NODEID = ? AND A.NODEID = B.NODE";
		root = new DefaultMutableTreeNode(new ProductOperator())
		
		DefaultMutableTreeNode lc1 = new DefaultMutableTreeNode(new ProjectOperator(1, 3));
		DefaultMutableTreeNode lc2 = new DefaultMutableTreeNode(new TableRIDListOperator(catalogDir + "SYS.NODES.tbl"));
		DefaultMutableTreeNode lc3 = new DefaultMutableTreeNode(new IndexScanOperator(catalogDir + "SYS.NODES.PKNODES.index", "@1"));
		
		DefaultMutableTreeNode rc1 = new DefaultMutableTreeNode(new ProjectOperator(1));
		DefaultMutableTreeNode rc2 = new DefaultMutableTreeNode(new TableRIDListOperator(catalogDir + "SYS.NODESTATE.tbl"));
		DefaultMutableTreeNode rc3 = new DefaultMutableTreeNode(new IndexScanOperator(catalogDir + "SYS.NODESTATE.PKNODESTATE.index", "@1"));
		
		root.add(lc1);
		lc1.add(lc2);
		lc2.add(lc3);
		root.add(rc1);
		rc1.add(rc2);
		rc2.add(rc3);
		
		treeList = new Vector<TreeNode>();
		treeList.add(root);
		args = new DataType[1];
		args[0] = new DataType(DataType.INTEGER, 0, 0);
		planCache.put(new SQL(sql),  new Plan(true, treeList, args));
		
		sql = "SELECT SECOND, THIRD FROM SYS.BACKUPS WHERE FIRST = ?";
		root = new DefaultMutableTreeNode(new ProjectOperator(2, 3));
		DefaultMutableTreeNode c1 = new DefaultMutableTreeNode(new TableRIDListOperator(catalogDir + "SYS.BACKUPS.tbl"));
		DefaultMutableTreeNode c2 = new DefaultMutableTreeNode(new IndexScanOperator(catalogDir + "SYS.BACKUPS.PKBACKUPS.index", "@1"));
		root.add(c1);
		c1.add(c2);
		
		treeList = new Vector<TreeNode>();
		treeList.add(root);
		args = new DataType[1];
		args[0] = new DataType(DataType.INTEGER, 0, 0);
		planCache.put(new SQL(sql), new Plan(true, treeList, args));
		
		sql = "SELECT HOSTNAME FROM SYS.NODES WHERE NODEID = ?";
		root = new DefaultMutableTreeNode(new ProjectOperator(1));
		c1 = new DefaultMutableTreeNode(new TableRIDListOperator(catalogDir + "SYS.NODES.tbl"));
		c2 = new DefaultMutableTreeNode(new IndexScanOperator(catalogDir + "SYS.NODES.PKNODES.index", "@1"));
		root.add(c1);
		c1.add(c2);
		
		treeList = new Vector<TreeNode>();
		treeList.add(root);
		args = new DataType[1];
		args[0] = new DataType(DataType.INTEGER, 0, 0);
		planCache.put(new SQL(sql), new Plan(true, treeList, args));
		
		sql = "SELECT STATE FROM SYS.NODESTATE WHERE NODE = ?";
		root = new DefaultMutableTreeNode(new ProjectOperator(1));
		c1 = new DefaultMutableTreeNode(new TableRIDListOperator(catalogDir + "SYS.NODESTATE.tbl"));
		c2 = new DefaultMutableTreeNode(new IndexScanOperator(catalogDir + "SYS.NODESTATE.PKNODESTATE.index", "@1"));
		root.add(c1);
		c1.add(c2);
		
		treeList = new Vector<TreeNode>();
		treeList.add(root);
		args = new DataType[1];
		args[0] = new DataType(DataType.INTEGER, 0, 0);
		planCache.put(new SQL(sql), new Plan(true, treeList, args));
		
		sql = "SELECT TABLEID FROM SYS.TABLES WHERE SCHEMA = ? AND TABNAME = ?";
		root = new DefaultMutableTreeNode(new ProjectOperator(0));
		c1 = new DefaultMutableTreeNode(new TableRIDListOperator(catalogDir + "SYS.TABLES.tbl"));
		c2 = new DefaultMutableTreeNode(new IndexScanOperator(catalogDir + "SYS.TABLES.PKTABLES.index", "@1", "@2"));
		
		root.add(c1);
		c1.add(c2);
		
		treeList = new Vector<TreeNode>();
		treeList.add(root);
		args = new DataType[2];
		args[0] = new DataType(DataType.VARCHAR, 128, 0);
		args[1] = new DataType(DataType.VARCHAR, 128, 0);
		planCache.put(new SQL(sql), new Plan(true, treeList, args));
		
		sql = "SELECT COUNT(*) FROM SYS.COLGROUPS WHERE TABLEID = ? AND COLGROUPNAME = ?";
		root = new DefaultMutableTreeNode(CountOperator());
		root.add(new DefaultMutableTreeNode(new IndexScanOperator(catalogDir + "SYS.COLGROUPS.PKCOLGROUPS.index", "@1", "@2")));
		
		treeList = new Vector<TreeNode>();
		treeList.add(root);
		args = new DataType[2];
		args[0] = new DataType(DataType.INTEGER, 0, 0);
		args[1] = new DataType(DataType.VARCHAR, 128, 0);
		planCache.put(new SQL(sql), new Plan(true, treeList, args));
		
		sql = "SELECT COUNT(DISTINCT COLGROUPNAME) FROM SYS.COLGROUPS WHERE TABLEID = ?";
		root = new DefaultMutableTreeNode(new CountOperator());
		DefaultMutableTreeNode c0 = new DefaultMutableTreeNode(new RemoveDupsOperator());
		c1 = new DefaultMutableTreeNode(new ProjectOperator(3));
		c2 = new DefaultMutableTreeNode(new TableRIDListOperator(catalogDir + "SYS.COLGROUPS.tbl"));
		DefaultMutableTreeNode c3 = new DefaultMutableTreeNode(new IndexScanOperator(catalogDir + "SYS.COLGROUPS.PKCOLGROUPS.index", "@1"));
		
		root.add(c0);
		c0.add(c1);
		c1.add(c2);
		c2.add(c3);
		treeList = new Vector<TreeNode>();
		treeList.add(root);
		args = new DataType[1];
		args[0] = new DataType(DataType.INTEGER, 0, 0);
		planCache.put(new SQL(sql), new Plan(true, treeList, args));
		
		sql = "SELECT DISTINCT COLGROUPID FROM SYS.COLGROUPS WHERE TABLEID = ? AND COLGROUPNAME = ?";
		root = new DefaultMutableTreeNode(new RemoveDupsOperator());
		c1 = new DefaultMutableTreeNode(new ProjectOperator(0));
		c2 = new DefaultMutableTreeNode(new TableRIDListOperator(catalogDir + "SYS.COLGROUPS.tbl"));
		c3 = new DefaultMutableTreeNode(new IndexScanOperator(catalogDir + "SYS.COLGROUPS.PKCOLGROUPS.index", "@1", "@2"));
		root.add(c1);
		c1.add(c2);
		c2.add(c3);
		
		treeList = new Vector<TreeNode>();
		treeList.add(root);
		args = new DataType[2];
		args[0] = new DataType(DataType.INTEGER, 0, 0);
		args[1] = new DataType(DataType.VARCHAR, 128, 0);
		planCache.put(new SQL(sql), new Plan(true, treeList, args));
		
		sql = "SELECT COUNT(*) FROM SYS.NODEGROUPS WHERE NAME = ?";
		root = new DefaultMutableTreeNode(new CountOperator());
		root.add(new DefaultMutableTreeNode(new IndexScanOperator(catalogDir + "SYS.NODEGROUPS.PKNODEGROUPS.index", "@1")));
		treeList = new Vector<TreeNode>();
		treeList.add(root);
		args = new DataType[1];
		args[0] = new DataType(DataType.VARCHAR, 128, 0);
		planCache.put(new SQL(sql), new Plan(true, treeList, args));
		
		sql = "SELECT COUNT(*) FROM SYS.VIEWS WHERE SCHEMA = ? AND NAME = ?";
		root = new DefaultMutableTreeNode(new CountOperator());
		root.add(new DefaultMutableTreeNode(new IndexScanOperator(catalogDir + "SYS.VIEWS.PKVIEWS.index", "@1", "@2")));
		treeList = new Vector<TreeNode>();
		treeList.add(root);
		args = new DataType[2];
		args[0] = new DataType(DataType.VARCHAR, 128, 0);
		args[1] = new DataType(DataType.VARCHAR, 128, 0);
		planCache.put(new SQL(sql), new Plan(true, treeList, args));
		
		sql = "SELECT COUNT(*) FROM SYS.INDEXES WHERE INDEXNAME = ? AND TABLEID = ?";
		root = new DefaultMutableTreeNode(new CountOperator());
		root.add(new DefaultMutableTreeNode(new IndexScanOperator(catalogDir + "SYS.INDEXES.PKINDEXES.index", "@2", "@1")));
		treeList = new Vector<TreeNode>();
		treeList.add(root);
		args = new DataType[2];
		args[0] = new DataType(DataType.VARCHAR, 128, 0);
		args[1] = new DataType(DataType.INTEGER, 0, 0);
		planCache.put(new SQL(sql), new Plan(true, treeList, args));
		
		sql = "SELECT COUNT(*) FROM SYS.INDEXES WHERE TABLEID = ?";
		root = new DefaultMutableTreeNode(new CountOperator());
		root.add(new DefaultMutableTreeNode(new IndexScanOperator(catalogDir + "SYS.INDEXES.PKINDEXES.index", "@1")));
		treeList = new Vector<TreeNode>();
		treeList.add(root);
		args = new DataType[1];
		args[0] = new DataType(DataType.INTEGER, 0, 0);
		planCache.put(new SQL(sql), new Plan(true, treeList, args));
		
		sql = "SELECT INDEXID FROM SYS.INDEXES WHERE TABLEID = ? AND INDEXNAME = ?";
		root = new DefaultMutableTreeNode(new ProjectOperator(0));
		c1 = new DefaultMutableTreeNode(new TreeRIDListOperator(catalogDir + "SYS.INDEXES.tbl"));
		c2 = new DefaultMutableTreeNode(new IndexScanOperator(catalogDir + "SYS.INDEXES.PKINDEXES.index", "@1", "@2"));
		root.add(c1);
		c1.add(c2);
		treeList = new Vector<TreeNode>();
		treeList.add(root);
		args = new DataType[2];
		args[0] = new DataType(DataType.INTEGER, 0, 0);
		args[1] = new DataType(DataType.VARCHAR, 128, 0);
		planCache.put(new SQL(sql), new Plan(true, treeList, args));
		
		sql = "SELECT * FROM SYS.TABLES WHERE SCHEMA = ? AND TABNAME = ?";
		root = new DefaultMutableTreeNode(new TableRIDListOperator(catalogDir + "SYS.TABLES.tbl"));
		root.add(new DefaultMutableTreeNode(new IndexScanOperator(catalogDir + "SYS.TABLES.PKTABLES.index", "@1", "@2")));
		treeList = new Vector<TreeNode>();
		treeList.add(root);
		args = new DataType[2];
		args[0] = new DataType(DataType.VARCHAR, 128, 0);
		args[1] = new DataType(DataType.VARCHAR, 128, 0);
		planCache.put(new SQL(sql), new Plan(true, treeList, args));
		
		sql = "SELECT * FROM SYS.COLUMNS WHERE TABLEID = ?";
		root = new DefaultMutableTreeNode(new TableRIDListOperator(catalogDir + "SYS.COLUMNS.tbl"));
		root.add(new DefaultMutableTreeNode(new IndexScanOperator(catalogDir + "SYS.COLUMNS.PKCOLUMNS.index", "@1")));
		treeList = new Vector<TreeNode>();
		treeList.add(root);
		args = new DataType[1];
		args[0] = new DataType(DataType.INTEGER, 0, 0);
		planCache.put(new SQL(sql), new Plan(true, treeList, args));
		
		sql = "SELECT * FROM SYS.INDEXES WHERE TABLEID = ?";
		root = new DefaultMutableTreeNode(new TableRIDListOperator(catalogDir + "SYS.INDEXES.tbl"));
		root.add(new DefaultMutableTreeNode(new IndexScanOperator(catalogDir + "SYS.INDEXES.PKINDEXES.index", "@1")));
		treeList = new Vector<TreeNode>();
		treeList.add(root);
		args = new DataType[1];
		args[0] = new DataType(DataType.INTEGER, 0, 0);
		planCache.put(new SQL(sql), new Plan(true, treeList, args));
		
		sql = "SELECT * FROM SYS.INDEXCOLS WHERE TABLEID = ?";
		root = new DefaultMutableTreeNode(new TableRIDListOperator(catalogDir + "SYS.INDEXCOLS.tbl"));
		root.add(new DefaultMutableTreeNode(new IndexScanOperator(catalogDir + "SYS.INDEXCOLS.PKINDEXCOLS.index", "@1")));
		treeList = new Vector<TreeNode>();
		treeList.add(root);
		args = new DataType[1];
		args[0] = new DataType(DataType.INTEGER, 0, 0);
		planCache.put(new SQL(sql), new Plan(true, treeList, args));
		
		sql = "SELECT * FROM SYS.COLGROUPS WHERE TABLEID = ?";
		root = new DefaultMutableTreeNode(new TableRIDListOperator(catalogDir + "SYS.COLGROUPS.tbl"));
		root.add(new DefaultMutableTreeNode(new IndexScanOperator(catalogDir + "SYS.COLGROUPS.PKCOLGROUPS.index", "@1")));
		treeList = new Vector<TreeNode>();
		treeList.add(root);
		args = new DataType[1];
		args[0] = new DataType(DataType.INTEGER, 0, 0);
		planCache.put(new SQL(sql), new Plan(true, treeList, args));
		
		sql = "SELECT * FROM SYS.TABLESTATS WHERE TABLEID = ?";
		root = new DefaultMutableTreeNode(new TableRIDListOperator(catalogDir + "SYS.TABLESTATS.tbl"));
		root.add(new DefaultMutableTreeNode(new IndexScanOperator(catalogDir + "SYS.TABLESTATS.PKTABLESTATS.index", "@1")));
		treeList = new Vector<TreeNode>();
		treeList.add(root);
		args = new DataType[1];
		args[0] = new DataType(DataType.INTEGER, 0, 0);
		planCache.put(new SQL(sql), new Plan(true, treeList, args));
		
		sql = "SELECT * FROM SYS.COLGROUPSTATS WHERE TABLEID = ?";
		root = new DefaultMutableTreeNode(new TableRIDListOperator(catalogDir + "SYS.COLGROUPSTATS.tbl"));
		root.add(new DefaultMutableTreeNode(new IndexScanOperator(catalogDir + "SYS.COLGROUPSTATS.PKCOLGROUPSTATS.index", "@1")));
		treeList = new Vector<TreeNode>();
		treeList.add(root);
		args = new DataType[1];
		args[0] = new DataType(DataType.INTEGER, 0, 0);
		planCache.put(new SQL(sql), new Plan(true, treeList, args));
		
		sql = "SELECT * FROM SYS.INDEXSTATS WHERE TABLEID = ?";
		root = new DefaultMutableTreeNode(new TableRIDListOperator(catalogDir + "SYS.INDEXSTATS.tbl"));
		root.add(new DefaultMutableTreeNode(new IndexScanOperator(catalogDir + "SYS.INDEXSTATS.PKINDEXSTATS.index", "@1")));
		treeList = new Vector<TreeNode>();
		treeList.add(root);
		args = new DataType[1];
		args[0] = new DataType(DataType.INTEGER, 0, 0);
		planCache.put(new SQL(sql), new Plan(true, treeList, args));
		
		sql = "SELECT * FROM SYS.PARTITIONING WHERE TABLEID = ?";
		root = new DefaultMutableTreeNode(new TableRIDListOperator(catalogDir + "SYS.PARTITIONING.tbl"));
		root.add(new DefaultMutableTreeNode(new IndexScanOperator(catalogDir + "SYS.PARTITIONING.PKPARTITONING.index", "@1")));
		treeList = new Vector<TreeNode>();
		treeList.add(root);
		args = new DataType[1];
		args[0] = new DataType(DataType.INTEGER, 0, 0);
		planCache.put(new SQL(sql), new Plan(true, treeList, args));
		
		sql = "SELECT * FROM SYS.COLSTATS WHERE TABLEID = ?";
		root = new DefaultMutableTreeNode(new TableRIDListOperator(catalogDir + "SYS.COLSTATS.tbl"));
		root.add(new DefaultMutableTreeNode(new IndexScanOperator(catalogDir + "SYS.COLSTATS.PKCOLSTATS.index", "@1")));
		treeList = new Vector<TreeNode>();
		treeList.add(root);
		args = new DataType[1];
		args[0] = new DataType(DataType.INTEGER, 0, 0);
		planCache.put(new SQL(sql), new Plan(true, treeList, args));
		
		sql = "SELECT * FROM SYS.COLDIST WHERE TABLEID = ?";
		root = new DefaultMutableTreeNode(new TableRIDListOperator(catalogDir + "SYS.COLDIST.tbl"));
		root.add(new DefaultMutableTreeNode(new IndexScanOperator(catalogDir + "SYS.COLDIST.PKCOLDIST.index", "@1")));
		treeList = new Vector<TreeNode>();
		treeList.add(root);
		args = new DataType[1];
		args[0] = new DataType(DataType.INTEGER, 0, 0);
		planCache.put(new SQL(sql), new Plan(true, treeList, args));
		
		sql = "INSERT INTO SYS.TABLES(SCHEMA, TABNAME, NUMCOLS, NUMKEYCOLS, TYPE) VALUES (?, ?, ?, ?, ?)";
		//get tableid + set next tableid
		root = new DefaultMutableTreeNode(new AssignToVariableOperator("@6"));
		c1 = new DefaultMutableTreeNode(new GetAndUpdateIdentityOperator(0, "TABLEID"))
		root.add(c1);
		treeList = new Vector<TreeNode>();
		treeList.add(root);
		
		//do insert
		root = new DefaultMutableTreeNode(new InsertOperator(catalogDir + "SYS.TABLES.tbl", "@6", "@1", "@2", "@3", "@4", "@5"));
		treeList.add(root);
		
		//inesrt row into all indexes
		root = new DefaultMutableTreeNode(new InsertAllIndexesOperator("SYS", "TABLES", "@6", "@1", "@2", "@3", "@4", "@5"));
		treeList.add(root);
		
		//update row count for all indexes in indexstats
		root = new DefaultMutableTreeNode(new UpdateAllIndexRowCountsOperator("SYS", "TABLES", 1));
		treeList.add(root);
		
		args = new DataType[5];
		args[0] = new DataType(DataType.VARCHAR, 128, 0);
		args[1] = new DataType(DataType.VARCHAR, 128, 0);
		args[2] = new DataType(DataType.INTEGER, 0, 0);
		args[3] = new DataType(DataType.INTEGER, 0, 0);
		args[4] = new DataType(DataType.VARCHAR, 1, 0);
		planCache.put(new SQL(sql), new Plan(true, treeList, args));
		
		sql = "INSERT INTO SYS.COLUMNS(COLID, TABLEID, COLNAME, COLTYPE, LENGTH, SCALE, IDENTITY, NEXTVAL, INCREMENT, DEFAULT, NULLABLE, PKPOSITION, CLUSTERPOS, GENERATED) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
		//do insert
		root = new DefaultMutableTreeNode(new InsertOperator(catalogDir + "SYS.COLUMNS.tbl", "@1", "@2", "@3", "@4", "@5", "@6", "@7", "@8", "@9", "@10", "@11", "@12", "@13", "@14"));
		treeList = new Vector<TreeNode>();
		treeList.add(root);
		
		//inesrt row into all indexes
		root = new DefaultMutableTreeNode(new InsertAllIndexesOperator("SYS", "COLUMNS", "@1", "@2", "@3", "@4", "@5", "@6", "@7", "@8", "@9", "@10", "@11", "@12", "@13", "@14"));
		treeList.add(root);
		
		//update row count for all indexes in indexstats
		root = new DefaultMutableTreeNode(new UpdateAllIndexRowCountsOperator("SYS", "COLUMNS", 1));
		treeList.add(root);
		
		args = new DataType[14];
		args[0] = new DataType(DataType.INTEGER, 0, 0);
		args[1] = new DataType(DataType.INTEGER, 0, 0);
		args[2] = new DataType(DataType.VARCHAR, 128, 0);
		args[3] = new DataType(DataType.VARCHAR, 16, 0);
		args[4] = new DataType(DataType.INTEGER, 0, 0);
		args[5] = new DataType(DataType.INTEGER, 0, 0);
		args[6] = new DataType(DataType.VARCHAR, 1, 0);
		args[7] = new DataType(DataType.BIGINT, 0, 0);
		args[8] = new DataType(DataType.INTEGER, 0, 0);
		args[9] = new DataType(DataType.VARBINARY, 32768, 0);
		args[10] = new DataType(DataType.VARCHAR, 1, 0);
		args[11] = new DataType(DataType.INTEGER, 0, 0);
		args[12] = new DataType(DataType.INTEGER, 0, 0);
		args[13] = new DataType(DataType.VARCHAR, 1, 0);
		planCache.put(new SQL(sql), new Plan(true, treeList, args));
		
		sql = "INSERT INTO SYS.TABLESTATS(TABLEID, NODEID, DEVID, CARD, PAGES, AVGROWLEN) VALUES (?, ?, ?, ?, ?, ?)";
		//do insert
		root = new DefaultMutableTreeNode(new InsertOperator(catalogDir + "SYS.TABLESTATS.tbl", "@1", "@2", "@3", "@4", "@5", "@6"));
		treeList = new Vector<TreeNode>();
		treeList.add(root);
		
		//inesrt row into all indexes
		root = new DefaultMutableTreeNode(new InsertAllIndexesOperator("SYS", "TABLESTATS", "@1", "@2", "@3", "@4", "@5", "@6"));
		treeList.add(root);
		
		//update row count for all indexes in indexstats
		root = new DefaultMutableTreeNode(new UpdateAllIndexRowCountsOperator("SYS", "TABLESTATS", 1));
		treeList.add(root);
		
		args = new DataType[6];
		args[0] = new DataType(DataType.INTEGER, 0, 0);
		args[1] = new DataType(DataType.INTEGER, 0, 0);
		args[2] = new DataType(DataType.INTEGER, 0, 0);
		args[3] = new DataType(DataType.BIGINT, 0, 0);
		args[4] = new DataType(DataType.INTEGER, 0, 0);
		args[5] = new DataType(DataType.INTEGER, 0, 0);
		planCache.put(new SQL(sql), new Plan(true, treeList, args));
		
		sql = "INSERT INTO SYS.COLSTATS(TABLEID, NODEID, DEVID, COLID, CARD, AVGCOLLEN, NUMNULLS) VALUES (?, ?, ?, ?, ?, ?, ?)";
		//do insert
		root = new DefaultMutableTreeNode(new InsertOperator(catalogDir + "SYS.COLSTATS.tbl", "@1", "@2", "@3", "@4", "@5", "@6", "@7"));
		treeList = new Vector<TreeNode>();
		treeList.add(root);
		
		//inesrt row into all indexes
		root = new DefaultMutableTreeNode(new InsertAllIndexesOperator("SYS", "TABLESTATS", "@1", "@2", "@3", "@4", "@5", "@6", "@7"));
		treeList.add(root);
		
		//update row count for all indexes in indexstats
		root = new DefaultMutableTreeNode(new UpdateAllIndexRowCountsOperator("SYS", "COLSTATS", 1));
		treeList.add(root);
		
		args = new DataType[7];
		args[0] = new DataType(DataType.INTEGER, 0, 0);
		args[1] = new DataType(DataType.INTEGER, 0, 0);
		args[2] = new DataType(DataType.INTEGER, 0, 0);
		args[3] = new DataType(DataType.INTEGER, 0, 0);
		args[4] = new DataType(DataType.BIGINT, 0, 0);
		args[5] = new DataType(DataType.INTEGER, 0, 0);
		args[6] = new DataType(DataType.BIGINT, 0, 0);
		planCache.put(new SQL(sql), new Plan(true, treeList, args));
		
		sql = "INSERT INTO SYS.PARTITIONING(TABLEID, LEVEL, GROUPEXP, NODEEXP, DEVEXP) VALUES (?, ?, ?, ?, ?)";
		//do insert
		root = new DefaultMutableTreeNode(new InsertOperator(catalogDir + "SYS.PARTITIONING.tbl", "@1", "@2", "@3", "@4", "@5"));
		treeList = new Vector<TreeNode>();
		treeList.add(root);
		
		//inesrt row into all indexes
		root = new DefaultMutableTreeNode(new InsertAllIndexesOperator("SYS", "PARTITIONING", "@1", "@2", "@3", "@4", "@5"));
		treeList.add(root);
		
		//update row count for all indexes in indexstats
		root = new DefaultMutableTreeNode(new UpdateAllIndexRowCountsOperator("SYS", "PARTITIONING", 1));
		treeList.add(root);
		
		args = new DataType[5];
		args[0] = new DataType(DataType.INTEGER, 0, 0);
		args[1] = new DataType(DataType.INTEGER, 0, 0);
		args[2] = new DataType(DataType.VARCHAR, 32768, 0);
		args[3] = new DataType(DataType.VARCHAR, 32768, 0);
		args[4] = new DataType(DataType.VARCHAR, 32768, 0);
		planCache.put(new SQL(sql), new Plan(true, treeList, args));
		
		sql = "INSERT INTO SYS.COLGROUPS(COLGROUPID, TABLEID, COLID, COLGROUPNAME) VALUES (?, ?, ?, ?)";
		//do insert
		root = new DefaultMutableTreeNode(new InsertOperator(catalogDir + "SYS.COLGROUPS.tbl", "@1", "@2", "@3", "@4"));
		treeList = new Vector<TreeNode>();
		treeList.add(root);
		
		//inesrt row into all indexes
		root = new DefaultMutableTreeNode(new InsertAllIndexesOperator("SYS", "COLGROUPS", "@1", "@2", "@3", "@4"));
		treeList.add(root);
		
		//update row count for all indexes in indexstats
		root = new DefaultMutableTreeNode(new UpdateAllIndexRowCountsOperator("SYS", "COLGROUPS", 1));
		treeList.add(root);
		
		args = new DataType[4];
		args[0] = new DataType(DataType.INTEGER, 0, 0);
		args[1] = new DataType(DataType.INTEGER, 0, 0);
		args[2] = new DataType(DataType.INTEGER, 0, 0);
		args[3] = new DataType(DataType.VARCHAR, 128, 0);
		planCache.put(new SQL(sql), new Plan(true, treeList, args));
		
		sql = "INSERT INTO SYS.NODEGROUPS(NAME, NODEID) VALUES (?, ?)";
		//get nodegroupid + set next nodegroupid
		root = new DefaultMutableTreeNode(new AssignToVariableOperator("@3"));
		c1 = new DefaultMutableTreeNode(new GetAndUpdateIdentityOperator(6, "NODEGROUPID"))
		root.add(c1);
		treeList = new Vector<TreeNode>();
		treeList.add(root);
		
		//do insert
		root = new DefaultMutableTreeNode(new InsertOperator(catalogDir + "SYS.NODEGROUPS.tbl", "@3", "@1", "@2"));
		treeList.add(root);
		
		//inesrt row into all indexes
		root = new DefaultMutableTreeNode(new InsertAllIndexesOperator("SYS", "NODEGROUPS", "@3", "@1", "@2"));
		treeList.add(root);
		
		//update row count for all indexes in indexstats
		root = new DefaultMutableTreeNode(new UpdateAllIndexRowCountsOperator("SYS", "NODEGROUPS", 1));
		treeList.add(root);
		
		args = new DataType[2];
		args[0] = new DataType(DataType.VARCHAR, 128, 0);
		args[1] = new DataType(DataType.INTEGER, 0, 0);
		planCache.put(new SQL(sql), new Plan(true, treeList, args));
		
		sql = "INSERT INTO SYS.VIEWS(SCHEMA, NAME, TEXT) VALUES (?, ?, ?)";
		//get viewid + set next viewid
		root = new DefaultMutableTreeNode(new AssignToVariableOperator("@4"));
		c1 = new DefaultMutableTreeNode(new GetAndUpdateIdentityOperator(4, "VIEWID"))
		root.add(c1);
		treeList = new Vector<TreeNode>();
		treeList.add(root);
		
		//do insert
		root = new DefaultMutableTreeNode(new InsertOperator(catalogDir + "SYS.VIEWS.tbl", "@4", "@1", "@2", "@3"));
		treeList.add(root);
		
		//inesrt row into all indexes
		root = new DefaultMutableTreeNode(new InsertAllIndexesOperator("SYS", "VIEWS", "@4", "@1", "@2", "@3"));
		treeList.add(root);
		
		//update row count for all indexes in indexstats
		root = new DefaultMutableTreeNode(new UpdateAllIndexRowCountsOperator("SYS", "VIEWS", 1));
		treeList.add(root);
				
		args = new DataType[3];
		args[0] = new DataType(DataType.VARCHAR, 128, 0);
		args[1] = new DataType(DataType.VARCHAR, 128, 0);
		args[2] = new DataType(DataType.VARCHAR, 32768, 0);
		planCache.put(new SQL(sql), new Plan(true, treeList, args));
		
		sql = "INSERT INTO SYS.INDEXES(INDEXID, INDEXNAME, TABLEID, UNIQUE, NUMCOLS, TYPE) VALUES (?, ?, ?, ?, ?, ?)";
		//do insert
		root = new DefaultMutableTreeNode(new InsertOperator(catalogDir + "SYS.INDEXES.tbl", "@1", "@2", "@3", "@4", "@5", "@6"));
		treeList = new Vector<TreeNode>();
		treeList.add(root);
		
		//inesrt row into all indexes
		root = new DefaultMutableTreeNode(new InsertAllIndexesOperator("SYS", "INDEXES", "@1", "@2", "@3", "@4", "@5", "@6"));
		treeList.add(root);
		
		//update row count for all indexes in indexstats
		root = new DefaultMutableTreeNode(new UpdateAllIndexRowCountsOperator("SYS", "INDEXES", 1));
		treeList.add(root);
		
		args = new DataType[6];
		args[0] = new DataType(DataType.INTEGER, 0, 0);
		args[1] = new DataType(DataType.VARCHAR, 128, 0);
		args[2] = new DataType(DataType.INTEGER, 0, 0);
		args[3] = new DataType(DataType.VARCHAR, 1, 0);
		args[4] = new DataType(DataType.INTEGER, 0, 0);
		args[5] = new DataType(DataType.VARCHAR, 1, 0);
		planCache.put(new SQL(sql), new Plan(true, treeList, args));
		
		sql = "INSERT INTO SYS.INDEXCOLS(INDEXID, TABLEID, COLID, POSITION, ORDER) VALUES (?, ?, ?, ?, ?)";
		//do insert
		root = new DefaultMutableTreeNode(new InsertOperator(catalogDir + "SYS.INDEXCOLS.tbl", "@1", "@2", "@3", "@4", "@5"));
		treeList = new Vector<TreeNode>();
		treeList.add(root);
		
		//inesrt row into all indexes
		root = new DefaultMutableTreeNode(new InsertAllIndexesOperator("SYS", "INDEXCOLS", "@1", "@2", "@3", "@4", "@5"));
		treeList.add(root);
		
		//update row count for all indexes in indexstats
		root = new DefaultMutableTreeNode(new UpdateAllIndexRowCountsOperator("SYS", "INDEXCOLS", 1));
		treeList.add(root);
		
		args = new DataType[5];
		args[0] = new DataType(DataType.INTEGER, 0, 0);
		args[1] = new DataType(DataType.INTEGER, 0, 0);
		args[2] = new DataType(DataType.INTEGER, 0, 0);
		args[3] = new DataType(DataType.INTEGER, 0, 0);
		args[4] = new DataType(DataType.INTEGER, 0, 0);
		planCache.put(new SQL(sql), new Plan(true, treeList, args));
		
		sql = "INSERT INTO SYS.INDEXSTATS(TABLEID, INDEXID, NLEAFS, NLEVELS, 1STKEYCARD, 2NDKEYCARD, 3RDKEYCARD, 4THKEYCARD, FULLKEYCARD, NROWS, NBLOCKS, NODEID, DEVID) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
		//do insert
		root = new DefaultMutableTreeNode(new InsertOperator(catalogDir + "SYS.INDEXSTATS.tbl", "@1", "@2", "@3", "@4", "@5", "@6", "@7", "@8", "@9", "@10", "@11", "@12", "@13"));
		treeList = new Vector<TreeNode>();
		treeList.add(root);
		
		//inesrt row into all indexes
		root = new DefaultMutableTreeNode(new InsertAllIndexesOperator("SYS", "INDEXSTATS", "@1", "@2", "@3", "@4", "@5", "@6", "@7", "@8", "@9", "@10", "@11", "@12", "@13"));
		treeList.add(root);
		
		//update row count for all indexes in indexstats
		root = new DefaultMutableTreeNode(new UpdateAllIndexRowCountsOperator("SYS", "INDEXSTATS", 1));
		treeList.add(root);
		
		args = new DataType[13];
		args[0] = new DataType(DataType.INTEGER, 0, 0);
		args[1] = new DataType(DataType.INTEGER, 0, 0);
		args[2] = new DataType(DataType.INTEGER, 0, 0);
		args[3] = new DataType(DataType.INTEGER, 0, 0);
		args[4] = new DataType(DataType.BIGINT, 0, 0);
		args[5] = new DataType(DataType.BIGINT, 0, 0);
		args[6] = new DataType(DataType.BIGINT, 0, 0);
		args[7] = new DataType(DataType.BIGINT, 0, 0);
		args[8] = new DataType(DataType.BIGINT, 0, 0);
		args[9] = new DataType(DataType.BIGINT, 0, 0);
		args[10] = new DataType(DataType.INTEGER, 0, 0);
		args[11] = new DataType(DataType.INTEGER, 0, 0);
		args[12] = new DataType(DataType.INTEGER, 0, 0);
		planCache.put(new SQL(sql), new Plan(true, treeList, args));
		
		sql = "UPDATE SYS.NODESTATE SET STATE = 'I' WHERE NODE = ?";
		int [] colIDS = new int[1];
		colIDs[0] = 1;
		Object[] vals = new Object[1];
		vals[0] = "I";
		root = new DefaultMutableTreeNode(new UpdateViaIndexDataRIDChangesAndKeysOperator(catalogDir + "SYS.NODESTATE.PKNODESTATE.index", colIDs, vals, "@1"));
		treeList = new Vector<TreeNode>();
		treeList.add(root);
		
		args = new DataType[1];
		args[0] = new DataType(DataType.INTEGER, 0, 0);
		planCache.put(new SQL(sql), new Plan(true, treeList, args));
		
		sql = "DELETE FROM SYS.TABLES WHERE TABLEID = ?";
		root = new DefaultMutableTreeNode(new DeleteViaIndexUpdateAllIndexesAndCountsOperator(catalogDir + "SYS.TABLES.PKTABLES.index", "@1"));
		treeList = new Vector<TreeNode>();
		treeList.add(node);
		args = new DataType[1];
		args[0] = new DataType(DataType.INTEGER, 0, 0);
		planCache.put(new SQL(sql), new Plan(true, treeList, args));
		
		sql = "DELETE FROM SYS.COLUMNS WHERE TABLEID = ?";
		root = new DefaultMutableTreeNode(new DeleteViaIndexUpdateAllIndexesAndCountsOperator(catalogDir + "SYS.COLUMNS.PKCOLUMNS.index", "@1"));
		treeList = new Vector<TreeNode>();
		treeList.add(node);
		args = new DataType[1];
		args[0] = new DataType(DataType.INTEGER, 0, 0);
		planCache.put(new SQL(sql), new Plan(true, treeList, args));
		
		sql = "DELETE FROM SYS.COLGROUPS WHERE TABLEID = ?";
		root = new DefaultMutableTreeNode(new DeleteViaIndexUpdateAllIndexesAndCountsOperator(catalogDir + "SYS.COLGROUPS.PKCOLGROUPS.index", "@1"));
		treeList = new Vector<TreeNode>();
		treeList.add(node);
		args = new DataType[1];
		args[0] = new DataType(DataType.INTEGER, 0, 0);
		planCache.put(new SQL(sql), new Plan(true, treeList, args));
		
		sql = "DELETE FROM SYS.TABLESTATS WHERE TABLEID = ?";
		root = new DefaultMutableTreeNode(new DeleteViaIndexUpdateAllIndexesAndCountsOperator(catalogDir + "SYS.TABLESTATS.PKTABLESTATS.index", "@1"));
		treeList = new Vector<TreeNode>();
		treeList.add(node);
		args = new DataType[1];
		args[0] = new DataType(DataType.INTEGER, 0, 0);
		planCache.put(new SQL(sql), new Plan(true, treeList, args));
		
		sql = "DELETE FROM SYS.COLGROUPSTATS WHERE TABLEID = ?";
		root = new DefaultMutableTreeNode(new DeleteViaIndexUpdateAllIndexesAndCountsOperator(catalogDir + "SYS.COLGROUPSTATS.PKCOLGROUPSTATS.index", "@1"));
		treeList = new Vector<TreeNode>();
		treeList.add(node);
		args = new DataType[1];
		args[0] = new DataType(DataType.INTEGER, 0, 0);
		planCache.put(new SQL(sql), new Plan(true, treeList, args));
		
		sql = "DELETE FROM SYS.PARTITIONING WHERE TABLEID = ?";
		root = new DefaultMutableTreeNode(new DeleteViaIndexUpdateAllIndexesAndCountsOperator(catalogDir + "SYS.PARTITIONING.PKPARTITIONING.index", "@1"));
		treeList = new Vector<TreeNode>();
		treeList.add(node);
		args = new DataType[1];
		args[0] = new DataType(DataType.INTEGER, 0, 0);
		planCache.put(new SQL(sql), new Plan(true, treeList, args));
		
		sql = "DELETE FROM SYS.COLSTATS WHERE TABLEID = ?";
		root = new DefaultMutableTreeNode(new DeleteViaIndexUpdateAllIndexesAndCountsOperator(catalogDir + "SYS.COLSTATS.PKCOLSTATS.index", "@1"));
		treeList = new Vector<TreeNode>();
		treeList.add(node);
		args = new DataType[1];
		args[0] = new DataType(DataType.INTEGER, 0, 0);
		planCache.put(new SQL(sql), new Plan(true, treeList, args));
		
		sql = "DELETE FROM SYS.COLDIST WHERE TABLEID = ?";
		root = new DefaultMutableTreeNode(new DeleteViaIndexUpdateAllIndexesAndCountsOperator(catalogDir + "SYS.COLDIST.PKCOLDIST.index", "@1"));
		treeList = new Vector<TreeNode>();
		treeList.add(node);
		args = new DataType[1];
		args[0] = new DataType(DataType.INTEGER, 0, 0);
		planCache.put(new SQL(sql), new Plan(true, treeList, args));
		
		sql = "DELETE FROM SYS.COLGROUPS WHERE TABLEID = ? AND COLGROUPNAME = ?";
		root = new DefaultMutableTreeNode(new DeleteViaIndexUpdateAllIndexesAndCountsOperator(catalogDir + "SYS.COLGROUPS.PKCOLGROUPS.index", "@1", "@2"));
		treeList = new Vector<TreeNode>();
		treeList.add(node);
		args = new DataType[2];
		args[0] = new DataType(DataType.INTEGER, 0, 0);
		args[1] = new DataType(DataType.VARCHAR, 128, 0);
		planCache.put(new SQL(sql), new Plan(true, treeList, args));
		
		sql = "DELETE FROM SYS.COLGROUPSTATS WHERE TABLEID = ? AND COLGROUPID = ?";
		root = new DefaultMutableTreeNode(new DeleteViaIndexUpdateAllIndexesAndCountsOperator(catalogDir + "SYS.COLGROUPSTATS.PKCOLGROUPSTATS.index", "@1", "@2"));
		treeList = new Vector<TreeNode>();
		treeList.add(node);
		args = new DataType[2];
		args[0] = new DataType(DataType.INTEGER, 0, 0);
		args[1] = new DataType(DataType.INTEGER, 0, 0);
		planCache.put(new SQL(sql), new Plan(true, treeList, args));
		
		sql = "DELETE FROM SYS.NODEGROUPS WHERE NAME = ?";
		root = new DefaultMutableTreeNode(new DeleteViaIndexUpdateAllIndexesAndCountsOperator(catalogDir + "SYS.NODEGROUPS.PKNODEGROUPS.index", "@1"));
		treeList = new Vector<TreeNode>();
		treeList.add(node);
		args = new DataType[1];
		args[0] = new DataType(DataType.VARCHAR, 128, 0);
		planCache.put(new SQL(sql), new Plan(true, treeList, args));
		
		sql = "DELETE FROM SYS.VIEWS WHERE SCHEMA = ? AND NAME = ?";
		root = new DefaultMutableTreeNode(new DeleteViaIndexUpdateAllIndexesAndCountsOperator(catalogDir + "SYS.VIEWS.PKVIEWS.index", "@1", "@2"));
		treeList = new Vector<TreeNode>();
		treeList.add(node);
		args = new DataType[2];
		args[0] = new DataType(DataType.VARCHAR, 128, 0);
		args[1] = new DataType(DataType.VARCHAR, 128, 0);
		planCache.put(new SQL(sql), new Plan(true, treeList, args));
		
		sql = "DELETE FROM SYS.INDEXES WHERE TABLEID = ? AND INDEXNAME = ?";
		root = new DefaultMutableTreeNode(new DeleteViaIndexUpdateAllIndexesAndCountsOperator(catalogDir + "SYS.INDEXES.PKINDEXES.index", "@1", "@2"));
		treeList = new Vector<TreeNode>();
		treeList.add(node);
		args = new DataType[2];
		args[0] = new DataType(DataType.INTEGER, 0, 0);
		args[1] = new DataType(DataType.VARCHAR, 128, 0);
		planCache.put(new SQL(sql), new Plan(true, treeList, args));
		
		sql = "DELETE FROM SYS.INDEXCOLS WHERE TABLEID = ? AND INDEXID = ?";
		root = new DefaultMutableTreeNode(new DeleteViaIndexUpdateAllIndexesAndCountsOperator(catalogDir + "SYS.INDEXCOLSTATS.PKINDEXCOLSTATS.index", "@1", "@2"));
		treeList = new Vector<TreeNode>();
		treeList.add(node);
		args = new DataType[2];
		args[0] = new DataType(DataType.INTEGER, 0, 0);
		args[1] = new DataType(DataType.INTEGER, 0, 0);
		planCache.put(new SQL(sql), new Plan(true, treeList, args));
		
		sql = "DELETE FROM SYS.INDEXSTATS WHERE TABLEID = ? AND INDEXID = ?";
		root = new DefaultMutableTreeNode(new DeleteViaIndexUpdateAllIndexesAndCountsOperator(catalogDir + "SYS.INDEXSTATS.PKINDEXSTATS.index", "@1", "@2"));
		treeList = new Vector<TreeNode>();
		treeList.add(node);
		args = new DataType[2];
		args[0] = new DataType(DataType.INTEGER, 0, 0);
		args[1] = new DataType(DataType.INTEGER, 0, 0);
		planCache.put(new SQL(sql), new Plan(true, treeList, args));
	}
	
	public static Plan checkPlanCache(String sql)
	{
		Plan plan = planCache.get(new SQL(sql));
		return new Plan(plan);
	}
	
	public static void addPlan(String sql, Plan p)
	{
		planCache.put(new SQL(sql), p);
	}
	
	public static void reduce()
	{
		double avg = 0;
		long num = -1;
		for (Plan p : planCache.values())
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
		
			avg =  avg / (newNum * 1.0 / num) + p.getTimeStamp() / newNum - avg;
			num = newNum;
		}
		
		for (Map.Entry<SQL, Plan> entry : planCache.entrySet())
		{
			Plan p = entry.getValue();
			if (!p.isReserved())
			{
				if (p.getTimeStamp() < (long)avg)
				{
					planCache.remove(entry.getKey());
				}
			}
		}
	}
}
