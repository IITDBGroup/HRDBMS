package com.exascale.optimizer.testing;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

import com.exascale.threads.ReadThread;

public class BuildIndexes 
{
	protected static final int BRANCH_FACTOR = 128;
	protected static final Runtime rt = Runtime.getRuntime();
	
	public static void main(String[] args) throws Exception
	{
		new ResourceManager().start();
		IndexThread it = null;
		it = new IndexThread("./xp_type.indx", "./part.tbl", "PART", "P_TYPE", "P_SIZE");
		it = new IndexThread("./xp_size.indx", "./part.tbl", "PART", "P_SIZE");
		it = new IndexThread("./xc_mktsegment.indx", "./customer.tbl", "CUSTOMER", "C_MKTSEGMENT");
		it = new IndexThread("./xo_orderdate.indx", "./orders.tbl", "ORDERS", "O_ORDERDATE");
		it = new IndexThread("./xl_shipdate.indx", "./lineitem.tbl", "LINEITEM", "L_SHIPDATE", "L_EXTENDEDPRICE", "L_QUANTITY", "L_DISCOUNT", "L_SUPPKEY");
		it = new IndexThread("./xp_name.indx", "./part.tbl", "PART", "P_NAME");
		it = new IndexThread("./xp_container.indx", "./part.tbl", "PART", "P_CONTAINER", "P_BRAND", "P_SIZE");
		it = new IndexThread("./xl_receiptdate.indx", "./lineitem.tbl", "LINEITEM", "L_RECEIPTDATE", "L_SHIPMODE", "L_SHIPINSTRUCT");
		it = new IndexThread("./xs_comment.indx", "./supplier.tbl", "SUPPLIER", "S_COMMENT");
		it = new IndexThread("./xps_partkey.indx", "./partsupp.tbl", "PARTSUPP", "PS_PARTKEY", "PS_SUPPKEY");
		it = new IndexThread("./xl_orderkey.indx", "./lineitem.tbl", "LINEITEM", "L_ORDERKEY", "L_SUPPKEY");
		it = new IndexThread("./xl_partkey.indx", "./lineitem.tbl", "LINEITEM", "L_PARTKEY");
		it = new IndexThread("./xo_custkey.indx", "./orders.tbl", "ORDERS", "O_CUSTKEY");
		it = new IndexThread("./xs_suppkey.indx", "./supplier.tbl", "SUPPLIER", "S_SUPPKEY");
		System.exit(0);
	}
	
	private static class IndexThread extends ThreadPoolThread
	{
		protected String indexFile;
		protected String tableFile;
		protected String tableName;
		protected String[] columns;
		protected int offload = 0;
		
		public IndexThread(String indexFile, String tableFile, String tableName, String... columns)
		{
			this.indexFile = indexFile;
			this.tableFile = tableFile;
			this.tableName = tableName;
			this.columns = columns;
		}
		
		public void run()
		{
			System.out.println("Starting build of " + indexFile);
			try
			{
				BufferedRandomAccessFile out = new BufferedRandomAccessFile(indexFile, "rw");
				MetaData meta = new MetaData();
				RandomAccessFile file = new RandomAccessFile(tableFile, "r");
				HashMap<String, Integer> cols2Pos = meta.getCols2PosForTable("TPCH",  tableName);
				HashMap<String, String> cols2Types = meta.getCols2TypesForTable("TPCH", tableName);
				ArrayList<String> keys = new ArrayList<String>(columns.length);
				ArrayList<Boolean> orders = new ArrayList<Boolean>(columns.length);
				ArrayList<String> types = new ArrayList<String>(columns.length);
				for (String column : columns)
				{
					keys.add(column);
					orders.add(true);
				}
				for (String k : keys)
				{
					types.add(cols2Types.get(k));
				}
				ConcurrentSkipListMap<String[], ArrayListLong> unique2RIDS = new ConcurrentSkipListMap<String[], ArrayListLong>(new RowComparator(orders, types));
				ArrayList<ReadThread> threads = new ArrayList<ReadThread>(Runtime.getRuntime().availableProcessors());
				int i = 0;
				while (i < Runtime.getRuntime().availableProcessors())
				{
					ReadThread rt = new ReadThread(file, keys, cols2Pos, unique2RIDS, indexFile, tableName);
					rt.start();
					threads.add(rt);
					i++;
				}
				
				i = 0;
				while (i < Runtime.getRuntime().availableProcessors())
				{
					threads.get(i).join();
					i++;
				}	
		
				StringBuilder outLine = new StringBuilder();
				outLine.append("                     \n");
				out.write(outLine.toString().getBytes("UTF-8"));
				ConcurrentSkipListMap<String[], ArrayListLong> newUnique2RIDS = new ConcurrentSkipListMap<String[], ArrayListLong>(new RowComparator(orders, types));
		
				while (true)
				{
					i = 0;
					int size = unique2RIDS.size();
					for (Map.Entry entry : unique2RIDS.entrySet())
					{
						if (i % BRANCH_FACTOR == 0)
						{
							ArrayListLong internalRIDs = new ArrayListLong(1);
							internalRIDs.add(-1 * out.getFilePointer());
							newUnique2RIDS.put((String[])entry.getKey(), internalRIDs);
						}
						outLine = new StringBuilder();
						for (String s : (String[])entry.getKey())
						{
							outLine.append(s + "|");
						}
						for (Object l : ((ArrayListLong)entry.getValue()))
						{
							outLine.append(l + "|");
						}
						outLine.append("\n");
						out.write(outLine.toString().getBytes("UTF-8"));
						i++;
						
						if (i % 1000000 == 0)
						{
							System.out.println("Wrote " + i + "/" + size + " entries to index " + indexFile);
						}
					}
		
					out.write("\u0000\n".getBytes("UTF-8"));
					if (unique2RIDS.size() == 1)
					{
						out.seek(0);
						for (ArrayListLong rids : newUnique2RIDS.values())
						{
							out.write((rids.get(0) + "\n").getBytes("UTF-8"));
						}
				
						break;
					}
			
					unique2RIDS = newUnique2RIDS;
					newUnique2RIDS = new ConcurrentSkipListMap<String[], ArrayListLong>(new RowComparator(orders, types));
				}
		
				file.close();
				out.close();
				unique2RIDS = null;
				newUnique2RIDS = null;
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(1);
			}
		}
	}
	
	private static class ReadThread extends ThreadPoolThread
	{
		protected static ConcurrentHashMap<String, AtomicLong> readCounts = new ConcurrentHashMap<String, AtomicLong>();
		protected static MetaData meta = new MetaData();
		protected RandomAccessFile file;
		protected ArrayList<String> keys;
		protected HashMap<String, Integer> cols2Pos;
		protected ConcurrentSkipListMap<String[], ArrayListLong> unique2RIDS;
		protected String indexFile;
		protected String table;
		protected long tableCount;
		
		public ReadThread(RandomAccessFile file, ArrayList<String> keys, HashMap<String, Integer> cols2Pos, ConcurrentSkipListMap<String[], ArrayListLong> unique2RIDS, String indexFile, String table)
		{
			this.file = file;
			this.keys = keys;
			this.cols2Pos = cols2Pos;
			this.unique2RIDS = unique2RIDS;
			this.indexFile = indexFile;
			this.table = table;
			readCounts.putIfAbsent(indexFile, new AtomicLong(0));
			tableCount = meta.getTableCard("TPCH", table);
		}
		
		public void run()
		{
			try
			{
				Long RID = new Long(0);
				String line = null;
			
				while (true)
				{
					synchronized(file)
					{
						RID = ResourceManager.internLong(file.getFilePointer()); 
						line = file.readLine();
					}
					
					if (line == null)
					{
						break;
					}
					
					String[] key = new String[keys.size()];
					FastStringTokenizer tokens = new FastStringTokenizer(line, "|", false);
					String[] all = tokens.allTokens();
					int i = 0;
					for (String k : keys)
					{
						key[i] = ResourceManager.internString(all[cols2Pos.get(k)]);
						i++;
					}
		
					ArrayListLong rid = new ArrayListLong(1);
					rid.add(RID);
					ArrayListLong prev = unique2RIDS.putIfAbsent(key, rid);
					if (prev == null)
					{
						long count = readCounts.get(indexFile).getAndIncrement();
						if (count % 1000000 == 0)
						{
							System.out.println("Read " + count + "/" + tableCount + " rows for building " + indexFile);
						}
						continue;
					}
						
					rid = unique2RIDS.get(key);
					synchronized(rid)
					{
						rid.add(RID);
					}
					long count = readCounts.get(indexFile).getAndIncrement();
					if (count % 1000000 == 0)
					{
						System.out.println("Read " + count + "/" + tableCount + " rows for building " + indexFile);
					}
				}
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(1);
			}
		}
	}	
}
