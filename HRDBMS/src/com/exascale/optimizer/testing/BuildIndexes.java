package com.exascale.optimizer.testing;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.exascale.threads.ReadThread;

public class BuildIndexes 
{
	protected static final int BRANCH_FACTOR = 128;
	protected static final Runtime rt = Runtime.getRuntime();
	protected static int NUM_THREADS = 1;
	protected static int SEGMENT_SIZE = 5000000;
	
	public static void main(String[] args) throws Exception
	{
		new ResourceManager().start();
		IndexThread it = null;
		IndexThread mark = null;
		
		it = new IndexThread("./xp_type.indx", "./part.tbl", "PART", "P_TYPE", "P_SIZE");
		it.start();
		it = new IndexThread("./xp_size.indx", "./part.tbl", "PART", "P_SIZE");
		it.start();
		it = new IndexThread("./xc_mktsegment.indx", "./customer.tbl", "CUSTOMER", "C_MKTSEGMENT");
		it.start();
		it = new IndexThread("./xp_name.indx", "./part.tbl", "PART", "P_NAME");
		it.start();
		it = new IndexThread("./xp_container.indx", "./part.tbl", "PART", "P_BRAND", "P_CONTAINER", "P_SIZE");
		it.start();
		it = new IndexThread("./xs_comment.indx", "./supplier.tbl", "SUPPLIER", "S_COMMENT");
		it.start();
		mark = new IndexThread("./xps_partkey.indx", "./partsupp.tbl", "PARTSUPP", "PS_PARTKEY", "PS_SUPPKEY");
		mark.start();
		it.join();
		mark.join();
		
		
		NUM_THREADS = 4;
		mark = new IndexThread("./xo_orderdate.indx", "./orders.tbl", "ORDERS", "O_ORDERDATE");
		mark.start();
		it = new IndexThread("./xo_custkey.indx", "./orders.tbl", "ORDERS", "O_CUSTKEY");
		it.start();
		mark.join();
		it.join();
		
		NUM_THREADS = 8;
		it = new IndexThread("./xl_receiptdate.indx", "./lineitem.tbl", "LINEITEM", "L_SHIPMODE", "L_RECEIPTDATE");
		it.start();
		it.join();
		it = new IndexThread("./xl_shipmode.indx", "./lineitem.tbl", "LINEITEM", "L_SHIPINSTRUCT", "L_SHIPMODE", "L_QUANTITY");
		it.start();
		it.join();
		it = new IndexThread("./xl_orderkey.indx", "./lineitem.tbl", "LINEITEM", "L_ORDERKEY", "L_SUPPKEY");
		it.start();
		it.join();
		it = new IndexThread("./xl_partkey.indx", "./lineitem.tbl", "LINEITEM", "L_PARTKEY");
		it.start();
		it.join();
		it = new IndexThread("./xl_shipdate.indx", "./lineitem.tbl", "LINEITEM", "L_SHIPDATE", "L_EXTENDEDPRICE", "L_QUANTITY", "L_DISCOUNT", "L_SUPPKEY");
		it.start();
		it.join();
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
			Thread.currentThread().setPriority(Thread.MAX_PRIORITY);
			System.out.println("Starting build of " + indexFile);
			try
			{
				BufferedRandomAccessFile out = new BufferedRandomAccessFile(indexFile, "rw");
				MetaData meta = new MetaData();
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
				//int avgLen = Integer.highestOneBit((meta.getTableCard("TPCH", tableName) > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int)meta.getTableCard("TPCH", tableName)) / meta.getNumDevices());
				DBConcurrentSkipTreeMap<String[], ArrayListLong> unique2RIDS = new DBConcurrentSkipTreeMap<String[], ArrayListLong>(new RowComparator(orders, types));
				ArrayList<ReadThread> threads = new ArrayList<ReadThread>(Runtime.getRuntime().availableProcessors());
				int i = 0;
				while (i < NUM_THREADS)
				{
					ReadThread rt = new ReadThread(keys, cols2Pos, unique2RIDS, indexFile, tableName, i, tableFile);
					rt.start();
					threads.add(rt);
					i++;
				}
				
				i = 0;
				while (i < NUM_THREADS)
				{
					threads.get(i).join();
					i++;
				}	
		
				StringBuilder outLine = new StringBuilder();
				outLine.append("                     \n");
				out.write(outLine.toString().getBytes("UTF-8"));
				DBConcurrentSkipTreeMap<String[], ArrayListLong> newUnique2RIDS = new DBConcurrentSkipTreeMap<String[], ArrayListLong>(new RowComparator(orders, types));
		
				while (true)
				{
					i = 0;
					long size = unique2RIDS.longSize();
					for (Object e : unique2RIDS)
					{
						Map.Entry entry = (Map.Entry)e;
						if (i % BRANCH_FACTOR == 0)
						{
							ArrayListLong internalRIDs = new ArrayListLong(1);
							internalRIDs.add(-1 * out.getFilePointer());
							newUnique2RIDS.putIfAbsent((String[])entry.getKey(), internalRIDs);
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
					if (unique2RIDS.longSize() == 1)
					{
						out.seek(0);
						for (ArrayListLong rids : newUnique2RIDS.values())
						{
							out.write((rids.get(0) + "\n").getBytes("UTF-8"));
						}
				
						break;
					}
			
					unique2RIDS = newUnique2RIDS;
					newUnique2RIDS = new DBConcurrentSkipTreeMap<String[], ArrayListLong>(new RowComparator(orders, types));
				}
		
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
		protected BufferedRandomAccessFile file;
		protected ArrayList<String> keys;
		protected HashMap<String, Integer> cols2Pos;
		protected DBConcurrentSkipTreeMap<String[], ArrayListLong> unique2RIDS;
		protected String indexFile;
		protected String table;
		protected long tableCount;
		protected int lineOffset;
		protected long lineCount = 0;
		
		public ReadThread(ArrayList<String> keys, HashMap<String, Integer> cols2Pos, DBConcurrentSkipTreeMap<String[], ArrayListLong> unique2RIDS, String indexFile, String table, int lineOffset, String tableFile)
		{
			try
			{
				this.file = new BufferedRandomAccessFile(tableFile, "r");
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(1);
			}
			this.keys = keys;
			this.cols2Pos = cols2Pos;
			this.unique2RIDS = unique2RIDS;
			this.indexFile = indexFile;
			this.table = table;
			readCounts.putIfAbsent(indexFile, new AtomicLong(0));
			tableCount = meta.getTableCard("TPCH", table) / (meta.getNumDevices() * meta.getNumNodes());
			this.lineOffset = lineOffset;
		}
		
		public void run()
		{
			Thread.currentThread().setPriority(Thread.NORM_PRIORITY);
			try
			{
				long RID = 0;
				String line = null;
			
				FastStringTokenizer tokens = new FastStringTokenizer("", "|", false);
				while (true)
				{
					while (true)
					{
						if (lineCount % NUM_THREADS == lineOffset)
						{
							RID = file.getFilePointer(); 
							line = file.readLine();
							lineCount++;
							break;
						}
						
						if (!file.skipLine())
						{
							line = null;
							break;
						}
						lineCount++;
					}
					
					//synchronized(file)
					//{
					//	RID = ResourceManager.internLong(file.getFilePointer()); 
					//	line = file.readLine();
					//}
					
					if (line == null)
					{
						break;
					}
					
					String[] key = new String[keys.size()];
					tokens.reuse(line, "|", false);
					String[] all = tokens.allTokens();
					int i = 0;
					for (String k : keys)
					{
						key[i] = all[cols2Pos.get(k)];
						i++;
					}
						
					ArrayListLong rid = unique2RIDS.get(key);
					if (rid == null)
					{
						rid = new ArrayListLong(1);
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
						rid = prev;
					}
					
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
	
	private static class DBConcurrentSkipTreeMap<K, V> extends ConcurrentSkipTreeMap<K, V> implements Iterable
	{
		protected long size = 0;
		protected int numFiles = 0;
		protected ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
		
		//constructor
		public DBConcurrentSkipTreeMap(RowComparator rc)
		{
			super(rc);
		}
		
		//size
		public long longSize()
		{
			return size;
		}
		
		//entrySet
		public Iterator<Map.Entry<K, V>> iterator()
		{
			return new DBCSTMPIter<K, V>();
		}
		
		public V get(Object key)
		{
			lock.readLock().lock();
			V retval = super.get(key);
			lock.readLock().unlock();
			return retval;
		}
		
		//putIfAbsent
		public V putIfAbsent(K key, V value)
		{
			if (super.size() < SEGMENT_SIZE)
			{
				lock.readLock().lock();
				V retval = super.putIfAbsent(key, value);
				lock.readLock().unlock();
				if (retval == null)
				{
					size++;
				}
				
				return retval;
			}
			else
			{
				synchronized(this)
				{
					if (super.size() < SEGMENT_SIZE)
					{
						V retval = super.putIfAbsent(key, value);
						if (retval == null)
						{
							size++;
						}
						
						return retval;
					}
					try
					{
						ObjectOutputStream out = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream("./" + super.toString() + "_" + numFiles++)));
						lock.writeLock().lock();
						for (Map.Entry entry : super.entrySet())
						{
							out.writeUnshared(entry.getKey());
							out.reset();
							out.writeUnshared(entry.getValue());
							out.reset();
						}
				
						super.clear();
						lock.writeLock().unlock();
						out.flush();
						out.close();
						return super.putIfAbsent(key, value);
					}
					catch(Exception e)
					{
						e.printStackTrace();
						System.exit(1);
						return null;
					}
				}
			}
		}
		
		private class DBCSTMPIter<K, V> implements Iterator<Map.Entry<K, V>>
		{
			protected ObjectInputStream[] ins;
			protected Object[] nexts;
			protected ArrayList<Integer> retvalPoses = new ArrayList<Integer>();
			
			@SuppressWarnings("unchecked")
			public DBCSTMPIter()
			{
				if (DBConcurrentSkipTreeMap.super.size() > 0)
				{
					try
					{
						ObjectOutputStream out = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream("./" + DBConcurrentSkipTreeMap.super.toString() + "_" + numFiles++)));
						for (Map.Entry entry : DBConcurrentSkipTreeMap.super.entrySet())
						{
							out.writeUnshared(entry.getKey());
							out.reset();
							out.writeUnshared(entry.getValue());
							out.reset();
						}
				
						out.flush();
						out.close();
						DBConcurrentSkipTreeMap.super.clear();
					}
					catch(Exception e)
					{
						e.printStackTrace();
						System.exit(1);
					}
				}
				
				int i = 0;
				ins = new ObjectInputStream[numFiles];
				nexts = new Object[numFiles];
				while (i < numFiles)
				{
					try
					{
						ins[i] = new ObjectInputStream(new BufferedInputStream(new FileInputStream("./" + DBConcurrentSkipTreeMap.super.toString() + "_" + i), 65536));
						K key = null;
						try
						{
							key = (K)ins[i].readUnshared();
						}
						catch(EOFException f)
						{
							nexts[i] = new DataEndMarker();
							i++;
							continue;
						}
						V value = (V)ins[i].readUnshared();
						nexts[i] = new MapEntry<K, V>(key, value);
						i++;
					}
					catch(Exception e)
					{
						e.printStackTrace();
						System.exit(1);
					}
				}
			}
			
			private class MapEntry<K, V> implements Map.Entry<K, V>
			{
				protected K key;
				protected V value;
				
				public MapEntry(K key, V value)
				{
					this.key = key;
					this.value = value;
				}
				
				@Override
				public K getKey() {
					return key;
				}

				@Override
				public V getValue() {
					return value;
				}

				@Override
				public V setValue(V value) {
					return null;
				}
				
			}
			
			@Override
			public boolean hasNext() {
				for (Object o : nexts)
				{
					if (!(o instanceof DataEndMarker))
					{
						return true;
					}
				}
				
				return false;
			}

			@Override
			public Map.Entry<K, V> next() {
				Map.Entry retval = null;
				retvalPoses.clear();
				int i = 0;
				for (Object o : nexts)
				{
					if (retval == null && !(o instanceof DataEndMarker))
					{
						retval = (Map.Entry)o;
						retvalPoses.add(i);
					}
					else 
					{
						if (!(o instanceof DataEndMarker))
						{
							Map.Entry candidate = (Map.Entry)o;
							if (((RowComparator)(DBConcurrentSkipTreeMap.super.comparator())).compare((K)candidate.getKey(), (K)retval.getKey()) < 0)
							{
								retval = candidate;
								retvalPoses.clear();
								retvalPoses.add(i);
							}
							else if (((RowComparator)(DBConcurrentSkipTreeMap.super.comparator())).compare((K)candidate.getKey(), (K)retval.getKey()) == 0)
							{
								retvalPoses.add(i);
							}
						}
					}
					
					i++;
				}
				
				ArrayListLong value = new ArrayListLong(1);
				for (int pos : retvalPoses)
				{
					value.addAll(((ArrayListLong)((Map.Entry)nexts[pos]).getValue()));
					//read next entry from disk
					K key = null;
					try
					{
						try
						{
							key = (K)ins[pos].readUnshared();
							V val = (V)ins[pos].readUnshared();
							nexts[pos] = new MapEntry<K, V>(key, val);
						}
						catch(EOFException f)
						{
							nexts[pos] = new DataEndMarker();
						}
					}
					catch(Exception e)
					{
						e.printStackTrace();
						System.exit(1);
					}
				}
				
				return new MapEntry(retval.getKey(), value);
			}

			@Override
			public void remove() {
			}
			
		}
	}
}
