package com.exascale.optimizer;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.net.Socket;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;
import com.exascale.compression.CompressedSocket;
import com.exascale.filesystem.RID;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.misc.DataEndMarker;
import com.exascale.misc.MultiHashMap;
import com.exascale.misc.MyDate;
import com.exascale.optimizer.MetaData.PartitionMetaData;
import com.exascale.tables.Plan;
import com.exascale.tables.Transaction;
import com.exascale.threads.HRDBMSThread;

public final class LoadOperator implements Operator, Serializable
{
	private Operator child;
	private final MetaData meta;
	private HashMap<String, String> cols2Types;
	private HashMap<String, Integer> cols2Pos;
	private TreeMap<Integer, String> pos2Col;
	private Operator parent;
	private int node;
	private transient Plan plan;
	private String schema;
	private String table;
	private AtomicInteger num = new AtomicInteger(0);
	private boolean done = false;
	private MultiHashMap map = new MultiHashMap<Integer, ArrayList<Object>>();
	private Transaction tx;
	private boolean replace;
	private String delimiter;
	private String glob;
	private ArrayList<String> types = new ArrayList<String>();
	
	public void setPlan(Plan plan)
	{
		this.plan = plan;
	}
	
	public String getSchema()
	{
		return schema;
	}
	
	public String getTable()
	{
		return table;
	}
	
	public Plan getPlan()
	{
		return plan;
	}

	public LoadOperator(String schema, String table, boolean replace, String delimiter, String glob, MetaData meta)
	{
		this.schema = schema;
		this.table = table;
		this.replace = replace;
		this.delimiter = delimiter;
		this.glob = glob;
		this.meta = meta;
	}
	
	public void setTransaction(Transaction tx)
	{
		this.tx = tx;
	}

	@Override
	public void add(Operator op) throws Exception
	{
		throw new Exception("LoadOperator does not support children");
	}

	@Override
	public ArrayList<Operator> children()
	{
		final ArrayList<Operator> retval = new ArrayList<Operator>(1);
		return retval;
	}

	@Override
	public LoadOperator clone()
	{
		final LoadOperator retval = new LoadOperator(schema, table, replace, delimiter, glob, meta);
		retval.node = node;
		return retval;
	}

	@Override
	public void close() throws Exception
	{
	}

	@Override
	public int getChildPos()
	{
		return 0;
	}

	@Override
	public HashMap<String, Integer> getCols2Pos()
	{
		return cols2Pos;
	}

	@Override
	public HashMap<String, String> getCols2Types()
	{
		return cols2Types;
	}

	@Override
	public MetaData getMeta()
	{
		return meta;
	}

	@Override
	public int getNode()
	{
		return node;
	}

	@Override
	public TreeMap<Integer, String> getPos2Col()
	{
		return pos2Col;
	}

	@Override
	public ArrayList<String> getReferences()
	{
		final ArrayList<String> retval = new ArrayList<String>();
		return retval;
	}

	@Override
	// @?Parallel
	public Object next(Operator op) throws Exception
	{
		while (!done)
		{
			Thread.sleep(1);
		}
		
		if (num.get() == Integer.MIN_VALUE)
		{
			throw new Exception("An error occured during a load operation");
		}
		
		if (num.get() < 0)
		{
			return new DataEndMarker();
		}
		
		int retval = num.get();
		num.set(-1);
		return retval;
	}

	@Override
	public void nextAll(Operator op) throws Exception
	{
		num.set(-1);
	}

	@Override
	public Operator parent()
	{
		return parent;
	}

	@Override
	public void registerParent(Operator op) throws Exception
	{
		throw new Exception("LoadOperator does not support parents");
	}

	@Override
	public void removeChild(Operator op)
	{
		
	}

	@Override
	public void removeParent(Operator op)
	{
		parent = null;
	}

	@Override
	public void reset() throws Exception
	{
		throw new Exception("LoadOperator does not support reset()");
	}

	@Override
	public void setChildPos(int pos)
	{
	}

	@Override
	public void setNode(int node)
	{
		this.node = node;
	}

	@Override
	public void start() throws Exception
	{
		if (replace)
		{
			MassDeleteOperator delete = new MassDeleteOperator(schema, table, meta);
			delete.setPlan(plan);
			delete.setTransaction(tx);
			delete.start();
			delete.next(this);
			delete.close();
		}
		
		TreeMap<Integer, String> pos2Cols = new MetaData().getPos2ColForTable(schema, table, tx);
		HashMap<String, String> cols2Types = new MetaData().getCols2TypesForTable(schema, table, tx);
		for (String col : pos2Cols.values())
		{
			types.add(cols2Types.get(col));
		}
		
		ArrayList<String> indexes = MetaData.getIndexFileNamesForTable(schema, table, tx);
		HashMap<Integer, Integer> pos2Length = new HashMap<Integer, Integer>();
		HashMap<String, Integer> cols2Pos = new MetaData().getCols2PosForTable(schema, table, tx);
		for (Map.Entry entry : new MetaData().getCols2TypesForTable(schema, table, tx).entrySet())
		{
			if (entry.getValue().equals("CHAR"))
			{
				int length = MetaData.getLengthForCharCol(schema, table, (String)entry.getKey(), tx);
				pos2Length.put(cols2Pos.get(entry.getKey()), length);
			}
		}
		
		//figure out what files to read from
		final ArrayList<Path> files = new ArrayList<Path>();
		final PathMatcher matcher = FileSystems.getDefault().getPathMatcher("glob:" + glob);
	    Files.walkFileTree(Paths.get("/"), new SimpleFileVisitor<Path>() {
	        @Override
	        public FileVisitResult visitFile(Path file, java.nio.file.attribute.BasicFileAttributes attrs) throws IOException {
	            if (matcher.matches(file)) {
	                files.add(file);
	            }
	            return FileVisitResult.CONTINUE;
	        }

	        @Override
	        public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
	            return FileVisitResult.CONTINUE;
	        }
	    });
	    
	    ArrayList<ReadThread> threads = new ArrayList<ReadThread>();
	    for (Path path : files)
	    {
	    	threads.add(new ReadThread(path.toFile(), pos2Length, indexes));
	    }
	    
	    for (ReadThread thread : threads)
	    {
	    	thread.start();
	    }
	    
	    boolean allOK = true;
	    for (ReadThread thread : threads)
	    {
	    	while (true)
	    	{
	    		try
	    		{
	    			thread.join();
	    			if (!thread.getOK())
	    			{
	    				allOK = false;
	    			}
	    			
	    			num.getAndAdd(thread.getNum());
	    			break;
	    		}
	    		catch(InterruptedException e)
	    		{}
	    	}
	    }
	    
	    if (!allOK)
	    {
	    	num.set(Integer.MIN_VALUE);
	    }
		
		done = true;
	}
	
	private FlushMasterThread flush(ArrayList<String> indexes) throws Exception
	{
		FlushMasterThread master = new FlushMasterThread(indexes);
		master.start();
		return master;
	}
	
	private class FlushMasterThread extends HRDBMSThread
	{
		private ArrayList<String> indexes;
		private boolean ok;
		
		public FlushMasterThread(ArrayList<String> indexes)
		{
			this.indexes = indexes;
		}
		
		public boolean getOK()
		{
			return ok;
		}
		
		public void run()
		{
			ok = true;
			ArrayList<FlushThread> threads = new ArrayList<FlushThread>();
			synchronized(map)
			{
				for (Object o : map.getKeySet())
				{
					int node = (Integer)o;
					Vector<ArrayList<Object>> list = map.get(node);
					threads.add(new FlushThread(list, indexes, node));
				}
			
				map.clear();
			}
			
			for (FlushThread thread : threads)
			{
				thread.start();
			}
			
			for (FlushThread thread : threads)
			{
				while (true)
				{
					try
					{
						thread.join();
						break;
					}
					catch(InterruptedException e)
					{}
				}
			
				if (!thread.getOK())
				{
					ok = false;
				}
			}
		}
	}
	
	private class ReadThread extends HRDBMSThread
	{
		private File file;
		private HashMap<Integer, Integer> pos2Length;
		private ArrayList<String> indexes;
		private boolean ok = true;
		private int num = 0;
		
		public ReadThread(File file, HashMap<Integer, Integer> pos2Length, ArrayList<String> indexes)
		{
			this.file = file;
			this.pos2Length = pos2Length;
			this.indexes = indexes;
		}
		
		public boolean getOK()
		{
			return ok;
		}
		
		public int getNum()
		{
			return num;
		}
		
		public void run()
		{
			FlushMasterThread master = null;
			try
			{
				TreeMap<Integer, String> pos2Col = MetaData.getPos2ColForTable(schema, table, tx);
				HashMap<String, String> cols2Types = new MetaData().getCols2TypesForTable(schema, table, tx);
				BufferedReader in = new BufferedReader(new FileReader(file));
				Object o = next(in);
				PartitionMetaData pmeta = new MetaData().new PartitionMetaData(schema, table, tx);
				HashMap<String, Integer> cols2Pos = MetaData.getCols2PosForTable(schema, table, tx);
				while (!(o instanceof DataEndMarker))
				{
					ArrayList<Object> row = (ArrayList<Object>)o;
					num++;
					for (Map.Entry entry : pos2Length.entrySet())
					{
						if (((String)row.get((Integer)entry.getKey())).length() > (Integer)entry.getValue())
						{
							ok = false;
							return;
						}
					}
					ArrayList<Integer> nodes = MetaData.determineNode(schema, table, row, tx, pmeta, cols2Pos);
					for (Integer node : nodes)
					{
						plan.addNode(node);
						map.multiPut(node, row);
					}
					if (map.size() > Integer.parseInt(HRDBMSWorker.getHParms().getProperty("max_neighbor_nodes")))
					{
						if (master != null)
						{
							master.join();
							if (!master.getOK())
							{
								throw new Exception("Error flushing inserts");
							}
						}
						master = flush(indexes);
					}
					else if (map.totalSize() > Integer.parseInt(HRDBMSWorker.getHParms().getProperty("max_batch")))
					{
						if (master != null)
						{
							master.join();
							if (!master.getOK())
							{
								throw new Exception("Error flushing inserts");
							}
						}
						master = flush(indexes);
					}
				
					o = next(in);
				}
			
				if (map.totalSize() > 0)
				{
					if (master != null)
					{
						master.join();
						if (!master.getOK())
						{
							throw new Exception("Error flushing inserts");
						}
					}
					master = flush(indexes);
					master.join();
					if (!master.getOK())
					{
						throw new Exception("Error flushing inserts");
					}
				}
			}
			catch(Exception e)
			{
				ok = false;
				HRDBMSWorker.logger.debug("", e);
			}
		}
		
		private Object next(BufferedReader in) throws Exception
		{
			String line = in.readLine();
			if (line == null)
			{
				return new DataEndMarker();
			}
			
			ArrayList<Object> row = new ArrayList<Object>();
			StringTokenizer tokens = new StringTokenizer(line, delimiter, false);
			int i = 0;
			while (tokens.hasMoreTokens())
			{
				String token = tokens.nextToken();
				String type = types.get(i);
				i++;
				
				if (type.equals("CHAR"))
				{
					row.add(token);
				}
				else if (type.equals("INT"))
				{
					row.add(Integer.parseInt(token));
				}
				else if (type.equals("LONG"))
				{
					row.add(Long.parseLong(token));
				}
				else if (type.equals("FLOAT"))
				{
					row.add(Double.parseDouble(token));
				}
				else if (type.equals("DATE"))
				{
					row.add(new MyDate(Integer.parseInt(token.substring(0, 4)), Integer.parseInt(token.substring(5, 7)), Integer.parseInt(token.substring(8, 10))));
				}
			}
			
			return row;
		}
	}
	
	private class FlushThread extends HRDBMSThread
	{
		private Vector<ArrayList<Object>> list;
		private ArrayList<String> indexes;
		private boolean ok = true;
		private int node;
		
		public FlushThread(Vector<ArrayList<Object>> list, ArrayList<String> indexes, int node)
		{
			this.list = list;
			this.indexes = indexes;
			this.node = node;
		}
		
		public boolean getOK()
		{
			return ok;
		}
		
		public void run()
		{
			//send schema, table, tx, indexes, list, and cols2Pos
			Socket sock = null;
			try
			{
				String hostname = new MetaData().getHostNameForNode(node, tx);
				sock = new CompressedSocket(hostname, Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
				OutputStream out = sock.getOutputStream();
				byte[] outMsg = "INSERT          ".getBytes("UTF-8");
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
				ObjectOutputStream objOut = new ObjectOutputStream(out);
				objOut.writeObject(indexes);
				objOut.writeObject(list);
				objOut.writeObject(MetaData.getKeys(indexes, tx));
				objOut.writeObject(MetaData.getTypes(indexes, tx));
				objOut.writeObject(MetaData.getOrders(indexes, tx));
				objOut.writeObject(new MetaData().getCols2PosForTable(schema, table, tx));
				objOut.writeObject(new MetaData().getPartMeta(schema, table, tx));
				objOut.flush();
				out.flush();
				getConfirmation(sock);
				objOut.close();
				sock.close();
			}
			catch(Exception e)
			{
				try
				{
					sock.close();
				}
				catch(Exception f)
				{}
				ok = false;
				HRDBMSWorker.logger.debug("", e);
			}
		}
		
		private void getConfirmation(Socket sock) throws Exception
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
	
	private byte[] stringToBytes(String string)
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

	@Override
	public String toString()
	{
		return "InsertOperator";
	}
	
	private void cast(ArrayList<Object> row, TreeMap<Integer, String> pos2Col, HashMap<String, String> cols2Types)
	{
		int i = 0;
		while (i < pos2Col.size())
		{
			String type = cols2Types.get(pos2Col.get(i));
			Object o = row.get(i);
			if (type.equals("INT"))
			{
				if (o instanceof Long)
				{
					row.remove(i);
					row.add(i, ((Long)o).intValue());
				}
				else if (o instanceof Double)
				{
					row.remove(i);
					row.add(i, ((Double)o).intValue());
				}
			}
			else if (type.equals("LONG"))
			{
				if (o instanceof Integer)
				{
					row.remove(i);
					row.add(i, ((Integer)o).longValue());
				}
				else if (o instanceof Double)
				{
					row.remove(i);
					row.add(i, ((Double)o).longValue());
				}
			}
			else if (type.equals("FLOAT"))
			{
				if (o instanceof Integer)
				{
					row.remove(i);
					row.add(i, ((Integer)o).doubleValue());
				}
				else if (o instanceof Long)
				{
					row.remove(i);
					row.add(i, ((Long)o).doubleValue());
				}
			}
			
			i++;
		}
	}
}
