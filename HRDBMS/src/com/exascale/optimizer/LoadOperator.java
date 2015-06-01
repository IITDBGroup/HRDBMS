package com.exascale.optimizer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.mapred.ALOWritable;
import com.exascale.mapred.LoadMapper;
import com.exascale.mapred.LoadOutputFormat;
import com.exascale.mapred.LoadReducer;
import com.exascale.mapred.MyLongWritable;
import com.exascale.misc.DataEndMarker;
import com.exascale.misc.FastStringTokenizer;
import com.exascale.misc.MultiHashMap;
import com.exascale.misc.MyDate;
import com.exascale.misc.Utils;
import com.exascale.optimizer.MetaData.PartitionMetaData;
import com.exascale.tables.Plan;
import com.exascale.tables.Transaction;
import com.exascale.threads.HRDBMSThread;

public final class LoadOperator implements Operator, Serializable
{
	private final MetaData meta;
	private HashMap<String, String> cols2Types;
	private HashMap<String, Integer> cols2Pos;
	private TreeMap<Integer, String> pos2Col;
	private Operator parent;
	private int node;
	private transient Plan plan;
	private final String schema;
	private final String table;
	private final AtomicInteger num = new AtomicInteger(0);
	private volatile boolean done = false;
	private final MultiHashMap map = new MultiHashMap<Long, ArrayList<Object>>();
	private Transaction tx;
	private final boolean replace;
	private final String delimiter;
	private final String glob;
	private final ArrayList<String> types2 = new ArrayList<String>();
	private final HashSet<Integer> touched = new HashSet<Integer>();
	private final ArrayList<FlushThread> fThreads = new ArrayList<FlushThread>();

	public LoadOperator(String schema, String table, boolean replace, String delimiter, String glob, MetaData meta)
	{
		this.schema = schema;
		this.table = table;
		this.replace = replace;
		this.delimiter = delimiter;
		this.glob = glob;
		this.meta = meta;
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

	private static boolean doSendLDMD(ArrayList<Object> tree, String key, LoadMetaData ldmd, Transaction tx)
	{
		Object obj = tree.get(0);
		while (obj instanceof ArrayList)
		{
			obj = ((ArrayList)obj).get(0);
		}

		Socket sock = null;
		try
		{
			String hostname = new MetaData().getHostNameForNode((Integer)obj, tx);
			// sock = new Socket(hostname,
			// Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
			sock = new Socket();
			sock.setReceiveBufferSize(262144);
			sock.setSendBufferSize(262144);
			sock.connect(new InetSocketAddress(hostname, Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number"))));
			OutputStream out = sock.getOutputStream();
			byte[] outMsg = "SETLDMD         ".getBytes(StandardCharsets.UTF_8);
			outMsg[8] = 0;
			outMsg[9] = 0;
			outMsg[10] = 0;
			outMsg[11] = 0;
			outMsg[12] = 0;
			outMsg[13] = 0;
			outMsg[14] = 0;
			outMsg[15] = 0;
			out.write(outMsg);
			byte[] data = key.getBytes(StandardCharsets.UTF_8);
			byte[] length = intToBytes(data.length);
			out.write(length);
			out.write(data);
			ObjectOutputStream objOut = new ObjectOutputStream(out);
			objOut.writeObject(convertToHosts(tree, tx));
			objOut.writeObject(ldmd);
			objOut.flush();
			out.flush();
			getConfirmation(sock);
			objOut.close();
			out.close();
			sock.close();
			return true;
		}
		catch (Exception e)
		{
			try
			{
				if (sock != null)
				{
					sock.close();
				}
			}
			catch (Exception f)
			{
			}
			return false;
		}
	}

	private static boolean doSendRemoveLDMD(ArrayList<Object> tree, String key, Transaction tx)
	{
		Object obj = tree.get(0);
		while (obj instanceof ArrayList)
		{
			obj = ((ArrayList)obj).get(0);
		}

		Socket sock = null;
		try
		{
			String hostname = new MetaData().getHostNameForNode((Integer)obj, tx);
			// sock = new Socket(hostname,
			// Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
			sock = new Socket();
			sock.setReceiveBufferSize(262144);
			sock.setSendBufferSize(262144);
			sock.connect(new InetSocketAddress(hostname, Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number"))));
			OutputStream out = sock.getOutputStream();
			byte[] outMsg = "DELLDMD         ".getBytes(StandardCharsets.UTF_8);
			outMsg[8] = 0;
			outMsg[9] = 0;
			outMsg[10] = 0;
			outMsg[11] = 0;
			outMsg[12] = 0;
			outMsg[13] = 0;
			outMsg[14] = 0;
			outMsg[15] = 0;
			out.write(outMsg);
			byte[] data = key.getBytes(StandardCharsets.UTF_8);
			byte[] length = intToBytes(data.length);
			out.write(length);
			out.write(data);
			ObjectOutputStream objOut = new ObjectOutputStream(out);
			objOut.writeObject(convertToHosts(tree, tx));
			objOut.flush();
			out.flush();
			getConfirmation(sock);
			objOut.close();
			out.close();
			sock.close();
			return true;
		}
		catch (Exception e)
		{
			try
			{
				if (sock != null)
				{
					sock.close();
				}
			}
			catch (Exception f)
			{
			}
			return false;
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

	private static int numDevicesPerNode()
	{
		String dirs = HRDBMSWorker.getHParms().getProperty("data_directories");
		FastStringTokenizer tokens = new FastStringTokenizer(dirs, ",", false);
		return tokens.allTokens().length;
	}

	private static final byte[] rsToBytes(Set<ArrayList<Object>> rows) throws Exception
	{
		final ArrayList<byte[]> results = new ArrayList<byte[]>(rows.size());
		ArrayList<byte[]> bytes = new ArrayList<byte[]>();
		for (ArrayList<Object> val : rows)
		{
			bytes.clear();
			int size = val.size() + 8;
			final byte[] header = new byte[size];
			int i = 8;
			for (final Object o : val)
			{
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
					size += 8;
				}
				else if (o instanceof String)
				{
					header[i] = (byte)4;
					byte[] b = ((String)o).getBytes(StandardCharsets.UTF_8);
					size += (4 + b.length);
					bytes.add(b);
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
						throw e;
					}
					header[i] = (byte)8;
				}
				else
				{
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
			int bOff = 0;
			for (final Object o : val)
			{
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
					retvalBB.putLong(((MyDate)o).getTime());
				}
				else if (retval[i] == 4)
				{
					byte[] temp = bytes.get(bOff++);
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

			results.add(retval);
		}

		int count = 0;
		for (final byte[] ba : results)
		{
			count += ba.length;
		}
		final byte[] retval = new byte[count + 4];
		ByteBuffer temp = ByteBuffer.allocate(4);
		temp.asIntBuffer().put(results.size());
		System.arraycopy(temp.array(), 0, retval, 0, 4);
		int retvalPos = 4;
		for (final byte[] ba : results)
		{
			System.arraycopy(ba, 0, retval, retvalPos, ba.length);
			retvalPos += ba.length;
		}

		return retval;
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

	public Plan getPlan()
	{
		return plan;
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

	public String getSchema()
	{
		return schema;
	}

	public String getTable()
	{
		return table;
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
	public void serialize(OutputStream out, IdentityHashMap<Object, Long> prev) throws Exception
	{
		throw new Exception("Tried to call serialize on load operator");
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
	public void setPlan(Plan plan)
	{
		this.plan = plan;
	}

	public void setTransaction(Transaction tx)
	{
		this.tx = tx;
	}

	@Override
	public void start() throws Exception
	{
		if (replace)
		{
			MassDeleteOperator delete = new MassDeleteOperator(schema, table, meta, false);
			delete.setPlan(plan);
			delete.setTransaction(tx);
			delete.start();
			delete.next(this);
			delete.close();
		}

		cols2Pos = new MetaData().getCols2PosForTable(schema, table, tx);
		pos2Col = MetaData.cols2PosFlip(cols2Pos);
		cols2Types = new MetaData().getCols2TypesForTable(schema, table, tx);

		for (String col : pos2Col.values())
		{
			types2.add(cols2Types.get(col));
		}

		ArrayList<String> indexes = meta.getIndexFileNamesForTable(schema, table, tx);
		ArrayList<ArrayList<String>> keys = meta.getKeys(indexes, tx);
		ArrayList<ArrayList<String>> types = meta.getTypes(indexes, tx);
		ArrayList<ArrayList<Boolean>> orders = meta.getOrders(indexes, tx);

		HashMap<Integer, Integer> pos2Length = new HashMap<Integer, Integer>();
		for (Map.Entry entry : cols2Types.entrySet())
		{
			if (entry.getValue().equals("CHAR"))
			{
				int length = meta.getLengthForCharCol(schema, table, (String)entry.getKey(), tx);
				pos2Length.put(cols2Pos.get(entry.getKey()), length);
			}
		}

		if (glob.startsWith("hdfs://"))
		{
			doHDFS(schema, table, tx, pos2Col, cols2Types, pos2Length, keys, types, orders, indexes);
			return;
		}

		// figure out what files to read from
		final ArrayList<Path> files = new ArrayList<Path>();
		final PathMatcher matcher = FileSystems.getDefault().getPathMatcher("glob:" + glob);
		Files.walkFileTree(Paths.get("/"), new SimpleFileVisitor<Path>() {
			@Override
			public FileVisitResult visitFile(Path file, java.nio.file.attribute.BasicFileAttributes attrs) throws IOException
			{
				if (matcher.matches(file))
				{
					files.add(file);
				}
				return FileVisitResult.CONTINUE;
			}

			@Override
			public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException
			{
				return FileVisitResult.CONTINUE;
			}
		});

		ArrayList<ReadThread> threads = new ArrayList<ReadThread>();
		PartitionMetaData spmd = new MetaData().getPartMeta(schema, table, tx);
		if (files.size() == 0)
		{
			throw new Exception("Load input files were not found!");
		}
		for (Path path : files)
		{
			threads.add(new ReadThread(path.toFile(), pos2Length, indexes, cols2Pos, cols2Types, pos2Col, spmd, keys, types, orders));
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
				catch (InterruptedException e)
				{
				}
			}
		}

		if (!allOK)
		{
			num.set(Integer.MIN_VALUE);
		}

		done = true;
	}

	@Override
	public String toString()
	{
		return "InsertOperator";
	}

	private void doHDFS(String schema, String table, Transaction tx, TreeMap<Integer, String> pos2Col, HashMap<String, String> cols2Types, HashMap<Integer, Integer> pos2Length, ArrayList<ArrayList<String>> keys, ArrayList<ArrayList<String>> types, ArrayList<ArrayList<Boolean>> orders, ArrayList<String> indexes)
	{
		try
		{
			// build LoadMetaData
			MetaData md = new MetaData();
			PartitionMetaData pmd = md.getPartMeta(schema, table, tx);
			ArrayList<Integer> coordNodes = MetaData.getCoordNodes();
			ArrayList<Integer> workerNodes = MetaData.getWorkerNodes();
			LoadMetaData ldmd = new LoadMetaData(workerNodes.size(), delimiter, pos2Col, cols2Types, pos2Length, pmd, workerNodes, coordNodes, tx.number(), keys, types, orders, indexes);
			String key = schema + "." + table;
			// send LoadMetaData everywhere
			ArrayList<Object> tree = makeTree(workerNodes);
			if (!sendLDMD(tree, key, ldmd, tx))
			{
				HRDBMSWorker.logger.debug("Load MetaData returned an error");
				HRDBMSWorker.logger.debug("Sending DELLDMD");
				tree = makeTree(workerNodes);
				sendRemoveLDMD(tree, key, tx);
				num.set(Integer.MIN_VALUE);
				done = true;
				return;
			}
			// build and start hadoop job
			try
			{
				HRDBMSWorker.logger.debug("Load MetaData was successful");
				Configuration conf = new Configuration();
				conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
				conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
				conf.addResource(new org.apache.hadoop.fs.Path("/usr/local/hadoop-2.5.1/etc/hadoop/core-site.xml"));
				conf.addResource(new org.apache.hadoop.fs.Path("/usr/local/hadoop-2.5.1/etc/hadoop/hdfs-site.xml"));
				conf.addResource(new org.apache.hadoop.fs.Path("/usr/local/hadoop-2.5.1/etc/hadoop/yarn-site.xml"));
				conf.set("hrdbms.port", HRDBMSWorker.getHParms().getProperty("port_number"));
				String hrdbmsHome = System.getProperty("user.dir");
				if (!hrdbmsHome.endsWith("/"))
				{
					hrdbmsHome += "/";
				}
				conf.set("hrdbms.home", hrdbmsHome);
				conf.set("mapreduce.framework.name", "yarn");
				conf.set("mapreduce.map.maxattempts", "1");
				conf.set("mapreduce.reduce.maxattempts", "1");
				conf.set("mapreduce.task.io.sort.mb", "512");
				conf.set("mapreduce.task.io.sort.factor", "100");
				// conf.set("mapreduce.reduce.shuffle.parallelcopies", "50");
				conf.set("mapreduce.map.memory.mb", "1536");
				conf.set("mapreduce.map.java.opts", "-Xmx1024M");
				conf.set("mapreduce.reduce.memory.mb", "1536");
				conf.set("mapreduce.reduce.java.opts", "-Xmx1280M");
				conf.set("hrdbms.num.devices", "" + numDevicesPerNode());
				conf.set("hrdbms.num.workers", "" + workerNodes.size());
				Job job = new Job(conf);
				job.setJarByClass(LoadMapper.class);
				job.setJobName("Load " + schema + "." + table);
				job.setMapperClass(LoadMapper.class);
				job.setReducerClass(LoadReducer.class);
				job.setOutputKeyClass(MyLongWritable.class);
				job.setOutputValueClass(ALOWritable.class);
				job.setMapOutputKeyClass(MyLongWritable.class);
				job.setMapOutputValueClass(ALOWritable.class);
				job.setNumReduceTasks(workerNodes.size() * numDevicesPerNode());
				HRDBMSWorker.logger.debug("Asking for " + workerNodes.size() * numDevicesPerNode() + " reducers");
				job.setOutputFormatClass(LoadOutputFormat.class);
				job.setReduceSpeculativeExecution(false);
				job.setMapSpeculativeExecution(false);
				String glob2 = glob.substring(6);
				FileInputFormat.addInputPath(job, new org.apache.hadoop.fs.Path(glob2));
				HRDBMSWorker.logger.debug("Submitting MR job");
				boolean allOK = job.waitForCompletion(true);
				HRDBMSWorker.logger.debug("Sending DELLDMD");
				tree = makeTree(workerNodes);
				sendRemoveLDMD(tree, key, tx);
				if (!allOK)
				{
					HRDBMSWorker.logger.debug("MR returned an error");
					num.set(Integer.MIN_VALUE);
				}
				else
				{
					HRDBMSWorker.logger.debug("MR was successful");
					num.set((int)job.getCounters().findCounter("org.apache.hadoop.mapred.Task$Counter", "MAP_INPUT_RECORDS").getValue());
					if (num.get() == Integer.MIN_VALUE)
					{
						num.getAndIncrement();
					}
				}

				done = true;
			}
			catch (Throwable f)
			{
				HRDBMSWorker.logger.debug("An exception occurred while building and submitting the MR job", f);
				tree = makeTree(workerNodes);
				sendRemoveLDMD(tree, key, tx);
				num.set(Integer.MIN_VALUE);
				done = true;
				return;
			}
		}
		catch (Exception e)
		{
			HRDBMSWorker.logger.debug("An exception occurred prior to building the MR job", e);
			num.set(Integer.MIN_VALUE);
			done = true;
		}
	}

	private FlushMasterThread flush(ArrayList<String> indexes, PartitionMetaData spmd, ArrayList<ArrayList<String>> keys, ArrayList<ArrayList<String>> types, ArrayList<ArrayList<Boolean>> orders) throws Exception
	{
		FlushMasterThread master = new FlushMasterThread(indexes, spmd, keys, types, orders);
		master.start();
		return master;
	}

	private boolean sendLDMD(ArrayList<Object> tree, String key, LoadMetaData ldmd, Transaction tx)
	{
		boolean allOK = true;
		ArrayList<SendLDMDThread> threads = new ArrayList<SendLDMDThread>();
		for (Object o : tree)
		{
			if (o instanceof Integer)
			{
				ArrayList<Object> list = new ArrayList<Object>(1);
				list.add(o);
				SendLDMDThread thread = new SendLDMDThread(list, key, ldmd, tx);
				threads.add(thread);
			}
			else
			{
				SendLDMDThread thread = new SendLDMDThread((ArrayList<Object>)o, key, ldmd, tx);
				threads.add(thread);
			}
		}

		for (SendLDMDThread thread : threads)
		{
			thread.start();
		}

		for (SendLDMDThread thread : threads)
		{
			while (true)
			{
				try
				{
					thread.join();
					break;
				}
				catch (InterruptedException e)
				{
				}
			}
			boolean ok = thread.getOK();
			if (!ok)
			{
				allOK = false;
			}
		}

		return allOK;
	}

	private boolean sendRemoveLDMD(ArrayList<Object> tree, String key, Transaction tx)
	{
		boolean allOK = true;
		ArrayList<SendRemoveLDMDThread> threads = new ArrayList<SendRemoveLDMDThread>();
		for (Object o : tree)
		{
			if (o instanceof Integer)
			{
				ArrayList<Object> list = new ArrayList<Object>(1);
				list.add(o);
				SendRemoveLDMDThread thread = new SendRemoveLDMDThread(list, key, tx);
				threads.add(thread);
			}
			else
			{
				SendRemoveLDMDThread thread = new SendRemoveLDMDThread((ArrayList<Object>)o, key, tx);
				threads.add(thread);
			}
		}

		for (SendRemoveLDMDThread thread : threads)
		{
			thread.start();
		}

		for (SendRemoveLDMDThread thread : threads)
		{
			while (true)
			{
				try
				{
					thread.join();
					break;
				}
				catch (InterruptedException e)
				{
				}
			}
			boolean ok = thread.getOK();
			if (!ok)
			{
				allOK = false;
			}
		}

		return allOK;
	}

	private byte[] stringToBytes(String string)
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

	private class FlushMasterThread extends HRDBMSThread
	{
		private final ArrayList<String> indexes;
		private boolean ok;
		private final PartitionMetaData spmd;
		private final ArrayList<ArrayList<String>> keys;
		private final ArrayList<ArrayList<String>> types;
		private final ArrayList<ArrayList<Boolean>> orders;

		public FlushMasterThread(ArrayList<String> indexes, PartitionMetaData spmd, ArrayList<ArrayList<String>> keys, ArrayList<ArrayList<String>> types, ArrayList<ArrayList<Boolean>> orders)
		{
			this.indexes = indexes;
			this.spmd = spmd;
			this.keys = keys;
			this.types = types;
			this.orders = orders;
		}

		public boolean getOK()
		{
			return ok;
		}

		@Override
		public void run()
		{
			ok = true;
			ArrayList<FlushThread> threads = new ArrayList<FlushThread>();
			synchronized (map)
			{
				for (Object o : map.getKeySet())
				{
					long key = (Long)o;
					Set<ArrayList<Object>> list = map.get(key);
					threads.add(new FlushThread(list, indexes, key, cols2Pos, spmd, keys, types, orders));
				}

				map.clear();
			}

			for (FlushThread thread : threads)
			{
				thread.start();
			}

			fThreads.addAll(threads);
		}
	}

	private class FlushThread extends HRDBMSThread
	{
		private Set<ArrayList<Object>> list;
		private ArrayList<String> indexes;
		private boolean ok = true;
		private final long key;
		private HashMap<String, Integer> cols2Pos;
		private ArrayList<ArrayList<String>> keys;
		private ArrayList<ArrayList<String>> types;
		private ArrayList<ArrayList<Boolean>> orders;

		public FlushThread(Set<ArrayList<Object>> list, ArrayList<String> indexes, long key, HashMap<String, Integer> cols2Pos, PartitionMetaData spmd, ArrayList<ArrayList<String>> keys, ArrayList<ArrayList<String>> types, ArrayList<ArrayList<Boolean>> orders)
		{
			this.list = list;
			this.indexes = indexes;
			this.key = key;
			this.cols2Pos = cols2Pos;
			this.keys = keys;
			this.types = types;
			this.orders = orders;
		}

		public boolean getOK()
		{
			return ok;
		}

		@Override
		public void run()
		{
			// send schema, table, tx, indexes, list, and cols2Pos
			Socket sock = null;
			try
			{
				int node = (int)(key >> 32);
				int device = (int)(key & 0x00000000FFFFFFFF);
				String hostname = new MetaData().getHostNameForNode(node, tx);
				// sock = new Socket(hostname,
				// Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
				sock = new Socket();
				sock.setReceiveBufferSize(262144);
				sock.setSendBufferSize(262144);
				sock.connect(new InetSocketAddress(hostname, Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number"))));
				OutputStream out = sock.getOutputStream();
				byte[] outMsg = "LOAD            ".getBytes(StandardCharsets.UTF_8);
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
				out.write(intToBytes(device));
				out.write(stringToBytes(schema));
				out.write(stringToBytes(table));
				out.write(rsToBytes(list));
				list = null;
				ObjectOutputStream objOut = new ObjectOutputStream(out);
				objOut.writeObject(indexes);
				objOut.writeObject(keys);
				objOut.writeObject(types);
				objOut.writeObject(orders);
				objOut.writeObject(cols2Pos);
				objOut.flush();
				out.flush();
				getConfirmation(sock);
				objOut.close();
				sock.close();
				indexes = null;
				cols2Pos = null;
				keys = null;
				types = null;
				orders = null;
				;
			}
			catch (Exception e)
			{
				try
				{
					if (sock != null)
					{
						sock.close();
					}
				}
				catch (Exception f)
				{
				}
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
	}

	private class ReadThread extends HRDBMSThread
	{
		private final File file;
		private final HashMap<Integer, Integer> pos2Length;
		private final ArrayList<String> indexes;
		private boolean ok = true;
		private int num = 0;
		private final HashMap<String, Integer> cols2Pos;
		PartitionMetaData spmd;
		private final ArrayList<ArrayList<String>> keys;
		private final ArrayList<ArrayList<String>> types;
		private final ArrayList<ArrayList<Boolean>> orders;

		public ReadThread(File file, HashMap<Integer, Integer> pos2Length, ArrayList<String> indexes, HashMap<String, Integer> cols2Pos, HashMap<String, String> cols2Types, TreeMap<Integer, String> pos2Col, PartitionMetaData spmd, ArrayList<ArrayList<String>> keys, ArrayList<ArrayList<String>> types, ArrayList<ArrayList<Boolean>> orders)
		{
			this.file = file;
			this.pos2Length = pos2Length;
			this.indexes = indexes;
			this.cols2Pos = cols2Pos;
			this.spmd = spmd;
			this.keys = keys;
			this.types = types;
			this.orders = orders;
		}

		public int getNum()
		{
			return num;
		}

		public boolean getOK()
		{
			return ok;
		}

		@Override
		public void run()
		{
			FlushMasterThread master = null;
			try
			{
				BufferedReader in = new BufferedReader(new FileReader(file), 64 * 1024);
				Object o = next(in);
				PartitionMetaData pmeta = new MetaData().new PartitionMetaData(schema, table, tx);
				int numNodes = MetaData.numWorkerNodes;
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
					ArrayList<Integer> nodes = MetaData.determineNode(schema, table, row, tx, pmeta, cols2Pos, numNodes);
					int device = MetaData.determineDevice(row, pmeta, cols2Pos);
					touched.addAll(nodes);
					for (Integer node : nodes)
					{
						plan.addNode(node);
						long key = (((long)node) << 32) + device;
						map.multiPut(key, row);
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
						master = flush(indexes, spmd, keys, types, orders);
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
						master = flush(indexes, spmd, keys, types, orders);
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
					master = flush(indexes, spmd, keys, types, orders);
					master.join();
					if (!master.getOK())
					{
						throw new Exception("Error flushing inserts");
					}
				}

				for (FlushThread thread : fThreads)
				{
					thread.join();
					if (!thread.getOK())
					{
						throw new Exception("Error flushing inserts");
					}
				}
			}
			catch (Exception e)
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
			FastStringTokenizer tokens = new FastStringTokenizer(line, delimiter, false);
			int i = 0;
			while (tokens.hasMoreTokens())
			{
				String token = tokens.nextToken();
				String type = types2.get(i);
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
					row.add(Utils.parseDouble(token));
				}
				else if (type.equals("DATE"))
				{
					row.add(new MyDate(Integer.parseInt(token.substring(0, 4)), Integer.parseInt(token.substring(5, 7)), Integer.parseInt(token.substring(8, 10))));
				}
			}

			return row;
		}
	}

	private static class SendLDMDThread extends HRDBMSThread
	{
		private final ArrayList<Object> tree;
		private final String key;
		private final LoadMetaData ldmd;
		private boolean ok;
		private final Transaction tx;

		public SendLDMDThread(ArrayList<Object> tree, String key, LoadMetaData ldmd, Transaction tx)
		{
			this.tree = tree;
			this.key = key;
			this.ldmd = ldmd;
			this.tx = tx;
		}

		public boolean getOK()
		{
			return ok;
		}

		@Override
		public void run()
		{
			ok = doSendLDMD(tree, key, ldmd, tx);
		}
	}

	private static class SendRemoveLDMDThread extends HRDBMSThread
	{
		private final ArrayList<Object> tree;
		private final String key;
		private boolean ok;
		private final Transaction tx;

		public SendRemoveLDMDThread(ArrayList<Object> tree, String key, Transaction tx)
		{
			this.tree = tree;
			this.key = key;
			this.tx = tx;
		}

		public boolean getOK()
		{
			return ok;
		}

		@Override
		public void run()
		{
			ok = doSendRemoveLDMD(tree, key, tx);
		}
	}
}
