package com.exascale.optimizer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitOption;
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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
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
import com.exascale.misc.LOMultiHashMap;
import com.exascale.misc.MyDate;
import com.exascale.misc.ScalableStampedRWLock;
import com.exascale.misc.Utils;
import com.exascale.tables.Plan;
import com.exascale.tables.Transaction;
import com.exascale.threads.HRDBMSThread;

public final class LoadOperator implements Operator, Serializable
{
	private static int MAX_NEIGHBOR_NODES = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("max_neighbor_nodes"));
	private static String DATA_DIRS = HRDBMSWorker.getHParms().getProperty("data_directories");
	private static long MAX_QUEUED = Long.parseLong(HRDBMSWorker.getHParms().getProperty("max_queued_load_flush_threads"));
	private static int PORT_NUMBER = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number"));
	private static int MAX_BATCH = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("max_batch"));
	private final MetaData meta;
	private HashMap<String, String> cols2Types;
	private HashMap<String, Integer> cols2Pos;
	private TreeMap<Integer, String> pos2Col;
	private Operator parent;
	private int node;
	private transient Plan plan;
	private final String schema;
	private final String table;
	private final AtomicLong num = new AtomicLong(0);
	private volatile boolean done = false;
	private final LOMultiHashMap map = new LOMultiHashMap<Long, ArrayList<Object>>();
	private Transaction tx;
	private final boolean replace;
	private final String delimiter;
	private final String glob;
	private final ArrayList<String> types2 = new ArrayList<String>();
	private final ArrayList<FlushThread> fThreads = new ArrayList<FlushThread>();
	private transient ConcurrentHashMap<Pair, AtomicInteger> waitTill;
	private volatile transient ArrayList<FlushThread> waitThreads;
	private transient ScalableStampedRWLock lock;

	public LoadOperator(final String schema, final String table, final boolean replace, final String delimiter, final String glob, final MetaData meta)
	{
		this.schema = schema;
		this.table = table;
		this.replace = replace;
		this.delimiter = delimiter;
		this.glob = glob;
		this.meta = meta;
	}

	private static boolean doSendLDMD(final ArrayList<Object> tree, final String key, final LoadMetaData ldmd, final Transaction tx)
	{
		Object obj = tree.get(0);
		while (obj instanceof ArrayList)
		{
			obj = ((ArrayList)obj).get(0);
		}

		Socket sock = null;
		try
		{
			// new MetaData();
			final String hostname = MetaData.getHostNameForNode((Integer)obj, tx);
			// sock = new Socket(hostname,
			// Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
			sock = new Socket();
			sock.setReceiveBufferSize(4194304);
			sock.setSendBufferSize(4194304);
			sock.connect(new InetSocketAddress(hostname, PORT_NUMBER));
			final OutputStream out = sock.getOutputStream();
			final byte[] outMsg = "SETLDMD         ".getBytes(StandardCharsets.UTF_8);
			outMsg[8] = 0;
			outMsg[9] = 0;
			outMsg[10] = 0;
			outMsg[11] = 0;
			outMsg[12] = 0;
			outMsg[13] = 0;
			outMsg[14] = 0;
			outMsg[15] = 0;
			out.write(outMsg);
			final byte[] data = key.getBytes(StandardCharsets.UTF_8);
			final byte[] length = intToBytes(data.length);
			out.write(length);
			out.write(data);
			final ObjectOutputStream objOut = new ObjectOutputStream(out);
			objOut.writeObject(Utils.convertToHosts(tree, tx));
			objOut.writeObject(ldmd);
			objOut.flush();
			out.flush();
			getConfirmation(sock);
			objOut.close();
			out.close();
			sock.close();
			return true;
		}
		catch (final Exception e)
		{
			try
			{
				if (sock != null)
				{
					sock.close();
				}
			}
			catch (final Exception f)
			{
			}
			return false;
		}
	}

	private static boolean doSendRemoveLDMD(final ArrayList<Object> tree, final String key, final Transaction tx)
	{
		Object obj = tree.get(0);
		while (obj instanceof ArrayList)
		{
			obj = ((ArrayList)obj).get(0);
		}

		Socket sock = null;
		try
		{
			// new MetaData();
			final String hostname = MetaData.getHostNameForNode((Integer)obj, tx);
			// sock = new Socket(hostname,
			// Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
			sock = new Socket();
			sock.setReceiveBufferSize(4194304);
			sock.setSendBufferSize(4194304);
			sock.connect(new InetSocketAddress(hostname, PORT_NUMBER));
			final OutputStream out = sock.getOutputStream();
			final byte[] outMsg = "DELLDMD         ".getBytes(StandardCharsets.UTF_8);
			outMsg[8] = 0;
			outMsg[9] = 0;
			outMsg[10] = 0;
			outMsg[11] = 0;
			outMsg[12] = 0;
			outMsg[13] = 0;
			outMsg[14] = 0;
			outMsg[15] = 0;
			out.write(outMsg);
			final byte[] data = key.getBytes(StandardCharsets.UTF_8);
			final byte[] length = intToBytes(data.length);
			out.write(length);
			out.write(data);
			final ObjectOutputStream objOut = new ObjectOutputStream(out);
			objOut.writeObject(Utils.convertToHosts(tree, tx));
			objOut.flush();
			out.flush();
			getConfirmation(sock);
			objOut.close();
			out.close();
			sock.close();
			return true;
		}
		catch (final Exception e)
		{
			try
			{
				if (sock != null)
				{
					sock.close();
				}
			}
			catch (final Exception f)
			{
			}
			return false;
		}
	}

	private static void getConfirmation(final Socket sock) throws Exception
	{
		final InputStream in = sock.getInputStream();
		final byte[] inMsg = new byte[2];

		int count = 0;
		while (count < 2)
		{
			try
			{
				final int temp = in.read(inMsg, count, 2 - count);
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

		final String inStr = new String(inMsg, StandardCharsets.UTF_8);
		if (!inStr.equals("OK"))
		{
			in.close();
			throw new Exception();
		}

		try
		{
			in.close();
		}
		catch (final Exception e)
		{
		}
	}

	private static byte[] intToBytes(final int val)
	{
		final byte[] buff = new byte[4];
		buff[0] = (byte)(val >> 24);
		buff[1] = (byte)((val & 0x00FF0000) >> 16);
		buff[2] = (byte)((val & 0x0000FF00) >> 8);
		buff[3] = (byte)((val & 0x000000FF));
		return buff;
	}

	private static byte[] longToBytes(final long val)
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

	private static ArrayList<Object> makeTree(final ArrayList<Integer> nodes)
	{
		final int max = MAX_NEIGHBOR_NODES;
		if (nodes.size() <= max)
		{
			final ArrayList<Object> retval = new ArrayList<Object>(nodes);
			return retval;
		}

		final ArrayList<Object> retval = new ArrayList<Object>();
		int i = 0;
		while (i < max)
		{
			retval.add(nodes.get(i));
			i++;
		}

		final int remaining = nodes.size() - i;
		final int perNode = remaining / max + 1;

		int j = 0;
		final int size = nodes.size();
		while (i < size)
		{
			final int first = (Integer)retval.get(j);
			retval.remove(j);
			final ArrayList<Integer> list = new ArrayList<Integer>(perNode + 1);
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
			final ArrayList<Integer> list = (ArrayList<Integer>)retval.remove(i);
			retval.add(i, makeTree(list));
			i++;
		}

		return retval;
	}

	private static int numDevicesPerNode()
	{
		final String dirs = DATA_DIRS;
		final FastStringTokenizer tokens = new FastStringTokenizer(dirs, ",", false);
		return tokens.allTokens().length;
	}

	private static final byte[] rsToBytes(final List<ArrayList<Object>> rows) throws Exception
	{
		final ByteBuffer[] results = new ByteBuffer[rows.size()];
		int rIndex = 0;
		final ArrayList<byte[]> bytes = new ArrayList<byte[]>();
		for (final ArrayList<Object> val : rows)
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
					size += 4;
				}
				else if (o instanceof String)
				{
					header[i] = (byte)4;
					final byte[] b = ((String)o).getBytes(StandardCharsets.UTF_8);
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
						final Exception e = new Exception("Non-zero size ArrayList in toBytes()");
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
					// if ((Long)o < 0)
					// {
					// HRDBMSWorker.logger.debug("Negative long value in
					// rsToBytes: "
					// + o);
					// }
				}
				else if (retval[i] == 1)
				{
					retvalBB.putInt((Integer)o);
					// if ((Integer)o < 0)
					// {
					// HRDBMSWorker.logger.debug("Negative int value in
					// rsToBytes: "
					// + o);
					// }
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
					final byte[] temp = bytes.get(bOff++);
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

			results[rIndex++] = retvalBB;
		}

		int count = 0;
		for (final ByteBuffer bb : results)
		{
			count += bb.capacity();
		}
		final byte[] retval = new byte[count + 4];
		final ByteBuffer temp = ByteBuffer.allocate(4);
		temp.asIntBuffer().put(results.length);
		System.arraycopy(temp.array(), 0, retval, 0, 4);
		int retvalPos = 4;
		for (final ByteBuffer bb : results)
		{
			final byte[] ba = bb.array();
			System.arraycopy(ba, 0, retval, retvalPos, ba.length);
			retvalPos += ba.length;
		}

		return retval;
	}

	private static boolean sendLDMD(final ArrayList<Object> tree, final String key, final LoadMetaData ldmd, final Transaction tx)
	{
		boolean allOK = true;
		final ArrayList<SendLDMDThread> threads = new ArrayList<SendLDMDThread>();
		for (final Object o : tree)
		{
			if (o instanceof Integer)
			{
				final ArrayList<Object> list = new ArrayList<Object>(1);
				list.add(o);
				final SendLDMDThread thread = new SendLDMDThread(list, key, ldmd, tx);
				threads.add(thread);
			}
			else
			{
				final SendLDMDThread thread = new SendLDMDThread((ArrayList<Object>)o, key, ldmd, tx);
				threads.add(thread);
			}
		}

		for (final SendLDMDThread thread : threads)
		{
			thread.start();
		}

		for (final SendLDMDThread thread : threads)
		{
			while (true)
			{
				try
				{
					thread.join();
					break;
				}
				catch (final InterruptedException e)
				{
				}
			}
			final boolean ok = thread.getOK();
			if (!ok)
			{
				allOK = false;
			}
		}

		return allOK;
	}

	private static boolean sendRemoveLDMD(final ArrayList<Object> tree, final String key, final Transaction tx)
	{
		boolean allOK = true;
		final ArrayList<SendRemoveLDMDThread> threads = new ArrayList<SendRemoveLDMDThread>();
		for (final Object o : tree)
		{
			if (o instanceof Integer)
			{
				final ArrayList<Object> list = new ArrayList<Object>(1);
				list.add(o);
				final SendRemoveLDMDThread thread = new SendRemoveLDMDThread(list, key, tx);
				threads.add(thread);
			}
			else
			{
				final SendRemoveLDMDThread thread = new SendRemoveLDMDThread((ArrayList<Object>)o, key, tx);
				threads.add(thread);
			}
		}

		for (final SendRemoveLDMDThread thread : threads)
		{
			thread.start();
		}

		for (final SendRemoveLDMDThread thread : threads)
		{
			while (true)
			{
				try
				{
					thread.join();
					break;
				}
				catch (final InterruptedException e)
				{
				}
			}
			final boolean ok = thread.getOK();
			if (!ok)
			{
				allOK = false;
			}
		}

		return allOK;
	}

	private static byte[] stringToBytes(final String string)
	{
		byte[] data = null;
		try
		{
			data = string.getBytes(StandardCharsets.UTF_8);
		}
		catch (final Exception e)
		{
		}
		final byte[] len = intToBytes(data.length);
		final byte[] retval = new byte[data.length + len.length];
		System.arraycopy(len, 0, retval, 0, len.length);
		System.arraycopy(data, 0, retval, len.length, data.length);
		return retval;
	}

	@Override
	public void add(final Operator op) throws Exception
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
	public Object next(final Operator op) throws Exception
	{
		while (!done)
		{
			LockSupport.parkNanos(500);
		}

		if (num.get() == Long.MIN_VALUE)
		{
			throw new Exception("An error occured during a load operation");
		}

		if (num.get() < 0)
		{
			return new DataEndMarker();
		}

		final long retval = num.get();
		num.set(-1);
		return new Integer((int)retval);
	}

	@Override
	public void nextAll(final Operator op) throws Exception
	{
		num.set(-1);
	}

	@Override
	public long numRecsReceived()
	{
		return 0;
	}

	@Override
	public Operator parent()
	{
		return parent;
	}

	@Override
	public boolean receivedDEM()
	{
		return false;
	}

	@Override
	public void registerParent(final Operator op) throws Exception
	{
		throw new Exception("LoadOperator does not support parents");
	}

	@Override
	public void removeChild(final Operator op)
	{

	}

	@Override
	public void removeParent(final Operator op)
	{
		parent = null;
	}

	@Override
	public void reset() throws Exception
	{
		throw new Exception("LoadOperator does not support reset()");
	}

	@Override
	public void serialize(final OutputStream out, final IdentityHashMap<Object, Long> prev) throws Exception
	{
		throw new Exception("Tried to call serialize on load operator");
	}

	@Override
	public void setChildPos(final int pos)
	{
	}

	@Override
	public void setNode(final int node)
	{
		this.node = node;
	}

	@Override
	public void setPlan(final Plan plan)
	{
		this.plan = plan;
	}

	public void setTransaction(final Transaction tx)
	{
		this.tx = tx;
	}

	@Override
	public void start() throws Exception
	{
		lock = new ScalableStampedRWLock();
		waitTill = new ConcurrentHashMap<Pair, AtomicInteger>(64 * 16 * 1024, 0.75f, 64);
		waitThreads = new ArrayList<FlushThread>();
		if (replace)
		{
			final MassDeleteOperator delete = new MassDeleteOperator(schema, table, meta, false);
			delete.setPlan(plan);
			delete.setTransaction(tx);
			delete.start();
			delete.next(this);
			delete.close();
		}

		// new MetaData();
		cols2Pos = MetaData.getCols2PosForTable(schema, table, tx);
		pos2Col = MetaData.cols2PosFlip(cols2Pos);
		// new MetaData();
		cols2Types = MetaData.getCols2TypesForTable(schema, table, tx);
		final int type = MetaData.getTypeForTable(schema, table, tx);

		for (final String col : pos2Col.values())
		{
			types2.add(cols2Types.get(col));
		}

		final ArrayList<String> indexes = MetaData.getIndexFileNamesForTable(schema, table, tx);
		final ArrayList<String> indexNames = new ArrayList<String>();
		for (final String s : indexes)
		{
			final int start = s.indexOf('.') + 1;
			final int end = s.indexOf('.', start);
			indexNames.add(s.substring(start, end));
		}
		// DEBUG
		// if (indexes.size() == 0)
		// {
		// Exception e = new Exception();
		// HRDBMSWorker.logger.debug("No indexes found", e);
		// }
		// DEBUG
		final ArrayList<ArrayList<String>> keys = MetaData.getKeys(indexes, tx);
		// DEBUG
		// HRDBMSWorker.logger.debug("Keys = " + keys);
		// DEBUG
		final ArrayList<ArrayList<String>> types = MetaData.getTypes(indexes, tx);
		// DEBUG
		// HRDBMSWorker.logger.debug("Types = " + types);
		// DEBUG
		final ArrayList<ArrayList<Boolean>> orders = MetaData.getOrders(indexes, tx);
		// DEBUG
		// HRDBMSWorker.logger.debug("Orders = " + orders);
		// DEBUG

		final HashMap<Integer, Integer> pos2Length = new HashMap<Integer, Integer>();
		for (final Map.Entry entry : cols2Types.entrySet())
		{
			if (entry.getValue().equals("CHAR"))
			{
				final int length = MetaData.getLengthForCharCol(schema, table, (String)entry.getKey(), tx);
				pos2Length.put(cols2Pos.get(entry.getKey()), length);
			}
		}

		if (glob.startsWith("hdfs://"))
		{
			doHDFS(schema, table, tx, pos2Col, cols2Types, pos2Length, keys, types, orders, indexes, type);
			MetaData.cluster(schema, table, tx, pos2Col, cols2Types, type);
			return;
		}

		// figure out what files to read from
		final ArrayList<Path> files = new ArrayList<Path>();
		final PathMatcher matcher = FileSystems.getDefault().getPathMatcher("glob:" + glob);
		int a = 0;
		int b = 0;
		while (a < glob.length())
		{
			if (glob.charAt(a) == '/')
			{
				b = a;
			}

			if (glob.charAt(a) == '*')
			{
				break;
			}

			a++;
		}

		final String startingPath = glob.substring(0, b + 1);
		final Set<FileVisitOption> options = new HashSet<FileVisitOption>();
		final HashSet<String> dirs = new HashSet<String>();
		options.add(FileVisitOption.FOLLOW_LINKS);
		HRDBMSWorker.logger.debug("Starting search with directory: " + startingPath);
		Files.walkFileTree(Paths.get(startingPath), options, Integer.MAX_VALUE, new SimpleFileVisitor<Path>() {
			@Override
			public FileVisitResult postVisitDirectory(final Path file, final IOException exc) throws IOException
			{
				return FileVisitResult.CONTINUE;
			}

			@Override
			public FileVisitResult visitFile(final Path file, final java.nio.file.attribute.BasicFileAttributes attrs) throws IOException
			{
				try
				{
					final String dir = file.getParent().toString();
					if (!dirs.contains(dir))
					{
						dirs.add(dir);
						HRDBMSWorker.logger.debug("New directory visited: " + dir);
					}
					if (matcher.matches(file))
					{
						files.add(file);
					}
					return FileVisitResult.CONTINUE;
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.debug("", e);
					return FileVisitResult.CONTINUE;
				}
			}

			@Override
			public FileVisitResult visitFileFailed(final Path file, final IOException exc) throws IOException
			{
				return FileVisitResult.CONTINUE;
			}
		});

		final ArrayList<ReadThread> threads = new ArrayList<ReadThread>();
		final PartitionMetaData spmd = new MetaData().getPartMeta(schema, table, tx);
		if (files.size() == 0)
		{
			throw new Exception("Load input files were not found!");
		}
		HRDBMSWorker.logger.debug("Going to load from: ");
		int debug = 1;
		for (final Path path : files)
		{
			HRDBMSWorker.logger.debug(debug + ") " + path);
			debug++;
			threads.add(new ReadThread(path.toFile(), pos2Length, indexes, cols2Pos, cols2Types, pos2Col, spmd, keys, types, orders, type));
		}

		for (final ReadThread thread : threads)
		{
			thread.start();
		}

		boolean allOK = true;
		for (final ReadThread thread : threads)
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
				catch (final InterruptedException e)
				{
				}
			}
		}

		if (!allOK)
		{
			num.set(Long.MIN_VALUE);
		}

		MetaData.cluster(schema, table, tx, pos2Col, cols2Types, type);

		for (final String index : indexNames)
		{
			MetaData.populateIndex(schema, index, table, tx, cols2Pos);
		}

		done = true;
	}

	@Override
	public String toString()
	{
		return "InsertOperator";
	}

	private void doHDFS(final String schema, final String table, final Transaction tx, final TreeMap<Integer, String> pos2Col, final HashMap<String, String> cols2Types, final HashMap<Integer, Integer> pos2Length, final ArrayList<ArrayList<String>> keys, final ArrayList<ArrayList<String>> types, final ArrayList<ArrayList<Boolean>> orders, final ArrayList<String> indexes, final int type)
	{
		try
		{
			// build LoadMetaData
			final MetaData md = new MetaData();
			final PartitionMetaData pmd = md.getPartMeta(schema, table, tx);
			final ArrayList<Integer> coordNodes = MetaData.getCoordNodes();
			final ArrayList<Integer> workerNodes = MetaData.getWorkerNodes();
			final LoadMetaData ldmd = new LoadMetaData(workerNodes.size(), delimiter, pos2Col, cols2Types, pos2Length, pmd, workerNodes, coordNodes, tx.number(), keys, types, orders, indexes, type);
			final String key = schema + "." + table;
			// send LoadMetaData everywhere
			ArrayList<Object> tree = makeTree(workerNodes);
			if (!sendLDMD(tree, key, ldmd, tx))
			{
				HRDBMSWorker.logger.debug("Load MetaData returned an error");
				HRDBMSWorker.logger.debug("Sending DELLDMD");
				tree = makeTree(workerNodes);
				sendRemoveLDMD(tree, key, tx);
				num.set(Long.MIN_VALUE);
				done = true;
				return;
			}
			// build and start hadoop job
			try
			{
				HRDBMSWorker.logger.debug("Load MetaData was successful");
				final Configuration conf = new Configuration();
				conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
				conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
				conf.addResource(new org.apache.hadoop.fs.Path("/usr/local/hadoop-2.5.1/etc/hadoop/core-site.xml"));
				conf.addResource(new org.apache.hadoop.fs.Path("/usr/local/hadoop-2.5.1/etc/hadoop/hdfs-site.xml"));
				conf.addResource(new org.apache.hadoop.fs.Path("/usr/local/hadoop-2.5.1/etc/hadoop/yarn-site.xml"));
				conf.addResource(new org.apache.hadoop.fs.Path("/usr/local/hadoop-2.5.1/etc/hadoop/mapreduce-site.xml"));
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
				final Job job = new Job(conf);
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
				final String glob2 = glob.substring(6);
				FileInputFormat.addInputPath(job, new org.apache.hadoop.fs.Path(glob2));
				HRDBMSWorker.logger.debug("Submitting MR job");
				final boolean allOK = job.waitForCompletion(true);
				HRDBMSWorker.logger.debug("Sending DELLDMD");
				tree = makeTree(workerNodes);
				sendRemoveLDMD(tree, key, tx);
				if (!allOK)
				{
					HRDBMSWorker.logger.debug("MR returned an error");
					num.set(Long.MIN_VALUE);
				}
				else
				{
					HRDBMSWorker.logger.debug("MR was successful");
					num.set(job.getCounters().findCounter("org.apache.hadoop.mapred.Task$Counter", "MAP_INPUT_RECORDS").getValue());
					final ArrayList<String> indexNames = new ArrayList<String>();
					for (final String s : indexes)
					{
						final int start = s.indexOf('.') + 1;
						final int end = s.indexOf('.', start);
						indexNames.add(s.substring(start, end));
					}
					for (final String index : indexNames)
					{
						MetaData.populateIndex(schema, index, table, tx, cols2Pos);
					}
					if (num.get() == Long.MIN_VALUE)
					{
						num.getAndIncrement();
					}
				}

				done = true;
			}
			catch (final Throwable f)
			{
				HRDBMSWorker.logger.debug("An exception occurred while building and submitting the MR job", f);
				tree = makeTree(workerNodes);
				sendRemoveLDMD(tree, key, tx);
				num.set(Long.MIN_VALUE);
				done = true;
				return;
			}
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.debug("An exception occurred prior to building the MR job", e);
			num.set(Long.MIN_VALUE);
			done = true;
		}
	}

	private FlushMasterThread flush(final ArrayList<String> indexes, final PartitionMetaData spmd, final ArrayList<ArrayList<String>> keys, final ArrayList<ArrayList<String>> types, final ArrayList<ArrayList<Boolean>> orders, final boolean force, final TreeMap<Integer, String> pos2Col, final HashMap<String, String> cols2Types, final int type) throws Exception
	{
		final FlushMasterThread master = new FlushMasterThread(indexes, spmd, keys, types, orders, true, pos2Col, cols2Types, type);
		master.start();
		return master;
	}

	private FlushMasterThread flush(final ArrayList<String> indexes, final PartitionMetaData spmd, final ArrayList<ArrayList<String>> keys, final ArrayList<ArrayList<String>> types, final ArrayList<ArrayList<Boolean>> orders, final TreeMap<Integer, String> pos2Col, final HashMap<String, String> cols2Types, final int type) throws Exception
	{
		final FlushMasterThread master = new FlushMasterThread(indexes, spmd, keys, types, orders, pos2Col, cols2Types, type);
		master.start();
		return master;
	}

	private class FlushMasterThread extends HRDBMSThread
	{
		private final ArrayList<String> indexes;
		private boolean ok;
		private final PartitionMetaData spmd;
		private final ArrayList<ArrayList<String>> keys;
		private final ArrayList<ArrayList<String>> types;
		private final ArrayList<ArrayList<Boolean>> orders;
		private boolean force = false;
		private final TreeMap<Integer, String> pos2Col;
		private final HashMap<String, String> cols2Types;
		private final int type;

		public FlushMasterThread(final ArrayList<String> indexes, final PartitionMetaData spmd, final ArrayList<ArrayList<String>> keys, final ArrayList<ArrayList<String>> types, final ArrayList<ArrayList<Boolean>> orders, final boolean force, final TreeMap<Integer, String> pos2Col, final HashMap<String, String> cols2Types, final int type)
		{
			this.indexes = indexes;
			this.spmd = spmd;
			this.keys = keys;
			this.types = types;
			this.orders = orders;
			this.force = force;
			this.pos2Col = pos2Col;
			this.cols2Types = cols2Types;
			this.type = type;
		}

		public FlushMasterThread(final ArrayList<String> indexes, final PartitionMetaData spmd, final ArrayList<ArrayList<String>> keys, final ArrayList<ArrayList<String>> types, final ArrayList<ArrayList<Boolean>> orders, final TreeMap<Integer, String> pos2Col, final HashMap<String, String> cols2Types, final int type)
		{
			this.indexes = indexes;
			this.spmd = spmd;
			this.keys = keys;
			this.types = types;
			this.orders = orders;
			this.pos2Col = pos2Col;
			this.cols2Types = cols2Types;
			this.type = type;
		}

		public boolean getOK()
		{
			return ok;
		}

		@Override
		public void run()
		{
			ok = true;
			if (!force && map.totalSize() <= MAX_BATCH)
			{
				return;
			}

			ArrayList<FlushThread> threads = null;

			{
				lock.writeLock().lock();
				{
					if (!force && map.totalSize() <= MAX_BATCH)
					{
						lock.writeLock().unlock();
						return;
					}

					threads = new ArrayList<FlushThread>();
					for (final Object o : map.getKeySet())
					{
						final long key = (Long)o;
						final List<ArrayList<Object>> list = map.get(key);
						threads.add(new FlushThread(list, indexes, key, cols2Pos, spmd, keys, types, orders, pos2Col, cols2Types, type));
					}

					map.clear();

					final ArrayList<FlushThread> temp = new ArrayList<FlushThread>();
					for (final FlushThread thread : threads)
					{
						final AtomicInteger ai = waitTill.get(new Pair(thread.getNode(), thread.getDevice()));

						if (ai != null)
						{
							final int newCount = ai.incrementAndGet();
							if (newCount > 2)
							{
								ai.decrementAndGet();
								temp.add(thread);
							}
							else
							{
								thread.start();
							}
						}
						else
						{
							waitTill.put(new Pair(thread.getNode(), thread.getDevice()), new AtomicInteger(1));
							thread.start();
						}
					}

					ArrayList<FlushThread> clone = new ArrayList<FlushThread>();
					for (final FlushThread thread : waitThreads)
					{
						final AtomicInteger ai = waitTill.get(new Pair(thread.getNode(), thread.getDevice()));

						if (ai != null)
						{
							final int newCount = ai.incrementAndGet();
							if (newCount > 2)
							{
								ai.decrementAndGet();
								clone.add(thread);
							}
							else
							{
								thread.start();
							}
						}
						else
						{
							waitTill.put(new Pair(thread.getNode(), thread.getDevice()), new AtomicInteger(1));
							thread.start();
						}
					}

					waitThreads = clone;
					waitThreads.addAll(temp);

					while (waitThreads.size() > MAX_QUEUED)
					{
						try
						{
							Thread.sleep(1);
						}
						catch (final InterruptedException e)
						{
						}

						// HRDBMSWorker.logger.debug("# of waiting threads = " +
						// waitThreads.size()); //DEBUG

						clone = new ArrayList<FlushThread>();
						for (final FlushThread thread : waitThreads)
						{
							final AtomicInteger ai = waitTill.get(new Pair(thread.getNode(), thread.getDevice()));

							if (ai != null)
							{
								final int newCount = ai.incrementAndGet();
								if (newCount > 2)
								{
									ai.decrementAndGet();
									clone.add(thread);
								}
								else
								{
									thread.start();
								}
							}
							else
							{
								waitTill.put(new Pair(thread.getNode(), thread.getDevice()), new AtomicInteger(1));
								thread.start();
							}
						}

						waitThreads = clone;
					}
				}
				lock.writeLock().unlock();
			}

			synchronized (fThreads)
			{
				fThreads.addAll(threads);
			}
		}
	}

	private class FlushThread extends HRDBMSThread
	{
		private List<ArrayList<Object>> list;
		private ArrayList<String> indexes;
		private boolean ok = true;
		private final long key;
		private HashMap<String, Integer> cols2Pos;
		private ArrayList<ArrayList<String>> keys;
		private ArrayList<ArrayList<String>> types;
		private ArrayList<ArrayList<Boolean>> orders;
		private final TreeMap<Integer, String> pos2Col;
		private final HashMap<String, String> cols2Types;
		private final int type;

		public FlushThread(final List<ArrayList<Object>> list, final ArrayList<String> indexes, final long key, final HashMap<String, Integer> cols2Pos, final PartitionMetaData spmd, final ArrayList<ArrayList<String>> keys, final ArrayList<ArrayList<String>> types, final ArrayList<ArrayList<Boolean>> orders, final TreeMap<Integer, String> pos2Col, final HashMap<String, String> cols2Types, final int type)
		{
			this.list = list;
			this.indexes = indexes;
			this.key = key;
			this.cols2Pos = cols2Pos;
			this.keys = keys;
			this.types = types;
			this.orders = orders;
			this.pos2Col = pos2Col;
			this.cols2Types = cols2Types;
			this.type = type;
		}

		public int getDevice()
		{
			return (int)(key & 0xFFFFFFFFL);
		}

		public int getNode()
		{
			return (int)(key >> 32);
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
				final int node = (int)(key >> 32);
				final int device = (int)(key & 0x00000000FFFFFFFF);
				// new MetaData();
				final String hostname = MetaData.getHostNameForNode(node, tx);
				// sock = new Socket(hostname,
				// Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
				int i = 0;
				while (true)
				{
					try
					{
						sock = new Socket();
						sock.setReceiveBufferSize(4194304);
						sock.setSendBufferSize(4194304);
						sock.connect(new InetSocketAddress(hostname, PORT_NUMBER));
						break;
					}
					catch (final ConnectException e)
					{
						i++;
						if (i == 60)
						{
							sock.close();
							throw e;
						}
						Thread.sleep(5000);
					}
				}
				final OutputStream out = sock.getOutputStream();
				final byte[] outMsg = "LOAD            ".getBytes(StandardCharsets.UTF_8);
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
				out.write(intToBytes(type));
				out.write(rsToBytes(list));
				list = null;
				final ObjectOutputStream objOut = new ObjectOutputStream(out);
				objOut.writeObject(indexes);
				objOut.writeObject(keys);
				objOut.writeObject(types);
				objOut.writeObject(orders);
				objOut.writeObject(cols2Pos);
				objOut.writeObject(pos2Col);
				objOut.writeObject(cols2Types);
				objOut.flush();
				out.flush();
				getConfirmation(sock);
				waitTill.get(new Pair(getNode(), getDevice())).decrementAndGet();

				objOut.close();
				sock.close();
				indexes = null;
				cols2Pos = null;
				keys = null;
				types = null;
				orders = null;
			}
			catch (final Exception e)
			{
				try
				{
					if (sock != null)
					{
						sock.close();
					}
				}
				catch (final Exception f)
				{
				}
				ok = false;
				HRDBMSWorker.logger.debug("", e);
			}
		}

		private void getConfirmation(final Socket sock) throws Exception
		{
			final InputStream in = sock.getInputStream();
			final byte[] inMsg = new byte[2];

			int count = 0;
			while (count < 2)
			{
				try
				{
					final int temp = in.read(inMsg, count, 2 - count);
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

			final String inStr = new String(inMsg, StandardCharsets.UTF_8);
			if (!inStr.equals("OK"))
			{
				in.close();
				throw new Exception();
			}

			try
			{
				in.close();
			}
			catch (final Exception e)
			{
			}
		}
	}

	private class Pair
	{
		private final int node;
		private final int device;

		public Pair(final int node, final int device)
		{
			this.node = node;
			this.device = device;
		}

		@Override
		public boolean equals(final Object o)
		{
			final Pair rhs = (Pair)o;
			return node == rhs.node && device == rhs.device;
		}

		@Override
		public int hashCode()
		{
			int val = 31;
			val = val * 23 + node;
			val = val * 23 + device;
			return val;
		}
	}

	private class ReadThread extends HRDBMSThread
	{
		private final File file;
		private final HashMap<Integer, Integer> pos2Length;
		private final ArrayList<String> indexes;
		private boolean ok = true;
		private volatile long num = 0;
		private final HashMap<String, Integer> cols2Pos;
		PartitionMetaData spmd;
		private final ArrayList<ArrayList<String>> keys;
		private final ArrayList<ArrayList<String>> types;
		private final ArrayList<ArrayList<Boolean>> orders;
		private final TreeMap<Integer, String> pos2Col;
		private final HashMap<String, String> cols2Types;
		private final int type;

		public ReadThread(final File file, final HashMap<Integer, Integer> pos2Length, final ArrayList<String> indexes, final HashMap<String, Integer> cols2Pos, final HashMap<String, String> cols2Types, final TreeMap<Integer, String> pos2Col, final PartitionMetaData spmd, final ArrayList<ArrayList<String>> keys, final ArrayList<ArrayList<String>> types, final ArrayList<ArrayList<Boolean>> orders, final int type)
		{
			this.file = file;
			this.pos2Length = pos2Length;
			this.indexes = indexes;
			this.cols2Pos = cols2Pos;
			this.spmd = spmd;
			this.keys = keys;
			this.types = types;
			this.orders = orders;
			this.pos2Col = pos2Col;
			this.cols2Types = cols2Types;
			this.type = type;
		}

		public long getNum()
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
				final BufferedReader in = new BufferedReader(new FileReader(file), 64 * 1024);
				Object o = next(in);
				final PartitionMetaData pmeta = new PartitionMetaData(schema, table, tx);
				final int numNodes = MetaData.numWorkerNodes;
				while (!(o instanceof DataEndMarker))
				{
					final ArrayList<Object> row = (ArrayList<Object>)o;
					num++;
					for (final Map.Entry entry : pos2Length.entrySet())
					{
						if (((String)row.get((Integer)entry.getKey())).length() > (Integer)entry.getValue())
						{
							ok = false;
							return;
						}
					}
					final ArrayList<Integer> nodes = MetaData.determineNode(schema, table, row, tx, pmeta, cols2Pos, numNodes);
					final int device = MetaData.determineDevice(row, pmeta, cols2Pos);

					lock.readLock().lock();
					for (final Integer node : nodes)
					{
						plan.addNode(node);
						final long key = (((long)node) << 32) + device;
						map.multiPut(key, row);
					}
					lock.readLock().unlock();
					if (map.totalSize() > MAX_BATCH)
					{
						if (master != null)
						{
							master.join();
							if (!master.getOK())
							{
								throw new Exception("Error flushing inserts");
							}
						}

						master = flush(indexes, spmd, keys, types, orders, pos2Col, cols2Types, type);
					}

					o = next(in);
				}

				if (master != null)
				{
					master.join();
					if (!master.getOK())
					{
						throw new Exception("Error flushing inserts");
					}
				}

				if (map.totalSize() > 0)
				{
					master = flush(indexes, spmd, keys, types, orders, true, pos2Col, cols2Types, type);
					master.join();
					if (!master.getOK())
					{
						throw new Exception("Error flushing inserts");
					}
				}

				int count;
				lock.readLock().lock();
				count = waitThreads.size();
				lock.readLock().unlock();

				while (count > 0 && map.totalSize() == 0)
				{
					master = flush(indexes, spmd, keys, types, orders, true, pos2Col, cols2Types, type);
					master.join();
					if (!master.getOK())
					{
						throw new Exception("Error flushing inserts");
					}

					lock.readLock().lock();
					count = waitThreads.size();
					lock.readLock().unlock();
				}

				synchronized (fThreads)
				{
					for (final FlushThread thread : fThreads)
					{
						if (thread.started())
						{
							thread.join();
							if (!thread.getOK())
							{
								throw new Exception("Error flushing inserts");
							}
						}
					}
				}
			}
			catch (final Exception e)
			{
				ok = false;
				HRDBMSWorker.logger.debug("", e);
			}
		}

		private Object next(final BufferedReader in) throws Exception
		{
			final String line = in.readLine();
			if (line == null)
			{
				return new DataEndMarker();
			}

			final ArrayList<Object> row = new ArrayList<Object>();
			final FastStringTokenizer tokens = new FastStringTokenizer(line, delimiter, false);
			int i = 0;
			while (tokens.hasMoreTokens())
			{
				final String token = tokens.nextToken();
				final String type = types2.get(i);
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

		public SendLDMDThread(final ArrayList<Object> tree, final String key, final LoadMetaData ldmd, final Transaction tx)
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

		public SendRemoveLDMDThread(final ArrayList<Object> tree, final String key, final Transaction tx)
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
