package com.exascale.optimizer.load;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.Socket;
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
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import com.exascale.optimizer.*;
import org.antlr.v4.runtime.misc.Pair;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.misc.DataEndMarker;
import com.exascale.misc.FastStringTokenizer;
import com.exascale.misc.LOMultiHashMap;
import com.exascale.misc.ScalableStampedRWLock;
import com.exascale.misc.Utils;
import com.exascale.tables.Plan;
import com.exascale.tables.Transaction;
import com.exascale.threads.HRDBMSThread;

/** Note that all the Thread classes in this package were once inner classes of LoadOperator.  They were broken out
 *  into top-level classes to improve maintainability.  But there's still weird interplay between all the classes. */
public final class LoadOperator implements Operator, Serializable
{
	static String DATA_DIRS = HRDBMSWorker.getHParms().getProperty("data_directories");
	static long MAX_QUEUED = Long.parseLong(HRDBMSWorker.getHParms().getProperty("max_queued_load_flush_threads"));
	static int PORT_NUMBER = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number"));
	static int MAX_BATCH = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("max_batch"));
	private final MetaData meta;
	private HashMap<String, String> cols2Types;
	private HashMap<String, Integer> cols2Pos;
	private TreeMap<Integer, String> pos2Col;
	private Operator parent;
	private int node;
	private transient Plan plan;
	private final String schema, table, externalTable;
	private final AtomicLong num = new AtomicLong(0);
	private volatile boolean done = false;
	private final LOMultiHashMap map = new LOMultiHashMap<Long, ArrayList<Object>>();
	private Transaction tx;
	private final boolean replace;
	private final String delimiter;
	private final String glob;
	private final ArrayList<FlushThread> fThreads = new ArrayList<FlushThread>();
	private transient ConcurrentHashMap<Pair, AtomicInteger> waitTill;
	private volatile transient ArrayList<FlushThread> waitThreads;
	private transient ScalableStampedRWLock lock;

	public LoadOperator(final String schema, final String table, final boolean replace, final String delimiter, final String glob, final MetaData meta, final String externalTable)
	{
		this.schema = schema;
		this.table = table;
		this.replace = replace;
		this.delimiter = delimiter;
		this.glob = glob;
		this.meta = meta;
		this.externalTable = externalTable;
	}

	/** Sends load metadata to worker */
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
			final String hostname = MetaData.getHostNameForNode((Integer)obj, tx);
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
			final byte[] length = OperatorUtils.intToBytes(data.length);
			out.write(length);
			out.write(data);
			final ObjectOutputStream objOut = new ObjectOutputStream(out);
			objOut.writeObject(Utils.convertToHosts(tree, tx));
			objOut.writeObject(ldmd);
			objOut.flush();
			out.flush();
			OperatorUtils.getConfirmation(sock);
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
			final String hostname = MetaData.getHostNameForNode((Integer)obj, tx);
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
			final byte[] length = OperatorUtils.intToBytes(data.length);
			out.write(length);
			out.write(data);
			final ObjectOutputStream objOut = new ObjectOutputStream(out);
			objOut.writeObject(Utils.convertToHosts(tree, tx));
			objOut.flush();
			out.flush();
			OperatorUtils.getConfirmation(sock);
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

	private static int numDevicesPerNode()
	{
		final String dirs = DATA_DIRS;
		final FastStringTokenizer tokens = new FastStringTokenizer(dirs, ",", false);
		return tokens.allTokens().length;
	}

	/** Sends load metadata to workers */
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
		final LoadOperator retval = new LoadOperator(schema, table, replace, delimiter, glob, meta, externalTable);
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
			throw new Exception("An error occurred during a load operation");
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

		cols2Pos = MetaData.getCols2PosForTable(schema, table, tx);
		pos2Col = MetaData.cols2PosFlip(cols2Pos);
		cols2Types = MetaData.getCols2TypesForTable(schema, table, tx);
		final int type = MetaData.getTypeForTable(schema, table, tx);

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

        final ArrayList<ReadThread> threads = new ArrayList<>();
        final PartitionMetaData spmd = new MetaData().getPartMeta(schema, table, tx);
        if (externalTable != null) {
            threads.add(new ExternalReadThread(this, pos2Length, indexes, spmd, keys, types, orders, type));
        } else {
            // figure out what files to read from
            final ArrayList<Path> files = new ArrayList<Path>();
            final PathMatcher matcher = FileSystems.getDefault().getPathMatcher("glob:" + glob);
            int a = 0;
            int b = 0;
            while (a < glob.length()) {
                if (glob.charAt(a) == '/') {
                    b = a;
                }

                if (glob.charAt(a) == '*') {
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
                public FileVisitResult postVisitDirectory(final Path file, final IOException exc) throws IOException {
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFile(final Path file, final java.nio.file.attribute.BasicFileAttributes attrs) throws IOException {
                    try {
                        final String dir = file.getParent().toString();
                        if (!dirs.contains(dir)) {
                            dirs.add(dir);
                            HRDBMSWorker.logger.debug("New directory visited: " + dir);
                        }
                        if (matcher.matches(file)) {
                            files.add(file);
                        }
                        return FileVisitResult.CONTINUE;
                    } catch (final Exception e) {
                        HRDBMSWorker.logger.debug("", e);
                        return FileVisitResult.CONTINUE;
                    }
                }

                @Override
                public FileVisitResult visitFileFailed(final Path file, final IOException exc) throws IOException {
                    return FileVisitResult.CONTINUE;
                }
            });


            if (files.size() == 0) {
                throw new Exception("Load input files were not found!");
            }
            HRDBMSWorker.logger.debug("Going to load from: ");
            int debug = 1;
            for (final Path path : files) {
                HRDBMSWorker.logger.debug(debug + ") " + path);
                debug++;
                threads.add(new ReadThread(this, path.toFile(), pos2Length, indexes, spmd, keys, types, orders, type));
            }
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
		return "LoadOperator";
	}

	Transaction getTransaction() { return tx; }

	ConcurrentHashMap<Pair, AtomicInteger> getWaitTill() { return waitTill; }

	ScalableStampedRWLock getLock() { return lock; }

	LOMultiHashMap getMap() { return map; }

	ArrayList<FlushThread> getWaitThreads() { return waitThreads; }

	void setWaitThreads(ArrayList<FlushThread> waitThreads) { this.waitThreads = waitThreads;}

	ArrayList<FlushThread> getFlushThreads() { return fThreads; }

	String getDelimiter() { return delimiter; }

	String getExternalTable() { return externalTable; }

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