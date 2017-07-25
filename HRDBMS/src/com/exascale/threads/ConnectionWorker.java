package com.exascale.threads;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.io.Reader;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.Connection;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.LockSupport;
import com.exascale.filesystem.Block;
import com.exascale.filesystem.Page;
import com.exascale.filesystem.RID;
import com.exascale.filesystem.SparseCompressedFileChannel2;
import com.exascale.logging.LogIterator;
import com.exascale.logging.LogRec;
import com.exascale.managers.BufferManager;
import com.exascale.managers.BufferManager.RequestPagesThread;
import com.exascale.managers.FileManager;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.managers.LockManager;
import com.exascale.managers.LogManager;
import com.exascale.managers.ResourceManager;
import com.exascale.managers.XAManager;
import com.exascale.misc.BufferedFileChannel;
import com.exascale.misc.CompressedBitSet;
import com.exascale.misc.DataEndMarker;
import com.exascale.misc.FastStringTokenizer;
import com.exascale.misc.HJOMultiHashMap;
import com.exascale.misc.HParms;
import com.exascale.misc.MultiHashMap;
import com.exascale.misc.MurmurHash;
import com.exascale.misc.MyDate;
import com.exascale.misc.SPSCQueue;
import com.exascale.optimizer.BFCOperator;
import com.exascale.optimizer.ColDef;
import com.exascale.optimizer.DeleteOperator;
import com.exascale.optimizer.Filter;
import com.exascale.optimizer.Index;
import com.exascale.optimizer.Index.IndexRecord;
import com.exascale.optimizer.IndexOperator;
import com.exascale.optimizer.InsertOperator;
import com.exascale.optimizer.load.LoadMetaData;
import com.exascale.optimizer.MetaData;
import com.exascale.optimizer.PartitionMetaData;
import com.exascale.optimizer.NetworkReceiveOperator;
import com.exascale.optimizer.NetworkSendOperator;
import com.exascale.optimizer.Operator;
import com.exascale.optimizer.OperatorUtils;
import com.exascale.optimizer.RIDAndIndexKeys;
import com.exascale.optimizer.RootOperator;
import com.exascale.optimizer.RoutingOperator;
import com.exascale.optimizer.SortOperator;
import com.exascale.optimizer.TableScanOperator;
import com.exascale.optimizer.TableScanOperator.CNFEntry;
import com.exascale.optimizer.UpdateOperator;
import com.exascale.tables.DataType;
import com.exascale.tables.HeaderPage;
import com.exascale.tables.Schema;
import com.exascale.tables.Schema.FieldValue;
import com.exascale.tables.Schema.Row;
import com.exascale.tables.Schema.RowIterator;
import com.exascale.tables.Transaction;
import com.sun.management.OperatingSystemMXBean;

public class ConnectionWorker extends HRDBMSThread
{
	private static ConcurrentHashMap<Integer, Operator> sends;
	private static int PREFETCH_REQUEST_SIZE;
	private static int PAGES_IN_ADVANCE;
	private static ConcurrentHashMap<String, LoadMetaData> ldmds = new ConcurrentHashMap<String, LoadMetaData>(16, 0.75f, 6 * ResourceManager.cpus);
	private static long MAX_PAGES;
	private static final ConcurrentHashMap<Long, Exception> loadExceptions = new ConcurrentHashMap<Long, Exception>(16, 0.75f, 64 * ResourceManager.cpus);
	private static final ConcurrentHashMap<FlushLoadThread, FlushLoadThread> flThreads = new ConcurrentHashMap<FlushLoadThread, FlushLoadThread>(12000000, 0.75f, 64 * ResourceManager.cpus);
	public static final ConcurrentHashMap<String, String> dmlTx = new ConcurrentHashMap<String, String>();
	private static final ConcurrentHashMap<Long, Long> delayedTxs = new ConcurrentHashMap<Long, Long>();
	private static sun.misc.Unsafe unsafe;
	private static long soffset;
	private static Charset scs = StandardCharsets.UTF_8;
	private static int BATCHES_PER_CHECK;
	private static Map<Integer, Integer> disk2BatchCount = new HashMap<Integer, Integer>();
	private static ConcurrentHashMap<String, FileChannel> loadFCs = new ConcurrentHashMap<String, FileChannel>();
	private static ConcurrentHashMap<ConnectionWorker, ConnectionWorker> delayedDML = new ConcurrentHashMap<ConnectionWorker, ConnectionWorker>();
	private static HJOMultiHashMap<ConnectionWorker, XAWorker> delayedWorkers = new HJOMultiHashMap<ConnectionWorker, XAWorker>();;

	static
	{
		sends = new ConcurrentHashMap<Integer, Operator>();
		final HParms hparms = HRDBMSWorker.getHParms();
		PREFETCH_REQUEST_SIZE = Integer.parseInt(hparms.getProperty("prefetch_request_size")); // 80
		PAGES_IN_ADVANCE = Integer.parseInt(hparms.getProperty("pages_in_advance")); // 40
		MAX_PAGES = Integer.parseInt(hparms.getProperty("create_index_batch_size"));
		BATCHES_PER_CHECK = Integer.parseInt(hparms.getProperty("batches_per_check"));
		try
		{
			final Field f = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
			f.setAccessible(true);
			unsafe = (sun.misc.Unsafe)f.get(null);
			final Field fieldToUpdate = String.class.getDeclaredField("value");
			// get unsafe offset to this field
			soffset = unsafe.objectFieldOffset(fieldToUpdate);
		}
		catch (final Exception e)
		{
			unsafe = null;
		}
	}
	private final Socket sock;
	private Transaction tx = null;
	private XAWorker worker;
	private final Random random = new Random();
	// private final CharsetDecoder scd = scs.newDecoder();

	private final CharsetEncoder sce = scs.newEncoder();

	public ConnectionWorker(final Socket sock2)
	{
		this.description = "Connection Worker";
		this.sock = sock2;
	}

	public static boolean isDelayed(final Transaction tx)
	{
		return delayedTxs.containsKey(tx.number());
	}

	public static double parseUptimeResult(final String uptimeCmdResult) throws Exception
	{
		final String load = uptimeCmdResult.substring(uptimeCmdResult.lastIndexOf(',') + 1).trim();
		return Double.parseDouble(load);
	}

	public static String runUptimeCommand(final String crunchifyCmd, final boolean waitForResult) throws Exception
	{
		ProcessBuilder crunchifyProcessBuilder = null;

		crunchifyProcessBuilder = new ProcessBuilder("/bin/bash", "-c", crunchifyCmd);
		crunchifyProcessBuilder.redirectErrorStream(true);
		Writer crunchifyWriter = null;
		try
		{
			final Process process = crunchifyProcessBuilder.start();
			if (waitForResult)
			{
				final InputStream crunchifyStream = process.getInputStream();

				if (crunchifyStream != null)
				{
					crunchifyWriter = new StringWriter();

					final char[] crunchifyBuffer = new char[2048];
					try
					{
						final Reader crunchifyReader = new BufferedReader(new InputStreamReader(crunchifyStream, StandardCharsets.UTF_8));
						int count;
						while ((count = crunchifyReader.read(crunchifyBuffer)) != -1)
						{
							crunchifyWriter.write(crunchifyBuffer, 0, count);
						}
					}
					finally
					{
						crunchifyStream.close();
					}

					crunchifyStream.close();
				}
			}
		}
		catch (final Exception e)
		{
			throw e;
		}
		if (crunchifyWriter == null)
		{
			throw new Exception("CrunchifyWriter is null in ConnectionWorker");
		}
		return crunchifyWriter.toString();
	}

	private static FieldValue[] aloToFieldValues(final List<Object> row)
	{
		final FieldValue[] retval = new FieldValue[row.size()];
		int i = 0;
		for (final Object o : row)
		{
			if (o instanceof Integer)
			{
				retval[i] = new Schema.IntegerFV((Integer)o);
			}
			else if (o instanceof Long)
			{
				retval[i] = new Schema.BigintFV((Long)o);
			}
			else if (o instanceof Double)
			{
				retval[i] = new Schema.DoubleFV((Double)o);
			}
			else if (o instanceof MyDate)
			{
				retval[i] = new Schema.DateFV((MyDate)o);
			}
			else if (o instanceof String)
			{
				retval[i] = new Schema.VarcharFV((String)o);
			}

			i++;
		}

		return retval;
	}

	private static int bytesToInt(final byte[] val)
	{
		final int ret = java.nio.ByteBuffer.wrap(val).getInt();
		return ret;
	}

	private static long bytesToLong(final byte[] val)
	{
		final long ret = java.nio.ByteBuffer.wrap(val).getLong();
		return ret;
	}

	private static void cloneInto(final List<Object> target, final List<Object> source)
	{
		for (final Object o : source)
		{
			target.add(o);
		}
	}

	private static void doMinMax(final Block block, final List<List<Object>> pageSet, final Map<Integer, String> pos2Col, final boolean isV5OrHigher, final boolean isV6OrHigher)
	{
		final int cols = pos2Col.size();
		final int rows = pageSet.size();
		int i = 0;
		while (i < cols)
		{
			final List col = new ArrayList(rows);
			int j = 0;
			while (j < rows)
			{
				col.add(pageSet.get(j++).get(i));
			}

			Collections.sort(col);
			final Object l = col.get(0);
			final Object u = col.get(rows - 1);
			String colName = pos2Col.get(i);

			try
			{
				Filter lower = null;
				Filter upper = null;
				if (l instanceof MyDate)
				{
					lower = new Filter(colName, "L", "DATE('" + l.toString() + "')");
					upper = new Filter(colName, "G", "DATE('" + u.toString() + "')");
				}
				else if (l instanceof String)
				{
					lower = new Filter(colName, "L", "'" + l + "'");
					upper = new Filter(colName, "G", "'" + u + "'");
				}
				else
				{
					lower = new Filter(colName, "L", "" + l);
					upper = new Filter(colName, "G", "" + u);
				}

				HashSet<Map<Filter, Filter>> hshm = new HashSet<Map<Filter, Filter>>();
				HashMap<Filter, Filter> hm = new HashMap<Filter, Filter>();
				hm.put(lower, lower);
				hshm.add(hm);
				if (!isV5OrHigher)
				{
					TableScanOperator.noResults.multiPut(block, hshm);
				}
				else
				{
					BitSet bitSet = null;
					if (isV6OrHigher)
					{
						bitSet = new CompressedBitSet();
					}
					else
					{
						bitSet = new BitSet();
					}
					bitSet.set(block.number());
					final CNFEntry cnfEntry = new CNFEntry(hshm, bitSet);
					MultiHashMap<Integer, CNFEntry> mhm = TableScanOperator.pbpeCache2.get(block.fileName());
					if (mhm == null)
					{
						mhm = new MultiHashMap<Integer, CNFEntry>();
						TableScanOperator.pbpeCache2.put(block.fileName(), mhm);
						mhm = TableScanOperator.pbpeCache2.get(block.fileName());
					}

					if (colName.contains("."))
					{
						colName = colName.substring(colName.indexOf('.') + 1);
						// HRDBMSWorker.logger.debug("Truncated column name");
					}
					final int hash = colName.hashCode();
					// HRDBMSWorker.logger.debug("While creating min/max
					// hashCode for " + colName + " is " + hash);
					final ConcurrentHashMap<CNFEntry, CNFEntry> map = mhm.getMap(hash);
					if (map != null)
					{
						final CNFEntry entry2 = map.get(cnfEntry);
						if (entry2 != null)
						{
							final BitSet bs2 = entry2.getBitSet();
							final BitSet bs = cnfEntry.getBitSet();
							synchronized (bs2)
							{
								if (isV6OrHigher)
								{
									((CompressedBitSet)bs2).or((CompressedBitSet)bs);
								}
								else
								{
									bs2.or(bs);
								}
							}
						}
						else
						{
							mhm.multiPut(hash, cnfEntry);
						}
					}
					else
					{
						mhm.multiPut(hash, cnfEntry);
					}
				}
				hshm = new HashSet<Map<Filter, Filter>>();
				hm = new HashMap<Filter, Filter>();
				hm.put(upper, upper);
				hshm.add(hm);
				if (!isV5OrHigher)
				{
					TableScanOperator.noResults.multiPut(block, hshm);
				}
				else
				{
					BitSet bitSet = null;
					if (isV6OrHigher)
					{
						bitSet = new CompressedBitSet();
					}
					else
					{
						bitSet = new BitSet();
					}
					bitSet.set(block.number());
					final CNFEntry cnfEntry = new CNFEntry(hshm, bitSet);
					final MultiHashMap<Integer, CNFEntry> mhm = TableScanOperator.pbpeCache2.get(block.fileName());
					final int hash = colName.hashCode();
					final ConcurrentHashMap<CNFEntry, CNFEntry> map = mhm.getMap(hash);
					if (map != null)
					{
						final CNFEntry entry2 = map.get(cnfEntry);
						if (entry2 != null)
						{
							final BitSet bs2 = entry2.getBitSet();
							final BitSet bs = cnfEntry.getBitSet();
							synchronized (bs2)
							{
								if (isV6OrHigher)
								{
									((CompressedBitSet)bs2).or((CompressedBitSet)bs);
								}
								else
								{
									bs2.or(bs);
								}
							}
						}
						else
						{
							mhm.multiPut(hash, cnfEntry);
						}
					}
					else
					{
						mhm.multiPut(hash, cnfEntry);
					}
				}
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.debug("", e);
			}

			i++;
		}

		pageSet.clear();
	}

	private static void doReorg(final String schema, final String table, final List<Index> indexes, final Transaction tx, final Map<String, String> cols2Types, final Map<Integer, String> pos2Col, final List<Boolean> uniques) throws Exception
	{
		final String dirList = HRDBMSWorker.getHParms().getProperty("data_directories");
		final FastStringTokenizer tokens = new FastStringTokenizer(dirList, ",", false);
		final String[] dirs = tokens.allTokens();
		Exception e = null;

		// new ArrayList<ReorgThread>();
		boolean allOK = true;
		for (final String dir : dirs)
		{
			// threads.add(new ReorgThread(dir, schema, table, indexes, tx));
			// FIX ME
			final ReorgThread thread = new ReorgThread(dir, schema, table, indexes, tx, cols2Types, pos2Col, uniques, 0);
			thread.start();
			thread.join();
			if (!thread.getOK())
			{
				allOK = false;
				e = thread.getException();
				break;
			}
			tx.releaseLocksAndPins();
		}

		// for (ReorgThread thread : threads)
		// {
		// thread.start();
		// }

		// for (ReorgThread thread : threads)
		// {
		// thread.join();
		// if (!thread.getOK())
		// {
		// allOK = false;
		// e = thread.getException();
		// }
		// }

		if (!allOK)
		{
			throw e;
		}
	}

	private static List<String> getAllNodes(final List<Object> tree, final String remove)
	{
		final List<String> retval = new ArrayList<String>();
		for (final Object o : tree)
		{
			if (o instanceof String)
			{
				if (!((String)o).equals(remove))
				{
					retval.add((String)o);
				}
			}
			else
			{
				// ArrayList<Object>
				retval.addAll(getAllNodes((List<Object>)o, remove));
			}
		}

		return retval;
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
					throw new Exception();
				}
				else
				{
					count += temp;
				}
			}
			catch (final Exception e)
			{
				throw e;
			}
		}

		final String inStr = new String(inMsg, StandardCharsets.UTF_8);
		if (!inStr.equals("OK"))
		{
			HRDBMSWorker.logger.debug("In getConfirmation(), received " + inStr);
			throw new Exception();
		}
	}

	private static String getDataDir(final int from, final int to)
	{
		return to + "," + HRDBMSWorker.getHParms().getProperty("data_directories");
	}

	private static List<String> getDataPaths()
	{
		final String paths = HRDBMSWorker.getHParms().getProperty("data_directories");
		final StringTokenizer tokens = new StringTokenizer(paths, ",", false);
		final List<String> retval = new ArrayList<String>();
		while (tokens.hasMoreTokens())
		{
			String token = tokens.nextToken();
			if (!token.endsWith("/"))
			{
				token += "/";
			}

			retval.add(token);
		}

		return retval;
	}

	private static File[] getDirs(final String list)
	{
		StringTokenizer tokens = new StringTokenizer(list, ",", false);
		int i = 0;
		while (tokens.hasMoreTokens())
		{
			tokens.nextToken();
			i++;
		}
		final File[] dirs = new File[i];
		tokens = new StringTokenizer(list, ",", false);
		i = 0;
		while (tokens.hasMoreTokens())
		{
			dirs[i] = new File(tokens.nextToken());
			i++;
		}

		return dirs;
	}

	private static List<String> getIndexFilesInPath(final String path) throws Exception
	{
		final List<Path> files = new ArrayList<Path>();
		final File dirFile = new File(path);
		final File[] files2 = dirFile.listFiles();
		for (final File f : files2)
		{
			if (f.getName().matches(".*\\..*\\.indx\\..*"))
			{
				files.add(f.toPath());
			}
		}

		final List<String> retval = new ArrayList<String>();
		for (final Path file : files)
		{
			retval.add(file.toAbsolutePath().toString());
		}

		return retval;
	}

	private static List<String> getTableFilesInPath(final String path) throws Exception
	{
		final List<Path> files = new ArrayList<Path>();
		final File dirFile = new File(path);
		final File[] files2 = dirFile.listFiles();
		for (final File f : files2)
		{
			if (f.getName().matches(".*\\..*\\.tbl\\..*"))
			{
				files.add(f.toPath());
			}
		}

		final List<String> retval = new ArrayList<String>();
		for (final Path file : files)
		{
			retval.add(file.toAbsolutePath().toString());
		}

		return retval;
	}

	private static long hash(final Object key) throws Exception
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
				final byte[] data = toBytesForHash((List<Object>)key);
				eHash = MurmurHash.hash64(data, data.length);
			}
			else
			{
				final byte[] data = key.toString().getBytes(StandardCharsets.UTF_8);
				eHash = MurmurHash.hash64(data, data.length);
			}
		}

		return eHash;
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

	private static List<Object> makeTree(final List<String> nodes)
	{
		final int max = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("max_neighbor_nodes"));
		if (nodes.size() <= max)
		{
			final List<Object> retval = new ArrayList<Object>(nodes);
			return retval;
		}

		final List<Object> retval = new ArrayList<Object>();
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
			final String first = (String)retval.get(j);
			retval.remove(j);
			final List<String> list = new ArrayList<String>(perNode + 1);
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

		if (((List<String>)retval.get(0)).size() <= max)
		{
			return retval;
		}

		// more than 2 tier
		i = 0;
		while (i < retval.size())
		{
			final List<String> list = (List<String>)retval.remove(i);
			retval.add(i, makeTree(list));
			i++;
		}

		return retval;
	}

	private static void putMedium(final ByteBuffer bb, final int val)
	{
		bb.put((byte)((val & 0xff0000) >> 16));
		bb.put((byte)((val & 0xff00) >> 8));
		bb.put((byte)(val & 0xff));
	}

	private static ByteBuffer readRawRS(final InputStream in2) throws Exception
	{
		int bbSize = 16 * 1024 * 1024 - 1;
		ByteBuffer bb = ByteBuffer.allocate(bbSize);
		final byte[] numBytes = new byte[4];
		readNonCoord(numBytes, in2);
		final int num = bytesToInt(numBytes);
		int pos = 0;
		// ArrayList<List<Object>> retval = new
		// ArrayList<List<Object>>(num);
		int i = 0;
		while (i < num)
		{
			// readNonCoord(numBytes);
			if (bb.capacity() - pos >= 4)
			{
				// bb.put(numBytes);
				readNonCoord(bb.array(), pos, 4, in2);
			}
			else
			{
				bbSize *= 2;
				final ByteBuffer newBB = ByteBuffer.allocate(bbSize);
				bb.limit(bb.position());
				bb.position(0);
				newBB.put(bb);
				bb = newBB;
				pos = bb.position();
				// bb.put(numBytes);
				readNonCoord(bb.array(), pos, 4, in2);
			}
			// int size = bytesToInt(numBytes);
			final int size = bb.getInt(pos);
			pos += 4;
			// byte[] data = new byte[size];
			// readNonCoord(data);
			if (bb.capacity() - pos >= size)
			{
				// bb.put(data);
				readNonCoord(bb.array(), pos, size, in2);
			}
			else
			{
				bbSize *= 2;
				while (bbSize < size)
				{
					bbSize *= 2;
				}
				final ByteBuffer newBB = ByteBuffer.allocate(bbSize);
				bb.limit(bb.position());
				bb.position(0);
				newBB.put(bb);
				bb = newBB;
				pos = bb.position();
				// bb.put(data);
				readNonCoord(bb.array(), pos, size, in2);
			}

			pos += size;
			// retval.add((List<Object>)fromBytes(data));
			i++;
		}

		// byte[] retval = new byte[bb.position()];
		// System.arraycopy(bb.array(), 0, retval, 0, retval.length);
		bb.limit(pos);
		return bb;
	}

	private static boolean rebuildTree(final List<Object> tree, final String remove)
	{
		final List<String> nodes = new ArrayList<String>();
		for (final Object o : tree)
		{
			if (o instanceof String)
			{
				if (!((String)o).equals(remove))
				{
					nodes.add((String)o);
				}
			}
			else
			{
				// ArrayList<Object>
				nodes.addAll(getAllNodes((List<Object>)o, remove));
			}
		}

		if (nodes.size() > 0)
		{
			tree.clear();
			cloneInto(tree, makeTree(nodes));
			return true;
		}

		return false;
	}

	private static void sendCommit(final List<Object> tree, final Transaction tx)
	{
		Object obj = tree.get(0);
		while (obj instanceof ArrayList)
		{
			obj = ((List)obj).get(0);
		}

		final String hostname = (String)obj;
		Socket sock = null;
		try
		{
			// sock = new Socket(hostname,
			// Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
			sock = new Socket();
			sock.setReceiveBufferSize(4194304);
			sock.setSendBufferSize(4194304);
			sock.connect(new InetSocketAddress(hostname, Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number"))));
			final OutputStream out = sock.getOutputStream();
			final byte[] outMsg = "LCOMMIT         ".getBytes(StandardCharsets.UTF_8);
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
			final ObjectOutputStream objOut = new ObjectOutputStream(out);
			objOut.writeObject(tree);
			objOut.flush();
			out.flush();
			getConfirmation(sock);
			objOut.close();
			sock.close();
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
			// TODO blackListByHost((String)obj);
			// TODO queueCommandByHost((String)obj, "COMMIT", tx);
			HRDBMSWorker.logger.fatal("BLACKLIST", e);
			System.exit(1);
			final boolean toDo = rebuildTree(tree, (String)obj);
			if (toDo)
			{
				sendCommit(tree, tx);
			}
		}
	}

	private static void sendRollback(final List<Object> tree, final Transaction tx)
	{
		Object obj = tree.get(0);
		while (obj instanceof ArrayList)
		{
			obj = ((List)obj).get(0);
		}

		final String hostname = (String)obj;
		Socket sock = null;
		try
		{
			// sock = new Socket(hostname,
			// Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
			sock = new Socket();
			sock.setReceiveBufferSize(4194304);
			sock.setSendBufferSize(4194304);
			sock.connect(new InetSocketAddress(hostname, Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number"))));
			final OutputStream out = sock.getOutputStream();
			final byte[] outMsg = "LROLLBCK        ".getBytes(StandardCharsets.UTF_8);
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
			final ObjectOutputStream objOut = new ObjectOutputStream(out);
			objOut.writeObject(tree);
			objOut.flush();
			out.flush();
			getConfirmation(sock);
			objOut.close();
			sock.close();
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
			// TODO blackListByHost((String)obj);
			// TODO queueCommandByHost((String)obj, "ROLLBACK", tx);
			HRDBMSWorker.logger.fatal("BLACKLIST", e);
			System.exit(1);
			final boolean toDo = rebuildTree(tree, (String)obj);
			if (toDo)
			{
				sendRollback(tree, tx);
			}
		}
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

	private static byte[] toBytesForHash(final List<Object> key)
	{
		final StringBuilder sb = new StringBuilder();
		for (final Object o : key)
		{
			if (o instanceof Double)
			{
				final DecimalFormat df = new DecimalFormat("0", DecimalFormatSymbols.getInstance(Locale.ENGLISH));
				df.setMaximumFractionDigits(340); // 340 =
													// DecimalFormat.DOUBLE_FRACTION_DIGITS

				sb.append(df.format(o));
				sb.append((char)0);
			}
			else if (o instanceof Number)
			{
				sb.append(o);
				sb.append((char)0);
			}
			else
			{
				sb.append(o.toString());
				sb.append((char)0);
			}
		}

		final int z = sb.length();
		final byte[] retval = new byte[z];
		int i = 0;
		while (i < z)
		{
			retval[i] = (byte)sb.charAt(i);
			i++;
		}

		return retval;
	}

	public void capacity()
	{
		try
		{
			@SuppressWarnings("restriction")
			final OperatingSystemMXBean osBean = ManagementFactory.getPlatformMXBean(OperatingSystemMXBean.class);
			@SuppressWarnings("restriction")
			final int me = (int)(100 * osBean.getSystemCpuLoad());
			sock.getOutputStream().write(intToBytes(me));
			sock.getOutputStream().flush();
			sock.close();
		}
		catch (final Exception e)
		{
		}
	}

	public void clientConnection()
	{
		if (!LogManager.recoverDone)
		{
			try
			{
				sock.close();
			}
			catch (final Exception e)
			{
			}
			return;
		}
		try
		{
			@SuppressWarnings("restriction")
			final OperatingSystemMXBean osBean = ManagementFactory.getPlatformMXBean(OperatingSystemMXBean.class);
			@SuppressWarnings("restriction")
			final int me = (int)(100 * osBean.getSystemCpuLoad());

			final Map<Integer, String> coords = MetaData.getCoordMap();

			boolean imLow = true;
			String lowHost = null;
			;
			int low = 0;
			for (final int node : coords.keySet())
			{
				try
				{
					final String hostname = coords.get(node);
					// Socket sock2 = new Socket(hostname,
					// Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
					final Socket sock2 = new Socket();
					sock2.setReceiveBufferSize(4194304);
					sock2.setSendBufferSize(4194304);
					sock2.setSoTimeout(5000 / coords.size());
					sock2.connect(new InetSocketAddress(hostname, Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number"))));
					final OutputStream out = sock2.getOutputStream();
					final byte[] outMsg = "CAPACITY        ".getBytes(StandardCharsets.UTF_8);
					outMsg[8] = 0;
					outMsg[9] = 0;
					outMsg[10] = 0;
					outMsg[11] = 0;
					outMsg[12] = 0;
					outMsg[13] = 0;
					outMsg[14] = 0;
					outMsg[15] = 0;
					out.write(outMsg);
					out.flush();
					final byte[] data = new byte[4];
					sock2.getInputStream().read(data);
					final int val = bytesToInt(data);
					if (imLow)
					{
						if (val < me)
						{
							imLow = false;
							low = val;
							lowHost = hostname;
						}
					}
					else
					{
						if (val < low)
						{
							low = val;
							lowHost = hostname;
						}
					}
					sock2.close();
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.debug("", e);
				}
			}

			if (imLow)
			{
				sock.getOutputStream().write("OK".getBytes(StandardCharsets.UTF_8));
				sock.getOutputStream().flush();
			}
			else
			{
				sock.getOutputStream().write("RD".getBytes(StandardCharsets.UTF_8));
				final byte[] hostData = lowHost.getBytes(StandardCharsets.UTF_8);
				HRDBMSWorker.logger.debug("Redirecting to " + lowHost);
				sock.getOutputStream().write(intToBytes(hostData.length));
				sock.getOutputStream().write(hostData);
				sock.getOutputStream().flush();
				sock.close();
			}
		}
		catch (final Exception e)
		{
			// HRDBMSWorker.logger.debug("Terminating connection due to
			// exception", e);
			try
			{
				sock.close();
				if (worker != null)
				{
					final List<Object> cmd2 = new ArrayList<Object>(1);
					cmd2.add("CLOSE");
					worker.in.put(cmd2);
				}
				this.terminate();
			}
			catch (final Exception f)
			{
			}
			return;
		}
	}

	public void clientConnection2()
	{
		if (!LogManager.recoverDone)
		{
			try
			{
				sock.close();
			}
			catch (final Exception e)
			{
			}
			return;
		}
		try
		{
			sock.getOutputStream().write("OK".getBytes(StandardCharsets.UTF_8));
			sock.getOutputStream().flush();
		}
		catch (final Exception e)
		{
			// HRDBMSWorker.logger.debug("Terminating connection due to
			// exception", e);
			try
			{
				sock.close();
				if (worker != null)
				{
					final List<Object> cmd2 = new ArrayList<Object>(1);
					cmd2.add("CLOSE");
					worker.in.put(cmd2);
				}
				this.terminate();
			}
			catch (final Exception f)
			{
			}
			return;
		}
	}

	public void delayDML()
	{
		ConnectionWorker.delayedDML.put(this, this);
		sendOK();
	}

	public void doCommit()
	{
		if (tx == null)
		{
			sendOK();
		}

		try
		{
			if (delayedDML.contains(this))
			{
				final IdentityHashMap covered = new IdentityHashMap<XAWorker, XAWorker>();
				while (true)
				{
					final List<XAWorker> workers = delayedWorkers.get(this);
					final List<XAWorker> clone = new ArrayList<XAWorker>(workers);

					boolean didSomething = false;
					for (final XAWorker worker : clone)
					{
						if (covered.containsKey(worker))
						{
							continue;
						}

						didSomething = true;
						InsertOperator.wakeUpDelayed(tx);
						DeleteOperator.wakeUpDelayed(tx);
						UpdateOperator.wakeUpDelayed(tx);
						final boolean joined = worker.join(1);

						if (joined)
						{
							final int updateCount = worker.getUpdateCount();

							if (updateCount == -1)
							{
								sendNo();
								// returnExceptionToClient(worker.getException());
								delayedWorkers.multiRemove(this);
								return;
							}

							covered.put(worker, worker);
						}
					}

					if (!didSomething)
					{
						break;
					}
				}

				delayedWorkers.multiRemove(this);
				delayedTxs.remove(tx.number());
			}

			XAManager.commit(tx);
			tx = null;
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.debug("", e);
			sendNo();
		}

		sendOK();
	}

	public void doPacing()
	{
		if (tx == null)
		{
			sendOK();
			return;
		}

		try
		{
			if (delayedDML.contains(this))
			{
				final List<XAWorker> workers = delayedWorkers.get(this);

				while (true)
				{
					if (workers.size() == 0)
					{
						break;
					}
					final List<XAWorker> clone = new ArrayList<XAWorker>(workers);
					int i = 0;
					for (final XAWorker worker : clone)
					{
						InsertOperator.wakeUpDelayed(tx);
						DeleteOperator.wakeUpDelayed(tx);
						UpdateOperator.wakeUpDelayed(tx);
						final boolean joined = worker.join(1);
						if (joined && worker.getUpdateCount() == -1)
						{
							sendNo();
							return;
						}

						if (!joined)
						{
							break;
						}

						i++;
					}

					int j = 0;
					while (j < i)
					{
						workers.remove(0);
						j++;
					}
				}
			}
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.debug("", e);
		}

		sendOK();
	}

	public void doRollback()
	{
		if (tx == null)
		{
			sendOK();
		}

		if (delayedDML.contains(this))
		{
			final IdentityHashMap<XAWorker, XAWorker> covered = new IdentityHashMap<XAWorker, XAWorker>();
			while (true)
			{
				final List<XAWorker> workers = delayedWorkers.get(this);
				final List<XAWorker> clone = new ArrayList<XAWorker>(workers);
				boolean didSomething = false;
				for (final XAWorker worker : clone)
				{
					if (covered.containsKey(worker))
					{
						continue;
					}

					didSomething = true;

					boolean joined = false;
					while (true)
					{
						try
						{
							InsertOperator.wakeUpDelayed(tx);
							DeleteOperator.wakeUpDelayed(tx);
							UpdateOperator.wakeUpDelayed(tx);
							joined = worker.join(1);
							break;
						}
						catch (final InterruptedException e)
						{
						}
					}

					if (joined)
					{
						covered.put(worker, worker);
					}
				}

				if (!didSomething)
				{
					break;
				}
			}

			delayedWorkers.multiRemove(this);
			delayedTxs.remove(tx.number());
		}

		try
		{
			XAManager.rollback(tx);
			tx = null;
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.debug("", e);
			sendNo();
		}

		sendOK();
	}

	@Override
	public void run()
	{
		// check if there are enough available resources or not
		/*
		 * if (HRDBMSWorker.type == HRDBMSWorker.TYPE_MASTER ||
		 * HRDBMSWorker.type == HRDBMSWorker.TYPE_COORD) { try { while (true) {
		 * String uptimeCmd = "uptime"; String uptimeCmdResult =
		 * runUptimeCommand(uptimeCmd, true); double load =
		 * parseUptimeResult(uptimeCmdResult); if (load <= (maxLoad * 1.0)) { if
		 * (!ResourceManager.criticalMem()) { break; } else { System.gc(); } }
		 *
		 * Thread.sleep(5000); } } catch (Exception e) {
		 * HRDBMSWorker.logger.debug("", e); } }
		 */

		// HRDBMSWorker.logger.debug("New connection worker is up and running");
		try
		{
			InputStream in = null;
			try
			{
				in = sock.getInputStream();
			}
			catch (final java.net.SocketException e)
			{
			}

			while (true)
			{
				final byte[] cmd = new byte[8];

				int num = 0;
				try
				{
					num = in.read(cmd);
				}
				catch (final Exception e)
				{
					if (worker != null)
					{
						final List<Object> cmd2 = new ArrayList<Object>(1);
						cmd2.add("CLOSE");
						while (true)
						{
							try
							{
								worker.in.put(cmd2);
								break;
							}
							catch (final Exception f)
							{
							}
						}
					}

					if (tx != null)
					{
						doRollback();
					}
					sock.close();
					this.terminate();
					return;
				}

				if (num != 8 && num != -1)
				{
					HRDBMSWorker.logger.error("Connection worker received less than 8 bytes when reading a command.  Terminating!");
					HRDBMSWorker.logger.error("Number of bytes received: " + num);
					HRDBMSWorker.logger.error("Command = " + new String(cmd, StandardCharsets.UTF_8));
					returnException("BadCommandException");
					sock.close();
					if (worker != null)
					{
						final List<Object> cmd2 = new ArrayList<Object>(1);
						cmd2.add("CLOSE");
						worker.in.put(cmd2);
					}
					if (tx != null)
					{
						doRollback();
					}
					this.terminate();
					return;
				}
				else if (num == -1)
				{
					// HRDBMSWorker.logger.debug("Received EOF when looking for
					// a command.");
					sock.close();
					if (worker != null)
					{
						final List<Object> cmd2 = new ArrayList<Object>(1);
						cmd2.add("CLOSE");
						worker.in.put(cmd2);
					}
					if (tx != null)
					{
						doRollback();
					}
					this.terminate();
					return;
				}
				final String command = new String(cmd, StandardCharsets.UTF_8);
				// HRDBMSWorker.logger.debug("Received " + num + " bytes");

				if (command.equals("EXECUTEU") || command.equals("INSERT  ") || command.equals("UPDATE  ") || command.equals("DELETE  ") || command.equals("PACING  ") || command.equals("REMOTTRE"))
				{
				}
				else
				{
					HRDBMSWorker.logger.debug("Command: " + command);
				}

				final byte[] fromBytes = new byte[4];
				final byte[] toBytes = new byte[4];
				num = in.read(fromBytes);
				if (num != 4)
				{
					HRDBMSWorker.logger.error("Received less than 4 bytes when reading from field in remote command.");
					HRDBMSWorker.logger.debug("Received " + num + " bytes");
					returnException("InvalidFromIDException");
					sock.close();
					if (worker != null)
					{
						final List<Object> cmd2 = new ArrayList<Object>(1);
						cmd2.add("CLOSE");
						worker.in.put(cmd2);
					}
					if (tx != null)
					{
						doRollback();
					}
					this.terminate();
					return;
				}

				num = in.read(toBytes);
				if (num != 4)
				{
					HRDBMSWorker.logger.error("Received less than 4 bytes when reading to field in remote command.");
					returnException("InvalidToIDException");
					sock.close();
					if (worker != null)
					{
						final List<Object> cmd2 = new ArrayList<Object>(1);
						cmd2.add("CLOSE");
						worker.in.put(cmd2);
					}
					if (tx != null)
					{
						doRollback();
					}
					this.terminate();
					return;
				}

				final int from = bytesToInt(fromBytes);
				final int to = bytesToInt(toBytes);

				if (command.equals("GET     "))
				{
					while (!XAManager.rP2)
					{
						Thread.sleep(1000);
					}
					get();
				}
				else if (command.equals("REMOVE  "))
				{
					while (!XAManager.rP2)
					{
						Thread.sleep(1000);
					}
					remove();
				}
				else if (command.equals("REMOVE2 "))
				{
					while (!XAManager.rP2)
					{
						Thread.sleep(1000);
					}
					remove2();
				}
				else if (command.equals("PUT     "))
				{
					while (!XAManager.rP2)
					{
						Thread.sleep(1000);
					}
					put();
				}
				else if (command.equals("PUT2    "))
				{
					while (!XAManager.rP2)
					{
						Thread.sleep(1000);
					}
					put2();
				}
				else if (command.equals("RGETDATD"))
				{
					try
					{
						final String retval = getDataDir(from, to);
						HRDBMSWorker.logger.debug("Responding with " + retval);
						respond(to, from, retval);
					}
					catch (final Exception e)
					{
						returnException(e.toString());
					}
				}
				else if (command.equals("REMOTTRE"))
				{
					// final ObjectInputStream objIn = new
					// ObjectInputStream(in);
					// final NetworkSendOperator op =
					// (NetworkSendOperator)objIn.readObject();
					HashMap<Long, Object> map = new HashMap<Long, Object>();
					final NetworkSendOperator op = (NetworkSendOperator)OperatorUtils.deserializeOperator(in, map);
					map.clear();
					map = null;
					op.setSocket(sock);
					while (!XAManager.rP2)
					{
						Thread.sleep(1000);
					}
					op.startChildren();
					ResourceManager.registerOperator(op);
					op.start();
					try
					{
						op.close();
						ResourceManager.deregisterOperator(op);
					}
					catch (final Exception e)
					{
					}
				}
				else if (command.equals("SNDRMTT2"))
				{
					final byte[] idBytes = new byte[4];
					num = in.read(idBytes);
					if (num != 4)
					{
						throw new Exception("Received less than 4 bytes when reading id field in ROUTING command.");
					}

					final int id = bytesToInt(idBytes);
					Operator send = sends.get(id);
					while (send == null)
					{
						LockSupport.parkNanos(500);
						send = sends.get(id);
					}
					// System.out.println("Adding connection from " + from +
					// " to " + send + " = " + sock);
					if (send instanceof NetworkSendOperator)
					{
						((NetworkSendOperator)send).addConnection(from, sock);
					}
					else
					{
						((RoutingOperator)send).addConnection(from, sock);
					}
					while (!XAManager.rP2)
					{
						Thread.sleep(1000);
					}
					synchronized (send)
					{
						if (send instanceof NetworkSendOperator)
						{
							if (((NetworkSendOperator)send).notStarted() && ((NetworkSendOperator)send).hasAllConnections())
							{
								ResourceManager.registerOperator(send);
								send.start();
								try
								{
									send.close();
									ResourceManager.deregisterOperator(send);
								}
								catch (final Exception e)
								{
								}
								sends.remove(id);
							}
						}
						else
						{
							if (((RoutingOperator)send).notStarted() && ((RoutingOperator)send).hasAllConnections())
							{
								ResourceManager.registerOperator(send);
								send.start();
								try
								{
									send.close();
									ResourceManager.deregisterOperator(send);
								}
								catch (final Exception e)
								{
								}
								sends.remove(id);
							}
						}
					}
				}
				else if (command.equals("SNDRMTTR"))
				{
					final byte[] idBytes = new byte[4];
					num = in.read(idBytes);
					if (num != 4)
					{
						throw new Exception("Received less than 4 bytes when reading id field in ROUTING command.");
					}

					final int id = bytesToInt(idBytes);
					HashMap<Long, Object> map = new HashMap<Long, Object>();
					final Operator op = OperatorUtils.deserializeOperator(in, map);
					map.clear();
					map = null;
					if (sends.putIfAbsent(id, op) == null)
					{
						if (op instanceof NetworkSendOperator)
						{
							((NetworkSendOperator)op).startChildren();
						}
						else
						{
							((RoutingOperator)op).startChildren();
						}
					}

					final Operator send = sends.get(id);
					// System.out.println("Adding connection from " + from +
					// " to " + send + " = " + sock);
					if (send instanceof NetworkSendOperator)
					{
						((NetworkSendOperator)send).addConnection(from, sock);
					}
					else
					{
						((RoutingOperator)send).addConnection(from, sock);
					}
					while (!XAManager.rP2)
					{
						Thread.sleep(1000);
					}
					synchronized (send)
					{
						if (send instanceof NetworkSendOperator)
						{
							if (((NetworkSendOperator)send).notStarted() && ((NetworkSendOperator)send).hasAllConnections())
							{
								ResourceManager.registerOperator(send);
								send.start();
								try
								{
									send.close();
									ResourceManager.deregisterOperator(send);
								}
								catch (final Exception e)
								{
								}
								sends.remove(id);
							}
						}
						else
						{
							if (((RoutingOperator)send).notStarted() && ((RoutingOperator)send).hasAllConnections())
							{
								ResourceManager.registerOperator(send);
								send.start();
								try
								{
									send.close();
									ResourceManager.deregisterOperator(send);
								}
								catch (final Exception e)
								{
								}
								sends.remove(id);
							}
						}
					}
				}
				else if (command.equals("ROUTING2"))
				{
					final byte[] idBytes = new byte[4];
					num = in.read(idBytes);
					if (num != 4)
					{
						throw new Exception("Received less than 4 bytes when reading id field in ROUTING command.");
					}

					final int id = bytesToInt(idBytes);
					Operator send = sends.get(id);
					while (send == null)
					{
						LockSupport.parkNanos(500);
						send = sends.get(id);
					}
					// System.out.println("Adding connection from " + from +
					// " to " + send + " = " + sock);
					if (send instanceof NetworkSendOperator)
					{
						((NetworkSendOperator)send).addConnection(from, sock);
					}
					else
					{
						((RoutingOperator)send).addConnection(from, sock);
					}
					while (!XAManager.rP2)
					{
						Thread.sleep(1000);
					}
					synchronized (send)
					{
						if (send instanceof NetworkSendOperator)
						{
							if (((NetworkSendOperator)send).notStarted() && ((NetworkSendOperator)send).hasAllConnections())
							{
								ResourceManager.registerOperator(send);
								send.start();
								try
								{
									send.close();
									ResourceManager.deregisterOperator(send);
								}
								catch (final Exception e)
								{
								}
								sends.remove(id);
							}
						}
						else
						{
							if (((RoutingOperator)send).notStarted() && ((RoutingOperator)send).hasAllConnections())
							{
								ResourceManager.registerOperator(send);
								send.start();
								try
								{
									send.close();
									ResourceManager.deregisterOperator(send);
								}
								catch (final Exception e)
								{
								}
								sends.remove(id);
							}
						}
					}
				}
				else if (command.equals("ROUTING "))
				{
					final byte[] idBytes = new byte[4];
					num = in.read(idBytes);
					if (num != 4)
					{
						throw new Exception("Received less than 4 bytes when reading id field in ROUTING command.");
					}

					final int id = bytesToInt(idBytes);
					HashMap<Long, Object> map = new HashMap<Long, Object>();
					final Operator op = OperatorUtils.deserializeOperator(in, map);
					map.clear();
					map = null;
					if (sends.putIfAbsent(id, op) == null)
					{
						if (op instanceof NetworkSendOperator)
						{
							((NetworkSendOperator)op).startChildren();
						}
						else
						{
							((RoutingOperator)op).startChildren();
						}
					}

					final Operator send = sends.get(id);
					// System.out.println("Adding connection from " + from +
					// " to " + send + " = " + sock);
					if (send instanceof NetworkSendOperator)
					{
						((NetworkSendOperator)send).addConnection(from, sock);
					}
					else
					{
						((RoutingOperator)send).addConnection(from, sock);
					}
					while (!XAManager.rP2)
					{
						Thread.sleep(1000);
					}
					synchronized (send)
					{
						if (send instanceof NetworkSendOperator)
						{
							if (((NetworkSendOperator)send).notStarted() && ((NetworkSendOperator)send).hasAllConnections())
							{
								ResourceManager.registerOperator(send);
								send.start();
								try
								{
									send.close();
									ResourceManager.deregisterOperator(send);
								}
								catch (final Exception e)
								{
								}
								sends.remove(id);
							}
						}
						else
						{
							if (((RoutingOperator)send).notStarted() && ((RoutingOperator)send).hasAllConnections())
							{
								ResourceManager.registerOperator(send);
								send.start();
								try
								{
									send.close();
									ResourceManager.deregisterOperator(send);
								}
								catch (final Exception e)
								{
								}
								sends.remove(id);
							}
						}
					}
				}
				else if (command.equals("CLOSE   "))
				{
					closeConnection();
					if (worker != null)
					{
						final List<Object> cmd2 = new ArrayList<Object>(1);
						cmd2.add("CLOSE");
						worker.in.put(cmd2);
					}
					if (tx != null)
					{
						doRollback();
					}
					HRDBMSWorker.logger.debug("Received request to terminate connection");
					this.terminate();
					return;
				}
				else if (command.equals("CLIENT  "))
				{
					while (!XAManager.rP2)
					{
						Thread.sleep(1000);
					}
					clientConnection();
				}
				else if (command.equals("CLIENT2 "))
				{
					while (!XAManager.rP2)
					{
						Thread.sleep(1000);
					}
					clientConnection2();
				}
				else if (command.equals("CAPACITY"))
				{
					capacity();
				}
				else if (command.equals("COMMIT  "))
				{
					while (!XAManager.rP2)
					{
						Thread.sleep(1000);
					}
					doCommit();
				}
				else if (command.equals("DELAYDML"))
				{
					while (!XAManager.rP2)
					{
						Thread.sleep(1000);
					}
					delayDML();
				}
				else if (command.equals("PACING  "))
				{
					while (!XAManager.rP2)
					{
						Thread.sleep(1000);
					}
					doPacing();
				}
				else if (command.equals("CHECKPNT"))
				{
					while (!XAManager.rP2)
					{
						Thread.sleep(1000);
					}
					checkpoint();
				}
				else if (command.equals("ROLLBACK"))
				{
					while (!XAManager.rP2)
					{
						Thread.sleep(1000);
					}
					doRollback();
				}
				else if (command.equals("ISOLATIO"))
				{
					while (!XAManager.rP2)
					{
						Thread.sleep(1000);
					}
					doIsolation();
				}
				else if (command.equals("TEST    "))
				{
					testConnection();
				}
				else if (command.equals("SETSCHMA"))
				{
					while (!XAManager.rP2)
					{
						Thread.sleep(1000);
					}
					setSchema();
				}
				else if (command.equals("GETSCHMA"))
				{
					while (!XAManager.rP2)
					{
						Thread.sleep(1000);
					}
					getSchema();
				}
				else if (command.equals("EXECUTEQ"))
				{
					while (!XAManager.rP2)
					{
						Thread.sleep(1000);
					}
					executeQuery();
				}
				else if (command.equals("RSMETA  "))
				{
					while (!XAManager.rP2)
					{
						Thread.sleep(1000);
					}
					getRSMeta();
				}
				else if (command.equals("NEXT    "))
				{
					while (!XAManager.rP2)
					{
						Thread.sleep(1000);
					}
					doNext();
				}
				else if (command.equals("CLOSERS "))
				{
					while (!XAManager.rP2)
					{
						Thread.sleep(1000);
					}
					closeRS();
				}
				else if (command.equals("LROLLBCK"))
				{
					localRollback();
				}
				else if (command.equals("PREPARE "))
				{
					while (!XAManager.rP2)
					{
						Thread.sleep(1000);
					}
					prepare();
				}
				else if (command.equals("LCOMMIT "))
				{
					localCommit();
				}
				else if (command.equals("CHECKTX "))
				{
					checkTx();
				}
				else if (command.equals("MDELETE "))
				{
					while (!XAManager.rP2)
					{
						Thread.sleep(1000);
					}
					massDelete();
				}
				else if (command.equals("DELETE  "))
				{
					while (!XAManager.rP2)
					{
						Thread.sleep(1000);
					}
					delete();
				}
				else if (command.equals("UPDATE  "))
				{
					while (!XAManager.rP2)
					{
						Thread.sleep(1000);
					}
					update();
				}
				else if (command.equals("INSERT  "))
				{
					while (!XAManager.rP2)
					{
						Thread.sleep(1000);
					}
					insert();
				}
				else if (command.equals("LOAD    "))
				{
					while (!XAManager.rP2)
					{
						Thread.sleep(1000);
					}
					load();
				}
				else if (command.equals("EXECUTEU"))
				{
					while (!XAManager.rP2)
					{
						Thread.sleep(1000);
					}
					executeUpdate();
				}
				else if (command.equals("NEWTABLE"))
				{
					while (!XAManager.rP2)
					{
						Thread.sleep(1000);
					}
					newTable();
				}
				else if (command.equals("CLUSTER "))
				{
					while (!XAManager.rP2)
					{
						Thread.sleep(1000);
					}
					cluster();
				}
				else if (command.equals("NEWINDEX"))
				{
					while (!XAManager.rP2)
					{
						Thread.sleep(1000);
					}
					newIndex();
				}
				else if (command.equals("POPINDEX"))
				{
					while (!XAManager.rP2)
					{
						Thread.sleep(1000);
					}
					popIndex();
				}
				else if (command.equals("GETLDMD "))
				{
					while (!XAManager.rP2)
					{
						Thread.sleep(1000);
					}
					getLoadMetaData();
				}
				else if (command.equals("HADOOPLD"))
				{
					while (!XAManager.rP2)
					{
						Thread.sleep(1000);
					}
					hadoopLoad();
				}
				else if (command.equals("SETLDMD "))
				{
					while (!XAManager.rP2)
					{
						Thread.sleep(1000);
					}
					setLoadMetaData();
				}
				else if (command.equals("DELLDMD "))
				{
					while (!XAManager.rP2)
					{
						Thread.sleep(1000);
					}
					delLoadMetaData();
				}
				else if (command.equals("DELFITBL"))
				{
					delFileTable();
				}
				else if (command.equals("DELFIIDX"))
				{
					delFileIndex();
				}
				else if (command.equals("REORG   "))
				{
					while (!XAManager.rP2)
					{
						Thread.sleep(1000);
					}
					reorg();
				}
				else
				{
					HRDBMSWorker.logger.error("Uknown command received by ConnectionWorker: " + command);
					returnException("BadCommandException");
				}
			}
		}
		catch (final Exception e)
		{
			try
			{
				returnException(e.toString());
				sock.close();
			}
			catch (final Exception f)
			{
				try
				{
					sock.close();
				}
				catch (final Exception g)
				{
				}
			}
			// HRDBMSWorker.logger.debug("Terminating connection due to
			// exception", e);
			if (worker != null)
			{
				final List<Object> cmd2 = new ArrayList<Object>(1);
				cmd2.add("CLOSE");
				while (true)
				{
					try
					{
						worker.in.put(cmd2);
						break;
					}
					catch (final Exception f)
					{
					}
				}
			}
			if (tx != null)
			{
				doRollback();
			}
			this.terminate();
			return;
		}
	}

	private void checkpoint()
	{
		List<Object> tree = null;
		try
		{
			final ObjectInputStream objIn = new ObjectInputStream(sock.getInputStream());
			tree = (List<Object>)objIn.readObject();
		}
		catch (final Exception e)
		{
			sendNo();
			return;
		}

		HRDBMSWorker.checkpoint.doCheckpoint();

		Object obj = tree.get(0);
		while (obj instanceof ArrayList)
		{
			obj = ((List)obj).get(0);
		}

		removeFromTree((String)obj, tree, null); // also delete parents if now
		// empty

		final List<SendCheckpointThread> threads = new ArrayList<SendCheckpointThread>();
		for (final Object o : tree)
		{
			threads.add(new SendCheckpointThread(o));
		}

		for (final SendCheckpointThread thread : threads)
		{
			thread.start();
		}

		boolean allOK = true;
		for (final SendCheckpointThread thread : threads)
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
			if (!thread.sendOK())
			{
				allOK = false;
			}
		}

		if (allOK)
		{
			sendOK();
		}
		else
		{
			sendNo();
			try
			{
				sock.close();
			}
			catch (final Exception f)
			{
			}
		}
	}

	private void checkTx()
	{
		final byte[] data = new byte[8];
		try
		{
			readNonCoord(data);
		}
		catch (final Exception e)
		{
			try
			{
				sock.close();
			}
			catch (final Exception f)
			{
				try
				{
					sock.getOutputStream().close();
				}
				catch (final Exception g)
				{
				}
			}
			return;
		}
		final long txnum = bytesToLong(data);
		String filename = HRDBMSWorker.getHParms().getProperty("log_dir");
		if (!filename.endsWith("/"))
		{
			filename += "/";
		}
		filename += "xa.log";
		Iterator<LogRec> iter = LogManager.iterator(filename, false);
		boolean sent = false;
		while (iter.hasNext())
		{
			final LogRec rec = iter.next();
			if (rec.txnum() == txnum)
			{
				if (rec.type() == LogRec.XACOMMIT)
				{
					sendOK();
					sent = true;
					break;
				}
				else if (rec.type() == LogRec.XAABORT)
				{
					sendNo();
					sent = true;
					break;
				}
				else if (rec.type() == LogRec.PREPARE)
				{
					sendNo();
					sent = true;
					break;
				}
			}
		}

		((LogIterator)iter).close();

		if (!sent)
		{
			iter = LogManager.archiveIterator(filename, false);
			sent = false;
			while (iter.hasNext())
			{
				final LogRec rec = iter.next();
				if (rec.txnum() == txnum)
				{
					if (rec.type() == LogRec.XACOMMIT)
					{
						sendOK();
						sent = true;
						break;
					}
					else if (rec.type() == LogRec.XAABORT)
					{
						sendNo();
						sent = true;
						break;
					}
					else if (rec.type() == LogRec.PREPARE)
					{
						sendNo();
						sent = true;
						break;
					}
				}
			}
		}
	}

	private void closeConnection()
	{
		if (tx != null)
		{
			try
			{
				XAManager.rollback(tx);
				tx = null;
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
			}
		}
		try
		{
			sock.close();
			if (worker != null)
			{
				final List<Object> cmd2 = new ArrayList<Object>(1);
				cmd2.add("CLOSE");
				worker.in.put(cmd2);
			}
			this.terminate();
		}
		catch (final Exception e)
		{
		}
	}

	private void closeRS()
	{
		if (worker == null)
		{
			return;
		}

		final List<Object> al = new ArrayList<Object>(1);
		al.add("CLOSE");
		while (true)
		{
			try
			{
				worker.in.put(al);
				worker = null;
				break;
			}
			catch (final InterruptedException e)
			{
			}
		}
	}

	private void cluster()
	{
		final byte[] lenBytes = new byte[4];
		int len;
		byte[] bytes;
		String schema;
		String table;
		List<Object> tree;
		Transaction tx;
		final byte[] txBytes = new byte[8];
		int type;
		Map<Integer, String> pos2Col;
		Map<String, String> cols2Types;

		try
		{
			readNonCoord(txBytes);
			tx = new Transaction(bytesToLong(txBytes));
			readNonCoord(lenBytes);
			len = bytesToInt(lenBytes);
			bytes = new byte[len];
			readNonCoord(bytes);
			schema = new String(bytes, StandardCharsets.UTF_8);
			readNonCoord(lenBytes);
			len = bytesToInt(lenBytes);
			bytes = new byte[len];
			readNonCoord(bytes);
			table = new String(bytes, StandardCharsets.UTF_8);
			readNonCoord(lenBytes);
			type = bytesToInt(lenBytes);
			final ObjectInputStream objIn = new ObjectInputStream(sock.getInputStream());
			tree = (List<Object>)objIn.readObject();
			pos2Col = (Map<Integer, String>)objIn.readObject();
			cols2Types = (Map<String, String>)objIn.readObject();

			final List<FlushLoadThread> threads = new ArrayList<FlushLoadThread>();
			// new MetaData();
			final int limit = MetaData.getNumDevices();

			int device = 0;
			while (device < limit)
			{
				threads.add(new FlushLoadThread(tx, schema, table, device, pos2Col, cols2Types, type));
				device++;
			}

			for (final FlushLoadThread thread : threads)
			{
				thread.start();
			}

			// ///////////////////////////////////////////
			Object obj = tree.get(0);
			while (obj instanceof ArrayList)
			{
				obj = ((List)obj).get(0);
			}

			removeFromTree((String)obj, tree, null); // also delete parents if
			// now empty

			final List<SendHierClusterThread> threads2 = new ArrayList<SendHierClusterThread>();
			for (final Object o : tree)
			{
				threads2.add(new SendHierClusterThread(schema, table, pos2Col, cols2Types, o, tx, type));
			}

			for (final SendHierClusterThread thread : threads2)
			{
				thread.start();
			}

			boolean allOK = true;
			for (final SendHierClusterThread thread : threads2)
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
				if (!thread.getOK())
				{
					allOK = false;
				}
			}
			// /////////////////////////////

			for (final FlushLoadThread thread : threads)
			{
				thread.join();
				if (!thread.getOK())
				{
					allOK = false;
				}
			}

			if (allOK)
			{
				sendOK();
			}
			else
			{
				sendNo();
				try
				{
					sock.close();
				}
				catch (final Exception f)
				{
				}
			}
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.debug("", e);
			HRDBMSWorker.logger.debug("Sending NO");
			sendNo();
			return;
		}
	}

	private void delete()
	{
		final byte[] txBytes = new byte[8];
		long txNum = -1;
		final byte[] schemaLenBytes = new byte[4];
		final byte[] tableLenBytes = new byte[4];
		int schemaLength = -1;
		int tableLength = -1;
		String schema = null;
		String table = null;
		byte[] schemaData = null;
		byte[] tableData = null;
		List<String> indexes;
		List<List<String>> keys;
		List<List<String>> types;
		List<List<Boolean>> orders;
		List<RIDAndIndexKeys> raiks = null;
		Map<Integer, String> pos2Col = null;
		Map<String, String> cols2Types = null;
		int type;
		try
		{
			final InputStream in = new BufferedInputStream(sock.getInputStream());
			readNonCoord(txBytes, in);
			txNum = bytesToLong(txBytes);
			tx = new Transaction(txNum);
			readNonCoord(schemaLenBytes, in);
			schemaLength = bytesToInt(schemaLenBytes);
			schemaData = new byte[schemaLength];
			readNonCoord(schemaData, in);
			schema = new String(schemaData, StandardCharsets.UTF_8);
			readNonCoord(tableLenBytes, in);
			tableLength = bytesToInt(tableLenBytes);
			tableData = new byte[tableLength];
			readNonCoord(tableData, in);
			table = new String(tableData, StandardCharsets.UTF_8);
			readNonCoord(tableLenBytes, in);
			type = bytesToInt(tableLenBytes);
			// ObjectInputStream objIn = new
			// ObjectInputStream(sock.getInputStream());
			final Map<Long, Object> prev = new HashMap<Long, Object>();
			indexes = OperatorUtils.deserializeALS(in, prev);
			// indexes = (List<String>)objIn.readObject();
			raiks = OperatorUtils.deserializeALRAIK(in, prev);
			// raiks = (List<RIDAndIndexKeys>)objIn.readObject();
			keys = OperatorUtils.deserializeALALS(in, prev);
			// keys = (List<List<String>>)objIn.readObject();
			types = OperatorUtils.deserializeALALS(in, prev);
			// types = (List<List<String>>)objIn.readObject();
			orders = OperatorUtils.deserializeALALB(in, prev);
			// orders = (List<List<Boolean>>)objIn.readObject();
			pos2Col = OperatorUtils.deserializeTM(in, prev);
			// pos2Col = (Map<Integer, String>)objIn.readObject();
			cols2Types = OperatorUtils.deserializeStringHM(in, prev);
			// cols2Types = (Map<String, String>)objIn.readObject();
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.debug("", e);
			sendNo();
			return;
		}

		final MultiHashMap<Integer, RIDAndIndexKeys> map = new MultiHashMap<Integer, RIDAndIndexKeys>();
		for (final RIDAndIndexKeys raik : raiks)
		{
			map.multiPut(raik.getRID().getDevice(), raik);
		}

		final List<FlushDeleteThread> threads = new ArrayList<FlushDeleteThread>();
		final List<String> dmlTxStrs = new ArrayList<String>();
		final List<Integer> sorted = new ArrayList(map.getKeySet());
		Collections.sort(sorted);
		for (final Object o : sorted)
		{
			final int device = (Integer)o;
			threads.add(new FlushDeleteThread(map.get(device), tx, schema, table, keys, types, orders, indexes, pos2Col, cols2Types, type));
			final String dmlTxStr = Long.toString(tx.number()) + "~" + device + "~" + schema + "." + table;
			if (dmlTx.putIfAbsent(dmlTxStr, dmlTxStr) != null)
			{
				while (dmlTx.putIfAbsent(dmlTxStr, dmlTxStr) != null)
				{
					LockSupport.parkNanos(1);
				}
			}

			dmlTxStrs.add(dmlTxStr);
		}

		for (final FlushDeleteThread thread : threads)
		{
			thread.start();
		}

		boolean allOK = true;
		for (final FlushDeleteThread thread : threads)
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
			if (!thread.getOK())
			{
				allOK = false;
				final Exception e = thread.getException();
				HRDBMSWorker.logger.debug("", e);
			}
		}

		for (final String s : dmlTxStrs)
		{
			dmlTx.remove(s);
		}

		if (allOK)
		{
			sendOK();
			return;
		}
		else
		{
			sendNo();
			return;
		}
	}

	private void delFileIndex()
	{
		List<Object> tree = null;
		List<String> indexes = null;
		try
		{
			final ObjectInputStream objIn = new ObjectInputStream(sock.getInputStream());
			tree = (List<Object>)objIn.readObject();
			indexes = (List<String>)objIn.readObject();
		}
		catch (final Exception e)
		{
			sendNo();
			return;
		}

		final List<String> paths = getDataPaths();
		for (final String path : paths)
		{
			try
			{
				final List<String> files = getIndexFilesInPath(path);
				for (final String file : files)
				{
					final String index = file.substring(0, file.indexOf(".indx"));
					if (!indexes.contains(index))
					{
						FileManager.removeFile(path + file);
					}
				}
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.debug("", e);
				sendNo();
			}
		}

		Object obj = tree.get(0);
		while (obj instanceof ArrayList)
		{
			obj = ((List)obj).get(0);
		}

		removeFromTree((String)obj, tree, null); // also delete parents if now
		// empty

		final List<SendDelFiIThread> threads = new ArrayList<SendDelFiIThread>();
		for (final Object o : tree)
		{
			threads.add(new SendDelFiIThread(o, indexes));
		}

		for (final SendDelFiIThread thread : threads)
		{
			thread.start();
		}

		boolean allOK = true;
		for (final SendDelFiIThread thread : threads)
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
			if (!thread.sendOK())
			{
				allOK = false;
			}
		}

		if (allOK)
		{
			sendOK();
		}
		else
		{
			sendNo();
			try
			{
				sock.close();
			}
			catch (final Exception f)
			{
			}
		}
	}

	private void delFileTable()
	{
		List<Object> tree = null;
		List<String> tables = null;
		try
		{
			final ObjectInputStream objIn = new ObjectInputStream(sock.getInputStream());
			tree = (List<Object>)objIn.readObject();
			tables = (List<String>)objIn.readObject();
		}
		catch (final Exception e)
		{
			sendNo();
			return;
		}

		final List<String> paths = getDataPaths();
		for (final String path : paths)
		{
			try
			{
				final List<String> files = getTableFilesInPath(path);
				for (final String file : files)
				{
					final String table = file.substring(0, file.indexOf(".tbl"));
					if (!tables.contains(table))
					{
						FileManager.removeFile(path + file);
					}
				}
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.debug("", e);
				sendNo();
			}
		}

		Object obj = tree.get(0);
		while (obj instanceof ArrayList)
		{
			obj = ((List)obj).get(0);
		}

		removeFromTree((String)obj, tree, null); // also delete parents if now
		// empty

		final List<SendDelFiTThread> threads = new ArrayList<SendDelFiTThread>();
		for (final Object o : tree)
		{
			threads.add(new SendDelFiTThread(o, tables));
		}

		for (final SendDelFiTThread thread : threads)
		{
			thread.start();
		}

		boolean allOK = true;
		for (final SendDelFiTThread thread : threads)
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
			if (!thread.sendOK())
			{
				allOK = false;
			}
		}

		if (allOK)
		{
			sendOK();
		}
		else
		{
			sendNo();
			try
			{
				sock.close();
			}
			catch (final Exception f)
			{
			}
		}
	}

	private void delLoadMetaData()
	{
		List<Object> tree = null;
		final byte[] keyLength = new byte[4];
		int length;
		byte[] data;
		String key;
		try
		{
			readNonCoord(keyLength);
			length = bytesToInt(keyLength);
			data = new byte[length];
			readNonCoord(data);
			key = new String(data, StandardCharsets.UTF_8);
			final ObjectInputStream objIn = new ObjectInputStream(sock.getInputStream());
			tree = (List<Object>)objIn.readObject();
		}
		catch (final Exception e)
		{
			sendNo();
			return;
		}

		ldmds.remove(key);

		Object obj = tree.get(0);
		while (obj instanceof ArrayList)
		{
			obj = ((List)obj).get(0);
		}

		removeFromTree((String)obj, tree, null); // also delete parents if now
		// empty

		final List<SendRemoveLDMDThread> threads = new ArrayList<SendRemoveLDMDThread>();
		for (final Object o : tree)
		{
			threads.add(new SendRemoveLDMDThread(o, keyLength, data));
		}

		for (final SendRemoveLDMDThread thread : threads)
		{
			thread.start();
		}

		boolean allOK = true;
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
			if (!thread.sendOK())
			{
				allOK = false;
			}
		}

		if (allOK)
		{
			sendOK();
		}
		else
		{
			sendNo();
			try
			{
				sock.close();
			}
			catch (final Exception f)
			{
			}
		}
	}

	private Object doGet(String name, Object key) throws Exception
	{
		// qualify table name if not qualified
		MetaData meta = new MetaData(this);
		if (!name.contains("."))
		{
			name = meta.getCurrentSchema() + "." + name;
		}

		meta = new MetaData();

		// build operator tree
		final String schema = name.substring(0, name.indexOf('.'));
		final String table = name.substring(name.indexOf('.') + 1);
		final Transaction tx = new Transaction(Transaction.ISOLATION_UR);
		final List<String> keys = new ArrayList<String>();
		final List<String> types = new ArrayList<String>();
		final List<Boolean> orders = new ArrayList<Boolean>();
		keys.add(table + ".KEY2");
		String keyVal = null;
		if (key instanceof Integer)
		{
			types.add("INT");
			keyVal = ((Integer)key).toString();
		}
		else if (key instanceof Long)
		{
			types.add("LONG");
			keyVal = ((Long)key).toString();
		}
		else if (key instanceof Float)
		{
			types.add("FLOAT");
			key = new Double((Float)key);
			keyVal = ((Double)key).toString();
		}
		else if (key instanceof Double)
		{
			types.add("FLOAT");
			keyVal = ((Double)key).toString();
		}
		else if (key instanceof String)
		{
			types.add("CHAR");
			keyVal = "'" + (String)key + "'";
		}
		else if (key instanceof Date)
		{
			final GregorianCalendar cal = new GregorianCalendar();
			cal.setTime((Date)key);
			types.add("DATE");
			key = new MyDate(cal.get(Calendar.YEAR), cal.get(Calendar.MONTH) + 1, cal.get(Calendar.DAY_OF_MONTH));
			keyVal = "DATE('" + ((MyDate)key).toString() + "')";
		}
		orders.add(true);
		// determine node and device
		final List<Object> partial = new ArrayList<Object>();
		partial.add(key);
		final long hash = 0x7FFFFFFFFFFFFFFFL & hash(partial);
		final int node = (int)(hash % MetaData.numWorkerNodes);
		final int device = (int)(hash % MetaData.getNumDevices());
		final Index index = new Index(schema + ".PK" + table + ".indx", keys, types, orders);
		final IndexOperator iOp = new IndexOperator(index, meta);
		iOp.setNode(node);
		iOp.setDevice(device);
		iOp.getIndex().setTransaction(tx);
		iOp.getIndex().setCondition(new Filter(table + ".KEY2", "E", keyVal));
		final Map<String, Integer> cols2Pos = new HashMap<String, Integer>();
		cols2Pos.put(table + ".KEY2", 0);
		cols2Pos.put(table + ".VAL", 1);
		final Map<Integer, String> pos2Col = new TreeMap<Integer, String>();
		pos2Col.put(0, table + ".KEY2");
		pos2Col.put(1, table + ".VAL");
		final TableScanOperator tOp = new TableScanOperator(schema, table, meta, tx, true, cols2Pos, pos2Col);
		final List<Integer> devs = new ArrayList<Integer>();
		devs.add(device);
		tOp.addActiveDevices(devs);
		tOp.setChildForDevice(device, iOp);
		final List<String> needed = new ArrayList<String>();
		needed.add(table + ".VAL");
		tOp.setNeededCols(needed);
		tOp.setNode(node);
		tOp.setPhase2Done();
		tOp.setTransaction(tx);
		tOp.add(iOp);
		final NetworkSendOperator send = new NetworkSendOperator(node, meta);
		send.add(tOp);
		final NetworkReceiveOperator receive = new NetworkReceiveOperator(meta);
		receive.setNode(MetaData.myNodeNum());
		receive.add(send);
		final RootOperator root = new RootOperator(meta);
		root.add(receive);

		final Get2Thread thread = new Get2Thread(root);
		thread.start();
		tx.commitNoFlush();
		thread.join();
		if (!thread.getOK())
		{
			throw thread.getException();
		}

		return thread.getRetval();
	}

	private void doIsolation()
	{
		final byte[] arg = new byte[4];
		int count = 0;
		while (count < 4)
		{
			try
			{
				final int temp = sock.getInputStream().read(arg, count, 4 - count);
				if (temp == -1)
				{
					HRDBMSWorker.logger.debug("End of input stream when expecting isolation level argument");
					if (tx != null)
					{
						XAManager.rollback(tx);
						tx = null;
					}

					try
					{
						sock.close();
						if (worker != null)
						{
							final List<Object> cmd2 = new ArrayList<Object>(1);
							cmd2.add("CLOSE");
							worker.in.put(cmd2);
						}
						this.terminate();
					}
					catch (final Exception f)
					{
						if (worker != null)
						{
							final List<Object> cmd2 = new ArrayList<Object>(1);
							cmd2.add("CLOSE");
							worker.in.put(cmd2);
						}
						this.terminate();
					}
				}
				else
				{
					count += temp;
				}
			}
			catch (final Exception e)
			{
				// HRDBMSWorker.logger.debug("Terminating connection due to
				// exception", e);
				if (tx != null)
				{
					try
					{
						XAManager.rollback(tx);
						tx = null;
					}
					catch (final Exception f)
					{
						HRDBMSWorker.logger.error("", f);
					}
				}

				try
				{
					sock.close();
					if (worker != null)
					{
						final List<Object> cmd2 = new ArrayList<Object>(1);
						cmd2.add("CLOSE");
						worker.in.put(cmd2);
					}
					this.terminate();
				}
				catch (final Exception f)
				{
					if (worker != null)
					{
						final List<Object> cmd2 = new ArrayList<Object>(1);
						cmd2.add("CLOSE");
						while (true)
						{
							try
							{
								worker.in.put(cmd2);
								break;
							}
							catch (final Exception g)
							{
							}
						}
					}
					this.terminate();
				}
			}
		}

		int iso = ConnectionWorker.bytesToInt(arg);
		if (tx == null)
		{
			tx = new Transaction(Transaction.ISOLATION_CS);
		}

		if (iso == Connection.TRANSACTION_NONE)
		{
			iso = Transaction.ISOLATION_UR;
		}
		else if (iso == Connection.TRANSACTION_REPEATABLE_READ)
		{
			iso = Transaction.ISOLATION_RR;
		}
		else if (iso == Connection.TRANSACTION_SERIALIZABLE)
		{
			iso = Transaction.ISOLATION_RR;
		}
		else
		{
			iso = Transaction.ISOLATION_CS;
		}
		tx.setIsolationLevel(iso);
	}

	private void doNext()
	{
		final List<Object> al = new ArrayList<Object>();
		al.add("NEXT");

		final byte[] arg = new byte[4];
		int count = 0;
		while (count < 4)
		{
			try
			{
				final int temp = sock.getInputStream().read(arg, count, 4 - count);
				if (temp == -1)
				{
					HRDBMSWorker.logger.debug("Unexpected end of input when expecting argument to NEXT command");
					if (tx != null)
					{
						XAManager.rollback(tx);
						tx = null;
					}

					try
					{
						sock.close();
						if (worker != null)
						{
							final List<Object> cmd2 = new ArrayList<Object>(1);
							cmd2.add("CLOSE");
							worker.in.put(cmd2);
						}
						this.terminate();
					}
					catch (final Exception f)
					{
						if (worker != null)
						{
							final List<Object> cmd2 = new ArrayList<Object>(1);
							cmd2.add("CLOSE");
							worker.in.put(cmd2);
						}
						this.terminate();
					}
				}
				else
				{
					count += temp;
				}
			}
			catch (final Exception e)
			{
				// HRDBMSWorker.logger.debug("Terminating connection due to
				// exception", e);
				if (tx != null)
				{
					try
					{
						XAManager.rollback(tx);
						tx = null;
					}
					catch (final Exception f)
					{
						HRDBMSWorker.logger.error("", f);
					}
				}

				try
				{
					sock.close();
					if (worker != null)
					{
						final List<Object> cmd2 = new ArrayList<Object>(1);
						cmd2.add("CLOSE");
						worker.in.put(cmd2);
					}
					this.terminate();
				}
				catch (final Exception f)
				{
					if (worker != null)
					{
						final List<Object> cmd2 = new ArrayList<Object>(1);
						cmd2.add("CLOSE");
						while (true)
						{
							try
							{
								worker.in.put(cmd2);
								break;
							}
							catch (final Exception g)
							{
							}
						}
					}
					this.terminate();
				}
			}
		}

		int num = bytesToInt(arg);
		// HRDBMSWorker.logger.debug("NEXT command requested " + num + " rows");
		al.add(num);

		while (true)
		{
			try
			{
				worker.in.put(al);
				break;
			}
			catch (final InterruptedException e)
			{
			}
		}

		final List<Object> buffer = new ArrayList<Object>(num);
		while (num > 0)
		{
			Object obj = null;
			while (true)
			{
				try
				{
					obj = worker.out.take();
					break;
				}
				catch (final InterruptedException e)
				{
				}
			}

			buffer.add(obj);
			if (obj instanceof Exception || obj instanceof DataEndMarker)
			{
				break;
			}

			num--;
		}

		final Object obj = buffer.get(buffer.size() - 1);
		if (obj instanceof Exception)
		{
			sendNo();
			returnExceptionToClient((Exception)obj);
			return;
		}

		sendOK();
		try
		{
			for (final Object obj2 : buffer)
			{
				final byte[] data = toBytes(obj2);
				sock.getOutputStream().write(data);
			}

			sock.getOutputStream().flush();
		}
		catch (final Exception e)
		{
			// HRDBMSWorker.logger.debug("Terminating connection due to
			// exception", e);
			if (tx != null)
			{
				try
				{
					XAManager.rollback(tx);
					tx = null;
				}
				catch (final Exception f)
				{
					HRDBMSWorker.logger.error("", f);
				}
			}

			try
			{
				sock.close();
				if (worker != null)
				{
					final List<Object> cmd2 = new ArrayList<Object>(1);
					cmd2.add("CLOSE");
					worker.in.put(cmd2);
				}
				this.terminate();
			}
			catch (final Exception f)
			{
				if (worker != null)
				{
					final List<Object> cmd2 = new ArrayList<Object>(1);
					cmd2.add("CLOSE");
					while (true)
					{
						try
						{
							worker.in.put(cmd2);
							break;
						}
						catch (final Exception g)
						{
						}
					}
				}
				this.terminate();
			}
		}
	}

	private void doPut(String name, final Object key, final Object value) throws Exception
	{
		// qualify table name if not qualified
		final MetaData meta = new MetaData(this);
		if (!name.contains("."))
		{
			name = meta.getCurrentSchema() + "." + name;
		}

		final String schema = name.substring(0, name.indexOf('.'));
		final String table = name.substring(name.indexOf('.') + 1);
		final Transaction tx = new Transaction(Transaction.ISOLATION_CS);
		final Transaction tx2 = new Transaction(Transaction.ISOLATION_CS);

		// determine node and device
		final List<Object> partial = new ArrayList<Object>();
		partial.add(key);
		final long hash = 0x7FFFFFFFFFFFFFFFL & hash(partial);
		final int node = (int)(hash % MetaData.numWorkerNodes);
		final int device = (int)(hash % MetaData.getNumDevices());
		final SendPut2Thread thread = new SendPut2Thread(schema, table, node, device, key, tx, value, tx2);
		thread.start();
		tx.commitNoFlush();
		tx2.commitNoFlush();
		thread.join();
		if (!thread.getOK())
		{
			throw thread.getException();
		}
	}

	private void doRemove(String name, final Object key) throws Exception
	{
		// qualify table name if not qualified
		final MetaData meta = new MetaData(this);
		if (!name.contains("."))
		{
			name = meta.getCurrentSchema() + "." + name;
		}

		final String schema = name.substring(0, name.indexOf('.'));
		final String table = name.substring(name.indexOf('.') + 1);
		final Transaction tx = new Transaction(Transaction.ISOLATION_CS);
		/*
		 * ArrayList<String> keys = new ArrayList<String>(); ArrayList<String>
		 * types = new ArrayList<String>(); ArrayList<Boolean> orders = new
		 * ArrayList<Boolean>(); keys.add(table + ".KEY"); if (key instanceof
		 * Integer) { types.add("INT"); } else if (key instanceof Long) {
		 * types.add("LONG"); } else if (key instanceof Float) {
		 * types.add("FLOAT"); key = new Double((Float)key); } else if (key
		 * instanceof Double) { types.add("FLOAT"); } else if (key instanceof
		 * String) { types.add("CHAR"); } else if (key instanceof Date) {
		 * GregorianCalendar cal = new GregorianCalendar();
		 * cal.setTime((Date)key); types.add("DATE"); key = new
		 * MyDate(cal.get(Calendar.YEAR), cal.get(Calendar.MONTH) + 1,
		 * cal.get(Calendar.DAY_OF_MONTH)); } orders.add(true);
		 */
		// determine node and device
		final List<Object> partial = new ArrayList<Object>();
		partial.add(key);
		final long hash = 0x7FFFFFFFFFFFFFFFL & hash(partial);
		final int node = (int)(hash % MetaData.numWorkerNodes);
		final int device = (int)(hash % MetaData.getNumDevices());
		final SendRemove2Thread thread = new SendRemove2Thread(schema, table, node, device, key, tx);
		thread.start();
		tx.commitNoFlush();
		thread.join();
		if (!thread.getOK())
		{
			throw thread.getException();
		}
	}

	private void executeQuery()
	{
		// HRDBMSWorker.logger.debug("Entered executeQuery()");
		final byte[] bLen = new byte[4];
		read(bLen);
		// HRDBMSWorker.logger.debug("Read SQL length");
		final int len = bytesToInt(bLen);
		// HRDBMSWorker.logger.debug("Length is " + len);
		final byte[] bStmt = new byte[len];
		read(bStmt);
		// HRDBMSWorker.logger.debug("Read sql");
		String sql = null;
		try
		{
			sql = new String(bStmt, StandardCharsets.UTF_8);
			// HRDBMSWorker.logger.debug("SQL is " + sql);
		}
		catch (final Exception e)
		{
		}

		if (tx == null)
		{
			tx = new Transaction(Transaction.ISOLATION_CS);
		}

		try
		{
			if (worker != null)
			{
				final List<Object> cmd2 = new ArrayList<Object>(1);
				cmd2.add("CLOSE");
				while (true)
				{
					try
					{
						worker.in.put(cmd2);
						break;
					}
					catch (final Exception g)
					{
					}
				}
			}

			// HRDBMSWorker.logger.debug("About to create XAWorker");
			worker = XAManager.executeQuery(sql, tx, this);
			// HRDBMSWorker.logger.debug("XAWorker created");
			worker.start();
			// HRDBMSWorker.logger.debug("XAWorker started");
			this.sendOK();
			// HRDBMSWorker.logger.debug("OK sent to client");
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.debug("Exception during executeQuery()", e);
			this.sendNo();
			returnExceptionToClient(e);
		}
	}

	private void executeUpdate()
	{
		final byte[] bLen = new byte[4];
		read(bLen);
		final byte[] bStmt = new byte[bytesToInt(bLen)];
		read(bStmt);
		String sql = null;
		try
		{
			sql = new String(bStmt, StandardCharsets.UTF_8);
		}
		catch (final Exception e)
		{
		}

		if (tx == null)
		{
			tx = new Transaction(Transaction.ISOLATION_CS);
		}

		try
		{
			if (worker != null)
			{
				final List<Object> cmd2 = new ArrayList<Object>(1);
				cmd2.add("CLOSE");
				while (true)
				{
					try
					{
						worker.in.put(cmd2);
						worker = null;
						break;
					}
					catch (final Exception g)
					{
					}
				}
			}

			if (delayedDML.contains(this))
			{
				delayedTxs.put(tx.number(), tx.number());
				worker = XAManager.executeUpdate(sql, tx, this);
				delayedWorkers.multiPut(this, worker);
				worker.start();
				worker = null;
				return;
			}
			else
			{
				worker = XAManager.executeUpdate(sql, tx, this);
				worker.start();
			}

			worker.join();
			final int updateCount = worker.getUpdateCount();

			if (updateCount == -1)
			{
				sendNo();
				returnExceptionToClient(worker.getException());
				worker = null;
				return;
			}
			worker = null;
			this.sendOK();
			sendInt(updateCount);
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.debug("", e);
			this.sendNo();
			returnExceptionToClient(e);
			worker = null;
		}
	}

	private void get()
	{
		final byte[] tableLen = new byte[4];
		byte[] tableBytes;
		String table;
		Object key;
		try
		{
			readNonCoord(tableLen);
			tableBytes = new byte[bytesToInt(tableLen)];
			readNonCoord(tableBytes);
			table = new String(tableBytes, StandardCharsets.UTF_8);
			final ObjectInputStream objIn = new ObjectInputStream(sock.getInputStream());
			key = objIn.readObject();
			;
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.debug("", e);
			sendNo();
			returnExceptionToClient(e);
			return;
		}

		try
		{
			final Object retval = doGet(table, key);
			sendOK();
			final ObjectOutputStream objOut = new ObjectOutputStream(sock.getOutputStream());
			objOut.writeObject(retval);
			objOut.flush();
			sock.getOutputStream().flush();
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.debug("", e);
			sendNo();
			returnExceptionToClient(e);
		}
	}

	private void getLoadMetaData() throws Exception
	{
		final byte[] slBytes = new byte[4];
		readNonCoord(slBytes);
		int sLen = bytesToInt(slBytes);
		byte[] data = new byte[sLen];
		readNonCoord(data);
		final String schema = new String(data, StandardCharsets.UTF_8);
		readNonCoord(slBytes);
		sLen = bytesToInt(slBytes);
		data = new byte[sLen];
		readNonCoord(data);
		final String table = new String(data, StandardCharsets.UTF_8);
		final LoadMetaData ldmd = ldmds.get(schema + "." + table);
		final ObjectOutputStream objOut = new ObjectOutputStream(sock.getOutputStream());
		// numNodes = (Integer)objIn.readObject();
		// delimiter = (String)objIn.readObject();
		// pos2Col = (Map<Integer, String>)objIn.readObject();
		// cols2Types = (Map<String, String>)objIn.readObject();
		// pos2Length = (Map<Integer, Integer>)objIn.readObject();
		// pmd = (PartitionMetaData)objIn.readObject();
		objOut.writeObject(new Integer(ldmd.numNodes));
		objOut.writeObject(ldmd.delimiter);
		objOut.writeObject(ldmd.pos2Col);
		objOut.writeObject(ldmd.cols2Types);
		objOut.writeObject(ldmd.pos2Length);
		objOut.writeObject(ldmd.pmd);
		objOut.writeObject(ldmd.workerNodes);
		objOut.writeObject(ldmd.coordNodes);
		objOut.flush();
		objOut.close();
		sock.close();
	}

	private void getRSMeta()
	{
		try
		{
			if (worker == null)
			{
				HRDBMSWorker.logger.debug("ResultSetMetaData was requested when no result set existed");
				closeConnection();
				this.terminate();
				return;
			}

			ArrayList<Object> cmd2 = new ArrayList<Object>(1);
			cmd2.add("META");
			while (true)
			{
				try
				{
					worker.in.put(cmd2);
					break;
				}
				catch (final Exception g)
				{
				}
			}

			final List<Object> buffer = new ArrayList<Object>();
			Object obj = worker.out.take();
			if (!(obj instanceof HashMap))
			{
				buffer.add(obj);
				obj = worker.out.take();
				while (!(obj instanceof HashMap))
				{
					buffer.add(obj);
					obj = worker.out.take();
				}
			}

			final ObjectOutputStream objOut = new ObjectOutputStream(sock.getOutputStream());
			objOut.writeObject(obj);
			obj = worker.out.take();
			objOut.writeObject(obj);
			obj = worker.out.take();
			objOut.writeObject(obj);
			objOut.flush();

			obj = worker.out.peek();
			if (obj != null)
			{
				cmd2 = new ArrayList<Object>(1);
				cmd2.add("CLOSE");
				while (true)
				{
					try
					{
						worker.in.put(cmd2);
						worker = null;
						break;
					}
					catch (final Exception g)
					{
					}
				}

				return;
			}

			if (buffer.size() > 0)
			{
				for (final Object o : buffer)
				{
					worker.out.put(o);
				}
			}
		}
		catch (final Exception e)
		{
			final List<Object> cmd2 = new ArrayList<Object>(1);
			cmd2.add("CLOSE");
			while (true)
			{
				try
				{
					worker.in.put(cmd2);
					worker = null;
					break;
				}
				catch (final Exception g)
				{
				}
			}
		}
	}

	private void getSchema()
	{
		final String schema = new MetaData(this).getCurrentSchema();
		sendString(schema);
	}

	private void hadoopLoad()
	{
		// ArrayList<List<Object>> list;
		ByteBuffer list;
		Map<String, Integer> cols2Pos;
		Map<Integer, String> pos2Col;
		String schema;
		String table;
		final byte[] devBytes = new byte[4];
		int device;
		try
		{
			sock.getInputStream();
			readNonCoord(devBytes);
			device = bytesToInt(devBytes);
			final byte[] length = new byte[4];
			readNonCoord(length);
			final int len = bytesToInt(length);
			final byte[] data = new byte[len];
			readNonCoord(data);
			final String tableName = new String(data, StandardCharsets.UTF_8);
			// list = readRS();
			list = readRawRS();
			final LoadMetaData ldmd = ldmds.get(tableName);
			pos2Col = ldmd.pos2Col;
			cols2Pos = new HashMap<String, Integer>();
			for (final Map.Entry entry : pos2Col.entrySet())
			{
				cols2Pos.put((String)entry.getValue(), (Integer)entry.getKey());
			}
			schema = tableName.substring(0, tableName.indexOf('.'));
			table = tableName.substring(tableName.indexOf('.') + 1);
		}
		catch (final Exception e)
		{
			sendNo();
			return;
		}

		// FlushLoadThread thread = new FlushLoadThread(list, new
		// Transaction(txNum), schema, table, keys, types, orders, indexes,
		// cols2Pos, device, pos2Col, cols2Types, type);
		// thread.run();

		// boolean allOK = thread.getOK();
		try
		{
			// MetaData meta = new MetaData();
			FileChannel fc = loadFCs.get(MetaData.getDevicePath(device) + schema + "." + table + ".tmp");
			if (fc == null)
			{
				final RandomAccessFile raf = new RandomAccessFile(MetaData.getDevicePath(device) + schema + "." + table + ".tmp", "rw");
				fc = raf.getChannel();
				loadFCs.put(MetaData.getDevicePath(device) + schema + "." + table + ".tmp", fc);
			}

			fc.write(list);
		}
		catch (final Exception e)
		{
			sendNo();
			return;
		}

		sendOK();
		return;
	}

	private void insert() throws Exception
	{
		final byte[] txBytes = new byte[8];
		long txNum = -1;
		final byte[] schemaLenBytes = new byte[4];
		final byte[] tableLenBytes = new byte[4];
		int schemaLength = -1;
		int tableLength = -1;
		String schema = null;
		String table = null;
		byte[] schemaData = null;
		byte[] tableData = null;
		List<String> indexes;
		List<List<String>> keys;
		List<List<String>> types;
		List<List<Boolean>> orders;
		List<List<Object>> list = null;
		Map<String, Integer> cols2Pos;
		PartitionMetaData partMeta;
		Map<Integer, String> pos2Col = null;
		Map<String, String> cols2Types = null;
		int type;
		String ngExp;
		String nExp;
		String dExp;
		try
		{
			final InputStream in = new BufferedInputStream(sock.getInputStream());
			readNonCoord(txBytes, in);
			txNum = bytesToLong(txBytes);
			readNonCoord(schemaLenBytes, in);
			schemaLength = bytesToInt(schemaLenBytes);
			schemaData = new byte[schemaLength];
			readNonCoord(schemaData, in);
			schema = new String(schemaData, StandardCharsets.UTF_8);
			readNonCoord(tableLenBytes, in);
			tableLength = bytesToInt(tableLenBytes);
			tableData = new byte[tableLength];
			readNonCoord(tableData, in);
			table = new String(tableData, StandardCharsets.UTF_8);
			readNonCoord(tableLenBytes, in);
			type = bytesToInt(tableLenBytes);

			readNonCoord(tableLenBytes, in);
			tableLength = bytesToInt(tableLenBytes);
			tableData = new byte[tableLength];
			readNonCoord(tableData, in);
			ngExp = new String(tableData, StandardCharsets.UTF_8);
			readNonCoord(tableLenBytes, in);
			tableLength = bytesToInt(tableLenBytes);
			tableData = new byte[tableLength];
			readNonCoord(tableData, in);
			nExp = new String(tableData, StandardCharsets.UTF_8);
			readNonCoord(tableLenBytes, in);
			tableLength = bytesToInt(tableLenBytes);
			tableData = new byte[tableLength];
			readNonCoord(tableData, in);
			dExp = new String(tableData, StandardCharsets.UTF_8);
			final Map<Long, Object> prev = new HashMap<Long, Object>();
			indexes = OperatorUtils.deserializeALS(in, prev);
			list = OperatorUtils.deserializeALALO(in, prev);
			keys = OperatorUtils.deserializeALALS(in, prev);
			types = OperatorUtils.deserializeALALS(in, prev);
			orders = OperatorUtils.deserializeALALB(in, prev);
			cols2Pos = OperatorUtils.deserializeStringIntHM(in, prev);
			pos2Col = OperatorUtils.deserializeTM(in, prev);
			cols2Types = OperatorUtils.deserializeStringHM(in, prev);
			// ObjectInputStream objIn = new
			// ObjectInputStream(sock.getInputStream());
			// indexes = (List<String>)objIn.readObject();
			// list = (List<List<Object>>)objIn.readObject();
			// keys = (List<List<String>>)objIn.readObject();
			// types = (List<List<String>>)objIn.readObject();
			// orders = (List<List<Boolean>>)objIn.readObject();
			// cols2Pos = (Map<String, Integer>)objIn.readObject();
			// pos2Col = (Map<Integer, String>)objIn.readObject();
			// cols2Types = (Map<String, String>)objIn.readObject();

			final Map<String, String> cols2Types2 = new HashMap<String, String>();
			for (final Map.Entry entry : cols2Types.entrySet())
			{
				String col = (String)entry.getKey();
				col = col.substring(col.indexOf('.') + 1);
				cols2Types2.put(col, (String)entry.getValue());
			}

			partMeta = new PartitionMetaData(schema, table, ngExp, nExp, dExp, new Transaction(txNum), cols2Types2);
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.debug("", e);
			sendNo();
			return;
		}

		final HJOMultiHashMap<Integer, List<Object>> map = new HJOMultiHashMap<Integer, List<Object>>();
		for (final List<Object> row : list)
		{
			map.multiPut(MetaData.determineDevice(row, partMeta, cols2Pos), row);
		}

		final List<FlushInsertThread> threads = new ArrayList<FlushInsertThread>();
		final List<String> dmlTxStrs = new ArrayList<String>();
		final List<Integer> sorted = new ArrayList(map.getKeySet());
		Collections.sort(sorted);
		for (final Object o : sorted)
		{
			final int device = (Integer)o;
			threads.add(new FlushInsertThread(map.get(device), new Transaction(txNum), schema, table, keys, types, orders, indexes, cols2Pos, device, pos2Col, cols2Types, type));
			final String dmlTxStr = Long.toString(txNum) + "~" + device + "~" + schema + "." + table;
			if (dmlTx.putIfAbsent(dmlTxStr, dmlTxStr) != null)
			{
				while (dmlTx.putIfAbsent(dmlTxStr, dmlTxStr) != null)
				{
					LockSupport.parkNanos(1);
				}
			}

			dmlTxStrs.add(dmlTxStr);
		}

		for (final FlushInsertThread thread : threads)
		{
			thread.start();
		}

		boolean allOK = true;
		for (final FlushInsertThread thread : threads)
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
			if (!thread.getOK())
			{
				allOK = false;
			}
		}

		for (final String s : dmlTxStrs)
		{
			dmlTx.remove(s);
		}

		if (allOK)
		{
			sendOK();
			return;
		}
		else
		{
			sendNo();
			return;
		}
	}

	private void load()
	{
		final byte[] txBytes = new byte[8];
		long txNum = -1;
		final byte[] schemaLenBytes = new byte[4];
		final byte[] tableLenBytes = new byte[4];
		int schemaLength = -1;
		int tableLength = -1;
		String schema = null;
		String table = null;
		byte[] schemaData = null;
		byte[] tableData = null;
		// ArrayList<List<Object>> list = null;
		ByteBuffer list = null;
		Map<Integer, String> pos2Col = null;
		Map<String, String> cols2Types = null;
		int type;
		final byte[] devBytes = new byte[4];
		int device;
		InputStream in2 = null;
		try
		{
			in2 = new BufferedInputStream(sock.getInputStream());
			readNonCoord(txBytes, in2);
			txNum = bytesToLong(txBytes);
			readNonCoord(devBytes, in2);
			device = bytesToInt(devBytes);
			readNonCoord(schemaLenBytes, in2);
			schemaLength = bytesToInt(schemaLenBytes);
			schemaData = new byte[schemaLength];
			readNonCoord(schemaData, in2);
			schema = new String(schemaData, StandardCharsets.UTF_8);
			readNonCoord(tableLenBytes, in2);
			tableLength = bytesToInt(tableLenBytes);
			tableData = new byte[tableLength];
			readNonCoord(tableData, in2);
			table = new String(tableData, StandardCharsets.UTF_8);
			readNonCoord(tableLenBytes, in2);
			type = bytesToInt(tableLenBytes);
			// list = readRS();
			list = readRawRS(in2);
			final ObjectInputStream objIn = new ObjectInputStream(in2);
			objIn.readObject();
			objIn.readObject();
			objIn.readObject();
			objIn.readObject();
			objIn.readObject();
			pos2Col = (Map<Integer, String>)objIn.readObject();
			cols2Types = (Map<String, String>)objIn.readObject();

			final Exception le = loadExceptions.get(txNum);
			if (le != null)
			{
				HRDBMSWorker.logger.debug("", le);
				loadExceptions.remove(txNum);
				sendNo();
				return;
			}
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.debug("", e);
			sendNo();
			return;
		}

		final Transaction newTx = new Transaction(txNum);
		final FlushLoadThread thread = new FlushLoadThread(newTx, schema, table, device, pos2Col, cols2Types, type);

		if (flThreads.putIfAbsent(thread, thread) != null)
		{
			while (flThreads.putIfAbsent(thread, thread) != null)
			{
				LockSupport.parkNanos(500);
			}
		}
		// thread.run();
		// boolean allOK = thread.getOK();

		try
		{
			// MetaData meta = new MetaData();
			FileChannel fc = loadFCs.get(MetaData.getDevicePath(device) + schema + "." + table + ".tmp");
			if (fc == null)
			{
				final RandomAccessFile raf = new RandomAccessFile(MetaData.getDevicePath(device) + schema + "." + table + ".tmp", "rw");
				fc = raf.getChannel();
				loadFCs.put(MetaData.getDevicePath(device) + schema + "." + table + ".tmp", fc);
			}

			fc.write(list);
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.debug("", e);
			sendNo();
			flThreads.remove(thread);
			return;
		}

		sendOK();
		try
		{
			flThreads.remove(thread);
		}
		catch (final Exception e)
		{
			loadExceptions.put(newTx.number(), e);
		}
		try
		{
			in2.close();
			sock.close();
		}
		catch (final Exception e)
		{
		}
		this.terminate();
		return;
	}

	private void localCommit()
	{
		List<Object> tree = null;
		final byte[] txBytes = new byte[8];
		long txNum = -1;
		try
		{
			readNonCoord(txBytes);
			txNum = bytesToLong(txBytes);
			final ObjectInputStream objIn = new ObjectInputStream(sock.getInputStream());
			tree = (List<Object>)objIn.readObject();

			final Exception le = loadExceptions.get(txNum);
			if (le != null)
			{
				loadExceptions.remove(txNum);
				sendNo();
				return;
			}
		}
		catch (final Exception e)
		{
			sendNo();
			return;
		}

		sendOK();
		final Transaction tx = new Transaction(txNum);
		try
		{
			tx.commit();
		}
		catch (final Exception e)
		{
			// TODO queueCommandSelf("COMMIT", tx);
			// TODO blackListSelf();
			HRDBMSWorker.logger.fatal("BLACKLIST", e);
			System.exit(1);
		}

		Object obj = tree.get(0);
		while (obj instanceof ArrayList)
		{
			obj = ((List)obj).get(0);
		}

		removeFromTree((String)obj, tree, null); // also delete parents if now
		// empty

		for (final Object o : tree)
		{
			new SendCommitThread(o, tx).start();
		}
	}

	private void localRollback()
	{
		List<Object> tree = null;
		final byte[] txBytes = new byte[8];
		long txNum = -1;
		try
		{
			readNonCoord(txBytes);
			txNum = bytesToLong(txBytes);
			final ObjectInputStream objIn = new ObjectInputStream(sock.getInputStream());
			tree = (List<Object>)objIn.readObject();
		}
		catch (final Exception e)
		{
			sendNo();
			return;
		}

		sendOK();
		final Transaction tx = new Transaction(txNum);
		try
		{
			tx.rollback();
		}
		catch (final Exception e)
		{
			// TODO queueCommandSelf("ROLLBACK", tx);
			// TODO blackListSelf();
			HRDBMSWorker.logger.fatal("BLACKLIST", e);
			System.exit(1);
		}

		Object obj = tree.get(0);
		while (obj instanceof ArrayList)
		{
			obj = ((List)obj).get(0);
		}

		removeFromTree((String)obj, tree, null); // also delete parents if now
		// empty

		for (final Object o : tree)
		{
			new SendRollbackThread(o, tx).start();
		}
	}

	private void massDelete()
	{
		List<Object> tree = null;
		final byte[] txBytes = new byte[8];
		long txNum = -1;
		final byte[] schemaLenBytes = new byte[4];
		final byte[] tableLenBytes = new byte[4];
		int schemaLength = -1;
		int tableLength = -1;
		String schema = null;
		String table = null;
		byte[] schemaData = null;
		byte[] tableData = null;
		List<String> indexes;
		List<List<String>> keys;
		List<List<String>> types;
		List<List<Boolean>> orders;
		Map<Integer, String> pos2Col;
		Map<String, String> cols2Types;
		int type;
		boolean logged;

		try
		{
			readNonCoord(txBytes);
			txNum = bytesToLong(txBytes);
			tx = new Transaction(txNum);
			readNonCoord(schemaLenBytes);
			schemaLength = bytesToInt(schemaLenBytes);
			schemaData = new byte[schemaLength];
			readNonCoord(schemaData);
			schema = new String(schemaData, StandardCharsets.UTF_8);
			readNonCoord(tableLenBytes);
			tableLength = bytesToInt(tableLenBytes);
			tableData = new byte[tableLength];
			readNonCoord(tableData);
			table = new String(tableData, StandardCharsets.UTF_8);
			final byte[] flag = new byte[1];
			readNonCoord(flag);
			if (flag[0] == (byte)0)
			{
				logged = false;
			}
			else
			{
				logged = true;
			}
			final byte[] typeData = new byte[4];
			readNonCoord(typeData);
			type = bytesToInt(typeData);
			final ObjectInputStream objIn = new ObjectInputStream(sock.getInputStream());
			tree = (List<Object>)objIn.readObject();
			indexes = (List<String>)objIn.readObject();
			keys = (List<List<String>>)objIn.readObject();
			types = (List<List<String>>)objIn.readObject();
			orders = (List<List<Boolean>>)objIn.readObject();
			pos2Col = (Map<Integer, String>)objIn.readObject();
			cols2Types = (Map<String, String>)objIn.readObject();
		}
		catch (final Exception e)
		{
			sendNo();
			return;
		}

		Object obj = tree.get(0);
		while (obj instanceof ArrayList)
		{
			obj = ((List)obj).get(0);
		}

		removeFromTree((String)obj, tree, null); // also delete parents if now
		// empty

		final List<SendMassDeleteThread> threads = new ArrayList<SendMassDeleteThread>();
		for (Object o : tree)
		{
			if (!(o instanceof ArrayList))
			{
				final List<Object> o2 = new ArrayList<Object>();
				o2.add(o);
				o = o2;
			}
			threads.add(new SendMassDeleteThread((List<Object>)o, tx, schema, table, keys, types, orders, indexes, pos2Col, cols2Types, logged, type));
		}

		for (final SendMassDeleteThread thread : threads)
		{
			thread.start();
		}

		boolean allOK = true;
		for (final SendMassDeleteThread thread : threads)
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
			if (!thread.getOK())
			{
				allOK = false;
			}
		}

		if (allOK)
		{
			try
			{
				// mass delete table and indexes
				final File[] dirs = getDirs(HRDBMSWorker.getHParms().getProperty("data_directories"));
				final List<MassDeleteThread> threads1 = new ArrayList<MassDeleteThread>();
				final List<String> dmlTxStrs = new ArrayList<String>();
				int device = 0;
				for (final File dir : dirs)
				{
					final File dir2 = new File(dir, schema + "." + table + ".tbl");
					threads1.add(new MassDeleteThread(dir2, tx, indexes, keys, types, orders, pos2Col, cols2Types, logged, type));
					final String dmlTxStr = Long.toString(tx.number()) + "~" + device + "~" + schema + "." + table;
					if (dmlTx.putIfAbsent(dmlTxStr, dmlTxStr) != null)
					{
						while (dmlTx.putIfAbsent(dmlTxStr, dmlTxStr) != null)
						{
							LockSupport.parkNanos(1);
						}
					}

					dmlTxStrs.add(dmlTxStr);
					device++;
				}

				for (final MassDeleteThread thread : threads1)
				{
					thread.start();
				}

				allOK = true;
				for (final MassDeleteThread thread : threads1)
				{
					while (true)
					{
						try
						{
							thread.join();
							break;
						}
						catch (final Exception e)
						{
						}
					}

					if (!thread.getOK())
					{
						allOK = false;
					}
				}

				for (final String s : dmlTxStrs)
				{
					dmlTx.remove(s);
				}

				if (!allOK)
				{
					sendNo();
					return;
				}

				sendOK();
				int num = 0;
				for (final MassDeleteThread thread : threads1)
				{
					num += thread.getNum();
				}
				sendInt(num);
			}
			catch (final Exception e)
			{
				sendNo();
				return;
			}
		}
		else
		{
			sendNo();
			try
			{
				sock.close();
			}
			catch (final Exception f)
			{
			}
		}
	}

	private void newIndex()
	{
		final byte[] fnLenBytes = new byte[4];
		int fnLen;
		byte[] fnBytes;
		String fn;
		final byte[] ncBytes = new byte[4];
		int numCols;
		final byte[] uBytes = new byte[4];
		int unique;
		List<Integer> devices;
		List<Object> tree;
		Transaction tx;
		final byte[] txBytes = new byte[8];

		try
		{
			readNonCoord(txBytes);
			tx = new Transaction(bytesToLong(txBytes));
			readNonCoord(ncBytes);
			numCols = bytesToInt(ncBytes);
			readNonCoord(uBytes);
			unique = bytesToInt(uBytes);
			readNonCoord(fnLenBytes);
			fnLen = bytesToInt(fnLenBytes);
			fnBytes = new byte[fnLen];
			readNonCoord(fnBytes);
			fn = new String(fnBytes, StandardCharsets.UTF_8);
			final ObjectInputStream objIn = new ObjectInputStream(sock.getInputStream());
			devices = (List<Integer>)objIn.readObject();
			tree = (List<Object>)objIn.readObject();

			final List<CreateIndexThread> threads = new ArrayList<CreateIndexThread>();
			for (final int device : devices)
			{
				threads.add(new CreateIndexThread(fn, numCols, device, unique, tx));
			}

			for (final CreateIndexThread thread : threads)
			{
				thread.start();
			}

			// ///////////////////////////////////////////
			Object obj = tree.get(0);
			while (obj instanceof ArrayList)
			{
				obj = ((List)obj).get(0);
			}

			removeFromTree((String)obj, tree, null); // also delete parents if
			// now empty

			final List<SendHierNewIndexThread> threads2 = new ArrayList<SendHierNewIndexThread>();
			for (final Object o : tree)
			{
				threads2.add(new SendHierNewIndexThread(ncBytes, uBytes, fnLenBytes, fnBytes, devices, o, tx));
			}

			for (final SendHierNewIndexThread thread : threads2)
			{
				thread.start();
			}

			boolean allOK = true;
			for (final SendHierNewIndexThread thread : threads2)
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
				if (!thread.getOK())
				{
					allOK = false;
				}
			}
			// /////////////////////////////

			for (final CreateIndexThread thread : threads)
			{
				thread.join();
				if (!thread.getOK())
				{
					allOK = false;
				}
			}

			if (allOK)
			{
				sendOK();
			}
			else
			{
				sendNo();
				try
				{
					sock.close();
				}
				catch (final Exception f)
				{
				}
			}
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.debug("", e);
			sendNo();
			return;
		}
	}

	private void newTable()
	{
		final byte[] fnLenBytes = new byte[4];
		int fnLen;
		byte[] fnBytes;
		String fn;
		final byte[] ncBytes = new byte[4];
		int numCols;
		List<Integer> devices;
		List<Object> tree;
		Transaction tx;
		final byte[] txBytes = new byte[8];
		int type;
		List<ColDef> defs = null;
		List<Integer> colOrder = null;
		List<Integer> organization = null;

		try
		{
			readNonCoord(txBytes);
			tx = new Transaction(bytesToLong(txBytes));
			readNonCoord(ncBytes);
			numCols = bytesToInt(ncBytes);
			readNonCoord(fnLenBytes);
			fnLen = bytesToInt(fnLenBytes);
			fnBytes = new byte[fnLen];
			readNonCoord(fnBytes);
			fn = new String(fnBytes, StandardCharsets.UTF_8);
			readNonCoord(fnLenBytes);
			type = bytesToInt(fnLenBytes);
			final ObjectInputStream objIn = new ObjectInputStream(sock.getInputStream());
			devices = (List<Integer>)objIn.readObject();
			tree = (List<Object>)objIn.readObject();
			defs = (List<ColDef>)objIn.readObject();
			if (type != 0)
			{
				colOrder = (List<Integer>)objIn.readObject();
				organization = (List<Integer>)objIn.readObject();
				HRDBMSWorker.logger.debug("Received message to create table with organization: " + organization);
			}

			final List<CreateTableThread> threads = new ArrayList<CreateTableThread>();
			for (final int device : devices)
			{
				threads.add(new CreateTableThread(fn, numCols, device, tx, type, defs, colOrder, organization));
			}

			for (final CreateTableThread thread : threads)
			{
				thread.start();
			}

			// ///////////////////////////////////////////
			Object obj = tree.get(0);
			while (obj instanceof ArrayList)
			{
				obj = ((List)obj).get(0);
			}

			removeFromTree((String)obj, tree, null); // also delete parents if
			// now empty

			final List<SendHierNewTableThread> threads2 = new ArrayList<SendHierNewTableThread>();
			for (final Object o : tree)
			{
				threads2.add(new SendHierNewTableThread(ncBytes, fnLenBytes, fnBytes, devices, o, tx, type, defs, colOrder, organization));
			}

			for (final SendHierNewTableThread thread : threads2)
			{
				thread.start();
			}

			boolean allOK = true;
			for (final SendHierNewTableThread thread : threads2)
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
				if (!thread.getOK())
				{
					allOK = false;
				}
			}
			// /////////////////////////////

			for (final CreateTableThread thread : threads)
			{
				thread.join();
				if (!thread.getOK())
				{
					allOK = false;
				}
			}

			if (allOK)
			{
				sendOK();
			}
			else
			{
				sendNo();
				try
				{
					sock.close();
				}
				catch (final Exception f)
				{
				}
			}
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.debug("", e);
			HRDBMSWorker.logger.debug("Sending NO");
			sendNo();
			return;
		}
	}

	private void popIndex()
	{
		final byte[] fnLenBytes = new byte[4];
		final byte[] fn2LenBytes = new byte[4];
		int fnLen;
		byte[] fnBytes;
		byte[] fn2Bytes;
		String iFn;
		String tFn;
		List<Integer> devices;
		List<String> keys;
		List<String> types;
		List<Boolean> orders;
		List<Integer> poses;
		Map<Integer, String> pos2Col;
		Map<String, String> cols2Types;
		final byte[] txBytes = new byte[8];
		long txnum;
		List<Object> tree;
		int type;

		try
		{
			readNonCoord(txBytes);
			txnum = bytesToLong(txBytes);
			readNonCoord(fnLenBytes);
			fnLen = bytesToInt(fnLenBytes);
			fnBytes = new byte[fnLen];
			readNonCoord(fnBytes);
			iFn = new String(fnBytes, StandardCharsets.UTF_8);
			readNonCoord(fn2LenBytes);
			fnLen = bytesToInt(fn2LenBytes);
			fn2Bytes = new byte[fnLen];
			readNonCoord(fn2Bytes);
			tFn = new String(fn2Bytes, StandardCharsets.UTF_8);
			readNonCoord(fnLenBytes);
			type = bytesToInt(fnLenBytes);
			final ObjectInputStream objIn = new ObjectInputStream(sock.getInputStream());
			devices = (List<Integer>)objIn.readObject();
			keys = (List<String>)objIn.readObject();
			types = (List<String>)objIn.readObject();
			orders = (List<Boolean>)objIn.readObject();
			poses = (List<Integer>)objIn.readObject();
			pos2Col = (Map<Integer, String>)objIn.readObject();
			cols2Types = (Map<String, String>)objIn.readObject();
			tree = (List<Object>)objIn.readObject();
			tx = new Transaction(txnum);

			final List<PopIndexThread> threads = new ArrayList<PopIndexThread>();
			for (final int device : devices)
			{
				threads.add(new PopIndexThread(iFn, tFn, device, keys, types, orders, poses, pos2Col, cols2Types, tx, type));
			}

			for (final PopIndexThread pop : threads)
			{
				pop.start();
			}

			// ///////////////////////////////////////////
			Object obj = tree.get(0);
			while (obj instanceof ArrayList)
			{
				obj = ((List)obj).get(0);
			}

			removeFromTree((String)obj, tree, null); // also delete parents if
			// now empty

			final List<SendHierPopIndexThread> threads2 = new ArrayList<SendHierPopIndexThread>();
			for (final Object o : tree)
			{
				threads2.add(new SendHierPopIndexThread(txBytes, fnLenBytes, fnBytes, fn2LenBytes, fn2Bytes, devices, keys, types, orders, poses, pos2Col, cols2Types, o, type));
			}

			for (final SendHierPopIndexThread thread : threads2)
			{
				thread.start();
			}

			boolean allOK = true;
			for (final SendHierPopIndexThread thread : threads2)
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
				if (!thread.getOK())
				{
					allOK = false;
				}
			}
			// /////////////////////////////

			for (final PopIndexThread pop : threads)
			{
				pop.join();
				if (!pop.getOK())
				{
					allOK = false;
				}
			}

			if (allOK)
			{
				sendOK();
			}
			else
			{
				sendNo();
			}
		}
		catch (final Exception e)
		{
			sendNo();
			return;
		}
	}

	private void prepare()
	{
		List<Object> tree = null;
		final byte[] txBytes = new byte[8];
		long txNum = -1;
		final byte[] lenBytes = new byte[4];
		int length = -1;
		String host = null;
		byte[] data = null;
		try
		{
			readNonCoord(txBytes);
			txNum = bytesToLong(txBytes);
			readNonCoord(lenBytes);
			length = bytesToInt(lenBytes);
			data = new byte[length];
			readNonCoord(data);
			host = new String(data, StandardCharsets.UTF_8);
			final ObjectInputStream objIn = new ObjectInputStream(sock.getInputStream());
			tree = (List<Object>)objIn.readObject();
		}
		catch (final Exception e)
		{
			sendNo();
			return;
		}

		final Transaction tx = new Transaction(txNum);
		try
		{
			tx.tryCommit(host);
		}
		catch (final Exception e)
		{
			sendNo();
			return;
		}

		Object obj = tree.get(0);
		while (obj instanceof ArrayList)
		{
			obj = ((List)obj).get(0);
		}

		removeFromTree((String)obj, tree, null); // also delete parents if now
		// empty

		final List<SendPrepareThread> threads = new ArrayList<SendPrepareThread>();
		for (final Object o : tree)
		{
			threads.add(new SendPrepareThread(o, tx, lenBytes, data));
		}

		for (final SendPrepareThread thread : threads)
		{
			thread.start();
		}

		boolean allOK = true;
		for (final SendPrepareThread thread : threads)
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
			if (!thread.sendOK())
			{
				allOK = false;
			}
		}

		if (allOK)
		{
			sendOK();
		}
		else
		{
			sendNo();
			try
			{
				sock.close();
			}
			catch (final Exception f)
			{
			}
		}
	}

	private void put()
	{
		final byte[] tableLen = new byte[4];
		byte[] tableBytes;
		String table;
		Object key;
		Object value;
		try
		{
			readNonCoord(tableLen);
			tableBytes = new byte[bytesToInt(tableLen)];
			readNonCoord(tableBytes);
			table = new String(tableBytes, StandardCharsets.UTF_8);
			final ObjectInputStream objIn = new ObjectInputStream(sock.getInputStream());
			key = objIn.readObject();
			value = objIn.readObject();
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.debug("", e);
			sendNo();
			returnExceptionToClient(e);
			return;
		}

		try
		{
			doPut(table, key, value);
			sendOK();
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.debug("", e);
			sendNo();
			returnExceptionToClient(e);
		}
	}

	private void put2()
	{
		final byte[] txBytes = new byte[8];
		long txNum = -1;
		final byte[] schemaLenBytes = new byte[4];
		final byte[] tableLenBytes = new byte[4];
		int schemaLength = -1;
		int tableLength = -1;
		String schema = null;
		String table = null;
		byte[] schemaData = null;
		byte[] tableData = null;
		final byte[] deviceBytes = new byte[4];
		int device;
		Object key;
		Object value;
		Transaction tx;
		Transaction tx2;
		try
		{
			readNonCoord(txBytes);
			txNum = bytesToLong(txBytes);
			readNonCoord(txBytes);
			tx2 = new Transaction(bytesToLong(txBytes));
			tx = new Transaction(txNum);
			readNonCoord(schemaLenBytes);
			schemaLength = bytesToInt(schemaLenBytes);
			schemaData = new byte[schemaLength];
			readNonCoord(schemaData);
			schema = new String(schemaData, StandardCharsets.UTF_8);
			readNonCoord(tableLenBytes);
			tableLength = bytesToInt(tableLenBytes);
			tableData = new byte[tableLength];
			readNonCoord(tableData);
			table = new String(tableData, StandardCharsets.UTF_8);
			readNonCoord(deviceBytes);
			device = bytesToInt(deviceBytes);
			final ObjectInputStream objIn = new ObjectInputStream(sock.getInputStream());
			key = objIn.readObject();
			value = objIn.readObject();

			// new MetaData();
			String fn = MetaData.getDevicePath(device);
			if (!fn.endsWith("/"))
			{
				fn += "/";
			}

			fn += (schema + ".PK" + table + ".indx");
			final List<String> keys = new ArrayList<String>();
			final List<String> types = new ArrayList<String>();
			final List<Boolean> orders = new ArrayList<Boolean>();
			keys.add(table + ".KEY2");
			final FieldValue[] fva = new FieldValue[1];
			final FieldValue[] fva2 = new FieldValue[2];
			if (key instanceof Integer)
			{
				types.add("INT");
				fva[0] = new Schema.IntegerFV((Integer)key);
				fva2[0] = new Schema.IntegerFV((Integer)key);
			}
			else if (key instanceof Long)
			{
				types.add("LONG");
				fva[0] = new Schema.BigintFV((Long)key);
				fva2[0] = new Schema.BigintFV((Long)key);
			}
			else if (key instanceof Float)
			{
				types.add("FLOAT");
				fva[0] = new Schema.DoubleFV(new Double((Float)key));
				fva2[0] = new Schema.DoubleFV(new Double((Float)key));
			}
			else if (key instanceof Double)
			{
				types.add("FLOAT");
				fva[0] = new Schema.DoubleFV((Double)key);
				fva2[0] = new Schema.DoubleFV((Double)key);
			}
			else if (key instanceof String)
			{
				types.add("CHAR");
				fva[0] = new Schema.VarcharFV((String)key);
				fva2[0] = new Schema.VarcharFV((String)key);
			}
			else if (key instanceof Date)
			{
				final GregorianCalendar cal = new GregorianCalendar();
				cal.setTime((Date)key);
				types.add("DATE");
				fva[0] = new Schema.DateFV(new MyDate(cal.get(Calendar.YEAR), cal.get(Calendar.MONTH) + 1, cal.get(Calendar.DAY_OF_MONTH)));
				fva2[0] = new Schema.DateFV(new MyDate(cal.get(Calendar.YEAR), cal.get(Calendar.MONTH) + 1, cal.get(Calendar.DAY_OF_MONTH)));
			}

			if (value instanceof Integer)
			{
				fva2[1] = new Schema.IntegerFV((Integer)value);
			}
			else if (value instanceof Long)
			{
				fva2[1] = new Schema.BigintFV((Long)value);
			}
			else if (value instanceof Float)
			{
				fva2[1] = new Schema.DoubleFV(new Double((Float)value));
			}
			else if (value instanceof Double)
			{
				fva2[1] = new Schema.DoubleFV((Double)value);
			}
			else if (value instanceof String)
			{
				fva2[1] = new Schema.VarcharFV((String)value);
			}
			else if (value instanceof Date)
			{
				final GregorianCalendar cal = new GregorianCalendar();
				cal.setTime((Date)value);
				fva2[1] = new Schema.DateFV(new MyDate(cal.get(Calendar.YEAR), cal.get(Calendar.MONTH) + 1, cal.get(Calendar.DAY_OF_MONTH)));
			}
			orders.add(true);
			final Index index = new Index(fn, keys, types, orders);
			index.setTransaction(tx);
			index.open();
			try
			{
				final IndexRecord line = index.get(fva);
				if (line == null)
				{
					// new MetaData();
					// doesn't exist yet
					// do insert
					final String tfn = MetaData.getDevicePath(device) + schema + "." + table + ".tbl";
					final int maxPlus = FileManager.numBlocks.get(tfn) - 2;
					// FileManager.getFile(tfn);
					final int block = 1 + random.nextInt(maxPlus + 1);
					// request block
					final Map<Integer, DataType> layout = new HashMap<Integer, DataType>();
					final Schema sch = new Schema(layout, MetaData.myNodeNum(), device);
					final Block toRequest = new Block(tfn, block);
					tx.requestPage(toRequest);
					tx.read(toRequest, sch, true);

					final RID rid = sch.insertRow(fva2);
					index.insert(fva, rid);
					tx.commitNoFlush();
					sendOK();
				}
				else
				{
					// already exists
					final Map<Integer, DataType> layout = new HashMap<Integer, DataType>();
					final Schema sch = new Schema(layout, MetaData.myNodeNum(), device);
					// new MetaData();
					final String tfn = MetaData.getDevicePath(device) + schema + "." + table + ".tbl";

					final RID rid = line.getRid();

					// FileManager.getFile(tfn);
					final int maxPlus = FileManager.numBlocks.get(tfn) - 2;
					int block = 1 + random.nextInt(maxPlus + 1);
					// request block
					Block toRequest = new Block(tfn, block);
					tx.requestPage(toRequest);
					tx.read(toRequest, sch, true);
					final RID newRid = sch.insertRow(fva2);
					try
					{
						// index.insert(fva, newRid);
						line.replaceRid(newRid);
						tx.commitNoFlush();
						sendOK();
					}
					catch (final Exception e)
					{
						HRDBMSWorker.logger.debug("Index insert failed", e);
						HRDBMSWorker.logger.debug("They old RID we deleted was " + rid);
						HRDBMSWorker.logger.debug("The new RID we were trying to insert was " + newRid);
						throw e;
					}

					block = rid.getBlockNum();
					// new MetaData();
					// request block
					toRequest = new Block(MetaData.getDevicePath(device) + schema + "." + table + ".tbl", block);
					tx2.requestPage(toRequest);
					tx2.read(toRequest, sch, true);
					sch.deleteRow(rid);
					tx2.commitNoFlush();
					// index.delete(fva, rid);
				}
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.debug("", e);
				tx.rollback();
				sendNo();
				return;
			}
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.debug("", e);
			sendNo();
		}
	}

	private void read(final byte[] arg)
	{
		int count = 0;
		final int length = arg.length;
		while (count < length)
		{
			try
			{
				final int temp = sock.getInputStream().read(arg, count, arg.length - count);
				if (temp == -1)
				{
					HRDBMSWorker.logger.debug("Unexpected end of input");
					if (tx != null)
					{
						XAManager.rollback(tx);
						tx = null;
					}

					try
					{
						sock.close();
						if (worker != null)
						{
							final List<Object> cmd2 = new ArrayList<Object>(1);
							cmd2.add("CLOSE");
							worker.in.put(cmd2);
						}
						this.terminate();
					}
					catch (final Exception f)
					{
						if (worker != null)
						{
							final List<Object> cmd2 = new ArrayList<Object>(1);
							cmd2.add("CLOSE");
							worker.in.put(cmd2);
						}
						this.terminate();
					}
				}
				else
				{
					count += temp;
				}
			}
			catch (final Exception e)
			{
				// HRDBMSWorker.logger.debug("Terminating connection due to
				// exception", e);
				if (tx != null)
				{
					try
					{
						XAManager.rollback(tx);
						tx = null;
					}
					catch (final Exception f)
					{
						HRDBMSWorker.logger.error("", f);
					}
				}

				try
				{
					sock.close();
					if (worker != null)
					{
						final List<Object> cmd2 = new ArrayList<Object>(1);
						cmd2.add("CLOSE");
						worker.in.put(cmd2);
					}
					this.terminate();
				}
				catch (final Exception f)
				{
					if (worker != null)
					{
						final List<Object> cmd2 = new ArrayList<Object>(1);
						cmd2.add("CLOSE");
						while (true)
						{
							try
							{
								worker.in.put(cmd2);
								break;
							}
							catch (final Exception g)
							{
							}
						}
					}
					this.terminate();
				}
			}
		}
	}

	/** Read from the socket into the passed byte array */
	private void readNonCoord(final byte[] arg) throws Exception
	{
		readNonCoord(arg, 0, arg.length, sock.getInputStream());
	}

	/** Read from the socket into the passed byte array */
	private void readNonCoord(final byte[] arg, final int offset, final int length) throws Exception
	{
		readNonCoord(arg, offset, length, sock.getInputStream());
	}

	/** Read into the passed byte array from the passed input stream */
	private static void readNonCoord(final byte[] arg, final InputStream in) throws Exception
	{
		readNonCoord(arg, 0, arg.length, in);
	}

	/** Read into the passed byte array from the passed input stream */
	private static void readNonCoord(final byte[] arg, final int offset, final int length, final InputStream in2) throws Exception
	{
		int count = 0;
		while (count < length)
		{
			final int temp = in2.read(arg, count + offset, length - count);
			if (temp == -1)
			{
				throw new Exception("Hit end of stream when reading from socket");
			}
			else
			{
				count += temp;
			}
		}
	}

	private ByteBuffer readRawRS() throws Exception
	{
		int bbSize = 16 * 1024 * 1024 - 1;
		ByteBuffer bb = ByteBuffer.allocate(bbSize);
		final byte[] numBytes = new byte[4];
		readNonCoord(numBytes);
		final int num = bytesToInt(numBytes);
		int pos = 0;
		// ArrayList<List<Object>> retval = new
		// ArrayList<List<Object>>(num);
		int i = 0;
		while (i < num)
		{
			// readNonCoord(numBytes);
			if (bb.capacity() - pos >= 4)
			{
				// bb.put(numBytes);
				readNonCoord(bb.array(), pos, 4);
			}
			else
			{
				bbSize *= 2;
				final ByteBuffer newBB = ByteBuffer.allocate(bbSize);
				bb.limit(bb.position());
				bb.position(0);
				newBB.put(bb);
				bb = newBB;
				pos = bb.position();
				// bb.put(numBytes);
				readNonCoord(bb.array(), pos, 4);
			}
			// int size = bytesToInt(numBytes);
			final int size = bb.getInt(pos);
			pos += 4;
			// byte[] data = new byte[size];
			// readNonCoord(data);
			if (bb.capacity() - pos >= size)
			{
				// bb.put(data);
				readNonCoord(bb.array(), pos, size);
			}
			else
			{
				bbSize *= 2;
				while (bbSize < size)
				{
					bbSize *= 2;
				}
				final ByteBuffer newBB = ByteBuffer.allocate(bbSize);
				bb.limit(bb.position());
				bb.position(0);
				newBB.put(bb);
				bb = newBB;
				pos = bb.position();
				// bb.put(data);
				readNonCoord(bb.array(), pos, size);
			}

			pos += size;
			// retval.add((List<Object>)fromBytes(data));
			i++;
		}

		// byte[] retval = new byte[bb.position()];
		// System.arraycopy(bb.array(), 0, retval, 0, retval.length);
		bb.limit(pos);
		return bb;
	}

	private void remove()
	{
		final byte[] tableLen = new byte[4];
		byte[] tableBytes;
		String table;
		Object key;
		try
		{
			readNonCoord(tableLen);
			tableBytes = new byte[bytesToInt(tableLen)];
			readNonCoord(tableBytes);
			table = new String(tableBytes, StandardCharsets.UTF_8);
			final ObjectInputStream objIn = new ObjectInputStream(sock.getInputStream());
			key = objIn.readObject();
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.debug("", e);
			sendNo();
			returnExceptionToClient(e);
			return;
		}

		try
		{
			doRemove(table, key);
			sendOK();
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.debug("", e);
			sendNo();
			returnExceptionToClient(e);
		}
	}

	private void remove2()
	{
		final byte[] txBytes = new byte[8];
		long txNum = -1;
		final byte[] schemaLenBytes = new byte[4];
		final byte[] tableLenBytes = new byte[4];
		int schemaLength = -1;
		int tableLength = -1;
		String schema = null;
		String table = null;
		byte[] schemaData = null;
		byte[] tableData = null;
		final byte[] deviceBytes = new byte[4];
		int device;
		Object key;
		Transaction tx;
		try
		{
			readNonCoord(txBytes);
			txNum = bytesToLong(txBytes);
			tx = new Transaction(txNum);
			readNonCoord(schemaLenBytes);
			schemaLength = bytesToInt(schemaLenBytes);
			schemaData = new byte[schemaLength];
			readNonCoord(schemaData);
			schema = new String(schemaData, StandardCharsets.UTF_8);
			readNonCoord(tableLenBytes);
			tableLength = bytesToInt(tableLenBytes);
			tableData = new byte[tableLength];
			readNonCoord(tableData);
			table = new String(tableData, StandardCharsets.UTF_8);
			readNonCoord(deviceBytes);
			device = bytesToInt(deviceBytes);
			final ObjectInputStream objIn = new ObjectInputStream(sock.getInputStream());
			key = objIn.readObject();

			// new MetaData();
			String fn = MetaData.getDevicePath(device);
			if (!fn.endsWith("/"))
			{
				fn += "/";
			}

			fn += (schema + ".PK" + table + ".indx");
			final List<String> keys = new ArrayList<String>();
			final List<String> types = new ArrayList<String>();
			final List<Boolean> orders = new ArrayList<Boolean>();
			keys.add(table + ".KEY2");
			final FieldValue[] fva = new FieldValue[1];
			if (key instanceof Integer)
			{
				types.add("INT");
				fva[0] = new Schema.IntegerFV((Integer)key);
			}
			else if (key instanceof Long)
			{
				types.add("LONG");
				fva[0] = new Schema.BigintFV((Long)key);
			}
			else if (key instanceof Float)
			{
				types.add("FLOAT");
				fva[0] = new Schema.DoubleFV(new Double((Float)key));
			}
			else if (key instanceof Double)
			{
				types.add("FLOAT");
				fva[0] = new Schema.DoubleFV((Double)key);
			}
			else if (key instanceof String)
			{
				types.add("CHAR");
				fva[0] = new Schema.VarcharFV((String)key);
			}
			else if (key instanceof Date)
			{
				final GregorianCalendar cal = new GregorianCalendar();
				cal.setTime((Date)key);
				types.add("DATE");
				fva[0] = new Schema.DateFV(new MyDate(cal.get(Calendar.YEAR), cal.get(Calendar.MONTH) + 1, cal.get(Calendar.DAY_OF_MONTH)));
			}
			orders.add(true);
			final Index index = new Index(fn, keys, types, orders);
			index.setTransaction(tx);
			index.open();
			try
			{
				final IndexRecord line = index.get(fva);
				if (line == null)
				{
					tx.commitNoFlush();
					sendOK();
					return;
				}

				final Map<Integer, DataType> layout = new HashMap<Integer, DataType>();
				final Schema sch = new Schema(layout, MetaData.myNodeNum(), device);
				final RID rid = line.getRid();
				final int block = rid.getBlockNum();
				// new MetaData();
				// request block
				final Block toRequest = new Block(MetaData.getDevicePath(device) + schema + "." + table + ".tbl", block);
				tx.requestPage(toRequest);
				tx.read(toRequest, sch, true);
				sch.deleteRow(rid);

				// for each index, delete row based on rid and key values
				index.delete(fva, rid);
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.debug("", e);
				tx.rollback();
				sendNo();
				return;
			}
			tx.commitNoFlush();
			sendOK();
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.debug("", e);
			sendNo();
		}
	}

	private void removeFromTree(final String host, final List<Object> tree, final List<Object> parent)
	{
		for (final Object o : tree)
		{
			if (o instanceof String)
			{
				if (((String)o).equals(host))
				{
					tree.remove(host);
					if (tree.size() == 0 && parent != null)
					{
						parent.remove(tree);
					}
					return;
				}
			}
			else
			{
				removeFromTree(host, (List<Object>)o, tree);
			}
		}
	}

	private void reorg()
	{
		List<Object> tree = null;
		final byte[] txBytes = new byte[8];
		long txNum = -1;
		final byte[] schemaLenBytes = new byte[4];
		byte[] schemaBytes;
		String schema;
		final byte[] tableLenBytes = new byte[4];
		byte[] tableBytes;
		String table;
		List<Index> indexes;
		Map<String, String> cols2Types;
		Map<Integer, String> pos2Col;
		List<Boolean> uniques;
		try
		{
			readNonCoord(schemaLenBytes);
			schemaBytes = new byte[bytesToInt(schemaLenBytes)];
			readNonCoord(schemaBytes);
			schema = new String(schemaBytes, StandardCharsets.UTF_8);
			readNonCoord(tableLenBytes);
			tableBytes = new byte[bytesToInt(tableLenBytes)];
			readNonCoord(tableBytes);
			table = new String(tableBytes, StandardCharsets.UTF_8);
			readNonCoord(txBytes);
			txNum = bytesToLong(txBytes);
			final ObjectInputStream objIn = new ObjectInputStream(sock.getInputStream());
			tree = (List<Object>)objIn.readObject();
			indexes = (List<Index>)objIn.readObject();
			cols2Types = (Map<String, String>)objIn.readObject();
			pos2Col = (Map<Integer, String>)objIn.readObject();
			uniques = (List<Boolean>)objIn.readObject();
		}
		catch (final Exception e)
		{
			sendNo();
			return;
		}

		Object obj = tree.get(0);
		while (obj instanceof ArrayList)
		{
			obj = ((List)obj).get(0);
		}

		removeFromTree((String)obj, tree, null); // also delete parents if now
		// empty

		final List<SendReorgThread> threads = new ArrayList<SendReorgThread>();
		for (final Object o : tree)
		{
			threads.add(new SendReorgThread(o, tx, schemaLenBytes, schemaBytes, tableLenBytes, tableBytes, indexes, cols2Types, pos2Col, uniques));
		}

		for (final SendReorgThread thread : threads)
		{
			thread.start();
		}

		final Transaction tx = new Transaction(txNum);
		boolean allOK = true;
		try
		{
			doReorg(schema, table, indexes, tx, cols2Types, pos2Col, uniques);
			tx.commit();
		}
		catch (final Exception e)
		{
			try
			{
				tx.rollback();
			}
			catch (final Exception f)
			{
			}
			allOK = false;
		}

		for (final SendReorgThread thread : threads)
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
			if (!thread.sendOK())
			{
				allOK = false;
			}
		}

		if (allOK)
		{
			sendOK();
		}
		else
		{
			sendNo();
			try
			{
				sock.close();
			}
			catch (final Exception f)
			{
			}
		}
	}

	private void respond(final int from, final int to, final String retval) throws UnsupportedEncodingException, IOException
	{
		final byte[] type = "RESPOK  ".getBytes(StandardCharsets.UTF_8);
		final byte[] fromBytes = intToBytes(from);
		final byte[] toBytes = intToBytes(to);
		final byte[] ret = retval.getBytes(StandardCharsets.UTF_8);
		final byte[] retSize = intToBytes(ret.length);
		final byte[] data = new byte[type.length + fromBytes.length + toBytes.length + retSize.length + ret.length];
		System.arraycopy(type, 0, data, 0, type.length);
		System.arraycopy(fromBytes, 0, data, type.length, fromBytes.length);
		System.arraycopy(toBytes, 0, data, type.length + fromBytes.length, toBytes.length);
		System.arraycopy(retSize, 0, data, type.length + fromBytes.length + toBytes.length, retSize.length);
		System.arraycopy(ret, 0, data, type.length + fromBytes.length + toBytes.length + retSize.length, ret.length);
		// HRDBMSWorker.logger.debug("In respond(), response is " + data);
		sock.getOutputStream().write(data);
	}

	private void returnException(final String e) throws UnsupportedEncodingException, IOException
	{
		final byte[] type = "EXCEPT  ".getBytes(StandardCharsets.UTF_8);
		final byte[] ret = e.getBytes(StandardCharsets.UTF_8);
		final byte[] retSize = intToBytes(ret.length);
		final byte[] data = new byte[type.length + retSize.length + ret.length];
		System.arraycopy(type, 0, data, 0, type.length);
		System.arraycopy(retSize, 0, data, type.length, retSize.length);
		System.arraycopy(ret, 0, data, type.length + retSize.length, ret.length);
		sock.getOutputStream().write(data);
	}

	private void returnExceptionToClient(final Exception e)
	{
		byte[] text = null;
		try
		{
			text = e.getMessage().getBytes(StandardCharsets.UTF_8);
		}
		catch (final Exception f)
		{
		}

		if (text == null)
		{
			try
			{
				text = e.getClass().toString().getBytes(StandardCharsets.UTF_8);
			}
			catch (final Exception f)
			{
			}
		}

		final byte[] textLen = intToBytes(text.length);
		try
		{
			sock.getOutputStream().write(textLen);
			sock.getOutputStream().write(text);
			sock.getOutputStream().flush();
		}
		catch (final Exception f)
		{
			// HRDBMSWorker.logger.debug("Terminating connection due to
			// exception", e);
			if (tx != null)
			{
				try
				{
					XAManager.rollback(tx);
					tx = null;
				}
				catch (final Exception g)
				{
					HRDBMSWorker.logger.error("", g);
				}
			}

			try
			{
				sock.close();
				if (worker != null)
				{
					final List<Object> cmd2 = new ArrayList<Object>(1);
					cmd2.add("CLOSE");
					worker.in.put(cmd2);
				}
				this.terminate();
			}
			catch (final Exception h)
			{
				if (worker != null)
				{
					final List<Object> cmd2 = new ArrayList<Object>(1);
					cmd2.add("CLOSE");
					while (true)
					{
						try
						{
							worker.in.put(cmd2);
							break;
						}
						catch (final Exception g)
						{
						}
					}
				}
				this.terminate();
			}
		}
	}

	private void sendInt(final int val)
	{
		try
		{
			final byte[] data = intToBytes(val);
			sock.getOutputStream().write(data);
			sock.getOutputStream().flush();
		}
		catch (final Exception e)
		{
			// HRDBMSWorker.logger.debug("Terminating connection due to
			// exception", e);
			if (tx != null)
			{
				try
				{
					XAManager.rollback(tx);
					tx = null;
				}
				catch (final Exception f)
				{
					HRDBMSWorker.logger.error("", f);
				}
			}

			try
			{
				sock.close();
				if (worker != null)
				{
					final List<Object> cmd2 = new ArrayList<Object>(1);
					cmd2.add("CLOSE");
					worker.in.put(cmd2);
				}
				this.terminate();
			}
			catch (final Exception f)
			{
				if (worker != null)
				{
					final List<Object> cmd2 = new ArrayList<Object>(1);
					cmd2.add("CLOSE");
					while (true)
					{
						try
						{
							worker.in.put(cmd2);
							break;
						}
						catch (final Exception g)
						{
						}
					}
				}
				this.terminate();
			}
		}
	}

	private void sendNo()
	{
		try
		{
			sock.getOutputStream().write("NO".getBytes(StandardCharsets.UTF_8));
			sock.getOutputStream().flush();
		}
		catch (final Exception e)
		{
			// HRDBMSWorker.logger.debug("Terminating connection due to
			// exception", e);
			if (tx != null)
			{
				try
				{
					XAManager.rollback(tx);
					tx = null;
				}
				catch (final Exception f)
				{
					HRDBMSWorker.logger.error("", f);
				}
			}

			try
			{
				sock.close();
				if (worker != null)
				{
					final List<Object> cmd2 = new ArrayList<Object>(1);
					cmd2.add("CLOSE");
					worker.in.put(cmd2);
				}
				this.terminate();
			}
			catch (final Exception f)
			{
				if (worker != null)
				{
					final List<Object> cmd2 = new ArrayList<Object>(1);
					cmd2.add("CLOSE");
					while (true)
					{
						try
						{
							worker.in.put(cmd2);
							break;
						}
						catch (final Exception g)
						{
						}
					}
				}
				this.terminate();
			}
		}
	}

	private void sendOK()
	{
		try
		{
			sock.getOutputStream().write("OK".getBytes(StandardCharsets.UTF_8));
			sock.getOutputStream().flush();
		}
		catch (final Exception e)
		{
			// HRDBMSWorker.logger.debug("Terminating connection due to
			// exception", e);
			if (tx != null)
			{
				try
				{
					XAManager.rollback(tx);
					tx = null;
				}
				catch (final Exception f)
				{
					HRDBMSWorker.logger.error("", f);
				}
			}

			try
			{
				sock.close();
				if (worker != null)
				{
					final List<Object> cmd2 = new ArrayList<Object>(1);
					cmd2.add("CLOSE");
					worker.in.put(cmd2);
				}
				this.terminate();
			}
			catch (final Exception f)
			{
				if (worker != null)
				{
					final List<Object> cmd2 = new ArrayList<Object>(1);
					cmd2.add("CLOSE");
					while (true)
					{
						try
						{
							worker.in.put(cmd2);
							break;
						}
						catch (final Exception g)
						{
						}
					}
				}
				this.terminate();
			}
		}
	}

	private void sendString(final String string)
	{
		try
		{
			final byte[] bytes = string.getBytes(StandardCharsets.UTF_8);
			sock.getOutputStream().write(intToBytes(bytes.length));
			sock.getOutputStream().write(bytes);
			sock.getOutputStream().flush();
		}
		catch (final Exception e)
		{
			// HRDBMSWorker.logger.debug("Terminating connection due to
			// exception", e);
			if (tx != null)
			{
				try
				{
					XAManager.rollback(tx);
					tx = null;
				}
				catch (final Exception f)
				{
					HRDBMSWorker.logger.error("", f);
				}
			}

			try
			{
				sock.close();
				if (worker != null)
				{
					final List<Object> cmd2 = new ArrayList<Object>(1);
					cmd2.add("CLOSE");
					worker.in.put(cmd2);
				}
				this.terminate();
			}
			catch (final Exception f)
			{
				if (worker != null)
				{
					final List<Object> cmd2 = new ArrayList<Object>(1);
					cmd2.add("CLOSE");
					while (true)
					{
						try
						{
							worker.in.put(cmd2);
							break;
						}
						catch (final Exception g)
						{
						}
					}
				}
				this.terminate();
			}
		}
	}

	private void setLoadMetaData()
	{
		List<Object> tree = null;
		final byte[] keyLength = new byte[4];
		int length;
		byte[] data;
		String key;
		LoadMetaData ldmd = null;
		try
		{
			readNonCoord(keyLength);
			length = bytesToInt(keyLength);
			data = new byte[length];
			readNonCoord(data);
			key = new String(data, StandardCharsets.UTF_8);
			final ObjectInputStream objIn = new ObjectInputStream(sock.getInputStream());
			tree = (List<Object>)objIn.readObject();
			ldmd = (LoadMetaData)objIn.readObject();
		}
		catch (final Exception e)
		{
			sendNo();
			return;
		}

		ldmds.put(key, ldmd);

		Object obj = tree.get(0);
		while (obj instanceof ArrayList)
		{
			obj = ((List)obj).get(0);
		}

		removeFromTree((String)obj, tree, null); // also delete parents if now
		// empty

		final List<SendLDMDThread> threads = new ArrayList<SendLDMDThread>();
		for (final Object o : tree)
		{
			threads.add(new SendLDMDThread(o, keyLength, data, ldmd));
		}

		for (final SendLDMDThread thread : threads)
		{
			thread.start();
		}

		boolean allOK = true;
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
			if (!thread.sendOK())
			{
				allOK = false;
			}
		}

		if (allOK)
		{
			sendOK();
		}
		else
		{
			sendNo();
			try
			{
				sock.close();
			}
			catch (final Exception f)
			{
			}
		}
	}

	private void setSchema()
	{
		byte[] arg = new byte[4];
		int count = 0;
		while (count < 4)
		{
			try
			{
				final int temp = sock.getInputStream().read(arg, count, 4 - count);
				if (temp == -1)
				{
					HRDBMSWorker.logger.debug("End of input when expecting schema");
					if (tx != null)
					{
						XAManager.rollback(tx);
						tx = null;
					}

					try
					{
						sock.close();
						if (worker != null)
						{
							final List<Object> cmd2 = new ArrayList<Object>(1);
							cmd2.add("CLOSE");
							worker.in.put(cmd2);
						}
						this.terminate();
					}
					catch (final Exception f)
					{
						if (worker != null)
						{
							final List<Object> cmd2 = new ArrayList<Object>(1);
							cmd2.add("CLOSE");
							worker.in.put(cmd2);
						}
						this.terminate();
					}
				}
				else
				{
					count += temp;
				}
			}
			catch (final Exception e)
			{
				// HRDBMSWorker.logger.debug("Terminating connection due to
				// exception", e);
				if (tx != null)
				{
					try
					{
						XAManager.rollback(tx);
						tx = null;
					}
					catch (final Exception f)
					{
						HRDBMSWorker.logger.error("", f);
					}
				}

				try
				{
					sock.close();
					if (worker != null)
					{
						final List<Object> cmd2 = new ArrayList<Object>(1);
						cmd2.add("CLOSE");
						worker.in.put(cmd2);
					}
					this.terminate();
				}
				catch (final Exception f)
				{
					if (worker != null)
					{
						final List<Object> cmd2 = new ArrayList<Object>(1);
						cmd2.add("CLOSE");
						while (true)
						{
							try
							{
								worker.in.put(cmd2);
								break;
							}
							catch (final Exception g)
							{
							}
						}
					}
					this.terminate();
				}
			}
		}

		final int length = bytesToInt(arg);
		// read schema string
		arg = new byte[length];
		read(arg);
		try
		{
			final String schema = new String(arg, StandardCharsets.UTF_8);
			MetaData.setDefaultSchema(this, schema);
		}
		catch (final Exception e)
		{
		}

	}

	private void testConnection()
	{
		sendOK();
	}

	private final byte[] toBytes(final Object v) throws Exception
	{
		List<byte[]> bytes = null;
		List<Object> val;
		if (v instanceof ArrayList)
		{
			val = (List<Object>)v;
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
			final Object o = val.get(z++);
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
				// byte[] b = ((String)o).getBytes(StandardCharsets.UTF_8);
				final byte[] ba = new byte[((String)o).length() << 2];
				final char[] value = (char[])unsafe.getObject(o, soffset);
				final int blen = ((sun.nio.cs.ArrayEncoder)sce).encode(value, 0, value.length, ba);
				final byte[] b = Arrays.copyOf(ba, blen);
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
				if (((List)o).size() != 0)
				{
					final Exception e = new Exception();
					HRDBMSWorker.logger.error("Non-zero size ArrayList in toBytes()", e);
					throw new Exception("Non-zero size ArrayList in toBytes()");
				}
				header[i] = (byte)8;
			}
			else if (o == null)
			{
				header[i] = (byte)9;
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
			final Object o = val.get(z++);
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
				final byte[] temp = bytes.get(x);
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
			else if (retval[i] == 9)
			{
			}

			i++;
		}

		return retval;
	}

	private void update() throws Exception
	{
		final byte[] txBytes = new byte[8];
		long txNum = -1;
		final byte[] schemaLenBytes = new byte[4];
		final byte[] tableLenBytes = new byte[4];
		int schemaLength = -1;
		int tableLength = -1;
		String schema = null;
		String table = null;
		byte[] schemaData = null;
		byte[] tableData = null;
		List<String> indexes;
		List<List<String>> keys;
		List<List<String>> types;
		List<List<Boolean>> orders;
		List<RIDAndIndexKeys> raiks = null;
		List<List<Object>> list2;
		Map<String, Integer> cols2Pos;
		Map<Integer, String> pos2Col = null;
		Map<String, String> cols2Types = null;
		int type;
		String ngExp, nExp, dExp;
		PartitionMetaData pmd;
		try
		{
			final BufferedInputStream in = new BufferedInputStream(sock.getInputStream());
			readNonCoord(txBytes, in);
			txNum = bytesToLong(txBytes);
			tx = new Transaction(txNum);
			readNonCoord(schemaLenBytes, in);
			schemaLength = bytesToInt(schemaLenBytes);
			schemaData = new byte[schemaLength];
			readNonCoord(schemaData, in);
			schema = new String(schemaData, StandardCharsets.UTF_8);
			readNonCoord(tableLenBytes, in);
			tableLength = bytesToInt(tableLenBytes);
			tableData = new byte[tableLength];
			readNonCoord(tableData, in);
			table = new String(tableData, StandardCharsets.UTF_8);
			readNonCoord(tableLenBytes, in);
			type = bytesToInt(tableLenBytes);
			readNonCoord(tableLenBytes, in);
			tableLength = bytesToInt(tableLenBytes);
			tableData = new byte[tableLength];
			readNonCoord(tableData, in);
			ngExp = new String(tableData, StandardCharsets.UTF_8);
			readNonCoord(tableLenBytes, in);
			tableLength = bytesToInt(tableLenBytes);
			tableData = new byte[tableLength];
			readNonCoord(tableData, in);
			nExp = new String(tableData, StandardCharsets.UTF_8);
			readNonCoord(tableLenBytes, in);
			tableLength = bytesToInt(tableLenBytes);
			tableData = new byte[tableLength];
			readNonCoord(tableData, in);
			dExp = new String(tableData, StandardCharsets.UTF_8);
			final Map<Long, Object> prev = new HashMap<Long, Object>();
			// ObjectInputStream objIn = new
			// ObjectInputStream(sock.getInputStream());
			indexes = OperatorUtils.deserializeALS(in, prev);
			// indexes = (List<String>)objIn.readObject();
			raiks = OperatorUtils.deserializeALRAIK(in, prev);
			// raiks = (List<RIDAndIndexKeys>)objIn.readObject();
			keys = OperatorUtils.deserializeALALS(in, prev);
			// keys = (List<List<String>>)objIn.readObject();
			types = OperatorUtils.deserializeALALS(in, prev);
			// types = (List<List<String>>)objIn.readObject();
			orders = OperatorUtils.deserializeALALB(in, prev);
			// orders = (List<List<Boolean>>)objIn.readObject();
			list2 = OperatorUtils.deserializeALALO(in, prev);
			// list2 = (List<List<Object>>)objIn.readObject();
			cols2Pos = OperatorUtils.deserializeStringIntHM(in, prev);
			// cols2Pos = (Map<String, Integer>)objIn.readObject();
			pos2Col = OperatorUtils.deserializeTM(in, prev);
			// pos2Col = (Map<Integer, String>)objIn.readObject();
			cols2Types = OperatorUtils.deserializeStringHM(in, prev);
			if (cols2Types == null)
			{
				HRDBMSWorker.logger.debug("CW update deserialized null cols2Types");
			}
			// cols2Types = (Map<String, String>)objIn.readObject();

			final Map<String, String> cols2Types2 = new HashMap<String, String>();
			for (final Map.Entry entry : cols2Types.entrySet())
			{
				String col = (String)entry.getKey();
				col = col.substring(col.indexOf('.') + 1);
				cols2Types2.put(col, (String)entry.getValue());
			}

			pmd = new PartitionMetaData(schema, table, ngExp, nExp, dExp, new Transaction(txNum), cols2Types2);
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.debug("", e);
			sendNo();
			return;
		}

		final MultiHashMap<Integer, RIDAndIndexKeys> map = new MultiHashMap<Integer, RIDAndIndexKeys>();
		for (final RIDAndIndexKeys raik : raiks)
		{
			map.multiPut(raik.getRID().getDevice(), raik);
		}

		final List<FlushDeleteThread> threads = new ArrayList<FlushDeleteThread>();
		final List<String> dmlTxStrs = new ArrayList<String>();
		List<Integer> sorted = new ArrayList(map.getKeySet());
		Collections.sort(sorted);
		for (final Object o : sorted)
		{
			final int device = (Integer)o;
			threads.add(new FlushDeleteThread(map.get(device), tx, schema, table, keys, types, orders, indexes, pos2Col, cols2Types, type));
			final String dmlTxStr = Long.toString(tx.number()) + "~" + device + "~" + schema + "." + table;
			if (dmlTx.putIfAbsent(dmlTxStr, dmlTxStr) != null)
			{
				while (dmlTx.putIfAbsent(dmlTxStr, dmlTxStr) != null)
				{
					LockSupport.parkNanos(1);
				}
			}

			dmlTxStrs.add(dmlTxStr);
		}

		for (final FlushDeleteThread thread : threads)
		{
			thread.start();
		}

		boolean allOK = true;
		for (final FlushDeleteThread thread : threads)
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
			if (!thread.getOK())
			{
				allOK = false;
			}
		}

		for (final String s : dmlTxStrs)
		{
			dmlTx.remove(s);
		}

		dmlTxStrs.clear();

		if (!allOK)
		{
			sendNo();
			return;
		}

		if (list2 != null)
		{
			final HJOMultiHashMap<Integer, List<Object>> map2 = new HJOMultiHashMap<Integer, List<Object>>();
			for (final List<Object> row : list2)
			{
				map2.multiPut(MetaData.determineDevice(row, pmd, cols2Pos), row);
			}

			final List<FlushInsertThread> threads2 = new ArrayList<FlushInsertThread>();
			sorted = new ArrayList(map2.getKeySet());
			Collections.sort(sorted);
			for (final Object o : sorted)
			{
				final int device = (Integer)o;
				threads2.add(new FlushInsertThread(map2.get(device), new Transaction(txNum), schema, table, keys, types, orders, indexes, cols2Pos, device, pos2Col, cols2Types, type));
				final String dmlTxStr = Long.toString(tx.number()) + "~" + device + "~" + schema + "." + table;
				if (dmlTx.putIfAbsent(dmlTxStr, dmlTxStr) != null)
				{
					while (dmlTx.putIfAbsent(dmlTxStr, dmlTxStr) != null)
					{
						LockSupport.parkNanos(1);
					}
				}

				dmlTxStrs.add(dmlTxStr);
			}

			for (final FlushInsertThread thread : threads2)
			{
				thread.start();
			}

			for (final FlushInsertThread thread : threads2)
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
				if (!thread.getOK())
				{
					allOK = false;
				}
			}

			for (final String s : dmlTxStrs)
			{
				dmlTx.remove(s);
			}
		}

		if (allOK)
		{
			sendOK();
			return;
		}
		else
		{
			sendNo();
			return;
		}
	}

	@Override
	protected void terminate()
	{
		MetaData.removeDefaultSchema(this);
		// super.terminate();
	}

	public static class CreateIndexThread extends HRDBMSThread
	{
		private final String indx;
		private final int numCols;
		private int device;
		private int unique;
		private boolean ok = true;;
		private Exception e;
		Transaction tx;

		public CreateIndexThread(final String fn, final int numCols, final boolean unique, final Transaction tx)
		{
			this.numCols = numCols;
			this.tx = tx;
			if (unique)
			{
				this.unique = 1;
			}
			else
			{
				this.unique = 0;
			}

			this.indx = fn.substring(fn.lastIndexOf("/") + 1);
			final String devicePath = fn.substring(0, fn.lastIndexOf("/") + 1);
			int i = 0;
			while (true)
			{
				// new MetaData();
				String path = MetaData.getDevicePath(i);
				if (!path.endsWith("/"))
				{
					path += "/";
				}

				if (path.equals(devicePath))
				{
					this.device = i;
					break;
				}

				i++;
			}
		}

		public CreateIndexThread(final String indx, final int numCols, final int device, final int unique, final Transaction tx)
		{
			this.indx = indx;
			this.numCols = numCols;
			this.device = device;
			this.unique = unique;
			this.tx = tx;
		}

		private static void createIndexHeader(final String indx, final int numCols, final int device, final int unique, final Transaction tx) throws Exception
		{
			// new MetaData();
			String fn = MetaData.getDevicePath(device);
			if (!fn.endsWith("/"))
			{
				fn += "/";
			}
			fn += indx;

			final FileChannel fc = FileManager.getFile(fn);
			if (fc.size() > 0)
			{
				fc.truncate(0);
			}
			if (tx != null)
			{
				tx.truncate(new Block(fn, 0));
				tx.commit();
			}
			BufferManager.invalidateFile(fn);
			FileManager.numBlocks.put(fn, 0);
			final ByteBuffer data = ByteBuffer.allocate(Page.BLOCK_SIZE);

			data.position(0);
			data.putInt(numCols); // num key cols
			if (unique != 0)
			{
				data.put((byte)1); // unique
			}
			else
			{
				data.put((byte)0); // not unique
			}
			data.putInt(0); // first free
			data.putInt(0); // head points to block 0
			data.putInt(17); // head point to offset 17

			data.put((byte)3); // start record
			data.putInt(0); // left
			data.putInt(0);
			data.putInt(0); // right
			data.putInt(0);
			data.putInt(0); // up
			data.putInt(0);
			data.putInt(0); // down
			data.putInt(0);

			// fill in first free val pointer
			int pos = data.position();
			data.position(5);
			data.putInt(pos); // first free byte
			data.position(pos);

			while (pos < (Page.BLOCK_SIZE))
			{
				data.put((byte)2); // fill page out with 2s
				pos++;
			}

			data.position(0);
			fc.write(data);
			fc.force(false);
			FileManager.numBlocks.put(fn, (int)(fc.size() / Page.BLOCK_SIZE));
		}

		public Exception getException()
		{
			return e;
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
				createIndexHeader(indx, numCols, device, unique, tx);
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.debug("", e);
				this.e = e;
				ok = false;
			}
		}
	}

	public static class CreateTableThread extends HRDBMSThread
	{
		private final String tbl;
		private final int cols;
		private int device;
		private boolean ok = true;
		private Exception e = null;
		private final Transaction tx;
		private final int type;
		private List<ColDef> defs;
		private List<Integer> colOrder;
		private List<Integer> organization;

		public CreateTableThread(final String tbl, final int cols, final int device, final Transaction tx, final int type, final List<ColDef> defs, final List<Integer> colOrder, final List<Integer> organization)
		{
			this.tbl = tbl;
			this.cols = cols;
			this.device = device;
			this.tx = tx;
			this.type = type;
			this.defs = defs;
			this.colOrder = colOrder;
			this.organization = organization;
		}

		public CreateTableThread(final String fn, final int cols, final Transaction tx, final int type)
		{
			this.cols = cols;
			this.tx = tx;
			this.type = type;
			this.tbl = fn.substring(fn.lastIndexOf("/") + 1);
			final String devicePath = fn.substring(0, fn.lastIndexOf("/") + 1);
			int i = 0;
			while (true)
			{
				// new MetaData();
				String path = MetaData.getDevicePath(i);
				if (!path.endsWith("/"))
				{
					path += "/";
				}

				if (path.equals(devicePath))
				{
					this.device = i;
					break;
				}

				i++;
			}
		}

		private static void createTableHeader(final String tbl, final int cols, final int device, final Transaction tx, final int type, final List<ColDef> defs, final List<Integer> colOrder, final List<Integer> organization) throws Exception
		{
			// new MetaData();
			String fn = MetaData.getDevicePath(device);
			if (!fn.endsWith("/"))
			{
				fn += "/";
			}
			fn += tbl;

			final FileChannel fc = FileManager.getFile(fn);
			if (fc.size() > 0)
			{
				fc.truncate(0);
			}
			if (tx != null)
			{
				tx.truncate(new Block(fn, 0));
				tx.commit();
			}
			BufferManager.invalidateFile(fn);
			FileManager.numBlocks.put(fn, 0);
			final ByteBuffer bb = ByteBuffer.allocate(Page.BLOCK_SIZE);
			bb.position(0);
			bb.putInt(MetaData.myNodeNum());
			bb.putInt(device);

			if (type == 0)
			{
				int i = 8;
				while (i < Page.BLOCK_SIZE)
				{
					bb.putInt(-1);
					i += 4;
				}

				bb.position(0);
				fc.write(bb);

				bb.position(0);
				bb.put(Schema.TYPE_ROW);
				putMedium(bb, 0); // next rec num
				putMedium(bb, 29 + 3 * cols); // headEnd
				putMedium(bb, Page.BLOCK_SIZE); // dataStart
				bb.putLong(System.currentTimeMillis()); // modTime
				putMedium(bb, 27 + (3 * cols)); // rowIDListOff
				putMedium(bb, 30 + (3 * cols)); // offset Array offset
				putMedium(bb, cols); // colIDListSize

				i = 0;
				while (i < cols)
				{
					putMedium(bb, i);
					i++;
				}

				putMedium(bb, 0); // rowIDListSize
				bb.position(0);
				fc.write(bb);
			}
			else
			{
				int i = 0;
				int j = 8;
				while (i < cols)
				{
					int length = 1;
					final ColDef def = defs.get(i);
					final String type2 = def.getType();
					if (type2.startsWith("CHAR"))
					{
						length = Integer.parseInt(type2.substring(5, type2.length() - 1));
					}

					if (!Schema.CVarcharFV.compress)
					{
						if (length < 256)
						{
							bb.position(j++);
							bb.put((byte)1);
						}
						else if (length < 65536)
						{
							bb.position(j++);
							bb.put((byte)2);
						}
						else
						{
							bb.position(j++);
							bb.put((byte)3);
						}
					}
					else
					{
						if (length < 85)
						{
							bb.position(j++);
							bb.put((byte)1);
						}
						else if (length < 21845)
						{
							bb.position(j++);
							bb.put((byte)2);
						}
						else
						{
							bb.position(j++);
							bb.put((byte)3);
						}
					}

					i++;
				}

				bb.position(26220);
				bb.putInt(colOrder.size());
				for (final int col : colOrder)
				{
					bb.putInt(col - 1);
				}

				final int pageSize = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("page_size"));
				if (pageSize >= 252 * 1024)
				{
					bb.position(131072);
					bb.putInt(organization.size());
					for (final int index : organization)
					{
						bb.putInt(index);
					}

					HRDBMSWorker.logger.debug("Created table with organization: " + organization);
				}

				bb.position(0);
				fc.write(bb);

				// col table
				bb.position(0);
				bb.put(Schema.TYPE_COL);
				putMedium(bb, 0); // RID list size
				bb.putInt(0); // just to clear

				int k = 0;
				while (k < colOrder.size())
				{
					i = colOrder.get(k);
					i--;
					int length = 1;
					final ColDef def = defs.get(i);
					final String type2 = def.getType();
					if (type2.startsWith("CHAR"))
					{
						length = Integer.parseInt(type2.substring(5, type2.length() - 1));
					}

					if (!Schema.CVarcharFV.compress)
					{
						if (length < 128)
						{
							bb.position(0);
							bb.put((byte)1);
						}
						else if (length < 32768)
						{
							bb.position(0);
							bb.put((byte)2);
						}
						else
						{
							bb.position(0);
							bb.put((byte)3);
						}
					}
					else
					{
						if (length < 85)
						{
							bb.position(0);
							bb.put((byte)1);
						}
						else if (length < 21845)
						{
							bb.position(0);
							bb.put((byte)2);
						}
						else
						{
							bb.position(0);
							bb.put((byte)3);
						}
					}

					bb.position(0);
					fc.write(bb);
					k++;
				}
			}

			fc.force(false);
			FileManager.numBlocks.put(fn, (int)(fc.size() / Page.BLOCK_SIZE));
		}

		public Exception getException()
		{
			return e;
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
				createTableHeader(tbl, cols, device, tx, type, defs, colOrder, organization);
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.debug("", e);
				this.e = e;
				ok = false;
			}
		}
	}

	private class FlushDeleteThread extends HRDBMSThread
	{
		private final Transaction tx;
		private boolean ok = true;
		String schema;
		String table;
		List<List<String>> keys;
		List<List<String>> types;
		List<List<Boolean>> orders;
		List<String> indexes;
		Set<RIDAndIndexKeys> raiks;
		Map<Integer, String> pos2Col;
		int type;
		private Exception e;

		public FlushDeleteThread(final Set<RIDAndIndexKeys> raiks, final Transaction tx, final String schema, final String table, final List<List<String>> keys, final List<List<String>> types, final List<List<Boolean>> orders, final List<String> indexes, final Map<Integer, String> pos2Col, final Map<String, String> cols2Types, final int type)
		{
			this.raiks = raiks;
			this.tx = tx;
			this.schema = schema;
			this.table = table;
			this.keys = keys;
			this.types = types;
			this.orders = orders;
			this.indexes = indexes;
			this.pos2Col = pos2Col;
			this.type = type;
		}

		public Exception getException()
		{
			return e;
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
				// delete row based on rid
				final MultiHashMap<Integer, RIDAndIndexKeys> map = new MultiHashMap<Integer, RIDAndIndexKeys>();
				for (final RIDAndIndexKeys raik : raiks)
				{
					// HRDBMSWorker.logger.debug("About to delete RID = " +
					// raik.getRID()); //DEBUG
					map.multiPut(raik.getRID().getBlockNum(), raik);
				}

				final Iterator<RIDAndIndexKeys> it = raiks.iterator();
				final int num = it.next().getRID().getDevice();
				final Map<Integer, DataType> layout = new HashMap<Integer, DataType>();
				final Schema sch = new Schema(layout, MetaData.myNodeNum(), num);
				for (final Object o : map.getKeySet())
				{
					final int block = (Integer)o;
					// request block
					// delete every rid

					if (type == 0)
					{
						// new MetaData();
						final Block toRequest = new Block(MetaData.getDevicePath(num) + schema + "." + table + ".tbl", block);
						tx.requestPage(toRequest);
						tx.read(toRequest, sch, true);
						for (final Object o2 : map.get(block))
						{
							final RIDAndIndexKeys raik = (RIDAndIndexKeys)o2;
							sch.deleteRow(raik.getRID());
						}
					}
					else
					{
						// new MetaData();
						// col table
						final Block toRequest = new Block(MetaData.getDevicePath(num) + schema + "." + table + ".tbl", block);
						tx.requestPage(toRequest, new ArrayList<Integer>(pos2Col.keySet()));
						tx.read(toRequest, sch, new ArrayList<Integer>(pos2Col.keySet()), false, true);
						for (final Object o2 : map.get(block))
						{
							final RIDAndIndexKeys raik = (RIDAndIndexKeys)o2;
							sch.deleteRowColTable(raik.getRID());
						}
					}

					// for each index, delete row based on rid and key values
					int i = 0;
					for (final String index : indexes)
					{
						// new MetaData();
						final Index idx = new Index(MetaData.getDevicePath(num) + index, keys.get(i), types.get(i), orders.get(i));
						idx.setTransaction(tx);
						idx.open();
						for (final Object o2 : map.get(block))
						{
							final RIDAndIndexKeys raik = (RIDAndIndexKeys)o2;
							idx.delete(raik.getIndexKeys().get(i), raik.getRID());
						}
						i++;
					}
				}
			}
			catch (final Exception e)
			{
				ok = false;
				this.e = e;
				HRDBMSWorker.logger.debug("", e);
			}
		}
	}

	private class FlushInsertThread extends HRDBMSThread
	{
		private final Transaction tx;
		private boolean ok = true;
		String schema;
		String table;
		List<List<String>> keys;
		List<List<String>> types;
		List<List<Boolean>> orders;
		List<String> indexes;
		List<List<Object>> list;
		Map<String, Integer> cols2Pos;
		int num;
		Map<Integer, String> pos2Col;
		Map<String, String> cols2Types;
		int type;

		public FlushInsertThread(final List<List<Object>> list, final Transaction tx, final String schema, final String table, final List<List<String>> keys, final List<List<String>> types, final List<List<Boolean>> orders, final List<String> indexes, final Map<String, Integer> cols2Pos, final int num, final Map<Integer, String> pos2Col, final Map<String, String> cols2Types, final int type)
		{
			this.list = list;
			this.tx = tx;
			this.schema = schema;
			this.table = table;
			this.keys = keys;
			this.types = types;
			this.orders = orders;
			this.indexes = indexes;
			this.cols2Pos = cols2Pos;
			this.num = num;
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
			try
			{
				ArrayList<RIDAndIndexKeys> raiks = null;

				if (type == 0)
				{
					// new MetaData();
					// insert row and create RAIKS
					final String tfn = MetaData.getDevicePath(num) + schema + "." + table + ".tbl";
					// FileManager.getFile(tfn);
					Integer maxPlus = FileManager.numBlocks.get(tfn);
					if (maxPlus == null)
					{
						FileManager.getFile(tfn);
						maxPlus = FileManager.numBlocks.get(tfn);
					}

					int block = maxPlus - 1;

					// request block
					final Map<Integer, DataType> layout = new HashMap<Integer, DataType>();
					Schema sch = new Schema(layout, MetaData.myNodeNum(), num);
					Block toRequest = new Block(tfn, block);
					// HRDBMSWorker.logger.debug("Requesting " + toRequest);
					tx.requestPage(toRequest);
					tx.read(toRequest, sch, true);
					raiks = new ArrayList<RIDAndIndexKeys>();
					for (final List<Object> row : list)
					{
						final RID rid = sch.insertRow(aloToFieldValues(row));
						if (rid.getBlockNum() != block)
						{
							block = rid.getBlockNum();
							toRequest = new Block(tfn, block);
							// HRDBMSWorker.logger.debug("Row was placed on " +
							// toRequest + " instead");
							tx.requestPage(toRequest);
							;
							sch = new Schema(layout, MetaData.myNodeNum(), num);
							tx.read(toRequest, sch, true);
						}

						final List<List<Object>> indexKeys = new ArrayList<List<Object>>();
						int i = 0;
						for (final String index : indexes)
						{
							final List<String> key = keys.get(i);
							final List<Object> k = new ArrayList<Object>();
							for (final String col : key)
							{
								try
								{
									k.add(row.get(cols2Pos.get(col)));
								}
								catch (final Exception e)
								{
									HRDBMSWorker.logger.debug("Row is " + row);
									HRDBMSWorker.logger.debug("Cols2Pos is " + cols2Pos);
									HRDBMSWorker.logger.debug("Col is " + col);
									throw e;
								}
							}

							indexKeys.add(k);
							i++;
						}

						raiks.add(new RIDAndIndexKeys(rid, indexKeys));
					}
				}
				else
				{
					// new MetaData();
					// col table
					// insert row and create RAIKS
					final String tfn = MetaData.getDevicePath(num) + schema + "." + table + ".tbl";
					// FileManager.getFile(tfn);
					Integer maxPlus = FileManager.numBlocks.get(tfn);
					if (maxPlus == null)
					{
						FileManager.getFile(tfn);
						maxPlus = FileManager.numBlocks.get(tfn);
					}
					maxPlus -= 1;
					maxPlus /= pos2Col.size();
					int block = (maxPlus - 1) * pos2Col.size() + 1;

					// request block
					Block toRequest = new Block(tfn, block);
					tx.requestPage(toRequest, new ArrayList<Integer>(pos2Col.keySet()));
					final Map<Integer, DataType> layout = new HashMap<Integer, DataType>();
					for (final Map.Entry entry : pos2Col.entrySet())
					{
						final String type = cols2Types.get(entry.getValue());
						DataType value = null;
						if (type.equals("INT"))
						{
							value = new DataType(DataType.INTEGER, 0, 0);
						}
						else if (type.equals("FLOAT"))
						{
							value = new DataType(DataType.DOUBLE, 0, 0);
						}
						else if (type.equals("CHAR"))
						{
							value = new DataType(DataType.VARCHAR, 0, 0);
						}
						else if (type.equals("LONG"))
						{
							value = new DataType(DataType.BIGINT, 0, 0);
						}
						else if (type.equals("DATE"))
						{
							value = new DataType(DataType.DATE, 0, 0);
						}

						layout.put((Integer)entry.getKey(), value);
					}
					Schema sch = new Schema(layout, MetaData.myNodeNum(), num);
					tx.read(toRequest, sch, new ArrayList<Integer>(pos2Col.keySet()), false, true);
					raiks = new ArrayList<RIDAndIndexKeys>();
					for (final List<Object> row : list)
					{
						final RID rid = sch.insertRowColTable(aloToFieldValues(row));
						if (rid.getBlockNum() != block)
						{
							block = rid.getBlockNum();
							toRequest = new Block(tfn, block);
							tx.requestPage(toRequest, new ArrayList<Integer>(pos2Col.keySet()));
							sch = new Schema(layout, MetaData.myNodeNum(), num);
							tx.read(toRequest, sch, new ArrayList<Integer>(pos2Col.keySet()), false, true);
						}

						final List<List<Object>> indexKeys = new ArrayList<List<Object>>();
						int i = 0;
						for (final String index : indexes)
						{
							final List<String> key = keys.get(i);
							final List<Object> k = new ArrayList<Object>();
							for (final String col : key)
							{
								try
								{
									k.add(row.get(cols2Pos.get(col)));
								}
								catch (final Exception e)
								{
									HRDBMSWorker.logger.debug("Row is " + row);
									HRDBMSWorker.logger.debug("Cols2Pos is " + cols2Pos);
									HRDBMSWorker.logger.debug("Col is " + col);
									throw e;
								}
							}

							indexKeys.add(k);
							i++;
						}

						raiks.add(new RIDAndIndexKeys(rid, indexKeys));
					}
				}

				// for each index, insert row based on rid and key values
				int i = 0;
				for (final String index : indexes)
				{
					// new MetaData();
					final Index idx = new Index(MetaData.getDevicePath(num) + index, keys.get(i), types.get(i), orders.get(i));
					idx.setTransaction(tx);
					idx.open();
					for (final RIDAndIndexKeys raik : raiks)
					{
						idx.insert(raik.getIndexKeys().get(i), raik.getRID());
					}
					i++;
				}
			}
			catch (final Exception e)
			{
				ok = false;
				HRDBMSWorker.logger.debug("", e);
			}
		}
	}

	private class FlushLoadThread extends HRDBMSThread
	{
		private final Transaction tx;
		private boolean ok = true;
		String schema;
		String table;
		// ArrayList<List<Object>> list;
		// byte[] list;
		private final int num;
		Map<Integer, String> pos2Col;
		Map<String, String> cols2Types;
		int type;

		// LinkedBlockingQueue<Object> queue = new
		// LinkedBlockingQueue<Object>();
		// SPSCQueue queue = new
		// SPSCQueue(Integer.parseInt(HRDBMSWorker.getHParms().getProperty("max_batch")));
		// IndexWriterThread thread;

		public FlushLoadThread(final Transaction tx, final String schema, final String table, final int num, final Map<Integer, String> pos2Col, final Map<String, String> cols2Types, final int type)
		{
			// this.list = list;
			this.tx = tx;
			this.schema = schema;
			this.table = table;
			this.num = num;
			this.pos2Col = pos2Col;
			this.cols2Types = cols2Types;
			this.type = type;
		}

		@Override
		public boolean equals(final Object r)
		{
			final FlushLoadThread rhs = (FlushLoadThread)r;
			return schema.equals(rhs.schema) && table.equals(rhs.table) && num == rhs.num;
		}

		public boolean getOK()
		{
			return ok;
		}

		@Override
		public int hashCode()
		{
			int hash = 23;
			hash = hash * 31 + schema.hashCode();
			hash = hash * 31 + table.hashCode();
			hash = hash * 31 + num;
			return hash;
		}

		@Override
		public void run()
		{
			try
			{
				final boolean doMM = HRDBMSWorker.getHParms().getProperty("do_min_max").equals("true");
				// HRDBMSWorker.logger.debug("FlushLoad thread started with type
				// = " + type);
				final MetaData meta = new MetaData();
				final FileChannel fc = loadFCs.remove(MetaData.getDevicePath(num) + schema + "." + table + ".tmp");
				// String tmpFile = meta.getDevicePath(num) + schema + "." +
				// table + ".tmp";
				// HRDBMSWorker.logger.debug("FC length for " + tmpFile + " = "
				// + fc.size());
				if (fc == null)
				{
					return;
				}
				final BufferedFileChannel bfc = new BufferedFileChannel(fc, 8 * 1024 * 1024);
				bfc.position(0);
				final Operator bfcOp = new BFCOperator(bfc, pos2Col, cols2Types, meta);

				// new ArrayList<Index>();
				// thread = new IndexWriterThread(queue, idxs);
				// thread.start();

				final String file = MetaData.getDevicePath(num) + schema + "." + table + ".tbl";
				// FileManager.getFile(file);
				Integer block = FileManager.numBlocks.get(file);
				if (block == null)
				{
					FileManager.getFile(file);
					block = FileManager.numBlocks.get(file);
				}

				if (type == 0)
				{
					final int maxBatch = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("max_batch"));
					int rows = 0;
					bfcOp.start();
					block -= 1;
					// request block
					final Map<Integer, DataType> layout = new HashMap<Integer, DataType>();
					Schema sch = new Schema(layout, MetaData.myNodeNum(), num);
					Block toRequest = new Block(file, block);
					tx.requestPage(toRequest);
					tx.read(toRequest, sch, true);
					// new ArrayList<RIDAndIndexKeys>();
					Object o = bfcOp.next(bfcOp);
					while (o instanceof ArrayList)
					{
						rows++;
						// for (Object o : row)
						// {
						// if (o instanceof Integer && (Integer)o < 0)
						// {
						// HRDBMSWorker.logger.debug("Received negative integer
						// "
						// + o);
						// }
						// else if (o instanceof Long && (Long)o < 0)
						// {
						// HRDBMSWorker.logger.debug("Reveived negative long " +
						// o);
						// }
						// }
						final List<Object> row = (List<Object>)o;
						final RID rid = sch.insertRowAppend(aloToFieldValues(row));
						// ArrayList<List<Object>> indexKeys = new
						// ArrayList<List<Object>>();
						// i = 0;
						// for (String index : indexes)
						// {
						// ArrayList<String> key = keys.get(i);
						// ArrayList<Object> k = new ArrayList<Object>();
						// for (String col : key)
						// {
						// k.add(row.get(cols2Pos.get(col)));
						// }

						// indexKeys.add(k);
						// i++;
						// }

						// queue.put(new RIDAndIndexKeys(rid, indexKeys));
						final int newBlock = rid.getBlockNum();
						if (newBlock != block)
						{
							block = newBlock;
							sch.close();

							if (rows > maxBatch)
							{
								rows = 0;
								boolean doCheck = false;
								synchronized (disk2BatchCount)
								{
									Integer count = disk2BatchCount.get(num);
									if (count == null)
									{
										disk2BatchCount.put(num, 1);
									}
									else
									{
										count++;

										if (count >= BATCHES_PER_CHECK)
										{
											disk2BatchCount.put(num, 0);
											doCheck = true;
										}
										else
										{
											disk2BatchCount.put(num, count);
										}
									}
								}

								if (doCheck)
								{
									// new MetaData();
									tx.checkpoint(MetaData.getDevicePath(num));
								}
							}

							sch = new Schema(layout, MetaData.myNodeNum(), num);
							toRequest = new Block(file, block);
							tx.requestPage(toRequest);
							tx.read(toRequest, sch, true);
						}

						o = bfcOp.next(bfcOp);
					}

					sch.close();

					// wait for index writer to finish
					// queue.put(new DataEndMarker());
					// thread.join();
					// if (!thread.getOK())
					// {
					// ok = false;
					// }

					if (o instanceof Exception)
					{
						ok = false;
					}

					bfcOp.close();
					fc.close();
					new File(MetaData.getDevicePath(num) + schema + "." + table + ".tmp").delete();

					// for (Index index : idxs)
					// {
					// index.myPages.clear();
					// index.cache.clear();
					// }
				}
				else
				{
					final int pbpeVer = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("pbpe_version"));
					final boolean isV5OrHigher = (pbpeVer >= 5);
					final boolean isV6OrHigher = (pbpeVer >= 6);
					final boolean isV9 = (pbpeVer == 9);
					// col table
					final Block b = new Block(file, 0);
					tx.requestPage(b);
					final HeaderPage hp = tx.readHeaderPage(b, 1);
					final List<Integer> intClustering = hp.getClustering();
					// HRDBMSWorker.logger.debug("Header page returned an
					// organization of: " + intClustering);
					Operator top = bfcOp;
					if (intClustering.size() != 0)
					{
						final List<String> keys = new ArrayList<String>(intClustering.size());
						final List<Boolean> orders = new ArrayList<Boolean>(intClustering.size());
						for (final int index : intClustering)
						{
							keys.add(pos2Col.get(index));
							orders.add(true);
						}

						final SortOperator sort = new SortOperator(keys, orders, meta);
						final long cc = fc.size() / 100;
						int icc = 0;
						if (cc > Integer.MAX_VALUE)
						{
							icc = Integer.MAX_VALUE;
						}
						else
						{
							icc = (int)cc;
						}
						sort.setChildCard(icc);
						// HRDBMSWorker.logger.debug("Set sort child card to " +
						// icc);
						sort.add(top);
						top = sort;
					}

					top.start();

					block -= 1;
					final int numCols = pos2Col.size();
					block /= numCols;
					block = (block - 1) * numCols + 1;
					Block toRequest = new Block(file, block);
					tx.requestPage(toRequest, new ArrayList<Integer>(pos2Col.keySet()));
					// request block
					final Map<Integer, DataType> layout = new HashMap<Integer, DataType>();
					for (final Map.Entry entry : pos2Col.entrySet())
					{
						final String type = cols2Types.get(entry.getValue());
						DataType value = null;
						if (type.equals("INT"))
						{
							value = new DataType(DataType.INTEGER, 0, 0);
						}
						else if (type.equals("FLOAT"))
						{
							value = new DataType(DataType.DOUBLE, 0, 0);
						}
						else if (type.equals("CHAR"))
						{
							value = new DataType(DataType.VARCHAR, 0, 0);
						}
						else if (type.equals("LONG"))
						{
							value = new DataType(DataType.BIGINT, 0, 0);
						}
						else if (type.equals("DATE"))
						{
							value = new DataType(DataType.DATE, 0, 0);
						}

						layout.put((Integer)entry.getKey(), value);
					}

					int rows = 0;
					final int maxBatch = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("max_batch"));
					Schema sch = new Schema(layout, MetaData.myNodeNum(), num);
					tx.read(toRequest, sch, new ArrayList<Integer>(pos2Col.keySet()), false, true);

					// new ArrayList<RIDAndIndexKeys>();
					Object o = top.next(top);
					final List<List<Object>> pageSet = new ArrayList<List<Object>>();
					while (o instanceof ArrayList)
					{
						rows++;
						final List<Object> row = (List<Object>)o;
						final RID rid = sch.insertRowColTableAppend(aloToFieldValues(row));
						final int newBlock = rid.getBlockNum();
						if (newBlock == block)
						{
							pageSet.add(row);
						}
						else
						{
							if (doMM)
							{
								doMinMax(toRequest, pageSet, pos2Col, isV5OrHigher, isV6OrHigher);
							}
							else
							{
								pageSet.clear();
							}
							pageSet.add(row);
							block = newBlock;

							if (rows > maxBatch)
							{
								rows = 0;
								boolean doCheck = false;
								synchronized (disk2BatchCount)
								{
									Integer count = disk2BatchCount.get(num);
									if (count == null)
									{
										disk2BatchCount.put(num, 1);
									}
									else
									{
										count++;

										if (count >= BATCHES_PER_CHECK)
										{
											disk2BatchCount.put(num, 0);
											doCheck = true;
										}
										else
										{
											disk2BatchCount.put(num, count);
										}
									}
								}

								if (doCheck)
								{
									// new MetaData();
									tx.checkpoint(MetaData.getDevicePath(num));
								}
							}

							sch = new Schema(layout, MetaData.myNodeNum(), num);
							toRequest = new Block(file, block);
							tx.requestPage(toRequest, new ArrayList<Integer>(pos2Col.keySet()));
							tx.read(toRequest, sch, new ArrayList<Integer>(pos2Col.keySet()), false, true);
						}

						o = top.next(top);
					}

					if (pageSet.size() != 0)
					{
						if (doMM)
						{
							doMinMax(toRequest, pageSet, pos2Col, isV5OrHigher, isV6OrHigher);
						}
						else
						{
							pageSet.clear();
						}
					}

					if (o instanceof Exception)
					{
						ok = false;
					}

					top.close();
					fc.close();
					new File(MetaData.getDevicePath(num) + schema + "." + table + ".tmp").delete();

					if (isV9)
					{
						final String msg = file + "," + System.currentTimeMillis();

						while (true)
						{
							try
							{
								TableScanOperator.prtq.put(msg);
								break;
							}
							catch (final InterruptedException e)
							{
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
	}

	private static class Get2Thread extends HRDBMSThread
	{
		private final RootOperator root;
		private Object retval = null;
		private boolean ok = true;
		private Exception e;

		public Get2Thread(final RootOperator root)
		{
			this.root = root;
		}

		public Exception getException()
		{
			return e;
		}

		public boolean getOK()
		{
			return ok;
		}

		public Object getRetval()
		{
			return retval;
		}

		@Override
		public void run()
		{
			try
			{
				// execute operator tree
				root.start();
				final Object next = root.next();
				if (next instanceof DataEndMarker)
				{
					root.close();
					return;
				}

				if (next instanceof Exception)
				{
					root.close();
					ok = false;
					this.e = (Exception)next;
					return;
				}

				final List<Object> row = (List<Object>)next;
				retval = row.get(0);
				root.close();
			}
			catch (final Exception e)
			{
				ok = false;
				this.e = e;
			}
		}
	}

	private static class IndexWriterThread extends HRDBMSThread
	{
		private final SPSCQueue queue;
		private final List<Index> indexes;
		private boolean ok = true;
		private boolean isFieldValues = false;

		public IndexWriterThread(final SPSCQueue queue, final List<Index> indexes, final boolean isFieldValues)
		{
			this.queue = queue;
			this.indexes = indexes;
			this.isFieldValues = isFieldValues;
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
				while (true)
				{
					final Object obj = queue.take();

					if (obj instanceof DataEndMarker)
					{
						return;
					}

					final RIDAndIndexKeys raik = (RIDAndIndexKeys)obj;
					// for each index, insert row based on rid and key values

					if (!isFieldValues)
					{
						int i = 0;
						for (final Index idx : indexes)
						{
							idx.insertNoLog(raik.getIndexKeys().get(i), raik.getRID());
							i++;
						}
					}
					else
					{
						int i = 0;
						for (final Index idx : indexes)
						{
							idx.insertNoLog(raik.getIndexKeys().get(i), raik.getRID());
							i++;
						}
					}

				}
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.debug("", e);
				ok = false;
			}
		}
	}

	private static class MassDeleteThread extends HRDBMSThread
	{
		private final File file;
		private final Transaction tx;
		private int num = 0;
		private boolean ok = true;
		private final List<String> indexes;
		private final List<List<String>> keys;
		private final List<List<String>> types;
		private final List<List<Boolean>> orders;
		private final Map<Integer, String> pos2Col;
		private final Map<String, String> cols2Types;
		private final boolean logged;
		private final int type;

		public MassDeleteThread(final File file, final Transaction tx, final List<String> indexes, final List<List<String>> keys, final List<List<String>> types, final List<List<Boolean>> orders, final Map<Integer, String> pos2Col, final Map<String, String> cols2Types, final boolean logged, final int type)
		{
			this.file = file;
			this.tx = tx;
			this.indexes = indexes;
			this.keys = keys;
			this.types = types;
			this.orders = orders;
			this.pos2Col = pos2Col;
			this.cols2Types = cols2Types;
			this.logged = logged;
			this.type = type;
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
			if (logged)
			{
				Integer numBlocks = -1;

				try
				{
					LockManager.sLock(new Block(file.getAbsolutePath(), -1), tx.number());
					// FileManager.getFile(file.getAbsolutePath());
					numBlocks = FileManager.numBlocks.get(file.getAbsolutePath());
					if (numBlocks == null)
					{
						FileManager.getFile(file.getAbsolutePath());
						numBlocks = FileManager.numBlocks.get(file.getAbsolutePath());
					}
				}
				catch (final Exception e)
				{
					ok = false;
					HRDBMSWorker.logger.debug("", e);
					return;
				}

				final Map<Integer, DataType> layout = new HashMap<Integer, DataType>();
				for (final Map.Entry entry : pos2Col.entrySet())
				{
					final String type = cols2Types.get(entry.getValue());
					DataType value = null;
					if (type.equals("INT"))
					{
						value = new DataType(DataType.INTEGER, 0, 0);
					}
					else if (type.equals("FLOAT"))
					{
						value = new DataType(DataType.DOUBLE, 0, 0);
					}
					else if (type.equals("CHAR"))
					{
						value = new DataType(DataType.VARCHAR, 0, 0);
					}
					else if (type.equals("LONG"))
					{
						value = new DataType(DataType.BIGINT, 0, 0);
					}
					else if (type.equals("DATE"))
					{
						value = new DataType(DataType.DATE, 0, 0);
					}

					layout.put((Integer)entry.getKey(), value);
				}
				final Schema sch = new Schema(layout);
				int onPage = 1;
				int lastRequested = 0;
				final int PREFETCH_REQUEST_SIZE = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("prefetch_request_size")); // 80
				final int PAGES_IN_ADVANCE = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("pages_in_advance")); // 40

				if (type == 0)
				{
					while (onPage < numBlocks)
					{
						if (lastRequested - onPage < PAGES_IN_ADVANCE)
						{
							final Block[] toRequest = new Block[lastRequested + PREFETCH_REQUEST_SIZE < numBlocks ? PREFETCH_REQUEST_SIZE : numBlocks - lastRequested - 1];
							int i = 0;
							final int length = toRequest.length;
							while (i < length)
							{
								toRequest[i] = new Block(file.getAbsolutePath(), lastRequested + i + 1);
								i++;
							}
							try
							{
								tx.requestPages(toRequest);
							}
							catch (final Exception e)
							{
								ok = false;
								return;
							}
							lastRequested += toRequest.length;
						}

						try
						{
							tx.read(new Block(file.getAbsolutePath(), onPage++), sch, true);
							final RowIterator rit = sch.rowIterator();
							while (rit.hasNext())
							{
								final Row r = rit.next();
								if (!r.getCol(0).exists())
								{
									continue;
								}

								final RID rid = r.getRID();
								sch.deleteRow(rid);
								num++;
							}
						}
						catch (final Exception e)
						{
							ok = false;
							HRDBMSWorker.logger.debug("", e);
							return;
						}
					}
				}
				else
				{
					// col table
					final int numCols = pos2Col.size();
					while (onPage < numBlocks)
					{
						if (lastRequested - onPage < PAGES_IN_ADVANCE)
						{
							final int length = lastRequested + PREFETCH_REQUEST_SIZE < numBlocks ? PREFETCH_REQUEST_SIZE : numBlocks - lastRequested - 1;
							int i = 0;
							final List<Block> toRequest = new ArrayList<Block>();
							while (i < length)
							{
								if ((lastRequested + i + 1) % numCols == 1)
								{
									toRequest.add(new Block(file.getAbsolutePath(), lastRequested + i + 1));
								}
								i++;
							}

							if (toRequest.size() > 0)
							{
								try
								{
									final Block[] ba = toRequest.toArray(new Block[toRequest.size()]);
									tx.requestPages(ba, new ArrayList<Integer>(pos2Col.keySet()));
								}
								catch (final Exception e)
								{
									ok = false;
									return;
								}
							}

							lastRequested += length;
						}

						try
						{
							if (onPage % numCols != 1)
							{
								onPage++;
								continue;
							}
							tx.read(new Block(file.getAbsolutePath(), onPage++), sch, new ArrayList<Integer>(pos2Col.keySet()), false, true);
							sch.massDelete();
						}
						catch (final Exception e)
						{
							ok = false;
							HRDBMSWorker.logger.debug("", e);
							return;
						}
					}
				}

				try
				{
					int i = 0;
					for (final String index : indexes)
					{
						final Index idx = new Index(new File(file.getParentFile().getAbsoluteFile(), index).getAbsolutePath(), keys.get(i), types.get(i), orders.get(i));
						idx.setTransaction(tx);
						idx.open();
						idx.massDelete();
						i++;
					}
				}
				catch (final Exception e)
				{
					ok = false;
					HRDBMSWorker.logger.debug("", e);
					return;
				}
			}
			else
			{
				// not logged
				Integer numBlocks = -1;

				try
				{
					LockManager.sLock(new Block(file.getAbsolutePath(), -1), tx.number());
					// FileManager.getFile(file.getAbsolutePath());
					numBlocks = FileManager.numBlocks.get(file.getAbsolutePath());
					if (numBlocks == null)
					{
						FileManager.getFile(file.getAbsolutePath());
						numBlocks = FileManager.numBlocks.get(file.getAbsolutePath());
					}
				}
				catch (final Exception e)
				{
					ok = false;
					HRDBMSWorker.logger.debug("", e);
					return;
				}

				final Map<Integer, DataType> layout = new HashMap<Integer, DataType>();
				for (final Map.Entry entry : pos2Col.entrySet())
				{
					final String type = cols2Types.get(entry.getValue());
					DataType value = null;
					if (type.equals("INT"))
					{
						value = new DataType(DataType.INTEGER, 0, 0);
					}
					else if (type.equals("FLOAT"))
					{
						value = new DataType(DataType.DOUBLE, 0, 0);
					}
					else if (type.equals("CHAR"))
					{
						value = new DataType(DataType.VARCHAR, 0, 0);
					}
					else if (type.equals("LONG"))
					{
						value = new DataType(DataType.BIGINT, 0, 0);
					}
					else if (type.equals("DATE"))
					{
						value = new DataType(DataType.DATE, 0, 0);
					}

					layout.put((Integer)entry.getKey(), value);
				}
				final Schema sch = new Schema(layout);
				int onPage = 1;
				int lastRequested = 0;
				final int PREFETCH_REQUEST_SIZE = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("prefetch_request_size")); // 80
				final int PAGES_IN_ADVANCE = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("pages_in_advance")); // 40

				if (type == 0)
				{
					while (onPage < numBlocks)
					{
						if (lastRequested - onPage < PAGES_IN_ADVANCE)
						{
							final Block[] toRequest = new Block[lastRequested + PREFETCH_REQUEST_SIZE < numBlocks ? PREFETCH_REQUEST_SIZE : numBlocks - lastRequested - 1];
							int i = 0;
							final int length = toRequest.length;
							while (i < length)
							{
								toRequest[i] = new Block(file.getAbsolutePath(), lastRequested + i + 1);
								i++;
							}
							try
							{
								tx.requestPages(toRequest);
							}
							catch (final Exception e)
							{
								ok = false;
								return;
							}
							lastRequested += toRequest.length;
						}

						try
						{
							tx.read(new Block(file.getAbsolutePath(), onPage++), sch, true);
							final RowIterator rit = sch.rowIterator();
							while (rit.hasNext())
							{
								final Row r = rit.next();
								if (!r.getCol(0).exists())
								{
									continue;
								}

								final RID rid = r.getRID();
								sch.deleteRowNoLog(rid);
								num++;
							}
						}
						catch (final Exception e)
						{
							ok = false;
							HRDBMSWorker.logger.debug("", e);
							return;
						}
					}
				}
				else
				{
					// col table
					final int numCols = pos2Col.size();
					while (onPage < numBlocks)
					{
						if (lastRequested - onPage < PAGES_IN_ADVANCE)
						{
							final int length = lastRequested + PREFETCH_REQUEST_SIZE < numBlocks ? PREFETCH_REQUEST_SIZE : numBlocks - lastRequested - 1;
							int i = 0;
							final List<Block> toRequest = new ArrayList<Block>();
							while (i < length)
							{
								if ((lastRequested + i + 1) % numCols == 1)
								{
									toRequest.add(new Block(file.getAbsolutePath(), lastRequested + i + 1));
								}
								i++;
							}

							if (toRequest.size() > 0)
							{
								try
								{
									final Block[] ba = toRequest.toArray(new Block[toRequest.size()]);
									tx.requestPages(ba, new ArrayList<Integer>(pos2Col.keySet()));
								}
								catch (final Exception e)
								{
									ok = false;
									return;
								}
							}

							lastRequested += length;
						}

						try
						{
							if (onPage % numCols != 1)
							{
								onPage++;
								continue;
							}
							tx.read(new Block(file.getAbsolutePath(), onPage++), sch, new ArrayList<Integer>(pos2Col.keySet()), false, true);
							sch.massDeleteNoLog();
						}
						catch (final Exception e)
						{
							ok = false;
							HRDBMSWorker.logger.debug("", e);
							return;
						}
					}
				}

				try
				{
					int i = 0;
					for (final String index : indexes)
					{
						final Index idx = new Index(new File(file.getParentFile().getAbsoluteFile(), index).getAbsolutePath(), keys.get(i), types.get(i), orders.get(i));
						idx.setTransaction(tx);
						idx.open();
						idx.massDeleteNoLog();
						i++;
					}
				}
				catch (final Exception e)
				{
					ok = false;
					HRDBMSWorker.logger.debug("", e);
					return;
				}
			}
		}
	}

	private static class PopIndexThread extends HRDBMSThread
	{
		private final String iFn;
		private String tFn;
		private final int device;
		private final List<String> keys;
		private final List<String> types;
		private final List<Boolean> orders;
		private final List<Integer> poses;
		private boolean ok = true;
		private final Map<Integer, String> pos2Col;
		private final Map<String, String> cols2Types;
		private final Transaction tx;
		private final int type;

		public PopIndexThread(final String iFn, final String tFn, final int device, final List<String> keys, final List<String> types, final List<Boolean> orders, final List<Integer> poses, final Map<Integer, String> pos2Col, final Map<String, String> cols2Types, final Transaction tx, final int type)
		{
			this.iFn = iFn;
			this.tFn = tFn;
			this.device = device;
			this.keys = keys;
			this.types = types;
			this.orders = orders;
			this.poses = poses;
			this.pos2Col = pos2Col;
			this.cols2Types = cols2Types;
			this.tx = tx;
			this.type = type;
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
				// new MetaData();
				String fn = MetaData.getDevicePath(device);
				if (!fn.endsWith("/"))
				{
					fn += "/";
				}
				fn += tFn;
				tFn = fn;

				// new MetaData();
				final Index idx = new Index(MetaData.getDevicePath(device) + iFn, keys, types, orders);
				idx.setTransaction(tx);
				idx.open();
				LockManager.sLock(new Block(tFn, -1), tx.number());
				// FileManager.getFile(tFn);
				Integer numBlocks = FileManager.numBlocks.get(tFn);
				if (numBlocks == null)
				{
					FileManager.getFile(tFn);
					numBlocks = FileManager.numBlocks.get(tFn);
				}
				int i = 1;
				while (i < numBlocks)
				{
					LockManager.sLock(new Block(tFn, i), tx.number());
					i++;
				}

				final Map<Integer, DataType> layout = new HashMap<Integer, DataType>();
				for (final Map.Entry entry : pos2Col.entrySet())
				{
					final String type = cols2Types.get(entry.getValue());
					DataType value = null;
					if (type.equals("INT"))
					{
						value = new DataType(DataType.INTEGER, 0, 0);
					}
					else if (type.equals("FLOAT"))
					{
						value = new DataType(DataType.DOUBLE, 0, 0);
					}
					else if (type.equals("CHAR"))
					{
						value = new DataType(DataType.VARCHAR, 0, 0);
					}
					else if (type.equals("LONG"))
					{
						value = new DataType(DataType.BIGINT, 0, 0);
					}
					else if (type.equals("DATE"))
					{
						value = new DataType(DataType.DATE, 0, 0);
					}

					layout.put((Integer)entry.getKey(), value);
				}

				int count = 0;
				final Schema sch = new Schema(layout, MetaData.myNodeNum(), device);
				int onPage = 1;
				int lastRequested = 0;

				if (type == 0)
				{
					while (onPage < numBlocks)
					{
						if (lastRequested - onPage < PAGES_IN_ADVANCE)
						{
							final Block[] toRequest = new Block[lastRequested + PREFETCH_REQUEST_SIZE < numBlocks ? PREFETCH_REQUEST_SIZE : numBlocks - lastRequested - 1];
							i = 0;
							final int length = toRequest.length;
							while (i < length)
							{
								toRequest[i] = new Block(tFn, lastRequested + i + 1);
								i++;
							}
							tx.requestPages(toRequest);
							lastRequested += toRequest.length;
						}

						tx.read(new Block(tFn, onPage++), sch);
						final RowIterator rit = sch.rowIterator();
						while (rit.hasNext())
						{
							final Row r = rit.next();
							if (!r.getCol(0).exists())
							{
								continue;
							}
							count++;
							if (count == MAX_PAGES)
							{
								count = 0;
								tx.checkpoint(onPage, tFn);
								// Block[] toRequest = new Block[lastRequested -
								// onPage + 1];
								// i = 0;
								// while (i < toRequest.length)
								// {
								// toRequest[i] = new Block(tFn, onPage+i);
								// i++;
								// }
								//
								// tx.requestPages(toRequest);
								idx.myPages.clear();
								idx.cache.clear();
							}
							final List<FieldValue> row = new ArrayList<FieldValue>(types.size());
							final RID rid = r.getRID();
							int j = 0;
							final int pSize = poses.size();
							while (j < pSize)
							{
								final FieldValue fv = r.getCol(poses.get(j));
								row.add(fv);
								j++;
							}

							// insert into index
							final FieldValue[] fva = new FieldValue[row.size()];
							int x = 0;
							final int rSize = row.size();
							while (x < rSize)
							{
								fva[x] = row.get(x);
								x++;
							}
							idx.insertNoLog(fva, rid);
						}
					}
				}
				else
				{
					// col table
					final List<Integer> cols = new ArrayList<Integer>(poses);
					Collections.sort(cols);
					final int numCols = pos2Col.size();
					final int MY_PREFETCH_REQUEST_SIZE = layout.size() * PREFETCH_REQUEST_SIZE / cols.size();

					RequestPagesThread raThread = null;
					final int MAX_PAGES_IN_ADVANCE = MY_PREFETCH_REQUEST_SIZE * 2;

					while (onPage < numBlocks)
					{
						if (lastRequested - onPage < MAX_PAGES_IN_ADVANCE)
						{
							if (raThread != null)
							{
								raThread.join();
							}

							// Block[] toRequest = new Block[lastRequested +
							// PREFETCH_REQUEST_SIZE < numBlocks ?
							// PREFETCH_REQUEST_SIZE : numBlocks - lastRequested
							// - 1];
							final List<Block> toRequest = new ArrayList<Block>();
							int j = 0;
							final int length = lastRequested + MY_PREFETCH_REQUEST_SIZE < numBlocks ? MY_PREFETCH_REQUEST_SIZE : numBlocks - lastRequested - 1;
							while (j < length)
							{
								if ((lastRequested + j + 1) % layout.size() == 1)
								{
									final Block block = new Block(tFn, lastRequested + j + 1);
									toRequest.add(block);
								}

								j++;
							}

							if (toRequest.size() > 0)
							{
								final Block[] toRequest2 = toRequest.toArray(new Block[toRequest.size()]);
								raThread = tx.requestPages(toRequest2, cols, layout.size());
							}

							lastRequested += length;
						}

						if (onPage % numCols != 1)
						{
							onPage++;
							continue;
						}

						tx.read(new Block(tFn, onPage++), sch, cols, true);
						final Iterator<Entry<RID, FieldValue[]>> rit = sch.colTableIteratorWithRIDs();
						while (rit.hasNext())
						{
							final Map.Entry entry = rit.next();
							final RID rid = (RID)entry.getKey();
							final FieldValue[] urow = (FieldValue[])entry.getValue();
							count++;
							if (count == MAX_PAGES)
							{
								count = 0;
								tx.checkpoint(onPage, tFn);
								// Block[] toRequest = new Block[lastRequested -
								// onPage + 1];
								// i = 0;
								// while (i < toRequest.length)
								// {
								// toRequest[i] = new Block(tFn, onPage+i);
								// i++;
								// }
								//
								// tx.requestPages(toRequest);
								idx.myPages.clear();
								idx.cache.clear();
							}

							final List<FieldValue> r = new ArrayList<FieldValue>(pos2Col.size() * 2);
							int index = 0;
							while (index < pos2Col.size())
							{
								r.add(null);
								index++;
							}

							index = 0;
							for (final FieldValue fv : urow)
							{
								r.add(cols.get(index++), fv);
							}

							final FieldValue[] fva = new FieldValue[poses.size()];
							int j = 0;
							final int pSize = poses.size();
							while (j < pSize)
							{
								final FieldValue fv = r.get(poses.get(j));
								fva[j] = fv;
								j++;
							}

							// insert into index
							idx.insertNoLog(fva, rid);
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
	}

	private static class ReorgThread extends HRDBMSThread
	{
		private final String dir;
		private final String schema;
		private final String table;
		private final List<Index> indexes;
		private final Transaction tx;
		private boolean ok = true;
		private final Exception e = null;
		private final Map<String, String> cols2Types;
		private final Map<Integer, String> pos2Col;
		private final List<Boolean> uniques;
		private int type;

		public ReorgThread(final String dir, final String schema, final String table, final List<Index> indexes, final Transaction tx, final Map<String, String> cols2Types, final Map<Integer, String> pos2Col, final List<Boolean> uniques, final int type)
		{
			this.dir = dir;
			this.schema = schema;
			this.table = table;
			this.indexes = indexes;
			this.tx = tx;
			this.cols2Types = cols2Types;
			this.pos2Col = pos2Col;
			this.uniques = uniques;
			this.type = type;
		}

		public Exception getException()
		{
			return e;
		}

		public boolean getOK()
		{
			return ok;
		}

		@Override
		public void run()
		{
			// FIXME
			type = 0;
			try
			{
				String fn = dir;
				if (!fn.endsWith("/"))
				{
					fn += "/";
				}

				fn += (schema + "." + table + ".tbl");

				if (!FileManager.fileExists(fn))
				{
					return;
				}

				final Map<String, Integer> cols2Pos = new HashMap<String, Integer>();
				for (final Map.Entry entry : pos2Col.entrySet())
				{
					cols2Pos.put((String)entry.getValue(), (Integer)entry.getKey());
				}

				final List<String> indexFNs = new ArrayList<String>();
				for (final Index index : indexes)
				{
					String temp = dir;
					if (!temp.endsWith("/"))
					{
						temp += "/";
					}

					temp += index.getFileName();
					indexFNs.add(temp);
				}

				final Block tSize = new Block(fn, -1);
				LockManager.xLock(tSize, tx.number());
				// FileManager.getFile(fn);
				Integer blocks = FileManager.numBlocks.get(fn);
				if (blocks == null)
				{
					FileManager.getFile(fn);
					blocks = FileManager.numBlocks.get(fn);
				}
				int i = 1;
				while (i < blocks)
				{
					LockManager.sLock(new Block(fn, i), tx.number());
					i++;
				}

				for (final String fn2 : indexFNs)
				{
					final Block iSize = new Block(fn2, -1);
					LockManager.xLock(iSize, tx.number());
					// FileManager.getFile(fn2);
					blocks = FileManager.numBlocks.get(fn2);
					if (blocks == null)
					{
						FileManager.getFile(fn2);
						blocks = FileManager.numBlocks.get(fn2);
					}
					i = 0;
					while (i < blocks)
					{
						LockManager.sLock(new Block(fn2, i), tx.number());
						i++;
					}
				}

				// create new table and index files
				final String newFN = fn + ".new";
				// FIXME type usage and col defs
				final CreateTableThread createT = new CreateTableThread(fn, pos2Col.size(), null, type);
				createT.run();
				final List<String> newIndexFNs = new ArrayList<String>();
				i = 0;
				for (final String fn2 : indexFNs)
				{
					final String temp = fn2 + ".new";
					final CreateIndexThread createI = new CreateIndexThread(fn2, indexes.get(i).getKeys().size(), uniques.get(i), null);
					createI.run();
					newIndexFNs.add(temp);
					i++;
				}

				final Map<Integer, DataType> layout = new HashMap<Integer, DataType>();
				for (final Map.Entry entry : pos2Col.entrySet())
				{
					final String type = cols2Types.get(entry.getValue());
					DataType value = null;
					if (type.equals("INT"))
					{
						value = new DataType(DataType.INTEGER, 0, 0);
					}
					else if (type.equals("FLOAT"))
					{
						value = new DataType(DataType.DOUBLE, 0, 0);
					}
					else if (type.equals("CHAR"))
					{
						value = new DataType(DataType.VARCHAR, 0, 0);
					}
					else if (type.equals("LONG"))
					{
						value = new DataType(DataType.BIGINT, 0, 0);
					}
					else if (type.equals("DATE"))
					{
						value = new DataType(DataType.DATE, 0, 0);
					}

					layout.put((Integer)entry.getKey(), value);
				}

				final Schema sch = new Schema(layout);
				int onPage = 1;
				int lastRequested = 0;
				final List<Index> idxs = new ArrayList<Index>();
				i = 0;
				for (final String index : newIndexFNs)
				{
					final Index idx = new Index(index, indexes.get(i).getKeys(), indexes.get(i).getTypes(), indexes.get(i).getOrders());
					idx.setTransaction(tx);
					idx.open();
					idxs.add(idx);
					i++;
				}

				// LinkedBlockingQueue<Object> queue = new
				// LinkedBlockingQueue<Object>();
				final SPSCQueue queue = new SPSCQueue(ResourceManager.QUEUE_SIZE);
				final IndexWriterThread thread = new IndexWriterThread(queue, idxs, true);
				thread.start();
				// FileManager.getFile(newFN);
				Integer block = FileManager.numBlocks.get(newFN);
				if (block == null)
				{
					FileManager.getFile(newFN);
					block = FileManager.numBlocks.get(newFN);
				}
				// request block
				final Map<Integer, DataType> layout2 = new HashMap<Integer, DataType>();
				Schema sch2 = new Schema(layout);
				Block toRequest2 = new Block(newFN, block);
				tx.requestPage(toRequest2);
				tx.read(toRequest2, sch2, true);
				// new ArrayList<RIDAndIndexKeys>();

				while (onPage < blocks)
				{
					if (lastRequested - onPage < PAGES_IN_ADVANCE)
					{
						final Block[] toRequest = new Block[lastRequested + PREFETCH_REQUEST_SIZE < blocks ? PREFETCH_REQUEST_SIZE : blocks - lastRequested - 1];
						i = 0;
						final int length = toRequest.length;
						while (i < length)
						{
							toRequest[i] = new Block(fn, lastRequested + i + 1);
							i++;
						}
						tx.requestPages(toRequest);
						lastRequested += toRequest.length;
					}

					tx.read(new Block(fn, onPage++), sch);
					final RowIterator rit = sch.rowIterator();
					while (rit.hasNext())
					{
						final Row r = rit.next();
						if (!r.getCol(0).exists())
						{
							continue;
						}

						final RID rid = sch2.insertRowAppend(r.getAllCols());
						final List<List<Object>> indexKeys = new ArrayList<List<Object>>();
						i = 0;
						for (final Index index : idxs)
						{
							final List<String> key = index.getKeys();
							final List<Object> k = new ArrayList<Object>();
							for (final String col : key)
							{
								k.add(r.getCol(cols2Pos.get(col)));
							}

							indexKeys.add(k);
							i++;
						}

						queue.put(new RIDAndIndexKeys(rid, indexKeys));
						final int newBlock = rid.getBlockNum();
						if (newBlock != block)
						{
							block = newBlock;
							sch2.close();
							sch2 = new Schema(layout2);
							toRequest2 = new Block(newFN, block);
							tx.requestPage(toRequest2);
							tx.read(toRequest2, sch2, true);
						}
					}
				}

				sch2.close();

				// wait for index writer to finish
				queue.put(new DataEndMarker());
				thread.join();
				if (!thread.getOK())
				{
					ok = false;
				}

				HRDBMSWorker.checkpoint.doCheckpoint();

				blocks = FileManager.numBlocks.get(fn);
				if (blocks == null)
				{
					FileManager.getFile(fn);
					blocks = FileManager.numBlocks.get(fn);
				}
				i = 1;
				while (i < blocks)
				{
					LockManager.xLock(new Block(fn, i), tx.number());
					i++;
				}

				for (final String fn2 : indexFNs)
				{
					// FileManager.getFile(fn2);
					blocks = FileManager.numBlocks.get(fn2);
					if (blocks == null)
					{
						FileManager.getFile(fn2);
						blocks = FileManager.numBlocks.get(fn2);
					}
					i = 0;
					while (i < blocks)
					{
						LockManager.xLock(new Block(fn2, i), tx.number());
						i++;
					}
				}

				FileChannel fc = FileManager.getFile(fn);
				((SparseCompressedFileChannel2)fc).copyFromFC((SparseCompressedFileChannel2)FileManager.getFile(newFN));

				for (final String fn2 : indexFNs)
				{
					fc = FileManager.getFile(fn2);
					((SparseCompressedFileChannel2)fc).copyFromFC((SparseCompressedFileChannel2)FileManager.getFile(fn2 + ".new"));
				}

				for (final Block b : TableScanOperator.noResults.getKeySet())
				{
					if (b.fileName().equals(fn))
					{
						TableScanOperator.noResults.remove(b);
					}
				}
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.debug("", e);
				ok = false;
			}
		}
	}

	private static class SendCheckpointThread extends HRDBMSThread
	{
		private final Object o;
		private boolean sendOK;

		public SendCheckpointThread(final Object o)
		{
			this.o = o;
		}

		@Override
		public void run()
		{
			if (o instanceof String)
			{
				Socket sock = null;
				try
				{
					// sock = new Socket((String)o,
					// Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
					sock = new Socket();
					sock.setReceiveBufferSize(4194304);
					sock.setSendBufferSize(4194304);
					sock.connect(new InetSocketAddress((String)o, Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number"))));
					final OutputStream out = sock.getOutputStream();
					final byte[] outMsg = "CHECKPNT        ".getBytes(StandardCharsets.UTF_8);
					outMsg[8] = 0;
					outMsg[9] = 0;
					outMsg[10] = 0;
					outMsg[11] = 0;
					outMsg[12] = 0;
					outMsg[13] = 0;
					outMsg[14] = 0;
					outMsg[15] = 0;
					out.write(outMsg);
					final ObjectOutputStream objOut = new ObjectOutputStream(out);
					final List<Object> alo = new ArrayList<Object>(1);
					alo.add(o);
					objOut.writeObject(alo);
					objOut.flush();
					out.flush();
					getConfirmation(sock);
					objOut.close();
					sock.close();
				}
				catch (final Exception e)
				{
					sendOK = false;
					return;
				}
				sendOK = true;
			}
			else if (((List<Object>)o).size() > 0)
			{
				Socket sock = null;
				Object obj2 = ((List<Object>)o).get(0);
				while (obj2 instanceof ArrayList)
				{
					obj2 = ((List<Object>)obj2).get(0);
				}

				final String hostname = (String)obj2;
				try
				{
					// sock = new Socket(hostname,
					// Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
					sock = new Socket();
					sock.setReceiveBufferSize(4194304);
					sock.setSendBufferSize(4194304);
					sock.connect(new InetSocketAddress(hostname, Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number"))));
					final OutputStream out = sock.getOutputStream();
					final byte[] outMsg = "PREPARE         ".getBytes(StandardCharsets.UTF_8);
					outMsg[8] = 0;
					outMsg[9] = 0;
					outMsg[10] = 0;
					outMsg[11] = 0;
					outMsg[12] = 0;
					outMsg[13] = 0;
					outMsg[14] = 0;
					outMsg[15] = 0;
					out.write(outMsg);
					final ObjectOutputStream objOut = new ObjectOutputStream(out);
					objOut.writeObject(o);
					objOut.flush();
					out.flush();
					getConfirmation(sock);
					objOut.close();
					sock.close();
				}
				catch (final Exception e)
				{
					sendOK = false;
					return;
				}

				sendOK = true;
			}
		}

		public boolean sendOK()
		{
			return sendOK;
		}
	}

	private static class SendCommitThread extends HRDBMSThread
	{
		private final Object o;
		private final Transaction tx;

		public SendCommitThread(final Object o, final Transaction tx)
		{
			this.o = o;
			this.tx = tx;
		}

		@Override
		public void run()
		{
			if (o instanceof String)
			{
				Socket sock = null;
				try
				{
					// sock = new Socket((String)o,
					// Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
					sock = new Socket();
					sock.setReceiveBufferSize(4194304);
					sock.setSendBufferSize(4194304);
					sock.connect(new InetSocketAddress((String)o, Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number"))));
					final OutputStream out = sock.getOutputStream();
					final byte[] outMsg = "LCOMMIT         ".getBytes(StandardCharsets.UTF_8);
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
					final ObjectOutputStream objOut = new ObjectOutputStream(out);
					final List<Object> alo = new ArrayList<Object>(1);
					alo.add(o);
					objOut.writeObject(alo);
					objOut.flush();
					out.flush();
					getConfirmation(sock);
					objOut.close();
					sock.close();
				}
				catch (final Exception e)
				{
					// TODO blackListByHost((String)o);
					// TODO queueCommandByHost((String)o, "COMMIT", tx);
					HRDBMSWorker.logger.fatal("BLACKLIST", e);
					System.exit(1);
				}
			}
			else if (((List<Object>)o).size() > 0)
			{
				Socket sock = null;
				Object obj2 = ((List<Object>)o).get(0);
				while (obj2 instanceof ArrayList)
				{
					obj2 = ((List<Object>)obj2).get(0);
				}

				final String hostname = (String)obj2;
				try
				{
					// sock = new Socket(hostname,
					// Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
					sock = new Socket();
					sock.setReceiveBufferSize(4194304);
					sock.setSendBufferSize(4194304);
					sock.connect(new InetSocketAddress(hostname, Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number"))));
					final OutputStream out = sock.getOutputStream();
					final byte[] outMsg = "LCOMMIT         ".getBytes(StandardCharsets.UTF_8);
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
					final ObjectOutputStream objOut = new ObjectOutputStream(out);
					objOut.writeObject(o);
					objOut.flush();
					out.flush();
					getConfirmation(sock);
					objOut.close();
					sock.close();
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
					// TODO blackListByHost((String)obj2);
					// TODO queueCommandByHost((String)obj2, "COMMIT", tx);
					HRDBMSWorker.logger.fatal("BLACKLIST", e);
					System.exit(1);
					// retry others
					final boolean toDo = rebuildTree((List<Object>)o, (String)obj2);
					if (toDo)
					{
						sendCommit((List<Object>)o, tx);
					}
				}
			}
		}
	}

	private static class SendDelFiIThread extends HRDBMSThread
	{
		private final Object o;
		private boolean sendOK;
		private final List<String> indexes;

		public SendDelFiIThread(final Object o, final List<String> indexes)
		{
			this.o = o;
			this.indexes = indexes;
		}

		@Override
		public void run()
		{
			if (o instanceof String)
			{
				Socket sock = null;
				try
				{
					// sock = new Socket((String)o,
					// Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
					sock = new Socket();
					sock.setReceiveBufferSize(4194304);
					sock.setSendBufferSize(4194304);
					sock.connect(new InetSocketAddress((String)o, Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number"))));
					final OutputStream out = sock.getOutputStream();
					final byte[] outMsg = "DELFIIDX        ".getBytes(StandardCharsets.UTF_8);
					outMsg[8] = 0;
					outMsg[9] = 0;
					outMsg[10] = 0;
					outMsg[11] = 0;
					outMsg[12] = 0;
					outMsg[13] = 0;
					outMsg[14] = 0;
					outMsg[15] = 0;
					out.write(outMsg);
					final ObjectOutputStream objOut = new ObjectOutputStream(out);
					final List<Object> alo = new ArrayList<Object>(1);
					alo.add(o);
					objOut.writeObject(alo);
					objOut.writeObject(indexes);
					objOut.flush();
					out.flush();
					getConfirmation(sock);
					objOut.close();
					sock.close();
				}
				catch (final Exception e)
				{
					sendOK = false;
					return;
				}
				sendOK = true;
			}
			else if (((List<Object>)o).size() > 0)
			{
				Socket sock = null;
				Object obj2 = ((List<Object>)o).get(0);
				while (obj2 instanceof ArrayList)
				{
					obj2 = ((List<Object>)obj2).get(0);
				}

				final String hostname = (String)obj2;
				try
				{
					// sock = new Socket(hostname,
					// Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
					sock = new Socket();
					sock.setReceiveBufferSize(4194304);
					sock.setSendBufferSize(4194304);
					sock.connect(new InetSocketAddress(hostname, Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number"))));
					final OutputStream out = sock.getOutputStream();
					final byte[] outMsg = "DELFIIDX        ".getBytes(StandardCharsets.UTF_8);
					outMsg[8] = 0;
					outMsg[9] = 0;
					outMsg[10] = 0;
					outMsg[11] = 0;
					outMsg[12] = 0;
					outMsg[13] = 0;
					outMsg[14] = 0;
					outMsg[15] = 0;
					out.write(outMsg);
					final ObjectOutputStream objOut = new ObjectOutputStream(out);
					objOut.writeObject(o);
					objOut.writeObject(indexes);
					objOut.flush();
					out.flush();
					getConfirmation(sock);
					objOut.close();
					sock.close();
				}
				catch (final Exception e)
				{
					sendOK = false;
					return;
				}

				sendOK = true;
			}
		}

		public boolean sendOK()
		{
			return sendOK;
		}
	}

	private static class SendDelFiTThread extends HRDBMSThread
	{
		private final Object o;
		private boolean sendOK;
		private final List<String> tables;

		public SendDelFiTThread(final Object o, final List<String> tables)
		{
			this.o = o;
			this.tables = tables;
		}

		@Override
		public void run()
		{
			if (o instanceof String)
			{
				Socket sock = null;
				try
				{
					// sock = new Socket((String)o,
					// Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
					sock = new Socket();
					sock.setReceiveBufferSize(4194304);
					sock.setSendBufferSize(4194304);
					sock.connect(new InetSocketAddress((String)o, Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number"))));
					final OutputStream out = sock.getOutputStream();
					final byte[] outMsg = "DELFITBL        ".getBytes(StandardCharsets.UTF_8);
					outMsg[8] = 0;
					outMsg[9] = 0;
					outMsg[10] = 0;
					outMsg[11] = 0;
					outMsg[12] = 0;
					outMsg[13] = 0;
					outMsg[14] = 0;
					outMsg[15] = 0;
					out.write(outMsg);
					final ObjectOutputStream objOut = new ObjectOutputStream(out);
					final List<Object> alo = new ArrayList<Object>(1);
					alo.add(o);
					objOut.writeObject(alo);
					objOut.writeObject(tables);
					objOut.flush();
					out.flush();
					getConfirmation(sock);
					objOut.close();
					sock.close();
				}
				catch (final Exception e)
				{
					sendOK = false;
					return;
				}
				sendOK = true;
			}
			else if (((List<Object>)o).size() > 0)
			{
				Socket sock = null;
				Object obj2 = ((List<Object>)o).get(0);
				while (obj2 instanceof ArrayList)
				{
					obj2 = ((List<Object>)obj2).get(0);
				}

				final String hostname = (String)obj2;
				try
				{
					// sock = new Socket(hostname,
					// Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
					sock = new Socket();
					sock.setReceiveBufferSize(4194304);
					sock.setSendBufferSize(4194304);
					sock.connect(new InetSocketAddress(hostname, Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number"))));
					final OutputStream out = sock.getOutputStream();
					final byte[] outMsg = "DELFITBL        ".getBytes(StandardCharsets.UTF_8);
					outMsg[8] = 0;
					outMsg[9] = 0;
					outMsg[10] = 0;
					outMsg[11] = 0;
					outMsg[12] = 0;
					outMsg[13] = 0;
					outMsg[14] = 0;
					outMsg[15] = 0;
					out.write(outMsg);
					final ObjectOutputStream objOut = new ObjectOutputStream(out);
					objOut.writeObject(o);
					objOut.writeObject(tables);
					objOut.flush();
					out.flush();
					getConfirmation(sock);
					objOut.close();
					sock.close();
				}
				catch (final Exception e)
				{
					sendOK = false;
					return;
				}

				sendOK = true;
			}
		}

		public boolean sendOK()
		{
			return sendOK;
		}
	}

	private static class SendHierClusterThread extends HRDBMSThread
	{
		private final String schema;
		private final String table;
		private final Map<Integer, String> pos2Col;
		private final Map<String, String> cols2Types;

		private final Object o;
		private boolean ok = true;
		private final Transaction tx;
		private final int type;

		public SendHierClusterThread(final String schema, final String table, final Map<Integer, String> pos2Col, final Map<String, String> cols2Types, final Object o, final Transaction tx, final int type)
		{
			this.schema = schema;
			this.table = table;
			this.pos2Col = pos2Col;
			this.cols2Types = cols2Types;
			this.o = o;
			this.tx = tx;
			this.type = type;
		}

		public boolean getOK()
		{
			return ok;
		}

		@Override
		public void run()
		{
			if (o instanceof String)
			{
				Socket sock = null;
				try
				{
					// sock = new Socket((String)o,
					// Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
					sock = new Socket();
					sock.setReceiveBufferSize(4194304);
					sock.setSendBufferSize(4194304);
					sock.connect(new InetSocketAddress((String)o, Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number"))));
					final OutputStream out = sock.getOutputStream();
					final byte[] outMsg = "CLUSTER         ".getBytes(StandardCharsets.UTF_8);
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
					final ObjectOutputStream objOut = new ObjectOutputStream(out);
					final List<Object> alo = new ArrayList<Object>(1);
					alo.add(o);
					objOut.writeObject(alo);
					objOut.writeObject(pos2Col);
					objOut.writeObject(cols2Types);
					objOut.flush();
					out.flush();
					getConfirmation(sock);
					objOut.close();
					sock.close();
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.debug("", e);
					ok = false;
				}
			}
			else if (((List<Object>)o).size() > 0)
			{
				Socket sock = null;
				Object obj2 = ((List<Object>)o).get(0);
				while (obj2 instanceof ArrayList)
				{
					obj2 = ((List<Object>)obj2).get(0);
				}

				final String hostname = (String)obj2;
				try
				{
					// sock = new Socket(hostname,
					// Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
					sock = new Socket();
					sock.setReceiveBufferSize(4194304);
					sock.setSendBufferSize(4194304);
					sock.connect(new InetSocketAddress(hostname, Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number"))));
					final OutputStream out = sock.getOutputStream();
					final byte[] outMsg = "CLUSTER         ".getBytes(StandardCharsets.UTF_8);
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
					final ObjectOutputStream objOut = new ObjectOutputStream(out);
					objOut.writeObject(o);
					objOut.writeObject(pos2Col);
					objOut.writeObject(cols2Types);
					objOut.flush();
					out.flush();
					getConfirmation(sock);
					objOut.close();
					sock.close();
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.debug("", e);
					ok = false;
				}
			}
		}
	}

	// SendHierClusterThread(schema, table, pos2Col, cols2Types, o, tx, type)

	private static class SendHierNewIndexThread extends HRDBMSThread
	{
		private final byte[] ncBytes;
		private final byte[] uBytes;
		private final byte[] fnLenBytes;
		private final byte[] fnBytes;
		List<Integer> devices;
		Object o;
		boolean ok = true;
		Transaction tx;

		public SendHierNewIndexThread(final byte[] ncBytes, final byte[] uBytes, final byte[] fnLenBytes, final byte[] fnBytes, final List<Integer> devices, final Object o, final Transaction tx)
		{
			this.ncBytes = ncBytes;
			this.uBytes = uBytes;
			this.fnLenBytes = fnLenBytes;
			this.fnBytes = fnBytes;
			this.devices = devices;
			this.o = o;
			this.tx = tx;
		}

		public boolean getOK()
		{
			return ok;
		}

		@Override
		public void run()
		{
			if (o instanceof String)
			{
				Socket sock = null;
				try
				{
					// sock = new Socket((String)o,
					// Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
					sock = new Socket();
					sock.setReceiveBufferSize(4194304);
					sock.setSendBufferSize(4194304);
					sock.connect(new InetSocketAddress((String)o, Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number"))));
					final OutputStream out = sock.getOutputStream();
					final byte[] outMsg = "NEWINDEX        ".getBytes(StandardCharsets.UTF_8);
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
					out.write(ncBytes);
					out.write(uBytes);
					out.write(fnLenBytes);
					out.write(fnBytes);
					final ObjectOutputStream objOut = new ObjectOutputStream(out);
					objOut.writeObject(devices);
					final List<Object> alo = new ArrayList<Object>(1);
					alo.add(o);
					objOut.writeObject(alo);
					objOut.flush();
					out.flush();
					getConfirmation(sock);
					objOut.close();
					sock.close();
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.debug("", e);
					ok = false;
				}
			}
			else if (((List<Object>)o).size() > 0)
			{
				Socket sock = null;
				Object obj2 = ((List<Object>)o).get(0);
				while (obj2 instanceof ArrayList)
				{
					obj2 = ((List<Object>)obj2).get(0);
				}

				final String hostname = (String)obj2;
				try
				{
					// sock = new Socket(hostname,
					// Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
					sock = new Socket();
					sock.setReceiveBufferSize(4194304);
					sock.setSendBufferSize(4194304);
					sock.connect(new InetSocketAddress(hostname, Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number"))));
					final OutputStream out = sock.getOutputStream();
					final byte[] outMsg = "NEWINDEX        ".getBytes(StandardCharsets.UTF_8);
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
					out.write(ncBytes);
					out.write(uBytes);
					out.write(fnLenBytes);
					out.write(fnBytes);
					final ObjectOutputStream objOut = new ObjectOutputStream(out);
					objOut.writeObject(devices);
					objOut.writeObject(o);
					objOut.flush();
					out.flush();
					getConfirmation(sock);
					objOut.close();
					sock.close();
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.debug("", e);
					ok = false;
				}
			}
		}
	}

	private static class SendHierNewTableThread extends HRDBMSThread
	{
		private final byte[] ncBytes;
		private final byte[] fnLenBytes;
		private final byte[] fnBytes;
		private final List<Integer> devices;
		private final Object o;
		private boolean ok = true;
		private final Transaction tx;
		private final int type;
		private final List<ColDef> defs;
		private final List<Integer> colOrder;
		private final List<Integer> organization;

		public SendHierNewTableThread(final byte[] ncBytes, final byte[] fnLenBytes, final byte[] fnBytes, final List<Integer> devices, final Object o, final Transaction tx, final int type, final List<ColDef> defs, final List<Integer> colOrder, final List<Integer> organization)
		{
			this.ncBytes = ncBytes;
			this.fnLenBytes = fnLenBytes;
			this.fnBytes = fnBytes;
			this.devices = devices;
			this.o = o;
			this.tx = tx;
			this.type = type;
			this.defs = defs;
			this.colOrder = colOrder;
			this.organization = organization;
		}

		public boolean getOK()
		{
			return ok;
		}

		@Override
		public void run()
		{
			if (o instanceof String)
			{
				Socket sock = null;
				try
				{
					// sock = new Socket((String)o,
					// Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
					sock = new Socket();
					sock.setReceiveBufferSize(4194304);
					sock.setSendBufferSize(4194304);
					sock.connect(new InetSocketAddress((String)o, Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number"))));
					final OutputStream out = sock.getOutputStream();
					final byte[] outMsg = "NEWTABLE        ".getBytes(StandardCharsets.UTF_8);
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
					out.write(ncBytes);
					out.write(fnLenBytes);
					out.write(fnBytes);
					out.write(intToBytes(type));
					final ObjectOutputStream objOut = new ObjectOutputStream(out);
					objOut.writeObject(devices);
					final List<Object> alo = new ArrayList<Object>(1);
					alo.add(o);
					objOut.writeObject(alo);
					objOut.writeObject(defs);
					if (type != 0)
					{
						objOut.writeObject(colOrder);
						objOut.writeObject(organization);
					}
					objOut.flush();
					out.flush();
					getConfirmation(sock);
					objOut.close();
					sock.close();
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.debug("", e);
					ok = false;
				}
			}
			else if (((List<Object>)o).size() > 0)
			{
				Socket sock = null;
				Object obj2 = ((List<Object>)o).get(0);
				while (obj2 instanceof ArrayList)
				{
					obj2 = ((List<Object>)obj2).get(0);
				}

				final String hostname = (String)obj2;
				try
				{
					// sock = new Socket(hostname,
					// Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
					sock = new Socket();
					sock.setReceiveBufferSize(4194304);
					sock.setSendBufferSize(4194304);
					sock.connect(new InetSocketAddress(hostname, Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number"))));
					final OutputStream out = sock.getOutputStream();
					final byte[] outMsg = "NEWTABLE        ".getBytes(StandardCharsets.UTF_8);
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
					out.write(ncBytes);
					out.write(fnLenBytes);
					out.write(fnBytes);
					out.write(intToBytes(type));
					final ObjectOutputStream objOut = new ObjectOutputStream(out);
					objOut.writeObject(devices);
					objOut.writeObject(o);
					objOut.writeObject(defs);
					if (type != 0)
					{
						objOut.writeObject(colOrder);
						objOut.writeObject(organization);
					}
					objOut.flush();
					out.flush();
					getConfirmation(sock);
					objOut.close();
					sock.close();
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.debug("", e);
					ok = false;
				}
			}
		}
	}

	private static class SendHierPopIndexThread extends HRDBMSThread
	{
		private final byte[] txBytes;
		private final byte[] fnLenBytes;
		private final byte[] fnBytes;
		private final byte[] fn2LenBytes;
		private final byte[] fn2Bytes;
		private final List<Integer> devices;
		private final List<String> keys;
		private final List<String> types;
		private final List<Boolean> orders;
		private final List<Integer> poses;
		private final Map<Integer, String> pos2Col;
		private final Map<String, String> cols2Types;
		Object o;
		boolean ok = true;
		int type;

		public SendHierPopIndexThread(final byte[] txBytes, final byte[] fnLenBytes, final byte[] fnBytes, final byte[] fn2LenBytes, final byte[] fn2Bytes, final List<Integer> devices, final List<String> keys, final List<String> types, final List<Boolean> orders, final List<Integer> poses, final Map<Integer, String> pos2Col, final Map<String, String> cols2Types, final Object o, final int type)
		{
			this.txBytes = txBytes;
			this.fnLenBytes = fnLenBytes;
			this.fnBytes = fnBytes;
			this.fn2LenBytes = fn2LenBytes;
			this.fn2Bytes = fn2Bytes;
			this.devices = devices;
			this.keys = keys;
			this.types = types;
			this.orders = orders;
			this.poses = poses;
			this.pos2Col = pos2Col;
			this.cols2Types = cols2Types;
			this.o = o;
			this.type = type;
		}

		public boolean getOK()
		{
			return ok;
		}

		@Override
		public void run()
		{
			if (o instanceof String)
			{
				Socket sock = null;
				try
				{
					// sock = new Socket((String)o,
					// Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
					sock = new Socket();
					sock.setReceiveBufferSize(4194304);
					sock.setSendBufferSize(4194304);
					sock.connect(new InetSocketAddress((String)o, Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number"))));
					final OutputStream out = sock.getOutputStream();
					final byte[] outMsg = "POPINDEX        ".getBytes(StandardCharsets.UTF_8);
					outMsg[8] = 0;
					outMsg[9] = 0;
					outMsg[10] = 0;
					outMsg[11] = 0;
					outMsg[12] = 0;
					outMsg[13] = 0;
					outMsg[14] = 0;
					outMsg[15] = 0;
					out.write(outMsg);
					out.write(txBytes);
					out.write(fnLenBytes);
					out.write(fnBytes);
					out.write(fn2LenBytes);
					out.write(fn2Bytes);
					out.write(intToBytes(type));
					final ObjectOutputStream objOut = new ObjectOutputStream(out);
					objOut.writeObject(devices);
					final List<Object> alo = new ArrayList<Object>(1);
					alo.add(o);
					objOut.writeObject(keys);
					objOut.writeObject(types);
					objOut.writeObject(orders);
					objOut.writeObject(poses);
					objOut.writeObject(pos2Col);
					objOut.writeObject(cols2Types);
					objOut.writeObject(alo);
					objOut.flush();
					out.flush();
					getConfirmation(sock);
					objOut.close();
					sock.close();
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.debug("", e);
					ok = false;
				}
			}
			else if (((List<Object>)o).size() > 0)
			{
				Socket sock = null;
				Object obj2 = ((List<Object>)o).get(0);
				while (obj2 instanceof ArrayList)
				{
					obj2 = ((List<Object>)obj2).get(0);
				}

				final String hostname = (String)obj2;
				try
				{
					// sock = new Socket(hostname,
					// Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
					sock = new Socket();
					sock.setReceiveBufferSize(4194304);
					sock.setSendBufferSize(4194304);
					sock.connect(new InetSocketAddress(hostname, Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number"))));
					final OutputStream out = sock.getOutputStream();
					final byte[] outMsg = "POPINDEX        ".getBytes(StandardCharsets.UTF_8);
					outMsg[8] = 0;
					outMsg[9] = 0;
					outMsg[10] = 0;
					outMsg[11] = 0;
					outMsg[12] = 0;
					outMsg[13] = 0;
					outMsg[14] = 0;
					outMsg[15] = 0;
					out.write(outMsg);
					out.write(txBytes);
					out.write(fnLenBytes);
					out.write(fnBytes);
					out.write(fn2LenBytes);
					out.write(fn2Bytes);
					out.write(intToBytes(type));
					final ObjectOutputStream objOut = new ObjectOutputStream(out);
					objOut.writeObject(devices);
					objOut.writeObject(keys);
					objOut.writeObject(types);
					objOut.writeObject(orders);
					objOut.writeObject(poses);
					objOut.writeObject(pos2Col);
					objOut.writeObject(cols2Types);
					objOut.writeObject(o);
					objOut.flush();
					out.flush();
					getConfirmation(sock);
					objOut.close();
					sock.close();
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.debug("", e);
					ok = false;
				}
			}
		}
	}

	private static class SendLDMDThread extends HRDBMSThread
	{
		private final Object o;
		private boolean sendOK;
		private final byte[] lenBytes;
		private final byte[] data;
		private final LoadMetaData ldmd;

		public SendLDMDThread(final Object o, final byte[] lenBytes, final byte[] data, final LoadMetaData ldmd)
		{
			this.o = o;
			this.lenBytes = lenBytes;
			this.data = data;
			this.ldmd = ldmd;
		}

		@Override
		public void run()
		{
			if (o instanceof String)
			{
				Socket sock = null;
				try
				{
					// sock = new Socket((String)o,
					// Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
					sock = new Socket();
					sock.setReceiveBufferSize(4194304);
					sock.setSendBufferSize(4194304);
					sock.connect(new InetSocketAddress((String)o, Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number"))));
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
					out.write(lenBytes);
					out.write(data);
					final ObjectOutputStream objOut = new ObjectOutputStream(out);
					final List<Object> alo = new ArrayList<Object>(1);
					alo.add(o);
					objOut.writeObject(alo);
					objOut.writeObject(ldmd);
					objOut.flush();
					out.flush();
					getConfirmation(sock);
					objOut.close();
					sock.close();
				}
				catch (final Exception e)
				{
					sendOK = false;
					return;
				}
				sendOK = true;
			}
			else if (((List<Object>)o).size() > 0)
			{
				Socket sock = null;
				Object obj2 = ((List<Object>)o).get(0);
				while (obj2 instanceof ArrayList)
				{
					obj2 = ((List<Object>)obj2).get(0);
				}

				final String hostname = (String)obj2;
				try
				{
					// sock = new Socket(hostname,
					// Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
					sock = new Socket();
					sock.setReceiveBufferSize(4194304);
					sock.setSendBufferSize(4194304);
					sock.connect(new InetSocketAddress(hostname, Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number"))));
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
					out.write(lenBytes);
					out.write(data);
					final ObjectOutputStream objOut = new ObjectOutputStream(out);
					objOut.writeObject(o);
					objOut.writeObject(ldmd);
					objOut.flush();
					out.flush();
					getConfirmation(sock);
					objOut.close();
					sock.close();
				}
				catch (final Exception e)
				{
					sendOK = false;
					return;
				}

				sendOK = true;
			}
		}

		public boolean sendOK()
		{
			return sendOK;
		}
	}

	private class SendMassDeleteThread extends HRDBMSThread
	{
		private final List<Object> tree;
		private final Transaction tx;
		private boolean ok;
		// int num;
		String schema;
		String table;
		List<List<String>> keys;
		List<List<String>> types;
		List<List<Boolean>> orders;
		List<String> indexes;
		Map<Integer, String> pos2Col;
		Map<String, String> cols2Types;
		boolean logged;
		int type;

		public SendMassDeleteThread(final List<Object> tree, final Transaction tx, final String schema, final String table, final List<List<String>> keys, final List<List<String>> types, final List<List<Boolean>> orders, final List<String> indexes, final Map<Integer, String> pos2Col, final Map<String, String> cols2Types, final boolean logged, final int type)
		{
			this.tree = tree;
			this.tx = tx;
			this.schema = schema;
			this.table = table;
			this.keys = keys;
			this.types = types;
			this.orders = orders;
			this.indexes = indexes;
			this.pos2Col = pos2Col;
			this.cols2Types = cols2Types;
			this.logged = logged;
			this.type = type;
		}

		public boolean getOK()
		{
			return ok;
		}

		@Override
		public void run()
		{
			sendMassDelete(tree, tx);
		}

		private void sendMassDelete(final List<Object> tree, final Transaction tx)
		{
			Object obj = tree.get(0);
			while (obj instanceof ArrayList)
			{
				obj = ((List)obj).get(0);
			}

			final String hostname = (String)obj;
			Socket sock = null;
			try
			{
				// sock = new Socket(hostname,
				// Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
				sock = new Socket();
				sock.setReceiveBufferSize(4194304);
				sock.setSendBufferSize(4194304);
				sock.connect(new InetSocketAddress(hostname, Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number"))));
				final OutputStream out = sock.getOutputStream();
				final byte[] outMsg = "MDELETE         ".getBytes(StandardCharsets.UTF_8);
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
				// write schema and table
				out.write(stringToBytes(schema));
				out.write(stringToBytes(table));
				if (logged)
				{
					out.write(1);
				}
				else
				{
					out.write(0);
				}
				out.write(intToBytes(type));
				final ObjectOutputStream objOut = new ObjectOutputStream(out);
				objOut.writeObject(tree);
				objOut.writeObject(indexes);
				objOut.writeObject(keys);
				objOut.writeObject(types);
				objOut.writeObject(orders);
				objOut.writeObject(pos2Col);
				objOut.writeObject(cols2Types);
				objOut.flush();
				out.flush();
				getConfirmation(sock);
				int count = 4;
				final int off = 0;
				final byte[] numBytes = new byte[4];
				while (count > 0)
				{
					final int temp = sock.getInputStream().read(numBytes, off, 4 - off);
					if (temp == -1)
					{
						ok = false;
						objOut.close();
						sock.close();
					}

					count -= temp;
				}

				// num = bytesToInt(numBytes);
				objOut.close();
				sock.close();
				ok = true;
			}
			catch (final Exception e)
			{
				ok = false;
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
				HRDBMSWorker.logger.debug("", e);
			}
		}
	}

	private static class SendPrepareThread extends HRDBMSThread
	{
		private final Object o;
		private final Transaction tx;
		private boolean sendOK;
		private final byte[] lenBytes;
		private final byte[] data;

		public SendPrepareThread(final Object o, final Transaction tx, final byte[] lenBytes, final byte[] data)
		{
			this.o = o;
			this.tx = tx;
			this.lenBytes = lenBytes;
			this.data = data;
		}

		@Override
		public void run()
		{
			if (o instanceof String)
			{
				Socket sock = null;
				try
				{
					// sock = new Socket((String)o,
					// Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
					sock = new Socket();
					sock.setReceiveBufferSize(4194304);
					sock.setSendBufferSize(4194304);
					sock.connect(new InetSocketAddress((String)o, Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number"))));
					final OutputStream out = sock.getOutputStream();
					final byte[] outMsg = "PREPARE         ".getBytes(StandardCharsets.UTF_8);
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
					out.write(lenBytes);
					out.write(data);
					final ObjectOutputStream objOut = new ObjectOutputStream(out);
					final List<Object> alo = new ArrayList<Object>(1);
					alo.add(o);
					objOut.writeObject(alo);
					objOut.flush();
					out.flush();
					getConfirmation(sock);
					objOut.close();
					sock.close();
				}
				catch (final Exception e)
				{
					sendOK = false;
					return;
				}
				sendOK = true;
			}
			else if (((List<Object>)o).size() > 0)
			{
				Socket sock = null;
				Object obj2 = ((List<Object>)o).get(0);
				while (obj2 instanceof ArrayList)
				{
					obj2 = ((List<Object>)obj2).get(0);
				}

				final String hostname = (String)obj2;
				try
				{
					// sock = new Socket(hostname,
					// Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
					sock = new Socket();
					sock.setReceiveBufferSize(4194304);
					sock.setSendBufferSize(4194304);
					sock.connect(new InetSocketAddress(hostname, Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number"))));
					final OutputStream out = sock.getOutputStream();
					final byte[] outMsg = "PREPARE         ".getBytes(StandardCharsets.UTF_8);
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
					out.write(lenBytes);
					out.write(data);
					final ObjectOutputStream objOut = new ObjectOutputStream(out);
					objOut.writeObject(o);
					objOut.flush();
					out.flush();
					getConfirmation(sock);
					objOut.close();
					sock.close();
				}
				catch (final Exception e)
				{
					sendOK = false;
					return;
				}

				sendOK = true;
			}
		}

		public boolean sendOK()
		{
			return sendOK;
		}
	}

	private static class SendPut2Thread extends HRDBMSThread
	{
		private final String schema;
		private final String table;
		private final int node;
		private final int device;
		private final Object key;
		private final Object value;
		private final Transaction tx;
		private boolean ok = true;
		private Exception e;
		private final Transaction tx2;

		public SendPut2Thread(final String schema, final String table, final int node, final int device, final Object key, final Transaction tx, final Object value, final Transaction tx2)
		{
			this.schema = schema;
			this.table = table;
			this.node = node;
			this.device = device;
			this.key = key;
			this.tx = tx;
			this.value = value;
			this.tx2 = tx2;
		}

		public Exception getException()
		{
			return e;
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
				// new MetaData();
				final String host = MetaData.getHostNameForNode(node);
				// Socket sock = new Socket(host,
				// Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
				final Socket sock = new Socket();
				sock.setReceiveBufferSize(4194304);
				sock.setSendBufferSize(4194304);
				sock.connect(new InetSocketAddress(host, Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number"))));
				final OutputStream out = sock.getOutputStream();
				final byte[] outMsg = "PUT2            ".getBytes(StandardCharsets.UTF_8);
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
				out.write(longToBytes(tx2.number()));
				out.write(stringToBytes(schema));
				out.write(stringToBytes(table));
				out.write(intToBytes(device));
				final ObjectOutputStream objOut = new ObjectOutputStream(out);
				objOut.writeObject(key);
				objOut.writeObject(value);
				objOut.flush();
				out.flush();
				getConfirmation(sock);
				objOut.close();
				sock.close();
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.debug("", e);
				ok = false;
				this.e = e;
			}
		}
	}

	private static class SendRemove2Thread extends HRDBMSThread
	{
		private final String schema;
		private final String table;
		private final int node;
		private final int device;
		private final Object key;
		private final Transaction tx;
		private boolean ok = true;
		private Exception e;

		public SendRemove2Thread(final String schema, final String table, final int node, final int device, final Object key, final Transaction tx)
		{
			this.schema = schema;
			this.table = table;
			this.node = node;
			this.device = device;
			this.key = key;
			this.tx = tx;
		}

		public Exception getException()
		{
			return e;
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
				// new MetaData();
				final String host = MetaData.getHostNameForNode(node);
				// Socket sock = new Socket(host,
				// Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
				final Socket sock = new Socket();
				sock.setReceiveBufferSize(4194304);
				sock.setSendBufferSize(4194304);
				sock.connect(new InetSocketAddress(host, Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number"))));
				final OutputStream out = sock.getOutputStream();
				final byte[] outMsg = "REMOVE2         ".getBytes(StandardCharsets.UTF_8);
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
				out.write(intToBytes(device));
				final ObjectOutputStream objOut = new ObjectOutputStream(out);
				objOut.writeObject(key);
				objOut.flush();
				out.flush();
				getConfirmation(sock);
				objOut.close();
				sock.close();
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.debug("", e);
				ok = false;
				this.e = e;
			}
		}
	}

	private static class SendRemoveLDMDThread extends HRDBMSThread
	{
		private final Object o;
		private boolean sendOK;
		private final byte[] lenBytes;
		private final byte[] data;

		public SendRemoveLDMDThread(final Object o, final byte[] lenBytes, final byte[] data)
		{
			this.o = o;
			this.lenBytes = lenBytes;
			this.data = data;
		}

		@Override
		public void run()
		{
			if (o instanceof String)
			{
				Socket sock = null;
				try
				{
					// sock = new Socket((String)o,
					// Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
					sock = new Socket();
					sock.setReceiveBufferSize(4194304);
					sock.setSendBufferSize(4194304);
					sock.connect(new InetSocketAddress((String)o, Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number"))));
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
					out.write(lenBytes);
					out.write(data);
					final ObjectOutputStream objOut = new ObjectOutputStream(out);
					final List<Object> alo = new ArrayList<Object>(1);
					alo.add(o);
					objOut.writeObject(alo);
					objOut.flush();
					out.flush();
					getConfirmation(sock);
					objOut.close();
					sock.close();
				}
				catch (final Exception e)
				{
					sendOK = false;
					return;
				}
				sendOK = true;
			}
			else if (((List<Object>)o).size() > 0)
			{
				Socket sock = null;
				Object obj2 = ((List<Object>)o).get(0);
				while (obj2 instanceof ArrayList)
				{
					obj2 = ((List<Object>)obj2).get(0);
				}

				final String hostname = (String)obj2;
				try
				{
					// sock = new Socket(hostname,
					// Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
					sock = new Socket();
					sock.setReceiveBufferSize(4194304);
					sock.setSendBufferSize(4194304);
					sock.connect(new InetSocketAddress(hostname, Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number"))));
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
					out.write(lenBytes);
					out.write(data);
					final ObjectOutputStream objOut = new ObjectOutputStream(out);
					objOut.writeObject(o);
					objOut.flush();
					out.flush();
					getConfirmation(sock);
					objOut.close();
					sock.close();
				}
				catch (final Exception e)
				{
					sendOK = false;
					return;
				}

				sendOK = true;
			}
		}

		public boolean sendOK()
		{
			return sendOK;
		}
	}

	private static class SendReorgThread extends HRDBMSThread
	{
		private final Object o;
		private final Transaction tx;
		private boolean sendOK;
		private final byte[] schemaLenBytes;
		private final byte[] schemaBytes;
		private final byte[] tableLenBytes;
		private final byte[] tableBytes;
		private final List<Index> indexes;
		private final Map<String, String> cols2Types;
		private final Map<Integer, String> pos2Col;
		private final List<Boolean> uniques;

		public SendReorgThread(final Object o, final Transaction tx, final byte[] schemaLenBytes, final byte[] schemaBytes, final byte[] tableLenBytes, final byte[] tableBytes, final List<Index> indexes, final Map<String, String> cols2Types, final Map<Integer, String> pos2Col, final List<Boolean> uniques)
		{
			this.o = o;
			this.tx = tx;
			this.schemaLenBytes = schemaLenBytes;
			this.schemaBytes = schemaBytes;
			this.tableLenBytes = tableLenBytes;
			this.tableBytes = tableBytes;
			this.indexes = indexes;
			this.cols2Types = cols2Types;
			this.pos2Col = pos2Col;
			this.uniques = uniques;
		}

		@Override
		public void run()
		{
			if (o instanceof String)
			{
				Socket sock = null;
				try
				{
					// sock = new Socket((String)o,
					// Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
					sock = new Socket();
					sock.setReceiveBufferSize(4194304);
					sock.setSendBufferSize(4194304);
					sock.connect(new InetSocketAddress((String)o, Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number"))));
					final OutputStream out = sock.getOutputStream();
					final byte[] outMsg = "REORG           ".getBytes(StandardCharsets.UTF_8);
					outMsg[8] = 0;
					outMsg[9] = 0;
					outMsg[10] = 0;
					outMsg[11] = 0;
					outMsg[12] = 0;
					outMsg[13] = 0;
					outMsg[14] = 0;
					outMsg[15] = 0;
					out.write(outMsg);
					out.write(schemaLenBytes);
					out.write(schemaBytes);
					out.write(tableLenBytes);
					out.write(tableBytes);
					out.write(longToBytes(tx.number()));
					final ObjectOutputStream objOut = new ObjectOutputStream(out);
					final List<Object> alo = new ArrayList<Object>(1);
					alo.add(o);
					objOut.writeObject(alo);
					objOut.writeObject(indexes);
					objOut.writeObject(cols2Types);
					objOut.writeObject(pos2Col);
					objOut.writeObject(uniques);
					objOut.flush();
					out.flush();
					getConfirmation(sock);
					objOut.close();
					sock.close();
				}
				catch (final Exception e)
				{
					sendOK = false;
					return;
				}
				sendOK = true;
			}
			else if (((List<Object>)o).size() > 0)
			{
				Socket sock = null;
				Object obj2 = ((List<Object>)o).get(0);
				while (obj2 instanceof ArrayList)
				{
					obj2 = ((List<Object>)obj2).get(0);
				}

				final String hostname = (String)obj2;
				try
				{
					// sock = new Socket(hostname,
					// Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
					sock = new Socket();
					sock.setReceiveBufferSize(4194304);
					sock.setSendBufferSize(4194304);
					sock.connect(new InetSocketAddress(hostname, Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number"))));
					final OutputStream out = sock.getOutputStream();
					final byte[] outMsg = "REORG           ".getBytes(StandardCharsets.UTF_8);
					outMsg[8] = 0;
					outMsg[9] = 0;
					outMsg[10] = 0;
					outMsg[11] = 0;
					outMsg[12] = 0;
					outMsg[13] = 0;
					outMsg[14] = 0;
					outMsg[15] = 0;
					out.write(outMsg);
					out.write(schemaLenBytes);
					out.write(schemaBytes);
					out.write(tableLenBytes);
					out.write(tableBytes);
					out.write(longToBytes(tx.number()));
					final ObjectOutputStream objOut = new ObjectOutputStream(out);
					objOut.writeObject(o);
					objOut.writeObject(indexes);
					objOut.flush();
					out.flush();
					getConfirmation(sock);
					objOut.close();
					sock.close();
				}
				catch (final Exception e)
				{
					sendOK = false;
					return;
				}

				sendOK = true;
			}
		}

		public boolean sendOK()
		{
			return sendOK;
		}
	}

	private static class SendRollbackThread extends HRDBMSThread
	{
		private final Object o;
		private final Transaction tx;

		public SendRollbackThread(final Object o, final Transaction tx)
		{
			this.o = o;
			this.tx = tx;
		}

		@Override
		public void run()
		{
			if (o instanceof String)
			{
				Socket sock = null;
				try
				{
					// sock = new Socket((String)o,
					// Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
					sock = new Socket();
					sock.setReceiveBufferSize(4194304);
					sock.setSendBufferSize(4194304);
					sock.connect(new InetSocketAddress((String)o, Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number"))));
					final OutputStream out = sock.getOutputStream();
					final byte[] outMsg = "LROLLBCK        ".getBytes(StandardCharsets.UTF_8);
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
					final ObjectOutputStream objOut = new ObjectOutputStream(out);
					final List<Object> alo = new ArrayList<Object>(1);
					alo.add(o);
					objOut.writeObject(alo);
					objOut.flush();
					out.flush();
					getConfirmation(sock);
					objOut.close();
					sock.close();
				}
				catch (final Exception e)
				{
					// TODO blackListByHost((String)o);
					// TODO queueCommandByHost((String)o, "ROLLBACK", tx);
					HRDBMSWorker.logger.fatal("BLACKLIST", e);
					System.exit(1);
				}
			}
			else if (((List<Object>)o).size() > 0)
			{
				Socket sock = null;
				Object obj2 = ((List<Object>)o).get(0);
				while (obj2 instanceof ArrayList)
				{
					obj2 = ((List<Object>)obj2).get(0);
				}

				final String hostname = (String)obj2;
				try
				{
					// sock = new Socket(hostname,
					// Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
					sock = new Socket();
					sock.setReceiveBufferSize(4194304);
					sock.setSendBufferSize(4194304);
					sock.connect(new InetSocketAddress(hostname, Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number"))));
					final OutputStream out = sock.getOutputStream();
					final byte[] outMsg = "LROLLBCK        ".getBytes(StandardCharsets.UTF_8);
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
					final ObjectOutputStream objOut = new ObjectOutputStream(out);
					objOut.writeObject(o);
					objOut.flush();
					out.flush();
					getConfirmation(sock);
					objOut.close();
					sock.close();
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
					// TODO blackListByHost((String)obj2);
					// TODO queueCommandByHost((String)obj2, "ROLLBACK", tx);
					HRDBMSWorker.logger.fatal("BLACKLIST", e);
					System.exit(1);
					// retry others
					final boolean toDo = rebuildTree((List<Object>)o, (String)obj2);
					if (toDo)
					{
						sendRollback((List<Object>)o, tx);
					}
				}
			}
		}
	}
}