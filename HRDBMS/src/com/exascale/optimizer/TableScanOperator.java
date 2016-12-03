package com.exascale.optimizer;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import org.sosy_lab.common.ShutdownManager;
import org.sosy_lab.common.configuration.Configuration;
import org.sosy_lab.common.configuration.InvalidConfigurationException;
import org.sosy_lab.common.log.LogManager;
import org.sosy_lab.java_smt.SolverContextFactory;
import org.sosy_lab.java_smt.SolverContextFactory.Solvers;
import org.sosy_lab.java_smt.api.BooleanFormula;
import org.sosy_lab.java_smt.api.BooleanFormulaManager;
import org.sosy_lab.java_smt.api.FormulaManager;
import org.sosy_lab.java_smt.api.NumeralFormula.RationalFormula;
import org.sosy_lab.java_smt.api.ProverEnvironment;
import org.sosy_lab.java_smt.api.RationalFormulaManager;
import org.sosy_lab.java_smt.api.SolverContext;
import org.sosy_lab.java_smt.api.SolverException;
import com.exascale.filesystem.Block;
import com.exascale.filesystem.RID;
import com.exascale.managers.BufferManager;
import com.exascale.managers.BufferManager.RequestPagesThread;
import com.exascale.managers.FileManager;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.managers.LockManager;
import com.exascale.managers.ResourceManager;
import com.exascale.managers.SMTLogManager;
import com.exascale.misc.BufferedLinkedBlockingQueue;
import com.exascale.misc.CompressedBitSet;
import com.exascale.misc.DataEndMarker;
import com.exascale.misc.HParms;
import com.exascale.misc.MultiHashMap;
import com.exascale.misc.MyDate;
import com.exascale.optimizer.MetaData.PartitionMetaData;
import com.exascale.tables.DataType;
import com.exascale.tables.Plan;
import com.exascale.tables.Schema;
import com.exascale.tables.Schema.FieldValue;
import com.exascale.tables.Schema.Row;
import com.exascale.tables.Schema.RowIterator;
import com.exascale.tables.Transaction;
import com.exascale.threads.ConnectionWorker;
import com.exascale.threads.HRDBMSThread;
import com.exascale.threads.ThreadPoolThread;

public final class TableScanOperator implements Operator, Serializable
{
	private static int PREFETCH_REQUEST_SIZE_STATIC;
	private static int PAGES_IN_ADVANCE_STATIC;
	private static int MAX_PBPE_TIME;
	public static AtomicInteger tsoCount = new AtomicInteger(0);

	private static sun.misc.Unsafe unsafe;
	private static long offset;

	public static MultiHashMap<Block, HashSet<HashMap<Filter, Filter>>> noResults;
	public static ConcurrentHashMap<HashSet<HashMap<Filter, Filter>>, AtomicLong> noResultCounts;
	public static AtomicInteger skippedPages = new AtomicInteger(0);
	private static Object intraTxLock = new Object();
	private static HashMap<String, AtomicInteger> sharedDmlTxCounters = new HashMap<String, AtomicInteger>();
	private static Configuration config;
	private static LogManager logger;
	private static ShutdownManager shutdown;
	public static volatile ConcurrentHashMap<String, MultiHashMap<Integer, CNFEntry>> pbpeCache2;
	private static ConcurrentLinkedQueue contextQ;
	public static AtomicLong figureOutProblemsTime = new AtomicLong(0);
	public static AtomicLong SMTSolveTime = new AtomicLong(0);
	public static AtomicLong nonSMTSolveTime = new AtomicLong(0);
	public static AtomicLong pbpeMaintenanceTime = new AtomicLong(0);
	public static AtomicInteger SMTSolverCalls = new AtomicInteger(0);
	public static ConcurrentHashMap<String, HashMap<HashSet<HashSet<HashMap<Filter, Filter>>>, BitSet>> problemCache;
	public static LinkedBlockingQueue<String> prtq;

	static
	{
		try
		{
			final HParms hparms = HRDBMSWorker.getHParms();
			PREFETCH_REQUEST_SIZE_STATIC = Integer.parseInt(hparms.getProperty("prefetch_request_size")); // 80
			PAGES_IN_ADVANCE_STATIC = Integer.parseInt(hparms.getProperty("pages_in_advance")); // 40
			MAX_PBPE_TIME = Integer.parseInt(hparms.getProperty("max_pbpe_time"));
			final Field f = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
			f.setAccessible(true);
			unsafe = (sun.misc.Unsafe)f.get(null);
			final Field fieldToUpdate = ArrayList.class.getDeclaredField("elementData");
			// get unsafe offset to this field
			offset = unsafe.objectFieldOffset(fieldToUpdate);

			final int pbpeVer = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("pbpe_version"));
			final boolean isV5OrHigher = (pbpeVer >= 5);
			final boolean isV9 = (pbpeVer == 9);

			final File file = new File("pbpe.dat");
			if (file.exists())
			{
				try
				{
					final ObjectInputStream in = new ObjectInputStream(new FileInputStream("pbpe.dat"));

					if (isV5OrHigher)
					{
						pbpeCache2 = (ConcurrentHashMap<String, MultiHashMap<Integer, CNFEntry>>)in.readObject();
						noResults = new MultiHashMap<Block, HashSet<HashMap<Filter, Filter>>>();
					}
					else
					{
						noResults = (MultiHashMap<Block, HashSet<HashMap<Filter, Filter>>>)in.readObject();
					}
					in.close();
				}
				catch (final Exception e)
				{
					noResults = new MultiHashMap<Block, HashSet<HashMap<Filter, Filter>>>();
					pbpeCache2 = new ConcurrentHashMap<String, MultiHashMap<Integer, CNFEntry>>();
				}
			}
			else
			{
				noResults = new MultiHashMap<Block, HashSet<HashMap<Filter, Filter>>>();
				pbpeCache2 = new ConcurrentHashMap<String, MultiHashMap<Integer, CNFEntry>>();
			}
			
			final File file2 = new File("pbpe.stats");
			if (file2.exists())
			{
				try
				{
					ObjectInputStream in2 = new ObjectInputStream(new FileInputStream("pbpe.stats"));
					noResultCounts = (ConcurrentHashMap<HashSet<HashMap<Filter, Filter>>, AtomicLong>)in2.readObject();
				}
				catch(Exception e)
				{
					noResultCounts = new ConcurrentHashMap<HashSet<HashMap<Filter, Filter>>, AtomicLong>();
				}
			}
			else
			{
				noResultCounts = new ConcurrentHashMap<HashSet<HashMap<Filter, Filter>>, AtomicLong>();
			}

			if (isV9)
			{
				problemCache = new ConcurrentHashMap<String, HashMap<HashSet<HashSet<HashMap<Filter, Filter>>>, BitSet>>();
				for (final Map.Entry entry : pbpeCache2.entrySet())
				{
					final HashSet<CNFEntry> entries = new HashSet<CNFEntry>();
					final MultiHashMap<Integer, CNFEntry> mhm = (MultiHashMap<Integer, CNFEntry>)entry.getValue();
					final Set<Integer> hashCodes = mhm.getKeySet();
					for (final int hash : hashCodes)
					{
						entries.addAll(mhm.get(hash));
					}

					int length = 1;
					for (final CNFEntry entry2 : entries)
					{
						final BitSet bs = entry2.getBitSet();
						synchronized (bs)
						{
							final int temp = bs.length();
							if (temp > length)
							{
								length = temp;
							}
						}
					}

					final String fn = (String)entry.getKey();
					final HashMap<HashSet<HashSet<HashMap<Filter, Filter>>>, BitSet> problems = buildProblems(entries, 1, length);
					problemCache.put(fn, problems);
				}

				prtq = new LinkedBlockingQueue<String>();
				new ProblemRebuildThread(prtq).start();
			}

			new PBPEThread().start();

			config = Configuration.defaultConfiguration();
			logger = SMTLogManager.create(config);
			shutdown = ShutdownManager.create();

			contextQ = new ConcurrentLinkedQueue();
			if (isV5OrHigher)
			{
				int i = 0;
				while (i < MetaData.getNumDevices())
				{
					final SolverContext context = SolverContextFactory.createSolverContext(config, logger, shutdown.getNotifier(), Solvers.SMTINTERPOL);
					contextQ.add(context);
					i++;
				}
			}
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.debug("", e);
		}
	}

	private int PREFETCH_REQUEST_SIZE;

	private int PAGES_IN_ADVANCE;

	private HashMap<String, String> cols2Types = new HashMap<String, String>();
	private HashMap<String, Integer> cols2Pos = new HashMap<String, Integer>();
	private TreeMap<Integer, String> pos2Col = new TreeMap<Integer, String>();
	private String name;
	private String schema;
	private transient ArrayList<String> ins;
	private ArrayList<Operator> parents = new ArrayList<Operator>();
	public transient volatile BufferedLinkedBlockingQueue readBuffer;
	private transient volatile HashMap<Operator, BufferedLinkedBlockingQueue> readBuffers;
	private boolean startDone = false;
	private transient boolean optimize;
	private transient HashMap<Operator, HashSet<HashMap<Filter, Filter>>> filters = new HashMap<Operator, HashSet<HashMap<Filter, Filter>>>();
	protected HashMap<Operator, CNFFilter> orderedFilters = new HashMap<Operator, CNFFilter>();
	private MetaData meta;
	private transient HashMap<Operator, Operator> opParents = new HashMap<Operator, Operator>();
	private ArrayList<Integer> neededPos;
	private ArrayList<Integer> fetchPos;
	private String[] midPos2Col;
	private HashMap<String, String> midCols2Types;
	private boolean set = false;
	private transient PartitionMetaData partMeta; // OK now that clone won't
	// happen at runtime?
	private transient HashMap<Operator, ArrayList<Integer>> activeDevices = new HashMap<Operator, ArrayList<Integer>>();
	private transient HashMap<Operator, ArrayList<Integer>> activeNodes = new HashMap<Operator, ArrayList<Integer>>();
	public ArrayList<Integer> devices = new ArrayList<Integer>();
	private int node;
	private boolean phase2Done = false;
	public HashMap<Integer, Operator> device2Child = new HashMap<Integer, Operator>();
	private ArrayList<Operator> children = new ArrayList<Operator>();
	private transient ArrayList<String> randomIns;
	private transient HashMap<String, Integer> ins2Device;
	private boolean indexOnly = false;
	private transient volatile boolean forceDone;
	public Transaction tx;
	private String alias = "";
	public boolean getRID = false;
	private HashMap<String, String> tableCols2Types;
	private TreeMap<Integer, String> tablePos2Col;
	private HashMap<String, Integer> tableCols2Pos;
	private boolean releaseLocks = false;
	private boolean sample = false;
	private long sPer;
	private Index scanIndex = null;
	private transient AtomicLong received;
	private transient volatile boolean demReceived;
	private int tType = 0;
	private volatile transient HashSet<Integer> referencesHash = null;

	public TableScanOperator(final String schema, final String name, final MetaData meta, final HashMap<String, Integer> cols2Pos, final TreeMap<Integer, String> pos2Col, final HashMap<String, String> cols2Types, final TreeMap<Integer, String> tablePos2Col, final HashMap<String, String> tableCols2Types, final HashMap<String, Integer> tableCols2Pos) throws Exception
	{
		this.meta = meta;
		this.name = name;
		this.schema = schema;
		this.cols2Types = (HashMap<String, String>)cols2Types.clone();
		this.cols2Pos = (HashMap<String, Integer>)cols2Pos.clone();
		this.pos2Col = (TreeMap<Integer, String>)pos2Col.clone();
		this.tableCols2Types = tableCols2Types;
		this.tablePos2Col = tablePos2Col;
		this.tableCols2Pos = tableCols2Pos;
		received = new AtomicLong(0);
	}

	public TableScanOperator(final String schema, final String name, final MetaData meta, final Transaction tx) throws Exception
	{
		this.meta = meta;
		this.name = name;
		this.schema = schema;
		cols2Types = MetaData.getCols2TypesForTable(schema, name, tx);
		cols2Pos = MetaData.getCols2PosForTable(schema, name, tx);
		pos2Col = MetaData.cols2PosFlip(cols2Pos);
		tableCols2Types = (HashMap<String, String>)cols2Types.clone();
		tablePos2Col = (TreeMap<Integer, String>)pos2Col.clone();
		tableCols2Pos = (HashMap<String, Integer>)cols2Pos.clone();
		received = new AtomicLong(0);
	}

	// private static long MAX_PAGES;

	// static
	// {
	// MAX_PAGES =
	// (Long.parseLong(HRDBMSWorker.getHParms().getProperty("bp_pages")) /
	// MetaData.getNumDevices()) / 15;
	// }

	public TableScanOperator(final String schema, final String name, final MetaData meta, final Transaction tx, final boolean releaseLocks) throws Exception
	{
		this.meta = meta;
		this.name = name;
		this.schema = schema;
		cols2Types = MetaData.getCols2TypesForTable(schema, name, tx);
		cols2Pos = MetaData.getCols2PosForTable(schema, name, tx);
		pos2Col = MetaData.cols2PosFlip(cols2Pos);
		tableCols2Types = (HashMap<String, String>)cols2Types.clone();
		tablePos2Col = (TreeMap<Integer, String>)pos2Col.clone();
		tableCols2Pos = (HashMap<String, Integer>)cols2Pos.clone();
		this.releaseLocks = releaseLocks;
		received = new AtomicLong(0);
	}

	public TableScanOperator(final String schema, final String name, final MetaData meta, final Transaction tx, final boolean releaseLocks, final HashMap<String, Integer> cols2Pos, final TreeMap<Integer, String> pos2Col) throws Exception
	{
		this.meta = meta;
		this.name = name;
		this.schema = schema;
		cols2Types = MetaData.getCols2TypesForTable(schema, name, tx);
		this.cols2Pos = cols2Pos;
		this.pos2Col = pos2Col;
		tableCols2Types = (HashMap<String, String>)cols2Types.clone();
		tablePos2Col = (TreeMap<Integer, String>)pos2Col.clone();
		tableCols2Pos = (HashMap<String, Integer>)cols2Pos.clone();
		this.releaseLocks = releaseLocks;
		received = new AtomicLong(0);
	}

	public static TableScanOperator deserialize(final InputStream in, final HashMap<Long, Object> prev) throws Exception
	{
		final TableScanOperator value = (TableScanOperator)unsafe.allocateInstance(TableScanOperator.class);
		prev.put(OperatorUtils.readLong(in), value);
		value.cols2Types = OperatorUtils.deserializeStringHM(in, prev);
		value.cols2Pos = OperatorUtils.deserializeStringIntHM(in, prev);
		value.pos2Col = OperatorUtils.deserializeTM(in, prev);
		value.name = OperatorUtils.readString(in, prev);
		value.schema = OperatorUtils.readString(in, prev);
		value.parents = OperatorUtils.deserializeALOp(in, prev);
		value.startDone = OperatorUtils.readBool(in);
		value.orderedFilters = OperatorUtils.deserializeHMOpCNF(in, prev);
		value.meta = new MetaData();
		value.neededPos = OperatorUtils.deserializeALI(in, prev);
		value.fetchPos = OperatorUtils.deserializeALI(in, prev);
		value.midPos2Col = OperatorUtils.deserializeStringArray(in, prev);
		value.midCols2Types = OperatorUtils.deserializeStringHM(in, prev);
		value.set = OperatorUtils.readBool(in);
		value.devices = OperatorUtils.deserializeALI(in, prev);
		value.node = OperatorUtils.readInt(in);
		value.phase2Done = OperatorUtils.readBool(in);
		value.device2Child = OperatorUtils.deserializeHMIntOp(in, prev);
		value.children = OperatorUtils.deserializeALOp(in, prev);
		value.indexOnly = OperatorUtils.readBool(in);
		value.tx = new Transaction(OperatorUtils.readLong(in));
		value.alias = OperatorUtils.readString(in, prev);
		value.getRID = OperatorUtils.readBool(in);
		value.tableCols2Types = OperatorUtils.deserializeStringHM(in, prev);
		value.tablePos2Col = OperatorUtils.deserializeTM(in, prev);
		value.tableCols2Pos = OperatorUtils.deserializeStringIntHM(in, prev);
		value.releaseLocks = OperatorUtils.readBool(in);
		value.sample = OperatorUtils.readBool(in);
		value.sPer = OperatorUtils.readLong(in);
		value.scanIndex = OperatorUtils.deserializeIndex(in, prev);
		value.received = new AtomicLong(0);
		value.demReceived = false;
		value.tType = OperatorUtils.readInt(in);
		return value;
	}

	private static HashMap<HashSet<HashSet<HashMap<Filter, Filter>>>, BitSet> buildProblems(final HashSet<CNFEntry> entries, final int stride, final int length)
	{
		final int pbpeVer = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("pbpe_version"));
		final boolean isV6OrHigher = (pbpeVer >= 6);
		final HashMap<HashSet<HashSet<HashMap<Filter, Filter>>>, BitSet> retval = new HashMap<HashSet<HashSet<HashMap<Filter, Filter>>>, BitSet>();

		final HashMap<HashSet<Integer>, HashSet<HashSet<HashMap<Filter, Filter>>>> tempMap = new HashMap<HashSet<Integer>, HashSet<HashSet<HashMap<Filter, Filter>>>>();
		int pos = 1;
		while (pos < length)
		{
			final HashSet<Integer> key = new HashSet<Integer>();
			final HashSet<HashSet<HashMap<Filter, Filter>>> tempHSHM = new HashSet<HashSet<HashMap<Filter, Filter>>>();
			int i = 0;
			for (final CNFEntry entry : entries)
			{
				final BitSet bs = entry.getBitSet();
				synchronized (bs)
				{
					if (bs.get(pos))
					{
						key.add(i);
						tempHSHM.add(entry.getCNF());
					}
				}

				i++;
			}

			HashSet<HashSet<HashMap<Filter, Filter>>> hshm = tempMap.get(key);
			if (hshm == null)
			{
				hshm = tempHSHM;
				tempMap.put(key, hshm);
			}

			BitSet bs = retval.get(hshm);
			if (bs == null)
			{
				if (isV6OrHigher)
				{
					bs = new CompressedBitSet();
				}
				else
				{
					bs = new BitSet();
				}

				retval.put(hshm, bs);
			}

			bs.set(pos);
			pos += stride;
		}

		return retval;
	}

	private static boolean canAnythingInRangeSatisfyFilters(final ArrayList<Filter> filters, Object lowLE, Object highLE) throws Exception
	{
		if (lowLE == null)
		{
			if (highLE instanceof Double)
			{
				lowLE = Double.MIN_VALUE;
			}
			else if (highLE instanceof Long)
			{
				lowLE = Long.MIN_VALUE;
			}
			else if (highLE instanceof Integer)
			{
				lowLE = Integer.MIN_VALUE;
			}
			else if (highLE instanceof MyDate)
			{
				lowLE = new MyDate(Integer.MIN_VALUE);
			}
			else if (highLE instanceof String)
			{
				lowLE = "";
			}
		}

		if (highLE == null)
		{
			if (lowLE instanceof Double)
			{
				highLE = Double.MAX_VALUE;
			}
			else if (lowLE instanceof Long)
			{
				highLE = Long.MAX_VALUE;
			}
			else if (lowLE instanceof Integer)
			{
				highLE = Integer.MAX_VALUE;
			}
			else if (lowLE instanceof MyDate)
			{
				highLE = new MyDate(Integer.MAX_VALUE);
			}
			else if (lowLE instanceof String)
			{
				highLE = "\uFFFF";
			}
		}

		for (final Filter filter : filters)
		{
			if (filter.op().equals("E"))
			{
				Object literal = null;
				if (filter.leftIsColumn())
				{
					literal = filter.rightLiteral();
				}
				else
				{
					literal = filter.leftLiteral();
				}

				if (((Comparable)lowLE).compareTo(literal) < 1 && ((Comparable)highLE).compareTo(literal) > -1)
				{
					continue;
				}
				else
				{
					return false;
				}
			}

			final HashMap<String, Integer> cols2Pos = new HashMap<String, Integer>();
			String col = null;
			if (filter.leftIsColumn())
			{
				col = filter.leftColumn();
			}
			else
			{
				col = filter.rightColumn();
			}
			cols2Pos.put(col, 0);
			final ArrayList<Object> row1 = new ArrayList<Object>(1);
			row1.add(lowLE);
			final ArrayList<Object> row2 = new ArrayList<Object>(1);
			row2.add(highLE);

			try
			{
				if (filter.passes(row1, cols2Pos) || filter.passes(row2, cols2Pos))
				{
					continue;
				}
				else
				{
					return false;
				}
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				throw e;
			}
		}

		return true;
	}

	@Override
	public void add(final Operator op) throws Exception
	{
		children.add(op);
		op.registerParent(this);
	}

	public void addActiveDeviceForParent(final int i, final Operator op)
	{
		ArrayList<Integer> list = activeDevices.get(op);

		if (list == null)
		{
			list = new ArrayList<Integer>(1);
			list.add(i);
			activeDevices.put(op, list);
			return;
		}

		list.add(i);
	}

	public void addActiveDevices(final ArrayList<Integer> devs)
	{
		devices.addAll(devs);
	}

	public void addActiveDevicesForParent(final ArrayList<Integer> is, final Operator op)
	{
		final ArrayList<Integer> list = activeDevices.get(op);

		if (list == null)
		{
			activeDevices.put(op, is);
			return;
		}

		list.addAll(is);
	}

	public void addActiveNodeForParent(final int i, final Operator op)
	{
		ArrayList<Integer> list = activeNodes.get(op);

		if (list == null)
		{
			list = new ArrayList<Integer>();
			list.add(i);
			activeNodes.put(op, list);
			return;
		}

		list.add(i);
	}

	public void addFilter(final ArrayList<Filter> filters, final Operator op, final Operator opParent, final Transaction tx) throws Exception
	{
		opParents.put(opParent, op);

		HashSet<HashMap<Filter, Filter>> f = this.filters.get(op);
		if (f == null)
		{
			final Operator op2 = opParents.get(op);
			if (op2 != null)
			{
				// parent has changed because of previous addFilter()
				opParents.remove(op);
				f = this.filters.get(op2);
				this.filters.remove(op2);
			}
			else
			{
				f = new HashSet<HashMap<Filter, Filter>>();
				final HashMap<Filter, Filter> map = new HashMap<Filter, Filter>();
				for (final Filter filter : filters)
				{
					map.put(filter, filter);
				}

				f.add(map);
				this.filters.put(op, f);
				// Transaction t = new Transaction(Transaction.ISOLATION_RR);
				orderedFilters.put(op, new CNFFilter(f, meta, cols2Pos, this));
				// t.commit();
				return;
			}
		}

		final HashMap<Filter, Filter> map = new HashMap<Filter, Filter>();
		for (final Filter filter : filters)
		{
			map.put(filter, filter);
		}

		f.add(map);
		this.filters.put(op, f);
		orderedFilters.put(op, new CNFFilter(f, meta, cols2Pos, this));
	}

	public boolean allDevices()
	{
		return partMeta.allDevices();
	}

	public boolean allNodes()
	{
		return partMeta.allNodes();
	}

	public boolean anyNode()
	{
		return partMeta.anyNode();
	}

	@Override
	public ArrayList<Operator> children()
	{
		return children;
	}

	public void cleanupOrderedFilters()
	{
		if (orderedFilters.size() <= 1)
		{
			return;
		}
		final CNFFilter theOne = orderedFilters.get(parents.get(0));
		orderedFilters.clear();
		if (theOne != null)
		{
			orderedFilters.put(parents.get(0), theOne);
		}
	}

	public void clearOpParents()
	{
		if (opParents.size() > 0)
		{
			opParents.clear();
		}
	}

	@Override
	public TableScanOperator clone()
	{
		TableScanOperator retval = null;
		try
		{
			retval = new TableScanOperator(schema, name, meta, cols2Pos, pos2Col, cols2Types, tablePos2Col, tableCols2Types, tableCols2Pos);
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.error("", e);
			return null;
		}
		if (neededPos != null)
		{
			retval.neededPos = (ArrayList<Integer>)neededPos.clone();
		}
		if (fetchPos != null)
		{
			retval.fetchPos = (ArrayList<Integer>)fetchPos.clone();
		}
		if (midPos2Col != null)
		{
			retval.midPos2Col = midPos2Col.clone();
		}
		if (midCols2Types != null)
		{
			retval.midCols2Types = (HashMap<String, String>)midCols2Types.clone();
		}
		retval.cols2Pos = (HashMap<String, Integer>)cols2Pos.clone();
		retval.pos2Col = (TreeMap<Integer, String>)pos2Col.clone();
		retval.cols2Types = (HashMap<String, String>)cols2Types.clone();
		retval.set = set;
		retval.partMeta = partMeta;
		retval.phase2Done = phase2Done;
		retval.node = node;
		retval.sample = sample;
		retval.sPer = sPer;
		retval.scanIndex = scanIndex;
		retval.tType = tType;
		if (devices != null)
		{
			retval.devices = (ArrayList<Integer>)devices.clone();
		}
		retval.indexOnly = indexOnly;
		if (alias != null && !alias.equals(""))
		{
			retval.setAlias(alias);
		}
		retval.getRID = getRID;
		retval.tx = tx;
		return retval;
	}

	@Override
	public void close() throws Exception
	{
		for (final Operator o : children)
		{
			try
			{
				o.close();
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				throw e;
			}
		}

		if (readBuffer != null)
		{
			readBuffer.close();
		}

		if (readBuffers != null)
		{
			for (final BufferedLinkedBlockingQueue readBuffer : readBuffers.values())
			{
				readBuffer.close();
			}
		}

		if (releaseLocks)
		{
			tx.releaseLocksAndPins();
		}

		ins = null;
		neededPos = null;
		activeDevices = null;
		randomIns = null;
		tablePos2Col = null;
		readBuffers = null;
		fetchPos = null;
		activeNodes = null;
		ins2Device = null;
		tableCols2Pos = null;
		filters = null;
		midPos2Col = null;
		devices = null;
		tableCols2Types = null;
		orderedFilters = null;
		midCols2Types = null;
		device2Child = null;
		cols2Pos = null;
		cols2Types = null;
		pos2Col = null;
	}

	public boolean deviceIsHash()
	{
		return partMeta.deviceIsHash();
	}

	public ArrayList<Integer> deviceSet()
	{
		return partMeta.deviceSet();
	}

	@Override
	public boolean equals(final Object rhs)
	{
		if (rhs == null || (!(rhs instanceof TableScanOperator)))
		{
			return false;
		}
		else
		{
			if (alias != null && !alias.equals(""))
			{
				if (!alias.equals(((TableScanOperator)rhs).alias))
				{
					return false;
				}
			}
			else
			{
				if (((TableScanOperator)rhs).alias != null && !((TableScanOperator)rhs).alias.equals(""))
				{
					return false;
				}
			}
			return (schema.equals(((TableScanOperator)rhs).schema) && name.equals(((TableScanOperator)rhs).name) && node == ((TableScanOperator)rhs).node);
		}
	}

	public Operator firstParent()
	{
		if (parents.size() > 0)
		{
			return parents.get(0);
		}
		else
		{
			return null;
		}
	}

	public String getAlias()
	{
		return alias;
	}

	@Override
	public int getChildPos()
	{
		return 0;
	}

	public CNFFilter getCNFForParent(final Operator op)
	{
		return orderedFilters.get(op);
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

	public ArrayList<String> getDeviceHash()
	{
		return partMeta.getDeviceHash();
	}

	public ArrayList<Integer> getDeviceList()
	{
		return devices;
	}

	public ArrayList<Integer> getDeviceList(final Operator op)
	{
		return activeDevices.get(op);
	}

	public String getDeviceRangeCol()
	{
		return partMeta.getDeviceRangeCol();
	}

	public ArrayList<Object> getDeviceRanges()
	{
		return partMeta.getDeviceRanges();
	}

	public ArrayList<Integer> getDevicesMatchingRangeFilters(final ArrayList<Filter> rangeFilters) throws Exception
	{
		final ArrayList<Integer> retval = new ArrayList<Integer>();
		ArrayList<Integer> deviceList = null;
		if (partMeta.allDevices())
		{
			final int num = partMeta.getNumDevices();
			deviceList = new ArrayList<Integer>(num);
			int i = 0;
			while (i < num)
			{
				deviceList.add(i);
				i++;
			}
		}
		else
		{
			deviceList = partMeta.deviceSet();
		}

		Object oldLE = null;
		int i = 0;
		for (final Object leVal : partMeta.getDeviceRanges())
		{
			if (canAnythingInRangeSatisfyFilters(rangeFilters, oldLE, leVal))
			{
				retval.add(deviceList.get(i));
			}

			i++;
			oldLE = leVal;
		}

		if (canAnythingInRangeSatisfyFilters(rangeFilters, oldLE, null))
		{
			retval.add(deviceList.get(i));
		}

		return retval;
	}

	public CNFFilter getFirstCNF()
	{
		for (final CNFFilter cnf : orderedFilters.values())
		{
			return cnf;
		}

		return null;
	}

	public HashSet<HashMap<Filter, Filter>> getHSHM()
	{
		final CNFFilter retval = orderedFilters.get(parents.get(0));
		if (retval == null)
		{
			return null;
		}

		return retval.getHSHM();
	}

	@Override
	public MetaData getMeta()
	{
		return meta;
	}

	public String[] getMidPos2Col()
	{
		return midPos2Col;
	}

	@Override
	public int getNode()
	{
		return node;
	}

	public ArrayList<String> getNodeGroupHash()
	{
		return partMeta.getNodeGroupHash();
	}

	public HashMap<Integer, ArrayList<Integer>> getNodeGroupHashMap()
	{
		return partMeta.getNodeGroupHashMap();
	}

	public String getNodeGroupRangeCol()
	{
		return partMeta.getNodeGroupRangeCol();
	}

	public ArrayList<Object> getNodeGroupRanges()
	{
		return partMeta.getNodeGroupRanges();
	}

	public ArrayList<Integer> getNodeGroupsMatchingRangeFilters(final ArrayList<Filter> rangeFilters) throws Exception
	{
		final ArrayList<Integer> retval = new ArrayList<Integer>();
		final ArrayList<Integer> nodeGroupList = partMeta.nodeGroupSet();

		Object oldLE = null;
		int i = 0;
		for (final Object leVal : partMeta.getNodeGroupRanges())
		{
			if (canAnythingInRangeSatisfyFilters(rangeFilters, oldLE, leVal))
			{
				retval.add(nodeGroupList.get(i));
			}

			i++;
			oldLE = leVal;
		}

		if (canAnythingInRangeSatisfyFilters(rangeFilters, oldLE, null))
		{
			retval.add(nodeGroupList.get(i));
		}

		return retval;
	}

	public ArrayList<String> getNodeHash()
	{
		return partMeta.getNodeHash();
	}

	public ArrayList<Integer> getNodeList()
	{
		return partMeta.nodeSet();
	}

	public ArrayList<Integer> getNodeList(final Operator op)
	{
		return activeNodes.get(op);
	}

	public String getNodeRangeCol()
	{
		return partMeta.getNodeRangeCol();
	}

	public ArrayList<Object> getNodeRanges()
	{
		return partMeta.getNodeRanges();
	}

	public ArrayList<Integer> getNodesMatchingRangeFilters(final ArrayList<Filter> rangeFilters) throws Exception
	{
		final ArrayList<Integer> retval = new ArrayList<Integer>();
		ArrayList<Integer> nodeList = null;
		if (partMeta.allNodes())
		{
			final int num = partMeta.getNumNodes();
			nodeList = new ArrayList<Integer>(num);
			int i = 0;

			if (partMeta.noNodeGroupSet())
			{
				while (i < num)
				{
					nodeList.add(i);
					i++;
				}
			}
			else
			{
				final Iterator iter = partMeta.getNodeGroupHashMap().values().iterator();
				iter.hasNext();
				final int size = ((ArrayList<Integer>)(iter.next())).size();
				while (i < size)
				{
					nodeList.add(i);
					i++;
				}
			}
		}
		else
		{
			nodeList = partMeta.nodeSet();
		}

		Object oldLE = null;
		int i = 0;
		for (final Object leVal : partMeta.getNodeRanges())
		{
			if (canAnythingInRangeSatisfyFilters(rangeFilters, oldLE, leVal))
			{
				retval.add(nodeList.get(i));
			}

			i++;
			oldLE = leVal;
		}

		if (canAnythingInRangeSatisfyFilters(rangeFilters, oldLE, null))
		{
			retval.add(nodeList.get(i));
		}

		return retval;
	}

	public int getNumDevices()
	{
		return partMeta.getNumDevices();
	}

	public int getNumNodeGroups()
	{
		return partMeta.getNumNodeGroups();
	}

	public int getNumNodes()
	{
		return partMeta.getNumNodes();
	}

	@Override
	public TreeMap<Integer, String> getPos2Col()
	{
		return pos2Col;
	}

	@Override
	public ArrayList<String> getReferences()
	{
		final ArrayList<String> retval = new ArrayList<String>(0);
		return retval;
	}

	public void getRID()
	{
		getRID = true;
		cols2Types.put("_RID1", "INT");
		cols2Types.put("_RID2", "INT");
		cols2Types.put("_RID3", "INT");
		cols2Types.put("_RID4", "INT");

		final HashMap<String, Integer> newCols2Pos = new HashMap<String, Integer>();
		final TreeMap<Integer, String> newPos2Col = new TreeMap<Integer, String>();
		newCols2Pos.put("_RID1", 0);
		newCols2Pos.put("_RID2", 1);
		newCols2Pos.put("_RID3", 2);
		newCols2Pos.put("_RID4", 3);
		newPos2Col.put(0, "_RID1");
		newPos2Col.put(1, "_RID2");
		newPos2Col.put(2, "_RID3");
		newPos2Col.put(3, "_RID4");
		for (final Map.Entry entry : cols2Pos.entrySet())
		{
			newCols2Pos.put((String)entry.getKey(), (Integer)entry.getValue() + 4);
			newPos2Col.put((Integer)entry.getValue() + 4, (String)entry.getKey());
		}
		cols2Pos = newCols2Pos;
		pos2Col = newPos2Col;
	}

	public String getSchema()
	{
		return schema;
	}

	public int getSingleDevice()
	{
		return partMeta.getSingleDevice();
	}

	public int getSingleNode()
	{
		return partMeta.getSingleNode();
	}

	public int getSingleNodeGroup()
	{
		return partMeta.getSingleNodeGroup();
	}

	public String getTable()
	{
		return name;
	}

	public HashMap<String, Integer> getTableCols2Pos()
	{
		return tableCols2Pos;
	}

	public int getType()
	{
		return tType;
	}

	@Override
	public int hashCode()
	{
		int hash = 23;
		hash = hash * 31 + schema.hashCode();
		hash = hash * 31 + name.hashCode();
		hash = hash * 31 + node;
		return hash;
		// return schema.hashCode() + name.hashCode();
	}

	public boolean isGetRID()
	{
		return getRID;
	}

	public boolean isSingleDeviceSet()
	{
		return partMeta.isSingleDeviceSet();
	}

	public boolean isSingleNodeGroupSet()
	{
		return partMeta.isSingleNodeGroupSet();
	}

	public boolean isSingleNodeSet()
	{
		return partMeta.isSingleNodeSet();
	}

	public boolean metaDataSet()
	{
		return set;
	}

	@Override
	public Object next(final Operator op) throws Exception
	{
		if (!optimize)
		{
			Object o;
			final BufferedLinkedBlockingQueue buffer = readBuffers.get(op);

			o = buffer.take();

			if (o instanceof DataEndMarker)
			{
				o = buffer.peek();
				if (o == null)
				{
					buffer.put(new DataEndMarker());
					return new DataEndMarker();
				}
				else
				{
					buffer.put(new DataEndMarker());
					return o;
				}
			}

			if (o instanceof Exception)
			{
				throw (Exception)o;
			}

			return o;
		}
		else
		{
			Object o = readBuffer.take();

			if (o instanceof DataEndMarker)
			{
				demReceived = true;
				o = readBuffer.peek();
				if (o == null)
				{
					readBuffer.put(new DataEndMarker());
					return new DataEndMarker();
				}
				else
				{
					readBuffer.put(new DataEndMarker());
					return o;
				}
			}
			else
			{
				received.getAndIncrement();
			}

			if (o instanceof Exception)
			{
				throw (Exception)o;
			}

			return o;
		}
	}

	@Override
	public void nextAll(final Operator op)
	{
		forceDone = true;
	}

	public boolean nodeGroupIsHash()
	{
		return partMeta.nodeGroupIsHash();
	}

	public ArrayList<Integer> nodeGroupSet()
	{
		return partMeta.nodeGroupSet();
	}

	public boolean nodeIsHash()
	{
		return partMeta.nodeIsHash();
	}

	public ArrayList<Integer> nodeSet()
	{
		return partMeta.nodeSet();
	}

	public boolean noNodeGroupSet()
	{
		return partMeta.noNodeGroupSet();
	}

	@Override
	public long numRecsReceived()
	{
		return received.get();
	}

	@Override
	public Operator parent() throws UnsupportedOperationException
	{
		throw new UnsupportedOperationException("TableScanOperator does not support parent()");
	}

	public ArrayList<Operator> parents()
	{
		if (parents.size() == 0)
		{
			final ArrayList<Operator> retval = new ArrayList<Operator>();
			{
				retval.add(null);
			}

			return retval;
		}

		return parents;
	}

	public boolean phase2Done()
	{
		return phase2Done;
	}

	public void rebuild()
	{
		opParents = new HashMap<Operator, Operator>();
	}

	@Override
	public boolean receivedDEM()
	{
		return demReceived;
	}

	@Override
	public void registerParent(final Operator op)
	{
		parents.add(op);
		if (opParents.containsKey(op))
		{
			orderedFilters.put(op, orderedFilters.get(opParents.get(op)));
			opParents.put(op.parent(), op);
		}
	}

	@Override
	public void removeChild(final Operator op)
	{
		children.remove(op);
		op.removeParent(this);
	}

	@Override
	public void removeParent(final Operator op)
	{
		parents.remove(op);
	}

	@Override
	public void reset() throws Exception
	{
		if (!startDone)
		{
			try
			{
				start();
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				throw e;
			}
		}
		else
		{
			for (final Operator o : children)
			{
				try
				{
					o.reset();
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.error("", e);
					throw e;
				}
			}

			readBuffer.clear();
			forceDone = false;
			init();
		}
	}

	@Override
	public void serialize(final OutputStream out, final IdentityHashMap<Object, Long> prev) throws Exception
	{
		final Long id = prev.get(this);
		if (id != null)
		{
			OperatorUtils.serializeReference(id, out);
			return;
		}

		OperatorUtils.writeType(78, out);
		prev.put(this, OperatorUtils.writeID(out));
		OperatorUtils.serializeStringHM(cols2Types, out, prev);
		OperatorUtils.serializeStringIntHM(cols2Pos, out, prev);
		OperatorUtils.serializeTM(pos2Col, out, prev);
		OperatorUtils.writeString(name, out, prev);
		OperatorUtils.writeString(schema, out, prev);
		OperatorUtils.serializeALOp(parents, out, prev);
		OperatorUtils.writeBool(startDone, out);
		OperatorUtils.serializeHMOpCNF(orderedFilters, out, prev);
		// recreate meta
		OperatorUtils.serializeALI(neededPos, out, prev);
		OperatorUtils.serializeALI(fetchPos, out, prev);
		OperatorUtils.serializeStringArray(midPos2Col, out, prev);
		OperatorUtils.serializeStringHM(midCols2Types, out, prev);
		OperatorUtils.writeBool(set, out);
		OperatorUtils.serializeALI(devices, out, prev);
		OperatorUtils.writeInt(node, out);
		OperatorUtils.writeBool(phase2Done, out);
		OperatorUtils.serializeHMIntOp(device2Child, out, prev);
		OperatorUtils.serializeALOp(children, out, prev);
		OperatorUtils.writeBool(indexOnly, out);
		OperatorUtils.writeLong(tx.number(), out); // notice type
		OperatorUtils.writeString(alias, out, prev);
		OperatorUtils.writeBool(getRID, out);
		OperatorUtils.serializeStringHM(tableCols2Types, out, prev);
		OperatorUtils.serializeTM(tablePos2Col, out, prev);
		OperatorUtils.serializeStringIntHM(tableCols2Pos, out, prev);
		OperatorUtils.writeBool(releaseLocks, out);
		OperatorUtils.writeBool(sample, out);
		OperatorUtils.writeLong(sPer, out);
		OperatorUtils.serializeIndex(scanIndex, out, prev);
		OperatorUtils.writeInt(tType, out);
	}

	public void setAlias(final String alias)
	{
		this.alias = alias;
		final TreeMap<Integer, String> newPos2Col = new TreeMap<Integer, String>();
		final HashMap<String, Integer> newCols2Pos = new HashMap<String, Integer>();
		final HashMap<String, String> newCols2Types = new HashMap<String, String>();
		for (final Map.Entry entry : pos2Col.entrySet())
		{
			String val = (String)entry.getValue();
			val = val.substring(val.indexOf('.') + 1);
			newPos2Col.put((Integer)entry.getKey(), alias + "." + val);
			newCols2Pos.put(alias + "." + val, (Integer)entry.getKey());
		}

		for (final Map.Entry entry : cols2Types.entrySet())
		{
			String val = (String)entry.getKey();
			val = val.substring(val.indexOf('.') + 1);
			newCols2Types.put(alias + "." + val, (String)entry.getValue());
		}

		pos2Col = newPos2Col;
		cols2Pos = newCols2Pos;
		cols2Types = newCols2Types;
	}

	public void setChildForDevice(final int device, final Operator child)
	{
		device2Child.put(device, child);
	}

	@Override
	public void setChildPos(final int pos)
	{
	}

	public void setCNFForParent(final Operator op, final CNFFilter filter)
	{
		orderedFilters.put(op, filter);
		if (op instanceof NetworkHashAndSendOperator || op instanceof NetworkSendMultipleOperator || op instanceof NetworkSendRROperator)
		{
			return;
		}
		opParents.put(op.parent(), op);
	}

	public void setIndexOnly()
	{
		indexOnly = true;
	}

	public void setIndexScan(final Index scanIndex)
	{
		this.scanIndex = scanIndex;
		scanIndex.setTransaction(new Transaction(0));
	}

	public void setMetaData(final Transaction t) throws Exception
	{
		set = true;
		partMeta = meta.getPartMeta(schema, name, t);
	}

	public void setNeededCols(ArrayList<String> needed)
	{
		if (getRID)
		{
			final ArrayList<String> newNeeded = new ArrayList<String>(needed.size());
			needed.remove(0);
			needed.remove(0);
			needed.remove(0);
			needed.remove(0);
			newNeeded.add("_RID1");
			newNeeded.add("_RID2");
			newNeeded.add("_RID3");
			newNeeded.add("_RID4");
			newNeeded.addAll(needed);
			needed = newNeeded;
		}
		fetchPos = new ArrayList<Integer>(needed.size());
		for (final String col : needed)
		{
			fetchPos.add(cols2Pos.get(col));
		}

		for (final CNFFilter filter : orderedFilters.values())
		{
			for (final String col2 : filter.getReferences())
			{
				final int pos = cols2Pos.get(col2);
				if (!fetchPos.contains(pos))
				{
					fetchPos.add(pos);
				}
			}
		}

		int i = 0;
		final HashMap<String, Integer> fetchCols2Pos = new HashMap<String, Integer>();
		midPos2Col = new String[fetchPos.size()];
		midCols2Types = new HashMap<String, String>();
		for (final int pos : fetchPos)
		{
			fetchCols2Pos.put(pos2Col.get(pos), i);
			midPos2Col[i] = pos2Col.get(pos);
			midCols2Types.put(pos2Col.get(pos), cols2Types.get(pos2Col.get(pos)));
			i++;
		}

		for (final CNFFilter filter : orderedFilters.values())
		{
			filter.updateCols2Pos(fetchCols2Pos);
		}

		// calculate how to go from what was fetched to what needs to be output
		neededPos = new ArrayList<Integer>(needed.size());
		for (final String col : needed)
		{
			neededPos.add(fetchCols2Pos.get(col));
		}

		// update internal metadata x3
		cols2Pos = new HashMap<String, Integer>();
		pos2Col = new TreeMap<Integer, String>();
		final HashMap<String, String> tempCols2Types = new HashMap<String, String>();
		i = 0;
		for (final String col : needed)
		{
			cols2Pos.put(col, i);
			pos2Col.put(i, col);
			tempCols2Types.put(col, cols2Types.get(col));
			i++;
		}

		cols2Types = tempCols2Types;
	}

	@Override
	public void setNode(final int node)
	{
		this.node = node;
	}

	public void setPhase2Done()
	{
		phase2Done = true;
	}

	@Override
	public void setPlan(final Plan plan)
	{
	}

	public void setSample(final long sPer)
	{
		sample = true;
		this.sPer = sPer;
	}

	public void setTransaction(final Transaction tx)
	{
		this.tx = tx;
	}

	public void setType(final int type)
	{
		tType = type;
	}

	@Override
	public synchronized void start() throws Exception
	{
		// HRDBMSWorker.logger.debug("Starting " + TableScanOperator.this);
		ins = new ArrayList<String>();
		readBuffers = new HashMap<Operator, BufferedLinkedBlockingQueue>();
		optimize = false;
		randomIns = new ArrayList<String>();
		ins2Device = new HashMap<String, Integer>();
		forceDone = false;

		if (!startDone)
		{
			// HRDBMSWorker.logger.debug(TableScanOperator.this +
			// " did need to be started");
			startDone = true;

			if (devices.size() == 0)
			{
				final String in = schema + "." + name + ".tbl";
				ins.add(in);
				// HRDBMSWorker.logger.debug(TableScanOperator.this +
				// " had 0 devices");
			}
			else
			{
				// HRDBMSWorker.logger.debug(TableScanOperator.this + " had " +
				// devices.size() + " devices");
				for (final int device : devices)
				{
					if (children.size() == 0)
					{
						final String in = MetaData.getDevicePath(device) + schema + "." + name + ".tbl";
						ins.add(in);
					}
					else
					{
						final String in = MetaData.getDevicePath(device) + schema + "." + name + ".tbl";
						randomIns.add(in);
						ins2Device.put(in, device);
					}
				}
			}

			for (final Operator parent : parents)
			{
				if (!orderedFilters.containsKey(parent))
				{
					orderedFilters.put(parent, new NullCNFFilter());
				}
			}

			if (parents.size() == 1)
			{
				optimize = true;
			}

			if (!optimize)
			{
				// HRDBMSWorker.logger.debug("WARNING: " +
				// TableScanOperator.this + " had " + parents.size() +
				// " parents");
				for (final Operator parent : parents)
				{
					readBuffers.put(parent, new BufferedLinkedBlockingQueue(ResourceManager.QUEUE_SIZE));
				}
			}
			else
			{
				readBuffer = new BufferedLinkedBlockingQueue(ResourceManager.QUEUE_SIZE);
			}

			init();
		}
	}

	@Override
	public String toString()
	{
		String table2 = null;
		if (alias == null || alias.equals(""))
		{
			table2 = name;
		}
		else
		{
			table2 = alias;
		}
		String retval = "TableScanOperator(" + node + ":" + devices + "): " + schema + "." + table2;
		for (final Map.Entry entry : orderedFilters.entrySet())
		{
			if (entry.getValue() != null)
			{
				retval += (", (" + entry.getValue().toString()) + ")";
			}
		}

		retval += (" : " + cols2Pos);

		return retval;
	}

	private void init()
	{
		final InitThread t = new InitThread();
		t.start();
	}

	private boolean needThisColForFilter(final String col, final CNFFilter filter)
	{
		if (referencesHash == null)
		{
			final HashSet<Integer> temp = new HashSet<Integer>();
			for (final String s : filter.getReferencesHash())
			{
				temp.add(s.hashCode());
			}

			referencesHash = temp;
		}

		return referencesHash.contains(col.hashCode());
	}

	public static class CNFEntry implements Serializable
	{
		private final HashSet<HashMap<Filter, Filter>> cnf;
		private final BitSet bitSet;
		private final AtomicLong usage = new AtomicLong(0);

		public CNFEntry(final HashSet<HashMap<Filter, Filter>> cnf, final BitSet bitSet)
		{
			this.cnf = cnf;
			this.bitSet = bitSet;
		}

		@Override
		public boolean equals(final Object r)
		{
			final CNFEntry rhs = (CNFEntry)r;
			return cnf.equals(rhs.cnf);
		}

		public BitSet getBitSet()
		{
			return bitSet;
		}

		public HashSet<HashMap<Filter, Filter>> getCNF()
		{
			return cnf;
		}

		public long getUsage()
		{
			return usage.get();
		}

		@Override
		public int hashCode()
		{
			return cnf.hashCode();
		}

		public void incrementUsage()
		{
			usage.incrementAndGet();
		}

		@Override
		public String toString()
		{
			return cnf.toString() + " : " + bitSet.toString();
		}
	}

	public final class ReaderThread extends ThreadPoolThread
	{
		private String in;
		private String in2;
		private Index scan;
		int myMaxBlock = 0;
		int start = 0;
		private String pbpeDebug1;
		private String pbpeDebug2;

		public ReaderThread(final String in)
		{
			this.in = in;
		}

		public ReaderThread(final String in2, final boolean marker)
		{
			this.in2 = in2;
		}

		public ReaderThread(final String in, final Index scan)
		{
			this.in = in;
			this.scan = scan;
		}

		public ReaderThread(final String in, final int start, final int max)
		{
			this.in = in;
			this.start = start;
			this.myMaxBlock = max;
		}

		public void colTableRT()
		{
			final int pbpeVer = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("pbpe_version"));
			final boolean v2OrHigher = (pbpeVer >= 2);
			boolean v3OrHigher = (pbpeVer >= 3);
			final boolean isV4 = (pbpeVer == 4);
			boolean v5OrHigher = (pbpeVer >= 5);
			boolean v6OrHigher = (pbpeVer >= 6);
			boolean v7 = (pbpeVer == 7);
			boolean v8 = (pbpeVer == 8);
			final boolean v9 = (pbpeVer == 9);

			SolverContext context = null;
			if (v3OrHigher)
			{
				context = (SolverContext)contextQ.poll();
				if (context == null)
				{
					try
					{
						context = SolverContextFactory.createSolverContext(config, logger, shutdown.getNotifier(), Solvers.SMTINTERPOL);
					}
					catch (final InvalidConfigurationException e)
					{
						// context = null;
						HRDBMSWorker.logger.debug("", e);
						v3OrHigher = false;
						v5OrHigher = false;
						v6OrHigher = false;
						v7 = false;
						v8 = false;
					}
				}
			}

			if (TableScanOperator.this.getRID)
			{
				if (!pos2Col.get(0).equals("_RID1") || !pos2Col.get(1).equals("_RID2") || !pos2Col.get(2).equals("_RID3") || !pos2Col.get(3).equals("_RID4"))
				{
					HRDBMSWorker.logger.debug("Col table internal error: order of RID cols is wrong");
				}
			}
			// new ArrayList<ReaderThread>();
			CNFFilter filter = orderedFilters.get(parents.get(0));
			if (filter != null && !(filter instanceof NullCNFFilter))
			{
				filter = filter.clone();
			}
			boolean neededPosNeeded = true;
			int get = 0;
			int skip = 0;
			boolean checkNoResults = (filter != null && !(filter instanceof NullCNFFilter) && !sample);
			HashSet<HashMap<Filter, Filter>> hshm = null;
			if (checkNoResults)
			{
				hshm = filter.getHSHM();
				if (hshm == null)
				{
					checkNoResults = false;
				}
				else
				{
					AtomicLong al = noResultCounts.get(hshm);
					if (al == null)
					{
						noResultCounts.put(hshm, new AtomicLong(1));
					}
					else
					{
						al.incrementAndGet();
					}
				}
			}

			if (sample)
			{
				get = (int)sPer;
				skip = 100 - get;
			}

			if (scan != null)
			{
				try
				{
					final Index index = new Index(in, scan.getKeys(), scan.getTypes(), scan.getOrders());
					index.open();
					index.scan(filter, sample, get, skip, readBuffer, midPos2Col, pos2Col, tx, getRID);
					return;
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.error("", e);
					try
					{
						readBuffer.put(e);
					}
					catch (final Exception f)
					{
					}
					return;
				}
			}

			if (filter != null)
			{
				if (neededPos.size() == fetchPos.size())
				{
					int i = 0;
					neededPosNeeded = false;
					for (final int pos : neededPos)
					{
						if (pos != i)
						{
							neededPosNeeded = true;
							break;
						}

						i++;
					}
				}
			}

			final ArrayList<String> types = new ArrayList<String>(midPos2Col.length);
			for (final String entry : midPos2Col)
			{
				types.add(midCols2Types.get(entry));
			}

			final ArrayList<Integer> cols = new ArrayList<Integer>(fetchPos.size());
			final HashMap<Integer, Integer> rowToIterator = new HashMap<Integer, Integer>();

			if (!getRID)
			{
				int w = 0;
				while (w < fetchPos.size())
				{
					cols.add(fetchPos.get(w++));
				}

				Collections.sort(cols);
				int pos = 0;
				for (final int col : cols)
				{
					final int index = fetchPos.indexOf(col);
					rowToIterator.put(index, pos);
					pos++;
				}
			}
			else
			{
				int w = 0;
				while (w < fetchPos.size())
				{
					final int col = fetchPos.get(w++);
					if (col >= 4)
					{
						cols.add(col - 4);
					}
				}

				Collections.sort(cols);
				int pos = 0;
				for (final int col : cols)
				{
					final int index = fetchPos.indexOf(col + 4);
					rowToIterator.put(index, pos);
					pos++;
				}

				// HRDBMSWorker.logger.debug("COLS is " + cols + ", fetchPos is
				// " + fetchPos);
			}

			try
			{
				if (in2 == null)
				{
					LockManager.sLock(new Block(in, -1), tx.number());
					Integer numBlocks = FileManager.numBlocks.get(in);
					if (numBlocks == null)
					{
						FileManager.getFile(in);
						numBlocks = FileManager.numBlocks.get(in);
					}

					if (numBlocks == 0)
					{
						throw new Exception("Unable to open file " + in);
					}

					final HashMap<Integer, DataType> layout = new HashMap<Integer, DataType>();
					for (final Map.Entry entry : tablePos2Col.entrySet())
					{
						final String type = tableCols2Types.get(entry.getValue());
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
					BufferManager.registerInterest(this, in, onPage, numBlocks - 1);

					int lastRequested = onPage - 1;
					// long count = 0;
					ArrayList<Object> row = new ArrayList<Object>(fetchPos.size());
					int get2 = get;
					int skip2 = skip;
					int get3 = get;
					int skip3 = skip;

					PREFETCH_REQUEST_SIZE = PREFETCH_REQUEST_SIZE_STATIC * layout.size() / cols.size();
					if (PREFETCH_REQUEST_SIZE < layout.size() * 2)
					{
						PREFETCH_REQUEST_SIZE = layout.size() * 2;
					}
					final int MAX_PAGES_IN_ADVANCE = PREFETCH_REQUEST_SIZE * 2;

					RequestPagesThread raThread = null;
					final ArrayList<Integer> skipped = new ArrayList<Integer>();

					BitSet pagesToSkip = null;
					BitSet newPagesToSkip = null;
					HashMap<Filter, CompressedBitSet> falseFilters = null;
					if (v5OrHigher && checkNoResults)
					{
						pagesToSkip = computePagesToSkip(hshm, context, in, layout.size(), v8, v9);

						if (v6OrHigher)
						{
							newPagesToSkip = new CompressedBitSet();
						}
						else
						{
							newPagesToSkip = new BitSet();
						}

						if (v7)
						{
							falseFilters = new HashMap<Filter, CompressedBitSet>();
						}
					}

					while (onPage < numBlocks)
					{
						if (lastRequested - onPage < MAX_PAGES_IN_ADVANCE)
						{
							if (raThread != null)
							{
								raThread.join();
							}

							BufferManager.updateProgress(this, onPage);

							if (!sample)
							{
								// Block[] toRequest = new Block[lastRequested +
								// PREFETCH_REQUEST_SIZE < numBlocks ?
								// PREFETCH_REQUEST_SIZE : numBlocks -
								// lastRequested - 1];
								final ArrayList<Block> toRequest = new ArrayList<Block>();
								int i = 0;
								final int length = lastRequested + PREFETCH_REQUEST_SIZE < numBlocks ? PREFETCH_REQUEST_SIZE : numBlocks - lastRequested - 1;
								while (i < length)
								{
									if ((lastRequested + i + 1) % layout.size() == 1)
									{
										final Block block = new Block(in, lastRequested + i + 1);
										if (v5OrHigher)
										{
											if (pagesToSkip != null)
											{
												if (pagesToSkip.get(lastRequested + i + 1))
												{
													skipped.add(lastRequested + i + 1);
													i++;
													continue;
												}
											}
										}
										else if (hshm != null && filter != null)
										{
											final ThreadMXBean tmxb = ManagementFactory.getThreadMXBean();
											final long start = tmxb.getCurrentThreadCpuTime();
											final Set<HashSet<HashMap<Filter, Filter>>> filters = noResults.get(block);
											if (filters.size() > 0)
											{
												// HRDBMSWorker.logger.debug("Filters
												// is size " + filters.size());
												try
												{
													if (isV4)
													{
														final boolean retval1 = canSatisfySMT(hshm, filters, context);
														final long end1 = tmxb.getCurrentThreadCpuTime();
														final boolean retval2 = canSatisfy(hshm, filters);
														final long end2 = tmxb.getCurrentThreadCpuTime();
														TableScanOperator.SMTSolveTime.getAndAdd(end1 - start);
														TableScanOperator.nonSMTSolveTime.getAndAdd(end2 - end1);
														if (retval1 != retval2)
														{
															HRDBMSWorker.logger.debug("SMT and non-SMT disagree: " + hshm + ", " + filters + ", " + pbpeDebug1 + ", " + pbpeDebug2);
														}

														if (!retval1)
														{
															skipped.add(lastRequested + i + 1);
															i++;
															continue;
														}
													}
													else if (v3OrHigher)
													{
														if (!canSatisfySMT(hshm, filters, context))
														{
															skipped.add(lastRequested + i + 1);
															i++;
															final long end1 = tmxb.getCurrentThreadCpuTime();
															TableScanOperator.SMTSolveTime.getAndAdd(end1 - start);
															continue;
														}
														else
														{
															final long end1 = tmxb.getCurrentThreadCpuTime();
															TableScanOperator.SMTSolveTime.getAndAdd(end1 - start);
														}
													}
													else if (!v3OrHigher)
													{
														if (!canSatisfy(hshm, filters))
														{
															skipped.add(lastRequested + i + 1);
															i++;
															final long end1 = tmxb.getCurrentThreadCpuTime();
															TableScanOperator.nonSMTSolveTime.getAndAdd(end1 - start);
															continue;
														}
														else
														{
															final long end1 = tmxb.getCurrentThreadCpuTime();
															TableScanOperator.nonSMTSolveTime.getAndAdd(end1 - start);
														}
													}
												}
												catch (final Throwable e)
												{
													HRDBMSWorker.logger.debug("", e);
												}
											}
										}

										toRequest.add(block);
									}

									i++;
								}

								if (toRequest.size() > 0)
								{
									final Block[] toRequest2 = toRequest.toArray(new Block[toRequest.size()]);
									raThread = tx.requestPages(toRequest2, cols, layout.size());
								}

								lastRequested += length;
							}
							else
							{
								final ArrayList<Block> toRequest = new ArrayList<Block>();
								int i = 0;
								final int length = lastRequested + PREFETCH_REQUEST_SIZE < numBlocks ? PREFETCH_REQUEST_SIZE : numBlocks - lastRequested - 1;
								while (i < length)
								{
									if ((lastRequested + i + 1) % layout.size() == 1)
									{
										if (skip3 == 0)
										{
											get3 = get - 1;
											skip3 = skip;
										}
										else if (get3 == 0)
										{
											skip3--;
											i += layout.size();
											continue;
										}
										else
										{
											get3--;
										}

										toRequest.add(new Block(in, lastRequested + i + 1));
										i += layout.size();
									}
									else
									{
										i++;
									}
								}

								if (toRequest.size() > 0)
								{
									final Block[] toRequest2 = new Block[toRequest.size()];
									int j = 0;
									int z = 0;
									final int limit = toRequest.size();
									// for (Block b : toRequest)
									while (z < limit)
									{
										final Block b = toRequest.get(z++);
										toRequest2[j] = b;
										j++;
									}

									raThread = tx.requestPages(toRequest2, cols, layout.size());
								}

								lastRequested += length;
							}
						}

						if (sample && skip2 == 0)
						{
							get2 = get - 1;
							skip2 = skip;
						}
						else if (sample && get2 == 0)
						{
							skip2--;
							onPage += layout.size();
							continue;
						}
						else if (sample)
						{
							get2--;
						}

						final Block thisBlock = new Block(in, onPage);
						if (hshm != null)
						{
							// Set<HashSet<HashMap<Filter, Filter>>> filters =
							// noResults.get(thisBlock);
							// if (filter != null && !canSatisfy(hshm, filters))
							// {
							// skippedPages.getAndIncrement();
							// onPage += layout.size();
							// continue;
							// }
							if (skipped.contains(onPage))
							{
								skippedPages.getAndIncrement();
								onPage += layout.size();
								continue;
							}
						}

						tx.read(new Block(in, onPage), sch, cols, true);
						Iterator rit = null;
						if (!getRID)
						{
							onPage += layout.size();
							rit = sch.colTableIterator();
						}
						else
						{
							onPage += layout.size();
							rit = sch.colTableIteratorWithRIDs();
						}

						boolean hadResults = false;
						if (v7 && checkNoResults)
						{
							filter.reset();
						}
						while (rit.hasNext())
						{
							final Object o = rit.next();
							FieldValue[] r = null;
							RID rid = null;
							if (getRID)
							{
								final Map.Entry entry = (Map.Entry)o;
								rid = (RID)entry.getKey();
								r = (FieldValue[])entry.getValue();
							}
							else
							{
								r = (FieldValue[])o;
							}

							int j = 0;
							int size = 0;
							try
							{
								size = fetchPos.size();
							}
							catch (final Exception e)
							{
								if (forceDone)
								{
									checkNoResults = false;
									BufferManager.unregisterInterest(this);
									readBuffer.put(new DataEndMarker());
									return;
								}
								else
								{
									throw e;
								}
							}
							row.clear();

							synchronized (r)
							{
								while (j < size)
								{
									if (!getRID)
									{
										try
										{
											row.add(r[rowToIterator.get(j)].getValue());
										}
										catch (final NullPointerException e)
										{
											HRDBMSWorker.logger.debug("Row is " + r);
											throw e;
										}
									}
									else
									{
										if (j >= 4)
										{

											try
											{
												row.add(r[rowToIterator.get(j)].getValue());
											}
											catch (final Exception e)
											{
												HRDBMSWorker.logger.debug("Row = " + row);
												HRDBMSWorker.logger.debug("R = " + Arrays.asList(r));
												HRDBMSWorker.logger.debug("J = " + j);
												HRDBMSWorker.logger.debug("RowToIterator = " + rowToIterator);
												throw e;
											}
										}
										else if (j == 0)
										{
											row.add(rid.getNode());
										}
										else if (j == 1)
										{
											row.add(rid.getDevice());
										}
										else if (j == 2)
										{
											row.add(rid.getBlockNum());
										}
										else if (j == 3)
										{
											row.add(rid.getRecNum());
										}
									}
									j++;
								}
							}

							{
								if (filter != null)
								{
									if (filter.passes(row))
									{
										hadResults = true;

										if (neededPosNeeded)
										{
											final ArrayList<Object> newRow = new ArrayList<Object>(neededPos.size());
											int z = 0;
											final int limit = neededPos.size();
											// for (final int pos : neededPos)
											while (z < limit)
											{
												newRow.add(row.get(neededPos.get(z++)));
											}
											if (!forceDone)
											{
												readBuffer.put(newRow);
											}
											else
											{
												checkNoResults = false;
												BufferManager.unregisterInterest(this);
												readBuffer.put(new DataEndMarker());
												return;
											}
										}
										else
										{
											if (!forceDone)
											{
												readBuffer.put(row);
												row = new ArrayList<Object>(fetchPos.size());
											}
											else
											{
												checkNoResults = false;
												BufferManager.unregisterInterest(this);
												readBuffer.put(new DataEndMarker());
												return;
											}
										}
									}
								}
								else
								{
									hadResults = true;
									// final ArrayList<Object> newRow = new
									// ArrayList<Object>(neededPos.size());
									// for (final int pos : neededPos)
									// {
									// newRow.add(row.get(pos));
									// }
									if (!forceDone)
									{
										readBuffer.put(row);
										row = new ArrayList<Object>(fetchPos.size());
									}
									else
									{
										checkNoResults = false;
										BufferManager.unregisterInterest(this);
										readBuffer.put(new DataEndMarker());
										return;
									}
								}
							}
						}

						if (checkNoResults && v7)
						{
							final ThreadMXBean tmxb = ManagementFactory.getThreadMXBean();
							final long start = tmxb.getCurrentThreadCpuTime();
							final HashSet<Filter> falseForPage = filter.getFalseResults();
							for (final Filter f : falseForPage)
							{
								CompressedBitSet bs = falseFilters.get(f);
								if (bs == null)
								{
									bs = new CompressedBitSet();
									bs.set(onPage - layout.size());
									falseFilters.put(f, bs);
								}
								else
								{
									bs.set(onPage - layout.size());
								}
							}
							final long end = tmxb.getCurrentThreadCpuTime();
							TableScanOperator.pbpeMaintenanceTime.getAndAdd(end - start);
						}

						if (checkNoResults && !hadResults && v2OrHigher)
						{
							final ThreadMXBean tmxb = ManagementFactory.getThreadMXBean();
							final long start = tmxb.getCurrentThreadCpuTime();
							if (v5OrHigher)
							{
								newPagesToSkip.set(thisBlock.number());
							}
							else
							{
								noResults.multiPut(thisBlock, hshm);
							}

							final long end = tmxb.getCurrentThreadCpuTime();
							TableScanOperator.pbpeMaintenanceTime.getAndAdd(end - start);
						}
					}

					BufferManager.unregisterInterest(this);
					if (v5OrHigher)
					{
						contextQ.offer(context);
					}
					if (v7 && checkNoResults && falseFilters.size() > 0)
					{
						final ThreadMXBean tmxb = ManagementFactory.getThreadMXBean();
						final long start = tmxb.getCurrentThreadCpuTime();
						MultiHashMap<Integer, CNFEntry> mhm = pbpeCache2.get(in);
						if (mhm == null)
						{
							mhm = new MultiHashMap<Integer, CNFEntry>();
							pbpeCache2.put(in, mhm);
							mhm = pbpeCache2.get(in);
						}

						for (final Map.Entry entry : falseFilters.entrySet())
						{
							final Filter f = (Filter)entry.getKey();
							final CompressedBitSet bs = (CompressedBitSet)entry.getValue();
							final HashMap<Filter, Filter> hm = new HashMap<Filter, Filter>();
							hm.put(f, f);
							final HashSet<HashMap<Filter, Filter>> hshm2 = new HashSet<HashMap<Filter, Filter>>();
							hshm2.add(hm);
							final CNFEntry cnfEntry = new CNFEntry(hshm2, bs);
							final HashSet<Integer> hashCodes = getAllHashCodes(hshm2);
							for (final int i : hashCodes)
							{
								final ConcurrentHashMap<CNFEntry, CNFEntry> map = mhm.getMap(i);
								if (map != null)
								{
									final CNFEntry entry2 = map.get(cnfEntry);
									if (entry2 != null)
									{
										final BitSet bs2 = entry2.getBitSet();
										final BitSet bs3 = cnfEntry.getBitSet();
										synchronized (bs2)
										{
											((CompressedBitSet)bs2).or((CompressedBitSet)bs3);
										}
									}
									else
									{
										mhm.multiPut(i, cnfEntry);
									}
								}
								else
								{
									mhm.multiPut(i, cnfEntry);
								}
							}
						}

						final long end = tmxb.getCurrentThreadCpuTime();
						TableScanOperator.pbpeMaintenanceTime.getAndAdd(end - start);
					}
					if (v5OrHigher && newPagesToSkip != null && !newPagesToSkip.isEmpty())
					{
						final ThreadMXBean tmxb = ManagementFactory.getThreadMXBean();
						final long start = tmxb.getCurrentThreadCpuTime();
						final CNFEntry cnfEntry = new CNFEntry(hshm, newPagesToSkip);
						MultiHashMap<Integer, CNFEntry> mhm = pbpeCache2.get(in);
						if (mhm == null)
						{
							mhm = new MultiHashMap<Integer, CNFEntry>();
							pbpeCache2.put(in, mhm);
							mhm = pbpeCache2.get(in);
						}

						final HashSet<Integer> hashCodes = getAllHashCodes(hshm);
						for (final int i : hashCodes)
						{
							final ConcurrentHashMap<CNFEntry, CNFEntry> map = mhm.getMap(i);
							if (map != null)
							{
								final CNFEntry entry2 = map.get(cnfEntry);
								if (entry2 != null)
								{
									final BitSet bs2 = entry2.getBitSet();
									final BitSet bs = cnfEntry.getBitSet();
									synchronized (bs2)
									{
										if (v6OrHigher)
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
									mhm.multiPut(i, cnfEntry);
								}
							}
							else
							{
								mhm.multiPut(i, cnfEntry);
							}
						}

						if (v9)
						{
							final String msg = in + "," + System.currentTimeMillis();
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

						final long end = tmxb.getCurrentThreadCpuTime();
						TableScanOperator.pbpeMaintenanceTime.getAndAdd(end - start);
					}
				}
				else
				{
					final Operator child = device2Child.get(ins2Device.get(in2));
					Object o = null;
					try
					{
						o = child.next(TableScanOperator.this);
					}
					catch (final Exception e)
					{
						HRDBMSWorker.logger.error("ins2Device = " + ins2Device, e);
						HRDBMSWorker.logger.error("ins2Device.get(in2) = " + ins2Device.get(in2));
						HRDBMSWorker.logger.error("device2Child = " + device2Child);
						HRDBMSWorker.logger.error("device2Child.get(ins2Device.get(in2)) = " + device2Child.get(ins2Device.get(in2)));
						readBuffer.put(e);
						return;
					}
					// @?Parallel
					final int device = ins2Device.get(in2);
					final int currentPage = -1;
					final HashMap<Integer, DataType> layout = new HashMap<Integer, DataType>();
					for (final Map.Entry entry : tablePos2Col.entrySet())
					{
						final String type = tableCols2Types.get(entry.getValue());
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
					// long count = 0;
					ArrayList<Object> row = new ArrayList<Object>(fetchPos.size());
					while (!(o instanceof DataEndMarker))
					{
						if (!indexOnly)
						{
							final long partialRid = (Long)(((ArrayList<Object>)o).get(0));
							final int blockNum = (int)(partialRid >> 32);
							final int recNum = (int)(partialRid & 0xFFFFFFFF);
							// HRDBMSWorker.logger.debug("Col table index fetch
							// for block " + blockNum + " and record " + recNum
							// + " with cols = " + cols);
							if (blockNum != currentPage)
							{
								final Block b = new Block(in2, blockNum);
								tx.requestPage(b, cols);
								tx.read(b, sch, cols, false);
							}
							int node2 = node;
							if (node2 < 0)
							{
								node2 = -1;
							}
							final ArrayList<FieldValue> r = sch.getRowForColTable(new RID(node2, device, blockNum, recNum));
							row.clear();

							int j = 0;
							final int size = fetchPos.size();
							while (j < size)
							{
								try
								{
									if (!getRID)
									{
										row.add(r.get(rowToIterator.get(j)).getValue());
									}
									else
									{
										if (j >= 4)
										{
											row.add(r.get(rowToIterator.get(j)).getValue());
										}
										else if (j == 0)
										{
											row.add(node2);
										}
										else if (j == 1)
										{
											row.add(device);
										}
										else if (j == 2)
										{
											row.add(blockNum);
										}
										else if (j == 3)
										{
											row.add(recNum);
										}
									}
									j++;
								}
								catch (final Exception e)
								{
									HRDBMSWorker.logger.debug("", e);
									throw e;
								}
							}

							// HRDBMSWorker.logger.debug("Row returned by index
							// fetch is " + row);
							// HRDBMSWorker.logger.debug("Pos2Col is " +
							// TableScanOperator.this.getPos2Col());

							if (!optimize)
							{
							}
							else
							{
								if (filter != null)
								{
									if (filter.passes(row))
									{
										if (neededPosNeeded)
										{
											final ArrayList<Object> newRow = new ArrayList<Object>(neededPos.size());
											for (final int pos : neededPos)
											{
												newRow.add(row.get(pos));
											}
											if (!forceDone)
											{
												readBuffer.put(newRow);
											}
											else
											{
												readBuffer.put(new DataEndMarker());
												return;
											}
										}
										else
										{
											if (!forceDone)
											{
												readBuffer.put(row);
												row = new ArrayList<Object>(fetchPos.size());
											}
											else
											{
												readBuffer.put(new DataEndMarker());
												return;
											}
										}
									}
								}
								else
								{
									if (!forceDone)
									{
										readBuffer.put(row);
										row = new ArrayList<Object>(fetchPos.size());
									}
									else
									{
										readBuffer.put(new DataEndMarker());
										return;
									}
								}
							}

							o = child.next(TableScanOperator.this);
						}
						else
						{
							filter = orderedFilters.get(parents.get(0));

							if (filter != null)
							{
								filter.updateCols2Pos(child.getCols2Pos());
								if (!filter.passes((ArrayList<Object>)o))
								{
									o = child.next(TableScanOperator.this);
									continue;
								}
							}

							final ArrayList<Object> row2 = new ArrayList<Object>(pos2Col.size());
							for (final String col : pos2Col.values())
							{
								row2.add(((ArrayList<Object>)o).get(child.getCols2Pos().get(col)));
							}

							if (!forceDone)
							{
								readBuffer.put(row2);
							}
							else
							{
								readBuffer.put(new DataEndMarker());
								return;
							}
							o = child.next(TableScanOperator.this);
						}
					}

					// System.out.println("TableScanOperator read " + count +
					// " rows based on a RID list");
				}
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				try
				{
					readBuffer.put(e);
				}
				catch (final Exception f)
				{
				}
				return;
			}
		}

		public void rowTableRT()
		{
			// ArrayList<ReaderThread> secondThreads = new
			// ArrayList<ReaderThread>();
			CNFFilter filter = orderedFilters.get(parents.get(0));
			boolean neededPosNeeded = true;
			int get = 0;
			int skip = 0;
			// boolean checkNoResults = (filter != null && !(filter instanceof
			// NullCNFFilter) && !sample);
			boolean checkNoResults = false;
			HashSet<HashMap<Filter, Filter>> hshm = null;
			if (checkNoResults)
			{
				hshm = filter.getHSHM();
				if (hshm == null)
				{
					checkNoResults = false;
				}
			}

			if (sample)
			{
				get = (int)sPer;
				skip = 100 - get;
			}

			if (scan != null)
			{
				try
				{
					final Index index = new Index(in, scan.getKeys(), scan.getTypes(), scan.getOrders());
					index.open();
					index.scan(filter, sample, get, skip, readBuffer, midPos2Col, pos2Col, tx, getRID);
					return;
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.error("", e);
					try
					{
						readBuffer.put(e);
					}
					catch (final Exception f)
					{
					}
					return;
				}
			}

			if (filter != null)
			{
				if (neededPos.size() == fetchPos.size())
				{
					int i = 0;
					neededPosNeeded = false;
					for (final int pos : neededPos)
					{
						if (pos != i)
						{
							neededPosNeeded = true;
							break;
						}

						i++;
					}
				}
			}
			final ArrayList<String> types = new ArrayList<String>(midPos2Col.length);
			for (final String entry : midPos2Col)
			{
				types.add(midCols2Types.get(entry));
			}

			try
			{
				if (in2 == null)
				{
					LockManager.sLock(new Block(in, -1), tx.number());
					// FileManager.getFile(in);
					// HRDBMSWorker.logger.debug("Opened " + in + " for " +
					// TableScanOperator.this);
					Integer numBlocks = FileManager.numBlocks.get(in);
					if (numBlocks == null)
					{
						FileManager.getFile(in);
						numBlocks = FileManager.numBlocks.get(in);
					}
					/*
					 * if (numBlocks > 5000 && myMaxBlock == 0) { int numThreads
					 * = 2; int blocksPerThread = (int)((numBlocks-4000) * 1.0 /
					 * numThreads); myMaxBlock = blocksPerThread + 4000;
					 * ReaderThread thread = new ReaderThread(in, myMaxBlock,
					 * numBlocks); thread.start(); secondThreads.add(thread); }
					 */
					// HRDBMSWorker.logger.debug(in + " has " + numBlocks +
					// " blocks");
					if (numBlocks == 0)
					{
						// HRDBMSWorker.logger.debug("Unable to open file " +
						// in);
						throw new Exception("Unable to open file " + in);
					}
					final HashMap<Integer, DataType> layout = new HashMap<Integer, DataType>();
					for (final Map.Entry entry : tablePos2Col.entrySet())
					{
						final String type = tableCols2Types.get(entry.getValue());
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

					PREFETCH_REQUEST_SIZE = PREFETCH_REQUEST_SIZE_STATIC;
					PAGES_IN_ADVANCE = PAGES_IN_ADVANCE_STATIC;
					final Schema[] schemas = new Schema[PREFETCH_REQUEST_SIZE * 4];
					int g = 0;
					while (g < schemas.length)
					{
						schemas[g++] = new Schema(layout);
					}
					final ConcurrentHashMap<Integer, Schema> schemaMap = new ConcurrentHashMap<Integer, Schema>();
					long schemaIndex = 0;

					int onPage = 1;
					if (myMaxBlock != 0)
					{
						numBlocks = myMaxBlock;
					}

					if (start != 0)
					{
						onPage = start;
					}

					BufferManager.registerInterest(this, in, onPage, numBlocks - 1);

					int lastRequested = onPage - 1;
					// long count = 0;
					ArrayList<Object> row = new ArrayList<Object>(fetchPos.size());
					int get2 = get;
					int skip2 = skip;
					int get3 = get;
					int skip3 = skip;

					while (onPage < numBlocks)
					{
						if (lastRequested - onPage < PAGES_IN_ADVANCE)
						{
							BufferManager.updateProgress(this, onPage);
							if (!sample)
							{
								// Block[] toRequest = new Block[lastRequested +
								// PREFETCH_REQUEST_SIZE < numBlocks ?
								// PREFETCH_REQUEST_SIZE : numBlocks -
								// lastRequested - 1];
								final ArrayList<Block> toRequest = new ArrayList<Block>();
								int i = 0;
								final int length = lastRequested + PREFETCH_REQUEST_SIZE < numBlocks ? PREFETCH_REQUEST_SIZE : numBlocks - lastRequested - 1;
								while (i < length)
								{
									final Block block = new Block(in, lastRequested + i + 1);
									// if (hshm != null)
									// {
									// Set<HashSet<HashMap<Filter, Filter>>>
									// filters = noResults.get(block);
									// if (filter != null &&
									// filters.contains(hshm))
									// {
									// i++;
									// continue;
									// }
									// }
									toRequest.add(block);
									i++;
								}

								final Block[] toRequest2 = new Block[toRequest.size()];
								i = 0;
								while (i < toRequest2.length)
								{
									toRequest2[i] = toRequest.get(i);
									i++;
								}

								if (!getRID)
								{
									tx.requestPages(toRequest2, schemas, (int)(schemaIndex % (PREFETCH_REQUEST_SIZE * 4)), schemaMap, fetchPos);
									schemaIndex += PREFETCH_REQUEST_SIZE;
								}
								else
								{
									tx.requestPages(toRequest2);
								}
								lastRequested += length;
							}
							else
							{
								final ArrayList<Block> toRequest = new ArrayList<Block>();
								int i = 0;
								final int length = lastRequested + PREFETCH_REQUEST_SIZE < numBlocks ? PREFETCH_REQUEST_SIZE : numBlocks - lastRequested - 1;
								while (i < length)
								{
									if (skip3 == 0)
									{
										get3 = get - 1;
										skip3 = skip;
									}
									else if (get3 == 0)
									{
										skip3--;
										i++;
										continue;
									}
									else
									{
										get3--;
									}

									toRequest.add(new Block(in, lastRequested + i + 1));
									i++;
								}

								if (toRequest.size() > 0)
								{
									final Block[] toRequest2 = new Block[toRequest.size()];
									int j = 0;
									int z = 0;
									final int limit = toRequest.size();
									// for (Block b : toRequest)
									while (z < limit)
									{
										final Block b = toRequest.get(z++);
										toRequest2[j] = b;
										j++;
									}

									if (!getRID)
									{
										tx.requestPages(toRequest2, schemas, (int)(schemaIndex % (PREFETCH_REQUEST_SIZE * 4)), schemaMap, fetchPos);
										schemaIndex += PREFETCH_REQUEST_SIZE;
									}
									else
									{
										tx.requestPages(toRequest2);
									}
								}

								lastRequested += length;
							}
						}

						if (sample && skip2 == 0)
						{
							get2 = get - 1;
							skip2 = skip;
						}
						else if (sample && get2 == 0)
						{
							skip2--;
							onPage++;
							continue;
						}
						else if (sample)
						{
							get2--;
						}

						// Block thisBlock = new Block(in, onPage);
						// if (hshm != null)
						// {
						// Set<HashSet<HashMap<Filter, Filter>>> filters =
						// noResults.get(thisBlock);
						// if (filter != null && filters.contains(hshm))
						// {
						// skippedPages.getAndIncrement();
						// onPage++;
						// continue;
						// }
						// }

						// tx.read(new Block(in, onPage++), sch);
						Schema sch = null;
						RowIterator rit = null;
						if (!getRID)
						{
							sch = schemaMap.get(onPage);
							while (sch == null)
							{
								LockSupport.parkNanos(500);
								sch = schemaMap.get(onPage);
							}

							schemaMap.remove(onPage);
							onPage++;

							synchronized (sch)
							{
								rit = sch.rowIterator(true);
							}
						}
						else
						{
							sch = schemas[0];
							tx.read(new Block(in, onPage++), sch);
							rit = sch.rowIterator(false);
						}

						outer: while (rit.hasNext())
						{
							final Row r = rit.next();
							RID rid = null;
							if (getRID)
							{
								rid = r.getRID();
							}
							int j = 0;
							boolean checked = false;
							final int size = fetchPos.size();
							row.clear();
							while (j < size)
							{
								if (!getRID)
								{
									if (filter != null && !needThisColForFilter(midPos2Col[j], filter))
									{
										row.add(null);
									}
									else
									{
										final FieldValue fv = r.getCol(fetchPos.get(j));
										if (!checked)
										{
											if (!fv.exists())
											{
												continue outer;
											}

											checked = true;
										}
										row.add(fv.getValue());
									}
								}
								else
								{
									final int colNum = fetchPos.get(j);
									if (colNum >= 4)
									{

										final FieldValue fv = r.getCol(colNum - 4);
										if (!checked)
										{
											if (!fv.exists())
											{
												continue outer;
											}

											checked = true;
										}
										row.add(fv.getValue());
									}
									else if (colNum == 0)
									{
										row.add(rid.getNode());
									}
									else if (colNum == 1)
									{
										row.add(rid.getDevice());
									}
									else if (colNum == 2)
									{
										row.add(rid.getBlockNum());
									}
									else if (colNum == 3)
									{
										row.add(rid.getRecNum());
									}
								}
								j++;
							}

							// if (!optimize)
							// {
							// for (final Map.Entry entry :
							// readBuffers.entrySet())
							// {
							// final BufferedLinkedBlockingQueue q =
							// (BufferedLinkedBlockingQueue)entry.getValue();
							// filter = orderedFilters.get(entry.getKey());

							// if (filter != null)
							// {
							// if (filter.passes(row))
							// {
							// final ArrayList<Object> newRow = new
							// ArrayList<Object>(neededPos.size());
							// for (final int pos : neededPos)
							// {
							// newRow.add(row.get(pos));
							// }
							// q.put(newRow);
							// }
							// }
							// else
							// {
							// final ArrayList<Object> newRow = new
							// ArrayList<Object>(neededPos.size());
							// for (final int pos : neededPos)
							// {
							// newRow.add(row.get(pos));
							// }
							// q.put(newRow);
							// }
							// }
							// }
							// else
							{
								if (filter != null)
								{
									if (filter.passes(row))
									{
										if (!getRID)
										{
											int i = 0;
											int z = 0;
											final int limit = row.size();
											// for (Object o : row)
											while (z < limit)
											{
												final Object o = row.get(z++);
												if (o == null)
												{
													final int temp = fetchPos.get(i);
													final FieldValue fv = r.getCol(temp);
													if (!fv.exists())
													{
														continue outer;
													}
													// row.set(i,
													// fv.getValue());
													final Object[] array = (Object[])unsafe.getObject(row, offset);
													array[i] = fv.getValue();
												}

												i++;
											}
										}

										if (neededPosNeeded)
										{
											final ArrayList<Object> newRow = new ArrayList<Object>(neededPos.size());
											int z = 0;
											final int limit = neededPos.size();
											// for (final int pos : neededPos)
											while (z < limit)
											{
												newRow.add(row.get(neededPos.get(z++)));
											}
											if (!forceDone)
											{
												readBuffer.put(newRow);
											}
											else
											{
												checkNoResults = false;
												BufferManager.unregisterInterest(this);
												readBuffer.put(new DataEndMarker());
												return;
											}
										}
										else
										{
											if (!forceDone)
											{
												readBuffer.put(row);
												row = new ArrayList<Object>(fetchPos.size());
											}
											else
											{
												checkNoResults = false;
												BufferManager.unregisterInterest(this);
												readBuffer.put(new DataEndMarker());
												return;
											}
										}
									}
								}
								else
								{
									// final ArrayList<Object> newRow = new
									// ArrayList<Object>(neededPos.size());
									// for (final int pos : neededPos)
									// {
									// newRow.add(row.get(pos));
									// }
									if (!forceDone)
									{
										readBuffer.put(row);
										row = new ArrayList<Object>(fetchPos.size());
									}
									else
									{
										checkNoResults = false;
										BufferManager.unregisterInterest(this);
										readBuffer.put(new DataEndMarker());
										return;
									}
								}
							}
						}

						// if (checkNoResults && !hadResults)
						// {
						// noResults.multiPut(thisBlock, hshm);
						// }
					}

					BufferManager.unregisterInterest(this);

					// if (start == 0 && myMaxBlock != 0)
					// {
					// for (ReaderThread thread : secondThreads)
					// {
					// thread.join();
					// }
					// }
				}
				else
				{
					final Operator child = device2Child.get(ins2Device.get(in2));
					Object o = null;
					try
					{
						o = child.next(TableScanOperator.this);
					}
					catch (final Exception e)
					{
						HRDBMSWorker.logger.error("ins2Device = " + ins2Device, e);
						HRDBMSWorker.logger.error("ins2Device.get(in2) = " + ins2Device.get(in2));
						HRDBMSWorker.logger.error("device2Child = " + device2Child);
						HRDBMSWorker.logger.error("device2Child.get(ins2Device.get(in2)) = " + device2Child.get(ins2Device.get(in2)));
						readBuffer.put(e);
						return;
					}
					// @?Parallel
					final int device = ins2Device.get(in2);
					final int currentPage = -1;
					final HashMap<Integer, DataType> layout = new HashMap<Integer, DataType>();
					for (final Map.Entry entry : tablePos2Col.entrySet())
					{
						final String type = tableCols2Types.get(entry.getValue());
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
					// long count = 0;
					ArrayList<Object> row = new ArrayList<Object>(fetchPos.size());
					while (!(o instanceof DataEndMarker))
					{
						if (!indexOnly)
						{
							final long partialRid = (Long)(((ArrayList<Object>)o).get(0));
							final int blockNum = (int)(partialRid >> 32);
							final int recNum = (int)(partialRid & 0xFFFFFFFF);
							if (blockNum != currentPage)
							{
								final Block b = new Block(in2, blockNum);
								tx.requestPage(b);
								tx.read(b, sch);
								// count++;

								// if (count > MAX_PAGES)
								// {
								// tx.checkpoint(in2, b);
								// count = 1;
								// }
							}
							int node2 = node;
							if (node2 < 0)
							{
								node2 = -1;
							}
							final Row r = sch.getRow(new RID(node2, device, blockNum, recNum));
							row.clear();

							int j = 0;
							final int size = fetchPos.size();
							while (j < size)
							{
								try
								{
									if (!getRID)
									{
										if (filter != null && !needThisColForFilter(midPos2Col[j], filter))
										{
											row.add(null);
										}
										else
										{
											final FieldValue fv = r.getCol(fetchPos.get(j));
											row.add(fv.getValue());
										}
									}
									else
									{
										final int colNum = fetchPos.get(j);
										if (colNum >= 4)
										{
											final FieldValue fv = r.getCol(colNum - 4);
											row.add(fv.getValue());
										}
										else if (colNum == 0)
										{
											row.add(node2);
										}
										else if (colNum == 1)
										{
											row.add(device);
										}
										else if (colNum == 2)
										{
											row.add(blockNum);
										}
										else if (colNum == 3)
										{
											row.add(recNum);
										}
									}
									j++;
								}
								catch (final Exception e)
								{
									HRDBMSWorker.logger.debug("", e);
									throw e;
								}
							}

							if (!optimize)
							{
								// for (final Map.Entry entry :
								// readBuffers.entrySet())
								// {
								// final BufferedLinkedBlockingQueue q =
								// (BufferedLinkedBlockingQueue)entry.getValue();
								// filter = orderedFilters.get(entry.getKey());

								// if (filter != null)
								// {
								// if (filter.passes(row))
								// {
								// final ArrayList<Object> newRow = new
								// ArrayList<Object>(neededPos.size());
								// for (final int pos : neededPos)
								// {
								// newRow.add(row.get(pos));
								// }
								// q.put(newRow);
								// }
								// }
								// else
								// {
								// final ArrayList<Object> newRow = new
								// ArrayList<Object>(neededPos.size());
								// for (final int pos : neededPos)
								// {
								// newRow.add(row.get(pos));
								// }
								// q.put(newRow);
								// }
								// }
							}
							else
							{
								if (filter != null)
								{
									if (filter.passes(row))
									{
										if (!getRID)
										{
											int i = 0;
											int z = 0;
											final int limit = row.size();
											// for (Object o2 : row)
											while (z < limit)
											{
												final Object o2 = row.get(z++);
												if (o2 == null)
												{
													final FieldValue fv = r.getCol(fetchPos.get(i));
													// row.set(i,
													// fv.getValue());
													final Object[] array = (Object[])unsafe.getObject(row, offset);
													array[i] = fv.getValue();
												}

												i++;
											}
										}

										if (neededPosNeeded)
										{
											final ArrayList<Object> newRow = new ArrayList<Object>(neededPos.size());
											for (final int pos : neededPos)
											{
												newRow.add(row.get(pos));
											}
											if (!forceDone)
											{
												readBuffer.put(newRow);
											}
											else
											{
												readBuffer.put(new DataEndMarker());
												return;
											}
										}
										else
										{
											if (!forceDone)
											{
												readBuffer.put(row);
												row = new ArrayList<Object>(fetchPos.size());
											}
											else
											{
												readBuffer.put(new DataEndMarker());
												return;
											}
										}
									}
								}
								else
								{
									// final ArrayList<Object> newRow = new
									// ArrayList<Object>(neededPos.size());
									// for (final int pos : neededPos)
									// {
									// newRow.add(row.get(pos));
									// }
									if (!forceDone)
									{
										readBuffer.put(row);
										row = new ArrayList<Object>(fetchPos.size());
									}
									else
									{
										readBuffer.put(new DataEndMarker());
										return;
									}
								}
							}

							o = child.next(TableScanOperator.this);
						}
						else
						{
							filter = orderedFilters.get(parents.get(0));

							if (filter != null)
							{
								filter.updateCols2Pos(child.getCols2Pos());
								if (!filter.passes((ArrayList<Object>)o))
								{
									o = child.next(TableScanOperator.this);
									continue;
								}
							}

							final ArrayList<Object> row2 = new ArrayList<Object>(pos2Col.size());
							for (final String col : pos2Col.values())
							{
								row2.add(((ArrayList<Object>)o).get(child.getCols2Pos().get(col)));
							}

							if (!forceDone)
							{
								readBuffer.put(row2);
							}
							else
							{
								readBuffer.put(new DataEndMarker());
								return;
							}
							o = child.next(TableScanOperator.this);
						}
					}

					// System.out.println("TableScanOperator read " + count +
					// " rows based on a RID list");
				}
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				try
				{
					readBuffer.put(e);
				}
				catch (final Exception f)
				{
				}
				return;
			}
		}

		@Override
		public final void run()
		{
			tsoCount.incrementAndGet();

			try
			{
				if (tType == 0)
				{
					rowTableRT();
				}
				else
				{
					colTableRT();
				}
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.debug("", e);
			}

			tsoCount.decrementAndGet();
		}

		private boolean canSatisfy(final HashSet<HashMap<Filter, Filter>> hshm, final Set<HashSet<HashMap<Filter, Filter>>> filters)
		{
			if (filters.contains(hshm))
			{
				// HRDBMSWorker.logger.debug("Skipping page because of exact
				// match");
				pbpeDebug2 = "Non-SMT says exact match";
				return false;
			}

			final long start = System.currentTimeMillis();
			final ArrayList<Filter> ands = new ArrayList<Filter>();
			for (final HashMap<Filter, Filter> hm : hshm)
			{
				if (hm.size() == 1)
				{
					for (final Filter f : hm.keySet())
					{
						if (f.leftIsColumn() && !f.rightIsColumn() && (f.op().equals("E") || f.op().equals("G") || f.op().equals("L") || f.op().equals("GE") || f.op().equals("LE")))
						{
							ands.add(f);
						}
					}
				}
			}

			if (ands.size() == 0)
			{
				pbpeDebug2 = "Non-SMT says returning true because of no ands";
				return true;
			}

			long end = System.currentTimeMillis();
			if (end - start > MAX_PBPE_TIME)
			{
				pbpeDebug2 = "Non-SMT says returning true because out of time";
				return true;
			}

			// if we can prove that one of the filters in "ands" cannot be
			// satisfied, we can return false
			for (final HashSet<HashMap<Filter, Filter>> hshm2 : filters)
			{
				if (hshm2.size() == 1)
				{
					for (final HashMap<Filter, Filter> hm : hshm2)
					{
						if (hm.size() == 1)
						{
							for (final Filter ns : hm.keySet())
							{
								if (ns.leftIsColumn() && !ns.rightIsColumn())
								{
									for (final Filter f : ands)
									{
										if (f.equals(ns))
										{
											// HRDBMSWorker.logger.debug("Skipping
											// page because of exact match of
											// anded predicate");
											pbpeDebug2 = "Non-SMT says returning false because of exact match of anded predicate";
											return false;
										}

										if (ns.leftColumn().equals(f.leftColumn()))
										{
											if (ns.op().equals("G"))
											{
												Object nsVal = ns.rightLiteral();
												if (f.op().equals("E") || f.op().equals("G") || f.op().equals("GE"))
												{
													Object fVal = f.rightLiteral();
													if (!nsVal.getClass().equals(fVal.getClass()))
													{
														if (nsVal instanceof Long)
														{
															nsVal = new Double((Long)nsVal);
														}
														else
														{
															fVal = new Double((Long)fVal);
														}
													}
													if (((Comparable)nsVal).compareTo(fVal) < 0)
													{
														// HRDBMSWorker.logger.debug("Skipping
														// page because of " +
														// ns + ". Search was on
														// " + f);
														pbpeDebug2 = "Non-SMT says returning false because of " + ns + ". Search was on " + f;
														return false;
													}

												}
											}
											else if (ns.op().equals("GE"))
											{
												Object nsVal = ns.rightLiteral();
												if (f.op().equals("E") || f.op().equals("G") || f.op().equals("GE"))
												{
													Object fVal = f.rightLiteral();
													if (!nsVal.getClass().equals(fVal.getClass()))
													{
														if (nsVal instanceof Long)
														{
															nsVal = new Double((Long)nsVal);
														}
														else
														{
															fVal = new Double((Long)fVal);
														}
													}
													if (((Comparable)nsVal).compareTo(fVal) < 1)
													{
														// HRDBMSWorker.logger.debug("Skipping
														// page because of " +
														// ns + ". Search was on
														// " + f);
														pbpeDebug2 = "Non-SMT says returning false because of " + ns + ". Search was on " + f;
														return false;
													}

												}
											}
											else if (ns.op().equals("L"))
											{
												Object nsVal = ns.rightLiteral();
												if (f.op().equals("E") || f.op().equals("L") || f.op().equals("LE"))
												{
													Object fVal = f.rightLiteral();
													if (!nsVal.getClass().equals(fVal.getClass()))
													{
														if (nsVal instanceof Long)
														{
															nsVal = new Double((Long)nsVal);
														}
														else
														{
															fVal = new Double((Long)fVal);
														}
													}
													if (((Comparable)nsVal).compareTo(fVal) > 0)
													{
														// HRDBMSWorker.logger.debug("Skipping
														// page because of " +
														// ns + ". Search was on
														// " + f);
														pbpeDebug2 = "Non-SMT says returning false because of " + ns + ". Search was on " + f;
														return false;
													}

												}
											}
											else if (ns.op().equals("LE"))
											{
												Object nsVal = ns.rightLiteral();
												if (f.op().equals("E") || f.op().equals("L") || f.op().equals("LE"))
												{
													Object fVal = f.rightLiteral();
													if (!nsVal.getClass().equals(fVal.getClass()))
													{
														if (nsVal instanceof Long)
														{
															nsVal = new Double((Long)nsVal);
														}
														else
														{
															fVal = new Double((Long)fVal);
														}
													}
													if (((Comparable)nsVal).compareTo(fVal) > -1)
													{
														// HRDBMSWorker.logger.debug("Skipping
														// page because of " +
														// ns + ". Search was on
														// " + f);
														
														pbpeDebug2 = "Non-SMT says returning false because of " + ns + ". Search was on " + f;
														return false;
													}

												}
											}
										}
									}
								}
							}
						}
					}
				}
			}

			end = System.currentTimeMillis();
			if (end - start > MAX_PBPE_TIME)
			{
				pbpeDebug2 = "Non-SMT says returning true because out of time";
				return true;
			}

			final ArrayList<Filter> ranges = new ArrayList<Filter>();
			for (final HashSet<HashMap<Filter, Filter>> hshm2 : filters)
			{
				if (hshm2.size() == 2)
				{
					Filter l = null;
					Filter g = null;
					String col = null;
					for (final HashMap<Filter, Filter> hm : hshm2)
					{
						if (hm.size() == 1)
						{
							for (final Filter f : hm.keySet())
							{
								if (f.leftIsColumn() && !f.rightIsColumn())
								{
									if (f.op().equals("L") || f.op().equals("LE"))
									{
										if (col == null || f.leftColumn().equals(col))
										{
											l = f;
											col = f.leftColumn();
										}
									}
									else if (f.op().equals("G") || f.op().equals("GE"))
									{
										if (col == null || f.leftColumn().equals(col))
										{
											g = f;
											col = f.leftColumn();
										}
									}
								}
							}
						}
					}

					if (l != null && g != null)
					{
						ranges.add(g);
						ranges.add(l);
					}
				}
			}

			int gIndex = -1;
			String col = null;
			if (ranges.size() > 0)
			{
				int index = 0;
				for (final Filter ns : ranges)
				{
					for (final Filter f : ands)
					{
						if (ns.leftColumn().equals(f.leftColumn()))
						{
							if (ns.op().equals("G"))
							{
								Object nsVal = ns.rightLiteral();
								if (f.op().equals("G") || f.op().equals("GE"))
								{
									Object fVal = f.rightLiteral();
									if (!nsVal.getClass().equals(fVal.getClass()))
									{
										if (nsVal instanceof Long)
										{
											nsVal = new Double((Long)nsVal);
										}
										else
										{
											fVal = new Double((Long)fVal);
										}
									}
									if (((Comparable)nsVal).compareTo(fVal) < 0)
									{
										gIndex = index;
										// HRDBMSWorker.logger.debug("Found G =
										// " + ns);
										pbpeDebug2 = "Non-SMT says returning false because G = " + ns;
										col = f.leftColumn();
									}

								}
							}
							else if (ns.op().equals("GE"))
							{
								Object nsVal = ns.rightLiteral();
								if (f.op().equals("G") || f.op().equals("GE"))
								{
									Object fVal = f.rightLiteral();
									if (!nsVal.getClass().equals(fVal.getClass()))
									{
										if (nsVal instanceof Long)
										{
											nsVal = new Double((Long)nsVal);
										}
										else
										{
											fVal = new Double((Long)fVal);
										}
									}
									if (((Comparable)nsVal).compareTo(fVal) < 1)
									{
										gIndex = index;
										// HRDBMSWorker.logger.debug("Found G =
										// " + ns);
										pbpeDebug2 = "Non-SMT says returning false because G = " + ns;
										col = f.leftColumn();
									}

								}
							}
							else if (ns.op().equals("L"))
							{
								Object nsVal = ns.rightLiteral();
								if (f.op().equals("L") || f.op().equals("LE"))
								{
									Object fVal = f.rightLiteral();
									if (!nsVal.getClass().equals(fVal.getClass()))
									{
										if (nsVal instanceof Long)
										{
											nsVal = new Double((Long)nsVal);
										}
										else
										{
											fVal = new Double((Long)fVal);
										}
									}
									if (((Comparable)nsVal).compareTo(fVal) > 0)
									{
										if (index == gIndex + 1 && f.leftColumn().equals(col))
										{
											// HRDBMSWorker.logger.debug("Found
											// L = " + ns);
											pbpeDebug2 += (" and L = " + ns);
											return false;
										}
									}

								}
							}
							else if (ns.op().equals("LE"))
							{
								Object nsVal = ns.rightLiteral();
								if (f.op().equals("L") || f.op().equals("LE"))
								{
									Object fVal = f.rightLiteral();
									if (!nsVal.getClass().equals(fVal.getClass()))
									{
										if (nsVal instanceof Long)
										{
											nsVal = new Double((Long)nsVal);
										}
										else
										{
											fVal = new Double((Long)fVal);
										}
									}
									if (((Comparable)nsVal).compareTo(fVal) > -1)
									{
										if (index == gIndex + 1 && f.leftColumn().equals(col))
										{
											// HRDBMSWorker.logger.debug("Found
											// L = " + ns);
											pbpeDebug2 += (" and L = " + ns);
											return false;
										}
									}

								}
							}
						}
					}

					index++;
					end = System.currentTimeMillis();
					if (end - start > MAX_PBPE_TIME)
					{
						pbpeDebug2 = "Non-SMT says true because out of time";
						return true;
					}
				}
			}

			pbpeDebug2 = "Non-SMT says true because all methods failed";
			return true;
		}

		private boolean canSatisfySMT(final HashSet<HashMap<Filter, Filter>> hshm, final Set<HashSet<HashMap<Filter, Filter>>> filters, final SolverContext context)
		{
			if (filters.contains(hshm))
			{
				// HRDBMSWorker.logger.debug("Exact match");
				// HRDBMSWorker.logger.debug("Skipping page because of exact
				// match");
				pbpeDebug1 = "SMT has an exact match";
				return false;
			}
			// else
			// {
			// HRDBMSWorker.logger.debug("No exact match");
			// }

			if (containsStringMatching(hshm))
			{
				// HRDBMSWorker.logger.debug("Contains string matching");
				pbpeDebug1 = "SMT says no exact match and contains string matching";
				return true;
			}
			// else
			// {
			// HRDBMSWorker.logger.debug("Does not contain string matching");
			// }

			// long start = System.currentTimeMillis();
			final FormulaManager fmgr = context.getFormulaManager();
			final BooleanFormulaManager bmgr = fmgr.getBooleanFormulaManager();
			final RationalFormulaManager rmgr = fmgr.getRationalFormulaManager();
			final HashMap<String, RationalFormula> vars = new HashMap<String, RationalFormula>();
			final HashSet<String> neededCols = getNeededCols(hshm);

			final ArrayList<BooleanFormula> clauses = new ArrayList<BooleanFormula>();
			for (final HashSet<HashMap<Filter, Filter>> hshm2 : filters)
			{
				if (containsNeededCol(hshm2, neededCols))
				{
					final BooleanFormula b = convertHSHMToBF(hshm2, bmgr, rmgr, vars);
					if (b != null)
					{
						clauses.add(bmgr.not(b));
					}
				}
			}

			clauses.add(convertHSHMToBF(hshm, bmgr, rmgr, vars));
			BooleanFormula b = clauses.get(0);
			int i = 1;
			while (i < clauses.size())
			{
				b = bmgr.and(b, clauses.get(i++));
			}

			// HRDBMSWorker.logger.debug("Formula is " + b);
			try (ProverEnvironment prover = context.newProverEnvironment())
			{
				try
				{
					prover.addConstraint(b);
				}
				catch (final NullPointerException e)
				{
					HRDBMSWorker.logger.debug("Caught NullPointerException, b is " + b);
					HRDBMSWorker.logger.debug("Converting HSHM results in " + convertHSHMToBF(hshm, bmgr, rmgr, vars));
					HRDBMSWorker.logger.debug("Does HSHM contain string matching: " + containsStringMatching(hshm));
					HRDBMSWorker.logger.debug("HSHM is " + hshm);
				}
				while (true)
				{
					try
					{
						final boolean retval = !prover.isUnsat();
						TableScanOperator.SMTSolverCalls.incrementAndGet();
						pbpeDebug1 = "SMT says that " + b + " is " + retval;
						// HRDBMSWorker.logger.debug("Computed return value of "
						// + retval);
						return retval;
					}
					catch (final InterruptedException e)
					{
					}
					catch (final SolverException e)
					{
						HRDBMSWorker.logger.debug("", e);
						pbpeDebug1 = "SMT says exception during solve";
						return true;
					}
				}
			}
		}

		private BitSet computePagesToSkip(final HashSet<HashMap<Filter, Filter>> hshm, final SolverContext context, final String fn, final int stride, final boolean v8, final boolean v9)
		{
			final ThreadMXBean tmxb = ManagementFactory.getThreadMXBean();
			HashMap<HashSet<HashSet<HashMap<Filter, Filter>>>, BitSet> problems = null;
			final HashSet<CNFEntry> entries = new HashSet<CNFEntry>();
			long end1 = 0;
			final long start = tmxb.getCurrentThreadCpuTime();

			if (!v9)
			{
				final HashSet<Integer> hashCodes = getAllHashCodes(hshm);
				// HRDBMSWorker.logger.debug("Hash codes for " + hshm + " are "
				// + hashCodes);
				final MultiHashMap<Integer, CNFEntry> mhm = pbpeCache2.get(fn);
				if (mhm == null)
				{
					final int pbpeVer = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("pbpe_version"));
					final boolean isV6OrHigher = (pbpeVer >= 6);
					BitSet retval = null;
					if (isV6OrHigher)
					{
						retval = new CompressedBitSet();
					}
					else
					{
						retval = new BitSet();
					}

					return retval;
				}
				for (final int hash : hashCodes)
				{
					entries.addAll(mhm.get(hash));
				}

				// HRDBMSWorker.logger.debug("Entries are " + entries);
				// 1 in bit set for a cnf means that cnf is false for that page
				int length = 1;
				for (final CNFEntry entry : entries)
				{
					final BitSet bs = entry.getBitSet();
					synchronized (bs)
					{
						final int temp = bs.length();
						if (temp > length)
						{
							length = temp;
						}
					}
				}

				// HRDBMSWorker.logger.debug("BuildProblems length is " +
				// length);

				problems = buildProblems(entries, stride, length);
				end1 = tmxb.getCurrentThreadCpuTime();
				// HRDBMSWorker.logger.debug("Output of buildProblems() is " +
				// problems);

				if (v8)
				{
					final int numPageSets = ((length - 2) / stride) + 1;
					final int pSize = problems.size();
					if (pSize <= 5 || pSize < (numPageSets / 2))
					{
						final BitSet retval = solveProblems(problems, context, hshm);
						final long end2 = tmxb.getCurrentThreadCpuTime();
						TableScanOperator.figureOutProblemsTime.getAndAdd(end1 - start);
						TableScanOperator.SMTSolveTime.getAndAdd(end2 - end1);
						return retval;
					}

					final BitSet retval = solveProblemsNonSMT(problems, hshm);
					final long end2 = tmxb.getCurrentThreadCpuTime();
					TableScanOperator.figureOutProblemsTime.getAndAdd(end1 - start);
					TableScanOperator.nonSMTSolveTime.getAndAdd(end2 - end1);
					return retval;
				}
			}
			else
			{
				final HashSet<Integer> hashCodes = getAllHashCodes(hshm);
				final HashMap<HashSet<HashSet<HashMap<Filter, Filter>>>, BitSet> temp = problemCache.get(fn);
				if (temp == null)
				{
					final MultiHashMap<Integer, CNFEntry> mhm = pbpeCache2.get(fn);
					if (mhm == null)
					{
						final BitSet retval = new CompressedBitSet();
						return retval;
					}

					for (final int hash : hashCodes)
					{
						entries.addAll(mhm.get(hash));
					}

					// HRDBMSWorker.logger.debug("Entries are " + entries);
					// 1 in bit set for a cnf means that cnf is false for that
					// page
					int length = 1;
					for (final CNFEntry entry : entries)
					{
						final BitSet bs = entry.getBitSet();
						synchronized (bs)
						{
							final int temp2 = bs.length();
							if (temp2 > length)
							{
								length = temp2;
							}
						}
					}

					// HRDBMSWorker.logger.debug("BuildProblems length is " +
					// length);

					problems = buildProblems(entries, stride, length);
				}
				else
				{
					problems = new HashMap<HashSet<HashSet<HashMap<Filter, Filter>>>, BitSet>();
					for (final Map.Entry entry : temp.entrySet())
					{
						HashSet<HashSet<HashMap<Filter, Filter>>> set = (HashSet<HashSet<HashMap<Filter, Filter>>>)entry.getKey();
						set = (HashSet<HashSet<HashMap<Filter, Filter>>>)set.clone();
						final Iterator<HashSet<HashMap<Filter, Filter>>> iter = set.iterator();
						while (iter.hasNext())
						{
							final HashSet<HashMap<Filter, Filter>> hshm2 = iter.next();
							final HashSet<Integer> hashCodes2 = getAllHashCodes(hshm2);
							hashCodes2.retainAll(hashCodes);
							if (hashCodes2.size() == 0)
							{
								iter.remove();
							}
						}

						if (set.size() == 0)
						{
							continue;
						}

						final BitSet bs = problems.get(set);
						if (bs == null)
						{
							problems.put(set, (BitSet)((BitSet)entry.getValue()).clone());
						}
						else
						{
							final BitSet bs2 = (BitSet)entry.getValue();
							bs.or(bs2);
						}
					}

					if (problems.size() == 0)
					{
						end1 = tmxb.getCurrentThreadCpuTime();
						TableScanOperator.figureOutProblemsTime.getAndAdd(end1 - start);
						return new CompressedBitSet();
					}
				}

				end1 = tmxb.getCurrentThreadCpuTime();
			}

			final BitSet retval = solveProblems(problems, context, hshm);
			if (!retval.isEmpty())
			{
				for (final CNFEntry entry : entries)
				{
					entry.incrementUsage();
				}
			}
			final long end2 = tmxb.getCurrentThreadCpuTime();
			TableScanOperator.figureOutProblemsTime.getAndAdd(end1 - start);
			TableScanOperator.SMTSolveTime.getAndAdd(end2 - end1);
			return retval;
		}

		private boolean containsNeededCol(final HashSet<HashMap<Filter, Filter>> hshm, final HashSet<String> needed)
		{
			for (final HashMap<Filter, Filter> hm : hshm)
			{
				for (final Filter f : hm.keySet())
				{
					if (f.leftIsColumn())
					{
						String col = f.leftColumn();
						if (col.contains("."))
						{
							col = col.substring(col.indexOf('.') + 1);
						}

						if (needed.contains(col))
						{
							return true;
						}
					}

					if (f.rightIsColumn())
					{
						String col = f.rightColumn();
						if (col.contains("."))
						{
							col = col.substring(col.indexOf('.') + 1);
						}

						if (needed.contains(col))
						{
							return true;
						}
					}
				}
			}

			return false;
		}

		private boolean containsStringMatching(final HashSet<HashMap<Filter, Filter>> hshm)
		{
			for (final HashMap<Filter, Filter> hm : hshm)
			{
				for (final Filter f : hm.keySet())
				{
					if (f.op().equals("LI") || f.op().equals("NL"))
					{
						return true;
					}
				}
			}

			return false;
		}

		private BooleanFormula convertFToBF(final Filter f, final BooleanFormulaManager bmgr, final RationalFormulaManager rmgr, final HashMap<String, RationalFormula> vars)
		{
			RationalFormula r = null;
			RationalFormula r2 = null;
			if (f.leftIsColumn())
			{
				String col = f.leftColumn();
				if (col.contains("."))
				{
					col = col.substring(col.indexOf('.') + 1);
				}

				r = vars.get(col);
				if (r == null)
				{
					r = rmgr.makeVariable(col);
					vars.put(col, r);
				}
			}
			else
			{
				final Object o = f.leftLiteral();
				if (o instanceof Double)
				{
					r = rmgr.makeNumber((Double)o);
				}
				else if (o instanceof Long)
				{
					r = rmgr.makeNumber((Long)o);
				}
				else if (o instanceof MyDate)
				{
					r = rmgr.makeNumber(((MyDate)o).getTime());
				}
				else
				{
					r = rmgr.makeNumber(stringToNumber((String)o));
				}
			}

			if (f.rightIsColumn())
			{
				String col = f.rightColumn();
				if (col.contains("."))
				{
					col = col.substring(col.indexOf('.') + 1);
				}

				r2 = vars.get(col);
				if (r2 == null)
				{
					r2 = rmgr.makeVariable(col);
					vars.put(col, r2);
				}
			}
			else
			{
				final Object o = f.rightLiteral();
				if (o instanceof Double)
				{
					r2 = rmgr.makeNumber((Double)o);
				}
				else if (o instanceof Long)
				{
					r2 = rmgr.makeNumber((Long)o);
				}
				else if (o instanceof MyDate)
				{
					r2 = rmgr.makeNumber(((MyDate)o).getTime());
				}
				else
				{
					r2 = rmgr.makeNumber(stringToNumber((String)o));
				}
			}

			final String op = f.op();
			if (op.equals("E"))
			{
				return rmgr.equal(r, r2);
			}
			else if (op.equals("NE"))
			{
				return bmgr.not(rmgr.equal(r, r2));
			}
			else if (op.equals("G"))
			{
				return rmgr.greaterThan(r, r2);
			}
			else if (op.equals("GE"))
			{
				return rmgr.greaterOrEquals(r, r2);
			}
			else if (op.equals("L"))
			{
				return rmgr.lessThan(r, r2);
			}
			else
			{
				return rmgr.lessOrEquals(r, r2);
			}
		}

		private BooleanFormula convertHMToBF(final HashMap<Filter, Filter> hm, final BooleanFormulaManager bmgr, final RationalFormulaManager rmgr, final HashMap<String, RationalFormula> vars)
		{
			final ArrayList<BooleanFormula> clauses = new ArrayList<BooleanFormula>();
			for (final Filter f : hm.keySet())
			{
				if (f.op().equals("LI") || f.op().equals("NL"))
				{
					continue;
				}

				clauses.add(convertFToBF(f, bmgr, rmgr, vars));
			}

			if (clauses.size() == 0)
			{
				return null;
			}

			BooleanFormula b = clauses.get(0);
			int i = 1;
			while (i < clauses.size())
			{
				b = bmgr.or(b, clauses.get(i++));
			}

			return b;
		}

		private BooleanFormula convertHSHMToBF(final HashSet<HashMap<Filter, Filter>> hshm, final BooleanFormulaManager bmgr, final RationalFormulaManager rmgr, final HashMap<String, RationalFormula> vars)
		{
			final ArrayList<BooleanFormula> clauses = new ArrayList<BooleanFormula>();
			for (final HashMap<Filter, Filter> hm : hshm)
			{
				final BooleanFormula b = convertHMToBF(hm, bmgr, rmgr, vars);
				if (b != null)
				{
					clauses.add(b);
				}
			}

			if (clauses.size() == 0)
			{
				return null;
			}

			BooleanFormula b = clauses.get(0);
			int i = 1;
			while (i < clauses.size())
			{
				b = bmgr.and(b, clauses.get(i++));
			}

			return b;
		}

		private HashSet<Integer> getAllHashCodes(final HashSet<HashMap<Filter, Filter>> hshm)
		{
			final HashSet<Integer> retval = new HashSet<Integer>();
			for (final HashMap<Filter, Filter> hm : hshm)
			{
				for (final Filter f : hm.keySet())
				{
					if (f.leftIsColumn())
					{
						String col = f.leftColumn();
						if (col.contains("."))
						{
							col = col.substring(col.indexOf('.') + 1);
						}

						retval.add(col.hashCode());
					}

					if (f.rightIsColumn())
					{
						String col = f.rightColumn();
						if (col.contains("."))
						{
							col = col.substring(col.indexOf('.') + 1);
						}

						retval.add(col.hashCode());
					}
				}
			}

			return retval;
		}

		private HashSet<String> getNeededCols(final HashSet<HashMap<Filter, Filter>> hshm)
		{
			final HashSet<String> retval = new HashSet<String>();
			for (final HashMap<Filter, Filter> hm : hshm)
			{
				for (final Filter f : hm.keySet())
				{
					if (f.leftIsColumn())
					{
						String col = f.leftColumn();
						if (col.contains("."))
						{
							col = col.substring(col.indexOf('.') + 1);
						}

						retval.add(col);
					}

					if (f.rightIsColumn())
					{
						String col = f.rightColumn();
						if (col.contains("."))
						{
							col = col.substring(col.indexOf('.') + 1);
						}

						retval.add(col);
					}
				}
			}

			return retval;
		}

		private BitSet solveProblems(final HashMap<HashSet<HashSet<HashMap<Filter, Filter>>>, BitSet> problems, final SolverContext context, final HashSet<HashMap<Filter, Filter>> hshm)
		{
			final int pbpeVer = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("pbpe_version"));
			final boolean isV6OrHigher = (pbpeVer >= 6);
			BitSet retval = null;
			if (isV6OrHigher)
			{
				retval = new CompressedBitSet();
			}
			else
			{
				retval = new BitSet();
			}
			for (final Map.Entry entry : problems.entrySet())
			{
				final HashSet<HashSet<HashMap<Filter, Filter>>> hshshm = (HashSet<HashSet<HashMap<Filter, Filter>>>)entry.getKey();
				if (!canSatisfySMT(hshm, hshshm, context))
				{
					if (isV6OrHigher)
					{
						((CompressedBitSet)retval).or((CompressedBitSet)entry.getValue());
					}
					else
					{
						retval.or((BitSet)entry.getValue());
					}
				}
			}

			return retval;
		}

		private BitSet solveProblemsNonSMT(final HashMap<HashSet<HashSet<HashMap<Filter, Filter>>>, BitSet> problems, final HashSet<HashMap<Filter, Filter>> hshm)
		{
			final BitSet retval = new CompressedBitSet();

			for (final Map.Entry entry : problems.entrySet())
			{
				final HashSet<HashSet<HashMap<Filter, Filter>>> hshshm = (HashSet<HashSet<HashMap<Filter, Filter>>>)entry.getKey();
				if (!canSatisfy(hshm, hshshm))
				{
					((CompressedBitSet)retval).or((CompressedBitSet)entry.getValue());
				}
			}

			return retval;
		}

		private BigDecimal stringToNumber(final String str)
		{
			final StringBuilder s = new StringBuilder();
			s.append("0.");
			int i = 0;
			while (i < str.length())
			{
				final char c = str.charAt(i);
				final int j = c;
				s.append(String.format("%05d", j));
				i++;
			}

			if (s.length() == 2)
			{
				s.append("0");
			}

			return new BigDecimal(s.toString());
		}
	}

	private final class InitThread extends ThreadPoolThread
	{
		private final ArrayList<ReaderThread> reads = new ArrayList<ReaderThread>(ins.size());

		@Override
		public void run()
		{
			final ArrayList<String> dmlTxStrs = new ArrayList<String>();
			final ArrayList<Integer> sorted = new ArrayList(MetaData.getNumDevices());
			int i = 0;
			while (i < MetaData.getNumDevices())
			{
				sorted.add(i++);
			}
			// Collections.sort(sorted);
			synchronized (intraTxLock)
			{
				for (final int device : sorted)
				{
					final String dmlTxStr = Long.toString(tx.number()) + "~" + device + "~" + schema + "." + name;
					AtomicInteger count = sharedDmlTxCounters.get(dmlTxStr);
					if (count == null)
					{
						count = new AtomicInteger(1);
						sharedDmlTxCounters.put(dmlTxStr, count);
						if (ConnectionWorker.dmlTx.putIfAbsent(dmlTxStr, dmlTxStr) != null)
						{
							while (ConnectionWorker.dmlTx.putIfAbsent(dmlTxStr, dmlTxStr) != null)
							{
								LockSupport.parkNanos(1);
							}
						}
					}
					else
					{
						count.getAndIncrement();
					}

					dmlTxStrs.add(dmlTxStr);
				}
			}

			try
			{
				for (final Operator o : children)
				{
					try
					{
						o.start();
					}
					catch (final Exception e)
					{
						HRDBMSWorker.logger.error("", e);
						throw e;
					}
				}

				// HRDBMSWorker.logger.debug("Going to start " + ins.size() +
				// " ReaderThreads for ins for " + TableScanOperator.this);
				if (scanIndex != null)
				{
					final String fn = scanIndex.getFileName();
					for (final int device : devices)
					{
						final String in = MetaData.getDevicePath(device) + fn;
						final ReaderThread read = new ReaderThread(in, scanIndex);
						read.start();
						reads.add(read);
					}
				}
				else
				{
					for (final String in : ins)
					{
						final ReaderThread read = new ReaderThread(in);
						read.start();
						reads.add(read);
					}

					// HRDBMSWorker.logger.debug("Going to start " +
					// randomIns.size() + " ReaderThreads for randomIns for " +
					// TableScanOperator.this);
					for (final String in : randomIns)
					{
						final ReaderThread read = new ReaderThread(in, true);
						read.start();
						reads.add(read);
					}
				}

				for (final ReaderThread read : reads)
				{
					read.join();
				}

				if (optimize)
				{
					readBuffer.put(new DataEndMarker());
				}
				else
				{
					for (final BufferedLinkedBlockingQueue q : readBuffers.values())
					{
						q.put(new DataEndMarker());
					}
				}
			}
			catch (final Exception e)
			{
				synchronized (intraTxLock)
				{
					for (final String dmlTxStr : dmlTxStrs)
					{
						final AtomicInteger count = sharedDmlTxCounters.get(dmlTxStr);
						final int newVal = count.decrementAndGet();
						if (newVal == 0)
						{
							sharedDmlTxCounters.remove(dmlTxStr);
							ConnectionWorker.dmlTx.remove(dmlTxStr);
						}
					}
				}
				HRDBMSWorker.logger.error("", e);
				try
				{
					readBuffer.put(e);
				}
				catch (final Exception f)
				{
				}
				return;
			}

			synchronized (intraTxLock)
			{
				for (final String dmlTxStr : dmlTxStrs)
				{
					final AtomicInteger count = sharedDmlTxCounters.get(dmlTxStr);
					final int newVal = count.decrementAndGet();
					if (newVal == 0)
					{
						sharedDmlTxCounters.remove(dmlTxStr);
						ConnectionWorker.dmlTx.remove(dmlTxStr);
					}
				}
			}
		}
	}

	private static class PBPEThread extends HRDBMSThread
	{
		@Override
		public void run()
		{
			final int sleep = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("pbpe_externalize_interval_s")) * 1000;
			final int pbpeVer = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("pbpe_version"));
			final boolean isV5OrHigher = (pbpeVer >= 5);
			while (true)
			{
				try
				{
					Thread.sleep(sleep);
				}
				catch (final InterruptedException e)
				{
				}

				try
				{
					final ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream("pbpe.dat.new", false));
					final ObjectOutputStream out2 = new ObjectOutputStream(new FileOutputStream("pbpe.stats.new", false));
					if (isV5OrHigher)
					{
						out.writeObject(pbpeCache2);
					}
					else
					{
						out.writeObject(noResults);
						out2.writeObject(noResultCounts);
					}
					out.close();
					out2.close();
					new File("pbpe.dat.new").renameTo(new File("pbpe.dat"));
					if (!isV5OrHigher)
					{
						new File("pbpe.stats.new").renameTo(new File("pbpe.stats"));
					}
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.warn("", e);
				}
			}
		}
	}

	private static class ProblemRebuildThread extends HRDBMSThread
	{
		private final LinkedBlockingQueue<String> q;
		private final HashMap<String, Long> times = new HashMap<String, Long>();

		public ProblemRebuildThread(final LinkedBlockingQueue<String> q)
		{
			this.q = q;
		}

		@Override
		public void run()
		{
			while (true)
			{
				try
				{
					final String msg = q.take();
					final StringTokenizer tokens = new StringTokenizer(msg, ",", false);
					final String fn = tokens.nextToken();
					final String t = tokens.nextToken();
					long time = Long.parseLong(t);
					final Long last = times.get(fn);
					if (last != null && time < last)
					{
						continue;
					}

					// rebuild data for fragment fn
					final HashSet<CNFEntry> entries = new HashSet<CNFEntry>();
					time = System.currentTimeMillis();
					final MultiHashMap<Integer, CNFEntry> mhm = pbpeCache2.get(fn);
					final Set<Integer> hashCodes = mhm.getKeySet();
					for (final int hash : hashCodes)
					{
						entries.addAll(mhm.get(hash));
					}

					int length = 1;
					for (final CNFEntry entry2 : entries)
					{
						final BitSet bs = entry2.getBitSet();
						synchronized (bs)
						{
							final int temp = bs.length();
							if (temp > length)
							{
								length = temp;
							}
						}
					}

					final HashMap<HashSet<HashSet<HashMap<Filter, Filter>>>, BitSet> problems = buildProblems(entries, 1, length);
					problemCache.put(fn, problems);
					times.put(fn, time);
				}
				catch (final InterruptedException e)
				{
					continue;
				}
			}
		}
	}
}
