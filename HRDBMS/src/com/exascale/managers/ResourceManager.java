package com.exascale.managers;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.StringWriter;
import java.io.Writer;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.Field;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import com.exascale.filesystem.CompressedFileChannel;
import com.exascale.misc.HParms;
import com.exascale.optimizer.AntiJoinOperator;
import com.exascale.optimizer.NestedLoopJoinOperator;
import com.exascale.optimizer.NetworkSendOperator;
import com.exascale.optimizer.Operator;
import com.exascale.optimizer.SemiJoinOperator;
import com.exascale.optimizer.TableScanOperator;
import com.exascale.tables.Transaction;
import com.exascale.threads.HRDBMSThread;
import com.exascale.threads.ThreadPoolThread;

public final class ResourceManager extends HRDBMSThread
{
	private static int SLEEP_TIME;
	private static int LOW_PERCENT_FREE;
	private static int CRITICAL_PERCENT_FREE = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("critical_mem_percent"));
	public static int QUEUE_SIZE;
	public static int CUDA_SIZE;
	public static ArrayList<String> TEMP_DIRS;
	private static final AtomicLong idGen = new AtomicLong(0);
	private static HashMap<Long, String> creations = new HashMap<Long, String>();
	private static boolean PROFILE;
	private static boolean DEADLOCK_DETECT;
	public static boolean GPU;
	public static final int cpus;
	public static final ExecutorService pool;
	public static final AtomicInteger objID = new AtomicInteger(0);
	public static final long maxMemory;
	public static volatile AtomicInteger NO_OFFLOAD = new AtomicInteger(0);
	private static long GC_TIME;
	private static Charset cs = StandardCharsets.UTF_8;
	private static sun.misc.Unsafe unsafe;
	private static long offset;
	private static long lastSystemGC = 0;
	public static volatile boolean panic = false;
	private static IdentityHashMap<Operator, Operator> ops = new IdentityHashMap<Operator, Operator>();
	public static int MAX_FCS;

	static
	{
		try
		{
			final Field f = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
			f.setAccessible(true);
			unsafe = (sun.misc.Unsafe)f.get(null);
			final Field fieldToUpdate = String.class.getDeclaredField("value");
			// get unsafe offset to this field
			offset = unsafe.objectFieldOffset(fieldToUpdate);
			MAX_FCS = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("num_fcs_per_cfc"));
		}
		catch (final Exception e)
		{
			unsafe = null;
		}
		final HParms hparms = HRDBMSWorker.getHParms();
		LOW_PERCENT_FREE = Integer.parseInt(hparms.getProperty("low_mem_percent")); // 30
		SLEEP_TIME = Integer.parseInt(hparms.getProperty("rm_sleep_time_ms")); // 10000
		PROFILE = (hparms.getProperty("profile")).equals("true");
		DEADLOCK_DETECT = (hparms.getProperty("detect_thread_deadlocks")).equals("true");
		QUEUE_SIZE = Integer.parseInt(hparms.getProperty("queue_size")); // 2500000
		CUDA_SIZE = Integer.parseInt(hparms.getProperty("cuda_batch_size")); // 30720
		GPU = (hparms.getProperty("gpu_offload")).equals("true");
		cpus = Runtime.getRuntime().availableProcessors();
		pool = Executors.newCachedThreadPool();
		maxMemory = Runtime.getRuntime().maxMemory();
		GC_TIME = 30000;
		if (GPU)
		{
			HRDBMSWorker.logger.debug("Going to load CUDA code");
			try
			{
				System.loadLibrary("extend_kernel");
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				System.exit(1);
			}

			HRDBMSWorker.logger.debug("CUDA code loaded");
		}
	}

	private final CharsetEncoder ce = cs.newEncoder();

	public ResourceManager()
	{
		HRDBMSWorker.logger.info("Starting initialization of the Resource Manager.");
		this.setWait(true);
		this.description = "Resource Manager";
		setDirs(HRDBMSWorker.getHParms().getProperty("temp_directories"));
		for (final String temp : TEMP_DIRS)
		{
			final File dir = new File(temp);
			for (final String file : dir.list())
			{
				if (file.endsWith("tmp"))
				{
					new File(dir, file).delete();
				}
			}
		}
	}

	public static void checkOpenFiles() throws Exception
	{
		ProcessBuilder crunchifyProcessBuilder = null;

		crunchifyProcessBuilder = new ProcessBuilder("/bin/bash", "-c", "bc -l  <<< \"`cat /proc/sys/fs/file-nr | cut -f1` - `cat /proc/sys/fs/file-nr | cut -f2`\"");
		crunchifyProcessBuilder.redirectErrorStream(true);
		Writer crunchifyWriter = null;
		try
		{
			Process process = crunchifyProcessBuilder.start();
			if (true)
			{
				InputStream crunchifyStream = process.getInputStream();

				if (crunchifyStream != null)
				{
					crunchifyWriter = new StringWriter();

					char[] crunchifyBuffer = new char[2048];
					try
					{
						Reader crunchifyReader = new BufferedReader(new InputStreamReader(crunchifyStream, StandardCharsets.UTF_8));
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
					crunchifyWriter.toString();
					crunchifyStream.close();
				}
			}
		}
		catch (Exception e)
		{
			panic = true;
		}
		if (crunchifyWriter == null)
		{
			panic = true;
		}

		if (panic)
		{
			panic = false;
			long maxOpen = Long.parseLong(HRDBMSWorker.getHParms().getProperty("max_open_files"));
			long openFiles = maxOpen;
			double factor = 0;
			closeSomeFiles(openFiles, maxOpen, factor);
		}
		else
		{
			long openFiles = Integer.parseInt(crunchifyWriter.toString().trim());
			HRDBMSWorker.logger.debug("Open files - " + openFiles);
			long maxOpen = Long.parseLong(HRDBMSWorker.getHParms().getProperty("max_open_files"));
			double factor = 0.5;
			closeSomeFiles(openFiles, maxOpen, factor);
		}

		for (CompressedFileChannel cfc : FileManager.openFiles.values())
		{
			cfc.accesses.set(0);
			cfc.didRead3.set(false);
			cfc.aoOK.set(true);
		}
	}

	public static boolean criticalMem()
	{
		return ((Runtime.getRuntime().freeMemory()) * 100.0) / (maxMemory * 1.0) < CRITICAL_PERCENT_FREE;
	}

	public static void deregisterOperator(Operator op)
	{
		synchronized (ops)
		{
			ops.remove(op);
		}
	}

	public static boolean display(Operator op, int indent)
	{
		boolean unpin = true;
		
		if (op instanceof TableScanOperator)
		{
			unpin = op.receivedDEM();
		}
		
		String line = "";
		int i = 0;
		while (i < indent)
		{
			line += " ";
			i++;
		}

		line += op;
		line += " : Received " + op.numRecsReceived() + " records. Received DataEndMarker? " + op.receivedDEM();
		HRDBMSWorker.logger.debug(line);

		if (( op instanceof SemiJoinOperator) || (op instanceof AntiJoinOperator) || (op instanceof NestedLoopJoinOperator) || (op.children() != null && op.children().size() > 0 && !(op.children().get(0) instanceof NetworkSendOperator)))
		{
			line = "";
			i = 0;
			while (i < indent)
			{
				line += " ";
				i++;
			}

			line += "(";
			HRDBMSWorker.logger.debug(line);

			if (op instanceof SemiJoinOperator && ((SemiJoinOperator)op).dynamicOp != null)
			{
				if (!display(((SemiJoinOperator)op).dynamicOp, indent + 3))
				{
					unpin = false;
				}
			}
			else if (op instanceof AntiJoinOperator && ((AntiJoinOperator)op).dynamicOp != null)
			{
				if (!display(((AntiJoinOperator)op).dynamicOp, indent + 3))
				{
					unpin = false;
				}
			}
			else if (op instanceof NestedLoopJoinOperator && ((NestedLoopJoinOperator)op).dynamicOp != null)
			{
				if (!display(((NestedLoopJoinOperator)op).dynamicOp, indent + 3))
				{
					unpin = false;
				}
			}
			else
			{
				for (Operator child : op.children())
				{
					if (!display(child, indent + 3))
					{
						unpin = false;
					}
				}
			}

			line = "";
			i = 0;
			while (i < indent)
			{
				line += " ";
				i++;
			}

			line += ")";
			HRDBMSWorker.logger.debug(line);
		}
		
		return unpin;
	}

	public static boolean lowMem()
	{
		return ((Runtime.getRuntime().freeMemory()) * 100.0) / (maxMemory * 1.0) < LOW_PERCENT_FREE;
	}

	public static void registerOperator(Operator op)
	{
		synchronized (ops)
		{
			ops.put(op, op);
		}
	}

	private static void closeSomeFiles(long numOpen, long maxOpen, double factor)
	{
		long totalAccesses = 0;
		long totalFiles = 0;
		HashMap<CompressedFileChannel, Long> cfc2Open = new HashMap<CompressedFileChannel, Long>();
		for (CompressedFileChannel cfc : FileManager.openFiles.values())
		{
			long get = cfc.accesses.get();
			if (get == 0)
			{
				get = 1;
			}
			cfc2Open.put(cfc, get);
			totalAccesses += get;
			totalFiles += cfc.fcs_size.get();
		}

		long otherFiles = numOpen - totalFiles;
		HRDBMSWorker.logger.debug("Open files - " + numOpen + "\tOther files - " + otherFiles);
		if (otherFiles < 0)
		{
			otherFiles = 0;
		}
		long target = (long)(maxOpen * factor);
		target -= otherFiles;
		if (target < 1)
		{
			target = 1;
		}

		for (CompressedFileChannel cfc : cfc2Open.keySet())
		{
			Long get = cfc2Open.get(cfc);
			double pct = get * 1.0 / totalAccesses;
			int num = (int)(pct * target);
			if (num == 0)
			{
				num = 1;
			}

			int realNum = num;

			if (num > MAX_FCS)
			{
				num = MAX_FCS;
			}

			cfc.MAX_FCS = num;
			int current = cfc.fcs_size.get();
			if (current > num && numOpen > maxOpen * factor)
			{
				// HRDBMSWorker.logger.debug("Current = " + current +
				// "\tTarget = " + num);
				if (cfc.trimInProgress.compareAndSet(false, true))
				{
					cfc.new TrimFCSThread().start();
					// HRDBMSWorker.logger.debug("Starting trim thread");
				}
				else
				{
					// HRDBMSWorker.logger.debug("Unable to start trim thread");
				}
			}
			else if (cfc.aoOK.get() && current + MAX_FCS < realNum && cfc.didRead3.get() && !cfc.fn.endsWith("indx"))
			{
				if (cfc.oaInProgress.compareAndSet(false, true))
				{
					cfc.new OpenAheadThread().start();
					numOpen += (MAX_FCS + 500);
				}
			}
			else if (current + MAX_FCS < realNum && cfc.aoBlocks.size() > 0)
			{
				if (cfc.oaInProgress.compareAndSet(false, true))
				{
					cfc.new OpenAheadThread(true).start();
					numOpen += (MAX_FCS + 500);
				}
			}
		}
	}

	private static void displayQueryProgress()
	{
		boolean unpin = true;
		if (HRDBMSWorker.type != HRDBMSWorker.TYPE_WORKER)
		{
			unpin = false;
		}
		
		synchronized (ops)
		{
			for (Operator op : ops.keySet())
			{
				try
				{
					if (!display(op, 0))
					{
						unpin = false;
					}
				}
				catch (Exception e)
				{
					HRDBMSWorker.logger.debug("", e);
				}
			}
		}
		
		/*
		if (unpin)
		{
			for (SubBufferManager sbm : BufferManager.managers)
			{
				sbm.lock.lock();
			}
			
			try
			{
				BufferManager.unpinAll();
			}
			catch(Exception e)
			{
				HRDBMSWorker.logger.debug("", e);
			}
			
			for (SubBufferManager sbm : BufferManager.managers)
			{
				sbm.lock.unlock();
			}
		}
		*/
	}

	private static void setDirs(String list)
	{
		StringTokenizer tokens = new StringTokenizer(list, ",", false);
		while (tokens.hasMoreTokens())
		{
			tokens.nextToken();
		}
		TEMP_DIRS = new ArrayList<String>();
		tokens = new StringTokenizer(list, ",", false);
		while (tokens.hasMoreTokens())
		{
			String dir = tokens.nextToken();
			if (!dir.endsWith("/"))
			{
				dir += "/";
			}
			TEMP_DIRS.add(dir);
		}
	}

	@Override
	public void run()
	{
		HRDBMSWorker.logger.info("Resource Manager initialization complete.");
		if (PROFILE)
		{
			new ProfileThread().start();
		}
		new MonitorThread().start();
		// new GCThread().start();
		if (DEADLOCK_DETECT)
		{
			new DeadlockThread().start();
		}
		// while (true)
		// {
		// System.out.println(((Runtime.getRuntime().freeMemory() +
		// Runtime.getRuntime().maxMemory() -
		// Runtime.getRuntime().totalMemory()) * 100.0) /
		// (Runtime.getRuntime().maxMemory() * 1.0) + "% free");
		// if (highMem() && hasBeenLowMem)
		// {
		// handleHighMem();
		// hasBeenLowMem = false;
		// }
		// if (lowMem())
		// {
		// System.gc();
		// if (lowMem())
		// {
		// lowMem = true;
		// handleLowMem();
		// lowMem = false;
		// }
		// }

		// try
		// {
		// Thread.sleep(SLEEP_TIME);
		// }
		// catch (final Exception e)
		// {
		// }
		// }
	}

	private static final class DeadlockThread extends ThreadPoolThread
	{
		@Override
		public void run()
		{
			final ThreadMXBean bean = ManagementFactory.getThreadMXBean();
			while (true)
			{
				final long[] threadIds = bean.findDeadlockedThreads(); // Returns
				// null
				// if no
				// threads
				// are
				// deadlocked.

				if (threadIds != null)
				{
					final ThreadInfo[] infos = bean.getThreadInfo(threadIds);

					for (final ThreadInfo info : infos)
					{
						final StackTraceElement[] stack = info.getStackTrace();
						for (final StackTraceElement trace : stack)
						{
							HRDBMSWorker.logger.debug(trace);
						}
					}
				}

				try
				{
					Thread.sleep(SLEEP_TIME);
				}
				catch (final Exception e)
				{
				}
			}
		}
	}

	private static final class MonitorThread extends ThreadPoolThread
	{
		@Override
		public void run()
		{
			// long last = System.currentTimeMillis();
			// HashMap<GarbageCollectorMXBean, Long> map = new
			// HashMap<GarbageCollectorMXBean, Long>();
			int i = 0;
			while (true)
			{
				// long temp = System.currentTimeMillis();
				// long elapsed = temp - last;
				// List<GarbageCollectorMXBean> list =
				// ManagementFactory.getGarbageCollectorMXBeans();
				// long sum = 0;
				// for (GarbageCollectorMXBean bean : list)
				// {
				// long time = bean.getCollectionTime();
				// Long prev = map.get(bean);
				// if (prev == null)
				// {
				// map.put(bean, time);
				// HRDBMSWorker.logger.debug("GC bean was not found");
				// }
				// else
				// {
				// long gcTime = time - prev;
				// sum += gcTime;
				// map.put(bean, time);
				// }
				// }
				//
				// double pct = (sum * 1.0) / (elapsed * 1.0);
				// if (pct > 0.3 && LOW_PERCENT_FREE != 70)
				// {
				// LOW_PERCENT_FREE = 70;
				// HRDBMSWorker.logger.debug("LOW_PERCENT_FREE now 70");
				// }
				// else if (pct < 0.1 && LOW_PERCENT_FREE >
				// ORIG_LOW_PERCENT_FREE)
				// {
				// LOW_PERCENT_FREE = ORIG_LOW_PERCENT_FREE;
				// HRDBMSWorker.logger.debug("LOW_PERCENT_FREE now " +
				// LOW_PERCENT_FREE);
				// }
				// last = temp;
				//
				HRDBMSWorker.logger.debug(((Runtime.getRuntime().freeMemory()) * 100.0) / (maxMemory * 1.0) + "% free - skipped " + TableScanOperator.skippedPages.get() + " pages");
				try
				{
					checkOpenFiles();
				}
				catch (Exception e)
				{
					HRDBMSWorker.logger.debug("", e);
				}
				// for (SubBufferManager sbm : BufferManager.managers)
				// {
				// HRDBMSWorker.logger.debug("Owner is " +
				// sbm.lock.whoIsOwner());
				// HRDBMSWorker.logger.debug("Owners is " +
				// sbm.lock.getOwners());
				// }
				// HRDBMSWorker.logger.debug("GC time was " + (pct * 100.0) +
				// "%");
				if (i % 12 == 0)
				{
					displayQueryProgress();
				}

				i++;
				try
				{
					Thread.sleep(SLEEP_TIME);
				}
				catch (final Exception e)
				{
				}
			}
		}
	}

	private static final class ProfileThread extends ThreadPoolThread
	{
		private final HashMap<CodePosition, CodePosition> counts = new HashMap<CodePosition, CodePosition>();

		long samples = 0;

		@Override
		public void run()
		{
			while (true)
			{
				final Map<Thread, StackTraceElement[]> map = Thread.getAllStackTraces();
				for (final Map.Entry entry : map.entrySet())
				{

					final StackTraceElement[] trace = (StackTraceElement[])entry.getValue();

					final int length = trace.length;
					int i = 0;
					while (i < length)
					{
						final String file = trace[i].getClassName();
						final int lineNum = trace[i].getLineNumber();
						final String method = trace[i].getMethodName();
						if (method.equals("next") || method.equals("put") || method.equals("take") || method.equals("join"))
						{
							break;
						}
						final CodePosition cp = new CodePosition(file, lineNum, method);
						final CodePosition cp2 = counts.get(cp);
						if (cp2 == null)
						{
							cp.count++;
							counts.put(cp, cp);
						}
						else
						{
							cp2.count++;
						}

						i++;
					}
				}

				samples++;

				final TreeSet<CodePosition> set = new TreeSet<CodePosition>();
				for (final CodePosition cp : counts.values())
				{
					if (cp.count * 100 / samples >= 1)
					{
						set.add(cp);
					}
				}
				try
				{
					final PrintWriter out = new PrintWriter(new File("./java.hprof.txt.new"));
					for (final CodePosition cp : set)
					{
						out.println(cp.file + "." + cp.method + ":" + cp.lineNum + " " + (cp.count * 100 / samples) + "%");
					}
					out.close();
					new File("./java.hprof.txt.new").renameTo(new File("./java.hprof.txt"));
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.error("", e);
					return;
				}
			}
		}

		private static final class CodePosition implements Comparable
		{
			String file;
			String method;
			int lineNum;
			long count = 0;

			public CodePosition(String file, int lineNum, String method)
			{
				this.file = file;
				this.lineNum = lineNum;
				this.method = method;
			}

			@Override
			public int compareTo(Object rhs)
			{
				final CodePosition cp = (CodePosition)rhs;
				if (count < cp.count)
				{
					return 1;
				}

				if (count > cp.count)
				{
					return -1;
				}

				return 0;
			}

			@Override
			public boolean equals(Object rhs)
			{
				if (rhs == null)
				{
					return false;
				}
				final CodePosition r = (CodePosition)rhs;
				if (file.equals(r.file) && lineNum == r.lineNum)
				{
					return true;
				}

				return false;
			}

			@Override
			public int hashCode()
			{
				return file.hashCode() + lineNum;
			}
		}
	}
}
