package com.exascale.managers;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.StampedLock;
import com.exascale.misc.DataEndMarker;
import com.exascale.misc.HParms;
import com.exascale.misc.InternalConcurrentHashMap;
import com.exascale.misc.InternalConcurrentHashMap.EntrySetView;
import com.exascale.misc.InternalConcurrentHashMap.MapEntry;
import com.exascale.misc.LongPrimitiveConcurrentHashMap;
import com.exascale.misc.LongPrimitiveConcurrentHashMap.EntryIterator;
import com.exascale.misc.MurmurHash;
import com.exascale.misc.MyDate;
import com.exascale.misc.ReverseConcurrentHashMap;
import com.exascale.optimizer.TableScanOperator;
import com.exascale.threads.HRDBMSThread;
import com.exascale.threads.ThreadPoolThread;

public final class ResourceManager extends HRDBMSThread
{
	private static int SLEEP_TIME;
	private static int LOW_PERCENT_FREE;
	private static int CRITICAL_PERCENT_FREE = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("critical_mem_percent"));
	public static int QUEUE_SIZE;
	public static int CUDA_SIZE;
	private static volatile boolean lowMem = false;
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

	public static boolean lowMem()
	{
		return ((Runtime.getRuntime().freeMemory()) * 100.0) / (maxMemory * 1.0) < LOW_PERCENT_FREE;
	}
	
	public static boolean criticalMem()
	{
		return ((Runtime.getRuntime().freeMemory()) * 100.0) / (maxMemory * 1.0) < CRITICAL_PERCENT_FREE;
	}

	private static void handleLowMem()
	{
		if (lowMem())
		{
			long time = System.currentTimeMillis();
			if (time - lastSystemGC > GC_TIME)
			{
				System.gc();
				lastSystemGC = System.currentTimeMillis();
				GC_TIME = (lastSystemGC - time) * 5;
			}
		}
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
		//while (true)
		//{
			// System.out.println(((Runtime.getRuntime().freeMemory() +
			// Runtime.getRuntime().maxMemory() -
			// Runtime.getRuntime().totalMemory()) * 100.0) /
			// (Runtime.getRuntime().maxMemory() * 1.0) + "% free");
			// if (highMem() && hasBeenLowMem)
			// {
			// handleHighMem();
			// hasBeenLowMem = false;
			// }
			//if (lowMem())
			//{
				// System.gc();
				// if (lowMem())
				// {
				//lowMem = true;
			//	handleLowMem();
				//lowMem = false;
				// }
			//}

			//try
			//{
			//	Thread.sleep(SLEEP_TIME);
			//}
			//catch (final Exception e)
			//{
			//}
		//}
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
				// HRDBMSWorker.logger.debug("GC time was " + (pct * 100.0) +
				// "%");
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
