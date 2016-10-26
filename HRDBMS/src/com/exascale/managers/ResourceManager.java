package com.exascale.managers;

import java.io.File;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.Field;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Random;
import java.util.StringTokenizer;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import com.exascale.misc.HParms;
import com.exascale.optimizer.AntiJoinOperator;
import com.exascale.optimizer.MetaData;
import com.exascale.optimizer.NestedLoopJoinOperator;
import com.exascale.optimizer.NetworkSendOperator;
import com.exascale.optimizer.Operator;
import com.exascale.optimizer.SemiJoinOperator;
import com.exascale.optimizer.TableScanOperator;
import com.exascale.threads.HRDBMSThread;
import com.exascale.threads.ThreadPoolThread;
import java.util.concurrent.ThreadPoolExecutor;

public final class ResourceManager extends HRDBMSThread
{
	private static int SLEEP_TIME;
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
	private static Charset cs = StandardCharsets.UTF_8;
	private static sun.misc.Unsafe unsafe;
	private static long offset;
	public static volatile boolean panic = false;
	private static ConcurrentHashMap<Operator, Operator> ops = new ConcurrentHashMap<Operator, Operator>();
	private static HashMap<Integer, HashSet<Integer>> links = new HashMap<Integer, HashSet<Integer>>();
	private static HashMap<Link, AtomicInteger> util = new HashMap<Link, AtomicInteger>();
	private static HashSet<Integer> goodDistances = new HashSet<Integer>();
	private static HashSet<Integer> okDistances = new HashSet<Integer>();
	private static Random random = new Random();
	private static int nodes;
	private static int nMax;
	public static volatile int MAX_HOPS = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("initial_max_hops"));  
	private static ConcurrentHashMap<Link, ArrayList<Integer>> routes = new ConcurrentHashMap<Link, ArrayList<Integer>>();
	private final CharsetEncoder ce = cs.newEncoder();

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
		SLEEP_TIME = Integer.parseInt(hparms.getProperty("rm_sleep_time_ms")); // 10000
		PROFILE = (hparms.getProperty("profile")).equals("true");
		DEADLOCK_DETECT = (hparms.getProperty("detect_thread_deadlocks")).equals("true");
		QUEUE_SIZE = Integer.parseInt(hparms.getProperty("queue_size")); // 2500000
		CUDA_SIZE = Integer.parseInt(hparms.getProperty("cuda_batch_size")); // 30720
		GPU = (hparms.getProperty("gpu_offload")).equals("true");
		cpus = Runtime.getRuntime().availableProcessors();
		pool = new ThreadPoolExecutor(0, Integer.MAX_VALUE,
                60, TimeUnit.SECONDS,
                new SynchronousQueue<Runnable>());
		maxMemory = Runtime.getRuntime().maxMemory();
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
	
	public static ArrayList<Integer> getRoute(int from, int to)
	{
		Link link = new Link(from, to);
		ArrayList<Integer> retval = (ArrayList<Integer>)routes.get(link).clone();
		if (retval != null)
		{
			return retval;
		}
		
		retval = computeRoute(from, to);
		routes.putIfAbsent(link, retval);
		return retval;
	}
	
	private static ArrayList<Integer> computeRoute(int from, int to)
	{
		ArrayList<Integer> retval = new ArrayList<Integer>();
		int current = from;
		while (current != to)
		{
			HashSet<Integer> receive = links.get(current);
			int temp = computeSendTo(to, receive);
			retval.add(temp);
			//used current -> temp
			current = temp;
		}
		
		return retval;
	}
	
	public static ArrayList<Integer> getNetworkTargetsForNode(int source)
	{
		ArrayList<Integer> retval = new ArrayList<Integer>();
		retval.add(source);
		HashSet<Integer> targets = links.get(source);
		retval.addAll(targets);
		return retval;
	}
	
	public static ArrayList<Integer> getAlternateMiddlemen(int source, int target, int primary)
	{
		if (primary == target)
		{
			return new ArrayList<Integer>();
		}
		
		HashSet<Integer> middlemen = (HashSet<Integer>)links.get(source).clone();
		HashSet<Integer> middlemen2 = links.get(target);
		middlemen.retainAll(middlemen2);
		middlemen.remove(primary);
		return new ArrayList<Integer>(middlemen);
	}
	
	private static class Link
	{
		private int a;
		private int b;
		
		public Link(int a, int b)
		{
			this.a = a;
			this.b = b;
		}
		
		public boolean equals(Object r)
		{
			Link rhs = (Link)r;
			return a == rhs.a && b == rhs.b;
		}
		
		public int hashCode()
		{
			return (a << 15) + b;
		}
		
		public String toString()
		{
			return "(" + a + ", " + b + ")";
		}
	}

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
		
		nodes = MetaData.numWorkerNodes;
		nMax = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("max_neighbor_nodes"));
		
		//build broadcast network
		int i = 0;
		while (i < nodes)
		{
			HashSet<Integer> receive = links.get(i);
			if (receive == null)
			{
				receive = new HashSet<Integer>();
				links.put(i, receive);
			}
			
			int k = 0;
			double x = Math.pow(nodes * 1.0, (2.0 / (nMax * 1.0)));
			while (Math.pow(x, k) <= (nodes))
			{
				int offset = (int)Math.round(Math.pow(x, k));
				offset = offset % nodes;
				
				if (offset > (nodes / 2))
				{
					goodDistances.add(nodes - offset);
				}
				else
				{
					goodDistances.add(offset);
				}
				int target = i + offset;
				target = target % nodes;
				if (target != i)
				{
					receive.add(target);
					HashSet<Integer> receive2 = links.get(target);
					if (receive2 == null)
					{
						receive2 = new HashSet<Integer>();
						links.put(target, receive2);
					}
					
					receive2.add(i);
				}
				
				k++;
			}
			
			i++;
		}
		
		for (int distance1 : goodDistances)
		{
			for (int distance2 : goodDistances)
			{
				int value = distance1 + distance2;
				value = value % nodes;
				if (value > (nodes / 2))
				{
					value = nodes - value;
					okDistances.add(value);
				}
				else
				{
					okDistances.add(value);
				}
			}
		}
	}

	public static boolean criticalMem()
	{
		return ((Runtime.getRuntime().freeMemory()) * 100.0) / (maxMemory * 1.0) < CRITICAL_PERCENT_FREE;
	}

	public static void deregisterOperator(Operator op)
	{
		ops.remove(op);
	}

	public static boolean display(Operator op, int indent)
	{
		try
		{
			boolean unpin = true;

			if (op instanceof TableScanOperator)
			{
				unpin = op.receivedDEM();
			}

			StringBuilder line = new StringBuilder();
			int i = 0;
			while (i < indent)
			{
				line.append(" ");
				i++;
			}

			line.append(op);
			line.append(" : Received " + op.numRecsReceived() + " records. Received DataEndMarker? " + op.receivedDEM());
			HRDBMSWorker.logger.debug(line.toString());

			if ((op instanceof SemiJoinOperator) || (op instanceof AntiJoinOperator) || (op instanceof NestedLoopJoinOperator) || (op.children() != null && op.children().size() > 0 && !(op.children().get(0) instanceof NetworkSendOperator)))
			{
				line = new StringBuilder();
				i = 0;
				while (i < indent)
				{
					line.append(" ");
					i++;
				}

				line.append("(");
				HRDBMSWorker.logger.debug(line.toString());

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

				line = new StringBuilder();
				i = 0;
				while (i < indent)
				{
					line.append(" ");
					i++;
				}

				line.append(")");
				HRDBMSWorker.logger.debug(line.toString());
			}

			return unpin;
		}
		catch (Exception e)
		{
			return false;
		}
	}

	public static void registerOperator(Operator op)
	{
		ops.put(op, op);
	}

	private static void displayQueryProgress()
	{
		if (HRDBMSWorker.type != HRDBMSWorker.TYPE_WORKER)
		{
		}

		synchronized (ops)
		{
			for (Operator op : ops.keySet())
			{
				try
				{
					if (!display(op, 0))
					{
					}
				}
				catch (Exception e)
				{
					HRDBMSWorker.logger.debug("", e);
				}
			}
		}

		/*
		 * if (unpin) { for (SubBufferManager sbm : BufferManager.managers) {
		 * sbm.lock.lock(); }
		 *
		 * try { BufferManager.unpinAll(); } catch(Exception e) {
		 * HRDBMSWorker.logger.debug("", e); }
		 *
		 * for (SubBufferManager sbm : BufferManager.managers) {
		 * sbm.lock.unlock(); } }
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
		
			//System.out.println("Calculating hops");
			AtomicInteger min = new AtomicInteger(Integer.MAX_VALUE);
			AtomicInteger max = new AtomicInteger(Integer.MIN_VALUE);
			AtomicInteger count = new AtomicInteger(0);
			AtomicLong total = new AtomicLong(0);
			ArrayList<HopThread> threads = new ArrayList<HopThread>();
			threads.add(new HopThread(0, nodes / 3, min, max, count, total));
			threads.add(new HopThread(nodes / 3, 2 * nodes / 3, min, max, count, total));
			threads.add(new HopThread(2 * nodes / 3, nodes, min, max, count, total));
			
			for (HopThread thread : threads)
			{
				thread.start();
			}
			
			for (HopThread thread : threads)
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
			}
			
			//System.out.println("Min hops = " + min.get() + ", max hops = " + max.get() + ", average hops = " + (total.get() * 1.0 / count.get()));
			MAX_HOPS = max.get();
			HRDBMSWorker.logger.info("Max hops is " + MAX_HOPS);
	}
	
	private static class HopThread extends Thread
	{
		private int start;
		private int stop;
		AtomicInteger min;
		AtomicInteger max;
		AtomicInteger count;
		AtomicLong total;
		
		public HopThread(int start, int stop, AtomicInteger min, AtomicInteger max, AtomicInteger count, AtomicLong total)
		{
			this.start = start;
			this.stop = stop;
			this.min = min;
			this.max = max;
			this.count = count;
			this.total = total;
		}
		
		public void run()
		{
			int i = start;
			while (i < stop)
			{
				int j = 0;
				while (j < nodes)
				{
					if (i != j)
					{
						//compute hops from i to j
						
						int current = i;
						int hops = 0;
						ArrayList<Integer> route = new ArrayList<Integer>();
						while (current != j)
						{
							HashSet<Integer> receive = links.get(current);
							int temp = computeSendTo(j, receive);
							route.add(temp);
							hops++;
							//used current -> temp
							synchronized(util)
							{
								AtomicInteger ai = util.get(new Link(current, temp));
								if (ai == null)
								{
									ai = new AtomicInteger(0);
									util.put(new Link(current, temp), ai);
								}
							
								ai.getAndIncrement();
							}
							current = temp;
						}
						
						routes.putIfAbsent(new Link(i, j), route);
						
						//count hops and update util
						//int hops = aStarSearch(i, j);
						
						synchronized(min)
						{
							if (hops < min.get())
							{
								min.set(hops);
							}
						}
						
						synchronized(max)
						{
							if (hops > max.get())
							{
								max.set(hops);
							}
						}
						
						count.getAndIncrement();
						total.addAndGet(hops);
					}
					
					j++;
				}
				
				i++;
				
				//if (i % 100 == 0)
				//{
				//	System.out.println(i);
				//}
			}
		}
	}
	
	private static int computeSendTo(int target, HashSet<Integer> list)
	{
		int minDifference = Integer.MAX_VALUE;
		int retval = -1;
		ArrayList<Integer> goldenOptions = new ArrayList<Integer>();
		ArrayList<Integer> silverOptions = new ArrayList<Integer>();
		
		for (int option : list)
		{
			int distance = Math.abs(target - option);
			if (distance > (nodes / 2))
			{
				distance = nodes - distance;
			}
			
			if (distance < minDifference)
			{
				minDifference = distance;
				retval = option;
				
				if (distance == 0)
				{
					return option;
				}
			}
			
			if (goodDistances.contains(distance))
			{
				goldenOptions.add(option);
			}
			
			if (okDistances.contains(distance))
			{
				silverOptions.add(option);
			}
		}
		
		if (goldenOptions.size() > 0)
		{
			return goldenOptions.get(random.nextInt(goldenOptions.size()));
		}
		
		if (silverOptions.size() > 0)
		{
			return silverOptions.get(random.nextInt(silverOptions.size()));
		}
		
		return retval;
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
				HRDBMSWorker.logger.debug(((Runtime.getRuntime().freeMemory()) * 100.0) / (maxMemory * 1.0) + "% free - skipped " + TableScanOperator.skippedPages.get() + " pages. figureOutProblemsTime = " + TableScanOperator.figureOutProblemsTime + ", SMTSolveTime = " + TableScanOperator.SMTSolveTime + ", nonSMTSolveTime = " + TableScanOperator.nonSMTSolveTime + ", pbpeMaintenanceTime = " + TableScanOperator.pbpeMaintenanceTime + ", SMTSolverCalls = " + TableScanOperator.SMTSolverCalls);

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
