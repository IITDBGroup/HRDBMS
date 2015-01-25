package com.exascale.managers;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.io.StringWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeSet;
import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import com.exascale.misc.AtomicDouble;
import com.exascale.misc.DataEndMarker;
import com.exascale.misc.HParms;
import com.exascale.misc.InternalConcurrentHashMap;
import com.exascale.misc.LongPrimitiveConcurrentHashMap;
import com.exascale.misc.LongPrimitiveConcurrentHashMap.EntryIterator;
import com.exascale.misc.LongPrimitiveConcurrentHashMap.EntrySet;
import com.exascale.misc.LongPrimitiveConcurrentHashMap.WriteThroughEntry;
import com.exascale.misc.MurmurHash;
import com.exascale.misc.MyDate;
import com.exascale.misc.ReverseConcurrentHashMap;
import com.exascale.threads.HRDBMSThread;
import com.exascale.threads.ThreadPoolThread;

public final class ResourceManager extends HRDBMSThread
{
	private static int SLEEP_TIME;

	private static int LOW_PERCENT_FREE;

	private static int HIGH_PERCENT_FREE;

	private static int PERCENT_TO_CUT;

	public static int QUEUE_SIZE;

	public static int CUDA_SIZE;

	private static final Vector<DiskBackedCollection> collections = new Vector<DiskBackedCollection>();

	private static volatile boolean lowMem = false;

	private static ArrayList<String> TEMP_DIRS;

	private static final AtomicLong idGen = new AtomicLong(0);
	private static HashMap<Long, String> creations = new HashMap<Long, String>();
	private static volatile boolean hasBeenLowMem = false;
	private static int MIN_CT_SIZE;
	private static int NUM_CTHREADS;
	private static boolean PROFILE;
	private static boolean DEADLOCK_DETECT;
	public static boolean GPU;
	public static final int cpus;

	public static final ExecutorService pool;

	public static final AtomicInteger objID = new AtomicInteger(0);

	public static final long maxMemory;

	public static volatile AtomicInteger NO_OFFLOAD = new AtomicInteger(0);

	static
	{
		final HParms hparms = HRDBMSWorker.getHParms();
		LOW_PERCENT_FREE = Integer.parseInt(hparms.getProperty("low_mem_percent")); // 30
		SLEEP_TIME = Integer.parseInt(hparms.getProperty("rm_sleep_time_ms")); // 10000
		HIGH_PERCENT_FREE = Integer.parseInt(hparms.getProperty("high_mem_percent")); // 70
		PERCENT_TO_CUT = Integer.parseInt(hparms.getProperty("percent_to_cut")); // 2
		MIN_CT_SIZE = Integer.parseInt(hparms.getProperty("min_cleaner_thread_size")); // 20000
		NUM_CTHREADS = Integer.parseInt(hparms.getProperty("max_cleaner_threads")); // 16
		PROFILE = (hparms.getProperty("profile")).equals("true");
		DEADLOCK_DETECT = (hparms.getProperty("detect_thread_deadlocks")).equals("true");
		QUEUE_SIZE = Integer.parseInt(hparms.getProperty("queue_size")); // 2500000
		CUDA_SIZE = Integer.parseInt(hparms.getProperty("cuda_batch_size")); // 30720
		GPU = (hparms.getProperty("gpu_offload")).equals("true");
		cpus = Runtime.getRuntime().availableProcessors();
		pool = Executors.newCachedThreadPool();
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

	public static DiskBackedArray newDiskBackedArray(boolean indexed, int estimate)
	{
		final DiskBackedArray array = new DiskBackedArray(indexed, estimate);
		return array;
	}

	public static DiskBackedArray newDiskBackedArray(int estimate)
	{
		final DiskBackedArray array = new DiskBackedArray(false, estimate);
		return array;
	}

	public static DiskBackedHashMap newDiskBackedHashMap(boolean indexed, int estimate)
	{
		final DiskBackedHashMap map = new DiskBackedHashMap(indexed, estimate);
		collections.add(map);
		return map;
	};

	public static DiskBackedHashSet newDiskBackedHashSet(boolean iterate, int estimate)
	{
		final DiskBackedHashSet set = new DiskBackedHashSet(iterate, estimate);
		return set;
	}

	public static void printOpenStructures()
	{
		try
		{
			ArrayList<Map.Entry<Long, String>> clone = new ArrayList<Map.Entry<Long, String>>(creations.entrySet());
			for (final Map.Entry entry : clone)
			{
				HRDBMSWorker.logger.debug(entry.getKey() + ": " + entry.getValue());
			}
		}
		catch(Exception e)
		{}
	}

	public static void waitForSync()
	{
		for (final DiskBackedCollection dbc : (Vector<DiskBackedCollection>)collections.clone())
		{
			if (dbc instanceof DiskBackedHashMap)
			{
				((DiskBackedHashMap)dbc).lock.readLock().lock();
				((DiskBackedHashMap)dbc).lock.readLock().unlock();
			}
		}
	}

	private static void handleHighMem()
	{
		// System.gc();
		while (highMem())
		{
			final ArrayList<ThreadPoolThread> threads = new ArrayList<ThreadPoolThread>(collections.size());
			// /System.out.println(((Runtime.getRuntime().freeMemory() +
			// maxMemory - Runtime.getRuntime().totalMemory()) * 100.0) /
			// (maxMemory * 1.0) + "% free");
			int i = 0;
			while (i < collections.size())
			{
				try
				{
					final DiskBackedCollection collection = collections.get(i);
					final ImportThread rt = new ImportThread(collection);
					threads.add(rt);
					rt.start();
				}
				catch (final Exception e)
				{
				}
				i++;
			}

			boolean didSomething = false;
			for (final ThreadPoolThread rt : threads)
			{
				while (true)
				{
					try
					{
						rt.join();
						if (((ImportThread)rt).didSomething())
						{
							didSomething = true;
						}
						// System.out.println(((Runtime.getRuntime().freeMemory()
						// + maxMemory - Runtime.getRuntime().totalMemory()) *
						// 100.0) / (maxMemory * 1.0) + "% free");
						break;
					}
					catch (final InterruptedException e)
					{
					}
				}
			}
			
			if (!didSomething)
			{
				return;
			}
		}
	}

	private static void handleLowMem()
	{
		// System.gc();
		long time = System.currentTimeMillis();
		while (lowMem())
		{
			long now = System.currentTimeMillis();
			if (now - time > (5 * 60 * 1000))
			{
				System.gc();
			}
			
			if (HRDBMSWorker.type != HRDBMSWorker.TYPE_WORKER)
			{
				PlanCacheManager.reduce();
			}
			
			final ArrayList<ThreadPoolThread> threads = new ArrayList<ThreadPoolThread>(collections.size());
			// /System.out.println(((Runtime.getRuntime().freeMemory() +
			// maxMemory - Runtime.getRuntime().totalMemory()) * 100.0) /
			// (maxMemory * 1.0) + "% free");
			int i = 0;
			while (i < collections.size())
			{
				try
				{
					final DiskBackedCollection collection = collections.get(i);
					if (collection != null)
					{
						final ReduceThread rt = new ReduceThread(collection);
						threads.add(rt);
						rt.start();
					}
				}
				catch (final Exception e)
				{
				}
				i++;
			}

			for (final ThreadPoolThread rt : threads)
			{
				while (true)
				{
					try
					{
						rt.join();
						// System.out.println(((Runtime.getRuntime().freeMemory()
						// + maxMemory - Runtime.getRuntime().totalMemory()) *
						// 100.0) / (maxMemory * 1.0) + "% free");
						break;
					}
					catch (final InterruptedException e)
					{
					}
				}
			}
		}
	}

	private static long hash(Object key)
	{
		long eHash;
		if (key == null)
		{
			eHash = 0;
		}
		else
		{
			eHash = MurmurHash.hash64(key.toString());
		}

		return eHash;
	}

	private static boolean highMem()
	{
		return ((Runtime.getRuntime().freeMemory() + maxMemory - Runtime.getRuntime().totalMemory()) * 100.0) / (maxMemory * 1.0) > HIGH_PERCENT_FREE;
	}

	private static boolean lowMem()
	{
		return ((Runtime.getRuntime().freeMemory() + maxMemory - Runtime.getRuntime().totalMemory()) * 100.0) / (maxMemory * 1.0) < LOW_PERCENT_FREE;
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
		if (DEADLOCK_DETECT)
		{
			new DeadlockThread().start();
		}
		while (true)
		{
			// System.out.println(((Runtime.getRuntime().freeMemory() +
			// Runtime.getRuntime().maxMemory() -
			// Runtime.getRuntime().totalMemory()) * 100.0) /
			// (Runtime.getRuntime().maxMemory() * 1.0) + "% free");
			if (highMem() && hasBeenLowMem)
			{
				handleHighMem();
				hasBeenLowMem = false;
			}
			else if (lowMem())
			{
				System.gc();
				if (lowMem())
				{
					lowMem = true;
					hasBeenLowMem = true;
					handleLowMem();
					lowMem = false;
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

	public static final class DiskBackedALOHashMap<V>
	{
		private DiskBackedHashMap keys;
		private DiskBackedHashMap values;

		public DiskBackedALOHashMap(int size)
		{
			keys = ResourceManager.newDiskBackedHashMap(false, size);
			values = ResourceManager.newDiskBackedHashMap(false, size);
		}

		public void close()
		{
			try
			{
				keys.close();
				values.close();
				keys = null;
				values = null;
			}
			catch (final Exception e)
			{
			}
		}

		public V get(ArrayList<Object> key) throws Exception
		{
			long plus = 0;
			final long hash = hash(key);
			try
			{
				ArrayList<Object> current = keys.get(0x0EFFFFFFFFFFFFFFL & (hash + plus * plus));
				if (current == null)
				{
					return null;
				}
				while (!current.equals(key))
				{
					plus++;
					current = keys.get(0x0EFFFFFFFFFFFFFFL & (hash + plus * plus));
					if (current == null)
					{
						return null;
					}
				}

				values.lock.readLock().lock();
				boolean disk = false;
				ArrayList<Object> valueAL = values.internal.get(0x0EFFFFFFFFFFFFFFL & (hash + plus * plus));
				if (valueAL == null)
				{
					disk = true;
					valueAL = (ArrayList<Object>)values.getFromDisk(0x0EFFFFFFFFFFFFFFL & (hash + plus * plus));
				}
				while (valueAL == null)
				{
					disk = false;
					valueAL = values.internal.get(0x0EFFFFFFFFFFFFFFL & (hash + plus * plus));
					if (valueAL == null)
					{
						disk = true;
						valueAL = (ArrayList<Object>)values.getFromDisk(0x0EFFFFFFFFFFFFFFL & (hash + plus * plus));
					}
				}

				if (disk)
				{
					final ArrayList<Object> result = values.internal.putIfAbsent(0x0EFFFFFFFFFFFFFFL & (hash + plus * plus), valueAL);
					if (result != null)
					{
						valueAL = result;
					}
				}
				values.lock.readLock().unlock();
				if (valueAL.size() == 1 && (valueAL.get(0) instanceof AtomicLong || valueAL.get(0) instanceof AtomicDouble))
				{
					final Object o = valueAL.get(0);
					return (V)o;
				}
				else
				{
					return (V)valueAL;
				}
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				throw e;
			}
		}

		public Set<ArrayList<Object>> keySet() throws Exception
		{
			keys.lock.readLock().lock();
			final HashSet retval = new HashSet<ArrayList<Object>>((keys.size() >= 0 && keys.size() <= Integer.MAX_VALUE) ? (int)keys.size() : Integer.MAX_VALUE);
			// Values set = keys.internal.values();
			// ValueIterator it = set.iterator();
			// while (it.hasNext())
			// {
			// retval.add(it.next());
			// }
			retval.addAll(keys.internal.values());

			if (keys.index != null)
			{
				// com.exascale.optimizer.testing.LongPrimitiveConcurrentHashMap.KeySet
				// set2 = keys.index.keySet();
				// com.exascale.optimizer.testing.LongPrimitiveConcurrentHashMap.KeyIterator
				// it2 = set2.iterator();
				// while (it2.hasNext())
				// {
				// try
				// {
				// retval.add(keys.getFromDisk(it2.next()));
				// }
				// catch(Exception e)
				// {
				// HRDBMSWorker.logger.error("", e);
				// System.exit(1);
				// }
				// }

				try
				{
					retval.addAll(keys.getAllFromDisk());
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.error("", e);
					throw e;
				}
			}

			return retval;
		}

		public void put(ArrayList<Object> key, V value) throws Exception
		{
			ArrayList<Object> temp;
			if (!(value instanceof ArrayList))
			{
				temp = new ArrayList<Object>();
				temp.add(value);
			}
			else
			{
				temp = (ArrayList)value;
			}
			long plus = 0;
			final long hash = hash(key);
			try
			{
				while (keys.putIfAbsent(0x0EFFFFFFFFFFFFFFL & (hash + plus * plus), key) != null)
				{
					plus++;
				}

				values.put(0x0EFFFFFFFFFFFFFFL & (hash + plus * plus), temp);
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				throw e;
			}
		}

		public Object putIfAbsent(ArrayList<Object> key, V value) throws Exception
		{
			ArrayList<Object> temp;
			if (!(value instanceof ArrayList))
			{
				temp = new ArrayList<Object>();
				temp.add(value);
			}
			else
			{
				temp = (ArrayList)value;
			}
			long plus = 0;
			final long hash = hash(key);
			try
			{
				while (true)
				{
					final Object retval = keys.putIfAbsent(0x0EFFFFFFFFFFFFFFL & (hash + plus * plus), key);
					if (retval == null)
					{
						break;
					}

					final ArrayList<Object> r = keys.get(0x0EFFFFFFFFFFFFFFL & (hash + plus * plus));
					if (r.equals(key))
					{
						return retval;
					}
					plus++;
				}

				values.put(0x0EFFFFFFFFFFFFFFL & (hash + plus * plus), temp);
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				throw e;
			}

			return null;
		}
	}

	public static final class DiskBackedArray extends DiskBackedCollection implements Iterable
	{
		private volatile DiskBackedHashMap internal;
		private final AtomicLong index = new AtomicLong(0);

		public DiskBackedArray(boolean indexed, int estimate)
		{
			internal = ResourceManager.newDiskBackedHashMap(indexed, estimate);
		}

		public void add(ArrayList<Object> o) throws Exception
		{
			final long myIndex = index.getAndIncrement();
			internal.put(myIndex, o);
		}

		public void clear()
		{
			internal.clear();
			index.set(0);
		}

		public void close() throws IOException
		{
			if (internal != null)
			{
				internal.close();
				internal = null;
			}
		}

		public boolean contains(Object val) throws Exception
		{
			return internal.containsValue(val);
		}

		public Object get(long index) throws Exception
		{
			return internal.get(index);
		}

		@Override
		public Iterator iterator()
		{
			return new DiskBackedArrayIterator(this);
		}

		@Override
		public void reduceResources() throws IOException
		{
			internal.reduceResources();
		}

		public long size()
		{
			return internal.size();
		}

		public void update(long index, ArrayList<Object> o) throws Exception
		{
			internal.update(index, o);
		}

		/*
		 * public void concat(DiskBackedArray array) throws Exception { for
		 * (Object o : array) { this.add(o); } }
		 */
	}

	public static final class DiskBackedArrayIterator implements Iterator
	{
		private volatile DiskBackedArray array;
		private long index = 0;

		public DiskBackedArrayIterator(DiskBackedArray array)
		{
			this.array = array;
		}

		@Override
		public boolean hasNext()
		{
			return (index < array.size());
		}

		@Override
		public Object next()
		{
			Object retval;
			try
			{
				retval = array.get(index);
				index++;
				return retval;
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				return null;
			}
		}

		@Override
		public void remove()
		{
			throw new UnsupportedOperationException();

		}
	}

	public static final class DiskBackedHashMap extends DiskBackedCollection
	{
		private InternalConcurrentHashMap internal;

		private final AtomicLong size = new AtomicLong(0);

		private volatile ArrayList<FileChannel> ofcs = new ArrayList<FileChannel>(TEMP_DIRS.size());

		private ArrayList<AtomicLong> ofcSizes = new ArrayList<AtomicLong>(TEMP_DIRS.size());
		private volatile ArrayList<Vector<FileChannel>> ifcsArrayList = new ArrayList<Vector<FileChannel>>(TEMP_DIRS.size());
		private ArrayList<Vector<Boolean>> locksArrayList = new ArrayList<Vector<Boolean>>(TEMP_DIRS.size());
		public final ReadWriteLock lock = new ReentrantReadWriteLock();
		private final boolean indexed;
		private volatile ReverseConcurrentHashMap valueIndex;
		private volatile DiskBackedHashSet diskValueIndex;
		private final long id;
		private volatile boolean filesAllocated = false;
		private int estimate;
		private volatile LongPrimitiveConcurrentHashMap index;
		private ArrayList<RandomAccessFile> rafs = new ArrayList<RandomAccessFile>();

		public DiskBackedHashMap(boolean indexed, int estimate)
		{
			this.indexed = indexed;
			estimate = estimate / 1024;
			if (estimate < 10)
			{
				estimate = 10;
			}
			// System.out.println("Estimate is " + estimate);
			// if (estimate * 16 * 30 * 35 < 0.25 * maxMemory)
			// {
			// this.estimate = estimate;
			// }
			// else
			// {
			// this.estimate = (int)((5.0 / 350000.0) * maxMemory);
			// }
			internal = new InternalConcurrentHashMap((int)(1.5 * estimate), 1.0f, cpus * 6); // FIXME
			if (indexed)
			{
				valueIndex = new ReverseConcurrentHashMap((int)(1.5 * estimate), 1.0f, cpus * 6);
				diskValueIndex = ResourceManager.newDiskBackedHashSet(false, estimate);
			}

			id = idGen.getAndIncrement();
			//DEBUG
			//Exception e = new Exception();
			//HRDBMSWorker.logger.debug("Creating DBHM #" + id, e);
			//printOpenStructures(); //DEBUG
		}

		public void clear()
		{
			lock.readLock().lock();
			try
			{
				internal.clear();
			}
			catch(Exception e)
			{
				lock.readLock().unlock();
				throw e;
			}
			lock.readLock().unlock();
			size.set(0);
			if (valueIndex != null)
			{
				valueIndex.clear();
			}

			if (diskValueIndex != null)
			{
				diskValueIndex.clear();
			}

			if (index != null)
			{
				index.clear();
			}
		}

		public synchronized void close() throws IOException
		{
			if (filesAllocated)
			{
				if (ofcs != null)
				{
					for (final FileChannel ofc : ofcs)
					{
						ofc.close();
					}

					ofcs = null;
				}

				if (ifcsArrayList != null)
				{
					for (final Vector<FileChannel> ifcs : ifcsArrayList)
					{
						for (final FileChannel fc : ifcs)
						{
							fc.close();
						}
					}

					ifcsArrayList = null;
				}
				
				if (rafs != null)
				{
					for (RandomAccessFile raf : rafs)
					{
						raf.close();
					}
					
					rafs = null;
				}

				index = null;

				for (final String TEMP_DIR : TEMP_DIRS)
				{
					File file = new File(TEMP_DIR + "DBHM" + id + ".tmp");
					if (!(file.delete()))
					{
						if (file.exists())
						{
							HRDBMSWorker.logger.debug("Delete of " + TEMP_DIR + "DBHM" + id + ".tmp failed");
						}
					}
				}
				
				filesAllocated = false;
			}

			collections.remove(this);

			if (internal != null)
			{
				internal.clear();
			}
			if (indexed)
			{
				if (valueIndex != null)
				{
					valueIndex.clear();
				}
				
				if (diskValueIndex != null)
				{
					diskValueIndex.close();
				}
			}
			
			internal = null;
			ofcs = null;
			ofcSizes = null;
			ifcsArrayList = null;
			locksArrayList = null;
			valueIndex = null;
			diskValueIndex = null;
			index = null;
			rafs = null;
		}

		public boolean containsValue(Object val) throws Exception
		{
			ArrayList<Object> val2 = null;
			if (val instanceof ArrayList)
			{
				val2 = (ArrayList<Object>)val;
			}
			else
			{
				val2 = new ArrayList<Object>(1);
				val2.add(val);
			}

			if (this.valueIndex.containsKey(val2))
			{
				return true;
			}

			Object val3 = null;
			if (val instanceof ArrayList && ((ArrayList)val).size() > 1)
			{
				val3 = ((ArrayList)val).get(0).getClass().toString() + "\u0000" + ((ArrayList)val).get(0).toString();
				int i = 1;
				while (i < ((ArrayList)val).size())
				{
					val3 = ((String)val3) + "\u0001" + ((ArrayList)val).get(i).getClass().toString() + "\u0000" + ((ArrayList)val).get(i).toString();
					i++;
				}
			}
			else if (val instanceof ArrayList)
			{
				val3 = ((ArrayList)val).get(0);
			}
			else
			{
				val3 = val;
			}
			// System.out.println("Calling diskContainsValue()");
			if (diskValueIndex.contains(val))
			{
				return true;
			}

			return false;
		}

		public final void freeFC(FileChannel ifc, int hash, int i)
		{
			final Vector<FileChannel> ifcs = ifcsArrayList.get(hash);
			final Vector<Boolean> locks = locksArrayList.get(hash);
			ifcs.get(i);
			locks.set(i, false);
			return;
		}

		public final ArrayList<Object> get(Long key) throws Exception
		{
			ArrayList<Object> o = null;
			o = internal.get(key);
			if (o != null)
			{
				return o;
			}

			return (ArrayList<Object>)getFromDisk(key);
		}

		public final ArrayList<Object> getAllFromDisk() throws Exception
		{
			if (!filesAllocated)
			{
				return null;
			}

			final long siz = this.size() - this.internal.size();
			final int s = (siz >= 0 && siz <= Integer.MAX_VALUE) ? (int)siz : Integer.MAX_VALUE;
			final ArrayList<Object> retval = new ArrayList<Object>(s);
			final EntryIterator it = this.index.entrySet().iterator();
			final FileChannelAndInt[] fcais = new FileChannelAndInt[TEMP_DIRS.size()];
			int i = 0;
			while (i < TEMP_DIRS.size())
			{
				fcais[i] = getFreeFC(i);
				i++;
			}

			while (it.hasNext())
			{
				final WriteThroughEntry entry = it.next();
				final long keyVal = entry.getKey();
				long resultVal = entry.getValue();

				ByteBuffer object = null;
				final FileChannel fc = fcais[(int)(keyVal % TEMP_DIRS.size())].fc;
				// synchronized(fc)
				{
					// fc.position(resultVal);
					final ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
					int count = 0;
					while (count < sizeBuffer.limit())
					{
						final int temp = fc.read(sizeBuffer, resultVal);
						count += temp;
						resultVal += temp;
					}
					sizeBuffer.position(0);
					final int size = sizeBuffer.getInt();
					object = ByteBuffer.allocate(size);
					count = 0;
					while (count < object.limit())
					{
						final int temp = fc.read(object, resultVal);
						count += temp;
						resultVal += temp;
					}
				}
				retval.add(fromBytes(object.array()));
			}

			i = 0;
			for (final FileChannelAndInt fcai : fcais)
			{
				freeFC(fcai.fc, i, fcai.i);
				i++;
			}
			return retval;
		}

		public final FileChannelAndInt getFreeFC(int hash) throws Exception
		{
			int i = 0;
			final Vector<Boolean> locks = locksArrayList.get(hash);
			final Vector<FileChannel> ifcs = ifcsArrayList.get(hash);
			while (i < locks.size())
			{
				final boolean locked = locks.get(i);
				if (!locked)
				{
					locks.set(i, true);
					return new FileChannelAndInt(ifcs.get(i), i);
				}
				i++;
			}

			FileChannel retval = null;
			try
			{
				RandomAccessFile raf = new RandomAccessFile(TEMP_DIRS.get(hash) + "DBHM" + id + ".tmp", "r");
				retval = raf.getChannel();
				locks.add(true);
				ifcs.add(retval);
				rafs.add(raf);
				i = 0;
				while (!ifcs.get(i).equals(retval))
				{
					i++;
				}
				return new FileChannelAndInt(retval, i);
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				throw e;
			}
		}

		public final Object getFromDisk(Long keyVal) throws Exception
		{
			// ByteBuffer keyBB = ByteBuffer.allocate(8);
			// keyBB.putLong(keyVal.longValue());
			// byte[] key = new byte[8];
			// keyBB.position(0);
			// keyBB.get(key);
			// byte[] result = indexes.get((int)(keyVal % TEMP_COUNT)).get(key);
			if (!filesAllocated)
			{
				return null;
			}

			long resultVal = getFromIndex(keyVal);
			if (resultVal == 0)
			{
				return null;
			}

			// if (result == null)
			// {
			// //System.out.println("Cannot find key " + keyVal);
			// return null;
			// }

			// ByteBuffer resultBB = ByteBuffer.wrap(result);
			// resultBB.position(0);
			// long resultVal = resultBB.getLong();

			/*
			 * fis.getChannel().position(resultVal); ObjectInputStream iis = new
			 * ObjectInputStream(new BufferedInputStream(fis)); return
			 * iis.readObject();
			 */

			ByteBuffer object = null;
			final FileChannelAndInt fcai = getFreeFC((int)(keyVal % TEMP_DIRS.size()));
			final FileChannel fc = fcai.fc;
			// synchronized(fc)
			{
				// long fcSize = fc.size();
				// if (resultVal + 4 > fcSize)
				// {
				// ofcs.get((int)((keyVal % TEMP_COUNT))).force(true);
				// }

				// fc.position(resultVal);
				final ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
				int count = 0;
				while (count < sizeBuffer.limit())
				{
					// count += fc.read(sizeBuffer);
					final int temp = fc.read(sizeBuffer, resultVal);
					count += temp;
					resultVal += temp;
				}
				sizeBuffer.position(0);
				final int size = sizeBuffer.getInt();
				// while (size == 0)
				// {
				// ofcs.get((int)((keyVal % TEMP_COUNT))).force(true);
				// sizeBuffer.position(0);
				// fc.position(resultVal);
				// count = 0;
				// while (count < sizeBuffer.limit())
				// {
				// count += fc.read(sizeBuffer);
				// }
				// sizeBuffer.position(0);
				// size = sizeBuffer.getInt();
				// }
				object = ByteBuffer.allocate(size);
				// if (resultVal + 4 + size > fcSize)
				// {
				// ofcs.get((int)((keyVal % TEMP_COUNT))).force(true);
				// }
				count = 0;
				while (count < object.limit())
				{
					final int temp = fc.read(object, resultVal);
					count += temp;
					resultVal += temp;
				}
				// object.position(0);
				// int numFields = object.getInt();
				// int failures = 0;
				// while (numFields == 0)
				// {
				// ofcs.get((int)((keyVal % TEMP_COUNT))).force(true);
				// object.position(0);
				// fc.position(resultVal+4);
				// count = 0;
				// while (count < object.limit())
				// {
				// count += fc.read(object);
				// }
				// object.position(0);
				// numFields = object.getInt();
				// failures++;
				//
				// if (failures % 100 == 0)
				// {
				// System.out.println("Read object with numFields == 0, " +
				// failures + " times.");
				// }
				// }
			}
			freeFC(fc, (int)(keyVal % TEMP_DIRS.size()), fcai.i);
			return fromBytes(object.array());
		}

		public final boolean importResources() throws IOException
		{
			if (NO_OFFLOAD.get() != 0)
			{
				try
				{
					Thread.sleep(SLEEP_TIME);
				}
				catch (final Exception e)
				{
				}
				return false;
			}

			if (index == null)
			{
				return false;
			}

			lock.writeLock().lock();
			EntrySet set = index.entrySet();
			final long size = set.size();
			int num2Cut = 0;
			num2Cut = (size * PERCENT_TO_CUT / 100) > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int)(size * PERCENT_TO_CUT / 100);
			if (num2Cut < MIN_CT_SIZE)
			{
				num2Cut = MIN_CT_SIZE;
				if (num2Cut > size)
				{
					num2Cut = (int)size;
				}
			}

			if (num2Cut == 0)
			{
				lock.writeLock().unlock();
				try
				{
					Thread.sleep(SLEEP_TIME);
				}
				catch (final Exception e)
				{
				}
				return false;
			}

			// System.out.println("Going to reduce " + num2Cut + "/" + size +
			// " entries.");
			int i = 0;
			int j = 0;
			EntryIterator it = set.iterator();
			int CT_SIZE = num2Cut / NUM_CTHREADS;
			if (CT_SIZE < MIN_CT_SIZE)
			{
				CT_SIZE = MIN_CT_SIZE;
			}
			ArrayList<WriteThroughEntry> entries = new ArrayList<WriteThroughEntry>(CT_SIZE);
			final ArrayList<ThreadPoolThread> threads = new ArrayList<ThreadPoolThread>(num2Cut / CT_SIZE + 1);

			while (it.hasNext())
			{
				final WriteThroughEntry entry = it.next();
				entries.add(entry);
				i++;
				j++;

				// if (i % 10000 == 0)
				// {
				// System.out.println("Marked " + i + "/" + num2Cut +
				// " entries for reduction.");
				// }

				if (j == CT_SIZE)
				{
					final Import2Thread ct = new Import2Thread(entries);
					threads.add(ct);
					j = 0;
					entries = new ArrayList<WriteThroughEntry>(CT_SIZE);
				}

				if (i == num2Cut)
				{
					if (entries.size() != 0)
					{
						final Import2Thread ct = new Import2Thread(entries);
						threads.add(ct);
					}
					break;
				}
			}

			it = null;
			set = null;
			for (final ThreadPoolThread t : threads)
			{
				t.start();
			}

			final Iterator<ThreadPoolThread> it2 = threads.iterator();
			while (it2.hasNext())
			{
				final ThreadPoolThread t = it2.next();
				while (true)
				{
					try
					{
						t.join();
						it2.remove();
						break;
					}
					catch (final InterruptedException e)
					{
					}
				}
			}

			lock.writeLock().unlock();
			return true;
		}

		public final void put(Long key, ArrayList<Object> val) throws Exception
		{
			if (lowMem)
			{
				putToDisk(key, val);
				// System.out.println("Wrote key " + key + " to disk");
				size.incrementAndGet();
				return;
			}

			internal.put(key, val);

			if (indexed)
			{
				valueIndex.put(val, key);
			}

			size.incrementAndGet();
		}

		public final Object putIfAbsent(long key, ArrayList<Object> val) throws Exception
		{
			Object retval = internal.get(key);
			if (retval != null)
			{
				return retval;
			}

			if (index != null)
			{
				final long r = index.get(key);
				if (r != -1)
				{
					return r;
				}
			}

			this.lock.readLock().lock();
			if (this.index != null)
			{
				final long r = this.index.get(key);
				if (r != -1)
				{
					this.lock.readLock().unlock();
					return r;
				}
			}

			retval = internal.putIfAbsent(key, val);
			this.lock.readLock().unlock();
			if (retval == null)
			{
				if (indexed)
				{
					valueIndex.put(val, key);
				}

				size.incrementAndGet();
			}
			return retval;
		}

		public final void putNoSize(Long key, ArrayList<Object> val) throws Exception
		{
			if (lowMem)
			{
				putToDisk(key, val);
				// System.out.println("Wrote key " + key + " to disk");
				return;
			}

			internal.put(key, val);

			if (indexed)
			{
				valueIndex.put(val, key);
			}
		}

		public final void putToDisk(ArrayList<InternalConcurrentHashMap.WriteThroughEntry> entries) throws Exception
		{
			if (!filesAllocated)
			{
				synchronized (this)
				{
					if (!filesAllocated)
					{
						//creations.put(id, ""); //DEBUG
						int i = 0;
						index = new LongPrimitiveConcurrentHashMap((int)(estimate * 1.5), 1.0f, cpus * 6);
						// index = new ConcurrentHashMap<Long, Long>(estimate,
						// 0.75f, Runtime.getRuntime().availableProcessors() *
						// 6);
						while (i < TEMP_DIRS.size())
						{
							// Properties props = new Properties();
							// props.setProperty(RecordManagerOptions.THREAD_SAFE,
							// "true");
							// props.setProperty(RecordManagerOptions.CACHE_TYPE,
							// "none");
							// props.setProperty(RecordManagerOptions.AUTO_COMMIT,
							// "true");
							// RecordManager recman =
							// RecordManagerFactory.createRecordManager("JDBM" +
							// id + "_" + i, props );
							// BTree tree = BTree.createInstance(recman);
							RandomAccessFile raf = new RandomAccessFile(TEMP_DIRS.get(i) + "DBHM" + id + ".tmp", "rw");
							rafs.add(raf);
							final FileChannel ofc = raf.getChannel();
							ofcs.add(ofc);
							final AtomicLong ofcSize = new AtomicLong(1);
							ofcSizes.add(ofcSize);
							final Vector<FileChannel> ifcs = new Vector<FileChannel>();
							raf = new RandomAccessFile(TEMP_DIRS.get(i) + "DBHM" + id + ".tmp", "r");
							rafs.add(raf);
							ifcs.add(raf.getChannel());
							ifcsArrayList.add(ifcs);
							final Vector<Boolean> locks = new Vector<Boolean>();
							locks.add(false);
							locksArrayList.add(locks);
							i++;
						}
						filesAllocated = true;
					}
				}
			}
			final ArrayList<InternalConcurrentHashMap.WriteThroughEntry>[] parts = new ArrayList[TEMP_DIRS.size()];
			int i = 0;
			while (i < TEMP_DIRS.size())
			{
				parts[i] = new ArrayList<InternalConcurrentHashMap.WriteThroughEntry>();
				i++;
			}
			for (final InternalConcurrentHashMap.WriteThroughEntry entry : entries)
			{
				parts[(int)((entry.getKey()) % TEMP_DIRS.size())].add(entry);
			}

			int index = 0;
			while (index < parts.length)
			{
				final ArrayList<InternalConcurrentHashMap.WriteThroughEntry> part = parts[index];
				if (part.size() > 0)
				{
					final byte[] bytes = arrayListToBytes(part);
					// System.out.println("Writing an buffer of size " +
					// (bytes.length / 1024) + "K");
					final long indexValue = ofcSizes.get((int)((part.get(0).getKey()) % TEMP_DIRS.size())).getAndAdd(bytes.length);
					final ByteBuffer bb = ByteBuffer.wrap(bytes);
					final FileChannel ofci = ofcs.get((int)((part.get(0).getKey()) % TEMP_DIRS.size()));
					// synchronized(ofci)
					{
						// ofci.position(indexValue);
						long pos = indexValue;
						int count = 0;
						while (count < bb.limit())
						{
							final int temp = ofci.write(bb, pos);
							count += temp;
							pos += temp;
						}
					}

					// ArrayList<Map.Entry> list = new ArrayList<Map.Entry>();
					// bb.position(0);
					// for (Map.Entry entry : part)
					// {
					// list.add(new
					// AbstractMap.SimpleEntry((Long)entry.getKey(), indexValue
					// + bb.position()));
					// int size = bb.getInt();
					// bb.position(bb.position() + size);
					// }
					// putToIndex(list);
					// list = null;

					bb.position(0);
					for (final InternalConcurrentHashMap.WriteThroughEntry entry : part)
					{
						putToIndex(entry.getKey(), indexValue + bb.position());
						if (indexed)
						{
							diskValueIndex.add((entry.getValue()));
							final ArrayList<Object> o = internal.get(entry.getKey());
							valueIndex.remove(o);
						}
						internal.remove(entry.getKey());
						final int size = bb.getInt();
						bb.position(bb.position() + size);
					}
				}

				parts[index] = null;
				index++;
			}
		}

		public final void putToDisk(long eKey, Object eVal) throws Exception
		{
			if (!filesAllocated)
			{
				synchronized (this)
				{
					if (!filesAllocated)
					{
						//creations.put(id, ""); //DEBUG
						int i = 0;
						index = new LongPrimitiveConcurrentHashMap((int)(estimate * 1.5), 1.0f, cpus * 6);
						while (i < TEMP_DIRS.size())
						{
							/*
							 * try { throw new
							 * Exception("Creation stack trace"); }
							 * catch(Exception e) { StringWriter sw = new
							 * StringWriter(); PrintWriter pw = new
							 * PrintWriter(sw); e.printStackTrace(pw);
							 * creations.put(id, sw.toString()); }
							 */
							// Properties props = new Properties();
							// props.setProperty(RecordManagerOptions.THREAD_SAFE,
							// "true");
							// props.setProperty(RecordManagerOptions.CACHE_TYPE,
							// "none");
							// props.setProperty(RecordManagerOptions.AUTO_COMMIT,
							// "true");
							// RecordManager recman =
							// RecordManagerFactory.createRecordManager("JDBM" +
							// id + "_" + i, props );
							// BTree tree = BTree.createInstance(recman)
							RandomAccessFile raf = new RandomAccessFile(TEMP_DIRS.get(i) + "DBHM" + id + ".tmp", "rw");
							rafs.add(raf);
							final FileChannel ofc = raf.getChannel();
							ofcs.add(ofc);
							final AtomicLong ofcSize = new AtomicLong(1);
							ofcSizes.add(ofcSize);
							final Vector<FileChannel> ifcs = new Vector<FileChannel>();
							raf = new RandomAccessFile(TEMP_DIRS.get(i) + "DBHM" + id + ".tmp", "r");
							rafs.add(raf);
							ifcs.add(raf.getChannel());
							ifcsArrayList.add(ifcs);
							final Vector<Boolean> locks = new Vector<Boolean>();
							locks.add(false);
							locksArrayList.add(locks);
							i++;
						}
						filesAllocated = true;
					}
				}
			}
			final byte[] bytes = toBytes(eVal);
			final long indexValue = ofcSizes.get((int)(eKey % TEMP_DIRS.size())).getAndAdd(bytes.length);
			// ByteBuffer keyBB = ByteBuffer.allocate(8);
			// ByteBuffer dataBB = ByteBuffer.allocate(8);
			// keyBB.putLong((Long)eKey);
			// dataBB.putLong((Long)indexValue);
			// byte[] key = new byte[8];
			// byte[] data = new byte[8];
			// keyBB.position(0);
			// dataBB.position(0);
			// keyBB.get(key);
			// dataBB.get(data);
			// indexes.get((int)(eKey % TEMP_COUNT)).insert(key, data);

			final ByteBuffer bb = ByteBuffer.wrap(bytes);
			final FileChannel ofci = ofcs.get((int)((eKey % TEMP_DIRS.size())));
			// synchronized(ofci)
			{
				// ofci.position(indexValue);
				long pos = indexValue;
				int count = 0;
				while (count < bb.limit())
				{
					final int temp = ofci.write(bb, pos);
					count += temp;
					pos += temp;
				}
			}

			putToIndex(eKey, indexValue);
			if (indexed)
			{
				diskValueIndex.add(((ArrayList<Object>)eVal));
			}
		}

		@Override
		public final void reduceResources() throws IOException
		{
			if (NO_OFFLOAD.get() != 0)
			{
				try
				{
					Thread.sleep(SLEEP_TIME);
				}
				catch (final Exception e)
				{
				}
				return;
			}
			lock.writeLock().lock();
			InternalConcurrentHashMap.EntrySet set = internal.entrySet();
			final long size = set.size();
			int num2Cut = 0;
			num2Cut = (size * PERCENT_TO_CUT / 100) > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int)(size * PERCENT_TO_CUT / 100);
			if (num2Cut < MIN_CT_SIZE)
			{
				num2Cut = MIN_CT_SIZE;
				if (num2Cut > size)
				{
					num2Cut = (int)size;
				}
			}

			if (num2Cut == 0)
			{
				lock.writeLock().unlock();
				return;
			}

			// System.out.println("Going to reduce " + num2Cut + "/" + size +
			// " entries.");
			int i = 0;
			int j = 0;
			InternalConcurrentHashMap.EntryIterator it = set.iterator();
			int CT_SIZE = num2Cut / NUM_CTHREADS;
			if (CT_SIZE < MIN_CT_SIZE)
			{
				CT_SIZE = MIN_CT_SIZE;
			}
			ArrayList<InternalConcurrentHashMap.WriteThroughEntry> entries = new ArrayList<InternalConcurrentHashMap.WriteThroughEntry>(CT_SIZE);
			final ArrayList<ThreadPoolThread> threads = new ArrayList<ThreadPoolThread>(num2Cut / CT_SIZE + 1);

			while (it.hasNext())
			{
				final InternalConcurrentHashMap.WriteThroughEntry entry = it.next();
				entries.add(entry);
				i++;
				j++;

				// if (i % 10000 == 0)
				// {
				// System.out.println("Marked " + i + "/" + num2Cut +
				// " entries for reduction.");
				// }

				if (j == CT_SIZE)
				{
					final CleanerThread ct = new CleanerThread(entries);
					threads.add(ct);
					j = 0;
					entries = new ArrayList<InternalConcurrentHashMap.WriteThroughEntry>(CT_SIZE);
				}

				if (i == num2Cut)
				{
					if (entries.size() != 0)
					{
						final CleanerThread ct = new CleanerThread(entries);
						threads.add(ct);
					}
					break;
				}
			}

			it = null;
			set = null;
			for (final ThreadPoolThread t : threads)
			{
				t.start();
			}

			final Iterator<ThreadPoolThread> it2 = threads.iterator();
			while (it2.hasNext())
			{
				final ThreadPoolThread t = it2.next();
				while (true)
				{
					try
					{
						t.join();
						it2.remove();
						break;
					}
					catch (final InterruptedException e)
					{
					}
				}
			}

			lock.writeLock().unlock();

			if (num2Cut == size)
			{
				try
				{
					Thread.sleep(SLEEP_TIME);
				}
				catch (final InterruptedException e)
				{
				}
			}
		}

		public boolean removeFromDisk(long keyVal) throws Exception
		{
			// ByteBuffer keyBB = ByteBuffer.allocate(8);
			// keyBB.putLong(keyVal);
			// byte[] key = new byte[8];
			// keyBB.position(0);
			// keyBB.get(key);
			Object o = null;
			if (indexed)
			{
				o = this.getFromDisk(keyVal);
			}

			final long offset = removeFromIndex(keyVal);
			final boolean retval = offset != 0;

			if (!retval)
			{
				return false;
			}

			if (indexed)
			{
				diskValueIndex.removeObject((o));
			}

			return true;
		}

		public long size()
		{
			return size.get();
		}

		public void update(Long key, ArrayList<Object> val) throws Exception
		{
			final ArrayList<Object> o = internal.replace(key, val);
			if (o != null)
			{
				return;
			}

			// removeFromDisk(key);
			putNoSize(key, val);
			return;
		}

		private final byte[] arrayListToBytes(ArrayList<InternalConcurrentHashMap.WriteThroughEntry> entries) throws Exception
		{
			final ArrayList<byte[]> results = new ArrayList<byte[]>(entries.size());
			for (final InternalConcurrentHashMap.WriteThroughEntry entry : entries)
			{
				final Object v = entry.getValue();
				ArrayList<Object> val;
				if (v instanceof ArrayList)
				{
					val = (ArrayList<Object>)v;
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
					results.add(retval);
					continue;
				}

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
						size += (4 + ((String)o).getBytes("UTF-8").length);
					}
					else if (o instanceof AtomicLong)
					{
						header[i] = (byte)6;
						size += 8;
					}
					else if (o instanceof AtomicDouble)
					{
						header[i] = (byte)7;
						size += 8;
					}
					else if (o instanceof ArrayList)
					{
						if (((ArrayList)o).size() != 0)
						{
							Exception e = new Exception("Non-zero size ArrayList in toBytes()");
							HRDBMSWorker.logger.error("Non-zero size ArrayList in toBytes()", e);
							throw e;
						}
						header[i] = (byte)8;
					}
					else
					{
						HRDBMSWorker.logger.error("Unknown type " + o.getClass() + " in toBytes()");
						HRDBMSWorker.logger.error(o);
						throw new Exception("Non-zero size ArrayList in toBytes()");
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
						byte[] temp = null;
						try
						{
							temp = ((String)o).getBytes("UTF-8");
						}
						catch (final Exception e)
						{
							HRDBMSWorker.logger.error("", e);
							throw e;
						}
						retvalBB.putInt(temp.length);
						retvalBB.put(temp);
					}
					else if (retval[i] == 6)
					{
						retvalBB.putLong(((AtomicLong)o).get());
					}
					else if (retval[i] == 7)
					{
						retvalBB.putDouble(((AtomicDouble)o).get());
					}
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
			final byte[] retval = new byte[count];
			int retvalPos = 0;
			for (final byte[] ba : results)
			{
				System.arraycopy(ba, 0, retval, retvalPos, ba.length);
				retvalPos += ba.length;
			}

			return retval;
		}

		private final Object fromBytes(byte[] val) throws Exception
		{
			final ByteBuffer bb = ByteBuffer.wrap(val);
			final int numFields = bb.getInt();

			if (numFields == 0)
			{
				return new ArrayList<Object>();
			}

			bb.position(bb.position() + numFields);
			final byte[] bytes = bb.array();
			if (bytes[4] == 5)
			{
				return new DataEndMarker();
			}
			final ArrayList<Object> retval = new ArrayList<Object>(numFields);
			int i = 0;
			while (i < numFields)
			{
				if (bytes[i + 4] == 0)
				{
					// long
					final Long o = bb.getLong();
					retval.add(o);
				}
				else if (bytes[i + 4] == 1)
				{
					// integer
					final Integer o = bb.getInt();
					retval.add(o);
				}
				else if (bytes[i + 4] == 2)
				{
					// double
					final Double o = bb.getDouble();
					retval.add(o);
				}
				else if (bytes[i + 4] == 3)
				{
					// date
					final MyDate o = new MyDate(bb.getLong());
					retval.add(o);
				}
				else if (bytes[i + 4] == 4)
				{
					// string
					final int length = bb.getInt();
					final byte[] temp = new byte[length];
					bb.get(temp);
					try
					{
						final String o = new String(temp, "UTF-8");
						retval.add(o);
					}
					catch (final Exception e)
					{
						HRDBMSWorker.logger.error("", e);
						throw e;
					}
				}
				else if (bytes[i + 4] == 6)
				{
					// AtomicLong
					final long o = bb.getLong();
					retval.add(new AtomicLong(o));
				}
				else if (bytes[i + 4] == 7)
				{
					// AtomicDouble
					final double o = bb.getDouble();
					retval.add(new AtomicDouble(o));
				}
				else if (bytes[i + 4] == 8)
				{
					// Empty ArrayList
					retval.add(new ArrayList<Object>());
				}
				else
				{
					HRDBMSWorker.logger.error("Unknown type in fromBytes()");
					throw new Exception("Unknown type in fromBytes()");
				}

				i++;
			}

			return retval;
		}

		private final long getFromIndex(long key) throws IOException
		{
			final long retval = index.get(key);
			if (retval == -1)
			{
				return 0;
			}

			return retval;
		}

		private final void putToIndex(long key, long value) throws Exception
		{
			index.put(key, value);
		}

		private void remove(long index) throws Exception
		{
			lock.readLock().lock();
			try
			{
				final ArrayList<Object> o = internal.remove(index);
				if (o != null)
				{
					lock.readLock().unlock();
					return;
				}

				removeFromDisk(index);
				lock.readLock().unlock();
			}
			catch(Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				lock.readLock().unlock();
				throw e;
			}
		}

		private long removeFromIndex(long keyVal) throws IOException
		{
			final long retval = index.remove(keyVal);
			if (retval == -1)
			{
				return 0;
			}

			return retval;
		}

		private final byte[] toBytes(Object v) throws Exception
		{
			ArrayList<Object> val;
			if (v instanceof ArrayList)
			{
				val = (ArrayList<Object>)v;
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
					size += (4 + ((String)o).getBytes("UTF-8").length);
				}
				else if (o instanceof AtomicLong)
				{
					header[i] = (byte)6;
					size += 8;
				}
				else if (o instanceof AtomicDouble)
				{
					header[i] = (byte)7;
					size += 8;
				}
				else if (o instanceof ArrayList)
				{
					if (((ArrayList)o).size() != 0)
					{
						Exception e = new Exception("Non-zero size ArrayList in toBytes()");
						HRDBMSWorker.logger.error("Non-zero size ArrayList in toBytes()", e);
						throw e;
					}
					header[i] = (byte)8;
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
					byte[] temp = null;
					try
					{
						temp = ((String)o).getBytes("UTF-8");
					}
					catch (final Exception e)
					{
						HRDBMSWorker.logger.error("", e);
						throw e;
					}
					retvalBB.putInt(temp.length);
					retvalBB.put(temp);
				}
				else if (retval[i] == 6)
				{
					retvalBB.putLong(((AtomicLong)o).get());
				}
				else if (retval[i] == 7)
				{
					retvalBB.putDouble(((AtomicDouble)o).get());
				}
				else if (retval[i] == 8)
				{
				}

				i++;
			}

			return retval;
		}

		private final class CleanerThread extends ThreadPoolThread
		{
			private final ArrayList<InternalConcurrentHashMap.WriteThroughEntry> entries;

			public CleanerThread(ArrayList<InternalConcurrentHashMap.WriteThroughEntry> entries)
			{
				this.entries = entries;
			}

			@Override
			public final void run()
			{
				try
				{
					putToDisk(entries);
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.error("", e);
				}
			}
		}

		private static final class FileChannelAndInt
		{
			private final FileChannel fc;
			private final int i;

			public FileChannelAndInt(FileChannel fc, int i)
			{
				this.fc = fc;
				this.i = i;
			}
		}

		private final class Import2Thread extends ThreadPoolThread
		{
			private final ArrayList<WriteThroughEntry> entries;

			public Import2Thread(ArrayList<WriteThroughEntry> entries)
			{
				this.entries = entries;
			}

			@Override
			public final void run()
			{
				try
				{
					for (final WriteThroughEntry entry : entries)
					{
						final ArrayList<Object> o = (ArrayList<Object>)getFromDisk(entry.getKey());
						internal.put(entry.getKey(), o);
						// putToIndex((Long)entry.getKey(), indexValue +
						// bb.position());
						if (indexed)
						{
							valueIndex.put(o, entry.getKey());
							diskValueIndex.removeObject(o);
						}
						removeFromDisk(entry.getKey());
					}
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.error("", e);
				}
			}
		}
	}

	public static final class DiskBackedHashSet extends DiskBackedCollection
	{
		private volatile DiskBackedHashMap internal;
		private final AtomicLong count = new AtomicLong(0);
		private final boolean iterate;
		private volatile DiskBackedArray internal2;

		public DiskBackedHashSet(boolean iterate, int estimate)
		{
			internal = ResourceManager.newDiskBackedHashMap(false, estimate);
			this.iterate = iterate;
			if (iterate)
			{
				internal2 = ResourceManager.newDiskBackedArray(estimate);
			}
		}

		public boolean add(ArrayList<Object> val) throws Exception
		{
			Object newVal = val;
			try
			{
				if (((ArrayList)val).size() == 1)
				{
					newVal = ((ArrayList)val).get(0);
				}
				else if (val.size() == 0)
				{
				}
				else
				{
					final StringBuilder val2 = new StringBuilder();
					val2.append(((ArrayList)val).get(0).getClass().toString());
					val2.append("\u0000");
					val2.append(((ArrayList)val).get(0).toString());
					int i = 1;
					while (i < ((ArrayList)val).size())
					{
						val2.append("\u0001");
						val2.append(((ArrayList)val).get(i).getClass().toString());
						val2.append("\u0000");
						val2.append(((ArrayList)val).get(i).toString());
						i++;
					}
					newVal = val2.toString();
				}

				final long hash = hash(newVal) & 0x0EFFFFFFFFFFFFFFL;

				synchronized (internal)
				{
					ArrayList<Object> chain = internal.get(new Long(hash));

					if (chain == null)
					{
						chain = new ArrayList<Object>();
						chain.add(newVal);
						count.getAndIncrement();
						internal.put(new Long(hash), chain);
					}
					else
					{
						if (!chain.contains(newVal))
						{
							internal.remove(hash);
							chain.add(newVal);
							internal.put(new Long(hash), chain);
							count.getAndIncrement();
						}
						else
						{
							return false;
						}
					}
				}

				if (iterate)
				{
					internal2.add(val);
				}

				return true;
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				throw e;
			}
		}

		public void clear()
		{
			internal.clear();
			if (internal2 != null)
			{
				internal2.clear();
			}
		}

		public void close() throws IOException
		{
			internal.close();
			internal = null;
			internal2 = null;
		}

		public boolean contains(Object val) throws Exception
		{
			try
			{
				if (val instanceof ArrayList)
				{
					if (((ArrayList)val).size() == 1)
					{
						val = ((ArrayList)val).get(0);
					}
					else if (((ArrayList)val).size() == 0)
					{
					}
					else
					{
						final StringBuilder val2 = new StringBuilder();
						val2.append(((ArrayList)val).get(0).getClass().toString());
						val2.append("\u0000");
						val2.append(((ArrayList)val).get(0).toString());
						int i = 1;
						while (i < ((ArrayList)val).size())
						{
							val2.append("\u0001");
							val2.append(((ArrayList)val).get(i).getClass().toString());
							val2.append("\u0000");
							val2.append(((ArrayList)val).get(i).toString());
							i++;
						}
						val = val2.toString();
					}
				}

				final long hash = hash(val) & 0x0EFFFFFFFFFFFFFFL;
				ArrayList<Object> chain = null;
				chain = internal.get(hash);

				if (chain == null)
				{
					return false;
				}

				return chain.contains(val);
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				throw e;
			}
		}

		public DiskBackedArray getArray()
		{
			return internal2;
		}

		@Override
		public void reduceResources() throws IOException
		{
			internal.reduceResources();
		}

		public void removeObject(Object val) throws Exception
		{
			try
			{
				if (val instanceof ArrayList)
				{
					if (((ArrayList)val).size() == 1)
					{
						val = ((ArrayList)val).get(0);
					}
					else if (((ArrayList)val).size() == 0)
					{
					}
					else
					{
						final StringBuilder val2 = new StringBuilder();
						val2.append(((ArrayList)val).get(0).getClass().toString());
						val2.append("\u0000");
						val2.append(((ArrayList)val).get(0).toString());
						int i = 1;
						while (i < ((ArrayList)val).size())
						{
							val2.append("\u0001");
							val2.append(((ArrayList)val).get(i).getClass().toString());
							val2.append("\u0000");
							val2.append(((ArrayList)val).get(i).toString());
							i++;
						}
						val = val2.toString();
					}
				}
				final long hash = hash(val) & 0x0EFFFFFFFFFFFFFFL;

				synchronized (internal)
				{
					final ArrayList<Object> chain = internal.get(new Long(hash));

					if (chain != null)
					{
						if (chain.contains(val))
						{
							internal.remove(hash);
							chain.remove(val);
							if (chain.size() != 0)
							{
								internal.put(new Long(hash), chain);
							}
							count.getAndDecrement();
						}
					}
				}
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				throw e;
			}
		}

		public long size()
		{
			return count.get();
		}
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

	private static abstract class DiskBackedCollection
	{
		public abstract void reduceResources() throws IOException;
	}

	private static final class ImportThread extends ThreadPoolThread
	{
		private volatile DiskBackedCollection collection;
		private boolean didSomething = false;

		public ImportThread(DiskBackedCollection collection)
		{
			this.collection = collection;
		}

		@Override
		public final void run()
		{
			try
			{
				didSomething = ((DiskBackedHashMap)collection).importResources();
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
			}
		}
		
		public boolean didSomething()
		{
			return didSomething;
		}
	}

	private static final class MonitorThread extends ThreadPoolThread
	{
		@Override
		public void run()
		{
			while (true)
			{
				HRDBMSWorker.logger.debug(((Runtime.getRuntime().freeMemory() + maxMemory - Runtime.getRuntime().totalMemory()) * 100.0) / (maxMemory * 1.0) + "% free");
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

					int i = 0;
					while (i < trace.length)
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

	private static final class ReduceThread extends ThreadPoolThread
	{
		private volatile DiskBackedCollection collection;

		public ReduceThread(DiskBackedCollection collection)
		{
			this.collection = collection;
		}

		@Override
		public final void run()
		{
			try
			{
				collection.reduceResources();
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
			}
		}
	}
}
