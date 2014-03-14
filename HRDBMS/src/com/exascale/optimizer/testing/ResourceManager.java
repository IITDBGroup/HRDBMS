package com.exascale.optimizer.testing;

import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.io.StringWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.lang.ref.WeakReference;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.ArrayList;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import sun.misc.Unsafe;

import com.exascale.optimizer.testing.InternalConcurrentHashMap.KeyIterator;
import com.exascale.optimizer.testing.InternalConcurrentHashMap.KeySet;
import com.exascale.optimizer.testing.InternalConcurrentHashMap.ValueIterator;
import com.exascale.optimizer.testing.InternalConcurrentHashMap.Values;
import com.exascale.optimizer.testing.LongPrimitiveConcurrentHashMap.EntryIterator;
import com.exascale.optimizer.testing.LongPrimitiveConcurrentHashMap.EntrySet;
import com.exascale.optimizer.testing.LongPrimitiveConcurrentHashMap.WriteThroughEntry;
import com.exascale.optimizer.testing.MetaData.PartitionMetaData;

public final class ResourceManager extends ThreadPoolThread
{
	protected static final int SLEEP_TIME = 10000;
	protected static final int LOW_PERCENT_FREE = 30;
	protected static final int HIGH_PERCENT_FREE = 70;
	protected static final int PERCENT_TO_CUT = 2;
	protected static final Vector<DiskBackedCollection> collections = new Vector<DiskBackedCollection>();
	protected static volatile boolean lowMem = false;
	protected static final int TEMP_COUNT = 1;
	protected static ArrayList<String> TEMP_DIRS;
	protected static final int MIN_CT_SIZE = 20000;
	protected static final int NUM_CTHREADS = 16;
	protected static final Long LARGE_PRIME =  1125899906842597L;
    protected static final Long LARGE_PRIME2 = 6920451961L;
    protected static final AtomicLong idGen = new AtomicLong(0);
    protected static HashMap<Long, String> creations = new HashMap<Long, String>();
    protected static volatile boolean hasBeenLowMem = false;
    protected static final boolean PROFILE = true;
    public static final int cpus;
    public static final ExecutorService pool;
    public static final AtomicInteger objID = new AtomicInteger(0);
    protected static final long maxMemory;
    public static volatile AtomicInteger NO_OFFLOAD = new AtomicInteger(0);
    public static final boolean GPU = false;
    
    static
    {
    	cpus = Runtime.getRuntime().availableProcessors();
    	pool = Executors.newCachedThreadPool();
		maxMemory = Runtime.getRuntime().maxMemory();
		if (GPU)
		{
			System.out.println("Going to load CUDA code");
			try
			{
				System.loadLibrary("extend_kernel");
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(1);
			}
		
			System.out.println("CUDA code loaded");
		}
    }
    
	public ResourceManager()
	{
		TEMP_DIRS = new ArrayList<String>(TEMP_COUNT);
		//TEMP_DIRS.add("/home/hrdbms/");
		//TEMP_DIRS.add("/temp1/");
		//TEMP_DIRS.add("/temp2/");
		//TEMP_DIRS.add("/temp3/");
		//TEMP_DIRS.add("/temp4/");
		//TEMP_DIRS.add("/temp5/");
		TEMP_DIRS.add("/mnt/ssd/");
		for (String temp : TEMP_DIRS)
		{
			File dir = new File(temp);
			for (String file : dir.list())
			{
				if (file.endsWith("tmp"))
				{
					new File(dir, file).delete();
				}
			}
		}
	}
	
	public static void printOpenStructures()
	{
		for (Map.Entry entry : creations.entrySet())
		{
			System.out.println(entry.getKey() + ": " + entry.getValue());
		}
	}
	
	public void run()
	{
		if (PROFILE)
		{
			new ProfileThread().start();
		}
		new MonitorThread().start();
		//new DeadlockThread().start();
		while (true)
		{
			//System.out.println(((Runtime.getRuntime().freeMemory() + Runtime.getRuntime().maxMemory() - Runtime.getRuntime().totalMemory()) * 100.0) / (Runtime.getRuntime().maxMemory() * 1.0) + "% free");
			if (highMem() && hasBeenLowMem)
			{
				handleHighMem();
			}
			else if (lowMem())
			{
				lowMem = true;
				hasBeenLowMem = true;
				handleLowMem();
				lowMem = false;
			}
			
			try
			{
				Thread.sleep(SLEEP_TIME);
			}
			catch(Exception e) {}
		}
	}
	
	private static final class ProfileThread extends ThreadPoolThread
	{
		protected HashMap<CodePosition, CodePosition> counts = new HashMap<CodePosition, CodePosition>();
		long samples = 0;
		
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
			
			public boolean equals(Object rhs)
			{
				CodePosition r = (CodePosition)rhs;
				if (file.equals(r.file) && lineNum == r.lineNum)
				{
					return true;
				}
				
				return false;
			}
			
			public int hashCode()
			{
				return file.hashCode() + lineNum;
			}
			
			public int compareTo(Object rhs)
			{
				CodePosition cp = (CodePosition)rhs;
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
		}
		
		public void run()
		{
			while (true)
			{
				Map<Thread, StackTraceElement[]> map = Thread.getAllStackTraces();
				for (Map.Entry entry : map.entrySet())
				{

					StackTraceElement[] trace = (StackTraceElement[])entry.getValue();
					
					int i = 0;
					while (i < trace.length)
					{
						String file = trace[i].getClassName();
						int lineNum = trace[i].getLineNumber();
						String method = trace[i].getMethodName();
						if (method.equals("next") || method.equals("put") || method.equals("take") || method.equals("join"))
						{
							break;
						}
						CodePosition cp = new CodePosition(file, lineNum, method);
						CodePosition cp2 = counts.get(cp);
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
			
				TreeSet<CodePosition> set = new TreeSet<CodePosition>();
				for (CodePosition cp : counts.values())
				{
					if (cp.count * 100 / samples >= 1)
					{
						set.add(cp);
					}
				}
				try
				{
					PrintWriter out = new PrintWriter(new File("./java.hprof.txt.new"));
					for (CodePosition cp : set)
					{
						out.println(cp.file + "." + cp.method + ":" + cp.lineNum + " " + (cp.count * 100 / samples) + "%");
					}
					out.close();
					new File("./java.hprof.txt.new").renameTo(new File("./java.hprof.txt"));
				}
				catch(Exception e)
				{
					e.printStackTrace();
					System.exit(1);
				}
			}
		}
	}
	
	private static final class DeadlockThread extends ThreadPoolThread
	{
		public void run()
		{
			ThreadMXBean bean = ManagementFactory.getThreadMXBean();
			while (true)
			{
				long[] threadIds = bean.findDeadlockedThreads(); // Returns null if no threads are deadlocked.

				if (threadIds != null) 
				{
					ThreadInfo[] infos = bean.getThreadInfo(threadIds);

					for (ThreadInfo info : infos) 
					{
						StackTraceElement[] stack = info.getStackTrace();
						for (StackTraceElement trace : stack)
						{
							System.err.println(trace);
						}
						
						System.err.println("");
					}
				}
				
				try
				{
					Thread.sleep(SLEEP_TIME);
				}
				catch(Exception e) {}
			}
		}
	}
	
	private static final class MonitorThread extends ThreadPoolThread
	{
		public void run()
		{
			while (true)
			{
				System.out.println(((Runtime.getRuntime().freeMemory() + maxMemory - Runtime.getRuntime().totalMemory()) * 100.0) / (maxMemory * 1.0) + "% free");
				try
				{
					Thread.sleep(SLEEP_TIME);
				}
				catch(Exception e) {}
			}
		}
	}
	
	protected static boolean lowMem()
	{
		return ((Runtime.getRuntime().freeMemory() + maxMemory - Runtime.getRuntime().totalMemory()) * 100.0) / (maxMemory * 1.0) < LOW_PERCENT_FREE;
	}
	
	protected static boolean extremeLowMem()
	{
		return ((Runtime.getRuntime().freeMemory() + maxMemory - Runtime.getRuntime().totalMemory()) * 100.0) / (maxMemory * 1.0) < 0;
	}
	
	protected static boolean highMem()
	{
		return ((Runtime.getRuntime().freeMemory() + maxMemory - Runtime.getRuntime().totalMemory()) * 100.0) / (maxMemory * 1.0) > HIGH_PERCENT_FREE;
	}
	
	protected static void handleLowMem()
	{
	//	System.gc(); 
		while (lowMem())
		{	
			ArrayList<ThreadPoolThread> threads = new ArrayList<ThreadPoolThread>(collections.size());
			///System.out.println(((Runtime.getRuntime().freeMemory() + maxMemory - Runtime.getRuntime().totalMemory()) * 100.0) / (maxMemory * 1.0) + "% free");
			int i = 0;
			while (i < collections.size())
			{
				try
				{
					DiskBackedCollection collection = collections.get(i);
					if (collection != null)
					{
						ReduceThread rt = new ReduceThread(collection);
						threads.add(rt);
						rt.start();
					}
				}
				catch(Exception e)
				{}
				i++;
			}
				
			for (ThreadPoolThread rt : threads)
			{
				while (true)
				{
					try
					{
						rt.join();
						//System.out.println(((Runtime.getRuntime().freeMemory() + maxMemory - Runtime.getRuntime().totalMemory()) * 100.0) / (maxMemory * 1.0) + "% free");
						break;
					}
					catch(InterruptedException e) {}
				}
			}
		}
	}
	
	public static void waitForSync()
	{
		for (DiskBackedCollection dbc : (Vector<DiskBackedCollection>)collections.clone())
		{
			if (dbc instanceof DiskBackedHashMap)
			{
				((DiskBackedHashMap) dbc).lock.readLock().lock();
				((DiskBackedHashMap) dbc).lock.readLock().unlock();
			}
		}
	}
	
	protected static void handleHighMem()
	{
	//	System.gc(); 
		while (highMem())
		{	
			ArrayList<ThreadPoolThread> threads = new ArrayList<ThreadPoolThread>(collections.size());
			///System.out.println(((Runtime.getRuntime().freeMemory() + maxMemory - Runtime.getRuntime().totalMemory()) * 100.0) / (maxMemory * 1.0) + "% free");
			int i = 0;
			while (i < collections.size())
			{
				try
				{
					DiskBackedCollection collection = collections.get(i);
					ImportThread rt = new ImportThread(collection);
					threads.add(rt);
					rt.start();
				}
				catch(Exception e)
				{}
				i++;
			}
				
			for (ThreadPoolThread rt : threads)
			{
				while (true)
				{
					try
					{
						rt.join();
						//System.out.println(((Runtime.getRuntime().freeMemory() + maxMemory - Runtime.getRuntime().totalMemory()) * 100.0) / (maxMemory * 1.0) + "% free");
						break;
					}
					catch(InterruptedException e) {}
				}
			}
		}
	}
	
	protected static final class ReduceThread extends ThreadPoolThread
	{
		protected volatile DiskBackedCollection collection;
		
		public ReduceThread(DiskBackedCollection collection)
		{
			this.collection = collection;
		}
		
		public final void run()
		{
			try
			{
				collection.reduceResources();
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
		}
	}
	
	protected static final class ImportThread extends ThreadPoolThread
	{
		protected volatile DiskBackedCollection collection;
		
		public ImportThread(DiskBackedCollection collection)
		{
			this.collection = collection;
		}
		
		public final void run()
		{
			try
			{
				((DiskBackedHashMap)collection).importResources();
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
		}
	}
	
	public static DiskBackedHashMap newDiskBackedHashMap(boolean indexed, int estimate)
	{
		DiskBackedHashMap map = new DiskBackedHashMap(indexed, estimate);
		collections.add(map);
		return map;
	}
	
	public static DiskBackedHashSet newDiskBackedHashSet(boolean iterate, int estimate)
	{
		DiskBackedHashSet set = new DiskBackedHashSet(iterate, estimate);
		return set;
	}
	
	public static DiskBackedArray newDiskBackedArray(int estimate)
	{
		DiskBackedArray array = new DiskBackedArray(false, estimate);
		return array;
	}
	
	public static DiskBackedArray newDiskBackedArray(boolean indexed, int estimate)
	{
		DiskBackedArray array = new DiskBackedArray(indexed, estimate);
		return array;
	}
	
	protected static abstract class DiskBackedCollection
	{
		public abstract void reduceResources() throws IOException;
	}
	
	public static final class DiskBackedHashSet extends DiskBackedCollection 
	{
		protected volatile DiskBackedHashMap internal;
		protected AtomicLong count = new AtomicLong(0);
		protected boolean iterate;
		protected volatile DiskBackedArray internal2;
		
		public DiskBackedHashSet(boolean iterate, int estimate)
		{
			internal = ResourceManager.newDiskBackedHashMap(false, estimate);
			this.iterate = iterate;
			if (iterate)
			{
				internal2 = ResourceManager.newDiskBackedArray(estimate);
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
		}
		
		public void reduceResources() throws IOException
		{
			internal.reduceResources();
		}
		
		public boolean add(ArrayList<Object> val)
		{
			Object newVal = val;
			try
			{
				if (((ArrayList)val).size() == 1)
				{
					newVal = ((ArrayList)val).get(0);
				}
				else if (val.size() == 0)
				{}
				else
				{
					StringBuilder val2 = new StringBuilder();
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
				
				long hash = hash(newVal) & 0x0EFFFFFFFFFFFFFFL;
				
				synchronized(internal)
				{
					ArrayList<Object> chain = (ArrayList<Object>)internal.get(new Long(hash));
			
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
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(1);
			}
			
			return false;
		}
		
		public void removeObject(Object val)
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
					{}
					else
					{
						StringBuilder val2 = new StringBuilder();
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
				long hash = hash(val) & 0x0EFFFFFFFFFFFFFFL;
				
				synchronized(internal)
				{
					ArrayList<Object> chain = (ArrayList<Object>)internal.get(new Long(hash));
			
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
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(1);
			}
		}
		
		public boolean contains(Object val)
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
					{}
					else
					{
						StringBuilder val2 = new StringBuilder();
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
				
				long hash = hash(val) & 0x0EFFFFFFFFFFFFFFL;
				ArrayList<Object> chain = null;
				chain = (ArrayList<Object>)internal.get(hash);
				
				if (chain == null)
				{
					return false;
				}
				
				return chain.contains(val);
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(1);
			}
			
			return false;
		}
		
		public long size()
		{
			return count.get();
		}
		
		public DiskBackedArray getArray()
		{
			return internal2;
		}
	}
	
	protected static long hash(Object key)
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
	
	public static final class DiskBackedArray extends DiskBackedCollection implements Iterable
	{
		protected volatile DiskBackedHashMap internal;
		protected AtomicLong index = new AtomicLong(0);
		
		public DiskBackedArray(boolean indexed, int estimate)
		{
			internal = ResourceManager.newDiskBackedHashMap(indexed, estimate);
		}
		
		public boolean contains(Object val)
		{
			return internal.containsValue(val);
		}
		
		public void add(ArrayList<Object> o) throws Exception
		{
			long myIndex = index.getAndIncrement();
			internal.put(myIndex, o);
		}
		
		public void clear()
		{
			internal.clear();
			index.set(0);
		}
		
		public void update(long index, ArrayList<Object> o) throws Exception
		{
			internal.update(index, o);
		}
		
		public long size()
		{
			return internal.size();
		}
		
		public Object get(long index) throws ClassNotFoundException, IOException
		{
			return internal.get(index);
		}
		
		public void reduceResources() throws IOException
		{
			internal.reduceResources();
		}
		
		public void close() throws IOException
		{
			internal.close();
		}

		@Override
		public Iterator iterator() {
			return new DiskBackedArrayIterator(this);
		}
		
		/*public void concat(DiskBackedArray array) throws Exception
		{
			for (Object o : array)
			{
				this.add(o);
			}
		} */
	}
	
	public static final class DiskBackedArrayIterator implements Iterator
	{
		protected volatile DiskBackedArray array;
		protected long index = 0;
		
		public DiskBackedArrayIterator(DiskBackedArray array)
		{
			this.array = array;
		}

		@Override
		public boolean hasNext() {
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
			catch(Exception e)
			{
				e.printStackTrace();
				return null;
			}
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
			
		}
	}
	
	public static final class DiskBackedHashMap extends DiskBackedCollection
	{
		protected final InternalConcurrentHashMap internal;
		protected final AtomicLong size = new AtomicLong(0);
		protected volatile ArrayList<FileChannel> ofcs = new ArrayList<FileChannel>(TEMP_COUNT);
		protected final ArrayList<AtomicLong> ofcSizes = new ArrayList<AtomicLong>(TEMP_COUNT);
		protected volatile ArrayList<Vector<FileChannel>> ifcsArrayList = new ArrayList<Vector<FileChannel>>(TEMP_COUNT);
		protected final ArrayList<Vector<Boolean>> locksArrayList = new ArrayList<Vector<Boolean>>(TEMP_COUNT);
		public final ReadWriteLock lock = new ReentrantReadWriteLock();
		protected AtomicLong ctCount;
		protected final boolean indexed;
		protected volatile ReverseConcurrentHashMap valueIndex;
		protected volatile DiskBackedHashSet diskValueIndex;
		protected long id;
		protected volatile boolean closed = false;
		protected volatile boolean filesAllocated = false;
		protected int estimate;
		protected Object IALock = new Boolean(false);
		protected volatile LongPrimitiveConcurrentHashMap index;
		
		public void clear()
		{
			lock.readLock().lock();
			internal.clear();
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
		
		public DiskBackedHashMap(boolean indexed, int estimate)
		{
			this.indexed = indexed;
			estimate = estimate / 1024;
			if (estimate < 10)
			{
				estimate = 10;
			}
			//System.out.println("Estimate is " + estimate);
			//if (estimate * 16 * 30 * 35 < 0.25 * maxMemory)
			//{
			//	this.estimate = estimate;
			//}
			//else
			//{
			//	this.estimate = (int)((5.0 / 350000.0) * maxMemory);
			//}
			internal = new InternalConcurrentHashMap((int)(1.5 * estimate), 1.0f, cpus*6); //FIXME
			if (indexed)
			{
				valueIndex = new ReverseConcurrentHashMap((int)(1.5 * estimate), 1.0f, cpus*6);
				diskValueIndex = ResourceManager.newDiskBackedHashSet(false, estimate);
			}

			id = idGen.getAndIncrement();
		}
		
		public boolean containsValue(Object val)
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
			//System.out.println("Calling diskContainsValue()");
			if (diskValueIndex.contains(val))
			{
				return true;
			}
			
			return false;
		}
		
		protected void remove(long index)
		{
			lock.readLock().lock();
			ArrayList<Object> o = internal.remove(index);
			if (o != null)
			{
				lock.readLock().unlock();
				return;
			}

			try
			{
				removeFromDisk(index);
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(1);
			}
			lock.readLock().unlock();
		}
		
		public void update(Long key, ArrayList<Object> val) throws Exception
		{
			ArrayList<Object> o = internal.replace(key, val);
			if (o != null)
			{
				return;
			}
			
			//removeFromDisk(key);
			putNoSize(key,  val);
			return;
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
				long r = index.get(key);
				if (r != -1)
				{
					return r;
				}
			}
			
			this.lock.readLock().lock();
			if (this.index != null)
			{
				long r = this.index.get(key);
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
		
		public final void put(Long key, ArrayList<Object> val) throws Exception
		{
			if (lowMem)
			{
				putToDisk(key, val);
				//System.out.println("Wrote key " + key + " to disk");
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
		
		public final void putNoSize(Long key, ArrayList<Object> val) throws Exception
		{
			if (lowMem)
			{
				putToDisk(key, val);
				//System.out.println("Wrote key " + key + " to disk");
				return;
			}
			
			internal.put(key,  val);
			
			if (indexed)
			{
				valueIndex.put(val,  key);
			}
		}
		
		public final ArrayList<Object> get(Long key) throws IOException, ClassNotFoundException
		{
			ArrayList<Object> o = null;
			o = internal.get(key);
			if (o != null)
			{
				return o;
			}
			
			return (ArrayList<Object>)getFromDisk(key);
		}
		
		public final void reduceResources() throws IOException
		{	
			if (NO_OFFLOAD.get() != 0)
			{
				try
				{
					Thread.sleep(SLEEP_TIME);
				}
				catch(Exception e)
				{}
				return;
			}
			lock.writeLock().lock();
			InternalConcurrentHashMap.EntrySet set = internal.entrySet();
			long size = set.size();
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
			
			//System.out.println("Going to reduce " + num2Cut + "/" + size + " entries.");
			int i = 0;
			int j = 0;
			InternalConcurrentHashMap.EntryIterator it = (InternalConcurrentHashMap.EntryIterator)set.iterator();
			int CT_SIZE = num2Cut / NUM_CTHREADS;
			if (CT_SIZE < MIN_CT_SIZE)
			{
				CT_SIZE = MIN_CT_SIZE;
			}
			ArrayList<InternalConcurrentHashMap.WriteThroughEntry> entries = new ArrayList<InternalConcurrentHashMap.WriteThroughEntry>(CT_SIZE);
			ArrayList<ThreadPoolThread> threads = new ArrayList<ThreadPoolThread>(num2Cut / CT_SIZE + 1);
			
			while (it.hasNext())
			{
				InternalConcurrentHashMap.WriteThroughEntry entry = (InternalConcurrentHashMap.WriteThroughEntry)it.next();
				entries.add(entry);
				i++;
				j++;
				
				//if (i % 10000 == 0)
				//{
				//	System.out.println("Marked " + i + "/" + num2Cut + " entries for reduction.");
				//}
				
				if (j == CT_SIZE)
				{
					CleanerThread ct = new CleanerThread(entries);
					threads.add(ct);
					j = 0;
					entries = new ArrayList<InternalConcurrentHashMap.WriteThroughEntry>(CT_SIZE);
				}
					
				if (i == num2Cut)
				{
					if (entries.size() != 0)
					{
						CleanerThread ct = new CleanerThread(entries);
						threads.add(ct);
					}
					break;
				}
			}
				
			it = null;
			set = null;
			ctCount = new AtomicLong(0);
			for (ThreadPoolThread t : threads)
			{
				t.start();
			}
			
			Iterator<ThreadPoolThread> it2 = threads.iterator();
			while (it2.hasNext())
			{
				ThreadPoolThread t = it2.next();
				while (true)
				{
					try
					{
						t.join();
						it2.remove();
						break;
					}
					catch(InterruptedException e)
					{}
				}
			}
			
			lock.writeLock().unlock();
			
			if (num2Cut == size)
			{
				try
				{
					Thread.sleep(SLEEP_TIME);
				}
				catch(InterruptedException e)
				{}
			}
		}
		
		protected final class CleanerThread extends ThreadPoolThread
		{
			protected ArrayList<InternalConcurrentHashMap.WriteThroughEntry> entries;
			
			public CleanerThread(ArrayList<InternalConcurrentHashMap.WriteThroughEntry> entries)
			{
				this.entries = entries;
			}
			
			public final void run()
			{
				try
				{
					putToDisk(entries);
				}
				catch(Exception e)
				{
					e.printStackTrace();
				}
			}
		}
		
		public final void importResources() throws IOException
		{	
			if (NO_OFFLOAD.get() != 0)
			{
				try
				{
					Thread.sleep(SLEEP_TIME);
				}
				catch(Exception e)
				{}
				return;
			}
			
			if (index == null)
			{
				return;
			}
			
			lock.writeLock().lock();
			EntrySet set = index.entrySet();
			long size = set.size();
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
				catch(Exception e)
				{}
				return;
			}
			
			//System.out.println("Going to reduce " + num2Cut + "/" + size + " entries.");
			int i = 0;
			int j = 0;
			EntryIterator it = set.iterator();
			int CT_SIZE = num2Cut / NUM_CTHREADS;
			if (CT_SIZE < MIN_CT_SIZE)
			{
				CT_SIZE = MIN_CT_SIZE;
			}
			ArrayList<WriteThroughEntry> entries = new ArrayList<WriteThroughEntry>(CT_SIZE);
			ArrayList<ThreadPoolThread> threads = new ArrayList<ThreadPoolThread>(num2Cut / CT_SIZE + 1);
			
			while (it.hasNext())
			{
				WriteThroughEntry entry = it.next();
				entries.add(entry);
				i++;
				j++;
				
				//if (i % 10000 == 0)
				//{
				//	System.out.println("Marked " + i + "/" + num2Cut + " entries for reduction.");
				//}
				
				if (j == CT_SIZE)
				{
					Import2Thread ct = new Import2Thread(entries);
					threads.add(ct);
					j = 0;
					entries = new ArrayList<WriteThroughEntry>(CT_SIZE);
				}
					
				if (i == num2Cut)
				{
					if (entries.size() != 0)
					{
						Import2Thread ct = new Import2Thread(entries);
						threads.add(ct);
					}
					break;
				}
			}
				
			it = null;
			set = null;
			ctCount = new AtomicLong(0);
			for (ThreadPoolThread t : threads)
			{
				t.start();
			}
			
			Iterator<ThreadPoolThread> it2 = threads.iterator();
			while (it2.hasNext())
			{
				ThreadPoolThread t = it2.next();
				while (true)
				{
					try
					{
						t.join();
						it2.remove();
						break;
					}
					catch(InterruptedException e)
					{}
				}
			}
			
			lock.writeLock().unlock();
		}
		
		protected final class Import2Thread extends ThreadPoolThread
		{
			protected ArrayList<WriteThroughEntry> entries;
			
			public Import2Thread(ArrayList<WriteThroughEntry> entries)
			{
				this.entries = entries;
			}
			
			public final void run()
			{
				try
				{
					for (WriteThroughEntry entry : entries)
					{
						ArrayList<Object> o = (ArrayList<Object>)getFromDisk((Long)entry.getKey());
						internal.put((Long)entry.getKey(), o);
						//putToIndex((Long)entry.getKey(), indexValue + bb.position());
						if (indexed)
						{
							valueIndex.put((ArrayList<Object>)o, (Long)entry.getKey());
							diskValueIndex.removeObject(o);
						}
						removeFromDisk((Long)entry.getKey());
					}
				}
				catch(Exception e)
				{
					e.printStackTrace();
				}
			}
		}
		
		public final Object getFromDisk(Long keyVal) throws IOException, ClassNotFoundException
		{	
			//ByteBuffer keyBB = ByteBuffer.allocate(8);
			//keyBB.putLong(keyVal.longValue());
			//byte[] key = new byte[8];
			//keyBB.position(0);
			//keyBB.get(key);
			//byte[] result = indexes.get((int)(keyVal % TEMP_COUNT)).get(key);
			if (!filesAllocated)
			{
				return null;
			}
			
			long resultVal = getFromIndex(keyVal);
			if (resultVal == 0)
			{
				return null;
			}
			
			//if (result == null)
			//{
			//	//System.out.println("Cannot find key " + keyVal);
			//	return null;
			//}
			
			//ByteBuffer resultBB = ByteBuffer.wrap(result);
			//resultBB.position(0);
			//long resultVal = resultBB.getLong();
			
			/*fis.getChannel().position(resultVal);
			ObjectInputStream iis = new ObjectInputStream(new BufferedInputStream(fis));
			return iis.readObject();*/

			ByteBuffer object = null;
			FileChannelAndInt fcai = getFreeFC((int)(keyVal % TEMP_COUNT));
			FileChannel fc = fcai.fc;
			//synchronized(fc)
			{
				//long fcSize = fc.size();
				//if (resultVal + 4 > fcSize)
				//{
				//	ofcs.get((int)((keyVal % TEMP_COUNT))).force(true);
				//}

				//fc.position(resultVal);
				ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
				int count = 0;
				while (count < sizeBuffer.limit())
				{
					//count += fc.read(sizeBuffer);
					int temp = fc.read(sizeBuffer, resultVal);
					count += temp;
					resultVal += temp;
				}
				sizeBuffer.position(0);
				int size = sizeBuffer.getInt();
				//while (size == 0)
				//{
				//	ofcs.get((int)((keyVal % TEMP_COUNT))).force(true);
				//	sizeBuffer.position(0);
				//	fc.position(resultVal);
				//	count = 0;
				//	while (count < sizeBuffer.limit())
				//	{
				//		count += fc.read(sizeBuffer);
				//	}
				//	sizeBuffer.position(0);
				//	size = sizeBuffer.getInt();
				//}
				object = ByteBuffer.allocate(size);
				//if (resultVal + 4 + size > fcSize)
				//{
				//	ofcs.get((int)((keyVal % TEMP_COUNT))).force(true);
				//}
				count = 0;
				while (count < object.limit())
				{
					int temp = fc.read(object, resultVal);
					count += temp;
					resultVal += temp;
				}
				//object.position(0);
				//int numFields = object.getInt();
				//int failures = 0;
				//while (numFields == 0)
				//{
				//	ofcs.get((int)((keyVal % TEMP_COUNT))).force(true);
				//	object.position(0);
				//	fc.position(resultVal+4);
				//	count = 0;
				//	while (count < object.limit())
				//	{
				//		count += fc.read(object);
				//	}
				//	object.position(0);
				//	numFields = object.getInt();
				//	failures++;
				//	
				//	if (failures % 100 == 0)
				//	{
				//		System.out.println("Read object with numFields == 0, " + failures + " times.");
				//	}
				//}
			}
			freeFC(fc, (int)(keyVal % TEMP_COUNT), fcai.i);
			return fromBytes(object.array());
		}
		
		public final ArrayList<Object> getAllFromDisk() throws IOException, ClassNotFoundException
		{	
			if (!filesAllocated)
			{
				return null;
			}
			
			long siz = this.size() - this.internal.size();
			int s = (siz >= 0 && siz <= Integer.MAX_VALUE) ? (int)siz : Integer.MAX_VALUE;
			ArrayList<Object> retval = new ArrayList<Object>(s);
			EntryIterator it = this.index.entrySet().iterator();
			FileChannelAndInt[] fcais = new FileChannelAndInt[TEMP_COUNT];
			int i = 0;
			while (i < TEMP_COUNT)
			{
				fcais[i] = getFreeFC(i);
				i++;
			}
			
			while (it.hasNext())
			{
				WriteThroughEntry entry = it.next();
				long keyVal = entry.getKey();
				long resultVal = entry.getValue();

				ByteBuffer object = null;
				FileChannel fc = fcais[(int)(keyVal % TEMP_COUNT)].fc;
				//synchronized(fc)
				{
					//fc.position(resultVal);
					ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
					int count = 0;
					while (count < sizeBuffer.limit())
					{
						int temp = fc.read(sizeBuffer, resultVal);
						count += temp;
						resultVal += temp;
					}
					sizeBuffer.position(0);
					int size = sizeBuffer.getInt();
					object = ByteBuffer.allocate(size);
					count = 0;
					while (count < object.limit())
					{
						int temp = fc.read(object, resultVal);
						count += temp;
						resultVal += temp;
					}
				}
				retval.add(fromBytes(object.array()));
			}
			
			i = 0;
			for (FileChannelAndInt fcai : fcais)
			{
				freeFC(fcai.fc, i, fcai.i);
				i++;
			}
			return retval;
		}
		
		protected final long getFromIndex(long key) throws IOException
		{
			long retval = index.get(key);
			if (retval == -1)
			{
				return 0;
			}
			
			return retval;
		}
		
		public final void freeFC(FileChannel ifc, int hash, int i)
		{
			Vector<FileChannel> ifcs = ifcsArrayList.get(hash);
			Vector<Boolean> locks = locksArrayList.get(hash);
			FileChannel fc = ifcs.get(i);
			locks.set(i,  false);
			return;
		}
		
		public final FileChannelAndInt getFreeFC(int hash)
		{
			int i = 0;
			Vector<Boolean> locks = locksArrayList.get(hash);
			Vector<FileChannel> ifcs = ifcsArrayList.get(hash);
			while (i < locks.size())
			{
				boolean locked = locks.get(i);
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
				retval = new RandomAccessFile(TEMP_DIRS.get(hash) + "DBHM" + id + ".tmp", "r").getChannel();
				locks.add(true);
				ifcs.add(retval);
				i = 0;
				while (!ifcs.get(i).equals(retval))
				{
					i++;
				}
				return new FileChannelAndInt(retval, i);
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(1);
				return null;
			}
		}
		
		private static final class FileChannelAndInt
		{
			protected FileChannel fc;
			protected int i;
			
			public FileChannelAndInt(FileChannel fc, int i)
			{
				this.fc = fc;
				this.i = i;
			}
		}
		
		public boolean removeFromDisk(long keyVal) throws Exception
		{
			//ByteBuffer keyBB = ByteBuffer.allocate(8);
			//keyBB.putLong(keyVal);
			//byte[] key = new byte[8];
			//keyBB.position(0);
			//keyBB.get(key);
			Object o = null;
			if (indexed)
			{
				o = this.getFromDisk(keyVal);
			}
			
			long offset = removeFromIndex(keyVal);
			boolean retval = offset != 0;
			
			if (!retval)
			{
				return false;
			}
			
			if (indexed)
			{
				diskValueIndex.removeObject(((ArrayList<Object>)o));
			}
			
			return true;
		}
		
		protected long removeFromIndex(long keyVal) throws IOException
		{
			long retval = index.remove(keyVal);
			if (retval == -1)
			{
				return 0;
			}
			
			return retval;
		}
		
		public final void putToDisk(long eKey, Object eVal) throws Exception
		{	
			if (!filesAllocated)
			{
				synchronized(this)
				{
					if (!filesAllocated)
					{
						int i = 0;
						index = new LongPrimitiveConcurrentHashMap((int)(estimate * 1.5), 1.0f, cpus*6);
						while (i < TEMP_COUNT)
						{
							/*
							try
							{
								throw new Exception("Creation stack trace");
							}
							catch(Exception e)
							{
								StringWriter sw = new StringWriter();
								PrintWriter pw = new PrintWriter(sw);
								e.printStackTrace(pw);
								creations.put(id, sw.toString());
							}
							*/
							//Properties props = new Properties();
							//props.setProperty(RecordManagerOptions.THREAD_SAFE, "true");
							//props.setProperty(RecordManagerOptions.CACHE_TYPE, "none");
							//props.setProperty(RecordManagerOptions.AUTO_COMMIT, "true");
							//RecordManager recman = RecordManagerFactory.createRecordManager("JDBM" + id + "_" + i, props );
							//BTree tree = BTree.createInstance(recman)
							FileChannel ofc = new RandomAccessFile(TEMP_DIRS.get(i) + "DBHM" + id + ".tmp", "rw").getChannel();
							ofcs.add(ofc);
							AtomicLong ofcSize = new AtomicLong(1);
							ofcSizes.add(ofcSize);
							Vector<FileChannel> ifcs = new Vector<FileChannel>();
							ifcs.add(new RandomAccessFile(TEMP_DIRS.get(i) + "DBHM" + id + ".tmp", "r").getChannel());
							ifcsArrayList.add(ifcs);
							Vector<Boolean> locks = new Vector<Boolean>();
							locks.add(false);
							locksArrayList.add(locks);
							i++;
						}
						filesAllocated = true;
					}
				}
			}
			byte[] bytes = toBytes(eVal);
			long indexValue = ofcSizes.get((int)(eKey % TEMP_COUNT)).getAndAdd(bytes.length);		
			//ByteBuffer keyBB = ByteBuffer.allocate(8);
			//ByteBuffer dataBB = ByteBuffer.allocate(8);		
			//keyBB.putLong((Long)eKey);
			//dataBB.putLong((Long)indexValue);
			//byte[] key = new byte[8];
			//byte[] data = new byte[8];
			//keyBB.position(0);
			//dataBB.position(0);
			//keyBB.get(key);
			//dataBB.get(data);
			//indexes.get((int)(eKey % TEMP_COUNT)).insert(key, data);
			
			ByteBuffer bb = ByteBuffer.wrap(bytes);
			FileChannel ofci = ofcs.get((int)((eKey % TEMP_COUNT)));
			//synchronized(ofci)
			{
				//ofci.position(indexValue);
				long pos = indexValue;
				int count = 0;
				while (count < bb.limit())
				{
					int temp = ofci.write(bb, pos);
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
		
		public final void putToDisk(ArrayList<InternalConcurrentHashMap.WriteThroughEntry> entries) throws Exception
		{	
			if (!filesAllocated)
			{
				synchronized(this)
				{
					if (!filesAllocated)
					{
						int i = 0;
						index = new LongPrimitiveConcurrentHashMap((int)(estimate * 1.5), 1.0f, cpus*6);
						//index = new ConcurrentHashMap<Long, Long>(estimate, 0.75f, Runtime.getRuntime().availableProcessors() * 6);
						while (i < TEMP_COUNT)
						{
							//Properties props = new Properties();
							//props.setProperty(RecordManagerOptions.THREAD_SAFE, "true");
							//props.setProperty(RecordManagerOptions.CACHE_TYPE, "none");
							//props.setProperty(RecordManagerOptions.AUTO_COMMIT, "true");
							//RecordManager recman = RecordManagerFactory.createRecordManager("JDBM" + id + "_" + i, props );
							//BTree tree = BTree.createInstance(recman);
							FileChannel ofc = new RandomAccessFile(TEMP_DIRS.get(i) + "DBHM" + id + ".tmp", "rw").getChannel();
							ofcs.add(ofc);
							AtomicLong ofcSize = new AtomicLong(1);
							ofcSizes.add(ofcSize);
							Vector<FileChannel> ifcs = new Vector<FileChannel>();
							ifcs.add(new RandomAccessFile(TEMP_DIRS.get(i) + "DBHM" + id + ".tmp", "r").getChannel());
							ifcsArrayList.add(ifcs);
							Vector<Boolean> locks = new Vector<Boolean>();
							locks.add(false);
							locksArrayList.add(locks);
							i++;
						}
						filesAllocated = true;
					}
				}
			}
			ArrayList<InternalConcurrentHashMap.WriteThroughEntry>[] parts = new ArrayList[TEMP_COUNT];
			int i = 0;
			while (i < TEMP_COUNT)
			{
				parts[i] = new ArrayList<InternalConcurrentHashMap.WriteThroughEntry>();
				i++;
			}
			for (InternalConcurrentHashMap.WriteThroughEntry entry : entries)
			{
				parts[(int)(((Long)entry.getKey()) % TEMP_COUNT)].add(entry);
			}
			
			int index = 0;
			while (index < parts.length)
			{
				ArrayList<InternalConcurrentHashMap.WriteThroughEntry> part = parts[index]; 
				if (part.size() > 0)
				{
					byte[] bytes = arrayListToBytes(part);
					//System.out.println("Writing an buffer of size " + (bytes.length / 1024) + "K");
					long indexValue = ofcSizes.get((int)(((Long)part.get(0).getKey()) % TEMP_COUNT)).getAndAdd(bytes.length);		
					ByteBuffer bb = ByteBuffer.wrap(bytes);
					FileChannel ofci = ofcs.get((int)(((Long)part.get(0).getKey()) % TEMP_COUNT));
					//synchronized(ofci)
					{
						//ofci.position(indexValue);
						long pos = indexValue;
						int count = 0;
						while (count < bb.limit())
						{
							int temp = ofci.write(bb, pos);
							count += temp;
							pos += temp;
						}
					}
					
					//ArrayList<Map.Entry> list = new ArrayList<Map.Entry>();
					//bb.position(0);
					//for (Map.Entry entry : part)
					//{
					//	list.add(new AbstractMap.SimpleEntry((Long)entry.getKey(), indexValue + bb.position()));
					//	int size = bb.getInt();
					//	bb.position(bb.position() + size);
					//}
					//putToIndex(list);
					//list = null;
					
					bb.position(0);
					for (InternalConcurrentHashMap.WriteThroughEntry entry : part)
					{
						putToIndex((Long)entry.getKey(), indexValue + bb.position());
						if (indexed)
						{
							diskValueIndex.add(((ArrayList<Object>)entry.getValue()));
							ArrayList<Object> o = internal.get((Long)entry.getKey());
							valueIndex.remove(o);
						}
						internal.remove((Long)entry.getKey());
						int size = bb.getInt();
						bb.position(bb.position() + size);
					}
				}
				
				parts[index] = null;
				index++;
			}
		}
		
		protected final void putToIndex(long key, long value) throws Exception
		{
			index.put(key, value);
		}
		
		public synchronized void close() throws IOException
		{	
			//creations.remove(id);
			if (filesAllocated)
			{
				if (ofcs != null)
				{
					for (FileChannel ofc : ofcs)
					{
						ofc.close();
					}
				
					ofcs = null;
				}
			
				if (ifcsArrayList != null)
				{
					for (Vector<FileChannel> ifcs : ifcsArrayList)
					{
						for (FileChannel fc : ifcs)
						{
							fc.close();
						}
					}
				
					ifcsArrayList = null;
				}
				
				index = null;
				
				for (String TEMP_DIR : TEMP_DIRS)
				{
					if (!(new File(TEMP_DIR + "DBHM" + id + ".tmp").delete()))
					{
						//System.out.println("Delete of " + TEMP_DIR + "DBHM" + id + ".tmp failed");
					}
					
					if (!(new File(TEMP_DIR + "DBHMX" + id + ".tmp").delete()))
					{
						//System.out.println("Delete of " + TEMP_DIR + "DBHM" + id + ".tmp failed");
					}
				}
			}
			
			collections.remove(this);
				
			internal.clear();
			if (indexed)
			{
				valueIndex.clear();
				diskValueIndex.close();
			}
			
			closed = true;
		}
		
		public long size()
		{
			return size.get();
		}
		
		protected final Object fromBytes(byte[] val)
		{	
			ByteBuffer bb = ByteBuffer.wrap(val);
			int numFields = bb.getInt();
			
			if (numFields == 0)
			{
				return new ArrayList<Object>();
			}
			
			bb.position(bb.position() + numFields);
			byte[] bytes = bb.array();
			if (bytes[4] == 5)
			{
				return new DataEndMarker();
			}
			ArrayList<Object> retval = new ArrayList<Object>(numFields);
			int i = 0;
			while (i < numFields)
			{
				if (bytes[i+4] == 0)
				{
					//long
					Long o = bb.getLong();
					retval.add(o);
				}
				else if (bytes[i+4] == 1)
				{
					//integer
					Integer o = bb.getInt();
					retval.add(o);
				}
				else if (bytes[i+4] == 2)
				{
					//double
					Double o = bb.getDouble();
					retval.add(o);
				}
				else if (bytes[i+4] == 3)
				{
					//date
				 MyDate o = new MyDate(bb.getLong());
					retval.add(o);
				}
				else if (bytes[i+4] == 4)
				{
					//string
					int length = bb.getInt();
					byte[] temp = new byte[length];
					bb.get(temp);
					try
					{
						String o = new String(temp, "UTF-8");
						retval.add(o);
					}
					catch(Exception e)
					{
						e.printStackTrace();
						System.exit(1);
					}
				}
				else if (bytes[i+4] == 6)
				{
					//AtomicLong
					long o = bb.getLong();
					retval.add(new AtomicLong(o));
				}
				else if (bytes[i+4] == 7)
				{
					//AtomicDouble
					double o = bb.getDouble();
					retval.add(new AtomicDouble(o));
				}
				else if (bytes[i+4] == 8)
				{
					//Empty ArrayList
					retval.add(new ArrayList<Object>());
				}
				else
				{
					System.out.println("Unknown type in fromBytes()");
				}
				
				i++;
			}
			
			return retval;
		}
		
		protected final byte[] toBytes(Object v)
		{
			ArrayList<Object> val;
			if (v instanceof ArrayList)
			{
				val = (ArrayList<Object>)v;
			}
			else
			{
				byte[] retval = new byte[9];
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
			byte[] header = new byte[size];
			int i = 8;
			for (Object o : val)
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
					size += (4 + ((String)o).length());
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
					if (((ArrayList) o).size() != 0)
					{
						System.out.println("Non-zero size ArrayList in toBytes()");
						Thread.dumpStack();
						System.exit(1);
					}
					header[i] = (byte)8;
				}
				else
				{
					System.out.println("Unknown type " + o.getClass() + " in toyBytes()");
					System.out.println(o);
					System.exit(1);
				}
				
				i++;
			}
			
			byte[] retval = new byte[size];
		//	System.out.println("In toBytes(), row has " + val.size() + " columns, object occupies " + size + " bytes");
			System.arraycopy(header, 0, retval, 0, header.length);
			i = 8;
			ByteBuffer retvalBB = ByteBuffer.wrap(retval);
			retvalBB.putInt(size-4);
			retvalBB.putInt(val.size());
			retvalBB.position(header.length);
			for (Object o : val)
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
					catch(Exception e)
					{
						e.printStackTrace();
						System.exit(1);
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
				{}
				
				i++;
			}
			
			return retval;
		}
		
		protected final byte[] arrayListToBytes(ArrayList<InternalConcurrentHashMap.WriteThroughEntry> entries)
		{
			ArrayList<byte[]> results = new ArrayList<byte[]>(entries.size());
			for (InternalConcurrentHashMap.WriteThroughEntry entry : entries)
			{
				Object v = entry.getValue();
				ArrayList<Object> val;
				if (v instanceof ArrayList)
				{
					val = (ArrayList<Object>)v;
				}
				else
				{
					byte[] retval = new byte[9];
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
				byte[] header = new byte[size];
				int i = 8;
				for (Object o : val)
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
						size += (4 + ((String)o).length());
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
						if (((ArrayList) o).size() != 0)
						{
							System.out.println("Non-zero size ArrayList in toBytes()");
							Thread.dumpStack();
							System.exit(1);
						}
						header[i] = (byte)8;
					}
					else
					{
						System.out.println("Unknown type " + o.getClass() + " in toyBytes()");
						System.out.println(o);
						System.exit(1);
					}
				
					i++;
				}
			
				byte[] retval = new byte[size];
				//	System.out.println("In toBytes(), row has " + val.size() + " columns, object occupies " + size + " bytes");
				System.arraycopy(header, 0, retval, 0, header.length);
				i = 8;
				ByteBuffer retvalBB = ByteBuffer.wrap(retval);
				retvalBB.putInt(size-4);
				retvalBB.putInt(val.size());
				retvalBB.position(header.length);
				for (Object o : val)
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
						catch(Exception e)
						{
							e.printStackTrace();
							System.exit(1);
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
					{}
				
					i++;
				}
			
				results.add(retval);
			}
			
			int count = 0;
			for (byte[] ba : results)
			{
				count += ba.length;
			}
			byte[] retval = new byte[count];
			int retvalPos = 0;
			for (byte[] ba : results)
			{
				System.arraycopy(ba, 0, retval, retvalPos, ba.length);
				retvalPos += ba.length;
			}
			
			return retval;
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
		
		public Set<ArrayList<Object>> keySet()
		{
			keys.lock.readLock().lock();
			HashSet retval = new HashSet<ArrayList<Object>>((keys.size() >= 0 && keys.size() <= Integer.MAX_VALUE) ? (int)keys.size() : Integer.MAX_VALUE);
			//Values set = keys.internal.values();
			//ValueIterator it = set.iterator();
			//while (it.hasNext())
			//{
			//	retval.add(it.next());
			//}
			retval.addAll(keys.internal.values());
			
			if (keys.index != null)
			{
				//com.exascale.optimizer.testing.LongPrimitiveConcurrentHashMap.KeySet set2 = keys.index.keySet();
				//com.exascale.optimizer.testing.LongPrimitiveConcurrentHashMap.KeyIterator it2 = set2.iterator();
				//while (it2.hasNext())
				//{
				//	try
				//	{
				//		retval.add(keys.getFromDisk(it2.next()));
				//	}
				//	catch(Exception e)
				//	{
				//		e.printStackTrace();
				//		System.exit(1);
				//	}
				//}
				
				try
				{
					retval.addAll(keys.getAllFromDisk());
				}
				catch(Exception e)
				{
					e.printStackTrace();
					System.exit(1);
				}
			}
			
			return retval;
		}
		
		public V get(ArrayList<Object> key)
		{
			long plus = 0;
			long hash = hash(key);
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
					ArrayList<Object> result = values.internal.putIfAbsent(0x0EFFFFFFFFFFFFFFL & (hash + plus * plus), valueAL);
					if (result != null)
					{
						valueAL = result;
					}
				}
				values.lock.readLock().unlock();
				if (valueAL.size() == 1 && (valueAL.get(0) instanceof AtomicLong || valueAL.get(0) instanceof AtomicDouble))
				{
					Object o = valueAL.get(0);
					return (V)o;
				}
				else
				{
					return (V)valueAL;
				}
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(1);
			}
			
			return null;
		}
		
		public V debugGet(ArrayList<Object> key)
		{
			System.out.println("In debugGet() with key = " + key);
			System.out.println("Keys.size() = " + keys.size());
			System.out.println("Values.size() = " + values.size());  
			long plus = 0;
			long hash = hash(key);
			try
			{
				ArrayList<Object> current = keys.get(0x0EFFFFFFFFFFFFFFL & (hash + plus * plus));
				System.out.println("keys.get(" + (0x0EFFFFFFFFFFFFFFL & (hash + plus * plus)) + ") = " + current);
				if (current == null)
				{
					com.exascale.optimizer.testing.InternalConcurrentHashMap.EntryIterator it = keys.internal.entrySet().iterator();
					while (it.hasNext())
					{
						com.exascale.optimizer.testing.InternalConcurrentHashMap.WriteThroughEntry entry = it.next();
						if (entry.getValue().equals(key))
						{
							System.out.println("But this key was found at hash = " + entry.getKey());
							System.out.println("with value " + values.get(entry.getKey()));
							return null;
						}
					}
					
					System.out.println("The key was not found in the internal key store");
					if (keys.index != null)
					{
						EntryIterator it2 = keys.index.entrySet().iterator();
						while (it2.hasNext())
						{
							WriteThroughEntry entry = it2.next();
							if (keys.getFromDisk(entry.getKey()).equals(key))
							{
								System.out.println("But this key was found at hash = " + entry.getKey());
								System.out.println("with value " + values.get(entry.getKey()));
								return null;
							}
						}
					}
					return null;
				}
				while (!current.equals(key))
				{
					plus++;
					current = keys.get(0x0EFFFFFFFFFFFFFFL & (hash + plus * plus));
					System.out.println("keys.get(" + (0x0EFFFFFFFFFFFFFFL & (hash + plus * plus)) + ") = " + current);
					if (current == null)
					{
						com.exascale.optimizer.testing.InternalConcurrentHashMap.EntryIterator it = keys.internal.entrySet().iterator();
						while (it.hasNext())
						{
							com.exascale.optimizer.testing.InternalConcurrentHashMap.WriteThroughEntry entry = it.next();
							if (entry.getValue().equals(key))
							{
								System.out.println("But this key was found at hash = " + entry.getKey());
								System.out.println("with value " + values.get(entry.getKey()));
								return null;
							}
						}
						
						System.out.println("The key was not found in the internal key store");
						if (keys.index != null)
						{
							EntryIterator it2 = keys.index.entrySet().iterator();
							while (it2.hasNext())
							{
								WriteThroughEntry entry = it2.next();
								if (keys.getFromDisk(entry.getKey()).equals(key))
								{
									System.out.println("But this key was found at hash = " + entry.getKey());
									System.out.println("with value " + values.get(entry.getKey()));
									return null;
								}
							}
						}
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
					ArrayList<Object> result = values.internal.putIfAbsent(0x0EFFFFFFFFFFFFFFL & (hash + plus * plus), valueAL);
					if (result != null)
					{
						valueAL = result;
					}
				}
				values.lock.readLock().unlock();
				if (valueAL.size() == 1 && (valueAL.get(0) instanceof AtomicLong || valueAL.get(0) instanceof AtomicDouble))
				{
					Object o = valueAL.get(0);
					return (V)o;
				}
				else
				{
					return (V)valueAL;
				}
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(1);
			}
			
			return null;
		}
		
		public Object putIfAbsent(ArrayList<Object> key, V value)
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
			long hash = hash(key);
			try
			{
				while (true) 
				{
					Object retval = keys.putIfAbsent(0x0EFFFFFFFFFFFFFFL & (hash + plus * plus), key);
					if (retval == null)
					{
						break;
					}
					
					ArrayList<Object> r = keys.get(0x0EFFFFFFFFFFFFFFL & (hash + plus * plus));
					if (r.equals(key))
					{
						return retval;
					}
					plus++;
				}
			
				values.put(0x0EFFFFFFFFFFFFFFL & (hash + plus * plus), temp);
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(1);
			}
			
			return null;
		}
		
		public void close()
		{
			try
			{
				keys.close();
				values.close();
			}
			catch(Exception e)
			{}
		}
		
		public void put(ArrayList<Object> key, V value)
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
			long hash = hash(key);
			try
			{
				while (keys.putIfAbsent(0x0EFFFFFFFFFFFFFFL & (hash + plus * plus), key) != null)
				{
					plus++;
				}
			
				values.put(0x0EFFFFFFFFFFFFFFL & (hash + plus * plus), temp);
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(1);
			}
		}
	}
}
