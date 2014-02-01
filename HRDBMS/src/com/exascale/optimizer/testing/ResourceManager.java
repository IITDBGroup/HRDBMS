package com.exascale.optimizer.testing;

import gnu.trove.map.hash.TCustomHashMap;

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
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import sun.misc.Unsafe;
import com.exascale.optimizer.testing.LongPrimitiveConcurrentHashMap.EntryIterator;
import com.exascale.optimizer.testing.LongPrimitiveConcurrentHashMap.EntrySet;
import com.exascale.optimizer.testing.LongPrimitiveConcurrentHashMap.WriteThroughEntry;
import com.exascale.optimizer.testing.MetaData.PartitionMetaData;

public class ResourceManager extends ThreadPoolThread
{
	protected static int SLEEP_TIME = 10000;
	protected static int LOW_PERCENT_FREE = 30;
	protected static int HIGH_PERCENT_FREE = 50;
	protected static int PERCENT_TO_CUT = 1;
	protected static volatile Vector<DiskBackedCollection> collections = new Vector<DiskBackedCollection>();
	protected static volatile boolean lowMem = false;
	protected static final int TEMP_COUNT = 2;
	protected static ArrayList<String> TEMP_DIRS;
	protected static final int MIN_CT_SIZE = 20000;
	protected static final int NUM_CTHREADS = 16;
	protected static final Long LARGE_PRIME =  1125899906842597L;
    protected static final Long LARGE_PRIME2 = 6920451961L;
    protected static AtomicLong idGen = new AtomicLong(0);
    protected static HashMap<Long, String> creations = new HashMap<Long, String>();
    protected static volatile boolean hasBeenLowMem = false;
    protected static boolean PROFILE = false;
    public static int cpus;
    public static final ExecutorService pool;
    public static final AtomicInteger objID = new AtomicInteger(0);
    protected static final ConcurrentHashMap<String, WeakReference<String>> internStringMap = new ConcurrentHashMap<String, WeakReference<String>>(1024, 1.0f, 64 * Runtime.getRuntime().availableProcessors());
    protected static final ConcurrentHashMap<Long, WeakReference<Long>> internLongMap = new ConcurrentHashMap<Long, WeakReference<Long>>(1024, 1.0f, 64 * Runtime.getRuntime().availableProcessors());
    protected static final ConcurrentHashMap<Integer, WeakReference<Integer>> internIntMap = new ConcurrentHashMap<Integer, WeakReference<Integer>>(1024, 1.0f, 64 * Runtime.getRuntime().availableProcessors());
    protected static final ConcurrentHashMap<Double, WeakReference<Double>> internDoubleMap = new ConcurrentHashMap<Double, WeakReference<Double>>(1024, 1.0f, 64 * Runtime.getRuntime().availableProcessors());
    protected static final ConcurrentHashMap<Date, WeakReference<Date>> internDateMap = new ConcurrentHashMap<Date, WeakReference<Date>>(1024, 1.0f, 64 * Runtime.getRuntime().availableProcessors());
    
    static
    {
    	pool = Executors.newCachedThreadPool();
    }
    
    public static String internString(final String str)
    {
        final WeakReference<String> cached = internStringMap.get(str);
        if (cached != null)
        {
            final String value = cached.get();
            if (value != null)
                return value;
        }
        internStringMap.put(str, new WeakReference<String>(str));
        return str;
    }
    
    public static Integer internInt(final Integer str)
    {
        final WeakReference<Integer> cached = internIntMap.get(str);
        if (cached != null)
        {
            final Integer value = cached.get();
            if (value != null)
                return value;
        }
        internIntMap.put(str, new WeakReference<Integer>(str));
        return str;
    }
    
    public static Double internDouble(final Double str)
    {
        final WeakReference<Double> cached = internDoubleMap.get(str);
        if (cached != null)
        {
            final Double value = cached.get();
            if (value != null)
                return value;
        }
        internDoubleMap.put(str, new WeakReference<Double>(str));
        return str;
    }
    
    public static Date internDate(final Date str)
    {
        final WeakReference<Date> cached = internDateMap.get(str);
        if (cached != null)
        {
            final Date value = cached.get();
            if (value != null)
                return value;
        }
        internDateMap.put(str, new WeakReference<Date>(str));
        return str;
    }
    
    public static Long internLong(final Long str)
    {
        final WeakReference<Long> cached = internLongMap.get(str);
        if (cached != null)
        {
            final Long value = cached.get();
            if (value != null)
                return value;
        }
        internLongMap.put(str, new WeakReference<Long>(str));
        return str;
    }
	
	public ResourceManager()
	{
		TEMP_DIRS = new ArrayList<String>(TEMP_COUNT);
		TEMP_DIRS.add("/temp3/");
		TEMP_DIRS.add("/home/hrdbms/");
		cpus = Runtime.getRuntime().availableProcessors();
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
	
	private static class ProfileThread extends ThreadPoolThread
	{
		protected HashMap<CodePosition, CodePosition> counts = new HashMap<CodePosition, CodePosition>();
		long samples = 0;
		
		private static class CodePosition implements Comparable
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
	
	private static class DeadlockThread extends ThreadPoolThread
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
	
	private static class MonitorThread extends ThreadPoolThread
	{
		public void run()
		{
			while (true)
			{
				System.out.println(((Runtime.getRuntime().freeMemory() + Runtime.getRuntime().maxMemory() - Runtime.getRuntime().totalMemory()) * 100.0) / (Runtime.getRuntime().maxMemory() * 1.0) + "% free");
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
		return ((Runtime.getRuntime().freeMemory() + Runtime.getRuntime().maxMemory() - Runtime.getRuntime().totalMemory()) * 100.0) / (Runtime.getRuntime().maxMemory() * 1.0) < LOW_PERCENT_FREE;
	}
	
	protected static boolean highMem()
	{
		return ((Runtime.getRuntime().freeMemory() + Runtime.getRuntime().maxMemory() - Runtime.getRuntime().totalMemory()) * 100.0) / (Runtime.getRuntime().maxMemory() * 1.0) > HIGH_PERCENT_FREE;
	}
	
	protected static void handleLowMem()
	{
	//	System.gc(); 
		while (lowMem())
		{	
			CleanInternThread cit1 = new CleanInternThread(internIntMap);
			CleanInternThread cit2 = new CleanInternThread(internLongMap);
			CleanInternThread cit3 = new CleanInternThread(internDoubleMap);
			CleanInternThread cit4 = new CleanInternThread(internStringMap);
			CleanInternThread cit5 = new CleanInternThread(internDateMap);
			cit1.start();
			cit2.start();
			cit3.start();
			cit4.start();
			cit5.start();
			ArrayList<ThreadPoolThread> threads = new ArrayList<ThreadPoolThread>(collections.size());
			///System.out.println(((Runtime.getRuntime().freeMemory() + Runtime.getRuntime().maxMemory() - Runtime.getRuntime().totalMemory()) * 100.0) / (Runtime.getRuntime().maxMemory() * 1.0) + "% free");
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
						//System.out.println(((Runtime.getRuntime().freeMemory() + Runtime.getRuntime().maxMemory() - Runtime.getRuntime().totalMemory()) * 100.0) / (Runtime.getRuntime().maxMemory() * 1.0) + "% free");
						break;
					}
					catch(InterruptedException e) {}
				}
			}
			
			try
			{
				cit1.join();
				cit2.join();
				cit3.join();
				cit4.join();
				cit5.join();
			}
			catch(Exception e)
			{}
		}
	}
	
	private static class CleanInternThread extends ThreadPoolThread
	{
		private ConcurrentHashMap map;
		
		public CleanInternThread(ConcurrentHashMap map)
		{
			this.map = map;
		}
		
		public void run()
		{
			Iterator it = map.entrySet().iterator();
			while (it.hasNext())
			{
				Map.Entry entry = (Map.Entry)it.next();
				if (((WeakReference)entry.getValue()).get() == null)
				{
					it.remove();
				}
			}
		}
	}
	
	protected static void handleHighMem()
	{
	//	System.gc(); 
		while (highMem())
		{	
			ArrayList<ThreadPoolThread> threads = new ArrayList<ThreadPoolThread>(collections.size());
			///System.out.println(((Runtime.getRuntime().freeMemory() + Runtime.getRuntime().maxMemory() - Runtime.getRuntime().totalMemory()) * 100.0) / (Runtime.getRuntime().maxMemory() * 1.0) + "% free");
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
						//System.out.println(((Runtime.getRuntime().freeMemory() + Runtime.getRuntime().maxMemory() - Runtime.getRuntime().totalMemory()) * 100.0) / (Runtime.getRuntime().maxMemory() * 1.0) + "% free");
						break;
					}
					catch(InterruptedException e) {}
				}
			}
		}
	}
	
	protected static class ReduceThread extends ThreadPoolThread
	{
		protected volatile DiskBackedCollection collection;
		
		public ReduceThread(DiskBackedCollection collection)
		{
			this.collection = collection;
		}
		
		public void run()
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
	
	protected static class ImportThread extends ThreadPoolThread
	{
		protected volatile DiskBackedCollection collection;
		
		public ImportThread(DiskBackedCollection collection)
		{
			this.collection = collection;
		}
		
		public void run()
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
	
	public static class DiskBackedHashSet extends DiskBackedCollection 
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
		
		protected long hash(Object e)
		{
			long hashCode = 1125899906842597L;
			long eHash = 1;
			if (e instanceof Integer)
			{
				long i = ((Integer)e).longValue();
				// Spread out values
			    long scaled = i * LARGE_PRIME;
			    // Fill in the lower bits
			    eHash = scaled + LARGE_PRIME2;
			}
			else if (e instanceof Long)
			{
				long i = (Long)e;
				// Spread out values
			    long scaled = i * LARGE_PRIME;
			    // Fill in the lower bits
			    eHash = scaled + LARGE_PRIME2;
			}
			else if (e instanceof String)
			{
				String string = (String)e;
				long h = 1125899906842597L; // prime
				int len = string.length();

				for (int i = 0; i < len; i++) 
				{
					h = 31*h + string.charAt(i);
				}
				eHash = h;
			}
			else if (e instanceof Double)
			{
				long i = Double.doubleToLongBits((Double)e);
				// Spread out values
			    long scaled = i * LARGE_PRIME;
			    // Fill in the lower bits
			    eHash = scaled + LARGE_PRIME2;
			}
			else if (e instanceof Date)
			{
				long i = ((Date)e).getTime();
				// Spread out values
			    long scaled = i * LARGE_PRIME;
			    // Fill in the lower bits
			    eHash = scaled + LARGE_PRIME2;
			}
			else
			{
				eHash = e.hashCode();
			}
				
			return eHash;
		}
	}
	
	public static class DiskBackedArray extends DiskBackedCollection implements Iterable
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
	
	public static class DiskBackedArrayIterator implements Iterator
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
	
	public static class DiskBackedHashMap extends DiskBackedCollection
	{
		protected volatile InternalConcurrentHashMap internal;
		protected AtomicLong size = new AtomicLong(0);
		protected volatile ArrayList<FileChannel> ofcs = new ArrayList<FileChannel>(TEMP_COUNT);
		protected volatile ArrayList<AtomicLong> ofcSizes = new ArrayList<AtomicLong>(TEMP_COUNT);
		protected volatile ArrayList<Vector<FileChannel>> ifcsArrayList = new ArrayList<Vector<FileChannel>>(TEMP_COUNT);
		protected volatile ArrayList<Vector<Boolean>> locksArrayList = new ArrayList<Vector<Boolean>>(TEMP_COUNT);
		protected volatile ReadWriteLock lock = new ReentrantReadWriteLock();
		protected AtomicLong ctCount;
		protected boolean indexed;
		protected volatile ReverseConcurrentHashMap valueIndex;
		protected volatile DiskBackedHashSet diskValueIndex;
		protected long id;
		protected volatile boolean closed = false;
		protected volatile boolean filesAllocated = false;
		protected int estimate;
		protected Object IALock = new Boolean(false);
		protected volatile LongPrimitiveConcurrentHashMap index;
		
		public DiskBackedHashMap(boolean indexed, int estimate)
		{
			this.indexed = indexed;
			estimate = estimate / 1024;
			if (estimate * 16 * 30 * 35 < 0.25 * Runtime.getRuntime().maxMemory())
			{
				this.estimate = estimate;
			}
			else
			{
				this.estimate = (int)((5.0 / 350000.0) * Runtime.getRuntime().maxMemory());
			}
			internal = new InternalConcurrentHashMap(estimate / 16, 16.0f, cpus*6);
			if (indexed)
			{
				valueIndex = new ReverseConcurrentHashMap(estimate / 16, 16.0f, cpus*6);
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
			lock.readLock().lock();
			ArrayList<Object> o = internal.replace(key, val);
			lock.readLock().unlock();
			if (o != null)
			{
				return;
			}
			
			removeFromDisk(key);
			putNoSize(key,  val);
			return;
		}
		
		public Object putIfAbsent(Long key, ArrayList<Object> val) throws Exception
		{	
			if (!lowMem)
			{
				ArrayList<Object> retval = internal.get(key);
				if (retval != null)
				{
					return retval;
				}
			
				this.lock.readLock().lock();
				retval = (ArrayList<Object>)this.getFromDisk(key);
				if (retval != null)
				{
					this.lock.readLock().unlock();
					return retval;
				}

				retval = internal.putIfAbsent(key,  val);
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
			else
			{
				synchronized(IALock)
				{
					Object retval = this.get(key);
					if (retval != null)
					{
						return retval;
					}
					
					putToDisk(key, val);
				}
				
				if (indexed)
				{
					valueIndex.put(val, key);
				}
				
				size.incrementAndGet();
				return null;
			}
		}
		
		public void put(Long key, ArrayList<Object> val) throws Exception
		{
			if (lowMem)
			{
				putToDisk(key, val);
				//System.out.println("Wrote key " + key + " to disk");
				size.incrementAndGet();
				return;
			}
			
			internal.put(key,  val);
			
			if (indexed)
			{
				valueIndex.put(val, key);
			}
			
			size.incrementAndGet();
		}
		
		public void putNoSize(Long key, ArrayList<Object> val) throws Exception
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
		
		public ArrayList<Object> get(Long key) throws IOException, ClassNotFoundException
		{
			ArrayList<Object> o = null;
			o = internal.get(key);
			if (o != null)
			{
				return o;
			}
			
			return (ArrayList<Object>)getFromDisk(key);
		}
		
		public void reduceResources() throws IOException
		{	
			lock.writeLock().lock();
			InternalConcurrentHashMap.EntrySet set = internal.entrySet();
			long size = set.size();
			int num2Cut = 0;
			if (size < MIN_CT_SIZE)
			{
				num2Cut = (int)size;
			}
			else
			{
				num2Cut = (size * PERCENT_TO_CUT / 100) > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int)(size * PERCENT_TO_CUT / 100);
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
		}
		
		protected class CleanerThread extends ThreadPoolThread
		{
			protected ArrayList<InternalConcurrentHashMap.WriteThroughEntry> entries;
			
			public CleanerThread(ArrayList<InternalConcurrentHashMap.WriteThroughEntry> entries)
			{
				this.entries = entries;
			}
			
			public void run()
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
		
		public void importResources() throws IOException
		{	
			if (index == null)
			{
				return;
			}
			
			lock.writeLock().lock();
			EntrySet set = index.entrySet();
			long size = set.size();
			int num2Cut = 0;
			if (size < MIN_CT_SIZE)
			{
				num2Cut = (int)size;
			}
			else
			{
				num2Cut = (size * PERCENT_TO_CUT / 100) > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int)(size * PERCENT_TO_CUT / 100);
			}
			
			if (num2Cut == 0)
			{
				lock.writeLock().unlock();
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
		
		protected class Import2Thread extends ThreadPoolThread
		{
			protected ArrayList<WriteThroughEntry> entries;
			
			public Import2Thread(ArrayList<WriteThroughEntry> entries)
			{
				this.entries = entries;
			}
			
			public void run()
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
		
		public Object getFromDisk(Long keyVal) throws IOException, ClassNotFoundException
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
			FileChannel fc = getFreeFC((int)(keyVal % TEMP_COUNT));
			synchronized(fc)
			{
				long fcSize = fc.size();
				if (resultVal + 4 > fcSize)
				{
					ofcs.get((int)((keyVal % TEMP_COUNT))).force(true);
				}

				fc.position(resultVal);
				ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
				int count = 0;
				while (count < sizeBuffer.limit())
				{
					count += fc.read(sizeBuffer);
				}
				sizeBuffer.position(0);
				int size = sizeBuffer.getInt();
				while (size == 0)
				{
					ofcs.get((int)((keyVal % TEMP_COUNT))).force(true);
					sizeBuffer.position(0);
					fc.position(resultVal);
					count = 0;
					while (count < sizeBuffer.limit())
					{
						count += fc.read(sizeBuffer);
					}
					sizeBuffer.position(0);
					size = sizeBuffer.getInt();
				}
				object = ByteBuffer.allocate(size);
				if (resultVal + 4 + size > fcSize)
				{
					ofcs.get((int)((keyVal % TEMP_COUNT))).force(true);
				}
				count = 0;
				while (count < object.limit())
				{
					count += fc.read(object);
				}
				object.position(0);
				int numFields = object.getInt();
				int failures = 0;
				while (numFields == 0)
				{
					ofcs.get((int)((keyVal % TEMP_COUNT))).force(true);
					object.position(0);
					fc.position(resultVal+4);
					count = 0;
					while (count < object.limit())
					{
						count += fc.read(object);
					}
					object.position(0);
					numFields = object.getInt();
					failures++;
					
					if (failures % 100 == 0)
					{
						System.out.println("Read object with numFields == 0, " + failures + " times.");
					}
				}
			}
			freeFC(fc, (int)(keyVal % TEMP_COUNT));
			return fromBytes(object.array());
		}
		
		protected long getFromIndex(long key) throws IOException
		{
			long retval = index.get(key);
			if (retval == -1)
			{
				return 0;
			}
			
			return retval;
		}
		
		public void freeFC(FileChannel ifc, int hash)
		{
			int i = 0;
			Vector<FileChannel> ifcs = ifcsArrayList.get(hash);
			Vector<Boolean> locks = locksArrayList.get(hash);
			while (i < ifcs.size())
			{
				FileChannel fc = ifcs.get(i);
				if (ifc.equals(fc))
				{
					locks.set(i,  false);
					return;
				}
				
				i++;
			}
		}
		
		public FileChannel getFreeFC(int hash)
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
					return ifcs.get(i);
				}
				i++;
			}
			
			FileChannel retval = null;
			try
			{
				retval = new RandomAccessFile(TEMP_DIRS.get(hash) + "DBHM" + id + ".tmp", "r").getChannel();
				locks.add(true);
				ifcs.add(retval);
				return retval;
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(1);
				return null;
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
		
		public void putToDisk(long eKey, Object eVal) throws Exception
		{	
			if (!filesAllocated)
			{
				synchronized(this)
				{
					if (!filesAllocated)
					{
						int i = 0;
						index = new LongPrimitiveConcurrentHashMap(estimate / 16, 16.0f, cpus*6);
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
			synchronized(ofci)
			{
				ofci.position(indexValue);
				int count = 0;
				while (count < bb.limit())
				{
					count += ofci.write(bb);
				}
			}
			
			putToIndex(eKey, indexValue);
			if (indexed)
			{
				diskValueIndex.add(((ArrayList<Object>)eVal));
			}
		}
		
		public void putToDisk(ArrayList<InternalConcurrentHashMap.WriteThroughEntry> entries) throws Exception
		{	
			if (!filesAllocated)
			{
				synchronized(this)
				{
					if (!filesAllocated)
					{
						int i = 0;
						index = new LongPrimitiveConcurrentHashMap(estimate / 16, 16.0f, cpus*6);
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
					synchronized(ofci)
					{
						ofci.position(indexValue);
						int count = 0;
						while (count < bb.limit())
						{
							count += ofci.write(bb);
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
		
		protected void putToIndex(long key, long value) throws Exception
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
		
		protected Object fromBytes(byte[] val)
		{	
			ByteBuffer bb = ByteBuffer.wrap(val);
			int numFields = bb.getInt();
			
			if (numFields <= 0)
			{
				System.out.println("Negative or zero number of fields in fromBytes()");
				System.out.println("NumFields = " + numFields);
				System.exit(1);
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
					Date o = new Date(bb.getLong());
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
				else
				{
					System.out.println("Unknown type in fromBytes()");
				}
				
				i++;
			}
			
			return retval;
		}
		
		protected byte[] toBytes(Object v)
		{
			ArrayList<Object> val;
			if (v instanceof ArrayList)
			{
				val = (ArrayList<Object>)v;
				if (val.size() == 0)
				{
					System.out.println("Zero sized array list in toBytes().");
					System.exit(1);
				}
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
				else if (o instanceof Date)
				{
					header[i] = (byte)3;
					size += 8;
				}
				else if (o instanceof String)
				{
					header[i] = (byte)4;
					size += (4 + ((String)o).length());
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
					retvalBB.putLong(((Date)o).getTime());
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
				
				i++;
			}
			
			return retval;
		}
		
		protected byte[] arrayListToBytes(ArrayList<InternalConcurrentHashMap.WriteThroughEntry> entries)
		{
			ArrayList<byte[]> results = new ArrayList<byte[]>(entries.size());
			for (InternalConcurrentHashMap.WriteThroughEntry entry : entries)
			{
				Object v = entry.getValue();
				ArrayList<Object> val;
				if (v instanceof ArrayList)
				{
					val = (ArrayList<Object>)v;
					if (val.size() == 0)
					{
						System.out.println("In arrayListToBytes() with zero size array.");
						System.exit(1);
					}
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
					else if (o instanceof Date)
					{
						header[i] = (byte)3;
						size += 8;
					}
					else if (o instanceof String)
					{
						header[i] = (byte)4;
						size += (4 + ((String)o).length());
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
						retvalBB.putLong(((Date)o).getTime());	
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
}
