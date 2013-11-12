package com.exascale.optimizer.testing;

import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ResourceManager extends Thread
{
	private static int SLEEP_TIME = 10000;
	private static int LOW_PERCENT_FREE = 30;
	private static int PERCENT_TO_CUT = 1;
	private static volatile Vector<DiskBackedCollection> collections = new Vector<DiskBackedCollection>();
	private static volatile boolean lowMem = false;
	private static final int TEMP_COUNT = 4;
	private static Vector<String> TEMP_DIRS;
	private static final int MIN_CT_SIZE = 20000;
	private static final Long LARGE_PRIME =  1125899906842597L;
    private static final Long LARGE_PRIME2 = 6920451961L;
    private static AtomicLong idGen = new AtomicLong(0);
    private static HashMap<Long, String> creations = new HashMap<Long, String>();
	
	public ResourceManager()
	{
		TEMP_DIRS = new Vector<String>();
		TEMP_DIRS.add("/temp1/");
		TEMP_DIRS.add("/temp2/");
		TEMP_DIRS.add("/temp3/");
		TEMP_DIRS.add("/home/hrdbms/");
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
		while (true)
		{
			System.out.println(((Runtime.getRuntime().freeMemory() + Runtime.getRuntime().maxMemory() - Runtime.getRuntime().totalMemory()) * 100.0) / (Runtime.getRuntime().maxMemory() * 1.0) + "% free");
			if (lowMem())
			{
				lowMem = true;
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
	
	private static boolean lowMem()
	{
		return ((Runtime.getRuntime().freeMemory() + Runtime.getRuntime().maxMemory() - Runtime.getRuntime().totalMemory()) * 100.0) / (Runtime.getRuntime().maxMemory() * 1.0) < LOW_PERCENT_FREE;
	}
	
	private static void handleLowMem()
	{
	//	System.gc(); 
		while (lowMem())
		{
			ArrayList<Thread> threads = new ArrayList<Thread>();
			//System.out.println(((Runtime.getRuntime().freeMemory() + Runtime.getRuntime().maxMemory() - Runtime.getRuntime().totalMemory()) * 100.0) / (Runtime.getRuntime().maxMemory() * 1.0) + "% free");
			synchronized(collections)
			{
				for (DiskBackedCollection collection : collections)
				{
					ReduceThread rt = new ReduceThread(collection);
					threads.add(rt);
					rt.start();
				}
				
				for (Thread rt : threads)
				{
					while (true)
					{
						try
						{
							rt.join();
							break;
						}
						catch(InterruptedException e) {}
					}
				}
			}
		
		//	System.gc(); 
			/*try
			{
				Thread.currentThread().sleep(SLEEP_TIME);
			}
			catch(InterruptedException e) {} */
		}
	}
	
	private static class ReduceThread extends Thread
	{
		private volatile DiskBackedCollection collection;
		
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
	
	public static DiskBackedHashMap newDiskBackedHashMap()
	{
		DiskBackedHashMap map = new DiskBackedHashMap();
		synchronized(collections)
		{
			collections.add(map);
		}
		return map;
	}
	
	public static DiskBackedHashMap newDiskBackedHashMap(boolean indexed)
	{
		DiskBackedHashMap map = new DiskBackedHashMap(indexed);
		synchronized(collections)
		{
			collections.add(map);
		}
		return map;
	}
	
	public static DiskBackedHashSet newDiskBackedHashSet()
	{
		DiskBackedHashSet set = new DiskBackedHashSet();
		return set;
	}
	
	public static DiskBackedArray newDiskBackedArray()
	{
		DiskBackedArray array = new DiskBackedArray();
		return array;
	}
	
	public static DiskBackedArray newDiskBackedArray(boolean indexed)
	{
		DiskBackedArray array = new DiskBackedArray(indexed);
		return array;
	}
	
	private static abstract class DiskBackedCollection
	{
		public abstract void reduceResources() throws IOException;
	}
	
	public static class DiskBackedHashSet extends DiskBackedCollection 
	{
		private volatile DiskBackedHashMap internal;
		private AtomicLong count = new AtomicLong(0);
		
		public DiskBackedHashSet()
		{
			internal = ResourceManager.newDiskBackedHashMap();
		}
		
		public void close() throws IOException
		{
			internal.close();
		}
		
		public void reduceResources() throws IOException
		{
			internal.reduceResources();
		}
		
		public void add(Object val)
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
						String val2 = ((ArrayList)val).get(0).getClass().toString() + "\u0000" + ((ArrayList)val).get(0).toString();
						int i = 1;
						while (i < ((ArrayList)val).size())
						{
							val2 += "\u0001" + ((ArrayList)val).get(i).getClass().toString() + "\u0000" + ((ArrayList)val).get(i).toString();
							i++;
						}
						val = val2;
					}
				}
				long hash = hash(val) & 0x0EFFFFFFFFFFFFFFL;
				
				synchronized(internal)
				{
					ArrayList<Object> chain = (ArrayList<Object>)internal.get(new Long(hash));
			
					if (chain == null)
					{
						chain = new ArrayList<Object>();
						chain.add(val);
						count.getAndIncrement();
						internal.put(new Long(hash), chain);
					}
					else
					{
						if (!chain.contains(val))
						{
							internal.remove(hash);
							chain.add(val);
							internal.put(new Long(hash), chain);
							count.getAndIncrement();
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
						String val2 = ((ArrayList)val).get(0).getClass().toString() + "\u0000" + ((ArrayList)val).get(0).toString();
						int i = 1;
						while (i < ((ArrayList)val).size())
						{
							val2 += "\u0001" + ((ArrayList)val).get(i).getClass().toString() + "\u0000" + ((ArrayList)val).get(i).toString();
							i++;
						}
						val = val2;
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
							internal.put(new Long(hash), chain);	
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
						String val2 = ((ArrayList)val).get(0).getClass().toString() + "\u0000" + ((ArrayList)val).get(0).toString();
						int i = 1;
						while (i < ((ArrayList)val).size())
						{
							val2 += "\u0001" + ((ArrayList)val).get(i).getClass().toString() + "\u0000" + ((ArrayList)val).get(i).toString();
							i++;
						}
						val = val2;
					}
				}
				
				long hash = hash(val) & 0x0EFFFFFFFFFFFFFFL;
				ArrayList<Object> chain = null;
				chain = (ArrayList<Object>)internal.get(new Long(hash));
				
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
		
		private long hash(Object e)
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
		private volatile DiskBackedHashMap internal;
		private AtomicLong index = new AtomicLong(0);
		
		public DiskBackedArray()
		{
			internal = ResourceManager.newDiskBackedHashMap();
		}
		
		public DiskBackedArray(boolean indexed)
		{
			internal = ResourceManager.newDiskBackedHashMap(indexed);
		}
		
		public boolean contains(Object val)
		{
			return internal.containsValue(val);
		}
		
		public void add(Object o) throws Exception
		{
			long myIndex = index.getAndIncrement();
			internal.put(myIndex, o);
		}
		
		public void update(long index, Object o) throws Exception
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
		private volatile DiskBackedArray array;
		private long index = 0;
		
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
		private volatile ConcurrentHashMap internal = new ConcurrentHashMap();
		private AtomicLong size = new AtomicLong(0);
		private volatile Vector<FileChannel> ofcs = new Vector<FileChannel>();
		private volatile Vector<AtomicLong> ofcSizes = new Vector<AtomicLong>();
		private volatile Vector<Vector<FileChannel>> ifcsVector = new Vector<Vector<FileChannel>>();
		private volatile Vector<Vector<Boolean>> locksVector = new Vector<Vector<Boolean>>();
		private volatile ReadWriteLock lock = new ReentrantReadWriteLock();
		private AtomicLong ctCount;
		private boolean indexed;
		private volatile ConcurrentHashMap valueIndex;
		private volatile DiskBackedHashSet diskValueIndex;
		private long id;
		private volatile boolean closed = false;
		private volatile ConcurrentHashMap index;
		private volatile boolean filesAllocated = false;
		
		public DiskBackedHashMap()
		{
			this(false);
		}
		
		public DiskBackedHashMap(boolean indexed)
		{
			this.indexed = indexed;
			if (indexed)
			{
				valueIndex = new ConcurrentHashMap();
				diskValueIndex = ResourceManager.newDiskBackedHashSet();
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
				val2 = new ArrayList<Object>();
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
		
		private void remove(long index)
		{
			lock.readLock().lock();
			Object o = internal.remove(index);
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
		
		public void update(Long key, Object val) throws Exception
		{
			lock.readLock().lock();
			Object o = internal.remove(key);
			if (o != null)
			{
				if (indexed)
				{
					valueIndex.remove(o);
				}
					
				lock.readLock().unlock();
				putNoSize(key,  val);
				return;
			}
			
			removeFromDisk(key);
			lock.readLock().unlock();
			putNoSize(key,  val);
			return;
		}
		
		public Object putIfAbsent(Long key, Object val) throws Exception
		{	
			Object retval = internal.get(key);
			if (retval != null)
			{
				return retval;
			}
			
			this.lock.readLock().lock();
			retval = this.getFromDisk(key);
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
		
		public void put(Long key, Object val) throws Exception
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
		
		public void putNoSize(Long key, Object val) throws Exception
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
		
		public Object get(Long key) throws IOException, ClassNotFoundException
		{
			Object o = null;
			o = internal.get(key);
			if (o != null)
			{
				return o;
			}
			
			return getFromDisk(key);
		}
		
		public void reduceResources() throws IOException
		{	
			lock.writeLock().lock();
			Set<Long> set = internal.entrySet();
			int size = set.size();
			int num2Cut = 0;
			if (size < MIN_CT_SIZE)
			{
				num2Cut = size;
			}
			else
			{
				num2Cut = size * PERCENT_TO_CUT / 100;
			}
			
			if (num2Cut == 0)
			{
				lock.writeLock().unlock();
				return;
			}
			
			System.out.println("Going to reduce " + num2Cut + "/" + size + " entries.");
			int i = 0;
			int j = 0;
			Iterator it = set.iterator();
			ArrayList<Map.Entry> entries = new ArrayList<Map.Entry>();
			ArrayList<Thread> threads = new ArrayList<Thread>();
			int CT_SIZE = num2Cut / 16;
			if (CT_SIZE < MIN_CT_SIZE)
			{
				CT_SIZE = MIN_CT_SIZE;
			}
			while (it.hasNext())
			{
				Map.Entry entry = (Map.Entry)it.next();
				entries.add(entry);
				i++;
				j++;
				
				if (i % 10000 == 0)
				{
					System.out.println("Marked " + i + "/" + num2Cut + " entries for reduction.");
				}
				
				if (j == CT_SIZE)
				{
					CleanerThread ct = new CleanerThread(entries);
					threads.add(ct);
					j = 0;
					entries = new ArrayList<Map.Entry>();
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
				
			ctCount = new AtomicLong(0);
			for (Thread t : threads)
			{
				t.start();
			}
				
			for (Thread t : threads)
			{
				while (true)
				{
					try
					{
						t.join();
						break;
					}
					catch(InterruptedException e)
					{}
				}
			}
			
			lock.writeLock().unlock();
		}
		
		private class CleanerThread extends Thread
		{
			private ArrayList<Map.Entry> entries;
			
			public CleanerThread(ArrayList<Map.Entry> entries)
			{
				this.entries = entries;
			}
			
			public void run()
			{
				try
				{
					for (Map.Entry entry : entries)
					{
						putToDisk((Long)entry.getKey(), entry.getValue());
						Object o = internal.get(entry.getKey());
						if (indexed)
						{
							valueIndex.remove(o);
						}
						internal.remove(entry.getKey());
						long i = ctCount.incrementAndGet();
						
						if (i % 10000 == 0)
						{
							System.out.println("Reduced " + i + " entries");
						}
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
			}
			freeFC(fc, (int)(keyVal % TEMP_COUNT));
			return fromBytes(object.array());
		}
		
		private long getFromIndex(long key) throws IOException
		{
			Long offset = (Long)index.get(key);
			if (offset == null)
			{
				return 0;
			}
			
			return offset;
		}
		
		public void freeFC(FileChannel ifc, int hash)
		{
			int i = 0;
			Vector<FileChannel> ifcs = ifcsVector.get(hash);
			Vector<Boolean> locks = locksVector.get(hash);
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
			Vector<Boolean> locks = locksVector.get(hash);
			Vector<FileChannel> ifcs = ifcsVector.get(hash);
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
		
		private long removeFromIndex(long keyVal) throws IOException
		{
			Long l = (Long)index.remove(keyVal);
			if (l == null)
			{
				return 0;
			}
			else
			{
				return l;
			}
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
							FileChannel ofc = new RandomAccessFile(TEMP_DIRS.get(i) + "DBHM" + id + ".tmp", "rw").getChannel();
							ofcs.add(ofc);
							index = new ConcurrentHashMap<Long, Long>();
							AtomicLong ofcSize = new AtomicLong(1);
							ofcSizes.add(ofcSize);
							Vector<FileChannel> ifcs = new Vector<FileChannel>();
							ifcs.add(new RandomAccessFile(TEMP_DIRS.get(i) + "DBHM" + id + ".tmp", "r").getChannel());
							ifcsVector.add(ifcs);
							Vector<Boolean> locks = new Vector<Boolean>();
							locks.add(false);
							locksVector.add(locks);
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
		
		private void putToIndex(long key, long value) throws Exception
		{
			index.put(key, value);
		}
		
		public synchronized void close() throws IOException
		{	
			//creations.remove(id);
			if (filesAllocated)
			{
				index.clear();
			
				if (ofcs != null)
				{
					for (FileChannel ofc : ofcs)
					{
						ofc.close();
					}
				
					ofcs = null;
				}
			
				if (ifcsVector != null)
				{
					for (Vector<FileChannel> ifcs : ifcsVector)
					{
						for (FileChannel fc : ifcs)
						{
							fc.close();
						}
					}
				
					ifcsVector = null;
				}
				
				for (String TEMP_DIR : TEMP_DIRS)
				{
					if (!(new File(TEMP_DIR + "DBHM" + id + ".tmp").delete()))
					{
						//System.out.println("Delete of " + TEMP_DIR + "DBHM" + id + ".tmp failed");
					}
				}
			}
			
			synchronized(collections)
			{
				collections.remove(this);
			}
				
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
		
		private Object fromBytes(byte[] val)
		{	
			ByteBuffer bb = ByteBuffer.wrap(val);
			int numFields = bb.getInt();
			
			if (numFields < 0)
			{
				System.out.println("Negative number of fields in fromBytes()");
				System.out.println("NumFields = " + numFields);
				System.exit(1);
			}
			
			bb.position(bb.position() + numFields);
			byte[] bytes = bb.array();
			if (bytes[4] == 5)
			{
				return new DataEndMarker();
			}
			ArrayList<Object> retval = new ArrayList<Object>();
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
					System.out.println("Unknown type " + bytes[i+4] + " in fromBytes()");
				}
				
				i++;
			}
			
			return retval;
		}
		
		private byte[] toBytes(Object v)
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
	}
}
