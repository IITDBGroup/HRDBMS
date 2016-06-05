package com.exascale.optimizer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.managers.ResourceManager;
import com.exascale.misc.BufferedLinkedBlockingQueue;
import com.exascale.misc.DataEndMarker;
import com.exascale.misc.MyDate;
import com.exascale.misc.SPSCQueue;
import com.exascale.tables.Plan;
import com.exascale.tables.Schema;
import com.exascale.threads.HRDBMSThread;
import com.exascale.threads.TempThread;
import com.exascale.threads.ThreadPoolThread;

public final class ProductOperator extends JoinOperator implements Serializable
{
	private static sun.misc.Unsafe unsafe;

	static
	{
		try
		{
			final Field f = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
			f.setAccessible(true);
			unsafe = (sun.misc.Unsafe)f.get(null);
		}
		catch (Exception e)
		{
			unsafe = null;
		}
	}

	private ArrayList<Operator> children = new ArrayList<Operator>(2);

	private Operator parent;
	private HashMap<String, String> cols2Types;
	private HashMap<String, Integer> cols2Pos;
	private TreeMap<Integer, String> pos2Col;
	private transient final MetaData meta;
	private transient volatile BufferedLinkedBlockingQueue outBuffer;
	private transient volatile Vector<ArrayList<Object>> inBuffer;
	private transient int NUM_RT_THREADS;
	private transient int NUM_PTHREADS;
	// private final AtomicLong outCount = new AtomicLong(0);
	private transient volatile boolean readersDone;
	private int childPos = -1;
	private int node;
	private transient boolean inMem;
	private transient ThreadPoolThread[] threads;
	private transient ConcurrentHashMap<String, RandomAccessFile> rafs;
	private transient ConcurrentHashMap<String, FileChannel> fcs;
	private transient byte[] types;
	private transient HashSet<HashMap<Filter, Filter>> hshm = null;
	private transient Boolean semi = null;
	private transient Boolean anti = null;
	private transient AtomicLong received;
	private transient volatile boolean demReceived;

	private int rightChildCard = 16;
	private int leftChildCard = 16;

	private boolean cardSet = false;
	private long txnum;

	public ProductOperator(MetaData meta)
	{
		this.meta = meta;
		received = new AtomicLong(0);
	}

	public static ProductOperator deserialize(InputStream in, HashMap<Long, Object> prev) throws Exception
	{
		ProductOperator value = (ProductOperator)unsafe.allocateInstance(ProductOperator.class);
		prev.put(OperatorUtils.readLong(in), value);
		value.children = OperatorUtils.deserializeALOp(in, prev);
		value.parent = OperatorUtils.deserializeOperator(in, prev);
		value.cols2Types = OperatorUtils.deserializeStringHM(in, prev);
		value.cols2Pos = OperatorUtils.deserializeStringIntHM(in, prev);
		value.pos2Col = OperatorUtils.deserializeTM(in, prev);
		value.childPos = OperatorUtils.readInt(in);
		value.node = OperatorUtils.readInt(in);
		value.rightChildCard = OperatorUtils.readInt(in);
		value.cardSet = OperatorUtils.readBool(in);
		value.received = new AtomicLong(0);
		value.demReceived = false;
		value.leftChildCard = OperatorUtils.readInt(in);
		value.txnum = OperatorUtils.readLong(in);
		return value;
	}

	@Override
	public void add(Operator op) throws Exception
	{
		if (children.size() < 2)
		{
			if (childPos == -1)
			{
				children.add(op);
			}
			else
			{
				children.add(childPos, op);
				childPos = -1;
			}
			op.registerParent(this);

			if (children.size() == 2 && children.get(0).getCols2Types() != null && children.get(1).getCols2Types() != null)
			{
				cols2Types = (HashMap<String, String>)children.get(0).getCols2Types().clone();
				cols2Pos = (HashMap<String, Integer>)children.get(0).getCols2Pos().clone();
				pos2Col = (TreeMap<Integer, String>)children.get(0).getPos2Col().clone();

				cols2Types.putAll(children.get(1).getCols2Types());
				for (final Map.Entry entry : children.get(1).getPos2Col().entrySet())
				{
					cols2Pos.put((String)entry.getValue(), cols2Pos.size());
					pos2Col.put(pos2Col.size(), (String)entry.getValue());
				}
			}
		}
		else
		{
			throw new Exception("ProductOperator only supports 2 children");
		}
	}

	@Override
	public void addJoinCondition(ArrayList<Filter> filters)
	{
		throw new UnsupportedOperationException("ProductOperator does not support addJoinCondition");

	}

	@Override
	public void addJoinCondition(String left, String right)
	{
		throw new UnsupportedOperationException("ProductOperator does not support addJoinCondition");

	}

	@Override
	public ArrayList<Operator> children()
	{
		return children;
	}

	@Override
	public ProductOperator clone()
	{
		final ProductOperator retval = new ProductOperator(meta);
		retval.node = node;
		retval.rightChildCard = rightChildCard;
		retval.cardSet = cardSet;
		retval.leftChildCard = leftChildCard;
		retval.txnum = txnum;
		return retval;
	}

	@Override
	public void close() throws Exception
	{
		for (final Operator child : children)
		{
			child.close();
		}

		if (outBuffer != null)
		{
			outBuffer.close();
		}

		cols2Pos = null;
		cols2Types = null;
		pos2Col = null;
	}

	@Override
	public int getChildPos()
	{
		return childPos;
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
	public HashSet<HashMap<Filter, Filter>> getHSHMFilter()
	{
		return null;
	}

	@Override
	public boolean getIndexAccess()
	{
		return false;
	}

	@Override
	public ArrayList<String> getJoinForChild(Operator op)
	{
		return null;
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

	@Override
	public Object next(Operator op) throws Exception
	{
		Object o;
		o = outBuffer.take();

		if (o instanceof DataEndMarker)
		{
			o = outBuffer.peek();
			if (o == null)
			{
				outBuffer.put(new DataEndMarker());
				return new DataEndMarker();
			}
			else
			{
				outBuffer.put(new DataEndMarker());
				return o;
			}
		}
		if (o instanceof Exception)
		{
			throw (Exception)o;
		}
		return o;
	}

	@Override
	public void nextAll(Operator op) throws Exception
	{
		children.get(0).nextAll(op);
		children.get(1).nextAll(op);
		Object o = next(op);
		while (!(o instanceof DataEndMarker) && !(o instanceof Exception))
		{
			o = next(op);
		}
	}

	@Override
	public long numRecsReceived()
	{
		return received.get();
	}

	@Override
	public Operator parent()
	{
		return parent;
	}

	@Override
	public boolean receivedDEM()
	{
		return demReceived;
	}

	@Override
	public void registerParent(Operator op) throws Exception
	{
		if (parent == null)
		{
			parent = op;
		}
		else
		{
			throw new Exception("ProductOperator can only have 1 parent.");
		}
	}

	@Override
	public void removeChild(Operator op)
	{
		childPos = children.indexOf(op);
		children.remove(op);
		op.removeParent(this);
	}

	@Override
	public void removeParent(Operator op)
	{
		parent = null;
	}

	@Override
	public void reset() throws Exception
	{
		HRDBMSWorker.logger.error("ProductOperator cannot be reset");
		throw new Exception("ProductOperator cannot be reset");
	}

	@Override
	public void serialize(OutputStream out, IdentityHashMap<Object, Long> prev) throws Exception
	{
		Long id = prev.get(this);
		if (id != null)
		{
			OperatorUtils.serializeReference(id, out);
			return;
		}

		OperatorUtils.writeType(58, out);
		prev.put(this, OperatorUtils.writeID(out));
		OperatorUtils.serializeALOp(children, out, prev);
		parent.serialize(out, prev);
		OperatorUtils.serializeStringHM(cols2Types, out, prev);
		OperatorUtils.serializeStringIntHM(cols2Pos, out, prev);
		OperatorUtils.serializeTM(pos2Col, out, prev);
		OperatorUtils.writeInt(childPos, out);
		OperatorUtils.writeInt(node, out);
		OperatorUtils.writeInt(rightChildCard, out);
		OperatorUtils.writeBool(cardSet, out);
		OperatorUtils.writeInt(leftChildCard, out);
		OperatorUtils.writeLong(txnum, out);
	}

	public void setAnti()
	{
		anti = new Boolean(true);
	}

	@Override
	public void setChildPos(int pos)
	{
		childPos = pos;
	}

	public void setHSHM(HashSet<HashMap<Filter, Filter>> hshm)
	{
		this.hshm = hshm;
	}

	@Override
	public void setNode(int node)
	{
		this.node = node;
	}

	@Override
	public void setPlan(Plan plan)
	{
	}

	public boolean setRightChildCard(int card, int card2)
	{
		if (cardSet)
		{
			return false;
		}

		cardSet = true;
		rightChildCard = card;
		leftChildCard = card2;
		return true;
	}

	public void setSemi()
	{
		semi = new Boolean(true);
	}

	public void setTXNum(long txnum)
	{
		this.txnum = txnum;
	}

	@Override
	public void start() throws Exception
	{
		int maxAllowed = (int)(ResourceManager.QUEUE_SIZE * Double.parseDouble(HRDBMSWorker.getHParms().getProperty("hash_external_factor")) / 2);
		inBuffer = new Vector<ArrayList<Object>>(maxAllowed);
		inMem = true;
		if (rightChildCard > maxAllowed)
		{
			inMem = false;
		}

		if (inMem)
		{
			NUM_RT_THREADS = ResourceManager.cpus;
			NUM_PTHREADS = ResourceManager.cpus;
		}
		else
		{
			NUM_RT_THREADS = 1;
			NUM_PTHREADS = 1;
		}
		readersDone = false;

		for (final Operator child : children)
		{
			child.start();
		}

		outBuffer = new BufferedLinkedBlockingQueue(ResourceManager.QUEUE_SIZE);
		new InitThread().start();

	}

	@Override
	public String toString()
	{
		return "ProductOperator";
	}

	private final class InitThread extends ThreadPoolThread
	{
		@Override
		public void run()
		{
			int i = 0;
			threads = new ReaderThread[NUM_RT_THREADS];
			while (i < NUM_RT_THREADS)
			{
				threads[i] = new ReaderThread();
				threads[i].start();
				i++;
			}

			i = 0;
			final ThreadPoolThread[] threads2 = new ProcessThread[NUM_PTHREADS];
			while (i < NUM_PTHREADS)
			{
				threads2[i] = new ProcessThread();
				threads2[i].start();
				i++;
			}

			i = 0;
			while (i < NUM_RT_THREADS)
			{
				while (true)
				{
					try
					{
						threads[i].join();
						i++;
						break;
					}
					catch (final InterruptedException e)
					{
					}
				}
			}

			readersDone = true;

			i = 0;
			while (i < NUM_PTHREADS)
			{
				while (true)
				{
					try
					{
						threads2[i].join();
						i++;
						break;
					}
					catch (final InterruptedException e)
					{
					}
				}
			}

			while (true)
			{
				try
				{
					outBuffer.put(new DataEndMarker());
					break;
				}
				catch (Exception e)
				{
				}
			}
		}
	}

	private class LeftThread extends HRDBMSThread
	{
		private final BufferedLinkedBlockingQueue q;
		private HashSet<ArrayList<Object>> toRemove = null;

		public LeftThread(BufferedLinkedBlockingQueue q)
		{
			this.q = q;
		}

		public LeftThread(BufferedLinkedBlockingQueue q, HashSet<ArrayList<Object>> toRemove)
		{
			this.q = q;
			this.toRemove = toRemove;
		}

		@Override
		public void run()
		{
			try
			{
				ArrayList<SubLeftThread> threads = new ArrayList<SubLeftThread>();
				for (FileChannel fc : fcs.values())
				{
					SubLeftThread thread = new SubLeftThread(q, fc, toRemove);
					thread.start();
					threads.add(thread);
				}

				for (SubLeftThread thread : threads)
				{
					thread.join();
					if (!thread.getOK())
					{
						q.put(thread.getException());
						return;
					}
				}

				q.put(new DataEndMarker());
			}
			catch (Exception e)
			{
				q.put(e);
			}
		}
	}

	private final class ProcessThread extends ThreadPoolThread
	{
		@Override
		public void run()
		{
			try
			{
				CNFFilter cnf = null;
				HashSet<ArrayList<Object>> toRemove = null;
				if (hshm != null)
				{
					cnf = new CNFFilter(hshm, meta, cols2Pos, ProductOperator.this);
				}

				if ((semi != null || anti != null) && !inMem)
				{
					toRemove = new HashSet<ArrayList<Object>>();
				}

				SPSCQueue writeQueue = null;
				BufferedLinkedBlockingQueue readQueue = null;
				WriteThread wt = null;
				boolean direct = true;
				if (!inMem)
				{
					writeQueue = new SPSCQueue(ResourceManager.QUEUE_SIZE);
					readQueue = new BufferedLinkedBlockingQueue(ResourceManager.QUEUE_SIZE);
					wt = new WriteThread(writeQueue);
					wt.start();
				}

				while (true)
				{
					final Operator left = children.get(0);
					Object o;
					if (direct)
					{
						o = left.next(ProductOperator.this);
						if (o instanceof DataEndMarker)
						{
							demReceived = true;
						}
						else
						{
							received.getAndIncrement();
						}
					}
					else
					{
						o = readQueue.take();
					}
					// @Parallel
					while (!(o instanceof DataEndMarker))
					{
						final ArrayList<Object> lRow = (ArrayList<Object>)o;
						int i = 0;
						boolean found = false;
						while (true)
						{
							if (i >= inBuffer.size())
							{
								if (readersDone)
								{
									if (i >= inBuffer.size())
									{
										break;
									}

									continue;
								}
								else
								{
									LockSupport.parkNanos(500);
									continue;
								}
							}
							final ArrayList<Object> orow = inBuffer.get(i);
							i++;
							final ArrayList<Object> rRow = orow;

							if (orow == null)
							{
								LockSupport.parkNanos(500);
								continue;
							}

							if (cnf == null || cnf.passes(lRow, rRow))
							{
								if (semi != null)
								{
									outBuffer.put(lRow);
									if (!inMem)
									{
										toRemove.add(lRow);
									}

									break;
								}
								else if (anti != null)
								{
									found = true;
									if (!inMem)
									{
										toRemove.add(lRow);
									}
								}
								else
								{
									final ArrayList<Object> out = new ArrayList<Object>(lRow.size() + rRow.size());
									out.addAll(lRow);
									out.addAll(rRow);
									outBuffer.put(out);
									// outCount.incrementAndGet();
								}
							}
						}

						if (anti != null && !found && inMem)
						{
							outBuffer.put(lRow);
						}

						if (wt != null)
						{
							writeQueue.put(lRow);
						}
						if (direct)
						{
							o = left.next(ProductOperator.this);
							if (o instanceof DataEndMarker)
							{
								demReceived = true;
							}
							else
							{
								received.getAndIncrement();
							}
						}
						else
						{
							o = readQueue.take();
						}
					}

					if (!inMem)
					{
						readersDone = false;
						boolean allDone = true;
						for (ThreadPoolThread thread : threads)
						{
							if (!((ReaderThread)thread).getDone())
							{
								allDone = false;
							}
						}

						if (!allDone)
						{
							// start new reader threads
							new StartReadersThread().start();
						}

						if (wt != null)
						{
							writeQueue.put(new DataEndMarker());
							wt.join();
							if (!wt.getOK())
							{
								throw wt.getException();
							}
							wt = null;
						}

						direct = false;
						if (allDone)
						{
							if (anti != null)
							{
								LeftThread thread = new LeftThread(outBuffer, toRemove);
								thread.start();
								thread.join();
							}
							cleanupExternal();
							break;
						}
						else
						{
							// start new LeftThread and loop
							if (toRemove == null || toRemove.size() == 0)
							{
								new LeftThread(readQueue).start();
							}
							else
							{
								new LeftThread(readQueue, (HashSet<ArrayList<Object>>)toRemove.clone()).start();
								toRemove.clear();
							}
						}
					}
					else
					{
						break;
					}
				}
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				try
				{
					outBuffer.put(e);
				}
				catch (Exception f)
				{
				}
				return;
			}
		}

		private void cleanupExternal() throws Exception
		{
			for (FileChannel fc : fcs.values())
			{
				fc.close();
			}

			for (RandomAccessFile raf : rafs.values())
			{
				raf.close();
			}

			for (String fn : rafs.keySet())
			{
				new File(fn).delete();
			}
		}
	}

	private final class ReaderThread extends ThreadPoolThread
	{
		private boolean done = false;

		public boolean getDone()
		{
			return done;
		}

		@Override
		public void run()
		{
			try
			{
				final Operator child = children.get(1);
				Object o = child.next(ProductOperator.this);
				if (o instanceof DataEndMarker)
				{
					demReceived = true;
				}
				else
				{
					received.getAndIncrement();
				}
				while (!(o instanceof DataEndMarker) && (inMem || inBuffer.size() < inBuffer.capacity()))
				{
					inBuffer.add((ArrayList<Object>)o);
					o = child.next(ProductOperator.this);
					if (o instanceof DataEndMarker)
					{
						demReceived = true;
					}
					else
					{
						received.getAndIncrement();
					}
				}

				if (o instanceof DataEndMarker)
				{
					done = true;
				}
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				try
				{
					outBuffer.put(e);
				}
				catch (Exception f)
				{
				}
				return;
			}
		}
	}

	private class StartReadersThread extends HRDBMSThread
	{
		@Override
		public void run()
		{
			int i = 0;
			threads = new ReaderThread[NUM_RT_THREADS];
			while (i < NUM_RT_THREADS)
			{
				threads[i] = new ReaderThread();
				threads[i].start();
				i++;
			}

			i = 0;
			while (i < NUM_RT_THREADS)
			{
				while (true)
				{
					try
					{
						threads[i].join();
						i++;
						break;
					}
					catch (final InterruptedException e)
					{
					}
				}
			}

			readersDone = true;
		}
	}

	private class SubLeftThread extends HRDBMSThread
	{
		private final BufferedLinkedBlockingQueue q;
		private final FileChannel fc;
		private boolean ok = true;
		private Exception e;
		private final HashSet<ArrayList<Object>> toRemove;

		public SubLeftThread(BufferedLinkedBlockingQueue q, FileChannel fc, HashSet<ArrayList<Object>> toRemove)
		{
			this.q = q;
			this.fc = fc;
			this.toRemove = toRemove;
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
				fc.position(0);
				ByteBuffer bb1 = ByteBuffer.allocate(4);
				while (true)
				{
					bb1.position(0);
					if (fc.read(bb1) == -1)
					{
						return;
					}
					bb1.position(0);
					int length = bb1.getInt();
					ByteBuffer bb = ByteBuffer.allocate(length);
					long pos = fc.position();
					fc.read(bb);
					ArrayList<Object> row = (ArrayList<Object>)fromBytes(bb.array(), types);
					if (toRemove != null && row != null && toRemove.contains(row))
					{
						bb = ByteBuffer.allocate(1);
						bb.put((byte)0);
						bb.position(0);
						fc.write(bb, pos);
					}
					else if (row != null)
					{
						q.put(row);
					}
				}
			}
			catch (Exception e)
			{
				ok = false;
				this.e = e;
			}
		}

		private final Object fromBytes(byte[] val, byte[] types) throws Exception
		{
			final ByteBuffer bb = ByteBuffer.wrap(val);
			final int numFields = types.length;

			if (numFields == 0)
			{
				return new ArrayList<Object>();
			}

			final ArrayList<Object> retval = new ArrayList<Object>(numFields);
			int i = 0;
			while (i < numFields)
			{
				if (types[i] == 0)
				{
					// long
					final Long o = getCLong(bb);
					retval.add(o);
				}
				else if (types[i] == 1)
				{
					// integer
					final Integer o = getCInt(bb);
					retval.add(o);
				}
				else if (types[i] == 2)
				{
					// double
					final Double o = bb.getDouble();
					retval.add(o);
				}
				else if (types[i] == 3)
				{
					// date
					final MyDate o = new MyDate(getMedium(bb));
					retval.add(o);
				}
				else if (types[i] == 4)
				{
					// string
					int length = getCInt(bb);

					byte[] temp = new byte[length];
					bb.get(temp);

					if (!Schema.CVarcharFV.compress)
					{
						try
						{
							final String o = new String(temp, StandardCharsets.UTF_8);
							retval.add(o);
						}
						catch (final Exception e)
						{
							HRDBMSWorker.logger.error("", e);
							throw e;
						}
					}
					else
					{
						byte[] out = new byte[temp.length << 1];
						int clen = Schema.CVarcharFV.decompress(temp, temp.length, out);
						temp = new byte[clen];
						System.arraycopy(out, 0, temp, 0, clen);
						final String o = new String(temp, StandardCharsets.UTF_8);
						retval.add(o);
					}
				}
				else
				{
					HRDBMSWorker.logger.error("Unknown type in fromBytes(): " + types[i]);
					HRDBMSWorker.logger.debug("So far the row is " + retval);
					throw new Exception("Unknown type in fromBytes(): " + types[i]);
				}

				i++;
			}

			return retval;
		}

		private int getCInt(ByteBuffer bb)
		{
			int temp = (bb.get() & 0xff);
			int length = (temp >>> 5);
			if (length == 1)
			{
				return (temp & 0x1f);
			}
			else if (length == 5)
			{
				return bb.getInt();
			}
			else
			{
				int retval = (temp & 0x1f);
				if (length == 2)
				{
					retval = (retval << 8);
					retval += (bb.get() & 0xff);
				}
				else if (length == 3)
				{
					retval = (retval << 16);
					retval += (bb.getShort() & 0xffff);
				}
				else
				{
					retval = (retval << 24);
					retval += getMedium(bb);
				}

				return retval;
			}
		}

		private long getCLong(ByteBuffer bb)
		{
			int temp = (bb.get() & 0xff);
			int length = (temp >>> 4);
			if (length == 1)
			{
				return (temp & 0x0f);
			}
			else if (length == 9)
			{
				return bb.getLong();
			}
			else
			{
				long retval = (temp & 0x0f);
				if (length == 2)
				{
					retval = (retval << 8);
					retval += (bb.get() & 0xff);
				}
				else if (length == 3)
				{
					retval = (retval << 16);
					retval += (bb.getShort() & 0xffff);
				}
				else if (length == 4)
				{
					retval = (retval << 24);
					retval += getMedium(bb);
				}
				else if (length == 5)
				{
					retval = (retval << 32);
					retval += (bb.getInt() & 0xffffffffl);
				}
				else if (length == 6)
				{
					retval = (retval << 40);
					retval += ((bb.get() & 0xffl) << 32);
					retval += (bb.getInt() & 0xffffffffl);
				}
				else if (length == 7)
				{
					retval = (retval << 48);
					retval += ((bb.getShort() & 0xffffl) << 32);
					retval += (bb.getInt() & 0xffffffffl);
				}
				else
				{
					retval = (retval << 56);
					retval += ((getMedium(bb) & 0xffffffl) << 32);
					retval += (bb.getInt() & 0xffffffffl);
				}

				return retval;
			}
		}

		private int getMedium(ByteBuffer bb)
		{
			int retval = ((bb.getShort() & 0xffff) << 8);
			retval += (bb.get() & 0xff);
			return retval;
		}
	}

	private class SubWriteThread extends HRDBMSThread
	{
		private final String fn;
		private final ArrayList<ArrayList<Object>> rows;
		private boolean ok = true;
		private Exception e;
		private final byte[] types;

		public SubWriteThread(String fn, ArrayList<ArrayList<Object>> rows, byte[] types)
		{
			this.fn = fn;
			this.rows = rows;
			this.types = types;
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
				FileChannel fc = fcs.get(fn);
				byte[] data = rsToBytes(rows, types);
				ByteBuffer bb = ByteBuffer.wrap(data);
				fc.write(bb);
			}
			catch (Exception e)
			{
				ok = false;
				this.e = e;
			}
		}

		private void putCInt(ByteBuffer bb, int val)
		{
			if (val <= 31)
			{
				// 1 byte
				val |= 0x20;
				bb.put((byte)val);
			}
			else if (val <= 8191)
			{
				// 2 bytes
				val |= 0x4000;
				bb.putShort((short)val);
			}
			else if (val <= 0x1fffff)
			{
				// 3 bytes
				val |= 0x600000;
				putMedium(bb, val);
			}
			else if (val <= 0x1FFFFFFF)
			{
				// 4 bytes
				val |= 0x80000000;
				bb.putInt(val);
			}
			else
			{
				// 5 bytes
				bb.put((byte)0xa0);
				bb.putInt(val);
			}
		}

		private void putCLong(ByteBuffer bb, long val)
		{
			if (val <= 15)
			{
				// 1 byte
				val |= 0x10;
				bb.put((byte)val);
			}
			else if (val <= 4095)
			{
				// 2 bytes
				val |= 0x2000;
				bb.putShort((short)val);
			}
			else if (val <= 0xfffff)
			{
				// 3 bytes
				val |= 0x300000;
				putMedium(bb, (int)val);
			}
			else if (val <= 0xfffffff)
			{
				// 4 bytes
				val |= 0x40000000;
				bb.putInt((int)val);
			}
			else if (val <= 0xfffffffffl)
			{
				// 5 bytes
				val |= 0x5000000000l;
				bb.put((byte)(val >>> 32));
				bb.putInt((int)val);
			}
			else if (val <= 0xfffffffffffl)
			{
				// 6 bytes
				val |= 0x600000000000l;
				bb.putShort((short)(val >>> 32));
				bb.putInt((int)val);
			}
			else if (val <= 0xfffffffffffffl)
			{
				// 7 bytes
				val |= 0x70000000000000l;
				putMedium(bb, (int)(val >> 32));
				bb.putInt((int)val);
			}
			else if (val <= 0xfffffffffffffffl)
			{
				// 8 bytes
				val |= 0x8000000000000000l;
				bb.putLong(val);
			}
			else
			{
				// 9 bytes
				bb.put((byte)0x90);
				bb.putLong(val);
			}
		}

		private void putMedium(ByteBuffer bb, int val)
		{
			bb.put((byte)((val & 0xff0000) >> 16));
			bb.put((byte)((val & 0xff00) >> 8));
			bb.put((byte)(val & 0xff));
		}

		private final byte[] rsToBytes(ArrayList<ArrayList<Object>> rows, final byte[] types) throws Exception
		{
			final ArrayList<ByteBuffer> results = new ArrayList<ByteBuffer>(rows.size());
			ArrayList<byte[]> bytes = new ArrayList<byte[]>();
			final ArrayList<Integer> stringCols = new ArrayList<Integer>(rows.get(0).size());
			int startSize = 4;
			int a = 0;
			for (byte b : types)
			{
				if (b == 4)
				{
					startSize += 4;
					stringCols.add(a);
				}
				else if (b == 1 || b == 3)
				{
					startSize += 4;
				}
				else
				{
					startSize += 8;
				}

				a++;
			}

			for (ArrayList<Object> val : rows)
			{
				int size = startSize;
				for (int y : stringCols)
				{
					Object o = val.get(y);
					byte[] b = ((String)o).getBytes(StandardCharsets.UTF_8);
					if (Schema.CVarcharFV.compress)
					{
						byte[] out = new byte[b.length * 3 + 1];
						int clen = Schema.CVarcharFV.compress(b, b.length, out);
						b = new byte[clen];
						System.arraycopy(out, 0, b, 0, clen);
					}
					size += b.length;
					bytes.add(b);
				}

				final byte[] retval = new byte[size];
				final ByteBuffer retvalBB = ByteBuffer.wrap(retval);
				// retvalBB.putInt(size - 4);
				int x = 0;
				int i = 0;
				for (final Object o : val)
				{
					if (types[i] == 0)
					{
						// retvalBB.putLong((Long)o);
						putCLong(retvalBB, (Long)o);
					}
					else if (types[i] == 1)
					{
						// retvalBB.putInt((Integer)o);
						putCInt(retvalBB, (Integer)o);
					}
					else if (types[i] == 2)
					{
						retvalBB.putDouble((Double)o);
					}
					else if (types[i] == 3)
					{
						int value = ((MyDate)o).getTime();
						// retvalBB.putInt(value);
						putMedium(retvalBB, value);
					}
					else if (types[i] == 4)
					{
						byte[] temp = bytes.get(x++);
						// retvalBB.putInt(temp.length);
						putCInt(retvalBB, temp.length);
						retvalBB.put(temp);
					}
					else
					{
						throw new Exception("Unknown type: " + types[i]);
					}

					i++;
				}

				results.add(retvalBB);
				bytes.clear();
			}

			int count = 0;
			for (ByteBuffer bb : results)
			{
				count += (bb.position() + 4);
			}
			final byte[] retval = new byte[count];
			ByteBuffer retvalBB = ByteBuffer.wrap(retval);
			int retvalPos = 0;
			for (ByteBuffer bb : results)
			{
				byte[] ba = bb.array();
				retvalBB.position(retvalPos);
				retvalBB.putInt(bb.position());
				retvalPos += 4;
				System.arraycopy(ba, 0, retval, retvalPos, bb.position());
				retvalPos += bb.position();
			}

			return retval;
		}
	}

	private class WriteThread extends HRDBMSThread
	{
		private final SPSCQueue q;
		private boolean ok = true;
		private Exception e;

		public WriteThread(SPSCQueue q)
		{
			this.q = q;
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
				// until we get DEM, write all data to disk
				ArrayList<ArrayList<ArrayList<Object>>> rows = new ArrayList<ArrayList<ArrayList<Object>>>();
				byte[] types1 = new byte[children.get(0).getPos2Col().size()];
				int j = 0;
				for (String col : children.get(0).getPos2Col().values())
				{
					String type = children.get(0).getCols2Types().get(col);
					if (type.equals("INT"))
					{
						types1[j] = (byte)1;
					}
					else if (type.equals("FLOAT"))
					{
						types1[j] = (byte)2;
					}
					else if (type.equals("CHAR"))
					{
						types1[j] = (byte)4;
					}
					else if (type.equals("LONG"))
					{
						types1[j] = (byte)0;
					}
					else if (type.equals("DATE"))
					{
						types1[j] = (byte)3;
					}
					else
					{
						outBuffer.put(new Exception("Unknown type: " + type));
						return;
					}

					j++;
				}
				rafs = new ConcurrentHashMap<String, RandomAccessFile>();
				fcs = new ConcurrentHashMap<String, FileChannel>();
				types = types1;
				int i = 0;
				int mod = ResourceManager.TEMP_DIRS.size();
				int limit = (int)(ResourceManager.QUEUE_SIZE * Double.parseDouble(HRDBMSWorker.getHParms().getProperty("hash_external_factor")) / 2) / mod;
				while (i < mod)
				{
					rows.add(new ArrayList<ArrayList<Object>>(limit));
					i++;
				}

				String name = this.toString() + System.currentTimeMillis() + ".ext";
				createFilesAndChannels(name);
				ArrayList<SubWriteThread> threads = new ArrayList<SubWriteThread>();

				i = 0;
				while (true)
				{
					Object o = q.take();
					if (o instanceof DataEndMarker)
					{
						if (threads.size() != 0)
						{
							for (SubWriteThread thread : threads)
							{
								thread.join();
								if (!thread.getOK())
								{
									ok = false;
									this.e = thread.getException();
									return;
								}
							}

							threads.clear();
						}

						// flush
						j = 0;
						for (String dir : ResourceManager.TEMP_DIRS)
						{
							SubWriteThread thread = new SubWriteThread(dir + name, rows.get(j), types1);
							TempThread.start(thread, txnum);
							threads.add(thread);
							j++;
						}

						for (SubWriteThread thread : threads)
						{
							thread.join();
							if (!thread.getOK())
							{
								ok = false;
								this.e = thread.getException();
								return;
							}
						}

						return;
					}
					else
					{
						rows.get(i % mod).add((ArrayList<Object>)o);
						if (i % mod == mod - 1 && rows.get(0).size() >= limit)
						{
							if (threads.size() != 0)
							{
								for (SubWriteThread thread : threads)
								{
									thread.join();
									if (!thread.getOK())
									{
										ok = false;
										this.e = thread.getException();
										return;
									}
								}

								threads.clear();
							}

							// flush
							j = 0;
							for (String dir : ResourceManager.TEMP_DIRS)
							{
								SubWriteThread thread = new SubWriteThread(dir + name, rows.get(j), types1);
								TempThread.start(thread, txnum);
								threads.add(thread);
								j++;
							}

							rows.clear();
							j = 0;
							while (j < mod)
							{
								rows.add(new ArrayList<ArrayList<Object>>(limit));
								j++;
							}
						}
						i++;
					}
				}
			}
			catch (Exception e)
			{
				ok = false;
				this.e = e;
			}
		}

		private void createFilesAndChannels(String name) throws Exception
		{
			for (String dir : ResourceManager.TEMP_DIRS)
			{
				String fn = dir + name;
				RandomAccessFile raf = null;
				while (true)
				{
					try
					{
						raf = new RandomAccessFile(fn, "rw");
						break;
					}
					catch (FileNotFoundException e)
					{
						ResourceManager.panic = true;
						try
						{
							Thread.sleep(Integer.parseInt(HRDBMSWorker.getHParms().getProperty("rm_sleep_time_ms")) / 2);
						}
						catch (Exception f)
						{
						}
					}
				}
				rafs.put(fn, raf);
				FileChannel fc = raf.getChannel();
				fcs.put(fn, fc);
			}
		}
	}
}
