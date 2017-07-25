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
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.managers.ResourceManager;
import com.exascale.misc.*;
import com.exascale.tables.Plan;
import com.exascale.threads.HRDBMSThread;
import com.exascale.threads.ThreadPoolThread;

public final class IntersectOperator implements Operator, Serializable
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
		catch (final Exception e)
		{
			unsafe = null;
		}
	}

	private transient MetaData meta;
	private List<Operator> children = new ArrayList<Operator>();
	private Operator parent;
	private Map<String, String> cols2Types;
	private Map<String, Integer> cols2Pos;
	private Map<Integer, String> pos2Col;
	private int node;
	private transient List<Set<List<Object>>> sets;
	private transient BufferedLinkedBlockingQueue buffer;
	private long estimate = 16;
	private transient boolean inMem;
	private transient int numFiles;

	private volatile boolean startDone = false;

	private boolean estimateSet = false;
	private transient List<List<String>> externalFiles;

	private transient List<List<RandomAccessFile>> rafs;

	private transient List<List<FileChannel>> fcs;
	private transient AtomicLong received;
	private transient volatile boolean demReceived;

	public IntersectOperator(final MetaData meta)
	{
		this.meta = meta;
		received = new AtomicLong(0);
	}

	public static IntersectOperator deserialize(final InputStream in, final Map<Long, Object> prev) throws Exception
	{
		final IntersectOperator value = (IntersectOperator)unsafe.allocateInstance(IntersectOperator.class);
		prev.put(OperatorUtils.readLong(in), value);
		value.children = OperatorUtils.deserializeALOp(in, prev);
		value.parent = OperatorUtils.deserializeOperator(in, prev);
		value.cols2Types = OperatorUtils.deserializeStringHM(in, prev);
		value.cols2Pos = OperatorUtils.deserializeStringIntHM(in, prev);
		value.pos2Col = OperatorUtils.deserializeTM(in, prev);
		value.node = OperatorUtils.readInt(in);
		value.estimate = OperatorUtils.readLong(in);
		value.startDone = OperatorUtils.readBool(in);
		value.estimateSet = OperatorUtils.readBool(in);
		value.received = new AtomicLong(0);
		value.demReceived = false;
		return value;
	}

	private static long hash(final byte[] key) throws Exception
	{
		long eHash;
		if (key == null)
		{
			eHash = 0;
		}
		else
		{
			eHash = MurmurHash.hash64(key, key.length);
		}

		return eHash & 0x7FFFFFFFFFFFFFFFL;
	}

	private static final byte[] toBytes(final Object v) throws Exception
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
				final byte[] b = ((String)o).getBytes(StandardCharsets.UTF_8);
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
					final Exception e = new Exception("Non-zero size ArrayList in toBytes()");
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

			i++;
		}

		return retval;
	}

	@Override
	public void add(final Operator op) throws Exception
	{
		children.add(op);
		op.registerParent(this);
		cols2Types = op.getCols2Types();
		cols2Pos = op.getCols2Pos();
		pos2Col = op.getPos2Col();
	}

	@Override
	public List<Operator> children()
	{
		return children;
	}

	@Override
	public IntersectOperator clone()
	{
		final IntersectOperator retval = new IntersectOperator(meta);
		retval.node = node;
		retval.estimate = estimate;
		return retval;
	}

	@Override
	public void close() throws Exception
	{
		for (final Operator o : children)
		{
			o.close();
		}

		if (buffer != null)
		{
			buffer.close();
		}

		cols2Pos = null;
		cols2Types = null;
		pos2Col = null;
	}

	@Override
	public int getChildPos()
	{
		return 0;
	}

	@Override
	public Map<String, Integer> getCols2Pos()
	{
		return cols2Pos;
	}

	@Override
	public Map<String, String> getCols2Types()
	{
		return cols2Types;
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
	public Map<Integer, String> getPos2Col()
	{
		return pos2Col;
	}

	@Override
	public List<String> getReferences()
	{
		return new ArrayList<String>();
	}

	@Override
	public Object next(final Operator op2) throws Exception
	{
		Object o;
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

	@Override
	public void nextAll(final Operator op) throws Exception
	{
		for (final Operator o : children)
		{
			o.nextAll(op);
		}
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
	public void registerParent(final Operator op) throws Exception
	{
		if (parent == null)
		{
			parent = op;
		}
		else
		{
			throw new Exception("IntersectOperator only supports 1 parent.");
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
		parent = null;
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
			for (final Operator op : children)
			{
				op.reset();
			}

			sets = new ArrayList<>();
			buffer.clear();
			new InitThread().start();
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

		OperatorUtils.writeType(HrdbmsType.INTERSECT, out);
		prev.put(this, OperatorUtils.writeID(out));
		OperatorUtils.serializeALOp(children, out, prev);
		parent.serialize(out, prev);
		OperatorUtils.serializeStringHM(cols2Types, out, prev);
		OperatorUtils.serializeStringIntHM(cols2Pos, out, prev);
		OperatorUtils.serializeTM(pos2Col, out, prev);
		OperatorUtils.writeInt(node, out);
		OperatorUtils.writeLong(estimate, out);
		OperatorUtils.writeBool(startDone, out);
		OperatorUtils.writeBool(estimateSet, out);
	}

	@Override
	public void setChildPos(final int pos)
	{
	}

	public boolean setEstimate(final long estimate)
	{
		if (estimateSet)
		{
			return false;
		}
		this.estimate = estimate;
		estimateSet = true;
		return true;
	}

	@Override
	public void setNode(final int node)
	{
		this.node = node;
	}

	@Override
	public void setPlan(final Plan plan)
	{
	}

	@Override
	public void start() throws Exception
	{
		sets = new ArrayList<>();
		startDone = true;
		for (final Operator op : children)
		{
			op.start();
		}

		buffer = new BufferedLinkedBlockingQueue(ResourceManager.QUEUE_SIZE);
		new InitThread().start();
	}

	@Override
	public String toString()
	{
		return "IntersectOperator";
	}

	private void cleanupExternal()
	{
		for (final List<FileChannel> fc : fcs)
		{
			for (final FileChannel f : fc)
			{
				try
				{
					f.close();
				}
				catch (final Exception e)
				{
				}
			}
		}

		for (final List<RandomAccessFile> raf : rafs)
		{
			for (final RandomAccessFile r : raf)
			{
				try
				{
					r.close();
				}
				catch (final Exception e)
				{
				}
			}
		}

		for (final List<String> files : externalFiles)
		{
			for (final String fn : files)
			{
				new File(fn).delete();
			}
		}
	}

	private void createTempFiles() throws Exception
	{
		int i = 0; // fileNum
		externalFiles = new ArrayList<List<String>>();
		rafs = new ArrayList<List<RandomAccessFile>>();
		fcs = new ArrayList<List<FileChannel>>();
		while (i < numFiles)
		{
			final List<String> files = new ArrayList<String>();
			final List<RandomAccessFile> raf = new ArrayList<RandomAccessFile>();
			final List<FileChannel> fc = new ArrayList<FileChannel>();
			int j = 0; // child num
			while (j < children.size())
			{
				final String fn = ResourceManager.TEMP_DIRS.get(i % ResourceManager.TEMP_DIRS.size()) + this.hashCode() + "" + System.currentTimeMillis() + ".exths" + i + "." + j;
				files.add(fn);
				RandomAccessFile r = null;
				while (true)
				{
					try
					{
						r = new RandomAccessFile(fn, "rw");
						break;
					}
					catch (final FileNotFoundException e)
					{
						ResourceManager.panic = true;
						try
						{
							Thread.sleep(Integer.parseInt(HRDBMSWorker.getHParms().getProperty("rm_sleep_time_ms")) / 2);
						}
						catch (final Exception f)
						{
						}
					}
				}
				raf.add(r);
				fc.add(r.getChannel());
				j++;
			}

			externalFiles.add(files);
			rafs.add(raf);
			fcs.add(fc);
			i++;
		}
	}

	private void doExternal()
	{
		int i = 0; // fileNum
		ArrayList<Set<List<Object>>> sets = new ArrayList<>();
		ReadBackThread thread = new ReadBackThread(i, sets);
		thread.start();
		while (i < numFiles)
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

			if (i + 1 < numFiles)
			{
				sets = new ArrayList<>();
				thread = new ReadBackThread(i + 1, sets);
				thread.start();
			}

			i++;
		}
	}

	private void flushBucket(final List<byte[]> bucket, final int fileNum, final int childNum) throws Exception
	{
		final FileChannel fc = fcs.get(fileNum).get(childNum);
		for (final byte[] data : bucket)
		{
			final ByteBuffer bb = ByteBuffer.wrap(data);
			fc.write(bb);
		}

		bucket.clear();
	}

	private void flushBuckets(final List<List<byte[]>> buckets, final int childNum) throws Exception
	{
		int i = 0;
		for (final List<byte[]> bucket : buckets)
		{
			flushBucket(bucket, i, childNum);
			i++;
		}
	}

	private final class InitThread extends ThreadPoolThread
	{
		private final List<ReadThread> threads = new ArrayList<ReadThread>(children.size());

		@Override
		public void run()
		{
			if (children.size() == 1)
			{
				// uses not distinct logic since this can only happen during
				// index processing
				int count = 0;
				try
				{
					Object o = children.get(0).next(IntersectOperator.this);
					if (o instanceof DataEndMarker)
					{
						demReceived = true;
					}
					else
					{
						received.getAndIncrement();
					}
					while (!(o instanceof DataEndMarker))
					{
						while (true)
						{
							try
							{
								buffer.put(o);
								count++;
								break;
							}
							catch (final Exception e)
							{
							}
						}

						o = children.get(0).next(IntersectOperator.this);
						if (o instanceof DataEndMarker)
						{
							demReceived = true;
						}
						else
						{
							received.getAndIncrement();
						}
					}

					HRDBMSWorker.logger.debug("Intersect operator returned " + count + " rows");

					while (true)
					{
						try
						{
							buffer.put(o);
							break;
						}
						catch (final Exception e)
						{
						}
					}
				}
				catch (final Exception f)
				{
					HRDBMSWorker.logger.error("", f);
					try
					{
						buffer.put(f);
					}
					catch (final Exception g)
					{
					}
					return;
				}

				return;
			}

			inMem = true;
			numFiles = 0;

			if (ResourceManager.criticalMem())
			{
				inMem = false;
				numFiles = (int)(estimate / (ResourceManager.QUEUE_SIZE * Double.parseDouble(HRDBMSWorker.getHParms().getProperty("external_factor"))) + 1);
				if (numFiles > 1024)
				{
					numFiles = 1024;
				}
				// HRDBMSWorker.logger.debug("Setting numFiles to " +
				// numFiles + " based on estimate of " + estimate);
				if (numFiles == 1)
				{
					inMem = true;
				}
			}
			else if (estimate > ResourceManager.QUEUE_SIZE * Double.parseDouble(HRDBMSWorker.getHParms().getProperty("hash_external_factor")) / 2)
			{
				inMem = false;
				numFiles = (int)(estimate / (ResourceManager.QUEUE_SIZE * Double.parseDouble(HRDBMSWorker.getHParms().getProperty("external_factor"))) + 1);
				if (numFiles > 1024)
				{
					numFiles = 1024;
				}
				// HRDBMSWorker.logger.debug("Setting numFiles to " +
				// numFiles + " based on estimate of " + estimate);
				if (numFiles == 1)
				{
					inMem = true;
				}
			}

			if (!inMem)
			{
				try
				{
					createTempFiles();
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.debug("", e);
					buffer.put(e);
					cleanupExternal();
					return;
				}

				int i = 0;
				final int size = children.size();
				while (i < size)
				{
					final ReadThread read = new ReadThread(i);
					threads.add(read);
					read.start();
					i++;
				}

				for (final ReadThread read : threads)
				{
					while (true)
					{
						try
						{
							read.join();
							break;
						}
						catch (final InterruptedException e)
						{
						}
					}
				}

				doExternal();
				cleanupExternal();
				while (true)
				{
					try
					{
						buffer.put(new DataEndMarker());
						break;
					}
					catch (final Exception e)
					{
					}
				}

				return;
			}

			int i = 0;
			final int size = children.size();
			while (i < size)
			{
				final ReadThread read = new ReadThread(i);
				threads.add(read);
				sets.add(new HashSet<List<Object>>());
				read.start();
				i++;
			}

			for (final ReadThread read : threads)
			{
				while (true)
				{
					try
					{
						read.join();
						break;
					}
					catch (final InterruptedException e)
					{
					}
				}
			}

			i = 0;
			long minCard = Long.MAX_VALUE;
			int minI = -1;
			for (final ReadThread read : threads)
			{
				final long card = sets.get(i).size();
				if (card < minCard)
				{
					minCard = card;
					minI = i;
				}

				i++;
			}

			int count = 0;
			for (final Object o : sets.get(minI))
			{
				boolean inAll = true;
				for (final Set<List<Object>> set : sets)
				{
					try
					{
						if (!set.contains(o))
						{
							inAll = false;
							break;
						}
					}
					catch (final Exception e)
					{
						try
						{
							buffer.put(e);
						}
						catch (final Exception f)
						{
						}
						return;
					}
				}

				if (inAll)
				{
					while (true)
					{
						try
						{
							buffer.put(o);
							count++;
							break;
						}
						catch (final Exception e)
						{
						}
					}
				}
			}

			HRDBMSWorker.logger.debug("Intersect operator returned " + count + " rows");

			while (true)
			{
				try
				{
					buffer.put(new DataEndMarker());
					break;
				}
				catch (final Exception e)
				{
				}
			}

			sets = null;
		}
	}

	private class ReadBackThread extends HRDBMSThread
	{
		private final int fileNum;
		private final List<Set<List<Object>>> sets;

		public ReadBackThread(final int fileNum, final List<Set<List<Object>>> sets)
		{
			this.fileNum = fileNum;
			this.sets = sets;
		}

		@Override
		public void run()
		{
			try
			{
				final List<FileChannel> fs = fcs.get(fileNum);
				for (final FileChannel f : fs)
				{
					final FileChannel fc = new BufferedFileChannel(f, 8 * 1024 * 1024);
					final Set<List<Object>> set = new HashSet<List<Object>>();
					sets.add(set);
					fc.position(0);
					final ByteBuffer bb1 = ByteBuffer.allocate(4);
					while (true)
					{
						bb1.position(0);
						if (fc.read(bb1) == -1)
						{
							break;
						}
						bb1.position(0);
						final int length = bb1.getInt();
						final ByteBuffer bb = ByteBuffer.allocate(length);
						fc.read(bb);
						final List<Object> row = (List<Object>)fromBytes(bb.array());
						set.add(row);
					}
				}

				int i = 0;
				long minCard = Long.MAX_VALUE;
				int minI = -1;
				for (final Set<List<Object>> set : sets)
				{
					final long card = set.size();
					if (card < minCard)
					{
						minCard = card;
						minI = i;
					}

					i++;
				}

				for (final Object o : sets.get(minI))
				{
					boolean inAll = true;
					for (final Set<List<Object>> set : sets)
					{
						try
						{
							if (!set.contains(o))
							{
								inAll = false;
								break;
							}
						}
						catch (final Exception e)
						{
							try
							{
								buffer.put(e);
							}
							catch (final Exception f)
							{
							}
							return;
						}
					}

					if (inAll)
					{
						while (true)
						{
							try
							{
								buffer.put(o);
								break;
							}
							catch (final Exception e)
							{
							}
						}
					}
				}

				sets.clear();
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.debug("", e);
				buffer.put(e);
			}
		}

		private final Object fromBytes(final byte[] val) throws Exception
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
			final List<Object> retval = new ArrayList<Object>(numFields);
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
					final MyDate o = new MyDate(bb.getInt());
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
						final String o = new String(temp, StandardCharsets.UTF_8);
						retval.add(o);
					}
					catch (final Exception e)
					{
						HRDBMSWorker.logger.error("", e);
						throw e;
					}
				}
				// else if (bytes[i + 4] == 6)
				// {
				// // AtomicLong
				// final long o = bb.getLong();
				// retval.add(new AtomicLong(o));
				// }
				// else if (bytes[i + 4] == 7)
				// {
				// // AtomicDouble
				// final double o = bb.getDouble();
				// retval.add(new AtomicBigDecimal(new
				// BigDecimalReplacement(o)));
				// }
				else if (bytes[i + 4] == 8)
				{
					// Empty ArrayList
					retval.add(new ArrayList<Object>());
				}
				else
				{
					HRDBMSWorker.logger.error("Unknown type in fromBytes(): " + bytes[i + 4]);
					HRDBMSWorker.logger.debug("So far the row is " + retval);
					throw new Exception("Unknown type in fromBytes(): " + bytes[i + 4]);
				}

				i++;
			}

			return retval;
		}
	}

	private final class ReadThread extends ThreadPoolThread
	{
		private final Operator op;
		private final int childNum;

		public ReadThread(final int childNum)
		{
			this.childNum = childNum;
			op = children.get(childNum);
		}

		@Override
		public void run()
		{
			try
			{
				if (inMem)
				{
					final Set<List<Object>> set = sets.get(childNum);
					Object o = op.next(IntersectOperator.this);
					if (o instanceof DataEndMarker)
					{
						demReceived = true;
					}
					else
					{
						received.getAndIncrement();
					}
					while (!(o instanceof DataEndMarker))
					{
						set.add((List<Object>)o);
						o = op.next(IntersectOperator.this);
						if (o instanceof DataEndMarker)
						{
							demReceived = true;
						}
						else
						{
							received.getAndIncrement();
						}
					}
				}
				else
				{
					try
					{
						final List<List<byte[]>> buckets = new ArrayList<List<byte[]>>();
						int i = 0;
						while (i < numFiles)
						{
							buckets.add(new ArrayList<byte[]>());
							i++;
						}

						Object o = null;
						o = op.next(IntersectOperator.this);
						if (o instanceof DataEndMarker)
						{
							demReceived = true;
						}
						else
						{
							received.getAndIncrement();
						}
						while (!(o instanceof DataEndMarker))
						{
							if (o instanceof Exception)
							{
								buffer.put(o);
								return;
							}

							final byte[] data = toBytes(o);
							final int hash = (int)(hash(data) % numFiles);
							final List<byte[]> bucket = buckets.get(hash);
							bucket.add(data);
							if (bucket.size() > 8192)
							{
								flushBucket(bucket, hash, childNum);
							}

							o = op.next(IntersectOperator.this);
							if (o instanceof DataEndMarker)
							{
								demReceived = true;
							}
							else
							{
								received.getAndIncrement();
							}
						}

						flushBuckets(buckets, childNum);
					}
					catch (final Exception e)
					{
						HRDBMSWorker.logger.debug("", e);
						buffer.put(e);
					}
				}
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				try
				{
					buffer.put(e);
				}
				catch (final Exception f)
				{
				}
				return;
			}
		}
	}
}
