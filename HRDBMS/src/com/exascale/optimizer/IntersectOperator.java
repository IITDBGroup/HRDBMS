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
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.managers.ResourceManager;
import com.exascale.misc.BufferedFileChannel;
import com.exascale.misc.BufferedLinkedBlockingQueue;
import com.exascale.misc.DataEndMarker;
import com.exascale.misc.MurmurHash;
import com.exascale.misc.MyDate;
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
		catch (Exception e)
		{
			unsafe = null;
		}
	}

	private transient MetaData meta;
	private ArrayList<Operator> children = new ArrayList<Operator>();
	private Operator parent;
	private HashMap<String, String> cols2Types;
	private HashMap<String, Integer> cols2Pos;
	private TreeMap<Integer, String> pos2Col;
	private int node;
	private transient ArrayList<HashSet<ArrayList<Object>>> sets;
	private transient BufferedLinkedBlockingQueue buffer;
	private int estimate = 16;
	private transient boolean inMem;
	private transient int numFiles;

	private volatile boolean startDone = false;

	private boolean estimateSet = false;
	private transient ArrayList<ArrayList<String>> externalFiles;

	private transient ArrayList<ArrayList<RandomAccessFile>> rafs;

	private transient ArrayList<ArrayList<FileChannel>> fcs;
	private transient AtomicLong received;
	private transient volatile boolean demReceived;

	public IntersectOperator(MetaData meta)
	{
		this.meta = meta;
		received = new AtomicLong(0);
	}

	public static IntersectOperator deserialize(InputStream in, HashMap<Long, Object> prev) throws Exception
	{
		IntersectOperator value = (IntersectOperator)unsafe.allocateInstance(IntersectOperator.class);
		prev.put(OperatorUtils.readLong(in), value);
		value.children = OperatorUtils.deserializeALOp(in, prev);
		value.parent = OperatorUtils.deserializeOperator(in, prev);
		value.cols2Types = OperatorUtils.deserializeStringHM(in, prev);
		value.cols2Pos = OperatorUtils.deserializeStringIntHM(in, prev);
		value.pos2Col = OperatorUtils.deserializeTM(in, prev);
		value.node = OperatorUtils.readInt(in);
		value.estimate = OperatorUtils.readInt(in);
		value.startDone = OperatorUtils.readBool(in);
		value.estimateSet = OperatorUtils.readBool(in);
		value.received = new AtomicLong(0);
		value.demReceived = false;
		return value;
	}

	@Override
	public void add(Operator op) throws Exception
	{
		children.add(op);
		op.registerParent(this);
		cols2Types = op.getCols2Types();
		cols2Pos = op.getCols2Pos();
		pos2Col = op.getPos2Col();
	}

	@Override
	public ArrayList<Operator> children()
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
		for (Operator o : children)
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
		return new ArrayList<String>();
	}

	@Override
	public Object next(Operator op2) throws Exception
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
	public void nextAll(Operator op) throws Exception
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
	public void registerParent(Operator op) throws Exception
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
	public void removeChild(Operator op)
	{
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

			sets = new ArrayList<HashSet<ArrayList<Object>>>();
			buffer.clear();
			new InitThread().start();
		}
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

		OperatorUtils.writeType(31, out);
		prev.put(this, OperatorUtils.writeID(out));
		OperatorUtils.serializeALOp(children, out, prev);
		parent.serialize(out, prev);
		OperatorUtils.serializeStringHM(cols2Types, out, prev);
		OperatorUtils.serializeStringIntHM(cols2Pos, out, prev);
		OperatorUtils.serializeTM(pos2Col, out, prev);
		OperatorUtils.writeInt(node, out);
		OperatorUtils.writeInt(estimate, out);
		OperatorUtils.writeBool(startDone, out);
		OperatorUtils.writeBool(estimateSet, out);
	}

	@Override
	public void setChildPos(int pos)
	{
	}

	public boolean setEstimate(int estimate)
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
	public void setNode(int node)
	{
		this.node = node;
	}

	@Override
	public void setPlan(Plan plan)
	{
	}

	@Override
	public void start() throws Exception
	{
		sets = new ArrayList<HashSet<ArrayList<Object>>>();
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
		for (ArrayList<FileChannel> fc : fcs)
		{
			for (FileChannel f : fc)
			{
				try
				{
					f.close();
				}
				catch (Exception e)
				{
				}
			}
		}

		for (ArrayList<RandomAccessFile> raf : rafs)
		{
			for (RandomAccessFile r : raf)
			{
				try
				{
					r.close();
				}
				catch (Exception e)
				{
				}
			}
		}

		for (ArrayList<String> files : externalFiles)
		{
			for (String fn : files)
			{
				new File(fn).delete();
			}
		}
	}

	private void createTempFiles() throws Exception
	{
		int i = 0; // fileNum
		externalFiles = new ArrayList<ArrayList<String>>();
		rafs = new ArrayList<ArrayList<RandomAccessFile>>();
		fcs = new ArrayList<ArrayList<FileChannel>>();
		while (i < numFiles)
		{
			ArrayList<String> files = new ArrayList<String>();
			ArrayList<RandomAccessFile> raf = new ArrayList<RandomAccessFile>();
			ArrayList<FileChannel> fc = new ArrayList<FileChannel>();
			int j = 0; // child num
			while (j < children.size())
			{
				String fn = ResourceManager.TEMP_DIRS.get(i % ResourceManager.TEMP_DIRS.size()) + this.hashCode() + "" + System.currentTimeMillis() + ".exths" + i + "." + j;
				files.add(fn);
				RandomAccessFile r = null;
				while (true)
				{
					try
					{
						r = new RandomAccessFile(fn, "rw");
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
		ArrayList<HashSet<ArrayList<Object>>> sets = new ArrayList<HashSet<ArrayList<Object>>>();
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
				catch (InterruptedException e)
				{
				}
			}

			if (i + 1 < numFiles)
			{
				sets = new ArrayList<HashSet<ArrayList<Object>>>();
				thread = new ReadBackThread(i + 1, sets);
				thread.start();
			}

			i++;
		}
	}

	private void flushBucket(ArrayList<byte[]> bucket, int fileNum, int childNum) throws Exception
	{
		FileChannel fc = fcs.get(fileNum).get(childNum);
		for (byte[] data : bucket)
		{
			ByteBuffer bb = ByteBuffer.wrap(data);
			fc.write(bb);
		}

		bucket.clear();
	}

	private void flushBuckets(ArrayList<ArrayList<byte[]>> buckets, int childNum) throws Exception
	{
		int i = 0;
		for (ArrayList<byte[]> bucket : buckets)
		{
			flushBucket(bucket, i, childNum);
			i++;
		}
	}

	private long hash(byte[] key) throws Exception
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

	private final byte[] toBytes(Object v) throws Exception
	{
		ArrayList<byte[]> bytes = null;
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
		int z = 0;
		int limit = val.size();
		// for (final Object o : val)
		while (z < limit)
		{
			Object o = val.get(z++);
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
				byte[] b = ((String)o).getBytes(StandardCharsets.UTF_8);
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
		int x = 0;
		z = 0;
		limit = val.size();
		// for (final Object o : val)
		while (z < limit)
		{
			Object o = val.get(z++);
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
				byte[] temp = bytes.get(x);
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

	private final class InitThread extends ThreadPoolThread
	{
		private final ArrayList<ReadThread> threads = new ArrayList<ReadThread>(children.size());

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
					catch (Exception g)
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
				catch (Exception e)
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
				sets.add(new HashSet<ArrayList<Object>>());
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
				for (final HashSet<ArrayList<Object>> set : sets)
				{
					try
					{
						if (!set.contains(o))
						{
							inAll = false;
							break;
						}
					}
					catch (Exception e)
					{
						try
						{
							buffer.put(e);
						}
						catch (Exception f)
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
		private final ArrayList<HashSet<ArrayList<Object>>> sets;

		public ReadBackThread(int fileNum, ArrayList<HashSet<ArrayList<Object>>> sets)
		{
			this.fileNum = fileNum;
			this.sets = sets;
		}

		@Override
		public void run()
		{
			try
			{
				ArrayList<FileChannel> fs = fcs.get(fileNum);
				for (FileChannel f : fs)
				{
					FileChannel fc = new BufferedFileChannel(f, 8*1024*1024);
					HashSet<ArrayList<Object>> set = new HashSet<ArrayList<Object>>();
					sets.add(set);
					fc.position(0);
					ByteBuffer bb1 = ByteBuffer.allocate(4);
					while (true)
					{
						bb1.position(0);
						if (fc.read(bb1) == -1)
						{
							break;
						}
						bb1.position(0);
						int length = bb1.getInt();
						ByteBuffer bb = ByteBuffer.allocate(length);
						fc.read(bb);
						ArrayList<Object> row = (ArrayList<Object>)fromBytes(bb.array());
						set.add(row);
					}
				}

				int i = 0;
				long minCard = Long.MAX_VALUE;
				int minI = -1;
				for (HashSet<ArrayList<Object>> set : sets)
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
					for (final HashSet<ArrayList<Object>> set : sets)
					{
						try
						{
							if (!set.contains(o))
							{
								inAll = false;
								break;
							}
						}
						catch (Exception e)
						{
							try
							{
								buffer.put(e);
							}
							catch (Exception f)
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
			catch (Exception e)
			{
				HRDBMSWorker.logger.debug("", e);
				buffer.put(e);
			}
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

		public ReadThread(int childNum)
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
					final HashSet<ArrayList<Object>> set = sets.get(childNum);
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
						set.add((ArrayList<Object>)o);
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
						ArrayList<ArrayList<byte[]>> buckets = new ArrayList<ArrayList<byte[]>>();
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

							byte[] data = toBytes(o);
							int hash = (int)(hash(data) % numFiles);
							ArrayList<byte[]> bucket = buckets.get(hash);
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
					catch (Exception e)
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
				catch (Exception f)
				{
				}
				return;
			}
		}
	}
}
