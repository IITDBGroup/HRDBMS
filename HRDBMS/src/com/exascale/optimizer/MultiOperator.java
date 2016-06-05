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
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.managers.ResourceManager;
import com.exascale.misc.BufferedFileChannel;
import com.exascale.misc.BufferedLinkedBlockingQueue;
import com.exascale.misc.DataEndMarker;
import com.exascale.misc.MurmurHash;
import com.exascale.misc.MyDate;
import com.exascale.misc.ScalableStampedReentrantRWLock;
import com.exascale.optimizer.AggregateOperator.AggregateResultThread;
import com.exascale.tables.Plan;
import com.exascale.tables.Schema;
import com.exascale.threads.HRDBMSThread;
import com.exascale.threads.TempThread;
import com.exascale.threads.ThreadPoolThread;

public final class MultiOperator implements Operator, Serializable
{
	private static int NUM_HGBR_THREADS;
	private static int SHIFT;

	private static sun.misc.Unsafe unsafe;

	static
	{
		try
		{
			final Field f = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
			f.setAccessible(true);
			unsafe = (sun.misc.Unsafe)f.get(null);
			SHIFT = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("mo_bucket_size_shift"));
			HRDBMSWorker.logger.debug("MO SHIFT is " + SHIFT);
			
			NUM_HGBR_THREADS = ResourceManager.cpus;
			int max = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("agg_max_par"));
			if (NUM_HGBR_THREADS > max)
			{
				NUM_HGBR_THREADS = max;
			}
		}
		catch (Exception e)
		{
			unsafe = null;
		}
	}

	private Operator child;
	private Operator parent;
	private HashMap<String, String> cols2Types;
	private HashMap<String, Integer> cols2Pos;
	private TreeMap<Integer, String> pos2Col;
	private transient final MetaData meta;
	private ArrayList<AggregateOperator> ops;
	private ArrayList<String> groupCols;
	private transient volatile BufferedLinkedBlockingQueue readBuffer;
	private boolean sorted;
	private int node;
	private int NUM_GROUPS = 16;
	private int childCard = 16 * 16;
	private transient ArrayList<String> externalFiles;
	private boolean external = false;

	private boolean cardSet = false;

	private volatile boolean startDone = false;
	private transient AtomicLong received;
	private transient volatile boolean demReceived;
	private long txnum;

	public void setTXNum(long txnum)
	{
		this.txnum = txnum;
	}

	public MultiOperator(ArrayList<AggregateOperator> ops, ArrayList<String> groupCols, MetaData meta, boolean sorted)
	{
		this.ops = ops;
		this.groupCols = groupCols;
		this.meta = meta;
		this.sorted = sorted;
		received = new AtomicLong(0);
	}

	public static MultiOperator deserialize(InputStream in, HashMap<Long, Object> prev) throws Exception
	{
		MultiOperator value = (MultiOperator)unsafe.allocateInstance(MultiOperator.class);
		prev.put(OperatorUtils.readLong(in), value);
		value.child = OperatorUtils.deserializeOperator(in, prev);
		value.parent = OperatorUtils.deserializeOperator(in, prev);
		value.cols2Types = OperatorUtils.deserializeStringHM(in, prev);
		value.cols2Pos = OperatorUtils.deserializeStringIntHM(in, prev);
		value.pos2Col = OperatorUtils.deserializeTM(in, prev);
		value.ops = OperatorUtils.deserializeALAgOp(in, prev);
		value.groupCols = OperatorUtils.deserializeALS(in, prev);
		value.sorted = OperatorUtils.readBool(in);
		value.node = OperatorUtils.readInt(in);
		value.NUM_GROUPS = OperatorUtils.readInt(in);
		value.childCard = OperatorUtils.readInt(in);
		value.cardSet = OperatorUtils.readBool(in);
		value.startDone = OperatorUtils.readBool(in);
		value.external = OperatorUtils.readBool(in);
		value.received = new AtomicLong(0);
		value.demReceived = false;
		value.txnum = OperatorUtils.readLong(in);
		return value;
	}

	private static long hash(Object key) throws Exception
	{
		long eHash;
		if (key == null)
		{
			eHash = 0;
		}
		else
		{
			if (key instanceof ArrayList)
			{
				byte[] data = toBytes(key);
				eHash = MurmurHash.hash64(data, data.length);
			}
			else
			{
				byte[] data = key.toString().getBytes(StandardCharsets.UTF_8);
				eHash = MurmurHash.hash64(data, data.length);
			}
		}

		return eHash;
	}

	private static final byte[] toBytes(Object v) throws Exception
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

		int size = 0;
		final byte[] header = new byte[val.size()];
		int i = 0;
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
		// System.arraycopy(header, 0, retval, 0, header.length);
		i = 0;
		final ByteBuffer retvalBB = ByteBuffer.wrap(retval);
		// retvalBB.putInt(size - 4);
		// retvalBB.putInt(val.size());
		// retvalBB.position(header.length);
		int x = 0;
		z = 0;
		limit = val.size();
		// for (final Object o : val)
		while (z < limit)
		{
			Object o = val.get(z++);
			if (header[i] == 0)
			{
				retvalBB.putLong((Long)o);
			}
			else if (header[i] == 1)
			{
				retvalBB.putInt((Integer)o);
			}
			else if (header[i] == 2)
			{
				retvalBB.putDouble((Double)o);
			}
			else if (header[i] == 3)
			{
				retvalBB.putInt(((MyDate)o).getTime());
			}
			else if (header[i] == 4)
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
			else if (header[i] == 8)
			{
			}

			i++;
		}

		return retval;
	}

	@Override
	public void add(Operator op) throws Exception
	{
		if (child == null)
		{
			child = op;
			op.registerParent(this);
			if (child.getCols2Types() != null)
			{
				try
				{
					final HashMap<String, String> tempCols2Types = child.getCols2Types();
					child.getCols2Pos();
					cols2Types = new HashMap<String, String>();
					cols2Pos = new HashMap<String, Integer>();
					pos2Col = new TreeMap<Integer, String>();

					int i = 0;
					ArrayList<String> newGroupCols = new ArrayList<String>();
					for (final String groupCol : groupCols)
					{
						if (!groupCol.startsWith("."))
						{
							cols2Types.put(groupCol, tempCols2Types.get(groupCol));
							cols2Pos.put(groupCol, i);
							pos2Col.put(i, groupCol);
							newGroupCols.add(groupCol);
						}
						else
						{
							int matches = 0;
							for (String col : tempCols2Types.keySet())
							{
								String col2 = col.substring(col.indexOf('.'));
								if (col2.equals(groupCol))
								{
									cols2Types.put(col, tempCols2Types.get(col));
									cols2Pos.put(col, i);
									pos2Col.put(i, col);
									newGroupCols.add(col);
									matches++;
								}
							}

							if (matches != 1)
							{
								HRDBMSWorker.logger.debug("Could not find " + groupCol + " in " + tempCols2Types.keySet());
								throw new Exception("Column does not exist or is ambiguous: " + groupCol);
							}
						}
						i++;
					}

					groupCols = newGroupCols;
					for (final AggregateOperator op2 : ops)
					{
						cols2Types.put(op2.outputColumn(), op2.outputType());
						cols2Pos.put(op2.outputColumn(), i);
						pos2Col.put(i, op2.outputColumn());
						i++;
					}
				}
				catch (Exception e)
				{
					HRDBMSWorker.logger.debug("", e);
					throw e;
				}
			}
		}
		else
		{
			throw new Exception("MultiOperator only supports 1 child.");
		}
	}

	public void addCount(String outCol)
	{
		ops.add(new CountOperator(outCol, meta));
	}

	public void changeCD2Add()
	{
		final ArrayList<AggregateOperator> remove = new ArrayList<AggregateOperator>();
		final ArrayList<AggregateOperator> add = new ArrayList<AggregateOperator>();
		for (final AggregateOperator op : ops)
		{
			if (op instanceof CountDistinctOperator)
			{
				remove.add(op);
				add.add(new SumOperator(op.getInputColumn(), op.outputColumn(), meta, true));
			}
		}

		int i = 0;
		for (final AggregateOperator op : remove)
		{
			final int pos = ops.indexOf(op);
			ops.remove(pos);
			ops.add(pos, add.get(i));
			i++;
		}
	}

	public void changeCountsToSums()
	{
		final ArrayList<AggregateOperator> remove = new ArrayList<AggregateOperator>();
		final ArrayList<AggregateOperator> add = new ArrayList<AggregateOperator>();
		for (final AggregateOperator op : ops)
		{
			if (op instanceof CountOperator)
			{
				remove.add(op);
				add.add(new SumOperator(op.getInputColumn(), op.outputColumn(), meta, true));
			}
		}

		int i = 0;
		for (final AggregateOperator op : remove)
		{
			final int pos = ops.indexOf(op);
			ops.remove(pos);
			ops.add(pos, add.get(i));
			i++;
		}
	}

	@Override
	public ArrayList<Operator> children()
	{
		final ArrayList<Operator> retval = new ArrayList<Operator>(1);
		retval.add(child);
		return retval;
	}

	@Override
	public MultiOperator clone()
	{
		final ArrayList<AggregateOperator> opsClone = new ArrayList<AggregateOperator>(ops.size());
		for (final AggregateOperator op : ops)
		{
			opsClone.add(op.clone());
		}

		final MultiOperator retval = new MultiOperator(opsClone, groupCols, meta, sorted);
		retval.node = node;
		retval.NUM_GROUPS = NUM_GROUPS;
		retval.childCard = childCard;
		retval.cardSet = cardSet;
		retval.external = external;
		retval.txnum = txnum;
		return retval;
	}

	@Override
	public void close() throws Exception
	{
		child.close();

		if (readBuffer != null)
		{
			readBuffer.close();
		}

		ops = null;
		groupCols = null;
		cols2Pos = null;
		cols2Types = null;
		pos2Col = null;
	}

	public boolean existsCountDistinct()
	{
		for (final AggregateOperator op : ops)
		{
			if (op instanceof CountDistinctOperator)
			{
				return true;
			}
		}

		return false;
	}

	public String getAvgCol()
	{
		for (final AggregateOperator op : ops)
		{
			if (op instanceof AvgOperator)
			{
				return op.outputColumn();
			}
		}

		return null;
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

	public ArrayList<String> getInputCols()
	{
		final ArrayList<String> retval = new ArrayList<String>(ops.size());
		for (final AggregateOperator op : ops)
		{
			retval.add(op.getInputColumn());
		}

		return retval;
	}

	public ArrayList<String> getKeys()
	{
		return groupCols;
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

	public ArrayList<AggregateOperator> getOps()
	{
		return ops;
	}

	public ArrayList<String> getOutputCols()
	{
		final ArrayList<String> retval = new ArrayList<String>(ops.size());
		for (final AggregateOperator op : ops)
		{
			retval.add(op.outputColumn());
		}

		return retval;
	}

	@Override
	public TreeMap<Integer, String> getPos2Col()
	{
		return pos2Col;
	}

	public ArrayList<String> getRealInputCols()
	{
		final ArrayList<String> retval = new ArrayList<String>(ops.size());
		for (final AggregateOperator op : ops)
		{
			if (op instanceof CountOperator)
			{
				String col = ((CountOperator)op).getRealInputColumn();
				if (col != null)
				{
					retval.add(col);
				}
			}
			else
			{
				retval.add(op.getInputColumn());
			}
		}

		return retval;
	}

	@Override
	public ArrayList<String> getReferences()
	{
		final ArrayList<String> retval = new ArrayList<String>(ops.size());
		for (final AggregateOperator op : ops)
		{
			retval.add(op.getInputColumn());
		}

		for (String col : groupCols)
		{
			if (!retval.contains(col))
			{
				retval.add(col);
			}
		}

		return retval;
	}

	public boolean hasAvg()
	{
		for (final AggregateOperator op : ops)
		{
			if (op instanceof AvgOperator)
			{
				return true;
			}
		}

		return false;
	}

	public boolean isSorted()
	{
		return sorted;
	}

	@Override
	public Object next(Operator op) throws Exception
	{
		Object o;
		o = readBuffer.take();

		if (o instanceof DataEndMarker)
		{
			o = readBuffer.peek();
			if (o == null)
			{
				readBuffer.put(new DataEndMarker());
				return new DataEndMarker();
			}
			else
			{
				readBuffer.put(new DataEndMarker());
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
		child.nextAll(op);
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
			throw new Exception("MultiOperator only supports 1 parent.");
		}
	}

	@Override
	public void removeChild(Operator op)
	{
		if (op == child)
		{
			child = null;
			op.removeParent(this);
		}
	}

	@Override
	public void removeParent(Operator op)
	{
		parent = null;
	}

	public void replaceAvgWithSumAndCount(HashMap<String, ArrayList<String>> old2New)
	{
		for (final AggregateOperator op : (ArrayList<AggregateOperator>)ops.clone())
		{
			if (op instanceof AvgOperator)
			{
				String outCol1 = null;
				String outCol2 = null;
				for (final Map.Entry entry : old2New.entrySet())
				{
					outCol1 = ((ArrayList<String>)entry.getValue()).get(0);
					outCol2 = ((ArrayList<String>)entry.getValue()).get(1);
				}
				ops.remove(op);
				ops.add(new SumOperator(op.getInputColumn(), outCol1, meta, false));
				ops.add(new CountOperator(op.getInputColumn(), outCol2, meta));
				return;
			}
		}
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
			child.reset();
			readBuffer.clear();
			if (sorted)
			{
				init();
			}
			else
			{
				new HashGroupByThread().start();
			}
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

		OperatorUtils.writeType(33, out);
		prev.put(this, OperatorUtils.writeID(out));
		child.serialize(out, prev);
		parent.serialize(out, prev);
		OperatorUtils.serializeStringHM(cols2Types, out, prev);
		OperatorUtils.serializeStringIntHM(cols2Pos, out, prev);
		OperatorUtils.serializeTM(pos2Col, out, prev);
		OperatorUtils.serializeALAgOp(ops, out, prev);
		OperatorUtils.serializeALS(groupCols, out, prev);
		OperatorUtils.writeBool(sorted, out);
		OperatorUtils.writeInt(node, out);
		OperatorUtils.writeInt(NUM_GROUPS, out);
		OperatorUtils.writeInt(childCard, out);
		OperatorUtils.writeBool(cardSet, out);
		OperatorUtils.writeBool(startDone, out);
		OperatorUtils.writeBool(external, out);
		OperatorUtils.writeLong(txnum, out);
	}

	@Override
	public void setChildPos(int pos)
	{
	}

	public void setExternal()
	{
		external = true;
	}

	@Override
	public void setNode(int node)
	{
		this.node = node;
	}

	public boolean setNumGroupsAndChildCard(int groups, int childCard)
	{
		if (cardSet)
		{
			return false;
		}

		cardSet = true;
		NUM_GROUPS = groups;
		this.childCard = childCard;
		for (final AggregateOperator op : ops)
		{
			op.setNumGroups(NUM_GROUPS);
			if (op instanceof CountDistinctOperator)
			{
				((CountDistinctOperator)op).setChildCard(childCard);
			}
		}

		return true;
	}

	@Override
	public void setPlan(Plan plan)
	{
	}

	public void setSorted()
	{
		sorted = true;
	}

	@Override
	public void start() throws Exception
	{
		startDone = true;
		child.start();
		readBuffer = new BufferedLinkedBlockingQueue(ResourceManager.QUEUE_SIZE);
		if (sorted)
		{
			init();
		}
		else if (!external)
		{
			// System.out.println("HasGroupByThread created via start()");
			new HashGroupByThread().start();
		}
		else if (ResourceManager.criticalMem())
		{
			new ExternalThread().start();
		}
		else
		{
			// double percentInMem = ResourceManager.QUEUE_SIZE *
			// Double.parseDouble(HRDBMSWorker.getHParms().getProperty("hash_external_factor"))
			// / childCard;
			// percentInMem = percentInMem / 8;
			double percentInMem = 0;
			new ExternalThread(percentInMem).start();
		}
	}

	@Override
	public String toString()
	{
		String retval = "MultiOperator: [";
		int i = 0;
		for (final String in : getInputCols())
		{
			retval += (in + "->" + getOutputCols().get(i) + "  ");
			i++;
		}

		retval += ("] group by " + groupCols);
		return retval;
	}

	public void updateInputColumns(ArrayList<String> outputs, ArrayList<String> inputs)
	{
		for (final AggregateOperator op : ops)
		{
			final int index = outputs.indexOf(op.outputColumn());
			op.setInputColumn(inputs.get(index));
		}
	}

	private ArrayList<FileChannel> createChannels(ArrayList<RandomAccessFile> files)
	{
		ArrayList<FileChannel> retval = new ArrayList<FileChannel>(files.size());
		for (RandomAccessFile raf : files)
		{
			retval.add(raf.getChannel());
		}

		return retval;
	}

	private ArrayList<RandomAccessFile> createFiles(ArrayList<String> fns) throws Exception
	{
		ArrayList<RandomAccessFile> retval = new ArrayList<RandomAccessFile>(fns.size());
		for (String fn : fns)
		{
			while (true)
			{
				try
				{
					RandomAccessFile raf = new RandomAccessFile(fn, "rw");
					retval.add(raf);
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
		}

		return retval;
	}

	private ArrayList<String> createFNs(int num, int extra)
	{
		ArrayList<String> retval = new ArrayList<String>(num);
		int i = 0;
		while (i < num)
		{
			String fn = ResourceManager.TEMP_DIRS.get(i % ResourceManager.TEMP_DIRS.size()) + this.hashCode() + "" + System.currentTimeMillis() + i + "_" + extra + ".exthash";
			retval.add(fn);
			i++;
		}

		return retval;
	}

	private void external(double percentInMem)
	{
		try
		{
			//int numBins = 257;
			int numBins = this.childCard / Integer.parseInt(HRDBMSWorker.getHParms().getProperty("mo_bin_size"));
			//numBins *= groupCols.size();
			
			if (numBins < 2)
			{
				numBins = 2;
			}
			
			int inMemBins = (int)(numBins * percentInMem);
			byte[] types1 = new byte[child.getPos2Col().size()];
			int j = 0;
			for (String col : child.getPos2Col().values())
			{
				String type = child.getCols2Types().get(col);
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
					readBuffer.put(new Exception("Unknown type: " + type));
					return;
				}

				j++;
			}

			externalFiles = new ArrayList<String>(numBins);
			ArrayList<String> fns1 = createFNs(numBins, 0);
			externalFiles.addAll(fns1);
			ArrayList<RandomAccessFile> files1 = createFiles(fns1);
			ArrayList<FileChannel> channels1 = createChannels(files1);
			LeftThread thread1 = new LeftThread(files1, channels1, numBins, types1, inMemBins);
			thread1.start();

			while (true)
			{
				try
				{
					thread1.join();
					break;
				}
				catch (InterruptedException e)
				{
				}
			}
			if (!thread1.getOK())
			{
				readBuffer.put(thread1.getException());
				return;
			}

			ArrayList<ArrayList<ArrayList<Object>>> lbins = thread1.getBins();
			ArrayList<ExternalProcessThread> epThreads = new ArrayList<ExternalProcessThread>();
			int z = 0;
			final int limit = lbins.size();
			int maxPar = Runtime.getRuntime().availableProcessors();

			if (maxPar > Integer.parseInt(HRDBMSWorker.getHParms().getProperty("mo_max_par")))
			{
				maxPar = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("mo_max_par"));
			}

			int numPar1 = -1;
			int numPar2 = -1;

			while (z < limit)
			{
				ArrayList<ArrayList<Object>> data = lbins.get(z++);
				if (numPar1 == -1)
				{
					numPar1 = data.size() / 250000;
				}

				if (numPar1 == 0)
				{
					numPar1 = 1;
				}

				if (numPar1 > maxPar)
				{
					numPar1 = maxPar;
				}

				numPar2 = maxPar / numPar1;

				ExternalProcessThread thread = new ExternalProcessThread(data, numPar1);
				int pri = Thread.MAX_PRIORITY - epThreads.size();
				if (pri < Thread.NORM_PRIORITY)
				{
					pri = Thread.NORM_PRIORITY;
				}
				thread.setPriority(pri);
				thread.start();
				epThreads.add(thread);

				if (epThreads.size() >= numPar2)
				{
					int k = numPar2 - 1;
					while (k >= 0)
					{
						if (epThreads.get(k).isDone())
						{
							epThreads.get(k).join();
							epThreads.remove(k);
						}

						k--;
					}

					if (epThreads.size() >= numPar2)
					{
						epThreads.get(0).join();
						epThreads.remove(0);
					}
				}
			}

			lbins = null;
			int i = inMemBins;
			ArrayList<ReadDataThread> leftThreads = new ArrayList<ReadDataThread>();
			numPar1 = -1;

			while (i < numBins)
			{
				ReadDataThread thread4 = new ReadDataThread(channels1.get(i), types1);
				int pri = Thread.MAX_PRIORITY - leftThreads.size();
				if (pri < Thread.NORM_PRIORITY)
				{
					pri = Thread.NORM_PRIORITY;
				}
				thread4.setPriority(pri);
				thread4.start();
				leftThreads.add(thread4);
				i++;

				if (numPar1 == -1)
				{
					thread4.join();
					numPar1 = thread4.getNumPar();
				}

				if (leftThreads.size() >= numPar1)
				{
					int k = leftThreads.size() - 1;
					while (k >= 0)
					{
						if (leftThreads.get(k).isDone())
						{
							leftThreads.get(k).join();
							leftThreads.remove(k);
						}

						k--;
					}

					if (leftThreads.size() >= numPar1)
					{
						leftThreads.get(0).join();
						leftThreads.remove(0);
					}
				}
			}

			for (ExternalProcessThread ept : epThreads)
			{
				ept.join();
			}

			for (ReadDataThread thread : leftThreads)
			{
				thread.join();
			}

			readBuffer.put(new DataEndMarker());

			for (FileChannel fc : channels1)
			{
				try
				{
					fc.close();
				}
				catch (Exception e)
				{
				}
			}

			for (RandomAccessFile raf : files1)
			{
				try
				{
					raf.close();
				}
				catch (Exception e)
				{
				}
			}

			for (String fn : externalFiles)
			{
				try
				{
					new File(fn).delete();
				}
				catch (Exception e)
				{
				}
			}
		}
		catch (Exception e)
		{
			readBuffer.put(e);
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

	private static int getMedium(ByteBuffer bb)
	{
		int retval = ((bb.getShort() & 0xffff) << 8);
		retval += (bb.get() & 0xff);
		return retval;
	}

	private static long getCLong(ByteBuffer bb)
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

	private static int getCInt(ByteBuffer bb)
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

	private void init()
	{
		new InitThread().start();
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

	private static void putCLong(ByteBuffer bb, long val)
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

	private static void putCInt(ByteBuffer bb, int val)
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

	private static void putMedium(ByteBuffer bb, int val)
	{
		bb.put((byte)((val & 0xff0000) >> 16));
		bb.put((byte)((val & 0xff00) >> 8));
		bb.put((byte)(val & 0xff));
	}

	public final class AggregateThread
	{
		private final ArrayList<ThreadPoolThread> threads = new ArrayList<ThreadPoolThread>();
		ArrayList<Object> row = new ArrayList<Object>();
		private boolean end = false;

		public AggregateThread()
		{
			end = true;
		}

		public AggregateThread(Object[] groupKeys, ArrayList<AggregateOperator> ops, ArrayList<ArrayList<Object>> rows)
		{
			for (final Object o : groupKeys)
			{
				row.add(o);
			}

			for (final AggregateOperator op : ops)
			{
				final ThreadPoolThread thread = op.newProcessingThread(rows, child.getCols2Pos());
				threads.add(thread);
			}
		}

		public ArrayList<Object> getResult()
		{
			for (final ThreadPoolThread thread : threads)
			{
				final AggregateResultThread t = (AggregateResultThread)thread;
				// while (true)
				// {
				// try
				// {
				// t.join();
				// break;
				// }
				// catch(InterruptedException e)
				// {
				// continue;
				// }
				// }
				row.add(t.getResult());
				t.close();
			}

			threads.clear();
			return row;
		}

		public boolean isEnd()
		{
			return end;
		}

		public void start()
		{
			if (end)
			{
				return;
			}

			for (final ThreadPoolThread thread : threads)
			{
				thread.run();
			}
		}
	}

	private class ExternalProcessThread extends HRDBMSThread
	{
		private ArrayList<ArrayList<Object>> rows;
		private final int par;
		private int pri = -1;

		public ExternalProcessThread(ArrayList<ArrayList<Object>> rows, int par)
		{
			this.rows = rows;
			this.par = par;
		}

		public void setPriority(int pri)
		{
			this.pri = pri;
		}

		@Override
		public void run()
		{
			if (pri != -1)
			{
				Thread.currentThread().setPriority(pri);
			}
			try
			{
				if (par == 1)
				{
					process(rows, 0, rows.size());
					return;
				}

				int i = 0;
				int pos = 0;
				int size = rows.size();
				int per = size / par;
				if (per == 0 && size != 0)
				{
					per = 1;
				}

				ArrayList<EPT2Thread> threads = new ArrayList<EPT2Thread>(par);
				while (i < par)
				{
					int end = pos + per;
					if (end > size)
					{
						end = size;
					}

					if (i == par - 1)
					{
						end = size;
					}

					EPT2Thread thread = new EPT2Thread(rows, pos, end);
					thread.start();
					threads.add(thread);
					pos = end;

					if (pos >= size)
					{
						break;
					}

					i++;
				}

				for (EPT2Thread thread : threads)
				{
					thread.join();
				}
			}
			catch (Exception e)
			{
				readBuffer.put(e);
			}
		}

		private void process(ArrayList<ArrayList<Object>> probe, int start, int end) throws Exception
		{
			HashSet<ArrayList<Object>> groups = new HashSet<ArrayList<Object>>();
			AggregateResultThread[] threads = new AggregateResultThread[ops.size()];
			ArrayList<Integer> groupPos = null;
			groupPos = new ArrayList<Integer>(groupCols.size());
			for (final String groupCol : groupCols)
			{
				groupPos.add(child.getCols2Pos().get(groupCol));
			}

			try
			{
				int i = 0;
				for (final AggregateOperator op : ops)
				{
					threads[i] = op.getHashThread(child.getCols2Pos());
					i++;
				}

				int z = 0;
				final int limit = probe.size();
				while (z < limit)
				{
					ArrayList<Object> o = probe.get(z++);
					final ArrayList<Object> row = o;
					final ArrayList<Object> groupKeys = new ArrayList<Object>();

					try
					{
						for (final int pos : groupPos)
						{
							groupKeys.add(row.get(pos));
						}
					}
					catch (Exception e)
					{
						HRDBMSWorker.logger.debug("Trying to group on " + groupCols);
						HRDBMSWorker.logger.debug("Child.getCols2Pos() = " + child.getCols2Pos());
						throw e;
					}

					groups.add(groupKeys);
					// groups.add(groupKeys);

					for (final AggregateResultThread thread : threads)
					{
						thread.put(row, groupKeys);
					}
				}

				for (final Object k : groups)
				// for (Object k : groups.getArray())
				{
					final ArrayList<Object> keys = (ArrayList<Object>)k;
					final ArrayList<Object> row = new ArrayList<Object>();
					for (final Object field : keys)
					{
						row.add(field);
					}

					for (final AggregateResultThread thread : threads)
					{
						row.add(thread.getResult(keys));
					}

					readBuffer.put(row);
				}

				for (final AggregateResultThread thread : threads)
				{
					thread.close();
				}

				rows = null;
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				try
				{
					readBuffer.put(e);
				}
				catch (Exception f)
				{
				}
				return;
			}
		}

		private class EPT2Thread extends HRDBMSThread
		{
			private final ArrayList<ArrayList<Object>> probe;
			private final int start;
			private final int end;

			public EPT2Thread(ArrayList<ArrayList<Object>> probe, int start, int end)
			{
				this.probe = probe;
				this.start = start;
				this.end = end;
			}

			@Override
			public void run()
			{
				try
				{
					process(probe, start, end);
				}
				catch (Exception e)
				{
					readBuffer.put(e);
				}
			}
		}
	}

	private class ExternalThread extends HRDBMSThread
	{
		private final double percentInMem;

		public ExternalThread()
		{
			percentInMem = 0;
		}

		public ExternalThread(double percentInMem)
		{
			this.percentInMem = percentInMem;
		}

		@Override
		public void run()
		{
			external(percentInMem);
		}
	}

	private class FlushBinThread extends HRDBMSThread
	{
		private final byte[] types;
		private ArrayList<ArrayList<Object>> bin;
		private final FileChannel fc;
		private boolean ok = true;
		private Exception e;
		private ByteBuffer direct;
		private boolean force = false;

		public FlushBinThread(ArrayList<ArrayList<Object>> bin, byte[] types, FileChannel fc, ByteBuffer direct)
		{
			this.types = types;
			this.bin = bin;
			this.fc = fc;
			this.direct = direct;
		}
		
		public FlushBinThread(ArrayList<ArrayList<Object>> bin, byte[] types, FileChannel fc, ByteBuffer direct, boolean force)
		{
			this.types = types;
			this.bin = bin;
			this.fc = fc;
			this.direct = direct;
			this.force = force;
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
			//Thread.currentThread().setPriority(Thread.MAX_PRIORITY);
			try
			{
				byte[] data = rsToBytes(bin, types);
				//HRDBMSWorker.logger.debug("HJO bin flush size = " + (data.length * 1.0 / 1048576) + "MB");
				bin = null;
				
				if (direct == null)
				{
					// fc.position(fc.size());
					ByteBuffer bb = ByteBuffer.wrap(data);
					fc.write(bb);
				}
				else
				{
					if (direct.position() + data.length <= 8 * 1024 * 1024)
					{
						try
						{
							direct.put(data);
						}
						catch(Exception e)
						{
							HRDBMSWorker.logger.debug("", e);
							HRDBMSWorker.logger.debug("Capacity: " + direct.capacity() + " Limit: " + direct.limit() + " Position: " + direct.position() + " Write size: " + data.length);
						}
						
						if (force)
						{
							direct.limit(direct.position());
							direct.position(0);
							fc.write(direct);
							direct.limit(direct.capacity());
						}
					}
					else
					{
						if (direct.position() > 0)
						{
							direct.limit(direct.position());
							direct.position(0);
							fc.write(direct);
							direct.limit(direct.capacity());
						}
						
						if (!force && data.length <= 8 * 1024 * 1024)
						{
							direct.position(0);
							direct.put(data);
						}
						else
						{
							ByteBuffer bb = ByteBuffer.wrap(data);
							fc.write(bb);
						}
					}
				}
			}
			catch (Exception e)
			{
				HRDBMSWorker.logger.debug("", e);
				ok = false;
				this.e = e;
			}
		}
	}

	private final class HashGroupByThread extends ThreadPoolThread
	{
		// private volatile DiskBackedHashSet groups =
		// ResourceManager.newDiskBackedHashSet(true, NUM_GROUPS > 0 ?
		// NUM_GROUPS : 16);
		private volatile ConcurrentHashMap<ArrayList<Object>, ArrayList<Object>> groups = new ConcurrentHashMap<ArrayList<Object>, ArrayList<Object>>(NUM_GROUPS, 0.75f, 6 * ResourceManager.cpus);

		private final AggregateResultThread[] threads = new AggregateResultThread[ops.size()];

		@Override
		public void run()
		{
			try
			{
				int i = 0;
				for (final AggregateOperator op : ops)
				{
					threads[i] = op.getHashThread(child.getCols2Pos());
					i++;
				}

				i = 0;
				final HashGroupByReaderThread[] threads2 = new HashGroupByReaderThread[NUM_HGBR_THREADS];
				while (i < NUM_HGBR_THREADS)
				{
					threads2[i] = new HashGroupByReaderThread();
					threads2[i].start();
					i++;
				}

				i = 0;
				while (i < NUM_HGBR_THREADS)
				{
					threads2[i].join();
					i++;
				}

				// groups.close();

				for (final Object k : groups.keySet())
				// for (Object k : groups.getArray())
				{
					final ArrayList<Object> keys = (ArrayList<Object>)k;
					final ArrayList<Object> row = new ArrayList<Object>();
					for (final Object field : keys)
					{
						row.add(field);
					}

					for (final AggregateResultThread thread : threads)
					{
						row.add(thread.getResult(keys));
					}

					readBuffer.put(row);
				}

				// groups.getArray().close();
				readBuffer.put(new DataEndMarker());

				for (final AggregateResultThread thread : threads)
				{
					thread.close();
				}
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				try
				{
					readBuffer.put(e);
				}
				catch (Exception f)
				{
				}
				return;
			}
		}

		private final class HashGroupByReaderThread extends ThreadPoolThread
		{
			private ArrayList<Integer> groupPos = null;

			@Override
			public void run()
			{
				double tF = NUM_GROUPS * 1.0 / childCard;
				try
				{
					Object o = child.next(MultiOperator.this);
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
						final ArrayList<Object> row = (ArrayList<Object>)o;
						final ArrayList<Object> groupKeys = new ArrayList<Object>();

						if (groupPos == null)
						{
							groupPos = new ArrayList<Integer>(groupCols.size());
							for (final String groupCol : groupCols)
							{
								groupPos.add(child.getCols2Pos().get(groupCol));
							}
						}

						try
						{
							for (final int pos : groupPos)
							{
								groupKeys.add(row.get(pos));
							}
						}
						catch (Exception e)
						{
							HRDBMSWorker.logger.debug("Trying to group on " + groupCols);
							HRDBMSWorker.logger.debug("Child.getCols2Pos() = " + child.getCols2Pos());
							throw e;
						}

						if (tF >= 0.5)
						{
							groups.putIfAbsent(groupKeys, groupKeys);
						}
						else
						{
							ArrayList<Object> obj = groups.get(groupKeys);
							if (obj == null)
							{
								groups.putIfAbsent(groupKeys, groupKeys);
							}
						}
						// groups.add(groupKeys);

						for (final AggregateResultThread thread : threads)
						{
							thread.put(row, groupKeys);
						}

						o = child.next(MultiOperator.this);
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
				catch (final Exception e)
				{
					HRDBMSWorker.logger.error("", e);
					try
					{
						readBuffer.put(e);
					}
					catch (Exception f)
					{
					}
					return;
				}
			}
		}
	}

	private final class InitThread extends ThreadPoolThread
	{
		@Override
		public void run()
		{
			try
			{
				final Object[] groupKeys = new Object[groupCols.size()];
				Object[] oldGroup = null;
				ArrayList<ArrayList<Object>> rows = null;
				boolean newGroup = false;

				ArrayList<Integer> indxs = new ArrayList<Integer>(groupCols.size());
				for (String groupCol : groupCols)
				{
					indxs.add(child.getCols2Pos().get(groupCol));
				}

				Object o = child.next(MultiOperator.this);
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
					newGroup = false;
					oldGroup = null;
					final ArrayList<Object> row = (ArrayList<Object>)o;
					int i = 0;
					for (final int indx : indxs)
					{
						if (row.get(indx).equals(groupKeys[i]))
						{
						}
						else
						{
							newGroup = true;
							if (oldGroup == null)
							{
								oldGroup = groupKeys.clone();
							}
							groupKeys[i] = row.get(indx);
						}

						i++;
					}

					if (newGroup)
					{
						if (rows != null)
						{
							final AggregateThread aggThread = new AggregateThread(oldGroup, ops, rows);
							aggThread.start();
							readBuffer.put(aggThread.getResult());
							rows.clear();
						}
						else
						{
							rows = new ArrayList<ArrayList<Object>>();
						}
					}

					rows.add(row);
					o = child.next(MultiOperator.this);
					if (o instanceof DataEndMarker)
					{
						demReceived = true;
					}
					else
					{
						received.getAndIncrement();
					}
				}

				AggregateThread aggThread = new AggregateThread(groupKeys, ops, rows);
				aggThread.start();
				readBuffer.put(aggThread.getResult());
				readBuffer.put(new DataEndMarker());
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				try
				{
					readBuffer.put(e);
				}
				catch (Exception f)
				{
				}
				return;
			}
		}
	}

	private class LeftThread extends HRDBMSThread
	{
		private final ArrayList<FileChannel> channels;
		private final int numBins;
		private final byte[] types;
		private boolean ok = true;
		private Exception e;
		private final int inMemBins;
		private ArrayList<ArrayList<ArrayList<Object>>> bins;
		private ScalableStampedReentrantRWLock rwLock;
		private ArrayList<ByteBuffer> directs;

		public LeftThread(ArrayList<RandomAccessFile> files, ArrayList<FileChannel> channels, int numBins, byte[] types, int inMemBins)
		{
			this.channels = channels;
			this.numBins = numBins;
			this.types = types;
			this.inMemBins = inMemBins;
		}

		public ArrayList<ArrayList<ArrayList<Object>>> getBins()
		{
			return bins;
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
			rwLock = new ScalableStampedReentrantRWLock();
			bins = new ArrayList<ArrayList<ArrayList<Object>>>();
			directs = new ArrayList<ByteBuffer>();
			ConcurrentHashMap<Integer, FlushBinThread> threads = new ConcurrentHashMap<Integer, FlushBinThread>();
			int size = (int)(ResourceManager.QUEUE_SIZE * Double.parseDouble(HRDBMSWorker.getHParms().getProperty("external_factor")) / (numBins >> SHIFT));
			int i = 0;
			while (i < numBins)
			{
				bins.add(new ArrayList<ArrayList<Object>>(size));
				directs.add(null);
				i++;
			}
			
			if (HRDBMSWorker.getHParms().getProperty("use_direct_buffers_for_flush").equals("true"))
			{
				i = 0;
				while (i < numBins)
				{
					directs.set(i, TempThread.getDirect());
					i++;
				}
			}
			
			ArrayList<SubLeftThread> slThreads = new ArrayList<SubLeftThread>();
			i = 0;
			while (i < 1)
			{
				SubLeftThread t = new SubLeftThread(this, threads, size);
				t.start();
				slThreads.add(t);
				i++;
			}

			try
			{
				final HashMap<String, Integer> childCols2Pos = child.getCols2Pos();
				Object o = child.next(MultiOperator.this);
				if (o instanceof DataEndMarker)
				{
					demReceived = true;
				}
				else
				{
					received.getAndIncrement();
				}

				int[] poses = new int[groupCols.size()];
				i = 0;
				for (String col : groupCols)
				{
					poses[i] = childCols2Pos.get(col);
					i++;
				}
				final ArrayList<Object> key = new ArrayList<Object>(groupCols.size());
				while (!(o instanceof DataEndMarker))
				{
					i = 0;
					key.clear();
					for (int pos : poses)
					{
						try
						{
							key.add(((ArrayList<Object>)o).get(pos));
							i++;
						}
						catch (Exception e)
						{
							HRDBMSWorker.logger.debug("Failed to find a column in " + childCols2Pos);
							throw e;
						}
					}

					final long hash = 0x7FFFFFFFFFFFFFFFL & hash(key);
					int x = (int)(hash % numBins);
					rwLock.readLock().lock();
					ArrayList<ArrayList<Object>> bin = bins.get(x);
					synchronized(bin)
					{
						// writeToHashTable(hash, (ArrayList<Object>)o);
						bin.add((ArrayList<Object>)o);
					}
					rwLock.readLock().unlock();

					if (x >= inMemBins && bin.size() >= size)
					{
						rwLock.writeLock().lock();
						bin = bins.get(x);
						if (bin.size() >= size)
						{
							FlushBinThread thread = new FlushBinThread(bin, types, channels.get(x), directs.get(x));
							if (threads.putIfAbsent(x, thread) != null)
							{
								threads.get(x).join();
								if (!threads.get(x).getOK())
								{
									rwLock.writeLock().unlock();
									throw threads.get(x).getException();
								}

								threads.put(x, thread);
							}
							TempThread.start(thread, txnum);
							bins.set(x, new ArrayList<ArrayList<Object>>(size));
						}
						
						rwLock.writeLock().unlock();
					}

					o = child.next(MultiOperator.this);
					if (o instanceof DataEndMarker)
					{
						demReceived = true;
					}
					else
					{
						received.getAndIncrement();
					}
				}
				
				for (SubLeftThread t : slThreads)
				{
					t.join();
					if (!t.getOK())
					{
						throw t.getException();
					}
				}

				i = 0;
				for (ArrayList<ArrayList<Object>> bin : bins)
				{
					if (i >= inMemBins && bin.size() > 0)
					{
						FlushBinThread thread = new FlushBinThread(bin, types, channels.get(i), directs.get(i), true);
						if (threads.putIfAbsent(i, thread) != null)
						{
							threads.get(i).join();
							if (!threads.get(i).getOK())
							{
								throw threads.get(i).getException();
							}

							threads.put(i, thread);
						}
						TempThread.start(thread, txnum);
					}

					i++;
				}

				for (FlushBinThread thread : threads.values())
				{
					thread.join();
					if (!thread.getOK())
					{
						throw thread.getException();
					}
				}

				// everything is written
				i = 0;
				while (i < numBins)
				{
					ByteBuffer bb = directs.get(i);
					if (bb != null)
					{
						TempThread.freeDirect(bb);
					}
					
					i++;
				}
				
				i = numBins - 1;
				while (i >= inMemBins)
				{
					bins.remove(i);
					i--;
				}
			}
			catch (final Exception e)
			{
				ok = false;
				this.e = e;
			}
		}
	}
	
	private class SubLeftThread extends HRDBMSThread
	{
		private LeftThread leftThread;
		private ConcurrentHashMap<Integer, FlushBinThread> threads;
		private final int size;
		private boolean ok = true;
		private Exception e;
		
		public SubLeftThread(LeftThread leftThread, ConcurrentHashMap<Integer, FlushBinThread> threads, int size)
		{
			this.leftThread = leftThread;
			this.threads = threads;
			this.size = size;
		}
		
		public boolean getOK()
		{
			return ok;
		}
		
		public Exception getException()
		{
			return e;
		}
		
		public void run()
		{
			try
			{
				final HashMap<String, Integer> childCols2Pos = child.getCols2Pos();
				Object o = child.next(MultiOperator.this);
				if (o instanceof DataEndMarker)
				{
					demReceived = true;
				}
				else
				{
					received.getAndIncrement();
				}

				int[] poses = new int[groupCols.size()];
				int i = 0;
				for (String col : groupCols)
				{
					poses[i] = childCols2Pos.get(col);
					i++;
				}
				final ArrayList<Object> key = new ArrayList<Object>(groupCols.size());
				while (!(o instanceof DataEndMarker))
				{
					i = 0;
					key.clear();
					for (int pos : poses)
					{
						try
						{
							key.add(((ArrayList<Object>)o).get(pos));
							i++;
						}
						catch (Exception e)
						{
							HRDBMSWorker.logger.debug("Failed to find a column in " + childCols2Pos);
							throw e;
						}
					}

					final long hash = 0x7FFFFFFFFFFFFFFFL & hash(key);
					int x = (int)(hash % leftThread.numBins);
					leftThread.rwLock.readLock().lock();
					ArrayList<ArrayList<Object>> bin = leftThread.bins.get(x);
					synchronized(bin)
					{
						// writeToHashTable(hash, (ArrayList<Object>)o);
						bin.add((ArrayList<Object>)o);
					}
					leftThread.rwLock.readLock().unlock();

					if (x >= leftThread.inMemBins && bin.size() >= size)
					{
						leftThread.rwLock.writeLock().lock();
						bin = leftThread.bins.get(x);
						if (bin.size() >= size)
						{
							FlushBinThread thread = new FlushBinThread(bin, leftThread.types, leftThread.channels.get(x), leftThread.directs.get(x));
							if (threads.putIfAbsent(x, thread) != null)
							{
								threads.get(x).join();
								if (!threads.get(x).getOK())
								{
									leftThread.rwLock.writeLock().unlock();
									throw threads.get(x).getException();
								}

								threads.put(x, thread);
							}
							TempThread.start(thread, txnum);
							leftThread.bins.set(x, new ArrayList<ArrayList<Object>>(size));
						}
						
						leftThread.rwLock.writeLock().unlock();
					}

					o = child.next(MultiOperator.this);
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
			catch(Exception e)
			{
				ok = false;
				this.e = e;
			}
		}
	}

	private class ReadDataThread extends HRDBMSThread
	{
		private final FileChannel fc;
		private boolean ok = true;
		private Exception e;
		private final byte[] types;
		private int num = 0;
		private int pri = -1;

		public ReadDataThread(FileChannel fc, byte[] types) throws Exception
		{
			this.fc = new BufferedFileChannel(fc, 8 * 1024 * 1024);
			this.types = types;
		}

		public void setPriority(int pri)
		{
			this.pri = pri;
		}

		public int getNumPar()
		{
			int internal = num / 250000;
			int maxPar = Runtime.getRuntime().availableProcessors();
			if (internal == 0)
			{
				internal = 1;
			}

			if (internal > maxPar)
			{
				internal = maxPar;
			}

			return maxPar / internal;
		}

		@Override
		public void run()
		{
			if (pri != -1)
			{
				Thread.currentThread().setPriority(pri);
			}
			try
			{
				ConcurrentHashMap<ArrayList<Object>, Integer> groups = new ConcurrentHashMap<ArrayList<Object>, Integer>();
				AggregateResultThread[] threads = new AggregateResultThread[ops.size()];
				ArrayList<Integer> groupPos = new ArrayList<Integer>(groupCols.size());
				for (final String groupCol : groupCols)
				{
					groupPos.add(child.getCols2Pos().get(groupCol));
				}

				int i = 0;
				for (final AggregateOperator op : ops)
				{
					threads[i] = op.getHashThread(child.getCols2Pos());
					i++;
				}

				fc.position(0);
				ConcurrentHashMap<byte[], Integer> bas = new ConcurrentHashMap<byte[], Integer>();
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
					num++;
					ByteBuffer bb = ByteBuffer.allocate(length);
					fc.read(bb);
					bas.put(bb.array(), Integer.valueOf(1));
				}
				
				bas.forEachKey(20000, (k)->process(k, groups, groupPos, threads));
				//bas = null;

				for (final Object k : groups.keySet())
				// for (Object k : groups.getArray())
				{
					final ArrayList<Object> keys = (ArrayList<Object>)k;
					final ArrayList<Object> row = new ArrayList<Object>();
					for (final Object field : keys)
					{
						row.add(field);
					}

					for (final AggregateResultThread thread : threads)
					{
						row.add(thread.getResult(keys));
					}

					readBuffer.put(row);
				}
				
				//groups.clear();

				//for (final AggregateResultThread thread : threads)
				//{
				//	thread.close();
				//}
			}
			catch (Exception e)
			{
				ok = false;
				this.e = e;
			}
		}
		
		private void process(byte[] k, ConcurrentHashMap<ArrayList<Object>, Integer> groups, ArrayList<Integer> groupPos, AggregateResultThread[] threads)
		{
			try
			{
				ArrayList<Object> row = (ArrayList<Object>)fromBytes(k, types);
				final ArrayList<Object> groupKeys = new ArrayList<Object>();

				try
				{
					for (final int pos : groupPos)
					{
						groupKeys.add(row.get(pos));
					}
				}
				catch (Exception e)
				{
					HRDBMSWorker.logger.debug("Trying to group on " + groupCols);
					HRDBMSWorker.logger.debug("Child.getCols2Pos() = " + child.getCols2Pos());
					throw e;
				}

				groups.put(groupKeys, Integer.valueOf(1));
				// groups.add(groupKeys);

				for (final AggregateResultThread thread : threads)
				{
					thread.put(row, groupKeys);
				}
			}
			catch (Exception e)
			{
				throw new RuntimeException(e);
			}
		}
	}
}
