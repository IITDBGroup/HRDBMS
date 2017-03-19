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
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.managers.ResourceManager;
import com.exascale.misc.*;
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
			final int max = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("agg_max_par"));
			if (NUM_HGBR_THREADS > max)
			{
				NUM_HGBR_THREADS = max;
			}
		}
		catch (final Exception e)
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
	private long NUM_GROUPS = 16;
	private long childCard = 16 * 16;
	private transient ArrayList<String> externalFiles;
	private boolean external = false;

	private boolean cardSet = false;

	private volatile boolean startDone = false;
	private transient AtomicLong received;
	private transient volatile boolean demReceived;
	private long txnum;

	public MultiOperator(final ArrayList<AggregateOperator> ops, final ArrayList<String> groupCols, final MetaData meta, final boolean sorted)
	{
		this.ops = ops;
		this.groupCols = groupCols;
		this.meta = meta;
		this.sorted = sorted;
		received = new AtomicLong(0);
	}

	public static MultiOperator deserialize(final InputStream in, final HashMap<Long, Object> prev) throws Exception
	{
		final MultiOperator value = (MultiOperator)unsafe.allocateInstance(MultiOperator.class);
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
		value.NUM_GROUPS = OperatorUtils.readLong(in);
		value.childCard = OperatorUtils.readLong(in);
		value.cardSet = OperatorUtils.readBool(in);
		value.startDone = OperatorUtils.readBool(in);
		value.external = OperatorUtils.readBool(in);
		value.received = new AtomicLong(0);
		value.demReceived = false;
		value.txnum = OperatorUtils.readLong(in);
		return value;
	}

	private static ArrayList<FileChannel> createChannels(final ArrayList<RandomAccessFile> files)
	{
		final ArrayList<FileChannel> retval = new ArrayList<FileChannel>(files.size());
		for (final RandomAccessFile raf : files)
		{
			retval.add(raf.getChannel());
		}

		return retval;
	}

	private static ArrayList<RandomAccessFile> createFiles(final ArrayList<String> fns) throws Exception
	{
		final ArrayList<RandomAccessFile> retval = new ArrayList<RandomAccessFile>(fns.size());
		for (final String fn : fns)
		{
			while (true)
			{
				try
				{
					final RandomAccessFile raf = new RandomAccessFile(fn, "rw");
					retval.add(raf);
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
		}

		return retval;
	}

	private static final Object fromBytes(final byte[] val, final byte[] types) throws Exception
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
			Object o = null;
			if (types[i] == 0)
			{
				// long
				o = getCLong(bb);
			}
			else if (types[i] == 1)
			{
				// integer
				o = getCInt(bb);
			}
			else if (types[i] == 2)
			{
				// double
				o = bb.getDouble();
			}
			else if (types[i] == 3)
			{
				// date
				o = new MyDate(getMedium(bb));
			}
			else if (types[i] == 4)
			{
				// string
				final int length = getCInt(bb);

				byte[] temp = new byte[length];
				bb.get(temp);

				if (!Schema.CVarcharFV.compress)
				{
					o = new String(temp, StandardCharsets.UTF_8);
				}
				else
				{
					final byte[] out = new byte[temp.length << 1];
					final int clen = Schema.CVarcharFV.decompress(temp, temp.length, out);
					temp = new byte[clen];
					System.arraycopy(out, 0, temp, 0, clen);
					o = new String(temp, StandardCharsets.UTF_8);
				}
			}
			else
			{
				throw new Exception("Unknown type in fromBytes(): " + types[i]);
			}

			retval.add(o);
			i++;
		}

		return retval;
	}

	private static int getCInt(final ByteBuffer bb)
	{
		final int temp = (bb.get() & 0xff);
		final int length = (temp >>> 5);
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

	private static long getCLong(final ByteBuffer bb)
	{
		final int temp = (bb.get() & 0xff);
		final int length = (temp >>> 4);
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

	private static int getMedium(final ByteBuffer bb)
	{
		int retval = ((bb.getShort() & 0xffff) << 8);
		retval += (bb.get() & 0xff);
		return retval;
	}

	private static long hash(final Object key) throws Exception
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
				final byte[] data = toBytesForHash((ArrayList<Object>)key);
				eHash = MurmurHash.hash64(data, data.length);
			}
			else
			{
				final byte[] data = key.toString().getBytes(StandardCharsets.UTF_8);
				eHash = MurmurHash.hash64(data, data.length);
			}
		}

		return eHash;
	}

	private static void putCInt(final ByteBuffer bb, int val)
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

	private static void putCLong(final ByteBuffer bb, long val)
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

	private static void putMedium(final ByteBuffer bb, final int val)
	{
		bb.put((byte)((val & 0xff0000) >> 16));
		bb.put((byte)((val & 0xff00) >> 8));
		bb.put((byte)(val & 0xff));
	}

	private static final byte[] rsToBytes(final ArrayList<ArrayList<Object>> rows, final byte[] types) throws Exception
	{
		final ByteBuffer[] results = new ByteBuffer[rows.size()];
		int rIndex = 0;
		final ArrayList<byte[]> bytes = new ArrayList<byte[]>();
		final ArrayList<Integer> stringCols = new ArrayList<Integer>(rows.get(0).size());
		int startSize = 4;
		int a = 0;
		for (final byte b : types)
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

		for (final ArrayList<Object> val : rows)
		{
			int size = startSize;
			for (final int y : stringCols)
			{
				final Object o = val.get(y);
				byte[] b = ((String)o).getBytes(StandardCharsets.UTF_8);
				if (Schema.CVarcharFV.compress)
				{
					final byte[] out = new byte[b.length * 3 + 1];
					final int clen = Schema.CVarcharFV.compress(b, b.length, out);
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
					final int value = ((MyDate)o).getTime();
					// retvalBB.putInt(value);
					putMedium(retvalBB, value);
				}
				else if (types[i] == 4)
				{
					final byte[] temp = bytes.get(x++);
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

			results[rIndex++] = retvalBB;
			bytes.clear();
		}

		int count = 0;
		for (final ByteBuffer bb : results)
		{
			count += (bb.position() + 4);
		}
		final byte[] retval = new byte[count];
		final ByteBuffer retvalBB = ByteBuffer.wrap(retval);
		int retvalPos = 0;
		for (final ByteBuffer bb : results)
		{
			final byte[] ba = bb.array();
			retvalBB.position(retvalPos);
			retvalBB.putInt(bb.position());
			retvalPos += 4;
			System.arraycopy(ba, 0, retval, retvalPos, bb.position());
			retvalPos += bb.position();
		}

		return retval;
	}

	private static byte[] toBytesForHash(final ArrayList<Object> key)
	{
		final StringBuilder sb = new StringBuilder();
		for (final Object o : key)
		{
			if (o instanceof Double)
			{
				final DecimalFormat df = new DecimalFormat("0", DecimalFormatSymbols.getInstance(Locale.ENGLISH));
				df.setMaximumFractionDigits(340); // 340 =
													// DecimalFormat.DOUBLE_FRACTION_DIGITS

				sb.append(df.format(o));
				sb.append((char)0);
			}
			else if (o instanceof Number)
			{
				sb.append(o);
				sb.append((char)0);
			}
			else
			{
				sb.append(o.toString());
				sb.append((char)0);
			}
		}

		final int z = sb.length();
		final byte[] retval = new byte[z];
		int i = 0;
		while (i < z)
		{
			retval[i] = (byte)sb.charAt(i);
			i++;
		}

		return retval;
	}

	@Override
	public void add(final Operator op) throws Exception
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
					final ArrayList<String> newGroupCols = new ArrayList<String>();
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
							for (final String col : tempCols2Types.keySet())
							{
								final String col2 = col.substring(col.indexOf('.'));
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
				catch (final Exception e)
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

	public void addCount(final String outCol)
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
				final String col = ((CountOperator)op).getRealInputColumn();
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

		for (final String col : groupCols)
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
	public Object next(final Operator op) throws Exception
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
	public void nextAll(final Operator op) throws Exception
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
	public void registerParent(final Operator op) throws Exception
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
	public void removeChild(final Operator op)
	{
		if (op == child)
		{
			child = null;
			op.removeParent(this);
		}
	}

	@Override
	public void removeParent(final Operator op)
	{
		parent = null;
	}

	public void replaceAvgWithSumAndCount(final HashMap<String, ArrayList<String>> old2New)
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
	public void serialize(final OutputStream out, final IdentityHashMap<Object, Long> prev) throws Exception
	{
		final Long id = prev.get(this);
		if (id != null)
		{
			OperatorUtils.serializeReference(id, out);
			return;
		}

		OperatorUtils.writeType(HrdbmsType.MULTI, out);
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
		OperatorUtils.writeLong(NUM_GROUPS, out);
		OperatorUtils.writeLong(childCard, out);
		OperatorUtils.writeBool(cardSet, out);
		OperatorUtils.writeBool(startDone, out);
		OperatorUtils.writeBool(external, out);
		OperatorUtils.writeLong(txnum, out);
	}

	@Override
	public void setChildPos(final int pos)
	{
	}

	public void setExternal()
	{
		external = true;
	}

	@Override
	public void setNode(final int node)
	{
		this.node = node;
	}

	public boolean setNumGroupsAndChildCard(final long groups, final long childCard)
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
	public void setPlan(final Plan plan)
	{
	}

	public void setSorted()
	{
		sorted = true;
	}

	public void setTXNum(final long txnum)
	{
		this.txnum = txnum;
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
			final double percentInMem = 0;
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

	public void updateInputColumns(final ArrayList<String> outputs, final ArrayList<String> inputs)
	{
		for (final AggregateOperator op : ops)
		{
			final int index = outputs.indexOf(op.outputColumn());
			op.setInputColumn(inputs.get(index));
		}
	}

	private ArrayList<String> createFNs(final int num, final int extra)
	{
		final ArrayList<String> retval = new ArrayList<String>(num);
		int i = 0;
		while (i < num)
		{
			final String fn = ResourceManager.TEMP_DIRS.get(i % ResourceManager.TEMP_DIRS.size()) + this.hashCode() + "" + System.currentTimeMillis() + i + "_" + extra + ".exthash";
			retval.add(fn);
			i++;
		}

		return retval;
	}

	private void external(final double percentInMem)
	{
		try
		{
			// int numBins = 257;
			int numBins = (int)(this.childCard / Integer.parseInt(HRDBMSWorker.getHParms().getProperty("mo_bin_size")));
			// numBins *= groupCols.size();

			if (numBins < 2)
			{
				numBins = 2;
			}

			final int inMemBins = (int)(numBins * percentInMem);
			final byte[] types1 = new byte[child.getPos2Col().size()];
			int j = 0;
			for (final String col : child.getPos2Col().values())
			{
				final String type = child.getCols2Types().get(col);
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
			final ArrayList<String> fns1 = createFNs(numBins, 0);
			externalFiles.addAll(fns1);
			final ArrayList<RandomAccessFile> files1 = createFiles(fns1);
			final ArrayList<FileChannel> channels1 = createChannels(files1);
			final LeftThread thread1 = new LeftThread(files1, channels1, numBins, types1, inMemBins);
			thread1.start();

			while (true)
			{
				try
				{
					thread1.join();
					break;
				}
				catch (final InterruptedException e)
				{
				}
			}
			if (!thread1.getOK())
			{
				readBuffer.put(thread1.getException());
				return;
			}

			ArrayList<ArrayList<ArrayList<Object>>> lbins = thread1.getBins();
			thread1.directs = null;
			final ArrayList<ExternalProcessThread> epThreads = new ArrayList<ExternalProcessThread>();
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
				final ArrayList<ArrayList<Object>> data = lbins.get(z++);
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

				final ExternalProcessThread thread = new ExternalProcessThread(data, numPar1);
				// int pri = Thread.MAX_PRIORITY - epThreads.size();
				// if (pri < Thread.NORM_PRIORITY)
				// {
				// pri = Thread.NORM_PRIORITY;
				// }
				// thread.setPriority(pri);
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
			thread1.bins = null;
			int i = inMemBins;
			final ArrayList<ReadDataThread> leftThreads = new ArrayList<ReadDataThread>();
			numPar1 = -1;

			while (i < numBins)
			{
				final ReadDataThread thread4 = new ReadDataThread(channels1.get(i), types1);
				// int pri = Thread.MAX_PRIORITY - leftThreads.size();
				// if (pri < Thread.NORM_PRIORITY)
				// {
				// pri = Thread.NORM_PRIORITY;
				// }
				// thread4.setPriority(pri);
				thread4.start();
				leftThreads.add(thread4);
				i++;

				if (numPar1 == -1)
				{
					thread4.join();
					numPar1 = thread4.getNumPar();
					if (numPar1 > maxPar)
					{
						numPar1 = maxPar;
					}
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

			for (final ExternalProcessThread ept : epThreads)
			{
				ept.join();
			}

			for (final ReadDataThread thread : leftThreads)
			{
				thread.join();
			}

			readBuffer.put(new DataEndMarker());

			for (final FileChannel fc : channels1)
			{
				try
				{
					fc.close();
				}
				catch (final Exception e)
				{
				}
			}

			for (final RandomAccessFile raf : files1)
			{
				try
				{
					raf.close();
				}
				catch (final Exception e)
				{
				}
			}

			for (final String fn : externalFiles)
			{
				try
				{
					new File(fn).delete();
				}
				catch (final Exception e)
				{
				}
			}
		}
		catch (final Exception e)
		{
			readBuffer.put(e);
		}
	}

	private void init()
	{
		new InitThread().start();
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

		public AggregateThread(final Object[] groupKeys, final ArrayList<AggregateOperator> ops, final ArrayList<ArrayList<Object>> rows)
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
		// private int pri = -1;

		public ExternalProcessThread(final ArrayList<ArrayList<Object>> rows, final int par)
		{
			this.rows = rows;
			this.par = par;
		}

		@Override
		public void run()
		{
			// if (pri != -1)
			// {
			// Thread.currentThread().setPriority(pri);
			// }
			try
			{
				if (par == 1)
				{
					process(rows, 0, rows.size());
					rows = null;
					return;
				}

				int i = 0;
				int pos = 0;
				final int size = rows.size();
				int per = size / par;
				if (per == 0 && size != 0)
				{
					per = 1;
				}

				final ArrayList<EPT2Thread> threads = new ArrayList<EPT2Thread>(par);
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

					final EPT2Thread thread = new EPT2Thread(rows, pos, end);
					thread.start();
					threads.add(thread);
					pos = end;

					if (pos >= size)
					{
						break;
					}

					i++;
				}

				for (final EPT2Thread thread : threads)
				{
					thread.join();
				}
				
				rows = null;
			}
			catch (final Exception e)
			{
				readBuffer.put(e);
			}
		}

		// public void setPriority(int pri)
		// {
		// this.pri = pri;
		// }

		private void process(final ArrayList<ArrayList<Object>> probe, final int start, final int end) throws Exception
		{
			final HashSet<ArrayList<Object>> groups = new HashSet<ArrayList<Object>>();
			final AggregateResultThread[] threads = new AggregateResultThread[ops.size()];
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
					final ArrayList<Object> o = probe.get(z++);
					final ArrayList<Object> row = o;
					final ArrayList<Object> groupKeys = new ArrayList<Object>();

					try
					{
						for (final int pos : groupPos)
						{
							groupKeys.add(row.get(pos));
						}
					}
					catch (final Exception e)
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
				catch (final Exception f)
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

			public EPT2Thread(final ArrayList<ArrayList<Object>> probe, final int start, final int end)
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
				catch (final Exception e)
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

		public ExternalThread(final double percentInMem)
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
		private final ByteBuffer direct;
		private boolean force = false;
		private final int directSize = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("direct_buffer_size"));

		public FlushBinThread(final ArrayList<ArrayList<Object>> bin, final byte[] types, final FileChannel fc, final ByteBuffer direct)
		{
			this.types = types;
			this.bin = bin;
			this.fc = fc;
			this.direct = direct;
		}

		public FlushBinThread(final ArrayList<ArrayList<Object>> bin, final byte[] types, final FileChannel fc, final ByteBuffer direct, final boolean force)
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
			// Thread.currentThread().setPriority(Thread.MAX_PRIORITY);
			try
			{
				final byte[] data = rsToBytes(bin, types);
				// HRDBMSWorker.logger.debug("HJO bin flush size = " +
				// (data.length * 1.0 / 1048576) + "MB");
				bin = null;

				if (direct == null)
				{
					// fc.position(fc.size());
					final ByteBuffer bb = ByteBuffer.wrap(data);
					fc.write(bb);
				}
				else
				{
					if (direct.position() + data.length <= directSize)
					{
						try
						{
							direct.put(data);
						}
						catch (final Exception e)
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

						if (!force && data.length <= directSize)
						{
							direct.position(0);
							direct.put(data);
						}
						else
						{
							final ByteBuffer bb = ByteBuffer.wrap(data);
							fc.write(bb);
						}
					}
				}
			}
			catch (final Exception e)
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
		private volatile ConcurrentHashMap<ArrayList<Object>, ArrayList<Object>> groups = new ConcurrentHashMap<ArrayList<Object>, ArrayList<Object>>(NUM_GROUPS <= Integer.MAX_VALUE ? (int)NUM_GROUPS : Integer.MAX_VALUE, 0.75f, 6 * ResourceManager.cpus);

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
				catch (final Exception f)
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
				final double tF = NUM_GROUPS * 1.0 / childCard;
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
						catch (final Exception e)
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
							final ArrayList<Object> obj = groups.get(groupKeys);
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
					catch (final Exception f)
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

				final ArrayList<Integer> indxs = new ArrayList<Integer>(groupCols.size());
				for (final String groupCol : groupCols)
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

				final AggregateThread aggThread = new AggregateThread(groupKeys, ops, rows);
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
				catch (final Exception f)
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

		public LeftThread(final ArrayList<RandomAccessFile> files, final ArrayList<FileChannel> channels, final int numBins, final byte[] types, final int inMemBins)
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
			final ConcurrentHashMap<Integer, FlushBinThread> threads = new ConcurrentHashMap<Integer, FlushBinThread>();
			final int size = (int)(ResourceManager.QUEUE_SIZE * Double.parseDouble(HRDBMSWorker.getHParms().getProperty("external_factor")) / (numBins >> SHIFT));
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

			final ArrayList<SubLeftThread> slThreads = new ArrayList<SubLeftThread>();
			i = 0;
			while (i < 1)
			{
				final SubLeftThread t = new SubLeftThread(this, threads, size);
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

				final int[] poses = new int[groupCols.size()];
				i = 0;
				for (final String col : groupCols)
				{
					poses[i] = childCols2Pos.get(col);
					i++;
				}
				final ArrayList<Object> key = new ArrayList<Object>(groupCols.size());
				while (!(o instanceof DataEndMarker))
				{
					i = 0;
					key.clear();
					for (final int pos : poses)
					{
						try
						{
							key.add(((ArrayList<Object>)o).get(pos));
							i++;
						}
						catch (final Exception e)
						{
							HRDBMSWorker.logger.debug("Failed to find a column in " + childCols2Pos);
							throw e;
						}
					}

					final long hash = 0x7FFFFFFFFFFFFFFFL & hash(key);
					final int x = (int)(hash % numBins);
					rwLock.readLock().lock();
					ArrayList<ArrayList<Object>> bin = bins.get(x);
					synchronized (bin)
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
							final FlushBinThread thread = new FlushBinThread(bin, types, channels.get(x), directs.get(x));
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

				for (final SubLeftThread t : slThreads)
				{
					t.join();
					if (!t.getOK())
					{
						throw t.getException();
					}
				}

				i = 0;
				for (final ArrayList<ArrayList<Object>> bin : bins)
				{
					if (i >= inMemBins && bin.size() > 0)
					{
						final FlushBinThread thread = new FlushBinThread(bin, types, channels.get(i), directs.get(i), true);
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

				for (final FlushBinThread thread : threads.values())
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
					final ByteBuffer bb = directs.get(i);
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

	private class ReadDataThread extends HRDBMSThread
	{
		private final FileChannel fc;
		private final byte[] types;
		private int num = 0;
		// private int pri = -1;

		public ReadDataThread(final FileChannel fc, final byte[] types) throws Exception
		{
			this.fc = new BufferedFileChannel(fc, 8 * 1024 * 1024);
			this.types = types;
		}

		public int getNumPar()
		{
			int internal = num / 250000;
			final int maxPar = Runtime.getRuntime().availableProcessors();
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
			// if (pri != -1)
			// {
			// Thread.currentThread().setPriority(pri);
			// }
			try
			{
				final ConcurrentHashMap<ArrayList<Object>, Integer> groups = new ConcurrentHashMap<ArrayList<Object>, Integer>();
				final AggregateResultThread[] threads = new AggregateResultThread[ops.size()];
				final ArrayList<Integer> groupPos = new ArrayList<Integer>(groupCols.size());
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
				final ConcurrentHashMap<byte[], Integer> bas = new ConcurrentHashMap<byte[], Integer>();
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
					num++;
					final ByteBuffer bb = ByteBuffer.allocate(length);
					fc.read(bb);
					bas.put(bb.array(), Integer.valueOf(1));
				}

				bas.forEachKey(20000, (k) -> process(k, groups, groupPos, threads));
				// bas = null;

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

				// groups.clear();

				// for (final AggregateResultThread thread : threads)
				// {
				// thread.close();
				// }
			}
			catch (final Exception e)
			{
			}
		}

		// public void setPriority(int pri)
		// {
		// this.pri = pri;
		// }

		private void process(final byte[] k, final ConcurrentHashMap<ArrayList<Object>, Integer> groups, final ArrayList<Integer> groupPos, final AggregateResultThread[] threads)
		{
			try
			{
				final ArrayList<Object> row = (ArrayList<Object>)fromBytes(k, types);
				final ArrayList<Object> groupKeys = new ArrayList<Object>();

				try
				{
					for (final int pos : groupPos)
					{
						groupKeys.add(row.get(pos));
					}
				}
				catch (final Exception e)
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
			catch (final Exception e)
			{
				throw new RuntimeException(e);
			}
		}
	}

	private class SubLeftThread extends HRDBMSThread
	{
		private final LeftThread leftThread;
		private final ConcurrentHashMap<Integer, FlushBinThread> threads;
		private final int size;
		private boolean ok = true;
		private Exception e;

		public SubLeftThread(final LeftThread leftThread, final ConcurrentHashMap<Integer, FlushBinThread> threads, final int size)
		{
			this.leftThread = leftThread;
			this.threads = threads;
			this.size = size;
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

				final int[] poses = new int[groupCols.size()];
				int i = 0;
				for (final String col : groupCols)
				{
					poses[i] = childCols2Pos.get(col);
					i++;
				}
				final ArrayList<Object> key = new ArrayList<Object>(groupCols.size());
				while (!(o instanceof DataEndMarker))
				{
					i = 0;
					key.clear();
					for (final int pos : poses)
					{
						try
						{
							key.add(((ArrayList<Object>)o).get(pos));
							i++;
						}
						catch (final Exception e)
						{
							HRDBMSWorker.logger.debug("Failed to find a column in " + childCols2Pos);
							throw e;
						}
					}

					final long hash = 0x7FFFFFFFFFFFFFFFL & hash(key);
					final int x = (int)(hash % leftThread.numBins);
					leftThread.rwLock.readLock().lock();
					ArrayList<ArrayList<Object>> bin = leftThread.bins.get(x);
					synchronized (bin)
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
							final FlushBinThread thread = new FlushBinThread(bin, leftThread.types, leftThread.channels.get(x), leftThread.directs.get(x));
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
			catch (final Exception e)
			{
				ok = false;
				this.e = e;
			}
		}
	}
}
