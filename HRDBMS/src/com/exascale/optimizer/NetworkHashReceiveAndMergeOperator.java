package com.exascale.optimizer;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import com.exascale.compression.CompressedInputStream;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.misc.AuxPairingHeap;
import com.exascale.misc.DataEndMarker;
import com.exascale.misc.HrdbmsType;
import com.exascale.misc.MyDate;
import com.exascale.threads.ThreadPoolThread;

public final class NetworkHashReceiveAndMergeOperator extends NetworkReceiveOperator implements Operator, Serializable
{
	private static Charset cs = StandardCharsets.UTF_8;

	private static long offset;

	private static sun.misc.Unsafe unsafe;

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
	}

	private boolean send = false;
	private List<String> sortCols;
	private boolean[] orders;
	private int ID;
	private int[] sortPos;

	public NetworkHashReceiveAndMergeOperator(final int ID, final List<String> sortCols, final List<Boolean> orders, final MetaData meta)
	{
		super(meta);
		this.sortCols = sortCols;
		this.orders = new boolean[orders.size()];
		int i = 0;
		for (final boolean b : orders)
		{
			this.orders[i++] = b;
		}
		this.ID = ID;
		received = new AtomicLong(0);
	}

	public NetworkHashReceiveAndMergeOperator(final int ID, final List<String> sortCols, final boolean[] orders, final MetaData meta)
	{
		super(meta);
		this.sortCols = sortCols;
		this.orders = orders;
		this.ID = ID;
		received = new AtomicLong(0);
	}

	public static NetworkHashReceiveAndMergeOperator deserialize(final InputStream in, final Map<Long, Object> prev) throws Exception
	{
		final NetworkHashReceiveAndMergeOperator value = (NetworkHashReceiveAndMergeOperator)unsafe.allocateInstance(NetworkHashReceiveAndMergeOperator.class);
		prev.put(OperatorUtils.readLong(in), value);
		value.meta = new MetaData();
		value.children = OperatorUtils.deserializeALOp(in, prev);
		value.parent = OperatorUtils.deserializeOperator(in, prev);
		value.cols2Types = OperatorUtils.deserializeStringHM(in, prev);
		value.cols2Pos = OperatorUtils.deserializeStringIntHM(in, prev);
		value.pos2Col = OperatorUtils.deserializeTM(in, prev);
		value.fullyStarted = OperatorUtils.readBool(in);
		value.start = OperatorUtils.readLong(in);
		value.node = OperatorUtils.readInt(in);
		value.sortCols = OperatorUtils.deserializeALS(in, prev);
		value.orders = OperatorUtils.deserializeBoolArray(in, prev);
		value.ID = OperatorUtils.readInt(in);
		value.sortPos = OperatorUtils.deserializeIntArray(in, prev);
		value.send = OperatorUtils.readBool(in);
		value.cd = cs.newDecoder();
		value.received = new AtomicLong(0);
		value.demReceived = false;
		return value;
	}

	private static int bytesToInt(final byte[] val)
	{
		final int ret = java.nio.ByteBuffer.wrap(val).getInt();
		return ret;
	}

	protected static byte[] intToBytes(final int val)
	{
		final byte[] buff = new byte[4];
		buff[0] = (byte)(val >> 24);
		buff[1] = (byte)((val & 0x00FF0000) >> 16);
		buff[2] = (byte)((val & 0x0000FF00) >> 8);
		buff[3] = (byte)((val & 0x000000FF));
		return buff;
	}

	@Override
	public void add(final Operator op) throws Exception
	{
		try
		{
			super.add(op);
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.error("", e);
			throw e;
		}
		if (sortPos == null)
		{
			int i = 0;
			sortPos = new int[sortCols.size()];
			for (final String sortCol : sortCols)
			{
				sortPos[i] = cols2Pos.get(sortCol);
				i++;
			}
		}
	}

	@Override
	public NetworkHashReceiveAndMergeOperator clone()
	{
		final NetworkHashReceiveAndMergeOperator retval = new NetworkHashReceiveAndMergeOperator(ID, sortCols, orders, meta);
		retval.node = node;
		retval.send = send;
		return retval;
	}

	public int getID()
	{
		return ID;
	}

	@Override
	public MetaData getMeta()
	{
		return meta;
	}

	@Override
	public Object next(final Operator op2) throws Exception
	{
		Object o;
		o = outBuffer.take();

		if (o instanceof DataEndMarker)
		{
			demReceived = true;
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
		else
		{
			received.getAndIncrement();
		}

		if (o instanceof Exception)
		{
			throw (Exception)o;
		}
		return o;
	}

	@Override
	public long numRecsReceived()
	{
		return received.get();
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
			throw new Exception("NetworkHashReceiveAndMergeOperator only supports 1 parent.");
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

		OperatorUtils.writeType(HrdbmsType.NHRAM, out);
		prev.put(this, OperatorUtils.writeID(out));
		// recreate meta
		if (send)
		{
			OperatorUtils.serializeALOp(children, out, prev);
		}
		else
		{
			OperatorUtils.serializeALOp(children, out, prev, false);
		}
		parent.serialize(out, prev);
		OperatorUtils.serializeStringHM(cols2Types, out, prev);
		OperatorUtils.serializeStringIntHM(cols2Pos, out, prev);
		OperatorUtils.serializeTM(pos2Col, out, prev);
		OperatorUtils.writeBool(fullyStarted, out);
		OperatorUtils.writeLong(start, out);
		OperatorUtils.writeInt(node, out);
		OperatorUtils.serializeALS(sortCols, out, prev);
		OperatorUtils.serializeBoolArray(orders, out, prev);
		OperatorUtils.writeInt(ID, out);
		OperatorUtils.serializeIntArray(sortPos, out, prev);
		OperatorUtils.writeBool(send, out);
	}

	public void setID(final int id)
	{
		this.ID = id;
	}

	public void setSend()
	{
		send = true;
	}

	@Override
	public void start() throws Exception
	{
		if (!fullyStarted)
		{
			synchronized (this)
			{
				if (!fullyStarted)
				{
					super.start(true);
					HRDBMSWorker.logger.debug("Starting NetworkHashReceiveAndMergeOperator ID = " + ID);
					fullyStarted = true;
					for (final Operator op : children)
					{
						final NetworkSendOperator child = (NetworkSendOperator)op;
						child.clearParent();
						Socket sock = null;
						try
						{
							// sock = new
							// Socket(meta.getHostNameForNode(child.getNode()),
							// WORKER_PORT);
							sock = new Socket();
							sock.setReceiveBufferSize(4194304);
							sock.setSendBufferSize(4194304);
							sock.connect(new InetSocketAddress(MetaData.getHostNameForNode(child.getNode()), WORKER_PORT));
						}
						catch (final java.net.ConnectException e)
						{
							HRDBMSWorker.logger.error("Connection failed to " + MetaData.getHostNameForNode(child.getNode()), e);
							throw e;
						}
						socks.put(child, sock);
						final OutputStream out = sock.getOutputStream();
						final BufferedOutputStream out2 = new BufferedOutputStream(out);
						outs.put(child, out2);
						final InputStream in = new BufferedInputStream(sock.getInputStream(), 65536);
						ins.put(child, in);

						if (send)
						{
							final byte[] command = "SNDRMTTR".getBytes(StandardCharsets.UTF_8);
							final byte[] from = intToBytes(node);
							final byte[] to = intToBytes(child.getNode());
							final byte[] idBytes = intToBytes(ID);
							final byte[] data = new byte[command.length + from.length + to.length + idBytes.length];
							System.arraycopy(command, 0, data, 0, 8);
							System.arraycopy(from, 0, data, 8, 4);
							System.arraycopy(to, 0, data, 12, 4);
							System.arraycopy(idBytes, 0, data, 16, 4);
							out2.write(data);
							// out.flush();

							IdentityHashMap<Object, Long> map = new IdentityHashMap<Object, Long>();
							child.serialize(out2, map);
							map.clear();
							map = null;
							out2.flush();
						}
						else
						{
							final byte[] command = "SNDRMTT2".getBytes(StandardCharsets.UTF_8);
							final byte[] from = intToBytes(node);
							final byte[] to = intToBytes(child.getNode());
							final byte[] idBytes = intToBytes(ID);
							final byte[] data = new byte[command.length + from.length + to.length + idBytes.length];
							System.arraycopy(command, 0, data, 0, 8);
							System.arraycopy(from, 0, data, 8, 4);
							System.arraycopy(to, 0, data, 12, 4);
							System.arraycopy(idBytes, 0, data, 16, 4);
							out2.write(data);
							out2.flush();
						}
					}

					new InitThread().start();
				}
			}
		}
	}

	@Override
	public String toString()
	{
		return "NetworkHashReceiveAndMergeOperator(" + node + ")";
	}

	private class ALOO
	{
		private final List<Object> alo;
		private final Operator op;

		public ALOO(final List<Object> alo, final Operator op)
		{
			this.alo = alo;
			this.op = op;
		}

		@Override
		public boolean equals(final Object o)
		{
			return this == o;
		}

		public List<Object> getALO()
		{
			return alo;
		}

		public Operator getOp()
		{
			return op;
		}
	}

	private final class InitThread extends ThreadPoolThread
	{
		@Override
		public void run()
		{
			start = System.currentTimeMillis();
			final ReadThread readThread = new ReadThread(children);
			readThread.start();
			while (true)
			{
				try
				{
					readThread.join();
					break;
				}
				catch (final InterruptedException e)
				{
				}
			}
			// HRDBMSWorker.logger.debug("NetworkReceiveOperator: " +
			// ((bytes.get() / ((System.currentTimeMillis() - start) / 1000.0))
			// / (1024.0 * 1024.0)) + "MB/sec");

			while (true)
			{
				try
				{
					outBuffer.put(new DataEndMarker());
					break;
				}
				catch (final Exception e)
				{
				}
			}
		}
	}

	private class MergeComparator implements Comparator<Object>
	{
		@Override
		public int compare(final Object l2, final Object r2)
		{
			final ALOO l = (ALOO)l2;
			final ALOO r = (ALOO)r2;
			if (l == r)
			{
				return 0;
			}

			final List<Object> lhs = l.getALO();
			final List<Object> rhs = r.getALO();
			int result = 0;
			int i = 0;

			for (final int pos : sortPos)
			{
				Object lField = lhs.get(pos);
				final Object rField = rhs.get(pos);

				if (lField instanceof Integer)
				{
					result = ((Integer)lField).compareTo((Integer)rField);
				}
				else if (lField instanceof Long)
				{
					result = ((Long)lField).compareTo((Long)rField);
				}
				else if (lField instanceof Double)
				{
					result = ((Double)lField).compareTo((Double)rField);
				}
				else if (lField instanceof String)
				{
					result = ((String)lField).compareTo((String)rField);
				}
				else if (lField instanceof MyDate)
				{
					result = ((MyDate)lField).compareTo(rField);
				}
				else
				{
					lField = null;
					lField.toString();
				}

				if (orders[i])
				{
					if (result > 0)
					{
						return 1;
					}
					else if (result < 0)
					{
						return -1;
					}
				}
				else
				{
					if (result > 0)
					{
						return -1;
					}
					else if (result < 0)
					{
						return 1;
					}
				}

				i++;
			}

			return 0;
		}
	}

	private final class ReadThread extends ThreadPoolThread
	{
		private final List<Operator> children;
		// private final Map<Operator, List<Object>> rows = new
		// HashMap<Operator, List<Object>>();
		// private final Map<List<Object>, Operator> rows = new
		// TreeMap<List<Object>, Operator>(new MergeComparator());
		private final AuxPairingHeap<ALOO> rows = new AuxPairingHeap<ALOO>(new MergeComparator());
		private ALOO minEntry;

		public ReadThread(final List<Operator> children)
		{
			this.children = children;
		}

		@Override
		public void run()
		{
			for (final Operator op : children)
			{
				try
				{
					final List<Object> row = readRow(op);
					if (row != null)
					{
						rows.insert(new ALOO(row, op));
					}
				}
				catch (final Exception e)
				{
					try
					{
						outBuffer.put(e);
					}
					catch (final Exception f)
					{
					}
					return;
				}
			}

			while (rows.size() > 0)
			{
				minEntry = rows.extractMin();

				while (true)
				{
					try
					{
						outBuffer.put(minEntry.getALO());
						break;
					}
					catch (final Exception e)
					{
					}
				}
				try
				{
					final List<Object> row = readRow(minEntry.getOp());
					if (row != null)
					{
						rows.insert(new ALOO(row, minEntry.getOp()));
					}
				}
				catch (final Exception e)
				{
					try
					{
						outBuffer.put(e);
					}
					catch (final Exception f)
					{
					}
					return;
				}
			}
		}

		private Object fromBytes(final byte[] val) throws Exception
		{
			final ByteBuffer bb = ByteBuffer.wrap(val);
			final int numFields = bb.getInt();

			if (numFields < 0)
			{
				HRDBMSWorker.logger.error("Negative number of fields in fromBytes()");
				HRDBMSWorker.logger.error("NumFields = " + numFields);
				throw new Exception("Negative number of fields in fromBytes()");
			}

			bb.position(bb.position() + numFields);
			final byte[] bytes = bb.array();
			if (bytes[4] == 5)
			{
				return new DataEndMarker();
			}
			if (bytes[4] == 10)
			{
				final int length = bb.getInt();
				final byte[] temp = new byte[length];
				bb.get(temp);
				try
				{
					final String o = new String(temp, StandardCharsets.UTF_8);
					return new Exception(o);
				}
				catch (final Exception e)
				{
					throw e;
				}
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
					final char[] ca = new char[length];
					bb.get(temp);
					try
					{
						// final String o = new String(temp,
						// StandardCharsets.UTF_8);
						final String value = (String)unsafe.allocateInstance(String.class);
						final int clen = ((sun.nio.cs.ArrayDecoder)cd).decode(temp, 0, length, ca);
						if (clen == ca.length)
						{
							unsafe.putObject(value, offset, ca);
						}
						else
						{
							final char[] v = Arrays.copyOf(ca, clen);
							unsafe.putObject(value, offset, v);
						}
						retval.add(value);
					}
					catch (final Exception e)
					{
						HRDBMSWorker.logger.error("", e);
						throw e;
					}
				}
				else
				{
					HRDBMSWorker.logger.error("Unknown type " + bytes[i + 4] + " in fromBytes()");
					throw new Exception("Unknown type " + bytes[i + 4] + " in fromBytes()");
				}

				i++;
			}

			return retval;
		}

		private List<Object> readRow(final Operator op) throws Exception
		{
			try
			{
				final InputStream i = ins.get(op);
				final InputStream in = new CompressedInputStream(i);
				final byte[] sizeBuff = new byte[4];
				byte[] data = null;
				while (true)
				{
					int count = 0;
					while (count < 4)
					{
						count += in.read(sizeBuff, count, 4 - count);
					}
					// bytes.getAndAdd(4);
					final int size = bytesToInt(sizeBuff);

					if (data == null || data.length < size)
					{
						data = new byte[size];
					}
					count = 0;
					while (count < size)
					{
						count += in.read(data, count, size - count);
					}
					// bytes.getAndAdd(size);
					final Object row = fromBytes(data);

					if (row instanceof DataEndMarker)
					{
						return null;
					}

					if (row instanceof Exception)
					{
						throw (Exception)row;
					}

					// readCounter.getAndIncrement();
					return (List<Object>)row;
				}
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				throw e;
			}
		}
	}
}
