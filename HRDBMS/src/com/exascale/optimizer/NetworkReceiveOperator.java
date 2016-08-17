package com.exascale.optimizer;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import com.exascale.compression.CompressedInputStream;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.managers.ResourceManager;
import com.exascale.misc.BufferedFileChannel;
import com.exascale.misc.BufferedLinkedBlockingQueue;
import com.exascale.misc.DataEndMarker;
import com.exascale.misc.MyDate;
import com.exascale.tables.Plan;
import com.exascale.threads.HRDBMSThread;
import com.exascale.threads.TempThread;
import com.exascale.threads.ThreadPoolThread;

public class NetworkReceiveOperator implements Operator, Serializable
{
	protected static final int WORKER_PORT = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number"));

	protected static Charset cs = StandardCharsets.UTF_8;
	protected static long offset;

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
		catch (Exception e)
		{
			unsafe = null;
		}
	}
	protected CharsetDecoder cd = cs.newDecoder();
	protected MetaData meta;
	protected ArrayList<Operator> children = new ArrayList<Operator>();
	protected Operator parent;
	protected HashMap<String, String> cols2Types;
	protected HashMap<String, Integer> cols2Pos;

	protected TreeMap<Integer, String> pos2Col;
	protected transient HashMap<Operator, Socket> socks;
	protected transient HashMap<Operator, OutputStream> outs;
	protected transient HashMap<Operator, InputStream> ins;
	protected transient BufferedLinkedBlockingQueue outBuffer;
	protected transient ArrayList<ReadThread> threads;
	protected volatile boolean fullyStarted = false;
	// protected AtomicLong readCounter = new AtomicLong(0);
	// protected AtomicLong bytes = new AtomicLong(0);
	protected long start;

	protected int node;

	protected transient AtomicLong received;
	protected transient volatile boolean demReceived;
	protected transient long txnum;
	protected transient Object[] readThrottle;

	public NetworkReceiveOperator(MetaData meta)
	{
		this.meta = meta;
		received = new AtomicLong(0);
	}

	protected NetworkReceiveOperator()
	{
		received = new AtomicLong(0);
	}

	public static NetworkReceiveOperator deserialize(InputStream in, HashMap<Long, Object> prev) throws Exception
	{
		NetworkReceiveOperator value = (NetworkReceiveOperator)unsafe.allocateInstance(NetworkReceiveOperator.class);
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
		value.cd = cs.newDecoder();
		value.received = new AtomicLong(0);
		value.demReceived = false;
		value.txnum = OperatorUtils.readLong(in);
		return value;
	}

	protected static byte[] intToBytes(int val)
	{
		final byte[] buff = new byte[4];
		buff[0] = (byte)(val >> 24);
		buff[1] = (byte)((val & 0x00FF0000) >> 16);
		buff[2] = (byte)((val & 0x0000FF00) >> 8);
		buff[3] = (byte)((val & 0x000000FF));
		return buff;
	}

	@Override
	public void add(Operator op) throws Exception
	{
		children.add(op);
		op.registerParent(this);
		cols2Types = op.getCols2Types();
		cols2Pos = op.getCols2Pos();
		pos2Col = op.getPos2Col();

		if (this instanceof NetworkHashReceiveOperator)
		{
			int id = ((NetworkHashReceiveOperator)this).getID();
			if (op instanceof NetworkHashAndSendOperator)
			{
				int id2 = ((NetworkHashAndSendOperator)op).getID();
				if (id2 != id)
				{
					Exception e = new Exception();
					HRDBMSWorker.logger.debug("IDs don't match!", e);
				}
			}
		}
	}

	@Override
	public ArrayList<Operator> children()
	{
		return children;
	}

	@Override
	public NetworkReceiveOperator clone()
	{
		final NetworkReceiveOperator retval = new NetworkReceiveOperator(meta);
		retval.node = node;
		retval.txnum = txnum;
		return retval;
	}

	@Override
	public void close() throws Exception
	{
		for (final OutputStream out : outs.values())
		{
			out.close();
		}

		outs = null;

		for (final InputStream in : ins.values())
		{
			in.close();
		}

		ins = null;

		for (final Socket sock : socks.values())
		{
			sock.close();
		}

		socks = null;

		if (outBuffer != null)
		{
			outBuffer.close();
		}

		outBuffer = null;
		cols2Types = null;
		cols2Pos = null;
		pos2Col = null;
		threads = null;
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
		return null;
	}

	@Override
	public Object next(Operator op2) throws Exception
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
	public void nextAll(Operator op) throws Exception
	{
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
			throw new Exception("NetworkReceiveOperator only supports 1 parent.");
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
		HRDBMSWorker.logger.error("NetworkReceiveOperator does not support reset()");
		throw new Exception("NetworkReceiveOperator does not support reset()");
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

		OperatorUtils.writeType(35, out);
		prev.put(this, OperatorUtils.writeID(out));
		// recreate meta
		OperatorUtils.serializeALOp(children, out, prev);
		parent.serialize(out, prev);
		OperatorUtils.serializeStringHM(cols2Types, out, prev);
		OperatorUtils.serializeStringIntHM(cols2Pos, out, prev);
		OperatorUtils.serializeTM(pos2Col, out, prev);
		OperatorUtils.writeBool(fullyStarted, out);
		OperatorUtils.writeLong(start, out);
		OperatorUtils.writeInt(node, out);
		OperatorUtils.writeLong(txnum, out);
	}

	@Override
	public void setChildPos(int pos)
	{
	}

	@Override
	public void setNode(int node)
	{
		if (this instanceof NetworkHashReceiveOperator && node < 0)
		{
			Exception e = new Exception();
			HRDBMSWorker.logger.debug("Coordinator node for NetworkHashReceiveOperator", e);
		}
		this.node = node;
	}

	@Override
	public void setPlan(Plan plan)
	{
	}

	public void setTXNum(long txnum)
	{
		this.txnum = txnum;
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
					outBuffer = new BufferedLinkedBlockingQueue(ResourceManager.QUEUE_SIZE);
					socks = new HashMap<Operator, Socket>();
					outs = new HashMap<Operator, OutputStream>();
					ins = new HashMap<Operator, InputStream>();
					threads = new ArrayList<ReadThread>();
					fullyStarted = true;
					for (final Operator op : children)
					{
						final NetworkSendOperator child = (NetworkSendOperator)op;
						child.clearParent();
						// final Socket sock = new
						// Socket(meta.getHostNameForNode(child.getNode()),
						// WORKER_PORT);
						Socket sock = new Socket();
						sock.setReceiveBufferSize(4194304);
						sock.setSendBufferSize(4194304);
						sock.connect(new InetSocketAddress(meta.getHostNameForNode(child.getNode()), WORKER_PORT));
						socks.put(child, sock);
						final OutputStream out = sock.getOutputStream();
						BufferedOutputStream out2 = new BufferedOutputStream(out);
						outs.put(child, out2);
						final InputStream in = sock.getInputStream();
						ins.put(child, new BufferedInputStream(in, 65536));
						final byte[] command = "REMOTTRE".getBytes(StandardCharsets.UTF_8);
						final byte[] from = intToBytes(-1);
						final byte[] to = intToBytes(child.getNode());
						final byte[] data = new byte[command.length + from.length + to.length];
						System.arraycopy(command, 0, data, 0, 8);
						System.arraycopy(from, 0, data, 8, 4);
						System.arraycopy(to, 0, data, 12, 4);
						out2.write(data);
						//out.flush();
						IdentityHashMap<Object, Long> map = new IdentityHashMap<Object, Long>();
						child.serialize(out2, map);
						map.clear();
						map = null;
						out2.flush();
					}

					new InitThread().start();
				}
			}
		}
	}

	public void start(boolean flag) throws Exception
	{
		outBuffer = new BufferedLinkedBlockingQueue(ResourceManager.QUEUE_SIZE);
		socks = new HashMap<Operator, Socket>();
		outs = new HashMap<Operator, OutputStream>();
		ins = new HashMap<Operator, InputStream>();
		threads = new ArrayList<ReadThread>();
	}

	@Override
	public String toString()
	{
		return "NetworkReceiveOperator(" + node + ")";
	}

	private int bytesToInt(byte[] val)
	{
		final int ret = java.nio.ByteBuffer.wrap(val).getInt();
		return ret;
	}

	private static class OverflowThread extends HRDBMSThread
	{
		private final FileChannel overFC;
		private final ByteBuffer buff;
		private boolean ok = true;
		private Exception e;

		public OverflowThread(FileChannel overFC, ByteBuffer buff)
		{
			this.overFC = overFC;
			this.buff = buff;
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
				overFC.write(buff);
			}
			catch (Exception e)
			{
				ok = false;
				this.e = e;
			}
		}
	}

	private final class ReadThread extends ThreadPoolThread
	{
		private Operator op;
		private Socket sock;
		private InputStream in;
		private FileChannel overFC;
		private final Random random = new Random();
		private String fn;
		private ByteBuffer buff;

		public ReadThread(Operator op)
		{
			this.op = op;
		}

		@Override
		public void run()
		{
			long start = System.currentTimeMillis();
			try
			{
				sock = socks.get(op);
				in = new CompressedInputStream(ins.get(op));
				op = null;
				final byte[] sizeBuff = new byte[4];
				byte[] data = null;

				while (true)
				{
					int count = 0;
					while (count < 4)
					{
						try
						{
							final int temp = in.read(sizeBuff, count, 4 - count);
							if (temp == -1)
							{
								HRDBMSWorker.logger.error("Early EOF reading from socket connected to " + sock.getRemoteSocketAddress());
								outBuffer.put(new Exception("Early EOF reading from socket connected to " + sock.getRemoteSocketAddress()));
								return;
							}
							else
							{
								count += temp;
							}
						}
						catch (final Throwable e)
						{
							try
							{
								HRDBMSWorker.logger.error("Early EOF reading from socket connected to " + sock.getRemoteSocketAddress(), e);
								outBuffer.put(new Exception("Early EOF reading from socket connected to " + sock.getRemoteSocketAddress()));
								return;
							}
							catch (Throwable f)
							{
								HRDBMSWorker.logger.error("Early EOF reading from socket", e);
								HRDBMSWorker.logger.error("", f);
								outBuffer.put(new Exception(e));
								return;
							}
						}
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
						try
						{
							count += in.read(data, count, size - count);
						}
						catch (final Exception e)
						{
							HRDBMSWorker.logger.error("Early EOF reading from socket connected to " + sock.getRemoteSocketAddress(), e);
							outBuffer.put(new Exception("Early EOF reading from socket connected to " + sock.getRemoteSocketAddress()));
							return;
						}
					}
					// bytes.getAndAdd(size);
					final Object row = fromBytes(data);

					if (row instanceof DataEndMarker)
					{
						break;
					}

					if (row instanceof Exception)
					{
						HRDBMSWorker.logger.debug("Exception received from " + sock.getRemoteSocketAddress(), (Exception)row);
						outBuffer.put(row);
						return;
					}

					boolean ok = outBuffer.putNow(row);

					if (!ok)
					{
						if (overFC == null)
						{
							int j = random.nextInt(ResourceManager.TEMP_DIRS.size());
							fn = ResourceManager.TEMP_DIRS.get(j) + this.hashCode() + "" + System.currentTimeMillis() + ".overflow";
							overFC = new RandomAccessFile(fn, "rw").getChannel();
							buff = ByteBuffer.allocate(9 * 1024 * 1024);
						}

						buff.put(sizeBuff);
						buff.put(data, 0, size);

						if (buff.position() >= 8 * 1024 * 1024)
						{
							int pos = buff.position();
							buff.position(0);
							buff.limit(pos);
							// overFC.write(buff);
							OverflowThread thread = new OverflowThread(overFC, buff);
							TempThread.start(thread, txnum);
							thread.join();
							if (!thread.getOK())
							{
								outBuffer.put(thread.getException());
								return;
							}
							buff.position(0);
							buff.limit(9 * 1024 * 1024);
						}
					}

					// readCounter.getAndIncrement();
				}

				if (overFC == null)
				{
					long end = System.currentTimeMillis();
					HRDBMSWorker.logger.debug("NRO ReadThread for " + sock.getRemoteSocketAddress() + " took " + ((end - start) / 1000) + "s");
					return;
				}

				if (buff.position() != 0)
				{
					int pos = buff.position();
					buff.position(0);
					buff.limit(pos);
					// overFC.write(buff);
					OverflowThread thread = new OverflowThread(overFC, buff);
					TempThread.start(thread, txnum);
					thread.join();
					if (!thread.getOK())
					{
						outBuffer.put(thread.getException());
						return;
					}
				}

				buff = null;
				BufferedFileChannel overFC2 = new BufferedFileChannel(overFC, 8 * 1024 * 1024);
				overFC2.position(0);
				while (true)
				{
					ByteBuffer bb = ByteBuffer.wrap(sizeBuff);
					int x = random.nextInt(ResourceManager.TEMP_DIRS.size());

					// synchronized(readThrottle[x])
					// {
					if (overFC2.read(bb, readThrottle[x]) == -1)
					{
						break;
					}

					final int size = bytesToInt(sizeBuff);

					if (data == null || data.length < size)
					{
						data = new byte[size];
					}

					bb = ByteBuffer.wrap(data);
					bb.limit(size);
					overFC2.read(bb, readThrottle[x]);
					// }

					final Object row = fromBytes(data);

					outBuffer.put(row);
				}

				overFC2.close();
				new File(fn).delete();
				long end = System.currentTimeMillis();
				HRDBMSWorker.logger.debug("NRO ReadThread for " + sock.getRemoteSocketAddress() + " took " + ((end - start) / 1000) + "s");
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

		private Object fromBytes(byte[] val) throws Exception
		{
			final ByteBuffer bb = ByteBuffer.wrap(val);
			final int numFields = bb.getInt();

			bb.position(bb.position() + numFields);
			final byte[] bytes = bb.array();
			if (bytes[4] == 5)
			{
				return new DataEndMarker();
			}
			if (bytes[4] == 10)
			{
				return fromBytesException(bb);
			}
			final ArrayList<Object> retval = new ArrayList<Object>(numFields);
			int i = 0;

			while (i < numFields)
			{
				Object o = null;
				if (bytes[i + 4] == 0)
				{
					// long
					o = bb.getLong();
				}
				else if (bytes[i + 4] == 1)
				{
					// integer
					o = bb.getInt();
				}
				else if (bytes[i + 4] == 2)
				{
					// double
					o = bb.getDouble();
				}
				else if (bytes[i + 4] == 3)
				{
					// date
					o = new MyDate(bb.getInt());
				}
				else if (bytes[i + 4] == 4)
				{
					// string
					final int length = bb.getInt();
					final byte[] temp = new byte[length];
					final char[] ca = new char[length];
					bb.get(temp);
					String value = (String)unsafe.allocateInstance(String.class);
					int clen = ((sun.nio.cs.ArrayDecoder)cd).decode(temp, 0, length, ca);
					if (clen == ca.length)
					{
						unsafe.putObject(value, offset, ca);
					}
					else
					{
						char[] v = Arrays.copyOf(ca, clen);
						unsafe.putObject(value, offset, v);
					}

					o = value;
				}
				else
				{
					throw new Exception("Unknown type " + bytes[i + 4] + " in fromBytes()");
				}

				retval.add(o);
				i++;
			}

			return retval;
		}

		private Object fromBytesException(ByteBuffer bb) throws Exception
		{
			final int length = bb.getInt();
			final byte[] temp = new byte[length];
			bb.get(temp);
			final String o = new String(temp, StandardCharsets.UTF_8);
			return new Exception(o);
		}
	}

	protected final class InitThread extends ThreadPoolThread
	{
		@Override
		public void run()
		{
			start = System.currentTimeMillis();
			readThrottle = new Object[ResourceManager.TEMP_DIRS.size()];
			int i = 0;
			while (i < readThrottle.length)
			{
				readThrottle[i++] = new Object();
			}

			for (final Operator op : children)
			{
				final ReadThread readThread = new ReadThread(op);
				threads.add(readThread);
				readThread.start();
			}

			if (node >= 0)
			{
				children = null;
			}

			for (final ReadThread thread : threads)
			{
				while (true)
				{
					try
					{
						thread.join();
						// HRDBMSWorker.logger.debug("NetworkReceiveOperator: "
						// + ((bytes.get() / ((System.currentTimeMillis() -
						// start) / 1000.0)) / (1024.0 * 1024.0)) + "MB/sec");
						break;
					}
					catch (final InterruptedException e)
					{
					}
				}
			}

			// System.out.println("NetworkReceiveOperator received " +
			// readCounter + " rows");

			while (true)
			{
				try
				{
					outBuffer.put(new DataEndMarker());
					HRDBMSWorker.logger.debug("Wrote DEM: " + NetworkReceiveOperator.this.toString());
					break;
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.debug("", e);
				}
			}
		}
	}
}
