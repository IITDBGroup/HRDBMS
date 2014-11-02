package com.exascale.optimizer;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import com.exascale.compression.CompressedSocket;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.managers.ResourceManager;
import com.exascale.misc.BufferedLinkedBlockingQueue;
import com.exascale.misc.DataEndMarker;
import com.exascale.misc.MyDate;
import com.exascale.tables.Plan;
import com.exascale.threads.ThreadPoolThread;

public class NetworkReceiveOperator implements Operator, Serializable
{
	protected MetaData meta;

	protected ArrayList<Operator> children = new ArrayList<Operator>();

	protected Operator parent;

	private HashMap<String, String> cols2Types;
	protected HashMap<String, Integer> cols2Pos;
	private TreeMap<Integer, String> pos2Col;
	protected static final int WORKER_PORT = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number"));
	protected HashMap<Operator, CompressedSocket> socks = new HashMap<Operator, CompressedSocket>();
	protected HashMap<Operator, OutputStream> outs = new HashMap<Operator, OutputStream>();
	protected HashMap<Operator, InputStream> ins = new HashMap<Operator, InputStream>();

	protected HashMap<Operator, ObjectOutputStream> objOuts = new HashMap<Operator, ObjectOutputStream>();

	protected BufferedLinkedBlockingQueue outBuffer;
	private final ArrayList<ReadThread> threads = new ArrayList<ReadThread>();
	protected volatile boolean fullyStarted = false;
	protected AtomicLong readCounter = new AtomicLong(0);
	protected AtomicLong bytes = new AtomicLong(0);
	protected long start;
	protected int node;
	private final HashSet<ObjectAndField> printed = new HashSet<ObjectAndField>();
	private transient Plan plan;
	
	public void setPlan(Plan plan)
	{
		this.plan = plan;
	}

	public NetworkReceiveOperator(MetaData meta)
	{
		this.meta = meta;
	}

	protected NetworkReceiveOperator()
	{
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
		return retval;
	}

	@Override
	public void close() throws Exception
	{
		for (final ObjectOutputStream objOut : objOuts.values())
		{
			objOut.close();
		}

		for (final OutputStream out : outs.values())
		{
			out.close();
		}

		for (final InputStream in : ins.values())
		{
			in.close();
		}

		for (final CompressedSocket sock : socks.values())
		{
			sock.close();
		}
		
		if (outBuffer != null)
		{
			outBuffer.close();
		}
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
		if (!fullyStarted)
		{
			synchronized (this)
			{
				if (!fullyStarted)
				{
					fullyStarted = true;
					for (final Operator op : children)
					{
						final NetworkSendOperator child = (NetworkSendOperator)op;
						child.clearParent();
						final CompressedSocket sock = new CompressedSocket(meta.getHostNameForNode(child.getNode()), WORKER_PORT);
						socks.put(child, sock);
						final OutputStream out = sock.getOutputStream();
						outs.put(child, out);
						final InputStream in = sock.getInputStream();
						ins.put(child, new BufferedInputStream(in, 65536));
						final byte[] command = "REMOTTRE".getBytes("UTF-8");
						final byte[] from = intToBytes(-1);
						final byte[] to = intToBytes(child.getNode());
						final byte[] data = new byte[command.length + from.length + to.length];
						System.arraycopy(command, 0, data, 0, 8);
						System.arraycopy(from, 0, data, 8, 4);
						System.arraycopy(to, 0, data, 12, 4);
						out.write(data);
						out.flush();
						final ObjectOutputStream objOut = new ObjectOutputStream(out);
						objOuts.put(child, objOut);
						// Field[] fields =
						// child.getClass().getDeclaredFields();
						// for (Field field : fields)
						// {
						// printHierarchy("child", field, child);
						// }
						objOut.writeObject(child);
						objOut.flush();
						out.flush();
					}

					new InitThread().start();
				}
			}
		}
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
		Object o = next(op);
		while (!(o instanceof DataEndMarker) && !(o instanceof Exception))
		{
			o = next(op);
		}
	}

	@Override
	public Operator parent()
	{
		return parent;
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
	public void setChildPos(int pos)
	{
	}

	@Override
	public void setNode(int node)
	{
		this.node = node;
	}

	@Override
	public void start() throws Exception
	{
		outBuffer = new BufferedLinkedBlockingQueue(ResourceManager.QUEUE_SIZE);
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

	private final class ObjectAndField
	{
		private final Object obj;
		private final Field field;

		public ObjectAndField(Object obj, Field field)
		{
			this.obj = obj;
			this.field = field;
		}

		@Override
		public boolean equals(Object rhs)
		{
			if (rhs != null && rhs instanceof ObjectAndField)
			{
				final ObjectAndField r = (ObjectAndField)rhs;
				return obj.equals(r.obj) && field.equals(r.field);
			}

			return false;
		}

		@Override
		public int hashCode()
		{
			return obj.hashCode() + field.hashCode();
		}
	}

	private final class ReadThread extends ThreadPoolThread
	{
		private final Operator op;

		public ReadThread(Operator op)
		{
			this.op = op;
		}

		@Override
		public void run()
		{
			try
			{
				final InputStream in = ins.get(op);
				final byte[] sizeBuff = new byte[4];
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
								HRDBMSWorker.logger.error("Early EOF reading from socket connected to " + socks.get(op).getRemoteSocketAddress());
								outBuffer.put(new Exception("Early EOF reading from socket connected to " + socks.get(op).getRemoteSocketAddress()));
								return;
							}
							else
							{
								count += temp;
							}
						}
						catch (final Exception e)
						{
							HRDBMSWorker.logger.error("Early EOF reading from socket connected to " + socks.get(op).getRemoteSocketAddress(), e);
							outBuffer.put(new Exception("Early EOF reading from socket connected to " + socks.get(op).getRemoteSocketAddress()));
							return;
						}
					}
					bytes.getAndAdd(4);
					final int size = bytesToInt(sizeBuff);

					final byte[] data = new byte[size];
					count = 0;
					while (count < size)
					{
						try
						{
							count += in.read(data, count, size - count);
						}
						catch (final Exception e)
						{
							HRDBMSWorker.logger.error("Early EOF reading from socket connected to " + socks.get(op).getRemoteSocketAddress(), e);
							outBuffer.put(new Exception("Early EOF reading from socket connected to " + socks.get(op).getRemoteSocketAddress()));
							return;
						}
					}
					bytes.getAndAdd(size);
					final Object row = fromBytes(data);

					if (row instanceof DataEndMarker)
					{
						return;
					}
					
					if (row instanceof Exception)
					{
						outBuffer.put((Exception)row);
						return;
					}

					outBuffer.put(row);
					readCounter.getAndIncrement();
				}
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				try
				{
					outBuffer.put(e);
				}
				catch(Exception f)
				{}
				return;
			}
		}

		private Object fromBytes(byte[] val) throws Exception
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
					final String o = new String(temp, "UTF-8");
					return new Exception(o);
				}
				catch (final Exception e)
				{
					throw e;
				}
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
					final MyDate o = new MyDate(bb.getLong());
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
						final String o = new String(temp, "UTF-8");
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
					HRDBMSWorker.logger.error("Unknown type " + bytes[i + 4] + " in fromBytes()");
					throw new Exception("Unknown type " + bytes[i + 4] + " in fromBytes()");
				}

				i++;
			}

			return retval;
		}
	}

	protected final class InitThread extends ThreadPoolThread
	{
		@Override
		public void run()
		{
			start = System.currentTimeMillis();
			for (final Operator op : children)
			{
				final ReadThread readThread = new ReadThread(op);
				threads.add(readThread);
				readThread.start();
			}

			for (final ReadThread thread : threads)
			{
				while (true)
				{
					try
					{
						thread.join();
						HRDBMSWorker.logger.debug("NetworkReceiveOperator: " + ((bytes.get() / ((System.currentTimeMillis() - start) / 1000.0)) / (1024.0 * 1024.0)) + "MB/sec");
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
					break;
				}
				catch (final Exception e)
				{
				}
			}
		}
	}
}
