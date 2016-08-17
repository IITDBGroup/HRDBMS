package com.exascale.optimizer;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.io.StringWriter;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Locale;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import com.exascale.compression.CompressedInputStream;
import com.exascale.compression.CompressedOutputStream;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.managers.ResourceManager;
import com.exascale.misc.BufferedFileChannel;
import com.exascale.misc.BufferedLinkedBlockingQueue;
import com.exascale.misc.DataEndMarker;
import com.exascale.misc.MurmurHash;
import com.exascale.misc.MyDate;
import com.exascale.tables.Plan;
import com.exascale.threads.TempThread;
import com.exascale.threads.ThreadPoolThread;

public final class RoutingOperator implements Operator
{
	private static Charset cs = StandardCharsets.UTF_8;
	private static sun.misc.Unsafe unsafe;
	private static long offset;
	private static final int WORKER_PORT = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number"));

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
	private CharsetEncoder ce = cs.newEncoder();
	private ArrayList<String> hashCols;
	private int fromID;
	private int toID;
	private ConcurrentHashMap<Integer, Socket> connections = new ConcurrentHashMap<Integer, Socket>(Phase3.MAX_INCOMING_CONNECTIONS, 1.0f, Phase3.MAX_INCOMING_CONNECTIONS);
	private transient OutputStream[] outs = null;
	private ArrayList<Operator> parents = new ArrayList<Operator>();
	private boolean error = false;
	private transient String errorText;
	private int connCount = 0;
	private MetaData meta;
	private AtomicLong received;
	private int numParents = -1;
	private boolean demReceived = false;
	private int node;
	private boolean first = false;
	private boolean last = false;
	private transient Operator child;
	private ArrayList<Operator> children;
	private int starting;
	private int numNodes;
	private HashMap<String, String> cols2Types;
	private HashMap<String, Integer> cols2Pos;
	private TreeMap<Integer, String> pos2Col;
	private volatile boolean fullyStarted = false;
	private transient HashMap<Operator, Socket> socks;
	private transient HashMap<Operator, OutputStream> outsMiddle;
	private transient HashMap<Operator, InputStream> ins;
	private transient ArrayList<ReadThread> threads;
	private boolean send = false;
	private transient OutputStream[] outs2;
	private transient ConcurrentHashMap<Integer, AtomicLong> usage;

	public RoutingOperator(MetaData meta) throws Exception
	{
		this.meta = meta;
		received = new AtomicLong(0);
	}
	
	public void setStarting(int starting)
	{
		this.starting = starting;
	}
	
	public void setNumNodes(int numNodes)
	{
		this.numNodes = numNodes;
	}
	
	public void setFirst(ArrayList<String> hashCols)
	{
		first = true;
		this.hashCols = hashCols;
	}
	
	public void setLast()
	{
		last = true;
	}
	
	public void setSend()
	{
		send = true;
	}

	public static RoutingOperator deserialize(InputStream in, HashMap<Long, Object> prev) throws Exception
	{
		RoutingOperator value = (RoutingOperator)unsafe.allocateInstance(RoutingOperator.class);
		prev.put(OperatorUtils.readLong(in), value);
		value.ce = RoutingOperator.cs.newEncoder();
		value.hashCols = OperatorUtils.deserializeALS(in, prev);
		value.fromID = OperatorUtils.readInt(in);
		value.toID = OperatorUtils.readInt(in);
		value.connections = new ConcurrentHashMap<Integer, Socket>(Phase3.MAX_INCOMING_CONNECTIONS, 1.0f, Phase3.MAX_INCOMING_CONNECTIONS);
		value.parents = OperatorUtils.deserializeALOp(in, prev);
		value.error = OperatorUtils.readBool(in);
		value.connCount = OperatorUtils.readInt(in);
		value.meta = new MetaData();
		value.received = new AtomicLong(0);
		value.numParents = OperatorUtils.readInt(in);
		value.demReceived = false;
		value.node = OperatorUtils.readInt(in);
		value.first = OperatorUtils.readBool(in);
		value.last = OperatorUtils.readBool(in);
		value.children = OperatorUtils.deserializeALOp(in, prev);
		value.starting = OperatorUtils.readInt(in);
		value.numNodes = OperatorUtils.readInt(in);
		value.cols2Types = OperatorUtils.deserializeStringHM(in, prev);
		value.cols2Pos = OperatorUtils.deserializeStringIntHM(in, prev);
		value.pos2Col = OperatorUtils.deserializeTM(in, prev);
		value.fullyStarted = OperatorUtils.readBool(in);
		value.send = OperatorUtils.readBool(in);;
		return value;
	}

	public synchronized void addConnection(int fromNode, Socket sock)
	{
		connections.put(fromNode, sock);
		connCount++;
		if (outs == null)
		{
			outs = new OutputStream[MetaData.numWorkerNodes];
		}
		try
		{
			outs[fromNode] = new BufferedOutputStream(sock.getOutputStream());
		}
		catch (final IOException e)
		{
			HRDBMSWorker.logger.error("", e);
			error = true;
			errorText = e.getMessage();
			// outs.putIfAbsent(fromNode, new BufferedOutputStream(new
			// ByteArrayOutputStream()));
		}
	}

	public void clearParent()
	{
		parents.clear();
	}

	@Override
	public RoutingOperator clone()
	{
		try
		{
			final RoutingOperator retval = new RoutingOperator(meta);
			retval.node = this.node;
			retval.numParents = numParents;
			retval.hashCols = hashCols;
			retval.fromID = fromID;
			retval.toID = toID;
			retval.first = first;
			retval.last = last;
			retval.starting = starting;
			retval.numNodes = numNodes;
			retval.send = send;
			return retval;
		}
		catch (Exception e)
		{
			return null;
		}
	}

	public void close() throws Exception
	{
		for (final OutputStream out : outs)
		{
			if (out != null)
			{
				try
				{
					out.close();
				}
				catch (final Exception e)
				{
				}
			}
		}

		outs = null;

		for (final Socket sock : connections.values())
		{
			try
			{
				sock.close();
			}
			catch (final Exception e)
			{
			}
		}
		
		for (final OutputStream out : outsMiddle.values())
		{
			out.close();
		}

		outsMiddle = null;

		for (final InputStream in : ins.values())
		{
			in.close();
		}

		ins = null;
		socks = null;
		cols2Types = null;
		cols2Pos = null;
		pos2Col = null;
		threads = null;
		connections = null;
		hashCols = null;
	}

	public ArrayList<String> getHashCols()
	{
		return hashCols;
	}

	public int getFromID()
	{
		return fromID;
	}
	
	public int getToID()
	{
		return toID;
	}

	public boolean hasAllConnections()
	{
		// HRDBMSWorker.logger.debug(this + " is expecting " + numParents +
		// " connections and has " + outs.size());
		return connCount == numParents;
	}

	public long numRecsReceived()
	{
		return received.get();
	}

	public Operator parent()
	{
		Exception e = new Exception();
		HRDBMSWorker.logger.error("RoutingOperator does not support parent()", e);
		return null;
	}

	public ArrayList<Operator> parents()
	{
		return parents;
	}

	public boolean receivedDEM()
	{
		return demReceived;
	}

	public void registerParent(Operator op)
	{
		parents.add(op);
	}

	public void removeParent(Operator op)
	{
		parents.remove(op);
	}

	public void serialize(OutputStream out, IdentityHashMap<Object, Long> prev) throws Exception
	{
		Long id = prev.get(this);
		if (id != null)
		{
			OperatorUtils.serializeReference(id, out);
			return;
		}

		OperatorUtils.writeType(85, out); 
		prev.put(this, OperatorUtils.writeID(out));
		OperatorUtils.serializeALS(hashCols, out, prev);
		OperatorUtils.writeInt(this.fromID, out);
		OperatorUtils.writeInt(this.toID, out);
		OperatorUtils.serializeALOp(parents, out, prev);
		OperatorUtils.writeBool(error, out);
		OperatorUtils.writeInt(connCount, out);
		OperatorUtils.writeInt(numParents, out);
		OperatorUtils.writeInt(node, out);
		OperatorUtils.writeBool(first, out);
		OperatorUtils.writeBool(last, out);
		OperatorUtils.serializeALOp(children, out, prev);
		OperatorUtils.writeInt(starting, out);
		OperatorUtils.writeInt(numNodes, out);
		OperatorUtils.serializeStringHM(cols2Types, out, prev);
		OperatorUtils.serializeStringIntHM(cols2Pos, out, prev);
		OperatorUtils.serializeTM(pos2Col, out, prev);
		OperatorUtils.writeBool(fullyStarted, out);
		OperatorUtils.writeBool(send, out);
	}

	public void setFromID(int id)
	{
		this.fromID = id;
	}
	
	public void setToID(int id)
	{
		this.toID = id;
	}

	public boolean setNumParents()
	{
		if (numParents == -1)
		{
			numParents = parents.size();
			return true;
		}
		
		return false;
	}
	
	public boolean notStarted()
	{
		return !fullyStarted;
	}

	public synchronized void start() throws Exception
	{
		if (fullyStarted)
		{
			return;
		}
		
		outs2 = new OutputStream[outs.length];
		int i = 0;
		for (OutputStream out : outs)
		{
			if (out != null)
			{
				outs2[i] = new CompressedOutputStream(out);
			}
			i++;
		}
		
		usage = new ConcurrentHashMap<Integer, AtomicLong>();
		try
		{
			if (error)
			{
				throw new Exception(errorText);
			}
			
			// child.start();
			if (first)
			{
				fullyStarted = true;
				child = children.get(0);
				try
				{
					Object o = child.next(this);
					if (o instanceof DataEndMarker)
					{
						demReceived = true;
					}
					else
					{
						received.getAndIncrement();
					}
					final ArrayList<Object> key = new ArrayList<Object>(hashCols.size());
					while (!(o instanceof DataEndMarker))
					{
						final byte[] obj;

						if (o instanceof Exception)
						{
							HRDBMSWorker.logger.debug("", (Exception)o);
							for (final OutputStream out : outs2)
							{
								if (out != null)
								{
									out.write(toBytes(o));
									out.flush();
								}
							}

							child.nextAll(this);
							child.close();
							return;
						}

						key.clear();
						int z = 0;
						final int limit = hashCols.size();
						// for (final String col : hashCols)
						while (z < limit)
						{
							final String col = hashCols.get(z++);
							int pos = -1;
							pos = child.getCols2Pos().get(col);
							key.add(((ArrayList<Object>)o).get(pos));
						}

						final int finalDest = (int)(starting + ((0x7FFFFFFFFFFFFFFFL & hash(key)) % numNodes));
						ArrayList<Integer> route = ResourceManager.getRoute(node, finalDest);
						int nextHop = route.get(0);
						if (route.size() >= 2)
						{
							int twoHops = route.get(1);
							ArrayList<Integer> alternatives = ResourceManager.getAlternateMiddlemen(node, twoHops, nextHop);
							AtomicLong primaryUsage = usage.get(nextHop);
							if (primaryUsage == null)
							{
								primaryUsage = new AtomicLong(0);
								usage.put(nextHop, primaryUsage);
							}
							
							long pu = primaryUsage.get();
							long bestAltUsage = Long.MAX_VALUE;
							int bestAlt = -1;
							for (int alt : alternatives)
							{
								AtomicLong altUsage = usage.get(alt);
								if (altUsage == null)
								{
									altUsage = new AtomicLong(0);
									usage.put(alt, altUsage);
								}
								
								long au = altUsage.get();
								if (au < bestAltUsage)
								{
									bestAltUsage = au;
									bestAlt = alt;
								}
							}
							
							if (bestAltUsage < pu)
							{
								nextHop = bestAlt;
								usage.get(bestAlt).incrementAndGet();
							}
							else
							{
								primaryUsage.incrementAndGet();
							}
						}
						final OutputStream out = outs2[nextHop];
						obj = toBytes((ArrayList<Object>)o, route);
						out.write(obj);

						o = child.next(this);
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
				catch (Exception e)
				{
					for (final OutputStream out : outs2)
					{
						if (out != null)
						{
							out.write(toBytes(e));
							out.flush();
						}
					}

					child.nextAll(this);
					child.close();
					return;
				}
			}
			else
			{
				//receive a row - last 4 bytes is where to send to next
				//take last 4 bytes off and decrement size by 4
				//send it
				if (!fullyStarted)
				{
					socks = new HashMap<Operator, Socket>();
					outsMiddle = new HashMap<Operator, OutputStream>();
					ins = new HashMap<Operator, InputStream>();
					threads = new ArrayList<ReadThread>();
					//HRDBMSWorker.logger.debug("Starting NetworkHashReceiveOperator(" + node + ") ID = " + ID);
					fullyStarted = true;
					for (final Operator op : children)
					{
						final RoutingOperator child = (RoutingOperator)op;
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
							sock.connect(new InetSocketAddress(meta.getHostNameForNode(child.getNode()), WORKER_PORT));
						}
						catch (final java.net.ConnectException e)
						{
							HRDBMSWorker.logger.error("Connection failed to " + meta.getHostNameForNode(child.getNode()), e);
							throw e;
						}
						socks.put(child, sock);
						final OutputStream out = sock.getOutputStream();
						BufferedOutputStream out2 = new BufferedOutputStream(out);
						outsMiddle.put(child, out2);
						final InputStream in = new BufferedInputStream(sock.getInputStream(), 65536);
						ins.put(child, in);

						if (send)
						{
							final byte[] command = "ROUTING ".getBytes(StandardCharsets.UTF_8);
							final byte[] from = intToBytes(node);
							final byte[] to = intToBytes(child.getNode());
							final byte[] idBytes = intToBytes(toID);
							final byte[] data = new byte[command.length + from.length + to.length + idBytes.length];
							System.arraycopy(command, 0, data, 0, 8);
							System.arraycopy(from, 0, data, 8, 4);
							System.arraycopy(to, 0, data, 12, 4);
							System.arraycopy(idBytes, 0, data, 16, 4);
							out2.write(data);
							//out.flush();

							IdentityHashMap<Object, Long> map = new IdentityHashMap<Object, Long>();
							child.serialize(out2, map);
							map.clear();
							map = null;
							out2.flush();
						}
						else
						{
							final byte[] command = "ROUTING2".getBytes(StandardCharsets.UTF_8);
							final byte[] from = intToBytes(node);
							final byte[] to = intToBytes(child.getNode());
							final byte[] idBytes = intToBytes(toID);
							final byte[] data = new byte[command.length + from.length + to.length + idBytes.length];
							System.arraycopy(command, 0, data, 0, 8);
							System.arraycopy(from, 0, data, 8, 4);
							System.arraycopy(to, 0, data, 12, 4);
							System.arraycopy(idBytes, 0, data, 16, 4);
							out2.write(data);
							out2.flush();
						}
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
								break;
							}
							catch (final InterruptedException e)
							{
							}
						}
					}
				}
			}

			final byte[] obj = toBytes(new DataEndMarker());
			for (final OutputStream out : outs2)
			{
				if (out != null)
				{
					out.write(obj);
					out.flush();
				}
			}
			// HRDBMSWorker.logger.debug("Wrote " + count + " rows");
			try
			{
				child.close();
			}
			catch (Exception e)
			{
			}
			// Thread.sleep(60 * 1000);
		}
		catch (Exception e)
		{
			HRDBMSWorker.logger.debug("", e);
			byte[] obj = null;
			try
			{
				obj = toBytes(e);
			}
			catch (Exception f)
			{
				for (final OutputStream out : outs2)
				{
					try
					{
						if (out != null)
						{
							out.close();
						}
					}
					catch (Exception g)
					{
					}
				}
				
				for (Operator o : children)
				{
					o.nextAll(this);
				}
			}
			for (final OutputStream out : outs2)
			{
				try
				{
					if (out != null)
					{
						out.write(obj);
						out.flush();
					}
				}
				catch (Exception f)
				{
					try
					{
						if (out != null)
						{
							out.close();
						}
					}
					catch (Exception g)
					{
					}
				}
			}
			
			for (Operator o : children)
			{
				o.close();
			}
		}
	}
	
	private static byte[] intToBytes(int val)
	{
		final byte[] buff = new byte[4];
		buff[0] = (byte)(val >> 24);
		buff[1] = (byte)((val & 0x00FF0000) >> 16);
		buff[2] = (byte)((val & 0x0000FF00) >> 8);
		buff[3] = (byte)((val & 0x000000FF));
		return buff;
	}

	@Override
	public String toString()
	{
		return "RoutingOperator(" + node + ") FROMID = " + fromID + " TOID = " + toID;
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
				byte[] data = toBytesForHash((ArrayList<Object>)key);
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
	
	private static byte[] toBytesForHash(ArrayList<Object> key)
	{
		StringBuilder sb = new StringBuilder();
		for (Object o : key)
		{
			if (o instanceof Double)
			{
				DecimalFormat df = new DecimalFormat("0", DecimalFormatSymbols.getInstance(Locale.ENGLISH));
				df.setMaximumFractionDigits(340); //340 = DecimalFormat.DOUBLE_FRACTION_DIGITS

				sb.append(df.format((Double)o));
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
		byte[] retval = new byte[z];
		int i = 0;
		while (i < z)
		{
			retval[i] = (byte)sb.charAt(i);
			i++;
		}
		
		return retval;
	}

	@Override
	public void add(Operator op) throws Exception
	{
		op.registerParent(this);
		
		if (cols2Types == null)
		{
			cols2Types = op.getCols2Types();
			cols2Pos = op.getCols2Pos();
			pos2Col = op.getPos2Col();
		}
	}

	@Override
	public ArrayList<Operator> children()
	{
		return children;
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
	public Object next(Operator op) throws Exception
	{
		throw new Exception("Unsupported operation");
	}

	@Override
	public void nextAll(Operator op) throws Exception
	{
	}

	@Override
	public void removeChild(Operator op)
	{
		child = null;
		op.removeParent(this);
	}

	@Override
	public void reset() throws Exception
	{
		HRDBMSWorker.logger.error("RoutingOperator does not support reset()");
		throw new Exception("RoutingOperator does not support reset()");
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
	public void setPlan(Plan p)
	{
	}
	
	private byte[] toBytes(ArrayList<Object> val, ArrayList<Integer> route) throws Exception
	{
		ArrayList<byte[]> bytes = null;
		if (val.size() == 0)
		{
			throw new Exception("Empty ArrayList in toBytes()");
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
				// byte[] b = ((String)o).getBytes("UTF-8");
				byte[] ba = new byte[((String)o).length() << 2];
				char[] value = (char[])unsafe.getObject(o, offset);
				int blen = ((sun.nio.cs.ArrayEncoder)ce).encode(value, 0, value.length, ba);
				byte[] b = Arrays.copyOf(ba, blen);
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
			else
			{
				throw new Exception("Unknown type " + o.getClass() + " in toBytes()");
			}

			i++;
		}

		while (route.size() < ResourceManager.MAX_HOPS)
		{
			route.add(route.get(route.size() - 1));
		}
		
		size += ((route.size() - 1) * 4);
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

			i++;
		}
		
		i = route.size() - 1;
		while (i > 0)
		{
			retvalBB.putInt(route.get(i));
			i--;
		}

		return retval;
	}
	
	private byte[] toBytes(Object v) throws Exception
	{
		ArrayList<byte[]> bytes = null;
		ArrayList<Object> val = null;
		if (v instanceof ArrayList)
		{
			val = (ArrayList<Object>)v;
			if (val.size() == 0)
			{
				throw new Exception("Empty ArrayList in toBytes()");
			}
		}
		else if (v instanceof Exception)
		{
			return toBytesException(v);
		}
		else
		{
			return toBytesDEM();
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
				// byte[] b = ((String)o).getBytes("UTF-8");
				byte[] ba = new byte[((String)o).length() << 2];
				char[] value = (char[])unsafe.getObject(o, offset);
				int blen = ((sun.nio.cs.ArrayEncoder)ce).encode(value, 0, value.length, ba);
				byte[] b = Arrays.copyOf(ba, blen);
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
			else
			{
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

			i++;
		}

		return retval;
	}
	
	private byte[] toBytesDEM()
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

	private byte[] toBytesException(Object v)
	{
		Exception e = (Exception)v;
		byte[] data = null;
		try
		{
			HRDBMSWorker.logger.debug("", (Exception)v);
			if (e.getMessage() == null)
			{
				StringWriter sw = new StringWriter();
				PrintWriter pw = new PrintWriter(sw);
				e.printStackTrace(pw);
				data = sw.toString().getBytes(StandardCharsets.UTF_8);
			}
			else
			{
				data = e.getMessage().getBytes(StandardCharsets.UTF_8);
			}
		}
		catch (Exception f)
		{
		}

		int dataLen = data.length;
		int recLen = 9 + dataLen;
		ByteBuffer bb = ByteBuffer.allocate(recLen + 4);
		bb.position(0);
		bb.putInt(recLen);
		bb.putInt(1);
		bb.put((byte)10);
		bb.putInt(dataLen);
		bb.put(data);
		return bb.array();
	}
	
	private final class ReadThread extends ThreadPoolThread
	{
		private Operator op;
		private Socket sock;
		private InputStream in;

		public ReadThread(Operator op)
		{
			this.op = op;
		}

		@Override
		public void run()
		{
			//long start = System.currentTimeMillis();
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
								routeAll(new Exception("Early EOF reading from socket connected to " + sock.getRemoteSocketAddress()));
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
								routeAll(new Exception("Early EOF reading from socket connected to " + sock.getRemoteSocketAddress()));
								return;
							}
							catch (Throwable f)
							{
								HRDBMSWorker.logger.error("Early EOF reading from socket", e);
								HRDBMSWorker.logger.error("", f);
								routeAll(new Exception(e));
								return;
							}
						}
					}
					// bytes.getAndAdd(4);
					int size = bytesToInt(sizeBuff);

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
							routeAll(new Exception("Early EOF reading from socket connected to " + sock.getRemoteSocketAddress()));
							return;
						}
					}
					// bytes.getAndAdd(size);
					
					if (data[4] == 5) //DEM
					{
						break;
					}

					if (data[4] == 10) //Exception
					{
						routeAll(fromBytesException(ByteBuffer.wrap(data)));
						return;
					}
					
					received.getAndIncrement();
					ByteBuffer bb = ByteBuffer.wrap(data);
					int nextHop = bb.getInt(size-4);
					size -= 4;
					
					if (!last)
					{
						int twoHops = bb.getInt(size-4);
						ArrayList<Integer> alternatives = ResourceManager.getAlternateMiddlemen(node, twoHops, nextHop);
						AtomicLong primaryUsage = usage.get(nextHop);
						if (primaryUsage == null)
						{
							primaryUsage = new AtomicLong(0);
							usage.put(nextHop, primaryUsage);
						}
						
						long pu = primaryUsage.get();
						long bestAltUsage = Long.MAX_VALUE;
						int bestAlt = -1;
						for (int alt : alternatives)
						{
							AtomicLong altUsage = usage.get(alt);
							if (altUsage == null)
							{
								altUsage = new AtomicLong(0);
								usage.put(alt, altUsage);
							}
							
							long au = altUsage.get();
							if (au < bestAltUsage)
							{
								bestAltUsage = au;
								bestAlt = alt;
							}
						}
						
						if (bestAltUsage < pu)
						{
							nextHop = bestAlt;
							usage.get(bestAlt).incrementAndGet();
						}
						else
						{
							primaryUsage.incrementAndGet();
						}
					}

					route(data, size, nextHop);
				}
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				try
				{
					routeAll(e);
				}
				catch (Exception f)
				{
				}
				return;
			}
		}
		
		private void route(byte[] data, int size, int nextHop) throws Exception
		{
			final OutputStream out = outs2[nextHop];
			synchronized(out)
			{
				out.write(intToBytes(size));
				out.write(data, 0, size);
			}
		}
		
		private void routeAll(Exception e) throws Exception
		{
			byte[] obj = toBytes(e);
			for (OutputStream out : outs2)
			{
				if (out != null)
				{
					synchronized(out)
					{
						out.write(obj);
						out.flush();
					}
				}
			}
		}
		
		private int bytesToInt(byte[] val)
		{
			final int ret = java.nio.ByteBuffer.wrap(val).getInt();
			return ret;
		}

		private Exception fromBytesException(ByteBuffer bb) throws Exception
		{
			int numFields = bb.getInt();
			bb.position(bb.position() + numFields);
			final int length = bb.getInt();
			final byte[] temp = new byte[length];
			bb.get(temp);
			final String o = new String(temp, StandardCharsets.UTF_8);
			return new Exception(o);
		}
	}
	
	public void startChildren() throws Exception
	{
		children.get(0).start();
	}
}
