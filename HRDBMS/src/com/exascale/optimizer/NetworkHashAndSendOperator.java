package com.exascale.optimizer;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Locale;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import com.exascale.compression.CompressedOutputStream;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.misc.DataEndMarker;
import com.exascale.misc.DirectConnectionRequest;
import com.exascale.misc.HrdbmsType;
import com.exascale.misc.MurmurHash;

public final class NetworkHashAndSendOperator extends NetworkSendOperator
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
	private ArrayList<String> hashCols;
	private int numNodes;
	private int id;
	private int starting;
	private ConcurrentHashMap<Integer, Socket> connections = new ConcurrentHashMap<Integer, Socket>(Phase3.MAX_INCOMING_CONNECTIONS, 1.0f, Phase3.MAX_INCOMING_CONNECTIONS);
	private transient OutputStream[] outs = null;
	private ArrayList<Operator> parents = new ArrayList<Operator>();
	private boolean error = false;

	private transient String errorText;

	private int connCount = 0;

	public NetworkHashAndSendOperator(final ArrayList<String> hashCols, final long numNodes, final int id, final int starting, final MetaData meta) throws Exception
	{
		this.hashCols = hashCols;
		if (numNodes > MetaData.numWorkerNodes)
		{
			this.numNodes = MetaData.numWorkerNodes;
		}
		else
		{
			this.numNodes = (int)numNodes;
		}
		this.id = id;
		this.starting = starting;
		this.meta = meta;
		received = new AtomicLong(0);
	}

	public static NetworkHashAndSendOperator deserialize(final InputStream in, final HashMap<Long, Object> prev) throws Exception
	{
		final NetworkHashAndSendOperator value = (NetworkHashAndSendOperator)unsafe.allocateInstance(NetworkHashAndSendOperator.class);
		prev.put(OperatorUtils.readLong(in), value);
		value.meta = new MetaData();
		value.child = OperatorUtils.deserializeOperator(in, prev);
		// prev = parent.serialize(out, prev);
		value.cols2Types = OperatorUtils.deserializeStringHM(in, prev);
		value.cols2Pos = OperatorUtils.deserializeStringIntHM(in, prev);
		value.pos2Col = OperatorUtils.deserializeTM(in, prev);
		value.node = OperatorUtils.readInt(in);
		value.numParents = OperatorUtils.readInt(in);
		value.started = OperatorUtils.readBool(in);
		value.numpSet = OperatorUtils.readBool(in);
		value.cardSet = OperatorUtils.readBool(in);
		value.hashCols = OperatorUtils.deserializeALS(in, prev);
		value.numNodes = OperatorUtils.readInt(in);
		value.id = OperatorUtils.readInt(in);
		value.starting = OperatorUtils.readInt(in);
		value.connections = new ConcurrentHashMap<Integer, Socket>(Phase3.MAX_INCOMING_CONNECTIONS, 1.0f, Phase3.MAX_INCOMING_CONNECTIONS);
		value.parents = OperatorUtils.deserializeALOp(in, prev);
		value.error = OperatorUtils.readBool(in);
		value.connCount = OperatorUtils.readInt(in);
		value.ce = NetworkSendOperator.cs.newEncoder();
		value.received = new AtomicLong(0);
		value.demReceived = false;
		return value;
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
	public synchronized void addConnection(final int fromNode, final Socket sock)
	{
		connections.put(fromNode, sock);
		connCount++;
		if (outs == null)
		{
			outs = new OutputStream[numParents];
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

	@Override
	public void clearParent()
	{
		parents.clear();
	}

	@Override
	public NetworkHashAndSendOperator clone()
	{
		try
		{
			final NetworkHashAndSendOperator retval = new NetworkHashAndSendOperator(hashCols, numNodes, id, starting, meta);
			retval.node = this.node;
			retval.numParents = numParents;
			retval.numpSet = numpSet;
			return retval;
		}
		catch (final Exception e)
		{
			return null;
		}
	}

	@Override
	public void close() throws Exception
	{
		for (final OutputStream out : outs)
		{
			try
			{
				out.close();
			}
			catch (final Exception e)
			{
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

		connections = null;
		hashCols = null;
		super.close();
	}

	public ArrayList<String> getHashCols()
	{
		return hashCols;
	}

	public int getID()
	{
		return id;
	}

	public int getNumNodes()
	{
		return numNodes;
	}

	public int getStarting()
	{
		return starting;
	}

	@Override
	public boolean hasAllConnections()
	{
		// HRDBMSWorker.logger.debug(this + " is expecting " + numParents +
		// " connections and has " + outs.size());
		return connCount == numParents;
	}

	@Override
	public long numRecsReceived()
	{
		return received.get();
	}

	@Override
	public Operator parent()
	{
		final Exception e = new Exception();
		HRDBMSWorker.logger.error("NetworkHashAndSendOperator does not support parent()", e);
		return null;
	}

	public ArrayList<Operator> parents()
	{
		return parents;
	}

	@Override
	public boolean receivedDEM()
	{
		return demReceived;
	}

	@Override
	public void registerParent(final Operator op)
	{
		parents.add(op);
	}

	@Override
	public void removeParent(final Operator op)
	{
		parents.remove(op);
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

		OperatorUtils.writeType(HrdbmsType.NHAS, out);
		prev.put(this, OperatorUtils.writeID(out));
		// recreate meta
		child.serialize(out, prev);
		// prev = parent.serialize(out, prev);
		OperatorUtils.serializeStringHM(cols2Types, out, prev);
		OperatorUtils.serializeStringIntHM(cols2Pos, out, prev);
		OperatorUtils.serializeTM(pos2Col, out, prev);
		OperatorUtils.writeInt(node, out);
		OperatorUtils.writeInt(numParents, out);
		OperatorUtils.writeBool(started, out);
		OperatorUtils.writeBool(numpSet, out);
		OperatorUtils.writeBool(cardSet, out);
		OperatorUtils.serializeALS(hashCols, out, prev);
		OperatorUtils.writeInt(numNodes, out);
		OperatorUtils.writeInt(this.id, out);
		OperatorUtils.writeInt(starting, out);
		// recreate connections
		OperatorUtils.serializeALOp(parents, out, prev);
		OperatorUtils.writeBool(error, out);
		OperatorUtils.writeInt(connCount, out);
	}

	@Override
	public void serialize(final OutputStream out, final IdentityHashMap<Object, Long> prev, final boolean flag) throws Exception
	{
		final Long id = prev.get(this);
		if (id != null)
		{
			OperatorUtils.serializeReference(id, out);
			return;
		}

		OperatorUtils.writeType(HrdbmsType.NHAS, out);
		prev.put(this, OperatorUtils.writeID(out));
		// recreate meta
		// child.serialize(out, prev);
		new DummyOperator(meta).serialize(out, prev);
		// prev = parent.serialize(out, prev);
		OperatorUtils.serializeStringHM(cols2Types, out, prev);
		OperatorUtils.serializeStringIntHM(cols2Pos, out, prev);
		OperatorUtils.serializeTM(pos2Col, out, prev);
		OperatorUtils.writeInt(node, out);
		OperatorUtils.writeInt(numParents, out);
		OperatorUtils.writeBool(started, out);
		OperatorUtils.writeBool(numpSet, out);
		OperatorUtils.writeBool(cardSet, out);
		OperatorUtils.serializeALS(hashCols, out, prev);
		OperatorUtils.writeInt(numNodes, out);
		OperatorUtils.writeInt(this.id, out);
		OperatorUtils.writeInt(starting, out);
		// recreate connections
		OperatorUtils.serializeALOp(parents, out, prev);
		OperatorUtils.writeBool(error, out);
		OperatorUtils.writeInt(connCount, out);
	}

	public void setID(final int id)
	{
		this.id = id;
	}

	public void setNumNodes(final int numNodes)
	{
		this.numNodes = numNodes;
	}

	@Override
	public boolean setNumParents()
	{
		if (numpSet)
		{
			return false;
		}

		numpSet = true;
		numParents = parents.size();
		return true;
	}

	public void setStarting(final int starting)
	{
		this.starting = starting;
	}

	@Override
	public synchronized void start() throws Exception
	{
		if (numNodes != numParents)
		{
			HRDBMSWorker.logger.debug("Wrong number of parents " + this);
			HRDBMSWorker.logger.debug("Expected " + numNodes + " during optimization");
			HRDBMSWorker.logger.debug("But found " + numParents + " at runtime");
			throw new Exception("NetworkHashAndSendOperator does not have the correct number of parents");
		}

		final OutputStream[] outs2 = new OutputStream[outs.length];
		int i = 0;
		for (final OutputStream out : outs)
		{
			outs2[i++] = new CompressedOutputStream(out);
		}
		try
		{
			if (error)
			{
				throw new Exception(errorText);
			}
			started = true;
			// child.start();
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
					obj = toBytes(o);

					if (o instanceof Exception)
					{
						HRDBMSWorker.logger.debug("", (Exception)o);
						for (final OutputStream out : outs2)
						{
							out.write(obj);
							out.flush();
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

					final int hash = (int)(starting + ((0x7FFFFFFFFFFFFFFFL & hash(key)) % numNodes));
					HRDBMSWorker.logger.debug("############ SENT " + o.toString() + " TO " + hash);

					final OutputStream out = outs2[hash];
					out.write(obj);

					out.write(toBytes(new DirectConnectionRequest((byte) 0, (byte) 0)));

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
			catch (final Exception e)
			{
				for (final OutputStream out : outs2)
				{
					out.write(toBytes(e));
					out.flush();
				}

				child.nextAll(this);
				child.close();
				return;
			}

			final byte[] obj = toBytes(new DataEndMarker());
			for (final OutputStream out : outs2)
			{
				out.write(obj);
				out.flush();
			}
			// HRDBMSWorker.logger.debug("Wrote " + count + " rows");
			try
			{
				child.close();
			}
			catch (final Exception e)
			{
			}
			// Thread.sleep(60 * 1000);
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.debug("", e);
			byte[] obj = null;
			try
			{
				obj = toBytes(e);
			}
			catch (final Exception f)
			{
				for (final OutputStream out : outs2)
				{
					try
					{
						out.close();
					}
					catch (final Exception g)
					{
					}
				}
			}
			for (final OutputStream out : outs2)
			{
				try
				{
					out.write(obj);
					out.flush();
					child.nextAll(this);
					child.close();
				}
				catch (final Exception f)
				{
					try
					{
						out.close();
						child.nextAll(this);
						child.close();
					}
					catch (final Exception g)
					{
					}
				}
			}
		}
	}

	@Override
	public String toString()
	{
		return "NetworkHashAndSendOperator(" + node + ") " + hashCols + " ID = " + id;
	}
}
