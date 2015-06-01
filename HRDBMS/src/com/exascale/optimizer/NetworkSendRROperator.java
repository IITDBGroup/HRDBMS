package com.exascale.optimizer;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.concurrent.ConcurrentHashMap;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.misc.DataEndMarker;

public final class NetworkSendRROperator extends NetworkSendOperator
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
	private int id;
	private ConcurrentHashMap<Integer, Socket> connections = new ConcurrentHashMap<Integer, Socket>(Phase3.MAX_INCOMING_CONNECTIONS, 1.0f, Phase3.MAX_INCOMING_CONNECTIONS);
	private ConcurrentHashMap<Integer, OutputStream> outs = new ConcurrentHashMap<Integer, OutputStream>(Phase3.MAX_INCOMING_CONNECTIONS, 1.0f, Phase3.MAX_INCOMING_CONNECTIONS);
	private ArrayList<Operator> parents = new ArrayList<Operator>();

	private boolean error = false;

	private transient String errorText;

	public NetworkSendRROperator(int id, MetaData meta)
	{
		this.id = id;
		this.meta = meta;
	}

	public static NetworkSendRROperator deserialize(InputStream in, HashMap<Long, Object> prev) throws Exception
	{
		NetworkSendRROperator value = (NetworkSendRROperator)unsafe.allocateInstance(NetworkSendRROperator.class);
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
		value.id = OperatorUtils.readInt(in);
		value.connections = new ConcurrentHashMap<Integer, Socket>(Phase3.MAX_INCOMING_CONNECTIONS, 1.0f, Phase3.MAX_INCOMING_CONNECTIONS);
		value.outs = new ConcurrentHashMap<Integer, OutputStream>(Phase3.MAX_INCOMING_CONNECTIONS, 1.0f, Phase3.MAX_INCOMING_CONNECTIONS);
		value.parents = OperatorUtils.deserializeALOp(in, prev);
		value.error = OperatorUtils.readBool(in);
		value.ce = NetworkSendOperator.cs.newEncoder();
		return value;
	}

	@Override
	public void addConnection(int fromNode, Socket sock)
	{
		connections.put(fromNode, sock);
		try
		{
			outs.put(fromNode, new BufferedOutputStream(sock.getOutputStream()));
		}
		catch (final IOException e)
		{
			HRDBMSWorker.logger.error("", e);
			error = true;
			errorText = e.getMessage();
			outs.putIfAbsent(fromNode, new BufferedOutputStream(new ByteArrayOutputStream()));
		}
	}

	@Override
	public void clearParent()
	{
		parents.clear();
	}

	@Override
	public NetworkSendRROperator clone()
	{
		final NetworkSendRROperator retval = new NetworkSendRROperator(id, meta);
		retval.node = this.node;
		retval.numParents = numParents;
		retval.numpSet = numpSet;
		return retval;
	}

	@Override
	public void close() throws Exception
	{
		for (final OutputStream out : outs.values())
		{
			try
			{
				out.close();
			}
			catch (final Exception e)
			{
			}
		}

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
		outs = null;
		super.close();
	}

	public int getID()
	{
		return id;
	}

	@Override
	public boolean hasAllConnections()
	{
		// HRDBMSWorker.logger.debug(this + " is expecting " + numParents +
		// " connections and has " + outs.size());
		return outs.size() == numParents;
	}

	@Override
	public Operator parent()
	{
		Exception e = new Exception();
		HRDBMSWorker.logger.error("NetworkSendRROperator does not support parent()", e);
		return null;
	}

	public ArrayList<Operator> parents()
	{
		return parents;
	}

	@Override
	public void registerParent(Operator op)
	{
		parents.add(op);
	}

	@Override
	public void removeParent(Operator op)
	{
		parents.remove(op);
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

		OperatorUtils.writeType(77, out);
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
		OperatorUtils.writeInt(this.id, out);
		// recreate connections
		// recreate outs
		OperatorUtils.serializeALOp(parents, out, prev);
		OperatorUtils.writeBool(error, out);
	}

	public void setID(int id)
	{
		this.id = id;
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

	@Override
	public synchronized void start()
	{
		try
		{
			if (error)
			{
				throw new Exception(errorText);
			}
			started = true;
			int i = 0;
			final ArrayList<OutputStream> outs2 = new ArrayList<OutputStream>(outs.values());
			child.start();
			Object o = child.next(this);
			while (!(o instanceof DataEndMarker))
			{
				final byte[] obj = toBytes(o);
				outs2.get(i % outs2.size()).write(obj);
				if (o instanceof Exception)
				{
					HRDBMSWorker.logger.debug("", (Exception)o);
					throw (Exception)o;
				}
				// count++;
				i++;
				o = child.next(this);
			}

			final byte[] obj = toBytes(o);
			for (final OutputStream out : outs.values())
			{
				out.write(obj);
				out.flush();
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
				for (final OutputStream out : outs.values())
				{
					try
					{
						out.close();
					}
					catch (Exception g)
					{
					}
				}
			}
			for (final OutputStream out : outs.values())
			{
				try
				{
					out.write(obj);
					out.flush();
					child.nextAll(this);
					child.close();
				}
				catch (Exception f)
				{
					try
					{
						out.close();
						child.nextAll(this);
						child.close();
					}
					catch (Exception g)
					{
					}
				}
			}
		}
	}

	@Override
	public String toString()
	{
		return "NetworkSendRROperator(" + node + ")";
	}
}
