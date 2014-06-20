package com.exascale.optimizer;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import com.exascale.compression.CompressedSocket;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.misc.DataEndMarker;

public final class NetworkSendMultipleOperator extends NetworkSendOperator
{
	private final int id;
	private final ConcurrentHashMap<Integer, CompressedSocket> connections = new ConcurrentHashMap<Integer, CompressedSocket>(Phase3.MAX_INCOMING_CONNECTIONS, 1.0f, Phase3.MAX_INCOMING_CONNECTIONS);
	private final ConcurrentHashMap<Integer, OutputStream> outs = new ConcurrentHashMap<Integer, OutputStream>(Phase3.MAX_INCOMING_CONNECTIONS, 1.0f, Phase3.MAX_INCOMING_CONNECTIONS);
	private final ArrayList<Operator> parents = new ArrayList<Operator>();
	private boolean error = false;
	private String errorText;

	public NetworkSendMultipleOperator(int id, MetaData meta)
	{
		this.id = id;
		this.meta = meta;
	}

	@Override
	public void addConnection(int fromNode, CompressedSocket sock)
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
			outs.putIfAbsent(fromNode,  new BufferedOutputStream(new ByteArrayOutputStream()));
		}
	}

	@Override
	public void clearParent()
	{
		parents.clear();
	}

	@Override
	public NetworkSendMultipleOperator clone()
	{
		final NetworkSendMultipleOperator retval = new NetworkSendMultipleOperator(id, meta);
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

		for (final CompressedSocket sock : connections.values())
		{
			try
			{
				sock.close();
			}
			catch (final Exception e)
			{
			}
		}
	}

	public int getID()
	{
		return id;
	}

	@Override
	public boolean hasAllConnections()
	{
		HRDBMSWorker.logger.debug(this + " is expecting " + numParents + " connections and has " + outs.size());
		return outs.size() == numParents;
	}

	@Override
	public Operator parent()
	{
		Exception e = new Exception();
		HRDBMSWorker.logger.error("NetworkSendMultipleOperator does not support parent()", e);
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
			child.start();
			Object o = child.next(this);
			while (!(o instanceof DataEndMarker))
			{
				if (o instanceof Exception)
				{
					throw (Exception)o;
				}
				final byte[] obj = toBytes(o);
				for (final OutputStream out : outs.values())
				{
					out.write(obj);
				}
				count++;
				o = child.next(this);
			}

			final byte[] obj = toBytes(o);
			for (final OutputStream out : outs.values())
			{
				out.write(obj);
				out.flush();
			}
			HRDBMSWorker.logger.debug("Wrote " + count + " rows");
			try
			{
				child.close();
			}
			catch(Exception e)
			{}
			Thread.sleep(60 * 1000);
		}
		catch(Exception e)
		{
			byte[] obj = null;
			try
			{
				obj = toBytes(e);
			}
			catch(Exception f)
			{
				for (final OutputStream out : outs.values())
				{
					try
					{
						out.close();
					}
					catch(Exception g)
					{}
				}
			}
			for (final OutputStream out : outs.values())
			{
				try
				{
					out.write(obj);
					out.flush();
				}
				catch(Exception f)
				{
					try
					{
						out.close();
					}
					catch(Exception g)
					{}
				}
			}
		}
	}

	@Override
	public String toString()
	{
		return "NetworkSendMultipleOperator(" + node + ")";
	}
}
