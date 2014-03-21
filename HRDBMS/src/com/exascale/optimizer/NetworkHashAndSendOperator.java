package com.exascale.optimizer;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import com.exascale.compression.CompressedSocket;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.misc.DataEndMarker;
import com.exascale.misc.MurmurHash;

public final class NetworkHashAndSendOperator extends NetworkSendOperator
{
	private final ArrayList<String> hashCols;
	private int numNodes;
	private int id;
	private int starting;
	private final ConcurrentHashMap<Integer, CompressedSocket> connections = new ConcurrentHashMap<Integer, CompressedSocket>(Phase3.MAX_INCOMING_CONNECTIONS, 1.0f, Phase3.MAX_INCOMING_CONNECTIONS);
	private final ConcurrentHashMap<Integer, OutputStream> outs = new ConcurrentHashMap<Integer, OutputStream>(Phase3.MAX_INCOMING_CONNECTIONS, 1.0f, Phase3.MAX_INCOMING_CONNECTIONS);
	private final ArrayList<Operator> parents = new ArrayList<Operator>();

	public NetworkHashAndSendOperator(ArrayList<String> hashCols, long numNodes, int id, int starting, MetaData meta)
	{
		this.hashCols = hashCols;
		if (numNodes > meta.getNumNodes())
		{
			this.numNodes = meta.getNumNodes();
		}
		else
		{
			this.numNodes = (int)numNodes;
		}
		this.id = id;
		this.starting = starting;
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
			System.exit(1);
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
		final NetworkHashAndSendOperator retval = new NetworkHashAndSendOperator(hashCols, numNodes, id, starting, meta);
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

	public int getStarting()
	{
		return starting;
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
		HRDBMSWorker.logger.error("NetworkHashAndSendOperator does not support parent()", e);
		System.exit(1);
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

	public void setID(int id)
	{
		this.id = id;
	}

	public void setNumNodes(int numNodes)
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

	public void setStarting(int starting)
	{
		this.starting = starting;
	}

	@Override
	public synchronized void start() throws Exception
	{
		started = true;
		child.start();
		Object o = child.next(this);
		while (!(o instanceof DataEndMarker))
		{
			final byte[] obj = toBytes(o);
			final ArrayList<Object> key = new ArrayList<Object>(hashCols.size());
			for (final String col : hashCols)
			{
				int pos = -1;
				try
				{
					pos = child.getCols2Pos().get(col);
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.error("Looking looking up column position in NetworkHashAndSend", e);
					HRDBMSWorker.logger.error("Looking for " + col + " in " + child.getCols2Pos());
					System.exit(1);
				}
				key.add(((ArrayList<Object>)o).get(pos));
			}

			final int hash = (int)(starting + ((0x0EFFFFFFFFFFFFFFL & hash(key)) % numNodes));
			try
			{
				final OutputStream out = outs.get(hash);
				out.write(obj);
			}
			catch (final NullPointerException e)
			{
				HRDBMSWorker.logger.error("HashAndSend is looking for a connection to node " + hash + " in " + outs + ". Starting is " + starting, e);
				HRDBMSWorker.logger.error("Outs = " + outs);
				HRDBMSWorker.logger.error("Outs.get(hash) = " + outs.get(hash));
				HRDBMSWorker.logger.error("Obj = " + obj);
				System.exit(1);
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
		child.close();
		Thread.sleep(60 * 1000);
	}

	@Override
	public String toString()
	{
		return "NetworkHashAndSendOperator(" + node + ")";
	}

	private long hash(Object key)
	{
		long eHash;
		if (key == null)
		{
			eHash = 0;
		}
		else
		{
			eHash = MurmurHash.hash64(key.toString());
		}

		return eHash;
	}
}
