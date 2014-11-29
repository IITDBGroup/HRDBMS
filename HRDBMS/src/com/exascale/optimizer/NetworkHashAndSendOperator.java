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
import com.exascale.misc.MurmurHash;
import com.exascale.tables.Transaction;

public final class NetworkHashAndSendOperator extends NetworkSendOperator
{
	private ArrayList<String> hashCols;
	private int numNodes;
	private int id;
	private int starting;
	private ConcurrentHashMap<Integer, CompressedSocket> connections = new ConcurrentHashMap<Integer, CompressedSocket>(Phase3.MAX_INCOMING_CONNECTIONS, 1.0f, Phase3.MAX_INCOMING_CONNECTIONS);
	private ConcurrentHashMap<Integer, OutputStream> outs = new ConcurrentHashMap<Integer, OutputStream>(Phase3.MAX_INCOMING_CONNECTIONS, 1.0f, Phase3.MAX_INCOMING_CONNECTIONS);
	private final ArrayList<Operator> parents = new ArrayList<Operator>();
	private boolean error = false;
	private String errorText;

	public NetworkHashAndSendOperator(ArrayList<String> hashCols, long numNodes, int id, int starting, MetaData meta, Transaction tx) throws Exception
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
			outs.putIfAbsent(fromNode, new BufferedOutputStream(new ByteArrayOutputStream()));
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
			Transaction tx = new Transaction(Transaction.ISOLATION_RR);
			final NetworkHashAndSendOperator retval = new NetworkHashAndSendOperator(hashCols, numNodes, id, starting, meta, tx);
			tx.commit();
			retval.node = this.node;
			retval.numParents = numParents;
			retval.numpSet = numpSet;
			return retval;
		}
		catch(Exception e)
		{
			return null;
		}
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
		
		outs = null;

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
		
		connections = null;
		hashCols = null;
		super.close();
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
				final byte[] obj;
				try
				{
					obj = toBytes(o);
				}
				catch(Exception e)
				{
					HRDBMSWorker.logger.debug("Received an empty array list to send from " + child);
					throw e;
				}
				
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
						throw e;
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
					throw e;
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
			HRDBMSWorker.logger.debug("", e);
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
