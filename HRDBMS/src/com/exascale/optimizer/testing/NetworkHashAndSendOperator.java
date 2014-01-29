package com.exascale.optimizer.testing;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.zip.GZIPOutputStream;

public class NetworkHashAndSendOperator extends NetworkSendOperator
{
	protected ArrayList<String> hashCols;
	protected int numNodes;
	protected int id;
	protected int starting;
	protected ConcurrentHashMap<Integer, CompressedSocket> connections = new ConcurrentHashMap<Integer, CompressedSocket>(Phase3.MAX_INCOMING_CONNECTIONS, 0.75f, Phase3.MAX_INCOMING_CONNECTIONS);
	protected ConcurrentHashMap<Integer, OutputStream> outs = new ConcurrentHashMap<Integer, OutputStream>(Phase3.MAX_INCOMING_CONNECTIONS, Phase3.MAX_INCOMING_CONNECTIONS);
	protected ArrayList<Operator> parents = new ArrayList<Operator>();
	protected static final Long LARGE_PRIME =  1125899906842597L;
    protected static final Long LARGE_PRIME2 = 6920451961L;
	
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
	
	public void close() throws Exception
	{
		for (OutputStream out : outs.values())
		{
			try
			{
				out.close();
			}
			catch(Exception e)
			{}
		}
		
		for (CompressedSocket sock : connections.values())
		{
			try
			{
				sock.close();
			}
			catch(Exception e)
			{}
		}
	}
	
	public void setStarting(int starting)
	{
		this.starting = starting;
	}
	
	public int getStarting()
	{
		return starting;
	}
	
	public void setNumNodes(int numNodes)
	{
		this.numNodes = numNodes;
	}
	
	public int getID()
	{
		return id;
	}
	
	public void setID(int id)
	{
		this.id = id;
	}
	
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
	
	public boolean hasAllConnections()
	{
		System.out.println(this + " is expecting " + numParents + " connections and has " + outs.size());
		return outs.size() == numParents;
	}
	
	public void addConnection(int fromNode, CompressedSocket sock)
	{
		connections.put(fromNode, sock);
		try
		{
			outs.put(fromNode, new BufferedOutputStream(sock.getOutputStream()));
		}
		catch(IOException e)
		{
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	public NetworkHashAndSendOperator clone()
	{
		NetworkHashAndSendOperator retval = new NetworkHashAndSendOperator(hashCols, numNodes, id, starting, meta);
		retval.node = this.node;
		retval.numParents = numParents;
		retval.numpSet = numpSet;
		return retval;
	}
	
	public Operator parent()
	{
		System.out.println("NetworkHashAndSendOperator does not support parent()");
		Thread.dumpStack();
		System.exit(1);
		return null;
	}
	
	public ArrayList<Operator> parents()
	{
		return parents;
	}
	
	public void removeParent(Operator op)
	{
		parents.remove(op);
	}
	
	public void clearParent()
	{
		parents.clear();
	}
	
	public String toString()
	{
		return "NetworkHashAndSendOperator(" + node + ")";
	}
	
	public void registerParent(Operator op)
	{
		parents.add(op);
	}
	
	public synchronized void start() throws Exception 
	{
		started = true;
		child.start();
		Object o = child.next(this);
		while (!(o instanceof DataEndMarker))
		{
			byte[] obj = toBytes(o);
			ArrayList<Object> key = new ArrayList<Object>(hashCols.size());
			for (String col : hashCols)
			{
				int pos = -1;
				try
				{
					pos = child.getCols2Pos().get(col);
				}
				catch(Exception e)
				{
					System.err.println("Looking looking up column position in NetworkHashAndSend");
					System.err.println("Looking for " + col + " in " + child.getCols2Pos());
					e.printStackTrace();
					System.exit(1);
				}
				key.add(((ArrayList<Object>)o).get(pos));
			}
			
			int hash = (int)(starting + ((0x0EFFFFFFFFFFFFFFL & hash(key)) % numNodes)); 
			try
			{
				OutputStream out = outs.get(hash);
				out.write(obj);
			}
			catch(NullPointerException e)
			{
				System.out.println("HashAndSend is looking for a connection to node " + hash + " in " + outs + ". Starting is " + starting);
				System.out.println("Outs = " + outs);
				System.out.println("Outs.get(hash) = " + outs.get(hash));
				System.out.println("Obj = " + obj);
				e.printStackTrace();
				System.exit(1);
			}
			count++;
			o = child.next(this);
		}
		
		byte[] obj = toBytes(o);
		for (OutputStream out : outs.values())
		{
			out.write(obj);
			out.flush();
		}
		System.out.println("Wrote " + count + " rows");
		child.close();
		Thread.sleep(60*1000);
	}
	
	protected long hash(ArrayList<Object> key)
	{
		long hashCode = 1125899906842597L;
		for (Object e : key)
		{
			long eHash = 1;
			if (e instanceof Integer)
			{
				long i = ((Integer)e).longValue();
				// Spread out values
			    long scaled = i * LARGE_PRIME;

			    // Fill in the lower bits
			    eHash = scaled + LARGE_PRIME2;
			}
			else if (e instanceof Long)
			{
				long i = (Long)e;
				// Spread out values
			    long scaled = i * LARGE_PRIME;

			    // Fill in the lower bits
			    eHash = scaled + LARGE_PRIME2;
			}
			else if (e instanceof String)
			{
				String string = (String)e;
				  long h = 1125899906842597L; // prime
				  int len = string.length();

				  for (int i = 0; i < len; i++) 
				  {
					   h = 31*h + string.charAt(i);
				  }
				  eHash = h;
			}
			else if (e instanceof Double)
			{
				long i = Double.doubleToLongBits((Double)e);
				// Spread out values
			    long scaled = i * LARGE_PRIME;

			    // Fill in the lower bits
			    eHash = scaled + LARGE_PRIME2;
			}
			else if (e instanceof Date)
			{
				long i = ((Date)e).getTime();
				// Spread out values
			    long scaled = i * LARGE_PRIME;

			    // Fill in the lower bits
			    eHash = scaled + LARGE_PRIME2;
			}
			else
			{
				eHash = e.hashCode();
			}
			
		    hashCode = 31*hashCode + (e==null ? 0 : eHash);
		}
		return hashCode;
	}
}
