package com.exascale.optimizer;

import java.io.BufferedOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeMap;
import com.exascale.compression.CompressedSocket;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.misc.DataEndMarker;
import com.exascale.misc.MyDate;
import com.exascale.tables.Plan;

public class NetworkSendOperator implements Operator, Serializable
{
	protected MetaData meta;

	protected Operator child;
	private Operator parent;
	private HashMap<String, String> cols2Types;
	private HashMap<String, Integer> cols2Pos;
	private TreeMap<Integer, String> pos2Col;
	private static final int WORKER_PORT = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number"));
	private CompressedSocket sock;
	private OutputStream out;
	protected int node;
	protected long count = 0;
	protected int numParents;
	protected boolean started = false;
	protected boolean numpSet = false;
	private boolean cardSet = false;
	private transient Plan plan;
	
	public void setPlan(Plan plan)
	{
		this.plan = plan;
	}

	public NetworkSendOperator(int node, MetaData meta)
	{
		this.meta = meta;
		this.node = node;
	}

	protected NetworkSendOperator()
	{
	}

	@Override
	public void add(Operator op) throws Exception
	{
		if (child == null)
		{
			child = op;
			op.registerParent(this);
			cols2Types = op.getCols2Types();
			cols2Pos = op.getCols2Pos();
			pos2Col = op.getPos2Col();
		}
		else
		{
			throw new Exception("NetworkSendOperator only supports 1 child");
		}
	}

	public void addConnection(int fromNode, CompressedSocket sock)
	{
	}

	@Override
	public ArrayList<Operator> children()
	{
		final ArrayList<Operator> retval = new ArrayList<Operator>(1);
		retval.add(child);
		return retval;
	}

	public void clearParent()
	{
		parent = null;
	}

	@Override
	public NetworkSendOperator clone()
	{
		final NetworkSendOperator retval = new NetworkSendOperator(node, meta);
		retval.numParents = numParents;
		retval.numpSet = numpSet;
		retval.cardSet = cardSet;
		return retval;
	}

	@Override
	public void close() throws Exception
	{
		if (out != null)
		{
			out.close();
		}
		
		if (sock != null)
		{
			sock.close();
		}
		
		cols2Pos = null;
		cols2Types = null;
		pos2Col = null;
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

	public boolean hasAllConnections()
	{
		return false;
	}

	@Override
	public Object next(Operator op) throws Exception
	{
		throw new Exception("Unsupported operation");
	}

	@Override
	public void nextAll(Operator op)
	{
	}

	public boolean notStarted()
	{
		return !started;
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
			throw new Exception("NetworkSendOperator only supports 1 parent.");
		}
	}

	@Override
	public void removeChild(Operator op)
	{
		child = null;
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
		HRDBMSWorker.logger.error("NetworkSendOperator does not support reset()");
		throw new Exception("NetworkSendOperator does not support reset()"); 
	}

	public boolean setCard()
	{
		if (cardSet)
		{
			return false;
		}

		cardSet = true;
		return true;
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

	public boolean setNumParents()
	{
		if (numpSet)
		{
			return false;
		}

		numpSet = true;
		return true;
	}

	public void setSocket(CompressedSocket sock) throws Exception
	{
		try
		{
			this.sock = sock;
			out = new BufferedOutputStream(sock.getOutputStream());
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.error("", e);
			throw e;
		}
	}

	@Override
	public synchronized void start() throws Exception
	{
		try
		{
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
				out.write(obj);
				count++;
				o = child.next(this);
			}
			
			final byte[] obj = toBytes(o);
			out.write(obj);
			out.flush();
			//HRDBMSWorker.logger.debug("Wrote " + count + " rows");
			child.close();
			//Thread.sleep(60 * 1000); WHY was this here?
		}
		catch(Exception e)
		{
			HRDBMSWorker.logger.debug("", e);
			try
			{
				byte[] obj = toBytes(e);
				out.write(obj);
				out.flush();
				out.close();
				child.close();
			}
			catch(Exception f)
			{
				try
				{
					out.close();
					child.close();
				}
				catch(Exception g)
				{
					try
					{
						child.close();
					}
					catch(Exception h)
					{}
				}
			}
		}
	}

	@Override
	public String toString()
	{
		return "NetworkSendOperator(" + node + ")";
	}

	protected byte[] toBytes(Object v) throws Exception
	{
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
			Exception e = (Exception)v;
			byte[] data = null;
			try
			{
				data = e.getMessage().getBytes("UTF-8");
			}
			catch(Exception f)
			{}
			
			int dataLen = data.length;
			int recLen = 9 + dataLen;
			ByteBuffer bb = ByteBuffer.allocate(recLen+4);
			bb.position(0);
			bb.putInt(recLen);
			bb.putInt(1);
			bb.put((byte)10);
			bb.putInt(dataLen);
			bb.put(data);
			return bb.array();
		}
		else
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

		int size = val.size() + 8;
		final byte[] header = new byte[size];
		int i = 8;
		for (final Object o : val)
		{
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
				size += 8;
			}
			else if (o instanceof String)
			{
				header[i] = (byte)4;
				size += (4 + ((String)o).getBytes("UTF-8").length);
			}
			else
			{
				HRDBMSWorker.logger.error("Unknown type " + o.getClass() + " in toBytes()");
				HRDBMSWorker.logger.error(o);
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
		for (final Object o : val)
		{
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
				retvalBB.putLong(((MyDate)o).getTime());
			}
			else if (retval[i] == 4)
			{
				byte[] temp = null;
				try
				{
					temp = ((String)o).getBytes("UTF-8");
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.error("", e);
					throw e;
				}
				retvalBB.putInt(temp.length);
				retvalBB.put(temp);
			}

			i++;
		}

		return retval;
	}
}
