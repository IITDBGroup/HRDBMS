package com.exascale.optimizer;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import com.exascale.compression.CompressedSocket;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.misc.DataEndMarker;

public final class NetworkHashReceiveOperator extends NetworkReceiveOperator
{
	private int ID;

	public NetworkHashReceiveOperator(int ID, MetaData meta)
	{
		this.ID = ID;
		this.meta = meta;
	}

	@Override
	public NetworkHashReceiveOperator clone()
	{
		final NetworkHashReceiveOperator retval = new NetworkHashReceiveOperator(ID, meta);
		retval.node = node;
		return retval;
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
						CompressedSocket sock = null;
						try
						{
							sock = CompressedSocket.newCompressedSocket(meta.getHostNameForNode(child.getNode()), WORKER_PORT);
						}
						catch (final java.net.ConnectException e)
						{
							HRDBMSWorker.logger.error("Connection failed to " + meta.getHostNameForNode(child.getNode()), e);
							throw e;
						}
						socks.put(child, sock);
						final OutputStream out = sock.getOutputStream();
						outs.put(child, out);
						final InputStream in = new BufferedInputStream(sock.getInputStream(), 65536);
						ins.put(child, in);
						final byte[] command = "SNDRMTTR".getBytes("UTF-8");
						final byte[] from = intToBytes(node);
						final byte[] to = intToBytes(child.getNode());
						final byte[] idBytes = intToBytes(ID);
						final byte[] data = new byte[command.length + from.length + to.length + idBytes.length];
						System.arraycopy(command, 0, data, 0, 8);
						System.arraycopy(from, 0, data, 8, 4);
						System.arraycopy(to, 0, data, 12, 4);
						System.arraycopy(idBytes, 0, data, 16, 4);
						out.write(data);
						out.flush();
						final int doSend = in.read();
						if (doSend != 0)
						{
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

	public void setID(int ID)
	{
		this.ID = ID;
	}

	@Override
	public String toString()
	{
		return "NetworkHashReceiveOperator(" + node + ")";
	}
}
