package com.exascale.optimizer.testing;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.rmi.ConnectException;

import com.exascale.optimizer.testing.NetworkReceiveOperator.InitThread;

public class NetworkHashReceiveOperator extends NetworkReceiveOperator
{
	protected int ID;
	
	public NetworkHashReceiveOperator(int ID, MetaData meta)
	{
		this.ID = ID;
		this.meta = meta;
	}
	
	public NetworkHashReceiveOperator clone()
	{
		NetworkHashReceiveOperator retval = new NetworkHashReceiveOperator(ID, meta);
		retval.node = node;
		return retval;
	}
	
	public void setID(int ID)
	{
		this.ID = ID;
	}
	
	public String toString()
	{
		return "NetworkHashReceiveOperator(" + node + ")";
	}
	
	public Object next(Operator op2) throws Exception
	{
		if (!fullyStarted)
		{
			synchronized(this)
			{
				if (!fullyStarted)
				{
					fullyStarted = true;
					for (Operator op : children)
					{
						NetworkSendOperator child = (NetworkSendOperator)op;
						CompressedSocket sock = null;
						try
						{
							sock = new CompressedSocket(meta.getHostNameForNode(child.getNode()), WORKER_PORT);
						}
						catch(java.net.ConnectException e)
						{
							e.printStackTrace();
							System.out.println("Connection failed to " + meta.getHostNameForNode(child.getNode()));
							System.exit(1);
						}
						socks.put(child, sock);
						OutputStream out = sock.getOutputStream();
						outs.put(child, out);
						InputStream in = new BufferedInputStream(sock.getInputStream(), 65536);
						ins.put(child, in);
						byte[] command = "SNDRMTTR".getBytes("UTF-8");
						byte[] from = intToBytes(node);
						byte[] to = intToBytes(child.getNode());
						byte[] idBytes = intToBytes(ID);
						byte[] data = new byte[command.length + from.length + to.length + idBytes.length];
						System.arraycopy(command, 0, data, 0, 8);
						System.arraycopy(from, 0, data, 8, 4);
						System.arraycopy(to, 0, data, 12, 4);
						System.arraycopy(idBytes, 0, data, 16, 4);
						out.write(data);
						out.flush();
						int doSend = in.read();
						if (doSend != 0)
						{
							ObjectOutputStream objOut = new ObjectOutputStream(out);
							objOuts.put(child, objOut);
							//Field[] fields = child.getClass().getDeclaredFields();
							//for (Field field : fields)
							//{
							//	printHierarchy("child", field, child);
							//}
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
		return o;
	}
}
