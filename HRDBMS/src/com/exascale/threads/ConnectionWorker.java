package com.exascale.threads;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import com.exascale.compression.CompressedSocket;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.optimizer.NetworkSendOperator;

public class ConnectionWorker extends HRDBMSThread
{
	private final CompressedSocket sock;
	private static HashMap<Integer, NetworkSendOperator> sends;

	static
	{
		sends = new HashMap<Integer, NetworkSendOperator>();
	}

	public ConnectionWorker(CompressedSocket sock)
	{
		this.description = "Connection Worker";
		this.sock = sock;
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
	public void run()
	{
		HRDBMSWorker.logger.debug("New connection worker is up and running");
		try
		{
			while (true)
			{
				final byte[] cmd = new byte[8];
				final InputStream in = sock.getInputStream();
				final OutputStream out = sock.getOutputStream();
				int num = in.read(cmd);
				if (num != 8 && num != -1)
				{
					HRDBMSWorker.logger.error("Connection worker received less than 8 bytes when reading a command.  Terminating!");
					returnException("BadCommandException");
					sock.close();
					this.terminate();
					return;
				}
				else if (num == -1)
				{
					HRDBMSWorker.logger.debug("Received EOF when looking for a command.");
					sock.close();
					this.terminate();
					return;
				}
				final String command = new String(cmd, "UTF-8");
				HRDBMSWorker.logger.debug("Received " + num + " bytes");
				HRDBMSWorker.logger.debug("Command received by connection worker: " + command);

				final byte[] fromBytes = new byte[4];
				final byte[] toBytes = new byte[4];
				num = in.read(fromBytes);
				if (num != 4)
				{
					HRDBMSWorker.logger.error("Received less than 4 bytes when reading from field in remote command.");
					HRDBMSWorker.logger.debug("Received " + num + " bytes");
					returnException("InvalidFromIDException");
					sock.close();
					this.terminate();
					return;
				}
				HRDBMSWorker.logger.debug("Read " + num + " bytes");
				HRDBMSWorker.logger.debug("From field received by connection worker.");
				num = in.read(toBytes);
				if (num != 4)
				{
					HRDBMSWorker.logger.error("Received less than 4 bytes when reading to field in remote command.");
					returnException("InvalidToIDException");
					sock.close();
					this.terminate();
					return;
				}
				HRDBMSWorker.logger.debug("Read " + num + " bytes");
				HRDBMSWorker.logger.debug("To field received by connection worker.");
				final int from = bytesToInt(fromBytes);
				final int to = bytesToInt(toBytes);

				if (command.equals("RGETDATD"))
				{
					try
					{
						final String retval = getDataDir(from, to);
						HRDBMSWorker.logger.debug("Responding with " + retval);
						respond(to, from, retval);
					}
					catch (final Exception e)
					{
						returnException(e.toString());
					}
				}
				else if (command.equals("REMOTTRE"))
				{
					final ObjectInputStream objIn = new ObjectInputStream(in);
					final NetworkSendOperator op = (NetworkSendOperator)objIn.readObject();
					op.setSocket(sock);
					op.start();
					op.close();
				}
				else if (command.equals("SNDRMTTR"))
				{
					final byte[] idBytes = new byte[4];
					num = in.read(idBytes);
					if (num != 4)
					{
						throw new Exception("Received less than 4 bytes when reading id field in SNDRMTTR command.");
					}

					final int id = bytesToInt(idBytes);
					if (!sends.containsKey(id))
					{
						out.write(1); // do send
						out.flush();
						final ObjectInputStream objIn = new ObjectInputStream(in);
						final NetworkSendOperator op = (NetworkSendOperator)objIn.readObject();
						synchronized (sends)
						{
							if (!sends.containsKey(id))
							{
								sends.put(id, op);
							}
						}
					}
					else
					{
						out.write(0); // don't send
						out.flush();
					}

					final NetworkSendOperator send = sends.get(id);
					//System.out.println("Adding connection from " + from + " to " + send + " = " + sock);
					send.addConnection(from, sock);
					synchronized (send)
					{
						if (send.notStarted() && send.hasAllConnections())
						{
							send.start();
							send.close();
							sends.remove(id);
						}
					}
				}
				else if (command.equals("CLOSE   "))
				{
					closeConnection();
					this.terminate();
					return;
				}
				else if (command.equals("CLIENT  "))
				{
					clientConnection();
				}
				else
				{
					HRDBMSWorker.logger.error("Uknown command received by ConnectionWorker: " + command);
					returnException("BadCommandException");
				}
			}
		}
		catch (final Exception e)
		{
			try
			{
				returnException(e.toString());
				sock.close();
			}
			catch (final Exception f)
			{
			}
			this.terminate();
			return;
		}
	}

	private int bytesToInt(byte[] val)
	{
		final int ret = java.nio.ByteBuffer.wrap(val).getInt();
		return ret;
	}

	private void closeConnection()
	{
		try
		{
			sock.close();
		}
		catch (final Exception e)
		{
		}
	}

	private String getDataDir(int from, int to)
	{
		return to + "," + HRDBMSWorker.getHParms().getProperty("data_directories");
	}

	private void respond(int from, int to, String retval) throws UnsupportedEncodingException, IOException
	{
		final byte[] type = "RESPOK  ".getBytes("UTF-8");
		final byte[] fromBytes = intToBytes(from);
		final byte[] toBytes = intToBytes(to);
		final byte[] ret = retval.getBytes("UTF-8");
		final byte[] retSize = intToBytes(ret.length);
		final byte[] data = new byte[type.length + fromBytes.length + toBytes.length + retSize.length + ret.length];
		System.arraycopy(type, 0, data, 0, type.length);
		System.arraycopy(fromBytes, 0, data, type.length, fromBytes.length);
		System.arraycopy(toBytes, 0, data, type.length + fromBytes.length, toBytes.length);
		System.arraycopy(retSize, 0, data, type.length + fromBytes.length + toBytes.length, retSize.length);
		System.arraycopy(ret, 0, data, type.length + fromBytes.length + toBytes.length + retSize.length, ret.length);
		HRDBMSWorker.logger.debug("In respond(), response is " + data);
		sock.getOutputStream().write(data);
	}

	private void returnException(String e) throws UnsupportedEncodingException, IOException
	{
		final byte[] type = "EXCEPT  ".getBytes("UTF-8");
		final byte[] ret = e.getBytes("UTF-8");
		final byte[] retSize = intToBytes(ret.length);
		final byte[] data = new byte[type.length + retSize.length + ret.length];
		System.arraycopy(type, 0, data, 0, type.length);
		System.arraycopy(retSize, 0, data, type.length, retSize.length);
		System.arraycopy(ret, 0, data, type.length + retSize.length, ret.length);
		sock.getOutputStream().write(data);
	}
	
	public void clientConnection()
	{
		
	}
}