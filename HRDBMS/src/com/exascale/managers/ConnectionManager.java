package com.exascale.managers;

import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.LinkedBlockingQueue;
import com.exascale.threads.ConnectionWorker;
import com.exascale.threads.HRDBMSThread;

public class ConnectionManager extends HRDBMSThread
{
	private static LinkedBlockingQueue in = new LinkedBlockingQueue();
	private static boolean accepting = true;

	public ConnectionManager()
	{
		HRDBMSWorker.logger.info("Starting initialization of the Connection Manager.");
		description = "Connection Manager";
		this.setWait(true);
	}

	public static java.util.concurrent.LinkedBlockingQueue getInputQueue()
	{
		return in;
	}

	public static String remoteGetDataDirs(int from, int to, String host) throws Exception
	{
		final String cmd = "RGETDATD";
		final byte[] fromBytes = intToBytes(from);
		final byte[] toBytes = intToBytes(to);
		final byte[] data = formCall2Args(cmd, fromBytes, toBytes);
		// HRDBMSWorker.logger.debug("About to make RGETDATD RMI call to host "
		// + host + " with data: " + data);
		final String result = rmiCall(data, host);
		return result;
	}

	private static int bytesToInt(byte[] val)
	{
		final int x = java.nio.ByteBuffer.wrap(val).getInt();
		return x;
	}

	private static byte[] formCall2Args(String cmd, byte[] arg1, byte[] arg2) throws UnsupportedEncodingException
	{
		final byte[] cmdBytes = cmd.getBytes(StandardCharsets.UTF_8);
		final int size = cmdBytes.length;
		final int size1 = arg1.length;
		final int size2 = arg2.length;
		final byte[] retval = new byte[cmdBytes.length + arg1.length + arg2.length];
		int i = 0;
		int j = 0;
		while (i < size)
		{
			retval[j] = cmdBytes[i];
			i++;
			j++;
		}

		i = 0;
		while (i < size1)
		{
			retval[j] = arg1[i];
			i++;
			j++;
		}

		i = 0;
		while (i < size2)
		{
			retval[j] = arg2[i];
			i++;
			j++;
		}

		return retval;
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

	private static String rmiCall(byte[] data, String host) throws Exception
	{
		final int port = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number"));
		HRDBMSWorker.logger.debug("In rmiCall(), creating connection to " + host + " on port " + port);
		// final Socket sock = new Socket();
		Socket sock = new Socket();
		sock.setReceiveBufferSize(4194304);
		sock.setSendBufferSize(4194304);
		sock.connect(new InetSocketAddress(host, port));
		HRDBMSWorker.logger.debug("Connection successful.");
		sock.getOutputStream().write(data);
		HRDBMSWorker.logger.debug("Length of data sent: " + data.length);
		final InputStream in = sock.getInputStream();
		final byte[] typeBytes = new byte[8];
		final int num = in.read(typeBytes);
		if (num != 8)
		{
			HRDBMSWorker.logger.error("Error reading type in RMI response.");
			final String value = new String(typeBytes, StandardCharsets.UTF_8);
			HRDBMSWorker.logger.debug("read " + num + " bytes. Expected 8.  Value was " + value);
			sock.close();
			throw new Exception("InvalidResponseTypeException");
		}

		final String type = new String(typeBytes, StandardCharsets.UTF_8);
		if (type.equals("EXCEPT  "))
		{
			final byte[] eLen = new byte[4];
			if (in.read(eLen) != 4)
			{
				HRDBMSWorker.logger.error("Error reading RMI exception text size.");
				sock.close();
				throw new Exception("InvalidRMIExceptionTextSizeException");
			}

			final int len = bytesToInt(eLen);
			final byte[] text = new byte[len];
			if (in.read(text) != len)
			{
				HRDBMSWorker.logger.error("Error reading RMI exception text.");
				sock.close();
				throw new Exception("InvalidRMIExceptionTextException");
			}

			final String e = new String(text, StandardCharsets.UTF_8);
			sock.close();
			throw new Exception(e);
		}
		else if (type.equals("RESPOK  "))
		{
			final byte[] fromto = new byte[8];
			if (in.read(fromto) != 8)
			{
				HRDBMSWorker.logger.error("Error reading from/to info in RMI response.");
				sock.close();
				throw new Exception("InvalidRMIResponseHeaderException");
			}

			final byte[] respLen = new byte[4];
			if (in.read(respLen) != 4)
			{
				HRDBMSWorker.logger.error("Error reading RMI response length.");
				sock.close();
				throw new Exception("InvalidRMIResponseTextSizeException");
			}

			final int len = bytesToInt(respLen);
			final byte[] response = new byte[len];
			if (in.read(response) != len)
			{
				HRDBMSWorker.logger.error("Error reading RMI response text.");
				sock.close();
				throw new Exception("InvalidRMIResponseTextException");
			}

			final String r = new String(response, StandardCharsets.UTF_8);
			sock.close();
			return r;
		}
		else
		{
			sock.close();
			throw new Exception("InvalidResponseTypeException");
		}
	}

	@Override
	public void run()
	{
		try
		{
			final ServerSocket server = new ServerSocket();
			server.setReceiveBufferSize(4194304);
			server.bind(new InetSocketAddress(Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number"))));
			HRDBMSWorker.logger.info("Connection Manager initialization complete.");
			while (true)
			{
				final String msg = (String)in.peek();
				if (msg != null)
				{
					processMessage(msg);
				}
				final Socket sock = server.accept();
				sock.setSendBufferSize(4194304);
				// HRDBMSWorker.logger.debug("Connection Manager accepted a
				// connection.");
				if (accepting)
				{
					HRDBMSWorker.addThread(new ConnectionWorker(sock));
				}
				else
				{
					sock.close();
				}
			}
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.error("", e);
			in = null;
			terminate();
			return;
		}
	}

	private void processMessage(String msg)
	{
		if (msg.equals("STOP ACCEPT"))
		{
			accepting = false;
		}
		else if (msg.equals("START ACCEPT"))
		{
			accepting = true;
		}
		else
		{
			HRDBMSWorker.logger.error("Unknown message received by the Connection Manager: " + msg);
			in = null;
			this.terminate();
			return;
		}
	}
}
