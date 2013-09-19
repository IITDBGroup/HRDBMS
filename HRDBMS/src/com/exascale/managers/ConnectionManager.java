package com.exascale.managers;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.exascale.threads.ConnectionWorker;
import com.exascale.threads.HRDBMSThread;

public class ConnectionManager extends HRDBMSThread
{
	private static BlockingQueue<String> in = new LinkedBlockingQueue<String>();
	private static boolean accepting = true;
	
	public ConnectionManager()
	{
		HRDBMSWorker.logger.info("Starting initialization of the Connection Manager.");
		description = "Connection Manager";
		this.setWait(true);
	}
	
	public static BlockingQueue<String> getInputQueue()
	{
		return in;
	}

	public void run()
	{
		try
		{
			ServerSocket server = new ServerSocket(Integer.parseInt(HRDBMSWorker.hparms.getProperty("port_number")));
			HRDBMSWorker.logger.info("Connection Manager initialization complete.");
			while (true)
			{
				String msg = in.poll();
				if (msg != null)
				{
					processMessage(msg);
				}
				Socket sock = server.accept();
				HRDBMSWorker.logger.debug("Connection Manager accepted a connection.");
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
		catch(Exception e)
		{
			e.printStackTrace();
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
	
	public static String remoteGetDataDirs(int from, int to, String host) throws Exception
	{
		String cmd = "RGETDATD";
		byte[] fromBytes = intToBytes(from);
		byte[] toBytes = intToBytes(to);
		byte[] data = formCall2Args(cmd, fromBytes, toBytes);
		HRDBMSWorker.logger.debug("About to make RGETDATD RMI call to host " + host + " with data: " + data);
		String result =  rmiCall(data, host);
		return result;
	}
	
	private static int bytesToInt(byte[] val)
	{
		int x = java.nio.ByteBuffer.wrap(val).getInt();
		return x;
	}
	
	private static String rmiCall(byte[] data, String host) throws Exception
	{
		int port = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number"));
		HRDBMSWorker.logger.debug("In rmiCall(), creating connection to " + host + " on port " + port);
		Socket sock = new Socket(host, port);
		HRDBMSWorker.logger.debug("Connection successful.");
		sock.getOutputStream().write(data);
		HRDBMSWorker.logger.debug("Length of data sent: " + data.length);
		InputStream in = sock.getInputStream();
		byte[] typeBytes = new byte[8];
		int num = in.read(typeBytes);
		if (num != 8)
		{
			HRDBMSWorker.logger.error("Error reading type in RMI response.");
			String value = new String(typeBytes, "UTF-8");
			HRDBMSWorker.logger.debug("read " + num + " bytes. Expected 8.  Value was " + value);
			throw new Exception("InvalidResponseTypeException");
		}
		
		String type = new String(typeBytes, "UTF-8");
		if (type.equals("EXCEPT  "))
		{
			byte[] eLen = new byte[4];
			if (in.read(eLen) != 4)
			{
				HRDBMSWorker.logger.error("Error reading RMI exception text size.");
				throw new Exception("InvalidRMIExceptionTextSizeException");
			}
			
			int len = bytesToInt(eLen);
			byte[] text = new byte[len];
			if (in.read(text) != len)
			{
				HRDBMSWorker.logger.error("Error reading RMI exception text.");
				throw new Exception("InvalidRMIExceptionTextException");
			}
			
			String e = new String(text, "UTF-8");
			sock.close();
			throw new Exception(e);
		}
		else if (type.equals("RESPOK  "))
		{
			byte[] fromto = new byte[8];
			if (in.read(fromto) != 8)
			{
				HRDBMSWorker.logger.error("Error reading from/to info in RMI response.");
				throw new Exception("InvalidRMIResponseHeaderException");
			}
			
			byte[] respLen = new byte[4];
			if (in.read(respLen) != 4)
			{
				HRDBMSWorker.logger.error("Error reading RMI response length.");
				throw new Exception("InvalidRMIResponseTextSizeException");
			}
			
			int len = bytesToInt(respLen);
			byte[] response = new byte[len];
			if (in.read(response) != len)
			{
				HRDBMSWorker.logger.error("Error reading RMI response text.");
				throw new Exception("InvalidRMIResponseTextException");
			}
			
			String r = new String(response, "UTF-8");
			sock.close();
			return r;
		}
		else
		{
			throw new Exception("InvalidResponseTypeException");
		}
	}
	
	private static byte[] intToBytes(int val)
	{
		byte[] buff = new byte[4];
		buff[0] = (byte)(val >> 24);
		buff[1] = (byte)((val & 0x00FF0000) >> 16);
		buff[2] = (byte)((val & 0x0000FF00) >> 8);
		buff[3] = (byte)((val & 0x000000FF));
		return buff;
	}
	
	private static byte[] formCall2Args(String cmd, byte[] arg1, byte[] arg2) throws UnsupportedEncodingException
	{
		byte[] cmdBytes = cmd.getBytes("UTF-8");
		byte[] retval = new byte[cmdBytes.length + arg1.length + arg2.length];
		int i = 0;
		int j = 0;
		while (i < cmdBytes.length)
		{
			retval[j] = cmdBytes[i];
			i++;
			j++;
		}
		
		i = 0;
		while (i < arg1.length)
		{
			retval[j] = arg1[i];
			i++;
			j++;
		}
		
		i = 0;
		while (i < arg2.length)
		{
			retval[j] = arg2[i];
			i++;
			j++;
		}
		
		return retval;
	}
}
