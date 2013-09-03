package com.exascale;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class ConnectionManager extends HRDBMSThread
{
	private static BlockingQueue<String> in = new LinkedBlockingQueue<String>();
	private static boolean accepting = true;
	
	public ConnectionManager()
	{
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
			while (true)
			{
				String msg = in.poll();
				if (msg != null)
				{
					processMessage(msg);
				}
				Socket sock = server.accept();
				
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
			System.err.println("Unknown message sent to ConnectionManager: " + msg);
			in = null;
			this.terminate();
		}
	}
	
	public static String remoteGetDataDirs(int from, int to, String host) throws Exception
	{
		String cmd = "RGETDATD";
		byte[] fromBytes = intToBytes(from);
		byte[] toBytes = intToBytes(to);
		byte[] data = formCall2Args(cmd, fromBytes, toBytes);
		String result =  rmiCall(data, host);
		cmd = "CLOSE   ";
		data = formCall2Args(cmd, fromBytes, toBytes);
		rmiCall(data, host);
		return result;
	}
	
	private static int bytesToInt(byte[] val)
	{
		int ret = val[0] << 24;
		ret += val[1] << 16;
		ret += val[2] << 8;
		ret += val[3];
		return ret;
	}
	
	private static String rmiCall(byte[] data, String host) throws Exception
	{
		Socket sock = new Socket(host, Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
		sock.getOutputStream().write(data);
		InputStream in = sock.getInputStream();
		byte[] typeBytes = new byte[8];
		if (in.read(typeBytes) != 8)
		{
			System.err.println("Error reading type in RMI response.");
			throw new Exception("InvalidResponseTypeException");
		}
		
		String type = new String(typeBytes, "UTF-8");
		if (type.equals("EXCEPT  "))
		{
			byte[] eLen = new byte[4];
			if (in.read(eLen) != 4)
			{
				System.err.println("Error reading RMI exception text size.");
				throw new Exception("InvalidRMIExceptionTextSizeException");
			}
			
			int len = bytesToInt(eLen);
			byte[] text = new byte[len];
			if (in.read(text) != len)
			{
				System.err.println("Error reading RMI exception text.");
				throw new Exception("InvalidRMIExceptionTextException");
			}
			
			String e = new String(text, "UTF-8");
			throw new Exception(e);
		}
		else if (type.equals("RESPOK  "))
		{
			byte[] fromto = new byte[8];
			if (in.read(fromto) != 8)
			{
				System.err.println("Error reading from/to info in RMI response.");
				throw new Exception("InvalidRMIResponseHeaderException");
			}
			
			byte[] respLen = new byte[4];
			if (in.read(respLen) != 4)
			{
				System.err.println("Error reading RMI response length.");
				throw new Exception("InvalidRMIResponseTextSizeException");
			}
			
			int len = bytesToInt(respLen);
			byte[] response = new byte[len];
			if (in.read(response) != len)
			{
				System.err.println("Error reading RMI response text.");
				throw new Exception("InvalidRMIResponseTextException");
			}
			
			String r = new String(response, "UTF-8");
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
