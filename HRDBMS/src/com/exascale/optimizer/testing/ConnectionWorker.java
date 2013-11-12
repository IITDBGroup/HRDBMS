package com.exascale.optimizer.testing;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.UnsupportedEncodingException;
import java.net.Socket;
import java.util.HashMap;
import java.util.Vector;
import java.util.concurrent.LinkedBlockingQueue;

public class ConnectionWorker extends Thread
{
	private Socket sock;
	
	public ConnectionWorker(Socket sock)
	{
		this.sock = sock;
	}
	
	public void run()
	{
		try
		{
			while (true)
			{
				byte[] cmd = new byte[8];
				InputStream in = sock.getInputStream();
				int num = in.read(cmd);
				if (num != 8 && num != -1)
				{
					throw new Exception("Connection worker received less than 8 bytes when reading a command.  Terminating!");
				}
				else if (num == -1)
				{
					sock.close();
					return;
				}
				String command = new String(cmd, "UTF-8");
			
				byte[] fromBytes = new byte[4];
				byte[] toBytes = new byte[4];
				num = in.read(fromBytes);
				if (num != 4)
				{
					throw new Exception("Received less than 4 bytes when reading from field in remote command.");
				}
				
				num = in.read(toBytes);
				if (num != 4)
				{
					throw new Exception("Received less than 4 bytes when reading to field in remote command.");
				}
				
				int from = bytesToInt(fromBytes);
				int to = bytesToInt(toBytes);
			
				if (command.equals("REMOTTRE"))
				{
					ObjectInputStream objIn = new ObjectInputStream(in);
					NetworkSendOperator op = (NetworkSendOperator)objIn.readObject();
					op.setSocket(sock);
					op.start();
					op.close();
				}
				else
				{
					throw new Exception("Unknown command received by ConnectionWorker: " + command);
				}
			}
		}
		catch(Exception e)
		{
			try
			{
				e.printStackTrace();
				returnException(e.toString());
				sock.close();
			}
			catch(Exception f) {}
			return;
		}
	}
	
	private void returnException(String e) throws UnsupportedEncodingException, IOException
	{
		byte[] type = "EXCEPT  ".getBytes("UTF-8");
		byte[] ret = e.getBytes("UTF-8");
		byte[] retSize = intToBytes(ret.length);
		byte[] data = new byte[type.length + retSize.length + ret.length];
		System.arraycopy(type, 0, data, 0, type.length);
		System.arraycopy(retSize, 0, data, type.length, retSize.length);
		System.arraycopy(ret, 0, data, type.length  + retSize.length, ret.length);
		sock.getOutputStream().write(data);
	}
	
	private void closeConnection()
	{
		try
		{
			sock.close();
		}
		catch(Exception e) {}
	}
	
	private void respond(int from, int to, String retval) throws UnsupportedEncodingException, IOException
	{
		byte[] type = "RESPOK  ".getBytes("UTF-8");
		byte[] fromBytes = intToBytes(from);
		byte[] toBytes = intToBytes(to);
		byte[] ret = retval.getBytes("UTF-8");
		byte[] retSize = intToBytes(ret.length);
		byte[] data = new byte[type.length + fromBytes.length + toBytes.length + retSize.length + ret.length];
		System.arraycopy(type, 0, data, 0, type.length);
		System.arraycopy(fromBytes, 0, data, type.length, fromBytes.length);
		System.arraycopy(toBytes, 0, data, type.length + fromBytes.length, toBytes.length);
		System.arraycopy(retSize, 0, data, type.length + fromBytes.length + toBytes.length, retSize.length);
		System.arraycopy(ret, 0, data, type.length + fromBytes.length + toBytes.length + retSize.length, ret.length);
		sock.getOutputStream().write(data);
	}
	
	private int bytesToInt(byte[] val)
	{
		int ret = java.nio.ByteBuffer.wrap(val).getInt();
		return ret;
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
}