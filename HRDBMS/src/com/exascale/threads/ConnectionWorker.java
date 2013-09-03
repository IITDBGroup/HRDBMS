package com.exascale;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.Socket;

public class ConnectionWorker extends HRDBMSThread
{
	private Socket sock;
	public ConnectionWorker(Socket sock)
	{
		this.description = "Connection Worker";
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
				if (in.read(cmd) != 8)
				{
					System.err.println("Connection worker received less than 8 bytes when reading a command.  Terminating!");
					returnException("BadCommandException");
					sock.close();
					this.terminate();
				}
			
				byte[] fromBytes = new byte[4];
				byte[] toBytes = new byte[4];
				if (in.read(fromBytes) != 4)
				{
					System.err.println("Received less than 4 bytes when reading from field in remote command.");
					returnException("InvalidFromIDException");
					sock.close();
					this.terminate();
				}
			
				if (in.read(toBytes) != 4)
				{
					System.err.println("Received less than 4 bytes when reading to field in remote command.");
					returnException("InvalidToIDException");
					sock.close();
					this.terminate();
				}
			
				int from = bytesToInt(fromBytes);
				int to = bytesToInt(toBytes);
				String command = new String(cmd, "UTF-8");
			
				if (command.equals("RGETDATD"))
				{
					try
					{
						String retval = getDataDir(from, to);
						respond(to, from, retval);
					}
					catch(Exception e)
					{
						returnException(e.toString());
					}
				}
				else if (command.equals("CLOSE   "))
				{
					closeConnection();
					this.terminate();
				}
				else
				{
					System.err.println("Uknown command received by ConnectionWorker: " + command);
					returnException("BadCommandException");
				}
			}
		}
		catch(Exception e)
		{
			try
			{
				returnException(e.toString());
				sock.close();
			}
			catch(Exception f) {}
			this.terminate();
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
		int ret = val[0] << 24;
		ret += val[1] << 16;
		ret += val[2] << 8;
		ret += val[3];
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
	
	private String getDataDir(int from, int to)
	{
		return to + HRDBMSWorker.getHParms().getProperty("data_directories");
	}
}
