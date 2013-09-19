package com.exascale.threads;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.Socket;
import java.util.HashMap;
import java.util.Vector;
import java.util.concurrent.LinkedBlockingQueue;

import com.exascale.managers.HRDBMSWorker;
import com.exascale.managers.XAManager;
import com.exascale.tables.Plan;
import com.exascale.tables.QueueEndMarker;
import com.exascale.tables.Transaction;

public class ConnectionWorker extends HRDBMSThread
{
	private Socket sock;
	private LinkedBlockingQueue rsQueue;
	
	public ConnectionWorker(Socket sock)
	{
		this.description = "Connection Worker";
		this.sock = sock;
	}
	
	public void run()
	{
		HRDBMSWorker.logger.debug("New connection worker is up and running");
		try
		{
			while (true)
			{
				byte[] cmd = new byte[8];
				InputStream in = sock.getInputStream();
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
				String command = new String(cmd, "UTF-8");
				HRDBMSWorker.logger.debug("Received " + num + " bytes");
				HRDBMSWorker.logger.debug("Command received by connection worker: " + command);
			
				byte[] fromBytes = new byte[4];
				byte[] toBytes = new byte[4];
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
				int from = bytesToInt(fromBytes);
				int to = bytesToInt(toBytes);
			
				if (command.equals("RGETDATD"))
				{
					try
					{
						String retval = getDataDir(from, to);
						HRDBMSWorker.logger.debug("Responding with " + retval);
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
					return;
				}
				//else if (command.equals("CLIENT  "))TODO
				//{
				//	clientConnection();
				//}
				//else if (command.equals("XAMANWRS")) TODO
				//{
				//	int iso = getXAManageIso();
				//	Plan p = receiveSubPlan();
				//	p.setDestination(sock.getRemoteSocketAddress().toString());
				//	Transaction global = XAManager.newTransaction(iso);
				//	XAManager.runWithRS(p, global);
				//	p.close();
				//	closeConnection();
				//	this.terminate();
				//  return;
				//}
				else
				{
					HRDBMSWorker.logger.error("Uknown command received by ConnectionWorker: " + command);
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
		HRDBMSWorker.logger.debug("In respond(), response is "+ data);
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
	
	private String getDataDir(int from, int to)
	{
		return to + "," + HRDBMSWorker.getHParms().getProperty("data_directories");
	}
	
	/*private void clientConnection() TODO
	{
		try
		{
			InputStream in = sock.getInputStream();
			byte[] isoLevel = new byte[4];
			if (in.read(isoLevel) != 4)
			{
				HRDBMSWorker.logger.error("Error receiving isolation level from client connection!");
				returnException("BadCommandException");
				sock.close();
				this.terminate();
				return;
			}
			
			int iso = bytesToInt(isoLevel);
			
			Transaction global = XAManager.newTransaction(iso);
			while (true)
			{
				byte[] cmd = new byte[8];
				if (in.read(cmd) != 8)
				{
					HRDBMSWorker.logger.error("Connection worker received less than 8 bytes when reading a command.  Terminating!");
					returnException("BadCommandException - Transaction will be rolled back.");
					XAManager.rollback(global);
					sock.close();
					this.terminate();
					return;
				}
			
				String command = new String(cmd, "UTF-8");
				if (command.equals("CLOSE   "))
				{
					XAManager.rollback(global);
					closeConnection();
					this.terminate();
					return;
				}
				else if (command.equals("EXECQRY "))
				{
					byte[] data = new byte[4];
					if (in.read(data) != 4)
					{
						HRDBMSWorker.logger.error("Expected 4 bytes for number of arguments and did not receive them.");
						returnException("InvalidNumArgsException - Transaction will be rolled back.");
						XAManager.rollback(global);
						sock.close();
						this.terminate();
						return;
					}
					else
					{
						int numArgs = bytesToInt(data);
						int i = 1;
						if (numArgs == 0)
						{
							HRDBMSWorker.logger.error("Expected at least 1 argument on executeQuery call.  None received.");
							returnException("InvalidNumArgsException - Transaction will be rolled back.");
							XAManager.rollback(global);
							sock.close();
							this.terminate();
							return;
						}
						
						if (in.read(data) != 4)
						{
							HRDBMSWorker.logger.error("Expected length for SQL statement on executeQuery call.");
							returnException("InvalidClientArgumentException - Transaction will be rolled back.");
							XAManager.rollback(global);
							sock.close();
							this.terminate();
							return;
						}
						else
						{
							int sqlLength = bytesToInt(data);
							byte[] sql = new byte[sqlLength];
							if (in.read(sql) != sqlLength)
							{
								HRDBMSWorker.logger.error("Did not receive enough bytes reading SQL statement on executeQuery.");
								returnException("InvalidClientArgumentException - Transaction will be rolled back.");
								XAManager.rollback(global);
								sock.close();
								this.terminate();
								return;
							}
							else
							{
								String text = new String(sql, "UTF-8");
								byte[][] args = new byte[numArgs-1][];
								while (i < numArgs)
								{
									data = new byte[4];
									if (in.read(data) != 4)
									{
										HRDBMSWorker.logger.error("Error reading argument length in executeQuery.");
										returnException("InvalidClientArgumentException - Transaction will be rolled back.");
										XAManager.rollback(global);
										sock.close();
										this.terminate();
										return;
									}
									else
									{
										data = new byte[bytesToInt(data)];
										if (in.read(data) != data.length)
										{
											HRDBMSWorker.logger.error("Error reading argument in executeQuery.");
											returnException("InvalidClientArgumentException - Transaction will be rolled back.");
											XAManager.rollback(global);
											sock.close();
											this.terminate();
											return;
										}
										else
										{
											args[i-1] = data;
										}
									}
									
									i++;
								}
								
								Plan plan = PlanManager.createPlan(text, args);
								rsQueue = XAManager.runWithRS(plan, global);
							}
						}
					}
				}
				else
				{
					HRDBMSWorker.logger.error("Unknown command received by ConnectionWorker: " + command);
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
			return;
		}
	} */
}

//TODO XAManager.next(global);