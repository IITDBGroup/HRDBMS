package com.exascale.threads;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.lang.management.ManagementFactory;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import com.exascale.compression.CompressedSocket;
import com.exascale.filesystem.Block;
import com.exascale.filesystem.Page;
import com.exascale.filesystem.RID;
import com.exascale.logging.LogIterator;
import com.exascale.logging.LogRec;
import com.exascale.managers.BufferManager;
import com.exascale.managers.FileManager;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.managers.LockManager;
import com.exascale.managers.LogManager;
import com.exascale.managers.PlanCacheManager;
import com.exascale.managers.ResourceManager;
import com.exascale.managers.XAManager;
import com.exascale.misc.AtomicDouble;
import com.exascale.misc.BufferedLinkedBlockingQueue;
import com.exascale.misc.DataEndMarker;
import com.exascale.misc.FastStringTokenizer;
import com.exascale.misc.HParms;
import com.exascale.misc.MultiHashMap;
import com.exascale.misc.MurmurHash;
import com.exascale.misc.MyDate;
import com.exascale.optimizer.Filter;
import com.exascale.optimizer.Index;
import com.exascale.optimizer.Index.IndexRecord;
import com.exascale.optimizer.IndexOperator;
import com.exascale.optimizer.LoadMetaData;
import com.exascale.optimizer.MetaData;
import com.exascale.optimizer.NetworkReceiveOperator;
import com.exascale.optimizer.Operator;
import com.exascale.optimizer.RootOperator;
import com.exascale.optimizer.TableScanOperator;
import com.exascale.optimizer.MetaData.PartitionMetaData;
import com.exascale.optimizer.NetworkSendOperator;
import com.exascale.optimizer.RIDAndIndexKeys;
import com.exascale.tables.DataType;
import com.exascale.tables.Plan;
import com.exascale.tables.RIDChange;
import com.exascale.tables.Schema;
import com.exascale.tables.Transaction;
import com.exascale.tables.Schema.FieldValue;
import com.exascale.tables.Schema.Row;
import com.exascale.tables.Schema.RowIterator;
import com.sun.management.OperatingSystemMXBean;

public class ConnectionWorker extends HRDBMSThread
{
	private final CompressedSocket sock;
	private static HashMap<Integer, NetworkSendOperator> sends;
	private static int PREFETCH_REQUEST_SIZE;
	private static int PAGES_IN_ADVANCE;
	private boolean clientConnection = false;
	private Transaction tx = null;
	private XAWorker worker;
	private static ConcurrentHashMap<String, LoadMetaData> ldmds = new ConcurrentHashMap<String, LoadMetaData>();
	private static int maxLoad = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("max_load_average")); 
	private static int criticalMem = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("critical_mem_percent"));
	private Random random = new Random();

	static
	{
		sends = new HashMap<Integer, NetworkSendOperator>();
		HParms hparms = HRDBMSWorker.getHParms();
		PREFETCH_REQUEST_SIZE = Integer.parseInt(hparms.getProperty("prefetch_request_size")); // 80
		PAGES_IN_ADVANCE = Integer.parseInt(hparms.getProperty("pages_in_advance")); // 40
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
	
	public static double parseUptimeResult(String uptimeCmdResult) throws Exception 
	{
		String load = uptimeCmdResult.substring(uptimeCmdResult.lastIndexOf(',') + 1).trim(); 
		return Double.parseDouble(load);
	}
	
	public static String runUptimeCommand(String crunchifyCmd, boolean waitForResult) throws Exception
	{
		ProcessBuilder crunchifyProcessBuilder = null;
 
		crunchifyProcessBuilder = new ProcessBuilder("/bin/bash", "-c", crunchifyCmd);
		crunchifyProcessBuilder.redirectErrorStream(true);
		Writer crunchifyWriter = null;
		try {
			Process process = crunchifyProcessBuilder.start();
			if (waitForResult) {
				InputStream crunchifyStream = process.getInputStream();
 
				if (crunchifyStream != null) {
					crunchifyWriter = new StringWriter();
 
					char[] crunchifyBuffer = new char[2048];
					try {
						Reader crunchifyReader = new BufferedReader(new InputStreamReader(crunchifyStream, "UTF-8"));
						int count;
						while ((count = crunchifyReader.read(crunchifyBuffer)) != -1) {
							crunchifyWriter.write(crunchifyBuffer, 0, count);
						}
					} finally {
						crunchifyStream.close();
					}
					crunchifyWriter.toString();
					crunchifyStream.close();
				}
			}
		} catch (Exception e) 
		{
			throw e;
		}
		return crunchifyWriter.toString();
	}
	
	private static boolean memoryOK()
	{
		return ((Runtime.getRuntime().freeMemory() + ResourceManager.maxMemory - Runtime.getRuntime().totalMemory()) * 100.0) / (ResourceManager.maxMemory * 1.0) > criticalMem;
	}

	@Override
	public void run()
	{
		//check if there are enough available resources or not
		if (HRDBMSWorker.type == HRDBMSWorker.TYPE_MASTER || HRDBMSWorker.type == HRDBMSWorker.TYPE_COORD)
		{
			try
			{
				while (true)
				{
					String uptimeCmd = "uptime";
					String uptimeCmdResult = runUptimeCommand(uptimeCmd, true);
					double load = parseUptimeResult(uptimeCmdResult);
					if (load <= (maxLoad * 1.0))
					{
						if (memoryOK())
						{
							break;
						}
					}
				
					Thread.sleep(5000);
				}
			}
			catch(Exception e)
			{
				HRDBMSWorker.logger.debug("", e);
			}
		}
		
		//HRDBMSWorker.logger.debug("New connection worker is up and running");
		try
		{
			while (true)
			{
				final byte[] cmd = new byte[8];
				InputStream in = null;
				try
				{
					in = sock.getInputStream();
				}
				catch(java.net.SocketException e)
				{}
				
				OutputStream out = null;
				int num = 0;
				try
				{
					out = sock.getOutputStream();
					num = in.read(cmd);
				}
				catch(Exception e)
				{
					if (worker != null)
					{
						ArrayList<Object> cmd2 = new ArrayList<Object>(1);
						cmd2.add("CLOSE");
						while(true)
						{
							try
							{
								worker.in.put(cmd2);
								break;
							}
							catch(Exception f)
							{}
						}
					}
					sock.close();
					this.terminate();
					return;
				}
				
				if (num != 8 && num != -1)
				{
					HRDBMSWorker.logger.error("Connection worker received less than 8 bytes when reading a command.  Terminating!");
					HRDBMSWorker.logger.error("Number of bytes received: " + num);
					HRDBMSWorker.logger.error("Command = " + new String(cmd, "UTF-8"));
					returnException("BadCommandException");
					sock.close();
					if (worker != null)
					{
						ArrayList<Object> cmd2 = new ArrayList<Object>(1);
						cmd2.add("CLOSE");
						worker.in.put(cmd2);
					}
					this.terminate();
					return;
				}
				else if (num == -1)
				{
					//HRDBMSWorker.logger.debug("Received EOF when looking for a command.");
					sock.close();
					if (worker != null)
					{
						ArrayList<Object> cmd2 = new ArrayList<Object>(1);
						cmd2.add("CLOSE");
						worker.in.put(cmd2);
					}
					this.terminate();
					return;
				}
				final String command = new String(cmd, "UTF-8");
				//HRDBMSWorker.logger.debug("Received " + num + " bytes");
				HRDBMSWorker.logger.debug("Command: " + command);

				final byte[] fromBytes = new byte[4];
				final byte[] toBytes = new byte[4];
				num = in.read(fromBytes);
				if (num != 4)
				{
					HRDBMSWorker.logger.error("Received less than 4 bytes when reading from field in remote command.");
					HRDBMSWorker.logger.debug("Received " + num + " bytes");
					returnException("InvalidFromIDException");
					sock.close();
					if (worker != null)
					{
						ArrayList<Object> cmd2 = new ArrayList<Object>(1);
						cmd2.add("CLOSE");
						worker.in.put(cmd2);
					}
					this.terminate();
					return;
				}
				
				num = in.read(toBytes);
				if (num != 4)
				{
					HRDBMSWorker.logger.error("Received less than 4 bytes when reading to field in remote command.");
					returnException("InvalidToIDException");
					sock.close();
					if (worker != null)
					{
						ArrayList<Object> cmd2 = new ArrayList<Object>(1);
						cmd2.add("CLOSE");
						worker.in.put(cmd2);
					}
					this.terminate();
					return;
				}
				
				final int from = bytesToInt(fromBytes);
				final int to = bytesToInt(toBytes);

				if (command.equals("GET     "))
				{
					while (!XAManager.rP2.get())
					{
						Thread.sleep(1000);
					}
					get();
				}
				else if (command.equals("REMOVE  "))
				{
					while (!XAManager.rP2.get())
					{
						Thread.sleep(1000);
					}
					remove();
				}
				else if (command.equals("REMOVE2 "))
				{
					while (!XAManager.rP2.get())
					{
						Thread.sleep(1000);
					}
					remove2();
				}
				else if (command.equals("PUT     "))
				{
					while (!XAManager.rP2.get())
					{
						Thread.sleep(1000);
					}
					put();
				}
				else if (command.equals("PUT2    "))
				{
					while (!XAManager.rP2.get())
					{
						Thread.sleep(1000);
					}
					put2();
				}
				else if (command.equals("RGETDATD"))
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
					while (!XAManager.rP2.get())
					{
						Thread.sleep(1000);
					}
					op.start();
					try
					{
						op.close();
					}
					catch(Exception e)
					{}
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
					while (!XAManager.rP2.get())
					{
						Thread.sleep(1000);
					}
					synchronized (send)
					{
						if (send.notStarted() && send.hasAllConnections())
						{
							send.start();
							try
							{
								send.close();
							}
							catch(Exception e)
							{}
							sends.remove(id);
						}
					}
				}
				else if (command.equals("CLOSE   "))
				{
					closeConnection();
					if (worker != null)
					{
						ArrayList<Object> cmd2 = new ArrayList<Object>(1);
						cmd2.add("CLOSE");
						worker.in.put(cmd2);
					}
					HRDBMSWorker.logger.debug("Received request to terminate connection");
					this.terminate();
					return;
				}
				else if (command.equals("CLIENT  "))
				{
					while (!XAManager.rP2.get())
					{
						Thread.sleep(1000);
					}
					clientConnection();
				}
				else if (command.equals("CLIENT2 "))
				{
					while (!XAManager.rP2.get())
					{
						Thread.sleep(1000);
					}
					clientConnection2();
				}
				else if (command.equals("CAPACITY"))
				{
					capacity();
				}
				else if (command.equals("COMMIT  "))
				{
					while (!XAManager.rP2.get())
					{
						Thread.sleep(1000);
					}
					doCommit();
				}
				else if (command.equals("CHECKPNT"))
				{
					while (!XAManager.rP2.get())
					{
						Thread.sleep(1000);
					}
					checkpoint();
				}
				else if (command.equals("ROLLBACK"))
				{
					while (!XAManager.rP2.get())
					{
						Thread.sleep(1000);
					}
					doRollback();
				}
				else if (command.equals("ISOLATIO"))
				{
					while (!XAManager.rP2.get())
					{
						Thread.sleep(1000);
					}
					doIsolation();
				}
				else if (command.equals("TEST    "))
				{
					testConnection();
				}
				else if (command.equals("SETSCHMA"))
				{
					while (!XAManager.rP2.get())
					{
						Thread.sleep(1000);
					}
					setSchema();
				}
				else if (command.equals("GETSCHMA"))
				{
					while (!XAManager.rP2.get())
					{
						Thread.sleep(1000);
					}
					getSchema();
				}
				else if (command.equals("EXECUTEQ"))
				{
					while (!XAManager.rP2.get())
					{
						Thread.sleep(1000);
					}
					executeQuery();
				}
				else if (command.equals("RSMETA  "))
				{
					while (!XAManager.rP2.get())
					{
						Thread.sleep(1000);
					}
					getRSMeta();
				}
				else if (command.equals("NEXT    "))
				{
					while (!XAManager.rP2.get())
					{
						Thread.sleep(1000);
					}
					doNext();
				}
				else if (command.equals("CLOSERS "))
				{
					while (!XAManager.rP2.get())
					{
						Thread.sleep(1000);
					}
					closeRS();
				}
				else if (command.equals("LROLLBCK"))
				{
					localRollback();
				}
				else if (command.equals("PREPARE "))
				{
					while (!XAManager.rP2.get())
					{
						Thread.sleep(1000);
					}
					prepare();
				}
				else if (command.equals("LCOMMIT "))
				{
					localCommit();
				}
				else if (command.equals("CHECKTX "))
				{
					checkTx();
				}
				else if (command.equals("MDELETE "))
				{
					while (!XAManager.rP2.get())
					{
						Thread.sleep(1000);
					}
					massDelete();
				}
				else if (command.equals("DELETE  "))
				{
					while (!XAManager.rP2.get())
					{
						Thread.sleep(1000);
					}
					delete();
				}
				else if (command.equals("UPDATE  "))
				{
					while (!XAManager.rP2.get())
					{
						Thread.sleep(1000);
					}
					update();
				}
				else if (command.equals("INSERT  "))
				{
					while (!XAManager.rP2.get())
					{
						Thread.sleep(1000);
					}
					insert();
				}
				else if (command.equals("LOAD    "))
				{
					while (!XAManager.rP2.get())
					{
						Thread.sleep(1000);
					}
					load();
				}
				else if (command.equals("EXECUTEU"))
				{
					while (!XAManager.rP2.get())
					{
						Thread.sleep(1000);
					}
					executeUpdate();
				}
				else if (command.equals("NEWTABLE"))
				{
					while (!XAManager.rP2.get())
					{
						Thread.sleep(1000);
					}
					newTable();
				}
				else if (command.equals("NEWINDEX"))
				{
					while (!XAManager.rP2.get())
					{
						Thread.sleep(1000);
					}
					newIndex();
				}
				else if (command.equals("POPINDEX"))
				{
					while (!XAManager.rP2.get())
					{
						Thread.sleep(1000);
					}
					popIndex();
				}
				else if (command.equals("GETLDMD "))
				{
					while (!XAManager.rP2.get())
					{
						Thread.sleep(1000);
					}
					getLoadMetaData();
				}
				else if (command.equals("HADOOPLD"))
				{
					while (!XAManager.rP2.get())
					{
						Thread.sleep(1000);
					}
					hadoopLoad();
				}
				else if (command.equals("SETLDMD "))
				{
					while (!XAManager.rP2.get())
					{
						Thread.sleep(1000);
					}
					setLoadMetaData();
				}
				else if (command.equals("DELLDMD "))
				{
					while (!XAManager.rP2.get())
					{
						Thread.sleep(1000);
					}
					delLoadMetaData();
				}
				else if (command.equals("DELFITBL"))
				{
					delFileTable();
				}
				else if (command.equals("DELFIIDX"))
				{
					delFileIndex();
				}
				else if (command.equals("REORG   "))
				{
					while (!XAManager.rP2.get())
					{
						Thread.sleep(1000);
					}
					reorg();
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
				try
				{
					sock.close();
				}
				catch(Exception g)
				{}
			}
			HRDBMSWorker.logger.debug("Terminating connection due to exception", e);
			if (worker != null)
			{
				ArrayList<Object> cmd2 = new ArrayList<Object>(1);
				cmd2.add("CLOSE");
				while(true)
				{
					try
					{
						worker.in.put(cmd2);
						break;
					}
					catch(Exception f)
					{}
				}
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
		//Exception f = new Exception();
		//HRDBMSWorker.logger.debug("Entered closeConnection(), trace =", f);
		clientConnection = false;
		if (tx != null)
		{
			try
			{
				XAManager.rollback(tx);
				tx = null;
			}
			catch(Exception e)
			{
				HRDBMSWorker.logger.error("", e);
			}
		}
		try
		{
			sock.close();
			if (worker != null)
			{
				ArrayList<Object> cmd2 = new ArrayList<Object>(1);
				cmd2.add("CLOSE");
				worker.in.put(cmd2);
			}
			this.terminate();
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
		if (!LogManager.recoverDone.get())
		{
			try
			{
				sock.close();
			}
			catch(Exception e)
			{}
			return;
		}
		try
		{ 
			@SuppressWarnings("restriction")
			OperatingSystemMXBean osBean = ManagementFactory.getPlatformMXBean(OperatingSystemMXBean.class);
			@SuppressWarnings("restriction")
			int me = (int)(100 * osBean.getSystemCpuLoad());
			
			HashMap<Integer, String> coords = MetaData.getCoordMap();
			
			boolean imLow = true;
			String lowHost = null;;
			int low = 0;
			for (int node : coords.keySet())
			{
				try
				{
					String hostname = coords.get(node);
					CompressedSocket sock2 = CompressedSocket.newCompressedSocket(hostname, Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
					sock2.setSoTimeout(5000 / coords.size());
					OutputStream out = sock2.getOutputStream();
					byte[] outMsg = "CAPACITY        ".getBytes("UTF-8");
					outMsg[8] = 0;
					outMsg[9] = 0;
					outMsg[10] = 0;
					outMsg[11] = 0;
					outMsg[12] = 0;
					outMsg[13] = 0;
					outMsg[14] = 0;
					outMsg[15] = 0;
					out.write(outMsg);
					out.flush();
					byte[] data = new byte[4];
					sock2.getInputStream().read(data);
					int val = bytesToInt(data);
					if (imLow)
					{
						if (val < me)
						{
							imLow = false;
							low = val;
							lowHost = hostname;
						}
					}
					else
					{
						if (val < low)
						{
							low = val;
							lowHost = hostname;
						}
					}
					sock2.close();
				}
				catch(Exception e)
				{
					HRDBMSWorker.logger.debug("", e);
				}
			}
			
			if (imLow)
			{
				sock.getOutputStream().write("OK".getBytes("UTF-8"));
				sock.getOutputStream().flush();
				clientConnection = true;
			}
			else
			{
				sock.getOutputStream().write("RD".getBytes("UTF-8"));
				byte[] hostData = lowHost.getBytes("UTF-8");
				HRDBMSWorker.logger.debug("Redirecting to " + lowHost);
				sock.getOutputStream().write(intToBytes(hostData.length));
				sock.getOutputStream().write(hostData);
				sock.getOutputStream().flush();
				sock.close();
			}
		}
		catch(Exception e)
		{
			HRDBMSWorker.logger.debug("Terminating connection due to exception", e);
			try
			{
				sock.close();
				if (worker != null)
				{
					ArrayList<Object> cmd2 = new ArrayList<Object>(1);
					cmd2.add("CLOSE");
					worker.in.put(cmd2);
				}
				this.terminate();
			}
			catch(Exception f)
			{}
			return;
		}
	}
	
	public void clientConnection2()
	{
		if (!LogManager.recoverDone.get())
		{
			try
			{
				sock.close();
			}
			catch(Exception e)
			{}
			return;
		}
		try
		{ 
			sock.getOutputStream().write("OK".getBytes("UTF-8"));
			sock.getOutputStream().flush();
			clientConnection = true;
		}
		catch(Exception e)
		{
			HRDBMSWorker.logger.debug("Terminating connection due to exception", e);
			try
			{
				sock.close();
				if (worker != null)
				{
					ArrayList<Object> cmd2 = new ArrayList<Object>(1);
					cmd2.add("CLOSE");
					worker.in.put(cmd2);
				}
				this.terminate();
			}
			catch(Exception f)
			{}
			return;
		}
	}
	
	public void capacity()
	{
		try
		{
			@SuppressWarnings("restriction")
			OperatingSystemMXBean osBean = ManagementFactory.getPlatformMXBean(OperatingSystemMXBean.class);
			@SuppressWarnings("restriction")
			int me = (int)(100 * osBean.getSystemCpuLoad());
			sock.getOutputStream().write(intToBytes(me));
			sock.getOutputStream().flush();
			sock.close();
		}
		catch(Exception e)
		{}
	}
	
	public void doCommit()
	{
		if (tx == null)
		{
			sendOK();
		}
		
		try
		{
			XAManager.commit(tx);
			tx = null;
		}
		catch(Exception e)
		{
			HRDBMSWorker.logger.debug("", e);
			sendNo();
		}
		
		sendOK();
	}
	
	public void doRollback()
	{
		if (tx == null)
		{
			sendOK();
		}
		
		try
		{
			XAManager.rollback(tx);
			tx = null;
		}
		catch(Exception e)
		{
			HRDBMSWorker.logger.debug("", e);
			sendNo();
		}
		
		sendOK();
	}
	
	private void sendOK()
	{
		try
		{
			sock.getOutputStream().write("OK".getBytes("UTF-8"));
			sock.getOutputStream().flush();
		}
		catch(Exception e)
		{
			HRDBMSWorker.logger.debug("Terminating connection due to exception", e);
			if (tx != null)
			{
				try
				{
					XAManager.rollback(tx);
					tx = null;
				}
				catch(Exception f)
				{
					HRDBMSWorker.logger.error("", f);
				}
			}
			
			clientConnection = false;
			try
			{
				sock.close();
				if (worker != null)
				{
					ArrayList<Object> cmd2 = new ArrayList<Object>(1);
					cmd2.add("CLOSE");
					worker.in.put(cmd2);
				}
				this.terminate();
			}
			catch(Exception f)
			{
				if (worker != null)
				{
					ArrayList<Object> cmd2 = new ArrayList<Object>(1);
					cmd2.add("CLOSE");
					while (true)
					{
						try
						{
							worker.in.put(cmd2);
							break;
						}
						catch(Exception g)
						{}
					}
				}
				this.terminate();
			}
		}
	}
	
	private void sendInt(int val)
	{
		try
		{
			byte[] data = intToBytes(val);
			sock.getOutputStream().write(data);
			sock.getOutputStream().flush();
		}
		catch(Exception e)
		{
			HRDBMSWorker.logger.debug("Terminating connection due to exception", e);
			if (tx != null)
			{
				try
				{
					XAManager.rollback(tx);
					tx = null;
				}
				catch(Exception f)
				{
					HRDBMSWorker.logger.error("", f);
				}
			}
			
			clientConnection = false;
			try
			{
				sock.close();
				if (worker != null)
				{
					ArrayList<Object> cmd2 = new ArrayList<Object>(1);
					cmd2.add("CLOSE");
					worker.in.put(cmd2);
				}
				this.terminate();
			}
			catch(Exception f)
			{
				if (worker != null)
				{
					ArrayList<Object> cmd2 = new ArrayList<Object>(1);
					cmd2.add("CLOSE");
					while (true)
					{
						try
						{
							worker.in.put(cmd2);
							break;
						}
						catch(Exception g)
						{}
					}
				}
				this.terminate();
			}
		}
	}
	
	private void sendNo()
	{
		try
		{
			sock.getOutputStream().write("NO".getBytes("UTF-8"));
			sock.getOutputStream().flush();
		}
		catch(Exception e)
		{
			HRDBMSWorker.logger.debug("Terminating connection due to exception", e);
			if (tx != null)
			{
				try
				{
					XAManager.rollback(tx);
					tx = null;
				}
				catch(Exception f)
				{
					HRDBMSWorker.logger.error("", f);
				}
			}
			
			clientConnection = false;
			try
			{
				sock.close();
				if (worker != null)
				{
					ArrayList<Object> cmd2 = new ArrayList<Object>(1);
					cmd2.add("CLOSE");
					worker.in.put(cmd2);
				}
				this.terminate();
			}
			catch(Exception f)
			{
				if (worker != null)
				{
					ArrayList<Object> cmd2 = new ArrayList<Object>(1);
					cmd2.add("CLOSE");
					while (true)
					{
						try
						{
							worker.in.put(cmd2);
							break;
						}
						catch(Exception g)
						{}
					}
				}
				this.terminate();
			}
		}
	}
	
	private void doIsolation()
	{
		byte[] arg = new byte[4];
		int count = 0;
		while (count < 4)
		{
			try
			{
				int temp = sock.getInputStream().read(arg, count, 4 - count);
				if (temp == -1)
				{
					HRDBMSWorker.logger.debug("End of input stream when expecting isolation level argument");
					if (tx != null)
					{
						XAManager.rollback(tx);
						tx = null;
					}
					
					clientConnection = false;
					try
					{
						sock.close();
						if (worker != null)
						{
							ArrayList<Object> cmd2 = new ArrayList<Object>(1);
							cmd2.add("CLOSE");
							worker.in.put(cmd2);
						}
						this.terminate();
					}
					catch(Exception f)
					{
						if (worker != null)
						{
							ArrayList<Object> cmd2 = new ArrayList<Object>(1);
							cmd2.add("CLOSE");
							worker.in.put(cmd2);
						}
						this.terminate();
					}
				}
				else
				{
					count += temp;
				}
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.debug("Terminating connection due to exception", e);
				if (tx != null)
				{
					try
					{
						XAManager.rollback(tx);
						tx = null;
					}
					catch(Exception f)
					{
						HRDBMSWorker.logger.error("", f);
					}
				}
				
				clientConnection = false;
				try
				{
					sock.close();
					if (worker != null)
					{
						ArrayList<Object> cmd2 = new ArrayList<Object>(1);
						cmd2.add("CLOSE");
						worker.in.put(cmd2);
					}
					this.terminate();
				}
				catch(Exception f)
				{
					if (worker != null)
					{
						ArrayList<Object> cmd2 = new ArrayList<Object>(1);
						cmd2.add("CLOSE");
						while (true)
						{
							try
							{
								worker.in.put(cmd2);
								break;
							}
							catch(Exception g)
							{}
						}
					}
					this.terminate();
				}
			}
		}
		
		int iso = this.bytesToInt(arg);
		if (tx == null)
		{
			tx = new Transaction(Transaction.ISOLATION_CS);
		}
		
		if (iso == Connection.TRANSACTION_NONE)
		{
			iso = Transaction.ISOLATION_UR;
		}
		else if (iso == Connection.TRANSACTION_REPEATABLE_READ)
		{
			iso = Transaction.ISOLATION_RR;
		}
		else if (iso == Connection.TRANSACTION_SERIALIZABLE)
		{
			iso = Transaction.ISOLATION_RR;
		}
		else
		{
			iso = Transaction.ISOLATION_CS;
		}
		tx.setIsolationLevel(iso);
	}
	
	private void testConnection()
	{
		sendOK();
	}
	
	private void setSchema()
	{
		byte[] arg = new byte[4];
		int count = 0;
		while (count < 4)
		{
			try
			{
				int temp = sock.getInputStream().read(arg, count, 4 - count);
				if (temp == -1)
				{
					HRDBMSWorker.logger.debug("End of input when expecting schema");
					if (tx != null)
					{
						XAManager.rollback(tx);
						tx = null;
					}
					
					clientConnection = false;
					try
					{
						sock.close();
						if (worker != null)
						{
							ArrayList<Object> cmd2 = new ArrayList<Object>(1);
							cmd2.add("CLOSE");
							worker.in.put(cmd2);
						}
						this.terminate();
					}
					catch(Exception f)
					{
						if (worker != null)
						{
							ArrayList<Object> cmd2 = new ArrayList<Object>(1);
							cmd2.add("CLOSE");
							worker.in.put(cmd2);
						}
						this.terminate();
					}
				}
				else
				{
					count += temp;
				}
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.debug("Terminating connection due to exception", e);
				if (tx != null)
				{
					try
					{
						XAManager.rollback(tx);
						tx = null;
					}
					catch(Exception f)
					{
						HRDBMSWorker.logger.error("", f);
					}
				}
				
				clientConnection = false;
				try
				{
					sock.close();
					if (worker != null)
					{
						ArrayList<Object> cmd2 = new ArrayList<Object>(1);
						cmd2.add("CLOSE");
						worker.in.put(cmd2);
					}
					this.terminate();
				}
				catch(Exception f)
				{
					if (worker != null)
					{
						ArrayList<Object> cmd2 = new ArrayList<Object>(1);
						cmd2.add("CLOSE");
						while (true)
						{
							try
							{
								worker.in.put(cmd2);
								break;
							}
							catch(Exception g)
							{}
						}
					}
					this.terminate();
				}
			}
		}
		
		int length = this.bytesToInt(arg);
		//read schema string
		arg = new byte[length];
		read(arg);
		try
		{
			String schema = new String(arg, "UTF-8");
			MetaData.setDefaultSchema(this, schema);
		}
		catch(Exception e)
		{}
		
		
	}
	
	protected void terminate()
	{
		MetaData.removeDefaultSchema(this);
		super.terminate();
	}
	
	private void read(byte[] arg)
	{
		int count = 0;
		while (count < arg.length)
		{
			try
			{
				int temp = sock.getInputStream().read(arg, count, arg.length - count);
				if (temp == -1)
				{
					HRDBMSWorker.logger.debug("Unexpected end of input");
					if (tx != null)
					{
						XAManager.rollback(tx);
						tx = null;
					}
					
					clientConnection = false;
					try
					{
						sock.close();
						if (worker != null)
						{
							ArrayList<Object> cmd2 = new ArrayList<Object>(1);
							cmd2.add("CLOSE");
							worker.in.put(cmd2);
						}
						this.terminate();
					}
					catch(Exception f)
					{
						if (worker != null)
						{
							ArrayList<Object> cmd2 = new ArrayList<Object>(1);
							cmd2.add("CLOSE");
							worker.in.put(cmd2);
						}
						this.terminate();
					}
				}
				else
				{
					count += temp;
				}
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.debug("Terminating connection due to exception", e);
				if (tx != null)
				{
					try
					{
						XAManager.rollback(tx);
						tx = null;
					}
					catch(Exception f)
					{
						HRDBMSWorker.logger.error("", f);
					}
				}
				
				clientConnection = false;
				try
				{
					sock.close();
					if (worker != null)
					{
						ArrayList<Object> cmd2 = new ArrayList<Object>(1);
						cmd2.add("CLOSE");
						worker.in.put(cmd2);
					}
					this.terminate();
				}
				catch(Exception f)
				{
					if (worker != null)
					{
						ArrayList<Object> cmd2 = new ArrayList<Object>(1);
						cmd2.add("CLOSE");
						while (true)
						{
							try
							{
								worker.in.put(cmd2);
								break;
							}
							catch(Exception g)
							{}
						}
					}
					this.terminate();
				}
			}
		}
	}
	
	private void getSchema()
	{
		String schema = new MetaData(this).getCurrentSchema();
		sendString(schema);
	}
	
	private void sendString(String string)
	{
		try
		{
			byte[] bytes = string.getBytes("UTF-8");
			sock.getOutputStream().write(intToBytes(bytes.length));
			sock.getOutputStream().write(bytes);
			sock.getOutputStream().flush();
		}
		catch(Exception e)
		{
			HRDBMSWorker.logger.debug("Terminating connection due to exception", e);
			if (tx != null)
			{
				try
				{
					XAManager.rollback(tx);
					tx = null;
				}
				catch(Exception f)
				{
					HRDBMSWorker.logger.error("", f);
				}
			}
			
			clientConnection = false;
			try
			{
				sock.close();
				if (worker != null)
				{
					ArrayList<Object> cmd2 = new ArrayList<Object>(1);
					cmd2.add("CLOSE");
					worker.in.put(cmd2);
				}
				this.terminate();
			}
			catch(Exception f)
			{
				if (worker != null)
				{
					ArrayList<Object> cmd2 = new ArrayList<Object>(1);
					cmd2.add("CLOSE");
					while (true)
					{
						try
						{
							worker.in.put(cmd2);
							break;
						}
						catch(Exception g)
						{}
					}
				}
				this.terminate();
			}
		}
	}
	
	private void executeQuery()
	{
		HRDBMSWorker.logger.debug("Entered executeQuery()");
		byte[] bLen = new byte[4];
		read(bLen);
		HRDBMSWorker.logger.debug("Read SQL length");
		int len = bytesToInt(bLen);
		HRDBMSWorker.logger.debug("Length is " + len);
		byte[] bStmt = new byte[len];
		read(bStmt);
		HRDBMSWorker.logger.debug("Read sql");
		String sql = null;
		try
		{
			sql = new String(bStmt, "UTF-8");
			HRDBMSWorker.logger.debug("SQL is " + sql);
		}
		catch(Exception e)
		{}
		
		if (tx == null)
		{
			tx = new Transaction(Transaction.ISOLATION_CS);
		}
		
		try
		{
			if (worker != null)
			{
				ArrayList<Object> cmd2 = new ArrayList<Object>(1);
				cmd2.add("CLOSE");
				while (true)
				{
					try
					{
						worker.in.put(cmd2);
						break;
					}
					catch(Exception g)
					{}
				}
			}
			
			HRDBMSWorker.logger.debug("About to create XAWorker");
			worker = XAManager.executeQuery(sql, tx, this);
			HRDBMSWorker.logger.debug("XAWorker created");
			HRDBMSWorker.addThread(worker);
			HRDBMSWorker.logger.debug("XAWorker started");
			this.sendOK();
			HRDBMSWorker.logger.debug("OK sent to client");
		}
		catch(Exception e)
		{
			HRDBMSWorker.logger.debug("Exception during executeQuery()", e);
			this.sendNo();
			returnExceptionToClient(e);
		}
	}
	
	private void returnExceptionToClient(Exception e)
	{
		byte[] text = null;
		try
		{
			text = e.getMessage().getBytes("UTF-8");
		}
		catch(Exception f)
		{}
		
		if (text == null)
		{
			try
			{
				text = e.getClass().toString().getBytes("UTF-8");
			}
			catch(Exception f)
			{}
		}
		
		byte[] textLen = intToBytes(text.length);
		try
		{
			sock.getOutputStream().write(textLen);
			sock.getOutputStream().write(text);
			sock.getOutputStream().flush();
		}
		catch(Exception f)
		{
			HRDBMSWorker.logger.debug("Terminating connection due to exception", e);
			if (tx != null)
			{
				try
				{
					XAManager.rollback(tx);
					tx = null;
				}
				catch(Exception g)
				{
					HRDBMSWorker.logger.error("", g);
				}
			}
			
			clientConnection = false;
			try
			{
				sock.close();
				if (worker != null)
				{
					ArrayList<Object> cmd2 = new ArrayList<Object>(1);
					cmd2.add("CLOSE");
					worker.in.put(cmd2);
				}
				this.terminate();
			}
			catch(Exception h)
			{
				if (worker != null)
				{
					ArrayList<Object> cmd2 = new ArrayList<Object>(1);
					cmd2.add("CLOSE");
					while (true)
					{
						try
						{
							worker.in.put(cmd2);
							break;
						}
						catch(Exception g)
						{}
					}
				}
				this.terminate();
			}
		}
	}
	
	private void getRSMeta()
	{
		try
		{
			if (worker == null)
			{
				HRDBMSWorker.logger.debug("ResultSetMetaData was requested when no result set existed");
				closeConnection();
				this.terminate();
				return;
			}
		
			ArrayList<Object> cmd2 = new ArrayList<Object>(1);
			cmd2.add("META");
			while (true)
			{
				try
				{
					worker.in.put(cmd2);
					break;
				}
				catch(Exception g)
				{}
			}
		
			ArrayList<Object> buffer = new ArrayList<Object>();
			Object obj = worker.out.take();
			if (!(obj instanceof HashMap))
			{
				buffer.add(obj);
				obj = worker.out.take();
				while (!(obj instanceof HashMap))
				{
					buffer.add(obj);
					obj = worker.out.take();
				}
			}
		
			ObjectOutputStream objOut = new ObjectOutputStream(sock.getOutputStream());
			objOut.writeObject(obj);
			obj = worker.out.take();
			objOut.writeObject(obj);
			obj = worker.out.take();
			objOut.writeObject(obj);
			objOut.flush();
		
			obj = worker.out.peek();
			if (obj != null)
			{
				cmd2 = new ArrayList<Object>(1);
				cmd2.add("CLOSE");
				while (true)
				{
					try
					{
						worker.in.put(cmd2);
						worker = null;
						break;
					}
					catch(Exception g)
					{}
				}
				
				return;
			}
		
			if (buffer.size() > 0)
			{
				for (Object o : buffer)
				{
					worker.out.put(o);
				}	
			}
		}
		catch(Exception e)
		{
			ArrayList<Object> cmd2 = new ArrayList<Object>(1);
			cmd2.add("CLOSE");
			while (true)
			{
				try
				{
					worker.in.put(cmd2);
					worker = null;
					break;
				}
				catch(Exception g)
				{}
			}
		}
	}
	
	private void closeRS()
	{
		if (worker == null)
		{
			return;
		}
		
		ArrayList<Object> al = new ArrayList<Object>(1);
		al.add("CLOSE");
		while (true)
		{
			try
			{
				worker.in.put(al);
				worker = null;
				break;
			}
			catch(InterruptedException e)
			{}
		}
	}
	
	private void doNext()
	{
		ArrayList<Object> al = new ArrayList<Object>();
		al.add("NEXT");
		
		byte[] arg = new byte[4];
		int count = 0;
		while (count < 4)
		{
			try
			{
				int temp = sock.getInputStream().read(arg, count, 4 - count);
				if (temp == -1)
				{
					HRDBMSWorker.logger.debug("Unexpected end of input when expecting argument to NEXT command");
					if (tx != null)
					{
						XAManager.rollback(tx);
						tx = null;
					}
					
					clientConnection = false;
					try
					{
						sock.close();
						if (worker != null)
						{
							ArrayList<Object> cmd2 = new ArrayList<Object>(1);
							cmd2.add("CLOSE");
							worker.in.put(cmd2);
						}
						this.terminate();
					}
					catch(Exception f)
					{
						if (worker != null)
						{
							ArrayList<Object> cmd2 = new ArrayList<Object>(1);
							cmd2.add("CLOSE");
							worker.in.put(cmd2);
						}
						this.terminate();
					}
				}
				else
				{
					count += temp;
				}
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.debug("Terminating connection due to exception", e);
				if (tx != null)
				{
					try
					{
						XAManager.rollback(tx);
						tx = null;
					}
					catch(Exception f)
					{
						HRDBMSWorker.logger.error("", f);
					}
				}
				
				clientConnection = false;
				try
				{
					sock.close();
					if (worker != null)
					{
						ArrayList<Object> cmd2 = new ArrayList<Object>(1);
						cmd2.add("CLOSE");
						worker.in.put(cmd2);
					}
					this.terminate();
				}
				catch(Exception f)
				{
					if (worker != null)
					{
						ArrayList<Object> cmd2 = new ArrayList<Object>(1);
						cmd2.add("CLOSE");
						while (true)
						{
							try
							{
								worker.in.put(cmd2);
								break;
							}
							catch(Exception g)
							{}
						}
					}
					this.terminate();
				}
			}
		}
		
		int num = this.bytesToInt(arg);
		HRDBMSWorker.logger.debug("NEXT command requested " + num + " rows");
		al.add(num);
		
		while (true)
		{
			try
			{
				worker.in.put(al);
				break;
			}
			catch(InterruptedException e)
			{}
		}
		
		ArrayList<Object> buffer = new ArrayList<Object>(num);
		while (num > 0)
		{
			Object obj = null;
			while (true)
			{
				try
				{
					obj = worker.out.take();
					break;
				}
				catch(InterruptedException e)
				{}
			}
			
			buffer.add(obj);
			if (obj instanceof Exception || obj instanceof DataEndMarker)
			{
				break;
			}
			
			num--;
		}
		
		
		Object obj = buffer.get(buffer.size() - 1);
		if (obj instanceof Exception)
		{
			sendNo();
			returnExceptionToClient((Exception)obj);
			return;
		}
		
		sendOK();
		try
		{
			for (Object obj2 : buffer)
			{
				byte[] data = toBytes(obj2);
				sock.getOutputStream().write(data);
			}
		
			sock.getOutputStream().flush();
		}
		catch(Exception e)
		{
			HRDBMSWorker.logger.debug("Terminating connection due to exception", e);
			if (tx != null)
			{
				try
				{
					XAManager.rollback(tx);
					tx = null;
				}
				catch(Exception f)
				{
					HRDBMSWorker.logger.error("", f);
				}
			}
			
			clientConnection = false;
			try
			{
				sock.close();
				if (worker != null)
				{
					ArrayList<Object> cmd2 = new ArrayList<Object>(1);
					cmd2.add("CLOSE");
					worker.in.put(cmd2);
				}
				this.terminate();
			}
			catch(Exception f)
			{
				if (worker != null)
				{
					ArrayList<Object> cmd2 = new ArrayList<Object>(1);
					cmd2.add("CLOSE");
					while (true)
					{
						try
						{
							worker.in.put(cmd2);
							break;
						}
						catch(Exception g)
						{}
					}
				}
				this.terminate();
			}
		}
	}
	
	private final byte[] toBytes(Object v) throws Exception
	{
		ArrayList<Object> val;
		if (v instanceof ArrayList)
		{
			val = (ArrayList<Object>)v;
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
			else if (o instanceof AtomicLong)
			{
				header[i] = (byte)6;
				size += 8;
			}
			else if (o instanceof AtomicDouble)
			{
				header[i] = (byte)7;
				size += 8;
			}
			else if (o instanceof ArrayList)
			{
				if (((ArrayList)o).size() != 0)
				{
					Exception e = new Exception();
					HRDBMSWorker.logger.error("Non-zero size ArrayList in toBytes()", e);
					throw new Exception("Non-zero size ArrayList in toBytes()");
				}
				header[i] = (byte)8;
			}
			else if (o == null)
			{
				header[i] = (byte)9;
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
			else if (retval[i] == 6)
			{
				retvalBB.putLong(((AtomicLong)o).get());
			}
			else if (retval[i] == 7)
			{
				retvalBB.putDouble(((AtomicDouble)o).get());
			}
			else if (retval[i] == 8)
			{
			}
			else if (retval[i] == 9)
			{
			}

			i++;
		}

		return retval;
	}
	
	private void readNonCoord(byte[] arg) throws Exception
	{
		int count = 0;
		while (count < arg.length)
		{
			int temp = sock.getInputStream().read(arg, count, arg.length - count);
			if (temp == -1)
			{
				throw new Exception("Hit end of stream when reading from socket");
			}
			else
			{
				count += temp;
			}
		}
	}
	
	private long bytesToLong(byte[] val)
	{
		final long ret = java.nio.ByteBuffer.wrap(val).getLong();
		return ret;
	}
	
	private void removeFromTree(String host, ArrayList<Object> tree, ArrayList<Object> parent)
	{
		for (Object o : tree)
		{
			if (o instanceof String)
			{
				if (((String)o).equals(host))
				{
					tree.remove(host);
					if (tree.size() == 0 && parent != null)
					{
						parent.remove(tree);
					}
					return;
				}
			}
			else
			{
				removeFromTree(host, (ArrayList<Object>)o, tree);
			}
		}
	}
	
	private void localRollback()
	{
		ArrayList<Object> tree = null;
		byte[] txBytes = new byte[8];
		long txNum = -1;
		try
		{
			readNonCoord(txBytes);
			txNum = bytesToLong(txBytes);
			ObjectInputStream objIn = new ObjectInputStream(sock.getInputStream());
			tree = (ArrayList<Object>)objIn.readObject();
		}
		catch(Exception e)
		{
			sendNo();
			return;
		}
		
		sendOK();
		Transaction tx = new Transaction(txNum);
		try
		{
			tx.rollback();
		}
		catch(Exception e)
		{
			//TODO queueCommandSelf("ROLLBACK", tx);
			//TODO blackListSelf();
			HRDBMSWorker.logger.fatal("BLACKLIST", e);
			System.exit(1);
		}
		
		Object obj = tree.get(0);
		while (obj instanceof ArrayList)
		{
			obj = ((ArrayList)obj).get(0);
		}
		
		removeFromTree((String)obj, tree, null); //also delete parents if now empty
		
		for (Object o : tree)
		{
			new SendRollbackThread(o, tx).start();
		}
	}
	
	private static class SendRollbackThread extends HRDBMSThread
	{
		private Object o;
		private Transaction tx;
		
		public SendRollbackThread(Object o, Transaction tx)
		{
			this.o = o;
			this.tx = tx;
		}
		
		public void run()
		{
			if (o instanceof String)
			{
				Socket sock = null;
				try
				{
					sock = CompressedSocket.newCompressedSocket((String)o, Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
					OutputStream out = sock.getOutputStream();
					byte[] outMsg = "LROLLBCK        ".getBytes("UTF-8");
					outMsg[8] = 0;
					outMsg[9] = 0;
					outMsg[10] = 0;
					outMsg[11] = 0;
					outMsg[12] = 0;
					outMsg[13] = 0;
					outMsg[14] = 0;
					outMsg[15] = 0;
					out.write(outMsg);
					out.write(longToBytes(tx.number()));
					ObjectOutputStream objOut = new ObjectOutputStream(out);
					ArrayList<Object> alo = new ArrayList<Object>(1);
					alo.add(o);
					objOut.writeObject(alo);
					objOut.flush();
					out.flush();
					getConfirmation(sock);
					objOut.close();
					sock.close();
				}
				catch(Exception e)
				{
					//TODO blackListByHost((String)o);
					//TODO queueCommandByHost((String)o, "ROLLBACK", tx);
					HRDBMSWorker.logger.fatal("BLACKLIST", e);
					System.exit(1);
				}
			}
			else if (((ArrayList<Object>)o).size() > 0)
			{
				Socket sock = null;
				Object obj2 = ((ArrayList<Object>)o).get(0);
				while (obj2 instanceof ArrayList)
				{
					obj2 = ((ArrayList<Object>)obj2).get(0);
				}
				
				String hostname = (String)obj2;
				try
				{
					sock = CompressedSocket.newCompressedSocket(hostname, Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
					OutputStream out = sock.getOutputStream();
					byte[] outMsg = "LROLLBCK        ".getBytes("UTF-8");
					outMsg[8] = 0;
					outMsg[9] = 0;
					outMsg[10] = 0;
					outMsg[11] = 0;
					outMsg[12] = 0;
					outMsg[13] = 0;
					outMsg[14] = 0;
					outMsg[15] = 0;
					out.write(outMsg);
					out.write(longToBytes(tx.number()));
					ObjectOutputStream objOut = new ObjectOutputStream(out);
					objOut.writeObject((ArrayList<Object>)o);
					objOut.flush();
					out.flush();
					getConfirmation(sock);
					objOut.close();
					sock.close();
				}
				catch(Exception e)
				{
					try
					{
						sock.close();
					}
					catch(Exception f)
					{}
					//TODO blackListByHost((String)obj2);
					//TODO queueCommandByHost((String)obj2, "ROLLBACK", tx);
					HRDBMSWorker.logger.fatal("BLACKLIST", e);
					System.exit(1);
					//retry others
					boolean toDo = rebuildTree((ArrayList<Object>)o, (String)obj2);
					if (toDo)
					{
						sendRollback((ArrayList<Object>)o, tx);
					}
				}
			}
		}
	}
	
	private static byte[] longToBytes(long val)
	{
		final byte[] buff = new byte[8];
		buff[0] = (byte)(val >> 56);
		buff[1] = (byte)((val & 0x00FF000000000000L) >> 48);
		buff[2] = (byte)((val & 0x0000FF0000000000L) >> 40);
		buff[3] = (byte)((val & 0x000000FF00000000L) >> 32);
		buff[4] = (byte)((val & 0x00000000FF000000L) >> 24);
		buff[5] = (byte)((val & 0x0000000000FF0000L) >> 16);
		buff[6] = (byte)((val & 0x000000000000FF00L) >> 8);
		buff[7] = (byte)((val & 0x00000000000000FFL));
		return buff;
	}
	
	private static void getConfirmation(Socket sock) throws Exception
	{
		InputStream in = sock.getInputStream();
		byte[] inMsg = new byte[2];
		
		int count = 0;
		while (count < 2)
		{
			try
			{
				int temp = in.read(inMsg, count, 2 - count);
				if (temp == -1)
				{
					throw new Exception();
				}
				else
				{
					count += temp;
				}
			}
			catch (final Exception e)
			{
				throw e;
			}
		}
		
		String inStr = new String(inMsg, "UTF-8");
		if (!inStr.equals("OK"))
		{
			HRDBMSWorker.logger.debug("In getConfirmation(), received " + inStr);
			throw new Exception();
		}
	}
	
	private static boolean rebuildTree(ArrayList<Object> tree, String remove)
	{
		ArrayList<String> nodes = new ArrayList<String>();
		for (Object o : tree)
		{
			if (o instanceof String)
			{
				if (!((String)o).equals(remove))
				{
					nodes.add((String)o);
				}
			}
			else
			{
				//ArrayList<Object>
				nodes.addAll(getAllNodes((ArrayList<Object>)o, remove));
			}
		}
		
		if (nodes.size() > 0)
		{
			tree.clear();
			cloneInto(tree, makeTree(nodes));
			return true;
		}
		
		return false;
	}
	
	private static ArrayList<String> getAllNodes(ArrayList<Object> tree, String remove)
	{
		ArrayList<String> retval = new ArrayList<String>();
		for (Object o : tree)
		{
			if (o instanceof String)
			{
				if (!((String)o).equals(remove))
				{
					retval.add((String)o);
				}
			}
			else
			{
				//ArrayList<Object>
				retval.addAll(getAllNodes((ArrayList<Object>)o, remove));
			}
		}
		
		return retval;
	}
	
	private static ArrayList<Object> makeTree(ArrayList<String> nodes)
	{
		int max = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("max_neighbor_nodes"));
		if (nodes.size() <= max)
		{
			ArrayList<Object> retval = new ArrayList<Object>(nodes);
			return retval;
		}
		
		ArrayList<Object> retval = new ArrayList<Object>();
		int i = 0;
		while (i < max)
		{
			retval.add(nodes.get(i));
			i++;
		}
		
		int remaining = nodes.size() - i;
		int perNode = remaining / max + 1;
		
		int j = 0;
		while (i < nodes.size())
		{
			String first = (String)retval.get(j);
			retval.remove(j);
			ArrayList<String> list = new ArrayList<String>(perNode+1);
			list.add(first);
			int k = 0;
			while (k < perNode && i < nodes.size())
			{
				list.add(nodes.get(i));
				i++;
				k++;
			}
			
			retval.add(j, list);
			j++;
		}
		
		if (((ArrayList<String>)retval.get(0)).size() <= max)
		{
			return retval;
		}
		
		//more than 2 tier
		i = 0;
		while (i < retval.size())
		{
			ArrayList<String> list = (ArrayList<String>)retval.remove(i);
			retval.add(i, makeTree(list));
			i++;
		}
		
		return retval;
	}
	
	private static void cloneInto(ArrayList<Object> target, ArrayList<Object> source)
	{
		for (Object o : source)
		{
			target.add(o);
		}
	}
	
	private static void sendRollback(ArrayList<Object> tree, Transaction tx)
	{
		Object obj = tree.get(0);
		while (obj instanceof ArrayList)
		{
			obj = ((ArrayList)obj).get(0);
		}
		
		String hostname = (String)obj;
		Socket sock = null;
		try
		{
			sock = CompressedSocket.newCompressedSocket(hostname, Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
			OutputStream out = sock.getOutputStream();
			byte[] outMsg = "LROLLBCK        ".getBytes("UTF-8");
			outMsg[8] = 0;
			outMsg[9] = 0;
			outMsg[10] = 0;
			outMsg[11] = 0;
			outMsg[12] = 0;
			outMsg[13] = 0;
			outMsg[14] = 0;
			outMsg[15] = 0;
			out.write(outMsg);
			out.write(longToBytes(tx.number()));
			ObjectOutputStream objOut = new ObjectOutputStream(out);
			objOut.writeObject(tree);
			objOut.flush();
			out.flush();
			getConfirmation(sock);
			objOut.close();
			sock.close();
		}
		catch(Exception e)
		{
			try
			{
				sock.close();
			}
			catch(Exception f)
			{}
			//TODO blackListByHost((String)obj);
			//TODO queueCommandByHost((String)obj, "ROLLBACK", tx);
			HRDBMSWorker.logger.fatal("BLACKLIST", e);
			System.exit(1);
			boolean toDo = rebuildTree(tree, (String)obj);
			if (toDo)
			{
				sendRollback(tree, tx);
			}
		}
	}
	
	private void localCommit()
	{
		ArrayList<Object> tree = null;
		byte[] txBytes = new byte[8];
		long txNum = -1;
		try
		{
			readNonCoord(txBytes);
			txNum = bytesToLong(txBytes);
			ObjectInputStream objIn = new ObjectInputStream(sock.getInputStream());
			tree = (ArrayList<Object>)objIn.readObject();
		}
		catch(Exception e)
		{
			sendNo();
			return;
		}
		
		sendOK();
		Transaction tx = new Transaction(txNum);
		try
		{
			tx.commit();
		}
		catch(Exception e)
		{
			//TODO queueCommandSelf("COMMIT", tx);
			//TODO blackListSelf();
			HRDBMSWorker.logger.fatal("BLACKLIST", e);
			System.exit(1);
		}
		
		Object obj = tree.get(0);
		while (obj instanceof ArrayList)
		{
			obj = ((ArrayList)obj).get(0);
		}
		
		removeFromTree((String)obj, tree, null); //also delete parents if now empty
		
		for (Object o : tree)
		{
			new SendCommitThread(o, tx).start();
		}
	}
	
	private static class SendCommitThread extends HRDBMSThread
	{
		private Object o;
		private Transaction tx;
		
		public SendCommitThread(Object o, Transaction tx)
		{
			this.o = o;
			this.tx= tx;
		}
		
		public void run()
		{
			if (o instanceof String)
			{
				Socket sock = null;
				try
				{
					sock = CompressedSocket.newCompressedSocket((String)o, Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
					OutputStream out = sock.getOutputStream();
					byte[] outMsg = "LCOMMIT         ".getBytes("UTF-8");
					outMsg[8] = 0;
					outMsg[9] = 0;
					outMsg[10] = 0;
					outMsg[11] = 0;
					outMsg[12] = 0;
					outMsg[13] = 0;
					outMsg[14] = 0;
					outMsg[15] = 0;
					out.write(outMsg);
					out.write(longToBytes(tx.number()));
					ObjectOutputStream objOut = new ObjectOutputStream(out);
					ArrayList<Object> alo = new ArrayList<Object>(1);
					alo.add(o);
					objOut.writeObject(alo);
					objOut.flush();
					out.flush();
					getConfirmation(sock);
					objOut.close();
					sock.close();
				}
				catch(Exception e)
				{
					//TODO blackListByHost((String)o);
					//TODO queueCommandByHost((String)o, "COMMIT", tx);
					HRDBMSWorker.logger.fatal("BLACKLIST", e);
					System.exit(1);
				}
			}
			else if (((ArrayList<Object>)o).size() > 0)
			{
				Socket sock = null;
				Object obj2 = ((ArrayList<Object>)o).get(0);
				while (obj2 instanceof ArrayList)
				{
					obj2 = ((ArrayList<Object>)obj2).get(0);
				}
				
				String hostname = (String)obj2;
				try
				{
					sock = CompressedSocket.newCompressedSocket(hostname, Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
					OutputStream out = sock.getOutputStream();
					byte[] outMsg = "LCOMMIT         ".getBytes("UTF-8");
					outMsg[8] = 0;
					outMsg[9] = 0;
					outMsg[10] = 0;
					outMsg[11] = 0;
					outMsg[12] = 0;
					outMsg[13] = 0;
					outMsg[14] = 0;
					outMsg[15] = 0;
					out.write(outMsg);
					out.write(longToBytes(tx.number()));
					ObjectOutputStream objOut = new ObjectOutputStream(out);
					objOut.writeObject((ArrayList<Object>)o);
					objOut.flush();
					out.flush();
					getConfirmation(sock);
					objOut.close();
					sock.close();
				}
				catch(Exception e)
				{
					try
					{
						sock.close();
					}
					catch(Exception f)
					{}
					//TODO blackListByHost((String)obj2);
					//TODO queueCommandByHost((String)obj2, "COMMIT", tx);
					HRDBMSWorker.logger.fatal("BLACKLIST", e);
					System.exit(1);
					//retry others
					boolean toDo = rebuildTree((ArrayList<Object>)o, (String)obj2);
					if (toDo)
					{
						sendCommit((ArrayList<Object>)o, tx);
					}
				}
			}
		}
	}
	
	private static void sendCommit(ArrayList<Object> tree, Transaction tx)
	{
		Object obj = tree.get(0);
		while (obj instanceof ArrayList)
		{
			obj = ((ArrayList)obj).get(0);
		}
		
		String hostname = (String)obj;
		Socket sock = null;
		try
		{
			sock = CompressedSocket.newCompressedSocket(hostname, Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
			OutputStream out = sock.getOutputStream();
			byte[] outMsg = "LCOMMIT         ".getBytes("UTF-8");
			outMsg[8] = 0;
			outMsg[9] = 0;
			outMsg[10] = 0;
			outMsg[11] = 0;
			outMsg[12] = 0;
			outMsg[13] = 0;
			outMsg[14] = 0;
			outMsg[15] = 0;
			out.write(outMsg);
			out.write(longToBytes(tx.number()));
			ObjectOutputStream objOut = new ObjectOutputStream(out);
			objOut.writeObject(tree);
			objOut.flush();
			out.flush();
			getConfirmation(sock);
			objOut.close();
			sock.close();
		}
		catch(Exception e)
		{
			try
			{
				sock.close();
			}
			catch(Exception f)
			{}
			//TODO blackListByHost((String)obj);
			//TODO queueCommandByHost((String)obj, "COMMIT", tx);
			HRDBMSWorker.logger.fatal("BLACKLIST", e);
			System.exit(1);
			boolean toDo = rebuildTree(tree, (String)obj);
			if (toDo)
			{
				sendCommit(tree, tx);
			}
		}
	}
	
	private void prepare()
	{
		ArrayList<Object> tree = null;
		byte[] txBytes = new byte[8];
		long txNum = -1;
		byte[] lenBytes = new byte[4];
		int length = -1;
		String host = null;
		byte[] data = null;
		try
		{
			readNonCoord(txBytes);
			txNum = bytesToLong(txBytes);
			readNonCoord(lenBytes);
			length = bytesToInt(lenBytes);
			data = new byte[length];
			readNonCoord(data);
			host = new String(data, "UTF-8");
			ObjectInputStream objIn = new ObjectInputStream(sock.getInputStream());
			tree = (ArrayList<Object>)objIn.readObject();
		}
		catch(Exception e)
		{
			sendNo();
			return;
		}
		
		Transaction tx = new Transaction(txNum);
		try
		{
			tx.tryCommit(host);
		}
		catch(Exception e)
		{
			sendNo();
			return;
		}
		
		Object obj = tree.get(0);
		while (obj instanceof ArrayList)
		{
			obj = ((ArrayList)obj).get(0);
		}
		
		removeFromTree((String)obj, tree, null); //also delete parents if now empty
		
		ArrayList<SendPrepareThread> threads = new ArrayList<SendPrepareThread>();
		for (Object o : tree)
		{
			threads.add(new SendPrepareThread(o, tx, lenBytes, data));
		}
		
		for (SendPrepareThread thread : threads)
		{
			thread.start();
		}
		
		boolean allOK = true;
		for (SendPrepareThread thread : threads)
		{
			while (true)
			{
				try
				{
					thread.join();
					break;
				}
				catch(InterruptedException e)
				{}
			}
			if (!thread.sendOK())
			{
				allOK = false;
			}
		}
		
		if (allOK)
		{
			sendOK();
		}
		else
		{
			sendNo();
			try
			{
				sock.close();
			}
			catch(Exception f)
			{}
		}
	}
	
	private void checkpoint()
	{
		ArrayList<Object> tree = null;
		try
		{
			ObjectInputStream objIn = new ObjectInputStream(sock.getInputStream());
			tree = (ArrayList<Object>)objIn.readObject();
		}
		catch(Exception e)
		{
			sendNo();
			return;
		}
		
		HRDBMSWorker.checkpoint.doCheckpoint();
		
		Object obj = tree.get(0);
		while (obj instanceof ArrayList)
		{
			obj = ((ArrayList)obj).get(0);
		}
		
		removeFromTree((String)obj, tree, null); //also delete parents if now empty
		
		ArrayList<SendCheckpointThread> threads = new ArrayList<SendCheckpointThread>();
		for (Object o : tree)
		{
			threads.add(new SendCheckpointThread(o));
		}
		
		for (SendCheckpointThread thread : threads)
		{
			thread.start();
		}
		
		boolean allOK = true;
		for (SendCheckpointThread thread : threads)
		{
			while (true)
			{
				try
				{
					thread.join();
					break;
				}
				catch(InterruptedException e)
				{}
			}
			if (!thread.sendOK())
			{
				allOK = false;
			}
		}
		
		if (allOK)
		{
			sendOK();
		}
		else
		{
			sendNo();
			try
			{
				sock.close();
			}
			catch(Exception f)
			{}
		}
	}
	
	private static class SendPrepareThread extends HRDBMSThread
	{
		private Object o;
		private Transaction tx;
		private boolean sendOK;
		private byte[] lenBytes;
		private byte[] data;
		
		public SendPrepareThread(Object o, Transaction tx, byte[] lenBytes, byte[] data)
		{
			this.o = o;
			this.tx = tx;
			this.lenBytes = lenBytes;
			this.data = data;
		}
		
		public boolean sendOK()
		{
			return sendOK;
		}
		
		public void run()
		{
			if (o instanceof String)
			{
				Socket sock = null;
				try
				{
					sock = CompressedSocket.newCompressedSocket((String)o, Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
					OutputStream out = sock.getOutputStream();
					byte[] outMsg = "PREPARE         ".getBytes("UTF-8");
					outMsg[8] = 0;
					outMsg[9] = 0;
					outMsg[10] = 0;
					outMsg[11] = 0;
					outMsg[12] = 0;
					outMsg[13] = 0;
					outMsg[14] = 0;
					outMsg[15] = 0;
					out.write(outMsg);
					out.write(longToBytes(tx.number()));
					out.write(lenBytes);
					out.write(data);
					ObjectOutputStream objOut = new ObjectOutputStream(out);
					ArrayList<Object> alo = new ArrayList<Object>(1);
					alo.add(o);
					objOut.writeObject(alo);
					objOut.flush();
					out.flush();
					getConfirmation(sock);
					objOut.close();
					sock.close();
				}
				catch(Exception e)
				{
					sendOK = false;
					return;
				}
				sendOK = true;
			}
			else if (((ArrayList<Object>)o).size() > 0)
			{
				Socket sock = null;
				Object obj2 = ((ArrayList<Object>)o).get(0);
				while (obj2 instanceof ArrayList)
				{
					obj2 = ((ArrayList<Object>)obj2).get(0);
				}
				
				String hostname = (String)obj2;
				try
				{
					sock = CompressedSocket.newCompressedSocket(hostname, Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
					OutputStream out = sock.getOutputStream();
					byte[] outMsg = "PREPARE         ".getBytes("UTF-8");
					outMsg[8] = 0;
					outMsg[9] = 0;
					outMsg[10] = 0;
					outMsg[11] = 0;
					outMsg[12] = 0;
					outMsg[13] = 0;
					outMsg[14] = 0;
					outMsg[15] = 0;
					out.write(outMsg);
					out.write(longToBytes(tx.number()));
					out.write(lenBytes);
					out.write(data);
					ObjectOutputStream objOut = new ObjectOutputStream(out);
					objOut.writeObject((ArrayList<Object>)o);
					objOut.flush();
					out.flush();
					getConfirmation(sock);
					objOut.close();
					sock.close();
				}
				catch(Exception e)
				{
					sendOK = false;
					return;
				}
				
				sendOK = true;
			}
		}
	}
	
	private static class SendCheckpointThread extends HRDBMSThread
	{
		private Object o;
		private boolean sendOK;
		
		public SendCheckpointThread(Object o)
		{
			this.o = o;
		}
		
		public boolean sendOK()
		{
			return sendOK;
		}
		
		public void run()
		{
			if (o instanceof String)
			{
				Socket sock = null;
				try
				{
					sock = CompressedSocket.newCompressedSocket((String)o, Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
					OutputStream out = sock.getOutputStream();
					byte[] outMsg = "CHECKPNT        ".getBytes("UTF-8");
					outMsg[8] = 0;
					outMsg[9] = 0;
					outMsg[10] = 0;
					outMsg[11] = 0;
					outMsg[12] = 0;
					outMsg[13] = 0;
					outMsg[14] = 0;
					outMsg[15] = 0;
					out.write(outMsg);
					ObjectOutputStream objOut = new ObjectOutputStream(out);
					ArrayList<Object> alo = new ArrayList<Object>(1);
					alo.add(o);
					objOut.writeObject(alo);
					objOut.flush();
					out.flush();
					getConfirmation(sock);
					objOut.close();
					sock.close();
				}
				catch(Exception e)
				{
					sendOK = false;
					return;
				}
				sendOK = true;
			}
			else if (((ArrayList<Object>)o).size() > 0)
			{
				Socket sock = null;
				Object obj2 = ((ArrayList<Object>)o).get(0);
				while (obj2 instanceof ArrayList)
				{
					obj2 = ((ArrayList<Object>)obj2).get(0);
				}
				
				String hostname = (String)obj2;
				try
				{
					sock = CompressedSocket.newCompressedSocket(hostname, Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
					OutputStream out = sock.getOutputStream();
					byte[] outMsg = "PREPARE         ".getBytes("UTF-8");
					outMsg[8] = 0;
					outMsg[9] = 0;
					outMsg[10] = 0;
					outMsg[11] = 0;
					outMsg[12] = 0;
					outMsg[13] = 0;
					outMsg[14] = 0;
					outMsg[15] = 0;
					out.write(outMsg);
					ObjectOutputStream objOut = new ObjectOutputStream(out);
					objOut.writeObject((ArrayList<Object>)o);
					objOut.flush();
					out.flush();
					getConfirmation(sock);
					objOut.close();
					sock.close();
				}
				catch(Exception e)
				{
					sendOK = false;
					return;
				}
				
				sendOK = true;
			}
		}
	}
	
	private void checkTx()
	{
		byte[] data = new byte[8];
		try
		{
			readNonCoord(data);
		}
		catch(Exception e)
		{
			try
			{
				sock.close();
			}
			catch(Exception f)
			{
				try
				{
					sock.getOutputStream().close();
				}
				catch(Exception g)
				{}
			}
			return;
		}
		long txnum = bytesToLong(data); 
		String filename = HRDBMSWorker.getHParms().getProperty("log_dir");
		if (!filename.endsWith("/"))
		{
			filename += "/";
		}
		filename += "xa.log";
		Iterator<LogRec> iter = LogManager.iterator(filename, false);
		boolean sent = false;
		while (iter.hasNext())
		{
			LogRec rec = iter.next();
			if (rec.txnum() == txnum)
			{
				if (rec.type() == LogRec.XACOMMIT)
				{
					sendOK();
					sent = true;
					break;
				}
				else if (rec.type() == LogRec.XAABORT)
				{
					sendNo();
					sent = true;
					break;
				}
				else if (rec.type() == LogRec.PREPARE)
				{
					sendNo();
					sent = true;
					break;
				}
			}
		}
		
		((LogIterator)iter).close();
		
		if (!sent)
		{
			iter = LogManager.archiveIterator(filename, false);
			sent = false;
			while (iter.hasNext())
			{
				LogRec rec = iter.next();
				if (rec.txnum() == txnum)
				{
					if (rec.type() == LogRec.XACOMMIT)
					{
						sendOK();
						sent = true;
						break;
					}
					else if (rec.type() == LogRec.XAABORT)
					{
						sendNo();
						sent = true;
						break;
					}
					else if (rec.type() == LogRec.PREPARE)
					{
						sendNo();
						sent = true;
						break;
					}
				}
			}
		}
	}
	
	private void massDelete()
	{
		ArrayList<Object> tree = null;
		byte[] txBytes = new byte[8];
		long txNum = -1;
		byte[] schemaLenBytes = new byte[4];
		byte[] tableLenBytes = new byte[4];
		int schemaLength = -1;
		int tableLength = -1;
		String schema = null;
		String table = null;
		byte[] schemaData = null;
		byte[] tableData = null;
		ArrayList<String> indexes;
		ArrayList<ArrayList<String>> keys;
		ArrayList<ArrayList<String>> types;
		ArrayList<ArrayList<Boolean>> orders;
		TreeMap<Integer, String> pos2Col;
		HashMap<String, String> cols2Types;
		boolean logged;
		
		try
		{
			readNonCoord(txBytes);
			txNum = bytesToLong(txBytes);
			tx = new Transaction(txNum);
			readNonCoord(schemaLenBytes);
			schemaLength = bytesToInt(schemaLenBytes);
			schemaData = new byte[schemaLength];
			readNonCoord(schemaData);
			schema = new String(schemaData, "UTF-8");
			readNonCoord(tableLenBytes);
			tableLength = bytesToInt(tableLenBytes);
			tableData = new byte[tableLength];
			readNonCoord(tableData);
			table = new String(tableData, "UTF-8");
			byte[] flag = new byte[1];
			readNonCoord(flag);
			if (flag[0] == (byte)0)
			{
				logged = false;
			}
			else
			{
				logged = true;
			}
			ObjectInputStream objIn = new ObjectInputStream(sock.getInputStream());
			tree = (ArrayList<Object>)objIn.readObject();
			indexes = (ArrayList<String>)objIn.readObject();
			keys = (ArrayList<ArrayList<String>>)objIn.readObject();
			types = (ArrayList<ArrayList<String>>)objIn.readObject();
			orders = (ArrayList<ArrayList<Boolean>>)objIn.readObject();
			pos2Col = (TreeMap<Integer, String>)objIn.readObject();
			cols2Types = (HashMap<String, String>)objIn.readObject();
		}
		catch(Exception e)
		{
			sendNo();
			return;
		}
		
		Object obj = tree.get(0);
		while (obj instanceof ArrayList)
		{
			obj = ((ArrayList)obj).get(0);
		}
		
		removeFromTree((String)obj, tree, null); //also delete parents if now empty
		
		ArrayList<SendMassDeleteThread> threads = new ArrayList<SendMassDeleteThread>();
		for (Object o : tree)
		{
			if (!(o instanceof ArrayList))
			{
				ArrayList<Object> o2 = new ArrayList<Object>();
				o2.add((String)o);
				o = o2;
			}
			threads.add(new SendMassDeleteThread((ArrayList<Object>)o, tx, schema, table, keys, types, orders, indexes, pos2Col, cols2Types, logged));
		}
		
		for (SendMassDeleteThread thread : threads)
		{
			thread.start();
		}
		
		boolean allOK = true;
		for (SendMassDeleteThread thread : threads)
		{
			while (true)
			{
				try
				{
					thread.join();
					break;
				}
				catch(InterruptedException e)
				{}
			}
			if (!thread.getOK())
			{
				allOK = false;
			}
		}
		
		if (allOK)
		{
			try
			{
				//mass delete table and indexes
				File[] dirs = getDirs(HRDBMSWorker.getHParms().getProperty("data_directories"));
				ArrayList<MassDeleteThread> threads1 = new ArrayList<MassDeleteThread>();
				for (File dir : dirs)
				{
					File dir2 = new File(dir, schema + "." + table + ".tbl");
					threads1.add(new MassDeleteThread(dir2, tx, indexes, keys, types, orders, pos2Col, cols2Types, logged));
				}
				
				for (MassDeleteThread thread : threads1)
				{
					thread.start();
				}
				
				allOK = true;
				for (MassDeleteThread thread : threads1)
				{
					while (true)
					{
						try
						{
							thread.join();
							break;
						}
						catch(Exception e)
						{}
					}
					
					if (!thread.getOK())
					{
						allOK = false;
					}
				}
				
				if (!allOK)
				{
					sendNo();
					return;
				}
				

				sendOK();
				int num = 0;
				for (MassDeleteThread thread : threads1)
				{
					num += thread.getNum();
				}
				sendInt(num);
			}
			catch(Exception e)
			{
				sendNo();
				return;
			}
		}
		else
		{
			sendNo();
			try
			{
				sock.close();
			}
			catch(Exception f)
			{}
		}
	}
	
	private static File[] getDirs(String list)
	{
		StringTokenizer tokens = new StringTokenizer(list, ",", false);
		int i = 0;
		while (tokens.hasMoreTokens())
		{
			tokens.nextToken();
			i++;
		}
		File[] dirs = new File[i];
		tokens = new StringTokenizer(list, ",", false);
		i = 0;
		while (tokens.hasMoreTokens())
		{
			dirs[i] = new File(tokens.nextToken());
			i++;
		}
		
		return dirs;
	}
	
	private static class MassDeleteThread extends HRDBMSThread
	{
		private File file;
		private Transaction tx;
		private int num = 0;
		private boolean ok = true;
		private ArrayList<String> indexes;
		private ArrayList<ArrayList<String>> keys;
		private ArrayList<ArrayList<String>> types;
		private ArrayList<ArrayList<Boolean>> orders;
		private TreeMap<Integer, String> pos2Col;
		private HashMap<String, String> cols2Types;
		private boolean logged;
		
		public MassDeleteThread(File file, Transaction tx, ArrayList<String> indexes, ArrayList<ArrayList<String>> keys, ArrayList<ArrayList<String>> types, ArrayList<ArrayList<Boolean>> orders, TreeMap<Integer, String> pos2Col, HashMap<String, String> cols2Types, boolean logged)
		{
			this.file = file;
			this.tx = tx;
			this.indexes = indexes;
			this.keys = keys;
			this.types = types;
			this.orders = orders;
			this.pos2Col = pos2Col;
			this.cols2Types = cols2Types;
			this.logged = logged;
		}
		
		public int getNum()
		{
			return num;
		}
		
		public boolean getOK()
		{
			return ok;
		}
		
		public void run()
		{
			if (logged)
			{
				int numBlocks = -1;
				
				try
				{
					LockManager.sLock(new Block(file.getAbsolutePath(), -1), tx.number());
					FileChannel xx = FileManager.getFile(file.getAbsolutePath());
					numBlocks = FileManager.numBlocks.get(file.getAbsolutePath());
				}
				catch(Exception e)
				{
					ok = false;
					HRDBMSWorker.logger.debug("", e);
					return;
				}

				HashMap<Integer, DataType> layout = new HashMap<Integer, DataType>();
				for (Map.Entry entry : pos2Col.entrySet())
				{
					String type = cols2Types.get(entry.getValue());
					DataType value = null;
					if (type.equals("INT"))
					{
						value = new DataType(DataType.INTEGER, 0, 0);
					}
					else if (type.equals("FLOAT"))
					{
						value = new DataType(DataType.DOUBLE, 0, 0);
					}
					else if (type.equals("CHAR"))
					{
						value = new DataType(DataType.VARCHAR, 0, 0);
					}
					else if (type.equals("LONG"))
					{
						value = new DataType(DataType.BIGINT, 0, 0);
					}
					else if (type.equals("DATE"))
					{
						value = new DataType(DataType.DATE, 0, 0);
					}

					layout.put((Integer)entry.getKey(), value);
				}
				Schema sch = new Schema(layout);
				int onPage = Schema.HEADER_SIZE;
				int lastRequested = Schema.HEADER_SIZE - 1;
				int PREFETCH_REQUEST_SIZE = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("prefetch_request_size")); // 80
				int PAGES_IN_ADVANCE = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("pages_in_advance")); // 40
				while (onPage < numBlocks)
				{
					if (lastRequested - onPage < PAGES_IN_ADVANCE)
					{
						Block[] toRequest = new Block[lastRequested + PREFETCH_REQUEST_SIZE < numBlocks ? PREFETCH_REQUEST_SIZE : numBlocks - lastRequested - 1];
						int i = 0;
						while (i < toRequest.length)
						{
							toRequest[i] = new Block(file.getAbsolutePath(), lastRequested + i + 1);
							i++;
						}
						try
						{
							tx.requestPages(toRequest);
						}
						catch(Exception e)
						{
							ok = false;
							return;
						}
						lastRequested += toRequest.length;
					}
				
					try
					{
						tx.read(new Block(file.getAbsolutePath(), onPage++), sch, true);
						RowIterator rit = sch.rowIterator();
						while (rit.hasNext())
						{
							Row r = rit.next();
							if (!r.getCol(0).exists())
							{
								continue;
							}
					
							RID rid = r.getRID();
							sch.deleteRow(rid);
							num++;
						}
					}
					catch(Exception e)
					{
						ok = false;
						HRDBMSWorker.logger.debug("", e);
						return;
					}
				}
			
				try
				{
					int i = 0;
					for (String index : indexes)
					{
						Index idx = new Index(new File(file.getParentFile().getAbsoluteFile(), index).getAbsolutePath(), keys.get(i), types.get(i), orders.get(i));
						idx.setTransaction(tx);
						idx.open();
						idx.massDelete();
						i++;
					}
				}
				catch(Exception e)
				{
					ok = false;
					HRDBMSWorker.logger.debug("", e);
					return;
				}
			}
			else
			{
				//not logged 
				int numBlocks = -1;
				
				try
				{
					LockManager.sLock(new Block(file.getAbsolutePath(), -1), tx.number());
					FileChannel xx = FileManager.getFile(file.getAbsolutePath());
					numBlocks = FileManager.numBlocks.get(file.getAbsolutePath());
				}
				catch(Exception e)
				{
					ok = false;
					HRDBMSWorker.logger.debug("", e);
					return;
				}

				HashMap<Integer, DataType> layout = new HashMap<Integer, DataType>();
				for (Map.Entry entry : pos2Col.entrySet())
				{
					String type = cols2Types.get(entry.getValue());
					DataType value = null;
					if (type.equals("INT"))
					{
						value = new DataType(DataType.INTEGER, 0, 0);
					}
					else if (type.equals("FLOAT"))
					{
						value = new DataType(DataType.DOUBLE, 0, 0);
					}
					else if (type.equals("CHAR"))
					{
						value = new DataType(DataType.VARCHAR, 0, 0);
					}
					else if (type.equals("LONG"))
					{
						value = new DataType(DataType.BIGINT, 0, 0);
					}
					else if (type.equals("DATE"))
					{
						value = new DataType(DataType.DATE, 0, 0);
					}

					layout.put((Integer)entry.getKey(), value);
				}
				Schema sch = new Schema(layout);
				int onPage = Schema.HEADER_SIZE;
				int lastRequested = Schema.HEADER_SIZE - 1;
				int PREFETCH_REQUEST_SIZE = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("prefetch_request_size")); // 80
				int PAGES_IN_ADVANCE = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("pages_in_advance")); // 40
				while (onPage < numBlocks)
				{
					if (lastRequested - onPage < PAGES_IN_ADVANCE)
					{
						Block[] toRequest = new Block[lastRequested + PREFETCH_REQUEST_SIZE < numBlocks ? PREFETCH_REQUEST_SIZE : numBlocks - lastRequested - 1];
						int i = 0;
						while (i < toRequest.length)
						{
							toRequest[i] = new Block(file.getAbsolutePath(), lastRequested + i + 1);
							i++;
						}
						try
						{
							tx.requestPages(toRequest);
						}
						catch(Exception e)
						{
							ok = false;
							return;
						}
						lastRequested += toRequest.length;
					}
				
					try
					{
						tx.read(new Block(file.getAbsolutePath(), onPage++), sch, true);
						RowIterator rit = sch.rowIterator();
						while (rit.hasNext())
						{
							Row r = rit.next();
							if (!r.getCol(0).exists())
							{
								continue;
							}
					
							RID rid = r.getRID();
							sch.deleteRowNoLog(rid);
							num++;
						}
					}
					catch(Exception e)
					{
						ok = false;
						HRDBMSWorker.logger.debug("", e);
						return;
					}
				}
			
				try
				{
					int i = 0;
					for (String index : indexes)
					{
						Index idx = new Index(new File(file.getParentFile().getAbsoluteFile(), index).getAbsolutePath(), keys.get(i), types.get(i), orders.get(i));
						idx.setTransaction(tx);
						idx.open();
						idx.massDeleteNoLog();
						i++;
					}
				}
				catch(Exception e)
				{
					ok = false;
					HRDBMSWorker.logger.debug("", e);
					return;
				}
			}
		}
	}
	
	private class SendMassDeleteThread extends HRDBMSThread
	{
		private ArrayList<Object> tree;
		private Transaction tx;
		private boolean ok;
		int num;
		String schema;
		String table;
		ArrayList<ArrayList<String>> keys;
		ArrayList<ArrayList<String>> types;
		ArrayList<ArrayList<Boolean>> orders;
		ArrayList<String> indexes;
		TreeMap<Integer, String> pos2Col;
		HashMap<String, String> cols2Types;
		boolean logged;
		
		public SendMassDeleteThread(ArrayList<Object> tree, Transaction tx, String schema, String table, ArrayList<ArrayList<String>> keys, ArrayList<ArrayList<String>> types, ArrayList<ArrayList<Boolean>> orders, ArrayList<String> indexes, TreeMap<Integer, String> pos2Col, HashMap<String, String> cols2Types, boolean logged)
		{
			this.tree = tree;
			this.tx = tx;
			this.schema = schema;
			this.table = table;
			this.keys = keys;
			this.types = types;
			this.orders = orders;
			this.indexes = indexes;
			this.pos2Col = pos2Col;
			this.cols2Types = cols2Types;
			this.logged = logged;
		}
		
		public int getNum()
		{
			return num;
		}
		
		public boolean getOK()
		{
			return ok;
		}
		
		public void run()
		{
			sendMassDelete(tree, tx);
		}
		
		private void sendMassDelete(ArrayList<Object> tree, Transaction tx)
		{
			Object obj = tree.get(0);
			while (obj instanceof ArrayList)
			{
				obj = ((ArrayList)obj).get(0);
			}
			
			String hostname = (String)obj;
			Socket sock = null;
			try
			{
				sock = CompressedSocket.newCompressedSocket(hostname, Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
				OutputStream out = sock.getOutputStream();
				byte[] outMsg = "MDELETE         ".getBytes("UTF-8");
				outMsg[8] = 0;
				outMsg[9] = 0;
				outMsg[10] = 0;
				outMsg[11] = 0;
				outMsg[12] = 0;
				outMsg[13] = 0;
				outMsg[14] = 0;
				outMsg[15] = 0;
				out.write(outMsg);
				out.write(longToBytes(tx.number()));
				//write schema and table
				out.write(stringToBytes(schema));
				out.write(stringToBytes(table));
				if (logged)
				{
					out.write(1);
				}
				else
				{
					out.write(0);
				}
				ObjectOutputStream objOut = new ObjectOutputStream(out);
				objOut.writeObject(tree);
				objOut.writeObject(indexes);
				objOut.writeObject(keys);
				objOut.writeObject(types);
				objOut.writeObject(orders);
				objOut.writeObject(pos2Col);
				objOut.writeObject(cols2Types);
				objOut.flush();
				out.flush();
				getConfirmation(sock);
				int count = 4;
				int off = 0;
				byte[] numBytes = new byte[4];
				while (count > 0)
				{
					int temp = sock.getInputStream().read(numBytes, off, 4-off);
					if (temp == -1)
					{
						ok = false;
						objOut.close();
						sock.close();
					}
					
					count -= temp;
				}
				
				num = bytesToInt(numBytes);
				objOut.close();
				sock.close();
				ok = true;
			}
			catch(Exception e)
			{
				ok = false;
				try
				{
					sock.close();
				}
				catch(Exception f)
				{}
				HRDBMSWorker.logger.debug("", e);
			}
		}
	}
	
	private void delete()
	{
		byte[] txBytes = new byte[8];
		long txNum = -1;
		byte[] schemaLenBytes = new byte[4];
		byte[] tableLenBytes = new byte[4];
		int schemaLength = -1;
		int tableLength = -1;
		String schema = null;
		String table = null;
		byte[] schemaData = null;
		byte[] tableData = null;
		ArrayList<String> indexes;
		ArrayList<ArrayList<String>> keys;
		ArrayList<ArrayList<String>> types;
		ArrayList<ArrayList<Boolean>> orders;
		ArrayList<RIDAndIndexKeys> raiks = null;
		try
		{
			readNonCoord(txBytes);
			txNum = bytesToLong(txBytes);
			tx = new Transaction(txNum);
			readNonCoord(schemaLenBytes);
			schemaLength = bytesToInt(schemaLenBytes);
			schemaData = new byte[schemaLength];
			readNonCoord(schemaData);
			schema = new String(schemaData, "UTF-8");
			readNonCoord(tableLenBytes);
			tableLength = bytesToInt(tableLenBytes);
			tableData = new byte[tableLength];
			readNonCoord(tableData);
			table = new String(tableData, "UTF-8");
			ObjectInputStream objIn = new ObjectInputStream(sock.getInputStream());
			indexes = (ArrayList<String>)objIn.readObject();
			raiks = (ArrayList<RIDAndIndexKeys>)objIn.readObject();
			keys = (ArrayList<ArrayList<String>>)objIn.readObject();
			types = (ArrayList<ArrayList<String>>)objIn.readObject();
			orders = (ArrayList<ArrayList<Boolean>>)objIn.readObject();
		}
		catch(Exception e)
		{
			sendNo();
			return;
		}
		
		MultiHashMap<Integer, RIDAndIndexKeys> map = new MultiHashMap<Integer, RIDAndIndexKeys>();
		for (RIDAndIndexKeys raik : raiks)
		{
			map.multiPut(raik.getRID().getDevice(), raik);
		}
		
		ArrayList<FlushDeleteThread> threads = new ArrayList<FlushDeleteThread>();
		for (Object o : map.getKeySet())
		{
			int device = (Integer)o;
			threads.add(new FlushDeleteThread(map.get(device), tx, schema, table, keys, types, orders, indexes));
		}
		
		for (FlushDeleteThread thread : threads)
		{
			thread.start();
		}
		
		boolean allOK = true;
		for (FlushDeleteThread thread : threads)
		{
			while (true)
			{
				try
				{
					thread.join();
					break;
				}
				catch(InterruptedException e)
				{}
			}
			if (!thread.getOK())
			{
				allOK = false;
			}
		}
		
		if (allOK)
		{
			sendOK();
			return;
		}
		else
		{
			sendNo();
			return;
		}
	}
	
	private void update()
	{
		byte[] txBytes = new byte[8];
		long txNum = -1;
		byte[] schemaLenBytes = new byte[4];
		byte[] tableLenBytes = new byte[4];
		int schemaLength = -1;
		int tableLength = -1;
		String schema = null;
		String table = null;
		byte[] schemaData = null;
		byte[] tableData = null;
		ArrayList<String> indexes;
		ArrayList<ArrayList<String>> keys;
		ArrayList<ArrayList<String>> types;
		ArrayList<ArrayList<Boolean>> orders;
		ArrayList<RIDAndIndexKeys> raiks = null;
		ArrayList<ArrayList<Object>> list2;
		HashMap<String, Integer> cols2Pos;
		PartitionMetaData pmd;
		try
		{
			readNonCoord(txBytes);
			txNum = bytesToLong(txBytes);
			tx = new Transaction(txNum);
			readNonCoord(schemaLenBytes);
			schemaLength = bytesToInt(schemaLenBytes);
			schemaData = new byte[schemaLength];
			readNonCoord(schemaData);
			schema = new String(schemaData, "UTF-8");
			readNonCoord(tableLenBytes);
			tableLength = bytesToInt(tableLenBytes);
			tableData = new byte[tableLength];
			readNonCoord(tableData);
			table = new String(tableData, "UTF-8");
			ObjectInputStream objIn = new ObjectInputStream(sock.getInputStream());
			indexes = (ArrayList<String>)objIn.readObject();
			raiks = (ArrayList<RIDAndIndexKeys>)objIn.readObject();
			keys = (ArrayList<ArrayList<String>>)objIn.readObject();
			types = (ArrayList<ArrayList<String>>)objIn.readObject();
			orders = (ArrayList<ArrayList<Boolean>>)objIn.readObject();
			list2 = (ArrayList<ArrayList<Object>>)objIn.readObject();
			cols2Pos = (HashMap<String, Integer>)objIn.readObject();
			pmd = (PartitionMetaData)objIn.readObject();
		}
		catch(Exception e)
		{
			sendNo();
			return;
		}
		
		MultiHashMap<Integer, RIDAndIndexKeys> map = new MultiHashMap<Integer, RIDAndIndexKeys>();
		for (RIDAndIndexKeys raik : raiks)
		{
			map.multiPut(raik.getRID().getDevice(), raik);
		}
		
		ArrayList<FlushDeleteThread> threads = new ArrayList<FlushDeleteThread>();
		for (Object o : map.getKeySet())
		{
			int device = (Integer)o;
			threads.add(new FlushDeleteThread(map.get(device), tx, schema, table, keys, types, orders, indexes));
		}
		
		for (FlushDeleteThread thread : threads)
		{
			thread.start();
		}
		
		boolean allOK = true;
		for (FlushDeleteThread thread : threads)
		{
			while (true)
			{
				try
				{
					thread.join();
					break;
				}
				catch(InterruptedException e)
				{}
			}
			if (!thread.getOK())
			{
				allOK = false;
			}
		}
		
		if (!allOK)
		{
			sendNo();
		}
		
		if (list2 != null)
		{
			MultiHashMap<Integer, ArrayList<Object>> map2 = new MultiHashMap<Integer, ArrayList<Object>>();
			for (ArrayList<Object> row : list2)
			{
				map2.multiPut(MetaData.determineDevice(row, pmd, cols2Pos), row);
			}
			
			ArrayList<FlushInsertThread> threads2 = new ArrayList<FlushInsertThread>();
			for (Object o : map2.getKeySet())
			{
				int device = (Integer)o;
				threads2.add(new FlushInsertThread(map2.get(device), new Transaction(txNum), schema, table, keys, types, orders, indexes, cols2Pos, device));
			}
			
			for (FlushInsertThread thread : threads2)
			{
				thread.start();
			}
			
			for (FlushInsertThread thread : threads2)
			{
				while (true)
				{
					try
					{
						thread.join();
						break;
					}
					catch(InterruptedException e)
					{}
				}
				if (!thread.getOK())
				{
					allOK = false;
				}
			}
		}
		
		if (allOK)
		{
			sendOK();
			return;
		}
		else
		{
			sendNo();
			return;
		}
	}
	
	private class FlushDeleteThread extends HRDBMSThread
	{
		private Transaction tx;
		private boolean ok = true;
		String schema;
		String table;
		ArrayList<ArrayList<String>> keys;
		ArrayList<ArrayList<String>> types;
		ArrayList<ArrayList<Boolean>> orders;
		ArrayList<String> indexes;
		Set<RIDAndIndexKeys> raiks;
		
		public FlushDeleteThread(Set<RIDAndIndexKeys> raiks, Transaction tx, String schema, String table, ArrayList<ArrayList<String>> keys, ArrayList<ArrayList<String>> types, ArrayList<ArrayList<Boolean>> orders, ArrayList<String> indexes)
		{
			this.raiks = raiks;
			this.tx = tx;
			this.schema = schema;
			this.table = table;
			this.keys = keys;
			this.types = types;
			this.orders = orders;
			this.indexes = indexes;
		}
		
		public boolean getOK()
		{
			return ok;
		}
		
		public void run()
		{
			try
			{
				//delete row based on rid
				MultiHashMap<Integer, RIDAndIndexKeys> map = new MultiHashMap<Integer, RIDAndIndexKeys>();
				for (RIDAndIndexKeys raik : raiks)
				{
					map.multiPut(raik.getRID().getBlockNum(), raik);
				}
			
				Iterator<RIDAndIndexKeys> it = raiks.iterator();
				int num = it.next().getRID().getDevice();
				HashMap<Integer, DataType> layout = new HashMap<Integer, DataType>();
				Schema sch = new Schema(layout);
				for (Object o : map.getKeySet())
				{
					int block = (Integer)o;
					//request block
					//delete every rid
					Block toRequest = new Block(new MetaData().getDevicePath(num) + schema + "." + table + ".tbl", block);
					tx.requestPage(toRequest);
					tx.read(toRequest, sch, true);
					for (Object o2 : map.get(block))
					{
						RIDAndIndexKeys raik = (RIDAndIndexKeys)o2;
						sch.deleteRow(raik.getRID());
					}
				
					//for each index, delete row based on rid and key values
					int i = 0;
					for (String index : indexes)
					{
						Index idx = new Index(new MetaData().getDevicePath(num) + index, keys.get(i), types.get(i), orders.get(i));
						idx.setTransaction(tx);
						idx.open();
						for (Object o2 : map.get(block))
						{
							RIDAndIndexKeys raik = (RIDAndIndexKeys)o2;
							idx.delete(aloToFieldValues(raik.getIndexKeys().get(i)),raik.getRID());
						}
						i++;
					}
				}
			}
			catch(Exception e)
			{
				ok = false;
				HRDBMSWorker.logger.debug("", e);
			}
		}
	}
	
	private static FieldValue[] toFVA(ArrayList<Object> row)
	{
		FieldValue[] retval = new FieldValue[row.size()];
		int i = 0;
		for (Object o : row)
		{
			retval[i] = (FieldValue)o;
			i++;
		}
		
		return retval;
	}
	
	private static FieldValue[] aloToFieldValues(ArrayList<Object> row)
	{
		FieldValue[] retval = new FieldValue[row.size()];
		int i = 0;
		for (Object o : row)
		{
			if (o instanceof Integer)
			{
				retval[i] = new Schema.IntegerFV((Integer)o);
			}
			else if (o instanceof Long)
			{
				retval[i] = new Schema.BigintFV((Long)o);
			}
			else if (o instanceof Double)
			{
				retval[i] = new Schema.DoubleFV((Double)o);
			}
			else if (o instanceof MyDate)
			{
				retval[i] = new Schema.DateFV((MyDate)o);
			}
			else if (o instanceof String)
			{
				retval[i] = new Schema.VarcharFV((String)o);
			}
			
			i++;
		}
		
		return retval;
	}
	
	private void insert()
	{
		byte[] txBytes = new byte[8];
		long txNum = -1;
		byte[] schemaLenBytes = new byte[4];
		byte[] tableLenBytes = new byte[4];
		int schemaLength = -1;
		int tableLength = -1;
		String schema = null;
		String table = null;
		byte[] schemaData = null;
		byte[] tableData = null;
		ArrayList<String> indexes;
		ArrayList<ArrayList<String>> keys;
		ArrayList<ArrayList<String>> types;
		ArrayList<ArrayList<Boolean>> orders;
		ArrayList<ArrayList<Object>> list = null;
		HashMap<String, Integer> cols2Pos;
		PartitionMetaData partMeta;
		try
		{
			readNonCoord(txBytes);
			txNum = bytesToLong(txBytes);
			readNonCoord(schemaLenBytes);
			schemaLength = bytesToInt(schemaLenBytes);
			schemaData = new byte[schemaLength];
			readNonCoord(schemaData);
			schema = new String(schemaData, "UTF-8");
			readNonCoord(tableLenBytes);
			tableLength = bytesToInt(tableLenBytes);
			tableData = new byte[tableLength];
			readNonCoord(tableData);
			table = new String(tableData, "UTF-8");
			ObjectInputStream objIn = new ObjectInputStream(sock.getInputStream());
			indexes = (ArrayList<String>)objIn.readObject();
			list = (ArrayList<ArrayList<Object>>)objIn.readObject();
			keys = (ArrayList<ArrayList<String>>)objIn.readObject();
			types = (ArrayList<ArrayList<String>>)objIn.readObject();
			orders = (ArrayList<ArrayList<Boolean>>)objIn.readObject();
			cols2Pos = (HashMap<String, Integer>)objIn.readObject();
			partMeta = ((PartitionMetaData)objIn.readObject());
		}
		catch(Exception e)
		{
			sendNo();
			return;
		}
		
		MultiHashMap<Integer, ArrayList<Object>> map = new MultiHashMap<Integer, ArrayList<Object>>();
		for (ArrayList<Object> row : list)
		{
			map.multiPut(MetaData.determineDevice(row, partMeta, cols2Pos), row);
		}
		
		ArrayList<FlushInsertThread> threads = new ArrayList<FlushInsertThread>();
		for (Object o : map.getKeySet())
		{
			int device = (Integer)o;
			threads.add(new FlushInsertThread(map.get(device), new Transaction(txNum), schema, table, keys, types, orders, indexes, cols2Pos, device));
		}
		
		for (FlushInsertThread thread : threads)
		{
			thread.start();
		}
		
		boolean allOK = true;
		for (FlushInsertThread thread : threads)
		{
			while (true)
			{
				try
				{
					thread.join();
					break;
				}
				catch(InterruptedException e)
				{}
			}
			if (!thread.getOK())
			{
				allOK = false;
			}
		}
		
		if (allOK)
		{
			sendOK();
			return;
		}
		else
		{
			sendNo();
			return;
		}
	}
	
	private void load()
	{
		byte[] txBytes = new byte[8];
		long txNum = -1;
		byte[] schemaLenBytes = new byte[4];
		byte[] tableLenBytes = new byte[4];
		int schemaLength = -1;
		int tableLength = -1;
		String schema = null;
		String table = null;
		byte[] schemaData = null;
		byte[] tableData = null;
		ArrayList<String> indexes;
		ArrayList<ArrayList<String>> keys;
		ArrayList<ArrayList<String>> types;
		ArrayList<ArrayList<Boolean>> orders;
		ArrayList<ArrayList<Object>> list = null;
		HashMap<String, Integer> cols2Pos;
		PartitionMetaData partMeta;
		byte[] devBytes = new byte[4];
		int device;
		try
		{
			readNonCoord(txBytes);
			txNum = bytesToLong(txBytes);
			readNonCoord(devBytes);
			device = bytesToInt(devBytes);
			readNonCoord(schemaLenBytes);
			schemaLength = bytesToInt(schemaLenBytes);
			schemaData = new byte[schemaLength];
			readNonCoord(schemaData);
			schema = new String(schemaData, "UTF-8");
			readNonCoord(tableLenBytes);
			tableLength = bytesToInt(tableLenBytes);
			tableData = new byte[tableLength];
			readNonCoord(tableData);
			table = new String(tableData, "UTF-8");
			list = readRS();
			ObjectInputStream objIn = new ObjectInputStream(sock.getInputStream());
			indexes = (ArrayList<String>)objIn.readObject();
			keys = (ArrayList<ArrayList<String>>)objIn.readObject();
			types = (ArrayList<ArrayList<String>>)objIn.readObject();
			orders = (ArrayList<ArrayList<Boolean>>)objIn.readObject();
			cols2Pos = (HashMap<String, Integer>)objIn.readObject();
			partMeta = ((PartitionMetaData)objIn.readObject());
		}
		catch(Exception e)
		{
			sendNo();
			return;
		}
		
		FlushLoadThread thread = new FlushLoadThread(list, new Transaction(txNum), schema, table, keys, types, orders, indexes, cols2Pos, device);
		thread.run();	
		boolean allOK = thread.getOK();
		
		if (allOK)
		{
			sendOK();
			return;
		}
		else
		{
			sendNo();
			return;
		}
	}
	
	private void hadoopLoad()
	{
		ArrayList<ArrayList<Object>> list;
		PartitionMetaData partMeta;
		HashMap<String, Integer> cols2Pos;
		TreeMap<Integer, String> pos2Col;
		long txNum;
		String schema;
		String table;
		ArrayList<ArrayList<String>> keys;
		ArrayList<ArrayList<String>> types;
		ArrayList<ArrayList<Boolean>> orders;
		ArrayList<String> indexes;
		byte[] devBytes = new byte[4];
		int device;
		
		try
		{
			InputStream in = sock.getInputStream();
			readNonCoord(devBytes);
			device = bytesToInt(devBytes);
			byte[] length = new byte[4];
			readNonCoord(length);
			int len = bytesToInt(length);
			byte[] data = new byte[len];
			readNonCoord(data);
			String tableName = new String(data, "UTF-8");
			list = readRS();
			LoadMetaData ldmd = ldmds.get(tableName);
			partMeta = ldmd.pmd;
			pos2Col = ldmd.pos2Col;
			cols2Pos = new HashMap<String, Integer>();
			for (Map.Entry entry : pos2Col.entrySet())
			{
				cols2Pos.put((String)entry.getValue(), (Integer)entry.getKey());
			}
			txNum = ldmd.txNum;
			schema = tableName.substring(0, tableName.indexOf('.'));
			table = tableName.substring(tableName.indexOf('.') + 1);
			keys = ldmd.keys;
			types = ldmd.types;
			orders = ldmd.orders;
			indexes = ldmd.indexes;
		}
		catch(Exception e)
		{
			sendNo();
			return;
		}
		
		FlushLoadThread thread = new FlushLoadThread(list, new Transaction(txNum), schema, table, keys, types, orders, indexes, cols2Pos, device);
		thread.run();
		
		boolean allOK = thread.getOK();
	
		if (allOK)
		{
			sendOK();
			return;
		}
		else
		{
			sendNo();
			return;
		}
	}
	
	private ArrayList<ArrayList<Object>> readRS() throws Exception
	{
		byte[] numBytes = new byte[4];
		readNonCoord(numBytes);
		int num = bytesToInt(numBytes);
		ArrayList<ArrayList<Object>> retval = new ArrayList<ArrayList<Object>>(num);
		int i = 0;
		while (i < num)
		{
			readNonCoord(numBytes);
			int size = bytesToInt(numBytes);
			byte[] data = new byte[size];
			readNonCoord(data);
			retval.add((ArrayList<Object>)fromBytes(data));
			i++;
		}
		
		return retval;
	}
	
	private static final Object fromBytes(byte[] val) throws Exception
	{
		final ByteBuffer bb = ByteBuffer.wrap(val);
		final int numFields = bb.getInt();

		if (numFields == 0)
		{
			return new ArrayList<Object>();
		}

		bb.position(bb.position() + numFields);
		final byte[] bytes = bb.array();
		if (bytes[4] == 5)
		{
			return new DataEndMarker();
		}
		final ArrayList<Object> retval = new ArrayList<Object>(numFields);
		int i = 0;
		while (i < numFields)
		{
			if (bytes[i + 4] == 0)
			{
				// long
				final Long o = bb.getLong();
				retval.add(o);
			}
			else if (bytes[i + 4] == 1)
			{
				// integer
				final Integer o = bb.getInt();
				retval.add(o);
			}
			else if (bytes[i + 4] == 2)
			{
				// double
				final Double o = bb.getDouble();
				retval.add(o);
			}
			else if (bytes[i + 4] == 3)
			{
				// date
				final MyDate o = new MyDate(bb.getLong());
				retval.add(o);
			}
			else if (bytes[i + 4] == 4)
			{
				// string
				final int length = bb.getInt();
				final byte[] temp = new byte[length];
				bb.get(temp);
				try
				{
					final String o = new String(temp, "UTF-8");
					retval.add(o);
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.error("", e);
					throw e;
				}
			}
			else if (bytes[i + 4] == 6)
			{
				// AtomicLong
				final long o = bb.getLong();
				retval.add(new AtomicLong(o));
			}
			else if (bytes[i + 4] == 7)
			{
				// AtomicDouble
				final double o = bb.getDouble();
				retval.add(new AtomicDouble(o));
			}
			else if (bytes[i + 4] == 8)
			{
				// Empty ArrayList
				retval.add(new ArrayList<Object>());
			}
			else
			{
				HRDBMSWorker.logger.error("Unknown type in fromBytes()");
				throw new Exception("Unknown type in fromBytes()");
			}

			i++;
		}

		return retval;
	}
	
	private class FlushInsertThread extends HRDBMSThread
	{
		private Transaction tx;
		private boolean ok = true;
		String schema;
		String table;
		ArrayList<ArrayList<String>> keys;
		ArrayList<ArrayList<String>> types;
		ArrayList<ArrayList<Boolean>> orders;
		ArrayList<String> indexes;
		Set<ArrayList<Object>> list;
		HashMap<String, Integer> cols2Pos;
		int num;
		
		public FlushInsertThread(Set<ArrayList<Object>> list, Transaction tx, String schema, String table, ArrayList<ArrayList<String>> keys, ArrayList<ArrayList<String>> types, ArrayList<ArrayList<Boolean>> orders, ArrayList<String> indexes, HashMap<String, Integer> cols2Pos, int num)
		{
			this.list = list;
			this.tx = tx;
			this.schema = schema;
			this.table = table;
			this.keys = keys;
			this.types = types;
			this.orders = orders;
			this.indexes = indexes;
			this.cols2Pos = cols2Pos;
			this.num = num;
		}
		
		public boolean getOK()
		{
			return ok;
		}
		
		public void run()
		{
			try
			{
				//insert row and create RAIKS
				String tfn = new MetaData().getDevicePath(num) + schema + "." + table + ".tbl";
				FileManager.getFile(tfn);
				int maxPlus = FileManager.numBlocks.get(tfn) - (Schema.HEADER_SIZE + 1);
				int block = 4096;
				try
				{
					block = Schema.HEADER_SIZE + random.nextInt(maxPlus + 1);
				}
				catch(Exception e)
				{
					HRDBMSWorker.logger.debug("tfn = " + tfn);
					HRDBMSWorker.logger.debug("FileManager says " + FileManager.numBlocks.get(tfn));
					HRDBMSWorker.logger.debug("maxPlus = " + maxPlus);
					throw e;
				}
				//request block
				HashMap<Integer, DataType> layout = new HashMap<Integer, DataType>();
				Schema sch = new Schema(layout);
				Block toRequest = new Block(tfn, block);
				tx.requestPage(toRequest);
				tx.read(toRequest, sch, true);
				ArrayList<RIDAndIndexKeys> raiks = new ArrayList<RIDAndIndexKeys>();
				for (ArrayList<Object> row : list)
				{
					RID rid = sch.insertRow(aloToFieldValues(row));
					ArrayList<ArrayList<Object>> indexKeys = new ArrayList<ArrayList<Object>>();
					int i = 0;
					for (String index : indexes)
					{
						ArrayList<String> key = keys.get(i);
						ArrayList<Object> k = new ArrayList<Object>();
						for (String col : key)
						{
							try
							{
								k.add(row.get(cols2Pos.get(col)));
							}
							catch(Exception e)
							{
								HRDBMSWorker.logger.debug("Row is " + row);
								HRDBMSWorker.logger.debug("Cols2Pos is " + cols2Pos);
								HRDBMSWorker.logger.debug("Col is " + col);
								throw e;
							}
						}
						
						indexKeys.add(k);
						i++;
					}
					
					raiks.add(new RIDAndIndexKeys(rid, indexKeys));
				}
				
				//for each index, insert row based on rid and key values
				int i = 0;
				for (String index : indexes)
				{
					Index idx = new Index(new MetaData().getDevicePath(num) + index, keys.get(i), types.get(i), orders.get(i));
					idx.setTransaction(tx);
					idx.open();
					for (RIDAndIndexKeys raik : raiks)
					{
						idx.insert(aloToFieldValues(raik.getIndexKeys().get(i)), raik.getRID());
					}
					i++;
				}
			}
			catch(Exception e)
			{
				ok = false;
				HRDBMSWorker.logger.debug("", e);
			}
		}
	}
	
	private class FlushLoadThread extends HRDBMSThread
	{
		private Transaction tx;
		private boolean ok = true;
		String schema;
		String table;
		ArrayList<ArrayList<String>> keys;
		ArrayList<ArrayList<String>> types;
		ArrayList<ArrayList<Boolean>> orders;
		ArrayList<String> indexes;
		ArrayList<ArrayList<Object>> list;
		HashMap<String, Integer> cols2Pos;
		int num;
		LinkedBlockingQueue<Object> queue = new LinkedBlockingQueue<Object>();
		IndexWriterThread thread;
		
		public FlushLoadThread(ArrayList<ArrayList<Object>> list, Transaction tx, String schema, String table, ArrayList<ArrayList<String>> keys, ArrayList<ArrayList<String>> types, ArrayList<ArrayList<Boolean>> orders, ArrayList<String> indexes, HashMap<String, Integer> cols2Pos, int num)
		{
			this.list = list;
			this.tx = tx;
			this.schema = schema;
			this.table = table;
			this.keys = keys;
			this.types = types;
			this.orders = orders;
			this.indexes = indexes;
			this.cols2Pos = cols2Pos;
			this.num = num;
		}
		
		public boolean getOK()
		{
			return ok;
		}
		
		public void run()
		{
			try
			{
				ArrayList<Index> idxs = new ArrayList<Index>();
				int i = 0;
				for (String index : indexes)
				{
					Index idx = new Index(new MetaData().getDevicePath(num) + index, keys.get(i), types.get(i), orders.get(i));
					idx.setTransaction(tx);
					idx.open();
					idxs.add(idx);
					i++;
				}
				
				thread = new IndexWriterThread(queue, idxs);
				thread.start();
				MetaData meta = new MetaData();
				String file = meta.getDevicePath(num) + schema + "." + table + ".tbl";
				//insert row and create RAIKS
				FileChannel xx = FileManager.getFile(file);
				int block = FileManager.numBlocks.get(file) - 1;
				//request block
				HashMap<Integer, DataType> layout = new HashMap<Integer, DataType>();
				Schema sch = new Schema(layout);
				Block toRequest = new Block(file, block);
				tx.requestPage(toRequest);
				tx.read(toRequest, sch, true);
				ArrayList<RIDAndIndexKeys> raiks = new ArrayList<RIDAndIndexKeys>();
				Collections.shuffle(list);
				for (ArrayList<Object> row : list)
				{
					RID rid = sch.insertRowAppend(aloToFieldValues(row));
					ArrayList<ArrayList<Object>> indexKeys = new ArrayList<ArrayList<Object>>();
					i = 0;
					for (String index : indexes)
					{
						ArrayList<String> key = keys.get(i);
						ArrayList<Object> k = new ArrayList<Object>();
						for (String col : key)
						{
							k.add(row.get(cols2Pos.get(col)));
						}
						
						indexKeys.add(k);
						i++;
					}
					
					queue.add(new RIDAndIndexKeys(rid, indexKeys));
					int newBlock = rid.getBlockNum();
					if (newBlock != block)
					{
						block = newBlock;
						sch.close();
						sch = new Schema(layout);
						toRequest = new Block(file, block);
						tx.requestPage(toRequest);
						tx.read(toRequest, sch, true);
					}
				}
				
				sch.close();
				
				//wait for index writer to finish
				queue.add(new DataEndMarker());
				thread.join();
				if (!thread.getOK())
				{
					ok = false;
				}
			}
			catch(Exception e)
			{
				ok = false;
				HRDBMSWorker.logger.debug("", e);
			}
		}
	}
	
	private static class IndexWriterThread extends HRDBMSThread
	{
		private LinkedBlockingQueue<Object> queue;
		private ArrayList<Index> indexes;
		private boolean ok = true;
		private boolean isFieldValues = false;
		
		public IndexWriterThread(LinkedBlockingQueue<Object> queue, ArrayList<Index> indexes)
		{
			this.queue = queue;
			this.indexes = indexes;
		}
		
		public IndexWriterThread(LinkedBlockingQueue<Object> queue, ArrayList<Index> indexes, boolean isFieldValues)
		{
			this.queue = queue;
			this.indexes = indexes;
			this.isFieldValues = isFieldValues;
		}
		
		public void run()
		{
			try
			{
				while (true)
				{
					Object obj = queue.take();
				
					if (obj instanceof DataEndMarker)
					{
						return;
					}
				
					RIDAndIndexKeys raik = (RIDAndIndexKeys)obj;
					//for each index, insert row based on rid and key values
					if (!isFieldValues)
					{
						int i = 0;
						for (Index idx : indexes)
						{
							idx.insertNoLog(aloToFieldValues(raik.getIndexKeys().get(i)), raik.getRID());
							i++;
						}
					}
					else
					{
						int i = 0;
						for (Index idx : indexes)
						{
							idx.insertNoLog(toFVA(raik.getIndexKeys().get(i)), raik.getRID());
							i++;
						}
					}
				}
			}
			catch(Exception e)
			{
				HRDBMSWorker.logger.debug("", e);
				ok = false;
			}
		}
		
		public boolean getOK()
		{
			return ok;
		}
	}
	
	private void executeUpdate()
	{
		byte[] bLen = new byte[4];
		read(bLen);
		byte[] bStmt = new byte[bytesToInt(bLen)];
		read(bStmt);
		String sql = null;
		try
		{
			sql = new String(bStmt, "UTF-8");
		}
		catch(Exception e)
		{}
		
		if (tx == null)
		{
			tx = new Transaction(Transaction.ISOLATION_CS);
		}
		
		try
		{
			if (worker != null)
			{
				ArrayList<Object> cmd2 = new ArrayList<Object>(1);
				cmd2.add("CLOSE");
				while (true)
				{
					try
					{
						worker.in.put(cmd2);
						worker = null;
						break;
					}
					catch(Exception g)
					{}
				}
			}
			
			worker = XAManager.executeUpdate(sql, tx, this);
			HRDBMSWorker.addThread(worker);
			worker.join();
			int updateCount = worker.getUpdateCount();
			
			if (updateCount == -1)
			{
				sendNo();
				returnExceptionToClient(worker.getException());
				worker = null;
				return;
			}
			worker = null;
			this.sendOK();
			sendInt(updateCount);
		}
		catch(Exception e)
		{
			HRDBMSWorker.logger.debug("", e);
			this.sendNo();
			returnExceptionToClient(e);
			worker = null;
		}
	}
	
	public static class CreateTableThread extends HRDBMSThread
	{
		private String tbl;
		private int cols;
		private int device;
		private boolean ok = true;
		private Exception e = null;
		Transaction tx;
		
		public CreateTableThread(String tbl, int cols, int device, Transaction tx)
		{
			this.tbl = tbl;
			this.cols = cols;
			this.device = device;
			this.tx = tx;
		}
		
		public CreateTableThread(String fn, int cols, Transaction tx)
		{
			this.cols = cols;
			this.tx = tx;
			this.tbl = fn.substring(fn.lastIndexOf("/") + 1);
			String devicePath = fn.substring(0, fn.lastIndexOf("/") + 1);
			int i = 0;
			while (true)
			{
				String path = new MetaData().getDevicePath(i);
				if (!path.endsWith("/"))
				{
					path += "/";
				}
				
				if (path.equals(devicePath))
				{
					this.device = i;
					break;
				}
				
				i++;
			}
		}
		
		public void run()
		{
			try
			{
				createTableHeader(tbl, cols, device, tx);
			}
			catch(Exception e)
			{
				HRDBMSWorker.logger.debug("", e);
				this.e = e;
				ok = false;
			}
		}
		
		public boolean getOK()
		{
			return ok;
		}
		
		public Exception getException()
		{
			return e;
		}
		
		private void createTableHeader(String tbl, int cols, int device, Transaction tx) throws Exception
		{
			String fn = new MetaData().getDevicePath(device);
			if (!fn.endsWith("/"))
			{
				fn += "/";
			}
			fn += tbl;

			FileChannel fc = FileManager.getFile(fn);
			if (fc.size() > 0)
			{
				fc.truncate(0);
			}
			if (tx != null)
			{
				tx.truncate(new Block(fn, 0));
				tx.commit();
			}
			BufferManager.invalidateFile(fn);
			FileManager.numBlocks.put(fn, 0);
			ByteBuffer bb = ByteBuffer.allocate(Page.BLOCK_SIZE);
			bb.position(0);
			bb.putInt(MetaData.myNodeNum());
			bb.putInt(device);
			bb.putInt(Page.BLOCK_SIZE - (57 + (cols * 4)));
		
			int i = 12;
			while (i < Page.BLOCK_SIZE)
			{
				bb.putInt(-1);
				i += 4;
			}
		
			bb.position(0);
			fc.write(bb);
		
			ByteBuffer head = ByteBuffer.allocate(Page.BLOCK_SIZE * 4095);
			i = 0;
			while (i < Page.BLOCK_SIZE * 4095)
			{
				head.putLong(-1);
				i += 8;
			}
			head.position(0);
			fc.write(head);
		
			bb.position(0);
			bb.put(Schema.TYPE_ROW);
			bb.putInt(0); //next rec num
			bb.putInt(56 + (cols * 4)); //headEnd
			bb.putInt(Page.BLOCK_SIZE); //dataStart
			bb.putLong(System.currentTimeMillis()); //modTime
			bb.putInt(57 + (4 * cols)); //nullArray offset
			bb.putInt(49); //colIDListOff
			bb.putInt(53 + (4 * cols)); //rowIDListOff
			bb.putInt(57 + (4 * cols)); // offset Array offset
			bb.putInt(1); //freeSpaceListEntries
			bb.putInt(57 + (cols * 4)); //free space start
			bb.putInt(Page.BLOCK_SIZE - 1); //free space end
			bb.putInt(cols); //colIDListSize
			
			i = 0;
			while (i < cols)
			{
				bb.putInt(i);
				i++;
			}
		
			bb.putInt(0); //rowIDListSize
			bb.position(0);
			fc.write(bb);
			fc.force(false);
			FileManager.numBlocks.put(fn, (int)(fc.size() / Page.BLOCK_SIZE));
		}
	}
	
	private static class SendHierNewTableThread extends HRDBMSThread
	{	
		private byte[] ncBytes;
		private byte[] fnLenBytes;
		private byte[] fnBytes;
		ArrayList<Integer> devices;
		Object o;
		boolean ok = true;
		Transaction tx;
		
		public SendHierNewTableThread(byte[] ncBytes, byte[] fnLenBytes, byte[] fnBytes, ArrayList<Integer> devices, Object o, Transaction tx)
		{
			this.ncBytes = ncBytes;
			this.fnLenBytes = fnLenBytes;
			this.fnBytes = fnBytes;
			this.devices = devices;
			this.o = o;
			this.tx = tx;
		}
		
		public void run()
		{
			if (o instanceof String)
			{
				Socket sock = null;
				try
				{
					sock = CompressedSocket.newCompressedSocket((String)o, Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
					OutputStream out = sock.getOutputStream();
					byte[] outMsg = "NEWTABLE        ".getBytes("UTF-8");
					outMsg[8] = 0;
					outMsg[9] = 0;
					outMsg[10] = 0;
					outMsg[11] = 0;
					outMsg[12] = 0;
					outMsg[13] = 0;
					outMsg[14] = 0;
					outMsg[15] = 0;
					out.write(outMsg);
					out.write(longToBytes(tx.number()));
					out.write(ncBytes);
					out.write(fnLenBytes);
					out.write(fnBytes);
					ObjectOutputStream objOut = new ObjectOutputStream(out);
					objOut.writeObject(devices);
					ArrayList<Object> alo = new ArrayList<Object>(1);
					alo.add(o);
					objOut.writeObject(alo);
					objOut.flush();
					out.flush();
					getConfirmation(sock);
					objOut.close();
					sock.close();
				}
				catch(Exception e)
				{
					HRDBMSWorker.logger.debug("", e);
					ok = false;
				}
			}
			else if (((ArrayList<Object>)o).size() > 0)
			{
				Socket sock = null;
				Object obj2 = ((ArrayList<Object>)o).get(0);
				while (obj2 instanceof ArrayList)
				{
					obj2 = ((ArrayList<Object>)obj2).get(0);
				}
				
				String hostname = (String)obj2;
				try
				{
					sock = CompressedSocket.newCompressedSocket(hostname, Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
					OutputStream out = sock.getOutputStream();
					byte[] outMsg = "NEWTABLE        ".getBytes("UTF-8");
					outMsg[8] = 0;
					outMsg[9] = 0;
					outMsg[10] = 0;
					outMsg[11] = 0;
					outMsg[12] = 0;
					outMsg[13] = 0;
					outMsg[14] = 0;
					outMsg[15] = 0;
					out.write(outMsg);
					out.write(longToBytes(tx.number()));
					out.write(ncBytes);
					out.write(fnLenBytes);
					out.write(fnBytes);
					ObjectOutputStream objOut = new ObjectOutputStream(out);
					objOut.writeObject(devices);
					objOut.writeObject((ArrayList<Object>)o);
					objOut.flush();
					out.flush();
					getConfirmation(sock);
					objOut.close();
					sock.close();
				}
				catch(Exception e)
				{
					HRDBMSWorker.logger.debug("", e);
					ok = false;
				}
			}
		}
		
		public boolean getOK()
		{
			return ok;
		}
	}
	
	private static class SendHierNewIndexThread extends HRDBMSThread
	{	
		private byte[] ncBytes;
		private byte[] uBytes;
		private byte[] fnLenBytes;
		private byte[] fnBytes;
		ArrayList<Integer> devices;
		Object o;
		boolean ok = true;
		Transaction tx;
		
		public SendHierNewIndexThread(byte[] ncBytes, byte[] uBytes, byte[] fnLenBytes, byte[] fnBytes, ArrayList<Integer> devices, Object o, Transaction tx)
		{
			this.ncBytes = ncBytes;
			this.uBytes = uBytes;
			this.fnLenBytes = fnLenBytes;
			this.fnBytes = fnBytes;
			this.devices = devices;
			this.o = o;
			this.tx = tx;
		}
		
		public void run()
		{
			if (o instanceof String)
			{
				Socket sock = null;
				try
				{
					sock = CompressedSocket.newCompressedSocket((String)o, Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
					OutputStream out = sock.getOutputStream();
					byte[] outMsg = "NEWINDEX        ".getBytes("UTF-8");
					outMsg[8] = 0;
					outMsg[9] = 0;
					outMsg[10] = 0;
					outMsg[11] = 0;
					outMsg[12] = 0;
					outMsg[13] = 0;
					outMsg[14] = 0;
					outMsg[15] = 0;
					out.write(outMsg);
					out.write(longToBytes(tx.number()));
					out.write(ncBytes);
					out.write(uBytes);
					out.write(fnLenBytes);
					out.write(fnBytes);
					ObjectOutputStream objOut = new ObjectOutputStream(out);
					objOut.writeObject(devices);
					ArrayList<Object> alo = new ArrayList<Object>(1);
					alo.add(o);
					objOut.writeObject(alo);
					objOut.flush();
					out.flush();
					getConfirmation(sock);
					objOut.close();
					sock.close();
				}
				catch(Exception e)
				{
					HRDBMSWorker.logger.debug("", e);
					ok = false;
				}
			}
			else if (((ArrayList<Object>)o).size() > 0)
			{
				Socket sock = null;
				Object obj2 = ((ArrayList<Object>)o).get(0);
				while (obj2 instanceof ArrayList)
				{
					obj2 = ((ArrayList<Object>)obj2).get(0);
				}
				
				String hostname = (String)obj2;
				try
				{
					sock = CompressedSocket.newCompressedSocket(hostname, Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
					OutputStream out = sock.getOutputStream();
					byte[] outMsg = "NEWINDEX        ".getBytes("UTF-8");
					outMsg[8] = 0;
					outMsg[9] = 0;
					outMsg[10] = 0;
					outMsg[11] = 0;
					outMsg[12] = 0;
					outMsg[13] = 0;
					outMsg[14] = 0;
					outMsg[15] = 0;
					out.write(outMsg);
					out.write(longToBytes(tx.number()));
					out.write(ncBytes);
					out.write(uBytes);
					out.write(fnLenBytes);
					out.write(fnBytes);
					ObjectOutputStream objOut = new ObjectOutputStream(out);
					objOut.writeObject(devices);
					objOut.writeObject((ArrayList<Object>)o);
					objOut.flush();
					out.flush();
					getConfirmation(sock);
					objOut.close();
					sock.close();
				}
				catch(Exception e)
				{
					HRDBMSWorker.logger.debug("", e);
					ok = false;
				}
			}
		}
		
		public boolean getOK()
		{
			return ok;
		}
	}
	
	private static class SendHierPopIndexThread extends HRDBMSThread
	{	
		private byte[] txBytes;
		private byte[] fnLenBytes;
		private byte[] fnBytes;
		private byte[] fn2LenBytes;
		private byte[] fn2Bytes;
		private ArrayList<Integer> devices;
		private ArrayList<String> keys;
		private ArrayList<String> types;
		private ArrayList<Boolean> orders;
		private ArrayList<Integer> poses;
		private TreeMap<Integer, String> pos2Col;
		private HashMap<String, String> cols2Types;
		Object o;
		boolean ok = true;
		
		public SendHierPopIndexThread(byte[] txBytes, byte[] fnLenBytes, byte[] fnBytes, byte[] fn2LenBytes, byte[] fn2Bytes, ArrayList<Integer> devices, ArrayList<String> keys, ArrayList<String> types, ArrayList<Boolean> orders, ArrayList<Integer> poses, TreeMap<Integer, String> pos2Col, HashMap<String, String> cols2Types, Object o)
		{
			this.txBytes = txBytes;
			this.fnLenBytes = fnLenBytes;
			this.fnBytes = fnBytes;
			this.fn2LenBytes = fn2LenBytes;
			this.fn2Bytes = fn2Bytes;
			this.devices = devices;
			this.keys = keys;
			this.types = types;
			this.orders = orders;
			this.poses = poses;
			this.pos2Col = pos2Col;
			this.cols2Types = cols2Types;
			this.o = o;
		}
		
		public void run()
		{
			if (o instanceof String)
			{
				Socket sock = null;
				try
				{
					sock = CompressedSocket.newCompressedSocket((String)o, Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
					OutputStream out = sock.getOutputStream();
					byte[] outMsg = "POPINDEX        ".getBytes("UTF-8");
					outMsg[8] = 0;
					outMsg[9] = 0;
					outMsg[10] = 0;
					outMsg[11] = 0;
					outMsg[12] = 0;
					outMsg[13] = 0;
					outMsg[14] = 0;
					outMsg[15] = 0;
					out.write(outMsg);
					out.write(txBytes);
					out.write(fnLenBytes);
					out.write(fnBytes);
					out.write(fn2LenBytes);
					out.write(fn2Bytes);
					ObjectOutputStream objOut = new ObjectOutputStream(out);
					objOut.writeObject(devices);
					ArrayList<Object> alo = new ArrayList<Object>(1);
					alo.add(o);
					objOut.writeObject(keys);
					objOut.writeObject(types);
					objOut.writeObject(orders);
					objOut.writeObject(poses);
					objOut.writeObject(pos2Col);
					objOut.writeObject(cols2Types);
					objOut.writeObject(alo);
					objOut.flush();
					out.flush();
					getConfirmation(sock);
					objOut.close();
					sock.close();
				}
				catch(Exception e)
				{
					HRDBMSWorker.logger.debug("", e);
					ok = false;
				}
			}
			else if (((ArrayList<Object>)o).size() > 0)
			{
				Socket sock = null;
				Object obj2 = ((ArrayList<Object>)o).get(0);
				while (obj2 instanceof ArrayList)
				{
					obj2 = ((ArrayList<Object>)obj2).get(0);
				}
				
				String hostname = (String)obj2;
				try
				{
					sock = CompressedSocket.newCompressedSocket(hostname, Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
					OutputStream out = sock.getOutputStream();
					byte[] outMsg = "POPINDEX        ".getBytes("UTF-8");
					outMsg[8] = 0;
					outMsg[9] = 0;
					outMsg[10] = 0;
					outMsg[11] = 0;
					outMsg[12] = 0;
					outMsg[13] = 0;
					outMsg[14] = 0;
					outMsg[15] = 0;
					out.write(outMsg);
					out.write(txBytes);
					out.write(fnLenBytes);
					out.write(fnBytes);
					out.write(fn2LenBytes);
					out.write(fn2Bytes);
					ObjectOutputStream objOut = new ObjectOutputStream(out);
					objOut.writeObject(devices);
					objOut.writeObject(keys);
					objOut.writeObject(types);
					objOut.writeObject(orders);
					objOut.writeObject(poses);
					objOut.writeObject(pos2Col);
					objOut.writeObject(cols2Types);
					objOut.writeObject(o);
					objOut.flush();
					out.flush();
					getConfirmation(sock);
					objOut.close();
					sock.close();
				}
				catch(Exception e)
				{
					HRDBMSWorker.logger.debug("", e);
					ok = false;
				}
			}
		}
		
		public boolean getOK()
		{
			return ok;
		}
	}
	
	private void newTable()
	{
		byte[] fnLenBytes = new byte[4];
		int fnLen;
		byte[] fnBytes;
		String fn;
		byte[] ncBytes = new byte[4];
		int numCols;
		ArrayList<Integer> devices;
		ArrayList<Object> tree;
		Transaction tx;
		byte[] txBytes = new byte[8];
		
		try
		{
			readNonCoord(txBytes);
			tx = new Transaction(bytesToLong(txBytes));
			readNonCoord(ncBytes);
			numCols = bytesToInt(ncBytes);
			readNonCoord(fnLenBytes);
			fnLen = bytesToInt(fnLenBytes);
			fnBytes = new byte[fnLen];
			readNonCoord(fnBytes);
			fn = new String(fnBytes, "UTF-8");
			ObjectInputStream objIn = new ObjectInputStream(sock.getInputStream());
			devices = (ArrayList<Integer>)objIn.readObject();
			tree = (ArrayList<Object>)objIn.readObject();
			
			ArrayList<CreateTableThread> threads = new ArrayList<CreateTableThread>();
			for (int device : devices)
			{
				threads.add(new CreateTableThread(fn, numCols, device, tx));
			}
			
			for (CreateTableThread thread : threads)
			{
				thread.start();
			}
			
			/////////////////////////////////////////////
			Object obj = tree.get(0);
			while (obj instanceof ArrayList)
			{
				obj = ((ArrayList)obj).get(0);
			}
			
			removeFromTree((String)obj, tree, null); //also delete parents if now empty
			
			ArrayList<SendHierNewTableThread> threads2 = new ArrayList<SendHierNewTableThread>();
			for (Object o : tree)
			{
				threads2.add(new SendHierNewTableThread(ncBytes, fnLenBytes, fnBytes, devices, o, tx));
			}
			
			for (SendHierNewTableThread thread : threads2)
			{
				thread.start();
			}
			
			boolean allOK = true;
			for (SendHierNewTableThread thread : threads2)
			{
				while (true)
				{
					try
					{
						thread.join();
						break;
					}
					catch(InterruptedException e)
					{}
				}
				if (!thread.getOK())
				{
					allOK = false;
				}
			}
			///////////////////////////////
			
			for (CreateTableThread thread : threads)
			{
				thread.join();
				if (!thread.getOK())
				{
					allOK = false;
				}
			}
			
			if (allOK)
			{
				sendOK();
			}
			else
			{
				sendNo();
				try
				{
					sock.close();
				}
				catch(Exception f)
				{}
			}
		}
		catch(Exception e)
		{
			HRDBMSWorker.logger.debug("", e);
			HRDBMSWorker.logger.debug("Sending NO");
			sendNo();
			return;
		}
	}
	
	private void newIndex()
	{
		byte[] fnLenBytes = new byte[4];
		int fnLen;
		byte[] fnBytes;
		String fn;
		byte[] ncBytes = new byte[4];
		int numCols;
		byte[] uBytes = new byte[4];
		int unique;
		ArrayList<Integer> devices;
		ArrayList<Object> tree;
		Transaction tx;
		byte[] txBytes = new byte[8];
		
		try
		{
			readNonCoord(txBytes);
			tx = new Transaction(bytesToLong(txBytes));
			readNonCoord(ncBytes);
			numCols = bytesToInt(ncBytes);
			readNonCoord(uBytes);
			unique = bytesToInt(uBytes);
			readNonCoord(fnLenBytes);
			fnLen = bytesToInt(fnLenBytes);
			fnBytes = new byte[fnLen];
			readNonCoord(fnBytes);
			fn = new String(fnBytes, "UTF-8");
			ObjectInputStream objIn = new ObjectInputStream(sock.getInputStream());
			devices = (ArrayList<Integer>)objIn.readObject();
			tree = (ArrayList<Object>)objIn.readObject();
			
			ArrayList<CreateIndexThread> threads = new ArrayList<CreateIndexThread>();
			for (int device : devices)
			{
				threads.add(new CreateIndexThread(fn, numCols, device, unique, tx));
			}
			
			for (CreateIndexThread thread : threads)
			{
				thread.start();
			}
			
			/////////////////////////////////////////////
			Object obj = tree.get(0);
			while (obj instanceof ArrayList)
			{
				obj = ((ArrayList)obj).get(0);
			}

			removeFromTree((String)obj, tree, null); //also delete parents if now empty

			ArrayList<SendHierNewIndexThread> threads2 = new ArrayList<SendHierNewIndexThread>();	
			for (Object o : tree)
			{
				threads2.add(new SendHierNewIndexThread(ncBytes, uBytes, fnLenBytes, fnBytes, devices, o, tx));
			}

			for (SendHierNewIndexThread thread : threads2)
			{
				thread.start();
			}

			boolean allOK = true;
			for (SendHierNewIndexThread thread : threads2)
			{
				while (true)
				{
					try
					{
						thread.join();
						break;
					}
					catch(InterruptedException e)
					{}
				}
				if (!thread.getOK())
				{
					allOK = false;
				}
			}
			///////////////////////////////
			
			for (CreateIndexThread thread : threads)
			{
				thread.join();
				if (!thread.getOK())
				{
					allOK = false;
				}
			}
			
			if (allOK)
			{
				sendOK();
			}
			else
			{
				sendNo();
				try
				{
					sock.close();
				}
				catch(Exception f)
				{}
			}
		}
		catch(Exception e)
		{
			HRDBMSWorker.logger.debug("", e);
			sendNo();
			return;
		}
	}
	
	public static class CreateIndexThread extends HRDBMSThread
	{
		private String indx;
		private int numCols;
		private int device;
		private int unique;
		private boolean ok = true;;
		private Exception e;
		Transaction tx;
		
		public CreateIndexThread(String indx, int numCols, int device, int unique, Transaction tx)
		{
			this.indx = indx;
			this.numCols = numCols;
			this.device = device;
			this.unique = unique;
			this.tx = tx;
		}
		
		public CreateIndexThread(String fn, int numCols, boolean unique, Transaction tx)
		{
			this.indx = indx;
			this.numCols = numCols;
			this.device = device;
			this.tx = tx;
			if (unique)
			{
				this.unique = 1;
			}
			else
			{
				this.unique = 0;
			}
			
			this.indx = fn.substring(fn.lastIndexOf("/") + 1);
			String devicePath = fn.substring(0, fn.lastIndexOf("/") + 1);
			int i = 0;
			while (true)
			{
				String path = new MetaData().getDevicePath(i);
				if (!path.endsWith("/"))
				{
					path += "/";
				}
				
				if (path.equals(devicePath))
				{
					this.device = i;
					break;
				}
				
				i++;
			}
		}
		
		public void run()
		{
			try
			{
				createIndexHeader(indx, numCols, device, unique, tx);
			}
			catch(Exception e)
			{
				HRDBMSWorker.logger.debug("", e);
				this.e = e;
				ok = false;
			}
		}
		
		public boolean getOK()
		{
			return ok;
		}
		
		public Exception getException()
		{
			return e;
		}
		
		private void createIndexHeader(String indx, int numCols, int device, int unique, Transaction tx) throws Exception
		{
			String fn = new MetaData().getDevicePath(device);
			if (!fn.endsWith("/"))
			{
				fn += "/";
			}
			fn += indx;

			FileChannel fc = FileManager.getFile(fn);
			if (fc.size() > 0)
			{
				fc.truncate(0);
			}
			if (tx != null)
			{
				tx.truncate(new Block(fn, 0));
				tx.commit();
			}
			BufferManager.invalidateFile(fn);
			FileManager.numBlocks.put(fn, 0);
			ByteBuffer data = ByteBuffer.allocate(Page.BLOCK_SIZE);
		
			data.position(0);
			data.putInt(numCols); // num key cols
			if (unique != 0)
			{
				data.put((byte)1); // unique
			}
			else
			{
				data.put((byte)0); //not unique
			}
			data.position(9); // first free byte @ 5
			data.putInt(Page.BLOCK_SIZE - 1); // last free byte

			data.put((byte)0); // not a leaf
			// offset of next free value from start of record (13)
			data.position(18);
			data.putInt(0); // zero valid key values in this internal node
			int i = 0;
			while (i < 128)
			{
				data.putInt(0); // down block
				data.putInt(0); // down offset
				i++;
			}

			data.putInt(0); // no up block
			data.putInt(0); // no up offset

			// fill in first free val pointer
			int pos = data.position();
			data.position(14);
			data.putInt(pos - 13);
			data.position(5);
			data.putInt(pos); // first free byte
			data.position(pos);

			while (pos < (Page.BLOCK_SIZE))
			{
				data.put((byte)2); // fill page out with 2s
				pos++;
			}
		
			data.position(0);
			fc.write(data);
			fc.force(false);
			FileManager.numBlocks.put(fn, (int)(fc.size() / Page.BLOCK_SIZE));
		}
	}
	
	private void popIndex()
	{
		byte[] fnLenBytes = new byte[4];
		byte[] fn2LenBytes = new byte[4];
		int fnLen;
		byte[] fnBytes;
		byte[] fn2Bytes;
		String iFn;
		String tFn;
		ArrayList<Integer> devices;
		ArrayList<String> keys;
		ArrayList<String> types;
		ArrayList<Boolean> orders;
		ArrayList<Integer> poses;
		TreeMap<Integer, String> pos2Col;
		HashMap<String, String> cols2Types;
		byte[] txBytes = new byte[8];
		long txnum;
		ArrayList<Object> tree;
		
		try
		{
			readNonCoord(txBytes);
			txnum = bytesToLong(txBytes);
			readNonCoord(fnLenBytes);
			fnLen = bytesToInt(fnLenBytes);
			fnBytes = new byte[fnLen];
			readNonCoord(fnBytes);
			iFn = new String(fnBytes, "UTF-8");
			readNonCoord(fn2LenBytes);
			fnLen = bytesToInt(fn2LenBytes);
			fn2Bytes = new byte[fnLen];
			readNonCoord(fn2Bytes);
			tFn = new String(fn2Bytes, "UTF-8");
			ObjectInputStream objIn = new ObjectInputStream(sock.getInputStream());
			devices = (ArrayList<Integer>)objIn.readObject();
			keys = (ArrayList<String>)objIn.readObject();
			types = (ArrayList<String>)objIn.readObject();
			orders = (ArrayList<Boolean>)objIn.readObject();
			poses = (ArrayList<Integer>)objIn.readObject();
			pos2Col = (TreeMap<Integer, String>)objIn.readObject();
			cols2Types = (HashMap<String, String>)objIn.readObject();
			tree = (ArrayList<Object>)objIn.readObject();
			tx = new Transaction(txnum);
			
			ArrayList<PopIndexThread> threads = new ArrayList<PopIndexThread>();
			for (int device : devices)
			{
				threads.add(new PopIndexThread(iFn, tFn, device, keys, types, orders, poses, pos2Col, cols2Types, tx));
			}
			
			for (PopIndexThread pop : threads)
			{
				pop.start();
			}
			
			/////////////////////////////////////////////
			Object obj = tree.get(0);
			while (obj instanceof ArrayList)
			{
				obj = ((ArrayList)obj).get(0);
			}

			removeFromTree((String)obj, tree, null); //also delete parents if now empty

			ArrayList<SendHierPopIndexThread> threads2 = new ArrayList<SendHierPopIndexThread>();	
			for (Object o : tree)
			{
				threads2.add(new SendHierPopIndexThread(txBytes, fnLenBytes, fnBytes, fn2LenBytes, fn2Bytes, devices, keys, types, orders, poses, pos2Col, cols2Types, o));
			}

			for (SendHierPopIndexThread thread : threads2)
			{
				thread.start();
			}

			boolean allOK = true;
			for (SendHierPopIndexThread thread : threads2)
			{
				while (true)
				{
					try
					{
						thread.join();
						break;
					}
					catch(InterruptedException e)
					{}
				}
				if (!thread.getOK())
				{
					allOK = false;
				}
			}
			///////////////////////////////
			
			for (PopIndexThread pop : threads)
			{
				pop.join();
				if (!pop.getOK())
				{
					allOK = false;
				}
			}
			
			if (allOK)
			{
				sendOK();
			}
			else
			{
				sendNo();
			}
		}
		catch(Exception e)
		{
			sendNo();
			return;
		}
	}
	
	private static class PopIndexThread extends HRDBMSThread
	{
		private String iFn;
		private String tFn;
		private int device;
		private ArrayList<String> keys;
		private ArrayList<String> types;
		private ArrayList<Boolean> orders;
		private ArrayList<Integer> poses;
		private boolean ok = true;
		private TreeMap<Integer, String> pos2Col;
		private HashMap<String, String> cols2Types;
		private Transaction tx;
		
		public PopIndexThread(String iFn, String tFn, int device, ArrayList<String> keys, ArrayList<String> types, ArrayList<Boolean> orders, ArrayList<Integer> poses, TreeMap<Integer, String> pos2Col, HashMap<String, String> cols2Types, Transaction tx)
		{
			this.iFn = iFn;
			this.tFn = tFn;
			this.device = device;
			this.keys = keys;
			this.types = types;
			this.orders = orders;
			this.poses = poses;
			this.pos2Col = pos2Col;
			this.cols2Types = cols2Types;
			this.tx = tx;
		}
		
		public boolean getOK()
		{
			return ok;
		}
		
		public void run()
		{
			try
			{
				String fn = new MetaData().getDevicePath(device);
				if (!fn.endsWith("/"))
				{
					fn += "/";
				}
				fn += tFn;
				tFn = fn;
				
				Index idx = new Index(new MetaData().getDevicePath(device) + iFn, keys, types, orders);
				idx.setTransaction(tx);
				idx.open();
				LockManager.sLock(new Block(tFn, -1), tx.number());
				FileManager.getFile(tFn);
				int numBlocks = FileManager.numBlocks.get(tFn);
				int i = Schema.HEADER_SIZE;
				while (i < numBlocks)
				{
					LockManager.xLock(new Block(tFn, i), tx.number());
					i++;
				}
			
				HashMap<Integer, DataType> layout = new HashMap<Integer, DataType>();
				for (Map.Entry entry : pos2Col.entrySet())
				{
					String type = cols2Types.get(entry.getValue());
					DataType value = null;
					if (type.equals("INT"))
					{
						value = new DataType(DataType.INTEGER, 0, 0);
					}
					else if (type.equals("FLOAT"))
					{
						value = new DataType(DataType.DOUBLE, 0, 0);
					}
					else if (type.equals("CHAR"))
					{
						value = new DataType(DataType.VARCHAR, 0, 0);
					}
					else if (type.equals("LONG"))
					{
						value = new DataType(DataType.BIGINT, 0, 0);
					}
					else if (type.equals("DATE"))
					{
						value = new DataType(DataType.DATE, 0, 0);
					}

					layout.put((Integer)entry.getKey(), value);
				}

				Schema sch = new Schema(layout);
				int onPage = Schema.HEADER_SIZE;
				int lastRequested = Schema.HEADER_SIZE - 1;
				while (onPage < numBlocks)
				{
					if (lastRequested - onPage < PAGES_IN_ADVANCE)
					{
						Block[] toRequest = new Block[lastRequested + PREFETCH_REQUEST_SIZE < numBlocks ? PREFETCH_REQUEST_SIZE : numBlocks - lastRequested - 1];
						i = 0;
						while (i < toRequest.length)
						{
							toRequest[i] = new Block(tFn, lastRequested + i + 1);
							i++;
						}
						tx.requestPages(toRequest);
						lastRequested += toRequest.length;
					}

					tx.read(new Block(tFn, onPage++), sch);
					RowIterator rit = sch.rowIterator();
					while (rit.hasNext())
					{
						Row r = rit.next();
						if (!r.getCol(0).exists())
						{
							continue;
						}
						final ArrayList<FieldValue> row = new ArrayList<FieldValue>(types.size());
						RID rid = r.getRID();
						int j = 0;
						while (j < poses.size())
						{
							FieldValue fv = r.getCol(poses.get(j));
							row.add(fv);
							j++;
						}
					
						//insert into index
						FieldValue[] fva = new FieldValue[row.size()];
						int x = 0;
						while (x < row.size())
						{
							fva[x] = row.get(x);
							x++;
						}
						idx.insertNoLog(fva, rid);
					}
				}
			}
			catch(Exception e)
			{
				ok = false;
				HRDBMSWorker.logger.debug("", e);
			}
		}
	}
	
	private void getLoadMetaData() throws Exception
	{
		byte[] slBytes = new byte[4];
		readNonCoord(slBytes);
		int sLen = bytesToInt(slBytes);
		byte[] data = new byte[sLen];
		readNonCoord(data);
		String schema = new String(data, "UTF-8");
		readNonCoord(slBytes);
		sLen = bytesToInt(slBytes);
		data = new byte[sLen];
		readNonCoord(data);
		String table = new String(data, "UTF-8");
		LoadMetaData ldmd = ldmds.get(schema + "." + table);
		ObjectOutputStream objOut = new ObjectOutputStream(sock.getOutputStream());
		//numNodes = (Integer)objIn.readObject();
		//delimiter = (String)objIn.readObject();
		//pos2Col = (TreeMap<Integer, String>)objIn.readObject();
		//cols2Types = (HashMap<String, String>)objIn.readObject();
		//pos2Length = (HashMap<Integer, Integer>)objIn.readObject();
		//pmd = (PartitionMetaData)objIn.readObject();
		objOut.writeObject(new Integer(ldmd.numNodes));
		objOut.writeObject(ldmd.delimiter);
		objOut.writeObject(ldmd.pos2Col);
		objOut.writeObject(ldmd.cols2Types);
		objOut.writeObject(ldmd.pos2Length);
		objOut.writeObject(ldmd.pmd);
		objOut.writeObject(ldmd.workerNodes);
		objOut.writeObject(ldmd.coordNodes);
		objOut.flush();
		objOut.close();
		sock.close();
	}
	
	private void setLoadMetaData()
	{
		ArrayList<Object> tree = null;
		byte[] keyLength = new byte[4];
		int length;
		byte[] data;
		String key;
		LoadMetaData ldmd = null;
		try
		{
			readNonCoord(keyLength);
			length = bytesToInt(keyLength);
			data = new byte[length];
			readNonCoord(data);
			key = new String(data, "UTF-8");
			ObjectInputStream objIn = new ObjectInputStream(sock.getInputStream());
			tree = (ArrayList<Object>)objIn.readObject();
			ldmd = (LoadMetaData)objIn.readObject();
		}
		catch(Exception e)
		{
			sendNo();
			return;
		}
		
		ldmds.put(key, ldmd);
		
		Object obj = tree.get(0);
		while (obj instanceof ArrayList)
		{
			obj = ((ArrayList)obj).get(0);
		}
		
		removeFromTree((String)obj, tree, null); //also delete parents if now empty
		
		ArrayList<SendLDMDThread> threads = new ArrayList<SendLDMDThread>();
		for (Object o : tree)
		{
			threads.add(new SendLDMDThread(o, keyLength, data, ldmd));
		}
		
		for (SendLDMDThread thread : threads)
		{
			thread.start();
		}
		
		boolean allOK = true;
		for (SendLDMDThread thread : threads)
		{
			while (true)
			{
				try
				{
					thread.join();
					break;
				}
				catch(InterruptedException e)
				{}
			}
			if (!thread.sendOK())
			{
				allOK = false;
			}
		}
		
		if (allOK)
		{
			sendOK();
		}
		else
		{
			sendNo();
			try
			{
				sock.close();
			}
			catch(Exception f)
			{}
		}
	}
	
	private static class SendLDMDThread extends HRDBMSThread
	{
		private Object o;
		private boolean sendOK;
		private byte[] lenBytes;
		private byte[] data;
		private LoadMetaData ldmd;
		
		public SendLDMDThread(Object o, byte[] lenBytes, byte[] data, LoadMetaData ldmd)
		{
			this.o = o;
			this.lenBytes = lenBytes;
			this.data = data;
			this.ldmd = ldmd;
		}
		
		public boolean sendOK()
		{
			return sendOK;
		}
		
		public void run()
		{
			if (o instanceof String)
			{
				Socket sock = null;
				try
				{
					sock = CompressedSocket.newCompressedSocket((String)o, Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
					OutputStream out = sock.getOutputStream();
					byte[] outMsg = "SETLDMD         ".getBytes("UTF-8");
					outMsg[8] = 0;
					outMsg[9] = 0;
					outMsg[10] = 0;
					outMsg[11] = 0;
					outMsg[12] = 0;
					outMsg[13] = 0;
					outMsg[14] = 0;
					outMsg[15] = 0;
					out.write(outMsg);
					out.write(lenBytes);
					out.write(data);
					ObjectOutputStream objOut = new ObjectOutputStream(out);
					ArrayList<Object> alo = new ArrayList<Object>(1);
					alo.add(o);
					objOut.writeObject(alo);
					objOut.writeObject(ldmd);
					objOut.flush();
					out.flush();
					getConfirmation(sock);
					objOut.close();
					sock.close();
				}
				catch(Exception e)
				{
					sendOK = false;
					return;
				}
				sendOK = true;
			}
			else if (((ArrayList<Object>)o).size() > 0)
			{
				Socket sock = null;
				Object obj2 = ((ArrayList<Object>)o).get(0);
				while (obj2 instanceof ArrayList)
				{
					obj2 = ((ArrayList<Object>)obj2).get(0);
				}
				
				String hostname = (String)obj2;
				try
				{
					sock = CompressedSocket.newCompressedSocket(hostname, Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
					OutputStream out = sock.getOutputStream();
					byte[] outMsg = "SETLDMD         ".getBytes("UTF-8");
					outMsg[8] = 0;
					outMsg[9] = 0;
					outMsg[10] = 0;
					outMsg[11] = 0;
					outMsg[12] = 0;
					outMsg[13] = 0;
					outMsg[14] = 0;
					outMsg[15] = 0;
					out.write(outMsg);
					out.write(lenBytes);
					out.write(data);
					ObjectOutputStream objOut = new ObjectOutputStream(out);
					objOut.writeObject((ArrayList<Object>)o);
					objOut.writeObject(ldmd);
					objOut.flush();
					out.flush();
					getConfirmation(sock);
					objOut.close();
					sock.close();
				}
				catch(Exception e)
				{
					sendOK = false;
					return;
				}
				
				sendOK = true;
			}
		}
	}
	
	private void delLoadMetaData()
	{
		ArrayList<Object> tree = null;
		byte[] keyLength = new byte[4];
		int length;
		byte[] data;
		String key;
		try
		{
			readNonCoord(keyLength);
			length = bytesToInt(keyLength);
			data = new byte[length];
			readNonCoord(data);
			key = new String(data, "UTF-8");
			ObjectInputStream objIn = new ObjectInputStream(sock.getInputStream());
			tree = (ArrayList<Object>)objIn.readObject();
		}
		catch(Exception e)
		{
			sendNo();
			return;
		}
		
		ldmds.remove(key);
		
		Object obj = tree.get(0);
		while (obj instanceof ArrayList)
		{
			obj = ((ArrayList)obj).get(0);
		}
		
		removeFromTree((String)obj, tree, null); //also delete parents if now empty
		
		ArrayList<SendRemoveLDMDThread> threads = new ArrayList<SendRemoveLDMDThread>();
		for (Object o : tree)
		{
			threads.add(new SendRemoveLDMDThread(o, keyLength, data));
		}
		
		for (SendRemoveLDMDThread thread : threads)
		{
			thread.start();
		}
		
		boolean allOK = true;
		for (SendRemoveLDMDThread thread : threads)
		{
			while (true)
			{
				try
				{
					thread.join();
					break;
				}
				catch(InterruptedException e)
				{}
			}
			if (!thread.sendOK())
			{
				allOK = false;
			}
		}
		
		if (allOK)
		{
			sendOK();
		}
		else
		{
			sendNo();
			try
			{
				sock.close();
			}
			catch(Exception f)
			{}
		}
	}
	
	private static class SendRemoveLDMDThread extends HRDBMSThread
	{
		private Object o;
		private boolean sendOK;
		private byte[] lenBytes;
		private byte[] data;
		
		public SendRemoveLDMDThread(Object o, byte[] lenBytes, byte[] data)
		{
			this.o = o;
			this.lenBytes = lenBytes;
			this.data = data;
		}
		
		public boolean sendOK()
		{
			return sendOK;
		}
		
		public void run()
		{
			if (o instanceof String)
			{
				Socket sock = null;
				try
				{
					sock = CompressedSocket.newCompressedSocket((String)o, Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
					OutputStream out = sock.getOutputStream();
					byte[] outMsg = "DELLDMD         ".getBytes("UTF-8");
					outMsg[8] = 0;
					outMsg[9] = 0;
					outMsg[10] = 0;
					outMsg[11] = 0;
					outMsg[12] = 0;
					outMsg[13] = 0;
					outMsg[14] = 0;
					outMsg[15] = 0;
					out.write(outMsg);
					out.write(lenBytes);
					out.write(data);
					ObjectOutputStream objOut = new ObjectOutputStream(out);
					ArrayList<Object> alo = new ArrayList<Object>(1);
					alo.add(o);
					objOut.writeObject(alo);
					objOut.flush();
					out.flush();
					getConfirmation(sock);
					objOut.close();
					sock.close();
				}
				catch(Exception e)
				{
					sendOK = false;
					return;
				}
				sendOK = true;
			}
			else if (((ArrayList<Object>)o).size() > 0)
			{
				Socket sock = null;
				Object obj2 = ((ArrayList<Object>)o).get(0);
				while (obj2 instanceof ArrayList)
				{
					obj2 = ((ArrayList<Object>)obj2).get(0);
				}
				
				String hostname = (String)obj2;
				try
				{
					sock = CompressedSocket.newCompressedSocket(hostname, Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
					OutputStream out = sock.getOutputStream();
					byte[] outMsg = "DELLDMD         ".getBytes("UTF-8");
					outMsg[8] = 0;
					outMsg[9] = 0;
					outMsg[10] = 0;
					outMsg[11] = 0;
					outMsg[12] = 0;
					outMsg[13] = 0;
					outMsg[14] = 0;
					outMsg[15] = 0;
					out.write(outMsg);
					out.write(lenBytes);
					out.write(data);
					ObjectOutputStream objOut = new ObjectOutputStream(out);
					objOut.writeObject((ArrayList<Object>)o);
					objOut.flush();
					out.flush();
					getConfirmation(sock);
					objOut.close();
					sock.close();
				}
				catch(Exception e)
				{
					sendOK = false;
					return;
				}
				
				sendOK = true;
			}
		}
	}
	
	private void delFileTable()
	{
		ArrayList<Object> tree = null;
		ArrayList<String> tables = null;
		try
		{
			ObjectInputStream objIn = new ObjectInputStream(sock.getInputStream());
			tree = (ArrayList<Object>)objIn.readObject();
			tables = (ArrayList<String>)objIn.readObject();
		}
		catch(Exception e)
		{
			sendNo();
			return;
		}
		
		ArrayList<String> paths = getDataPaths();
		for (String path : paths)
		{
			try
			{
				ArrayList<String> files = getTableFilesInPath(path);
				for (String file : files)
				{
					String table = file.substring(0, file.indexOf(".tbl"));
					if (!tables.contains(table))
					{
						FileManager.removeFile(path + file);
					}
				}
			}
			catch(Exception e)
			{
				HRDBMSWorker.logger.debug("", e);
				sendNo();
			}
		}
		
		Object obj = tree.get(0);
		while (obj instanceof ArrayList)
		{
			obj = ((ArrayList)obj).get(0);
		}
		
		removeFromTree((String)obj, tree, null); //also delete parents if now empty
		
		ArrayList<SendDelFiTThread> threads = new ArrayList<SendDelFiTThread>();
		for (Object o : tree)
		{
			threads.add(new SendDelFiTThread(o, tables));
		}
		
		for (SendDelFiTThread thread : threads)
		{
			thread.start();
		}
		
		boolean allOK = true;
		for (SendDelFiTThread thread : threads)
		{
			while (true)
			{
				try
				{
					thread.join();
					break;
				}
				catch(InterruptedException e)
				{}
			}
			if (!thread.sendOK())
			{
				allOK = false;
			}
		}
		
		if (allOK)
		{
			sendOK();
		}
		else
		{
			sendNo();
			try
			{
				sock.close();
			}
			catch(Exception f)
			{}
		}
	}
	
	private void delFileIndex()
	{
		ArrayList<Object> tree = null;
		ArrayList<String> indexes = null;
		try
		{
			ObjectInputStream objIn = new ObjectInputStream(sock.getInputStream());
			tree = (ArrayList<Object>)objIn.readObject();
			indexes = (ArrayList<String>)objIn.readObject();
		}
		catch(Exception e)
		{
			sendNo();
			return;
		}
		
		ArrayList<String> paths = getDataPaths();
		for (String path : paths)
		{
			try
			{
				ArrayList<String> files = getIndexFilesInPath(path);
				for (String file : files)
				{
					String index = file.substring(0, file.indexOf(".indx"));
					if (!indexes.contains(index))
					{
						FileManager.removeFile(path + file);
					}
				}
			}
			catch(Exception e)
			{
				HRDBMSWorker.logger.debug("", e);
				sendNo();
			}
		}
		
		Object obj = tree.get(0);
		while (obj instanceof ArrayList)
		{
			obj = ((ArrayList)obj).get(0);
		}
		
		removeFromTree((String)obj, tree, null); //also delete parents if now empty
		
		ArrayList<SendDelFiIThread> threads = new ArrayList<SendDelFiIThread>();
		for (Object o : tree)
		{
			threads.add(new SendDelFiIThread(o, indexes));
		}
		
		for (SendDelFiIThread thread : threads)
		{
			thread.start();
		}
		
		boolean allOK = true;
		for (SendDelFiIThread thread : threads)
		{
			while (true)
			{
				try
				{
					thread.join();
					break;
				}
				catch(InterruptedException e)
				{}
			}
			if (!thread.sendOK())
			{
				allOK = false;
			}
		}
		
		if (allOK)
		{
			sendOK();
		}
		else
		{
			sendNo();
			try
			{
				sock.close();
			}
			catch(Exception f)
			{}
		}
	}
	
	private static class SendDelFiTThread extends HRDBMSThread
	{
		private Object o;
		private boolean sendOK;
		private ArrayList<String> tables;
		
		public SendDelFiTThread(Object o, ArrayList<String> tables)
		{
			this.o = o;
			this.tables = tables;
		}
		
		public boolean sendOK()
		{
			return sendOK;
		}
		
		public void run()
		{
			if (o instanceof String)
			{
				Socket sock = null;
				try
				{
					sock = CompressedSocket.newCompressedSocket((String)o, Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
					OutputStream out = sock.getOutputStream();
					byte[] outMsg = "DELFITBL        ".getBytes("UTF-8");
					outMsg[8] = 0;
					outMsg[9] = 0;
					outMsg[10] = 0;
					outMsg[11] = 0;
					outMsg[12] = 0;
					outMsg[13] = 0;
					outMsg[14] = 0;
					outMsg[15] = 0;
					out.write(outMsg);
					ObjectOutputStream objOut = new ObjectOutputStream(out);
					ArrayList<Object> alo = new ArrayList<Object>(1);
					alo.add(o);
					objOut.writeObject(alo);
					objOut.writeObject(tables);
					objOut.flush();
					out.flush();
					getConfirmation(sock);
					objOut.close();
					sock.close();
				}
				catch(Exception e)
				{
					sendOK = false;
					return;
				}
				sendOK = true;
			}
			else if (((ArrayList<Object>)o).size() > 0)
			{
				Socket sock = null;
				Object obj2 = ((ArrayList<Object>)o).get(0);
				while (obj2 instanceof ArrayList)
				{
					obj2 = ((ArrayList<Object>)obj2).get(0);
				}
				
				String hostname = (String)obj2;
				try
				{
					sock = CompressedSocket.newCompressedSocket(hostname, Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
					OutputStream out = sock.getOutputStream();
					byte[] outMsg = "DELFITBL        ".getBytes("UTF-8");
					outMsg[8] = 0;
					outMsg[9] = 0;
					outMsg[10] = 0;
					outMsg[11] = 0;
					outMsg[12] = 0;
					outMsg[13] = 0;
					outMsg[14] = 0;
					outMsg[15] = 0;
					out.write(outMsg);
					ObjectOutputStream objOut = new ObjectOutputStream(out);
					objOut.writeObject((ArrayList<Object>)o);
					objOut.writeObject(tables);
					objOut.flush();
					out.flush();
					getConfirmation(sock);
					objOut.close();
					sock.close();
				}
				catch(Exception e)
				{
					sendOK = false;
					return;
				}
				
				sendOK = true;
			}
		}
	}
	
	private static class SendDelFiIThread extends HRDBMSThread
	{
		private Object o;
		private boolean sendOK;
		private ArrayList<String> indexes;
		
		public SendDelFiIThread(Object o, ArrayList<String> indexes)
		{
			this.o = o;
			this.indexes = indexes;
		}
		
		public boolean sendOK()
		{
			return sendOK;
		}
		
		public void run()
		{
			if (o instanceof String)
			{
				Socket sock = null;
				try
				{
					sock = CompressedSocket.newCompressedSocket((String)o, Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
					OutputStream out = sock.getOutputStream();
					byte[] outMsg = "DELFIIDX        ".getBytes("UTF-8");
					outMsg[8] = 0;
					outMsg[9] = 0;
					outMsg[10] = 0;
					outMsg[11] = 0;
					outMsg[12] = 0;
					outMsg[13] = 0;
					outMsg[14] = 0;
					outMsg[15] = 0;
					out.write(outMsg);
					ObjectOutputStream objOut = new ObjectOutputStream(out);
					ArrayList<Object> alo = new ArrayList<Object>(1);
					alo.add(o);
					objOut.writeObject(alo);
					objOut.writeObject(indexes);
					objOut.flush();
					out.flush();
					getConfirmation(sock);
					objOut.close();
					sock.close();
				}
				catch(Exception e)
				{
					sendOK = false;
					return;
				}
				sendOK = true;
			}
			else if (((ArrayList<Object>)o).size() > 0)
			{
				Socket sock = null;
				Object obj2 = ((ArrayList<Object>)o).get(0);
				while (obj2 instanceof ArrayList)
				{
					obj2 = ((ArrayList<Object>)obj2).get(0);
				}
				
				String hostname = (String)obj2;
				try
				{
					sock = CompressedSocket.newCompressedSocket(hostname, Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
					OutputStream out = sock.getOutputStream();
					byte[] outMsg = "DELFIIDX        ".getBytes("UTF-8");
					outMsg[8] = 0;
					outMsg[9] = 0;
					outMsg[10] = 0;
					outMsg[11] = 0;
					outMsg[12] = 0;
					outMsg[13] = 0;
					outMsg[14] = 0;
					outMsg[15] = 0;
					out.write(outMsg);
					ObjectOutputStream objOut = new ObjectOutputStream(out);
					objOut.writeObject((ArrayList<Object>)o);
					objOut.writeObject(indexes);
					objOut.flush();
					out.flush();
					getConfirmation(sock);
					objOut.close();
					sock.close();
				}
				catch(Exception e)
				{
					sendOK = false;
					return;
				}
				
				sendOK = true;
			}
		}
	}
	
	private static ArrayList<String> getDataPaths()
	{
		String paths = HRDBMSWorker.getHParms().getProperty("data_directories");
		StringTokenizer tokens = new StringTokenizer(paths, ",", false);
		ArrayList<String> retval = new ArrayList<String>();
		while (tokens.hasMoreTokens())
		{
			String token = tokens.nextToken();
			if (!token.endsWith("/"))
			{
				token += "/";
			}
			
			retval.add(token);
		}
		
		return retval;
	}
	
	private static ArrayList<String> getTableFilesInPath(String path) throws Exception
	{
		final ArrayList<Path> files = new ArrayList<Path>();
		String search = path;
		if (!search.endsWith("/"))
		{
			search += "/";
		}
		
		search += "*.*.tbl.*";
		final PathMatcher matcher = FileSystems.getDefault().getPathMatcher("glob:" + search);
	    Files.walkFileTree(Paths.get("/"), new SimpleFileVisitor<Path>() {
	        @Override
	        public FileVisitResult visitFile(Path file, java.nio.file.attribute.BasicFileAttributes attrs) throws IOException {
	            if (matcher.matches(file)) {
	                files.add(file);
	            }
	            return FileVisitResult.CONTINUE;
	        }

	        @Override
	        public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
	            return FileVisitResult.CONTINUE;
	        }
	    });
	    
	    ArrayList<String> retval = new ArrayList<String>();
	    for (Path file : files)
	    {
	    	retval.add(file.toAbsolutePath().toString());
	    }
	    
	    return retval;
	}
	
	private static ArrayList<String> getIndexFilesInPath(String path) throws Exception
	{
		final ArrayList<Path> files = new ArrayList<Path>();
		String search = path;
		if (!search.endsWith("/"))
		{
			search += "/";
		}
		
		search += "*.*.indx.*";
		final PathMatcher matcher = FileSystems.getDefault().getPathMatcher("glob:" + search);
	    Files.walkFileTree(Paths.get("/"), new SimpleFileVisitor<Path>() {
	        @Override
	        public FileVisitResult visitFile(Path file, java.nio.file.attribute.BasicFileAttributes attrs) throws IOException {
	            if (matcher.matches(file)) {
	                files.add(file);
	            }
	            return FileVisitResult.CONTINUE;
	        }

	        @Override
	        public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
	            return FileVisitResult.CONTINUE;
	        }
	    });
	    
	    ArrayList<String> retval = new ArrayList<String>();
	    for (Path file : files)
	    {
	    	retval.add(file.toAbsolutePath().toString());
	    }
	    
	    return retval;
	}
	
	private void reorg()
	{
		ArrayList<Object> tree = null;
		byte[] txBytes = new byte[8];
		long txNum = -1;
		byte[] schemaLenBytes = new byte[4];
		byte[] schemaBytes;
		String schema;
		byte[] tableLenBytes = new byte[4];
		byte[] tableBytes;
		String table;
		ArrayList<Index> indexes;
		HashMap<String, String> cols2Types;
		TreeMap<Integer, String> pos2Col;
		ArrayList<Boolean> uniques;
		try
		{
			readNonCoord(schemaLenBytes);
			schemaBytes = new byte[bytesToInt(schemaLenBytes)];
			readNonCoord(schemaBytes);
			schema = new String(schemaBytes, "UTF-8");
			readNonCoord(tableLenBytes);
			tableBytes = new byte[bytesToInt(tableLenBytes)];
			readNonCoord(tableBytes);
			table = new String(tableBytes, "UTF-8");
			readNonCoord(txBytes);
			txNum = bytesToLong(txBytes);
			ObjectInputStream objIn = new ObjectInputStream(sock.getInputStream());
			tree = (ArrayList<Object>)objIn.readObject();
			indexes = (ArrayList<Index>)objIn.readObject();
			cols2Types = (HashMap<String, String>)objIn.readObject();
			pos2Col = (TreeMap<Integer, String>)objIn.readObject();
			uniques = (ArrayList<Boolean>)objIn.readObject();
		}
		catch(Exception e)
		{
			sendNo();
			return;
		}
		
		Object obj = tree.get(0);
		while (obj instanceof ArrayList)
		{
			obj = ((ArrayList)obj).get(0);
		}
		
		removeFromTree((String)obj, tree, null); //also delete parents if now empty
		
		ArrayList<SendReorgThread> threads = new ArrayList<SendReorgThread>();
		for (Object o : tree)
		{
			threads.add(new SendReorgThread(o, tx, schemaLenBytes, schemaBytes, tableLenBytes, tableBytes, indexes, cols2Types, pos2Col, uniques));
		}
		
		for (SendReorgThread thread : threads)
		{
			thread.start();
		}
		
		Transaction tx = new Transaction(txNum);
		boolean allOK = true;
		try
		{
			doReorg(schema, table, indexes, tx, cols2Types, pos2Col, uniques);
			tx.commit();
		}
		catch(Exception e)
		{
			try
			{
				tx.rollback();
			}
			catch(Exception f)
			{}
			allOK = false;
		}
		
		for (SendReorgThread thread : threads)
		{
			while (true)
			{
				try
				{
					thread.join();
					break;
				}
				catch(InterruptedException e)
				{}
			}
			if (!thread.sendOK())
			{
				allOK = false;
			}
		}
		
		if (allOK)
		{
			sendOK();
		}
		else
		{
			sendNo();
			try
			{
				sock.close();
			}
			catch(Exception f)
			{}
		}
	}
	
	private static class SendReorgThread extends HRDBMSThread
	{
		private Object o;
		private Transaction tx;
		private boolean sendOK;
		private byte[] schemaLenBytes;
		private byte[] schemaBytes;
		private byte[] tableLenBytes;
		private byte[] tableBytes;
		private ArrayList<Index> indexes;
		private HashMap<String, String> cols2Types;
		private TreeMap<Integer, String> pos2Col;
		private ArrayList<Boolean> uniques;
		
		public SendReorgThread(Object o, Transaction tx, byte[] schemaLenBytes, byte[] schemaBytes, byte[] tableLenBytes, byte[] tableBytes, ArrayList<Index> indexes, HashMap<String, String> cols2Types, TreeMap<Integer, String> pos2Col, ArrayList<Boolean> uniques)
		{
			this.o = o;
			this.tx = tx;
			this.schemaLenBytes = schemaLenBytes;
			this.schemaBytes = schemaBytes;
			this.tableLenBytes = tableLenBytes;
			this.tableBytes = tableBytes;
			this.indexes = indexes;
			this.cols2Types = cols2Types;
			this.pos2Col = pos2Col;
			this.uniques = uniques;
		}
		
		public boolean sendOK()
		{
			return sendOK;
		}
		
		public void run()
		{
			if (o instanceof String)
			{
				Socket sock = null;
				try
				{
					sock = CompressedSocket.newCompressedSocket((String)o, Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
					OutputStream out = sock.getOutputStream();
					byte[] outMsg = "REORG           ".getBytes("UTF-8");
					outMsg[8] = 0;
					outMsg[9] = 0;
					outMsg[10] = 0;
					outMsg[11] = 0;
					outMsg[12] = 0;
					outMsg[13] = 0;
					outMsg[14] = 0;
					outMsg[15] = 0;
					out.write(outMsg);
					out.write(schemaLenBytes);
					out.write(schemaBytes);
					out.write(tableLenBytes);
					out.write(tableBytes);
					out.write(longToBytes(tx.number()));
					ObjectOutputStream objOut = new ObjectOutputStream(out);
					ArrayList<Object> alo = new ArrayList<Object>(1);
					alo.add(o);
					objOut.writeObject(alo);
					objOut.writeObject(indexes);
					objOut.writeObject(cols2Types);
					objOut.writeObject(pos2Col);
					objOut.writeObject(uniques);
					objOut.flush();
					out.flush();
					getConfirmation(sock);
					objOut.close();
					sock.close();
				}
				catch(Exception e)
				{
					sendOK = false;
					return;
				}
				sendOK = true;
			}
			else if (((ArrayList<Object>)o).size() > 0)
			{
				Socket sock = null;
				Object obj2 = ((ArrayList<Object>)o).get(0);
				while (obj2 instanceof ArrayList)
				{
					obj2 = ((ArrayList<Object>)obj2).get(0);
				}
				
				String hostname = (String)obj2;
				try
				{
					sock = CompressedSocket.newCompressedSocket(hostname, Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
					OutputStream out = sock.getOutputStream();
					byte[] outMsg = "REORG           ".getBytes("UTF-8");
					outMsg[8] = 0;
					outMsg[9] = 0;
					outMsg[10] = 0;
					outMsg[11] = 0;
					outMsg[12] = 0;
					outMsg[13] = 0;
					outMsg[14] = 0;
					outMsg[15] = 0;
					out.write(outMsg);
					out.write(schemaLenBytes);
					out.write(schemaBytes);
					out.write(tableLenBytes);
					out.write(tableBytes);
					out.write(longToBytes(tx.number()));
					ObjectOutputStream objOut = new ObjectOutputStream(out);
					objOut.writeObject((ArrayList<Object>)o);
					objOut.writeObject(indexes);
					objOut.flush();
					out.flush();
					getConfirmation(sock);
					objOut.close();
					sock.close();
				}
				catch(Exception e)
				{
					sendOK = false;
					return;
				}
				
				sendOK = true;
			}
		}
	}
	
	private void doReorg(String schema, String table, ArrayList<Index> indexes, Transaction tx, HashMap<String, String> cols2Types, TreeMap<Integer, String> pos2Col, ArrayList<Boolean> uniques) throws Exception
	{
		String dirList = HRDBMSWorker.getHParms().getProperty("data_directories");
		FastStringTokenizer tokens = new FastStringTokenizer(dirList, ",", false);
		String[] dirs = tokens.allTokens();
		Exception e = null;
		
		ArrayList<ReorgThread> threads = new ArrayList<ReorgThread>();
		boolean allOK = true;
		for (String dir : dirs)
		{
			//threads.add(new ReorgThread(dir, schema, table, indexes, tx));
			ReorgThread thread = new ReorgThread(dir, schema, table, indexes, tx, cols2Types, pos2Col, uniques);
			thread.start();
			thread.join();
			if (!thread.getOK())
			{
				allOK = false;
				e = thread.getException();
				break;
			}
			tx.releaseLocksAndPins();
		}
		
		//for (ReorgThread thread : threads)
		//{
		//	thread.start();
		//}
		
		//for (ReorgThread thread : threads)
		//{
		//	thread.join();
		//	if (!thread.getOK())
		//	{
		//		allOK = false;
		//		e = thread.getException();
		//	}
		//}
		
		if (!allOK)
		{
			throw e;
		}
	}
	
	private static class ReorgThread extends HRDBMSThread
	{
		private String dir;
		private String schema;
		private String table;
		private ArrayList<Index> indexes;
		private Transaction tx;
		private boolean ok = true;
		private Exception e;
		private HashMap<String, String> cols2Types;
		private TreeMap<Integer, String> pos2Col;
		private ArrayList<Boolean> uniques;
		
		public ReorgThread(String dir, String schema, String table, ArrayList<Index> indexes, Transaction tx, HashMap<String, String> cols2Types, TreeMap<Integer, String> pos2Col, ArrayList<Boolean> uniques)
		{
			this.dir = dir;
			this.schema = schema;
			this.table = table;
			this.indexes = indexes;
			this.tx = tx;
			this.cols2Types = cols2Types;
			this.pos2Col = pos2Col;
			this.uniques = uniques;
		}
		
		public void run()
		{
			try
			{
				String fn = dir;
				if (!fn.endsWith("/"))
				{
					fn += "/";
				}
			
				fn += (schema + "." + table + ".tbl");
			
				if (!FileManager.fileExists(fn))
				{
					return;
				}
			
				HashMap<String, Integer> cols2Pos = new HashMap<String, Integer>();
				for (Map.Entry entry : pos2Col.entrySet())
				{
					cols2Pos.put((String)entry.getValue(), (Integer)entry.getKey());
				}
			
				ArrayList<String> indexFNs = new ArrayList<String>();
				for (Index index : indexes)
				{
					String temp = dir;
					if (!temp.endsWith("/"))
					{
						temp += "/";
					}
				
					temp += index.getFileName();
					indexFNs.add(temp);
				}
			
				Block tSize = new Block(fn, -1);
				LockManager.xLock(tSize, tx.number());
				FileManager.getFile(fn);
				int blocks = FileManager.numBlocks.get(fn);
				int i = 4096;
				while (i < blocks)
				{
					LockManager.sLock(new Block(fn, i), tx.number());
					i++;
				}
			
				for (String fn2 : indexFNs)
				{
					Block iSize = new Block(fn2, -1);
					LockManager.xLock(iSize, tx.number());
					FileManager.getFile(fn2);
					int blocks2 = FileManager.numBlocks.get(fn2);
					i = 0;
					while (i < blocks)
					{
						LockManager.sLock(new Block(fn2, i), tx.number());
						i++;
					}
				}
			
				//create new table and index files
				String newFN = fn + ".new";
				CreateTableThread createT = new CreateTableThread(fn, pos2Col.size(), null);
				createT.run();
				ArrayList<String> newIndexFNs = new ArrayList<String>();
				i = 0;
				for (String fn2 : indexFNs)
				{
					String temp = fn2 + ".new";
					CreateIndexThread createI = new CreateIndexThread(fn2, indexes.get(i).getKeys().size(), uniques.get(i), null);
					createI.run();
					newIndexFNs.add(temp);
					i++;
				}
			
				HashMap<Integer, DataType> layout = new HashMap<Integer, DataType>();
				for (Map.Entry entry : pos2Col.entrySet())
				{
					String type = cols2Types.get(entry.getValue());
					DataType value = null;
					if (type.equals("INT"))
					{
						value = new DataType(DataType.INTEGER, 0, 0);
					}
					else if (type.equals("FLOAT"))
					{
						value = new DataType(DataType.DOUBLE, 0, 0);
					}
					else if (type.equals("CHAR"))
					{
						value = new DataType(DataType.VARCHAR, 0, 0);
					}
					else if (type.equals("LONG"))
					{
						value = new DataType(DataType.BIGINT, 0, 0);
					}
					else if (type.equals("DATE"))
					{
						value = new DataType(DataType.DATE, 0, 0);
					}

					layout.put((Integer)entry.getKey(), value);
				}

				Schema sch = new Schema(layout);
				int onPage = Schema.HEADER_SIZE;
				int lastRequested = Schema.HEADER_SIZE - 1;
				ArrayList<Index> idxs = new ArrayList<Index>();
				i = 0;
				for (String index : newIndexFNs)
				{
					Index idx = new Index(index, indexes.get(i).getKeys(), indexes.get(i).getTypes(), indexes.get(i).getOrders());
					idx.setTransaction(tx);
					idx.open();
					idxs.add(idx);
					i++;
				}
			
				LinkedBlockingQueue<Object> queue = new LinkedBlockingQueue<Object>();
				IndexWriterThread thread = new IndexWriterThread(queue, idxs, true);
				thread.start();
				//insert row and create RAIKS
				FileChannel xx = FileManager.getFile(newFN);
				int block = FileManager.numBlocks.get(newFN) - 1;
				//request block
				HashMap<Integer, DataType> layout2 = new HashMap<Integer, DataType>();
				Schema sch2 = new Schema(layout);
				Block toRequest2 = new Block(newFN, block);
				tx.requestPage(toRequest2);
				tx.read(toRequest2, sch2, true);
				ArrayList<RIDAndIndexKeys> raiks = new ArrayList<RIDAndIndexKeys>();
		
				while (onPage < blocks)
				{
					if (lastRequested - onPage < PAGES_IN_ADVANCE)
					{
						Block[] toRequest = new Block[lastRequested + PREFETCH_REQUEST_SIZE < blocks ? PREFETCH_REQUEST_SIZE : blocks - lastRequested - 1];
						i = 0;
						while (i < toRequest.length)
						{
							toRequest[i] = new Block(fn, lastRequested + i + 1);
							i++;
						}
						tx.requestPages(toRequest);
						lastRequested += toRequest.length;
					}

					tx.read(new Block(fn, onPage++), sch);
					RowIterator rit = sch.rowIterator();
					while (rit.hasNext())
					{
						Row r = rit.next();
						if (!r.getCol(0).exists())
						{
							continue;
						}
					
						RID rid = sch2.insertRowAppend(r.getAllCols());
						ArrayList<ArrayList<Object>> indexKeys = new ArrayList<ArrayList<Object>>();
						i = 0;
						for (Index index : idxs)
						{
							ArrayList<String> key = index.getKeys();
							ArrayList<Object> k = new ArrayList<Object>();
							for (String col : key)
							{
								k.add(r.getCol(cols2Pos.get(col))); 
							}
							
							indexKeys.add(k);
							i++;
						}
						
						queue.add(new RIDAndIndexKeys(rid, indexKeys));
						int newBlock = rid.getBlockNum();
						if (newBlock != block)
						{
							block = newBlock;
							sch2.close();
							sch2 = new Schema(layout2);
							toRequest2 = new Block(newFN, block);
							tx.requestPage(toRequest2);
							tx.read(toRequest2, sch2, true);
						}
					}
				}
			
				sch2.close();
			
				//wait for index writer to finish
				queue.add(new DataEndMarker());
				thread.join();
				if (!thread.getOK())
				{
					ok = false;
				}
			
				HRDBMSWorker.checkpoint.doCheckpoint();
			
				i = 4096;
				while (i < blocks)
				{
					LockManager.xLock(new Block(fn, i), tx.number());
					i++;
				}
			
				for (String fn2 : indexFNs)
				{
					FileManager.getFile(fn2);
					int blocks2 = FileManager.numBlocks.get(fn2);
					i = 0;
					while (i < blocks)
					{
						LockManager.xLock(new Block(fn2, i), tx.number());
						i++;
					}
				}
			
				FileManager.getFile(fn).copyFromFC(FileManager.getFile(newFN));
			
				for (String fn2 : indexFNs)
				{
					FileManager.getFile(fn2).copyFromFC(FileManager.getFile(fn2 + ".new"));
				}
			}
			catch(Exception e)
			{
				HRDBMSWorker.logger.debug("", e);
				ok = false;
			}
		}
		
		public boolean getOK()
		{
			return ok;
		}
		
		public Exception getException()
		{
			return e;
		}
	}
	
	private void get()
	{
		byte[] tableLen = new byte[4];
		byte[] tableBytes;
		String table;
		Object key;
		try
		{
			readNonCoord(tableLen);
			tableBytes = new byte[bytesToInt(tableLen)];
			readNonCoord(tableBytes);
			table = new String(tableBytes, "UTF-8");
			ObjectInputStream objIn = new ObjectInputStream(sock.getInputStream());
			key = (Object)objIn.readObject();;
		}
		catch(Exception e)
		{
			HRDBMSWorker.logger.debug("", e);
			sendNo();
			returnExceptionToClient(e);
			return;
		}
		
		try
		{
			Object retval = doGet(table, key);
			sendOK();
			ObjectOutputStream objOut = new ObjectOutputStream(sock.getOutputStream());
			objOut.writeObject(retval);
			objOut.flush();
			sock.getOutputStream().flush();
		}
		catch(Exception e)
		{
			HRDBMSWorker.logger.debug("", e);
			sendNo();
			returnExceptionToClient(e);
		}
	}
	
	private Object doGet(String name, Object key) throws Exception
	{
		//qualify table name if not qualified
		MetaData meta = new MetaData(this);
		if (!name.contains("."))
		{
			name = meta.getCurrentSchema() + "." + name;
		}
		
		meta = new MetaData();
		
		//build operator tree
		String schema = name.substring(0, name.indexOf('.'));
		String table = name.substring(name.indexOf('.') + 1);
		Transaction tx = new Transaction(Transaction.ISOLATION_UR);
		ArrayList<String> keys = new ArrayList<String>();
		ArrayList<String> types = new ArrayList<String>();
		ArrayList<Boolean> orders = new ArrayList<Boolean>();
		keys.add(table + ".KEY2");
		String keyVal = null;
		if (key instanceof Integer)
		{
			types.add("INT");
			keyVal = ((Integer)key).toString();
		}
		else if (key instanceof Long)
		{
			types.add("LONG");
			keyVal = ((Long)key).toString();
		}
		else if (key instanceof Float)
		{
			types.add("FLOAT");
			key = new Double((Float)key);
			keyVal = ((Float)key).toString();
		}
		else if (key instanceof Double)
		{
			types.add("FLOAT");
			keyVal = ((Double)key).toString();
		}
		else if (key instanceof String)
		{
			types.add("CHAR");
			keyVal = "'" + (String)key + "'";
		}
		else if (key instanceof Date)
		{
			GregorianCalendar cal = new GregorianCalendar();
			cal.setTime((Date)key);
			types.add("DATE");
			key = new MyDate(cal.get(Calendar.YEAR), cal.get(Calendar.MONTH) + 1, cal.get(Calendar.DAY_OF_MONTH));
			keyVal = "DATE('" + ((MyDate)key).toString() + "')";
		}
		orders.add(true);
		//determine node and device
		ArrayList<Object> partial = new ArrayList<Object>();
		partial.add(key);
		long hash = 0x0EFFFFFFFFFFFFFFL & hash(partial);
		int node = (int)(hash % MetaData.numWorkerNodes);
		int device = (int)(hash % MetaData.getNumDevices());
		Index index = new Index(schema + ".PK" + table + ".indx", keys, types, orders);
		IndexOperator iOp = new IndexOperator(index, meta);
		iOp.setNode(node);
		iOp.setDevice(device);
		iOp.getIndex().setTransaction(tx);
		iOp.getIndex().setCondition(new Filter(table + ".KEY2", "E", keyVal));
		HashMap<String, Integer> cols2Pos = new HashMap<String, Integer>();
		cols2Pos.put(table + ".KEY2", 0);
		cols2Pos.put(table + ".VAL", 1);
		TreeMap<Integer, String> pos2Col = new TreeMap<Integer, String>();
		pos2Col.put(0, table + ".KEY2");
		pos2Col.put(1, table + ".VAL");
		TableScanOperator tOp = new TableScanOperator(schema, table, meta, tx, true, cols2Pos, pos2Col);
		ArrayList<Integer> devs = new ArrayList<Integer>();
		devs.add(device);
		tOp.addActiveDevices(devs);
		tOp.setChildForDevice(device, iOp);
		ArrayList<String> needed = new ArrayList<String>();
		needed.add(table + ".VAL");
		tOp.setNeededCols(needed);
		tOp.setNode(node);
		tOp.setPhase2Done();
		tOp.setTransaction(tx);
		tOp.add(iOp);
		NetworkSendOperator send = new NetworkSendOperator(node, meta);
		send.add(tOp);
		NetworkReceiveOperator receive = new NetworkReceiveOperator(meta);
		receive.setNode(MetaData.myNodeNum());
		receive.add(send);
		RootOperator root = new RootOperator(meta);
		root.add(receive);
		
		Get2Thread thread = new Get2Thread(root);
		thread.start();
		tx.commitNoFlush();
		thread.join();
		if (!thread.getOK())
		{
			throw thread.getException();
		}
		
		return thread.getRetval();
	}
	
	private static class Get2Thread extends HRDBMSThread
	{
		private RootOperator root;
		private Object retval = null;
		private boolean ok = true;
		private Exception e;
		
		public Get2Thread(RootOperator root)
		{
			this.root = root;
		}
		
		public boolean getOK()
		{
			return ok;
		}
		
		public Exception getException()
		{
			return e;
		}
		
		public Object getRetval()
		{
			return retval;
		}
		
		public void run()
		{
			try
			{
				//execute operator tree
				root.start();
				Object next = root.next();
				if (next instanceof DataEndMarker)
				{
					root.close();
					return;
				}
			
				if (next instanceof Exception)
				{
					root.close();
					ok = false;
					this.e = (Exception)next;
					return;
				}
			
				ArrayList<Object> row = (ArrayList<Object>)next;
				retval = row.get(0);
				root.close();
			}
			catch(Exception e)
			{
				ok = false;
				this.e = e;
			}
		}
	}
	
	private static long hash(Object key)
	{
		long eHash;
		if (key == null)
		{
			eHash = 0;
		}
		else
		{
			eHash = MurmurHash.hash64(key.toString());
		}

		return eHash;
	}
	
	private void remove()
	{
		byte[] tableLen = new byte[4];
		byte[] tableBytes;
		String table;
		Object key;
		try
		{
			readNonCoord(tableLen);
			tableBytes = new byte[bytesToInt(tableLen)];
			readNonCoord(tableBytes);
			table = new String(tableBytes, "UTF-8");
			ObjectInputStream objIn = new ObjectInputStream(sock.getInputStream());
			key = (Object)objIn.readObject();
		}
		catch(Exception e)
		{
			HRDBMSWorker.logger.debug("", e);
			sendNo();
			returnExceptionToClient(e);
			return;
		}
		
		try
		{
			doRemove(table, key);
			sendOK();
		}
		catch(Exception e)
		{
			HRDBMSWorker.logger.debug("", e);
			sendNo();
			returnExceptionToClient(e);
		}
	}
	
	private void doRemove(String name, Object key) throws Exception
	{
		//qualify table name if not qualified
		MetaData meta = new MetaData(this);
		if (!name.contains("."))
		{
			name = meta.getCurrentSchema() + "." + name;
		}
		
		String schema = name.substring(0, name.indexOf('.'));
		String table = name.substring(name.indexOf('.') + 1);
		Transaction tx = new Transaction(Transaction.ISOLATION_CS);
		/*
		ArrayList<String> keys = new ArrayList<String>();
		ArrayList<String> types = new ArrayList<String>();
		ArrayList<Boolean> orders = new ArrayList<Boolean>();
		keys.add(table + ".KEY");
		if (key instanceof Integer)
		{
			types.add("INT");
		}
		else if (key instanceof Long)
		{
			types.add("LONG");
		}
		else if (key instanceof Float)
		{
			types.add("FLOAT");
			key = new Double((Float)key);
		}
		else if (key instanceof Double)
		{
			types.add("FLOAT");
		}
		else if (key instanceof String)
		{
			types.add("CHAR");
		}
		else if (key instanceof Date)
		{
			GregorianCalendar cal = new GregorianCalendar();
			cal.setTime((Date)key);
			types.add("DATE");
			key = new MyDate(cal.get(Calendar.YEAR), cal.get(Calendar.MONTH) + 1, cal.get(Calendar.DAY_OF_MONTH));
		}
		orders.add(true);
		*/
		//determine node and device
		ArrayList<Object> partial = new ArrayList<Object>();
		partial.add(key);
		long hash = 0x0EFFFFFFFFFFFFFFL & hash(partial);
		int node = (int)(hash % MetaData.numWorkerNodes);
		int device = (int)(hash % MetaData.getNumDevices());
		SendRemove2Thread thread = new SendRemove2Thread(schema, table, node, device, key, tx);
		thread.start();
		tx.commitNoFlush();
		thread.join();
		if (!thread.getOK())
		{
			throw thread.getException();
		}
	}
	
	private static class SendRemove2Thread extends HRDBMSThread
	{
		private String schema;
		private String table;
		private int node;
		private int device;
		private Object key;
		private Transaction tx;
		private boolean ok = true;
		private Exception e;
		
		public SendRemove2Thread(String schema, String table, int node, int device, Object key, Transaction tx)
		{
			this.schema = schema;
			this.table = table;
			this.node = node;
			this.device = device;
			this.key = key;
			this.tx = tx;
		}
		
		public void run()
		{
			try
			{
				String host = new MetaData().getHostNameForNode(node);
				Socket sock = CompressedSocket.newCompressedSocket(host, Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
				OutputStream out = sock.getOutputStream();
				byte[] outMsg = "REMOVE2         ".getBytes("UTF-8");
				outMsg[8] = 0;
				outMsg[9] = 0;
				outMsg[10] = 0;
				outMsg[11] = 0;
				outMsg[12] = 0;
				outMsg[13] = 0;
				outMsg[14] = 0;
				outMsg[15] = 0;
				out.write(outMsg);
				out.write(longToBytes(tx.number()));
				out.write(stringToBytes(schema));
				out.write(stringToBytes(table));
				out.write(intToBytes(device));
				ObjectOutputStream objOut = new ObjectOutputStream(out);
				objOut.writeObject(key);
				objOut.flush();
				out.flush();
				getConfirmation(sock);
				objOut.close();
				sock.close();
			}
			catch(Exception e)
			{
				HRDBMSWorker.logger.debug("", e);
				ok = false;
				this.e = e;
			}
		}
		
		public boolean getOK()
		{
			return ok;
		}
		
		public Exception getException()
		{
			return e;
		}
	}
	
	private static byte[] stringToBytes(String string)
	{
		byte[] data = null;
		try
		{
			data = string.getBytes("UTF-8");
		}
		catch(Exception e)
		{}
		byte[] len = intToBytes(data.length);
		byte[] retval = new byte[data.length + len.length];
		System.arraycopy(len, 0, retval, 0, len.length);
		System.arraycopy(data, 0, retval, len.length, data.length);
		return retval;
	}
	
	private void remove2()
	{
		byte[] txBytes = new byte[8];
		long txNum = -1;
		byte[] schemaLenBytes = new byte[4];
		byte[] tableLenBytes = new byte[4];
		int schemaLength = -1;
		int tableLength = -1;
		String schema = null;
		String table = null;
		byte[] schemaData = null;
		byte[] tableData = null;
		byte[] deviceBytes = new byte[4];
		int device;
		Object key;
		Transaction tx;
		try
		{
			readNonCoord(txBytes);
			txNum = bytesToLong(txBytes);
			tx = new Transaction(txNum);
			readNonCoord(schemaLenBytes);
			schemaLength = bytesToInt(schemaLenBytes);
			schemaData = new byte[schemaLength];
			readNonCoord(schemaData);
			schema = new String(schemaData, "UTF-8");
			readNonCoord(tableLenBytes);
			tableLength = bytesToInt(tableLenBytes);
			tableData = new byte[tableLength];
			readNonCoord(tableData);
			table = new String(tableData, "UTF-8");
			readNonCoord(deviceBytes);
			device = bytesToInt(deviceBytes);
			ObjectInputStream objIn = new ObjectInputStream(sock.getInputStream());
			key = (Object)objIn.readObject();
			
			String fn = new MetaData().getDevicePath(device);
			if (!fn.endsWith("/"))
			{
				fn += "/";
			}
			
			fn += (schema + ".PK" + table + ".indx");
			ArrayList<String> keys = new ArrayList<String>();
			ArrayList<String> types = new ArrayList<String>();
			ArrayList<Boolean> orders = new ArrayList<Boolean>();
			keys.add(table + ".KEY2");
			FieldValue[] fva = new FieldValue[1];
			if (key instanceof Integer)
			{
				types.add("INT");
				fva[0] = new Schema.IntegerFV((Integer)key);
			}
			else if (key instanceof Long)
			{
				types.add("LONG");
				fva[0] = new Schema.BigintFV((Long)key);
			}
			else if (key instanceof Float)
			{
				types.add("FLOAT");
				fva[0] = new Schema.DoubleFV(new Double((Float)key));
			}
			else if (key instanceof Double)
			{
				types.add("FLOAT");
				fva[0] = new Schema.DoubleFV((Double)key);
			}
			else if (key instanceof String)
			{
				types.add("CHAR");
				fva[0] = new Schema.VarcharFV((String)key);
			}
			else if (key instanceof Date)
			{
				GregorianCalendar cal = new GregorianCalendar();
				cal.setTime((Date)key);
				types.add("DATE");
				fva[0] = new Schema.DateFV(new MyDate(cal.get(Calendar.YEAR), cal.get(Calendar.MONTH) + 1, cal.get(Calendar.DAY_OF_MONTH)));
			}
			orders.add(true);
			Index index = new Index(fn, keys, types, orders);
			index.setTransaction(tx);
			index.open();
			try
			{
				IndexRecord line = index.get(fva);
				if (line == null)
				{
					tx.commitNoFlush();
					sendOK();
					return;
				}
			
				HashMap<Integer, DataType> layout = new HashMap<Integer, DataType>();
				Schema sch = new Schema(layout);
				RID rid = line.getRid();
				int block = rid.getBlockNum();
				//request block
				Block toRequest = new Block(new MetaData().getDevicePath(device) + schema + "." + table + ".tbl", block);
				tx.requestPage(toRequest);
				tx.read(toRequest, sch, true);
				sch.deleteRow(rid);
			
				//for each index, delete row based on rid and key values
				index.delete(fva, rid);
			}
			catch(Exception e)
			{
				HRDBMSWorker.logger.debug("", e);
				tx.rollback();
				sendNo();
				return;
			}
			tx.commitNoFlush();
			sendOK();
		}
		catch(Exception e)
		{
			HRDBMSWorker.logger.debug("", e);
			sendNo();
		}
	}
	
	private void put()
	{
		byte[] tableLen = new byte[4];
		byte[] tableBytes;
		String table;
		Object key;
		Object value;
		try
		{
			readNonCoord(tableLen);
			tableBytes = new byte[bytesToInt(tableLen)];
			readNonCoord(tableBytes);
			table = new String(tableBytes, "UTF-8");
			ObjectInputStream objIn = new ObjectInputStream(sock.getInputStream());
			key = (Object)objIn.readObject();
			value = (Object)objIn.readObject();
		}
		catch(Exception e)
		{
			HRDBMSWorker.logger.debug("", e);
			sendNo();
			returnExceptionToClient(e);
			return;
		}
		
		try
		{
			doPut(table, key, value);
			sendOK();
		}
		catch(Exception e)
		{
			HRDBMSWorker.logger.debug("", e);
			sendNo();
			returnExceptionToClient(e);
		}
	}
	
	private void doPut(String name, Object key, Object value) throws Exception
	{
		//qualify table name if not qualified
		MetaData meta = new MetaData(this);
		if (!name.contains("."))
		{
			name = meta.getCurrentSchema() + "." + name;
		}
		
		String schema = name.substring(0, name.indexOf('.'));
		String table = name.substring(name.indexOf('.') + 1);
		Transaction tx = new Transaction(Transaction.ISOLATION_CS);
		Transaction tx2 = new Transaction(Transaction.ISOLATION_CS);
		
		//determine node and device
		ArrayList<Object> partial = new ArrayList<Object>();
		partial.add(key);
		long hash = 0x0EFFFFFFFFFFFFFFL & hash(partial);
		int node = (int)(hash % MetaData.numWorkerNodes);
		int device = (int)(hash % MetaData.getNumDevices());
		SendPut2Thread thread = new SendPut2Thread(schema, table, node, device, key, tx, value, tx2);
		thread.start();
		tx.commitNoFlush();
		tx2.commitNoFlush();
		thread.join();
		if (!thread.getOK())
		{
			throw thread.getException();
		}
	}
	
	private static class SendPut2Thread extends HRDBMSThread
	{
		private String schema;
		private String table;
		private int node;
		private int device;
		private Object key;
		private Object value;
		private Transaction tx;
		private boolean ok = true;
		private Exception e;
		private Transaction tx2;
		
		public SendPut2Thread(String schema, String table, int node, int device, Object key, Transaction tx, Object value, Transaction tx2)
		{
			this.schema = schema;
			this.table = table;
			this.node = node;
			this.device = device;
			this.key = key;
			this.tx = tx;
			this.value = value;
			this.tx2 = tx2;
		}
		
		public void run()
		{
			try
			{
				String host = new MetaData().getHostNameForNode(node);
				Socket sock = CompressedSocket.newCompressedSocket(host, Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
				OutputStream out = sock.getOutputStream();
				byte[] outMsg = "PUT2            ".getBytes("UTF-8");
				outMsg[8] = 0;
				outMsg[9] = 0;
				outMsg[10] = 0;
				outMsg[11] = 0;
				outMsg[12] = 0;
				outMsg[13] = 0;
				outMsg[14] = 0;
				outMsg[15] = 0;
				out.write(outMsg);
				out.write(longToBytes(tx.number()));
				out.write(longToBytes(tx2.number()));
				out.write(stringToBytes(schema));
				out.write(stringToBytes(table));
				out.write(intToBytes(device));
				ObjectOutputStream objOut = new ObjectOutputStream(out);
				objOut.writeObject(key);
				objOut.writeObject(value);
				objOut.flush();
				out.flush();
				getConfirmation(sock);
				objOut.close();
				sock.close();
			}
			catch(Exception e)
			{
				HRDBMSWorker.logger.debug("", e);
				ok = false;
				this.e = e;
			}
		}
		
		public boolean getOK()
		{
			return ok;
		}
		
		public Exception getException()
		{
			return e;
		}
	}
	
	private void put2()
	{
		byte[] txBytes = new byte[8];
		long txNum = -1;
		byte[] schemaLenBytes = new byte[4];
		byte[] tableLenBytes = new byte[4];
		int schemaLength = -1;
		int tableLength = -1;
		String schema = null;
		String table = null;
		byte[] schemaData = null;
		byte[] tableData = null;
		byte[] deviceBytes = new byte[4];
		int device;
		Object key;
		Object value;
		Transaction tx;
		Transaction tx2;
		try
		{
			readNonCoord(txBytes);
			txNum = bytesToLong(txBytes);
			readNonCoord(txBytes);
			tx2 = new Transaction(bytesToLong(txBytes));
			tx = new Transaction(txNum);
			readNonCoord(schemaLenBytes);
			schemaLength = bytesToInt(schemaLenBytes);
			schemaData = new byte[schemaLength];
			readNonCoord(schemaData);
			schema = new String(schemaData, "UTF-8");
			readNonCoord(tableLenBytes);
			tableLength = bytesToInt(tableLenBytes);
			tableData = new byte[tableLength];
			readNonCoord(tableData);
			table = new String(tableData, "UTF-8");
			readNonCoord(deviceBytes);
			device = bytesToInt(deviceBytes);
			ObjectInputStream objIn = new ObjectInputStream(sock.getInputStream());
			key = (Object)objIn.readObject();
			value = (Object)objIn.readObject();
			
			String fn = new MetaData().getDevicePath(device);
			if (!fn.endsWith("/"))
			{
				fn += "/";
			}
			
			fn += (schema + ".PK" + table + ".indx");
			ArrayList<String> keys = new ArrayList<String>();
			ArrayList<String> types = new ArrayList<String>();
			ArrayList<Boolean> orders = new ArrayList<Boolean>();
			keys.add(table + ".KEY2");
			FieldValue[] fva = new FieldValue[1];
			FieldValue[] fva2 = new FieldValue[2];
			if (key instanceof Integer)
			{
				types.add("INT");
				fva[0] = new Schema.IntegerFV((Integer)key);
				fva2[0] = new Schema.IntegerFV((Integer)key);
			}
			else if (key instanceof Long)
			{
				types.add("LONG");
				fva[0] = new Schema.BigintFV((Long)key);
				fva2[0] = new Schema.BigintFV((Long)key);
			}
			else if (key instanceof Float)
			{
				types.add("FLOAT");
				fva[0] = new Schema.DoubleFV(new Double((Float)key));
				fva2[0] = new Schema.DoubleFV(new Double((Float)key));
			}
			else if (key instanceof Double)
			{
				types.add("FLOAT");
				fva[0] = new Schema.DoubleFV((Double)key);
				fva2[0] = new Schema.DoubleFV((Double)key);
			}
			else if (key instanceof String)
			{
				types.add("CHAR");
				fva[0] = new Schema.VarcharFV((String)key);
				fva2[0] = new Schema.VarcharFV((String)key);
			}
			else if (key instanceof Date)
			{
				GregorianCalendar cal = new GregorianCalendar();
				cal.setTime((Date)key);
				types.add("DATE");
				fva[0] = new Schema.DateFV(new MyDate(cal.get(Calendar.YEAR), cal.get(Calendar.MONTH) + 1, cal.get(Calendar.DAY_OF_MONTH)));
				fva2[0] = new Schema.DateFV(new MyDate(cal.get(Calendar.YEAR), cal.get(Calendar.MONTH) + 1, cal.get(Calendar.DAY_OF_MONTH)));
			}
			
			if (value instanceof Integer)
			{
				fva2[1] = new Schema.IntegerFV((Integer)value);
			}
			else if (value instanceof Long)
			{
				fva2[1] = new Schema.BigintFV((Long)value);
			}
			else if (value instanceof Float)
			{
				fva2[1] = new Schema.DoubleFV(new Double((Float)value));
			}
			else if (value instanceof Double)
			{
				fva2[1] = new Schema.DoubleFV((Double)value);
			}
			else if (value instanceof String)
			{
				fva2[1] = new Schema.VarcharFV((String)value);
			}
			else if (value instanceof Date)
			{
				GregorianCalendar cal = new GregorianCalendar();
				cal.setTime((Date)value);
				fva2[1] = new Schema.DateFV(new MyDate(cal.get(Calendar.YEAR), cal.get(Calendar.MONTH) + 1, cal.get(Calendar.DAY_OF_MONTH)));
			}
			orders.add(true);
			Index index = new Index(fn, keys, types, orders);
			index.setTransaction(tx);
			index.open();
			try
			{
				IndexRecord line = index.get(fva);
				if (line == null)
				{
					//doesn't exist yet
					//do insert
					String tfn = new MetaData().getDevicePath(device) + schema + "." + table + ".tbl";
					int maxPlus = FileManager.numBlocks.get(tfn) - (Schema.HEADER_SIZE + 1);
					FileManager.getFile(tfn);
					int block = Schema.HEADER_SIZE + random.nextInt(maxPlus + 1);
					//request block
					HashMap<Integer, DataType> layout = new HashMap<Integer, DataType>();
					Schema sch = new Schema(layout);
					Block toRequest = new Block(tfn, block);
					tx.requestPage(toRequest);
					tx.read(toRequest, sch, true);
			
					RID rid = sch.insertRow(fva2);
					index.insert(fva, rid);
					tx.commitNoFlush();
					sendOK();
				}
				else
				{
					//already exists
					HashMap<Integer, DataType> layout = new HashMap<Integer, DataType>();
					Schema sch = new Schema(layout);
					String tfn = new MetaData().getDevicePath(device) + schema + "." + table + ".tbl";
					
					RID rid = line.getRid();
					
					FileManager.getFile(tfn);
					int maxPlus = FileManager.numBlocks.get(tfn) - (Schema.HEADER_SIZE + 1);
					int block = Schema.HEADER_SIZE + random.nextInt(maxPlus + 1);
					//request block
					Block toRequest = new Block(tfn, block);
					tx.requestPage(toRequest);
					tx.read(toRequest, sch, true);
					RID newRid = sch.insertRow(fva2);
					try
					{
						//index.insert(fva, newRid);
						line.replaceRid(newRid);
						tx.commitNoFlush();
						sendOK();
					}
					catch(Exception e)
					{
						HRDBMSWorker.logger.debug("Index insert failed", e);
						HRDBMSWorker.logger.debug("They old RID we deleted was " + rid);
						HRDBMSWorker.logger.debug("The new RID we were trying to insert was " + newRid);
						throw e;
					}
					
					block = rid.getBlockNum();
					//request block
					toRequest = new Block(new MetaData().getDevicePath(device) + schema + "." + table + ".tbl", block);
					tx2.requestPage(toRequest);
					tx2.read(toRequest, sch, true);
					sch.deleteRow(rid);
					tx2.commitNoFlush();
					//index.delete(fva, rid);
				}
			}
			catch(Exception e)
			{
				HRDBMSWorker.logger.debug("", e);
				tx.rollback();
				sendNo();
				return;
			}
		}
		catch(Exception e)
		{
			HRDBMSWorker.logger.debug("", e);
			sendNo();
		}
	}
}