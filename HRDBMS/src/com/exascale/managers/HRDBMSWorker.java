package com.exascale.managers;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Vector;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.FileAppender;
import org.apache.logging.log4j.core.appender.RollingFileAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.DefaultConfiguration;
import org.apache.logging.log4j.core.layout.PatternLayout;

import com.exascale.misc.HParms;
import com.exascale.threads.HRDBMSThread;
import com.exascale.threads.StartCoordsThread;
import com.exascale.threads.StartWorkersThread;

public class HRDBMSWorker
{
	protected static HParms hparms;
	public static final int TYPE_MASTER = 0, TYPE_COORD = 1, TYPE_WORKER = 2;
	public static int type;
	protected static ConcurrentHashMap<Long, HRDBMSThread> threadList = new ConcurrentHashMap<Long, HRDBMSThread>();
	protected static long connectionThread;
	protected static long bufferThread;
	protected static long logThread;
	protected static long resourceThread;
	protected static long checkpointThread; 
	private static long failureThread = -1;
	private static long metaDataThread = -1;
	protected static BlockingQueue<String> in = new LinkedBlockingQueue<String>();
	protected static Long THREAD_NUMBER = new Long(0);
	protected static ConcurrentHashMap<Long, Vector<Thread>> waitList = new ConcurrentHashMap<Long, Vector<Thread>>();
	public static org.apache.logging.log4j.Logger logger = org.apache.logging.log4j.LogManager.getLogger("Log");
	
	public static void main(String[] args) throws Exception
	{	
		org.apache.logging.log4j.Logger logger
	    = org.apache.logging.log4j.LogManager.getLogger("Log");
	  org.apache.logging.log4j.core.Logger coreLogger
	    = (org.apache.logging.log4j.core.Logger)logger;
	  org.apache.logging.log4j.core.LoggerContext context
	    = (org.apache.logging.log4j.core.LoggerContext)coreLogger.getContext();
	  org.apache.logging.log4j.core.config.BaseConfiguration conf
	    = (org.apache.logging.log4j.core.config.BaseConfiguration)context.getConfiguration();
	  FileAppender fa = FileAppender.createAppender("hrdbms.log", "false", "false", "FileLogger", "true", "false", "true", PatternLayout.createLayout("%d{ISO8601}\t%p\t%C{1}: %m%ex%n", conf, null, "UTF-8", "no"), null, "false", null, conf);
	  fa.start();
	  coreLogger.addAppender(fa);
	  coreLogger.setLevel(Level.ALL);
	  logger.info("Starting HRDBMS.");
		
		type = Integer.parseInt(args[0]);
		try
		{
			hparms = HParms.getHParms();
		}
		catch(Exception e)
		{
			logger.error("Could not load HParms", e);
			System.exit(1);
		}
		
		/*File f1 = new File("hrdbms.log");
		f1.createNewFile();
		PrintStream ps = new PrintStream(new FileOutputStream(f1), false);
		System.setOut(ps);*/
		
		if (type == TYPE_MASTER)
		{
			long[] threads = new long[2];
			threads[0] = addThread(startWorkers());
			threads[1] = addThread(startCoordinators());
			waitOnThreads(threads, Thread.currentThread());
			logger.info("Done starting all nodes.");
		}
		
		if (type == TYPE_MASTER || type == TYPE_COORD)
		{
			//new PlanGenerationManager(); TODO
			//new XAManager(); TODO
			//new PlanCacheManager(); TODO
			//failureThread = addThread(new FailureManager()); TODO
			metaDataThread = addThread(new MetaDataManager()); 
		}
		
		//new ExecutionManager(); TODO
		new FileManager();
		bufferThread = addThread(new BufferManager(true));
		logThread = addThread(new LogManager());
		logger.info("Starting initialization of the Lock Manager.");
		new LockManager();
		logger.info("Lock Manager initialization complete.");
		resourceThread = addThread(new ResourceManager());
		connectionThread = addThread(new ConnectionManager());
		checkpointThread = addThread(new CheckpointManager());
		hibernate();
	}
	
	public static HParms getHParms()
	{
		return hparms;
	}
	
	public static ConcurrentHashMap<Long, HRDBMSThread> getThreadList()
	{
		return threadList;
	}
	
	public static long getConnectionThread()
	{
		return connectionThread;
	}
	
	public static long getFailureThread()
	{
		return failureThread;
	}

	public static long getBufferThread()
	{
		return bufferThread;
	}
	
	public static long getLogThread()
	{
		return logThread;
	}
	
	public static long getResourceThread()
	{
		return resourceThread;
	}
	
	public static BlockingQueue<String> getInputQueue()
	{
		return in;
	}
	
	protected static void hibernate()
	{
		String msg;
		logger.info("Main thread is about to hibernate.");
		while (true)
		{
			try
			{
				msg = in.take();
				processMessage(msg);
			}
			catch(InterruptedException e)
			{
				continue;
			}
		}
	}
	
	protected static void processMessage(String msg)
	{
		if (msg.equals("SHUTDOWN"))
		{
			System.exit(0);
		}
		
		if (msg.startsWith("TERMINATE THREAD "))
		{
			terminateThread(Integer.parseInt(msg.substring(17)));
		}
	}
	
	public static long addThread(HRDBMSThread thread)
	{
		long retval;
		synchronized(THREAD_NUMBER)
		{
			retval = THREAD_NUMBER;
			THREAD_NUMBER++;
		}
		
		thread.setIndex(retval);
		threadList.putIfAbsent(retval, thread);
		thread.start();
		return retval;
	}
	
	public static void waitOnThreads(long[] threads, Thread waiter)
	{
		boolean done = false;
		while (!done)
		{
			done = true;
			for (long thread : threads)
			{
				if (threadList.containsKey(thread))
				{
					boolean needsWait = false;
					synchronized(threadList)
					{
						if (threadList.containsKey(thread))
						{
							needsWait = true;
							done = false;
							//if thread in list, add to wait list that this thread is waiting on it
							waitListPut(thread, waiter);
						}
					}
					
					if (needsWait)
					{
						try
						{
							Thread.currentThread().wait();
							break;
						}
						catch(Exception e)
						{
							break;
						}
					}
				}
			}
		}
	}
	
	protected static void waitListPut(long thread, Thread waiter)
	{
		synchronized(waitList)
		{
			if (!waitList.containsKey(thread))
			{
				Vector<Thread> temp = new Vector<Thread>();
				temp.add(waiter);
				waitList.putIfAbsent(thread,  temp);
			}
			else
			{
				Vector<Thread> temp = waitList.get(thread);
				temp.add(waiter);
				waitList.replace(thread, temp);
			}
		}
	}
	
	private static void terminateThread(long index)
	{
		//if anyone is waiting on this thread, wait for Thread.getState() == WAITING and notify them
		if (waitList.containsKey(index))
		{
			Vector<Thread> threads = waitList.get(index);
			for (Thread thread : threads)
			{
				while (thread.getState() != Thread.State.WAITING) {}
				thread.notify();
			}
		}
	}
	
	private static StartWorkersThread startWorkers()
	{
		return new StartWorkersThread();
	}
	
	private static StartCoordsThread startCoordinators()
	{
		return new StartCoordsThread();
	}
}