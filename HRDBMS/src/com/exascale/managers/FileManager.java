package com.exascale.managers;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.StringTokenizer;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;
import com.exascale.filesystem.Block;
import com.exascale.filesystem.Page;
import com.exascale.logging.ExtendLogRec;
import com.exascale.misc.CatalogCode;
import com.exascale.threads.ReadThread;
import com.exascale.tables.Transaction;

public class FileManager
{
	private static File[] dirs;
	private static Map<String, FileChannel> openFiles = new HashMap<String, FileChannel>();

	public FileManager()
	{
		HRDBMSWorker.logger.info("Starting initialization of the File Manager.");
		setDirs(HRDBMSWorker.getHParms().getProperty("data_directories"));

		for (final File dir : dirs)
		{
			if (!dir.exists())
			{
				HRDBMSWorker.logger.error("Data directory " + dir + " does not exist!");
				HRDBMSWorker.logger.error("Failed to create the File Manager");
				System.exit(1);
			}

			for (final String file : dir.list())
			{
				if (file.endsWith("tmp"))
				{
					new File(dir, file).delete();
				}
			}
		}

		HRDBMSWorker.logger.info("File Manager initialization complete.");
	}

	public static int addNewBlock(String fn, ByteBuffer data, Transaction tx) throws Exception
	{
		LockManager.xLock(new Block(fn, -1), tx.number());
		final FileChannel fc = getFile(fn);
		int retval;
		synchronized (fc)
		{
			retval = (int)(fc.size() / Page.BLOCK_SIZE);
			Block bl = new Block(fn, retval);
			ExtendLogRec rec = LogManager.extend(tx.number(), bl);
			LogManager.flush(rec.lsn());
			data.position(0);
			fc.write(data, fc.size());
		}

		return retval;
	}
	
	public static void trim(String fn, int blockNum) throws Exception
	{
		HRDBMSWorker.logger.debug("Undoing extend of " + fn + ":" + blockNum);
		final FileChannel fc = getFile(fn);
		int retval = (int)(fc.size() / Page.BLOCK_SIZE) - 1;
		if (retval > blockNum)
		{
			throw new Exception("Can't trim anything but the last block");
		}
		else if (retval == blockNum)
		{
			fc.truncate(fc.size() - Page.BLOCK_SIZE);
			BufferManager.throwAwayPage(fn, blockNum);
		}
	}
	
	public static void redoExtend(Block bl) throws Exception
	{
		String fn = bl.fileName();
		int blockNum = bl.number();
		final FileChannel fc = getFile(fn);
		int retval = (int)(fc.size() / Page.BLOCK_SIZE) - 1;
		if (retval >= blockNum)
		{
			HRDBMSWorker.logger.debug("Went to redo extend of " + bl + " but nothing needed to be done");
			return;
		}
		else if (retval == blockNum-1)
		{
			HRDBMSWorker.logger.debug("Needed to redo extend of " + bl);
			ByteBuffer data = ByteBuffer.allocate(Page.BLOCK_SIZE);
			int i = 0;
			data.position(0);
			while (i < Page.BLOCK_SIZE)
			{
				data.putLong(-1);
				i += 8;
			}
			data.position(0);
			fc.write(data, fc.size());
		}
		else
		{
			throw new Exception("Trying to redo an extend, but we are missing blocks prior to this one");
		}
	}

	public static void createCatalog() throws Exception
	{
		new CatalogCode();
		CatalogCode.buildCode();
		HRDBMSWorker.logger.debug("Done building source code.");
		// compile source
		HRDBMSWorker.logger.debug("Starting compilation.");
		boolean result = true;
		try
		{
			final JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
			final FileWriter javacOut = new FileWriter("javac.out");
			//result = compiler.run(System.in, javacOut, javacOut, "CatalogCreator.java");
			//javacOut.close();
			
			List<String> optionList = new ArrayList<String>();
			// set compiler's classpath to be same as the runtime's
			optionList.addAll(Arrays.asList("-classpath",System.getProperty("java.class.path")));
			StandardJavaFileManager fileManager = compiler.getStandardFileManager(null, Locale.getDefault(), null);
			File file1 = new File("CatalogCreator.java");
			File []files = new File[]{file1} ;
			Iterable<? extends JavaFileObject> jfos =
			           fileManager.getJavaFileObjectsFromFiles(Arrays.asList(files));


			JavaCompiler.CompilationTask task = compiler.getTask(javacOut, null, null, optionList, null, jfos);
			result = task.call();
			fileManager.close();
			javacOut.close();
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.error("Exception during compilation.");
			HRDBMSWorker.logger.error("Exception is ", e);
			System.exit(1);
		}
		if (!result)
		{
			HRDBMSWorker.logger.error("Failure compiling CatalogCreator. Catalog creation will abort.");
			System.exit(1);
		}

		// create CatalogCreator object and execute it
		try
		{
			HRDBMSWorker.logger.debug("About to load CatalogCreator");
			Class clazz = Class.forName("CatalogCreator");
			HRDBMSWorker.logger.debug("About to instantiate CatalogCreator");
			clazz.newInstance();
			HRDBMSWorker.logger.debug("Done instantiating CatalogCreator");
		}
		catch (Throwable e)
		{
			HRDBMSWorker.logger.error("Error during CatalogCreator execution", e);
			throw e;
		}
	}

	public static File[] getDirs()
	{
		return dirs;
	}

	public static synchronized FileChannel getFile(String filename) throws IOException
	{
		FileChannel fc = openFiles.get(filename);

		if (fc == null)
		{
			final File table = new File(filename);
			final RandomAccessFile f = new RandomAccessFile(table, "rwd");
			fc = f.getChannel();
			openFiles.put(filename, fc);
		}

		return fc;
	}

	public static void read(Page p, Block b, ByteBuffer bb) throws IOException
	{
		HRDBMSWorker.addThread(new ReadThread(p, b, bb));
	}

	public static boolean sysTablesExists()
	{
		File sysTables;
		String fn = HRDBMSWorker.getHParms().getProperty("data_directories");
		StringTokenizer tokens = new StringTokenizer(fn, ",", false);
		fn = tokens.nextToken();

		if (!fn.endsWith("/"))
		{
			fn += "/";
		}

		fn += "SYS.TABLES.tbl";

		sysTables = new File(fn);

		if (!sysTables.exists())
		{
			return false;
		}
		else
		{
			return true;
		}
	}

	public static void write(Block b, ByteBuffer bb) throws IOException
	{
		bb.position(0);
		final FileChannel fc = getFile(b.fileName());
		fc.write(bb, b.number() * bb.capacity());
	}

	private static void setDirs(String list)
	{
		StringTokenizer tokens = new StringTokenizer(list, ",", false);
		int i = 0;
		while (tokens.hasMoreTokens())
		{
			tokens.nextToken();
			i++;
		}
		dirs = new File[i];
		tokens = new StringTokenizer(list, ",", false);
		i = 0;
		while (tokens.hasMoreTokens())
		{
			dirs[i] = new File(tokens.nextToken());
			i++;
		}
	}
}
