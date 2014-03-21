package com.exascale.managers;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;
import com.exascale.filesystem.Block;
import com.exascale.filesystem.Page;
import com.exascale.misc.CatalogCode;
import com.exascale.threads.ReadThread;

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

	public static int addNewBlock(String fn, ByteBuffer data) throws IOException
	{
		final FileChannel fc = getFile(fn);
		int retval;
		synchronized (fc)
		{
			retval = (int)(fc.size() / Page.BLOCK_SIZE);
			data.position(0);
			fc.write(data, fc.size());
		}

		return retval;
	}

	public static void createCatalog() throws Exception
	{
		new CatalogCode();
		CatalogCode.buildCode();
		HRDBMSWorker.logger.debug("Done building source code.");
		// compile source
		HRDBMSWorker.logger.debug("Starting compilation.");
		int result = -1;
		try
		{
			final JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
			final FileOutputStream javacOut = new FileOutputStream(new File("javac.out"), false);
			result = compiler.run(System.in, javacOut, javacOut, "CatalogCreator.java");
			javacOut.close();
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.error("Exception during compilation.");
			HRDBMSWorker.logger.error("Exception is ", e);
			System.exit(1);
		}
		if (result != 0)
		{
			HRDBMSWorker.logger.error("Failure compiling CatalogCreator. Catalog creation will abort.");
			System.exit(1);
		}

		// create CatalogCreator object and execute it
		try
		{
			Class.forName("CatalogCreator").newInstance();
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.error("" + e.getClass());
			System.exit(1);
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
			final RandomAccessFile f = new RandomAccessFile(table, "rws");
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
		String fn = HRDBMSWorker.getHParms().getProperty("catalog_directory");

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
		bb.rewind();
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
