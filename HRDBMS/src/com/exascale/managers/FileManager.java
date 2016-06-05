package com.exascale.managers;

import java.io.File;
import java.io.FileWriter;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentHashMap;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;
import com.exascale.filesystem.Block;
import com.exascale.filesystem.CompressedRandomAccessFile;
import com.exascale.filesystem.Page;
import com.exascale.logging.ExtendLogRec;
import com.exascale.misc.CatalogCode;
import com.exascale.tables.Schema;
import com.exascale.tables.Transaction;
import com.exascale.threads.HRDBMSThread;
import com.exascale.threads.Read3Thread;
import com.exascale.threads.ReadThread;

public class FileManager
{
	private static File[] dirs;
	public static final boolean SCFC;
	public static ConcurrentHashMap<String, FileChannel> openFiles = new ConcurrentHashMap<String, FileChannel>();
	public static ConcurrentHashMap<String, Integer> numBlocks = new ConcurrentHashMap<String, Integer>(2000, 0.75f, 64 * ResourceManager.cpus);

	static
	{
		SCFC = HRDBMSWorker.getHParms().getProperty("scfc").equals("true");
	}

	public FileManager()
	{
		HRDBMSWorker.logger.info("Starting initialization of the File Manager.");
		setDirs(HRDBMSWorker.getHParms().getProperty("data_directories"));

		ArrayList<OpenThread> threads = new ArrayList<OpenThread>();
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

			OpenThread thread = new OpenThread(dir.getAbsolutePath());
			thread.start();
			threads.add(thread);
		}

		String temps = HRDBMSWorker.getHParms().getProperty("temp_directories");
		StringTokenizer tokens = new StringTokenizer(temps, ",", false);
		while (tokens.hasMoreTokens())
		{
			String tempDir = tokens.nextToken();
			File dir = new File(tempDir);
			for (String file : dir.list())
			{
				File f = new File(dir, file);
				if (!f.isDirectory())
				{
					f.delete();
				}
			}
		}

		for (OpenThread thread : threads)
		{
			while (true)
			{
				try
				{
					thread.join();
					break;
				}
				catch (Exception e)
				{
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
		retval = numBlocks.get(fn);
		Block bl = new Block(fn, retval);
		ExtendLogRec rec = LogManager.extend(tx.number(), bl);
		LogManager.flush(rec.lsn());
		data.position(0);
		fc.write(data, retval * Page.BLOCK_SIZE);
		// fc.force(false);
		numBlocks.put(fn, retval + 1);

		return retval;
	}

	public static int addNewBlockNoLog(String fn, ByteBuffer data, Transaction tx) throws Exception
	{
		LockManager.xLock(new Block(fn, -1), tx.number());
		// getFile(fn);
		Integer retval = numBlocks.get(fn);
		if (retval == null)
		{
			getFile(fn);
			retval = numBlocks.get(fn);
		}

		// write page to bufferpool
		Block b = new Block(fn, retval);
		int hash = (b.fileName().hashCode() & 0x7FFFFFFF) % BufferManager.managers.length;
		BufferManager.managers[hash].pinFromMemory(b, tx.number(), data);
		numBlocks.put(fn, retval + 1);

		return retval;
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
			// result = compiler.run(System.in, javacOut, javacOut,
			// "CatalogCreator.java");
			// javacOut.close();

			List<String> optionList = new ArrayList<String>();
			// set compiler's classpath to be same as the runtime's
			optionList.addAll(Arrays.asList("-classpath", System.getProperty("java.class.path")));
			StandardJavaFileManager fileManager = compiler.getStandardFileManager(null, Locale.getDefault(), null);
			File file1 = new File("CatalogCreator.java");
			File[] files = new File[] { file1 };
			Iterable<? extends JavaFileObject> jfos = fileManager.getJavaFileObjectsFromFiles(Arrays.asList(files));

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
			for (FileChannel fc : openFiles.values())
			{
				fc.force(false);
				fc.close();
			}

			openFiles.clear();
			numBlocks.clear();
		}
		catch (Throwable e)
		{
			HRDBMSWorker.logger.error("Error during CatalogCreator execution", e);
			throw e;
		}
	}

	public static EndDelayThread endDelay(String file) throws Exception
	{
		EndDelayThread thread = new EndDelayThread(file);
		thread.start();
		return thread;
	}

	public static boolean fileExists(String filename)
	{
		File file = new File(filename + ".0");
		return file.exists();
	}

	public static File[] getDirs()
	{
		return dirs;
	}

	public static FileChannel getFile(String filename) throws Exception
	{
		FileChannel fc = openFiles.get(filename);

		if (fc == null)
		{
			synchronized (FileManager.class)
			{
				fc = openFiles.get(filename);
				if (fc == null)
				{
					final File table = new File(filename);
					final CompressedRandomAccessFile f = new CompressedRandomAccessFile(table, "rw");
					fc = f.getChannel();
					long size = fc.size();
					HRDBMSWorker.logger.debug("Size of " + filename + " is " + size); // DEBUG
					numBlocks.put(filename, (int)(size / Page.BLOCK_SIZE));
					openFiles.put(filename, fc);
				}
			}
		}

		return fc;
	}

	public static synchronized FileChannel getFile(String filename, int suffix) throws Exception
	{
		FileChannel fc = openFiles.get(filename);

		if (fc == null)
		{
			synchronized (FileManager.class)
			{
				fc = openFiles.get(filename);
				if (fc == null)
				{
					final File table = new File(filename);
					final CompressedRandomAccessFile f = new CompressedRandomAccessFile(table, "rw", suffix);
					fc = f.getChannel();
					numBlocks.put(filename, (int)(fc.size() / Page.BLOCK_SIZE));
					openFiles.put(filename, fc);
				}
			}
		}

		return fc;
	}

	public static ReadThread read(Page p, Block b, ByteBuffer bb) throws Exception
	{
		// final FileChannel fc = FileManager.getFile(b.fileName());
		// if (b.number() > numBlocks.get(b.fileName()) + 1)
		// {
		// throw new IOException("Trying to read block " + b.number() + " from "
		// + b.fileName() + " which doesn't exist");
		// }
		ReadThread retval = new ReadThread(p, b, bb);
		retval.start();
		return retval;
	}

	public static ReadThread read(Page p, Block b, ByteBuffer bb, ArrayList<Integer> cols, int layoutSize) throws Exception
	{
		// final FileChannel fc = FileManager.getFile(b.fileName());
		// if (b.number() > numBlocks.get(b.fileName()) + 1)
		// {
		// throw new IOException("Trying to read block " + b.number() + " from "
		// + b.fileName() + " which doesn't exist");
		// }
		ReadThread retval = new ReadThread(p, b, bb, cols, layoutSize);
		retval.start();
		return retval;
	}

	public static ReadThread read(Page p, Block b, ByteBuffer bb, ArrayList<Integer> cols, int layoutSize, int rank, int rankSize) throws Exception
	{
		// final FileChannel fc = FileManager.getFile(b.fileName());
		// if (b.number() > numBlocks.get(b.fileName()) + 1)
		// {
		// throw new IOException("Trying to read block " + b.number() + " from "
		// + b.fileName() + " which doesn't exist");
		// }
		ReadThread retval = new ReadThread(p, b, bb, cols, layoutSize);
		retval.setRank(rank);
		retval.setRankSize(rankSize);
		retval.start();
		return retval;
	}

	public static void read(Page p, Block b, ByteBuffer bb, boolean flag) throws Exception
	{
		// final FileChannel fc = FileManager.getFile(b.fileName());
		// if (b.number() > numBlocks.get(b.fileName()) + 1)
		// {
		// throw new IOException("Trying to read block " + b.number() + " from "
		// + b.fileName() + " which doesn't exist");
		// }
		new ReadThread(p, b, bb).run();
	}

	public static ReadThread read(Page p, Block b, ByteBuffer bb, int rank, int rankSize) throws Exception
	{
		// final FileChannel fc = FileManager.getFile(b.fileName());
		// if (b.number() > numBlocks.get(b.fileName()) + 1)
		// {
		// throw new IOException("Trying to read block " + b.number() + " from "
		// + b.fileName() + " which doesn't exist");
		// }
		ReadThread retval = new ReadThread(p, b, bb);
		retval.setRank(rank);
		retval.setRankSize(rankSize);
		retval.start();
		return retval;
	}

	public static void read(Page p, Block b, ByteBuffer bb, Schema schema, ConcurrentHashMap<Integer, Schema> schemaMap, Transaction tx, ArrayList<Integer> fetchPos) throws Exception
	{
		ReadThread retval = new ReadThread(p, b, bb, schema, schemaMap, tx, fetchPos);
		retval.start();
	}

	public static void read(Page p, Block b, ByteBuffer bb, Schema schema, ConcurrentHashMap<Integer, Schema> schemaMap, Transaction tx, ArrayList<Integer> fetchPos, int rank, int rankSize) throws Exception
	{
		ReadThread retval = new ReadThread(p, b, bb, schema, schemaMap, tx, fetchPos);
		retval.setRank(rankSize);
		retval.setRankSize(rankSize);
		retval.start();
	}

	public static ReadThread read(Page p, int num, ArrayList<Integer> indexes, Page[] bp, int rank, int rankSize) throws Exception
	{
		// final FileChannel fc = FileManager.getFile(b.fileName());
		// if (b.number() > numBlocks.get(b.fileName()) + 1)
		// {
		// throw new IOException("Trying to read block " + b.number() + " from "
		// + b.fileName() + " which doesn't exist");
		// }
		ReadThread retval = new ReadThread(p, num, indexes, bp, rank, rankSize);
		retval.start();
		return retval;
	}

	public static Read3Thread read3(Page p, Page p2, Page p3, Block b, ByteBuffer bb, ByteBuffer bb2, ByteBuffer bb3) throws Exception
	{
		// final FileChannel fc = FileManager.getFile(b.fileName());
		// if (b.number() > numBlocks.get(b.fileName()) + 1)
		// {
		// throw new IOException("Trying to read block " + b.number() + " from "
		// + b.fileName() + " which doesn't exist");
		// }
		Read3Thread retval = new Read3Thread(p, p2, p3, b, bb, bb2, bb3);
		retval.start();
		return retval;
	}

	public static Read3Thread read3(Page p, Page p2, Page p3, Block b, ByteBuffer bb, ByteBuffer bb2, ByteBuffer bb3, ArrayList<Integer> cols, int layoutSize) throws Exception
	{
		// final FileChannel fc = FileManager.getFile(b.fileName());
		// if (b.number() > numBlocks.get(b.fileName()) + 1)
		// {
		// throw new IOException("Trying to read block " + b.number() + " from "
		// + b.fileName() + " which doesn't exist");
		// }
		Read3Thread retval = new Read3Thread(p, p2, p3, b, bb, bb2, bb3, cols, layoutSize);
		retval.start();
		return retval;
	}

	public static Read3Thread read3(Page p, Page p2, Page p3, Block b, ByteBuffer bb, ByteBuffer bb2, ByteBuffer bb3, int rank, int rankSize) throws Exception
	{
		// final FileChannel fc = FileManager.getFile(b.fileName());
		// if (b.number() > numBlocks.get(b.fileName()) + 1)
		// {
		// throw new IOException("Trying to read block " + b.number() + " from "
		// + b.fileName() + " which doesn't exist");
		// }
		Read3Thread retval = new Read3Thread(p, p2, p3, b, bb, bb2, bb3);
		retval.setRank(rank);
		retval.setRankSize(rankSize);
		retval.start();
		return retval;
	}

	public static void read3(Page p, Page p2, Page p3, Block b, ByteBuffer bb, ByteBuffer bb2, ByteBuffer bb3, Schema schema1, Schema schema2, Schema schema3, ConcurrentHashMap<Integer, Schema> schemaMap, Transaction tx, ArrayList<Integer> fetchPos) throws Exception
	{
		Read3Thread retval = new Read3Thread(p, p2, p3, b, bb, bb2, bb3, schema1, schema2, schema3, schemaMap, tx, fetchPos);
		retval.start();
	}

	public static void read3(Page p, Page p2, Page p3, Block b, ByteBuffer bb, ByteBuffer bb2, ByteBuffer bb3, Schema schema1, Schema schema2, Schema schema3, ConcurrentHashMap<Integer, Schema> schemaMap, Transaction tx, ArrayList<Integer> fetchPos, int rank, int rankSize) throws Exception
	{
		Read3Thread retval = new Read3Thread(p, p2, p3, b, bb, bb2, bb3, schema1, schema2, schema3, schemaMap, tx, fetchPos);
		retval.setRank(rank);
		retval.setRankSize(rankSize);
		retval.start();
	}

	public static ReadThread readSync(Page p, Block b, ByteBuffer bb) throws Exception
	{
		// final FileChannel fc = FileManager.getFile(b.fileName());
		// if (b.number() > numBlocks.get(b.fileName()) + 1)
		// {
		// throw new IOException("Trying to read block " + b.number() + " from "
		// + b.fileName() + " which doesn't exist");
		// }
		ReadThread retval = new ReadThread(p, b, bb);
		retval.run();
		return retval;
	}

	public static void readSync(Page p, Block b, ByteBuffer bb, Schema schema, ConcurrentHashMap<Integer, Schema> schemaMap, Transaction tx, ArrayList<Integer> fetchPos) throws Exception
	{
		ReadThread retval = new ReadThread(p, b, bb, schema, schemaMap, tx, fetchPos);
		retval.run();
	}

	public static void redoExtend(Block bl) throws Exception
	{
		String fn = bl.fileName();
		int blockNum = bl.number();
		final FileChannel fc = getFile(fn);
		synchronized (fc)
		{
			int retval = numBlocks.get(bl.fileName()) - 1;
			if (retval >= blockNum)
			{
				HRDBMSWorker.logger.debug("Went to redo extend of " + bl + " but nothing needed to be done");
				return;
			}
			else if (retval == blockNum - 1)
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
				fc.write(data, blockNum * Page.BLOCK_SIZE);
				// fc.force(false);
				numBlocks.put(bl.fileName(), blockNum + 1);
			}
			else
			{
				throw new Exception("Trying to redo an extend, but we are missing blocks prior to this one");
			}
		}
	}

	public static synchronized void removeFile(String filename) throws Exception
	{
		FileChannel fc = openFiles.get(filename);
		if (fc != null)
		{
			fc.truncate(0);
			fc.close();
			numBlocks.remove(filename);
			openFiles.remove(filename);
		}
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

		fn += "SYS.TABLES.tbl.0";

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

	public static void trim(String fn, int blockNum) throws Exception
	{
		// HRDBMSWorker.logger.debug("Undoing extend of " + fn + ":" +
		// blockNum);
		final FileChannel fc = getFile(fn);
		synchronized (fc)
		{
			int retval = (int)(fc.size() / Page.BLOCK_SIZE) - 1;
			if (retval > blockNum)
			{
				throw new Exception("Can't trim anything but the last block");
			}
			else if (retval == blockNum)
			{
				fc.truncate(fc.size() - Page.BLOCK_SIZE);
				BufferManager.throwAwayPage(fn, blockNum);
				numBlocks.put(fn, retval);
			}
		}
	}

	public static void write(Block b, ByteBuffer bb) throws Exception
	{
		bb.position(0);
		final FileChannel fc = getFile(b.fileName());
		fc.write(bb, ((long)b.number()) * bb.capacity());
		// fc.force(false);
	}

	public static void writeDelayed(Block b, ByteBuffer bb) throws Exception
	{
		bb.position(0);
		final FileChannel fc = getFile(b.fileName());
		fc.write(bb, ((long)b.number()) * bb.capacity());
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

	public static class EndDelayThread extends HRDBMSThread
	{
		private final String file;
		private boolean ok = true;

		public EndDelayThread(String file)
		{
			this.file = file;
		}

		public boolean getOK()
		{
			return ok;
		}

		@Override
		public void run()
		{
			try
			{
				FileChannel fc = getFile(file);
				fc.force(false);
			}
			catch (Exception e)
			{
				ok = false;
			}
		}
	}

	private static class OpenThread extends HRDBMSThread
	{
		private String dir;

		public OpenThread(String dir)
		{
			if (dir.endsWith("/"))
			{
				this.dir = dir;
			}
			else
			{
				this.dir = dir + "/";
			}
		}

		@Override
		public void run()
		{
			try
			{
				final ArrayList<Path> files = new ArrayList<Path>();
				File path = new File(dir);
				File[] files2 = path.listFiles();
				for (File f : files2)
				{
					String name = f.getName();
					if (name.matches(".*\\..*\\.(tbl|indx)\\..*"))
					{
						files.add(f.toPath());
					}
				}

				HashSet<String> set = new HashSet<String>();
				HashMap<String, Integer> tops = new HashMap<String, Integer>();
				for (Path file2 : files)
				{
					String s = file2.toAbsolutePath().toString().substring(0, file2.toAbsolutePath().toString().lastIndexOf('.'));
					int suffix = Integer.parseInt(file2.toAbsolutePath().toString().substring(file2.toAbsolutePath().toString().lastIndexOf('.') + 1));
					set.add(s);
					Integer high = tops.get(s);
					if (high == null)
					{
						tops.put(s, suffix);
					}
					else if (suffix > high)
					{
						tops.put(s, suffix);
					}
				}

				for (String s : set)
				{
					HRDBMSWorker.logger.debug("Opening " + s);
					try
					{
						FileManager.getFile(s, tops.get(s));
					}
					catch (Exception e)
					{
						HRDBMSWorker.logger.debug("", e);
					}
				}
			}
			catch (Exception e)
			{
				HRDBMSWorker.logger.error("", e);
			}
		}
	}
}
