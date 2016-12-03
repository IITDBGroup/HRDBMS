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

		final ArrayList<OpenThread> threads = new ArrayList<OpenThread>();
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

			final OpenThread thread = new OpenThread(dir.getAbsolutePath());
			thread.start();
			threads.add(thread);
		}

		final String temps = HRDBMSWorker.getHParms().getProperty("temp_directories");
		final StringTokenizer tokens = new StringTokenizer(temps, ",", false);
		while (tokens.hasMoreTokens())
		{
			final String tempDir = tokens.nextToken();
			final File dir = new File(tempDir);
			for (final String file : dir.list())
			{
				final File f = new File(dir, file);
				if (!f.isDirectory())
				{
					f.delete();
				}
			}
		}

		for (final OpenThread thread : threads)
		{
			while (true)
			{
				try
				{
					thread.join();
					break;
				}
				catch (final Exception e)
				{
				}
			}
		}

		HRDBMSWorker.logger.info("File Manager initialization complete.");
	}

	public static int addNewBlock(final String fn, final ByteBuffer data, final Transaction tx) throws Exception
	{
		LockManager.xLock(new Block(fn, -1), tx.number());
		final FileChannel fc = getFile(fn);
		int retval;
		retval = numBlocks.get(fn);
		final Block bl = new Block(fn, retval);
		final ExtendLogRec rec = LogManager.extend(tx.number(), bl);
		LogManager.flush(rec.lsn());
		data.position(0);
		fc.write(data, (retval * 1L) * Page.BLOCK_SIZE);
		// fc.force(false);
		numBlocks.put(fn, retval + 1);

		return retval;
	}

	public static int addNewBlockNoLog(final String fn, final ByteBuffer data, final Transaction tx) throws Exception
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
		final Block b = new Block(fn, retval);
		final int hash = (b.fileName().hashCode() & 0x7FFFFFFF) % BufferManager.managers.length;
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

			final List<String> optionList = new ArrayList<String>();
			// set compiler's classpath to be same as the runtime's
			optionList.addAll(Arrays.asList("-classpath", System.getProperty("java.class.path")));
			final StandardJavaFileManager fileManager = compiler.getStandardFileManager(null, Locale.getDefault(), null);
			final File file1 = new File("CatalogCreator.java");
			final File[] files = new File[] { file1 };
			final Iterable<? extends JavaFileObject> jfos = fileManager.getJavaFileObjectsFromFiles(Arrays.asList(files));

			final JavaCompiler.CompilationTask task = compiler.getTask(javacOut, null, null, optionList, null, jfos);
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
			final Class clazz = Class.forName("CatalogCreator");
			HRDBMSWorker.logger.debug("About to instantiate CatalogCreator");
			clazz.newInstance();
			HRDBMSWorker.logger.debug("Done instantiating CatalogCreator");
			for (final FileChannel fc : openFiles.values())
			{
				fc.force(false);
				fc.close();
			}

			openFiles.clear();
			numBlocks.clear();
		}
		catch (final Throwable e)
		{
			HRDBMSWorker.logger.error("Error during CatalogCreator execution", e);
			throw e;
		}
	}

	public static EndDelayThread endDelay(final String file) throws Exception
	{
		final EndDelayThread thread = new EndDelayThread(file);
		thread.start();
		return thread;
	}

	public static boolean fileExists(final String filename)
	{
		final File file = new File(filename + ".0");
		return file.exists();
	}

	public static File[] getDirs()
	{
		return dirs;
	}

	public static FileChannel getFile(final String filename) throws Exception
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
					final long size = fc.size();
					HRDBMSWorker.logger.debug("Size of " + filename + " is " + size); // DEBUG
					numBlocks.put(filename, (int)(size / Page.BLOCK_SIZE));
					openFiles.put(filename, fc);
				}
			}
		}

		return fc;
	}

	public static synchronized FileChannel getFile(final String filename, final int suffix) throws Exception
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

	public static ReadThread read(final Page p, final Block b, final ByteBuffer bb) throws Exception
	{
		// final FileChannel fc = FileManager.getFile(b.fileName());
		// if (b.number() > numBlocks.get(b.fileName()) + 1)
		// {
		// throw new IOException("Trying to read block " + b.number() + " from "
		// + b.fileName() + " which doesn't exist");
		// }
		final ReadThread retval = new ReadThread(p, b, bb);
		retval.start();
		return retval;
	}

	public static ReadThread read(final Page p, final Block b, final ByteBuffer bb, final ArrayList<Integer> cols, final int layoutSize) throws Exception
	{
		// final FileChannel fc = FileManager.getFile(b.fileName());
		// if (b.number() > numBlocks.get(b.fileName()) + 1)
		// {
		// throw new IOException("Trying to read block " + b.number() + " from "
		// + b.fileName() + " which doesn't exist");
		// }
		final ReadThread retval = new ReadThread(p, b, bb, cols, layoutSize);
		retval.start();
		return retval;
	}

	public static ReadThread read(final Page p, final Block b, final ByteBuffer bb, final ArrayList<Integer> cols, final int layoutSize, final int rank, final int rankSize) throws Exception
	{
		// final FileChannel fc = FileManager.getFile(b.fileName());
		// if (b.number() > numBlocks.get(b.fileName()) + 1)
		// {
		// throw new IOException("Trying to read block " + b.number() + " from "
		// + b.fileName() + " which doesn't exist");
		// }
		final ReadThread retval = new ReadThread(p, b, bb, cols, layoutSize);
		retval.setRank(rank);
		retval.setRankSize(rankSize);
		retval.start();
		return retval;
	}

	public static void read(final Page p, final Block b, final ByteBuffer bb, final boolean flag) throws Exception
	{
		// final FileChannel fc = FileManager.getFile(b.fileName());
		// if (b.number() > numBlocks.get(b.fileName()) + 1)
		// {
		// throw new IOException("Trying to read block " + b.number() + " from "
		// + b.fileName() + " which doesn't exist");
		// }
		new ReadThread(p, b, bb).run();
	}

	public static ReadThread read(final Page p, final Block b, final ByteBuffer bb, final int rank, final int rankSize) throws Exception
	{
		// final FileChannel fc = FileManager.getFile(b.fileName());
		// if (b.number() > numBlocks.get(b.fileName()) + 1)
		// {
		// throw new IOException("Trying to read block " + b.number() + " from "
		// + b.fileName() + " which doesn't exist");
		// }
		final ReadThread retval = new ReadThread(p, b, bb);
		retval.setRank(rank);
		retval.setRankSize(rankSize);
		retval.start();
		return retval;
	}

	public static void read(final Page p, final Block b, final ByteBuffer bb, final Schema schema, final ConcurrentHashMap<Integer, Schema> schemaMap, final Transaction tx, final ArrayList<Integer> fetchPos) throws Exception
	{
		final ReadThread retval = new ReadThread(p, b, bb, schema, schemaMap, tx, fetchPos);
		retval.start();
	}

	public static void read(final Page p, final Block b, final ByteBuffer bb, final Schema schema, final ConcurrentHashMap<Integer, Schema> schemaMap, final Transaction tx, final ArrayList<Integer> fetchPos, final int rank, final int rankSize) throws Exception
	{
		final ReadThread retval = new ReadThread(p, b, bb, schema, schemaMap, tx, fetchPos);
		retval.setRank(rankSize);
		retval.setRankSize(rankSize);
		retval.start();
	}

	public static ReadThread read(final Page p, final int num, final ArrayList<Integer> indexes, final Page[] bp, final int rank, final int rankSize) throws Exception
	{
		// final FileChannel fc = FileManager.getFile(b.fileName());
		// if (b.number() > numBlocks.get(b.fileName()) + 1)
		// {
		// throw new IOException("Trying to read block " + b.number() + " from "
		// + b.fileName() + " which doesn't exist");
		// }
		final ReadThread retval = new ReadThread(p, num, indexes, bp, rank, rankSize);
		retval.start();
		return retval;
	}

	public static Read3Thread read3(final Page p, final Page p2, final Page p3, final Block b, final ByteBuffer bb, final ByteBuffer bb2, final ByteBuffer bb3) throws Exception
	{
		// final FileChannel fc = FileManager.getFile(b.fileName());
		// if (b.number() > numBlocks.get(b.fileName()) + 1)
		// {
		// throw new IOException("Trying to read block " + b.number() + " from "
		// + b.fileName() + " which doesn't exist");
		// }
		final Read3Thread retval = new Read3Thread(p, p2, p3, b, bb, bb2, bb3);
		retval.start();
		return retval;
	}

	public static Read3Thread read3(final Page p, final Page p2, final Page p3, final Block b, final ByteBuffer bb, final ByteBuffer bb2, final ByteBuffer bb3, final ArrayList<Integer> cols, final int layoutSize) throws Exception
	{
		// final FileChannel fc = FileManager.getFile(b.fileName());
		// if (b.number() > numBlocks.get(b.fileName()) + 1)
		// {
		// throw new IOException("Trying to read block " + b.number() + " from "
		// + b.fileName() + " which doesn't exist");
		// }
		final Read3Thread retval = new Read3Thread(p, p2, p3, b, bb, bb2, bb3, cols, layoutSize);
		retval.start();
		return retval;
	}

	public static Read3Thread read3(final Page p, final Page p2, final Page p3, final Block b, final ByteBuffer bb, final ByteBuffer bb2, final ByteBuffer bb3, final int rank, final int rankSize) throws Exception
	{
		// final FileChannel fc = FileManager.getFile(b.fileName());
		// if (b.number() > numBlocks.get(b.fileName()) + 1)
		// {
		// throw new IOException("Trying to read block " + b.number() + " from "
		// + b.fileName() + " which doesn't exist");
		// }
		final Read3Thread retval = new Read3Thread(p, p2, p3, b, bb, bb2, bb3);
		retval.setRank(rank);
		retval.setRankSize(rankSize);
		retval.start();
		return retval;
	}

	public static void read3(final Page p, final Page p2, final Page p3, final Block b, final ByteBuffer bb, final ByteBuffer bb2, final ByteBuffer bb3, final Schema schema1, final Schema schema2, final Schema schema3, final ConcurrentHashMap<Integer, Schema> schemaMap, final Transaction tx, final ArrayList<Integer> fetchPos) throws Exception
	{
		final Read3Thread retval = new Read3Thread(p, p2, p3, b, bb, bb2, bb3, schema1, schema2, schema3, schemaMap, tx, fetchPos);
		retval.start();
	}

	public static void read3(final Page p, final Page p2, final Page p3, final Block b, final ByteBuffer bb, final ByteBuffer bb2, final ByteBuffer bb3, final Schema schema1, final Schema schema2, final Schema schema3, final ConcurrentHashMap<Integer, Schema> schemaMap, final Transaction tx, final ArrayList<Integer> fetchPos, final int rank, final int rankSize) throws Exception
	{
		final Read3Thread retval = new Read3Thread(p, p2, p3, b, bb, bb2, bb3, schema1, schema2, schema3, schemaMap, tx, fetchPos);
		retval.setRank(rank);
		retval.setRankSize(rankSize);
		retval.start();
	}

	public static ReadThread readSync(final Page p, final Block b, final ByteBuffer bb) throws Exception
	{
		// final FileChannel fc = FileManager.getFile(b.fileName());
		// if (b.number() > numBlocks.get(b.fileName()) + 1)
		// {
		// throw new IOException("Trying to read block " + b.number() + " from "
		// + b.fileName() + " which doesn't exist");
		// }
		final ReadThread retval = new ReadThread(p, b, bb);
		retval.run();
		return retval;
	}

	public static void readSync(final Page p, final Block b, final ByteBuffer bb, final Schema schema, final ConcurrentHashMap<Integer, Schema> schemaMap, final Transaction tx, final ArrayList<Integer> fetchPos) throws Exception
	{
		final ReadThread retval = new ReadThread(p, b, bb, schema, schemaMap, tx, fetchPos);
		retval.run();
	}

	public static void redoExtend(final Block bl) throws Exception
	{
		final String fn = bl.fileName();
		final int blockNum = bl.number();
		final FileChannel fc = getFile(fn);
		synchronized (fc)
		{
			final int retval = numBlocks.get(bl.fileName()) - 1;
			if (retval >= blockNum)
			{
				HRDBMSWorker.logger.debug("Went to redo extend of " + bl + " but nothing needed to be done");
				return;
			}
			else if (retval == blockNum - 1)
			{
				HRDBMSWorker.logger.debug("Needed to redo extend of " + bl);
				final ByteBuffer data = ByteBuffer.allocate(Page.BLOCK_SIZE);
				int i = 0;
				data.position(0);
				while (i < Page.BLOCK_SIZE)
				{
					data.putLong(-1);
					i += 8;
				}
				data.position(0);
				fc.write(data, (blockNum * 1L) * Page.BLOCK_SIZE);
				// fc.force(false);
				numBlocks.put(bl.fileName(), blockNum + 1);
			}
			else
			{
				throw new Exception("Trying to redo an extend, but we are missing blocks prior to this one");
			}
		}
	}

	public static synchronized void removeFile(final String filename) throws Exception
	{
		final FileChannel fc = openFiles.get(filename);
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
		final StringTokenizer tokens = new StringTokenizer(fn, ",", false);
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

	public static void trim(final String fn, final int blockNum) throws Exception
	{
		// HRDBMSWorker.logger.debug("Undoing extend of " + fn + ":" +
		// blockNum);
		final FileChannel fc = getFile(fn);
		synchronized (fc)
		{
			final int retval = (int)(fc.size() / Page.BLOCK_SIZE) - 1;
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

	public static void write(final Block b, final ByteBuffer bb) throws Exception
	{
		bb.position(0);
		final FileChannel fc = getFile(b.fileName());
		fc.write(bb, ((long)b.number()) * bb.capacity());
		// fc.force(false);
	}

	public static void writeDelayed(final Block b, final ByteBuffer bb) throws Exception
	{
		bb.position(0);
		final FileChannel fc = getFile(b.fileName());
		fc.write(bb, ((long)b.number()) * bb.capacity());
	}

	private static void setDirs(final String list)
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

		public EndDelayThread(final String file)
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
				final FileChannel fc = getFile(file);
				fc.force(false);
			}
			catch (final Exception e)
			{
				ok = false;
			}
		}
	}

	private static class OpenThread extends HRDBMSThread
	{
		private String dir;

		public OpenThread(final String dir)
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
				final File path = new File(dir);
				final File[] files2 = path.listFiles();
				for (final File f : files2)
				{
					final String name = f.getName();
					if (name.matches(".*\\..*\\.(tbl|indx)\\..*"))
					{
						files.add(f.toPath());
					}
				}

				final HashSet<String> set = new HashSet<String>();
				final HashMap<String, Integer> tops = new HashMap<String, Integer>();
				for (final Path file2 : files)
				{
					final String s = file2.toAbsolutePath().toString().substring(0, file2.toAbsolutePath().toString().lastIndexOf('.'));
					final int suffix = Integer.parseInt(file2.toAbsolutePath().toString().substring(file2.toAbsolutePath().toString().lastIndexOf('.') + 1));
					set.add(s);
					final Integer high = tops.get(s);
					if (high == null)
					{
						tops.put(s, suffix);
					}
					else if (suffix > high)
					{
						tops.put(s, suffix);
					}
				}

				for (final String s : set)
				{
					HRDBMSWorker.logger.debug("Opening " + s);
					try
					{
						FileManager.getFile(s, tops.get(s));
					}
					catch (final Exception e)
					{
						HRDBMSWorker.logger.debug("", e);
					}
				}
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
			}
		}
	}
}
