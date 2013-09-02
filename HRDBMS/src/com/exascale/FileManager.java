package com.exascale;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;

public class FileManager
{
	private static File[] dirs;
	private static Map<String, FileChannel> openFiles = new HashMap<String, FileChannel>();
	
	public FileManager() 
	{
		setDirs(HRDBMSWorker.getHParms().getProperty("data_directories"));
		
		for (File dir : dirs)
		{
			if (!dir.exists())
			{
				System.err.println("Data directory " + dir + " does not exist!");
				System.err.println("Failed to create the File Manager");
				System.exit(1);
			}
			
			for (String file : dir.list())
			{
				if (file.endsWith("tmp"))
				{
					new File(dir, file).delete();
				}
			}
		}
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
	
	public static void createCatalog() throws Exception
	{
		CatalogCode cc = new CatalogCode();
		cc.buildCode();
		//compile source
		JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
		int result = compiler.run(System.in, System.out, System.err, "CatalogCreator.java");
		if (result != 0)
		{
			System.err.println("Failure compiling CatalogCreator. Catalog creation will abort.");
			System.exit(1);
		}
		
		//create CatalogCreator object and execute it
		Class.forName("com.exascale.CatalogCreator").newInstance();
	}
	
	private static void putString(ByteBuffer bb, String val) throws UnsupportedEncodingException
	{
		byte[] bytes = val.getBytes("UTF-8");
		
		int i = 0;
		while (i < bytes.length)
		{
			bb.put(bytes[i]);
			i++;
		}
	}
	
	/*create table is something like this - this was written for row table with 6 cols
	 * public static void createCatalog() throws IOException
	{
	//instead use data directories and create a file on all
		File table;
		String fn = HRDBMSWorker.getHParms().getProperty("catalog_directory");
		
		if (!fn.endsWith("/"))
		{
			fn += "/";
		}
		
		fn += "SYS.TABLES.tbl";
		
		table = new File(fn);
		table.createNewFile();
		
		FileChannel fc = getFile(fn);
		ByteBuffer bb = ByteBuffer.allocate(Page.BLOCK_SIZE);
		bb.position(0);
		bb.putInt(0); //node 0
		bb.putInt(0); //device 0
		bb.putInt(Page.BLOCK_SIZE - (53 + (4 * 6))); //free space if there are 6 columns
		
		int i = 12;
		while (i < Page.BLOCK_SIZE)
		{
			bb.putInt(-1);
			i += 4;
		}
		
		fc.write(bb);
		//done writing first header page
		
		int j = 1;
		while (j < 4096)
		{
			i = 0;
			bb.position(0);
			while (i < Page.BLOCK_SIZE)
			{
				bb.putInt(-1);
				i += 4;
			}
			
			fc.write(bb);
			j++;
		}
		
		//done writing header pages
		bb.position(0);
		bb.put(Schema.TYPE_ROW);
		bb.putInt(0); //nextRecNum
		bb.putInt(52 + (4 * 6)); //headEnd
		bb.putInt(Page.BLOCK_SIZE); //dataStart
		bb.putLong(System.currentTimeMillis()); //modTime
		bb.putInt(-1); //null ArrayOff
		bb.putInt(49); //colIDListOff
		bb.putInt(53 + (4 * 6)); //rowIDListOff
		bb.putInt(-1); //offArrayOff
		bb.putInt(1); //freeSpaceListEntries
		bb.putInt(53 + (4 * 6)); //free space start = headEnd + 1
		bb.putInt(Page.BLOCK_SIZE - 1); //free space end
		bb.putInt(6); //colIDListSize - start of colIDs
		
		i = 0;
		while (i < 6)
		{
			bb.putInt(i);
			i++;
		}
		
		bb.putInt(0); //rowIDListSize - start of rowIDs
		//null Array start
		//offset array start
		 
		 i = bb.position();
		 while (i < Page.BLOCK_SIZE)
		 {
		 	bb.put((byte)0);
		 	i++;
		 }
		 fc.write(bb);
	} */
	
	public static synchronized FileChannel getFile(String filename) throws IOException
	{
		FileChannel fc = openFiles.get(filename);
		
		if (fc == null)
		{
			File table = new File(filename);
			RandomAccessFile f = new RandomAccessFile(table, "rws");
			fc = f.getChannel();
			openFiles.put(filename, fc);
		}
		
		return fc;
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
	
	public static void read(Page p, Block b, ByteBuffer bb) throws IOException
	{
		HRDBMSWorker.addThread(new ReadThread(p, b, bb));
	}
	
	public static void write(Block b, ByteBuffer bb) throws IOException
	{
		bb.rewind();
		FileChannel fc = getFile(b.fileName());
		fc.write(bb, b.number() * bb.capacity());
	}
	
	public static int addNewBlock(String fn, ByteBuffer data) throws IOException
	{
		FileChannel fc = getFile(fn);
		int retval;
		synchronized(fc)
		{
			retval = (int)(fc.size() / Page.BLOCK_SIZE);
			fc.write(data, fc.size());
		}
		
		return retval;
	}
}
