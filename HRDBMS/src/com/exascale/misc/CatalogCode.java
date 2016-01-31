package com.exascale.misc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.Vector;
import com.exascale.filesystem.Page;
import com.exascale.filesystem.RID;
import com.exascale.managers.FileManager;
import com.exascale.managers.HRDBMSWorker;

public class CatalogCode
{
	private static TreeMap<String, Long> vars = new TreeMap<String, Long>();

	private static Vector<Integer> sizes = new Vector<Integer>();
	private static Vector<String> tableLines = new Vector<String>();
	private static Vector<Vector<String>> data = new Vector<Vector<String>>();
	private static Vector<Vector<String>> colTypes = new Vector<Vector<String>>();
	private static HashSet<String> racks = new HashSet<String>();
	private static HashMap<String, Socket> sockets = new HashMap<String, Socket>();
	private static ServerSocket listen;
	private static InputStream in;
	private static OutputStream sockOut;
	private static int methodNum = 0;

	public static void buildCode() throws Exception
	{
		HRDBMSWorker.logger.debug("Starting build of catalog java code.");
		if (HRDBMSWorker.type == HRDBMSWorker.TYPE_COORD)
		{
			final int port = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("catalog_sync_port"));
			HRDBMSWorker.logger.debug("Attempting to listen on port " + port);
			listen = new ServerSocket();
			listen.setReceiveBufferSize(262144);
			listen.bind(new InetSocketAddress(port));
			HRDBMSWorker.logger.debug("ServerSocket created.  Now attempting accept()");
			final Socket listenSock = listen.accept();
			HRDBMSWorker.logger.debug("Accepted connection on port " + port);
			final byte[] dummy = new byte[8];
			in = listenSock.getInputStream();
			HRDBMSWorker.logger.debug("Got socket input stream.");
			sockOut = listenSock.getOutputStream();
			HRDBMSWorker.logger.debug("Got socket output stream.");
			final int num = in.read(dummy);
			if (num != 8)
			{
				HRDBMSWorker.logger.error("Initial handshake on catalog sync port failed!");
				System.exit(1);
			}
			HRDBMSWorker.logger.debug("Initial handshake on catalog sync port successful.");
		}

		final String in =
		// SYS.TABLES(ID, SCHEMA, NAME, TYPE)
				"SYS.TABLES(INT, VARCHAR, VARCHAR, VARCHAR)\n" + "13\n" + "(0, SYS, TABLES, R)\n" + "(1, SYS, COLUMNS, R)\n" + "(2, SYS, INDEXES, R)\n" + "(3, SYS, INDEXCOLS, R)\n" + "(4, SYS, VIEWS, R)\n" + "(5, SYS, TABLESTATS, R)\n" + "(6, SYS, NODES, R)\n" + "(7, SYS, COLSTATS, R)\n" + "(8, SYS, COLDIST, R)\n" + "(9, SYS, BACKUPS, R)\n" + "(10, SYS, NODESTATE, R)\n" + "(11, SYS, PARTITIONING, R)\n" + "(12, SYS, INDEXSTATS, R)\n"

		+ "SYS.BACKUPS(INT, INT, INT)\n" + "0\n"

		+ "SYS.NODESTATE(INT, VARCHAR)\n" + "0\n"

		// SYS.COLUMNS(COLID, TABLEID, NAME, TYPE, LENGTH, SCALE, PKPOS, NULL)
		+ "SYS.COLUMNS(INT, INT, VARCHAR, VARCHAR, INT, INT, INT, VARCHAR)\n" + "53\n" + "(0, 1, COLID, INT, 4, 0, -1, N)\n" + "(1, 1, TABLEID, INT, 4, 0, 0, N)\n" + "(2, 1, COLNAME, VARCHAR, 128, 0, 1, N)\n" + "(3, 1, COLTYPE, VARCHAR, 16, 0, -1, N)\n" + "(4, 1, LENGTH, INT, 4, 0, -1, N)\n" + "(5, 1, SCALE, INT, 4, 0, -1, N)\n" + "(6, 1, PKPOS, INT, 4, 0, -1, N)\n" + "(7, 1, NULLABLE, VARCHAR, 1, 0, -1, N)\n"

		+ "(0, 0, TABLEID, INT, 4, 0, -1, N)\n" + "(1, 0, SCHEMA, VARCHAR, 128, 0, 0, N)\n" + "(2, 0, TABNAME, VARCHAR, 128, 0, 1, N)\n" + "(3, 0, TYPE, VARCHAR, 1, 0, -1, N)\n"

		+ "(0, 2, INDEXID, INT, 4, 0, -1, N)\n" + "(1, 2, INDEXNAME, VARCHAR, 128, 0, 1, N)\n" + "(2, 2, TABLEID, INT, 4, 0, 0, N)\n" + "(3, 2, UNIQUE, VARCHAR, 1, 0, -1, N)\n"

		+ "(0, 3, INDEXID, INT, 4, 0, 1, N)\n" + "(1, 3, TABLEID, INT, 4, 0, 0, N)\n" + "(2, 3, COLID, INT, 4, 0, 2, N)\n" + "(3, 3, POSITION, INT, 4, 0, -1, N)\n" + "(4, 3, ORDER, VARCHAR, 1, 0, -1, N)\n"

		+ "(0, 4, VIEWID, INT, 4, 0, -1, N)\n" + "(1, 4, SCHEMA, VARCHAR, 128, 0, 0, N)\n" + "(2, 4, NAME, VARCHAR, 128, 0, 1, N)\n" + "(3, 4, TEXT, VARCHAR, 32768, 0, -1, N)\n"

		+ "(0, 5, TABLEID, INT, 4, 0, 0, N)\n" + "(1, 5, CARD, BIGINT, 8, 0, -1, N)\n"

		+ "(0, 6, NODEID, INT, 4, 0, -1, N)\n" + "(1, 6, HOSTNAME, VARCHAR, 128, 0, 0, N)\n" + "(2, 6, TYPE, VARCHAR, 1, 0, -1, N)\n" + "(3, 6, RACK, VARCHAR, 128, 0, -1, N)\n"

		+ "(0, 7, TABLEID, INT, 4, 0, 0, N)\n" + "(1, 7, COLID, INT, 4, 0, 1, N)\n" + "(2, 7, CARD, BIGINT, 8, 0, -1, N)\n"

		+ "(0, 8, TABLEID, INT, 4, 0, 0, N)\n" + "(1, 8, COLID, INT, 4, 0, 1, N)\n" + "(2, 8, LOW, VARCHAR, 4096, 0, -1, N)\n" + "(3, 8, Q1, VARCHAR, 4096, 0, -1, N)\n" + "(4, 8, Q2, VARCHAR, 4096, 0, -1, N)\n" + "(5, 8, Q3, VARCHAR, 4096, 0, -1, N)\n" + "(6, 8, HIGH, VARCHAR, 4096, 0, -1, N)\n"

		+ "(0, 9, FIRST, INT, 4, 0, 0, N)\n" + "(1, 9, SECOND, INT, 4, 0, -1, N)\n" + "(2, 9, THIRD, INT, 4, 0, -1, N)\n"

		+ "(0, 10, NODE, INT, 4, 0, 0, N)\n" + "(1, 10, STATE, VARCHAR, 1, 0, -1, N)\n"

		+ "(0, 11, TABLEID, INT, 4, 0, 0, N)\n" + "(1, 11, GROUPEXP, VARCHAR, 8192, 0, -1, N)\n" + "(2, 11, NODEEXP, VARCHAR, 8192, 0, -1, N)\n" + "(3, 11, DEVICEEXP, VARCHAR, 8192, 0, -1, N)\n"

		+ "(0, 12, TABLEID, INT, 4, 0, 0, N)\n" + "(1, 12, INDEXID, INT, 4, 0, 1, N)\n" + "(2, 12, NUMDISTINCT, BIGINT, 8, 0, -1, N)\n"

		+ "SYS.VIEWS(INT, VARCHAR, VARCHAR, VARCHAR)\n" + "0\n"

		+ "SYS.INDEXES(INT, VARCHAR, INT, VARCHAR)\n" + "15\n" + "(0, PKTABLES, 0, Y)\n" + "(0, PKCOLUMNS, 1, Y)\n" + "(0, PKINDEXES, 2, Y)\n" + "(0, PKINDEXCOLS, 3, Y)\n" + "(0, PKVIEWS, 4, Y)\n" + "(0, PKTABLESTATS, 5, Y)\n" + "(0, PKNODES, 6, Y)\n" + "(0, PKCOLSTATS, 7, Y)\n" + "(0, PKCOLDIST, 8, Y)\n" + "(0, PKBACKUPS, 9, Y)\n" + "(0, PKNODESTATE, 10, Y)\n" + "(0, PKPARTITIONING, 11, Y)\n" + "(0, PKINDEXSTATS, 12, Y)\n" + "(1, SKNODES, 6, Y)\n" + "(1, SKINDEXES, 2, N)\n"

		+ "SYS.INDEXCOLS(INT, INT, INT, INT, VARCHAR)\n" + "24\n" + "(0, 0, 1, 0, A)\n" // TABLES(SCHEMA,
		// NAME)
				+ "(0, 0, 2, 1, A)\n" + "(0, 1, 1, 0, A)\n" // COLUMNS(TABLEID,
				// COLNAME)
				+ "(0, 1, 2, 1, A)\n" + "(0, 2, 1, 1, A)\n" // INDEXES(TABLEID,
				// INDEXNAME)
				+ "(0, 2, 2, 0, A)\n" + "(0, 3, 0, 1, A)\n" // INDEXCOLS(TABLE,
				// INDEX, COL)
				+ "(0, 3, 1, 0, A)\n" + "(0, 3, 2, 2, A)\n" + "(0, 4, 1, 0, A)\n" // VIEWS(SCHEMA,
				// NAME)
				+ "(0, 4, 2, 1, A)\n" + "(0, 5, 0, 0, A)\n" // TABLESTATS(ID)
				+ "(0, 6, 1, 0, A)\n" // NODES(HOSTNAME)
				+ "(0, 7, 0, 0, A)\n" // COLSTATS(TABLE, COL)
				+ "(0, 7, 1, 1, A)\n" + "(0, 8, 0, 0, A)\n" // COLDIST(TABLE,
				// COL)
				+ "(0, 8, 1, 1, A)\n" + "(0, 9, 0, 0, A)\n" // BACKUPS(FIRST)
				+ "(0, 10, 0, 0, A)\n" // NODESTATE(NODE)
				+ "(0, 11, 0, 0, A)\n" // PARTITIONING(TABLE)
				+ "(0, 12, 0, 0, A)\n" // INDEXSTATS(TABLE, INDEX)
				+ "(0, 12, 1, 1, A)\n" + "(1, 6, 0, 0, A)\n" // NODES(NODEID)
				+ "(1, 2, 1, 0, A)\n" // INDEXES(INDEXNAME)

				+ "SYS.TABLESTATS(INT, BIGINT)\n" + "0\n"

				+ "SYS.NODES(INT, VARCHAR, VARCHAR, VARCHAR)\n" + "0\n"

				+ "SYS.COLSTATS(INT, INT, BIGINT)\n" + "0\n"

				+ "SYS.COLDIST(INT, INT, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR)\n" + "0\n"

				+ "SYS.PARTITIONING(INT, VARCHAR, VARCHAR, VARCHAR)\n" + "13\n" + "(0, NONE, {-1}, {0})\n" + "(1, NONE, {-1}, {0})\n" + "(2, NONE, {-1}, {0})\n" + "(3, NONE, {-1}, {0})\n" + "(4, NONE, {-1}, {0})\n" + "(5, NONE, {-1}, {0})\n" + "(6, NONE, {-1}, {0})\n" + "(7, NONE, {-1}, {0})\n" + "(8, NONE, {-1}, {0})\n" + "(9, NONE, {-1}, {0})\n" + "(10, NONE, {-1}, {0})\n" + "(11, NONE, {-1}, {0})\n" + "(12, NONE, {-1}, {0})\n"

				+ "SYS.INDEXSTATS(INT, INT, BIGINT)\n" + "0\n";

		final PrintWriter out = new PrintWriter(new FileWriter("CatalogCreator.java", false));

		final Vector<String> tableNames = new Vector<String>();

		final StringTokenizer lines = new StringTokenizer(in, "\n", false);
		boolean tableLine = true;
		boolean numberLine = false;
		int numLines = 0;
		Vector<String> tbldata = null;
		while (lines.hasMoreTokens())
		{
			final String line = lines.nextToken();
			if (tableLine)
			{
				if (!line.startsWith("SYS"))
				{
					HRDBMSWorker.logger.error("Looking for a table header but found: " + line);
					System.exit(1);
				}

				tableLines.add(line);
				tableLine = false;
				numberLine = true;
				tbldata = new Vector<String>();
				HRDBMSWorker.logger.debug("Added a table line.");
			}
			else if (numberLine)
			{
				numLines = Integer.parseInt(line);
				numberLine = false;
				HRDBMSWorker.logger.debug("Parsed a number line.");

				if (numLines == 0)
				{
					tableLine = true;
					data.add(tbldata);
				}
			}
			else
			{
				tbldata.add(line);
				HRDBMSWorker.logger.debug("Added a data line.");
				numLines--;

				if (numLines == 0)
				{
					tableLine = true;
					data.add(tbldata);
				}
			}
		}

		if (!tableLine)
		{
			HRDBMSWorker.logger.error("Hit end of input unexpectedly");
			System.exit(1);
		}

		HRDBMSWorker.logger.debug("Done parsing catalog input data.");

		for (final String header : tableLines)
		{
			final StringTokenizer parse = new StringTokenizer(header, "(,", false);
			final Vector<String> temp = new Vector<String>();
			tableNames.add(parse.nextToken().trim());
			String token = parse.nextToken().trim();
			while (!token.endsWith(")"))
			{
				temp.add(token.trim());
				token = parse.nextToken().trim();
			}

			token = token.substring(0, token.length() - 1);
			temp.add(token.trim());
			colTypes.add(temp);
		}
		HRDBMSWorker.logger.debug("Done parsing table names and column types.");

		if (HRDBMSWorker.type == HRDBMSWorker.TYPE_MASTER)
		{
			HRDBMSWorker.logger.debug("Calling createNodes().");
			createNodes();
			HRDBMSWorker.logger.debug("createNodes() has finished.");
			sendNodesToCoords();
			HRDBMSWorker.logger.debug("sendNodesToCoords() has finished.");
		}
		else
		{
			HRDBMSWorker.logger.debug("Calling receiveAndCreateNodes().");
			receiveAndCreateNodes();
			HRDBMSWorker.logger.debug("receiveAndCreateNodes() has finished.");
		}

		createOutputHeader(out);
		HRDBMSWorker.logger.debug("createOutputHeader() finished.");
		createTableStats(tableLines, data);
		HRDBMSWorker.logger.debug("createTableStats() finished.");
		createIndexStats(tableLines, data);
		HRDBMSWorker.logger.debug("createIndexStats() finished.");
		createColStats(tableLines, data);
		HRDBMSWorker.logger.debug("createColStats() finished.");
		createBackups();
		HRDBMSWorker.logger.debug("createBackups() finished.");
		createNodeState();
		HRDBMSWorker.logger.debug("createNodeState() finished.");

		int i = 0;
		for (final Vector<String> table : data)
		{
			HRDBMSWorker.logger.debug("Starting processing for table " + tableNames.get(i));
			final int dataSize = calcDataSize(table, colTypes.get(i));
			HRDBMSWorker.logger.debug("calcDataSize() done for table " + i);
			sizes.add(dataSize);
			createTableHeader(out, tableNames.get(i), table.size(), colTypes.get(i).size(), dataSize);
			HRDBMSWorker.logger.debug("createTableHeader() done for table " + i);
			writeOffsetArray(out, table, dataSize, colTypes.get(i));
			HRDBMSWorker.logger.debug("writeOffsetArray() done for table " + i);
			writeData(out, table, dataSize, colTypes.get(i));
			HRDBMSWorker.logger.debug("writeData() done for table " + i);
			i++;
		}
		HRDBMSWorker.logger.debug("Finished writing code for all tables.");

		createIndexes(out);
		HRDBMSWorker.logger.debug("createIndexes() finished.");
		createOutputTrailer(out, data, tableLines);
		HRDBMSWorker.logger.debug("createOutputTrailer() finished.");
		out.close();
		HRDBMSWorker.logger.info("Processed " + i + " tables while building catalog");

		if (HRDBMSWorker.type == HRDBMSWorker.TYPE_MASTER)
		{
			sendCompletionChecks();
			HRDBMSWorker.logger.debug("sendCompletionChecks() finished.");
			for (final Socket sock : sockets.values())
			{
				sock.close();
			}
		}
		else
		{
			receiveCompletionCheck();
			HRDBMSWorker.logger.debug("receiveCompletionCheck() finished.");
		}

		HRDBMSWorker.logger.debug("buildCode() is returning!");
	}

	public static boolean isThisMyIpAddress(InetAddress addr)
	{
		// Check if the address is a valid special local or loop back
		if (addr.isAnyLocalAddress() || addr.isLoopbackAddress())
		{
			return true;
		}

		// Check if the address is defined on any interface
		try
		{
			return NetworkInterface.getByInetAddress(addr) != null;
		}
		catch (final SocketException e)
		{
			return false;
		}
	}

	private static void buildIndexData(PrintWriter out, TreeMap<TextRowSorter, RID> keys2RID, int numKeys, String name, boolean unique) throws Exception
	{
		final ByteBuffer data = ByteBuffer.allocate(Page.BLOCK_SIZE);
		buildIndexDataBuffer(keys2RID, numKeys, data, unique);
		String fn = HRDBMSWorker.getHParms().getProperty("data_directories");
		StringTokenizer tokens = new StringTokenizer(fn, ",", false);
		fn = tokens.nextToken();
		if (!fn.endsWith("/"))
		{
			fn += "/";
		}
		fn += (name + ".indx");

		try
		{
			final FileChannel fc = FileManager.getFile(fn);
			data.position(0);
			fc.write(data);
			fc.force(false);
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.debug("Exception while creating index: " + fn, e);
			throw e;
		}
	}

	private static void buildIndexDataBuffer(TreeMap<TextRowSorter, RID> keys2RID, int numKeys, ByteBuffer data, boolean unique) throws UnsupportedEncodingException
	{
		data.position(0);
		data.putInt(numKeys); // num key cols
		if (unique)
		{
			data.put((byte)1); // unique
		}
		else
		{
			data.put((byte)0); // not unique
		}
		data.putInt(0); // first free
		data.putInt(0); // head points to block 0
		data.putInt(17); // head point to offset 17

		data.put((byte)3); // start record
		data.putInt(0); // left
		data.putInt(0);
		data.putInt(0); // right
		data.putInt(0);
		data.putInt(0); // up
		data.putInt(0);
		data.putInt(0); // down
		data.putInt(0);

		// fill in first free val pointer
		int pos = data.position();
		data.position(5);
		data.putInt(pos); // first free byte
		data.position(pos);

		while (pos < (Page.BLOCK_SIZE))
		{
			data.put((byte)2); // fill page out with 2s
			pos++;
		}

		for (final Map.Entry<TextRowSorter, RID> entry : keys2RID.entrySet())
		{
			indexPut(data, entry.getKey().getKeyBytes(), entry.getValue());
		}
	}

	private static int calcDataSize(Vector<String> table, Vector<String> types)
	{
		HRDBMSWorker.logger.debug("In calcDataSize().  Table has " + table.size() + " rows");
		int total = 0;

		for (final String row : table)
		{
			HRDBMSWorker.logger.debug("Starting processing of a row");
			final StringTokenizer tokens = new StringTokenizer(row, ",", false);
			int i = 0;
			while (tokens.hasMoreTokens())
			{
				String token = tokens.nextToken().trim();
				HRDBMSWorker.logger.debug("Starting next col: " + token);
				if (token.startsWith("("))
				{
					token = token.substring(1);
					HRDBMSWorker.logger.debug("Processed first token of the row");
				}

				if (token.endsWith(")"))
				{
					token = token.substring(0, token.length() - 1);
					HRDBMSWorker.logger.debug("Processed last token of the row.");
				}

				HRDBMSWorker.logger.debug("About to get type for position " + i + ". Type list has size " + types.size());
				final String type = types.get(i);
				token = token.trim();
				HRDBMSWorker.logger.debug("Type for this column is " + type);
				if (type.equals("INT"))
				{
					if (!token.equals("null"))
					{
						total += 4;
					}
				}
				else if (type.equals("BIGINT"))
				{
					if (!token.equals("null"))
					{
						total += 8;
					}
				}
				else if (type.equals("VARCHAR"))
				{
					if (!token.equals("null"))
					{
						total += (3 + token.length());
					}
				}
				else
				{
					HRDBMSWorker.logger.error("Unknown type: " + type);
					System.exit(1);
				}

				HRDBMSWorker.logger.debug("Done with token");
				i++;
			}
			HRDBMSWorker.logger.debug("Done with all cols.");
			if (i != types.size())
			{
				HRDBMSWorker.logger.error("Row has wrong number of columns: " + row);
				System.exit(1);
			}
			HRDBMSWorker.logger.debug("Done with row.");
		}

		return total;
	}

	private static void calculateVariables(Vector<Vector<String>> data, Vector<String> tableLines)
	{
		HRDBMSWorker.logger.debug("In calculateVariables() with " + vars.keySet().size() + " variables to process.");
		for (final String var : vars.keySet())
		{
			HRDBMSWorker.logger.debug("Processing " + var);
			if (var.equals("!tablerows!"))
			{
				int i = 0;
				for (final String header : tableLines)
				{
					if (header.startsWith("SYS.TABLES("))
					{
						vars.put("!tablerows!", new Long(data.get(i).size()));
						break;
					}

					i++;
				}
			}
			else if (var.equals("!tableavgrow!"))
			{
				int i = 0;
				for (final String header : tableLines)
				{
					if (header.startsWith("SYS.TABLES("))
					{
						try
						{
							vars.put("!tableavgrow!", new Long(sizes.get(i) / data.get(i).size()));
						}
						catch (final Exception e)
						{
							vars.put("!tableavgrow!", new Long(0));
						}
						break;
					}

					i++;
				}
			}
			else if (var.equals("!tablefkc!"))
			{
				long card;

				card = getFullKeyCard("SYS.TABLES");
				if (card != -1)
				{
					vars.put("!tablefkc!", card);
				}
			}
			if (var.equals("!backuprows!"))
			{
				int i = 0;
				for (final String header : tableLines)
				{
					if (header.startsWith("SYS.BACKUPS("))
					{
						vars.put("!backuprows!", new Long(data.get(i).size()));
						break;
					}

					i++;
				}
			}
			else if (var.equals("!backupfkc!"))
			{
				long card;

				card = getFullKeyCard("SYS.BACKUPS");
				if (card != -1)
				{
					vars.put("!backupfkc!", card);
				}
			}
			if (var.equals("!nodestaterows!"))
			{
				int i = 0;
				for (final String header : tableLines)
				{
					if (header.startsWith("SYS.NODESTATE("))
					{
						vars.put("!nodestaterows!", new Long(data.get(i).size()));
						break;
					}

					i++;
				}
			}
			else if (var.equals("!nodestatefkc!"))
			{
				long card;
				card = getFullKeyCard("SYS.NODESTATE");
				if (card != -1)
				{
					vars.put("!nodestatefkc!", card);
				}
			}
			else if (var.equals("!columnrows!"))
			{
				int i = 0;
				for (final String header : tableLines)
				{
					if (header.startsWith("SYS.COLUMNS("))
					{
						vars.put("!columnrows!", new Long(data.get(i).size()));
						break;
					}

					i++;
				}
			}
			else if (var.equals("!columnfkc!"))
			{
				long card;

				card = getFullKeyCard("SYS.COLUMNS");
				if (card != -1)
				{
					vars.put("!columnfkc!", card);
				}
			}
			else if (var.equals("!indexrows!"))
			{
				int i = 0;
				for (final String header : tableLines)
				{
					if (header.startsWith("SYS.INDEXES("))
					{
						vars.put("!indexrows!", new Long(data.get(i).size()));
						break;
					}

					i++;
				}
			}
			else if (var.equals("!indexfkc!"))
			{
				long card;

				card = getFullKeyCard("SYS.INDEXES");
				if (card != -1)
				{
					vars.put("!indexfkc!", card);
				}
			}
			else if (var.equals("!indexcolrows!"))
			{
				int i = 0;
				for (final String header : tableLines)
				{
					if (header.startsWith("SYS.INDEXCOLS("))
					{
						vars.put("!indexcolrows!", new Long(data.get(i).size()));
						break;
					}

					i++;
				}
			}
			else if (var.equals("!indexcolfkc!"))
			{
				long card;

				card = getFullKeyCard("SYS.INDEXCOLS");
				if (card != -1)
				{
					vars.put("!indexcolfkc!", card);
				}
			}
			else if (var.equals("!viewrows!"))
			{
				int i = 0;
				for (final String header : tableLines)
				{
					if (header.startsWith("SYS.VIEWS("))
					{
						vars.put("!viewrows!", new Long(data.get(i).size()));
						break;
					}

					i++;
				}
			}
			else if (var.equals("!viewfkc!"))
			{
				long card;

				card = getFullKeyCard("SYS.VIEWS");
				if (card != -1)
				{
					vars.put("!viewfkc!", card);
				}
			}
			else if (var.equals("!tablestatrows!"))
			{
				int i = 0;
				for (final String header : tableLines)
				{
					if (header.startsWith("SYS.TABLESTATS("))
					{
						vars.put("!tablestatrows!", new Long(data.get(i).size()));
						break;
					}

					i++;
				}
			}
			else if (var.equals("!tablestatfkc!"))
			{
				long card;

				card = getFullKeyCard("SYS.TABLESTATS");
				if (card != -1)
				{
					vars.put("!tablestatfkc!", card);
				}
			}
			else if (var.equals("!indexstatrows!"))
			{
				int i = 0;
				for (final String header : tableLines)
				{
					if (header.startsWith("SYS.INDEXSTATS("))
					{
						vars.put("!indexstatrows!", new Long(data.get(i).size()));
						break;
					}

					i++;
				}
			}
			else if (var.equals("!indexstatfkc!"))
			{
				long card;

				card = getFullKeyCard("SYS.INDEXSTATS");
				if (card != -1)
				{
					vars.put("!indexstatfkc!", card);
				}
			}
			else if (var.equals("!noderows!"))
			{
				int i = 0;
				for (final String header : tableLines)
				{
					if (header.startsWith("SYS.NODES("))
					{
						vars.put("!noderows!", new Long(data.get(i).size()));
						break;
					}

					i++;
				}
			}
			else if (var.equals("!nodefkc!"))
			{
				long card;

				card = getFullKeyCard("SYS.NODES");
				if (card != -1)
				{
					vars.put("!nodefkc!", card);
				}
			}
			else if (var.equals("!partrows!"))
			{
				int i = 0;
				for (final String header : tableLines)
				{
					if (header.startsWith("SYS.PARTITIONING("))
					{
						vars.put("!partrows!", new Long(data.get(i).size()));
						break;
					}

					i++;
				}
			}
			else if (var.equals("!partfkc!"))
			{
				long card;

				card = getFullKeyCard("SYS.PARTITIONING");
				if (card != -1)
				{
					vars.put("!partfkc!", card);
				}
			}
			else if (var.equals("!colstatrows!"))
			{
				int i = 0;
				for (final String header : tableLines)
				{
					if (header.startsWith("SYS.COLSTATS("))
					{
						vars.put("!colstatrows!", new Long(data.get(i).size()));
						break;
					}

					i++;
				}
			}
			else if (var.equals("!colstatfkc!"))
			{
				long card;

				card = getFullKeyCard("SYS.COLSTATS");
				if (card != -1)
				{
					vars.put("!colstatfkc!", card);
				}
			}
			else if (var.equals("!coldistrows!"))
			{
				int i = 0;
				for (final String header : tableLines)
				{
					if (header.startsWith("SYS.COLDIST"))
					{
						vars.put("!coldistrows!", new Long(data.get(i).size()));
						break;
					}

					i++;
				}
			}
			else if (var.equals("!coldistfkc!"))
			{
				long card;

				card = getFullKeyCard("SYS.COLDIST");
				if (card != -1)
				{
					vars.put("!coldistfkc!", card);
				}
			}
		}

		HRDBMSWorker.logger.debug("Calling createColStatVars()");
		createColStatVars();
	}

	private static void createBackups()
	{
		final Vector<String> nTable = getTable("SYS.NODES", tableLines, data);
		final Vector<String> bTable = getTable("SYS.BACKUPS", tableLines, data);
		final HashMap<Integer, String> nodes2Rack = new HashMap<Integer, String>();

		for (final String nRow : nTable)
		{
			final StringTokenizer tokens = new StringTokenizer(nRow, ",", false);
			final int id = Integer.parseInt(tokens.nextToken().substring(1).trim());
			tokens.nextToken();
			final String type = tokens.nextToken().trim().toUpperCase();
			final String rack = tokens.nextToken().trim();

			if (type.equals("W"))
			{
				nodes2Rack.put(id, rack);
			}
		}

		if (nodes2Rack.size() == 0)
		{
			HRDBMSWorker.logger.error("No worker nodes are defined!");
			HRDBMSWorker.logger.error("Aborting catalog creation.");
			System.exit(1);
		}

		if (nodes2Rack.size() == 1)
		{
			for (final Map.Entry<Integer, String> entry : nodes2Rack.entrySet())
			{
				bTable.add("(" + entry.getKey() + ", null, null)");
			}

			return;
		}

		if (nodes2Rack.size() == 2)
		{
			final int[] ids = new int[2];
			int i = 0;
			for (final Map.Entry<Integer, String> entry : nodes2Rack.entrySet())
			{
				ids[i] = entry.getKey();
				i++;
			}
			bTable.add("(" + ids[0] + "," + ids[1] + ",null)");
			bTable.add("(" + ids[1] + "," + ids[0] + ",null)");
			return;
		}

		final HashMap<Integer, Integer> nodes2NumTables = new HashMap<Integer, Integer>();
		for (final Map.Entry<Integer, String> entry : nodes2Rack.entrySet())
		{
			nodes2NumTables.put(entry.getKey(), 1);
		}

		for (final Map.Entry<Integer, String> entry : nodes2Rack.entrySet())
		{
			final int local = getNodePreferLocal(entry.getKey(), entry.getValue(), nodes2Rack, nodes2NumTables);
			bTable.add("(" + entry.getKey() + "," + local + "," + getNodePreferNonLocal(entry.getKey(), local, entry.getValue(), nodes2Rack, nodes2NumTables) + ")");
		}
	}

	private static void createColStats(Vector<String> tableLines, Vector<Vector<String>> data)
	{
		final Vector<String> tTable = getTable("SYS.TABLES", tableLines, data);

		for (final String tRow : tTable)
		{
			final StringTokenizer tokens = new StringTokenizer(tRow, ",", false);
			final int tableId = Integer.parseInt(tokens.nextToken().substring(1).trim());
			final int numCols = numCols(tableId);
			int i = 0;
			final Vector<String> csTable = getTable("SYS.COLSTATS", tableLines, data);
			while (i < numCols)
			{
				final String row = "(" + tableId + "," + i + ",!colcard" + tableId + "_" + i + "!)";
				csTable.add(row);
				i++;
			}
		}
	}

	private static void createColStatVars()
	{
		HRDBMSWorker.logger.debug("Entered createColStatVars()");
		final Vector<String> tTable = getTable("SYS.TABLES", tableLines, data);

		for (final String tRow : tTable)
		{
			HRDBMSWorker.logger.debug("Processing row " + tRow);
			final StringTokenizer tokens = new StringTokenizer(tRow, ",", false);
			final int tableId = Integer.parseInt(tokens.nextToken().substring(1).trim());
			tokens.nextToken();
			final String tName = tokens.nextToken().trim();
			final int numCols = numCols(tableId);
			HRDBMSWorker.logger.debug("ID: " + tableId + " Name: " + tName + " Cols: " + numCols);
			int i = 0;
			// Vector<String> table = getTable("SYS." + tName, tableLines,
			// data);
			while (i < numCols)
			{
				final int[] index = new int[1];
				index[0] = i;
				HRDBMSWorker.logger.debug("Calculating # of nulls for col " + i);
				vars.put("!nulls" + tableId + "_" + i + "!", new Long(getNulls("SYS." + tName, index[0])));
				HRDBMSWorker.logger.debug("Calculating card for col " + i);
				final int card = getCompositeColCard("SYS." + tName, index, 0);

				if (card != -1)
				{
					vars.put("!colcard" + tableId + "_" + i + "!", new Long(card));
				}

				i++;
			}
		}
	}

	private static void createIndexes(PrintWriter out) throws Exception
	{
		HRDBMSWorker.logger.debug("Entered createIndexes()");
		final Vector<String> iTable = getTable("SYS.INDEXES", tableLines, data);
		final Vector<String> icTable = getTable("SYS.INDEXCOLS", tableLines, data);
		final Vector<String> cTable = getTable("SYS.COLUMNS", tableLines, data);
		final Vector<String> tTable = getTable("SYS.TABLES", tableLines, data);

		for (final String iRow : iTable)
		{
			HRDBMSWorker.logger.debug("Processing row " + iRow);
			final StringTokenizer iRowST = new StringTokenizer(iRow, ",", false);
			final int indexID = Integer.parseInt(iRowST.nextToken().substring(1).trim());
			final String iName = iRowST.nextToken().trim();
			final int tableID = Integer.parseInt(iRowST.nextToken().trim());
			boolean unique = iRowST.nextToken().trim().substring(0, 1).equals("Y");
			final int numKeys = numKeys(tableID, indexID);
			String tName = null;

			for (final String tRow : tTable)
			{
				final StringTokenizer tRowST = new StringTokenizer(tRow, ",", false);
				final int tableID2 = Integer.parseInt(tRowST.nextToken().substring(1).trim());
				if (tableID == tableID2)
				{
					HRDBMSWorker.logger.debug("Found matching TABLES row.");
					tRowST.nextToken();
					tName = tRowST.nextToken().trim();
				}
			}

			final TreeMap<Integer, Integer> pos2ColID = new TreeMap<Integer, Integer>();

			for (final String icRow : icTable)
			{
				final StringTokenizer icRowST = new StringTokenizer(icRow, ",", false);
				final int indexID2 = Integer.parseInt(icRowST.nextToken().substring(1).trim());
				final int tableID2 = Integer.parseInt(icRowST.nextToken().trim());

				if (tableID == tableID2 && indexID == indexID2)
				{
					HRDBMSWorker.logger.debug("Found a matching row in INDEXCOLS");
					final int colID = Integer.parseInt(icRowST.nextToken().trim());
					final int pos = Integer.parseInt(icRowST.nextToken().trim());
					pos2ColID.put(pos, colID);
				}
			}

			final TreeMap<Integer, String> pos2Type = new TreeMap<Integer, String>();
			for (final Map.Entry<Integer, Integer> entry : pos2ColID.entrySet())
			{
				for (final String cRow : cTable)
				{
					// if table id and col id matches, get column type and store
					// in pos2Type
					final StringTokenizer cRowTS = new StringTokenizer(cRow, ",", false);
					final int colID3 = Integer.parseInt(cRowTS.nextToken().substring(1).trim());
					final int tableID3 = Integer.parseInt(cRowTS.nextToken().trim());

					if (colID3 == entry.getValue() && tableID3 == tableID)
					{
						HRDBMSWorker.logger.debug("Found a matching row in COLUMNS");
						cRowTS.nextToken();
						final String type = cRowTS.nextToken().trim();
						pos2Type.put(entry.getKey(), type);
					}
				}
			}

			HRDBMSWorker.logger.debug("Ready to start reading base table: " + tName);
			final Vector<String> table = getTable("SYS." + tName, tableLines, data);
			HRDBMSWorker.logger.debug("Fetched the base table.");
			HRDBMSWorker.logger.debug("Table has " + table.size() + " rows");
			final TreeMap<TextRowSorter, RID> keys2RIDs = new TreeMap<TextRowSorter, RID>();
			int rowNum = 0;
			for (final String row : table)
			{
				HRDBMSWorker.logger.debug("Reading base table row");
				// get cols in order, create a TextRowSorter object and RID and
				// add to keys2RIDs
				final TextRowSorter keys = new CatalogCode().new TextRowSorter();
				for (final Map.Entry<Integer, Integer> entry : pos2ColID.entrySet())
				{
					HRDBMSWorker.logger.debug("Index position " + entry.getKey() + " is colid " + entry.getValue());
					final StringTokenizer tokens = new StringTokenizer(row, ",", false);
					int i = 0;
					while (i < entry.getValue())
					{
						tokens.nextToken();
						i++;
					}

					String col = tokens.nextToken().trim();

					if (entry.getValue() == 0)
					{
						col = col.substring(1);
					}

					if (col.endsWith(")"))
					{
						col = col.substring(0, col.length() - 1);
					}
					HRDBMSWorker.logger.debug("Index token is " + col);
					final String type = pos2Type.get(entry.getKey());
					HRDBMSWorker.logger.debug("Type is " + type);
					if (type.equals("INT"))
					{
						keys.add(Integer.parseInt(col));
					}
					else if (type.equals("VARCHAR"))
					{
						keys.add(col);
					}
					else
					{
						HRDBMSWorker.logger.error("Unknown type in createIndex()");
						System.exit(1);
					}
				}

				keys2RIDs.put(keys, new RID(-1, 0, 1, rowNum));
				HRDBMSWorker.logger.debug("Adding key/RID pair.");
				rowNum++;
			}

			HRDBMSWorker.logger.debug("Calling buildIndexData()");
			try
			{
				buildIndexData(out, keys2RIDs, numKeys, "SYS." + iName, unique);
			}
			catch (Exception e)
			{
				HRDBMSWorker.logger.debug("", e);
				throw e;
			}
		}
	}

	private static void createIndexStats(Vector<String> tableLines, Vector<Vector<String>> data)
	{
		Vector<String> isTable = null;
		Vector<String> iTable = null;
		Vector<String> tTable = null;
		isTable = getTable("SYS.INDEXSTATS", tableLines, data);
		iTable = getTable("SYS.INDEXES", tableLines, data);
		tTable = getTable("SYS.TABLES", tableLines, data);

		for (final String iRow : iTable)
		{
			final StringTokenizer tokens = new StringTokenizer(iRow, ",", false);
			final int indexId = Integer.parseInt(tokens.nextToken().trim().substring(1));
			tokens.nextToken();
			final int tableId = Integer.parseInt(tokens.nextToken().trim());
			String token = null;

			for (final String tRow : tTable)
			{
				final StringTokenizer tableRow = new StringTokenizer(tRow, ",", false);
				if (Integer.parseInt(tableRow.nextToken().trim().substring(1)) == tableId)
				{
					tableRow.nextToken();
					token = tableRow.nextToken().trim(); // table name
					break;
				}
			}

			String row = "(" + tableId + "," + indexId + ",";
			if (token.equals("TABLES"))
			{
				row += "!tablefkc!)";
			}
			else if (token.equals("BACKUPS"))
			{
				row += "!backupfkc!)";
			}
			else if (token.equals("NODESTATE"))
			{
				row += "!nodestatefkc!)";
			}
			else if (token.equals("COLUMNS"))
			{
				row += "!columnfkc!)";
			}
			else if (token.equals("INDEXES"))
			{
				row += "!indexfkc!)";
			}
			else if (token.equals("INDEXCOLS"))
			{
				row += "!indexcolfkc!)";
			}
			else if (token.equals("VIEWS"))
			{
				row += "!viewfkc!)";
			}
			else if (token.equals("TABLESTATS"))
			{
				row += "!tablestatfkc!)";
			}
			else if (token.equals("INDEXSTATS"))
			{
				row += "!indexstatfkc!)";
			}
			else if (token.equals("COLSTATS"))
			{
				row += "!colstatfkc!)";
			}
			else if (token.equals("COLGROUPSTATS"))
			{
				row += "!colgroupstatfkc!)";
			}
			else if (token.equals("NODES"))
			{
				row += "!nodefkc!)";
			}
			else if (token.equals("PARTITIONING"))
			{
				row += "!partfkc!)";
			}
			else if (token.equals("COLDIST"))
			{
				row += "!coldistfkc!)";
			}
			else
			{
				HRDBMSWorker.logger.error("Unknown table: " + token);
				System.exit(1);
			}

			isTable.add(row);
		}
	}

	private static void createNodes() throws IOException
	{
		final BufferedReader nodes = new BufferedReader(new FileReader(new File("nodes.cfg")));
		String line = nodes.readLine();
		int workerId = 0;
		int coordId = -2;
		while (line != null)
		{
			final StringTokenizer tokens = new StringTokenizer(line, ",", false);
			final String host = tokens.nextToken().trim();
			final String type = tokens.nextToken().trim().toUpperCase();
			final String rack = tokens.nextToken().trim();
			racks.add(rack);
			final Vector<String> nTable = getTable("SYS.NODES", tableLines, data);

			if (type.equals("C"))
			{
				final String row = "(" + coordId + "," + host + "," + type + "," + rack + ")";
				nTable.add(row);
				coordId--;
			}
			else if (type.equals("W"))
			{
				final String row = "(" + workerId + "," + host + "," + type + "," + rack + ")";
				nTable.add(row);
				workerId++;
			}
			else
			{
				HRDBMSWorker.logger.error("Type found in nodes.cfg was not valid: " + type);
				System.exit(1);
			}

			if (type.equals("C") && !isThisMyIpAddress(InetAddress.getByName(host)))
			{
				final int port = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("catalog_sync_port"));
				HRDBMSWorker.logger.debug("Attempting to create connection to " + host + " on port " + port);
				final Socket sock = new Socket();
				sock.setReceiveBufferSize(262144);
				sock.setSendBufferSize(262144);
				HRDBMSWorker.logger.debug("Socket created.");
				while (true)
				{
					try
					{
						Thread.sleep(Long.parseLong(HRDBMSWorker.getHParms().getProperty("catalog_creation_tcp_wait_ms")));
						HRDBMSWorker.logger.debug("Attempting connection to " + host + " on port " + port + " with a timeout of 1 sec");
						sock.connect(new InetSocketAddress(InetAddress.getByName(host), port), 1000);
						final byte[] dummy = new String("DUMMY   ").getBytes("UTF-8");
						sock.getOutputStream().write(dummy);
						break;
					}
					catch (final SocketTimeoutException e)
					{
						HRDBMSWorker.logger.debug("Socket connection timed out.");
						continue;
					}
					catch (final Exception e)
					{
						HRDBMSWorker.logger.error("Exception thrown during connection", e);
						System.exit(1);
					}
				}
				HRDBMSWorker.logger.debug("Connection was successful.");
				sockets.put(host, sock);
			}

			line = nodes.readLine();
		}

		nodes.close();
	}

	private static void createNodeState()
	{
		final Vector<String> nsTable = getTable("SYS.NODESTATE", tableLines, data);
		final Vector<String> nTable = getTable("SYS.NODES", tableLines, data);

		for (final String nRow : nTable)
		{
			final StringTokenizer tokens = new StringTokenizer(nRow, ",", false);
			final int id = Integer.parseInt(tokens.nextToken().substring(1).trim());
			nsTable.add("(" + id + ",A)");
		}
	}

	private static void createOutputHeader(PrintWriter out)
	{
		out.println("import java.io.File;");
		out.println("import java.nio.ByteBuffer;");
		out.println("import java.nio.channels.FileChannel;");
		out.println("import java.io.UnsupportedEncodingException;");
		out.println("import com.exascale.filesystem.Page;");
		out.println("import com.exascale.managers.HRDBMSWorker;");
		out.println("import com.exascale.managers.FileManager;");
		out.println("import com.exascale.tables.Schema;");
		out.println("import java.io.IOException;");
		out.println("import java.io.UnsupportedEncodingException;");
		out.println("import java.util.StringTokenizer;");
		out.println("");
		out.println("public class CatalogCreator");
		out.println("{");
		out.println("\tFile table;");
		out.println("\tString fn;");
		out.println("\tFileChannel fc = null;");
		out.println("\tByteBuffer bb = null;");
		out.println("\tint i = 0;");
		out.println("\tint j = 0;");
		out.println("\tString base = HRDBMSWorker.getHParms().getProperty(\"data_directories\");");
		out.println("");
		out.println("\tpublic CatalogCreator() throws Exception, UnsupportedEncodingException");
		out.println("\t{");
		out.println("StringTokenizer tokens = new StringTokenizer(base, \",\", false);");
		out.println("base = tokens.nextToken();");
		out.println("HRDBMSWorker.logger.debug(\"CatalogCreator is starting\");");
		out.println("\t\tif (!base.endsWith(\"/\"))");
		out.println("\t\t{");
		out.println("\t\t\tbase += \"/\";");
		out.println("\t\t}");
		HRDBMSWorker.logger.debug("Number of tables is " + data.size());

		int i = 0;
		while (i < data.size())
		{
			out.println("\t\tcreateTable" + i + "();");
			i++;
		}
		out.println("\t}");
	}

	private static void createOutputTrailer(PrintWriter out, Vector<Vector<String>> data, Vector<String> tableLines)
	{
		out.println("");
		out.println("\tprivate static void putMedium(ByteBuffer bb, int val)");
		out.println("\t{");
		out.println("\t\tbb.put((byte)((val & 0xff0000) >> 16));");
		out.println("\t\tbb.put((byte)((val & 0xff00) >> 8));");
		out.println("\t\tbb.put((byte)(val & 0xff));");
		out.println("\t}");
		out.println("");
		out.println("\tprivate static void putString(ByteBuffer bb, String val) throws UnsupportedEncodingException");
		out.println("\t{");
		out.println("\t\tbyte[] bytes = val.getBytes(\"UTF-8\");");
		out.println("");
		out.println("\t\tint i = 0;");
		out.println("\t\twhile (i < bytes.length)");
		out.println("\t\t{");
		out.println("\t\t\tbb.put(bytes[i]);");
		out.println("\t\t\ti++;");
		out.println("\t\t}");
		out.println("\t}");
		out.println("");
		HRDBMSWorker.logger.debug("Calling calculateVariables()");
		calculateVariables(data, tableLines);
		HRDBMSWorker.logger.debug("calculateVariables() finished.");
		writeVariables(out);
		HRDBMSWorker.logger.debug("writeVariables() finished.");
		out.println("}");
	}

	private static void createTableHeader(PrintWriter out, String name, int rows, int cols, int dataSize)
	{
		out.println("\tpublic void createTable" + methodNum + "() throws Exception, UnsupportedEncodingException");
		out.println("\t{");
		methodNum++;
		out.println("HRDBMSWorker.logger.debug(\"Starting creation of table " + name + "\");");
		out.println("\t\tfn = base + (\"" + name + ".tbl\");");
		out.println("");
		out.println("\t\ttable = new File(fn);");
		out.println("");
		out.println("\t\tfc = FileManager.getFile(fn);");
		out.println("\t\tbb = ByteBuffer.allocate(Page.BLOCK_SIZE);");
		out.println("\t\tbb.position(0);");
		out.println("\t\tbb.putInt(-1); //node -1");
		out.println("\t\tbb.putInt(0); //device 0");
		out.println("");
		out.println("\t\ti = 8;");
		out.println("\t\twhile (i < Page.BLOCK_SIZE)");
		out.println("\t\t{");
		out.println("\t\t\tbb.putInt(-1);");
		out.println("\t\t\ti += 4;");
		out.println("\t\t}");
		out.println("");
		out.println("\t\tbb.position(0);");
		out.println("\t\tfc.write(bb);");
		out.println("\t\t//done writing first header page");
		out.println("HRDBMSWorker.logger.debug(\"Done writing first header page.\");");
		out.println("");
		// out.println("\t\t\ti = 0;");
		// out.println("\t\t\tbb.position(0);");
		// out.println("\t\t\twhile (i < Page.BLOCK_SIZE)");
		// out.println("\t\t\t{");
		// out.println("\t\t\t\tbb.putLong(-1);");
		// out.println("\t\t\t\ti += 8;");
		// out.println("\t\t\t}");
		// out.println("\t\tj = 1;");
		// out.println("\t\twhile (j < 4096)");
		// out.println("\t\t{");
		// out.println("\t\t\tbb.position(0);");
		// out.println("\t\t\tfc.write(bb);");
		// out.println("\t\t\tj++;");
		// out.println("\t\t}");
		// out.println("\t\thead.position(0);");
		// out.println("\t\tfc.write(head);");
		// out.println("\t\tfc.force(false);");
		// out.println("");
		// out.println("\t\t//done writing header pages");
		out.println("\t\tbb.position(0);");
		out.println("\t\tbb.put(Schema.TYPE_ROW);");
		out.println("\t\tputMedium(bb, " + rows + "); //nextRecNum");
		out.println("\t\tputMedium(bb, 29 + (12 * " + rows + ") + (3 * " + rows + " * " + cols + ") + (" + cols + " * 3)); //headEnd for " + rows + " rows, " + cols + " cols");
		out.println("\t\tputMedium(bb, Page.BLOCK_SIZE - " + dataSize + "); //dataStart, total data size " + dataSize);
		out.println("\t\tbb.putLong(System.currentTimeMillis()); //modTime");
		out.println("\t\tputMedium(bb, 27 + (3 * " + cols + ")); //rowIDListOff for " + cols + " cols");
		out.println("\t\tputMedium(bb, 30 + (12 * " + rows + ") + (3 * " + cols + "));"); // offset
																							// Array
																							// offset
																							// for
																							// " + rows + "rows
																							// " + cols + "
																							// cols");		out.println("\t\tbb.putInt(1);
																							// //freeSpaceListEntries");
		out.println("\t\tputMedium(bb, " + cols + "); //colIDListSize - start of colIDs");
		out.println("");
		out.println("\t\ti = 0;");
		out.println("\t\twhile (i < " + cols + ")");
		out.println("\t\t{");
		out.println("\t\t\tputMedium(bb, i);");
		out.println("\t\t\ti++;");
		out.println("\t\t}");
		out.println("");
		out.println("\t\tputMedium(bb, " + rows + "); //rowIDListSize - start of rowIDs");
		out.println("\t\ti = 0;");
		out.println("\t\twhile (i < " + rows + ")");
		out.println("\t\t{");
		out.println("\t\t\tputMedium(bb, -1); //node -1");
		out.println("\t\t\tputMedium(bb, 0); //device 0");
		out.println("\t\t\tputMedium(bb, 1); //block 1");
		out.println("\t\t\tputMedium(bb, i); //record i");
		out.println("\t\t\ti++;");
		out.println("\t\t}");
		out.println("");
	}

	private static void createTableStats(Vector<String> tableLines, Vector<Vector<String>> data)
	{
		Vector<String> tsTable = null;
		Vector<String> tTable = null;

		int i = 0;
		for (final String header : tableLines)
		{
			if (header.startsWith("SYS.TABLESTATS("))
			{
				tsTable = data.get(i);
			}

			if (header.startsWith("SYS.TABLES("))
			{
				tTable = data.get(i);
			}

			i++;
		}

		for (final String tRow : tTable)
		{
			final StringTokenizer tokens = new StringTokenizer(tRow, ",", false);
			final int tableId = Integer.parseInt(tokens.nextToken().trim().substring(1));
			tokens.nextToken();
			final String token = tokens.nextToken().trim(); // table name

			String row = "(" + tableId + ","; // card, pages, avgrow
			if (token.equals("TABLES"))
			{
				row += "!tablerows!)";
			}
			else if (token.equals("BACKUPS"))
			{
				row += "!backuprows!)";
			}
			else if (token.equals("NODESTATE"))
			{
				row += "!nodestaterows!)";
			}
			else if (token.equals("COLUMNS"))
			{
				row += "!columnrows!)";
			}
			else if (token.equals("INDEXES"))
			{
				row += "!indexrows!)";
			}
			else if (token.equals("INDEXCOLS"))
			{
				row += "!indexcolrows!)";
			}
			else if (token.equals("VIEWS"))
			{
				row += "!viewrows!)";
			}
			else if (token.equals("TABLESTATS"))
			{
				row += "!tablestatrows!)";
			}
			else if (token.equals("INDEXSTATS"))
			{
				row += "!indexstatrows!)";
			}
			else if (token.equals("COLSTATS"))
			{
				row += "!colstatrows!)";
			}
			else if (token.equals("NODES"))
			{
				row += "!noderows!)";
			}
			else if (token.equals("PARTITIONING"))
			{
				row += "!partrows!)";
			}
			else if (token.equals("COLDIST"))
			{
				row += "!coldistrows!)";
			}
			else
			{
				HRDBMSWorker.logger.error("Unknown table: " + token);
				System.exit(1);
			}

			tsTable.add(row);
		}
	}

	private static int getCompositeColCard(String table, int[] colIndexes, int depth)
	{
		HRDBMSWorker.logger.debug("Entering getCompositeColCard().");
		int i = 0;
		while (i <= depth)
		{
			if (colIndexes[i] == -1)
			{
				return 0;
			}
			i++;
		}

		final Vector<String> t = getTable(table, tableLines, data);

		final HashSet<String> unique = new HashSet<String>();
		for (final String row : t)
		{
			String comp = "";
			i = 0;
			while (i <= depth)
			{
				final StringTokenizer tokens = new StringTokenizer(row, ",", false);
				int j = 0;
				while (j < colIndexes[i])
				{
					tokens.nextToken();
					j++;
				}

				String token = null;
				try
				{
					token = tokens.nextToken().trim();
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.error("Exception looking for column " + colIndexes[i] + " for table " + table, e);
					System.exit(0);
				}

				if (token.startsWith("("))
				{
					token = token.substring(1);
				}

				if (token.endsWith(")"))
				{
					token = token.substring(0, token.length() - 1);
				}

				if (token.startsWith("!"))
				{
					HRDBMSWorker.logger.debug("Came across a variable while calculating cardinality");
					final Long val = vars.get(token);

					if (val == null)
					{
						HRDBMSWorker.logger.debug("Variable was not set. Using null.");
						token = "null";
					}
					else
					{
						HRDBMSWorker.logger.debug("Variable was set.");
						token = val.toString();
					}
				}

				comp += (token + "~");
				i++;
			}

			unique.add(comp);
		}

		return unique.size();
	}

	private static int getFullKeyCard(String table)
	{
		final Vector<String> t = getTable("SYS.TABLES", tableLines, data);
		final Vector<String> actual = getTable(table, tableLines, data);

		for (final String row : t)
		{
			final StringTokenizer tokens = new StringTokenizer(row, ",", false);
			final int id = Integer.parseInt(tokens.nextToken().trim().substring(1));
			tokens.nextToken();
			if (tokens.nextToken().trim().equals(table.substring(table.indexOf(".") + 1)))
			{
				final Vector<String> t2 = getTable("SYS.INDEXES", tableLines, data);
				for (final String r : t2)
				{
					final StringTokenizer tokens2 = new StringTokenizer(r, ",", false);
					tokens2.nextToken();
					tokens2.nextToken();
					if (id == Integer.parseInt(tokens2.nextToken().trim()))
					{
						return actual.size();
					}
				}
			}
		}

		return -1;
	}

	/*
	 * private static int getAvgLen(String table, int colIndex) { final
	 * Vector<String> t = getTable(table, tableLines, data); final
	 * Vector<String> types = getTypes(table);
	 * 
	 * int total = 0; int num = 0; for (final String row : t) { final
	 * StringTokenizer tokens = new StringTokenizer(row, ",", false); int j = 0;
	 * while (j < colIndex) { tokens.nextToken(); j++; }
	 * 
	 * String token = tokens.nextToken().trim();
	 * 
	 * if (token.startsWith("(")) { token = token.substring(1); }
	 * 
	 * if (token.endsWith(")")) { token = token.substring(0, token.length() -
	 * 1); }
	 * 
	 * if (token.startsWith("!")) { final Long val = vars.get(token);
	 * 
	 * if (val == null) { token = "null"; } else { token = val.toString(); } }
	 * 
	 * if (token.equals("null")) { continue; }
	 * 
	 * final String type = types.get(colIndex); if (type.equals("INT")) { total
	 * += 4; } else if (type.equals("BIGINT")) { total += 8; } else if
	 * (type.equals("VARCHAR")) { total += (4 + token.length()); } else if
	 * (type.equals("VARBINARY")) { total += (8 + token.length()); } else {
	 * HRDBMSWorker.logger.error("Unknown type: " + type); System.exit(1); }
	 * 
	 * num++; }
	 * 
	 * if (num == 0) { return 0; } else { return total / num; } }
	 */

	/*
	 * private static int getColIndexForIndex(String table, int pos) {
	 * Vector<String> t = getTable("SYS.TABLES", tableLines, data);
	 * 
	 * for (final String row : t) { final StringTokenizer tokens = new
	 * StringTokenizer(row, ",", false); final int id =
	 * Integer.parseInt(tokens.nextToken().trim().substring(1));
	 * tokens.nextToken(); if
	 * (tokens.nextToken().trim().equals(table.substring(table.indexOf(".") +
	 * 1))) { t = getTable("SYS.INDEXCOLS", tableLines, data); for (final String
	 * r : t) { final StringTokenizer tokens2 = new StringTokenizer(r, ",",
	 * false); tokens2.nextToken(); if (id ==
	 * Integer.parseInt(tokens2.nextToken().trim())) { final int colid =
	 * Integer.parseInt(tokens2.nextToken().trim());
	 * 
	 * if (Integer.parseInt(tokens2.nextToken().trim()) == pos) { return colid;
	 * } } } } }
	 * 
	 * return -1; }
	 */

	private static int getNodePreferLocal(int primary, String priRack, HashMap<Integer, String> nodes2Rack, HashMap<Integer, Integer> nodes2NumTables)
	{
		final HashMap<Integer, Integer> candidates = new HashMap<Integer, Integer>();
		for (final Map.Entry<Integer, String> entry : nodes2Rack.entrySet())
		{
			if (entry.getKey().intValue() >= 0 && entry.getKey().intValue() != primary)
			{
				if (entry.getValue().equals(priRack))
				{
					candidates.put(entry.getKey(), nodes2NumTables.get(entry.getKey()));
				}
			}
		}

		if (candidates.size() > 0)
		{
			int lowKey = Integer.MIN_VALUE;
			int lowValue = Integer.MAX_VALUE;
			for (final Map.Entry<Integer, Integer> entry : candidates.entrySet())
			{
				if (entry.getValue() < lowValue)
				{
					lowValue = entry.getValue();
					lowKey = entry.getKey();
				}
			}

			nodes2NumTables.put(lowKey, nodes2NumTables.get(lowKey) + 1);
			return lowKey;
		}

		for (final Map.Entry<Integer, String> entry : nodes2Rack.entrySet())
		{
			if (entry.getKey().intValue() >= 0 && entry.getKey().intValue() != primary)
			{
				if (!entry.getValue().equals(priRack))
				{
					candidates.put(entry.getKey(), nodes2NumTables.get(entry.getKey()));
				}
			}
		}

		int lowKey = -1;
		int lowValue = Integer.MAX_VALUE;
		for (final Map.Entry<Integer, Integer> entry : candidates.entrySet())
		{
			if (entry.getValue() < lowValue)
			{
				lowValue = entry.getValue();
				lowKey = entry.getKey();
			}
		}

		nodes2NumTables.put(lowKey, nodes2NumTables.get(lowKey) + 1);
		return lowKey;
	}

	private static int getNodePreferNonLocal(int primary, int secondary, String priRack, HashMap<Integer, String> nodes2Rack, HashMap<Integer, Integer> nodes2NumTables)
	{
		final HashMap<Integer, Integer> candidates = new HashMap<Integer, Integer>();
		for (final Map.Entry<Integer, String> entry : nodes2Rack.entrySet())
		{
			if (entry.getKey().intValue() >= 0 && entry.getKey().intValue() != primary && entry.getKey().intValue() != secondary)
			{
				if (!entry.getValue().equals(priRack))
				{
					candidates.put(entry.getKey(), nodes2NumTables.get(entry.getKey()));
				}
			}
		}

		if (candidates.size() > 0)
		{
			int lowKey = -1;
			int lowValue = Integer.MAX_VALUE;
			for (final Map.Entry<Integer, Integer> entry : candidates.entrySet())
			{
				if (entry.getValue() < lowValue)
				{
					lowValue = entry.getValue();
					lowKey = entry.getKey();
				}
			}

			nodes2NumTables.put(lowKey, nodes2NumTables.get(lowKey) + 1);
			return lowKey;
		}

		for (final Map.Entry<Integer, String> entry : nodes2Rack.entrySet())
		{
			if (entry.getKey().intValue() >= 0 && entry.getKey().intValue() != primary && entry.getKey().intValue() != secondary)
			{
				if (entry.getValue().equals(priRack))
				{
					candidates.put(entry.getKey(), nodes2NumTables.get(entry.getKey()));
				}
			}
		}

		int lowKey = -1;
		int lowValue = Integer.MAX_VALUE;
		for (final Map.Entry<Integer, Integer> entry : candidates.entrySet())
		{
			if (entry.getValue() < lowValue)
			{
				lowValue = entry.getValue();
				lowKey = entry.getKey();
			}
		}

		nodes2NumTables.put(lowKey, nodes2NumTables.get(lowKey) + 1);
		return lowKey;
	}

	private static int getNulls(String table, int colIndex)
	{
		final Vector<String> t = getTable(table, tableLines, data);

		int total = 0;
		for (final String row : t)
		{
			final StringTokenizer tokens = new StringTokenizer(row, ",", false);
			int j = 0;
			while (j < colIndex)
			{
				tokens.nextToken();
				j++;
			}

			String token = tokens.nextToken().trim();

			if (token.startsWith("("))
			{
				token = token.substring(1);
			}

			if (token.endsWith(")"))
			{
				token = token.substring(0, token.length() - 1);
			}

			if (token.equals("null"))
			{
				total++;
			}
		}

		return total;
	}

	private static Vector<String> getTable(String table, Vector<String> tableLines, Vector<Vector<String>> data)
	{
		int i = 0;
		for (final String header : tableLines)
		{
			if (header.startsWith(table + "("))
			{
				return data.get(i);
			}

			i++;
		}

		return null;
	}

	private static void indexPut(ByteBuffer data, ByteBuffer keyBytes, RID rid)
	{
		data.position(5); // offset of first slot
		final int freeOff = data.getInt();
		data.position(13); // head offset
		int headOff = data.getInt();
		while (true)
		{
			int temp = data.getInt(headOff + 13); // get right offset
			if (temp == 0)
			{
				break;
			}

			headOff = temp;
		}
		data.position(freeOff);

		data.put((byte)1);
		data.putInt(0); // left
		data.putInt(headOff);
		data.putInt(0); // right
		data.putInt(0);
		data.putInt(0); // up
		data.putInt(0);
		// data.putInt(0); // no down
		// data.putInt(0);
		data.putInt(rid.getNode());
		data.putInt(rid.getDevice());
		data.putInt(rid.getBlockNum());
		data.putInt(rid.getRecNum());
		keyBytes.position(0);
		data.put(keyBytes);
		int newFreeOff = data.position();
		data.position(headOff + 13);
		data.putInt(freeOff);
		data.position(5);
		data.putInt(newFreeOff);
	}

	private static int numCols(int tableId)
	{
		final Vector<String> cTable = getTable("SYS.COLUMNS", tableLines, data);

		int num = 0;
		for (final String cRow : cTable)
		{
			final StringTokenizer tokens = new StringTokenizer(cRow, ",", false);
			tokens.nextToken();

			if (Integer.parseInt(tokens.nextToken().trim()) == tableId)
			{
				num++;
			}
		}

		return num;
	}

	private static int numKeys(int tableId, int indexId)
	{
		final Vector<String> cTable = getTable("SYS.INDEXCOLS", tableLines, data);

		int num = 0;
		for (final String cRow : cTable)
		{
			final StringTokenizer tokens = new StringTokenizer(cRow, ",", false);
			if (Integer.parseInt(tokens.nextToken().trim().substring(1)) == indexId)
			{
				if (Integer.parseInt(tokens.nextToken().trim()) == tableId)
				{
					num++;
				}
			}
		}

		return num;
	}

	private static void receiveAndCreateNodes() throws IOException
	{
		HRDBMSWorker.logger.debug("Got input stream.");

		byte[] buff = new byte[8];
		if (in.read(buff) != 8)
		{
			HRDBMSWorker.logger.error("Tried to read command from master, but did not receive 8 bytes.");
			HRDBMSWorker.logger.error("Catalog synchronization will be aborted.");
			System.exit(1);
		}
		HRDBMSWorker.logger.debug("Read command from master.");
		final String cmd = new String(buff, "UTF-8");

		if (!cmd.equals("SENDNODE"))
		{
			HRDBMSWorker.logger.error("Expected SENDNODE command from master.  Received " + cmd + " command.");
			HRDBMSWorker.logger.error("Catalog synchronization will be aborted.");
			System.exit(1);
		}
		HRDBMSWorker.logger.debug("Command was SENDNODE.");

		buff = new byte[4];
		if (in.read(buff) != 4)
		{
			HRDBMSWorker.logger.error("Tried to read message size from master, bit did not receive 4 bytes.");
			HRDBMSWorker.logger.error("Catalog synchronization will be aborted.");
			System.exit(1);
		}

		final int size = java.nio.ByteBuffer.wrap(buff).getInt();
		HRDBMSWorker.logger.debug("Read size from master: " + size);
		buff = new byte[size];
		if (in.read(buff) != size)
		{
			HRDBMSWorker.logger.error("Tried to read SENDNODE message from master, but did not receive all the expected bytes.");
			HRDBMSWorker.logger.error("Catalog synchronization will be aborted.");
			System.exit(1);
		}

		final String msg = new String(buff, "UTF-8");
		HRDBMSWorker.logger.debug("Read message from master: " + msg);
		final StringTokenizer tokens = new StringTokenizer(msg, "~", false);
		final Vector<String> nTable = getTable("SYS.NODES", tableLines, data);

		while (tokens.hasMoreTokens())
		{
			nTable.add(tokens.nextToken());
		}
	}

	private static void receiveCompletionCheck() throws IOException
	{
		final byte[] buff = new byte[8];
		if (in.read(buff) != 8)
		{
			HRDBMSWorker.logger.error("Tried to read command from master, but did not receive 8 bytes.");
			HRDBMSWorker.logger.error("Catalog synchronization will be aborted.");
			System.exit(1);
		}
		final String cmd = new String(buff, "UTF-8");

		if (!cmd.equals("CHECKCMP"))
		{
			HRDBMSWorker.logger.error("Expected CHECKCMP command from master.  Received " + cmd + " command.");
			HRDBMSWorker.logger.error("Catalog synchronization will be aborted.");
			System.exit(1);
		}

		sockOut.write("OKOKOKOK".getBytes("UTF-8"));
	}

	private static void sendCompletionChecks() throws IOException
	{
		for (final Socket sock : sockets.values())
		{
			final OutputStream out = sock.getOutputStream();
			final String msg = "CHECKCMP";

			out.write(msg.getBytes("UTF-8"));
		}
		HRDBMSWorker.logger.debug("All completion check requests have been sent.");

		for (final Socket sock : sockets.values())
		{
			final InputStream in = sock.getInputStream();
			final byte[] data = new byte[8];
			if (in.read(data) != 8)
			{
				HRDBMSWorker.logger.error("Error receiving response to check completion request.");
				System.exit(1);
			}

			final String response = new String(data, "UTF-8");
			if (!response.equals("OKOKOKOK"))
			{
				HRDBMSWorker.logger.error("Received not OK response to check completion request.");
				System.exit(1);
			}
		}
	}

	private static void sendNodesToCoords() throws IOException
	{
		for (final Socket sock : sockets.values())
		{
			final OutputStream out = sock.getOutputStream();
			final String msg = "SENDNODE";
			String msgPart = "";

			final Vector<String> nodes = getTable("SYS.NODES", tableLines, data);
			int i = 0;
			for (final String node : nodes)
			{
				if (i == 0)
				{
					msgPart += node;
				}
				else
				{
					msgPart += "~" + node;
				}

				i++;
			}

			final int size = msgPart.length();
			out.write(msg.getBytes("UTF-8"));
			final byte[] buff = new byte[4];
			buff[0] = (byte)(size >> 24);
			buff[1] = (byte)((size & 0x00FF0000) >> 16);
			buff[2] = (byte)((size & 0x0000FF00) >> 8);
			buff[3] = (byte)((size & 0x000000FF));
			out.write(buff);
			out.write(msgPart.getBytes("UTF-8"));
		}
	}

	private static void writeData(PrintWriter out, Vector<String> table, int dataSize, Vector<String> types)
	{
		out.println("HRDBMSWorker.logger.debug(\"Writing table data.\");");
		out.println("");
		out.println("\t\ti = bb.position();");
		out.println("\t\twhile (i < (Page.BLOCK_SIZE - " + dataSize + "))");
		out.println("\t\t{");
		out.println("\t\t\tbb.put((byte)0);");
		out.println("\t\t\ti++;");
		out.println("\t\t}");
		out.println("");
		out.println("\t\t//start of data");

		for (final String row : table)
		{
			final StringTokenizer tokens = new StringTokenizer(row.trim(), ",", false);
			int i = 0;
			while (tokens.hasMoreTokens())
			{
				String token = tokens.nextToken().trim();
				if (token.startsWith("("))
				{
					token = token.substring(1);
				}

				if (token.endsWith(")"))
				{
					token = token.substring(0, token.length() - 1);
				}

				final String type = types.get(i);
				token = token.trim();

				if (type.equals("INT"))
				{
					if (!token.equals("null"))
					{
						if (token.startsWith("!"))
						{
							out.println("\t\tbb.putInt(" + token.substring(0, token.length() - 1).substring(1).toUpperCase() + ");");
							vars.put(token, null);
						}
						else
						{
							final int val = Integer.parseInt(token);
							out.println("\t\tbb.putInt(" + val + ");");
						}
					}
				}
				else if (type.equals("BIGINT"))
				{
					if (!token.equals("null"))
					{
						if (token.startsWith("!"))
						{
							out.println("\t\tbb.putLong(" + token.substring(0, token.length() - 1).substring(1).toUpperCase() + ");");
							vars.put(token, null);
						}
						else
						{
							final long val = Long.parseLong(token);
							out.println("\t\tbb.putLong(" + val + ");");
						}
					}
				}
				else if (type.equals("VARCHAR"))
				{
					if (!token.equals("null"))
					{
						out.println("\t\tbb.put((byte)" + (token.length() >> 16) + ");");
						out.println("\t\tbb.put((byte)" + ((token.length() >> 8) & 0xff) + ");");
						out.println("\t\tbb.put((byte)" + (token.length() & 0xff) + ");");
						out.println("\t\tputString(bb, \"" + token + "\");");
					}
				}
				else
				{
					HRDBMSWorker.logger.error("Unknown type: " + type);
					System.exit(1);
				}

				i++;
			}
		}

		out.println("\t\tbb.position(0);");
		out.println("\t\tfc.write(bb);");
		out.println("\t\tfc.force(false);");
		out.println("\t}");
		out.println("");
	}

	private static void writeOffsetArray(PrintWriter out, Vector<String> table, int dataSize, Vector<String> types)
	{
		out.println("");
		out.println("HRDBMSWorker.logger.debug(\"Writing offset array\");");
		out.println("\t\t//start of offset array");

		int off = dataSize;

		for (final String row : table)
		{
			final StringTokenizer tokens = new StringTokenizer(row, ",", false);
			int i = 0;
			while (tokens.hasMoreTokens())
			{
				String token = tokens.nextToken().trim();
				if (token.startsWith("("))
				{
					token = token.substring(1);
				}

				if (token.endsWith(")"))
				{
					token = token.substring(0, token.length() - 1);
				}

				final String type = types.get(i);
				token = token.trim();

				if (type.equals("INT"))
				{
					out.println("\t\tputMedium(bb, Page.BLOCK_SIZE - " + off + ");");
					if (!token.equals("null"))
					{
						off -= 4;
					}
				}
				else if (type.equals("BIGINT"))
				{
					out.println("\t\tputMedium(bb, Page.BLOCK_SIZE - " + off + ");");
					if (!token.equals("null"))
					{
						off -= 8;
					}
				}
				else if (type.equals("VARCHAR"))
				{
					out.println("\t\tputMedium(bb, Page.BLOCK_SIZE - " + off + ");");
					if (!token.equals("null"))
					{
						off -= (3 + token.length());
					}
				}
				else
				{
					HRDBMSWorker.logger.error("Unknown type: " + type);
					System.exit(1);
				}

				i++;
			}
		}
	}

	private static void writeVariables(PrintWriter out)
	{
		for (final Entry<String, Long> entry : vars.entrySet())
		{
			out.println("\tprivate static final int " + entry.getKey().substring(0, entry.getKey().length() - 1).substring(1).toUpperCase() + " = " + entry.getValue() + ";");
		}
	}

	private class TextRowSorter implements Comparable
	{
		private final Vector<Comparable> cols = new Vector<Comparable>();

		public void add(Integer col)
		{
			cols.add(col);
		}

		public void add(String col)
		{
			cols.add(col);
		}

		@Override
		public int compareTo(Object r) throws IllegalArgumentException
		{
			if (!(r instanceof TextRowSorter))
			{
				throw new IllegalArgumentException();
			}

			final TextRowSorter rhs = (TextRowSorter)r;
			if (this.cols.size() != rhs.cols.size())
			{
				throw new IllegalArgumentException();
			}

			final Iterator<Comparable> lhsIt = this.cols.iterator();
			final Iterator<Comparable> rhsIt = rhs.cols.iterator();

			while (lhsIt.hasNext())
			{
				final int result = lhsIt.next().compareTo(rhsIt.next());

				if (result != 0)
				{
					return result;
				}
			}

			return 0;
		}

		@Override
		public boolean equals(Object rhs)
		{

			if (rhs == null)
			{
				return false;
			}

			if (rhs instanceof TextRowSorter)
			{
				final TextRowSorter trs = (TextRowSorter)rhs;
				if (trs.cols.size() == this.cols.size())
				{
					final Iterator<Comparable> lhsIt = this.cols.iterator();
					final Iterator<Comparable> rhsIt = trs.cols.iterator();

					while (lhsIt.hasNext())
					{
						if (lhsIt.next().equals(rhsIt.next()))
						{

						}
						else
						{
							return false;
						}
					}

					return true;
				}
			}

			return false;
		}

		private ByteBuffer getKeyBytes() throws UnsupportedEncodingException
		{
			int i = this.cols.size();
			for (final Comparable col : this.cols)
			{
				if (col instanceof Integer)
				{
					i += 4;
				}
				else if (col instanceof String)
				{
					i += (4 + ((String)col).length());
				}
				else
				{
					HRDBMSWorker.logger.error("Unknown data type " + col.getClass() + " in key cols during catalog index build.");
					System.exit(1);
				}
			}

			final ByteBuffer ret = ByteBuffer.allocate(i);
			ret.position(0);
			for (final Comparable col : this.cols)
			{
				ret.put((byte)0); // not null

				if (col instanceof Integer)
				{
					ret.putInt((Integer)col);
				}
				else if (col instanceof String)
				{
					ret.putInt(((String)col).length());
					ret.put(((String)col).getBytes("UTF-8"));
				}
			}

			return ret;
		}
	}
}
