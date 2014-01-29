package com.exascale.misc;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.Inet4Address;
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
import java.util.Map.Entry;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.Vector;

import com.exascale.filesystem.Page;
import com.exascale.filesystem.RID;
import com.exascale.managers.ConnectionManager;
import com.exascale.managers.FileManager;
import com.exascale.managers.HRDBMSWorker;

public class CatalogCode 
{
	protected static TreeMap<String, Long> vars = new TreeMap<String, Long>();
	protected static Vector<Integer> sizes = new Vector<Integer>();
	protected static Vector<String> tableLines = new Vector<String>();
	protected static Vector<Vector<String>> data = new Vector<Vector<String>>();
	protected static Vector<Vector<String>> colTypes = new Vector<Vector<String>>();
	protected static HashSet<String> racks = new HashSet<String>();
	protected static HashMap<String, Socket> sockets = new HashMap<String, Socket>();
	protected static ServerSocket listen;
	protected static Socket listenSock;
	protected static InputStream in;
	protected static OutputStream sockOut;
	protected static int methodNum = 0;
	
	public static void buildCode() throws Exception
	{
		HRDBMSWorker.logger.debug("Starting build of catalog java code.");
		if (HRDBMSWorker.type == HRDBMSWorker.TYPE_COORD)
		{
			int port = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("catalog_sync_port"));
			HRDBMSWorker.logger.debug("Attempting to listen on port " + port);
			listen = new ServerSocket(port);
			HRDBMSWorker.logger.debug("ServerSocket created.  Now attempting accept()");
			Socket listenSock = listen.accept();
			HRDBMSWorker.logger.debug("Accepted connection on port " + port);
			byte[] dummy = new byte[8];
			in = listenSock.getInputStream();
			HRDBMSWorker.logger.debug("Got socket input stream.");
			sockOut = listenSock.getOutputStream();
			HRDBMSWorker.logger.debug("Got socket output stream.");
			int num = in.read(dummy);
			if (num != 8)
			{
				HRDBMSWorker.logger.error("Initial handshake on catalog sync port failed!");
				System.exit(1);
			}
			HRDBMSWorker.logger.debug("Initial handshake on catalog sync port successful.");
		}
		
		String in = "SYS.TABLES(INT, VARCHAR, VARCHAR, INT, INT, VARCHAR)\n" +
				"18\n" + 
				"(0, SYS, TABLES, 6, 2, R)\n" + 
				"(1, SYS, COLUMNS, 14, 2, R)\n" + 
				"(2, SYS, INDEXES, 6, 2, R)\n" + 
				"(3, SYS, INDEXCOLS, 5, 3, R)\n" + 
				"(4, SYS, VIEWS, 4, 2, R)\n" + 
				"(5, SYS, COLGROUPS, 4, 3, R)\n" + 
				"(6, SYS, NODEGROUPS, 3, 2, R)\n" + 
				"(7, SYS, TABLESTATS, 6, 3, R)\n" + 
				"(8, SYS, COLGROUPSTATS, 5, 4, R)\n" + 
				"(9, SYS, NODES, 5, 1, R)\n" + 
				"(10, SYS, NETWORK, 3, 2, R)\n" + 
				"(11, SYS, COLSTATS, 7, 4, R)\n" + 
				"(12, SYS, COLDIST, 15, 5, R)\n" + 
				"(13, SYS, BACKUPS, 3, 1, R)\n" +
				"(14, SYS, NODESTATE, 2, 1, R)\n" + 
				"SYS.BACKUPS(INT, INT, INT)\n" + 
				"0\n" + 
				"SYS.NODESTATE(INT, VARCHAR)\n" + 
				"0\n" + 
				"SYS.COLUMNS(INT, INT, VARCHAR, VARCHAR, INT, INT, VARCHAR, BIGINT, INT, VARBINARY, VARCHAR, INT, INT, VARCHAR)\n" + 
				"111\n" + 
				"(0, 1, COLID, INT, null, null, N, null, null, null, N, null, null, N)\n" +  
				"(1, 1, TABLEID, INT, null, null, N, null, null, null, N, 0, 0, N)\n" + 
				"(2, 1, COLNAME, VARCHAR, 128, null, N, null, null, null, N, 1, 1, N)\n" + 
				"(3, 1, COLTYPE, VARCHAR, 16, null, N, null, null, null, N, null, null, N)\n" + 
				"(4, 1, LENGTH, INT, null, null, N, null, null, null, Y, null, null, N)\n" + 
				"(5, 1, SCALE, INT, null, null, N, null, null, null, Y, null, null, N)\n" + 
				"(6, 1, IDENTITY, VARCHAR, 1, null, N, null, null, N, N, null, null, N)\n" + 
				"(7, 1, NEXTVAL, BIGINT, null, null, N, null, null, null, Y, null, null, N)\n" + 
				"(8, 1, INCREMENT, INT, null, null, N, null, null, null, Y, null, null, N)\n" + 
				"(9, 1, DEFAULT, VARBINARY, 32768, null, N, null, null, null, Y, null, null, N)\n" + 
				"(10, 1, NULLABLE, VARCHAR, 1, null, N, null, null, Y, N, null, null, N)\n" + 
				"(11, 1, PKPOSITION, INT, null, null, N, null, null, null, Y, null, null, N)\n" + 
				"(12, 1, CLUSTERPOS, INT, null, null, N, null, null, null, Y, null, null, N)\n" + 
				"(13, 1, GENERATED, VARCHAR, 1, null, N, null, null, N, N, null, null, N)\n" + 
				"(0, 0, TABLEID, INT, null, null, Y, !tablerows!, 1, null, N, null, null, A)\n" +  
				"(1, 0, SCHEMA, VARCHAR, 128, null, N, null, null, null, N, 0, 0, N)\n" + 
				"(2, 0, TABNAME, VARCHAR, 128, null, N, null, null, null, N, 1, 1, N)\n" + 
				"(3, 0, NUMCOLS, INT, null, null, N, null, null, null, N, null, null, N)\n" + 
				"(4, 0, NUMKEYCOLS, INT, null, null, N, null, null, null, N, null, null, N)\n" + 
				"(5, 0, TYPE, VARCHAR, 1, null, N, null, null, R, N, null, null, N)\n" +  
				"(0, 2, INDEXID, INT, null, null, N, null, null, null, N, null, null, N)\n" + 
				"(1, 2, INDEXNAME, VARCHAR, 128, null, N, null, null, null, N, 1, 1, N)\n" + 
				"(2, 2, TABLEID, INT, null, null, N, null, null, null, N, 0, 0, N)\n" + 
				"(3, 2, UNIQUE, VARCHAR, 1, null, N, null, null, null, N, null, null, N)\n" + 
				"(4, 2, NUMCOLS, INT, null, null, N, null, null,  null, N, null, null, N)\n" + 
				"(5, 2, TYPE, VARCHAR, 1, null, N, null, null, N, N, null, null, N)\n" + 
				"(0, 3, INDEXID, INT, null, null, N, null, null, null, N, 1, 1, N)\n" + 
				"(1, 3, TABLEID, INT, null, null, N, null, null, null, N, 0, 0, N)\n" + 
				"(2, 3, COLID, INT, null, null, N, null, null, null, N, 2, 2, N)\n" + 
				"(3, 3, POSITION, INT, null, null, N, null, null, null, N, null, null, N)\n" + 
				"(4, 3, ORDER, VARCHAR, 1, null, N, null, null, A, N, null, null, N)\n" + 
				"(0, 4, VIEWID, INT, null, null, Y, 0, 1, null, N, null, null, A)\n" + 
				"(1, 4, SCHEMA, VARCHAR, 128, null, N, null, null, null, N, 0, 0, N)\n" + 
				"(2, 4, NAME, VARCHAR, 128, null, N, null, null, null, N, 1, 1, N)\n" + 
				"(3, 4, TEXT, VARCHAR, 32768, null, N, null, null, null, N, null, null, N)\n" +  
				"(0, 5, COLGROUPID, INT, null, null, N, null, null, null, N, null, null, N)\n" + 
				"(1, 5, TABLEID, INT, null, null, N, null, null, null, N, 0, 0, N)\n" + 
				"(2, 5, COLID, INT, null, null, N, null, null, null, N, 2, 2, N)\n" + 
				"(3, 5, COLGROUPNAME, VARCHAR, 128, null, N, null, null, null, N, 1, 1, N)\n" + 
				"(0, 6, NODEGROUPID, INT, null, null, Y, !nodegrouprows!, 1, null, N, null, null, A)\n" +
				"(1, 6, NAME, VARCHAR, 128, null, N, null, null, null, N, 0, 0, N)\n" + 
				"(2, 6, NODEID, INT, null, null, N, null, null, null, N, 1, 1, N)\n" + 
				"(0, 7, TABLEID, INT, null, null, N, null, null, null, N, 0, 0, N)\n" +
				"(1, 7, CARD, BIGINT, null, null, N, null, null, null, Y, null, null, N)\n" +   
				"(0, 8, TABLEID, INT, null, null, N, null, null, null, N, 0, 0, N)\n" + 
				"(1, 8, COLGROUPID, INT, null, null, N, null, null, null, N, 1, 1, N)\n" + 
				"(2, 8, CARD, BIGINT, null, null, N, null, null, null, Y, null, null, N)\n" +
				"(0, 9, NODEID, INT, null, null, Y, 0, 1, null, N, 0, 0, D)\n" + 
				"(1, 9, HOSTNAME, VARCHAR, 128, null, N, null, null, null, N, null, null, N)\n" + 
				"(2, 9, TYPE, VARCHAR, 1, null, N, null, null, W, N, null, null, N)\n" + 
				"(3, 9, RACK, VARCHAR, 128, null, N, null, null, null, N, null, null, N)\n" + 
				"(0, 10, TABLEID, INT, null, null, N, null, null, null, N, 0, 0, N)\n" + 
				"(1, 10, LEVEL, INT, null, null, N, null, null, null, N, null, null, N)\n" + 
				"(2, 10, GROUPEXP, VARCHAR, 32768, null, N, null, null, null, Y, null, null, N)\n" + 
				"(3, 10, NODEEXP, VARCHAR, 32768, null, N, null, null, null, Y, null, null, N)\n" + 
				"(4, 10, DEVEXP, VARCHAR, 32768, null, N, null, null, null, N, null, null, N)\n" + 
				"(0, 11, TABLEID, INT, null, null, N, null, null, null, N, 0, 0, N)\n" + 
				"(1, 11, COLID, INT, null, null, N, null, null, null, N, 3, 3, N)\n" + 
				"(2, 11, CARD, BIGINT, null, null, N, null, null, null, Y, null, null, N)\n" + 
				"(0, 12, TABLEID, INT, null, null, N, null, null, null, N, 0, 0, N)\n" + 
				"(1, 12, COLID, INT, null, null, N, null, null, null, N, 3, 3, N)\n" + 
				"(4, 15, CUTOFFLE, VARBINARY, 32768, null, N, null, null, null, N, 4, 4, N)\n" + 
				"(0, 16, FIRST, INT, null, null, N, null, null, null, N, 0, 0, N)\n" + 
				"(1, 16, SECOND, INT, null, null, N, null, null, null, Y, null, null, N)\n" + 
				"(2, 16, THIRD, INT, null, null, N, null, null, null, Y, null, null, N)\n" +
				"(0, 17, NODE, INT, null, null, N, null, null, null, N, 0, 0, N)\n" + 
				"(1, 17, STATE, VARCHAR, 1, null, N, null, null, A, N, null, null, N)\n" + 
				"SYS.VIEWS(INT, VARCHAR, VARCHAR, VARCHAR)\n" + 
				"0\n" + 
				"SYS.INDEXES(INT, VARCHAR, INT, VARCHAR, INT, VARCHAR)\n" + 
				"19\n" + 
				"(0, PKTABLES, 0, Y, 2, N)\n" + 
				"(0, PKCOLUMNS, 1, Y, 2, N)\n" + 
				"(0, PKINDEXES, 2, Y, 2, N)\n" + 
				"(0, PKINDEXCOLS, 3, Y, 3, N)\n" + 
				"(0, PKVIEWS, 4, Y, 2, N)\n" + 
				"(0, PKCOLGROUPS, 5, Y, 3, N)\n" + 
				"(0, PKNODEGROUPS, 6, Y, 2, N)\n" + 
				"(0, PKTABLESTATS, 7, Y, 3, N)\n" + 
				"(0, PKCOLGROUPSTATS, 8, Y, 4, N)\n" + 
				"(0, PKINDEXSTATS, 9, Y, 4, N)\n" + 
				"(0, PKNODES, 10, Y, 1, N)\n" + 
				"(0, PKNETWORK1, 11, Y, 2, N)\n" + 
				"(1, PKNETWORK2, 11, Y, 2, N)\n" + 
				"(0, PKDEVICES, 12, Y, 2, N)\n" + 
				"(0, PKPARTITIONING, 13, Y, 1, N)\n" + 
				"(0, PKCOLSTATS, 14, Y, 4, N)\n" + 
				"(0, PKCOLDIST, 15, Y, 5, N)\n" +
				"(0, PKBACKUPS, 16, Y, 1, N)\n" + 
				"(0, PKNODESTATE, 17, Y, 1, N)\n" + 
				"SYS.INDEXCOLS(INT, INT, INT, INT, VARCHAR)\n" + 
				"46\n" + 
				"(0, 0, 1, 0, A)\n" + 
				"(0, 0, 2, 1, A)\n" + 
				"(0, 1, 1, 0, A)\n" + 
				"(0, 1, 2, 1, A)\n" + 
				"(0, 2, 1, 1, A)\n" + 
				"(0, 2, 2, 0, A)\n" + 
				"(0, 3, 0, 1, A)\n" + 
				"(0, 3, 1, 0, A)\n" + 
				"(0, 3, 2, 2, A)\n" + 
				"(0, 4, 1, 0, A)\n" + 
				"(0, 4, 2, 1, A)\n" + 
				"(0, 5, 0, 1, A)\n" + 
				"(0, 5, 1, 0, A)\n" + 
				"(0, 5, 2, 2, A)\n" + 
				"(0, 6, 1, 0, A)\n" + 
				"(0, 6, 2, 1, A)\n" + 
				"(0, 7, 0, 0, A)\n" + 
				"(0, 7, 1, 1, A)\n" + 
				"(0, 7, 2, 2, A)\n" + 
				"(0, 8, 0, 0, A)\n" + 
				"(0, 8, 1, 1, A)\n" +
				"(0, 8, 3, 2, A)\n" + 
				"(0, 8, 4, 3, A)\n" + 
				"(0, 9, 0, 0, A)\n" + 
				"(0, 9, 1, 1, A)\n" + 
				"(0, 9, 11, 2, A)\n" + 
				"(0, 9, 12, 3, A)\n" + 
				"(0, 10, 0, 0, A)\n" + 
				"(0, 11, 0, 0, A)\n" + 
				"(0, 11, 1, 1, A)\n" + 
				"(1, 11, 1, 0, A)\n" + 
				"(1, 11, 0, 1, A)\n" + 
				"(0, 12, 0, 0, A)\n" + 
				"(0, 12, 1, 1, A)\n" + 
				"(0, 13, 0, 0, A)\n" + 
				"(0, 14, 0, 0, A)\n" + 
				"(0, 14, 1, 1, A)\n" + 
				"(0, 14, 2, 2, A)\n" + 
				"(0, 14, 3, 3, A)\n" + 
				"(0, 15, 0, 0, A)\n" + 
				"(0, 15, 1, 1, A)\n" + 
				"(0, 15, 2, 2, A)\n" + 
				"(0, 15, 3, 3, A)\n" + 
				"(0, 15, 4, 4, A)\n" +
				"(0, 16, 0, 0, A)\n" + 
				"(0, 17, 0, 0, A)\n" + 
				"SYS.COLGROUPS(INT, INT, INT)\n" + 
				"0\n" + 
				"SYS.NODEGROUPS(INT, INT)\n" + 
				"0\n" + 
				"SYS.TABLESTATS(INT, INT, INT, BIGINT, INT, INT)\n" + 
				"0\n" + 
				"SYS.COLGROUPSTATS(INT, INT, BIGINT)\n" + 
				"0\n" + 
				"SYS.NODES(INT, VARCHAR, VARCHAR, VARCHAR, BIGINT)\n" + 
				"0\n" + 
				"SYS.NETWORK(VARCHAR, VARCHAR, BIGINT)\n" + 
				"0\n" + 
				"SYS.DEVICES(INT, INT, VARCHAR, BIGINT, BIGINT)\n" + 
				"0\n" + 
				"SYS.PARTITIONING(INT, INT, VARCHAR, VARCHAR, VARCHAR)\n" + 
				"18\n" + 
				"(0, 2, null, 0, 0)\n" + 
				"(1, 2, null, 0, 0)\n" + 
				"(2, 2, null, 0, 0)\n" + 
				"(3, 2, null, 0, 0)\n" + 
				"(4, 2, null, 0, 0)\n" + 
				"(5, 2, null, 0, 0)\n" + 
				"(6, 2, null, 0, 0)\n" + 
				"(7, 2, null, 0, 0)\n" + 
				"(8, 2, null, 0, 0)\n" + 
				"(9, 2, null, 0, 0)\n" + 
				"(10, 2, null, 0, 0)\n" + 
				"(11, 2, null, 0, 0)\n" + 
				"(12, 2, null, 0, 0)\n" + 
				"(13, 2, null, 0, 0)\n" + 
				"(14, 2, null, 0, 0)\n" + 
				"(15, 2, null, 0, 0)\n" +
				"(16, 2, null, 0, 0)\n" + 
				"(17, 2, null, 0, 0)\n" + 
				"SYS.INDEXSTATS(INT, INT, INT, INT, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, INT, INT, INT)\n" + 
				"0\n" + 
				"SYS.COLSTATS(INT, INT, INT, INT, BIGINT, INT, BIGINT)\n" + 
				"0\n" + 
				"SYS.COLDIST(INT, INT, INT, INT, VARBINARY, BIGINT, BIGINT, VARCHAR, BIGINT, VARCHAR, BIGINT, VARCHAR, BIGINT, VARCHAR, BIGINT)\n" + 
				"0";
		
		PrintWriter out = new PrintWriter(new FileWriter("CatalogCreator.java", false));
		
		Vector<String> tableNames = new Vector<String>();
		
		StringTokenizer lines = new StringTokenizer(in, "\n", false);
		boolean tableLine = true;
		boolean numberLine = false;
		int numLines = 0;
		Vector<String> tbldata = null;
		while (lines.hasMoreTokens())
		{
			String line = lines.nextToken();
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
		
		for (String header : tableLines)
		{
			StringTokenizer parse = new StringTokenizer(header, "(,", false);
			Vector<String> temp = new Vector<String>();
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
			createNetwork();
			HRDBMSWorker.logger.debug("createNetwork() has finished.");
			sendNodesToCoords();
			HRDBMSWorker.logger.debug("sendNodesToCoords() has finished.");
			sendNetworkToCoords();
			HRDBMSWorker.logger.debug("sendNetworkToCoords() has finished.");
			receiveDevicesFromAllNodes();
			HRDBMSWorker.logger.debug("receiveDevicesFromAllNodes() has finished.");
			sendDevicesToCoords();
		}
		else
		{
			HRDBMSWorker.logger.debug("Calling receiveAndCreateNodes().");
			receiveAndCreateNodes();
			HRDBMSWorker.logger.debug("receiveAndCreateNodes() has finished.");
			receiveAndCreateNetwork();
			HRDBMSWorker.logger.debug("receiveAndCreateNetwork() has finished.");
			receiveAndCreateDevices();
			HRDBMSWorker.logger.debug("receiveAndCreateDevices() has finished.");
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
		for (Vector<String> table : data)
		{
			HRDBMSWorker.logger.debug("Starting processing for table " + tableNames.get(i));
			int dataSize = calcDataSize(table, colTypes.get(i));
			HRDBMSWorker.logger.debug("calcDataSize() done for table " + i);
			sizes.add(dataSize);
			createTableHeader(out, tableNames.get(i), table.size(), colTypes.get(i).size(), dataSize);
			HRDBMSWorker.logger.debug("createTableHeader() done for table " + i);
			writeNullArray(out, table);
			HRDBMSWorker.logger.debug("writeNullArray() done for table " + i);
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
			for (Socket sock : sockets.values())
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
	
	protected static void createIndexes(PrintWriter out) throws UnsupportedEncodingException
	{
		HRDBMSWorker.logger.debug("Entered createIndexes()");
		Vector<String> iTable = getTable("SYS.INDEXES", tableLines, data);
		Vector<String> icTable = getTable("SYS.INDEXCOLS", tableLines, data);
		Vector<String> cTable = getTable("SYS.COLUMNS", tableLines, data);
		Vector<String> tTable = getTable("SYS.TABLES", tableLines, data);
		
		for (String iRow : iTable)
		{
			HRDBMSWorker.logger.debug("Processing row " + iRow);
			StringTokenizer iRowST = new StringTokenizer(iRow, ",", false);
			int indexID = Integer.parseInt(iRowST.nextToken().substring(1).trim());
			String iName = iRowST.nextToken().trim();
			int tableID = Integer.parseInt(iRowST.nextToken().trim());
			iRowST.nextToken();
			int numKeys = Integer.parseInt(iRowST.nextToken().trim());
			String tName = null;
			
			for (String tRow : tTable)
			{
				StringTokenizer tRowST = new StringTokenizer(tRow, ",", false);
				int tableID2 = Integer.parseInt(tRowST.nextToken().substring(1).trim());
				if (tableID == tableID2)
				{
					HRDBMSWorker.logger.debug("Found matching TABLES row.");
					tRowST.nextToken();
					tName = tRowST.nextToken().trim();
				}
			}
			
			TreeMap<Integer, Integer> pos2ColID = new TreeMap<Integer, Integer>();
			
			for (String icRow : icTable)
			{
				StringTokenizer icRowST = new StringTokenizer(icRow, ",", false);
				int indexID2 = Integer.parseInt(icRowST.nextToken().substring(1).trim());
				int tableID2 = Integer.parseInt(icRowST.nextToken().trim());
				
				if (tableID == tableID2 && indexID == indexID2)
				{
					HRDBMSWorker.logger.debug("Found a matching row in INDEXCOLS");
					int colID = Integer.parseInt(icRowST.nextToken().trim());
					int pos = Integer.parseInt(icRowST.nextToken().trim());
					pos2ColID.put(pos,  colID);
				}
			}
			
			TreeMap<Integer, String> pos2Type = new TreeMap<Integer, String>();
			for (Map.Entry<Integer, Integer> entry : pos2ColID.entrySet())
			{
				for (String cRow : cTable)
				{
					//if table id and col id matches, get column type and store in pos2Type
					StringTokenizer cRowTS = new StringTokenizer(cRow, ",", false);
					int colID3 = Integer.parseInt(cRowTS.nextToken().substring(1).trim());
					int tableID3 = Integer.parseInt(cRowTS.nextToken().trim());
					
					if (colID3 == entry.getValue() && tableID3 == tableID)
					{
						HRDBMSWorker.logger.debug("Found a matching row in COLUMNS");
						cRowTS.nextToken();
						String type = cRowTS.nextToken().trim();
						pos2Type.put(entry.getKey(), type);
					}
				}
			}
			
			HRDBMSWorker.logger.debug("Ready to start reading base table: " + tName);
			Vector<String> table = getTable("SYS." + tName, tableLines, data);
			HRDBMSWorker.logger.debug("Fetched the base table.");
			HRDBMSWorker.logger.debug("Table has " + table.size() + " rows");
			TreeMap<TextRowSorter, RID> keys2RIDs = new TreeMap<TextRowSorter, RID>();
			int rowNum = 0;
			for (String row : table)
			{
				HRDBMSWorker.logger.debug("Reading base table row");
				//get cols in order, create a TextRowSorter object and RID and add to keys2RIDs
				TextRowSorter keys = new CatalogCode().new TextRowSorter();
				for (Map.Entry<Integer, Integer> entry : pos2ColID.entrySet())
				{
					HRDBMSWorker.logger.debug("Index position " + entry.getKey() + " is colid " + entry.getValue()); 
					StringTokenizer tokens = new StringTokenizer(row, ",", false);
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
					String type = pos2Type.get(entry.getKey());
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
				
				keys2RIDs.put(keys, new RID(0, 0, 4097, rowNum));
				HRDBMSWorker.logger.debug("Adding key/RID pair.");
				rowNum++;
			}
			
			HRDBMSWorker.logger.debug("Calling buildIndexData()");
			buildIndexData(out, keys2RIDs, numKeys, "SYS." + tName + "." + iName);
		}
	}
	
	protected static void buildIndexData(PrintWriter out, TreeMap<TextRowSorter, RID> keys2RID, int numKeys, String name) throws UnsupportedEncodingException
	{	
		ByteBuffer data = ByteBuffer.allocate(Page.BLOCK_SIZE );
		buildIndexDataBuffer(keys2RID, numKeys, data);
		String fn = HRDBMSWorker.getHParms().getProperty("catalog_directory");
		if (!fn.endsWith("/"))
		{
			fn += "/";
		}
		fn += (name + ".index");
		File index = new File(fn);
		try
		{
			index.createNewFile();
			FileChannel fc = FileManager.getFile(fn);
			data.position(0);
			fc.write(data);
		}
		catch(IOException e)
		{
			HRDBMSWorker.logger.debug("IOException while creating index: " + fn, e);
		}
	}
	
	protected static void buildIndexDataBuffer(TreeMap<TextRowSorter, RID> keys2RID, int numKeys, ByteBuffer data) throws UnsupportedEncodingException
	{
		data.position(0);
		data.putInt(numKeys); //num key cols
		data.put((byte)1); //unique
		data.position(9); //first free byte @ 5
		data.putInt(Page.BLOCK_SIZE - 1); //last free byte
		
		data.put((byte)0); //not a leaf
		//offset of next free value from start of record (13)
		data.position(18);
		data.putInt(0); //zero valid key values in this internal node
		int i = 0;
		while (i < 128)
		{
			data.putInt(0); //down block
			data.putInt(0); //down offset
			i++;
		}
		
		data.putInt(0); //no up block
		data.putInt(0); //no up offset
		
		//fill in first free val pointer
		int pos = data.position();
		data.position(14);
		data.putInt(pos - 13);
		data.position(5);
		data.putInt(pos); //first free byte
		data.position(pos);
		
		while (pos < (Page.BLOCK_SIZE))
		{
			data.put((byte)2); //fill page out with 2s
			pos++;
		}
		
		for (Map.Entry<TextRowSorter, RID> entry : keys2RID.entrySet())
		{
			indexPut(data, entry.getKey().getKeyBytes(), entry.getValue());
		}
	}
	
	protected static void indexPut(ByteBuffer data, ByteBuffer keyBytes, RID rid)
	{
		data.position(14); //offset of first free key value
		int freeKeyOff = data.getInt();
		int numKeyVals = data.getInt();
		int pointerOff = 22 + (8 * numKeyVals);
		data.position(18); //num valid keys
		data.putInt(numKeyVals + 1);
		
		//key val
		data.position(freeKeyOff + 13);
		data.put(keyBytes);
		
		//next free val off
		int pos = data.position();
		data.position(14); //first free key offset
		data.putInt(pos - 13);
		
		//first free byte
		data.position(5);
		data.putInt(pos);
		
		//leaf + last free byte
		int prevOff, nextOff;
		int prevPointerOff = pointerOff - 4;
		if (prevPointerOff < 26)
		{
			prevOff = 0;
		}
		else
		{
			data.position(prevPointerOff);
			prevOff = data.getInt();
		}
		int nextPointerOff = pointerOff + 12;
		if (nextPointerOff > 1042)
		{
			nextOff = 0;
		}
		else
		{
			data.position(nextPointerOff);
			nextOff = data.getInt();
		}
		
		ByteBuffer leaf = createLeaf(keyBytes, rid, prevOff, nextOff);
		data.position(9); //last free byte
		int lastFree = data.getInt();
		data.position(9);
		data.putInt(lastFree - leaf.limit()); //new last free byte
		data.position(lastFree - leaf.limit() + 1); //start of leaf record
		int downOff = data.position();
		data.put(leaf);
		
		//down pointer
		data.position(pointerOff);
		data.putInt(0); //block 0
		data.putInt(downOff); //offset
	}
	
	protected static ByteBuffer createLeaf(ByteBuffer keyBytes, RID rid, int prevOff, int nextOff)
	{
		ByteBuffer data = ByteBuffer.allocate(41 + keyBytes.limit());
		data.position(0);
		data.put((byte)1); //leaf
		data.putInt(0); //prev block
		data.putInt(prevOff);
		data.putInt(0); //next block
		data.putInt(nextOff);
		data.putInt(0); //up block
		data.putInt(13); //up offset
		data.put(keyBytes);
		data.putInt(rid.getNode());
		data.putInt(rid.getDevice());
		data.putInt(rid.getBlockNum());
		data.putInt(rid.getRecNum());
		return data;
	}
	
	protected static void receiveDevicesFromAllNodes() throws Exception
	{
		Vector<String> nTable = getTable("SYS.NODES", tableLines, data);
		String responses = "";
		int i = 0;
		for (String row : nTable)
		{
			StringTokenizer tokens = new StringTokenizer(row, ",", false);
			int id = Integer.parseInt(tokens.nextToken().substring(1).trim());
			String host = tokens.nextToken().trim();
			String type = tokens.nextToken().trim();
			if (type.equals("W"))
			{
				if (i == 0)
				{
					responses += ConnectionManager.remoteGetDataDirs(0, id, host);
					HRDBMSWorker.logger.debug("Received RMI response.");
				}
				else
				{
					responses += ("~" + ConnectionManager.remoteGetDataDirs(0, id, host));
					HRDBMSWorker.logger.debug("Received RMI response.");
				}
				i++;
			}
		}
		
		HRDBMSWorker.logger.debug("Done receiving responses from all worker nodes");
		
		StringTokenizer response = new StringTokenizer(responses, "~", false);
		while (response.hasMoreTokens())
		{
			String rsp = response.nextToken();
			StringTokenizer dirs = new StringTokenizer(rsp, ",", false);
			int fromID = Integer.parseInt(dirs.nextToken());
			i = 0;
			Vector<String> dTable = getTable("SYS.DEVICES", tableLines, data);
			while (dirs.hasMoreTokens())
			{
				dTable.add("(" + fromID + "," + i + "," + dirs.nextToken() + ",null,null)");
				i++;
			}
		}
	}
	
	protected static void sendNodesToCoords() throws IOException
	{
		for (Socket sock : sockets.values())
		{
			OutputStream out = sock.getOutputStream();
			String msg = "SENDNODE";
			String msgPart = "";
			
			Vector<String> nodes = getTable("SYS.NODES", tableLines, data);
			int i = 0;
			for (String node : nodes)
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
			
			int size = msgPart.length();
			out.write(msg.getBytes("UTF-8"));
			byte[] buff = new byte[4];
			buff[0] = (byte)(size >> 24);
			buff[1] = (byte)((size & 0x00FF0000) >> 16);
			buff[2] = (byte)((size & 0x0000FF00) >> 8);
			buff[3] = (byte)((size & 0x000000FF));
			out.write(buff);
			out.write(msgPart.getBytes("UTF-8"));
		}
	}
	
	protected static void sendCompletionChecks() throws IOException
	{
		for (Socket sock : sockets.values())
		{
			OutputStream out = sock.getOutputStream();
			String msg = "CHECKCMP";

			out.write(msg.getBytes("UTF-8"));
		}
		HRDBMSWorker.logger.debug("All completion check requests have been sent.");
		
		for (Socket sock : sockets.values())
		{
			InputStream in = sock.getInputStream();
			byte[] data = new byte[8];
			if (in.read(data) != 8)
			{
				HRDBMSWorker.logger.error("Error receiving response to check completion request.");
				System.exit(1);
			}
			
			String response = new String(data, "UTF-8");
			if (!response.equals("OKOKOKOK"))
			{
				HRDBMSWorker.logger.error("Received not OK response to check completion request.");
				System.exit(1);
			}
		}
	}
	
	protected static void sendDevicesToCoords() throws IOException
	{
		for (Socket sock : sockets.values())
		{
			OutputStream out = sock.getOutputStream();
			String msg = "SENDDEV ";
			String msgPart = "";
			
			Vector<String> rows = getTable("SYS.DEVICES", tableLines, data);
			int i = 0;
			HRDBMSWorker.logger.debug("In sendDevicesToCoords(), DEVICES table has " + rows.size() + " rows");
			for (String row : rows)
			{
				if (i == 0)
				{
					msgPart += row;
				}
				else
				{
					msgPart += "~" + row;
				}
				
				i++;
			}
			 HRDBMSWorker.logger.debug("Devices message is: " + msgPart);
			int size = msgPart.length();
			HRDBMSWorker.logger.debug("Devices message length is: " + size);
			out.write(msg.getBytes("UTF-8"));
			byte[] buff = new byte[4];
			buff[0] = (byte)(size >> 24);
			buff[1] = (byte)((size & 0x00FF0000) >> 16);
			buff[2] = (byte)((size & 0x0000FF00) >> 8);
			buff[3] = (byte)((size & 0x000000FF));
			HRDBMSWorker.logger.debug("" + buff[0] + " " + buff[1] + " " + buff[2] + " " + buff[3]);
			out.write(buff);
			out.write(msgPart.getBytes("UTF-8"));
		}
	}
	
	protected static void sendNetworkToCoords() throws IOException
	{
		for (Socket sock : sockets.values())
		{
			OutputStream out = sock.getOutputStream();
			String msg = "SENDNET ";
			String msgPart = "";
			
			Vector<String> rows = getTable("SYS.NETWORK", tableLines, data);
			int i = 0;
			for (String row : rows)
			{
				if (i == 0)
				{
					msgPart += row;
				}
				else
				{
					msgPart += "~" + row;
				}
				
				i++;
			}
			
			int size = msgPart.length();
			out.write(msg.getBytes("UTF-8"));
			byte[] buff = new byte[4];
			buff[0] = (byte)(size >> 24);
			buff[1] = (byte)((size & 0x00FF0000) >> 16);
			buff[2] = (byte)((size & 0x0000FF00) >> 8);
			buff[3] = (byte)((size & 0x000000FF));
			out.write(buff);
			out.write(msgPart.getBytes("UTF-8"));
		}
	}
	
	protected static void createNetwork()
	{
		Vector<String> nTable = getTable("SYS.NETWORK", tableLines, data);
		int i = 0;
		for (String rack : racks)
		{
			int j = 0;
			for (String rack2 : racks)
			{
				if (j > i)
				{
					String row = "(" + rack + "," + rack2 + ",null)";
					nTable.add(row);
				}
				
				j++;
			}
			
			i++;
		}
	}
	
	protected static void createNodeState()
	{
		Vector<String> nsTable = getTable("SYS.NODESTATE", tableLines, data);
		Vector<String> nTable = getTable("SYS.NODES", tableLines, data);
		
		for (String nRow : nTable)
		{
			StringTokenizer tokens = new StringTokenizer(nRow, ",", false);
			int id = Integer.parseInt(tokens.nextToken().substring(1).trim());
			nsTable.add("(" + id + ",A)");
		}
	}
	
	protected static void createBackups()
	{
		Vector<String> nTable = getTable("SYS.NODES", tableLines, data);
		Vector<String> bTable = getTable("SYS.BACKUPS", tableLines, data);
		HashMap<Integer, String> nodes2Rack = new HashMap<Integer, String>();
		
		for (String nRow : nTable)
		{
			StringTokenizer tokens = new StringTokenizer(nRow, ",", false);
			int id = Integer.parseInt(tokens.nextToken().substring(1).trim());
			tokens.nextToken();
			String type = tokens.nextToken().trim().toUpperCase();
			String rack = tokens.nextToken().trim();
			
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
			for (Map.Entry<Integer, String> entry : nodes2Rack.entrySet())
			{
				bTable.add("(" + entry.getKey() + ", null, null)");
			}
			
			return;
		}
		
		if (nodes2Rack.size() == 2)
		{
			int[] ids = new int[2];
			int i = 0;
			for (Map.Entry<Integer, String> entry : nodes2Rack.entrySet())
			{
				ids[i] = entry.getKey();
				i++;
			}
			bTable.add("(" + ids[0] + "," + ids[1] + ",null)");
			bTable.add("(" + ids[1] + "," + ids[0] + ",null)");
			return;
		}
		
		HashMap<Integer, Integer> nodes2NumTables = new HashMap<Integer, Integer>();
		for (Map.Entry<Integer, String> entry : nodes2Rack.entrySet())
		{
			nodes2NumTables.put(entry.getKey(), 1);
		}
		
		for (Map.Entry<Integer, String> entry : nodes2Rack.entrySet())
		{
			int local = getNodePreferLocal(entry.getKey(), entry.getValue(), nodes2Rack, nodes2NumTables);
			bTable.add("(" + entry.getKey() + "," + local + "," + getNodePreferNonLocal(entry.getKey(), local, entry.getValue(), nodes2Rack, nodes2NumTables) + ")");
		}
	}
	
	protected static int getNodePreferLocal(int primary, String priRack, HashMap<Integer, String> nodes2Rack, HashMap<Integer, Integer> nodes2NumTables)
	{
		HashMap<Integer, Integer> candidates = new HashMap<Integer, Integer>();
		for (Map.Entry<Integer, String> entry : nodes2Rack.entrySet())
		{
			if (entry.getKey().intValue() != primary)
			{
				if (entry.getValue().equals(priRack))
				{
					candidates.put(entry.getKey(), nodes2NumTables.get(entry.getKey()));
				}
			}
		}
		
		if (candidates.size() > 0)
		{
			int lowKey = -1;
			int lowValue = Integer.MAX_VALUE;
			for (Map.Entry<Integer, Integer> entry : candidates.entrySet())
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
		
		for (Map.Entry<Integer, String> entry : nodes2Rack.entrySet())
		{
			if (entry.getKey().intValue() != primary)
			{
				if (!entry.getValue().equals(priRack))
				{
					candidates.put(entry.getKey(), nodes2NumTables.get(entry.getKey()));
				}
			}
		}
		
		int lowKey = -1;
		int lowValue = Integer.MAX_VALUE;
		for (Map.Entry<Integer, Integer> entry : candidates.entrySet())
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
	
	protected static int getNodePreferNonLocal(int primary, int secondary, String priRack, HashMap<Integer, String> nodes2Rack, HashMap<Integer, Integer> nodes2NumTables)
	{
		HashMap<Integer, Integer> candidates = new HashMap<Integer, Integer>();
		for (Map.Entry<Integer, String> entry : nodes2Rack.entrySet())
		{
			if (entry.getKey().intValue() != primary && entry.getKey().intValue() != secondary)
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
			for (Map.Entry<Integer, Integer> entry : candidates.entrySet())
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
		
		for (Map.Entry<Integer, String> entry : nodes2Rack.entrySet())
		{
			if (entry.getKey().intValue() != primary && entry.getKey().intValue() != secondary)
			{
				if (entry.getValue().equals(priRack))
				{
					candidates.put(entry.getKey(), nodes2NumTables.get(entry.getKey()));
				}
			}
		}
		
		int lowKey = -1;
		int lowValue = Integer.MAX_VALUE;
		for (Map.Entry<Integer, Integer> entry : candidates.entrySet())
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
	
	protected static void receiveAndCreateNodes() throws IOException
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
		String cmd = new String(buff, "UTF-8");
		
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
		
		int size = java.nio.ByteBuffer.wrap(buff).getInt();
		HRDBMSWorker.logger.debug("Read size from master: " + size);
		buff = new byte[size];
		if (in.read(buff) != size)
		{
			HRDBMSWorker.logger.error("Tried to read SENDNODE message from master, but did not receive all the expected bytes.");
			HRDBMSWorker.logger.error("Catalog synchronization will be aborted.");
			System.exit(1);
		}
		
		String msg = new String(buff, "UTF-8");
		HRDBMSWorker.logger.debug("Read message from master: " + msg);
		StringTokenizer tokens = new StringTokenizer(msg, "~", false);
		Vector<String> nTable = getTable("SYS.NODES", tableLines, data);
		
		while (tokens.hasMoreTokens())
		{
			nTable.add(tokens.nextToken());
		}
	}
	
	protected static void receiveCompletionCheck() throws IOException
	{	
		byte[] buff = new byte[8];
		if (in.read(buff) != 8)
		{
			HRDBMSWorker.logger.error("Tried to read command from master, but did not receive 8 bytes.");
			HRDBMSWorker.logger.error("Catalog synchronization will be aborted.");
			System.exit(1);
		}
		String cmd = new String(buff, "UTF-8");
		
		if (!cmd.equals("CHECKCMP"))
		{
			HRDBMSWorker.logger.error("Expected CHECKCMP command from master.  Received " + cmd + " command.");
			HRDBMSWorker.logger.error("Catalog synchronization will be aborted.");
			System.exit(1);
		}
		
		sockOut.write("OKOKOKOK".getBytes("UTF-8"));
	}
	
	protected static void receiveAndCreateDevices() throws IOException
	{	
		byte[] buff = new byte[8];
		if (in.read(buff) != 8)
		{
			HRDBMSWorker.logger.error("Tried to read command from master, but did not receive 8 bytes.");
			HRDBMSWorker.logger.error("Catalog synchronization will be aborted.");
			System.exit(1);
		}
		String cmd = new String(buff, "UTF-8");
		HRDBMSWorker.logger.debug("Received command: " + cmd);
		
		if (!cmd.equals("SENDDEV "))
		{
			HRDBMSWorker.logger.error("Expected SENDDEV command from master.  Received " + cmd + " command.");
			HRDBMSWorker.logger.error("Catalog synchronization will be aborted.");
			System.exit(1);
		}
		
		buff = new byte[4];
		if (in.read(buff) != 4)
		{
			HRDBMSWorker.logger.error("Tried to read message size from master, bit did not receive 4 bytes.");
			HRDBMSWorker.logger.error("Catalog synchronization will be aborted.");
			System.exit(1);
		}
		
		int size = java.nio.ByteBuffer.wrap(buff).getInt();
		HRDBMSWorker.logger.debug("Message size is: " + size);
		HRDBMSWorker.logger.debug("" + buff[0] + " " + buff[1] + " " + buff[2] + " " + buff[3]);
		buff = new byte[size];
		if (in.read(buff) != size)
		{
			HRDBMSWorker.logger.error("Tried to read SENDDEV message from master, but did not receive all the expected bytes.");
			HRDBMSWorker.logger.error("Catalog synchronization will be aborted.");
			System.exit(1);
		}
		
		String msg = new String(buff, "UTF-8");
		HRDBMSWorker.logger.debug("Message is: " + msg);
		StringTokenizer tokens = new StringTokenizer(msg, "~", false);
		Vector<String> dTable = getTable("SYS.DEVICES", tableLines, data);
		
		while (tokens.hasMoreTokens())
		{
			String dRow = tokens.nextToken();
			HRDBMSWorker.logger.debug("Adding row to DEVICES: " + dRow);
			dTable.add(dRow);
		}
	}
	
	protected static void receiveAndCreateNetwork() throws IOException
	{
		byte[] buff = new byte[8];
		if (in.read(buff) != 8)
		{
			HRDBMSWorker.logger.error("Tried to read command from master, but did not receive 8 bytes.");
			HRDBMSWorker.logger.error("Catalog synchronization will be aborted.");
			System.exit(1);
		}
		
		String cmd = new String(buff, "UTF-8");
		
		if (!cmd.equals("SENDNET "))
		{
			HRDBMSWorker.logger.error("Expected SENDNET command from master.  Received " + cmd + " command.");
			HRDBMSWorker.logger.error("Catalog synchronization will be aborted.");
			System.exit(1);
		}
		
		buff = new byte[4];
		if (in.read(buff) != 4)
		{
			HRDBMSWorker.logger.error("Tried to read message size from master, bit did not receive 4 bytes.");
			HRDBMSWorker.logger.error("Catalog synchronization will be aborted.");
			System.exit(1);
		}
		
		int size = java.nio.ByteBuffer.wrap(buff).getInt();
		
		buff = new byte[size];
		if (in.read(buff) != size)
		{
			HRDBMSWorker.logger.error("Tried to read SENDNET message from master, but did not receive all the expected bytes.");
			HRDBMSWorker.logger.error("Catalog synchronization will be aborted.");
			System.exit(1);
		}
		
		String msg = new String(buff, "UTF-8");
		StringTokenizer tokens = new StringTokenizer(msg, "~", false);
		Vector<String> nTable = getTable("SYS.NETWORK", tableLines, data);
		
		while (tokens.hasMoreTokens())
		{
			nTable.add(tokens.nextToken());
		}
	}
	
	protected static void createNodes() throws IOException
	{
		BufferedReader nodes = new BufferedReader(new FileReader(new File("nodes.cfg")));
		String line = nodes.readLine();
		int id = 0;
		while (line != null)
		{
			StringTokenizer tokens = new StringTokenizer(line, ",", false);
			String host = tokens.nextToken().trim();
			String type = tokens.nextToken().trim().toUpperCase();
			if (type.equals("C") || type.equals("W"))
			{
			}
			else
			{
				HRDBMSWorker.logger.error("Type found in nodes.cfg was not valid: " + type);
				System.exit(1);
			}
			String rack = tokens.nextToken().trim();
			racks.add(rack);
			Vector<String> nTable = getTable("SYS.NODES", tableLines, data);
			
			String row = "(" + id + "," + host + "," + type + "," + rack + ",null)";
			nTable.add(row);
			
			if (type.equals("C") && !isThisMyIpAddress(Inet4Address.getByName(host)))
			{
				int port = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("catalog_sync_port"));
				HRDBMSWorker.logger.debug("Attempting to create connection to " + host + " on port " + port);
				Socket sock = new Socket();
				HRDBMSWorker.logger.debug("Socket created.");
				while (true)
				{
					try
					{	
						Thread.sleep(Long.parseLong(HRDBMSWorker.getHParms().getProperty("catalog_creation_tcp_wait_ms")));
						HRDBMSWorker.logger.debug("Attempting connection to " + host + " on port " + port + " with a timeout of 1 sec");
						sock.connect(new InetSocketAddress(Inet4Address.getByName(host), port), 1000);
						byte[] dummy = new String("DUMMY   ").getBytes("UTF-8");
						sock.getOutputStream().write(dummy);
						break;
					}
					catch(SocketTimeoutException e)
					{
						HRDBMSWorker.logger.debug("Socket connection timed out.");
					}
					catch(Exception e)
					{
						HRDBMSWorker.logger.error("Exception thrown during connection", e);
						System.exit(1);
					}
				}
				HRDBMSWorker.logger.debug("Connection was successful.");
				sockets.put(host, sock);
			}
			
			line = nodes.readLine();
			id++;
		}
		
		nodes.close();
	}
	
	protected static void createColStats(Vector<String> tableLines, Vector<Vector<String>> data)
	{
		Vector<String> tTable = getTable("SYS.TABLES", tableLines, data);
		
		for (String tRow : tTable)
		{
			StringTokenizer tokens = new StringTokenizer(tRow, ",", false);
			int tableId = Integer.parseInt(tokens.nextToken().substring(1).trim());
			tokens.nextToken();
			String tName = tokens.nextToken().trim();
			int numCols = Integer.parseInt(tokens.nextToken().trim());
			int i = 0;
			Vector<String> csTable = getTable("SYS.COLSTATS", tableLines, data);
			while (i < numCols)
			{
				String row = "(" + tableId + ",0,0," + i + ",!colcard" + tableId + "_" + i + "!,!avglen" + tableId +"_" + i + "!,!nulls" + tableId + "_" + i +  "!)";
				csTable.add(row);
				i++;
			}
		}
	}
	
	protected static void createColStatVars()
	{
		HRDBMSWorker.logger.debug("Entered createColStatVars()");
		Vector<String> tTable = getTable("SYS.TABLES", tableLines, data);
		
		for (String tRow : tTable)
		{
			HRDBMSWorker.logger.debug("Processing row " + tRow);
			StringTokenizer tokens = new StringTokenizer(tRow, ",", false);
			int tableId = Integer.parseInt(tokens.nextToken().substring(1).trim());
			tokens.nextToken();
			String tName = tokens.nextToken().trim();
			int numCols = Integer.parseInt(tokens.nextToken().trim());
			HRDBMSWorker.logger.debug("ID: " + tableId + " Name: " + tName + " Cols: " + numCols);
			int i = 0;
			//Vector<String> table = getTable("SYS." + tName, tableLines, data);
			while (i < numCols)
			{
				int[] index = new int[1];
				index[0] = i;
				HRDBMSWorker.logger.debug("Calculating # of nulls for col " + i);
				vars.put("!nulls" + tableId + "_" + i + "!", new Long(getNulls("SYS." + tName, index[0])));
				HRDBMSWorker.logger.debug("Calculating card for col " + i);
				int card = getCompositeColCard("SYS." + tName, index, 0);
				
				if (card != -1)
				{
					vars.put("!colcard" + tableId + "_" + i + "!", new Long(card));
				}
				
				HRDBMSWorker.logger.debug("Calculating average length for col " + i);
				int avglen = getAvgLen("SYS." + tName, index[0]);
				if (avglen != -1)
				{
					vars.put("!avglen" + tableId + "_" + i + "!", new Long(avglen));
				}
	
				i++;
			}
		}
	}
	
	protected static void createIndexStats(Vector<String> tableLines, Vector<Vector<String>> data)
	{
		Vector<String> isTable = null;
		Vector<String> iTable = null;
		Vector<String> tTable = null;
		isTable = getTable("SYS.INDEXSTATS", tableLines, data);
		iTable = getTable("SYS.INDEXES", tableLines, data);
		tTable = getTable("SYS.TABLES", tableLines, data);
		
		for (String iRow : iTable)
		{
			StringTokenizer tokens = new StringTokenizer(iRow, ",", false);
			int indexId = Integer.parseInt(tokens.nextToken().trim().substring(1));
			tokens.nextToken();
			int tableId = Integer.parseInt(tokens.nextToken().trim());
			String token = null;
			
			for (String tRow : tTable)
			{
				StringTokenizer tableRow = new StringTokenizer(tRow, ",", false);
				if (Integer.parseInt(tableRow.nextToken().trim().substring(1)) == tableId)
				{
					tableRow.nextToken();
					token = tableRow.nextToken().trim(); //table name
					break;
				}
			}
			
			String row = "(" + tableId + "," + indexId + ",1,2,"; //1-4 key card, full, rows, blocks
			if (token.equals("TABLES"))
			{
				row += "!table1kc!,!table2kc!,!table3kc!,!table4kc!,!tablefkc!,!tablerows!,2,0,0)";
			}
			else if (token.equals("BACKUPS"))
			{
				row += "!backup1kc!,!backup2kc!,!backup3kc!,!backup4kc!,!backupfkc!,!backuprows!,2,0,0)";
			}
			else if (token.equals("NODESTATE"))
			{
				row += "!nodestate1kc!,!nodestate2kc!,!nodestate3kc!,!nodestate4kc!,!nodestatefkc!,!nodestaterows!,2,0,0)";
			}
			else if (token.equals("COLUMNS"))
			{
				row += "!column1kc!,!column2kc!,!column3kc!,!column4kc!,!columnfkc!,!columnrows!,2,0,0)";
			}
			else if (token.equals("INDEXES"))
			{
				row += "!index1kc!,!index2kc!,!index3kc!,!index4kc!,!indexfkc!,!indexrows!,2,0,0)";
			}
			else if (token.equals("INDEXCOLS"))
			{
				row += "!indexcol1kc!,!indexcol2kc!,!indexcol3kc!,!indexcol4kc!,!indexcolfkc!,!indexcolrows!,2,0,0)";
			}
			else if (token.equals("COLGROUPS"))
			{
				row += "!colgroup1kc!,!colgroup2kc!,!colgroup3kc!,!colgroup4kc!,!colgroupfkc!,!colgrouprows!,2,0,0)";
			}
			else if (token.equals("VIEWS"))
			{
				row += "!view1kc!,!view2kc!,!view3kc!,!view4kc!,!viewfkc!,!viewrows!,2,0,0)";
			}
			else if (token.equals("TABLESTATS"))
			{
				row += "!tablestat1kc!,!tablestat2kc!,!tablestat3kc!,!tablestat4kc!,!tablestatfkc!,!tablestatrows!,2,0,0)";
			}
			else if (token.equals("INDEXSTATS"))
			{
				row += "!indexstat1kc!,!indexstat2kc!,!indexstat3kc!,!indexstat4kc!,!indexstatfkc!,!indexstatrows!,2,0,0)";
			}
			else if (token.equals("COLSTATS"))
			{
				row += "!colstat1kc!,!colstat2kc!,!colstat3kc!,!colstat4kc!,!colstatfkc!,!colstatrows!,2,0,0)";
			}
			else if (token.equals("COLGROUPSTATS"))
			{
				row += "!colgroupstat1kc!,!colgroupstat2kc!,!colgroupstat3kc!,!colgroupstat4kc!,!colgroupstatfkc!,!colgroupstatrows!,2,0,0)";
			}
			else if (token.equals("DEVICES"))
			{
				row += "!device1kc!,!device2kc!,!device3kc!,!device4kc!,!devicefkc!,!devicerows!,2,0,0)";
			}
			else if (token.equals("NODES"))
			{
				row += "!node1kc!,!node2kc!,!node3kc!,!node4kc!,!nodefkc!,!noderows!,2,0,0)";
			}
			else if (token.equals("NODEGROUPS"))
			{
				row += "!nodegroup1kc!,!nodegroup2kc!,!nodegroup3kc!,!nodegroup4kc!,!nodegroupfkc!,!nodegrouprows!,2,0,0)";
			}
			else if (token.equals("NETWORK"))
			{
				row += "!network1kc!,!network2kc!,!network3kc!,!network4kc!,!networkfkc!,!networkrows!,2,0,0)";
			}
			else if (token.equals("PARTITIONING"))
			{
				row += "!part1kc!,!part2kc!,!part3kc!,!part4kc!,!partfkc!,!partrows!,2,0,0)";
			}
			else if (token.equals("COLDIST"))
			{
				row += "!coldist1kc!,!coldist2kc!,!coldist3kc!,!coldist4kc!,!coldistfkc!,!coldistrows!,2,0,0)";
			}
			else
			{
				HRDBMSWorker.logger.error("Unknown table: " + token);
				System.exit(1);
			}
			
			isTable.add(row);
		}
	}
	
	protected static Vector<String> getTable(String table, Vector<String> tableLines, Vector<Vector<String>> data)
	{
		int i = 0;
		for (String header : tableLines)
		{
			if (header.startsWith(table + "("))
			{
				return data.get(i);
			}
			
			i++;
		}
		
		return null;
	}
	
	protected static void createTableStats(Vector<String> tableLines, Vector<Vector<String>> data)
	{
		Vector<String> tsTable = null;
		Vector<String> tTable = null;
		
		int i = 0;
		for (String header : tableLines)
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
		
		for (String tRow : tTable)
		{
			StringTokenizer tokens = new StringTokenizer(tRow, ",", false);
			int tableId = Integer.parseInt(tokens.nextToken().trim().substring(1));
			tokens.nextToken();
			String token = tokens.nextToken().trim(); //table name
			
			String row = "(" + tableId + ",0,0,"; //card, pages, avgrow
			if (token.equals("TABLES"))
			{
				row += "!tablerows!,4097,!tableavgrow!)";
			}
			else if (token.equals("BACKUPS"))
			{
				row += "!backuprows!, 4097, !backupavgrow!)";
			}
			else if (token.equals("NODESTATE"))
			{
				row += "!nodestaterows!, 4097, !nodestateavgrow!)";
			}
			else if (token.equals("COLUMNS"))
			{
				row += "!columnrows!,4097,!columnavgrow!)";
			}
			else if (token.equals("INDEXES"))
			{
				row += "!indexrows!,4097,!indexavgrow!)";
			}
			else if (token.equals("INDEXCOLS"))
			{
				row += "!indexcolrows!,4097,!indexcolavgrow!)";
			}
			else if (token.equals("COLGROUPS"))
			{
				row += "!colgrouprows!,4097,!colgroupavgrow!)";
			}
			else if (token.equals("VIEWS"))
			{
				row += "!viewrows!,4097,!viewavgrow!)";
			}
			else if (token.equals("TABLESTATS"))
			{
				row += "!tablestatrows!,4097,!tablestatavgrow!)";
			}
			else if (token.equals("INDEXSTATS"))
			{
				row += "!indexstatrows!,4097,!indexstatavgrow!)";
			}
			else if (token.equals("COLSTATS"))
			{
				row += "!colstatrows!,4097,!colstatavgrow!)";
			}
			else if (token.equals("COLGROUPSTATS"))
			{
				row += "!colgroupstatrows!,4097,!colgroupstatavgrow!)";
			}
			else if (token.equals("DEVICES"))
			{
				row += "!devicerows!,4097,!deviceavgrow!)";
			}
			else if (token.equals("NODES"))
			{
				row += "!noderows!,4097,!nodeavgrow!)";
			}
			else if (token.equals("NODEGROUPS"))
			{
				row += "!nodegrouprows!,4097,!nodegroupavgrow!)";
			}
			else if (token.equals("NETWORK"))
			{
				row += "!networkrows!,4097,!networkavgrow!)";
			}
			else if (token.equals("PARTITIONING"))
			{
				row += "!partrows!,4097,!partavgrow!)";
			}
			else if (token.equals("COLDIST"))
			{
				row += "!coldistrows!,4097,!coldistavgrow!)";
			}
			else
			{
				HRDBMSWorker.logger.error("Unknown table: " + token);
				System.exit(1);
			}
			
			tsTable.add(row);
		}
	}
	
	protected static void createOutputTrailer(PrintWriter out, Vector<Vector<String>> data, Vector<String> tableLines)
	{
		out.println("");
		out.println("\tprotected static void putString(ByteBuffer bb, String val) throws UnsupportedEncodingException");
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
	
	protected static void calculateVariables(Vector<Vector<String>> data, Vector<String> tableLines)
	{
		HRDBMSWorker.logger.debug("In calculateVariables() with " + vars.keySet().size() + " variables to process.");
		for (String var : vars.keySet())
		{
			HRDBMSWorker.logger.debug("Processing " + var);
			if (var.equals("!tablerows!"))
			{
				int i = 0;
				for (String header : tableLines)
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
				for (String header : tableLines)
				{
					if (header.startsWith("SYS.TABLES("))
					{
						try
						{
							vars.put("!tableavgrow!", new Long(sizes.get(i) / data.get(i).size()));
						}
						catch(Exception e)
						{
							vars.put("!tableavgrow!",  new Long(0));
						}
						break;
					}
					
					i++;
				}
			}
			else if (var.equals("!table1kc!"))
			{
				int i = 0;
				long card;
				int[] colIndexes = new int[getNumIndexCols("SYS.TABLES")];
				while (i < 4)
				{
					colIndexes[i] = getColIndexForIndex("SYS.TABLES", i);
					card = getCompositeColCard("SYS.TABLES", colIndexes, i);
					
					if (card != -1)
					{
						vars.put("!table" + (i+1) + "kc!", card);
					}
					i++;
				}
				
				card = getFullKeyCard("SYS.TABLES", colIndexes);
				if (card != -1)
				{
					vars.put("!tablefkc!", card);
				}
			}
			if (var.equals("!backuprows!"))
			{
				int i = 0;
				for (String header : tableLines)
				{
					if (header.startsWith("SYS.BACKUPS("))
					{
						vars.put("!backuprows!", new Long(data.get(i).size()));
						break;
					}
					
					i++;
				}
			}
			else if (var.equals("!backupavgrow!"))
			{
				int i = 0;
				for (String header : tableLines)
				{
					if (header.startsWith("SYS.BACKUPS("))
					{
						try
						{
							vars.put("!backupavgrow!", new Long(sizes.get(i) / data.get(i).size()));
						}
						catch(Exception e)
						{
							vars.put("!backupavgrow!",  new Long(0));
						}
						break;
					}
					
					i++;
				}
			}
			else if (var.equals("!backup1kc!"))
			{
				int i = 0;
				long card;
				int[] colIndexes = new int[getNumIndexCols("SYS.BACKUPS")];
				while (i < 4)
				{
					colIndexes[i] = getColIndexForIndex("SYS.BACKUPS", i);
					card = getCompositeColCard("SYS.BACKUPS", colIndexes, i);
					
					if (card != -1)
					{
						vars.put("!backup" + (i+1) + "kc!", card);
					}
					i++;
				}
				
				card = getFullKeyCard("SYS.BACKUPS", colIndexes);
				if (card != -1)
				{
					vars.put("!backupfkc!", card);
				}
			}
			if (var.equals("!nodestaterows!"))
			{
				int i = 0;
				for (String header : tableLines)
				{
					if (header.startsWith("SYS.NODESTATE("))
					{
						vars.put("!nodestaterows!", new Long(data.get(i).size()));
						break;
					}
					
					i++;
				}
			}
			else if (var.equals("!nodestateavgrow!"))
			{
				int i = 0;
				for (String header : tableLines)
				{
					if (header.startsWith("SYS.NODESTATE("))
					{
						try
						{
							vars.put("!nodestateavgrow!", new Long(sizes.get(i) / data.get(i).size()));
						}
						catch(Exception e)
						{
							vars.put("!nodestateavgrow!",  new Long(0));
						}
						break;
					}
					
					i++;
				}
			}
			else if (var.equals("!nodestate1kc!"))
			{
				int i = 0;
				long card;
				int[] colIndexes = new int[getNumIndexCols("SYS.NODESTATE")];
				while (i < 4)
				{
					colIndexes[i] = getColIndexForIndex("SYS.NODESTATE", i);
					card = getCompositeColCard("SYS.NODESTATE", colIndexes, i);
					
					if (card != -1)
					{
						vars.put("!nodestate" + (i+1) + "kc!", card);
					}
					i++;
				}
				
				card = getFullKeyCard("SYS.NODESTATE", colIndexes);
				if (card != -1)
				{
					vars.put("!nodestatefkc!", card);
				}
			}
			else if (var.equals("!columnrows!"))
			{
				int i = 0;
				for (String header : tableLines)
				{
					if (header.startsWith("SYS.COLUMNS("))
					{
						vars.put("!columnrows!", new Long(data.get(i).size()));
						break;
					}
					
					i++;
				}
			}
			else if (var.equals("!columnavgrow!"))
			{
				int i = 0;
				for (String header : tableLines)
				{
					if (header.startsWith("SYS.COLUMNS("))
					{
						try
						{
							vars.put("!columnavgrow!", new Long(sizes.get(i) / data.get(i).size()));
						}
						catch(Exception e)
						{
							vars.put("!columnavgrow!",  new Long(0));
						}
						break;
					}
					
					i++;
				}
			}
			else if (var.equals("!column1kc!"))
			{
				int i = 0;
				long card;
				int[] colIndexes = new int[getNumIndexCols("SYS.COLUMNS")];
				while (i < 4)
				{
					colIndexes[i] = getColIndexForIndex("SYS.COLUMNS", i);
					card = getCompositeColCard("SYS.COLUMNS", colIndexes, i);
					
					if (card != -1)
					{
						vars.put("!column" + (i+1) + "kc!", card);
					}
					i++;
				}
				
				card = getFullKeyCard("SYS.COLUMNS", colIndexes);
				if (card != -1)
				{
					vars.put("!columnfkc!", card);
				}
			}
			else if (var.equals("!indexrows!"))
			{
				int i = 0;
				for (String header : tableLines)
				{
					if (header.startsWith("SYS.INDEXES("))
					{
						vars.put("!indexrows!", new Long(data.get(i).size()));
						break;
					}
					
					i++;
				}
			}
			else if (var.equals("!indexavgrow!"))
			{
				int i = 0;
				for (String header : tableLines)
				{
					if (header.startsWith("SYS.INDEXES("))
					{
						try
						{
							vars.put("!indexavgrow!", new Long(sizes.get(i) / data.get(i).size()));
						}
						catch(Exception e)
						{
							vars.put("!indexavgrow!",  new Long(0));
						}
						break;
					}
					
					i++;
				}
			}
			else if (var.equals("!index1kc!"))
			{
				int i = 0;
				long card;
				int[] colIndexes = new int[getNumIndexCols("SYS.INDEXES")];
				while (i < 4)
				{
					colIndexes[i] = getColIndexForIndex("SYS.INDEXES", i);
					card = getCompositeColCard("SYS.INDEXES", colIndexes, i);
					
					if (card != -1)
					{
						vars.put("!index" + (i+1) + "kc!", card);
					}
					i++;
				}
				
				card = getFullKeyCard("SYS.INDEXES", colIndexes);
				if (card != -1)
				{
					vars.put("!indexfkc!", card);
				}
			}
			else if (var.equals("!indexcolrows!"))
			{
				int i = 0;
				for (String header : tableLines)
				{
					if (header.startsWith("SYS.INDEXCOLS("))
					{
						vars.put("!indexcolrows!", new Long(data.get(i).size()));
						break;
					}
					
					i++;
				}
			}
			else if (var.equals("!indexcolavgrow!"))
			{
				int i = 0;
				for (String header : tableLines)
				{
					if (header.startsWith("SYS.INDEXCOLS("))
					{
						try
						{
							vars.put("!indexcolavgrow!", new Long(sizes.get(i) / data.get(i).size()));
						}
						catch(Exception e)
						{
							vars.put("!indexcolavgrow!",  new Long(0));
						}
						break;
					}
					
					i++;
				}
			}
			else if (var.equals("!indexcol1kc!"))
			{
				int i = 0;
				long card;
				int[] colIndexes = new int[getNumIndexCols("SYS.INDEXCOLS")];
				while (i < 4)
				{
					colIndexes[i] = getColIndexForIndex("SYS.INDEXCOLS", i);
					card = getCompositeColCard("SYS.INDEXCOLS", colIndexes, i);
					
					if (card != -1)
					{
						vars.put("!indexcol" + (i+1) + "kc!", card);
					}
					i++;
				}
				
				card = getFullKeyCard("SYS.INDEXCOLS", colIndexes);
				if (card != -1)
				{
					vars.put("!indexcolfkc!", card);
				}
			}
			else if (var.equals("!viewrows!"))
			{
				int i = 0;
				for (String header : tableLines)
				{
					if (header.startsWith("SYS.VIEWS("))
					{
						vars.put("!viewrows!", new Long(data.get(i).size()));
						break;
					}
					
					i++;
				}
			}
			else if (var.equals("!viewavgrow!"))
			{
				int i = 0;
				for (String header : tableLines)
				{
					if (header.startsWith("SYS.VIEWS("))
					{
						try
						{
							vars.put("!viewavgrow!", new Long(sizes.get(i) / data.get(i).size()));
						}
						catch(Exception e)
						{
							vars.put("!viewavgrow!",  new Long(0));
						}
						break;
					}
					
					i++;
				}
			}
			else if (var.equals("!view1kc!"))
			{
				int i = 0;
				long card;
				int[] colIndexes = new int[getNumIndexCols("SYS.VIEWS")];
				while (i < 4)
				{
					colIndexes[i] = getColIndexForIndex("SYS.VIEWS", i);
					card = getCompositeColCard("SYS.VIEWS", colIndexes, i);
					
					if (card != -1)
					{
						vars.put("!view" + (i+1) + "kc!", card);
					}
					i++;
				}
				
				card = getFullKeyCard("SYS.VIEWS", colIndexes);
				if (card != -1)
				{
					vars.put("!viewfkc!", card);
				}
			}
			else if (var.equals("!colgrouprows!"))
			{
				int i = 0;
				for (String header : tableLines)
				{
					if (header.startsWith("SYS.COLGROUPS("))
					{
						vars.put("!colgrouprows!", new Long(data.get(i).size()));
						break;
					}
					
					i++;
				}
			}
			else if (var.equals("!colgroupavgrow!"))
			{
				int i = 0;
				for (String header : tableLines)
				{
					if (header.startsWith("SYS.COLGROUPS("))
					{
						try
						{
							vars.put("!colgroupavgrow!", new Long(sizes.get(i) / data.get(i).size()));
						}
						catch(Exception e)
						{
							vars.put("!colgroupavgrow!",  new Long(0));
						}
						break;
					}
					
					i++;
				}
			}
			else if (var.equals("!colgroup1kc!"))
			{
				int i = 0;
				long card;
				int[] colIndexes = new int[getNumIndexCols("SYS.COLGROUPS")];
				while (i < 4)
				{
					colIndexes[i] = getColIndexForIndex("SYS.COLGROUPS", i);
					card = getCompositeColCard("SYS.COLGROUPS", colIndexes, i);
					
					if (card != -1)
					{
						vars.put("!colgroup" + (i+1) + "kc!", card);
					}
					i++;
				}
				
				card = getFullKeyCard("SYS.COLGROUPS", colIndexes);
				if (card != -1)
				{
					vars.put("!colgroupfkc!", card);
				}
			}
			else if (var.equals("!nodegrouprows!"))
			{
				int i = 0;
				for (String header : tableLines)
				{
					if (header.startsWith("SYS.NODEGROUPS("))
					{
						vars.put("!nodegrouprows!", new Long(data.get(i).size()));
						break;
					}
					
					i++;
				}
			}
			else if (var.equals("!nodegroupavgrow!"))
			{
				int i = 0;
				for (String header : tableLines)
				{
					if (header.startsWith("SYS.NODEGROUPS("))
					{
						try
						{
							vars.put("!nodegroupavgrow!", new Long(sizes.get(i) / data.get(i).size()));
						}
						catch(Exception e)
						{
							vars.put("!nodegroupavgrow!",  new Long(0));
						}
						break;
					}
					
					i++;
				}
			}
			else if (var.equals("!nodegroup1kc!"))
			{
				int i = 0;
				long card;
				int[] colIndexes = new int[getNumIndexCols("SYS.NODEGROUPS")];
				while (i < 4)
				{
					colIndexes[i] = getColIndexForIndex("SYS.NODEGROUPS", i);
					card = getCompositeColCard("SYS.NODEGROUPS", colIndexes, i);
					
					if (card != -1)
					{
						vars.put("!nodegroup" + (i+1) + "kc!", card);
					}
					i++;
				}
				
				card = getFullKeyCard("SYS.NODEGROUPS", colIndexes);
				if (card != -1)
				{
					vars.put("!nodegroupfkc!", card);
				}
			}
			else if (var.equals("!tablestatrows!"))
			{
				int i = 0;
				for (String header : tableLines)
				{
					if (header.startsWith("SYS.TABLESTATS("))
					{
						vars.put("!tablestatrows!", new Long(data.get(i).size()));
						break;
					}
					
					i++;
				}
			}
			else if (var.equals("!tablestatavgrow!"))
			{
				int i = 0;
				for (String header : tableLines)
				{
					if (header.startsWith("SYS.TABLESTATS("))
					{
						try
						{
							vars.put("!tablestatavgrow!", new Long(sizes.get(i) / data.get(i).size()));
						}
						catch(Exception e)
						{
							vars.put("!tablestatavgrow!",  new Long(0));
						}
						break;
					}
					
					i++;
				}
			}
			else if (var.equals("!tablestat1kc!"))
			{
				int i = 0;
				long card;
				int[] colIndexes = new int[getNumIndexCols("SYS.TABLESTATS")];
				while (i < 4)
				{
					colIndexes[i] = getColIndexForIndex("SYS.TABLESTATS", i);
					card = getCompositeColCard("SYS.TABLESTATS", colIndexes, i);
					
					if (card != -1)
					{
						vars.put("!tablestat" + (i+1) + "kc!", card);
					}
					i++;
				}
				
				card = getFullKeyCard("SYS.TABLESTATS", colIndexes);
				if (card != -1)
				{
					vars.put("!tablestatfkc!", card);
				}
			}
			else if (var.equals("!colgroupstatrows!"))
			{
				int i = 0;
				for (String header : tableLines)
				{
					if (header.startsWith("SYS.COLGROUPSTATS("))
					{
						vars.put("!colgroupstatrows!", new Long(data.get(i).size()));
						break;
					}
					
					i++;
				}
			}
			else if (var.equals("!colgroupstatavgrow!"))
			{
				int i = 0;
				for (String header : tableLines)
				{
					if (header.startsWith("SYS.COLGROUPSTATS("))
					{
						try
						{
							vars.put("!colgroupstatavgrow!", new Long(sizes.get(i) / data.get(i).size()));
						}
						catch(Exception e)
						{
							vars.put("!colgroupstatavgrow!",  new Long(0));
						}
						break;
					}
					
					i++;
				}
			}
			else if (var.equals("!colgroupstat1kc!"))
			{
				int i = 0;
				long card;
				int[] colIndexes = new int[getNumIndexCols("SYS.COLGROUPSTATS")];
				while (i < 4)
				{
					colIndexes[i] = getColIndexForIndex("SYS.COLGROUPSTATS", i);
					card = getCompositeColCard("SYS.COLGROUPSTATS", colIndexes, i);
					
					if (card != -1)
					{
						vars.put("!colgroupstat" + (i+1) + "kc!", card);
					}
					i++;
				}
				
				card = getFullKeyCard("SYS.COLGROUPSTATS", colIndexes);
				if (card != -1)
				{
					vars.put("!colgroupstatfkc!", card);
				}
			}
			else if (var.equals("!indexstatrows!"))
			{
				int i = 0;
				for (String header : tableLines)
				{
					if (header.startsWith("SYS.INDEXSTATS("))
					{
						vars.put("!indexstatrows!", new Long(data.get(i).size()));
						break;
					}
					
					i++;
				}
			}
			else if (var.equals("!indexstatavgrow!"))
			{
				int i = 0;
				for (String header : tableLines)
				{
					if (header.startsWith("SYS.INDEXSTATS("))
					{
						try
						{
							vars.put("!indexstatavgrow!", new Long(sizes.get(i) / data.get(i).size()));
						}
						catch(Exception e)
						{
							vars.put("!indexstatavgrow!",  new Long(0));
						}
						break;
					}
					
					i++;
				}
			}
			else if (var.equals("!indexstat1kc!"))
			{
				int i = 0;
				long card;
				int[] colIndexes = new int[getNumIndexCols("SYS.INDEXSTATS")];
				while (i < 4)
				{
					colIndexes[i] = getColIndexForIndex("SYS.INDEXSTATS", i);
					card = getCompositeColCard("SYS.INDEXSTATS", colIndexes, i);
					
					if (card != -1)
					{
						vars.put("!indexstat" + (i+1) + "kc!", card);
					}
					i++;
				}
				
				card = getFullKeyCard("SYS.INDEXSTATS", colIndexes);
				if (card != -1)
				{
					vars.put("!indexstatfkc!", card);
				}
			}
			else if (var.equals("!noderows!"))
			{
				int i = 0;
				for (String header : tableLines)
				{
					if (header.startsWith("SYS.NODES("))
					{
						vars.put("!noderows!", new Long(data.get(i).size()));
						break;
					}
					
					i++;
				}
			}
			else if (var.equals("!nodeavgrow!"))
			{
				int i = 0;
				for (String header : tableLines)
				{
					if (header.startsWith("SYS.NODES("))
					{
						try
						{
							vars.put("!nodeavgrow!", new Long(sizes.get(i) / data.get(i).size()));
						}
						catch(Exception e)
						{
							vars.put("!nodeavgrow!",  new Long(0));
						}
						break;
					}
					
					i++;
				}
			}
			else if (var.equals("!node1kc!"))
			{
				int i = 0;
				long card;
				int[] colIndexes = new int[getNumIndexCols("SYS.NODES")];
				while (i < 4)
				{
					colIndexes[i] = getColIndexForIndex("SYS.NODES", i);
					card = getCompositeColCard("SYS.NODES", colIndexes, i);
					
					if (card != -1)
					{
						vars.put("!node" + (i+1) + "kc!", card);
					}
					i++;
				}
				
				card = getFullKeyCard("SYS.NODES", colIndexes);
				if (card != -1)
				{
					vars.put("!nodefkc!", card);
				}
			}
			else if (var.equals("!networkrows!"))
			{
				int i = 0;
				for (String header : tableLines)
				{
					if (header.startsWith("SYS.NETWORK("))
					{
						vars.put("!networkrows!", new Long(data.get(i).size()));
						break;
					}
					
					i++;
				}
			}
			else if (var.equals("!networkavgrow!"))
			{
				int i = 0;
				for (String header : tableLines)
				{
					if (header.startsWith("SYS.NETWORK("))
					{
						try
						{
							vars.put("!networkavgrow!", new Long(sizes.get(i) / data.get(i).size()));
						}
						catch(Exception e)
						{
							vars.put("!networkavgrow!",  new Long(0));
						}
						break;
					}
					
					i++;
				}
			}
			else if (var.equals("!network1kc!"))
			{
				int i = 0;
				long card;
				int[] colIndexes = new int[getNumIndexCols("SYS.NETWORK")];
				while (i < 4)
				{
					colIndexes[i] = getColIndexForIndex("SYS.NETWORK", i);
					card = getCompositeColCard("SYS.NETWORK", colIndexes, i);
					
					if (card != -1)
					{
						vars.put("!network" + (i+1) + "kc!", card);
					}
					i++;
				}
				
				card = getFullKeyCard("SYS.NETWORK", colIndexes);
				if (card != -1)
				{
					vars.put("!networkfkc!", card);
				}
			}
			else if (var.equals("!devicerows!"))
			{
				int i = 0;
				for (String header : tableLines)
				{
					if (header.startsWith("SYS.DEVICES("))
					{
						vars.put("!devicerows!", new Long(data.get(i).size()));
						break;
					}
					
					i++;
				}
			}
			else if (var.equals("!deviceavgrow!"))
			{
				int i = 0;
				for (String header : tableLines)
				{
					if (header.startsWith("SYS.DEVICES("))
					{
						try
						{
							vars.put("!deviceavgrow!", new Long(sizes.get(i) / data.get(i).size()));
						}
						catch(Exception e)
						{
							vars.put("!deviceavgrow!",  new Long(0));
						}
						break;
					}
					
					i++;
				}
			}
			else if (var.equals("!device1kc!"))
			{
				int i = 0;
				long card;
				int[] colIndexes = new int[getNumIndexCols("SYS.DEVICES")];
				while (i < 4)
				{
					colIndexes[i] = getColIndexForIndex("SYS.DEVICES", i);
					card = getCompositeColCard("SYS.DEVICES", colIndexes, i);
					
					if (card != -1)
					{
						vars.put("!device" + (i+1) + "kc!", card);
					}
					i++;
				}
				
				card = getFullKeyCard("SYS.DEVICES", colIndexes);
				if (card != -1)
				{
					vars.put("!devicefkc!", card);
				}
			}
			else if (var.equals("!partrows!"))
			{
				int i = 0;
				for (String header : tableLines)
				{
					if (header.startsWith("SYS.PARTITIONING("))
					{
						vars.put("!partrows!", new Long(data.get(i).size()));
						break;
					}
					
					i++;
				}
			}
			else if (var.equals("!partavgrow!"))
			{
				int i = 0;
				for (String header : tableLines)
				{
					if (header.startsWith("SYS.PARTITIONING("))
					{
						try
						{
							vars.put("!partavgrow!", new Long(sizes.get(i) / data.get(i).size()));
						}
						catch(Exception e)
						{
							vars.put("!partavgrow!",  new Long(0));
						}
						break;
					}
					
					i++;
				}
			}
			else if (var.equals("!part1kc!"))
			{
				int i = 0;
				long card;
				int[] colIndexes = new int[getNumIndexCols("SYS.PARTITIONING")];
				while (i < 4)
				{
					colIndexes[i] = getColIndexForIndex("SYS.PARTITIONING", i);
					card = getCompositeColCard("SYS.PARTITIONING", colIndexes, i);
					
					if (card != -1)
					{
						vars.put("!part" + (i+1) + "kc!", card);
					}
					i++;
				}
				
				card = getFullKeyCard("SYS.PARTITIONING", colIndexes);
				if (card != -1)
				{
					vars.put("!partfkc!", card);
				}
			}
			else if (var.equals("!colstatrows!"))
			{
				int i = 0;
				for (String header : tableLines)
				{
					if (header.startsWith("SYS.COLSTATS("))
					{
						vars.put("!colstatrows!", new Long(data.get(i).size()));
						break;
					}
					
					i++;
				}
			}
			else if (var.equals("!colstatavgrow!"))
			{
				int i = 0;
				for (String header : tableLines)
				{
					if (header.startsWith("SYS.COLSTATS("))
					{
						try
						{
							vars.put("!colstatavgrow!", new Long(sizes.get(i) / data.get(i).size()));
						}
						catch(Exception e)
						{
							vars.put("!colstatavgrow!",  new Long(0));
						}
						break;
					}
					
					i++;
				}
			}
			else if (var.equals("!colstat1kc!"))
			{
				int i = 0;
				long card;
				int[] colIndexes = new int[getNumIndexCols("SYS.COLSTATS")];
				while (i < 4)
				{
					colIndexes[i] = getColIndexForIndex("SYS.COLSTATS", i);
					card = getCompositeColCard("SYS.COLSTATS", colIndexes, i);
					
					if (card != -1)
					{
						vars.put("!colstat" + (i+1) + "kc!", card);
					}
					i++;
				}
				
				card = getFullKeyCard("SYS.COLSTATS", colIndexes);
				if (card != -1)
				{
					vars.put("!colstatfkc!", card);
				}
			}
			else if (var.equals("!coldistrows!"))
			{
				int i = 0;
				for (String header : tableLines)
				{
					if (header.startsWith("SYS.COLDIST"))
					{
						vars.put("!coldistrows!", new Long(data.get(i).size()));
						break;
					}
					
					i++;
				}
			}
			else if (var.equals("!coldistavgrow!"))
			{
				int i = 0;
				for (String header : tableLines)
				{
					if (header.startsWith("SYS.COLDIST"))
					{
						try
						{
							vars.put("!coldistavgrow!", new Long(sizes.get(i) / data.get(i).size()));
						}
						catch(Exception e)
						{
							vars.put("!coldistavgrow!",  new Long(0));
						}
						break;
					}
					
					i++;
				}
			}
			else if (var.equals("!coldist1kc!"))
			{
				int i = 0;
				long card;
				int[] colIndexes = new int[getNumIndexCols("SYS.COLDIST")];
				while (i < 4)
				{
					colIndexes[i] = getColIndexForIndex("SYS.COLDIST", i);
					card = getCompositeColCard("SYS.COLDIST", colIndexes, i);
					
					if (card != -1)
					{
						vars.put("!coldist" + (i+1) + "kc!", card);
					}
					i++;
				}
				
				card = getFullKeyCard("SYS.COLDIST", colIndexes);
				if (card != -1)
				{
					vars.put("!coldistfkc!", card);
				}
			}
		}
		
		HRDBMSWorker.logger.debug("Calling createColStatVars()");
		createColStatVars();
	}
	
	protected static int getFullKeyCard(String table, int[] indexes)
	{
		Vector<String> t = getTable("SYS.TABLES", tableLines, data);
		Vector<String> actual = getTable(table, tableLines, data);
		
		for (String row : t)
		{
			StringTokenizer tokens = new StringTokenizer(row, ",", false);
			int id = Integer.parseInt(tokens.nextToken().trim().substring(1));
			tokens.nextToken();
			if (tokens.nextToken().trim().equals(table.substring(table.indexOf(".") + 1)))
			{
				Vector<String> t2 = getTable("SYS.INDEXES", tableLines, data);
				for (String r : t2)
				{
					StringTokenizer tokens2 = new StringTokenizer(r, ",", false);
					tokens2.nextToken();
					tokens2.nextToken();
					if (id == Integer.parseInt(tokens2.nextToken().trim()))
					{
						if (tokens2.nextToken().trim().equals("Y"))
						{
							return actual.size();
						}
						else
						{
							HRDBMSWorker.logger.error("Non-unique index for table: " + table);
							System.exit(1);
						}
					}
				}
			}
		}
		
		return -1;
	}
	
	protected static int getCompositeColCard(String table, int[] colIndexes, int depth)
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
		
		Vector<String> t = getTable(table, tableLines, data);
		
		HashSet<String> unique = new HashSet<String>();
		for (String row : t)
		{
			String comp = "";
			i = 0;
			while (i <= depth)
			{
				StringTokenizer tokens = new StringTokenizer(row, ",", false);
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
				catch(Exception e)
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
					Long val = vars.get(token);
					
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
	
	protected static Vector<String> getTypes(String table)
	{
		int i = 0;
		for (String line : tableLines)
		{
			if (line.startsWith(table + "("))
			{
				return colTypes.get(i);
			}
			
			i++;
		}
		
		return null;
	}
	
	protected static int getAvgLen(String table, int colIndex)
	{	
		Vector<String> t = getTable(table, tableLines, data);
		Vector<String> types = getTypes(table);
		
		int total = 0;
		int num = 0;
		for (String row : t)
		{
			StringTokenizer tokens = new StringTokenizer(row, ",", false);
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
			
			if (token.startsWith("!"))
			{
				Long val = vars.get(token);
				
				if (val == null)
				{
					token = "null";
				}
				else
				{
					token = val.toString();
				}
			}
			
			if (token.equals("null"))
			{
				continue;
			}
			
			String type = types.get(colIndex);
			if (type.equals("INT"))
			{
				total += 4;
			}
			else if (type.equals("BIGINT"))
			{
				total += 8;
			}
			else if (type.equals("VARCHAR"))
			{
				total += (4 + token.length());
			}
			else if (type.equals("VARBINARY"))
			{
				total += (8 + token.length());
			}
			else
			{
				HRDBMSWorker.logger.error("Unknown type: " + type);
				System.exit(1);
			}
				
			num++;
		}
		
		if (num == 0)
		{
			return 0;
		}
		else
		{
			return total / num;
		}
	}
	
	protected static int getNulls(String table, int colIndex)
	{	
		Vector<String> t = getTable(table, tableLines, data);
		
		int total = 0;
		for (String row : t)
		{
			StringTokenizer tokens = new StringTokenizer(row, ",", false);
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
	
	protected static int getColIndexForIndex(String table, int pos)
	{
		Vector<String> t = getTable("SYS.TABLES", tableLines, data);
		
		for (String row : t)
		{
			StringTokenizer tokens = new StringTokenizer(row, ",", false);
			int id = Integer.parseInt(tokens.nextToken().trim().substring(1));
			tokens.nextToken();
			if (tokens.nextToken().trim().equals(table.substring(table.indexOf(".") + 1)))
			{
				t = getTable("SYS.INDEXCOLS", tableLines, data);
				for (String r : t)
				{
					StringTokenizer tokens2 = new StringTokenizer(r, ",", false);
					tokens2.nextToken();
					if (id == Integer.parseInt(tokens2.nextToken().trim()))
					{
						int colid = Integer.parseInt(tokens2.nextToken().trim());
						
						if (Integer.parseInt(tokens2.nextToken().trim()) == pos)
						{
							return colid;
						}
					}
				}
			}
		}
		
		return -1;
	}
	
	protected static int getNumIndexCols(String table)
	{
		Vector<String> t = getTable("SYS.TABLES", tableLines, data);
		
		for (String row : t)
		{
			StringTokenizer tokens = new StringTokenizer(row, ",", false);
			int id = Integer.parseInt(tokens.nextToken().trim().substring(1));
			tokens.nextToken();
			if (tokens.nextToken().trim().equals(table.substring(table.indexOf(".") + 1)))
			{
				t = getTable("SYS.INDEXES", tableLines, data);
				for (String r : t)
				{
					StringTokenizer tokens2 = new StringTokenizer(r, ",", false);
					tokens2.nextToken();
					tokens2.nextToken();
					if (id == Integer.parseInt(tokens2.nextToken().trim()))
					{
						tokens2.nextToken();
						int retval = Integer.parseInt(tokens2.nextToken().trim());
						if (retval < 4)
						{
							retval = 4;
						}
						return retval;
					}
				}
			}
		}
		
		return -1;
	}
	
	protected static void writeVariables(PrintWriter out)
	{
		for (Entry<String, Long> entry : vars.entrySet())
		{
			out.println("\tprotected static final int " + entry.getKey().substring(0, entry.getKey().length() - 1).substring(1).toUpperCase() + " = " + entry.getValue() + ";");
		}
	}
	
	protected static void writeData(PrintWriter out, Vector<String> table, int dataSize, Vector<String> types)
	{
		out.println("\t\tHRDBMSWorker.logger.debug(\"Writing table data.\");");
		out.println("");
		out.println("\t\ti = bb.position();");
		out.println("\t\twhile (i < (Page.BLOCK_SIZE - " + dataSize + "))");
		out.println("\t\t{");
		out.println("\t\t\tbb.put((byte)0);");
		out.println("\t\t\ti++;");
		out.println("\t\t}");
		out.println("");
		out.println("\t\t//start of data");
		
		for (String row : table)
		{
			StringTokenizer tokens = new StringTokenizer(row.trim(), ",", false);
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
				
				String type = types.get(i);
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
							int val = Integer.parseInt(token);
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
							vars.put(token,  null);
						}
						else
						{
							long val = Long.parseLong(token);
							out.println("\t\tbb.putLong(" + val + ");");
						}
					}
				}
				else if (type.equals("VARCHAR"))
				{
					if (!token.equals("null"))
					{
						out.println("\t\tbb.putInt(" + token.length() + ");");
						out.println("\t\tputString(bb, \"" + token + "\");");
					}
				}
				else if (type.equals("VARBINARY"))
				{
					//always assumes it varchar
					if (!token.equals("null"))
					{
						out.println("\t\tbb.putInt(" + (token.length() + 4) + ");");
						out.println("\t\tbb.putInt(" + token.length() + ");");
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
		out.println("\t}");
		out.println("");
	}
	
	protected static void writeOffsetArray(PrintWriter out, Vector<String> table, int dataSize, Vector<String> types)
	{
		out.println("");
		out.println("HRDBMSWorker.logger.debug(\"Writing offset array\");");
		out.println("\t\t//start of offset array");
		
		int off = dataSize;
		
		for (String row : table)
		{
			StringTokenizer tokens = new StringTokenizer(row, ",", false);
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
				
				String type = types.get(i);
				token = token.trim();
				
				if (type.equals("INT"))
				{
					out.println("\t\tbb.putInt(Page.BLOCK_SIZE - " + off + ");");
					if (!token.equals("null"))
					{
						off -= 4;
					}
				}
				else if (type.equals("BIGINT"))
				{
					out.println("\t\tbb.putInt(Page.BLOCK_SIZE - " + off + ");");
					if (!token.equals("null"))
					{
						off -= 8;
					}
				}
				else if (type.equals("VARCHAR"))
				{
					out.println("\t\tbb.putInt(Page.BLOCK_SIZE - " + off + ");");
					if (!token.equals("null"))
					{
						off -= (4 + token.length());
					}
				}
				else if (type.equals("VARBINARY"))
				{
					out.println("\t\tbb.putInt(Page.BLOCK_SIZE - " + off + ");");
					//always assumes it varchar
					if (!token.equals("null"))
					{
						off -= (8 + token.length());
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
	
	protected static void writeNullArray(PrintWriter out, Vector<String> table)
	{
		out.println("\t\tHRDBMSWorker.logger.debug(\"Writing null array.\");");
		for (String row : table)
		{
			StringTokenizer tokens = new StringTokenizer(row, ",", false);
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
				
				token = token.trim();
				if (token.equals("null"))
				{
					out.println("\t\tbb.put((byte)1);");
				}
				else
				{
					out.println("\t\tbb.put((byte)0);");
				}
			}
		}
	}
	
	protected static int calcDataSize(Vector<String> table, Vector<String> types)
	{
		HRDBMSWorker.logger.debug("In calcDataSize().  Table has " + table.size() + " rows");
		int total = 0;
		
		for (String row : table)
		{
			HRDBMSWorker.logger.debug("Starting processing of a row");
			StringTokenizer tokens = new StringTokenizer(row, ",", false);
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
				String type = types.get(i);
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
						total += (4 + token.length());
					}
				}
				else if (type.equals("VARBINARY"))
				{
					//always assumes it varchar
					if (!token.equals("null"))
					{
						total += (8 + token.length());
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
	
	protected static void createOutputHeader(PrintWriter out)
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
		out.println("");
		out.println("public class CatalogCreator");
		out.println("{");
		out.println("\tFile table;");
		out.println("\tString fn;");
		out.println("\tFileChannel fc = null;");
		out.println("\tByteBuffer bb = null;");
		out.println("\tByteBuffer head = ByteBuffer.allocate(Page.BLOCK_SIZE * 4095);");
		out.println("\tint i = 0;");
		out.println("\tint j = 0;");
		out.println("\tString base = HRDBMSWorker.getHParms().getProperty(\"catalog_directory\");");
		out.println("");
		out.println("\tpublic CatalogCreator() throws IOException, UnsupportedEncodingException");
		out.println("\t{");
		out.println("\t\tHRDBMSWorker.logger.debug(\"CatalogCreator is starting.\");");
		out.println("\t\tif (!base.endsWith(\"/\"))");
		out.println("\t\t{");
		out.println("\t\t\tbase += \"/\";");
		out.println("\t\t}");
		HRDBMSWorker.logger.debug("Number of tables is " + data.size());
		out.println("\t\t i = 0;");
		out.println("\t\thead.position(0);");
		out.println("\t\twhile (i < Page.BLOCK_SIZE * 4095)");
		out.println("\t\t{");
		out.println("\t\t\thead.putLong(-1);");
		out.println("\t\t\ti += 8;");
		out.println("\t\t}");
		int i = 0;
		while (i < data.size())
		{
			out.println("\t\tcreateTable" + i + "();");
			i++;
		}
		out.println("\t}");
	}
	
	protected static void createTableHeader(PrintWriter out, String name, int rows, int cols, int dataSize)
	{
		out.println("\tpublic void createTable" + methodNum + "() throws IOException, UnsupportedEncodingException");
		out.println("\t{");
		methodNum++;
		out.println("\t\tHRDBMSWorker.logger.debug(\"Starting creation of table " + name + "\");");
		out.println("\t\tfn = base + (\"" + name + ".tbl\");");
		out.println("");
		out.println("\t\ttable = new File(fn);");
		out.println("\t\ttable.createNewFile();");
		out.println("");
		out.println("\t\tfc = FileManager.getFile(fn);");
		out.println("\t\tbb = ByteBuffer.allocate(Page.BLOCK_SIZE);");
		out.println("\t\tbb.position(0);");
		out.println("\t\tbb.putInt(0); //node 0");
		out.println("\t\tbb.putInt(0); //device 0");
		out.println("");
		out.println("\t\tbb.putInt(Page.BLOCK_SIZE - (57 + " + dataSize + " + (16 * " + rows + ") + (5 * " + rows + " * " + cols + ") + (" + cols + " * 4))); //largest free size");
		HRDBMSWorker.logger.info("Free Space in block = " + (64 * 1024 - (57 + dataSize + (16 * rows) + (5 * rows * cols) + (cols * 4))));
		out.println("");
		out.println("\t\ti = 12;");
		out.println("\t\twhile (i < Page.BLOCK_SIZE)");
		out.println("\t\t{");
		out.println("\t\t\tbb.putInt(-1);");
		out.println("\t\t\ti += 4;");
		out.println("\t\t}");
		out.println("");
		out.println("\t\tbb.position(0);");
		out.println("\t\tfc.write(bb);");
		out.println("\t\t//done writing first header page");
		out.println("\t\tHRDBMSWorker.logger.debug(\"Done writing first header page.\");");
		out.println("");
		//out.println("\t\t\ti = 0;");
		//out.println("\t\t\tbb.position(0);");
		//out.println("\t\t\twhile (i < Page.BLOCK_SIZE)");
		//out.println("\t\t\t{");
		//out.println("\t\t\t\tbb.putLong(-1);");
		//out.println("\t\t\t\ti += 8;");
		//out.println("\t\t\t}");
		//out.println("\t\tj = 1;");
		//out.println("\t\twhile (j < 4096)");
		//out.println("\t\t{");
		//out.println("\t\t\tbb.position(0);");
		//out.println("\t\t\tfc.write(bb);");
		//out.println("\t\t\tj++;");
		//out.println("\t\t}");
		out.println("\t\thead.position(0);");
		out.println("\t\tfc.write(head);");
		out.println("");
		out.println("\t\t//done writing header pages");
		out.println("\t\tbb.position(0);");
		out.println("\t\tbb.put(Schema.TYPE_ROW);");
		out.println("\t\tbb.putInt(" + rows + "); //nextRecNum");
		out.println("\t\tbb.putInt(56 + (16 * " + rows + ") + (5 * " + rows + " * " + cols + ") + (" + cols + " * 4)); //headEnd for " + rows + " rows, " + cols + " cols");
		out.println("\t\tbb.putInt(Page.BLOCK_SIZE - " + dataSize + "); //dataStart, total data size " + dataSize); 
		out.println("\t\tbb.putLong(System.currentTimeMillis()); //modTime");
		out.println("\t\tbb.putInt(57 + (16 * " + rows + ") + (4 * " + cols + ")); //nullArray offset for " + rows + " rows " + cols + " cols");
		out.println("\t\tbb.putInt(49); //colIDListOff");
		out.println("\t\tbb.putInt(53 + (4 * " + cols + ")); //rowIDListOff for " + cols + " cols");
		out.println("\t\tbb.putInt(57 + (16 * " + rows + ") + (4 * " + cols + ") + (" + rows + " * " + cols + ")); // offset Array offset for " + rows + "rows " + cols + " cols");
		out.println("\t\tbb.putInt(1); //freeSpaceListEntries");
		out.println("\t\tbb.putInt(57 + (16 * " + rows + ") + (5 * " + rows + " * " + cols + ") + (" + cols + " * 4)); //free space start = headEnd + 1");
		out.println("\t\tbb.putInt(Page.BLOCK_SIZE - 1 - " + dataSize + "); //free space end = dataStart - 1");
		out.println("\t\tbb.putInt(" + cols + "); //colIDListSize - start of colIDs");
		out.println("");
		out.println("\t\ti = 0;");
		out.println("\t\twhile (i < " + cols + ")");
		out.println("\t\t{");
		out.println("\t\t\tbb.putInt(i);");
		out.println("\t\t\ti++;");
		out.println("\t\t}");
		out.println("");
		out.println("\t\tbb.putInt(" + rows + "); //rowIDListSize - start of rowIDs");
		out.println("\t\ti = 0;");
		out.println("\t\twhile (i < 0)");
		out.println("\t\t{");
		out.println("\t\t\tbb.putInt(0); //node 0");
		out.println("\t\t\tbb.putInt(0); //device 0");
		out.println("\t\t\tbb.putInt(4096); //block 4096");
		out.println("\t\t\tbb.putInt(i); //record i");
		out.println("\t\t\ti++;");
		out.println("\t\t}");
		out.println("");
		out.println("\t\t//null Array start");
	}
	
	protected class TextRowSorter implements Comparable
	{
		protected Vector<Comparable> cols = new Vector<Comparable>();
		
		public void add(Integer col)
		{
			cols.add(col);
		}
		
		public void add(String col)
		{
			cols.add(col);
		}
		
		public boolean equals(Object rhs)
		{
			
			if (rhs == null)
			{
				return false;
			}
			
			if (rhs instanceof TextRowSorter)
			{
				TextRowSorter trs = (TextRowSorter)rhs;
				if (trs.cols.size() == this.cols.size())
				{
					Iterator<Comparable> lhsIt = this.cols.iterator();
					Iterator<Comparable> rhsIt = trs.cols.iterator();
					
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
	
		public int compareTo(Object r) throws IllegalArgumentException
		{
			if (! (r instanceof TextRowSorter))
			{
				throw new IllegalArgumentException();
			}
			
			TextRowSorter rhs = (TextRowSorter)r;
			if (this.cols.size() != rhs.cols.size())
			{
				throw new IllegalArgumentException();
			}
		
			Iterator<Comparable> lhsIt = this.cols.iterator();
			Iterator<Comparable> rhsIt = rhs.cols.iterator();
			
			while (lhsIt.hasNext())
			{
				int result = lhsIt.next().compareTo(rhsIt.next());
				
				if (result != 0)
				{
					return result;
				}
			}
			
			return 0;
		}
		
		protected ByteBuffer getKeyBytes() throws UnsupportedEncodingException
		{
			int i = this.cols.size();
			for (Comparable col : this.cols)
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
			
			ByteBuffer ret = ByteBuffer.allocate(i);
			ret.position(0);
			for (Comparable col : this.cols)
			{
				ret.put((byte)0); //not null
				
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
	
	public static boolean isThisMyIpAddress(InetAddress addr) {
	    // Check if the address is a valid special local or loop back
	    if (addr.isAnyLocalAddress() || addr.isLoopbackAddress())
	        return true;

	    // Check if the address is defined on any interface
	    try {
	        return NetworkInterface.getByInetAddress(addr) != null;
	    } catch (SocketException e) {
	        return false;
	    }
	}
}
