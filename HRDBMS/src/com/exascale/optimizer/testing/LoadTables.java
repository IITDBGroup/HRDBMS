package com.exascale.optimizer.testing;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;

public class LoadTables 
{
	protected static HashMap<Integer, ArrayList<Integer>> nodeGroupMap = null;
	protected static ArrayList<Integer> nodeGroupSet = null;
	protected static ArrayList<String> nodeGroupHash = null;;
	protected static ArrayList<Object> nodeGroupRange = null;
	protected static ArrayList<Integer> nodeSet = null;
	protected static ArrayList<String> nodeHash = null;
	protected static ArrayList<Object> nodeRange = null;
	protected static ArrayList<Integer> deviceSet = null;
	protected static ArrayList<String> deviceHash = null;
	protected static ArrayList<Object> deviceRange = null;
	protected static boolean allNodes = false;
	protected static boolean anyNode = false;
	protected static boolean allDevices = false;
	protected static TreeMap<Integer, String> pos2Col = null;
	protected static HashMap<String, String> cols2Types = null;
	protected static HashMap<String, Integer> cols2Pos = null;
	protected static String nodeGroupRangeCol = null;
	protected static String nodeRangeCol = null;
	protected static String deviceRangeCol = null;
	protected static HashMap<String, PrintWriter> writers = null;
	//protected static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	protected static final Long LARGE_PRIME =  1125899906842597L;
    protected static final Long LARGE_PRIME2 = 6920451961L;
	
	public static void main(String[] args)
	{
		try
		{
			MetaData meta = new MetaData();
			BufferedReader in = new BufferedReader(new FileReader("./customer.dat"));
			TableScanOperator t = new TableScanOperator("TPCH", "CUSTOMER", meta);
			t.setMetaData();
			nodeGroupMap = null;;
			nodeGroupSet = null;
			nodeGroupHash = null;;
			nodeGroupRange = null;
			nodeGroupRangeCol = null;
			nodeSet = null;
			nodeHash = null;
			nodeRange = null;
			nodeRangeCol = null;
			deviceSet = null;
			deviceHash = null;
			deviceRange = null;
			deviceRangeCol = null;
			allNodes = false;
			anyNode = false;
			allDevices = false;
			pos2Col = null;
			cols2Types = null;
			cols2Pos = null;
			if (t.noNodeGroupSet())
			{
				nodeGroupMap = new HashMap<Integer, ArrayList<Integer>>();
				int i = 0;
				ArrayList<Integer> nodes = new ArrayList<Integer>();
				while (i < meta.getNumNodes())
				{
					nodes.add(i);
					i++;
				}
				nodeGroupMap.put(0, nodes);
				nodeGroupSet = new ArrayList<Integer>();
				nodeGroupSet.add(0);
			}
			else
			{
				nodeGroupMap = t.getNodeGroupHashMap();
				nodeGroupSet = t.nodeGroupSet();
				nodeGroupHash = t.getNodeGroupHash();
				nodeGroupRange = t.getNodeGroupRanges();
				nodeGroupRangeCol = t.getNodeGroupRangeCol();
			}
			
			if (t.allNodes())
			{
				allNodes = true;
			}
			
			if (t.anyNode())
			{
				anyNode = true;
			}
			
			if (t.allDevices())
			{
				allDevices = true;
			}
			
			nodeSet = t.nodeSet();
			if (allNodes)
			{
				nodeSet = new ArrayList<Integer>();
				int i = 0;
				while (i < meta.getNumNodes())
				{
					nodeSet.add(i);
					i++;
				}
			}
			deviceSet = t.deviceSet();
			if (allDevices)
			{
				deviceSet = new ArrayList<Integer>();
				int i = 0;
				while (i < t.getNumDevices())
				{
					deviceSet.add(i);
					i++;
				}
			}
			nodeHash = t.getNodeHash();
			deviceHash = t.getDeviceHash();
			nodeRange = t.getNodeRanges();
			deviceRange = t.getDeviceRanges();
			nodeRangeCol = t.getNodeRangeCol();
			deviceRangeCol = t.getDeviceRangeCol();
			writers = new HashMap<String, PrintWriter>();
			pos2Col = t.getPos2Col();
			cols2Types = t.getCols2Types();
			cols2Pos = t.getCols2Pos();
			
			String line = in.readLine();
			while (line != null)
			{
				ArrayList<PrintWriter> outs = getOutsForRow(line, pos2Col, cols2Types, cols2Pos, "CUSTOMER");
				for (PrintWriter out : outs)
				{
					out.println(line);
				}
				
				line = in.readLine();
			}
			
			in.close();
			
			for (PrintWriter writer : writers.values())
			{
				writer.close();
			}
			
			in = new BufferedReader(new FileReader("./orders.dat"));
			t = new TableScanOperator("TPCH", "ORDERS", meta);
			t.setMetaData();
			nodeGroupMap = null;;
			nodeGroupSet = null;
			nodeGroupHash = null;;
			nodeGroupRange = null;
			nodeGroupRangeCol = null;
			nodeSet = null;
			nodeHash = null;
			nodeRange = null;
			nodeRangeCol = null;
			deviceSet = null;
			deviceHash = null;
			deviceRange = null;
			deviceRangeCol = null;
			allNodes = false;
			anyNode = false;
			allDevices = false;
			pos2Col = null;
			cols2Types = null;
			cols2Pos = null;
			if (t.noNodeGroupSet())
			{
				nodeGroupMap = new HashMap<Integer, ArrayList<Integer>>();
				int i = 0;
				ArrayList<Integer> nodes = new ArrayList<Integer>();
				while (i < meta.getNumNodes())
				{
					nodes.add(i);
					i++;
				}
				nodeGroupMap.put(0, nodes);
				nodeGroupSet = new ArrayList<Integer>();
				nodeGroupSet.add(0);
			}
			else
			{
				nodeGroupMap = t.getNodeGroupHashMap();
				nodeGroupSet = t.nodeGroupSet();
				nodeGroupHash = t.getNodeGroupHash();
				nodeGroupRange = t.getNodeGroupRanges();
				nodeGroupRangeCol = t.getNodeGroupRangeCol();
			}
			
			if (t.allNodes())
			{
				allNodes = true;
			}
			
			if (t.anyNode())
			{
				anyNode = true;
			}
			
			if (t.allDevices())
			{
				allDevices = true;
			}
			
			nodeSet = t.nodeSet();
			if (allNodes)
			{
				nodeSet = new ArrayList<Integer>();
				int i = 0;
				while (i < meta.getNumNodes())
				{
					nodeSet.add(i);
					i++;
				}
			}
			deviceSet = t.deviceSet();
			if (allDevices)
			{
				deviceSet = new ArrayList<Integer>();
				int i = 0;
				while (i < t.getNumDevices())
				{
					deviceSet.add(i);
					i++;
				}
			}
			nodeHash = t.getNodeHash();
			deviceHash = t.getDeviceHash();
			nodeRange = t.getNodeRanges();
			deviceRange = t.getDeviceRanges();
			nodeRangeCol = t.getNodeRangeCol();
			deviceRangeCol = t.getDeviceRangeCol();
			writers = new HashMap<String, PrintWriter>();
			pos2Col = t.getPos2Col();
			cols2Types = t.getCols2Types();
			cols2Pos = t.getCols2Pos();
			
			line = in.readLine();
			while (line != null)
			{
				ArrayList<PrintWriter> outs = getOutsForRow(line, pos2Col, cols2Types, cols2Pos, "ORDERS");
				for (PrintWriter out : outs)
				{
					out.println(line);
				}
				
				line = in.readLine();
			}
			
			in.close();
			
			for (PrintWriter writer : writers.values())
			{
				writer.close();
			}
			
			in = new BufferedReader(new FileReader("./lineitem.dat"));
			t = new TableScanOperator("TPCH", "LINEITEM", meta);
			t.setMetaData();
			nodeGroupMap = null;;
			nodeGroupSet = null;
			nodeGroupHash = null;;
			nodeGroupRange = null;
			nodeGroupRangeCol = null;
			nodeSet = null;
			nodeHash = null;
			nodeRange = null;
			nodeRangeCol = null;
			deviceSet = null;
			deviceHash = null;
			deviceRange = null;
			deviceRangeCol = null;
			allNodes = false;
			anyNode = false;
			allDevices = false;
			pos2Col = null;
			cols2Types = null;
			cols2Pos = null;
			if (t.noNodeGroupSet())
			{
				nodeGroupMap = new HashMap<Integer, ArrayList<Integer>>();
				int i = 0;
				ArrayList<Integer> nodes = new ArrayList<Integer>();
				while (i < meta.getNumNodes())
				{
					nodes.add(i);
					i++;
				}
				nodeGroupMap.put(0, nodes);
				nodeGroupSet = new ArrayList<Integer>();
				nodeGroupSet.add(0);
			}
			else
			{
				nodeGroupMap = t.getNodeGroupHashMap();
				nodeGroupSet = t.nodeGroupSet();
				nodeGroupHash = t.getNodeGroupHash();
				nodeGroupRange = t.getNodeGroupRanges();
				nodeGroupRangeCol = t.getNodeGroupRangeCol();
			}
			
			if (t.allNodes())
			{
				allNodes = true;
			}
			
			if (t.anyNode())
			{
				anyNode = true;
			}
			
			if (t.allDevices())
			{
				allDevices = true;
			}
			
			nodeSet = t.nodeSet();
			if (allNodes)
			{
				nodeSet = new ArrayList<Integer>();
				int i = 0;
				while (i < meta.getNumNodes())
				{
					nodeSet.add(i);
					i++;
				}
			}
			deviceSet = t.deviceSet();
			if (allDevices)
			{
				deviceSet = new ArrayList<Integer>();
				int i = 0;
				while (i < t.getNumDevices())
				{
					deviceSet.add(i);
					i++;
				}
			}
			nodeHash = t.getNodeHash();
			deviceHash = t.getDeviceHash();
			nodeRange = t.getNodeRanges();
			deviceRange = t.getDeviceRanges();
			nodeRangeCol = t.getNodeRangeCol();
			deviceRangeCol = t.getDeviceRangeCol();
			writers = new HashMap<String, PrintWriter>();
			pos2Col = t.getPos2Col();
			cols2Types = t.getCols2Types();
			cols2Pos = t.getCols2Pos();
			
			line = in.readLine();
			while (line != null)
			{
				ArrayList<PrintWriter> outs = getOutsForRow(line, pos2Col, cols2Types, cols2Pos, "LINEITEM");
				for (PrintWriter out : outs)
				{
					out.println(line);
				}
				
				line = in.readLine();
			}
			
			in.close();
			
			for (PrintWriter writer : writers.values())
			{
				writer.close();
			}
			
			in = new BufferedReader(new FileReader("./part.dat"));
			t = new TableScanOperator("TPCH", "PART", meta);
			t.setMetaData();
			nodeGroupMap = null;;
			nodeGroupSet = null;
			nodeGroupHash = null;;
			nodeGroupRange = null;
			nodeGroupRangeCol = null;
			nodeSet = null;
			nodeHash = null;
			nodeRange = null;
			nodeRangeCol = null;
			deviceSet = null;
			deviceHash = null;
			deviceRange = null;
			deviceRangeCol = null;
			allNodes = false;
			anyNode = false;
			allDevices = false;
			pos2Col = null;
			cols2Types = null;
			cols2Pos = null;
			if (t.noNodeGroupSet())
			{
				nodeGroupMap = new HashMap<Integer, ArrayList<Integer>>();
				int i = 0;
				ArrayList<Integer> nodes = new ArrayList<Integer>();
				while (i < meta.getNumNodes())
				{
					nodes.add(i);
					i++;
				}
				nodeGroupMap.put(0, nodes);
				nodeGroupSet = new ArrayList<Integer>();
				nodeGroupSet.add(0);
			}
			else
			{
				nodeGroupMap = t.getNodeGroupHashMap();
				nodeGroupSet = t.nodeGroupSet();
				nodeGroupHash = t.getNodeGroupHash();
				nodeGroupRange = t.getNodeGroupRanges();
				nodeGroupRangeCol = t.getNodeGroupRangeCol();
			}
			
			if (t.allNodes())
			{
				allNodes = true;
			}
			
			if (t.anyNode())
			{
				anyNode = true;
			}
			
			if (t.allDevices())
			{
				allDevices = true;
			}
			
			nodeSet = t.nodeSet();
			if (allNodes)
			{
				nodeSet = new ArrayList<Integer>();
				int i = 0;
				while (i < meta.getNumNodes())
				{
					nodeSet.add(i);
					i++;
				}
			}
			deviceSet = t.deviceSet();
			if (allDevices)
			{
				deviceSet = new ArrayList<Integer>();
				int i = 0;
				while (i < t.getNumDevices())
				{
					deviceSet.add(i);
					i++;
				}
			}
			nodeHash = t.getNodeHash();
			deviceHash = t.getDeviceHash();
			nodeRange = t.getNodeRanges();
			deviceRange = t.getDeviceRanges();
			nodeRangeCol = t.getNodeRangeCol();
			deviceRangeCol = t.getDeviceRangeCol();
			writers = new HashMap<String, PrintWriter>();
			pos2Col = t.getPos2Col();
			cols2Types = t.getCols2Types();
			cols2Pos = t.getCols2Pos();
			
			line = in.readLine();
			while (line != null)
			{
				ArrayList<PrintWriter> outs = getOutsForRow(line, pos2Col, cols2Types, cols2Pos, "PART");
				for (PrintWriter out : outs)
				{
					out.println(line);
				}
				
				line = in.readLine();
			}
			
			in.close();
			
			for (PrintWriter writer : writers.values())
			{
				writer.close();
			}
			
			in = new BufferedReader(new FileReader("./supplier.dat"));
			t = new TableScanOperator("TPCH", "SUPPLIER", meta);
			t.setMetaData();
			nodeGroupMap = null;;
			nodeGroupSet = null;
			nodeGroupHash = null;;
			nodeGroupRange = null;
			nodeGroupRangeCol = null;
			nodeSet = null;
			nodeHash = null;
			nodeRange = null;
			nodeRangeCol = null;
			deviceSet = null;
			deviceHash = null;
			deviceRange = null;
			deviceRangeCol = null;
			allNodes = false;
			anyNode = false;
			allDevices = false;
			pos2Col = null;
			cols2Types = null;
			cols2Pos = null;
			if (t.noNodeGroupSet())
			{
				nodeGroupMap = new HashMap<Integer, ArrayList<Integer>>();
				int i = 0;
				ArrayList<Integer> nodes = new ArrayList<Integer>();
				while (i < meta.getNumNodes())
				{
					nodes.add(i);
					i++;
				}
				nodeGroupMap.put(0, nodes);
				nodeGroupSet = new ArrayList<Integer>();
				nodeGroupSet.add(0);
			}
			else
			{
				nodeGroupMap = t.getNodeGroupHashMap();
				nodeGroupSet = t.nodeGroupSet();
				nodeGroupHash = t.getNodeGroupHash();
				nodeGroupRange = t.getNodeGroupRanges();
				nodeGroupRangeCol = t.getNodeGroupRangeCol();
			}
			
			if (t.allNodes())
			{
				allNodes = true;
			}
			
			if (t.anyNode())
			{
				anyNode = true;
			}
			
			if (t.allDevices())
			{
				allDevices = true;
			}
			
			nodeSet = t.nodeSet();
			if (allNodes)
			{
				nodeSet = new ArrayList<Integer>();
				int i = 0;
				while (i < meta.getNumNodes())
				{
					nodeSet.add(i);
					i++;
				}
			}
			deviceSet = t.deviceSet();
			if (allDevices)
			{
				deviceSet = new ArrayList<Integer>();
				int i = 0;
				while (i < t.getNumDevices())
				{
					deviceSet.add(i);
					i++;
				}
			}
			nodeHash = t.getNodeHash();
			deviceHash = t.getDeviceHash();
			nodeRange = t.getNodeRanges();
			deviceRange = t.getDeviceRanges();
			nodeRangeCol = t.getNodeRangeCol();
			deviceRangeCol = t.getDeviceRangeCol();
			writers = new HashMap<String, PrintWriter>();
			pos2Col = t.getPos2Col();
			cols2Types = t.getCols2Types();
			cols2Pos = t.getCols2Pos();
			
			line = in.readLine();
			while (line != null)
			{
				ArrayList<PrintWriter> outs = getOutsForRow(line, pos2Col, cols2Types, cols2Pos, "SUPPLIER");
				for (PrintWriter out : outs)
				{
					out.println(line);
				}
				
				line = in.readLine();
			}
			
			in.close();
			
			for (PrintWriter writer : writers.values())
			{
				writer.close();
			}
			
			in = new BufferedReader(new FileReader("./partsupp.dat"));
			t = new TableScanOperator("TPCH", "PARTSUPP", meta);
			t.setMetaData();
			nodeGroupMap = null;;
			nodeGroupSet = null;
			nodeGroupHash = null;;
			nodeGroupRange = null;
			nodeGroupRangeCol = null;
			nodeSet = null;
			nodeHash = null;
			nodeRange = null;
			nodeRangeCol = null;
			deviceSet = null;
			deviceHash = null;
			deviceRange = null;
			deviceRangeCol = null;
			allNodes = false;
			anyNode = false;
			allDevices = false;
			pos2Col = null;
			cols2Types = null;
			cols2Pos = null;
			if (t.noNodeGroupSet())
			{
				nodeGroupMap = new HashMap<Integer, ArrayList<Integer>>();
				int i = 0;
				ArrayList<Integer> nodes = new ArrayList<Integer>();
				while (i < meta.getNumNodes())
				{
					nodes.add(i);
					i++;
				}
				nodeGroupMap.put(0, nodes);
				nodeGroupSet = new ArrayList<Integer>();
				nodeGroupSet.add(0);
			}
			else
			{
				nodeGroupMap = t.getNodeGroupHashMap();
				nodeGroupSet = t.nodeGroupSet();
				nodeGroupHash = t.getNodeGroupHash();
				nodeGroupRange = t.getNodeGroupRanges();
				nodeGroupRangeCol = t.getNodeGroupRangeCol();
			}
			
			if (t.allNodes())
			{
				allNodes = true;
			}
			
			if (t.anyNode())
			{
				anyNode = true;
			}
			
			if (t.allDevices())
			{
				allDevices = true;
			}
			
			nodeSet = t.nodeSet();
			if (allNodes)
			{
				nodeSet = new ArrayList<Integer>();
				int i = 0;
				while (i < meta.getNumNodes())
				{
					nodeSet.add(i);
					i++;
				}
			}
			deviceSet = t.deviceSet();
			if (allDevices)
			{
				deviceSet = new ArrayList<Integer>();
				int i = 0;
				while (i < t.getNumDevices())
				{
					deviceSet.add(i);
					i++;
				}
			}
			nodeHash = t.getNodeHash();
			deviceHash = t.getDeviceHash();
			nodeRange = t.getNodeRanges();
			deviceRange = t.getDeviceRanges();
			nodeRangeCol = t.getNodeRangeCol();
			deviceRangeCol = t.getDeviceRangeCol();
			writers = new HashMap<String, PrintWriter>();
			pos2Col = t.getPos2Col();
			cols2Types = t.getCols2Types();
			cols2Pos = t.getCols2Pos();
			
			line = in.readLine();
			while (line != null)
			{
				ArrayList<PrintWriter> outs = getOutsForRow(line, pos2Col, cols2Types, cols2Pos, "PARTSUPP");
				for (PrintWriter out : outs)
				{
					out.println(line);
				}
				
				line = in.readLine();
			}
			
			in.close();
			
			for (PrintWriter writer : writers.values())
			{
				writer.close();
			}
			
			in = new BufferedReader(new FileReader("./nation.dat"));
			t = new TableScanOperator("TPCH", "NATION", meta);
			t.setMetaData();
			nodeGroupMap = null;;
			nodeGroupSet = null;
			nodeGroupHash = null;;
			nodeGroupRange = null;
			nodeGroupRangeCol = null;
			nodeSet = null;
			nodeHash = null;
			nodeRange = null;
			nodeRangeCol = null;
			deviceSet = null;
			deviceHash = null;
			deviceRange = null;
			deviceRangeCol = null;
			allNodes = false;
			anyNode = false;
			allDevices = false;
			pos2Col = null;
			cols2Types = null;
			cols2Pos = null;
			if (t.noNodeGroupSet())
			{
				nodeGroupMap = new HashMap<Integer, ArrayList<Integer>>();
				int i = 0;
				ArrayList<Integer> nodes = new ArrayList<Integer>();
				while (i < meta.getNumNodes())
				{
					nodes.add(i);
					i++;
				}
				nodeGroupMap.put(0, nodes);
				nodeGroupSet = new ArrayList<Integer>();
				nodeGroupSet.add(0);
			}
			else
			{
				nodeGroupMap = t.getNodeGroupHashMap();
				nodeGroupSet = t.nodeGroupSet();
				nodeGroupHash = t.getNodeGroupHash();
				nodeGroupRange = t.getNodeGroupRanges();
				nodeGroupRangeCol = t.getNodeGroupRangeCol();
			}
			
			if (t.allNodes())
			{
				allNodes = true;
			}
			
			if (t.anyNode())
			{
				anyNode = true;
			}
			
			if (t.allDevices())
			{
				allDevices = true;
			}
			
			nodeSet = t.nodeSet();
			if (allNodes)
			{
				nodeSet = new ArrayList<Integer>();
				int i = 0;
				while (i < meta.getNumNodes())
				{
					nodeSet.add(i);
					i++;
				}
			}
			deviceSet = t.deviceSet();
			if (allDevices)
			{
				deviceSet = new ArrayList<Integer>();
				int i = 0;
				while (i < t.getNumDevices())
				{
					deviceSet.add(i);
					i++;
				}
			}
			nodeHash = t.getNodeHash();
			deviceHash = t.getDeviceHash();
			nodeRange = t.getNodeRanges();
			deviceRange = t.getDeviceRanges();
			nodeRangeCol = t.getNodeRangeCol();
			deviceRangeCol = t.getDeviceRangeCol();
			writers = new HashMap<String, PrintWriter>();
			pos2Col = t.getPos2Col();
			cols2Types = t.getCols2Types();
			cols2Pos = t.getCols2Pos();
			
			line = in.readLine();
			while (line != null)
			{
				ArrayList<PrintWriter> outs = getOutsForRow(line, pos2Col, cols2Types, cols2Pos, "NATION");
				for (PrintWriter out : outs)
				{
					out.println(line);
				}
				
				line = in.readLine();
			}
			
			in.close();
			
			for (PrintWriter writer : writers.values())
			{
				writer.close();
			}
			
			in = new BufferedReader(new FileReader("./region.dat"));
			t = new TableScanOperator("TPCH", "REGION", meta);
			t.setMetaData();
			nodeGroupMap = null;;
			nodeGroupSet = null;
			nodeGroupHash = null;;
			nodeGroupRange = null;
			nodeGroupRangeCol = null;
			nodeSet = null;
			nodeHash = null;
			nodeRange = null;
			nodeRangeCol = null;
			deviceSet = null;
			deviceHash = null;
			deviceRange = null;
			deviceRangeCol = null;
			allNodes = false;
			anyNode = false;
			allDevices = false;
			pos2Col = null;
			cols2Types = null;
			cols2Pos = null;
			if (t.noNodeGroupSet())
			{
				nodeGroupMap = new HashMap<Integer, ArrayList<Integer>>();
				int i = 0;
				ArrayList<Integer> nodes = new ArrayList<Integer>();
				while (i < meta.getNumNodes())
				{
					nodes.add(i);
					i++;
				}
				nodeGroupMap.put(0, nodes);
				nodeGroupSet = new ArrayList<Integer>();
				nodeGroupSet.add(0);
			}
			else
			{
				nodeGroupMap = t.getNodeGroupHashMap();
				nodeGroupSet = t.nodeGroupSet();
				nodeGroupHash = t.getNodeGroupHash();
				nodeGroupRange = t.getNodeGroupRanges();
				nodeGroupRangeCol = t.getNodeGroupRangeCol();
			}
			
			if (t.allNodes())
			{
				allNodes = true;
			}
			
			if (t.anyNode())
			{
				anyNode = true;
			}
			
			if (t.allDevices())
			{
				allDevices = true;
			}
			
			nodeSet = t.nodeSet();
			if (allNodes)
			{
				nodeSet = new ArrayList<Integer>();
				int i = 0;
				while (i < meta.getNumNodes())
				{
					nodeSet.add(i);
					i++;
				}
			}
			deviceSet = t.deviceSet();
			if (allDevices)
			{
				deviceSet = new ArrayList<Integer>();
				int i = 0;
				while (i < t.getNumDevices())
				{
					deviceSet.add(i);
					i++;
				}
			}
			nodeHash = t.getNodeHash();
			deviceHash = t.getDeviceHash();
			nodeRange = t.getNodeRanges();
			deviceRange = t.getDeviceRanges();
			nodeRangeCol = t.getNodeRangeCol();
			deviceRangeCol = t.getDeviceRangeCol();
			writers = new HashMap<String, PrintWriter>();
			pos2Col = t.getPos2Col();
			cols2Types = t.getCols2Types();
			cols2Pos = t.getCols2Pos();
			
			line = in.readLine();
			while (line != null)
			{
				ArrayList<PrintWriter> outs = getOutsForRow(line, pos2Col, cols2Types, cols2Pos, "REGION");
				for (PrintWriter out : outs)
				{
					out.println(line);
				}
				
				line = in.readLine();
			}
			
			in.close();
			
			for (PrintWriter writer : writers.values())
			{
				writer.close();
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	public static ArrayList<PrintWriter> getOutsForRow(String line, TreeMap<Integer, String> pos2Col, HashMap<String, String> cols2Types, HashMap<String, Integer> cols2Pos, String table)
	{
		ArrayList<Integer> allNodeList = null;
		ArrayList<Object> row = parseRow(line, pos2Col, cols2Types);
		if (nodeGroupSet.size() == 1)
		{
			allNodeList = nodeGroupMap.get(nodeGroupSet.get(0));
			if (allNodeList.size() == 0)
			{
				System.out.println("nodeGroupSet = " + nodeGroupSet);
				System.out.println("nodeGroupMap = " + nodeGroupMap);
				Thread.dumpStack();
			}
		}
		else if (nodeGroupHash != null)
		{
			long hash = getHashValueForRow(row, nodeGroupHash, cols2Pos);
			allNodeList = nodeGroupMap.get(nodeGroupSet.get((int)(hash % nodeGroupSet.size())));
		}
		else
		{
			Object o = row.get(cols2Pos.get(nodeGroupRangeCol));
			int i = 0;
			for (Object comp : nodeGroupRange)
			{
				if (((Comparable)o).compareTo(comp) < 1)
				{
					allNodeList = nodeGroupMap.get(nodeGroupSet.get(i));
					break;
				}
				
				i++;
			}
			
			if (allNodeList == null)
			{
				allNodeList = nodeGroupMap.get(nodeGroupSet.get(i));
			}
		}
		ArrayList<Integer> chosenNodes = new ArrayList<Integer>();
		if (anyNode)
		{
			chosenNodes = allNodeList;
		}
		else if (nodeSet.size() == 1)
		{
			chosenNodes.add(allNodeList.get(nodeSet.get(0)));
		}
		else if (nodeHash != null)
		{
			long hash = getHashValueForRow(row, nodeHash, cols2Pos);
			if (allNodes)
			{
				chosenNodes.add(allNodeList.get((int)(hash % allNodeList.size())));
			}
			else
			{
				chosenNodes.add(allNodeList.get(nodeSet.get((int)(hash % nodeSet.size()))));
			}
		}
		else
		{
			Object o = row.get(cols2Pos.get(nodeRangeCol));
			int i = 0;
			for (Object comp : nodeRange)
			{
				if (((Comparable)o).compareTo(comp) < 1)
				{
					if (allNodes)
					{
						chosenNodes.add(allNodeList.get(i));
					}
					else
					{
						chosenNodes.add(allNodeList.get(nodeSet.get(i)));
					}
					break;
				}
				
				i++;
			}
			
			if (chosenNodes.size() == 0)
			{
				if (allNodes)
				{
					chosenNodes.add(allNodeList.get(i));
				}
				else
				{
					chosenNodes.add(allNodeList.get(nodeSet.get(i)));
				}
			}
		}
		int chosenDevice = -1;
		if (deviceSet.size() == 1)
		{
			chosenDevice = deviceSet.get(0);
		}
		else if (deviceHash != null)
		{
			long hash = getHashValueForRow(row, deviceHash, cols2Pos);
			chosenDevice = deviceSet.get((int)(hash % deviceSet.size()));
		}
		else
		{
			Object o = row.get(cols2Pos.get(deviceRangeCol));
			int i = 0;
			for (Object comp : deviceRange)
			{
				if (((Comparable)o).compareTo(comp) < 1)
				{
					chosenDevice = deviceSet.get(i);
					break;
				}
				
				i++;
			}
			
			if (chosenDevice == -1)
			{
				chosenDevice = deviceSet.get(i);
			}
		}
		
		ArrayList<PrintWriter> retval = new ArrayList<PrintWriter>();
		for (int node : chosenNodes)
		{
			String name = table.toLowerCase() + ".tbl.N" + node + ".D" + chosenDevice;
			PrintWriter writer = writers.get(name);
			if (writer != null)
			{
				retval.add(writer);
			}
			else
			{
				try
				{
					writer = new PrintWriter(new FileWriter(name));
				}
				catch(Exception e)
				{
					e.printStackTrace();
					System.exit(1);
				}
				writers.put(name, writer);
				retval.add(writer);
			}
		}
		
		return retval;
	}
	
	protected static ArrayList<Object> parseRow(String line, TreeMap<Integer, String> pos2Col, HashMap<String, String> cols2Types)
	{
		FastStringTokenizer tokens = new FastStringTokenizer(line, "|", false);
		ArrayList<Object> row = new ArrayList<Object>();
		int i = 0;
		try
		{
			while (tokens.hasMoreTokens())
			{
				String type = cols2Types.get(pos2Col.get(i));
				if (type.equals("INT"))
				{
					row.add(Utils.parseInt(tokens.nextToken()));
				}
				else if (type.equals("FLOAT"))
				{
					row.add(Utils.parseDouble(tokens.nextToken()));
				}
				else if (type.equals("CHAR"))
				{
					row.add(tokens.nextToken());
				}
				else if (type.equals("LONG"))
				{
					row.add(Utils.parseLong(tokens.nextToken()));
				}
				else if (type.equals("DATE"))
				{
					row.add(DateParser.parse(tokens.nextToken()));
				}
				
				i++;
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
			System.exit(1);
		}
		
		return row;
	}
	
	public static long getHashValueForRow(ArrayList<Object> row, ArrayList<String> hashCols, HashMap<String, Integer> cols2Pos)
	{
		ArrayList<Object> temp = new ArrayList<Object>();
		for (String col : hashCols)
		{
			temp.add(row.get(cols2Pos.get(col)));
		}
		
		return 0x0EFFFFFFFFFFFFFFL & hash(temp);
	}
	
	protected static long hash(ArrayList<Object> key)
	{
		long hashCode = 1125899906842597L;
		for (Object e : key)
		{
			long eHash = 1;
			if (e instanceof Integer)
			{
				long i = ((Integer)e).longValue();
				// Spread out values
			    long scaled = i * LARGE_PRIME;

			    // Fill in the lower bits
			    eHash = scaled + LARGE_PRIME2;
			}
			else if (e instanceof Long)
			{
				long i = (Long)e;
				// Spread out values
			    long scaled = i * LARGE_PRIME;

			    // Fill in the lower bits
			    eHash = scaled + LARGE_PRIME2;
			}
			else if (e instanceof String)
			{
				String string = (String)e;
				  long h = 1125899906842597L; // prime
				  int len = string.length();

				  for (int i = 0; i < len; i++) 
				  {
					   h = 31*h + string.charAt(i);
				  }
				  eHash = h;
			}
			else if (e instanceof Double)
			{
				long i = Double.doubleToLongBits((Double)e);
				// Spread out values
			    long scaled = i * LARGE_PRIME;

			    // Fill in the lower bits
			    eHash = scaled + LARGE_PRIME2;
			}
			else if (e instanceof Date)
			{
				long i = ((Date)e).getTime();
				// Spread out values
			    long scaled = i * LARGE_PRIME;

			    // Fill in the lower bits
			    eHash = scaled + LARGE_PRIME2;
			}
			else
			{
				eHash = e.hashCode();
			}
			
		    hashCode = 31*hashCode + (e==null ? 0 : eHash);
		}
		return hashCode;
	}
}
