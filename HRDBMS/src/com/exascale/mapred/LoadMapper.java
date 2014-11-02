package com.exascale.mapred;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import com.exascale.compression.CompressedSocket;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.misc.DataEndMarker;
import com.exascale.misc.MurmurHash;
import com.exascale.misc.MyDate;
import com.exascale.optimizer.MetaData;
import com.exascale.optimizer.MetaData.PartitionMetaData;
import com.exascale.tables.Transaction;

public class LoadMapper extends Mapper<LongWritable, Text, MyLongWritable, ALOWritable>
{
	private TreeMap<Integer, String> pos2Col;
	private HashMap<String, String> cols2Types;
	private PartitionMetaData pmd;
	private String schema;
	private String table;
	private HashMap<String, Integer> cols2Pos = new HashMap<String, Integer>();
	private int numNodes;
	private String delimiter;
	private ArrayList<String> types2 = new ArrayList<String>();
	private HashMap<Integer, Integer> pos2Length;
	private ArrayList<Integer> workerNodes;
	private ArrayList<Integer> coordNodes;
	private String portString;
	private int numDevices;
	
	public void setup(Context context) throws IOException
	{
		//get pos2Col, cols2Types, numNodes, pos2Length, delimiter, and PartitionMetaData based on schema and table
		String jobName = context.getJobName();
		portString = context.getConfiguration().get("hrdbms.port");
		numDevices = Integer.parseInt(context.getConfiguration().get("hrdbms.num.devices"));
		String tableName = jobName.substring(5);
		schema = tableName.substring(0, tableName.indexOf('.'));
		table = tableName.substring(tableName.indexOf('.') + 1);
		getMetaData(schema, table);
		for (Map.Entry entry : pos2Col.entrySet())
		{
			cols2Pos.put((String)entry.getValue(), (Integer)entry.getKey());
		}
		
		for (String col : pos2Col.values())
		{
			types2.add(cols2Types.get(col));
		}
	}
	
	public void map(LongWritable key, Text value, Context context) throws IOException
	{
		try
		{
			ArrayList<Object> row = parseValue(value);
			
			for (Map.Entry entry : pos2Length.entrySet())
			{
				if (((String)row.get((Integer)entry.getKey())).length() > (Integer)entry.getValue())
				{
					throw new IOException("Value for column is too long in row " + row);
				}
			}
		
			ArrayList<Integer> nodes = MetaData.determineNodeNoLookups(schema, table, row, pmd, cols2Pos, numNodes, workerNodes, coordNodes);
			int device = determineDevice(row, pmd, cols2Pos, numDevices);
			for (Integer node : nodes)
			{
				ALOWritable aloW = new ALOWritable();
				aloW.set(row);
				long key2 = (((long)node) << 32) + device;
				context.write(new MyLongWritable(key2), aloW);
			}
		}
		catch(Exception e)
		{
			if (e instanceof IOException)
			{
				throw (IOException)e;
			}
			else
			{
				throw new IOException(e.getMessage());
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
	
	public static int determineDevice(ArrayList<Object> row, PartitionMetaData pmeta, HashMap<String, Integer> cols2Pos, int numDevices)
	{
		String schema = pmeta.getTable();
		String table = pmeta.getTable();
		if (pmeta.isSingleDeviceSet())
		{
			return pmeta.getSingleDevice();
		}
		else if (pmeta.deviceIsHash())
		{
			ArrayList<String> devHash = pmeta.getDeviceHash();
			ArrayList<Object> partial = new ArrayList<Object>();
			for (String col : devHash)
			{
				try
				{
					partial.add(row.get(cols2Pos.get(table + "." + col)));
				}
				catch(Exception e)
				{
					HRDBMSWorker.logger.debug("Looking for " + table + "." + col);
					HRDBMSWorker.logger.debug("Cols2Pos is " + cols2Pos);
					HRDBMSWorker.logger.debug("Row is " + row);
				}
			}
			
			long hash = 0x0EFFFFFFFFFFFFFFL & hash(partial);
			if (pmeta.allDevices())
			{
				return (int)(hash % numDevices);
			}
			else
			{
				return pmeta.deviceSet().get((int)(hash % pmeta.deviceSet().size()));
			}
		}
		else
		{
			String col = table + "." + pmeta.getDeviceRangeCol();
			Object obj = row.get(cols2Pos.get(col));
			ArrayList<Object> ranges = pmeta.getDeviceRanges();
			int i = 0;
			while (i < ranges.size())
			{
				if (((Comparable)obj).compareTo((Comparable)ranges.get(i)) <= 0)
				{
					if (pmeta.allDevices())
					{
						return i;
					}
					else
					{
						return pmeta.deviceSet().get(i);
					}
				}
				
				i++;
			}
			
			if (pmeta.allNodes())
			{
				return i;
			}
			else
			{
				return pmeta.deviceSet().get(i);
			}
		}
	}
	
	private ArrayList<Object> parseValue(Text in) throws Exception
	{
		String line = in.toString();
		ArrayList<Object> row = new ArrayList<Object>();
		StringTokenizer tokens = new StringTokenizer(line, delimiter, false);
		int i = 0;
		while (tokens.hasMoreTokens())
		{
			String token = tokens.nextToken();
			String type = types2.get(i);
			i++;
			
			if (type.equals("CHAR"))
			{
				row.add(token);
			}
			else if (type.equals("INT"))
			{
				row.add(Integer.parseInt(token));
			}
			else if (type.equals("LONG"))
			{
				row.add(Long.parseLong(token));
			}
			else if (type.equals("FLOAT"))
			{
				row.add(Double.parseDouble(token));
			}
			else if (type.equals("DATE"))
			{
				row.add(new MyDate(Integer.parseInt(token.substring(0, 4)), Integer.parseInt(token.substring(5, 7)), Integer.parseInt(token.substring(8, 10))));
			}
		}
		
		return row;
	}
	
	private void getMetaData(String schema, String table) throws IOException
	{
		CompressedSocket sock = new CompressedSocket("localhost", Integer.parseInt(portString));
		OutputStream out = sock.getOutputStream();
		byte[] outMsg = "GETLDMD         ".getBytes("UTF-8");
		outMsg[8] = 0;
		outMsg[9] = 0;
		outMsg[10] = 0;
		outMsg[11] = 0;
		outMsg[12] = 0;
		outMsg[13] = 0;
		outMsg[14] = 0;
		outMsg[15] = 0;
		out.write(outMsg);
		byte[] schemaBytes = schema.getBytes("UTF-8");
		byte[] tableBytes = table.getBytes("UTF-8");
		out.write(intToBytes(schemaBytes.length));
		out.write(schemaBytes);
		out.write(intToBytes(tableBytes.length));
		out.write(tableBytes);
		out.flush();
		
		InputStream in = sock.getInputStream();
		//get pos2Col, cols2Types, numNodes, pos2Length, delimiter, and PartitionMetaData based on schema and table
		try
		{
			ObjectInputStream objIn = new ObjectInputStream(in);
			numNodes = (Integer)objIn.readObject();
			delimiter = (String)objIn.readObject();
			pos2Col = (TreeMap<Integer, String>)objIn.readObject();
			cols2Types = (HashMap<String, String>)objIn.readObject();
			pos2Length = (HashMap<Integer, Integer>)objIn.readObject();
			pmd = (PartitionMetaData)objIn.readObject();
			workerNodes = (ArrayList<Integer>)objIn.readObject();
			coordNodes = (ArrayList<Integer>)objIn.readObject();
		}
		catch(ClassNotFoundException e)
		{
			throw new IOException(e.getMessage());
		}
		
		in.close();
		out.close();
		sock.close();
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
}
