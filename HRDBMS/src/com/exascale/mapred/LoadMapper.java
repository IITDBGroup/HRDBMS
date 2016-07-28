package com.exascale.mapred;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.misc.MurmurHash;
import com.exascale.misc.MyDate;
import com.exascale.optimizer.MetaData;
import com.exascale.optimizer.MetaData.PartitionMetaData;

public class LoadMapper extends Mapper<LongWritable, Text, MyLongWritable, ALOWritable>
{
	private TreeMap<Integer, String> pos2Col;
	private HashMap<String, String> cols2Types;
	private PartitionMetaData pmd;
	private String schema;
	private String table;
	private final HashMap<String, Integer> cols2Pos = new HashMap<String, Integer>();
	private int numNodes;
	private String delimiter;
	private final ArrayList<String> types2 = new ArrayList<String>();
	private HashMap<Integer, Integer> pos2Length;
	private ArrayList<Integer> workerNodes;
	private ArrayList<Integer> coordNodes;
	private String portString;
	private int numDevices;
	private int numWorkers;

	public static int determineDevice(ArrayList<Object> row, PartitionMetaData pmeta, HashMap<String, Integer> cols2Pos, int numDevices) throws Exception
	{
		pmeta.getTable();
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
				catch (Exception e)
				{
					HRDBMSWorker.logger.debug("Looking for " + table + "." + col);
					HRDBMSWorker.logger.debug("Cols2Pos is " + cols2Pos);
					HRDBMSWorker.logger.debug("Row is " + row);
				}
			}

			long hash = 0x7FFFFFFFFFFFFFFFL & hash(partial);
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
			final int size = ranges.size();
			while (i < size)
			{
				if (((Comparable)obj).compareTo(ranges.get(i)) <= 0)
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

	private static long hash(Object key) throws Exception
	{
		long eHash;
		if (key == null)
		{
			eHash = 0;
		}
		else
		{
			if (key instanceof ArrayList)
			{
				byte[] data = toBytesForHash((ArrayList<Object>)key);
				eHash = MurmurHash.hash64(data, data.length);
			}
			else
			{
				byte[] data = key.toString().getBytes(StandardCharsets.UTF_8);
				eHash = MurmurHash.hash64(data, data.length);
			}
		}

		return eHash;
	}
	
	private static byte[] toBytesForHash(ArrayList<Object> key)
	{
		StringBuilder sb = new StringBuilder();
		for (Object o : key)
		{
			if (o instanceof Double)
			{
				DecimalFormat df = new DecimalFormat("0", DecimalFormatSymbols.getInstance(Locale.ENGLISH));
				df.setMaximumFractionDigits(340); //340 = DecimalFormat.DOUBLE_FRACTION_DIGITS

				sb.append(df.format((Double)o));
				sb.append((char)0);
			}
			else if (o instanceof Number)
			{
				sb.append(o);
				sb.append((char)0);
			}
			else
			{
				sb.append(o.toString());
				sb.append((char)0);
			}
		}
		
		final int z = sb.length();
		byte[] retval = new byte[z];
		int i = 0;
		while (i < z)
		{
			retval[i] = (byte)sb.charAt(i);
			i++;
		}
		
		return retval;
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

	private static final byte[] toBytes(Object v) throws Exception
	{
		ArrayList<byte[]> bytes = null;
		ArrayList<Object> val;
		if (v instanceof ArrayList)
		{
			val = (ArrayList<Object>)v;
		}
		else
		{
			final byte[] retval = new byte[9];
			retval[0] = 0;
			retval[1] = 0;
			retval[2] = 0;
			retval[3] = 5;
			retval[4] = 0;
			retval[5] = 0;
			retval[6] = 0;
			retval[7] = 1;
			retval[8] = 5;
			return retval;
		}

		int size = val.size() + 8;
		final byte[] header = new byte[size];
		int i = 8;
		for (final Object o : val)
		{
			if (o instanceof Long)
			{
				header[i] = (byte)0;
				size += 8;
			}
			else if (o instanceof Integer)
			{
				header[i] = (byte)1;
				size += 4;
			}
			else if (o instanceof Double)
			{
				header[i] = (byte)2;
				size += 8;
			}
			else if (o instanceof MyDate)
			{
				header[i] = (byte)3;
				size += 4;
			}
			else if (o instanceof String)
			{
				header[i] = (byte)4;
				byte[] b = ((String)o).getBytes(StandardCharsets.UTF_8);
				size += (4 + b.length);

				if (bytes == null)
				{
					bytes = new ArrayList<byte[]>();
					bytes.add(b);
				}
				else
				{
					bytes.add(b);
				}
			}
			// else if (o instanceof AtomicLong)
			// {
			// header[i] = (byte)6;
			// size += 8;
			// }
			// else if (o instanceof AtomicBigDecimal)
			// {
			// header[i] = (byte)7;
			// size += 8;
			// }
			else if (o instanceof ArrayList)
			{
				if (((ArrayList)o).size() != 0)
				{
					Exception e = new Exception("Non-zero size ArrayList in toBytes()");
					HRDBMSWorker.logger.error("Non-zero size ArrayList in toBytes()", e);
					throw e;
				}
				header[i] = (byte)8;
			}
			else
			{
				HRDBMSWorker.logger.error("Unknown type " + o.getClass() + " in toBytes()");
				HRDBMSWorker.logger.error(o);
				throw new Exception("Unknown type " + o.getClass() + " in toBytes()");
			}

			i++;
		}

		final byte[] retval = new byte[size];
		// System.out.println("In toBytes(), row has " + val.size() +
		// " columns, object occupies " + size + " bytes");
		System.arraycopy(header, 0, retval, 0, header.length);
		i = 8;
		final ByteBuffer retvalBB = ByteBuffer.wrap(retval);
		retvalBB.putInt(size - 4);
		retvalBB.putInt(val.size());
		retvalBB.position(header.length);
		int x = 0;
		for (final Object o : val)
		{
			if (retval[i] == 0)
			{
				retvalBB.putLong((Long)o);
			}
			else if (retval[i] == 1)
			{
				retvalBB.putInt((Integer)o);
			}
			else if (retval[i] == 2)
			{
				retvalBB.putDouble((Double)o);
			}
			else if (retval[i] == 3)
			{
				retvalBB.putInt(((MyDate)o).getTime());
			}
			else if (retval[i] == 4)
			{
				byte[] temp = bytes.get(x);
				x++;
				retvalBB.putInt(temp.length);
				retvalBB.put(temp);
			}
			// else if (retval[i] == 6)
			// {
			// retvalBB.putLong(((AtomicLong)o).get());
			// }
			// else if (retval[i] == 7)
			// {
			// retvalBB.putDouble(((AtomicBigDecimal)o).get().doubleValue());
			// }
			else if (retval[i] == 8)
			{
			}

			i++;
		}

		return retval;
	}

	@Override
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
		catch (Exception e)
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

	@Override
	public void setup(Context context) throws IOException
	{
		// get pos2Col, cols2Types, numNodes, pos2Length, delimiter, and
		// PartitionMetaData based on schema and table
		String jobName = context.getJobName();
		portString = context.getConfiguration().get("hrdbms.port");
		numDevices = Integer.parseInt(context.getConfiguration().get("hrdbms.num.devices"));
		numWorkers = Integer.parseInt(context.getConfiguration().get("hrdbms.num.workers"));
		MyLongWritable.setup(numWorkers, numDevices);
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

	private void getMetaData(String schema, String table) throws IOException
	{
		// Socket sock = new Socket("localhost", Integer.parseInt(portString));
		Socket sock = new Socket();
		sock.setReceiveBufferSize(4194304);
		sock.setSendBufferSize(4194304);
		sock.connect(new InetSocketAddress("localhost", Integer.parseInt(portString)));
		OutputStream out = sock.getOutputStream();
		byte[] outMsg = "GETLDMD         ".getBytes(StandardCharsets.UTF_8);
		outMsg[8] = 0;
		outMsg[9] = 0;
		outMsg[10] = 0;
		outMsg[11] = 0;
		outMsg[12] = 0;
		outMsg[13] = 0;
		outMsg[14] = 0;
		outMsg[15] = 0;
		out.write(outMsg);
		byte[] schemaBytes = schema.getBytes(StandardCharsets.UTF_8);
		byte[] tableBytes = table.getBytes(StandardCharsets.UTF_8);
		out.write(intToBytes(schemaBytes.length));
		out.write(schemaBytes);
		out.write(intToBytes(tableBytes.length));
		out.write(tableBytes);
		out.flush();

		InputStream in = sock.getInputStream();
		// get pos2Col, cols2Types, numNodes, pos2Length, delimiter, and
		// PartitionMetaData based on schema and table
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
		catch (ClassNotFoundException e)
		{
			throw new IOException(e.getMessage());
		}

		in.close();
		out.close();
		sock.close();
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
}
