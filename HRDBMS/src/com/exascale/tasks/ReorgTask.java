package com.exascale.tasks;

import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeMap;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.managers.MaintenanceManager;
import com.exascale.optimizer.Index;
import com.exascale.optimizer.MetaData;
import com.exascale.tables.Transaction;
import com.exascale.threads.HRDBMSThread;

public class ReorgTask extends Task
{
	private final String table;

	public ReorgTask(String table)
	{
		this.table = table;
	}

	private static void getConfirmation(Socket sock) throws Exception
	{
		InputStream in = sock.getInputStream();
		byte[] inMsg = new byte[2];

		int count = 0;
		while (count < 2)
		{
			try
			{
				int temp = in.read(inMsg, count, 2 - count);
				if (temp == -1)
				{
					in.close();
					throw new Exception();
				}
				else
				{
					count += temp;
				}
			}
			catch (final Exception e)
			{
				in.close();
				throw new Exception();
			}
		}

		String inStr = new String(inMsg, StandardCharsets.UTF_8);
		if (!inStr.equals("OK"))
		{
			in.close();
			throw new Exception();
		}

		try
		{
			in.close();
		}
		catch (Exception e)
		{
		}
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

	private static byte[] longToBytes(long val)
	{
		final byte[] buff = new byte[8];
		buff[0] = (byte)(val >> 56);
		buff[1] = (byte)((val & 0x00FF000000000000L) >> 48);
		buff[2] = (byte)((val & 0x0000FF0000000000L) >> 40);
		buff[3] = (byte)((val & 0x000000FF00000000L) >> 32);
		buff[4] = (byte)((val & 0x00000000FF000000L) >> 24);
		buff[5] = (byte)((val & 0x0000000000FF0000L) >> 16);
		buff[6] = (byte)((val & 0x000000000000FF00L) >> 8);
		buff[7] = (byte)((val & 0x00000000000000FFL));
		return buff;
	}

	private static ArrayList<Object> makeTree(ArrayList<Integer> nodes)
	{
		int max = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("max_neighbor_nodes"));
		if (nodes.size() <= max)
		{
			ArrayList<Object> retval = new ArrayList<Object>(nodes);
			return retval;
		}

		ArrayList<Object> retval = new ArrayList<Object>();
		int i = 0;
		while (i < max)
		{
			retval.add(nodes.get(i));
			i++;
		}

		int remaining = nodes.size() - i;
		int perNode = remaining / max + 1;

		int j = 0;
		final int size = nodes.size();
		while (i < size)
		{
			int first = (Integer)retval.get(j);
			retval.remove(j);
			ArrayList<Integer> list = new ArrayList<Integer>(perNode + 1);
			list.add(first);
			int k = 0;
			while (k < perNode && i < size)
			{
				list.add(nodes.get(i));
				i++;
				k++;
			}

			retval.add(j, list);
			j++;
		}

		if (((ArrayList<Integer>)retval.get(0)).size() <= max)
		{
			return retval;
		}

		// more than 2 tier
		i = 0;
		while (i < retval.size())
		{
			ArrayList<Integer> list = (ArrayList<Integer>)retval.remove(i);
			retval.add(i, makeTree(list));
			i++;
		}

		return retval;
	}

	private static byte[] stringToBytes(String string)
	{
		byte[] data = null;
		try
		{
			data = string.getBytes(StandardCharsets.UTF_8);
		}
		catch (Exception e)
		{
		}
		byte[] len = intToBytes(data.length);
		byte[] retval = new byte[data.length + len.length];
		System.arraycopy(len, 0, retval, 0, len.length);
		System.arraycopy(data, 0, retval, len.length, data.length);
		return retval;
	}

	@Override
	public void run()
	{
		new InitReorgThread().start();
	}

	private class InitReorgThread extends HRDBMSThread
	{
		@Override
		public void run()
		{
			try
			{
				long start = System.currentTimeMillis();
				MetaData meta = new MetaData();
				Transaction tx = new Transaction(Transaction.ISOLATION_CS);
				ArrayList<Index> indexes = meta.getIndexesForTable(table.substring(0, table.indexOf('.')), table.substring(table.indexOf('.') + 1), tx);
				ArrayList<Integer> nodes = meta.getNodesForTable(table.substring(0, table.indexOf('.')), table.substring(table.indexOf('.') + 1), tx);
				HashMap<String, String> cols2Types = meta.getCols2TypesForTable(table.substring(0, table.indexOf('.')), table.substring(table.indexOf('.') + 1), tx);
				TreeMap<Integer, String> pos2Col = meta.getPos2ColForTable(table.substring(0, table.indexOf('.')), table.substring(table.indexOf('.') + 1), tx);

				ArrayList<Object> tree = makeTree(nodes);
				ArrayList<Boolean> uniques = meta.getUnique(table.substring(0, table.indexOf('.')), table.substring(table.indexOf('.') + 1), tx);

				tx.commit();
				tx = new Transaction(Transaction.ISOLATION_CS);
				sendReorgs(tree, table.substring(0, table.indexOf('.')), table.substring(table.indexOf('.') + 1), indexes, tx, cols2Types, pos2Col, uniques);
				tx.commit();

				long end = System.currentTimeMillis();

				// reschedule myself
				MaintenanceManager.schedule(ReorgTask.this, -1, end - start, end + Long.parseLong(HRDBMSWorker.getHParms().getProperty("reorg_refresh_target_days")) * 24 * 60 * 60 * 1000);
			}
			catch (Exception e)
			{
				HRDBMSWorker.logger.warn("Error running REORG on " + table, e);
				MaintenanceManager.reorgFailed.put(table, table);
			}
		}

		private void sendReorgs(ArrayList<Object> tree, String schema, String table, ArrayList<Index> indexes, Transaction tx, HashMap<String, String> cols2Types, TreeMap<Integer, String> pos2Col, ArrayList<Boolean> uniques) throws Exception
		{
			boolean allOK = true;
			ArrayList<SendReorgThread> threads = new ArrayList<SendReorgThread>();
			for (Object o : tree)
			{
				if (o instanceof Integer)
				{
					ArrayList<Object> list = new ArrayList<Object>(1);
					list.add(o);
					SendReorgThread thread = new SendReorgThread(list, schema, table, indexes, tx, cols2Types, pos2Col, uniques);
					threads.add(thread);
				}
				else
				{
					SendReorgThread thread = new SendReorgThread((ArrayList<Object>)o, schema, table, indexes, tx, cols2Types, pos2Col, uniques);
					threads.add(thread);
				}
			}

			for (SendReorgThread thread : threads)
			{
				thread.start();
			}

			for (SendReorgThread thread : threads)
			{
				while (true)
				{
					try
					{
						thread.join();
						break;
					}
					catch (InterruptedException e)
					{
					}
				}
				boolean ok = thread.getOK();
				if (!ok)
				{
					allOK = false;
				}
			}

			if (!allOK)
			{
				throw new Exception("REORG failed");
			}
		}
	}

	private static class SendReorgThread extends HRDBMSThread
	{
		private final ArrayList<Object> tree;
		private final String schema;
		private final String table;
		private final ArrayList<Index> indexes;
		private final Transaction tx;
		private boolean ok;
		private final HashMap<String, String> cols2Types;
		private final TreeMap<Integer, String> pos2Col;
		private final ArrayList<Boolean> uniques;

		public SendReorgThread(ArrayList<Object> tree, String schema, String table, ArrayList<Index> indexes, Transaction tx, HashMap<String, String> cols2Types, TreeMap<Integer, String> pos2Col, ArrayList<Boolean> uniques)
		{
			this.tree = tree;
			this.schema = schema;
			this.table = table;
			this.indexes = indexes;
			this.tx = tx;
			this.cols2Types = cols2Types;
			this.pos2Col = pos2Col;
			this.uniques = uniques;
		}

		private static ArrayList<Object> convertToHosts(ArrayList<Object> tree, Transaction tx) throws Exception
		{
			ArrayList<Object> retval = new ArrayList<Object>();
			int i = 0;
			final int size = tree.size();
			while (i < size)
			{
				Object obj = tree.get(i);
				if (obj instanceof Integer)
				{
					retval.add(new MetaData().getHostNameForNode((Integer)obj, tx));
				}
				else
				{
					retval.add(convertToHosts((ArrayList<Object>)obj, tx));
				}

				i++;
			}

			return retval;
		}

		private static boolean sendReorg(ArrayList<Object> tree, String schema, String table, ArrayList<Index> indexes, Transaction tx, HashMap<String, String> cols2Types, TreeMap<Integer, String> pos2Col, ArrayList<Boolean> uniques)
		{
			Object obj = tree.get(0);
			while (obj instanceof ArrayList)
			{
				obj = ((ArrayList)obj).get(0);
			}

			Socket sock = null;
			try
			{
				String hostname = new MetaData().getHostNameForNode((Integer)obj, tx);
				// sock = new Socket(hostname,
				// Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
				sock = new Socket();
				sock.setReceiveBufferSize(4194304);
				sock.setSendBufferSize(4194304);
				sock.connect(new InetSocketAddress(hostname, Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number"))));
				OutputStream out = sock.getOutputStream();
				byte[] outMsg = "REORG           ".getBytes(StandardCharsets.UTF_8);
				outMsg[8] = 0;
				outMsg[9] = 0;
				outMsg[10] = 0;
				outMsg[11] = 0;
				outMsg[12] = 0;
				outMsg[13] = 0;
				outMsg[14] = 0;
				outMsg[15] = 0;
				out.write(outMsg);
				out.write(stringToBytes(schema));
				out.write(stringToBytes(table));
				byte[] txBytes = longToBytes(tx.number());
				out.write(txBytes);
				ObjectOutputStream objOut = new ObjectOutputStream(out);
				objOut.writeObject(convertToHosts(tree, tx));
				objOut.writeObject(indexes);
				objOut.writeObject(cols2Types);
				objOut.writeObject(pos2Col);
				objOut.writeObject(uniques);
				objOut.flush();
				out.flush();
				getConfirmation(sock);
				objOut.close();
				sock.close();
				return true;
			}
			catch (Exception e)
			{
				try
				{
					if (sock != null)
					{
						sock.close();
					}
				}
				catch (Exception f)
				{
				}
				return false;
			}
		}

		public boolean getOK()
		{
			return ok;
		}

		@Override
		public void run()
		{
			ok = sendReorg(tree, schema, table, indexes, tx, cols2Types, pos2Col, uniques);
		}
	}
}
