package com.exascale.tasks;

import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import com.exascale.managers.HRDBMSWorker;
import com.exascale.managers.MaintenanceManager;
import com.exascale.misc.Utils;
import com.exascale.optimizer.MetaData;
import com.exascale.tables.Transaction;
import com.exascale.threads.HRDBMSThread;

public class DeleteFilesTask extends Task
{
	private static void getConfirmation(final Socket sock) throws Exception
	{
		final InputStream in = sock.getInputStream();
		final byte[] inMsg = new byte[2];

		int count = 0;
		while (count < 2)
		{
			try
			{
				final int temp = in.read(inMsg, count, 2 - count);
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

		final String inStr = new String(inMsg, StandardCharsets.UTF_8);
		if (!inStr.equals("OK"))
		{
			in.close();
			throw new Exception();
		}

		try
		{
			in.close();
		}
		catch (final Exception e)
		{
		}
	}

	@Override
	public void run()
	{
		new DeleteFilesThread().start();
	}

	private class DeleteFilesThread extends HRDBMSThread
	{
		@Override
		public void run()
		{
			try
			{
				final long start = System.currentTimeMillis();
				final List<String> tables = new ArrayList<String>();
				final List<String> indexes = new ArrayList<String>();
				final long target = Long.parseLong(HRDBMSWorker.getHParms().getProperty("old_file_cleanup_target_days")) * 24 * 60 * 60 * 1000;
				String sql = "SELECT SCHEMA, TABNAME FROM SYS.TABLES";
				final Connection conn = DriverManager.getConnection("jdbc:hrdbms://localhost:" + HRDBMSWorker.getHParms().getProperty("port_number"));
				conn.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
				conn.setAutoCommit(false);
				final Statement stmt = conn.createStatement();
				ResultSet rs = stmt.executeQuery(sql);
				while (rs.next())
				{
					final String table = rs.getString(1) + "." + rs.getString(2);
					tables.add(table);
				}

				sendDeleteTables(tables);
				rs.close();
				conn.commit();

				sql = "SELECT A.SCHEMA, B.INDEXNAME FROM SYS.TABLES A, SYS.INDEXES B WHERE A.TABLEID = B.TABLEID";
				rs = stmt.executeQuery(sql);
				while (rs.next())
				{
					final String index = rs.getString(1) + "." + rs.getString(2);
					indexes.add(index);
				}

				sendDeleteIndexes(indexes);
				rs.close();
				conn.commit();
				conn.close();

				final long end = System.currentTimeMillis();
				final long elapsed = end - start;
				MaintenanceManager.schedule(DeleteFilesTask.this, -1, elapsed, end + target);
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.warn("DeleteFilesTask failed", e);
				final long target = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("old_file_cleanup_target_days")) * 24L * 60 * 60 * 1000;
				MaintenanceManager.schedule(DeleteFilesTask.this, System.currentTimeMillis() + target);
			}
		}

		private List<Object> makeTree(final List<Integer> nodes)
		{
			final int max = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("max_neighbor_nodes"));
			if (nodes.size() <= max)
			{
				final List<Object> retval = new ArrayList<Object>(nodes);
				return retval;
			}

			final List<Object> retval = new ArrayList<Object>();
			int i = 0;
			while (i < max)
			{
				retval.add(nodes.get(i));
				i++;
			}

			final int remaining = nodes.size() - i;
			final int perNode = remaining / max + 1;

			int j = 0;
			final int size = nodes.size();
			while (i < size)
			{
				final int first = (Integer)retval.get(j);
				retval.remove(j);
				final List<Integer> list = new ArrayList<Integer>(perNode + 1);
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

			if (((List<Integer>)retval.get(0)).size() <= max)
			{
				return retval;
			}

			// more than 2 tier
			i = 0;
			while (i < retval.size())
			{
				final List<Integer> list = (List<Integer>)retval.remove(i);
				retval.add(i, makeTree(list));
				i++;
			}

			return retval;
		}

		private void sendDeleteIndexes(final List<String> indexes) throws Exception
		{
			final Transaction tx = new Transaction(Transaction.ISOLATION_CS);

			final List<Integer> nodes = MetaData.getWorkerNodes();
			final List<Object> tree = makeTree(nodes);

			boolean allOK = true;
			final List<SendIndexThread> threads = new ArrayList<SendIndexThread>();
			for (final Object o : tree)
			{
				if (o instanceof Integer)
				{
					final List<Object> list = new ArrayList<Object>(1);
					list.add(o);
					final SendIndexThread thread = new SendIndexThread(list, tx, indexes);
					threads.add(thread);
				}
				else
				{
					final SendIndexThread thread = new SendIndexThread((List<Object>)o, tx, indexes);
					threads.add(thread);
				}
			}

			for (final SendIndexThread thread : threads)
			{
				thread.start();
			}

			for (final SendIndexThread thread : threads)
			{
				while (true)
				{
					try
					{
						thread.join();
						break;
					}
					catch (final InterruptedException e)
					{
					}
				}
				final boolean ok = thread.getOK();
				if (!ok)
				{
					allOK = false;
				}
			}

			tx.commit();

			if (!allOK)
			{
				throw new Exception();
			}
		}

		private void sendDeleteTables(final List<String> tables) throws Exception
		{
			final Transaction tx = new Transaction(Transaction.ISOLATION_CS);
			final List<Integer> nodes = MetaData.getWorkerNodes();
			final List<Object> tree = makeTree(nodes);

			boolean allOK = true;
			final List<SendTableThread> threads = new ArrayList<SendTableThread>();
			for (final Object o : tree)
			{
				if (o instanceof Integer)
				{
					final List<Object> list = new ArrayList<Object>(1);
					list.add(o);
					final SendTableThread thread = new SendTableThread(list, tx, tables);
					threads.add(thread);
				}
				else
				{
					final SendTableThread thread = new SendTableThread((List<Object>)o, tx, tables);
					threads.add(thread);
				}
			}

			for (final SendTableThread thread : threads)
			{
				thread.start();
			}

			for (final SendTableThread thread : threads)
			{
				while (true)
				{
					try
					{
						thread.join();
						break;
					}
					catch (final InterruptedException e)
					{
					}
				}
				final boolean ok = thread.getOK();
				if (!ok)
				{
					allOK = false;
				}
			}

			tx.commit();

			if (!allOK)
			{
				throw new Exception();
			}
		}
	}

	private class SendIndexThread extends HRDBMSThread
	{
		private final List<Object> tree;
		private final Transaction tx;
		private final List<String> indexes;
		private boolean ok = true;

		public SendIndexThread(final List<Object> tree, final Transaction tx, final List<String> indexes)
		{
			this.tree = tree;
			this.tx = tx;
			this.indexes = indexes;
		}

		public boolean getOK()
		{
			return ok;
		}

		@Override
		public void run()
		{
			Object obj = tree.get(0);
			while (obj instanceof ArrayList)
			{
				obj = ((List)obj).get(0);
			}

			Socket sock = null;
			try
			{
				// new MetaData();
				final String hostname = MetaData.getHostNameForNode((Integer)obj, tx);
				// sock = new Socket(hostname,
				// Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
				sock = new Socket();
				sock.setReceiveBufferSize(4194304);
				sock.setSendBufferSize(4194304);
				sock.connect(new InetSocketAddress(hostname, Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number"))));
				final OutputStream out = sock.getOutputStream();
				final byte[] outMsg = "DELFIIDX        ".getBytes(StandardCharsets.UTF_8);
				outMsg[8] = 0;
				outMsg[9] = 0;
				outMsg[10] = 0;
				outMsg[11] = 0;
				outMsg[12] = 0;
				outMsg[13] = 0;
				outMsg[14] = 0;
				outMsg[15] = 0;
				out.write(outMsg);
				final ObjectOutputStream objOut = new ObjectOutputStream(out);
				objOut.writeObject(Utils.convertToHosts(tree, tx));
				objOut.writeObject(indexes);
				objOut.flush();
				out.flush();
				getConfirmation(sock);
				objOut.close();
				out.close();
				sock.close();
			}
			catch (final Exception e)
			{
				try
				{
					if (sock != null)
					{
						sock.close();
					}
				}
				catch (final Exception f)
				{
				}
				ok = false;
			}
		}
	}

	private class SendTableThread extends HRDBMSThread
	{
		private final List<Object> tree;
		private final Transaction tx;
		private final List<String> tables;
		private boolean ok = true;

		public SendTableThread(final List<Object> tree, final Transaction tx, final List<String> tables)
		{
			this.tree = tree;
			this.tx = tx;
			this.tables = tables;
		}

		public boolean getOK()
		{
			return ok;
		}

		@Override
		public void run()
		{
			Object obj = tree.get(0);
			while (obj instanceof ArrayList)
			{
				obj = ((List)obj).get(0);
			}

			Socket sock = null;
			try
			{
				// new MetaData();
				final String hostname = MetaData.getHostNameForNode((Integer)obj, tx);
				// sock = new Socket(hostname,
				// Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
				sock = new Socket();
				sock.setReceiveBufferSize(4194304);
				sock.setSendBufferSize(4194304);
				sock.connect(new InetSocketAddress(hostname, Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number"))));
				final OutputStream out = sock.getOutputStream();
				final byte[] outMsg = "DELFITBL        ".getBytes(StandardCharsets.UTF_8);
				outMsg[8] = 0;
				outMsg[9] = 0;
				outMsg[10] = 0;
				outMsg[11] = 0;
				outMsg[12] = 0;
				outMsg[13] = 0;
				outMsg[14] = 0;
				outMsg[15] = 0;
				out.write(outMsg);
				final ObjectOutputStream objOut = new ObjectOutputStream(out);
				objOut.writeObject(Utils.convertToHosts(tree, tx));
				objOut.writeObject(tables);
				objOut.flush();
				out.flush();
				getConfirmation(sock);
				objOut.close();
				out.close();
				sock.close();
			}
			catch (final Exception e)
			{
				try
				{
					if (sock != null)
					{
						sock.close();
					}
				}
				catch (final Exception f)
				{
				}
				ok = false;
			}
		}
	}
}
