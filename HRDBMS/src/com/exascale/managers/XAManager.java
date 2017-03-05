package com.exascale.managers;

import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import com.exascale.exceptions.LockAbortException;
import com.exascale.filesystem.Block;
import com.exascale.logging.LogRec;
import com.exascale.logging.PrepareLogRec;
import com.exascale.logging.ReadyLogRec;
import com.exascale.logging.XAAbortLogRec;
import com.exascale.logging.XACommitLogRec;
import com.exascale.misc.VHJOMultiHashMap;
import com.exascale.optimizer.CreateIndexOperator;
import com.exascale.optimizer.DeleteOperator;
import com.exascale.optimizer.InsertOperator;
import com.exascale.optimizer.LoadOperator;
import com.exascale.optimizer.MassDeleteOperator;
import com.exascale.optimizer.MetaData;
import com.exascale.optimizer.NetworkReceiveOperator;
import com.exascale.optimizer.NetworkSendOperator;
import com.exascale.optimizer.Operator;
import com.exascale.optimizer.Phase1;
import com.exascale.optimizer.Phase2;
import com.exascale.optimizer.Phase3;
import com.exascale.optimizer.Phase4;
import com.exascale.optimizer.Phase5;
import com.exascale.optimizer.RootOperator;
import com.exascale.optimizer.RoutingOperator;
import com.exascale.optimizer.SQLParser;
import com.exascale.optimizer.UpdateOperator;
import com.exascale.tables.Plan;
import com.exascale.tables.SQL;
import com.exascale.tables.Transaction;
import com.exascale.threads.ConnectionWorker;
import com.exascale.threads.HRDBMSThread;
import com.exascale.threads.XAWorker;

public class XAManager extends HRDBMSThread
{
	public static VHJOMultiHashMap<Transaction, Plan> txs = new VHJOMultiHashMap<Transaction, Plan>();
	public static volatile boolean rP1 = false;
	public static volatile boolean rP2 = false;
	public static BlockingQueue<Object> in = new LinkedBlockingQueue<Object>();
	public static int PORT_NUMBER = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number"));
	public static int MAX_NEIGHBORS = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("max_neighbor_nodes"));
	public static String LOG_DIR = HRDBMSWorker.getHParms().getProperty("log_dir");

	public XAManager()
	{
		HRDBMSWorker.logger.info("Starting initialization of the XA Manager.");
		this.setWait(false);
		this.description = "XA Manager";
	}

	public static boolean askXAManager(final ReadyLogRec xa)
	{
		try
		{
			Socket sock = null;
			int i = 0;
			while (i < 50)
			{
				try
				{
					// sock = new Socket(xa.getHost(),
					// Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
					sock = new Socket();
					sock.setReceiveBufferSize(4194304);
					sock.setSendBufferSize(4194304);
					sock.connect(new InetSocketAddress(xa.getHost(), PORT_NUMBER));
					break;
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.debug(xa.getHost(), e);
					Thread.sleep(5000);
					i++;
				}
			}

			if (sock == null)
			{
				throw new Exception("Unable to connect to " + xa.getHost() + " after 50 tries");
			}
			final OutputStream out = sock.getOutputStream();
			final byte[] outMsg = "CHECKTX         ".getBytes(StandardCharsets.UTF_8);
			outMsg[8] = 0;
			outMsg[9] = 0;
			outMsg[10] = 0;
			outMsg[11] = 0;
			outMsg[12] = 0;
			outMsg[13] = 0;
			outMsg[14] = 0;
			outMsg[15] = 0;
			out.write(outMsg);
			out.write(longToBytes(xa.txnum()));
			out.flush();

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
						sock.close();
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
					sock.close();
					throw new Exception();
				}
			}

			final String inStr = new String(inMsg, StandardCharsets.UTF_8);
			if (!inStr.equals("OK"))
			{
				in.close();
				out.close();
				sock.close();
				return false;
			}

			try
			{
				in.close();
			}
			catch (final Exception e)
			{
			}

			out.close();
			sock.close();
			return true;
		}
		catch (final Exception e)
		{
			// TODO return askReplicas(xa);
			HRDBMSWorker.logger.fatal("ASK REPLICAS", e);
			System.exit(1);
			return false;
		}
	}

	public static void commit(final Transaction tx) throws Exception
	{
		final List<Plan> ps = txs.get(tx);
		if (ps == null || ps.size() == 0)
		{
			throw new Exception("The XAManager does not own this transaction");
		}

		ArrayList<Integer> nodes = new ArrayList<Integer>();
		for (final Plan p : ps)
		{
			nodes.addAll(getNodes(p, tx));
		}

		nodes = consolidateNodes(nodes);
		ArrayList<Object> tree = makeTree(nodes);
		boolean checkpoint = false;
		ArrayList<Integer> checkpointNodes = new ArrayList<Integer>();
		for (final Plan p : ps)
		{
			if (containsLoad(p))
			{
				checkpoint = true;
				checkpointNodes.addAll(getLoadNodes(p, tx));
			}
			else if (containsPop(p))
			{
				checkpoint = true;
				checkpointNodes.addAll(getPopNodes(p, tx));
			}
		}
		sendCommits(tree, tx);

		if (checkpoint)
		{
			checkpointNodes = consolidateNodes(checkpointNodes);
			tree = makeTree(checkpointNodes);
			sendCheckpoints(tree, tx);
		}
	}

	public static XAWorker executeAuthorizedUpdate(final String sql, final Transaction tx) throws Exception
	{
		final SQL sql3 = new SQL(sql);
		final String sql2 = sql3.toString();

		if (sql2.startsWith("SELECT") || sql2.startsWith("WITH"))
		{
			throw new Exception("SELECT statement is not allowed");
		}

		final SQLParser parse = new SQLParser(sql3, null, tx);
		parse.authorize();
		final ArrayList<Operator> array = parse.parse();
		String table = null;
		final Operator op = array.get(0);
		if (op instanceof InsertOperator)
		{
			table = ((InsertOperator)op).getTable();
		}
		else if (op instanceof UpdateOperator)
		{
			table = ((UpdateOperator)op).getTable();
		}
		else if (op instanceof DeleteOperator)
		{
			table = ((DeleteOperator)op).getTable();
		}
		else
		{
			throw new Exception("Unexpected operator in executeAuthorizedUpdate()");
		}

		try
		{
			LockManager.xLock(new Block(table, 0), tx.number());
		}
		catch (final LockAbortException e)
		{
		}

		final Plan plan = new Plan(false, array);
		txs.multiPut(tx, plan);
		return new XAWorker(plan, tx, false);
	}

	public static XAWorker executeCatalogQuery(final Plan p, final Transaction tx) throws Exception
	{
		txs.multiPut(tx, p);
		return new XAWorker(p, tx, true);
	}

	public static XAWorker executeQuery(final String sql, final Transaction tx, final ConnectionWorker conn) throws Exception
	{
		return executeQuery(sql, tx, conn, null);
	}

	/** Main logic to run through the query planning and execution of a select statement */
	public static XAWorker executeQuery(final String sql, final Transaction tx, final ConnectionWorker conn, final Long sPer) throws Exception
	{
		final SQL sql3 = new SQL(sql);
		final String sql2 = sql3.toString();
		if (!(sql2.startsWith("SELECT") || sql2.startsWith("WITH")))
		{
			throw new Exception("Not a select statement");
		}
		// HRDBMSWorker.logger.debug("About to check plan cache");
		Plan plan = PlanCacheManager.checkPlanCache(sql2);

		if (plan == null)
		{
			try
			{
				// HRDBMSWorker.logger.debug("Did not find plan in cache");
				final SQLParser parse = new SQLParser(sql3, conn, tx);
				// HRDBMSWorker.logger.debug("Created SQL parser");
				final ArrayList<Operator> array = parse.parse();
				final Operator op = array.get(0);
				// HRDBMSWorker.logger.debug("Parsing completed");
				final Phase1 p1 = new Phase1((RootOperator)op, tx);
				p1.optimize();
				// HRDBMSWorker.logger.debug("Phase 1 completed");
				new Phase2((RootOperator)op, tx).optimize();
				// HRDBMSWorker.logger.debug("Phase 2 completed");
				new Phase3((RootOperator)op, tx).optimize();
				// HRDBMSWorker.logger.debug("Phase 3 completed");
				new Phase4((RootOperator)op, tx).optimize();
				// HRDBMSWorker.logger.debug("Phase 4 completed");
				new Phase5((RootOperator)op, tx, p1.likelihoodCache).optimize();
				// HRDBMSWorker.logger.debug("Phase 5 completed");
				// Phase1.printTree(op, 0); // DEBUG
				plan = new Plan(false, array);

				// if (parse.doesNotUseCurrentSchema())
				// {
				// PlanCacheManager.addPlan(sql2, new Plan(plan));
				// }
			}
			catch (final Throwable e)
			{
				HRDBMSWorker.logger.debug("Error in executeQuery", e);
				throw e;
			}
		}
		else
		{
			// HRDBMSWorker.logger.debug("Did find plan in cache");
		}

		txs.multiPut(tx, plan);
		if(sPer != null) {
			plan.setSample(sPer);
		}
		return new XAWorker(plan, tx, true);
	}

	public static XAWorker executeUpdate(final String sql, final Transaction tx, final ConnectionWorker conn) throws Exception
	{
		final SQL sql3 = new SQL(sql);
		String sql2 = sql3.toString();
		if (sql2.startsWith("SELECT") || sql2.startsWith("WITH"))
		{
			throw new Exception("SELECT statement is not allowed");
		}

		SQLParser parse = null;
		if (sql2.startsWith("CREATE KEY/VALUE PAIR"))
		{
			sql2 = rewriteKeyValue(sql2);
			parse = new SQLParser(sql2, conn, tx);
		}
		else
		{
			parse = new SQLParser(sql3, conn, tx);
		}

		final ArrayList<Operator> array = parse.parse();
		final Plan plan = new Plan(false, array);
		txs.multiPut(tx, plan);
		return new XAWorker(plan, tx, false);
	}

	public static XAWorker executeUpdateInline(final String sql, final Transaction tx, final ConnectionWorker conn) throws Exception
	{
		final SQL sql3 = new SQL(sql);
		String sql2 = sql3.toString();
		if (sql2.startsWith("SELECT") || sql2.startsWith("WITH"))
		{
			throw new Exception("SELECT statement is not allowed");
		}

		SQLParser parse = null;
		if (sql2.startsWith("CREATE KEY/VALUE PAIR"))
		{
			sql2 = rewriteKeyValue(sql2);
			parse = new SQLParser(sql2, conn, tx);
		}
		else
		{
			parse = new SQLParser(sql3, conn, tx);
		}

		final ArrayList<Operator> array = parse.parse();
		final Plan plan = new Plan(false, array);
		txs.multiPut(tx, plan);
		final XAWorker retval = new XAWorker(plan, tx, false);
		// if (sql2.startsWith("INSERT "))
		// {
		// if (InsertOperator.allOwnersPresent(tx,
		// ((InsertOperator)op).getSchema(), ((InsertOperator)op).getTable()))
		// {
		// if (containsSingleThreadedExtend(op))
		// {
		// retval.setRunInline();
		// }
		// }
		// }
		return retval;
	}

	// private static boolean containsSingleThreadedExtend(Operator op)
	// {
	// if (op instanceof ExtendOperator)
	// {
	// return ((ExtendOperator)op).isSingleThreaded();
	// }
	//
	// if (op.children().size() > 0)
	// {
	// return containsSingleThreadedExtend(op.children().get(0));
	// }
	//
	// return false;
	// }

	public static void phase2(final long txnum, final ArrayList<Integer> nodes)
	{
		final ArrayList<Object> tree = makeTree(nodes);
		sendPhase2s(tree, new Transaction(txnum));
	}

	public static void rollback(final long txnum, final ArrayList<Integer> nodes) throws Exception
	{
		final ArrayList<Object> tree = makeTree(nodes);
		sendRollbacks(tree, new Transaction(txnum));
	}

	public static void rollback(final Transaction tx) throws Exception
	{
		final List<Plan> ps = txs.get(tx);
		if (ps == null || ps.size() == 0)
		{
			return;
		}

		ArrayList<Integer> nodes = new ArrayList<Integer>();
		for (final Plan p : ps)
		{
			nodes.addAll(getNodes(p, tx));
		}
		nodes = consolidateNodes(nodes);
		final ArrayList<Object> tree = makeTree(nodes);
		sendRollbacks(tree, tx);
	}

	public static void rollbackP2(final long txnum, final ArrayList<Integer> nodes)
	{
		final ArrayList<Object> tree = makeTree(nodes);
		sendRollbacksP2(tree, new Transaction(txnum));
	}

	private static void cloneInto(final ArrayList<Object> target, final ArrayList<Object> source)
	{
		for (final Object o : source)
		{
			target.add(o);
		}
	}

	private static ArrayList<Integer> consolidateNodes(final ArrayList<Integer> nodes)
	{
		final HashSet<Integer> set = new HashSet<Integer>(nodes);
		return new ArrayList<Integer>(set);
	}

	private static boolean containsLoad(final Operator o)
	{
		if (o instanceof LoadOperator)
		{
			return true;
		}

		return false;
	}

	private static boolean containsLoad(final Plan p)
	{
		for (final Operator o : p.getTrees())
		{
			if (containsLoad(o))
			{
				return true;
			}
		}

		return false;
	}

	private static boolean containsPop(final Operator o)
	{
		if (o instanceof CreateIndexOperator)
		{
			return true;
		}

		return false;
	}

	private static boolean containsPop(final Plan p)
	{
		for (final Operator o : p.getTrees())
		{
			if (containsPop(o))
			{
				return true;
			}
		}

		return false;
	}

	private static ArrayList<Integer> getAllNodes(final ArrayList<Object> tree, final int remove)
	{
		final ArrayList<Integer> retval = new ArrayList<Integer>();
		for (final Object o : tree)
		{
			if (o instanceof Integer)
			{
				if (((Integer)o).intValue() != remove)
				{
					retval.add((Integer)o);
				}
			}
			else
			{
				// ArrayList<Object>
				retval.addAll(getAllNodes((ArrayList<Object>)o, remove));
			}
		}

		return retval;
	}

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
				throw e;
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

	private static ArrayList<Integer> getLoadNodes(final Operator o, final Transaction tx) throws Exception
	{
		final ArrayList<Integer> retval = new ArrayList<Integer>();
		final Transaction tx2 = new Transaction(Transaction.ISOLATION_CS);
		final LoadOperator op = (LoadOperator)o;
		// o.getMeta();
		retval.addAll(MetaData.getNodesForTable(op.getSchema(), op.getTable(), tx2));
		tx2.commit();
		return retval;
	}

	private static ArrayList<Integer> getLoadNodes(final Plan p, final Transaction tx) throws Exception
	{
		final ArrayList<Integer> retval = new ArrayList<Integer>();
		for (final Operator o : p.getTrees())
		{
			if (o instanceof LoadOperator)
			{
				retval.addAll(getLoadNodes(o, tx));
			}
		}

		return retval;
	}

	private static ArrayList<Integer> getNodes(final Operator o, final Transaction tx, final MetaData meta, final HashSet<Operator> complete) throws Exception
	{
		if (o instanceof MassDeleteOperator)
		{
			final Transaction tx2 = new Transaction(Transaction.ISOLATION_CS);
			final ArrayList<Integer> retval = MetaData.getNodesForTable(((MassDeleteOperator)o).getSchema(), ((MassDeleteOperator)o).getTable(), tx2);
			tx2.commit();
			return retval;
		}
		final ArrayList<Integer> list = new ArrayList<Integer>();
		if (complete.contains(o))
		{
			return list;
		}
		complete.add(o);
		if ((o instanceof NetworkSendOperator) || (o instanceof NetworkReceiveOperator) || (o instanceof RoutingOperator))
		{
			list.add(o.getNode());
		}

		for (final Operator op : o.children())
		{
			list.addAll(getNodes(op, tx, op.getMeta(), complete));
		}

		if (o instanceof InsertOperator)
		{
			list.addAll(((InsertOperator)o).getPlan().getTouchedNodes());
		}

		if (o instanceof DeleteOperator)
		{
			if (((DeleteOperator)o).getSchema().equals("SYS"))
			{
				final ArrayList<Integer> coords = MetaData.getCoordNodes();
				list.addAll(coords);
			}
		}

		if (o instanceof UpdateOperator)
		{
			if (((UpdateOperator)o).getSchema().equals("SYS"))
			{
				final ArrayList<Integer> coords = MetaData.getCoordNodes();
				list.addAll(coords);
			}

			list.addAll(((UpdateOperator)o).getPlan().getTouchedNodes());
		}

		if (o instanceof CreateIndexOperator)
		{
			final Transaction tx2 = new Transaction(Transaction.ISOLATION_CS);
			final CreateIndexOperator op = (CreateIndexOperator)o;
			list.addAll(MetaData.getNodesForTable(op.getSchema(), op.getTable(), tx2));
			tx2.commit();
		}

		if (o instanceof LoadOperator)
		{
			final Transaction tx2 = new Transaction(Transaction.ISOLATION_CS);
			final LoadOperator op = (LoadOperator)o;
			list.addAll(MetaData.getNodesForTable(op.getSchema(), op.getTable(), tx2));
			tx2.commit();
		}

		list.add(-1);
		return list;
	}

	private static ArrayList<Integer> getNodes(final Plan p, final Transaction tx) throws Exception
	{
		final HashSet<Integer> set = new HashSet<Integer>();
		for (final Operator o : p.getTrees())
		{
			set.addAll(getNodes(o, tx, p.getTrees().get(0).getMeta(), new HashSet<Operator>()));
		}

		return new ArrayList<Integer>(set);
	}

	private static ArrayList<Integer> getPopNodes(final Operator o, final Transaction tx) throws Exception
	{
		final ArrayList<Integer> retval = new ArrayList<Integer>();
		final Transaction tx2 = new Transaction(Transaction.ISOLATION_CS);
		final CreateIndexOperator op = (CreateIndexOperator)o;
		// o.getMeta();
		retval.addAll(MetaData.getNodesForTable(op.getSchema(), op.getTable(), tx2));
		tx2.commit();
		return retval;
	}

	private static ArrayList<Integer> getPopNodes(final Plan p, final Transaction tx) throws Exception
	{
		final ArrayList<Integer> retval = new ArrayList<Integer>();
		for (final Operator o : p.getTrees())
		{
			if (o instanceof CreateIndexOperator)
			{
				retval.addAll(getPopNodes(o, tx));
			}
		}

		return retval;
	}

	private static byte[] intToBytes(final int val)
	{
		final byte[] buff = new byte[4];
		buff[0] = (byte)(val >> 24);
		buff[1] = (byte)((val & 0x00FF0000) >> 16);
		buff[2] = (byte)((val & 0x0000FF00) >> 8);
		buff[3] = (byte)((val & 0x000000FF));
		return buff;
	}

	private static byte[] longToBytes(final long val)
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

	private static ArrayList<Object> makeTree(final ArrayList<Integer> nodes)
	{
		final int max = MAX_NEIGHBORS;
		if (nodes.size() <= max)
		{
			final ArrayList<Object> retval = new ArrayList<Object>(nodes);
			return retval;
		}

		final ArrayList<Object> retval = new ArrayList<Object>();
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
			final ArrayList<Integer> list = new ArrayList<Integer>(perNode + 1);
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
			final ArrayList<Integer> list = (ArrayList<Integer>)retval.remove(i);
			retval.add(i, makeTree(list));
			i++;
		}

		return retval;
	}

	private static boolean rebuildTree(final ArrayList<Object> tree, final int remove)
	{
		final ArrayList<Integer> nodes = new ArrayList<Integer>();
		for (final Object o : tree)
		{
			if (o instanceof Integer)
			{
				if (((Integer)o).intValue() != remove)
				{
					nodes.add((Integer)o);
				}
			}
			else
			{
				// ArrayList<Object>
				nodes.addAll(getAllNodes((ArrayList<Object>)o, remove));
			}
		}

		if (nodes.size() > 0)
		{
			tree.clear();
			cloneInto(tree, makeTree(nodes));
			return true;
		}

		return false;
	}

	private static String rewriteKeyValue(String sql)
	{
		final int headerSize = "CREATE KEY/VALUE PAIR ".length();
		sql = sql.substring(headerSize);
		final String table = sql.substring(0, sql.indexOf('(')).trim();
		sql = sql.substring(sql.indexOf('(') + 1);
		final String keyType = sql.substring(0, sql.indexOf(',')).trim();
		sql = sql.substring(sql.indexOf(',') + 1);
		final String valType = sql.substring(0, sql.indexOf(')')).trim();
		return "CREATE TABLE " + table + "(KEY2 " + keyType + " NOT NULL PRIMARY KEY, VAL " + valType + ") NONE ALL,HASH,{KEY2} ALL,HASH,{KEY2}";
	}

	private static boolean sendCheckpoint(final ArrayList<Object> tree, final Transaction tx)
	{
		Object obj = tree.get(0);
		while (obj instanceof ArrayList)
		{
			obj = ((ArrayList)obj).get(0);
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
			sock.connect(new InetSocketAddress(hostname, PORT_NUMBER));
			final OutputStream out = sock.getOutputStream();
			final byte[] outMsg = "CHECKPNT        ".getBytes(StandardCharsets.UTF_8);
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
			objOut.writeObject(convertToHosts(tree, tx));
			objOut.flush();
			out.flush();
			getConfirmation(sock);
			objOut.close();
			sock.close();
			return true;
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
			return false;
		}
	}

	private static void sendCheckpoints(final ArrayList<Object> tree, final Transaction tx) throws Exception
	{
		final ArrayList<SendCheckpointThread> threads = new ArrayList<SendCheckpointThread>();
		for (final Object o : tree)
		{
			if (o instanceof Integer)
			{
				final ArrayList<Object> list = new ArrayList<Object>(1);
				list.add(o);
				final SendCheckpointThread thread = new SendCheckpointThread(list, tx);
				threads.add(thread);
			}
			else
			{
				final SendCheckpointThread thread = new SendCheckpointThread((ArrayList<Object>)o, tx);
				threads.add(thread);
			}
		}

		for (final SendCheckpointThread thread : threads)
		{
			thread.start();
		}
	}

	private static void sendCommit(final ArrayList<Object> tree, final Transaction tx)
	{
		Object obj = tree.get(0);
		while (obj instanceof ArrayList)
		{
			obj = ((ArrayList)obj).get(0);
		}

		Socket sock = null;
		try
		{
			final Transaction tx2 = null;
			// new MetaData();
			final String hostname = MetaData.getHostNameForNode((Integer)obj, tx2);
			int i = 0;
			while (i < 50)
			{
				try
				{
					// sock = new Socket(hostname,
					// Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
					sock = new Socket();
					sock.setReceiveBufferSize(4194304);
					sock.setSendBufferSize(4194304);
					sock.connect(new InetSocketAddress(hostname, PORT_NUMBER));
					break;
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.debug(hostname, e);
					Thread.sleep(5000);
					i++;
				}
			}

			if (sock == null)
			{
				throw new Exception("Unable to connect to " + hostname + " after 50 tries");
			}

			final OutputStream out = sock.getOutputStream();
			final byte[] outMsg = "LCOMMIT         ".getBytes(StandardCharsets.UTF_8);
			outMsg[8] = 0;
			outMsg[9] = 0;
			outMsg[10] = 0;
			outMsg[11] = 0;
			outMsg[12] = 0;
			outMsg[13] = 0;
			outMsg[14] = 0;
			outMsg[15] = 0;
			out.write(outMsg);
			out.write(longToBytes(tx.number()));
			final ObjectOutputStream objOut = new ObjectOutputStream(out);
			objOut.writeObject(convertToHosts(tree, tx2));
			// tx2.commit();
			objOut.flush();
			out.flush();
			getConfirmation(sock);
			objOut.close();
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
			// TODO blackList((Integer)obj); - tx2?
			// TODO queueCommand((Integer)obj, "COMMIT", tx);
			HRDBMSWorker.logger.fatal("BLACKLIST", e);
			System.exit(1);
			final boolean toDo = rebuildTree(tree, (Integer)obj);
			if (toDo)
			{
				sendCommit(tree, tx);
			}
		}
	}

	private static void sendCommits(final ArrayList<Object> tree, final Transaction tx) throws Exception
	{
		String filename = LOG_DIR;
		if (!filename.endsWith("/"))
		{
			filename += "/";
		}
		filename += "xa.log";
		LogRec rec = new PrepareLogRec(tx.number(), toList(tree));
		LogManager.write(rec, filename);
		try
		{
			LogManager.flush(rec.lsn(), filename);
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.error("Unable to flush XA log when writing prepare record!", e);
		}
		boolean allOK = true;
		final ArrayList<SendPrepareThread> threads = new ArrayList<SendPrepareThread>();
		for (final Object o : tree)
		{
			if (o instanceof Integer)
			{
				final ArrayList<Object> list = new ArrayList<Object>(1);
				list.add(o);
				final SendPrepareThread thread = new SendPrepareThread(list, tx);
				threads.add(thread);

				// boolean ok = sendPrepare(list, tx);
				// if (!ok)
				// {
				// allOK = false;
				// }
			}
			else
			{
				final SendPrepareThread thread = new SendPrepareThread((ArrayList<Object>)o, tx);
				threads.add(thread);
				// boolean ok = sendPrepare((ArrayList<Object>)o, tx);
				// if (!ok)
				// {
				// allOK = false;
				// }
			}
		}

		for (final SendPrepareThread thread : threads)
		{
			thread.start();
		}

		for (final SendPrepareThread thread : threads)
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

		if (allOK)
		{
			rec = new XACommitLogRec(tx.number(), toList(tree));
			LogManager.write(rec, filename);

			try
			{
				LogManager.flush(rec.lsn(), filename);
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("Unable to flush XA log when writing commit record!", e);
			}

			txs.multiRemove(tx);

			final ArrayList<SendCommitThread> threads2 = new ArrayList<SendCommitThread>();
			for (final Object o : tree)
			{
				if (o instanceof Integer)
				{
					final ArrayList<Object> list = new ArrayList<Object>(1);
					list.add(o);
					threads2.add(new SendCommitThread(list, tx));
				}
				else
				{
					threads2.add(new SendCommitThread((ArrayList<Object>)o, tx));
				}
			}

			for (final SendCommitThread thread : threads2)
			{
				thread.start();
			}

			for (final SendCommitThread thread : threads2)
			{
				thread.join();
			}
		}
		else
		{
			rec = new XAAbortLogRec(tx.number(), toList(tree));
			LogManager.write(rec, filename);

			try
			{
				LogManager.flush(rec.lsn(), filename);
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("Unable to flush XA log when writing abort record!", e);
			}

			txs.multiRemove(tx);

			for (final Object o : tree)
			{
				if (o instanceof Integer)
				{
					final ArrayList<Object> list = new ArrayList<Object>(1);
					list.add(o);
					sendRollback(list, tx);
				}
				else
				{
					sendRollback((ArrayList<Object>)o, tx);
				}
			}
		}
	}

	private static void sendPhase2s(final ArrayList<Object> tree, final Transaction tx)
	{
		final ArrayList<SendCommitThread> threads = new ArrayList<SendCommitThread>();
		for (final Object o : tree)
		{
			if (o instanceof Integer)
			{
				final ArrayList<Object> list = new ArrayList<Object>(1);
				list.add(o);
				final SendCommitThread thread = new SendCommitThread(list, tx);
				threads.add(thread);
				thread.start();
			}
			else
			{
				final SendCommitThread thread = new SendCommitThread((ArrayList<Object>)o, tx);
				threads.add(thread);
				thread.start();
			}
		}

		for (final SendCommitThread thread : threads)
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
		}
	}

	private static boolean sendPrepare(final ArrayList<Object> tree, final Transaction tx)
	{
		Object obj = tree.get(0);
		while (obj instanceof ArrayList)
		{
			obj = ((ArrayList)obj).get(0);
		}

		Socket sock = null;
		try
		{
			// new MetaData();
			final String hostname = MetaData.getHostNameForNode((Integer)obj, tx);
			// new MetaData();
			final String host = MetaData.getMyHostName(tx);
			final byte[] data = host.getBytes(StandardCharsets.UTF_8);
			final byte[] length = intToBytes(data.length);
			// sock = new Socket(hostname,
			// Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
			sock = new Socket();
			sock.setReceiveBufferSize(4194304);
			sock.setSendBufferSize(4194304);
			sock.connect(new InetSocketAddress(hostname, PORT_NUMBER));
			final OutputStream out = sock.getOutputStream();
			final byte[] outMsg = "PREPARE         ".getBytes(StandardCharsets.UTF_8);
			outMsg[8] = 0;
			outMsg[9] = 0;
			outMsg[10] = 0;
			outMsg[11] = 0;
			outMsg[12] = 0;
			outMsg[13] = 0;
			outMsg[14] = 0;
			outMsg[15] = 0;
			out.write(outMsg);
			out.write(longToBytes(tx.number()));
			out.write(length);
			out.write(data);
			final ObjectOutputStream objOut = new ObjectOutputStream(out);
			objOut.writeObject(convertToHosts(tree, tx));
			objOut.flush();
			out.flush();
			getConfirmation(sock);
			objOut.close();
			sock.close();
			return true;
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
			return false;
		}
	}

	private static void sendRollback(final ArrayList<Object> tree, final Transaction tx)
	{
		Object obj = tree.get(0);
		while (obj instanceof ArrayList)
		{
			obj = ((ArrayList)obj).get(0);
		}

		Socket sock = null;
		try
		{
			final Transaction tx2 = null;
			// new MetaData();
			final String hostname = MetaData.getHostNameForNode((Integer)obj, tx2);

			int i = 0;
			while (i < 50)
			{
				try
				{
					// sock = new Socket(hostname,
					// Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
					sock = new Socket();
					sock.setReceiveBufferSize(4194304);
					sock.setSendBufferSize(4194304);
					sock.connect(new InetSocketAddress(hostname, PORT_NUMBER));
					break;
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.debug(hostname, e);
					Thread.sleep(5000);
					i++;
				}
			}

			if (sock == null)
			{
				throw new Exception("Unable to connect to " + hostname + " after 50 tries");
			}

			final OutputStream out = sock.getOutputStream();
			final byte[] outMsg = "LROLLBCK        ".getBytes(StandardCharsets.UTF_8);
			outMsg[8] = 0;
			outMsg[9] = 0;
			outMsg[10] = 0;
			outMsg[11] = 0;
			outMsg[12] = 0;
			outMsg[13] = 0;
			outMsg[14] = 0;
			outMsg[15] = 0;
			out.write(outMsg);
			out.write(longToBytes(tx.number()));
			final ObjectOutputStream objOut = new ObjectOutputStream(out);
			objOut.writeObject(convertToHosts(tree, tx2));
			// tx2.commit();
			objOut.flush();
			out.flush();
			getConfirmation(sock);
			objOut.close();
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
			// TODO blackList((Integer)obj); - tx2?
			// TODO queueCommand((Integer)obj, "ROLLBACK", tx);
			HRDBMSWorker.logger.fatal("BLACKLIST", e);
			System.exit(1);
			final boolean toDo = rebuildTree(tree, (Integer)obj);
			if (toDo)
			{
				sendRollback(tree, tx);
			}
		}
	}

	private static void sendRollbacks(final ArrayList<Object> tree, final Transaction tx) throws Exception
	{
		String filename = LOG_DIR;
		if (!filename.endsWith("/"))
		{
			filename += "/";
		}
		filename += "xa.log";

		final LogRec rec = new XAAbortLogRec(tx.number(), toList(tree));
		LogManager.write(rec, filename);

		try
		{
			LogManager.flush(rec.lsn(), filename);
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.error("Unable to flush XA log when writing abort record!", e);
		}

		txs.multiRemove(tx);
		final ArrayList<SendRollbackThread> threads2 = new ArrayList<SendRollbackThread>();
		for (final Object o : tree)
		{
			if (o instanceof Integer)
			{
				final ArrayList<Object> list = new ArrayList<Object>(1);
				list.add(o);
				threads2.add(new SendRollbackThread(list, tx));
			}
			else
			{
				threads2.add(new SendRollbackThread((ArrayList<Object>)o, tx));
			}
		}

		for (final SendRollbackThread thread : threads2)
		{
			thread.start();
		}

		for (final SendRollbackThread thread : threads2)
		{
			thread.join();
		}
	}

	private static void sendRollbacksP2(final ArrayList<Object> tree, final Transaction tx)
	{
		txs.multiRemove(tx);
		final ArrayList<SendRollbackThread> threads = new ArrayList<SendRollbackThread>();
		for (final Object o : tree)
		{
			if (o instanceof Integer)
			{
				final ArrayList<Object> list = new ArrayList<Object>(1);
				list.add(o);
				final SendRollbackThread thread = new SendRollbackThread(list, tx);
				threads.add(thread);
				thread.start();
			}
			else
			{
				final SendRollbackThread thread = new SendRollbackThread((ArrayList<Object>)o, tx);
				threads.add(thread);
				thread.start();
			}
		}

		for (final SendRollbackThread thread : threads)
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
		}
	}

	private static ArrayList<Integer> toList(final ArrayList<Object> tree)
	{
		final ArrayList<Integer> retval = new ArrayList<Integer>();
		for (final Object o : tree)
		{
			if (o instanceof Integer)
			{
				retval.add((Integer)o);
			}
			else
			{
				retval.addAll(toList((ArrayList<Object>)o));
			}
		}

		return retval;
	}

	@Override
	public void run()
	{
		try
		{
			while (!rP1)
			{
				try
				{
					Thread.sleep(1000);
				}
				catch (final Exception e)
				{
				}
			}

			while (in.size() > 0)
			{
				while (true)
				{
					try
					{
						final String cmd = (String)in.take();
						final Long txnum = (Long)in.take();
						final ArrayList<Integer> nodes = (ArrayList<Integer>)in.take();
						if (cmd.equals("COMMIT"))
						{
							XAManager.phase2(txnum, nodes);
						}
						else
						{
							rollbackP2(txnum, nodes);
						}
						break;
					}
					catch (final InterruptedException e)
					{
					}
				}
			}

			rP2 = true;
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.debug("", e);
		}
	}

	private static class SendCheckpointThread extends HRDBMSThread
	{
		private final ArrayList<Object> tree;
		private final Transaction tx;
		// private boolean ok;

		public SendCheckpointThread(final ArrayList<Object> tree, final Transaction tx)
		{
			this.tree = tree;
			this.tx = tx;
		}

		@Override
		public void run()
		{
			sendCheckpoint(tree, tx);
		}
	}

	private static class SendCommitThread extends HRDBMSThread
	{
		private final ArrayList<Object> tree;
		private final Transaction tx;

		public SendCommitThread(final ArrayList<Object> tree, final Transaction tx)
		{
			this.tree = tree;
			this.tx = tx;
		}

		@Override
		public void run()
		{
			sendCommit(tree, tx);
		}
	}

	private static class SendPrepareThread extends HRDBMSThread
	{
		private final ArrayList<Object> tree;
		private final Transaction tx;
		private boolean ok;

		public SendPrepareThread(final ArrayList<Object> tree, final Transaction tx)
		{
			this.tree = tree;
			this.tx = tx;
		}

		public boolean getOK()
		{
			return ok;
		}

		@Override
		public void run()
		{
			ok = sendPrepare(tree, tx);
		}
	}

	private static class SendRollbackThread extends HRDBMSThread
	{
		private final ArrayList<Object> tree;
		private final Transaction tx;

		public SendRollbackThread(final ArrayList<Object> tree, final Transaction tx)
		{
			this.tree = tree;
			this.tx = tx;
		}

		@Override
		public void run()
		{
			sendRollback(tree, tx);
		}
	}
}
