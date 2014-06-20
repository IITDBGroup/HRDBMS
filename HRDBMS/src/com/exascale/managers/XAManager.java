package com.exascale.managers;

import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import com.exascale.logging.CommitLogRec;
import com.exascale.logging.LogRec;
import com.exascale.logging.PrepareLogRec;
import com.exascale.logging.ReadyLogRec;
import com.exascale.logging.RollbackLogRec;
import com.exascale.logging.XAAbortLogRec;
import com.exascale.logging.XACommitLogRec;
import com.exascale.misc.MultiHashMap;
import com.exascale.optimizer.InsertOperator;
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
import com.exascale.optimizer.SQLParser;
import com.exascale.tables.Plan;
import com.exascale.tables.Transaction;
import com.exascale.threads.ConnectionWorker;
import com.exascale.threads.HRDBMSThread;
import com.exascale.threads.XAWorker;
import com.exascale.optimizer.RootOperator;

public class XAManager
{
	public static MultiHashMap<Transaction, Plan> txs = new MultiHashMap<Transaction, Plan>();
	
	public static XAWorker executeQuery(String sql, Transaction tx, ConnectionWorker conn) throws Exception
	{
		String sql2 = sql.toUpperCase();
		if (!(sql2.startsWith("SELECT") || sql2.startsWith("WITH")))
		{
			throw new Exception("Not a select statement");
		}
		Plan plan = PlanCacheManager.checkPlanCache(sql);
		
		if (plan == null)
		{
			SQLParser parse = new SQLParser(sql, conn);
			Operator op = parse.parse();
			new Phase1((RootOperator)op).optimize();
			new Phase2((RootOperator)op).optimize();
			new Phase3((RootOperator)op).optimize();
			new Phase4((RootOperator)op).optimize();
			new Phase5((RootOperator)op).optimize();
			ArrayList<Operator> array = new ArrayList<Operator>(1);
			array.add(op);
			plan = new Plan(false, array);
			
			if (parse.doesNotUseCurrentSchema())
			{
				PlanCacheManager.addPlan(sql, plan);
			}
		}
		
		txs.multiPut(tx, plan);
		return new XAWorker(plan, tx, true);
	}
	
	public static XAWorker executeUpdate(String sql, Transaction tx, ConnectionWorker conn) throws Exception
	{
		String sql2 = sql.toUpperCase();
		if (sql2.startsWith("SELECT") || sql2.startsWith("WITH"))
		{
			throw new Exception("SELECT statement is not allowed");
		}
		
		
		SQLParser parse = new SQLParser(sql, conn);
		Operator op = parse.parse();
		ArrayList<Operator> array = new ArrayList<Operator>(1);
		array.add(op);
		Plan plan = new Plan(false, array);
		txs.multiPut(tx, plan);
		return new XAWorker(plan, tx, false);
	}
	
	public static void commit(Transaction tx) throws Exception
	{
		Vector<Plan> ps = txs.get(tx);
		if (ps == null || ps.size() == 0)
		{
			throw new Exception("The XAManager does not own this transaction");
		}
		
		ArrayList<Integer> nodes = new ArrayList<Integer>();
		for (Plan p : ps)
		{
			nodes.addAll(getNodes(p));
		}
		
		nodes = consolidateNodes(nodes);
		ArrayList<Object> tree = makeTree(nodes);
		sendCommits(tree, tx);
	}
	
	public static void phase2(long txnum, ArrayList<Integer> nodes)
	{
		ArrayList<Object> tree = makeTree(nodes);
		sendPhase2s(tree, new Transaction(txnum));
	}
	
	public static void rollback(long txnum, ArrayList<Integer> nodes)
	{
		ArrayList<Object> tree = makeTree(nodes);
		sendRollbacks(tree, new Transaction(txnum));
	}
	
	public static void rollback(Transaction tx) throws Exception
	{
		Vector<Plan> ps = txs.get(tx);
		if (ps == null || ps.size() == 0)
		{
			throw new Exception("The XAManager does not own this transaction");
		}
		
		ArrayList<Integer> nodes = new ArrayList<Integer>();
		for (Plan p : ps)
		{
			nodes.addAll(getNodes(p));
		}
		nodes = consolidateNodes(nodes);
		ArrayList<Object> tree = makeTree(nodes);
		sendRollbacks(tree, tx);
	}
	
	private static ArrayList<Integer> consolidateNodes(ArrayList<Integer> nodes)
	{
		HashSet<Integer> set = new HashSet<Integer>(nodes);
		return new ArrayList<Integer>(set);
	}
	
	private static void sendRollbacks(ArrayList<Object> tree, Transaction tx)
	{
		String filename = HRDBMSWorker.getHParms().getProperty("log_dir");
		if (!filename.endsWith("/"))
		{
			filename += "/";
		}
		filename += "xa.log";
		LogRec rec = new XAAbortLogRec(tx.number(), toList(tree));
		LogManager.write(rec, filename);
		try
		{
			LogManager.flush(rec.lsn(), filename);
		}
		catch(Exception e)
		{
			HRDBMSWorker.logger.error("Unable to flush XA log when writing abort record!", e);
		}
		
		txs.remove(tx);
		for (Object o : tree)
		{
			if (o instanceof Integer)
			{
				ArrayList<Object> list = new ArrayList<Object>(1);
				list.add(o);
				new SendRollbackThread(list, tx).start();
			}
			else
			{
				new SendRollbackThread((ArrayList<Object>)o, tx).start();
			}
		}
	}
	
	private static class SendRollbackThread extends HRDBMSThread
	{
		private ArrayList<Object> tree;
		private Transaction tx;
		
		public SendRollbackThread(ArrayList<Object> tree, Transaction tx)
		{
			this.tree = tree;
			this.tx = tx;
		}
		
		public void run()
		{
			sendRollback(tree, tx);
		}
	}
	
	private static void sendRollback(ArrayList<Object> tree, Transaction tx)
	{
		Object obj = tree.get(0);
		while (obj instanceof ArrayList)
		{
			obj = ((ArrayList)obj).get(0);
		}
		
		String hostname = new MetaData().getHostNameForNode((Integer)obj);
		Socket sock = null;
		try
		{
			sock = new Socket(hostname, Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
			OutputStream out = sock.getOutputStream();
			byte[] outMsg = "LROLLBCK        ".getBytes("UTF-8");
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
			ObjectOutputStream objOut = new ObjectOutputStream(out);
			objOut.writeObject(convertToHosts(tree));
			objOut.flush();
			out.flush();
			objOut.close();
			getConfirmation(sock);
			out.close();
			sock.close();
		}
		catch(Exception e)
		{
			sock.close();
			blackList((Integer)obj);
			queueCommand((Integer)obj, "ROLLBACK", tx);
			boolean toDo = rebuildTree(tree, (Integer)obj);
			if (toDo)
			{
				sendRollback(tree, tx);
			}
		}
	}
	
	private static boolean rebuildTree(ArrayList<Object> tree, int remove)
	{
		ArrayList<Integer> nodes = new ArrayList<Integer>();
		for (Object o : tree)
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
				//ArrayList<Object>
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
	
	private static void cloneInto(ArrayList<Object> target, ArrayList<Object> source)
	{
		for (Object o : source)
		{
			target.add(o);
		}
	}
	
	private static ArrayList<Integer> getAllNodes(ArrayList<Object> tree, int remove)
	{
		ArrayList<Integer> retval = new ArrayList<Integer>();
		for (Object o : tree)
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
				//ArrayList<Object>
				retval.addAll(getAllNodes((ArrayList<Object>)o, remove));
			}
		}
		
		return retval;
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
	
	private static void sendCommits(ArrayList<Object> tree, Transaction tx)
	{
		String filename = HRDBMSWorker.getHParms().getProperty("log_dir");
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
		catch(Exception e)
		{
			HRDBMSWorker.logger.error("Unable to flush XA log when writing prepare record!", e);
		}
		boolean allOK = true;
		ArrayList<SendPrepareThread> threads = new ArrayList<SendPrepareThread>();
		for (Object o : tree)
		{
			if (o instanceof Integer)
			{
				ArrayList<Object> list = new ArrayList<Object>(1);
				list.add(o);
				SendPrepareThread thread = new SendPrepareThread(list, tx);
				threads.add(thread);
				
				//boolean ok = sendPrepare(list, tx);
				//if (!ok)
				//{
				//	allOK = false;
				//}
			}
			else
			{
				SendPrepareThread thread = new SendPrepareThread((ArrayList<Object>)o, tx);
				threads.add(thread);
				//boolean ok = sendPrepare((ArrayList<Object>)o, tx);
				//if (!ok)
				//{
				//	allOK = false;
				//}
			}
		}
		
		for (SendPrepareThread thread : threads)
		{
			thread.start();
		}
		
		for (SendPrepareThread thread : threads)
		{
			while (true)
			{
				try
				{
					thread.join();
					break;
				}
				catch(InterruptedException e)
				{}
			}
			boolean ok = thread.getOK();
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
			catch(Exception e)
			{
				HRDBMSWorker.logger.error("Unable to flush XA log when writing commit record!", e);
			}
			
			txs.remove(tx);
			
			for (Object o : tree)
			{
				if (o instanceof Integer)
				{
					ArrayList<Object> list = new ArrayList<Object>(1);
					list.add(o);
					new SendCommitThread(list, tx).start();
				}
				else
				{
					new SendCommitThread((ArrayList<Object>)o, tx).start();
				}
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
			catch(Exception e)
			{
				HRDBMSWorker.logger.error("Unable to flush XA log when writing abort record!", e);
			}
			
			txs.remove(tx);
			
			for (Object o : tree)
			{
				if (o instanceof Integer)
				{
					ArrayList<Object> list = new ArrayList<Object>(1);
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
	
	private static void sendPhase2s(ArrayList<Object> tree, Transaction tx)
	{
		for (Object o : tree)
		{
			if (o instanceof Integer)
			{
				ArrayList<Object> list = new ArrayList<Object>(1);
				list.add(o);
				new SendCommitThread(list, tx).start();
			}
			else
			{
				new SendCommitThread((ArrayList<Object>)o, tx).start();
			}
		}
	}
	
	public static boolean askXAManager(ReadyLogRec xa)
	{
		try
		{
			Socket sock = new Socket(xa.getHost(), Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
			OutputStream out = sock.getOutputStream();
			byte[] outMsg = "CHECKTX         ".getBytes("UTF-8");
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
			
			String inStr = new String(inMsg, "UTF-8");
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
			catch(Exception e)
			{}
			
			out.close();
			sock.close();
			return true;
		}
		catch(Exception e)
		{
			return askReplicas(xa);
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
	
	private static class SendPrepareThread extends HRDBMSThread
	{
		private ArrayList<Object> tree;
		private Transaction tx;
		private boolean ok;
		
		public SendPrepareThread(ArrayList<Object> tree, Transaction tx)
		{
			this.tree = tree;
			this.tx = tx;
		}
		
		public void run()
		{
			ok = sendPrepare(tree, tx);
		}
		
		public boolean getOK()
		{
			return ok;
		}
	}
	
	private static boolean sendPrepare(ArrayList<Object> tree, Transaction tx)
	{
		Object obj = tree.get(0);
		while (obj instanceof ArrayList)
		{
			obj = ((ArrayList)obj).get(0);
		}
		
		String hostname = new MetaData().getHostNameForNode((Integer)obj);
		Socket sock = null;
		String host = new MetaData().getMyHostName();
		try
		{
			byte[] data = host.getBytes("UTF-8");
			byte[] length = intToBytes(data.length);
			sock = new Socket(hostname, Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
			OutputStream out = sock.getOutputStream();
			byte[] outMsg = "PREPARE         ".getBytes("UTF-8");
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
			ObjectOutputStream objOut = new ObjectOutputStream(out);
			objOut.writeObject(convertToHosts(tree));
			objOut.flush();
			out.flush();
			objOut.close();
			getConfirmation(sock);
			out.close();
			sock.close();
			return true;
		}
		catch(Exception e)
		{
			try
			{
				sock.close();
			}
			catch(Exception f)
			{}
			return false;
		}
	}
	
	private static class SendCommitThread extends HRDBMSThread
	{
		private ArrayList<Object> tree;
		private Transaction tx;
		
		public SendCommitThread(ArrayList<Object> tree, Transaction tx)
		{
			this.tree = tree;
			this.tx = tx;
		}
		
		public void run()
		{
			sendCommit(tree, tx);
		}
	}
	
	private static void sendCommit(ArrayList<Object> tree, Transaction tx)
	{
		Object obj = tree.get(0);
		while (obj instanceof ArrayList)
		{
			obj = ((ArrayList)obj).get(0);
		}
		
		String hostname = new MetaData().getHostNameForNode((Integer)obj);
		Socket sock = null;
		try
		{
			sock = new Socket(hostname, Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
			OutputStream out = sock.getOutputStream();
			byte[] outMsg = "LCOMMIT         ".getBytes("UTF-8");
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
			ObjectOutputStream objOut = new ObjectOutputStream(out);
			objOut.writeObject(convertToHosts(tree));
			objOut.flush();
			out.flush();
			objOut.close();
			getConfirmation(sock);
			out.close();
			sock.close();
		}
		catch(Exception e)
		{
			sock.close();
			blackList((Integer)obj);
			queueCommand((Integer)obj, "COMMIT", tx);
			boolean toDo = rebuildTree(tree, (Integer)obj);
			if (toDo)
			{
				sendCommit(tree, tx);
			}
		}
	}
	
	private static ArrayList<Object> convertToHosts(ArrayList<Object> tree)
	{
		ArrayList<Object> retval = new ArrayList<Object>();
		int i = 0;
		while (i < retval.size())
		{
			Object obj = tree.get(i);
			if (obj instanceof Integer)
			{
				retval.add(new MetaData().getHostNameForNode((Integer)obj));
			}
			else
			{
				retval.add(convertToHosts((ArrayList<Object>)obj));
			}
			
			i++;
		}
		
		return retval;
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
		
		String inStr = new String(inMsg, "UTF-8");
		if (!inStr.equals("OK"))
		{
			in.close();
			throw new Exception();
		}
		
		try
		{
			in.close();
		}
		catch(Exception e)
		{}
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
		while (i < nodes.size())
		{
			int first = (Integer)retval.get(j);
			retval.remove(j);
			ArrayList<Integer> list = new ArrayList<Integer>(perNode+1);
			list.add(first);
			int k = 0;
			while (k < perNode && i < nodes.size())
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
		
		//more than 2 tier
		i = 0;
		while (i < retval.size())
		{
			ArrayList<Integer> list = (ArrayList<Integer>)retval.remove(i);
			retval.add(i, makeTree(list));
			i++;
		}
		
		return retval;
	}
	
	private static ArrayList<Integer> getNodes(Plan p)
	{
		HashSet<Integer> set = new HashSet<Integer>();
		for (Operator o : p.getTrees())
		{
			set.addAll(getNodes(o));
		}
		
		return new ArrayList<Integer>(set);
	}
	
	private static ArrayList<Integer> getNodes(Operator o)
	{
		if (o instanceof MassDeleteOperator)
		{
			return MetaData.getAllTableNodes(((MassDeleteOperator)o).getSchema(), ((MassDeleteOperator)o).getTable);
		}
		ArrayList<Integer> list = new ArrayList<Integer>();
		if (o instanceof NetworkSendOperator)
		{
			list.add(o.getNode());
		}
		else if (o instanceof NetworkReceiveOperator)
		{
			list.add(o.getNode());
		}
		
		for (Operator op : o.children())
		{
			list.addAll(getNodes(op));
		}
		
		if (o instanceof InsertOperator && list.isEmpty())
		{
			((InsertOperator)o).getPlan().getTouchedNodes();
		}
		return list;
	}
	
	private static ArrayList<Integer> toList(ArrayList<Object> tree)
	{
		ArrayList<Integer> retval = new ArrayList<Integer>();
		for (Object o : tree)
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
}
