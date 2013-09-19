package com.exascale.managers;

import java.net.Socket;
import java.util.HashMap;
import java.util.Vector;
import java.util.concurrent.LinkedBlockingQueue;

import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.TreeNode;

import com.exascale.operators.Operator;
import com.exascale.tables.Plan;
import com.exascale.tables.Transaction;
import com.exascale.threads.XAWorker;

public class XAManager 
{
	private String log;
	public static HashMap<Transaction, Vector<Socket>> transMap = new HashMap<Transaction, Vector<Socket>>();
	private boolean doLog;
	
	public XAManager(boolean doLog)
	{
		this.doLog = doLog;
		if (doLog)
		{
			String dir = HRDBMSWorker.getHParms().getProperty("xa_log_dir");
			if (!dir.endsWith("/"))
			{
				dir += "/";
			}
		
			dir += "xa.log";
			while (true)
			{
				try
				{
					LogManager.getInputQueue().put("ADD LOG " + dir);
					break;
				}
				catch(InterruptedException e)
				{}
			}
			log = dir;
		}
	}
	
	public static Transaction newTransaction(int iso)
	{
		Transaction t = new Transaction(iso);
		return t;
	}
	
	public static LinkedBlockingQueue runWithRS(Plan p, Transaction g)
	{
		LinkedBlockingQueue retval = new LinkedBlockingQueue();
		HRDBMSWorker.addThread(new XAWorker(p, g, retval));
		return retval;
	}
}