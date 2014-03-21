package com.exascale.threads;

import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Vector;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.TreeNode;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.misc.BufferedLinkedBlockingQueue;
import com.exascale.misc.DataEndMarker;
import com.exascale.optimizer.IndexOperator;
import com.exascale.optimizer.Operator;
import com.exascale.optimizer.TableScanOperator;
import com.exascale.tables.Plan;
import com.exascale.tables.Transaction;

public class XAWorker extends HRDBMSThread
{
	private final Plan p;
	private final Transaction tx;
	private final boolean result;

	public XAWorker(Plan p, Transaction tx, boolean result)
	{
		this.description = "XA Worker";
		this.setWait(false);
		this.p = p;
		this.tx = tx;
		this.result = result;
	}

	@Override
	public void run()
	{
		for (Operator tree : p.getTrees())
		{
			setPlanAndTransaction(tree);
		}
		
		if (result)
		{
			p.execute(); //TODO someone needs to call next() repeatedly, do something with rows, and call close() on the return value
		}
		else
		{
			p.executeNoResult();
		}
	}
	
	private void setPlanAndTransaction(Operator op)
	{
		if (op instanceof TableScanOperator)
		{
			((TableScanOperator)op).setTransaction(tx);
		}
		else if (op instanceof IndexOperator)
		{
			((IndexOperator)op).getIndex().setTransaction(tx);
		}
		
		op.setPlan(p);
		
		for (Operator o : op.children())
		{
			setPlanAndTransaction(o);
		}
	}
}
