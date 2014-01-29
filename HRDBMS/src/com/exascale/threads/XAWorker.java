package com.exascale.threads;

import java.net.Socket;
import java.util.Vector;
import java.util.concurrent.BufferedLinkedBlockingQueue;

import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.TreeNode;

import com.exascale.managers.HRDBMSWorker;
import com.exascale.managers.XAManager;
import com.exascale.operators.Operator;
import com.exascale.tables.Plan;
import com.exascale.optimizer.testing.*;
import com.exascale.tables.Transaction;
import java.util.Collections;
import java.util.ArrayList;

public class XAWorker extends HRDBMSThread
{
	protected Plan p;
	protected Transaction tx;
	protected BufferedLinkedBlockingQueue q;
	
	public XAWorker(Plan p, Transaction tx, BufferedLinkedBlockingQueue q)
	{
		this.description = "XA Worker";
		this.setWait(false);
		this.p = p;
		this.tx = tx;
		this.q = q;
	}
	
	public void run()
	{
		try
		{
			boolean first = true;
			for (TreeNode root : p.getTrees())
			{
				linkAndSendOperators(root, tx);
				Operator rootOp = (Operator)((DefaultMutableTreeNode)root).getUserObject();
				rootOp.start(tx);
			
				if (first)
				{
					while (rootOp.next())
					{
						Vector<Object> data = new Vector<Object>();
						int i = 1;
						while (i <= rootOp.getNumCols())
						{
							data.add(rootOp.getVal(i));
							i++;
						}
					
						while (true)
						{
							try
							{
								q.put(data);
								break;
							}
							catch(InterruptedException e)
							{}
						}
					}
					
					first = false;
					q.put(new DataEndMarker());
				}
			}
		}
		catch(Exception e)
		{
			while (true)
			{
				try
				{
					q.put(e);
					break;
				}
				catch(InterruptedException f)
				{}
			}
			this.terminate();
			return;
		}
	}
	
	protected void linkAndSendOperators(TreeNode root, Transaction tx)
	{
		ArrayList<TreeNode> children = Collections.list(root.children());
		Operator rootOp = ((Operator)((DefaultMutableTreeNode)root).getUserObject());
		for (TreeNode child : children)
		{
			Operator childOp = ((Operator)((DefaultMutableTreeNode)child).getUserObject());
			
			if (rootOp instanceof ReceiveFromNetworkOperator)
			{
				HashAndSendToNetworkOperator sendOp = (HashAndSendToNetworkOperator)childOp;
				String host = sendOp.getSource();
				Socket sock = new Socket(host, Integer.parseInt(HRDBMSWorker.getHParms().getProperty("port_number")));
				sendXAManagerConnection(sock, tx.getIsolationLevel());
				sendSubPlan(child, sock);
				Vector<Socket> sockets = XAManager.transMap.get(tx);
				if (sockets == null)
				{
					sockets = new Vector<Socket>();
				}
				
				sockets.add(sock);
				XAManager.transMap.put(tx,  sockets);
			}
			else
			{
				rootOp.addChild(childOp);
				linkAndSendOperators(child, tx);
			}
		}
	}
}
