package com.exascale.threads;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Vector;
import java.util.concurrent.ArrayBlockingQueue;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.managers.ResourceManager;
import com.exascale.misc.DataEndMarker;
import com.exascale.misc.SPSCQueue;
import com.exascale.optimizer.AntiJoinOperator;
import com.exascale.optimizer.CreateIndexOperator;
import com.exascale.optimizer.CreateTableOperator;
import com.exascale.optimizer.CreateExternalTableOperator;
import com.exascale.optimizer.CreateViewOperator;
import com.exascale.optimizer.DeleteOperator;
import com.exascale.optimizer.DropIndexOperator;
import com.exascale.optimizer.DropTableOperator;
import com.exascale.optimizer.DropViewOperator;
import com.exascale.optimizer.HashJoinOperator;
import com.exascale.optimizer.IndexOperator;
import com.exascale.optimizer.InsertOperator;
import com.exascale.optimizer.LoadOperator;
import com.exascale.optimizer.MassDeleteOperator;
import com.exascale.optimizer.MultiOperator;
import com.exascale.optimizer.NestedLoopJoinOperator;
import com.exascale.optimizer.NetworkReceiveOperator;
import com.exascale.optimizer.Operator;
import com.exascale.optimizer.ProductOperator;
import com.exascale.optimizer.RunstatsOperator;
import com.exascale.optimizer.SemiJoinOperator;
import com.exascale.optimizer.SortOperator;
import com.exascale.optimizer.TableScanOperator;
import com.exascale.optimizer.UpdateOperator;
import com.exascale.tables.Plan;
import com.exascale.tables.Transaction;

public class XAWorker extends HRDBMSThread
{
	private static Vector<ArrayBlockingQueue> free = new Vector<ArrayBlockingQueue>();
	private static Vector<SPSCQueue> free2 = new Vector<SPSCQueue>();
	private final Plan p;
	private final Transaction tx;
	private final boolean result;
	public volatile ArrayBlockingQueue<Object> in;
	public volatile SPSCQueue out;
	private int updateCount;
	private Exception ex;

	public XAWorker(Plan p, Transaction tx, boolean result)
	{
		this.description = "XA Worker";
		this.setWait(false);
		this.p = p;
		this.tx = tx;
		this.result = result;
		try
		{
			in = free.remove(0);
		}
		catch (Exception e)
		{
			in = new ArrayBlockingQueue<Object>(ResourceManager.QUEUE_SIZE);
		}

		try
		{
			out = free2.remove(0);
		}
		catch (Exception e)
		{
			out = new SPSCQueue(ResourceManager.QUEUE_SIZE);
		}
	}

	public Exception getException()
	{
		return ex;
	}

	public int getUpdateCount()
	{
		return updateCount;
	}

	@Override
	public void run()
	{
		for (Operator tree : p.getTrees())
		{
			try
			{
				setPlanAndTransaction(tree, new HashSet<Operator>());
			}
			catch (Exception e)
			{
				HRDBMSWorker.logger.debug("", e);
			}
		}

		if (result)
		{
			try
			{
				Operator op = p.execute();
				ResourceManager.registerOperator(op);
				while (true)
				{
					try
					{
						ArrayList<Object> command = (ArrayList<Object>)in.take();
						String text = (String)command.get(0);
						if (text.equals("CLOSE"))
						{
							op.nextAll(op);
							op.close();
							ResourceManager.deregisterOperator(op);
							in.clear();
							out.clear();
							free.add(in);
							free2.add(out);
							in = null;
							out = null;
							this.terminate();
							return;
						}
						else if (text.equals("META"))
						{
							out.put(op.getCols2Pos());
							out.put(op.getPos2Col());
							out.put(op.getCols2Types());
						}
						else if (text.equals("NEXT"))
						{
							int howMany = (Integer)command.get(1);
							while (howMany > 0)
							{
								try
								{
									Object obj = op.next(op);
									out.put(obj);
									if (obj instanceof DataEndMarker)
									{
										break;
									}
									howMany--;
								}
								catch (Exception e)
								{
									HRDBMSWorker.logger.debug("", e);
									out.put(e);
									op.nextAll(op);
									op.close();
									ResourceManager.deregisterOperator(op);
									this.terminate();
									return;
								}
							}
						}
						else
						{
							HRDBMSWorker.logger.debug("Unknown command received by XAWorker: " + text);
							op.close();
							ResourceManager.deregisterOperator(op);
							this.terminate();
						}
					}
					catch (InterruptedException e)
					{
					}
				}
			}
			catch (Exception e)
			{
				try
				{
					HRDBMSWorker.logger.debug("", e);
					out.put(e);
				}
				catch (Exception f)
				{
				}
			}
		}
		else
		{
			try
			{
				updateCount = p.executeNoResult();
				in.clear();
				out.clear();
				free.add(in);
				free2.add(out);
				in = null;
				out = null;
				this.terminate();
				return;
			}
			catch (Exception e)
			{
				HRDBMSWorker.logger.debug("", e);
				updateCount = -1;
				ex = e;
			}
		}

		this.terminate();
	}

	private void setPlanAndTransaction(Operator op, HashSet<Operator> visited) throws Exception
	{
		if ((!(op instanceof TableScanOperator)) && visited.contains(op))
		{
			return;
		}

		if (op instanceof TableScanOperator)
		{
			if (((TableScanOperator)op).tx != null)
			{
				return;
			}
		}

		visited.add(op);

		if (op instanceof TableScanOperator)
		{
			((TableScanOperator)op).setTransaction(tx);
		}
		else if (op instanceof HashJoinOperator)
		{
			((HashJoinOperator)op).setTXNum(tx.number());
		}
		else if (op instanceof MultiOperator)
		{
			((MultiOperator)op).setTXNum(tx.number());
		}
		else if (op instanceof SortOperator)
		{
			((SortOperator)op).setTXNum(tx.number());
		}
		else if (op instanceof ProductOperator)
		{
			((ProductOperator)op).setTXNum(tx.number());
		}
		else if (op instanceof SemiJoinOperator)
		{
			((SemiJoinOperator)op).setTXNum(tx.number());
		}
		else if (op instanceof AntiJoinOperator)
		{
			((AntiJoinOperator)op).setTXNum(tx.number());
		}
		else if (op instanceof NestedLoopJoinOperator)
		{
			((NestedLoopJoinOperator)op).setTXNum(tx.number());
		}
		else if (op instanceof NetworkReceiveOperator)
		{
			((NetworkReceiveOperator)op).setTXNum(tx.number());
		}
		else if (op instanceof IndexOperator)
		{
			((IndexOperator)op).getIndex().setTransaction(tx);
		}
		else if (op instanceof MassDeleteOperator)
		{
			((MassDeleteOperator)op).setTransaction(tx);
		}
		else if (op instanceof DeleteOperator)
		{
			((DeleteOperator)op).setTransaction(tx);
		}
		else if (op instanceof InsertOperator)
		{
			((InsertOperator)op).setTransaction(tx);
		}
		else if (op instanceof UpdateOperator)
		{
			((UpdateOperator)op).setTransaction(tx);
		}
		else if (op instanceof CreateViewOperator)
		{
			((CreateViewOperator)op).setTransaction(tx);
		}
		else if (op instanceof DropViewOperator)
		{
			((DropViewOperator)op).setTransaction(tx);
		}
		else if (op instanceof CreateTableOperator)
		{
			((CreateTableOperator)op).setTransaction(tx);
		}
		else if (op instanceof CreateExternalTableOperator)
		{
			((CreateExternalTableOperator)op).setTransaction(tx);
		}
		else if (op instanceof DropTableOperator)
		{
			((DropTableOperator)op).setTransaction(tx);
		}
		else if (op instanceof CreateIndexOperator)
		{
			((CreateIndexOperator)op).setTransaction(tx);
		}
		else if (op instanceof DropIndexOperator)
		{
			((DropIndexOperator)op).setTransaction(tx);
		}
		else if (op instanceof LoadOperator)
		{
			((LoadOperator)op).setTransaction(tx);
		}
		else if (op instanceof RunstatsOperator)
		{
			((RunstatsOperator)op).setTransaction(tx);
		}

		op.setPlan(p);

		for (Operator o : op.children())
		{
			setPlanAndTransaction(o, visited);
		}
	}
}
