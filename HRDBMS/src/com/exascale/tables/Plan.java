package com.exascale.tables;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.Map;
import java.util.Vector;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.TreeNode;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.optimizer.CNFFilter;
import com.exascale.optimizer.Operator;
import com.exascale.optimizer.TableScanOperator;

public class Plan implements Serializable
{
	private final long time;
	private final boolean reserved;
	private final ArrayList<Operator> trees;
	private DataType[] argTypes;
	private Object[] args;

	public Plan(boolean reserved, ArrayList<Operator> trees)
	{
		time = System.currentTimeMillis();
		this.reserved = reserved;
		this.trees = trees;
	}

	public Plan(boolean reserved, ArrayList<Operator> trees, DataType[] argTypes)
	{
		time = System.currentTimeMillis();
		this.reserved = reserved;
		this.trees = trees;
		this.argTypes = argTypes;
	}

	public Plan(Plan p)
	{
		this.time = p.time;
		this.reserved = p.reserved;
		this.trees = cloneArray(p.trees);
		this.argTypes = argTypes;
	}

	public long getTimeStamp()
	{
		return time;
	}

	public ArrayList<Operator> getTrees()
	{
		return trees;
	}

	public boolean isReserved()
	{
		return reserved;
	}
	
	public ArrayList<Operator> cloneArray(ArrayList<Operator> source)
	{
		ArrayList<Operator> retval = new ArrayList<Operator>(source.size());
		for (Operator tree : source)
		{
			retval.add(clone(tree));
		}
		
		return retval;
	}
	
	private Operator clone(Operator op)
	{
		final Operator clone = op.clone();
		int i = 0;
		for (final Operator o : op.children())
		{
			try
			{
				clone.add(clone(o));
				clone.setChildPos(op.getChildPos());
				if (o instanceof TableScanOperator)
				{
					final CNFFilter cnf = ((TableScanOperator)o).getCNFForParent(op);
					if (cnf != null)
					{
						final Operator child = clone.children().get(i);
						((TableScanOperator)child).setCNFForParent(clone, cnf);
					}
				}

				if (op instanceof TableScanOperator)
				{
					final Operator child = clone.children().get(i);
					int device = -1;

					for (final Map.Entry entry : (((TableScanOperator)op).device2Child).entrySet())
					{
						if (entry.getValue() == o)
						{
							device = (Integer)entry.getKey();
						}
					}
					((TableScanOperator)clone).setChildForDevice(device, child);
				}
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				System.exit(1);
			}

			i++;
		}

		return clone;
	}

	public void setArgs(Object[] args) throws Exception
	{
		if (args.length != argTypes.length)
		{
			throw new Exception("Wrong number of arguments!");
		}

		int i = 0;
		for (final DataType type : argTypes)
		{
			if (args[i] == null)
			{
				continue;
			}

			if (type.getType() == DataType.BIGINT)
			{
				if (args[i] instanceof Long || args[i] instanceof Integer || args[i] instanceof Short)
				{
				}
				else
				{
					throw new Exception("Argument " + args[i] + " is invalid where used.");
				}
			}
			else if (type.getType() == DataType.BINARY)
			{
				if (args[i] instanceof byte[])
				{
					final int length = ((byte[])args[i]).length;
					if (length != type.getLength())
					{
						throw new Exception("The length of " + args[i] + " is incorrect.");
					}
				}
				else
				{
					throw new Exception("Argument " + args[i] + " is invalid where used.");
				}
			}
			else if (type.getType() == DataType.DATE)
			{
				if (args[i] instanceof Date)
				{
				}
				else
				{
					throw new Exception("Argument " + args[i] + " is invalid where used.");
				}
			}
			else if (type.getType() == DataType.DECIMAL)
			{
				if (args[i] instanceof Short || args[i] instanceof Integer || args[i] instanceof Long || args[i] instanceof Float || args[i] instanceof Double || args[i] instanceof BigDecimal)
				{
				}
				else
				{
					throw new Exception("Argument " + args[i] + " is invalid where used.");
				}
			}
			else if (type.getType() == DataType.DOUBLE)
			{
				if (args[i] instanceof Short || args[i] instanceof Integer || args[i] instanceof Long || args[i] instanceof Float || args[i] instanceof Double)
				{
				}
				else
				{
					throw new Exception("Argument " + args[i] + " is invalid where used.");
				}
			}
			else if (type.getType() == DataType.FLOAT)
			{
				if (args[i] instanceof Short || args[i] instanceof Integer || args[i] instanceof Long || args[i] instanceof Float)
				{
				}
				else
				{
					throw new Exception("Argument " + args[i] + " is invalid where used.");
				}
			}
			else if (type.getType() == DataType.INTEGER)
			{
				if (args[i] instanceof Short || args[i] instanceof Integer)
				{
				}
				else
				{
					throw new Exception("Argument " + args[i] + " is invalid where used.");
				}
			}
			else if (type.getType() == DataType.SMALLINT)
			{
				if (args[i] instanceof Short)
				{
				}
				else
				{
					throw new Exception("Argument " + args[i] + " is invalid where used.");
				}
			}
			else if (type.getType() == DataType.TIME)
			{
				if (args[i] instanceof Date)
				{
				}
				else
				{
					throw new Exception("Argument " + args[i] + " is invalid where used.");
				}
			}
			else if (type.getType() == DataType.TIMESTAMP)
			{
				if (args[i] instanceof Date)
				{
				}
				else
				{
					throw new Exception("Argument " + args[i] + " is invalid where used.");
				}
			}
			else if (type.getType() == DataType.VARBINARY)
			{
				if (args[i] instanceof byte[])
				{
					final int length = ((byte[])args[i]).length;
					if (length > type.getLength())
					{
						throw new Exception("The length of " + args[i] + " is too long.");
					}
				}
				else
				{
					throw new Exception("Argument " + args[i] + " is invalid where used.");
				}
			}
			else if (type.getType() == DataType.VARCHAR)
			{
				if (args[i] instanceof String)
				{
					final int length = ((String)args[i]).length();
					if (length > type.getLength())
					{
						throw new Exception("The length of " + args[i] + " is too long.");
					}
				}
				else
				{
					throw new Exception("Argument " + args[i] + " is invalid where used.");
				}
			}

			i++;
		}
		
		this.args = args;
	}
	
	public Operator execute() throws Exception
	{
		int i = 0;
		while (i < trees.size() - 1)
		{
			trees.get(i).start();
			trees.get(i).close();
			i++;
		}
		
		Operator retval = trees.get(trees.size()-1);
		retval.start();
		return retval;
	}
	
	public void executeNoResult() throws Exception
	{
		for (Operator op : trees)
		{
			op.start();
			op.close();
		}
	}
}
