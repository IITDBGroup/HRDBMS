package com.exascale.tables;

import java.math.BigDecimal;
import java.util.Date;
import java.util.Vector;

import javax.swing.tree.TreeNode;

import com.exascale.operators.Operator;
import com.exascale.tables.Schema.FieldValue;

import javax.swing.tree.DefaultMutableTreeNode;

public class Plan 
{
	protected long time;
	protected boolean reserved;
	protected Vector<TreeNode> trees;
	protected Object[] args;
	protected DataType[] argTypes;
	
	public Plan(Plan p)
	{
		this.time = p.time;
		this.reserved = p.reserved;
		this.trees = p.trees;
		this.args = null;
		this.argTypes = argTypes;
	}
	
	public Plan(boolean reserved, Vector<TreeNode> trees)
	{
		time = System.currentTimeMillis();
		this.reserved = reserved;
		this.trees = trees;
	}
	
	public Plan(boolean reserved, Vector<TreeNode> tree, DataType[] argTypes)
	{
		time = System.currentTimeMillis();
		this.reserved = reserved;
		this.trees = trees;
		this.argTypes = argTypes;
	}
	
	public long getTimeStamp()
	{
		return time;
	}
	
	public boolean isReserved()
	{
		return reserved;
	}
	
	public long computeCost()
	{
		long cost = 0;
		for (TreeNode root : trees)
		{
			cost += subtreeCost(root);
		}
	
		return cost;
	}
	
	protected long subtreeCost(TreeNode root)
	{
		long cost = ((Operator)((DefaultMutableTreeNode)root).getUserObject()).cost();
		int childCount = root.getChildCount();
		int i = 0;
		while (i < childCount)
		{
			cost += subtreeCost(root.getChildAt(i));
		}
		
		return cost;
	}
	
	public void setArgs(Object[] args) throws Exception
	{
		if (args.length != argTypes.length)
		{
			throw new Exception("Wrong number of arguments!");
		}
		
		int i = 0;
		for (DataType type : argTypes)
		{
			if (args[i] == null)
			{
				continue;
			}
			
			if (type.getType() == DataType.BIGINT)
			{
				if (args[i] instanceof Long || args[i] instanceof Integer || args[i] instanceof Short)
				{}
				else
				{
					throw new Exception("Argument " + args[i] + " is invalid where used.");
				}
			}
			else if (type.getType() == DataType.BINARY)
			{
				if (args[i] instanceof byte[])
				{
					int length = ((byte[])args[i]).length;
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
				{}
				else
				{
					throw new Exception("Argument " + args[i] + " is invalid where used.");
				}
			}
			else if (type.getType() == DataType.DECIMAL)
			{
				if (args[i] instanceof Short || args[i] instanceof Integer || args[i] instanceof Long || args[i] instanceof Float || args[i] instanceof Double || args[i] instanceof BigDecimal)
				{}
				else
				{
					throw new Exception("Argument " + args[i] + " is invalid where used.");
				}
			}
			else if (type.getType() == DataType.DOUBLE)
			{
				if (args[i] instanceof Short || args[i] instanceof Integer || args[i] instanceof Long || args[i] instanceof Float || args[i] instanceof Double)
				{}
				else
				{
					throw new Exception("Argument " + args[i] + " is invalid where used.");
				}
			}
			else if (type.getType() == DataType.FLOAT)
			{
				if (args[i] instanceof Short || args[i] instanceof Integer || args[i] instanceof Long || args[i] instanceof Float)
				{}
				else
				{
					throw new Exception("Argument " + args[i] + " is invalid where used.");
				}
			}
			else if (type.getType() == DataType.INTEGER)
			{
				if (args[i] instanceof Short || args[i] instanceof Integer)
				{}
				else
				{
					throw new Exception("Argument " + args[i] + " is invalid where used.");
				}
			}
			else if (type.getType() == DataType.SMALLINT)
			{
				if (args[i] instanceof Short)
				{}
				else
				{
					throw new Exception("Argument " + args[i] + " is invalid where used.");
				}
			}
			else if (type.getType() == DataType.TIME)
			{
				if (args[i] instanceof Date)
				{}
				else
				{
					throw new Exception("Argument " + args[i] + " is invalid where used.");
				}
			}
			else if (type.getType() == DataType.TIMESTAMP)
			{
				if (args[i] instanceof Date)
				{}
				else
				{
					throw new Exception("Argument " + args[i] + " is invalid where used.");
				}
			}
			else if (type.getType() == DataType.VARBINARY)
			{
				if (args[i] instanceof byte[])
				{
					int length = ((byte[])args[i]).length;
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
					int length = ((String)args[i]).length();
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
	
	public Vector<TreeNode> getTrees()
	{
		return trees;
	}
}
