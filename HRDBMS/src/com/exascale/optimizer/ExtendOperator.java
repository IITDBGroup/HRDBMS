package com.exascale.optimizer;

import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;
import com.exascale.gpu.Kernel;
import com.exascale.gpu.Rootbeer;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.managers.ResourceManager;
import com.exascale.misc.BufferedLinkedBlockingQueue;
import com.exascale.misc.DataEndMarker;
import com.exascale.misc.FastStringTokenizer;
import com.exascale.misc.Utils;
import com.exascale.tables.Plan;
import com.exascale.threads.ThreadPoolThread;

public final class ExtendOperator implements Operator, Serializable
{
	private Operator child;

	private Operator parent;

	private HashMap<String, String> cols2Types;
	private HashMap<String, Integer> cols2Pos;
	private TreeMap<Integer, String> pos2Col;
	private final String prefix;
	private final MetaData meta;
	private final String name;
	private int node;
	private FastStringTokenizer tokens;
	private final ArrayDeque<String> master;
	private BufferedLinkedBlockingQueue queue;
	private volatile ArrayList<Integer> poses;
	private volatile boolean startDone = false;
	private Plan plan;
	
	public void setPlan(Plan plan)
	{
		this.plan = plan;
	}

	public ExtendOperator(String prefix, String name, MetaData meta)
	{
		this.prefix = prefix;
		this.meta = meta;
		this.name = name;
		this.tokens = new FastStringTokenizer(prefix, ",", false);
		this.master = new ArrayDeque<String>();
		for (final String token : tokens.allTokens())
		{
			master.push(token);
		}
	}

	@Override
	public void add(Operator op) throws Exception
	{
		if (child == null)
		{
			child = op;
			child.registerParent(this);
			if (child.getCols2Types() != null)
			{
				cols2Types = (HashMap<String, String>)child.getCols2Types().clone();
				cols2Types.put(name, "FLOAT");
				cols2Pos = (HashMap<String, Integer>)child.getCols2Pos().clone();
				cols2Pos.put(name, cols2Pos.size());
				pos2Col = (TreeMap<Integer, String>)child.getPos2Col().clone();
				pos2Col.put(pos2Col.size(), name);
			}
		}
		else
		{
			throw new Exception("ExtendOperator only supports 1 child.");
		}
	}

	@Override
	public ArrayList<Operator> children()
	{
		final ArrayList<Operator> retval = new ArrayList<Operator>(1);
		retval.add(child);
		return retval;
	}

	@Override
	public ExtendOperator clone()
	{
		final ExtendOperator retval = new ExtendOperator(prefix, name, meta);
		retval.node = node;
		retval.tokens = tokens.clone();
		return retval;
	}

	@Override
	public void close() throws Exception
	{
		child.close();
	}

	@Override
	public int getChildPos()
	{
		return 0;
	}

	@Override
	public HashMap<String, Integer> getCols2Pos()
	{
		return cols2Pos;
	}

	@Override
	public HashMap<String, String> getCols2Types()
	{
		return cols2Types;
	}

	@Override
	public MetaData getMeta()
	{
		return meta;
	}

	@Override
	public int getNode()
	{
		return node;
	}

	public String getOutputCol()
	{
		return name;
	}

	@Override
	public TreeMap<Integer, String> getPos2Col()
	{
		return pos2Col;
	}

	@Override
	public ArrayList<String> getReferences()
	{
		final ArrayList<String> retval = new ArrayList<String>();
		final FastStringTokenizer tokens = new FastStringTokenizer(prefix, ",", false);
		while (tokens.hasMoreTokens())
		{
			final String temp = tokens.nextToken();
			if (Character.isLetter(temp.charAt(0)) || (temp.charAt(0) == '_'))
			{
				retval.add(temp);
			}
		}

		return retval;
	}

	// @?Parallel
	@Override
	public Object next(Operator op) throws Exception
	{
		Object o;
		o = queue.take();

		if (o instanceof DataEndMarker)
		{
			o = queue.peek();
			if (o == null)
			{
				queue.put(new DataEndMarker());
				return new DataEndMarker();
			}
			else
			{
				queue.put(new DataEndMarker());
				return o;
			}
		}
		return o;
	}

	@Override
	public void nextAll(Operator op) throws Exception
	{
		child.nextAll(op);
		Object o = next(op);
		while (!(o instanceof DataEndMarker))
		{
			o = next(op);
		}
	}

	@Override
	public Operator parent()
	{
		return parent;
	}

	@Override
	public void registerParent(Operator op) throws Exception
	{
		if (parent == null)
		{
			parent = op;
		}
		else
		{
			throw new Exception("ExtendOperator only supports 1 parent.");
		}
	}

	@Override
	public void removeChild(Operator op)
	{
		if (op == child)
		{
			child = null;
			op.removeParent(this);
		}
	}

	@Override
	public void removeParent(Operator op)
	{
		parent = null;
	}

	@Override
	public void reset()
	{
		if (!startDone)
		{
			try
			{
				start();
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				System.exit(1);
			}
		}
		else
		{
			queue.clear();
			child.reset();
			new GPUThread().start();
		}
	}

	@Override
	public void setChildPos(int pos)
	{
	}

	@Override
	public void setNode(int node)
	{
		this.node = node;
	}

	@Override
	public void start() throws Exception
	{
		startDone = true;
		child.start();
		queue = new BufferedLinkedBlockingQueue(ResourceManager.QUEUE_SIZE);
		new GPUThread().start();
	}

	@Override
	public String toString()
	{
		return "ExtendOperator: " + name + "=" + prefix;
	}

	private final Double parsePrefixDouble(ArrayList<Object> row)
	{
		final ArrayList<Integer> p = new ArrayList<Integer>();
		final ArrayDeque<String> parseStack = master.clone();
		final ArrayDeque<Object> execStack = new ArrayDeque<Object>();
		// System.out.println("Starting parse stack = " + parseStack);

		while (parseStack.size() > 0)
		{
			// System.out.println("Exec stack = " + execStack);
			final String temp = parseStack.pop();
			// System.out.println("We popped " + temp);
			if (temp.equals("*"))
			{
				final Double lhs = (Double)execStack.pop();
				final Double rhs = (Double)execStack.pop();
				// System.out.println("Arguments are " + lhs + " and " + rhs);
				execStack.push(lhs * rhs);

			}
			else if (temp.equals("-"))
			{
				final Double lhs = (Double)execStack.pop();
				final Double rhs = (Double)execStack.pop();
				// System.out.println("Arguments are " + lhs + " and " + rhs);
				execStack.push(lhs - rhs);
			}
			else if (temp.equals("+"))
			{
				final Double lhs = (Double)execStack.pop();
				final Double rhs = (Double)execStack.pop();
				// System.out.println("Arguments are " + lhs + " and " + rhs);
				execStack.push(lhs + rhs);
			}
			else if (temp.equals("/"))
			{
				final Double lhs = (Double)execStack.pop();
				final Double rhs = (Double)execStack.pop();
				// System.out.println("Arguments are " + lhs + " and " + rhs);
				execStack.push(lhs / rhs);
			}
			else
			{
				try
				{
					if (Character.isLetter(temp.charAt(0)) || (temp.charAt(0) == '_'))
					{
						Object field = null;
						try
						{
							// System.out.println("Fetching field " + temp +
							// " from " + cols2Pos);
							// System.out.println("Row is " + row);
							final int x = cols2Pos.get(temp);
							p.add(x);
							field = row.get(x);
							// System.out.println("Fetched value is " + field);
						}
						catch (final Exception e)
						{
							HRDBMSWorker.logger.error("Error getting column " + temp + " from row " + row, e);
							HRDBMSWorker.logger.error("Cols2Pos = " + cols2Pos);
							System.exit(1);
						}
						if (field instanceof Long)
						{
							execStack.push(new Double(((Long)field).longValue()));
						}
						else if (field instanceof Integer)
						{
							execStack.push(new Double(((Integer)field).intValue()));
						}
						else if (field instanceof Double)
						{
							execStack.push(field);
						}
						else
						{
							HRDBMSWorker.logger.error("Unknown type in ExtendOperator: " + field.getClass());
							HRDBMSWorker.logger.error("Row: " + row);
							HRDBMSWorker.logger.error("Cols2Pos: " + cols2Pos);
							System.exit(1);
						}
					}
					else
					{
						final double d = Utils.parseDouble(temp);
						// System.out.println("Parsed a literal numeric value and got "
						// + d);
						execStack.push(d);
					}
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.error("", e);
					System.exit(1);
				}
			}
		}

		poses = p;
		final Double retval = (Double)execStack.pop();
		// System.out.println("Going to return " + retval);
		return retval;
	}

	public static final class ExtendKernel implements Kernel
	{
		public ArrayList<Object> row;
		public ArrayDeque master;
		public ArrayList<Integer> poses;
		public ArrayList<Double> calced;

		public ExtendKernel(ArrayList<Object> row, ArrayDeque master, ArrayList<Integer> poses, ArrayList<Double> calced)
		{
			this.row = row;
			this.master = master;
			this.poses = poses;
			this.calced = calced;
		}

		public ArrayList<Object> getRow()
		{
			return row;
		}
	}

	private final class GPUThread extends ThreadPoolThread
	{
		@Override
		public void run()
		{
			HRDBMSWorker.logger.debug("GPUThread is starting");
			final List<Kernel> jobs = new ArrayList<Kernel>(ResourceManager.CUDA_SIZE);
			final ArrayList<Double> calced = new ArrayList<Double>();
			int i = 0;
			while (true)
			{
				Object o = null;
				try
				{
					o = child.next(ExtendOperator.this);
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.error("", e);
					System.exit(1);
				}

				if (o instanceof DataEndMarker)
				{
					if (i > 0)
					{
						final Rootbeer rootbeer = new Rootbeer();
						rootbeer.runAll(jobs);
						i = 0;

						for (final Kernel k : jobs)
						{
							((ExtendKernel)k).getRow().add(calced.get(i));
							queue.put(((ExtendKernel)k).getRow());
							i++;
						}
					}

					queue.put(o);
					return;
				}

				final ArrayList<Object> row = (ArrayList<Object>)o;
				if (poses == null || !ResourceManager.GPU)
				{
					final Double ppd = parsePrefixDouble(row);
					row.add(ppd);
					queue.put(row);
				}
				else
				{
					jobs.add(new ExtendKernel(row, master, poses, calced));
					i++;

					if (i == ResourceManager.CUDA_SIZE)
					{
						final Rootbeer rootbeer = new Rootbeer();
						rootbeer.runAll(jobs);
						i = 0;

						for (final Kernel k : jobs)
						{
							final Double ppd = calced.get(i);
							((ExtendKernel)k).getRow().add(ppd);
							queue.put(((ExtendKernel)k).getRow());
							i++;
						}

						i = 0;
						jobs.clear();
						calced.clear();
					}
				}
			}
		}
	}
}
