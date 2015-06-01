package com.exascale.optimizer;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.LockSupport;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.managers.ResourceManager;
import com.exascale.managers.ResourceManager.DiskBackedArray;
import com.exascale.misc.BufferedLinkedBlockingQueue;
import com.exascale.misc.DataEndMarker;
import com.exascale.tables.Plan;
import com.exascale.threads.ThreadPoolThread;

public final class ProductOperator extends JoinOperator implements Serializable
{
	private static sun.misc.Unsafe unsafe;

	static
	{
		try
		{
			final Field f = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
			f.setAccessible(true);
			unsafe = (sun.misc.Unsafe)f.get(null);
		}
		catch (Exception e)
		{
			unsafe = null;
		}
	}

	private ArrayList<Operator> children = new ArrayList<Operator>(2);

	private Operator parent;
	private HashMap<String, String> cols2Types;
	private HashMap<String, Integer> cols2Pos;
	private TreeMap<Integer, String> pos2Col;
	private transient final MetaData meta;
	private transient volatile BufferedLinkedBlockingQueue outBuffer;
	private transient volatile DiskBackedArray inBuffer;
	private transient int NUM_RT_THREADS;
	private transient int NUM_PTHREADS;
	// private final AtomicLong outCount = new AtomicLong(0);
	private transient volatile boolean readersDone;
	private int childPos = -1;
	private int node;

	private int rightChildCard = 16;

	private boolean cardSet = false;

	public ProductOperator(MetaData meta)
	{
		this.meta = meta;
	}

	public static ProductOperator deserialize(InputStream in, HashMap<Long, Object> prev) throws Exception
	{
		ProductOperator value = (ProductOperator)unsafe.allocateInstance(ProductOperator.class);
		prev.put(OperatorUtils.readLong(in), value);
		value.children = OperatorUtils.deserializeALOp(in, prev);
		value.parent = OperatorUtils.deserializeOperator(in, prev);
		value.cols2Types = OperatorUtils.deserializeStringHM(in, prev);
		value.cols2Pos = OperatorUtils.deserializeStringIntHM(in, prev);
		value.pos2Col = OperatorUtils.deserializeTM(in, prev);
		value.childPos = OperatorUtils.readInt(in);
		value.node = OperatorUtils.readInt(in);
		value.rightChildCard = OperatorUtils.readInt(in);
		value.cardSet = OperatorUtils.readBool(in);
		return value;
	}

	@Override
	public void add(Operator op) throws Exception
	{
		if (children.size() < 2)
		{
			if (childPos == -1)
			{
				children.add(op);
			}
			else
			{
				children.add(childPos, op);
				childPos = -1;
			}
			op.registerParent(this);

			if (children.size() == 2 && children.get(0).getCols2Types() != null && children.get(1).getCols2Types() != null)
			{
				cols2Types = (HashMap<String, String>)children.get(0).getCols2Types().clone();
				cols2Pos = (HashMap<String, Integer>)children.get(0).getCols2Pos().clone();
				pos2Col = (TreeMap<Integer, String>)children.get(0).getPos2Col().clone();

				cols2Types.putAll(children.get(1).getCols2Types());
				for (final Map.Entry entry : children.get(1).getPos2Col().entrySet())
				{
					cols2Pos.put((String)entry.getValue(), cols2Pos.size());
					pos2Col.put(pos2Col.size(), (String)entry.getValue());
				}
			}
		}
		else
		{
			throw new Exception("ProductOperator only supports 2 children");
		}
	}

	@Override
	public void addJoinCondition(ArrayList<Filter> filters)
	{
		throw new UnsupportedOperationException("ProductOperator does not support addJoinCondition");

	}

	@Override
	public void addJoinCondition(String left, String right)
	{
		throw new UnsupportedOperationException("ProductOperator does not support addJoinCondition");

	}

	@Override
	public ArrayList<Operator> children()
	{
		return children;
	}

	@Override
	public ProductOperator clone()
	{
		final ProductOperator retval = new ProductOperator(meta);
		retval.node = node;
		retval.rightChildCard = rightChildCard;
		retval.cardSet = cardSet;
		return retval;
	}

	@Override
	public void close() throws Exception
	{
		for (final Operator child : children)
		{
			child.close();
		}

		inBuffer.close();

		if (outBuffer != null)
		{
			outBuffer.close();
		}

		cols2Pos = null;
		cols2Types = null;
		pos2Col = null;
	}

	@Override
	public int getChildPos()
	{
		return childPos;
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
	public HashSet<HashMap<Filter, Filter>> getHSHMFilter()
	{
		return null;
	}

	@Override
	public boolean getIndexAccess()
	{
		return false;
	}

	@Override
	public ArrayList<String> getJoinForChild(Operator op)
	{
		return null;
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

	@Override
	public TreeMap<Integer, String> getPos2Col()
	{
		return pos2Col;
	}

	@Override
	public ArrayList<String> getReferences()
	{
		final ArrayList<String> retval = new ArrayList<String>(0);
		return retval;
	}

	@Override
	public Object next(Operator op) throws Exception
	{
		Object o;
		o = outBuffer.take();

		if (o instanceof DataEndMarker)
		{
			o = outBuffer.peek();
			if (o == null)
			{
				outBuffer.put(new DataEndMarker());
				return new DataEndMarker();
			}
			else
			{
				outBuffer.put(new DataEndMarker());
				return o;
			}
		}
		if (o instanceof Exception)
		{
			throw (Exception)o;
		}
		return o;
	}

	@Override
	public void nextAll(Operator op) throws Exception
	{
		children.get(0).nextAll(op);
		children.get(1).nextAll(op);
		Object o = next(op);
		while (!(o instanceof DataEndMarker) && !(o instanceof Exception))
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
			throw new Exception("ProductOperator can only have 1 parent.");
		}
	}

	@Override
	public void removeChild(Operator op)
	{
		childPos = children.indexOf(op);
		children.remove(op);
		op.removeParent(this);
	}

	@Override
	public void removeParent(Operator op)
	{
		parent = null;
	}

	@Override
	public void reset() throws Exception
	{
		HRDBMSWorker.logger.error("ProductOperator cannot be reset");
		throw new Exception("ProductOperator cannot be reset");
	}

	@Override
	public void serialize(OutputStream out, IdentityHashMap<Object, Long> prev) throws Exception
	{
		Long id = prev.get(this);
		if (id != null)
		{
			OperatorUtils.serializeReference(id, out);
			return;
		}

		OperatorUtils.writeType(58, out);
		prev.put(this, OperatorUtils.writeID(out));
		OperatorUtils.serializeALOp(children, out, prev);
		parent.serialize(out, prev);
		OperatorUtils.serializeStringHM(cols2Types, out, prev);
		OperatorUtils.serializeStringIntHM(cols2Pos, out, prev);
		OperatorUtils.serializeTM(pos2Col, out, prev);
		OperatorUtils.writeInt(childPos, out);
		OperatorUtils.writeInt(node, out);
		OperatorUtils.writeInt(rightChildCard, out);
		OperatorUtils.writeBool(cardSet, out);
	}

	@Override
	public void setChildPos(int pos)
	{
		childPos = pos;
	}

	@Override
	public void setNode(int node)
	{
		this.node = node;
	}

	@Override
	public void setPlan(Plan plan)
	{
	}

	public boolean setRightChildCard(int card)
	{
		if (cardSet)
		{
			return false;
		}

		cardSet = true;
		rightChildCard = card;
		return true;
	}

	@Override
	public void start() throws Exception
	{
		inBuffer = ResourceManager.newDiskBackedArray(rightChildCard);
		NUM_RT_THREADS = 4 * ResourceManager.cpus;
		NUM_PTHREADS = 4 * ResourceManager.cpus;
		readersDone = false;

		for (final Operator child : children)
		{
			child.start();
		}

		outBuffer = new BufferedLinkedBlockingQueue(ResourceManager.QUEUE_SIZE);
		new InitThread().start();

	}

	@Override
	public String toString()
	{
		return "ProductOperator";
	}

	private final class InitThread extends ThreadPoolThread
	{
		@Override
		public void run()
		{
			ThreadPoolThread[] threads;
			int i = 0;
			threads = new ReaderThread[NUM_RT_THREADS];
			while (i < NUM_RT_THREADS)
			{
				threads[i] = new ReaderThread();
				threads[i].start();
				i++;
			}

			i = 0;
			final ThreadPoolThread[] threads2 = new ProcessThread[NUM_PTHREADS];
			while (i < NUM_PTHREADS)
			{
				threads2[i] = new ProcessThread();
				threads2[i].start();
				i++;
			}

			i = 0;
			while (i < NUM_RT_THREADS)
			{
				while (true)
				{
					try
					{
						threads[i].join();
						i++;
						break;
					}
					catch (final InterruptedException e)
					{
					}
				}
			}

			readersDone = true;

			i = 0;
			while (i < NUM_PTHREADS)
			{
				while (true)
				{
					try
					{
						threads2[i].join();
						i++;
						break;
					}
					catch (final InterruptedException e)
					{
					}
				}
			}

			while (true)
			{
				try
				{
					outBuffer.put(new DataEndMarker());
					inBuffer.close();
					break;
				}
				catch (Exception e)
				{
				}
				try
				{
					inBuffer.close();
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.error("", e);
				}
			}
		}
	}

	private final class ProcessThread extends ThreadPoolThread
	{
		@Override
		public void run()
		{
			try
			{
				final Operator left = children.get(0);
				Object o = left.next(ProductOperator.this);
				// @Parallel
				while (!(o instanceof DataEndMarker))
				{
					final ArrayList<Object> lRow = (ArrayList<Object>)o;
					long i = 0;
					while (true)
					{
						if (i >= inBuffer.size())
						{
							if (readersDone)
							{
								if (i >= inBuffer.size())
								{
									break;
								}

								continue;
							}
							else
							{
								LockSupport.parkNanos(75000);
								continue;
							}
						}
						final Object orow = inBuffer.get(i);
						i++;
						final ArrayList<Object> rRow = (ArrayList<Object>)orow;

						if (orow == null)
						{
							LockSupport.parkNanos(75000);
							continue;
						}

						final ArrayList<Object> out = new ArrayList<Object>(lRow.size() + rRow.size());
						out.addAll(lRow);
						out.addAll(rRow);
						outBuffer.put(out);
						// outCount.incrementAndGet();
					}

					o = left.next(ProductOperator.this);
				}
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				try
				{
					outBuffer.put(e);
				}
				catch (Exception f)
				{
				}
				return;
			}
		}
	}

	private final class ReaderThread extends ThreadPoolThread
	{
		@Override
		public void run()
		{
			try
			{
				final Operator child = children.get(1);
				Object o = child.next(ProductOperator.this);
				while (!(o instanceof DataEndMarker))
				{
					inBuffer.add((ArrayList<Object>)o);
					o = child.next(ProductOperator.this);
				}
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
				try
				{
					outBuffer.put(e);
				}
				catch (Exception f)
				{
				}
				return;
			}
		}
	}
}
