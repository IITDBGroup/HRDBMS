package com.exascale.optimizer;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.managers.ResourceManager;
import com.exascale.managers.ResourceManager.DiskBackedHashSet;

public final class CountDistinctOperator implements AggregateOperator, Serializable
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

	private String input;
	private String output;
	private transient final MetaData meta;

	private int NUM_GROUPS = 16;

	private int childCard = 16 * 16;

	public CountDistinctOperator(String input, String output, MetaData meta)
	{
		this.input = input;
		this.output = output;
		this.meta = meta;
	}

	public static CountDistinctOperator deserialize(InputStream in, HashMap<Long, Object> prev) throws Exception
	{
		CountDistinctOperator value = (CountDistinctOperator)unsafe.allocateInstance(CountDistinctOperator.class);
		prev.put(OperatorUtils.readLong(in), value);
		value.input = OperatorUtils.readString(in, prev);
		value.output = OperatorUtils.readString(in, prev);
		value.NUM_GROUPS = OperatorUtils.readInt(in);
		value.childCard = OperatorUtils.readInt(in);
		return value;
	}

	@Override
	public CountDistinctOperator clone()
	{
		return new CountDistinctOperator(input, output, meta);
	}

	@Override
	public AggregateResultThread getHashThread(HashMap<String, Integer> cols2Pos)
	{
		return new CountDistinctHashThread(cols2Pos);
	}

	@Override
	public String getInputColumn()
	{
		return input;
	}

	@Override
	public AggregateResultThread newProcessingThread(ArrayList<ArrayList<Object>> rows, HashMap<String, Integer> cols2Pos)
	{
		return new CountDistinctThread(rows, cols2Pos);
	}

	@Override
	public String outputColumn()
	{
		return output;
	}

	@Override
	public String outputType()
	{
		return "LONG";
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

		OperatorUtils.writeType(52, out);
		prev.put(this, OperatorUtils.writeID(out));
		OperatorUtils.writeString(input, out, prev);
		OperatorUtils.writeString(output, out, prev);
		OperatorUtils.writeInt(NUM_GROUPS, out);
		OperatorUtils.writeInt(childCard, out);
	}

	public void setChildCard(int card)
	{
		childCard = card;
	}

	@Override
	public void setInput(String col)
	{
		input = col;
	}

	@Override
	public void setInputColumn(String col)
	{
		input = col;
	}

	@Override
	public void setNumGroups(int groups)
	{
		NUM_GROUPS = groups;
	}

	private final class CountDistinctHashThread extends AggregateResultThread
	{
		// private final DiskBackedALOHashMap<AtomicLong> results = new
		// DiskBackedALOHashMap<AtomicLong>(NUM_GROUPS > 0 ? NUM_GROUPS : 16);
		private final ConcurrentHashMap<ArrayList<Object>, AtomicLong> results = new ConcurrentHashMap<ArrayList<Object>, AtomicLong>(NUM_GROUPS, 0.75f, 6 * ResourceManager.cpus);
		private DiskBackedHashSet hashSet2;
		private ConcurrentHashMap<ArrayList<Object>, ArrayList<Object>> hashSet;
		private final int pos;
		private boolean inMem = true;

		public CountDistinctHashThread(HashMap<String, Integer> cols2Pos)
		{
			pos = cols2Pos.get(input);
			if (ResourceManager.lowMem())
			{
				inMem = false;
				hashSet2 = ResourceManager.newDiskBackedHashSet(false, childCard);
			}
			else if (childCard > ResourceManager.QUEUE_SIZE * Double.parseDouble(HRDBMSWorker.getHParms().getProperty("external_factor")))
			{
				inMem = false;
				hashSet2 = ResourceManager.newDiskBackedHashSet(false, childCard);
			}

			if (inMem)
			{
				hashSet = new ConcurrentHashMap<ArrayList<Object>, ArrayList<Object>>(childCard);
			}
		}

		@Override
		public void close()
		{
			try
			{
				if (hashSet2 != null)
				{
					hashSet2.close();
				}
				// results.close();
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("", e);
			}
		}

		@Override
		public Object getResult(ArrayList<Object> keys)
		{
			return results.get(keys).longValue();
		}

		// @Parallel
		@Override
		public final void put(ArrayList<Object> row, ArrayList<Object> group) throws Exception
		{
			final ArrayList<Object> consolidated = new ArrayList<Object>();
			consolidated.addAll(group);
			consolidated.add(row.get(pos));
			if (inMem)
			{
				if (hashSet.put(consolidated, consolidated) == null)
				{
					final AtomicLong al = results.get(group);
					if (al != null)
					{
						al.incrementAndGet();
						return;
					}

					if (results.putIfAbsent(group, new AtomicLong(1)) != null)
					{
						results.get(group).incrementAndGet();
					}
				}
			}
			else
			{
				if (hashSet2.add(consolidated))
				{
					final AtomicLong al = results.get(group);
					if (al != null)
					{
						al.incrementAndGet();
						return;
					}

					if (results.putIfAbsent(group, new AtomicLong(1)) != null)
					{
						results.get(group).incrementAndGet();
					}
				}
			}
		}
	}

	private final class CountDistinctThread extends AggregateResultThread
	{
		private final ArrayList<ArrayList<Object>> rows;
		private long result;
		private final int pos;
		private final HashSet<Object> distinct = new HashSet<Object>();

		public CountDistinctThread(ArrayList<ArrayList<Object>> rows, HashMap<String, Integer> cols2Pos)
		{
			this.rows = rows;
			pos = cols2Pos.get(input);
		}

		@Override
		public void close()
		{
		}

		@Override
		public Object getResult()
		{
			return new Long(result);
		}

		@Override
		public void run()
		{
			for (final Object o : rows)
			{
				final ArrayList<Object> row = (ArrayList<Object>)o;
				final Object val = row.get(pos);
				distinct.add(val);
			}

			result = distinct.size();
		}
	}
}
