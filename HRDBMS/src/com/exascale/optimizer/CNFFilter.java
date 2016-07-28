package com.exascale.optimizer;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Locale;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.misc.DataEndMarker;
import com.exascale.misc.MurmurHash;
import com.exascale.misc.MyDate;

public class CNFFilter implements Serializable
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

	private ArrayList<ArrayList<Filter>> filters = new ArrayList<ArrayList<Filter>>();

	private transient MetaData meta;

	private volatile HashMap<String, Integer> cols2Pos;

	private transient ArrayList<Object> partHash;

	private transient ArrayList<Filter> rangeFilters;

	private volatile HashSet<HashMap<Filter, Filter>> hshm = null;

	private DataEndMarker hshmLock = new DataEndMarker();

	private volatile HashSet<String> references;

	public CNFFilter()
	{
	}

	public CNFFilter(HashSet<HashMap<Filter, Filter>> clause, HashMap<String, Integer> cols2Pos)
	{
		this.cols2Pos = cols2Pos;
		this.setHSHM(clause);
	}

	public CNFFilter(HashSet<HashMap<Filter, Filter>> clause, MetaData meta, HashMap<String, Integer> cols2Pos, HashMap<String, Double> generated, Operator tree) throws Exception
	{
		this.meta = meta;
		this.cols2Pos = cols2Pos;
		this.setHSHM(clause);
	}

	public CNFFilter(HashSet<HashMap<Filter, Filter>> clause, MetaData meta, HashMap<String, Integer> cols2Pos, Operator tree) throws Exception
	{
		this.meta = meta;
		this.cols2Pos = cols2Pos;
		this.setHSHM(clause);
	}

	public CNFFilter(HashSet<HashMap<Filter, Filter>> clause, MetaData meta, HashMap<String, Integer> cols2Pos, RootOperator op) throws Exception
	{
		this(clause, meta, cols2Pos, op.getGenerated(), op);
	}

	public static CNFFilter deserializeKnown(InputStream in, HashMap<Long, Object> prev) throws Exception
	{
		CNFFilter value = (CNFFilter)unsafe.allocateInstance(CNFFilter.class);
		prev.put(OperatorUtils.readLong(in), value);
		value.filters = OperatorUtils.deserializeALALF(in, prev);
		value.cols2Pos = OperatorUtils.deserializeStringIntHM(in, prev);
		value.partHash = OperatorUtils.deserializeALO(in, prev);
		value.rangeFilters = OperatorUtils.deserializeALF(in, prev);
		value.hshm = OperatorUtils.deserializeHSHM(in, prev);
		value.hshmLock = new DataEndMarker();
		value.references = OperatorUtils.deserializeHSS(in, prev);
		return value;
	}

	public static void quicksort(ArrayList main, ArrayList<Double> scores)
	{
		quicksort(main, scores, 0, scores.size() - 1);
	}

	// quicksort a[left] to a[right]
	public static void quicksort(ArrayList<Object> a, ArrayList<Double> scores, int left, int right)
	{
		if (right <= left)
		{
			return;
		}
		final int i = partition(a, scores, left, right);
		quicksort(a, scores, left, i - 1);
		quicksort(a, scores, i + 1, right);
	}

	public static void reverseQuicksort(ArrayList main, ArrayList<Double> scores)
	{
		reverseQuicksort(main, scores, 0, scores.size() - 1);
	}

	// quicksort a[left] to a[right]
	public static void reverseQuicksort(ArrayList<Object> a, ArrayList<Double> scores, int left, int right)
	{
		if (right <= left)
		{
			return;
		}
		final int i = reversePartition(a, scores, left, right);
		reverseQuicksort(a, scores, left, i - 1);
		reverseQuicksort(a, scores, i + 1, right);
	}

	private static boolean areEquivalent(String l, String r)
	{
		String lhs = l;
		String rhs = r;

		if (lhs.contains("."))
		{
			lhs = lhs.substring(lhs.indexOf('.') + 1);
		}

		if (rhs.contains("."))
		{
			rhs = rhs.substring(rhs.indexOf('.') + 1);
		}

		return lhs.equals(rhs);
	}

	// exchange a[i] and a[j]
	private static void exch(ArrayList<Object> a, ArrayList<Double> scores, int i, int j)
	{
		final Object swap1 = a.get(i);
		final Object swap2 = a.get(j);
		final Double swap3 = scores.get(i);
		final Double swap4 = scores.get(j);
		// a[i] = a[j];
		a.remove(i);
		a.add(i, swap2);
		scores.remove(i);
		scores.add(i, swap4);
		// a[j] = swap;
		a.remove(j);
		a.add(j, swap1);
		scores.remove(j);
		scores.add(j, swap3);
	}

	private static long hash(Object key) throws Exception
	{
		long eHash;
		if (key == null)
		{
			eHash = 0;
		}
		else
		{
			if (key instanceof ArrayList)
			{
				byte[] data = toBytesForHash((ArrayList<Object>)key);
				eHash = MurmurHash.hash64(data, data.length);
			}
			else
			{
				byte[] data = key.toString().getBytes(StandardCharsets.UTF_8);
				eHash = MurmurHash.hash64(data, data.length);
			}
		}

		return eHash;
	}
	
	private static byte[] toBytesForHash(ArrayList<Object> key)
	{
		StringBuilder sb = new StringBuilder();
		for (Object o : key)
		{
			if (o instanceof Double)
			{
				DecimalFormat df = new DecimalFormat("0", DecimalFormatSymbols.getInstance(Locale.ENGLISH));
				df.setMaximumFractionDigits(340); //340 = DecimalFormat.DOUBLE_FRACTION_DIGITS

				sb.append(df.format((Double)o));
				sb.append((char)0);
			}
			else if (o instanceof Number)
			{
				sb.append(o);
				sb.append((char)0);
			}
			else
			{
				sb.append(o.toString());
				sb.append((char)0);
			}
		}
		
		final int z = sb.length();
		byte[] retval = new byte[z];
		int i = 0;
		while (i < z)
		{
			retval[i] = (byte)sb.charAt(i);
			i++;
		}
		
		return retval;
	}

	// is x < y ?
	private static boolean less(Double x, Double y)
	{
		return x.compareTo(y) < 0;
	}

	private static boolean more(Double x, Double y)
	{
		return x.compareTo(y) > 0;
	}

	// partition a[left] to a[right], assumes left < right
	private static int partition(ArrayList<Object> a, ArrayList<Double> scores, int left, int right)
	{
		int i = left - 1;
		int j = right;
		while (true)
		{
			while (less(scores.get(++i), scores.get(right)))
			{
				; // a[right] acts as sentinel
			}
			while (less(scores.get(right), scores.get(--j)))
			{
				if (j == left)
				{
					break; // don't go out-of-bounds
				}
			}
			if (i >= j)
			{
				break; // check if pointers cross
			}
			exch(a, scores, i, j); // swap two elements into place
		}
		exch(a, scores, i, right); // swap with partition element
		return i;
	}

	// partition a[left] to a[right], assumes left < right
	private static int reversePartition(ArrayList<Object> a, ArrayList<Double> scores, int left, int right)
	{
		int i = left - 1;
		int j = right;
		while (true)
		{
			while (more(scores.get(++i), scores.get(right)))
			{
				; // a[right] acts as sentinel
			}
			while (more(scores.get(right), scores.get(--j)))
			{
				if (j == left)
				{
					break; // don't go out-of-bounds
				}
			}
			if (i >= j)
			{
				break; // check if pointers cross
			}
			exch(a, scores, i, j); // swap two elements into place
		}
		exch(a, scores, i, right); // swap with partition element
		return i;
	}

	private static final byte[] toBytes(Object v) throws Exception
	{
		ArrayList<byte[]> bytes = null;
		ArrayList<Object> val;
		if (v instanceof ArrayList)
		{
			val = (ArrayList<Object>)v;
		}
		else
		{
			final byte[] retval = new byte[9];
			retval[0] = 0;
			retval[1] = 0;
			retval[2] = 0;
			retval[3] = 5;
			retval[4] = 0;
			retval[5] = 0;
			retval[6] = 0;
			retval[7] = 1;
			retval[8] = 5;
			return retval;
		}

		int size = val.size() + 8;
		final byte[] header = new byte[size];
		int i = 8;
		int z = 0;
		int limit = val.size();
		// for (final Object o : val)
		while (z < limit)
		{
			Object o = val.get(z++);
			if (o instanceof Long)
			{
				header[i] = (byte)0;
				size += 8;
			}
			else if (o instanceof Integer)
			{
				header[i] = (byte)1;
				size += 4;
			}
			else if (o instanceof Double)
			{
				header[i] = (byte)2;
				size += 8;
			}
			else if (o instanceof MyDate)
			{
				header[i] = (byte)3;
				size += 4;
			}
			else if (o instanceof String)
			{
				header[i] = (byte)4;
				byte[] b = ((String)o).getBytes(StandardCharsets.UTF_8);
				size += (4 + b.length);

				if (bytes == null)
				{
					bytes = new ArrayList<byte[]>();
					bytes.add(b);
				}
				else
				{
					bytes.add(b);
				}
			}
			// else if (o instanceof AtomicLong)
			// {
			// header[i] = (byte)6;
			// size += 8;
			// }
			// else if (o instanceof AtomicBigDecimal)
			// {
			// header[i] = (byte)7;
			// size += 8;
			// }
			else if (o instanceof ArrayList)
			{
				if (((ArrayList)o).size() != 0)
				{
					Exception e = new Exception("Non-zero size ArrayList in toBytes()");
					HRDBMSWorker.logger.error("Non-zero size ArrayList in toBytes()", e);
					throw e;
				}
				header[i] = (byte)8;
			}
			else
			{
				HRDBMSWorker.logger.error("Unknown type " + o.getClass() + " in toBytes()");
				HRDBMSWorker.logger.error(o);
				throw new Exception("Unknown type " + o.getClass() + " in toBytes()");
			}

			i++;
		}

		final byte[] retval = new byte[size];
		// System.out.println("In toBytes(), row has " + val.size() +
		// " columns, object occupies " + size + " bytes");
		System.arraycopy(header, 0, retval, 0, header.length);
		i = 8;
		final ByteBuffer retvalBB = ByteBuffer.wrap(retval);
		retvalBB.putInt(size - 4);
		retvalBB.putInt(val.size());
		retvalBB.position(header.length);
		int x = 0;
		z = 0;
		limit = val.size();
		// for (final Object o : val)
		while (z < limit)
		{
			Object o = val.get(z++);
			if (retval[i] == 0)
			{
				retvalBB.putLong((Long)o);
			}
			else if (retval[i] == 1)
			{
				retvalBB.putInt((Integer)o);
			}
			else if (retval[i] == 2)
			{
				retvalBB.putDouble((Double)o);
			}
			else if (retval[i] == 3)
			{
				retvalBB.putInt(((MyDate)o).getTime());
			}
			else if (retval[i] == 4)
			{
				byte[] temp = bytes.get(x);
				x++;
				retvalBB.putInt(temp.length);
				retvalBB.put(temp);
			}
			// else if (retval[i] == 6)
			// {
			// retvalBB.putLong(((AtomicLong)o).get());
			// }
			// else if (retval[i] == 7)
			// {
			// retvalBB.putDouble(((AtomicBigDecimal)o).get().doubleValue());
			// }
			else if (retval[i] == 8)
			{
			}

			i++;
		}

		return retval;
	}

	@Override
	public CNFFilter clone()
	{
		final CNFFilter retval = new CNFFilter();
		retval.filters = cloneFilters();
		retval.meta = meta;
		retval.cols2Pos = cols2Pos;
		return retval;
	}

	public ArrayList<ArrayList<Filter>> cloneFilters()
	{
		final ArrayList<ArrayList<Filter>> retval = new ArrayList<ArrayList<Filter>>(filters.size());
		for (final ArrayList<Filter> list : filters)
		{
			retval.add((ArrayList<Filter>)list.clone());
		}

		return retval;
	}

	public ArrayList<ArrayList<Filter>> getALAL()
	{
		return filters;
	}

	public HashSet<HashMap<Filter, Filter>> getHSHM()
	{
		if (hshm == null)
		{
			synchronized (hshmLock)
			{
				if (hshm == null)
				{
					final HashSet<HashMap<Filter, Filter>> hshmTemp = new HashSet<HashMap<Filter, Filter>>();
					// release
					for (final ArrayList<Filter> filter : filters)
					{
						final HashMap<Filter, Filter> hm = new HashMap<Filter, Filter>();
						for (final Filter f : filter)
						{
							hm.put(f, f);
						}

						hshmTemp.add(hm);
					}

					hshm = hshmTemp;
				}
			}
		}

		return hshm;
	}

	public long getPartitionHash() throws Exception
	{
		return 0x7FFFFFFFFFFFFFFFL & hash(partHash);
	}

	public ArrayList<Filter> getRangeFilters()
	{
		return rangeFilters;
	}

	public ArrayList<String> getReferences()
	{
		final ArrayList<String> retval = new ArrayList<String>(filters.size());
		for (final ArrayList<Filter> f : filters)
		{
			for (final Filter filter : f)
			{
				if (filter.leftIsColumn())
				{
					if (!retval.contains(filter.leftColumn()))
					{
						retval.add(filter.leftColumn());
					}
				}

				if (filter.rightIsColumn())
				{
					if (!retval.contains(filter.rightColumn()))
					{
						retval.add(filter.rightColumn());
					}
				}
			}
		}

		return retval;
	}

	public HashSet<String> getReferencesHash()
	{
		if (references == null)
		{
			final HashSet<String> retval = new HashSet<String>(filters.size());
			for (final ArrayList<Filter> f : filters)
			{
				for (final Filter filter : f)
				{
					if (filter.leftIsColumn())
					{
						retval.add(filter.leftColumn());
					}

					if (filter.rightIsColumn())
					{
						retval.add(filter.rightColumn());
					}
				}
			}

			references = retval;
		}

		return references;
	}

	public boolean hashFiltersPartitions(ArrayList<String> hashCols)
	{
		partHash = null;
		// true if CNFFilter has an equality op (compared to literal) for each
		// col in hashCols that is not ored with anything else
		boolean retval;
		for (final String hashCol : hashCols)
		{
			retval = false;
			for (final ArrayList<Filter> filter : filters)
			{
				if (filter.size() == 1)
				{
					final Filter f = filter.get(0);
					if (f.op().equals("E"))
					{
						if (f.leftIsColumn() && !f.rightIsColumn())
						{
							if (areEquivalent(f.leftColumn(), hashCol))
							{
								if (partHash == null)
								{
									partHash = new ArrayList<Object>();
								}
								retval = true;
								partHash.add(f.rightLiteral());
								break;
							}
						}

						if (!f.leftIsColumn() && f.rightIsColumn())
						{
							if (areEquivalent(f.rightColumn(), hashCol))
							{
								if (partHash == null)
								{
									partHash = new ArrayList<Object>();
								}
								retval = true;
								partHash.add(f.leftLiteral());
								break;
							}
						}
					}
				}
			}

			if (!retval)
			{
				return false;
			}
		}

		return true;
	}

	// @Parallel
	public boolean passes(ArrayList<Object> row) throws Exception
	{
		int z = 0;
		final int limit = filters.size();
		// for (final ArrayList<Filter> filter : filters)
		while (z < limit)
		{
			final ArrayList<Filter> filter = filters.get(z++);
			if (!passesOredCondition(filter, row))
			{
				return false;
			}
		}

		return true;
	}

	// @Parallel
	public boolean passes(ArrayList<Object> lRow, ArrayList<Object> rRow) throws Exception
	{
		int z = 0;
		final int limit = filters.size();
		// for (final ArrayList<Filter> filter : filters)
		while (z < limit)
		{
			final ArrayList<Filter> filter = filters.get(z++);
			if (!passesOredCondition(filter, lRow, rRow))
			{
				return false;
			}
		}

		return true;
	}

	public boolean rangeFiltersPartitions(String col)
	{
		rangeFilters = null;
		boolean retval = false;
		// true if CNFFilter has a L, LE, E, G, or GE op (compared to literal)
		// on col that is not ored with anything else
		for (final ArrayList<Filter> filter : filters)
		{
			if (filter.size() == 1)
			{
				final Filter f = filter.get(0);
				if (f.op().equals("L") || f.op().equals("LE") || f.op().equals("E") || f.op().equals("G") || f.op().equals("GE"))
				{
					if (f.leftIsColumn() && !f.rightIsColumn())
					{
						if (areEquivalent(f.leftColumn(), col))
						{
							if (rangeFilters == null)
							{
								rangeFilters = new ArrayList<Filter>();
							}

							rangeFilters.add(f);
							retval = true;
						}
					}

					if (!f.leftIsColumn() && f.rightIsColumn())
					{
						if (areEquivalent(f.rightColumn(), col))
						{
							if (rangeFilters == null)
							{
								rangeFilters = new ArrayList<Filter>();
							}

							rangeFilters.add(f);
							retval = true;
						}
					}
				}
			}
		}

		return retval;
	}

	public void serialize(OutputStream out, IdentityHashMap<Object, Long> prev) throws Exception
	{
		Long id = prev.get(this);
		if (id != null)
		{
			OperatorUtils.serializeReference(id, out);
			return;
		}

		OperatorUtils.writeType(63, out);
		prev.put(this, OperatorUtils.writeID(out));
		OperatorUtils.serializeALALF(filters, out, prev);
		OperatorUtils.serializeStringIntHM(cols2Pos, out, prev);
		OperatorUtils.serializeALO(partHash, out, prev);
		OperatorUtils.serializeALF(rangeFilters, out, prev);
		OperatorUtils.serializeHSHM(hshm, out, prev);
		// recreate hshmLock
		OperatorUtils.serializeHSS(references, out, prev);
	}

	public void setHSHM(HashSet<HashMap<Filter, Filter>> hshm)
	{
		filters = new ArrayList<ArrayList<Filter>>(hshm.size());
		for (final HashMap<Filter, Filter> hm : hshm)
		{
			final ArrayList<Filter> ors = new ArrayList<Filter>(hm.keySet());
			filters.add(ors);
			synchronized (hshmLock)
			{
				this.hshm = hshm;
			}
		}
	}

	@Override
	public String toString()
	{
		if (!(this instanceof NullCNFFilter))
		{
			return filters.toString();
		}

		return "NullCNFFilter";
	}

	public void updateCols2Pos(HashMap<String, Integer> cols2Pos)
	{
		this.cols2Pos = cols2Pos;
	}

	private final boolean passesOredCondition(ArrayList<Filter> filter, ArrayList<Object> row) throws Exception
	{
		try
		{
			int z = 0;
			final int limit = filter.size();
			// for (final Filter f : filter)
			while (z < limit)
			{
				final Filter f = filter.get(z++);
				if (f.passes(row, cols2Pos))
				{
					return true;
				}
			}
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.error("", e);
			throw e;
		}

		return false;
	}

	private final boolean passesOredCondition(ArrayList<Filter> filter, ArrayList<Object> lRow, ArrayList<Object> rRow) throws Exception
	{
		try
		{
			int z = 0;
			final int limit = filter.size();
			// for (final Filter f : filter)
			while (z < limit)
			{
				final Filter f = filter.get(z++);
				if (f.passes(lRow, rRow, cols2Pos))
				{
					return true;
				}
			}
		}
		catch (final Exception e)
		{
			HRDBMSWorker.logger.error("", e);
			throw e;
		}

		return false;
	}
}
