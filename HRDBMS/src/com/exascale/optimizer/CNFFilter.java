package com.exascale.optimizer;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.misc.DataEndMarker;
import com.exascale.misc.MurmurHash;
import com.exascale.tables.Transaction;

public class CNFFilter implements Serializable
{
	private ArrayList<ArrayList<Filter>> filters = new ArrayList<ArrayList<Filter>>();

	private MetaData meta;

	private HashMap<String, Integer> cols2Pos;

	private ArrayList<Object> partHash;

	private ArrayList<Filter> rangeFilters;

	private volatile HashSet<HashMap<Filter, Filter>> hshm = null;

	private final DataEndMarker hshmLock = new DataEndMarker();

	public CNFFilter()
	{
	}

	public CNFFilter(HashSet<HashMap<Filter, Filter>> clause, HashMap<String, Integer> cols2Pos)
	{
		this.cols2Pos = cols2Pos;
		this.setHSHM(clause);
	}

	public CNFFilter(HashSet<HashMap<Filter, Filter>> clause, MetaData meta, HashMap<String, Integer> cols2Pos, Transaction tx, Operator tree) throws Exception
	{
		this.meta = meta;
		this.cols2Pos = cols2Pos;
		this.setHSHM(clause);
	}

	public CNFFilter(HashSet<HashMap<Filter, Filter>> clause, MetaData meta, HashMap<String, Integer> cols2Pos, HashMap<String, Double> generated, Transaction tx, Operator tree) throws Exception
	{
		this.meta = meta;
		this.cols2Pos = cols2Pos;
		this.setHSHM(clause);
	}

	public CNFFilter(HashSet<HashMap<Filter, Filter>> clause, MetaData meta, HashMap<String, Integer> cols2Pos, RootOperator op, Transaction tx) throws Exception
	{
		this(clause, meta, cols2Pos, op.getGenerated(), tx, op);
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

	public long getPartitionHash()
	{
		return 0x0EFFFFFFFFFFFFFFL & hash(partHash);
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
						if (f.leftIsColumn() && f.leftColumn().equals(hashCol) && !f.rightIsColumn())
						{
							if (partHash == null)
							{
								partHash = new ArrayList<Object>();
							}
							retval = true;
							partHash.add(f.rightLiteral());
							break;
						}

						if (!f.leftIsColumn() && f.rightIsColumn() && f.rightColumn().equals(hashCol))
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
		for (final ArrayList<Filter> filter : filters)
		{
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
		for (final ArrayList<Filter> filter : filters)
		{
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
					if (f.leftIsColumn() && f.leftColumn().equals(col) && !f.rightIsColumn())
					{
						if (rangeFilters == null)
						{
							rangeFilters = new ArrayList<Filter>();
						}

						rangeFilters.add(f);
						retval = true;
					}

					if (!f.leftIsColumn() && f.rightIsColumn() && f.rightColumn().equals(col))
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

		return retval;
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

	private long hash(Object key)
	{
		long eHash;
		if (key == null)
		{
			eHash = 0;
		}
		else
		{
			eHash = MurmurHash.hash64(key.toString());
		}

		return eHash;
	}

	private final boolean passesOredCondition(ArrayList<Filter> filter, ArrayList<Object> row) throws Exception
	{
		try
		{
			for (final Filter f : filter)
			{
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
			for (final Filter f : filter)
			{
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
