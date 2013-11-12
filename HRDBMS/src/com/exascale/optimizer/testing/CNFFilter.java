package com.exascale.optimizer.testing;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;

public class CNFFilter implements Serializable
{
	private ArrayList<ArrayList<Filter>> filters = new ArrayList<ArrayList<Filter>>();
	private MetaData meta;
	private HashMap<String, Integer> cols2Pos;
	private ArrayList<Object> partHash;
	private ArrayList<Filter> rangeFilters;
	protected static final Long LARGE_PRIME =  1125899906842597L;
    protected static final Long LARGE_PRIME2 = 6920451961L;
	
	public long getPartitionHash()
	{
		return 0x0EFFFFFFFFFFFFFFL & hash(partHash);
	}
	
	private long hash(ArrayList<Object> key)
	{
		long hashCode = 1125899906842597L;
		for (Object e : key)
		{
			long eHash = 1;
			if (e instanceof Integer)
			{
				long i = ((Integer)e).longValue();
				// Spread out values
			    long scaled = i * LARGE_PRIME;

			    // Fill in the lower bits
			    eHash = scaled + LARGE_PRIME2;
			}
			else if (e instanceof Long)
			{
				long i = (Long)e;
				// Spread out values
			    long scaled = i * LARGE_PRIME;

			    // Fill in the lower bits
			    eHash = scaled + LARGE_PRIME2;
			}
			else if (e instanceof String)
			{
				String string = (String)e;
				  long h = 1125899906842597L; // prime
				  int len = string.length();

				  for (int i = 0; i < len; i++) 
				  {
					   h = 31*h + string.charAt(i);
				  }
				  eHash = h;
			}
			else if (e instanceof Double)
			{
				long i = Double.doubleToLongBits((Double)e);
				// Spread out values
			    long scaled = i * LARGE_PRIME;

			    // Fill in the lower bits
			    eHash = scaled + LARGE_PRIME2;
			}
			else if (e instanceof Date)
			{
				long i = ((Date)e).getTime();
				// Spread out values
			    long scaled = i * LARGE_PRIME;

			    // Fill in the lower bits
			    eHash = scaled + LARGE_PRIME2;
			}
			else
			{
				eHash = e.hashCode();
			}
			
		    hashCode = 31*hashCode + (e==null ? 0 : eHash);
		}
		return hashCode;
	}
	
	public ArrayList<Filter> getRangeFilters()
	{
		return rangeFilters;
	}
	
	public boolean rangeFiltersPartitions(String col)
	{
		rangeFilters = null;
		boolean retval = false;
		//true if CNFFilter has a L, LE, E, G, or GE op (compared to literal) on col that is not ored with anything else
		for (ArrayList<Filter> filter : filters)
		{
			if (filter.size() == 1)
			{
				Filter f = filter.get(0);
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
	
	public boolean hashFiltersPartitions(ArrayList<String> hashCols)
	{
		partHash = null;
		//true if CNFFilter has an equality op (compared to literal) for each col in hashCols that is not ored with anything else
		boolean retval;
		for (String hashCol : hashCols)
		{
			retval = false;
			for (ArrayList<Filter> filter : filters)
			{
				if (filter.size() == 1)
				{
					Filter f = filter.get(0);
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
	
	public void updateCols2Pos(HashMap<String, Integer> cols2Pos)
	{
		this.cols2Pos = cols2Pos;
	}
	
	public ArrayList<String> getReferences()
	{
		ArrayList<String> retval = new ArrayList<String>();
		for (ArrayList<Filter> f : filters)
		{
			for (Filter filter : f)
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
		
		return retval;
	}
	
	public boolean passes(ArrayList<Object> row)
	{
		for (ArrayList<Filter> filter : filters)
		{
			if (!passesOredCondition(filter, row))
			{
				return false;
			}
		}
		
		return true;
	}
	
	public boolean passes(ArrayList<Object> lRow, ArrayList<Object> rRow)
	{
		for (ArrayList<Filter> filter : filters)
		{
			if (!passesOredCondition(filter, lRow, rRow))
			{
				return false;
			}
		}
		
		return true;
	}
	
	private boolean passesOredCondition(ArrayList<Filter> filter, ArrayList<Object> row)
	{
		try
		{
			for (Filter f : filter)
			{
				if (f.passes(row, cols2Pos))
				{
					return true;
				}
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
			System.exit(1);
		}
		
		return false;
	}
	
	private boolean passesOredCondition(ArrayList<Filter> filter, ArrayList<Object> lRow, ArrayList<Object> rRow)
	{
		try
		{
			for (Filter f : filter)
			{
				if (f.passes(lRow, rRow, cols2Pos))
				{
					return true;
				}
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
			System.exit(1);
		}
		
		return false;
	}
	
	public CNFFilter()
	{}
	
	public CNFFilter(HashSet<HashMap<Filter, Filter>> clause, MetaData meta, HashMap<String, Integer> cols2Pos, RootOperator op)
	{
		this(clause, meta, cols2Pos, op.getGenerated());
	}
	
	public CNFFilter(HashSet<HashMap<Filter, Filter>> clause, MetaData meta, HashMap<String, Integer> cols2Pos, HashMap<String, Double> generated)
	{
		this.meta = meta;
		this.cols2Pos = cols2Pos;
		
		for (HashMap<Filter, Filter> ored : clause)
		{
			ArrayList<Filter> oredArray = new ArrayList<Filter>();
			ArrayList<Double> scores = new ArrayList<Double>();
			for (Filter filter : ored.keySet())
			{
				oredArray.add(filter);
				scores.add(meta.likelihood(filter, generated)); //more likely = better
			}
			
			//sort oredArray and scores
			reverseQuicksort(oredArray, scores);
			filters.add(oredArray);
		}
		
		ArrayList<Double> scores = new ArrayList<Double>();
		for (ArrayList<Filter> ored : filters)
		{
			scores.add(meta.likelihood(ored, generated)); //less likely = better
		}
		
		//sort filters and scores
		quicksort(filters, scores);
	}
	
	public CNFFilter(HashSet<HashMap<Filter, Filter>> clause, MetaData meta, HashMap<String, Integer> cols2Pos)
	{
		this.meta = meta;
		this.cols2Pos = cols2Pos;
		
		for (HashMap<Filter, Filter> ored : clause)
		{
			ArrayList<Filter> oredArray = new ArrayList<Filter>();
			ArrayList<Double> scores = new ArrayList<Double>();
			for (Filter filter : ored.keySet())
			{
				oredArray.add(filter);
				scores.add(meta.likelihood(filter));
			}
			
			//sort oredArray and scores
			reverseQuicksort(oredArray, scores);
			filters.add(oredArray);
		}
		
		ArrayList<Double> scores = new ArrayList<Double>();
		for (ArrayList<Filter> ored : filters)
		{
			scores.add(meta.likelihood(ored));
		}
		
		//sort filters and scores
		quicksort(filters, scores);
	}
	
	public static void quicksort(ArrayList main, ArrayList<Double> scores) {
	    quicksort(main, scores, 0, scores.size() - 1);
	}
	
	public static void reverseQuicksort(ArrayList main, ArrayList<Double> scores) {
	    reverseQuicksort(main, scores, 0, scores.size() - 1);
	}

	// quicksort a[left] to a[right]
	public static void quicksort(ArrayList<Object> a, ArrayList<Double> scores, int left, int right)
	{
	    if (right <= left) return;
	    int i = partition(a, scores, left, right);
	    quicksort(a, scores, left, i-1);
	    quicksort(a, scores, i+1, right);
	}
	
	// quicksort a[left] to a[right]
		public static void reverseQuicksort(ArrayList<Object> a, ArrayList<Double> scores, int left, int right)
		{
		    if (right <= left) return;
		    int i = reversePartition(a, scores, left, right);
		    reverseQuicksort(a, scores, left, i-1);
		    reverseQuicksort(a, scores, i+1, right);
		}

	// partition a[left] to a[right], assumes left < right
	private static int partition(ArrayList<Object> a, ArrayList<Double> scores,	int left, int right) 
	{
	    int i = left - 1;
	    int j = right;
	    while (true) {
	        while (less(scores.get(++i), scores.get(right)))      // find item on left to swap
	            ;                               // a[right] acts as sentinel
	        while (less(scores.get(right), scores.get(--j)))      // find item on right to swap
	            if (j == left) break;           // don't go out-of-bounds
	        if (i >= j) break;                  // check if pointers cross
	        exch(a, scores, i, j);               // swap two elements into place
	    }
	    exch(a, scores, i, right);               // swap with partition element
	    return i;
	}
	
	// partition a[left] to a[right], assumes left < right
		private static int reversePartition(ArrayList<Object> a, ArrayList<Double> scores,	int left, int right) 
		{
		    int i = left - 1;
		    int j = right;
		    while (true) {
		        while (more(scores.get(++i), scores.get(right)))      // find item on left to swap
		            ;                               // a[right] acts as sentinel
		        while (more(scores.get(right), scores.get(--j)))      // find item on right to swap
		            if (j == left) break;           // don't go out-of-bounds
		        if (i >= j) break;                  // check if pointers cross
		        exch(a, scores, i, j);               // swap two elements into place
		    }
		    exch(a, scores, i, right);               // swap with partition element
		    return i;
		}

	// is x < y ?
	private static boolean less(Double x, Double y) {
	    return x.compareTo(y) < 0;
	}
	
	private static boolean more(Double x, Double y) {
	    return x.compareTo(y) > 0;
	}

	// exchange a[i] and a[j]
	private static void exch(ArrayList<Object> a, ArrayList<Double> scores, int i, int j) {
	    Object swap1 = a.get(i);
	    Object swap2 = a.get(j);
	    Double swap3 = scores.get(i);
	    Double swap4 = scores.get(j);
	    //a[i] = a[j];
	    a.remove(i);
	    a.add(i, swap2);
	    scores.remove(i);
	    scores.add(i, swap4);
	    //a[j] = swap;
	    a.remove(j);
	    a.add(j, swap1);
	    scores.remove(j);
	    scores.add(j, swap3);
	}
	public String toString()
	{
		if (! (this instanceof NullCNFFilter))
		{
			return filters.toString();
		}
		
		return "NullCNFFilter";
	}
}
