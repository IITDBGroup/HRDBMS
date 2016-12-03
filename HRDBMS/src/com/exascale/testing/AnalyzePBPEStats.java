package com.exascale.testing;

import java.io.File;
import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import com.exascale.optimizer.CNFFilter;
import com.exascale.optimizer.Filter;

public class AnalyzePBPEStats
{
	private static ConcurrentHashMap<HashSet<HashMap<Filter, Filter>>, AtomicLong> noResultCounts;
	
	public static void main(String[] args)
	{
		final File file2 = new File("pbpe.stats");
		if (file2.exists())
		{
			try
			{
				ObjectInputStream in2 = new ObjectInputStream(new FileInputStream("pbpe.stats"));
				noResultCounts = (ConcurrentHashMap<HashSet<HashMap<Filter, Filter>>, AtomicLong>)in2.readObject();
			}
			catch(Exception e)
			{
				noResultCounts = new ConcurrentHashMap<HashSet<HashMap<Filter, Filter>>, AtomicLong>();
			}
		}
		else
		{
			noResultCounts = new ConcurrentHashMap<HashSet<HashMap<Filter, Filter>>, AtomicLong>();
		}
		
		Map<HashSet<HashMap<Filter, Filter>>, AtomicLong> sorted = sortByValue(noResultCounts);
		ArrayList<HashSet<HashMap<Filter, Filter>>> l_hshms = new ArrayList<HashSet<HashMap<Filter, Filter>>>();
		ArrayList<HashSet<HashMap<Filter, Filter>>> o_hshms = new ArrayList<HashSet<HashMap<Filter, Filter>>>();
		ArrayList<HashSet<HashMap<Filter, Filter>>> ps_hshms = new ArrayList<HashSet<HashMap<Filter, Filter>>>();
		ArrayList<HashSet<HashMap<Filter, Filter>>> p_hshms = new ArrayList<HashSet<HashMap<Filter, Filter>>>();
		ArrayList<HashSet<HashMap<Filter, Filter>>> s_hshms = new ArrayList<HashSet<HashMap<Filter, Filter>>>();
		ArrayList<HashSet<HashMap<Filter, Filter>>> c_hshms = new ArrayList<HashSet<HashMap<Filter, Filter>>>();
		for (HashSet<HashMap<Filter, Filter>> hshm : sorted.keySet())
		{
			String colName = null;
			for (HashMap<Filter, Filter> hm : hshm)
			{
				for (Filter f : hm.keySet())
				{
					colName = f.leftColumn();
					break;
				}
				
				break;
			}
			
			if (colName.contains("."))
			{
				colName = colName.substring(colName.indexOf('.') + 1);
			}
			
			if (colName.startsWith("L_"))
			{
				//lineitem
				String not = CNFFilter.notTrue(l_hshms);
				String pos = CNFFilter.isTrue(hshm);
				if (pos.length() < 200)
				{
					String sql = "INSERT INTO TPCH3.LINEITEM SELECT * FROM TPCH2.LINEITEM WHERE " + pos;
					if (not != null)
					{
						sql += (" AND " + not);
					}
				
					l_hshms.add(hshm);
					System.out.println(sql);
				}
			}
			else if (colName.startsWith("O_"))
			{
				String not = CNFFilter.notTrue(o_hshms);
				String pos = CNFFilter.isTrue(hshm);
				if (pos.length() < 200)
				{
					String sql = "INSERT INTO TPCH3.ORDERS SELECT * FROM TPCH2.ORDERS WHERE " + pos;
					if (not != null)
					{
						sql += (" AND " + not);
					}
				
					o_hshms.add(hshm);
					System.out.println(sql);
				}
			}
			else if (colName.startsWith("PS_"))
			{
				String not = CNFFilter.notTrue(ps_hshms);
				String pos = CNFFilter.isTrue(hshm);
				if (pos.length() < 200)
				{
					String sql = "INSERT INTO TPCH3.PARTSUPP SELECT * FROM TPCH2.PARTSUPP WHERE " + pos;
					if (not != null)
					{
						sql += (" AND " + not);
					}
				
					ps_hshms.add(hshm);
					System.out.println(sql);
				}
			}
			else if (colName.startsWith("P_"))
			{
				String not = CNFFilter.notTrue(p_hshms);
				String pos = CNFFilter.isTrue(hshm);
				if (pos.length() < 200)
				{
					String sql = "INSERT INTO TPCH3.PART SELECT * FROM TPCH2.PART WHERE " + pos;
					if (not != null)
					{
						sql += (" AND " + not);
					}
				
					p_hshms.add(hshm);
					System.out.println(sql);
				}
			}
			else if (colName.startsWith("S_"))
			{
				String not = CNFFilter.notTrue(s_hshms);
				String pos = CNFFilter.isTrue(hshm);
				if (pos.length() < 200)
				{
					String sql = "INSERT INTO TPCH3.SUPPLIER SELECT * FROM TPCH2.SUPPLIER WHERE " + pos;
					if (not != null)
					{
						sql += (" AND " + not);
					}
				
					s_hshms.add(hshm);
					System.out.println(sql);
				}
			}
			else if (colName.startsWith("C_"))
			{
				String not = CNFFilter.notTrue(c_hshms);
				String pos = CNFFilter.isTrue(hshm);
				if (pos.length() < 200)
				{
					String sql = "INSERT INTO TPCH3.CUSTOMER SELECT * FROM TPCH2.CUSTOMER WHERE " + pos;
					if (not != null)
					{
						sql += (" AND " + not);
					}
				
					c_hshms.add(hshm);
					System.out.println(sql);
				}
			}
		}
		
		String sql = "INSERT INTO TPCH3.LINEITEM SELECT * FROM TPCH2.LINEITEM";
		if (!l_hshms.isEmpty())
		{
			sql += (" WHERE " + CNFFilter.notTrue(l_hshms));
		}
		
		System.out.println(sql);
		
		sql = "INSERT INTO TPCH3.ORDERS SELECT * FROM TPCH2.ORDERS";
		if (!o_hshms.isEmpty())
		{
			sql += (" WHERE " + CNFFilter.notTrue(o_hshms));
		}
		
		System.out.println(sql);
		
		sql = "INSERT INTO TPCH3.PARTSUPP SELECT * FROM TPCH2.PARTSUPP";
		if (!ps_hshms.isEmpty())
		{
			sql += (" WHERE " + CNFFilter.notTrue(ps_hshms));
		}
		
		System.out.println(sql);
		
		sql = "INSERT INTO TPCH3.PART SELECT * FROM TPCH2.PART";
		if (!p_hshms.isEmpty())
		{
			sql += (" WHERE " + CNFFilter.notTrue(p_hshms));
		}
		
		System.out.println(sql);
		
		sql = "INSERT INTO TPCH3.SUPPLIER SELECT * FROM TPCH2.SUPPLIER";
		if (!s_hshms.isEmpty())
		{
			sql += (" WHERE " + CNFFilter.notTrue(s_hshms));
		}
		
		System.out.println(sql);
		
		sql = "INSERT INTO TPCH3.CUSTOMER SELECT * FROM TPCH2.CUSTOMER";
		if (!c_hshms.isEmpty())
		{
			sql += (" WHERE " + CNFFilter.notTrue(c_hshms));
		}
		
		System.out.println(sql);
	}
	
	 public static Map<HashSet<HashMap<Filter, Filter>>, AtomicLong> 
     sortByValue( ConcurrentHashMap<HashSet<HashMap<Filter, Filter>>, AtomicLong> map )
 {
     List<Map.Entry<HashSet<HashMap<Filter, Filter>>, AtomicLong>> list =
         new LinkedList<Map.Entry<HashSet<HashMap<Filter, Filter>>, AtomicLong>>( map.entrySet() );
     Collections.sort( list, new Comparator<Map.Entry<HashSet<HashMap<Filter, Filter>>, AtomicLong>>()
     {
         public int compare( Map.Entry<HashSet<HashMap<Filter, Filter>>, AtomicLong> o1, Map.Entry<HashSet<HashMap<Filter, Filter>>, AtomicLong> o2 )
         {
             return -1 * Long.compare(o1.getValue().longValue(), o2.getValue().longValue());
         }
     } );

     Map<HashSet<HashMap<Filter, Filter>>, AtomicLong> result = new LinkedHashMap<HashSet<HashMap<Filter, Filter>>, AtomicLong>();
     for (Map.Entry<HashSet<HashMap<Filter, Filter>>, AtomicLong> entry : list)
     {
         result.put( entry.getKey(), entry.getValue() );
     }
     return result;
 }
}
