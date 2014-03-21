package com.exascale.managers;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import com.exascale.tables.Plan;
import com.exascale.tables.SQL;

public class PlanCacheManager
{
	private static ConcurrentHashMap<SQL, Plan> planCache = new ConcurrentHashMap<SQL, Plan>();

	// Plans have creation timestamp and reserved flag

	public PlanCacheManager()
	{
		// TODO add catalog plans
	}

	public static void addPlan(String sql, Plan p)
	{
		planCache.put(new SQL(sql), p);
	}

	public static Plan checkPlanCache(String sql)
	{
		final Plan plan = planCache.get(new SQL(sql));
		if (plan == null)
		{
			return null;
		}
		
		return new Plan(plan);
	}

	public static void reduce()
	{
		double avg = 0;
		long num = -1;
		for (final Plan p : planCache.values())
		{
			long newNum;
			if (num == -1)
			{
				newNum = 1;
			}
			else
			{
				newNum = num + 1;
			}

			avg = avg / (newNum * 1.0 / num) + p.getTimeStamp() / newNum - avg;
			num = newNum;
		}

		for (final Map.Entry<SQL, Plan> entry : planCache.entrySet())
		{
			final Plan p = entry.getValue();
			if (!p.isReserved())
			{
				if (p.getTimeStamp() < (long)avg)
				{
					planCache.remove(entry.getKey());
				}
			}
		}
	}
}
