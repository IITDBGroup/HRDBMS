package com.exascale.testing;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ColOrderP
{
	public static void main(final String[] args)
	{
		// supplier
		List<Integer> columns = new ArrayList<Integer>();
		columns.add(0);
		columns.add(1);
		columns.add(2);
		columns.add(3);
		columns.add(4);
		columns.add(5);
		columns.add(6);
		List<List<Integer>> accesses = new ArrayList<List<Integer>>();
		List<Integer> access = new ArrayList<Integer>();
		access.add(0);
		access.add(1);
		access.add(2);
		access.add(3);
		access.add(4);
		access.add(5);
		access.add(6);
		accesses.add(access);
		access = new ArrayList<Integer>();
		access.add(0);
		access.add(3);
		accesses.add(access);
		access = new ArrayList<Integer>();
		access.add(0);
		access.add(3);
		accesses.add(access);
		access = new ArrayList<Integer>();
		access.add(0);
		access.add(3);
		accesses.add(access);
		access = new ArrayList<Integer>();
		access.add(0);
		access.add(3);
		accesses.add(access);
		access = new ArrayList<Integer>();
		access.add(0);
		access.add(3);
		accesses.add(access);
		access = new ArrayList<Integer>();
		access.add(0);
		access.add(3);
		accesses.add(access);
		access = new ArrayList<Integer>();
		access.add(0);
		access.add(3);
		accesses.add(access);
		access = new ArrayList<Integer>();
		access.add(0);
		access.add(1);
		access.add(2);
		access.add(4);
		accesses.add(access);
		access = new ArrayList<Integer>();
		access.add(0);
		access.add(6);
		accesses.add(access);
		access = new ArrayList<Integer>();
		access.add(0);
		access.add(1);
		access.add(2);
		access.add(3);
		accesses.add(access);
		access = new ArrayList<Integer>();
		access.add(0);
		access.add(1);
		access.add(3);
		accesses.add(access);

		List<Integer> result = doIt(columns, accesses);
		displayResults("SUPPLIER", result);

		columns = new ArrayList<Integer>();
		columns.add(0);
		columns.add(1);
		columns.add(2);
		columns.add(3);
		columns.add(4);
		accesses = new ArrayList<List<Integer>>();
		access = new ArrayList<Integer>();
		access.add(0);
		access.add(1);
		access.add(3);
		accesses.add(access);
		access = new ArrayList<Integer>();
		access.add(0);
		access.add(1);
		access.add(3);
		accesses.add(access);
		access = new ArrayList<Integer>();
		access.add(0);
		access.add(1);
		access.add(2);
		access.add(3);
		accesses.add(access);
		access = new ArrayList<Integer>();
		access.add(1);
		access.add(2);
		access.add(3);
		accesses.add(access);
		access = new ArrayList<Integer>();
		access.add(0);
		access.add(1);
		accesses.add(access);
		access = new ArrayList<Integer>();
		access.add(0);
		access.add(1);
		access.add(2);
		accesses.add(access);

		result = doIt(columns, accesses);
		displayResults("PARTSUPP", result);

		columns = new ArrayList<Integer>();
		columns.add(0);
		columns.add(1);
		columns.add(2);
		columns.add(3);
		columns.add(4);
		columns.add(5);
		columns.add(6);
		columns.add(7);
		accesses = new ArrayList<List<Integer>>();
		access = new ArrayList<Integer>();
		access.add(0);
		access.add(6);
		accesses.add(access);
		access = new ArrayList<Integer>();
		access.add(0);
		access.add(3);
		accesses.add(access);
		access = new ArrayList<Integer>();
		access.add(0);
		access.add(3);
		accesses.add(access);
		access = new ArrayList<Integer>();
		access.add(0);
		access.add(3);
		accesses.add(access);
		access = new ArrayList<Integer>();
		access.add(0);
		access.add(1);
		access.add(2);
		access.add(3);
		access.add(4);
		access.add(5);
		access.add(6);
		accesses.add(access);
		access = new ArrayList<Integer>();
		access.add(0);
		access.add(1);
		accesses.add(access);
		access = new ArrayList<Integer>();
		access.add(4);
		access.add(5);
		accesses.add(access);
		access = new ArrayList<Integer>();
		access.add(0);
		access.add(4);
		access.add(5);
		accesses.add(access);

		result = doIt(columns, accesses);
		displayResults("CUSTOMER", result);

		columns = new ArrayList<Integer>();
		columns.add(0);
		columns.add(1);
		columns.add(2);
		columns.add(3);
		columns.add(4);
		columns.add(5);
		columns.add(6);
		columns.add(7);
		columns.add(8);
		accesses = new ArrayList<List<Integer>>();
		access = new ArrayList<Integer>();
		access.add(0);
		access.add(2);
		access.add(4);
		access.add(5);
		accesses.add(access);
		access = new ArrayList<Integer>();
		access.add(0);
		accesses.add(access);
		access = new ArrayList<Integer>();
		access.add(0);
		access.add(4);
		accesses.add(access);
		access = new ArrayList<Integer>();
		access.add(0);
		access.add(1);
		accesses.add(access);
		access = new ArrayList<Integer>();
		access.add(0);
		access.add(4);
		accesses.add(access);
		access = new ArrayList<Integer>();
		access.add(0);
		access.add(3);
		access.add(4);
		access.add(5);
		accesses.add(access);
		access = new ArrayList<Integer>();
		access.add(0);
		access.add(3);
		access.add(6);
		accesses.add(access);
		access = new ArrayList<Integer>();
		access.add(0);
		access.add(3);
		access.add(5);
		access.add(6);
		accesses.add(access);
		access = new ArrayList<Integer>();
		access.add(0);
		access.add(1);
		accesses.add(access);

		result = doIt(columns, accesses);
		displayResults("PART", result);

		columns = new ArrayList<Integer>();
		columns.add(0);
		columns.add(1);
		columns.add(2);
		columns.add(3);
		columns.add(4);
		columns.add(5);
		columns.add(6);
		columns.add(7);
		columns.add(8);
		accesses = new ArrayList<List<Integer>>();
		access = new ArrayList<Integer>();
		access.add(0);
		access.add(1);
		access.add(4);
		access.add(7);
		accesses.add(access);
		access = new ArrayList<Integer>();
		access.add(0);
		access.add(4);
		access.add(5);
		accesses.add(access);
		access = new ArrayList<Integer>();
		access.add(0);
		access.add(1);
		access.add(4);
		accesses.add(access);
		access = new ArrayList<Integer>();
		access.add(0);
		access.add(1);
		accesses.add(access);
		access = new ArrayList<Integer>();
		access.add(0);
		access.add(1);
		access.add(4);
		accesses.add(access);
		access = new ArrayList<Integer>();
		access.add(0);
		access.add(4);
		accesses.add(access);
		access = new ArrayList<Integer>();
		access.add(0);
		access.add(1);
		access.add(4);
		accesses.add(access);
		access = new ArrayList<Integer>();
		access.add(0);
		access.add(5);
		accesses.add(access);
		access = new ArrayList<Integer>();
		access.add(0);
		access.add(1);
		access.add(3);
		access.add(4);
		accesses.add(access);
		access = new ArrayList<Integer>();
		access.add(0);
		access.add(2);
		accesses.add(access);
		access = new ArrayList<Integer>();
		access.add(1);
		accesses.add(access);
		final long start = System.currentTimeMillis();
		result = doIt(columns, accesses);
		final long end = System.currentTimeMillis();
		System.out.println((end - start) + "ms");
		displayResults("ORDERS", result);
	}

	private static void displayResults(final String table, final List<Integer> result)
	{
		String out = table + " = COLORDER(" + (result.get(0) + 1);
		int i = 1;
		while (i < result.size())
		{
			out += ("," + (result.get(i++) + 1));
		}

		out += ")";

		System.out.println(out);
	}

	private static List<Integer> doIt(final List<Integer> columns, final List<List<Integer>> accesses)
	{
		int lowScore = Integer.MAX_VALUE;
		List<Integer> lowOrder = null;
		final List<List<Integer>> perms = permutations(columns);
		final int size = perms.size();
		System.out.println(size + " permutations");

		for (final List<Integer> order : perms)
		{
			final int score = score(order, accesses);
			if (score < lowScore)
			{
				lowScore = score;
				lowOrder = order;
			}

			// if (i % 1000 == 0)
			// {
			// System.out.println("Completed " + i + "/" + size);
			// }
		}

		return lowOrder;
	}

	private static List<List<Integer>> permutations(final List<Integer> arr)
	{
		final List<List<Integer>> resultList = new ArrayList<List<Integer>>();
		final int l = arr.size();
		if (l == 0)
		{
			return resultList;
		}

		if (l == 1)
		{
			resultList.add(arr);
			return resultList;
		}

		final List<Integer> subClone = new ArrayList<Integer>();
		int i = 1;
		while (i < l)
		{
			subClone.add(arr.get(i++));
		}

		for (i = 0; i < l; ++i)
		{
			final int e = arr.get(i);
			if (i > 0)
			{
				subClone.set(i - 1, arr.get(0));
			}
			final List<List<Integer>> subPermutations = permutations(subClone);
			for (final List<Integer> sc : subPermutations)
			{
				final List<Integer> clone = new ArrayList<Integer>();
				clone.add(e);
				int j = 0;
				while (j < l - 1)
				{
					clone.add(sc.get(j++));
				}

				resultList.add(clone);
			}

			if (i > 0)
			{
				subClone.set(i - 1, e);
			}
		}

		return resultList;
	}

	private static int score(final List<Integer> order, final List<List<Integer>> accesses)
	{
		final List<Integer> disk = new ArrayList<Integer>();
		disk.add(-1);
		disk.addAll(order);
		int copies = 1;

		if (disk.size() % 3 == 1)
		{
			disk.addAll(order);
			copies++;
		}
		else
		{
			while (disk.size() % 3 != 1)
			{
				disk.addAll(order);
				copies++;
			}
		}

		int score = 0;
		for (final List<Integer> access : accesses)
		{
			final Set<Integer> sbs = new HashSet<Integer>();
			for (final int col : access)
			{
				int found = 0;
				int i = 1;
				while (found < copies)
				{
					try
					{
						if (disk.get(i) == col)
						{
							final int sb = i / 3;
							sbs.add(sb);
							found++;
						}
					}
					catch (final Exception e)
					{
						System.out.println("Looking for " + col + " in " + disk);
						System.out.println("Found " + found + " instances");
						System.out.println("But there should be " + copies);
					}

					i++;
				}
			}

			score += sbs.size();
		}

		return score;
	}
}
