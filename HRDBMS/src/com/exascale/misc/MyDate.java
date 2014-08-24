package com.exascale.misc;

import java.io.Serializable;

public class MyDate implements Comparable, Serializable
{
	private final int year;
	private final int month;
	private final int day;

	public MyDate(int year, int month, int day)
	{
		this.year = year;
		this.month = month;
		this.day = day;
	}

	public MyDate(long time)
	{
		year = (int)(time >> 9);
		day = (int)(time & 0x000000000000001F);
		month = (int)((time & 0x00000000000001E0) >> 5);
	}

	@Override
	public int compareTo(Object r)
	{
		final MyDate rhs = (MyDate)r;
		if (year < rhs.year)
		{
			return -1;
		}
		else if (year > rhs.year)
		{
			return 1;
		}
		else
		{
			if (month < rhs.month)
			{
				return -1;
			}
			else if (month > rhs.month)
			{
				return 1;
			}
			else
			{
				if (day < rhs.day)
				{
					return -1;
				}
				else if (day > rhs.day)
				{
					return 1;

				}
				else
				{
					return 0;
				}
			}
		}
	}

	@Override
	public boolean equals(Object r)
	{
		if (r == null || !(r instanceof MyDate))
		{
			return false;
		}
		final MyDate rhs = (MyDate)r;
		return (year == rhs.year && month == rhs.month && day == rhs.day);
	}

	public String format()
	{
		final StringBuilder b = new StringBuilder();
		b.append(year);
		b.append("-");
		if (month < 10)
		{
			b.append("0");
		}
		b.append(month);
		b.append("-");
		if (day < 10)
		{
			b.append("0");
		}
		b.append(day);
		return b.toString();
	}

	public long getTime()
	{
		return day + (month << 5) + (year << 9);
	}

	public int getYear()
	{
		return year;
	}

	@Override
	public int hashCode()
	{
		return (int)getTime();
	}

	@Override
	public String toString()
	{
		return this.format();
	}
}
