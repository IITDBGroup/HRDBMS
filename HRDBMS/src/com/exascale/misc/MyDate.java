package com.exascale.misc;

import java.io.Serializable;

public class MyDate implements Comparable, Serializable
{
	private final int time;

	public MyDate(int time)
	{
		this.time = time;
	}

	public MyDate(int year, int month, int day)
	{
		this.time = day + (month << 5) + (year << 9);
	}

	@Override
	public int compareTo(Object r)
	{
		final MyDate rhs = (MyDate)r;
		return Integer.compare(time, rhs.time);
	}

	@Override
	public boolean equals(Object r)
	{
		if (r == null || !(r instanceof MyDate))
		{
			return false;
		}
		final MyDate rhs = (MyDate)r;
		return time == rhs.time;
	}

	public String format()
	{
		int year = (time >> 9);
		int day = (time & 0x000000000000001F);
		int month = ((time & 0x00000000000001E0) >> 5);
		final StringBuilder b = new StringBuilder(10);
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

	public int getTime()
	{
		return time;
	}

	public int getYear()
	{
		return time >> 9;
	}

	@Override
	public int hashCode()
	{
		return time;
	}

	@Override
	public String toString()
	{
		return this.format();
	}
}
