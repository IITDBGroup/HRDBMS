package com.exascale.optimizer.testing;

import java.io.Serializable;

public class MyDate implements Comparable, Serializable
{
	protected int year;
	protected int month;
	protected int day;
	
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
	
	public long getTime()
	{
		return day + (month << 5) + (year << 9);
	}
	
	public int getYear()
	{
		return year;
	}
	
	public String format()
	{
		StringBuilder b = new StringBuilder();
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
	
	public boolean equals(Object r)
	{
		MyDate rhs = (MyDate)r;
		return (year == rhs.year && month == rhs.month && day == rhs.day);
	}
	
	public int hashCode()
	{
		return (int)getTime();
	}
	
	public int compareTo(Object r)
	{
		MyDate rhs = (MyDate)r;
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
	
	public String toString()
	{
		return this.format();
	}
}
