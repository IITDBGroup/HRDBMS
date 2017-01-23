package com.exascale.misc;

import java.io.Serializable;

public class MySimpleDateFormat implements Serializable
{
	public MySimpleDateFormat(String format)
	{
	}

	public String format(Object date)
	{
		final MyDate d = (MyDate)date;
		return d.format();
	}
}
