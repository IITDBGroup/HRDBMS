package com.exascale.misc;

import java.io.Serializable;

public class MySimpleDateFormat implements Serializable
{
	public MySimpleDateFormat(final String format)
	{
	}

	public String format(final Object date)
	{
		final MyDate d = (MyDate)date;
		return d.format();
	}
}
