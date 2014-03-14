package com.exascale.optimizer.testing;

import java.io.Serializable;

public class MySimpleDateFormat implements Serializable
{
	public MySimpleDateFormat(String format)
	{}
	
	public String format(Object date)
	{
		MyDate d = (MyDate)date;
		return d.format();
	}
}
