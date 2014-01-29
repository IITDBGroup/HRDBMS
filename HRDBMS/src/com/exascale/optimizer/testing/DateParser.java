package com.exascale.optimizer.testing;

import java.util.Date;
import java.util.GregorianCalendar;

public class DateParser 
{
	public static Date parse(String s)
	{
		String year = s.substring(0, 4);
		String month = s.substring(5, 7);
		String day = s. substring(8, 10);
		int iYear = Integer.parseInt(year);
		int iMonth = Integer.parseInt(month);
		int iDay = Integer.parseInt(day);
		return new GregorianCalendar(iYear, iMonth - 1, iDay).getTime();
		//return new Date(iYear - 1900, iMonth - 1, iDay);
	}
}
