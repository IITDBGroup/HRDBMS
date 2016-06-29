package com.exascale.misc;

public final class DateParser
{
	public static final MyDate parse(String s)
	{
		final String year = s.substring(0, 4);
		final String month = s.substring(5, 7);
		final String day = s.substring(8, 10);
		final int iYear = Utils.parseInt(year);
		final int iMonth = Utils.parseInt(month);
		final int iDay = Utils.parseInt(day);
		return new MyDate(iYear, iMonth, iDay);
	}
}
