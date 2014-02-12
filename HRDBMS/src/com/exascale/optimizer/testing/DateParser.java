package com.exascale.optimizer.testing;

import java.util.Date;
import java.util.GregorianCalendar;

public final class DateParser 
{
	private static final ThreadLocal<GregorianCalendar> cals = new ThreadLocal<GregorianCalendar>(){
		protected GregorianCalendar initialValue()
		{
			return new GregorianCalendar();
		}
	};
	
	public static final Date parse(String s)
	{
		String year = s.substring(0, 4);
		String month = s.substring(5, 7);
		String day = s. substring(8, 10);
		int iYear = Utils.parseInt(year);
		int iMonth = Utils.parseInt(month);
		int iDay = Utils.parseInt(day);
		cals.get().set(iYear, iMonth - 1, iDay);
		return cals.get().getTime();
		//return new Date(iYear - 1900, iMonth - 1, iDay);
	}
}
