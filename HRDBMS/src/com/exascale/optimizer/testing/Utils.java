package com.exascale.optimizer.testing;

public final class Utils 
{   
	public static final int parseInt(String s)
	{
		boolean negative = false;
		int offset = 0;
		int result = 0;
		int length = s.length();
		
		if (s.charAt(0) == '-')
		{
			negative = true;
			offset = 1;
		}
		
		while (offset < length)
		{
			byte b = (byte)s.charAt(offset);
			b -= 48;
			result *= 10;
			result += b;
			offset++;
		}
		
		if (negative)
		{
			result *= -1;
		}
		
		return result;
	}
	
	public static final long parseLong(String s)
	{
		boolean negative = false;
		int offset = 0;
		long result = 0;
		int length = s.length();
		
		if (s.charAt(0) == '-')
		{
			negative = true;
			offset = 1;
		}
		
		while (offset < length)
		{
			byte b = (byte)s.charAt(offset);
			b -= 48;
			result *= 10;
			result += b;
			offset++;
		}
		
		if (negative)
		{
			result *= -1;
		}
		
		return result;
	}
	
	public static final double parseDouble(String s)
	{
		int p = s.indexOf('.');
		if (p < 0)
		{
			return parseLong(s);
		}
		
		boolean negative = false;
		int offset = 0;
		if (s.charAt(0) == '-')
		{
			negative = true;
			offset = 1;
		}
		
		while (s.charAt(offset) == '0')
		{
			offset++;
		}
		
		String s2 = s.substring(offset, p) + s.substring(p+1, s.length());
		long n = parseLong(s2);
		int x = s.length() - p - 1;
		int i = 0;
		long d = 1;
		while (i < x)
		{
			d *= 10;
			i++;
		}
		
		double retval = (n*1.0) / (d*1.0);
		if (negative)
		{
			retval *= -1;
		}
		
		return retval;
	}
}
