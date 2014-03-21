package com.exascale.misc;

import java.io.Serializable;

public final class FastStringTokenizer implements Serializable
{
	private int index;
	private String delim;
	private String string;
	private String[] temp;
	private int limit;

	public FastStringTokenizer(String string, String delim, boolean bool)
	{
		this.delim = delim;
		this.string = string;
		final char delimiter = delim.charAt(0);

		temp = new String[string.length() / 2 + 1];
		int wordCount = 0;
		int i = 0;
		int j = string.indexOf(delimiter);

		while (j >= 0)
		{
			temp[wordCount++] = string.substring(i, j);
			i = j + 1;
			j = string.indexOf(delimiter, i);
		}

		if (i < string.length())
		{
			temp[wordCount++] = string.substring(i);
		}

		limit = wordCount;
		index = 0;
	}

	public String[] allTokens()
	{
		final String[] result = new String[limit];
		System.arraycopy(temp, 0, result, 0, limit);
		return result;
	}

	@Override
	public FastStringTokenizer clone()
	{
		return new FastStringTokenizer(string, delim, false);
	}

	public int getLimit()
	{
		return limit;
	}

	public boolean hasMoreTokens()
	{
		return index < limit;
	}

	public String nextToken()
	{
		return temp[index++];
	}

	public final void reuse(String string, String delim, boolean bool)
	{
		this.delim = delim;
		this.string = string;
		final char delimiter = delim.charAt(0);

		if (temp.length < string.length() / 2 + 1)
		{
			temp = new String[string.length() / 2 + 1];
		}

		int wordCount = 0;
		int i = 0;
		int j = string.indexOf(delimiter);

		while (j >= 0)
		{
			temp[wordCount++] = string.substring(i, j);
			i = j + 1;
			j = string.indexOf(delimiter, i);
		}

		if (i < string.length())
		{
			temp[wordCount++] = string.substring(i);
		}

		limit = wordCount;
		index = 0;
	}

	public void setIndex(int index)
	{
		this.index = index;
	}
}