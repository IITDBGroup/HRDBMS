package com.exascale.misc;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.IdentityHashMap;
import java.util.Map;

import com.exascale.optimizer.OperatorUtils;

public final class FastStringTokenizer implements Serializable
{
	private static sun.misc.Unsafe unsafe;

	static
	{
		try
		{
			final Field f = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
			f.setAccessible(true);
			unsafe = (sun.misc.Unsafe)f.get(null);
		}
		catch (final Exception e)
		{
			unsafe = null;
		}
	}
	private int index;
	private String delim;
	private String string;

	private String[] temp;

	private int limit;

	public FastStringTokenizer(final String string, final String delim, final boolean bool)
	{
		this.delim = delim;
		this.string = string;
		final char delimiter = delim.charAt(0);

		temp = new String[(string.length() >> 1) + 1];
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

	public static FastStringTokenizer deserializeKnown(final InputStream in, final Map<Long, Object> prev) throws Exception
	{
		final FastStringTokenizer value = (FastStringTokenizer)unsafe.allocateInstance(FastStringTokenizer.class);
		prev.put(OperatorUtils.readLong(in), value);
		value.index = OperatorUtils.readInt(in);
		value.delim = OperatorUtils.readString(in, prev);
		value.string = OperatorUtils.readString(in, prev);
		value.temp = OperatorUtils.deserializeStringArray(in, prev);
		value.limit = OperatorUtils.readInt(in);
		return value;
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

	public final void reuse(final String string, final String delim, final boolean bool)
	{
		this.delim = delim;
		this.string = string;
		final char delimiter = delim.charAt(0);

		if (temp.length < (string.length() >> 1) + 1)
		{
			temp = new String[(string.length() >> 1) + 1];
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

	public void serialize(final OutputStream out, final IdentityHashMap<Object, Long> prev) throws Exception
	{
		final Long id = prev.get(this);
		if (id != null)
		{
			OperatorUtils.serializeReference(id, out);
			return;
		}

		OperatorUtils.writeType(HrdbmsType.FST, out);
		prev.put(this, OperatorUtils.writeID(out));
		OperatorUtils.writeInt(index, out);
		OperatorUtils.writeString(delim, out, prev);
		OperatorUtils.writeString(string, out, prev);
		OperatorUtils.serializeStringArray(temp, out, prev);
		OperatorUtils.writeInt(limit, out);
	}

	public void setIndex(final int index)
	{
		this.index = index;
	}
}