package com.exascale.misc;

public final class IndexedByteArray
{
	public byte[] array;
	public int index;

	public IndexedByteArray(final byte[] array, final int idx)
	{
		if (array == null)
		{
			throw new NullPointerException("The array cannot be null");
		}

		this.array = array;
		this.index = idx;
	}

	@Override
	public boolean equals(final Object o)
	{
		try
		{
			if (o == null)
			{
				return false;
			}

			if (this == o)
			{
				return true;
			}

			final IndexedByteArray iba = (IndexedByteArray)o;
			return ((this.array == iba.array) && (this.index == iba.index));
		}
		catch (final ClassCastException e)
		{
			return false;
		}
	}

	@Override
	public int hashCode()
	{
		// Non constant !
		return this.index + ((this.array == null) ? 0 : (17 * this.array.hashCode()));
	}

	@Override
	public String toString()
	{
		final StringBuilder builder = new StringBuilder(100);
		builder.append("[");
		builder.append(String.valueOf(this.array));
		builder.append(",");
		builder.append(this.index);
		builder.append("]");
		return builder.toString();
	}
}