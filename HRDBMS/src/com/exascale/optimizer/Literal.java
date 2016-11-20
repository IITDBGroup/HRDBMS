package com.exascale.optimizer;

public class Literal
{
	private Object value;
	private boolean isNull = false;

	public Literal()
	{
		isNull = true;
	}

	public Literal(final Object value)
	{
		this.value = value;
		if (value == null)
		{
			isNull = true;
		}
	}

	@Override
	public Literal clone()
	{
		if (isNull)
		{
			return new Literal();
		}

		return new Literal(value);
	}

	@Override
	public boolean equals(final Object o)
	{
		if (o == null || !(o instanceof Literal))
		{
			return false;
		}

		final Literal rhs = (Literal)o;
		if (value == null && rhs.value == null && isNull == rhs.isNull)
		{
			return true;
		}

		if (value == null)
		{
			return false;
		}

		if (value.equals(rhs.value) && isNull == rhs.isNull)
		{
			return true;
		}

		return false;
	}

	public Object getValue()
	{
		return value;
	}

	@Override
	public int hashCode()
	{
		int hash = 23;
		hash = hash * 31 + (isNull ? 1 : 0);
		hash = hash * 31 + (value != null ? value.hashCode() : 1);
		return hash;
	}

	public boolean isNull()
	{
		return isNull;
	}
}
