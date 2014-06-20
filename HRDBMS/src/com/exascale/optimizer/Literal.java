package com.exascale.optimizer;

public class Literal
{
	private Object value;
	private boolean isNull = false;
	
	public Literal(Object value)
	{
		this.value = value;
		if (value == null)
		{
			isNull = true;
		}
	}
	
	public Literal()
	{
		isNull = true;
	}
	
	public Object getValue()
	{
		return value;
	}
	
	public boolean isNull()
	{
		return isNull;
	}
	
	public boolean equals(Object o)
	{
		if (o == null || !(o instanceof Literal))
		{
			return false;
		}
		
		Literal rhs = (Literal)o;
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
}
