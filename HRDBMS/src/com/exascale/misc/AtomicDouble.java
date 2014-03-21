package com.exascale.misc;

import java.util.Comparator;
import java.util.concurrent.atomic.AtomicReference;

public final class AtomicDouble extends Number implements Comparable<AtomicDouble>
{
	private static final long serialVersionUID = -2419445336101038676L;

	private final AtomicReference<Double> value;

	public static AtomicDoubleComparator comparator = new AtomicDoubleComparator();

	// Constructors
	public AtomicDouble()
	{
		this(0.0);
	}

	public AtomicDouble(AtomicDouble initVal)
	{
		this(initVal.getDoubleValue());
	}

	public AtomicDouble(double initVal)
	{
		this(new Double(initVal));
	}

	public AtomicDouble(Double initVal)
	{
		value = new AtomicReference<Double>(initVal);
	}

	public AtomicDouble(String initStrVal)
	{
		this(Double.valueOf(initStrVal));
	}

	public double addAndGet(double delta)
	{
		while (true)
		{
			final double origVal = get();
			final double newVal = origVal + delta;
			if (compareAndSet(origVal, newVal))
			{
				return newVal;
			}
		}
	}

	@Override
	public byte byteValue()
	{
		return (byte)intValue();
	}

	public char charValue()
	{
		return (char)intValue();
	}

	public boolean compareAndSet(double expect, double update)
	{
		Double origVal, newVal;

		newVal = new Double(update);
		while (true)
		{
			origVal = getDoubleValue();

			if (Double.compare(origVal.doubleValue(), expect) == 0)
			{
				if (value.compareAndSet(origVal, newVal))
				{
					return true;
				}
			}
			else
			{
				return false;
			}
		}
	}

	@Override
	public int compareTo(AtomicDouble aValue)
	{
		return comparator.compare(this, aValue);
	}

	public int compareTo(Double aValue)
	{
		return comparator.compare(this, aValue);
	}

	public double decrementAndGet()
	{
		return addAndGet(-1.0);
	}

	@Override
	public double doubleValue()
	{
		return getDoubleValue().doubleValue();
	}

	@Override
	public boolean equals(Object obj)
	{
		if (obj == null)
		{
			return false;
		}
		if (obj instanceof Double)
		{
			return (compareTo((Double)obj) == 0);
		}
		if (obj instanceof AtomicDouble)
		{
			return (compareTo((AtomicDouble)obj) == 0);
		}
		return false;
	}

	@Override
	public float floatValue()
	{
		return getDoubleValue().floatValue();
	}

	public double get()
	{
		return getDoubleValue().doubleValue();
	}

	public double getAndAdd(double delta)
	{
		while (true)
		{
			final double origVal = get();
			final double newVal = origVal + delta;
			if (compareAndSet(origVal, newVal))
			{
				return origVal;
			}
		}
	}

	public double getAndDecrement()
	{
		return getAndAdd(-1.0);
	}

	public double getAndIncrement()
	{
		return getAndAdd(1.0);
	}

	public double getAndMultiply(double multiple)
	{
		while (true)
		{
			final double origVal = get();
			final double newVal = origVal * multiple;
			if (compareAndSet(origVal, newVal))
			{
				return origVal;
			}
		}
	}

	public double getAndSet(double setVal)
	{
		while (true)
		{
			final double origVal = get();

			if (compareAndSet(origVal, setVal))
			{
				return origVal;
			}
		}
	}

	// Atomic methods
	public Double getDoubleValue()
	{
		return value.get();
	}

	@Override
	public int hashCode()
	{
		return getDoubleValue().hashCode();
	}

	public double incrementAndGet()
	{
		return addAndGet(1.0);
	}

	// Methods of the Number class
	@Override
	public int intValue()
	{
		return getDoubleValue().intValue();
	}

	public boolean isInfinite()
	{
		return getDoubleValue().isInfinite();
	}

	public boolean isNaN()
	{
		return getDoubleValue().isNaN();
	}

	public void lazySet(double newVal)
	{
		set(newVal);
	}

	@Override
	public long longValue()
	{
		return getDoubleValue().longValue();
	}

	public double multiplyAndGet(double multiple)
	{
		while (true)
		{
			final double origVal = get();
			final double newVal = origVal * multiple;
			if (compareAndSet(origVal, newVal))
			{
				return newVal;
			}
		}
	}

	public void set(double newVal)
	{
		value.set(new Double(newVal));
	}

	@Override
	public short shortValue()
	{
		return (short)intValue();
	}

	// Support methods for hashing and comparing
	@Override
	public String toString()
	{
		return getDoubleValue().toString();
	}

	public boolean weakCompareAndSet(double expect, double update)
	{
		return compareAndSet(expect, update);
	}

	public final static class AtomicDoubleComparator implements Comparator<AtomicDouble>
	{
		@Override
		public int compare(AtomicDouble d1, AtomicDouble d2)
		{
			return Double.compare(d1.doubleValue(), d2.doubleValue());
		}

		public int compare(AtomicDouble d1, Double d2)
		{
			return Double.compare(d1.doubleValue(), d2.doubleValue());
		}

		public int compare(Double d1, AtomicDouble d2)
		{
			return Double.compare(d1.doubleValue(), d2.doubleValue());
		}
	}
}