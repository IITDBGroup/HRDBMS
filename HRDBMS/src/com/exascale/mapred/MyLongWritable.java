package com.exascale.mapred;

import org.apache.hadoop.io.LongWritable;

public class MyLongWritable extends LongWritable
{
	private static int numDevices;

	public MyLongWritable()
	{
		super();
	}

	public MyLongWritable(final long key2)
	{
		super(key2);
	}

	public static void setup(final int numWorkers, final int numDevices)
	{
		MyLongWritable.numDevices = numDevices;
	}

	@Override
	public int hashCode()
	{
		final int retval = (int)(super.get() >> 32) * numDevices + (int)(super.get() & 0x00000000FFFFFFFF);
		return retval;
	}
}
