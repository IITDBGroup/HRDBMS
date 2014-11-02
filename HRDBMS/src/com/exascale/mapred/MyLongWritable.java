package com.exascale.mapred;

import org.apache.hadoop.io.LongWritable;

public class MyLongWritable extends LongWritable
{
	public MyLongWritable()
	{
		super();
	}
	
	public MyLongWritable(long key2)
	{
		super(key2);
	}

	public int hashCode()
	{
		int retval =  17;
		retval = 22 * retval + (int)(super.get() >> 32);
		retval = 22 * retval + (int)(super.get() & 0x00000000FFFFFFFF);
		return retval;
	}
}
