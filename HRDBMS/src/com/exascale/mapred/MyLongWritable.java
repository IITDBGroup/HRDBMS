package com.exascale.mapred;

import org.apache.hadoop.io.LongWritable;

public class MyLongWritable extends LongWritable
{
	private static int numWorkers;
	private static int numDevices; 
	
	public static void setup(int numWorkers, int numDevices)
	{
		MyLongWritable.numWorkers = numWorkers;
		MyLongWritable.numDevices = numDevices;
	}
	
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
		int retval = (int)(super.get() >> 32) * numDevices + (int)(super.get() & 0x00000000FFFFFFFF);
		return retval;
	}
}
