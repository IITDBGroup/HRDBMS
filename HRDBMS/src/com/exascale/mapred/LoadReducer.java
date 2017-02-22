package com.exascale.mapred;

import java.io.IOException;
import java.io.PrintWriter;
import org.apache.hadoop.mapreduce.Reducer;

public class LoadReducer extends Reducer<MyLongWritable, ALOWritable, MyLongWritable, ALOWritable>
{
	public static PrintWriter out = null;

	static
	{
		try
		{
			// out = new PrintWriter(new FileWriter("hrdbms_reducer.log",
			// true));
		}
		catch (final Exception e)
		{
		}
	}

	@Override
	public void reduce(final MyLongWritable key, final Iterable<ALOWritable> values, final Context context) throws InterruptedException, IOException
	{
		// out.println("Reducer started to process key = " + key.get());
		// out.flush();
		for (final ALOWritable value : values)
		{
			context.write(key, value);
		}
	}
}
