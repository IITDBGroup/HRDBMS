package com.exascale.mapred;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

public class LoadReducer extends Reducer<MyLongWritable, ALOWritable, MyLongWritable, ALOWritable>
{
	public static PrintWriter out = null;
	
	static
	{
		try
		{
			//out = new PrintWriter(new FileWriter("hrdbms_reducer.log", true));
		}
		catch(Exception e) {}
	}
	
	public void reduce(MyLongWritable key, Iterable<ALOWritable> values, Context context) throws InterruptedException, IOException
	{
		//out.println("Reducer started to process key = " + key.get());
		//out.flush();
		for (ALOWritable value : values)
		{
			context.write(key, value);
		}
	}
}
