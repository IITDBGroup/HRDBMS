import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;

public class DB2InputFormat<K, V> extends InputFormat<K, V> 
{	
	public List<InputSplit> getSplits(JobContext jobContext)
	{
		List<InputSplit> retval = new Vector<InputSplit>();
		Configuration conf = jobContext.getConfiguration();
		String splits = conf.get("db2inputformat.splits");
		StringTokenizer tokens = new StringTokenizer(splits, "^", false);
		while (tokens.hasMoreTokens())
		{
			retval.add(DB2InputSplit.splitFromString(tokens.nextToken()));
		}
		
		return retval;
	}
	
	public RecordReader<K,V> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException
	{
		RecordReader<K, V> retval = new DB2RecordReader<K, V>();
		retval.initialize(split, context);
		return retval;
	}
	
	public static void setSplits(JobContext job, DB2InputSplit[] splits)
	{
		Configuration conf = job.getConfiguration();
		String value = splits[0].toString();
		int i = 1;
		while (i < splits.length)
		{
			value += ("^" + splits[i].toString());
					i++;
		}
		conf.set("db2inputformat.splits", value);
	}
}