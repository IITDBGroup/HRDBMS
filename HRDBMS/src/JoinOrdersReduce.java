import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;
import java.util.Vector;

import jdbm.PrimaryHashMap;
import jdbm.RecordManager;
import jdbm.RecordManagerFactory;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;


public class JoinOrdersReduce extends Reducer<Text, Text, Text, NullWritable>
{
	
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	{
		String year  = null;
		Vector<String> lines = new Vector<String>();
		
		for (Text row : values)
		{
			StringTokenizer tokens = new StringTokenizer(row.toString(), "|", false);
			String type = tokens.nextToken();
			
			if (type.equals("O"))
			{
				year = tokens.nextToken();
			}
			else
			{
				String line = row.toString();
				lines.add(line.substring(line.indexOf("|") + 1));
			}
		}
		
		for (String line : lines)
		{
			context.write(new Text(year + "|" + line), NullWritable.get());
		}
	}
}