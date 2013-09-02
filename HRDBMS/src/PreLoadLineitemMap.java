import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

import jdbm.PrimaryHashMap;
import jdbm.RecordManager;
import jdbm.RecordManagerFactory;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class PreLoadLineitemMap extends Mapper<Object, Text, Text, Text>
{
	private Text outKey;
	
	private int partNum(String year, String nodes)
	{
		int value = Integer.parseInt(year);
		int num = Integer.parseInt(nodes);
		
		if (num == 2)
		{
			if (value < 1995)
			{
				return 0 ;
			}
			else
			{
				return 1;
			}
		}
		else if (num == 4)
		{
			if (value < 1994)
			{
				return 0;
			}
			else if (value < 1996)
			{
				return 1;
			}
			else if (value < 1997)
			{
				return 2;
			}
			else
			{
				return 3;
			}
		}
		return -1;
	}
	
	protected void map(Object key, Text row, Context context) throws IOException, InterruptedException
	{
		StringTokenizer tokens = new StringTokenizer(row.toString(), "|", false);
	    String year = tokens.nextToken(); 
	    String line = row.toString();
	    line = line.substring(line.indexOf("|") + 1);
	    outKey = new Text("L" + partNum(year, context.getConfiguration().get("HRDBMS.nodes")));
		context.write(outKey, new Text(line));
	}
}