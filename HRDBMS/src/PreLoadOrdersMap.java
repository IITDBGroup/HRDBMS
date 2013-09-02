import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class PreLoadOrdersMap extends Mapper<Object, Text, Text, Text>
{
	private Text outKey;
	
	protected void map(Object key, Text row, Context context) throws IOException, InterruptedException
	{
		StringTokenizer tokens = new StringTokenizer(row.toString(), "|", false);
		tokens.nextToken(); tokens.nextToken(); tokens.nextToken(); tokens.nextToken();
		String partKey = tokens.nextToken().substring(0, 4);
		outKey = new Text("O" + partNum(partKey, context.getConfiguration().get("HRDBMS.nodes")));
		context.write(outKey, row);
	}
	
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
}
