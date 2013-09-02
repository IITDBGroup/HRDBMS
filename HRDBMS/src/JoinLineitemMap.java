import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class JoinLineitemMap extends Mapper<Object, Text, Text, Text>
{
	
	protected void map(Object key, Text row, Context context) throws IOException, InterruptedException
	{
		StringTokenizer tokens = new StringTokenizer(row.toString(), "|", false);
		String orderKey = tokens.nextToken();
		context.write(new Text(orderKey), new Text("L|" + row));
	}
}

