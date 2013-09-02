import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class JoinOrdersMap extends Mapper<Object, Text, Text, Text>
{
	
	protected void map(Object key, Text row, Context context) throws IOException, InterruptedException
	{
		StringTokenizer tokens = new StringTokenizer(row.toString(), "|", false);
		String orderKey = tokens.nextToken();
		tokens.nextToken(); tokens.nextToken(); tokens.nextToken();
		String partKey = tokens.nextToken().substring(0, 4);
		context.write(new Text(orderKey), new Text("O|" + partKey));
	}
}

