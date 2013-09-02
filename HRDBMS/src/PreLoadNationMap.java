import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class PreLoadNationMap extends Mapper<Object, Text, Text, Text>
{	
	protected void map(Object key, Text row, Context context) throws IOException, InterruptedException
	{
		if (context.getConfiguration().get("HRDBMS.nodes").equals("2"))
		{
			context.write(new Text("N0"), row);
			context.write(new Text("N1"), row);
		}
		else if (context.getConfiguration().get("HRDBMS.nodes").equals("4"))
		{
			context.write(new Text("N0"), row);
			context.write(new Text("N1"), row);
			context.write(new Text("N2"), row);
			context.write(new Text("N3"), row);
		}
	}
}