import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class PreLoadRegionMap extends Mapper<Object, Text, Text, Text>
{
	protected void map(Object key, Text row, Context context) throws IOException, InterruptedException
	{
		if (context.getConfiguration().get("HRDBMS.nodes").equals("2"))
		{
			context.write(new Text("R0"), row);
			context.write(new Text("R1"), row);
		}
		else if (context.getConfiguration().get("HRDBMS.nodes").equals("4"))
		{
			context.write(new Text("R0"), row);
			context.write(new Text("R1"), row);
			context.write(new Text("R2"), row);
			context.write(new Text("R3"), row);
		}
	}
}