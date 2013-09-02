import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


public class PreLoadPartition extends Partitioner<Text, Text>
{
	public int getPartition(Text key, Text value, int NumPartitions)
	{
		return Integer.parseInt(key.toString().substring(1));
	}
}
