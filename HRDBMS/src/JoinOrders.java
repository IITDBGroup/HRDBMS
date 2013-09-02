import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class JoinOrders 
{
	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		fs.copyFromLocalFile(new Path("/filesystems/load/dbgen/lineitem.tbl"), 
				  new Path("/lineitem.tbl"));
		fs.copyFromLocalFile(new Path("/filesystems/load/dbgen/orders.tbl"), 
				  new Path("/orders.tbl"));
		Job job = new Job(conf, "Join of Orders and Lineitem");
		job.setJarByClass(JoinOrders.class);
		FileOutputFormat.setOutputPath(job, new Path("/out"));
		MultipleInputs.addInputPath(job,  new Path("/lineitem.tbl"), TextInputFormat.class, JoinLineitemMap.class);
		MultipleInputs.addInputPath(job,  new Path("/orders.tbl"), TextInputFormat.class, JoinOrdersMap.class);
		job.setReducerClass(JoinOrdersReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
