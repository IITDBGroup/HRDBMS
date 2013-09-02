import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class PreLoad 
{
	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		fs.copyFromLocalFile(new Path("/filesystems/load/dbgen/customer.tbl"), 
		  new Path("/customer.tbl"));
		fs.copyFromLocalFile(new Path("/filesystems/load/dbgen/nation.tbl"), 
				  new Path("/nation.tbl"));
		fs.copyFromLocalFile(new Path("/filesystems/load/dbgen/partsupp.tbl"), 
				  new Path("/partsupp.tbl"));
		fs.copyFromLocalFile(new Path("/filesystems/load/dbgen/part.tbl"), 
				  new Path("/part.tbl"));
		fs.copyFromLocalFile(new Path("/filesystems/load/dbgen/region.tbl"), 
				  new Path("/region.tbl"));
		fs.copyFromLocalFile(new Path("/filesystems/load/dbgen/supplier.tbl"), 
				  new Path("/supplier.tbl"));
		Job job = new Job(conf, "Pre Load Partitioning");
		job.setJarByClass(PreLoad.class);
		MultipleInputs.addInputPath(job,  new Path("/customer.tbl"), TextInputFormat.class, PreLoadCustomerMap.class);
		MultipleInputs.addInputPath(job,  new Path("/out/part-r-00000"), TextInputFormat.class, PreLoadLineitemMap.class);
		MultipleInputs.addInputPath(job,  new Path("/nation.tbl"), TextInputFormat.class, PreLoadNationMap.class);
		MultipleInputs.addInputPath(job,  new Path("/orders.tbl"), TextInputFormat.class, PreLoadOrdersMap.class);
		MultipleInputs.addInputPath(job,  new Path("/partsupp.tbl"), TextInputFormat.class, PreLoadPartsuppMap.class);
		MultipleInputs.addInputPath(job,  new Path("/part.tbl"), TextInputFormat.class, PreLoadPartMap.class);
		MultipleInputs.addInputPath(job,  new Path("/region.tbl"), TextInputFormat.class, PreLoadRegionMap.class);
		MultipleInputs.addInputPath(job,  new Path("/supplier.tbl"), TextInputFormat.class, PreLoadSupplierMap.class);
		TextOutputFormat.setOutputPath(job, new Path("/out2"));
		MultipleOutputs.addNamedOutput(job, "customer", TextOutputFormat.class, Text.class, NullWritable.class);
		MultipleOutputs.addNamedOutput(job, "lineitem", TextOutputFormat.class, Text.class, NullWritable.class);
		MultipleOutputs.addNamedOutput(job, "nation", TextOutputFormat.class, Text.class, NullWritable.class);
		MultipleOutputs.addNamedOutput(job, "orders", TextOutputFormat.class, Text.class, NullWritable.class);
		MultipleOutputs.addNamedOutput(job, "partsupp", TextOutputFormat.class, Text.class, NullWritable.class);
		MultipleOutputs.addNamedOutput(job, "parttbl", TextOutputFormat.class, Text.class, NullWritable.class);
		MultipleOutputs.addNamedOutput(job, "region", TextOutputFormat.class, Text.class, NullWritable.class);
		MultipleOutputs.addNamedOutput(job, "supplier", TextOutputFormat.class, Text.class, NullWritable.class);
		job.setReducerClass(PreLoadReduce.class);
		job.setPartitionerClass(PreLoadPartition.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.getConfiguration().set("HRDBMS.nodes", args[0]);
		job.setNumReduceTasks(Integer.parseInt(args[0]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
