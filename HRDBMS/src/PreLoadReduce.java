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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;


public class PreLoadReduce extends Reducer<Text, Text, Text, NullWritable>
{
	private MultipleOutputs<Text,NullWritable> mos;
	
	public void setup(Context context)
	{
		try
		{
			mos = new MultipleOutputs<Text, NullWritable>(context);
		}
		catch(Exception e)
		{
			System.out.println(e);
			e.printStackTrace();
			System.exit(0);
		}
	}
	
	public void cleanup(Context context)
	{
		try
		{
			mos.close();
		}
		catch(Exception e) 
		{
			System.out.println(e);
			e.printStackTrace();
			System.exit(0);
		}
	}
	
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	{
		String code = key.toString().substring(0, 1);
		String file = "";
		
		if (code.equals("C"))
		{
			file = "customer";
		}
		else if (code.equals("L"))
		{
			file = "lineitem";
		}
		else if (code.equals("N"))
		{
			file = "nation";
		}
		else if (code.equals("O"))
		{
			file = "orders";
		}
		else if (code.equals("X"))
		{
			file = "partsupp";
		}
		else if (code.equals("P"))
		{
			file = "parttbl";
		}
		else if (code.equals("R"))
		{
			file = "region";
		}
		else if (code.equals("S"))
		{
			file = "supplier";
		}
		else
		{
			System.out.println("Invalid table code seen in reduce");
			System.out.flush();
			System.exit(0);
		}
		
		for (Text value : values)
		{
			mos.write(file, value, NullWritable.get());
		}
	}
}
