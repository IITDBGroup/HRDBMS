import java.io.IOException;
import java.net.Socket;

import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class TCPIPOutputFormat<K, V> extends OutputFormat<K, V>
{
	private TCPIPOutputCommitter commit;
	
	public TCPIPOutputFormat()
	{
		super();
		commit = new TCPIPOutputCommitter();
	}
	
	public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException
	{

	}
	
	public OutputCommitter getOutputCommitter(TaskAttemptContext context)
	{
		return commit;
	}
	
	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context)
	{
		return new TCPIPRecordWriter<K, V>(commit);
	}
}
