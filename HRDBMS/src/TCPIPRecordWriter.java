import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;


public class TCPIPRecordWriter<K, V> extends RecordWriter<K, V>
{	
	TCPIPOutputCommitter commit;
	
	public TCPIPRecordWriter(TCPIPOutputCommitter commit)
	{
		super();
		this.commit = commit;
	}
	
	@Override
	public void close(TaskAttemptContext context) throws IOException,
			InterruptedException {
		
	}

	@Override
	public void write(K key, V value) throws IOException, InterruptedException {
		
		commit.write((Writable)key, (Writable)value);
	}

}
