import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Vector;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.*;

public class TCPIPOutputCommitter extends OutputCommitter
{
	private Vector<Writable> values = new Vector<Writable>();
	
	public TCPIPOutputCommitter()
	{
		super();
	}

	@Override
	public void abortTask(TaskAttemptContext arg0) throws IOException {
		values.clear();
	}

	@Override
	public void commitTask(TaskAttemptContext arg0) throws IOException {
		if (values.isEmpty())
		{
			return;
		}
		
		Socket sock = null;
		try
		{
			sock = new Socket("pri", 40001);
		}
		catch(Exception e)
		{
			System.out.println(e);
			e.printStackTrace();
		}
		
		OutputStream stream = sock.getOutputStream();
		MyDataOutput out = new MyDataOutput(stream);
		Text text = new Text("TCPIPOutputFormat");
		text.write(out);
		
		for (Writable value : values)
		{
			value.write(out);
		}
		stream.flush();
		stream.close();
		sock.close();
	}

	@Override
	public boolean needsTaskCommit(TaskAttemptContext arg0) throws IOException {
		// TODO Auto-generated method stub
		return true;
	}

	@Override
	public void setupJob(JobContext arg0) throws IOException {
		
	}

	@Override
	public void setupTask(TaskAttemptContext arg0) throws IOException {
		
	}
	
	public void write(Writable key, Writable value) throws IOException
	{
		if (!(key instanceof NullWritable))
		{
			throw new IOException("TCPIPOutputFormat only supports NullWritable keys!");
		}
		
		values.add(value);
	}
}
