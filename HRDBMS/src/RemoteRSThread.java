import java.io.DataInput;
import java.io.IOException;


public class RemoteRSThread extends RSThread
{
	private DataInput in;
	
	public RemoteRSThread(DataInput in) 
	{
		this.in = in;
		rs = new ResultSetWritable();
	}
	
	public void run()
	{
		try
		{
			((ResultSetWritable)rs).readFields(in);
		}
		catch(Exception e)
		{
			System.out.println(e);
			e.printStackTrace();
			System.exit(0);
		}
	}
}
