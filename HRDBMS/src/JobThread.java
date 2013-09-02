import org.apache.hadoop.mapreduce.Job;


public class JobThread extends Thread 
{
	private Job job;
	
	public JobThread(Job job)
	{
		this.job = job;
	}
	
	public void run()
	{
		try
		{
			job.waitForCompletion(true);
		}
		catch(Exception e)
		{
			System.out.println(e);
			e.printStackTrace();
			System.exit(0);
		}
	}

}
