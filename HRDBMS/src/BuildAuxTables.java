import java.sql.Connection;
import java.sql.DriverManager;


public class BuildAuxTables 
{
	public static void main(String[] args)
	{
		int nodes = Integer.parseInt(args[0]);
		int i = 0;
		BuildAuxThread[] threads = new BuildAuxThread[nodes];
		BuildAuxThread2[] threads2 = new BuildAuxThread2[nodes];
		while (i < nodes)
		{
			threads[i] = new BuildAuxThread(i, nodes);
			threads[i].start();
			threads2[i] = new BuildAuxThread2(i, nodes);
			threads2[i].start();
			i++;
		}
		
		i = 0;
		while (i < nodes)
		{
			try
			{
				threads[i].join();
				threads2[i].join();
			}
			catch(Exception e) {}
			i++;
		}
	}
}