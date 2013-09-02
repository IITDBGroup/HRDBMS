import java.util.Vector;


public class SpeedTest 
{
	public static void main(String[] args)
	{
		Vector<Integer> source = new Vector<Integer>();
		int i = 0;
		while (i < 5000000)
		{
			source.add(i);
			i++;
		}
		
		Vector<Integer> list = new Vector<Integer>();
		long time = System.currentTimeMillis();
		for (int x : source)
		{
			list.add(x);
		}
		long tTime = System.currentTimeMillis() - time;
		System.out.println(5000000.0 / (tTime / 1000.0) + " moves per second");
	}
}
