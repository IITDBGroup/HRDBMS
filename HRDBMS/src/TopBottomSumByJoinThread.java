import java.math.BigDecimal;
import java.sql.ResultSet;
import java.util.HashSet;


public class TopBottomSumByJoinThread<K extends Object> extends Thread
{
	public double top = 0;
	public double bottom = 0;
	private HashSet<K> keys;
	private ResultSet rs;
	
	public TopBottomSumByJoinThread(ResultSet rs, HashSet<K> keys)
	{
		this.keys = keys;
		this.rs = rs;
	}
	
	public void run()
	{
		try
		{
			while (rs.next())
			{
				bottom += (rs.getDouble(2));
			
				if (keys.contains((K)rs.getObject(1)))
				{
					top += (rs.getDouble(2));
				}
			}
		}
		catch(Exception e)
		{
			System.out.println(e);
			e.printStackTrace();
			System.exit(0);
		}
	}
}
