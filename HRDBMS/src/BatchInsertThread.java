import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Map;
import java.util.Set;
import java.util.Vector;


public class BatchInsertThread extends Thread
{
	private Connection con;
	private String sql;
	private Set<Integer> keys;
	private int params;
	private Map map;
	
	public BatchInsertThread(Connection con, String sql, Set<Integer> keys, int params)
	{
		this.con = con;
		this.sql = sql;
		this.keys = keys;
		this.params = params;
	}
	
	public BatchInsertThread(Connection con, String sql, Map map, int params)
	{
		this.con = con;
		this.sql = sql;
		this.map = map;
		this.params = params;
	}
	
	public void run()
	{
		long i = 0;
		try
		{
			if (params == 1)
			{
				PreparedStatement p = con.prepareStatement(sql);
				for (int key : keys)
				{
					p.setInt(1, key);
					p.addBatch();
					i++;
					if (i % 5000 == 0 )
					{
						p.executeBatch();
						con.commit();
						p.clearBatch();
					}
				}
				p.executeBatch();
				con.commit();
			}
			else if (params == 2)
			{
				PreparedStatement p = con.prepareStatement(sql);
				for (Object e : map.entrySet())
				{
					Map.Entry entry = (Map.Entry)e;
					double[] cols = ((double[])entry.getValue());
					Integer key = ((Integer)entry.getKey());
					p.setInt(1, key);
					p.setDouble(2, cols[0] / cols[1]);
					p.addBatch();
					i++;
					if (i % 5000 == 0 )
					{
						p.executeBatch();
						con.commit();
						p.clearBatch();
					}
				}
				p.executeBatch();
				con.commit();
			}
			else if (params == 4)
			{
				PreparedStatement p = con.prepareStatement(sql);
				for (Object e : map.entrySet())
				{
					Map.Entry entry = (Map.Entry)e;
					Object[] cols = ((Object[])entry.getValue());
					Vector<Integer> key = ((Vector<Integer>)entry.getKey());
					p.setString(1, (String)cols[0]);
					p.setDouble(2, ((Double)cols[1]));
					p.setInt(3, key.get(0));
					p.setInt(4, key.get(1));
					p.addBatch();
					i++;
					if (i % 5000 == 0 )
					{
						p.executeBatch();
						con.commit();
						p.clearBatch();
					}
				}
				p.executeBatch();
				con.commit();
			}
			else if (params == 5)
			{
				PreparedStatement p = con.prepareStatement(sql);
				for (Object e : map.entrySet())
				{
					Map.Entry entry = (Map.Entry)e;
					Object[] cols = ((Object[])entry.getValue());
					Vector<Integer> key = ((Vector<Integer>)entry.getKey());
					p.setInt(1, key.get(0));
					p.setInt(2, key.get(1));
					p.setInt(3, (Integer)cols[0]);
					p.setString(4,(String)cols[1]);
					p.setString(5,  (String)cols[2]);
					p.addBatch();
					i++;
					if (i % 5000 == 0 )
					{
						p.executeBatch();
						con.commit();
						p.clearBatch();
					}
				}
				p.executeBatch();
				con.commit();
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
			BatchUpdateException exception = (BatchUpdateException)e;
			exception.getNextException().printStackTrace();
		}
	}
}
