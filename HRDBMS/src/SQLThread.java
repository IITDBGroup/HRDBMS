import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;


public class SQLThread extends RSThread
{
	private Connection con;
	private String sql;
	public Statement stmt;
	
	public SQLThread(Connection con, String sql)
	{
		this.con = con;
		this.sql = sql;
	}
	
	public void run()
	{
		try
		{
			stmt = con.createStatement();
			rs = stmt.executeQuery(sql);
		}
		catch(Exception e)
		{
			System.out.println(e);
			e.printStackTrace();
			System.exit(0);
		}
	}
}
