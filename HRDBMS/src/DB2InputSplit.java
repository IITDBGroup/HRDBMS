import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.StringTokenizer;
import java.util.Vector;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

public class DB2InputSplit extends InputSplit implements Writable
{
	private String[] locations;
	private String instructions;
	private String url;
	private String sql;
	private String userid;
	private String pwd;
	
	public static DB2InputSplit splitFromString(String in)
	{
		StringTokenizer tokens = new StringTokenizer(in, "~", false);
		DB2InputSplit retval = new DB2InputSplit();
		String[] locations = new String[1];
		locations[0] = tokens.nextToken();
		retval.setLocations(locations);
		retval.setInstructions(tokens.nextToken());
		retval.setURL(tokens.nextToken());
		retval.setSQL(tokens.nextToken());
		retval.setUserid(tokens.nextToken());
		retval.setPassword(tokens.nextToken());
		return retval;
	}
	
	public String toString()
	{
		return locations[0] + "~" + instructions + "~" + url + "~" + sql + "~" + userid + "~" + pwd + "~";
	}
	
	public String[] getLocations() throws IOException, InterruptedException
	{
		return locations;
	}
	
	public void setLocations(String[] locations)
	{
		this.locations = new String[1];
		this.locations[0] = locations[0];
	}
	
	public void setInstructions(String instructions)
	{
		this.instructions = instructions;
	}
	
	public String getInstructions()
	{
		return instructions;
	}
	
	public void setURL(String url)
	{
		this.url = url;
	}
	
	public void setSQL(String sql)
	{
		this.sql = sql;
	}
	
	public String getSQL()
	{
		return sql;
	}
	
	public void setUserid(String userid)
	{
		this.userid = userid;
	}
	
	public String getUserid()
	{
		return userid;
	}
	
	public void setPassword(String pwd)
	{
		this.pwd = pwd;
	}
	
	public String getPassword()
	{
		return pwd;
	}
	
	public Connection getConnection()
	{
		try
		{
			Class.forName("com.ibm.db2.jcc.DB2Driver");
		}
		catch(Exception e)
		{
			System.out.println("Unable to load DB2 driver");
			e.printStackTrace(System.out);
			return null;
		}
		
		Connection con = null;
		
		try
		{
			con = DriverManager.getConnection(
					url,
					userid,
					pwd);
		}
		catch(Exception e)
		{
			System.out.println("Unable to connect to DB2 @ " + url + " with userid: " + userid + " and password: " + pwd);
			e.printStackTrace(System.out);
			return null;
		}
		
		return con;
	}

	@Override
	public long getLength() throws IOException, InterruptedException {
		
		return 1;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		/*
		 private String[] locations;
		private String instructions;
		private String url;
		private String sql;
		private String userid;
		private String pwd;
		 */
		Text temp = new Text();
		temp.readFields(in);
		instructions = temp.toString();
		temp.readFields(in);
		url = temp.toString();
		temp.readFields(in);
		sql = temp.toString();
		temp.readFields(in);
		userid = temp.toString();
		temp.readFields(in);
		pwd = temp.toString();
		
		IntWritable temp2 = new IntWritable();
		temp2.readFields(in);
		locations = new String[temp2.get()];
		
		int i = 0;
		while (i < temp2.get())
		{
			temp.readFields(in);
			locations[i] = temp.toString();
			i++;
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		/*
		 private String[] locations;
		private String instructions;
		private String url;
		private String sql;
		private String userid;
		private String pwd;
		 */
		
		(new Text(instructions)).write(out);
		(new Text(url)).write(out);
		(new Text(sql)).write(out);
		(new Text(userid)).write(out);
		(new Text(pwd)).write(out);
		(new IntWritable(locations.length)).write(out);
		
		int i = 0;
		while (i < locations.length)
		{
			(new Text(locations[i])).write(out);
			i++;
		}
	}
}
