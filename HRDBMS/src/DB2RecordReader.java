import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;

public class DB2RecordReader<K, V> extends RecordReader<K, V>
{
	private DB2InputSplit split;
	private Connection con;
	private ResultSet rs;
	private boolean processed = false;
	private boolean dummy = false;
	private ResultSetMetaDataWritable meta;
	
	public void close()
	{
		try
		{
			rs.close();
			con.close();
		}
		catch(Exception e)
		{
			System.err.println("Failed to close DB2 connection.");
			e.printStackTrace(System.err);
		}
	}
	
	public K getCurrentKey()
	{
		return ((K)(NullWritable.get()));
	}
	
	public V getCurrentValue()
	{
		if (dummy)
		{
			ResultSetWritable retval = new ResultSetWritable();
			retval.setInstructions(split.getInstructions());
			retval.setResultSetMetaData(meta);
			dummy = false; 
			return (V)retval;
		}
		ResultSetWritable retval = new ResultSetWritable(rs, 500000);
		retval.setInstructions(split.getInstructions());
		return (V)(retval);
	}
	
	public float getProgress()
	{
		if (!processed)
		{
			return 0;
		}
		else
		{
			return 1;
		}
	}
	
	public boolean nextKeyValue()
	{
		if (!processed)
		{
			try
			{
				Statement stmt = con.createStatement();
				rs = stmt.executeQuery(this.split.getSQL());
				processed = true;
				meta = new ResultSetMetaDataWritable(rs.getMetaData());
				if (!rs.next())
				{
					dummy = true;
				}
				return true;
			}
			catch(Exception e)
			{
				System.err.println("Error executing SQL: " + this.split.getSQL());
				e.printStackTrace(System.err);
			}
		}
		
		try
		{
			return rs.next();
		}
		catch(Exception e)
		{
			e.printStackTrace();
			return false;
		}
	}
	
	public void initialize(InputSplit split, TaskAttemptContext context)
	{
		this.split = (DB2InputSplit)split;
		con = this.split.getConnection();
	}
}
