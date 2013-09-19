import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Reader;
import java.io.Serializable;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;
import java.util.Vector;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class ResultSetWritable implements WritableComparable, ResultSet, Serializable
{  
	/**
	 * 
	 */
	private static final long serialVersionUID = 1697540686825938464L;
	private ResultSetMetaDataWritable meta; 
	private MyVector data; 
	public int rows;
	public int pos = -1;
	private int dir = ResultSet.FETCH_FORWARD;
	private int fetchSize = 0;
	private int hold = ResultSet.HOLD_CURSORS_OVER_COMMIT;
	private transient Statement stmt = null;
	private boolean closed = false;
	private boolean wasNull = false;
	private String instructions = "";
	
	public void print()
	{
		try
		{
			int cols = meta.getColumnCount();
			int j = 0;
			while (j < data.size())
			{
				String line = "";
				int i = 0;
				while (i < cols)
				{
					Vector<Object> row = (Vector<Object>)data.get(j);
					line += (row.get(i).toString() + "     ");
					i++;
				}
				System.out.println(line);
				j++;
			}
			System.out.println("Pos = " + pos);
		}
		catch(Exception e)
		{
			System.out.println(e);
			e.printStackTrace();
			System.exit(0);
		}
	}
	
	private void readObject(
		     ObjectInputStream aInputStream
		   ) throws ClassNotFoundException, IOException {
		     //always perform the default de-serialization first
		     aInputStream.defaultReadObject();
		  }

		    /**
		    * This is the default implementation of writeObject.
		    * Customise if necessary.
		    */
		    private void writeObject(
		      ObjectOutputStream aOutputStream
		    ) throws IOException {
		      //perform the default serialization for all non-transient, non-static fields
		      aOutputStream.defaultWriteObject();
		    }
	
	public int pages()
	{
		return data.pages;
	}
	
	ResultSetWritable getPage(int x)
	{
		ResultSetWritable retval = new ResultSetWritable();
		retval.meta = this.meta;
		
		//read data from disk
		retval.data = this.data.getPage(x);
		retval.rows = retval.data.size();
		retval.pos = -1;
		retval.instructions = this.instructions;
		return retval;
	}
	
	public void setInstructions(String in)
	{
		instructions = in;
	}
	
	public String getInstructions()
	{
		return instructions;
	}
	
	public ResultSetWritable()
	{
		data = new MyVector();
	}
	
	public void setResultSetMetaData(ResultSetMetaData meta)
	{
		try
		{
			this.meta = new ResultSetMetaDataWritable(meta);
		}
		catch(Exception e)
		{
			e.printStackTrace(System.out);
		}
	}
	
	public void addRow(Vector<Object> row)
	{
		data.add(row);
		rows++;
	}
	
	public void addRow(ResultSet rs)
	{
		try
		{
			ResultSetMetaData meta2 = rs.getMetaData();
			int limit = meta2.getColumnCount();

			Vector<Object> row = new Vector<Object>();
			int j = 0;
			while (j < limit)
			{
				Object obj = rs.getObject(j+1);

				if (obj == null)
				{
					row.add(NullWritable.get());
				}	
				else
				{
					row.add(rs.getObject(j+1));
				}
				j++;
			}
					
			data.add(row);
			rows++;
		}
		catch(Exception e)
		{
			e.printStackTrace(System.out);
		}
	}
	
	public ResultSetWritable(ResultSet rs)
	{
		int x = -1;
		
		if (rs instanceof ResultSetWritable)
		{
			this.instructions = ((ResultSetWritable)rs).getInstructions();
			try
			{
				x = rs.getRow();
				rs.beforeFirst();
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
		}
		try
		{
			pos = -1;
			this.meta = new ResultSetMetaDataWritable(rs.getMetaData());
			data = new MyVector();
			ResultSetMetaData meta2 = rs.getMetaData();
		
			int i = 0;
			while (rs.next())
			{
				int j = 0;
				data.add(new Vector<Object>());
				while (j < meta2.getColumnCount())
				{
					Object obj = rs.getObject(j+1);
					if (obj == null)
					{
						((Vector<Object>)data.get(i)).add(NullWritable.get());
					}	
					else
					{
						((Vector<Object>)data.get(i)).add(rs.getObject(j+1));
					}
					j++;
				}
			
				i++;
			}
			
			rows = i;
			
			if (rs instanceof ResultSetWritable)
			{
				rs.absolute(x);
			}
		}
		catch(Exception e)
		{
			e.printStackTrace(System.out);
		}
	}
	
	public ResultSetWritable(ResultSet rs, int maxSize)
	{
		try
		{
			pos = -1;
			this.meta = new ResultSetMetaDataWritable(rs.getMetaData());
			data = new MyVector();
			ResultSetMetaData meta2 = rs.getMetaData();
		
			int i = 0;
			do
			{
				int j = 0;
				data.add(new Vector<Object>());
				while (j < meta2.getColumnCount())
				{
					Object obj = rs.getObject(j+1);
					if (obj == null)
					{
						((Vector<Object>)data.get(i)).add(NullWritable.get());
					}	
					else
					{
						((Vector<Object>)data.get(i)).add(rs.getObject(j+1));
					}
					j++;
				}
			
				i++;
				
				if (i == maxSize)
				{
					break;
				}
				
			} while (rs.next()); 
			
			rows = i;
		}
		catch(Exception e)
		{
			e.printStackTrace(System.out);
		}
	}
	
   //Writable#write() implementation
   public void write(DataOutput out) throws IOException 
   {
	   new Text(instructions).write(out);
	   meta.write(out);
	   (new IntWritable(rows)).write(out);
	   int cols = 0;
	   try
	   {
		   cols = meta.getColumnCount();
	   }
	   catch(Exception e)
	   {
		   e.printStackTrace(System.out);
	   }
	   int i = 0;
	   while (i < rows)
	   {
		   int j = 0;
		   while (j < cols)
		   {
			   Object obj = ((Vector<Object>)data.get(i)).get(j);
			   if (obj instanceof NullWritable)
			   {
				   (new VIntWritable(0)).write(out);
				   NullWritable.get().write(out);
			   }
			   else if (obj instanceof Integer)
			   {
				   //1 = VIntWritable
				   (new VIntWritable(1)).write(out);
				   (new VIntWritable((Integer)obj)).write(out);
			   }
			   else if (obj instanceof Long)
			   {
				   //2 = VLongWritable
				   (new VIntWritable(2)).write(out);
				   (new VLongWritable((Long)obj)).write(out);
			   }
			   else if (obj instanceof Float)
			   {
				   //3 = FloatWritable
				   (new VIntWritable(3)).write(out);
				   (new FloatWritable((Float)obj)).write(out); 
			   }
			   else if (obj instanceof Double)
			   {
				   //4 = DoubleWritable
				   (new VIntWritable(4)).write(out);
				   (new DoubleWritable((Double)obj)).write(out);
			   }
			   else if (obj instanceof BigDecimal)
			   {
				   //5 = BigDecimalWritable
				   (new VIntWritable(5)).write(out);
				   (new BigDecimalWritable((BigDecimal)obj)).write(out);
			   }
			   else if (obj instanceof String)
			   {
				   //6 = Text
				   (new VIntWritable(6)).write(out);
				   (new Text((String)obj)).write(out);
			   }
			   else if (obj instanceof byte[])
			   {
				   //7 = BytesWritable
				   (new VIntWritable(7)).write(out);
				   (new BytesWritable((byte[])obj)).write(out);
			   }
			   else if (obj instanceof Date)
			   {
				   //8 = Date
				   (new VIntWritable(8)).write(out);
				   long time = ((Date)(obj)).getTime();
				   (new VLongWritable(time)).write(out);
			   }
			   else if (obj instanceof Time)
			   {
				   //9 = Time
				   (new VIntWritable(9)).write(out);
				   long time = ((Time)(obj)).getTime();
				   (new VLongWritable(time)).write(out);
			   }
			   else if (obj instanceof Timestamp)
			   {
				   //10 = Timestamp
				   (new VIntWritable(10)).write(out);
				   long time = ((Timestamp)(obj)).getTime();
				   (new VLongWritable(time)).write(out);
			   }
			   else
			   {
				   throw new IOException("Unknown data type: " + obj.getClass().toString());
			   }
			   j++;
		   }
		   i++;
	   }
	}
       
   //Writable#readFields() implementation
   public void readFields(DataInput in) throws IOException 
   {
	   pos = -1;
	   Text tmp = new Text();
	   tmp.readFields(in);
	   instructions = tmp.toString();
	   data.clear();
	   meta = new ResultSetMetaDataWritable();
	   meta.readFields(in);
	   IntWritable temp = new IntWritable();
	   temp.readFields(in);
	   rows = temp.get();
	   int cols = 0;
	   try
	   {
		   cols = meta.getColumnCount();
	   }
	   catch(Exception e)
	   {
		   e.printStackTrace(System.out);
	   }
	   int i = 0;
	   while (i < rows)
	   {
		   int j = 0;
		   data.add(new Vector<Object>());
		   while (j < cols)
		   {
			   VIntWritable temp2 = new VIntWritable();
			   temp2.readFields(in);
			   if (temp2.get() == 0)
			   {
				   ((Vector<Object>)data.get(i)).add(NullWritable.get());
			   }
			   else if (temp2.get() == 1)
			   {
				   //1 = VIntWritable
				   temp2.readFields(in);
				   ((Vector<Object>)data.get(i)).add(temp2.get());
			   }
			   else if (temp2.get() == 2)
			   {
				   //2 = VLongWritable
				   VLongWritable temp3 = new VLongWritable();
				   temp3.readFields(in);
				   ((Vector<Object>)data.get(i)).add(temp3.get());
			   }
			   else if (temp2.get() == 3)
			   {
				   //3 = FloatWritable
				   FloatWritable temp3 = new FloatWritable();
				   temp3.readFields(in);
				   ((Vector<Object>)data.get(i)).add(temp3.get());
			   }
			   else if (temp2.get() == 4)
			   {
				   //4 = DoubleWritable
				   DoubleWritable temp3 = new DoubleWritable();
				   temp3.readFields(in);
				   ((Vector<Object>)data.get(i)).add(temp3.get());
			   }
			   else if (temp2.get() == 5)
			   {
				   //5 = BigDecimalWritable
				   BigDecimalWritable temp3 = new BigDecimalWritable();
				   temp3.readFields(in);
				   ((Vector<Object>)data.get(i)).add(temp3.get());
			   }
			   else if (temp2.get() == 6)
			   {
				   //6 = Text
				   Text temp3 = new Text();
				   temp3.readFields(in);
				   ((Vector<Object>)data.get(i)).add(temp3.toString());
			   }
			   else if (temp2.get() == 7)
			   {
				   //7 = BytesWritable
				   BytesWritable temp3 = new BytesWritable();
				   temp3.readFields(in);
				   ((Vector<Object>)data.get(i)).add(temp3.getBytes());
			   }
			   else if (temp2.get() == 8)
			   {
				   //8 = Date
				   VLongWritable temp3 = new VLongWritable();
				   temp3.readFields(in);
				   ((Vector<Object>)data.get(i)).add(new Date(temp3.get()));
			   }
			   else if (temp2.get() == 9)
			   {
				   //9 = Time
				   VLongWritable temp3 = new VLongWritable();
				   temp3.readFields(in);
				   ((Vector<Object>)data.get(i)).add(new Time(temp3.get()));
			   }
			   else if (temp2.get() == 10)
			   {
				   //10 = Timestamp
				   VLongWritable temp3 = new VLongWritable();
				   temp3.readFields(in);
				   ((Vector<Object>)data.get(i)).add(new Timestamp(temp3.get()));
			   }
			   else
			   {
				   throw new IOException("Unknown data type: " + temp2.get());
			   }
			   j++;
		   }
		   i++;
	   }
   }

@Override
public boolean isWrapperFor(Class<?> iface) throws SQLException {
	return false;
}

@Override
public <T> T unwrap(Class<T> iface) throws SQLException {
	return null;
}

@Override
public boolean absolute(int row) throws SQLException {
	if (row > 0)
	{
		pos = row - 1;
	}
	else
	{
		pos = rows - row;
	}
	
	if (pos <= -1)
	{
		pos = -1;
		return false;
	}
	
	if (pos >= rows)
	{
		pos = rows;
		return false;
	}
	
	return true;
}

@Override
public void afterLast() throws SQLException {
	pos = rows;
	
}

@Override
public void beforeFirst() throws SQLException {
	pos = -1;
	
}

@Override
public void cancelRowUpdates() throws SQLException {
	throw new SQLException("Result sets are read-only");
}

@Override
public void clearWarnings() throws SQLException {
}

@Override
public void close() throws SQLException {
	closed = true;
}

@Override
public void deleteRow() throws SQLException {
	throw new SQLException("Result sets are read only.");
}

@Override
public int findColumn(String columnLabel) throws SQLException {
	return meta.findColumn(columnLabel);
}

@Override
public boolean first() throws SQLException {
	pos = 0;
	if (rows == 0)
	{
		return false;
	}
	return true;
}

@Override
public Array getArray(int columnIndex) throws SQLException {
	throw new SQLException("getArray is not implemented.");
}

@Override
public Array getArray(String columnLabel) throws SQLException {
	throw new SQLException("getArray is not implemented.");
}

@Override
public InputStream getAsciiStream(int columnIndex) throws SQLException {
	throw new SQLException("getAsciiStream is not implemented.");
}

@Override
public InputStream getAsciiStream(String columnLabel) throws SQLException {
	throw new SQLException("getAsciiStream is not implemented.");
}

@Override
public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
	Object obj = ((Vector<Object>)data.get(pos)).get(columnIndex - 1);
	if (obj instanceof NullWritable)
	{
		wasNull = true;
		return null;
	}
	else if (obj instanceof BigDecimal)
	{
		wasNull = false;
		return (BigDecimal)obj;
	}
	throw new SQLException("Column is not of type BigDecimal.");
}
 
@Override
public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
	Object obj = ((Vector<Object>)data.get(pos)).get(meta.findColumn(columnLabel) - 1);
	if (obj instanceof NullWritable)
	{
		wasNull = true;
		return null;
	}
	else if (obj instanceof BigDecimal)
	{
		wasNull = false;
		return (BigDecimal)obj;
	}
	throw new SQLException("Column is not of type BigDecimal.");
}

@Override
public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
	throw new SQLException("getBigDecimal(int, int) is not implemented.");
}

@Override
public BigDecimal getBigDecimal(String columnLabel, int scale)
		throws SQLException {
	throw new SQLException("getBigDecimal(int, int) is not implemented.");
}

@Override
public InputStream getBinaryStream(int columnIndex) throws SQLException {
	throw new SQLException("getBinaryStream is not implemented.");
}

@Override
public InputStream getBinaryStream(String columnLabel) throws SQLException {
	throw new SQLException("getBinaryStream is not implemented.");
}

@Override
public Blob getBlob(int columnIndex) throws SQLException {
	throw new SQLException("getBlob is not implemented.");
}

@Override
public Blob getBlob(String columnLabel) throws SQLException {
	throw new SQLException("getBlob is not implemented.");
}

@Override
public boolean getBoolean(int columnIndex) throws SQLException {
	throw new SQLException("getBoolean is not implemented.");
}

@Override
public boolean getBoolean(String columnLabel) throws SQLException {
	throw new SQLException("getBoolean is not implemented.");
}

@Override
public byte getByte(int columnIndex) throws SQLException {
	throw new SQLException("getByte is not implemented.");
}

@Override
public byte getByte(String columnLabel) throws SQLException {
	throw new SQLException("getByte is not implemented.");
}

@Override
public byte[] getBytes(int columnIndex) throws SQLException {
	Object obj = ((Vector<Object>)data.get(pos)).get(columnIndex - 1);
	if (obj instanceof NullWritable)
	{
		wasNull = true;
		return null;
	}
	else if (obj instanceof byte[])
	{
		wasNull = false;
		return (byte[])obj;
	}
	throw new SQLException("Column is not of type byte[].");
}

@Override
public byte[] getBytes(String columnLabel) throws SQLException {
	Object obj = ((Vector<Object>)data.get(pos)).get(meta.findColumn(columnLabel) - 1);
	if (obj instanceof NullWritable)
	{
		wasNull = true;
		return null;
	}
	else if (obj instanceof byte[])
	{
		wasNull = false;
		return (byte[])obj;
	}
	throw new SQLException("Column is not of type byte[].");
}

@Override
public Reader getCharacterStream(int columnIndex) throws SQLException {
	throw new SQLException("getCharacterStream is not implemented.");
}

@Override
public Reader getCharacterStream(String columnLabel) throws SQLException {
	throw new SQLException("getCharacterStream is not implemented.");
}

@Override
public Clob getClob(int columnIndex) throws SQLException {
	throw new SQLException("getClob is not implemented.");
}

@Override
public Clob getClob(String columnLabel) throws SQLException {
	throw new SQLException("getClob is not implemented.");
}

@Override
public int getConcurrency() throws SQLException {
	return ResultSet.CONCUR_READ_ONLY;
}

@Override
public String getCursorName() throws SQLException {
	return "cursor1";
}

@Override
public Date getDate(int columnIndex) throws SQLException {
	Object obj = ((Vector<Object>)data.get(pos)).get(columnIndex - 1);
	if (obj instanceof NullWritable)
	{
		wasNull = true;
		return null;
	}
	else if (obj instanceof Date)
	{
		wasNull = false;
		return (Date)obj;
	}
	throw new SQLException("Column is not of type Date.");
}

@Override
public Date getDate(String columnLabel) throws SQLException {
	Object obj = ((Vector<Object>)data.get(pos)).get(meta.findColumn(columnLabel) - 1);
	if (obj instanceof NullWritable)
	{
		wasNull = true;
		return null;
	}
	else if (obj instanceof Date)
	{
		wasNull = false;
		return (Date)obj;
	}
	throw new SQLException("Column is not of type Date.");
}

@Override
public Date getDate(int columnIndex, Calendar cal) throws SQLException {
	throw new SQLException("getDate(int, Calendar) not implemented.");
}

@Override
public Date getDate(String columnLabel, Calendar cal) throws SQLException {
	throw new SQLException("getDate(String, Calendar) not implemented.");
}

@Override
public double getDouble(int columnIndex) throws SQLException {
	Object obj = ((Vector<Object>)data.get(pos)).get(columnIndex - 1);
	if (obj instanceof NullWritable)
	{
		wasNull = true;
		return 0;
	}
	else if (obj instanceof Double)
	{
		wasNull = false;
		return (Double)obj;
	}
	else if (obj instanceof Float)
	{
		wasNull = false;
		return ((Float)obj);
	}
	throw new SQLException("Column is not of type Double or Float.");
}

@Override
public double getDouble(String columnLabel) throws SQLException {
	Object obj = ((Vector<Object>)data.get(pos)).get(meta.findColumn(columnLabel) - 1);
	if (obj instanceof NullWritable)
	{
		wasNull = true;
		return 0;
	}
	else if (obj instanceof Double)
	{
		wasNull = false;
		return (Double)obj;
	}
	else if (obj instanceof Float)
	{
		wasNull = false;
		return ((Float)obj);
	}
	throw new SQLException("Column is not of type Double or Float.");
}

@Override
public int getFetchDirection() throws SQLException {
	return dir;
}

@Override
public int getFetchSize() throws SQLException {
	return fetchSize;
}

@Override
public float getFloat(int columnIndex) throws SQLException {
	Object obj = ((Vector<Object>)data.get(pos)).get(columnIndex - 1);
	if (obj instanceof NullWritable)
	{
		wasNull = true;
		return 0;
	}
	else if (obj instanceof Float)
	{
		wasNull = false;
		return (Float)obj;
	}
	throw new SQLException("Column is not of type Float." + obj.toString() + obj.getClass().toString());
}

@Override
public float getFloat(String columnLabel) throws SQLException {
	Object obj = ((Vector<Object>)data.get(pos)).get(meta.findColumn(columnLabel) - 1);
	if (obj instanceof NullWritable)
	{
		wasNull = true;
		return 0;
	}
	else if (obj instanceof Float)
	{
		wasNull = false;
		return (Float)obj;
	}
	throw new SQLException("Column is not of type Float.");
}

@Override
public int getHoldability() throws SQLException {
	return hold;
}

@Override
public int getInt(int columnIndex) throws SQLException {
	Object obj = ((Vector<Object>)data.get(pos)).get(columnIndex - 1);
	if (obj instanceof NullWritable)
	{
		wasNull = true;
		return 0;
	}
	else if (obj instanceof Integer)
	{
		wasNull = false;
		return (Integer)obj;
	}
	throw new SQLException("Column is not of type Integer.");
}

@Override
public int getInt(String columnLabel) throws SQLException {
	Object obj = ((Vector<Object>)data.get(pos)).get(meta.findColumn(columnLabel) - 1);
	if (obj instanceof NullWritable)
	{
		wasNull = true;
		return 0;
	}
	else if (obj instanceof Integer)
	{
		wasNull = false;
		return (Integer)obj;
	}
	throw new SQLException("Column is not of type Integer.");
}

@Override
public long getLong(int columnIndex) throws SQLException {
	Object obj = ((Vector<Object>)data.get(pos)).get(columnIndex - 1);
	if (obj instanceof NullWritable)
	{
		wasNull = true;
		return 0;
	}
	else if (obj instanceof Integer)
	{
		wasNull = false;
		return (Integer)obj;
	}
	else if (obj instanceof Long)
	{
		wasNull = false;
		return (Long) obj;
	}
	throw new SQLException("Column is not of type Integer or Long.");
}

@Override
public long getLong(String columnLabel) throws SQLException {
	Object obj = ((Vector<Object>)data.get(pos)).get(meta.findColumn(columnLabel) - 1);
	if (obj instanceof NullWritable)
	{
		wasNull = true;
		return 0;
	}
	else if (obj instanceof Integer)
	{
		wasNull = false;
		return (Integer)obj;
	}
	else if (obj instanceof Long)
	{
		wasNull = false;
		return (Long) obj;
	}
	throw new SQLException("Column is not of type Integer or Long.");
}

@Override
public ResultSetMetaData getMetaData() throws SQLException {
	return meta;
}

@Override
public Reader getNCharacterStream(int columnIndex) throws SQLException {
	throw new SQLException("getNCharacterStream is not implemented.");
}

@Override
public Reader getNCharacterStream(String columnLabel) throws SQLException {
	throw new SQLException("getNCharacterStream is not implemented.");
}

@Override
public NClob getNClob(int columnIndex) throws SQLException {
	throw new SQLException("getNClob is not implemented.");
}

@Override
public NClob getNClob(String columnLabel) throws SQLException {
	throw new SQLException("getNClob is not implemented.");
}

@Override
public String getNString(int columnIndex) throws SQLException {
	Object obj = ((Vector<Object>)data.get(pos)).get(columnIndex - 1);
	if (obj instanceof NullWritable)
	{
		wasNull = true;
		return null;
	}
	else if (obj instanceof String)
	{
		wasNull = false;
		return (String)obj;
	}
	throw new SQLException("Column is not of type String.");
}

@Override
public String getNString(String columnLabel) throws SQLException {
	Object obj = ((Vector<Object>)data.get(pos)).get(meta.findColumn(columnLabel) - 1);
	if (obj instanceof NullWritable)
	{
		wasNull = true;
		return null;
	}
	else if (obj instanceof String)
	{
		wasNull = false;
		return (String)obj;
	}
	throw new SQLException("Column is not of type String.");
}

@Override
public Object getObject(int columnIndex) throws SQLException {
	Object obj = ((Vector<Object>)data.get(pos)).get(columnIndex - 1);
	if (obj instanceof NullWritable)
	{
		wasNull = true;
		return null;
	}
	
	wasNull = false;
	return obj;
}

@Override
public Object getObject(String columnLabel) throws SQLException {
	Object obj = ((Vector<Object>)data.get(pos)).get(meta.findColumn(columnLabel) - 1);
	if (obj instanceof NullWritable)
	{
		wasNull = true;
		return null;
	}
	
	wasNull = false;
	return obj;
}

@Override
public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
	throw new SQLException("getObject(int, Map) is not implemented.");
}

@Override
public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
	throw new SQLException("getObject(String, Map) is not implemented.");
}

@Override
public Ref getRef(int columnIndex) throws SQLException {
	throw new SQLException("getRef is not implemented.");
}

@Override
public Ref getRef(String columnLabel) throws SQLException {
	throw new SQLException("getRef is not implemented.");
}

@Override
public int getRow() throws SQLException {
	return pos + 1;
}

@Override
public RowId getRowId(int columnIndex) throws SQLException {
	throw new SQLException("getRowID is not implemented.");
}

@Override
public RowId getRowId(String columnLabel) throws SQLException {
	throw new SQLException("getRowID is not implemented.");
}

@Override
public SQLXML getSQLXML(int columnIndex) throws SQLException {
	throw new SQLException("getSQLXML is not implemented.");
}

@Override
public SQLXML getSQLXML(String columnLabel) throws SQLException {
	throw new SQLException("getSQLXML is not implemented.");
}

@Override
public short getShort(int columnIndex) throws SQLException {
	throw new SQLException("getShort is not implemented.");
}

@Override
public short getShort(String columnLabel) throws SQLException {
	throw new SQLException("getShort is not implemented.");
}

@Override
public Statement getStatement() throws SQLException {
	return stmt;
}

@Override
public String getString(int columnIndex) throws SQLException {
	Object obj = ((Vector<Object>)data.get(pos)).get(columnIndex - 1);
	if (obj instanceof NullWritable)
	{
		wasNull = true;
		return null;
	}
	else if (obj instanceof String)
	{
		wasNull = false;
		return (String)obj;
	}
	throw new SQLException("Column is not of type String.");
}

@Override
public String getString(String columnLabel) throws SQLException {
	Object obj = ((Vector<Object>)data.get(pos)).get(meta.findColumn(columnLabel) - 1);
	if (obj instanceof NullWritable)
	{
		wasNull = true;
		return null;
	}
	else if (obj instanceof String)
	{
		wasNull = false;
		return (String)obj;
	}
	throw new SQLException("Column is not of type String.");
}

@Override
public Time getTime(int columnIndex) throws SQLException {
	Object obj = ((Vector<Object>)data.get(pos)).get(columnIndex - 1);
	if (obj instanceof NullWritable)
	{
		wasNull = true;
		return null;
	}
	else if (obj instanceof Time)
	{
		wasNull = false;
		return (Time)obj;
	}
	throw new SQLException("Column is not of type Time.");
}

@Override
public Time getTime(String columnLabel) throws SQLException {
	Object obj = ((Vector<Object>)data.get(pos)).get(meta.findColumn(columnLabel) - 1);
	if (obj instanceof NullWritable)
	{
		wasNull = true;
		return null;
	}
	else if (obj instanceof Time)
	{
		wasNull = false;
		return (Time)obj;
	}
	throw new SQLException("Column is not of type Time.");
}

@Override
public Time getTime(int columnIndex, Calendar cal) throws SQLException {
	throw new SQLException("getTime(int, Calendar) is not implemented.");
}

@Override
public Time getTime(String columnLabel, Calendar cal) throws SQLException {
	throw new SQLException("getTime(int, Calendar) is not implemented.");
}

@Override
public Timestamp getTimestamp(int columnIndex) throws SQLException {
	Object obj = ((Vector<Object>)data.get(pos)).get(columnIndex - 1);
	if (obj instanceof NullWritable)
	{
		wasNull = true;
		return null;
	}
	else if (obj instanceof Timestamp)
	{
		wasNull = false;
		return (Timestamp)obj;
	}
	throw new SQLException("Column is not of type Timestamp.");
}

@Override
public Timestamp getTimestamp(String columnLabel) throws SQLException {
	Object obj = ((Vector<Object>)data.get(pos)).get(meta.findColumn(columnLabel) - 1);
	if (obj instanceof NullWritable)
	{
		wasNull = true;
		return null;
	}
	else if (obj instanceof Timestamp)
	{
		wasNull = false;
		return (Timestamp)obj;
	}
	throw new SQLException("Column is not of type Timestamp.");
}

@Override
public Timestamp getTimestamp(int columnIndex, Calendar cal)
		throws SQLException {
	throw new SQLException("getTimestamp(int, Calendar) is not implemented.");
}

@Override
public Timestamp getTimestamp(String columnLabel, Calendar cal)
		throws SQLException {
	throw new SQLException("getTimestamp(String, Calendar) is not implemented.");
}

@Override
public int getType() throws SQLException {
	return ResultSet.TYPE_SCROLL_INSENSITIVE;
}

@Override
public URL getURL(int columnIndex) throws SQLException {
	throw new SQLException("getURL is not implemented.");
}

@Override
public URL getURL(String columnLabel) throws SQLException {
	throw new SQLException("getURL is not implemented.");
}

@Override
public InputStream getUnicodeStream(int columnIndex) throws SQLException {
	throw new SQLException("getUnicodeStream is not implemented.");
}

@Override
public InputStream getUnicodeStream(String columnLabel) throws SQLException {
	throw new SQLException("getUnicodeStream is not implemented.");
}

@Override
public SQLWarning getWarnings() throws SQLException {
	return null;
}

@Override
public void insertRow() throws SQLException {
	throw new SQLException("Result sets are read-only.");
}

@Override
public boolean isAfterLast() throws SQLException {
	if (pos == rows)
	{
		return true;
	}
	
	return false;
}

@Override
public boolean isBeforeFirst() throws SQLException {
	if (pos == -1)
	{
		return true;
	}
	
	return false;
}

@Override
public boolean isClosed() throws SQLException {
	return closed;
}

@Override
public boolean isFirst() throws SQLException {
	if (pos == 0)
	{
		return true;
	}
	
	return false;
}

@Override
public boolean isLast() throws SQLException {
	if (pos == rows - 1)
	{
		return true;
	}
	
	return false;
}

@Override
public boolean last() throws SQLException {
	pos = rows;
	
	return (rows > 0);
}

@Override
public void moveToCurrentRow() throws SQLException {
	
}

@Override
public void moveToInsertRow() throws SQLException {
	throw new SQLException("Result sets are read-only.");
	
}

@Override
public boolean next() throws SQLException {
	pos++;
	if (pos < rows && pos >= 0)
	{
		return true;
	}
	
	return false;
}

@Override
public boolean previous() throws SQLException {
	pos--;
	
	if (pos < rows && pos >= 0)
	{
		return true;
	}
	
	return false;
}

@Override
public void refreshRow() throws SQLException {
	throw new SQLException("refreshRow is not implemented");
}

@Override
public boolean relative(int rows) throws SQLException {
	pos += rows;
	
	if (pos < rows && pos >= 0)
	{
		return true;
	}
	
	return false;
}

@Override
public boolean rowDeleted() throws SQLException {
	return false;
}

@Override
public boolean rowInserted() throws SQLException {
	return false;
}

@Override
public boolean rowUpdated() throws SQLException {
	return false;
}

@Override
public void setFetchDirection(int direction) throws SQLException {
	dir = direction;
	
}

@Override
public void setFetchSize(int rows) throws SQLException {
	fetchSize = rows;
}

@Override
public void updateArray(int columnIndex, Array x) throws SQLException {
	throw new SQLException("Result sets are read-only.");
	
}

@Override
public void updateArray(String columnLabel, Array x) throws SQLException {
	throw new SQLException("Result sets are read-only.");
	
}

@Override
public void updateAsciiStream(int columnIndex, InputStream x)
		throws SQLException {
	throw new SQLException("Result sets are read-only.");
	
}

@Override
public void updateAsciiStream(String columnLabel, InputStream x)
		throws SQLException {
	throw new SQLException("Result sets are read-only.");
	
}

@Override
public void updateAsciiStream(int columnIndex, InputStream x, int length)
		throws SQLException {
	throw new SQLException("Result sets are read-only.");
	
}

@Override
public void updateAsciiStream(String columnLabel, InputStream x, int length)
		throws SQLException {
	throw new SQLException("Result sets are read-only.");
	
}

@Override
public void updateAsciiStream(int columnIndex, InputStream x, long length)
		throws SQLException {
	throw new SQLException("Result sets are read-only.");
	
}

@Override
public void updateAsciiStream(String columnLabel, InputStream x, long length)
		throws SQLException {
	throw new SQLException("Result sets are read-only.");
	
}

@Override
public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
	throw new SQLException("Result sets are read-only.");
	
}

@Override
public void updateBigDecimal(String columnLabel, BigDecimal x)
		throws SQLException {
	throw new SQLException("Result sets are read-only.");
	
}

@Override
public void updateBinaryStream(int columnIndex, InputStream x)
		throws SQLException {
	throw new SQLException("Result sets are read-only.");
	
}

@Override
public void updateBinaryStream(String columnLabel, InputStream x)
		throws SQLException {
	throw new SQLException("Result sets are read-only.");
	
}

@Override
public void updateBinaryStream(int columnIndex, InputStream x, int length)
		throws SQLException {
	throw new SQLException("Result sets are read-only.");
	
}

@Override
public void updateBinaryStream(String columnLabel, InputStream x, int length)
		throws SQLException {
	throw new SQLException("Result sets are read-only.");
	
}

@Override
public void updateBinaryStream(int columnIndex, InputStream x, long length)
		throws SQLException {
	throw new SQLException("Result sets are read-only.");
	
}

@Override
public void updateBinaryStream(String columnLabel, InputStream x, long length)
		throws SQLException {
	throw new SQLException("Result sets are read-only.");
	
}

@Override
public void updateBlob(int columnIndex, Blob x) throws SQLException {
	throw new SQLException("Result sets are read-only.");
	
}

@Override
public void updateBlob(String columnLabel, Blob x) throws SQLException {
	throw new SQLException("Result sets are read-only.");
	
}

@Override
public void updateBlob(int columnIndex, InputStream inputStream)
		throws SQLException {
	throw new SQLException("Result sets are read-only.");
	
}

@Override
public void updateBlob(String columnLabel, InputStream inputStream)
		throws SQLException {
	throw new SQLException("Result sets are read-only.");
	
}

@Override
public void updateBlob(int columnIndex, InputStream inputStream, long length)
		throws SQLException {
	throw new SQLException("Result sets are read-only.");
	
}

@Override
public void updateBlob(String columnLabel, InputStream inputStream, long length)
		throws SQLException {
	throw new SQLException("Result sets are read-only.");
	
}

@Override
public void updateBoolean(int columnIndex, boolean x) throws SQLException {
	throw new SQLException("Result sets are read-only.");
	
}

@Override
public void updateBoolean(String columnLabel, boolean x) throws SQLException {
	throw new SQLException("Result sets are read-only.");
	
}

@Override
public void updateByte(int columnIndex, byte x) throws SQLException {
	throw new SQLException("Result sets are read-only.");
	
}

@Override
public void updateByte(String columnLabel, byte x) throws SQLException {
	throw new SQLException("Result sets are read-only.");
	
}

@Override
public void updateBytes(int columnIndex, byte[] x) throws SQLException {
	throw new SQLException("Result sets are read-only.");
	
}

@Override
public void updateBytes(String columnLabel, byte[] x) throws SQLException {
	throw new SQLException("Result sets are read-only.");
	
}

@Override
public void updateCharacterStream(int columnIndex, Reader x)
		throws SQLException {
	throw new SQLException("Result sets are read-only.");
	
}

@Override
public void updateCharacterStream(String columnLabel, Reader reader)
		throws SQLException {
	throw new SQLException("Result sets are read-only.");
	
}

@Override
public void updateCharacterStream(int columnIndex, Reader x, int length)
		throws SQLException {
	throw new SQLException("Result sets are read-only.");
	
}

@Override
public void updateCharacterStream(String columnLabel, Reader reader, int length)
		throws SQLException {
	throw new SQLException("Result sets are read-only.");
	
}

@Override
public void updateCharacterStream(int columnIndex, Reader x, long length)
		throws SQLException {
	throw new SQLException("Result sets are read-only.");
	
}

@Override
public void updateCharacterStream(String columnLabel, Reader reader, long length)
		throws SQLException {
	throw new SQLException("Result sets are read-only.");
	
}

@Override
public void updateClob(int columnIndex, Clob x) throws SQLException {
	throw new SQLException("Result sets are read-only.");
	
}

@Override
public void updateClob(String columnLabel, Clob x) throws SQLException {
	throw new SQLException("Result sets are read-only.");
	
}

@Override
public void updateClob(int columnIndex, Reader reader) throws SQLException {
	throw new SQLException("Result sets are read-only.");
	
}

@Override
public void updateClob(String columnLabel, Reader reader) throws SQLException {
	throw new SQLException("Result sets are read-only.");
	
}

@Override
public void updateClob(int columnIndex, Reader reader, long length)
		throws SQLException {
	throw new SQLException("Result sets are read-only.");
	
}

@Override
public void updateClob(String columnLabel, Reader reader, long length)
		throws SQLException {
	throw new SQLException("Result sets are read-only.");
	
}

@Override
public void updateDate(int columnIndex, Date x) throws SQLException {
	throw new SQLException("Result sets are read-only.");
	
}

@Override
public void updateDate(String columnLabel, Date x) throws SQLException {
	throw new SQLException("Result sets are read-only.");
	
}

@Override
public void updateDouble(int columnIndex, double x) throws SQLException {
	throw new SQLException("Result sets are read-only.");
	
}

@Override
public void updateDouble(String columnLabel, double x) throws SQLException {
	throw new SQLException("Result sets are read-only.");
	
}

@Override
public void updateFloat(int columnIndex, float x) throws SQLException {
	throw new SQLException("Result sets are read-only.");
	
}

@Override
public void updateFloat(String columnLabel, float x) throws SQLException {
	throw new SQLException("Result sets are read-only.");
	
}

@Override
public void updateInt(int columnIndex, int x) throws SQLException {
	throw new SQLException("Result sets are read-only.");
	
}

@Override
public void updateInt(String columnLabel, int x) throws SQLException {
	throw new SQLException("Result sets are read-only.");
	
}

@Override
public void updateLong(int columnIndex, long x) throws SQLException {
	throw new SQLException("Result sets are read-only.");
	
}

@Override
public void updateLong(String columnLabel, long x) throws SQLException {
	throw new SQLException("Result sets are read-only.");
	
}

@Override
public void updateNCharacterStream(int columnIndex, Reader x)
		throws SQLException {
	throw new SQLException("Result sets are read-only.");
	
}

@Override
public void updateNCharacterStream(String columnLabel, Reader reader)
		throws SQLException {
	throw new SQLException("Result sets are read-only.");
	
}

@Override
public void updateNCharacterStream(int columnIndex, Reader x, long length)
		throws SQLException {
	throw new SQLException("Result sets are read-only.");
	
}

@Override
public void updateNCharacterStream(String columnLabel, Reader reader,
		long length) throws SQLException {
	throw new SQLException("Result sets are read-only.");
	
}

@Override
public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
	throw new SQLException("Result sets are read-only.");
	
}

@Override
public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
	throw new SQLException("Result sets are read-only.");
	
}

@Override
public void updateNClob(int columnIndex, Reader reader) throws SQLException {
	throw new SQLException("Result sets are read-only.");
	
}

@Override
public void updateNClob(String columnLabel, Reader reader) throws SQLException {
	throw new SQLException("Result sets are read-only.");
	
}

@Override
public void updateNClob(int columnIndex, Reader reader, long length)
		throws SQLException {
	throw new SQLException("Result sets are read-only.");
	
}

@Override
public void updateNClob(String columnLabel, Reader reader, long length)
		throws SQLException {
	throw new SQLException("Result sets are read-only.");
	
}

@Override
public void updateNString(int columnIndex, String nString) throws SQLException {
	throw new SQLException("Result sets are read-only.");
	
}

@Override
public void updateNString(String columnLabel, String nString)
		throws SQLException {
	throw new SQLException("Result sets are read-only.");
	
}

@Override
public void updateNull(int columnIndex) throws SQLException {
	throw new SQLException("Result sets are read-only.");
	
}

@Override
public void updateNull(String columnLabel) throws SQLException {
	throw new SQLException("Result sets are read-only.");
	
}

@Override
public void updateObject(int columnIndex, Object x) throws SQLException {
	throw new SQLException("Result sets are read-only.");
	
}

@Override
public void updateObject(String columnLabel, Object x) throws SQLException {
	throw new SQLException("Result sets are read-only.");
	
}

@Override
public void updateObject(int columnIndex, Object x, int scaleOrLength)
		throws SQLException {
	throw new SQLException("Result sets are read-only.");
	
}

@Override
public void updateObject(String columnLabel, Object x, int scaleOrLength)
		throws SQLException {
	throw new SQLException("Result sets are read-only.");
	
}

@Override
public void updateRef(int columnIndex, Ref x) throws SQLException {
	throw new SQLException("Result sets are read-only.");
	
}

@Override
public void updateRef(String columnLabel, Ref x) throws SQLException {
	throw new SQLException("Result sets are read-only.");
	
}

@Override
public void updateRow() throws SQLException {
	throw new SQLException("Result sets are read-only.");
	
}

@Override
public void updateRowId(int columnIndex, RowId x) throws SQLException {
	throw new SQLException("Result sets are read-only.");
	
}

@Override
public void updateRowId(String columnLabel, RowId x) throws SQLException {
	throw new SQLException("Result sets are read-only.");
	
}

@Override
public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
	throw new SQLException("Result sets are read-only.");
	
}

@Override
public void updateSQLXML(String columnLabel, SQLXML xmlObject)
		throws SQLException {
	throw new SQLException("Result sets are read-only.");
	
}

@Override
public void updateShort(int columnIndex, short x) throws SQLException {
	throw new SQLException("Result sets are read-only.");
	
}

@Override
public void updateShort(String columnLabel, short x) throws SQLException {
	throw new SQLException("Result sets are read-only.");
	
}

@Override
public void updateString(int columnIndex, String x) throws SQLException {
	throw new SQLException("Result sets are read-only.");
	
}

@Override
public void updateString(String columnLabel, String x) throws SQLException {
	throw new SQLException("Result sets are read-only.");
	
}

@Override
public void updateTime(int columnIndex, Time x) throws SQLException {
	throw new SQLException("Result sets are read-only.");
	
}

@Override
public void updateTime(String columnLabel, Time x) throws SQLException {
	throw new SQLException("Result sets are read-only.");
	
}

@Override
public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
	throw new SQLException("Result sets are read-only.");
	
}

@Override
public void updateTimestamp(String columnLabel, Timestamp x)
		throws SQLException {
	throw new SQLException("Result sets are read-only.");
	
}

@Override
public boolean wasNull() throws SQLException {
	return wasNull;
}

@Override
public int compareTo(Object arg0) {
	return 0;
}

}