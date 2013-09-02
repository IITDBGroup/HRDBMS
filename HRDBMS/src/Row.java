import java.sql.ResultSet;
import java.util.Vector;


public class Row implements Comparable<Row>
{
	public Vector<Comparable> cols = new Vector<Comparable>();
	private boolean[] dir;
	
	public Row(ResultSet rs, int[] sortCols, boolean[] dir) 
	{
		this.dir = dir;
		for (int colNum : sortCols)
		{
			try
			{
				cols.add((Comparable)(rs.getObject(colNum)));
			}
			catch(Exception e)
			{
				System.out.println(e);
				e.printStackTrace();
				System.exit(0);
			}
		}
	}
	
	public Row(Vector<Comparable> cols, boolean[] dir) 
	{
		this.dir = dir;
		this.cols = cols;
	}
	
	public boolean equals(Object rhs)
	{
		if (rhs instanceof Row)
		{
			Row row = (Row)rhs;
			return (this.compareTo(row) == 0);
		}
		
		return false;
	}
	
	public int compareTo(Row rhs)
	{
		int i = 0;
		for (Comparable obj : cols)
		{
			if (dir[i])
			{
				int result = obj.compareTo(rhs.cols.get(i));
				if (result < 0)
				{
					return -1;
				}
				else if (result > 0)
				{
					return 1;
				}
			}
			else
			{
				int result = obj.compareTo(rhs.cols.get(i));
				if (result < 0)
				{
					return 1;
				}
				else if (result > 0)
				{
					return -1;
				}
			}
			
			i++;
		}
		
		return 0;
	}
	
	public String toString()
	{
		return cols.toString();
	}
}
