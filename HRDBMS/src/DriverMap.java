import java.io.IOException;
import java.sql.ResultSet;
import java.util.StringTokenizer;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class DriverMap extends Mapper<NullWritable, ResultSetWritable, Text, ResultSetWritable>
{
	private static void printResultSet(ResultSet rs)
	{
		try
		{
			int cols = rs.getMetaData().getColumnCount();
			while (rs.next())
			{
				String line = "";
				int i = 0;
				while (i < cols)
				{
					line += (rs.getObject(i+1).toString() + "     ");
					i++;
				}
				System.out.println(line);
			}
		}
		catch(Exception e)
		{
			System.out.println(e);
			e.printStackTrace();
			System.exit(0);
		}
	}
	
	protected void map(NullWritable key, ResultSetWritable rs, Context context) throws IOException, InterruptedException
	{
		String instructions = rs.getInstructions();
		if (instructions.equals("Q1") || instructions.equals("Q3") || instructions.equals("Q7") || instructions.equals("Q12") || instructions.equals("Q18") || instructions.equals("Q21") || instructions.equals("Q8A") || instructions.equals("Q8B") || instructions.equals("Q8C") || instructions.equals("Q13A") || instructions.equals("Q13B") || instructions.equals("Q17A") || instructions.equals("Q17B") || instructions.equals("Q17C") || instructions.equals("Q20A") || instructions.equals("Q20B") || instructions.equals("Q20C"))
		{
			if (rs.pages() == 1)
			{
				context.write(new Text("1"), rs);
			}
			else
			{
				int i = 0;
				while (i < rs.pages())
				{
					context.write(new Text("1"), rs.getPage(i));
					i++;
				}
			}
		}
		else if (instructions.equals("Q8A1"))
		{
			if (rs.pages() == 1)
			{
				context.write(new Text("1"), rs);
			}
			else
			{
				int i = 0;
				while (i < rs.pages())
				{
					context.write(new Text("1"), rs.getPage(i));
					i++;
				}
			}
		}
		else if (instructions.equals("Q8A2"))
		{
			if (rs.pages() == 1)
			{
				context.write(new Text("2"), rs);
			}
			else
			{
				int i = 0;
				while (i < rs.pages())
				{
					context.write(new Text("2"), rs.getPage(i));
					i++;
				}
			}
		}
		else if (instructions.equals("Q8B0") || instructions.equals("Q8C0"))
		{
			if (rs.pages() == 1)
			{
				context.write(new Text("1"), rs);
				context.write(new Text("2"), rs);
			}
			else
			{
				int i = 0;
				while (i < rs.pages())
				{
					ResultSetWritable temp = rs.getPage(i);
					context.write(new Text("1"), temp);
					context.write(new Text("2"), temp);
					i++;
				}
			}
		}
		else if (instructions.startsWith("Q9A"))
		{
			if (rs.pages() == 1)
			{
				context.write(new Text(instructions.substring(3)), rs);
			}
			else
			{
				int i = 0;
				while (i < rs.pages())
				{
					context.write(new Text(instructions.substring(3)), rs.getPage(i));
					i++;
				}
			}
		}
		else if (instructions.equals("Q9B") || instructions.equals("Q9C"))
		{
				if (rs.pages() == 1)
				{
					context.write(new Text("1"), rs);
				}
				else
				{
					int j = 0;
					while (j < rs.pages())
					{
						context.write(new Text("1"), rs.getPage(j));
						j++;
					}
				}
		}
		else if (instructions.equals("Q19C"))
		{
			if (rs.pages() == 1)
			{
				context.write(new Text("1"), rs);
				context.write(new Text("2"), rs);
				context.write(new Text("3"), rs);
			}
			else
			{
				int i = 0;
				while (i < rs.pages())
				{
					ResultSetWritable temp = rs.getPage(i);
					context.write(new Text("1"), temp);
					context.write(new Text("2"), temp);
					context.write(new Text("3"), temp);
					i++;
				}
			}
		}
		else if (instructions.startsWith("Q19"))
		{
			if (rs.pages() == 1)
			{
				context.write(new Text(instructions.substring(4)), rs);
			}
			else
			{
				int i = 0;
				while (i < rs.pages())
				{
					context.write(new Text(instructions.substring(4)), rs.getPage(i));
					i++;
				}
			}
		}
	}
}
