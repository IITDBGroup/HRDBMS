import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.Vector;
import java.util.Map.Entry;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


public class DriverReduce extends Reducer<Text, ResultSetWritable, NullWritable, ResultSetWritable>
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
	
	protected void reduce(Text key, Iterable<ResultSetWritable> values, Context context) throws IOException, InterruptedException
	{	
		MyIterable t = new MyIterable(values);
		values = t;
		
		ResultSetWritable value = t.browse();
		String instructions = value.getInstructions();
		
		if (instructions.equals("Q1"))
		{
			TreeMap<String, double[]> map = new TreeMap<String, double[]>();
			
			for (ResultSetWritable rs : values)
			{
				try
				{
					while (rs.next())
					{
						String mapKey = rs.getString(1) + rs.getString(2);
						if (map.containsKey(mapKey))
						{
							//exists
							double[] sums = map.get(mapKey);
							sums[0] += rs.getDouble(3);
							sums[1] += rs.getDouble(4);
							sums[2] += rs.getDouble(5);
							sums[3] += rs.getDouble(6);
							sums[4] += (rs.getDouble(9) * rs.getInt(10));
							sums[5] += rs.getInt(10);
						}
						else
						{
							//not exists
							double[] sums = new double[6];
							//float sum_qty = 0;
							sums[0] = rs.getDouble(3);
							//float sum_extended_price = 0;
							sums[1] = rs.getDouble(4);
							//float sum_disc_price = 0;
							sums[2] = rs.getDouble(5);
							//float sum_charge = 0;
							sums[3] = rs.getDouble(6);
							//float sum_disc = 0;
							sums[4] = rs.getDouble(9) * rs.getInt(10);
							//float count = 0;
							sums[5] = rs.getInt(10);
							map.put(mapKey, sums);
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
			
			ResultSetWritable retval = new ResultSetWritable();
			try
			{
				retval.setResultSetMetaData(value.getMetaData());
			}
			catch(Exception e)
			{
				throw new IOException(e.getMessage());
			}
			
			for (Map.Entry<String, double[]> entry : map.entrySet())
			{
				String mapKey = entry.getKey();
				double[] sums = entry.getValue();
				Vector<Object> row = new Vector<Object>();
				row.add(mapKey.substring(0, 1));
				row.add(mapKey.substring(1,  2));
				row.add(sums[0]);
				row.add(sums[1]);
				row.add(sums[2]);
				row.add(sums[3]);
				row.add(sums[0] / sums[5]);
				row.add(sums[1] / sums[5]);
				row.add(sums[4] / sums[5]);
				row.add(Math.floor(sums[5]));
				retval.addRow(row);
			}
			
			try
			{
				assert (retval.getMetaData().getColumnCount() > 0);
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
			context.write(NullWritable.get(), retval);
		}
		else if (instructions.equals("Q3"))
		{
			int[] sortCols = new int[2];
			sortCols[0] = 2;
			sortCols[1] = 3;
			boolean[] direction = new boolean[2];
			direction[0] = false;
			direction[1] = true;
			
			Vector<ResultSetWritable> rss = new Vector<ResultSetWritable>();
			
			for (ResultSetWritable rs : values)
			{
				rss.add(new ResultSetWritable(rs));
			}
			
			ResultSetWritable retval = sort(rss, sortCols, direction, 10);
			try
			{
				assert (retval.getMetaData().getColumnCount() > 0);
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
			context.write(NullWritable.get(),  retval);
		}
		else if (instructions.equals("Q7"))
		{
			TreeMap<String, double[]> map = new TreeMap<String, double[]>();
			
			for (ResultSetWritable rs : values)
			{
				try
				{
					while (rs.next())
					{
						String mapKey = rs.getString(1) + "^" + rs.getString(2) + "^" + rs.getInt(3);
						if (map.containsKey(mapKey))
						{
							//exists
							double[] sums = map.get(mapKey);
							sums[0] += rs.getDouble(4);
						}
						else
						{
							//not exists
							double[] sums = new double[1];
							sums[0] = rs.getDouble(4);
							map.put(mapKey, sums);
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
			
			ResultSetWritable retval = new ResultSetWritable();
			try
			{
				retval.setResultSetMetaData(value.getMetaData());
			}
			catch(Exception e)
			{
				throw new IOException(e.getMessage());
			}
			
			for (Map.Entry<String, double[]> entry : map.entrySet())
			{
				String mapKey = entry.getKey();
				StringTokenizer tokens = new StringTokenizer(mapKey, "^", false);
				double[] sums = entry.getValue();
				Vector<Object> row = new Vector<Object>();
				row.add(tokens.nextToken());
				row.add(tokens.nextToken());
				row.add(Integer.parseInt(tokens.nextToken()));
				row.add(sums[0]);
				retval.addRow(row);
			}
			
			context.write(NullWritable.get(), retval);
		}
		else if (instructions.equals("Q12"))
		{
			TreeMap<String, int[]> map = new TreeMap<String, int[]>();
			
			for (ResultSetWritable rs : values)
			{
				try
				{
					while (rs.next())
					{
						String mapKey = rs.getString(1);
						if (map.containsKey(mapKey))
						{
							//exists
							int[] sums = map.get(mapKey);
							sums[0] += rs.getInt(2);
							sums[1] += rs.getInt(3);
						}
						else
						{
							//not exists
							int[] sums = new int[2];
							sums[0] = rs.getInt(2);
							sums[1] = rs.getInt(3);
							map.put(mapKey, sums);
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
			
			ResultSetWritable retval = new ResultSetWritable();
			try
			{
				retval.setResultSetMetaData(value.getMetaData());
			}
			catch(Exception e)
			{
				throw new IOException(e.getMessage());
			}
			
			for (Map.Entry<String, int[]> entry : map.entrySet())
			{
				String mapKey = entry.getKey();
				int[] sums = entry.getValue();
				Vector<Object> row = new Vector<Object>();
				row.add(mapKey);
				row.add(sums[0]);
				row.add(sums[1]);
				retval.addRow(row);
			}
			
			try
			{
				assert (retval.getMetaData().getColumnCount() > 0);
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
			context.write(NullWritable.get(), retval);
		}
		else if (instructions.equals("Q18"))
		{
			int[] sortCols = new int[2];
			sortCols[0] = 5;
			sortCols[1] = 4;
			boolean[] direction = new boolean[2];
			direction[0] = false;
			direction[1] = true;
			
			Vector<ResultSetWritable> temp = new Vector<ResultSetWritable>();
			for (ResultSetWritable row : values)
			{
				temp.add(new ResultSetWritable(row));
			}
			
			ResultSetWritable retval = sort(temp, sortCols, direction, 100);
			try
			{
				assert (retval.getMetaData().getColumnCount() > 0);
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
			context.write(NullWritable.get(),  retval);
		}
		else if (instructions.equals("Q21"))
		{
			HashMap<String, int[]> map = new HashMap<String, int[]>();
			
			for (ResultSetWritable rs : values)
			{
				try
				{
					while (rs.next())
					{
						String mapKey = rs.getString(1);
						if (map.containsKey(mapKey))
						{
							//exists
							int[] sums = map.get(mapKey);
							sums[0] += rs.getInt(2);
						}
						else
						{
							//not exists
							int[] sums = new int[1];
							sums[0] = rs.getInt(2);
							map.put(mapKey, sums);
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
			
			List<Map.Entry<String, int[]>> list = new LinkedList<Map.Entry<String, int[]>>(map.entrySet());

	        Collections.sort(list, new Comparator<Map.Entry<String, int[]>>() {

	            public int compare(Map.Entry<String, int[]> m1, Map.Entry<String, int[]> m2) {
	            	
	            	int comp = new Integer(m1.getValue()[0]).compareTo(m2.getValue()[0]);
	                
	            	if (comp == -1)
	            	{
	            		return 1;
	            	}
	            	
	            	if (comp == 1)
	            	{
	            		return -1;
	            	}
	            	
	            	return m1.getKey().compareTo(m2.getKey());
	            }
	        });
			
			ResultSetWritable retval = new ResultSetWritable();
			try
			{
				retval.setResultSetMetaData(value.getMetaData());
			}
			catch(Exception e)
			{
				throw new IOException(e.getMessage());
			}
			
			int i = 0;
			for (Map.Entry<String, int[]> entry : list)
			{
				String mapKey = entry.getKey();
				int[] sums = entry.getValue();
				Vector<Object> row = new Vector<Object>();
				row.add(mapKey);
				row.add(sums[0]);
				retval.addRow(row);
				i++;
				if (i == 100)
				{
					break;
				}
			}
			
			context.write(NullWritable.get(), retval);
		}
		else if (instructions.startsWith("Q8"))
		{
			HashSet<Integer> set = new HashSet<Integer>();
			
			ResultSetWritable retval = new ResultSetWritable();
			
			for (ResultSetWritable rs : values)
			{
				if (rs.getInstructions().startsWith("Q8C"))
				{
					try
					{
						retval.setResultSetMetaData(rs.getMetaData());
					}
					catch(Exception e)
					{
						throw new IOException(e.getMessage());
					}
				}
				else
				{
					try
					{
						while (rs.next())
						{
							set.add(rs.getInt(1));
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
			
			int nodes = Integer.parseInt(context.getConfiguration().get("HRDBMS.nodes"));
			Connection[] con = new Connection[nodes];
			
			try
			{
				Class.forName("com.ibm.db2.jcc.DB2Driver");
				
				con[0] = DriverManager.getConnection(
				"jdbc:db2://db21:50000/TPCH",
				"db2inst1",
				"db2inst1");
				
				if (nodes == 2 || nodes == 4)
				{
					con[1] = DriverManager.getConnection(
							"jdbc:db2://db22:50000/TPCH",
							"db2inst1",
							"db2inst1");
				}
				
				if (nodes == 4)
				{
					con[2] = DriverManager.getConnection(
							"jdbc:db2://db23:50000/TPCH",
							"db2inst1",
							"db2inst1");
					con[3] = DriverManager.getConnection(
							"jdbc:db2://db24:50000/TPCH",
							"db2inst1",
							"db2inst1");
				}
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(0);
			}
			
			int i = 0;
			Statement[] stmts = new Statement[nodes];
			while (i < nodes)
			{
				try
				{
					stmts[i] = con[i].createStatement();
					stmts[i].execute("CREATE TABLE TPCD.TEMPTABLE (P_PARTKEY INT NOT NULL PRIMARY KEY)");
					stmts[i].close();
				}
				catch(Exception e)
				{
					e.printStackTrace();
				}
				i++;
			}
			
			i = 0;
			BatchInsertThread[] batches = new BatchInsertThread[nodes];
			while (i < nodes)
			{
				batches[i] = new BatchInsertThread(con[i], "INSERT INTO TPCD.TEMPTABLE VALUES (?)", set, 1);
				batches[i].start();
				i++;
			}
			
			i = 0;
			while (i < nodes)
			{
				batches[i].join();
				i++;
			}
			
			String sql = "select year(o_orderdate) as o_year, l_extendedprice * (1 - l_discount) as volume, n2.n_name as nation, L_PARTKEY from (SELECT * FROM tpcd.supplier UNION SELECT * FROM TPCD.SUPPLIER_AUX), tpcd.lineitem, tpcd.orders, (SELECT * FROM tpcd.customer UNION SELECT * FROM TPCD.CUSTOMER_AUX), tpcd.nation n1, tpcd.nation n2,tpcd.region, tpcd.temptable where s_suppkey = l_suppkey and l_orderkey = o_orderkey and o_custkey = c_custkey and c_nationkey = n1.n_nationkey and n1.n_regionkey = r_regionkey and r_name = 'AMERICA' and s_nationkey = n2.n_nationkey and o_orderdate between date('1995-01-01') and date ('1996-12-31') and p_partkey = l_partkey";
			
			SQLThread[] threads = new SQLThread[nodes];
			i = 0;
			while (i < nodes)
			{
				threads[i] = new SQLThread(con[i], sql);
				threads[i].start();
				i++;
			}
			
			i = 0;
			TreeMap<Integer, double[]> map = new TreeMap<Integer, double[]>();
			while (i < nodes)
			{
				threads[i].join();
				try
				{
					while (threads[i].rs.next())
					{
						Integer mapKey = threads[i].rs.getInt(1);
						if (map.containsKey(mapKey))
						{
							//exists
							double[] sums = map.get(mapKey);
							double temp = threads[i].rs.getDouble(2);
							sums[1] += temp;
							
							if (threads[i].rs.getString(3).trim().equals("BRAZIL"))
							{
								sums[0] += temp;
							}
						}
						else
						{
							//not exists
							double[] sums = new double[2];
							double temp = threads[i].rs.getDouble(2);
							sums[1] = temp;
							
							if (threads[i].rs.getString(3).trim().equals("BRAZIL"))
							{
								sums[0] = temp;
							}
							else
							{
								sums[0] = 0;
							}
							map.put(mapKey, sums);
						}
						
					}
					i++;
				}
				catch(Exception e)
				{
					e.printStackTrace();
				}
			}
			i = 0;
			while (i < nodes)
			{
				try
				{
					stmts[i] = con[i].createStatement();
					stmts[i].execute("DROP TABLE TPCD.TEMPTABLE");
					stmts[i].close();
				}
				catch(Exception e)
				{
					e.printStackTrace();
				}
				i++;
			}
		
			for (Map.Entry<Integer, double[]> entry : map.entrySet())
			{
				Integer mapKey = entry.getKey();
				double[] sums = entry.getValue();
				Vector<Object> row = new Vector<Object>();
				row.add(mapKey);
				row.add(sums[0] / sums[1]);
				retval.addRow(row);
			}
			
			context.write(NullWritable.get(), retval);
		}
		else if (instructions.startsWith("Q9"))
		{
			ResultSetWritable retval = new ResultSetWritable();
			int nodes = Integer.parseInt(context.getConfiguration().get("HRDBMS.nodes"));
			Connection[] con = new Connection[nodes];
		
			try
			{
				Class.forName("com.ibm.db2.jcc.DB2Driver");
			
				con[0] = DriverManager.getConnection(
						"jdbc:db2://db21:50000/TPCH",
						"db2inst1",
						"db2inst1");
			
				if (nodes == 2 || nodes == 4)
				{
					con[1] = DriverManager.getConnection(
						"jdbc:db2://db22:50000/TPCH",
						"db2inst1",
						"db2inst1");
				}
			
				if (nodes == 4)
				{
					con[2] = DriverManager.getConnection(
						"jdbc:db2://db23:50000/TPCH",
						"db2inst1",
						"db2inst1");
					con[3] = DriverManager.getConnection(
							"jdbc:db2://db24:50000/TPCH",
						"db2inst1",
						"db2inst1");
				}
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(0);
			}
			
			int i;
			BatchInsertThread[] batches = new BatchInsertThread[nodes];
			{
				HashMap<Vector<Integer>, Object[]> set = new HashMap<Vector<Integer>, Object[]>();
			
				for (ResultSetWritable rs : values)
				{
					if (rs.getInstructions().startsWith("Q9C"))
					{
						try
						{
							retval.setResultSetMetaData(rs.getMetaData());
						}
						catch(Exception e)
						{
							throw new IOException(e.getMessage());
						}
					}
					else
					{
						try
						{
							while (rs.next())
							{
								Vector<Integer> keys = new Vector<Integer>();
								keys.add(rs.getInt(3));
								keys.add(rs.getInt(4));
								Object[] cols = new Object[2];
								cols[0] = rs.getString(1);
								cols[1] = rs.getDouble(2);
								set.put(keys, cols);
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
			
				System.out.println("Done building set.  Size = " + set.size());
				i = 0;
				try
				{
					Statement[] stmts = new Statement[nodes];
					while (i < nodes)
					{
						stmts[i] = con[i].createStatement();
						stmts[i].execute("CREATE TABLE TPCD.TEMPTABLE (NATION CHAR(25) NOT NULL, SUPPLYCOST FLOAT NOT NULL, PARTKEY INT NOT NULL, SUPPKEY INT NOT NULL, PRIMARY KEY(PARTKEY, SUPPKEY))");
						stmts[i].close();
						i++;
					}
				}
				catch(Exception e)
				{
					e.printStackTrace();
				}
				System.out.println("Done creating temp table.");
			
				i = 0;
				
				while (i < nodes)
				{
					batches[i] = new BatchInsertThread(con[i], "INSERT INTO TPCD.TEMPTABLE VALUES (?,?,?,?)", set, 4);
					batches[i].start();
					i++;
				}
			}
			
			i = 0;
			while (i < nodes)
			{
				batches[i].join();
				i++;
			}
			System.out.println("Done populating temp table");
			
			String sql = "select nation, o_year, sum(amount) as sum_profit from ( select nation, extract(year from o_orderdate) as o_year, l_extendedprice * (1 - l_discount) - supplycost * l_quantity as amount from tpcd.temptable, tpcd.lineitem, tpcd.orders where suppkey = l_suppkey and partkey = l_partkey and o_orderkey = l_orderkey) as profit group by nation, o_year";
			
			SQLThread[] threads = new SQLThread[nodes];
			i = 0;
			while (i < nodes)
			{
				threads[i] = new SQLThread(con[i], sql);
				threads[i].start();
				i++;
			}
			
			i = 0;
			while (i < nodes)
			{
				threads[i].join();
				i++;
			}
			System.out.println("Done with final queries");
			
			i = 0;
			Statement[] stmts = new Statement[nodes];
			try
			{
				while (i < nodes)
				{
					stmts[i] = con[i].createStatement();
					stmts[i].execute("DROP TABLE TPCD.TEMPTABLE");
					stmts[i].close();
					i++;
				}
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
			System.out.println("Dropped temp table");
			
			TreeMap<String, double[]> map = new TreeMap<String, double[]>();
			for (SQLThread thread : threads)
			{
				ResultSet rs = thread.rs;
				try
				{
					System.out.println("Adding a rs to map");
					while (rs.next())
					{
							String mapKey = rs.getString(1) + "^" + (2000 -rs.getInt(2));
							if (map.containsKey(mapKey))
							{
								//exists
								double[] sums = map.get(mapKey);
								sums[0] += (rs.getDouble(3));
							}
							else
							{
								//not exists
								double[] sums = new double[1];
								sums[0] = (rs.getDouble(3));
								map.put(mapKey, sums);
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
			System.out.println("Done building map. Size = " + map.size());
			for (Map.Entry<String, double[]> entry : map.entrySet())
			{
				String mapKey = entry.getKey();
				double[] sums = entry.getValue();
				Vector<Object> row = new Vector<Object>();
				StringTokenizer tokens = new StringTokenizer(mapKey, "^", false);
				row.add(tokens.nextToken());
				row.add(2000 - Integer.parseInt(tokens.nextToken()));
				row.add(sums[0]);
				retval.addRow(row);
			}
			System.out.println("Done building final result set");
			context.write(NullWritable.get(), retval);
		}
		else if (instructions.startsWith("Q13"))
		{
			HashMap<Integer, Integer> set = new HashMap<Integer, Integer>();
			
			ResultSetWritable retval = new ResultSetWritable();
			
			for (ResultSetWritable rs : values)
			{	
					try
					{
						while (rs.next())
						{
							set.put(rs.getInt(1), 0);
						}
					}
					catch(Exception e)
					{
						System.out.println(e);
						e.printStackTrace();
						System.exit(0);
					}
			}
			
			String sql = "select O_CUSTKEY, 1 FROM TPCD.ORDERS WHERE o_comment not like '%special%requests%'";
			
			int nodes = Integer.parseInt(context.getConfiguration().get("HRDBMS.nodes"));
			Connection[] con = new Connection[nodes];
			
			try
			{
				Class.forName("com.ibm.db2.jcc.DB2Driver");
				
				con[0] = DriverManager.getConnection(
				"jdbc:db2://db21:50000/TPCH",
				"db2inst1",
				"db2inst1");
				
				if (nodes == 2 || nodes == 4)
				{
					con[1] = DriverManager.getConnection(
							"jdbc:db2://db22:50000/TPCH",
							"db2inst1",
							"db2inst1");
				}
				
				if (nodes == 4)
				{
					con[2] = DriverManager.getConnection(
							"jdbc:db2://db23:50000/TPCH",
							"db2inst1",
							"db2inst1");
					con[3] = DriverManager.getConnection(
							"jdbc:db2://db24:50000/TPCH",
							"db2inst1",
							"db2inst1");
				}
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(0);
			}
			
			SQLThread[] threads = new SQLThread[nodes];
			int i = 0;
			while (i < nodes)
			{
				threads[i] = new SQLThread(con[i], sql);
				threads[i].start();
				i++;
			}
			
			i = 0;
			TreeMap<Integer, double[]> map = new TreeMap<Integer, double[]>();
			while (i < nodes)
			{
				threads[i].join();
				try
				{
					retval.setResultSetMetaData(threads[i].rs.getMetaData());
					while (threads[i].rs.next())
					{
						Integer mapKey = threads[i].rs.getInt(1);
						if (set.containsKey(mapKey))
						{
							set.put(mapKey, set.get(mapKey) + 1);
						}
					}
				}
				catch(Exception e)
				{
					System.out.println(e);
					e.printStackTrace();
					System.exit(0);
				}
				i++;
			}
			
			HashMap<Integer, Integer> count = new HashMap<Integer, Integer>();
			
			for (Integer numOrders : set.values())
			{
				if (count.containsKey(numOrders))
				{
					count.put(numOrders, count.get(numOrders) + 1);
				}
				else
				{
					count.put(numOrders, 1);
				}
			}
			
			Vector<Map.Entry<Integer, Integer>> sort = new Vector<Map.Entry<Integer, Integer>>();
			sort.addAll(count.entrySet());
			//<numOrders, occurrences>
			Collections.sort(sort, new Comparator<Map.Entry<Integer, Integer>>() {

	            public int compare(Map.Entry<Integer, Integer> m1, Map.Entry<Integer, Integer> m2) {
	            	
	            	int comp = m2.getValue().compareTo(m1.getValue());
	            	
	            	if (comp != 0)
	            	{
	            		return comp;
	            	}
	            	
	            	return m2.getKey().compareTo(m1.getKey());
	            }
	        });
			
			
			for (Entry<Integer, Integer> cols : sort)
			{
				Vector<Object> row = new Vector<Object>();
				row.add(cols.getKey());
				row.add(cols.getValue());
				retval.addRow(row);
			}
			
			try
			{
				assert (retval.getMetaData().getColumnCount() > 0);
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
			context.write(NullWritable.get(), retval);
		}
		else if (instructions.startsWith("Q17"))
		{
			HashSet<Integer> set = new HashSet<Integer>();
			
			ResultSetWritable retval = new ResultSetWritable();
			
			for (ResultSetWritable rs : values)
			{
				if (rs.getInstructions().startsWith("Q17C"))
				{
					try
					{
						retval.setResultSetMetaData(rs.getMetaData());
					}
					catch(Exception e)
					{
						throw new IOException(e.getMessage());
					}
				}
				else
				{
					try
					{
						while (rs.next())
						{
							set.add(rs.getInt(1));
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
			
			int nodes = Integer.parseInt(context.getConfiguration().get("HRDBMS.nodes"));
			Connection[] con = new Connection[nodes];
			
			try
			{
				Class.forName("com.ibm.db2.jcc.DB2Driver");
				
				con[0] = DriverManager.getConnection(
				"jdbc:db2://db21:50000/TPCH",
				"db2inst1",
				"db2inst1");
				
				if (nodes == 2 || nodes == 4)
				{
					con[1] = DriverManager.getConnection(
							"jdbc:db2://db22:50000/TPCH",
							"db2inst1",
							"db2inst1");
				}
				
				if (nodes == 4)
				{
					con[2] = DriverManager.getConnection(
							"jdbc:db2://db23:50000/TPCH",
							"db2inst1",
							"db2inst1");
					con[3] = DriverManager.getConnection(
							"jdbc:db2://db24:50000/TPCH",
							"db2inst1",
							"db2inst1");
				}
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(0);
			}
			
			int i = 0;
			Statement[] stmts = new Statement[nodes];
			while (i < nodes)
			{
				try
				{
					stmts[i] = con[i].createStatement();
					stmts[i].execute("CREATE TABLE TPCD.TEMPTABLE (P_PARTKEY INT NOT NULL PRIMARY KEY)");
					stmts[i].close();
				}
				catch(Exception e)
				{
					e.printStackTrace();
				}
				i++;
			}
			System.out.println("Done building temp table");
			
			i = 0;
			BatchInsertThread[] batches = new BatchInsertThread[nodes];
			while (i < nodes)
			{
				batches[i] = new BatchInsertThread(con[i], "INSERT INTO TPCD.TEMPTABLE VALUES (?)", set, 1);
				batches[i].start();
				i++;
			}
			
			i = 0;
			while (i < nodes)
			{
				batches[i].join();
				i++;
			}
			System.out.println("Done populating temp table");
			
			double sum = 0;
			String sql = "select sum(0.2 * l_quantity) as total, count(*) as quantity, l_partkey from tpcd.lineitem, tpcd.temptable where l_partkey = p_partkey group by l_partkey"; 
			
			i = 0;
			SQLThread[] threads = new SQLThread[nodes];
			while (i < nodes)
			{
				threads[i] = new SQLThread(con[i], sql);
				threads[i].start();
				i++;
			}
			
			HashMap<Integer, double[]> map = new HashMap<Integer, double[]>();
			i = 0;
			while (i < nodes)
			{
				threads[i].join();
				try
				{
					while (threads[i].rs.next())
					{
						if (map.containsKey(threads[i].rs.getInt(3)))
						{
							double[] cols = map.get(threads[i].rs.getInt(3));
							cols[0] += threads[i].rs.getDouble(1);
							cols[1] += threads[i].rs.getInt(2);
							map.put(threads[i].rs.getInt(3), cols);
						}
						else
						{
							double[] cols = new double[2];
							cols[0] = threads[i].rs.getDouble(1);
							cols[1] = threads[i].rs.getInt(2);
							map.put(threads[i].rs.getInt(3), cols);
						}
					}
				}
				catch(Exception e)
				{
					e.printStackTrace();
				}
				i++;
			}
			System.out.println("First queries done");
			i = 0;
			while (i < nodes)
			{
				try
				{
					stmts[i] = con[i].createStatement();
					stmts[i].execute("DROP TABLE TPCD.TEMPTABLE");
					con[i].commit();
					stmts[i].close();
				}
				catch(Exception e)
				{
					e.printStackTrace();
				}
				i++;
			}
			System.out.println("Dropped temp table");
			
			i = 0;
			while (i < nodes)
			{
				try
				{
					stmts[i] = con[i].createStatement();
					stmts[i].execute("CREATE TABLE TPCD.TEMPTABLE (P_PARTKEY INT NOT NULL PRIMARY KEY, THRESHOLD FLOAT NOT NULL)");
					stmts[i].close();
				}
				catch(Exception e)
				{
					e.printStackTrace();
				}
				i++;
			}
			System.out.println("Created temp table");
			i = 0;
			while (i < nodes)
			{
				batches[i] = new BatchInsertThread(con[i], "INSERT INTO TPCD.TEMPTABLE VALUES (?,?)", map, 2);
				batches[i].start();
				i++;
			}
			
			i = 0;
			while (i < nodes)
			{
				batches[i].join();
				i++;
			}
			System.out.println("Populated temp table");
			
			sql = "select sum(l_extendedprice) / 7.0 as avg_yearly from tpcd.lineitem, tpcd.temptable where p_partkey = l_partkey and l_quantity < threshold"; 
			
			i = 0;
			threads = new SQLThread[nodes];
			while (i < nodes)
			{
				threads[i] = new SQLThread(con[i], sql);
				threads[i].start();
				i++;
			}
			
			i = 0;
			while (i < nodes)
			{
				threads[i].join();
				try
				{
					while (threads[i].rs.next())
					{
						sum += threads[i].rs.getDouble(1);
					}
					i++;
				}
				catch(Exception e)
				{
					e.printStackTrace();
				}
			}
			System.out.println("Final queries done");
			
			i = 0;
			while (i < nodes)
			{
				try
				{
					stmts[i] = con[i].createStatement();
					stmts[i].execute("DROP TABLE TPCD.TEMPTABLE");
					con[i].commit();
					stmts[i].close();
				}
				catch(Exception e)
				{
					e.printStackTrace();
				}
				i++;
			}
			
			Vector<Object> row = new Vector<Object>();
			row.add(sum);
			retval.addRow(row);
			
			context.write(NullWritable.get(), retval);
		}
		else if (instructions.startsWith("Q19"))
		{
			HashSet<Integer> set = new HashSet<Integer>();
			MyVector<ResultSetWritable> list = new MyVector<ResultSetWritable>(4);
			
			ResultSetWritable retval = new ResultSetWritable();
			
			for (ResultSetWritable rs : values)
			{
				if (rs.getInstructions().startsWith("Q19A"))
				{
					list.add(new ResultSetWritable(rs));
				}
				else if (rs.getInstructions().startsWith("Q19C"))
				{
					try
					{
						retval.setResultSetMetaData(rs.getMetaData());
					}
					catch(Exception e)
					{
						throw new IOException(e.getMessage());
					}
				}
				else
				{
					try
					{
						while (rs.next())
						{
							set.add(rs.getInt(1));
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
			
			double sum = 0;
			
			for (ResultSetWritable rs : list)
			{
				try
				{
					while (rs.next())
					{
						if (set.contains(rs.getInt(2)))
						{
							sum += rs.getDouble(1);
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
			
			Vector<Object> row = new Vector<Object>();
			row.add(sum);
			retval.addRow(row);
			
			try
			{
				assert (retval.getMetaData().getColumnCount() > 0);
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
			context.write(NullWritable.get(), retval);
		}
		else if (instructions.startsWith("Q20"))
		{
			HashMap<Vector<Integer>, Object[]> set = new HashMap<Vector<Integer>, Object[]>();
			
			ResultSetWritable retval = new ResultSetWritable();
			
			for (ResultSetWritable rs : values)
			{
				if (rs.getInstructions().startsWith("Q20C"))
				{
					try
					{
						retval.setResultSetMetaData(rs.getMetaData());
					}
					catch(Exception e)
					{
						throw new IOException(e.getMessage());
					}
				}
				else
				{
					try
					{
						while (rs.next())
						{
							Vector<Integer> keys = new Vector<Integer>();
							Object[] cols = new Object[3];
							cols[0] = rs.getInt(3);
							cols[1] = rs.getString(4);
							cols[2] = rs.getString(5);
							keys.add(rs.getInt(1));
							keys.add(rs.getInt(2));
							set.put(keys, cols);
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
			
			String sql = "select 0.5 * sum(l_quantity), l_partkey, l_suppkey from tpcd.lineitem, tpcd.temptable where l_shipdate >= date ('1994-01-01') and l_shipdate < date ('1994-01-01') + 1 year and l_partkey = ps_partkey and l_suppkey = ps_suppkey group by l_partkey, l_suppkey";
			TreeMap<String, String> map = new TreeMap<String, String>();
			int nodes = Integer.parseInt(context.getConfiguration().get("HRDBMS.nodes"));
			Connection[] con = new Connection[nodes];
			
			try
			{
				Class.forName("com.ibm.db2.jcc.DB2Driver");
				
				con[0] = DriverManager.getConnection(
				"jdbc:db2://db21:50000/TPCH",
				"db2inst1",
				"db2inst1");
				
				if (nodes == 2 || nodes == 4)
				{
					con[1] = DriverManager.getConnection(
							"jdbc:db2://db22:50000/TPCH",
							"db2inst1",
							"db2inst1");
				}
				
				if (nodes == 4)
				{
					con[2] = DriverManager.getConnection(
							"jdbc:db2://db23:50000/TPCH",
							"db2inst1",
							"db2inst1");
					con[3] = DriverManager.getConnection(
							"jdbc:db2://db24:50000/TPCH",
							"db2inst1",
							"db2inst1");
				}
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(0);
			}
			
			int i = 0;
			Statement[] stmts = new Statement[nodes];
			while (i < nodes)
			{
				try
				{
					stmts[i] = con[i].createStatement();
					stmts[i].execute("CREATE TABLE TPCD.TEMPTABLE ( PS_PARTKEY INT NOT NULL, PS_SUPPKEY INT NOT NULL, PS_AVAILQTY INT NOT NULL, S_NAME CHAR(25) NOT NULL, S_ADDRESS VARCHAR(40) NOT NULL, PRIMARY KEY (PS_PARTKEY,PS_SUPPKEY))");
					stmts[i].close();
				}
				catch(Exception e)
				{
					e.printStackTrace();
				}
				i++;
			}
			
			i = 0;
			BatchInsertThread[] batches = new BatchInsertThread[nodes];
			while (i < nodes)
			{
				batches[i] = new BatchInsertThread(con[i], "INSERT INTO TPCD.TEMPTABLE VALUES (?,?,?,?,?)", set, 5);
				batches[i].start();
				i++;
			}
			
			i = 0;
			while (i < nodes)
			{
				batches[i].join();
				i++;
			}
			
			SQLThread[] threads = new SQLThread[nodes];
			i = 0;
			while (i < nodes)
			{
				threads[i] = new SQLThread(con[i], sql);
				threads[i].start();
				i++;
			}
			
			i = 0;
			while (i < nodes)
			{
				threads[i].join();
				try
				{
					while (threads[i].rs.next())
					{
						Vector<Integer> lookup = new Vector<Integer>();
						lookup.add(threads[i].rs.getInt(2));
						lookup.add(threads[i].rs.getInt(3));

							Object[] temp = set.get(lookup);
							if (((Integer)temp[0]) > threads[i].rs.getDouble(1))
							{
								if (!map.containsKey(temp[1]))
								{
									map.put((String)temp[1], (String)temp[2]);
								}
							}
					}
				}
				catch(Exception e)
				{
					System.out.println(e);
					e.printStackTrace();
					System.exit(0);
				}
				i++;
			}
			
			i = 0;
			while (i < nodes)
			{
				try
				{
					stmts[i] = con[i].createStatement();
					stmts[i].execute("DROP TABLE TPCD.TEMPTABLE");
					stmts[i].close();
				}
				catch(Exception e)
				{
					e.printStackTrace();
				}
				i++;
			}
			
			for (Map.Entry<String, String> entry : map.entrySet())
			{
				Vector<Object> row = new Vector<Object>();
				row.add(entry.getKey());
				row.add(entry.getValue());
				retval.addRow(row);
			}
			
			try
			{
				assert (retval.getMetaData().getColumnCount() > 0);
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
			context.write(NullWritable.get(), retval);
		}
	}
	
	private static ResultSetWritable sort(Vector<ResultSetWritable> rss, int[] sortCols, boolean[] dir, int first)
	{
		ResultSetWritable retval = null;
		
		try
		{
			retval = new ResultSetWritable();
			retval.setResultSetMetaData(rss.get(0).getMetaData());
			int i = 0;
			TreeMap<Row, Integer> tree = new TreeMap<Row, Integer>();
		
			while (i < rss.size())
			{
				if (rss.get(i).next())
				{
					tree.put(new Row(rss.get(i), sortCols, dir), i);
				}
				i++;
			}
		
			i = 0;
			while (i < first && !tree.isEmpty())
			{
				Entry<Row, Integer> entry = tree.firstEntry();
				tree.remove(entry.getKey());
				retval.addRow(rss.get(entry.getValue()));
				
				if (rss.get(entry.getValue()).next())
				{
					tree.put(new Row(rss.get(entry.getValue()), sortCols, dir), entry.getValue());
				}
			}
		
			for (ResultSetWritable rs : rss)
			{
				rs.close();
			}
		}
		catch(Exception e)
		{
			System.out.println(e);
			e.printStackTrace();
		}
		
		return retval;
	}
}
