import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Driver 
{
	private static int nodes;
	public static void main(String[] args)
	{
		System.out.println(Runtime.getRuntime().maxMemory());
		nodes = Integer.parseInt(args[0]);
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
		
		if (nodes == 1)
		{
			query1_1(con[0]);
			query2_1(con[0]);
			query3_1(con[0]);
			query4_1(con[0]);
			query5_1(con[0]);
			query6_1(con[0]);
			query7_1(con[0]);
			query8_1(con[0]);
			query9_1(con[0]);
			query10_1(con[0]);
			query11_1(con[0]);
			query12_1(con[0]);
			query13_1(con[0]);
			query14_1(con[0]);
			query15_1(con[0]);
			query16_1(con[0]);
			query17_1(con[0]);
			query18_1(con[0]);
			query19_1(con[0]);
			query20_1(con[0]);
			query21_1(con[0]);
			query22_1(con[0]);
		}
		else 
		{
			query1_n(con, nodes); 
			query2_n(con, nodes);
			query3_n(con, nodes);
			query4_n(con, nodes);
			query5_n(con, nodes);
			query6_n(con, nodes);
			query7_n(con, nodes); 
			query8_n(con, nodes); 
			query9_n(con, nodes); 
			query10_n(con, nodes);
			query11_n(con, nodes);
			query12_n(con, nodes); 
			query13_n(con, nodes); 
			query14_n(con, nodes); 
			query15_n(con, nodes); 
			query16_n(con, nodes);
			query17_n(con, nodes); 
			query18_n(con, nodes); 
			query19_n(con, nodes);  
			query20_n(con, nodes); 
			query21_n(con, nodes); 
			query22_n(con, nodes);
		}
		
		for (Connection c : con)
		{
			try
			{
				c.close();
			}
			catch(Exception e) {}
		}
	}
	
	private static String runCommand(String cmd)
	{
		String line;
	    String output = "";
	    try {
	        Process p = Runtime.getRuntime().exec(cmd);
	        BufferedReader input = new BufferedReader
	            (new InputStreamReader(p.getInputStream()));
	        while ((line = input.readLine()) != null) {
	            output += (line + '\n');
	        }
	        input.close();
	        }
	    catch (Exception ex) {
	        ex.printStackTrace();
	        System.exit(0);
	    }
	    
	    return output;
	}
	
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
	
	private static void query1_1(Connection con)
	{
		System.out.println("Query #1 start");
		System.out.println(runCommand("date"));
		try
		{
			Statement stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery("select l_returnflag,l_linestatus,sum(l_quantity) as sum_qty,sum(l_extendedprice) as sum_base_price,sum(l_extendedprice*(1-l_discount)) as sum_disc_price,sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge,avg(l_quantity) as avg_qty,avg(l_extendedprice) as avg_price,avg(l_discount) as avg_disc,count(*) as count_order from tpcd.lineitem where l_shipdate <= date('1998-12-01') - 90 day group by l_returnflag, l_linestatus order by l_returnflag, l_linestatus");
			printResultSet(rs);
			rs.close();
		}
		catch(Exception e)
		{
			System.out.println(e);
			e.printStackTrace();
			System.exit(0);
		}
		
		System.out.println(runCommand("date"));
		System.out.println("Query #1 end");
	}
	
	private static void query2_1(Connection con)
	{
		System.out.println("Query #2 start");
		System.out.println(runCommand("date"));
		try
		{
			Statement stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery("select s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment from tpcd.part, tpcd.supplier, tpcd.partsupp, tpcd.nation, tpcd.region where p_partkey = ps_partkey and s_suppkey = ps_suppkey and p_size = 15 and p_type like '%BRASS' and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'EUROPE' and ps_supplycost = (select min(ps_supplycost) from tpcd.partsupp, tpcd.supplier, tpcd.nation, tpcd.region where p_partkey = ps_partkey and s_suppkey = ps_suppkey and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'EUROPE' ) order by s_acctbal desc, n_name, s_name, p_partkey fetch first 100 rows only");
			printResultSet(rs);
			rs.close();
		}
		catch(Exception e)
		{
			System.out.println(e);
			e.printStackTrace();
			System.exit(0);
		}
		
		System.out.println(runCommand("date"));
		System.out.println("Query #2 end");
	}
	
	private static void query3_1(Connection con)
	{
		System.out.println("Query #3 start");
		System.out.println(runCommand("date"));
		try
		{
			Statement stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery("select l_orderkey, sum(l_extendedprice * (1 - l_discount)) as revenue, o_orderdate, o_shippriority from tpcd.customer, tpcd.orders, tpcd.lineitem where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate < date ('1995-03-15') and l_shipdate > date ('1995-03-15') group by l_orderkey, o_orderdate, o_shippriority order by revenue desc, o_orderdate fetch first 10 rows only");
			printResultSet(rs);
			rs.close();
		}
		catch(Exception e)
		{
			System.out.println(e);
			e.printStackTrace();
			System.exit(0);
		}
		
		System.out.println(runCommand("date"));
		System.out.println("Query #3 end");
	}
	
	private static void query4_1(Connection con)
	{
		System.out.println("Query #4 start");
		System.out.println(runCommand("date"));
		try
		{
			Statement stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery("select o_orderpriority, count(*) as order_count from tpcd.orders where o_orderdate >= date ('1993-07-01') and o_orderdate < date ('1993-07-01') + 3 month and exists ( select * from tpcd.lineitem where l_orderkey = o_orderkey and l_commitdate < l_receiptdate ) group by o_orderpriority order by o_orderpriority");
			printResultSet(rs);
			rs.close();
		}
		catch(Exception e)
		{
			System.out.println(e);
			e.printStackTrace();
			System.exit(0);
		}
		
		System.out.println(runCommand("date"));
		System.out.println("Query #4 end");
	}
	
	private static void query5_1(Connection con)
	{
		System.out.println("Query #5 start");
		System.out.println(runCommand("date"));
		try
		{
			Statement stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery("select n_name, sum(l_extendedprice * (1 - l_discount)) as revenue from tpcd.customer, tpcd.orders, tpcd.lineitem, tpcd.supplier, tpcd.nation, tpcd.region where c_custkey = o_custkey and o_orderkey = l_orderkey and l_suppkey = s_suppkey and c_nationkey = s_nationkey and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'ASIA' and o_orderdate >= date ('1994-01-01') and o_orderdate < date ('1994-01-01') + 1 year group by n_name order by revenue desc");
			printResultSet(rs);
			rs.close();
		}
		catch(Exception e)
		{
			System.out.println(e);
			e.printStackTrace();
			System.exit(0);
		}
		
		System.out.println(runCommand("date"));
		System.out.println("Query #5 end");
	}
	
	private static void query6_1(Connection con)
	{
		System.out.println("Query #6 start");
		System.out.println(runCommand("date"));
		try
		{
			Statement stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery("select sum(l_extendedprice * l_discount) as revenue from tpcd.lineitem where l_shipdate >= date ('1994-01-01') and l_shipdate < date ('1994-01-01') + 1 year and l_discount between .06 - 0.01 and .06 + 0.01 and l_quantity < 24");
			printResultSet(rs);
			rs.close();
		}
		catch(Exception e)
		{
			System.out.println(e);
			e.printStackTrace();
			System.exit(0);
		}
		
		System.out.println(runCommand("date"));
		System.out.println("Query #6 end");
	}
	
	private static void query7_1(Connection con)
	{
		System.out.println("Query #7 start");
		System.out.println(runCommand("date"));
		try
		{
			Statement stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery("select supp_nation, cust_nation, l_year, sum(volume) as revenue from ( select n1.n_name as supp_nation, n2.n_name as cust_nation, year (l_shipdate) as l_year, l_extendedprice * (1 - l_discount) as volume from tpcd.supplier, tpcd.lineitem,tpcd.orders, tpcd.customer, tpcd.nation n1, tpcd.nation n2 where s_suppkey = l_suppkey and o_orderkey = l_orderkey and c_custkey = o_custkey and s_nationkey = n1.n_nationkey and c_nationkey = n2.n_nationkey and ( (n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY') or (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE') ) and l_shipdate between date('1995-01-01') and date('1996-12-31') ) as shipping group by supp_nation, cust_nation, l_year order by supp_nation, cust_nation, l_year");
			printResultSet(rs);
			rs.close();
		}
		catch(Exception e)
		{
			System.out.println(e);
			e.printStackTrace();
			System.exit(0);
		}
		
		System.out.println(runCommand("date"));
		System.out.println("Query #7 end");
	}
	
	private static void query8_1(Connection con)
	{
		System.out.println("Query #8 start");
		System.out.println(runCommand("date"));
		try
		{
			Statement stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery("select o_year, sum(case when nation = 'BRAZIL' then volume else 0 end) / sum(volume) as mkt_share from ( select year(o_orderdate) as o_year, l_extendedprice * (1 - l_discount) as volume, n2.n_name as nation from tpcd.part, tpcd.supplier, tpcd.lineitem, tpcd.orders, tpcd.customer, tpcd.nation n1, tpcd.nation n2,tpcd.region where p_partkey = l_partkey and s_suppkey = l_suppkey and l_orderkey = o_orderkey and o_custkey = c_custkey and c_nationkey = n1.n_nationkey and n1.n_regionkey = r_regionkey and r_name = 'AMERICA' and s_nationkey = n2.n_nationkey and o_orderdate between date('1995-01-01') and date ('1996-12-31') and p_type = 'ECONOMY ANODIZED STEEL' ) as all_nations group by o_year order by o_year");
			printResultSet(rs);
			rs.close();
		}
		catch(Exception e)
		{
			System.out.println(e);
			e.printStackTrace();
			System.exit(0);
		}
		
		System.out.println(runCommand("date"));
		System.out.println("Query #8 end");
	}
	
	private static void query9_1(Connection con)
	{
		System.out.println("Query #9 start");
		System.out.println(runCommand("date"));
		try
		{
			Statement stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery("select nation, o_year, sum(amount) as sum_profit from ( select n_name as nation, year(o_orderdate) as o_year, l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount from tpcd.part, tpcd.supplier, tpcd.lineitem, tpcd.partsupp, tpcd.orders, tpcd.nation where s_suppkey = l_suppkey and ps_suppkey = l_suppkey and ps_partkey = l_partkey and p_partkey = l_partkey and o_orderkey = l_orderkey and s_nationkey = n_nationkey and p_name like '%green%' ) as profit group by nation, o_year order by nation, o_year desc");
			printResultSet(rs);
			rs.close();
		}
		catch(Exception e)
		{
			System.out.println(e);
			e.printStackTrace();
			System.exit(0);
		}
		
		System.out.println(runCommand("date"));
		System.out.println("Query #9 end");
	}
	
	private static void query10_1(Connection con)
	{
		System.out.println("Query #10 start");
		System.out.println(runCommand("date"));
		try
		{
			Statement stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery("select c_custkey, c_name, sum(l_extendedprice * (1 - l_discount)) as revenue, c_acctbal, n_name, c_address, c_phone, c_comment from tpcd.customer, tpcd.orders, tpcd.lineitem, tpcd.nation where c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate >= date ('1993-10-01') and o_orderdate < date ('1993-10-01') + 3 month and l_returnflag = 'R' and c_nationkey = n_nationkey group by c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment order by revenue desc fetch first 20 rows only");
			printResultSet(rs);
			rs.close();
		}
		catch(Exception e)
		{
			System.out.println(e);
			e.printStackTrace();
			System.exit(0);
		}
		
		System.out.println(runCommand("date"));
		System.out.println("Query #10 end");
	}
	
	private static void query11_1(Connection con)
	{
		System.out.println("Query #11 start");
		System.out.println(runCommand("date"));
		try
		{
			Statement stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery("select ps_partkey, sum(ps_supplycost * ps_availqty) as value from tpcd.partsupp, tpcd.supplier, tpcd.nation where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'GERMANY' group by ps_partkey having sum(ps_supplycost * ps_availqty) > ( select sum(ps_supplycost * ps_availqty) * 0.0001000000 from tpcd.partsupp, tpcd.supplier, tpcd.nation where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'GERMANY' ) order by value desc");
			printResultSet(rs);
			rs.close();
		}
		catch(Exception e)
		{
			System.out.println(e);
			e.printStackTrace();
			System.exit(0);
		}
		
		System.out.println(runCommand("date"));
		System.out.println("Query #11 end");
	}
	
	private static void query12_1(Connection con)
	{
		System.out.println("Query #12 start");
		System.out.println(runCommand("date"));
		try
		{
			Statement stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery("select l_shipmode, sum(case when o_orderpriority = '1-URGENT' or o_orderpriority = '2-HIGH' then 1 else 0 end) as high_line_count, sum(case when o_orderpriority <> '1-URGENT' and o_orderpriority <> '2-HIGH' then 1 else 0 end) as low_line_count from tpcd.orders, tpcd.lineitem where o_orderkey = l_orderkey and l_shipmode in ('MAIL', 'SHIP') and l_commitdate < l_receiptdate and l_shipdate < l_commitdate and l_receiptdate >= date ('1994-01-01') and l_receiptdate < date ('1994-01-01') + 1 year group by l_shipmode order by l_shipmode"); 
			printResultSet(rs);
			rs.close();
		}
		catch(Exception e)
		{
			System.out.println(e);
			e.printStackTrace();
			System.exit(0);
		}
		
		System.out.println(runCommand("date"));
		System.out.println("Query #12 end");
	}
	
	private static void query13_1(Connection con)
	{
		System.out.println("Query #13 start");
		System.out.println(runCommand("date"));
		try
		{
			Statement stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery("select c_count, count(*) as custdist from ( select c_custkey, count(o_orderkey) from tpcd.customer left outer join tpcd.orders on c_custkey = o_custkey and o_comment not like '%special%requests%' group by c_custkey ) as c_orders (c_custkey, c_count) group by c_count order by custdist desc, c_count desc");
			printResultSet(rs);
			rs.close();
		}
		catch(Exception e)
		{
			System.out.println(e);
			e.printStackTrace();
			System.exit(0);
		}
		
		System.out.println(runCommand("date"));
		System.out.println("Query #13 end");
	}
	
	private static void query14_1(Connection con)
	{
		System.out.println("Query #14 start");
		System.out.println(runCommand("date"));
		try
		{
			Statement stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery("select 100.00 * sum(case when p_type like 'PROMO%' then l_extendedprice * (1 - l_discount) else 0 end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue from tpcd.lineitem, tpcd.part where l_partkey = p_partkey and l_shipdate >= date ('1995-09-01') and l_shipdate < date ('1995-09-01') + 1 month");
			printResultSet(rs);
			rs.close();
		}
		catch(Exception e)
		{
			System.out.println(e);
			e.printStackTrace();
			System.exit(0);
		}
		
		System.out.println(runCommand("date"));
		System.out.println("Query #14 end");
	}
	
	private static void query15_1(Connection con)
	{
		System.out.println("Query #15 start");
		System.out.println(runCommand("date"));
		try
		{
			Statement stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery("with revenue (supplier_no, total_revenue) as ( select l_suppkey, sum(l_extendedprice * (1-l_discount)) from tpcd.lineitem where l_shipdate >= date ('1996-01-01') and l_shipdate < date ('1996-01-01') + 3 month group by l_suppkey ) select s_suppkey, s_name, s_address, s_phone, total_revenue from tpcd.supplier, revenue where s_suppkey = supplier_no and total_revenue = ( select max(total_revenue) from revenue ) order by s_suppkey");
			printResultSet(rs);
			rs.close();
		}
		catch(Exception e)
		{
			System.out.println(e);
			e.printStackTrace();
			System.exit(0);
		}
		
		System.out.println(runCommand("date"));
		System.out.println("Query #15 end");
	}
	
	private static void query16_1(Connection con)
	{
		System.out.println("Query #16 start");
		System.out.println(runCommand("date"));
		try
		{
			Statement stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery("select p_brand, p_type, p_size, count(distinct ps_suppkey) as supplier_cnt from tpcd.partsupp, tpcd.part where p_partkey = ps_partkey and p_brand <> 'Brand#45' and p_type not like 'MEDIUM POLISHED%' and p_size in (49, 14, 23, 45, 19, 3, 36, 9) and ps_suppkey not in ( select s_suppkey from tpcd.supplier where s_comment like '%Customer%Complaints%' ) group by p_brand, p_type, p_size order by supplier_cnt desc, p_brand, p_type, p_size");
			printResultSet(rs);
			rs.close();
		}
		catch(Exception e)
		{
			System.out.println(e);
			e.printStackTrace();
			System.exit(0);
		}
		
		System.out.println(runCommand("date"));
		System.out.println("Query #16 end");
	}
	
	private static void query17_1(Connection con)
	{
		System.out.println("Query #17 start");
		System.out.println(runCommand("date"));
		try
		{
			Statement stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery("select sum(l_extendedprice) / 7.0 as avg_yearly from tpcd.lineitem, tpcd.part where p_partkey = l_partkey and p_brand = 'Brand#23' and p_container = 'MED BOX' and l_quantity < ( select 0.2 * avg(l_quantity) from tpcd.lineitem where l_partkey = p_partkey )");
			printResultSet(rs);
			rs.close();
		}
		catch(Exception e)
		{
			System.out.println(e);
			e.printStackTrace();
			System.exit(0);
		}
		
		System.out.println(runCommand("date"));
		System.out.println("Query #17 end");
	}
	
	private static void query18_1(Connection con)
	{
		System.out.println("Query #18 start");
		System.out.println(runCommand("date"));
		try
		{
			Statement stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery("select c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, sum(l_quantity) from tpcd.customer, tpcd.orders, tpcd.lineitem where o_orderkey in ( select l_orderkey from tpcd.lineitem group by l_orderkey having sum(l_quantity) > 300 ) and c_custkey = o_custkey and o_orderkey = l_orderkey group by c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice order by o_totalprice desc, o_orderdate fetch first 100 rows only"); 
			printResultSet(rs);
			rs.close();
		}
		catch(Exception e)
		{
			System.out.println(e);
			e.printStackTrace();
			System.exit(0);
		}
		
		System.out.println(runCommand("date"));
		System.out.println("Query #18 end");
	}
	
	private static void query19_1(Connection con)
	{
		System.out.println("Query #19 start");
		System.out.println(runCommand("date"));
		try
		{
			Statement stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery("select sum(l_extendedprice* (1 - l_discount)) as revenue from tpcd.lineitem, tpcd.part where ( p_partkey = l_partkey and p_brand = 'Brand#12' and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') and l_quantity >= 1 and l_quantity <= 1 + 10 and p_size between 1 and 5 and l_shipmode in ('AIR', 'AIR REG') and l_shipinstruct = 'DELIVER IN PERSON' ) or ( p_partkey = l_partkey and p_brand = 'Brand#23' and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') and l_quantity >= 10 and l_quantity <= 10 + 10 and p_size between 1 and 10 and l_shipmode in ('AIR', 'AIR REG') and l_shipinstruct = 'DELIVER IN PERSON' ) or ( p_partkey = l_partkey and p_brand = 'Brand#34' and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') and l_quantity >= 20 and l_quantity <= 20 + 10 and p_size between 1 and 15 and l_shipmode in ('AIR', 'AIR REG') and l_shipinstruct = 'DELIVER IN PERSON' )");
			printResultSet(rs);
			rs.close();
		}
		catch(Exception e)
		{
			System.out.println(e);
			e.printStackTrace();
			System.exit(0);
		}
		
		System.out.println(runCommand("date"));
		System.out.println("Query #19 end");
	}
	
	private static void query20_1(Connection con)
	{
		System.out.println("Query #20 start");
		System.out.println(runCommand("date"));
		try
		{
			Statement stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery("select s_name, s_address from tpcd.supplier, tpcd.nation where s_suppkey in ( select ps_suppkey from tpcd.partsupp where ps_partkey in ( select p_partkey from tpcd.part where p_name like 'forest%' ) and ps_availqty > ( select 0.5 * sum(l_quantity) from tpcd.lineitem where l_partkey = ps_partkey and l_suppkey = ps_suppkey and l_shipdate >= date ('1994-01-01') and l_shipdate < date ('1994-01-01') + 1 year ) ) and s_nationkey = n_nationkey and n_name = 'CANADA' order by s_name"); 
			printResultSet(rs);
			rs.close();
		}
		catch(Exception e)
		{
			System.out.println(e);
			e.printStackTrace();
			System.exit(0);
		}
		
		System.out.println(runCommand("date"));
		System.out.println("Query #20 end");
	}
	
	private static void query21_1(Connection con)
	{
		System.out.println("Query #21 start");
		System.out.println(runCommand("date"));
		try
		{
			Statement stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery("select s_name, count(*) as numwait from tpcd.supplier, tpcd.lineitem l1, tpcd.orders, tpcd.nation where s_suppkey = l1.l_suppkey and o_orderkey = l1.l_orderkey and o_orderstatus = 'F' and l1.l_receiptdate > l1.l_commitdate and exists ( select * from tpcd.lineitem l2 where l2.l_orderkey = l1.l_orderkey and l2.l_suppkey <> l1.l_suppkey ) and not exists ( select * from tpcd.lineitem l3 where l3.l_orderkey = l1.l_orderkey and l3.l_suppkey <> l1.l_suppkey and l3.l_receiptdate > l3.l_commitdate ) and s_nationkey = n_nationkey and n_name = 'SAUDI ARABIA' group by s_name order by numwait desc, s_name fetch first 100 rows only");
			printResultSet(rs);
			rs.close();
		}
		catch(Exception e)
		{
			System.out.println(e);
			e.printStackTrace();
			System.exit(0);
		}
		
		System.out.println(runCommand("date"));
		System.out.println("Query #21 end");
	}
	
	private static void query22_1(Connection con)
	{
		System.out.println("Query #22 start");
		System.out.println(runCommand("date"));
		try
		{
			Statement stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery("select cntrycode, count(*) as numcust, sum(c_acctbal) as totacctbal from ( select substr(c_phone, 1, 2) as cntrycode, c_acctbal from tpcd.customer where substr(c_phone, 1, 2) in ('13', '31', '23', '29', '30', '18', '17') and c_acctbal > ( select avg(c_acctbal) from tpcd.customer where c_acctbal > 0.00 and substr(c_phone, 1, 2) in ('13', '31', '23', '29', '30', '18', '17') ) and not exists ( select * from tpcd.orders where o_custkey = c_custkey ) ) as custsale group by cntrycode order by cntrycode");
			printResultSet(rs);
			rs.close();
		}
		catch(Exception e)
		{
			System.out.println(e);
			e.printStackTrace();
			System.exit(0);
		}
		
		System.out.println(runCommand("date"));
		System.out.println("Query #22 end");
	}
	
	private static void query2_n(Connection[] con, int nodes)
	{
		System.out.println("Query #2 start");
		System.out.println(runCommand("date"));
		try
		{
			//create nodes# of threads to issue SQL statement
			int i = 0;
			SQLThread[] threads = new SQLThread[nodes];
			while (i < nodes)
			{
				threads[i] = new SQLThread(con[i], "select s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment from tpcd.part, (SELECT * FROM TPCD.SUPPLIER, TPCD.NATION, TPCD.REGION WHERE s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'EUROPE' UNION ALL SELECT * FROM TPCD.SUPPLIER_AUX, TPCD.NATION, TPCD.REGION WHERE s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'EUROPE'), tpcd.partsupp where p_partkey = ps_partkey and s_suppkey = ps_suppkey and p_size = 15 and p_type like '%BRASS' and ps_supplycost = (select min(ps_supplycost) from tpcd.partsupp, (SELECT * FROM TPCD.SUPPLIER, TPCD.NATION, TPCD.REGION WHERE s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'EUROPE' UNION ALL SELECT * FROM TPCD.SUPPLIER_AUX, TPCD.NATION, TPCD.REGION WHERE s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'EUROPE') where p_partkey = ps_partkey and s_suppkey = ps_suppkey) order by s_acctbal desc, n_name, s_name, p_partkey fetch first 100 rows only");
				threads[i].start();
				i++;
			}
			

			i = 0;
			while (i < nodes)
			{
				threads[i].join();
				i++;
			}
			
			int[] sortCols = new int[4];
			sortCols[0] = 1;
			sortCols[1] = 3;
			sortCols[2] = 2;
			sortCols[3] = 4;
			boolean[] direction = new boolean[4];
			direction[0] = false;
			direction[1] = true;
			direction[2] = true;
			direction[3] = true;
			
			printResultSet(sort(threads, sortCols, direction, 100));
		}
		catch(Exception e)
		{
			System.out.println(e);
			e.printStackTrace();
			System.exit(0);
		}
		
		System.out.println(runCommand("date"));
		System.out.println("Query #2 end");
	}
	
	private static ResultSet sort(RSThread[] threads, int[] sortCols, boolean[] dir, int first)
	{
		ResultSetWritable retval = null;
		
		try
		{
			retval = new ResultSetWritable();
			retval.setResultSetMetaData(threads[0].rs.getMetaData());
			int i = 0;
			TreeMap<Row, Integer> tree = new TreeMap<Row, Integer>();
		
			for (RSThread thread : threads)
			{
				if (thread.rs.next())
				{
					tree.put(new Row(thread.rs, sortCols, dir), i);
				}
				i++;
			}
		
			i = 0;
			while (i < first && !tree.isEmpty())
			{
				Entry<Row, Integer> entry = tree.firstEntry();
				tree.remove(entry.getKey());
				retval.addRow(threads[entry.getValue()].rs);
				
				if (threads[entry.getValue()].rs.next())
				{
					tree.put(new Row(threads[entry.getValue()].rs, sortCols, dir), entry.getValue());
				}
			}
		
			for (RSThread thread : threads)
			{
				thread.rs.close();
				
				if (thread instanceof SQLThread)
				{
					((SQLThread)thread).stmt.close();
				}
			}
		}
		catch(Exception e)
		{
			System.out.println(e);
			e.printStackTrace();
		}
		
		return retval;
	}
	
	private static void query6_n(Connection[] con, int nodes)
	{
		System.out.println("Query #6 start");
		System.out.println(runCommand("date"));
		try
		{
			//create nodes# of threads to issue SQL statement
			int i = 0;
			SQLThread[] threads = new SQLThread[nodes];
			while (i < nodes)
			{
				threads[i] = new SQLThread(con[i], "select sum(l_extendedprice * l_discount) as revenue from tpcd.lineitem where l_shipdate >= date ('1994-01-01') and l_shipdate < date ('1994-01-01') + 1 year and l_discount between .06 - 0.01 and .06 + 0.01 and l_quantity < 24");
				threads[i].start();
				i++;
			}
			

			i = 0;
			double sum = 0;
			while (i < nodes)
			{
				threads[i].join();
				if (threads[i].rs.next())
				{
					sum += threads[i].rs.getDouble(1);
				}
				i++;
			}
			
			ResultSetWritable result = new ResultSetWritable();
			result.setResultSetMetaData(threads[0].rs.getMetaData());
			Vector<Object> row = new Vector<Object>();
			row.add(sum);
			result.addRow(row);
			printResultSet(result);
		}
		catch(Exception e)
		{
			System.out.println(e);
			e.printStackTrace();
			System.exit(0);
		}
		
		System.out.println(runCommand("date"));
		System.out.println("Query #6 end");
	}
	
	private static void query11_n(Connection[] con, int nodes)
	{
		System.out.println("Query #11 start");
		System.out.println(runCommand("date"));
		try
		{
			//create nodes# of threads to issue SQL statement
			int i = 0;
			SQLThread[] threads = new SQLThread[nodes];
			while (i < nodes)
			{
				threads[i] = new SQLThread(con[i], "select ps_partkey, sum(ps_supplycost * ps_availqty) as value from tpcd.partsupp, (SELECT * FROM TPCD.SUPPLIER, TPCD.NATION WHERE s_nationkey = n_nationkey and n_name = 'GERMANY' UNION ALL SELECT * FROM TPCD.SUPPLIER_AUX, TPCD.NATION WHERE s_nationkey = n_nationkey and n_name = 'GERMANY') where ps_suppkey = s_suppkey group by ps_partkey having sum(ps_supplycost * ps_availqty) > ( select sum(ps_supplycost * ps_availqty) * 0.0001000000 from tpcd.partsupp, (SELECT * FROM TPCD.SUPPLIER, TPCD.NATION where s_nationkey = n_nationkey and n_name = 'GERMANY' UNION ALL SELECT * FROM TPCD.SUPPLIER_AUX, TPCD.NATION WHERE s_nationkey = n_nationkey and n_name = 'GERMANY') WHERE ps_suppkey = s_suppkey) order by value desc");
				threads[i].start();
				i++;
			}
			

			i = 0;
			while (i < nodes)
			{
				threads[i].join();
				i++;
			}
			
			int[] sortCols = new int[1];
			sortCols[0] = 2;
			boolean[] direction = new boolean[1];
			direction[0] = false;
			
			printResultSet(sort(threads, sortCols, direction, Integer.MAX_VALUE));
		}
		catch(Exception e)
		{
			System.out.println(e);
			e.printStackTrace();
			System.exit(0);
		}
		
		System.out.println(runCommand("date"));
		System.out.println("Query #11 end");
	}
	
	private static void query14_n(Connection[] con, int nodes)
	{
		System.out.println("Query #14 start");
		System.out.println(runCommand("date"));
		try
		{
			SQLThread[] joins = new SQLThread[nodes];
			int i = 0;
			while (i < nodes)
			{
				joins[i] = new SQLThread(con[i], "SELECT P_PARTKEY FROM TPCD.PART WHERE p_type like 'PROMO%'");
				joins[i].start();
				i++;
			}
			
			i = 0;
			SQLThread[] threads = new SQLThread[nodes];
			while (i < nodes)
			{
				threads[i] = new SQLThread(con[i], "SELECT L_PARTKEY, l_extendedprice*(1-l_discount) FROM TPCD.LINEITEM WHERE l_shipdate >= date ('1995-09-01') and l_shipdate < date ('1995-09-01') + 1 month");
				threads[i].start();
				i++;
			}

			i = 0;
			HashSet<Integer> keys = new HashSet<Integer>();
			while (i < nodes)
			{
				joins[i].join();
				while (joins[i].rs.next())
				{
					keys.add(new Integer(joins[i].rs.getInt(1)));
				}
				i++;
			}
			
			i = 0;
			TopBottomSumByJoinThread<Integer>[] sums = new TopBottomSumByJoinThread[nodes];
			while (i < nodes)
			{
				threads[i].join();
				sums[i] = new TopBottomSumByJoinThread<Integer>(threads[i].rs, keys);
				sums[i].start();
				i++;
			}
			
			i = 0;
			double top = 0;
			double bottom = 0;
			while (i < nodes)
			{
				sums[i].join();
				top += (sums[i].top);
				bottom += (sums[i].bottom);
				i++;
			}
			
			SQLThread meta = new SQLThread(con[0], "SELECT l_extendedprice*(1-l_discount) FROM TPCD.LINEITEM WHERE 1 = 0");
			meta.start();
			meta.join();
			ResultSetWritable result = new ResultSetWritable();
			result.setResultSetMetaData(meta.rs.getMetaData());
			Vector<Object> row = new Vector<Object>();
			row.add(top * 100.0 / bottom);
			result.addRow(row);
			
			printResultSet(result);
		}
		catch(Exception e)
		{
			System.out.println(e);
			e.printStackTrace();
			System.exit(0);
		}
		
		System.out.println(runCommand("date"));
		System.out.println("Query #14 end");
	}
	
	private static void query22_n(Connection[] con, int nodes)
	{
		System.out.println("Query #22 start");
		System.out.println(runCommand("date"));
		try
		{
			//create nodes# of threads to issue SQL statement
			int i = 0;
			SQLThread[] threads = new SQLThread[nodes];
			while (i < nodes)
			{
				threads[i] = new SQLThread(con[i], "select avg(c_acctbal), count(*) from tpcd.customer where c_acctbal > 0.00 and substr(c_phone, 1, 2) in ('13', '31', '23', '29', '30', '18', '17')");
				threads[i].start();
				i++;
			}
			
			long count = 0;
			float total = 0;
			i = 0;
			while (i < nodes)
			{
				threads[i].join();
				threads[i].rs.next();
				int num = threads[i].rs.getInt(2);
				count += num;
				total += num * threads[i].rs.getDouble(1);
				threads[i].rs.close();
				i++;
			}
			float avg = total / count;
			
			i = 0;
			while (i < nodes)
			{
				threads[i] = new SQLThread(con[i], "select c_custkey from tpcd.customer where c_acctbal > " + avg + " and substr(c_phone, 1, 2) in ('13', '31', '23', '29', '30', '18', '17')");
				threads[i].start();
				i++;
			}
			
			HashSet<Integer> custs = new HashSet<Integer>();
			i = 0;
			while (i < nodes)
			{
				threads[i].join();
				while (threads[i].rs.next())
				{
					custs.add(threads[i].rs.getInt(1));
				}

				threads[i].rs.close();
				i++;
			}
			
			i = 0;
			Statement[] stmts = new Statement[nodes];
			while (i < nodes)
			{
				stmts[i] = con[i].createStatement();
				stmts[i].execute("CREATE TABLE TPCD.TEMPTABLE (C_CUSTKEY INT NOT NULL PRIMARY KEY) IN DATA_INDEX");
				stmts[i].close();
				i++;
			}
			
			i = 0;
			BatchInsertThread[] batches = new BatchInsertThread[nodes];
			while (i < nodes)
			{
				batches[i] = new BatchInsertThread(con[i], "INSERT INTO TPCD.TEMPTABLE VALUES (?)", custs, 1);
				batches[i].start();
				i++;
			}
			
			i = 0;
			while (i < nodes)
			{
				batches[i].join();
				i++;
			}
			
			String sql = "SELECT DISTINCT O_CUSTKEY FROM TPCD.ORDERS WHERE O_CUSTKEY IN (SELECT C_CUSTKEY FROM TPCD.TEMPTABLE)";
			
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
				while (threads[i].rs.next())
				{
					custs.remove(threads[i].rs.getInt(1));
				}

				threads[i].rs.close();
				i++;
			}
			i = 0;
			while (i < nodes)
			{
				stmts[i] = con[i].createStatement();
				stmts[i].execute("TRUNCATE TABLE TPCD.TEMPTABLE REUSE STORAGE IMMEDIATE");
				con[i].commit();
				batches[i] = new BatchInsertThread(con[i], "INSERT INTO TPCD.TEMPTABLE VALUES (?)", custs, 1);
				batches[i].start();
				i++;
			}
			i = 0;
			while (i < nodes)
			{
				batches[i].join();
				i++;
			}
			
			sql = "select cntrycode, count(*) as numcust, sum(c_acctbal) as totacctbal from (select substr(c_phone, 1, 2) as cntrycode, c_acctbal from tpcd.customer where C_CUSTKEY IN (SELECT C_CUSTKEY FROM TPCD.TEMPTABLE)) group by cntrycode order by cntrycode";
			
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
			i = 0;
			while (i < nodes)
			{
				stmts[i] = con[i].createStatement();
				stmts[i].execute("DROP TABLE TPCD.TEMPTABLE");
				stmts[i].close();
				i++;
			}
			
			int[] sortCols = new int[1];
			sortCols[0] = 1;
			boolean[] direction = new boolean[1];
			direction[0] = true;
			
			printResultSet(sort(threads, sortCols, direction, Integer.MAX_VALUE));
		}
		catch(Exception e)
		{
			System.out.println(e);
			e.printStackTrace();
			System.exit(0);
		}
		
		System.out.println(runCommand("date"));
		System.out.println("Query #22 end");
	}
	
	private static void query16_n(Connection[] con, int nodes)
	{
		System.out.println("Query #16 start");
		System.out.println(runCommand("date"));
		try
		{
			//create nodes# of threads to issue SQL statement
			int i = 0;
			SQLThread[] threads = new SQLThread[nodes];
			while (i < nodes)
			{
				threads[i] = new SQLThread(con[i], "select p_brand, p_type, p_size, count(distinct ps_suppkey) as supplier_cnt from tpcd.partsupp, tpcd.part where p_partkey = ps_partkey and p_brand <> 'Brand#45' and p_type not like 'MEDIUM POLISHED%' and p_size in (49, 14, 23, 45, 19, 3, 36, 9) and ps_suppkey not in ( select s_suppkey from (SELECT * FROM TPCD.SUPPLIER UNION ALL SELECT * FROM TPCD.SUPPLIER_AUX) where s_comment like '%Customer%Complaints%' ) group by p_brand, p_type, p_size order by supplier_cnt desc, p_brand, p_type, p_size");
				threads[i].start();
				i++;
			}
			

			i = 0;
			while (i < nodes)
			{
				threads[i].join();
				i++;
			}
			
			ResultSetWritable temp = new ResultSetWritable();
			temp.setResultSetMetaData(threads[0].rs.getMetaData());
			
			i = 0;
			HashMap<Vector<String>, Integer> map = new HashMap<Vector<String>, Integer>();
			while (i < nodes)
			{
				try
				{
					while (threads[i].rs.next())
					{
						Vector<String> group = new Vector<String>();
						group.add(threads[i].rs.getString(1));
						group.add(threads[i].rs.getString(2));
						group.add("" + threads[i].rs.getInt(3));
						if (!map.containsKey(group))
						{
							map.put(group, threads[i].rs.getInt(4));
						}
						else
						{
							map.put(group, map.remove(group) + threads[i].rs.getInt(4));
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
			
			for (Map.Entry entry : map.entrySet())
			{
				Vector<Object> row = new Vector<Object>();
				Vector<String> keys = new Vector<String>();
				keys = (Vector<String>)entry.getKey();
				row.add(keys.get(0));
				row.add(keys.get(1));
				row.add(Integer.parseInt(keys.get(2)));
				row.add(entry.getValue());
				temp.addRow(row);
			}
			
			int[] sortCols = new int[4];
			sortCols[0] = 4;
			sortCols[1] = 1;
			sortCols[2] = 2;
			sortCols[3] = 3;
			boolean[] dir = new boolean[4];
			dir[0] = false;
			dir[1] = true;
			dir[2] = true;
			dir[3] = true;
			TreeSet<Row> tree = new TreeSet<Row>();
			
			while (temp.next())
			{
				tree.add(new Row(temp, sortCols, dir));
			}
			
			ResultSetWritable retval = new ResultSetWritable();
			retval.setResultSetMetaData(temp.getMetaData());
			for (Row row : tree)
			{
				Vector<Object> data = new Vector<Object>();
				data.add(row.cols.get(0));
				data.add(row.cols.get(1));
				data.add(row.cols.get(2));
				data.add(row.cols.get(3));
				retval.addRow(data);
			}
			
			printResultSet(retval);
		}
		catch(Exception e)
		{
			System.out.println(e);
			e.printStackTrace();
			System.exit(0);
		}
		
		System.out.println(runCommand("date"));
		System.out.println("Query #16 end");
	}
	
	private static void query15_n(Connection[] con, int nodes)
	{
		System.out.println("Query #15 start");
		System.out.println(runCommand("date"));
		try
		{
			//create nodes# of threads to issue SQL statement
			int i = 0;
			SQLThread[] threads = new SQLThread[nodes];
			while (i < nodes)
			{
				threads[i] = new SQLThread(con[i], "with revenue (supplier_no, total_revenue) as ( select l_suppkey, sum(l_extendedprice * (1-l_discount)) from tpcd.lineitem where l_shipdate >= date ('1996-01-01') and l_shipdate < date ('1996-01-01') + 3 month group by l_suppkey ) select s_suppkey, s_name, s_address, s_phone, total_revenue from (SELECT * FROM TPCD.SUPPLIER UNION ALL SELECT * FROM TPCD.SUPPLIER_AUX), revenue where s_suppkey = supplier_no and total_revenue = ( select max(total_revenue) from revenue ) order by s_suppkey");
				threads[i].start();
				i++;
			}
			

			i = 0;
			while (i < nodes)
			{
				threads[i].join();
				i++;
			}
			
			int[] sortCols = new int[1];
			sortCols[0] = 1;
			boolean[] direction = new boolean[1];
			direction[0] = true;
			
			printResultSet(sort(threads, sortCols, direction, Integer.MAX_VALUE));
		}
		catch(Exception e)
		{
			System.out.println(e);
			e.printStackTrace();
			System.exit(0);
		}
		
		System.out.println(runCommand("date"));
		System.out.println("Query #15 end");
	}
	
	private static void query1_n(Connection[] cons, int nodes)
	{
		System.out.println("Query #1 start");
		System.out.println(runCommand("date"));
		
		try
		{
			Configuration conf = new Configuration();
			Job job = new Job(conf, "Query #1");
			configureJob(job);
			job.setNumReduceTasks(1);
			
			int i = 0;
			DB2InputSplit[] splits = new DB2InputSplit[nodes];
			while (i < nodes)
			{
				splits[i] = new DB2InputSplit();
				splits[i].setInstructions("Q1");
				String[] locations = new String[1];
				locations[0] = "db2" + (i+1);
				splits[i].setLocations(locations);
				splits[i].setPassword("db2inst1");
				splits[i].setSQL("select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price, sum(l_extendedprice*(1-l_discount)) as sum_disc_price, sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge, avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order from tpcd.lineitem where l_shipdate <= date('1998-12-01') - 90 day group by l_returnflag, l_linestatus order by l_returnflag, l_linestatus");
				splits[i].setURL("jdbc:db2://" + locations[0] + ":50000/TPCH");
				splits[i].setUserid("db2inst1");
				i++;
			}
			DB2InputFormat.setSplits(job, splits);
			
			JobThread jobThread = new JobThread(job);
			jobThread.start();
			
			ServerSocket sock = new ServerSocket(40001);
			boolean ok = false;
			while (!ok)
			{
				ok = true;
				Socket client = sock.accept();
				InputStream stream = client.getInputStream();
				BlockingDataInput in2 = new BlockingDataInput(stream);
				
				try
				{
					Text text = new Text();
					text.readFields(in2);
					String string = text.toString();
				
					if (!string.equals("TCPIPOutputFormat"))
					{
						ok = false;
						sock.close();
						System.out.println("Connection will be closed!  Invalid client command sent: " + string);
						continue;
					}
				}
				catch(Exception e)
				{
					System.out.println(e);
					e.printStackTrace();
					ok = false;
					continue;
				}
				
				ResultSetWritable rs = new ResultSetWritable();
				rs.readFields(in2);
				printResultSet(rs);
			}
			
			System.out.println(runCommand("date"));
			System.out.println("Query #1 end");
			sock.close();
		}
		catch(Exception e)
		{
			System.out.println(e);
			e.printStackTrace();
			System.exit(0);
		}
	}
	
	private static void configureJob(Job job)
	{
		job.setJarByClass(Driver.class);
		job.setMapperClass(DriverMap.class);
		job.setReducerClass(DriverReduce.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(ResultSetWritable.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(ResultSetWritable.class);
		job.setInputFormatClass(DB2InputFormat.class);
		job.setOutputFormatClass(TCPIPOutputFormat.class);
	}
	
	private static void query3_n(Connection[] cons, int nodes)
	{
		System.out.println("Query #3 start");
		System.out.println(runCommand("date"));
		
		try
		{
			Configuration conf = new Configuration();
			Job job = new Job(conf, "Query #3");
			configureJob(job);
			job.setNumReduceTasks(1);
			
			int i = 0;
			DB2InputSplit[] splits = new DB2InputSplit[2];
			while (i < 2)
			{
				splits[i] = new DB2InputSplit();
				splits[i].setInstructions("Q3");
				String[] locations = new String[1];
				locations[0] = "db2" + (i+1);
				splits[i].setLocations(locations);
				splits[i].setPassword("db2inst1");
				splits[i].setSQL("select l_orderkey, sum(l_extendedprice * (1 - l_discount)) as revenue, o_orderdate, o_shippriority from (SELECT * from tpcd.customer where c_mktsegment = 'BUILDING' UNION SELECT * FROM TPCD.CUSTOMER_AUX WHERE c_mktsegment = 'BUILDING'), tpcd.orders, tpcd.lineitem where c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate < date ('1995-03-15') and l_shipdate > date ('1995-03-15') group by l_orderkey, o_orderdate, o_shippriority order by revenue desc, o_orderdate fetch first 10 rows only");
				splits[i].setURL("jdbc:db2://" + locations[0] + ":50000/TPCH");
				splits[i].setUserid("db2inst1");
				i++;
			}
			DB2InputFormat.setSplits(job, splits);
			
			JobThread jobThread = new JobThread(job);
			jobThread.start();
			
			ServerSocket sock = new ServerSocket(40001);
			boolean ok = false;
			while (!ok)
			{
				ok = true;
				Socket client = sock.accept();
				InputStream stream = client.getInputStream();
				BlockingDataInput in2 = new BlockingDataInput(stream);
				
				try
				{
					Text text = new Text();
					text.readFields(in2);
					String string = text.toString();
				
					if (!string.equals("TCPIPOutputFormat"))
					{
						ok = false;
						sock.close();
						continue;
					}
				}
				catch(Exception e)
				{
					System.out.println(e);
					e.printStackTrace();
					ok = false;
					continue;
				}
				
				ResultSetWritable rs = new ResultSetWritable();
				rs.readFields(in2);
				printResultSet(rs);
			}
			
			System.out.println(runCommand("date"));
			System.out.println("Query #3 end");
			sock.close();
		}
		catch(Exception e)
		{
			System.out.println(e);
			e.printStackTrace();
			System.exit(0);
		}
	}
	
	private static void query4_n(Connection[] cons, int nodes)
	{
		System.out.println("Query #4 start");
		System.out.println(runCommand("date"));
		try
		{
			//create nodes# of threads to issue SQL statement
			SQLThread thread = new SQLThread(cons[0], "select o_orderpriority, count(*) as order_count from tpcd.orders where o_orderdate >= date ('1993-07-01') and o_orderdate < date ('1993-07-01') + 3 month and exists ( select * from tpcd.lineitem where l_orderkey = o_orderkey and l_commitdate < l_receiptdate ) group by o_orderpriority order by o_orderpriority");
			thread.start();
			thread.join();
			printResultSet(thread.rs);
		}
		catch(Exception e)
		{
			System.out.println(e);
			e.printStackTrace();
			System.exit(0);
		}
		
		System.out.println(runCommand("date"));
		System.out.println("Query #4 end");
	}
	
	private static void query5_n(Connection[] cons, int nodes)
	{
		System.out.println("Query #5 start");
		System.out.println(runCommand("date"));
		try
		{
			SQLThread thread;
			//create nodes# of threads to issue SQL statement
			if (nodes == 2)
			{
				thread = new SQLThread(cons[0], "select 	n_name, 	sum(l_extendedprice * (1 - l_discount)) as revenue 	from 	(SELECT * FROM tpcd.customer UNION SELECT * FROM TPCD.CUSTOMER_AUX), 	tpcd.orders, 	tpcd.lineitem, 	(SELECT * FROM tpcd.supplier UNION SELECT * FROM TPCD.SUPPLIER_AUX), 	tpcd.nation, 	tpcd.region 	where 	c_custkey = o_custkey 	and o_orderkey = l_orderkey 	and l_suppkey = s_suppkey 	and c_nationkey = s_nationkey 	and s_nationkey = n_nationkey 	and n_regionkey = r_regionkey 	and r_name = 'ASIA' 	and o_orderdate >= date ('1994-01-01') 	and o_orderdate < date ('1994-01-01') + 1 year 	group by 	n_name 	order by revenue desc");
			}
			else
			{
				thread = new SQLThread(cons[1], "select 	n_name, 	sum(l_extendedprice * (1 - l_discount)) as revenue 	from 	(SELECT * FROM tpcd.customer UNION SELECT * FROM TPCD.CUSTOMER_AUX), 	tpcd.orders, 	tpcd.lineitem, 	(SELECT * FROM tpcd.supplier UNION SELECT * FROM TPCD.SUPPLIER_AUX), 	tpcd.nation, 	tpcd.region 	where 	c_custkey = o_custkey 	and o_orderkey = l_orderkey 	and l_suppkey = s_suppkey 	and c_nationkey = s_nationkey 	and s_nationkey = n_nationkey 	and n_regionkey = r_regionkey 	and r_name = 'ASIA' 	and o_orderdate >= date ('1994-01-01') 	and o_orderdate < date ('1994-01-01') + 1 year 	group by 	n_name 	order by revenue desc");
			}
			thread.start();
			thread.join();
			printResultSet(thread.rs);
		}
		catch(Exception e)
		{
			System.out.println(e);
			e.printStackTrace();
			System.exit(0);
		}
		
		System.out.println(runCommand("date"));
		System.out.println("Query #5 end");
	}
	
	private static void query7_n(Connection[] cons, int nodes)
	{
		System.out.println("Query #7 start");
		System.out.println(runCommand("date"));
		
		try
		{
			Configuration conf = new Configuration();
			Job job = new Job(conf, "Query #7");
			configureJob(job);
			job.setNumReduceTasks(1);
			
			int i = 0;
			DB2InputSplit[] splits = new DB2InputSplit[nodes];
			while (i < nodes)
			{
				splits[i] = new DB2InputSplit();
				splits[i].setInstructions("Q7");
				String[] locations = new String[1];
				locations[0] = "db2" + (i+1);
				splits[i].setLocations(locations);
				splits[i].setPassword("db2inst1");
				splits[i].setSQL("select supp_nation, cust_nation, l_year, sum(volume) as revenue from ( select n1.n_name as supp_nation, n2.n_name as cust_nation, year  (l_shipdate) as l_year, l_extendedprice * (1 - l_discount) as volume from (SELECT * FROM tpcd.supplier UNION SELECT * FROM TPCD.SUPPLIER_AUX), tpcd.lineitem,tpcd.orders, (SELECT * FROM tpcd.customer UNION SELECT * FROM TPCD.CUSTOMER_AUX), tpcd.nation n1, tpcd.nation n2 where s_suppkey = l_suppkey and o_orderkey = l_orderkey and c_custkey = o_custkey and s_nationkey = n1.n_nationkey and c_nationkey = n2.n_nationkey and ( (n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY') or (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE') ) and l_shipdate between date('1995-01-01') and date('1996-12-31') ) as shipping group by supp_nation, cust_nation, l_year order by supp_nation, cust_nation, l_year");
				splits[i].setURL("jdbc:db2://" + locations[0] + ":50000/TPCH");
				splits[i].setUserid("db2inst1");
				i++;
			}
			DB2InputFormat.setSplits(job, splits);
			
			JobThread jobThread = new JobThread(job);
			jobThread.start();
			
			ServerSocket sock = new ServerSocket(40001);
			boolean ok = false;
			while (!ok)
			{
				ok = true;
				Socket client = sock.accept();
				InputStream stream = client.getInputStream();
				BlockingDataInput in2 = new BlockingDataInput(stream);
				
				try
				{
					Text text = new Text();
					text.readFields(in2);
					String string = text.toString();
				
					if (!string.equals("TCPIPOutputFormat"))
					{
						ok = false;
						sock.close();
						continue;
					}
				}
				catch(Exception e)
				{
					System.out.println(e);
					e.printStackTrace();
					ok = false;
					continue;
				}
				
				ResultSetWritable rs = new ResultSetWritable();
				rs.readFields(in2);
				printResultSet(rs);
			}
			
			System.out.println(runCommand("date"));
			System.out.println("Query #7 end");
			sock.close();
		}
		catch(Exception e)
		{
			System.out.println(e);
			e.printStackTrace();
			System.exit(0);
		}
	}
	
	private static void query10_n(Connection[] cons, int nodes)
	{
		System.out.println("Query #10 start");
		System.out.println(runCommand("date"));
		try
		{
			//create nodes# of threads to issue SQL statement
			SQLThread thread = new SQLThread(cons[0], "select c_custkey, c_name, sum(l_extendedprice * (1 - l_discount)) as revenue, c_acctbal, n_name, c_address, c_phone, c_comment from (SELECT * FROM tpcd.customer UNION SELECT * FROM TPCD.CUSTOMER_AUX), tpcd.orders, tpcd.lineitem, tpcd.nation where c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate >= date ('1993-10-01') and o_orderdate < date ('1993-10-01') + 3 month and l_returnflag = 'R' and c_nationkey = n_nationkey group by c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment order by revenue desc fetch first 20 rows only");
			thread.start();
			thread.join();
			printResultSet(thread.rs);
		}
		catch(Exception e)
		{
			System.out.println(e);
			e.printStackTrace();
			System.exit(0);
		}
		
		System.out.println(runCommand("date"));
		System.out.println("Query #10 end");
	}
	
	private static void query12_n(Connection[] cons, int nodes)
	{
		System.out.println("Query #12 start");
		System.out.println(runCommand("date"));
		
		try
		{
			Configuration conf = new Configuration();
			Job job = new Job(conf, "Query #12");
			configureJob(job);
			job.setNumReduceTasks(1);
			
			int i = 0;
			DB2InputSplit[] splits = new DB2InputSplit[nodes];
			while (i < nodes)
			{
				splits[i] = new DB2InputSplit();
				splits[i].setInstructions("Q12");
				String[] locations = new String[1];
				locations[0] = "db2" + (i+1);
				splits[i].setLocations(locations);
				splits[i].setPassword("db2inst1");
				splits[i].setSQL("select l_shipmode, sum(case when o_orderpriority = '1-URGENT' or o_orderpriority = '2-HIGH' then 1 else 0 end) as high_line_count, sum(case when o_orderpriority <> '1-URGENT' and o_orderpriority <> '2-HIGH' then 1 else 0 end) as low_line_count from tpcd.orders, tpcd.lineitem where o_orderkey = l_orderkey and l_shipmode in ('MAIL', 'SHIP') and l_commitdate < l_receiptdate and l_shipdate < l_commitdate and l_receiptdate >= date ('1994-01-01') and l_receiptdate < date ('1994-01-01') + 1 year group by l_shipmode order by l_shipmode");
				splits[i].setURL("jdbc:db2://" + locations[0] + ":50000/TPCH");
				splits[i].setUserid("db2inst1");
				i++;
			}
			DB2InputFormat.setSplits(job, splits);
			
			JobThread jobThread = new JobThread(job);
			jobThread.start();
			
			ServerSocket sock = new ServerSocket(40001);
			boolean ok = false;
			while (!ok)
			{
				ok = true;
				Socket client = sock.accept();
				InputStream stream = client.getInputStream();
				BlockingDataInput in2 = new BlockingDataInput(stream);
				
				try
				{
					Text text = new Text();
					text.readFields(in2);
					String string = text.toString();
				
					if (!string.equals("TCPIPOutputFormat"))
					{
						ok = false;
						sock.close();
						continue;
					}
				}
				catch(Exception e)
				{
					System.out.println(e);
					e.printStackTrace();
					ok = false;
					continue;
				}
				
				ResultSetWritable rs = new ResultSetWritable();
				rs.readFields(in2);
				printResultSet(rs);
			}
			
			System.out.println(runCommand("date"));
			System.out.println("Query #12 end");
			sock.close();
		}
		catch(Exception e)
		{
			System.out.println(e);
			e.printStackTrace();
			System.exit(0);
		}
	}
	
	private static void query18_n(Connection[] cons, int nodes)
	{
		System.out.println("Query #18 start");
		System.out.println(runCommand("date"));
		
		try
		{
			Configuration conf = new Configuration();
			Job job = new Job(conf, "Query #18");
			configureJob(job);
			job.setNumReduceTasks(1);
			
			int i = 0;
			DB2InputSplit[] splits = new DB2InputSplit[nodes];
			while (i < nodes)
			{
				splits[i] = new DB2InputSplit();
				splits[i].setInstructions("Q18");
				String[] locations = new String[1];
				locations[0] = "db2" + (i+1);
				splits[i].setLocations(locations);
				splits[i].setPassword("db2inst1");
				splits[i].setSQL("select c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, sum(l_quantity) from (SELECT * FROM tpcd.customer UNION SELECT * FROM TPCD.CUSTOMER_AUX), tpcd.orders, tpcd.lineitem where o_orderkey in ( select l_orderkey from tpcd.lineitem group by l_orderkey having sum(l_quantity) > 300 ) and c_custkey = o_custkey and o_orderkey = l_orderkey group by c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice order by o_totalprice desc, o_orderdate fetch first 100 rows only");
				splits[i].setURL("jdbc:db2://" + locations[0] + ":50000/TPCH");
				splits[i].setUserid("db2inst1");
				i++;
			}
			DB2InputFormat.setSplits(job, splits);
			
			JobThread jobThread = new JobThread(job);
			jobThread.start();
			
			ServerSocket sock = new ServerSocket(40001);
			boolean ok = false;
			while (!ok)
			{
				ok = true;
				Socket client = sock.accept();
				InputStream stream = client.getInputStream();
				BlockingDataInput in2 = new BlockingDataInput(stream);
				
				try
				{
					Text text = new Text();
					text.readFields(in2);
					String string = text.toString();
				
					if (!string.equals("TCPIPOutputFormat"))
					{
						ok = false;
						sock.close();
						continue;
					}
				}
				catch(Exception e)
				{
					System.out.println(e);
					e.printStackTrace();
					ok = false;
					continue;
				}
				
				ResultSetWritable rs = new ResultSetWritable();
				rs.readFields(in2);
				printResultSet(rs);
			}
			
			System.out.println(runCommand("date"));
			System.out.println("Query #18 end");
			sock.close();
		}
		catch(Exception e)
		{
			System.out.println(e);
			e.printStackTrace();
			System.exit(0);
		}
	}
	
	private static void query21_n(Connection[] cons, int nodes)
	{
		System.out.println("Query #21 start");
		System.out.println(runCommand("date"));
		
		try
		{
			Configuration conf = new Configuration();
			Job job = new Job(conf, "Query #21");
			configureJob(job);
			job.setNumReduceTasks(1);
			
			int i = 0;
			DB2InputSplit[] splits = new DB2InputSplit[nodes];
			while (i < nodes)
			{
				splits[i] = new DB2InputSplit();
				splits[i].setInstructions("Q21");
				String[] locations = new String[1];
				locations[0] = "db2" + (i+1);
				splits[i].setLocations(locations);
				splits[i].setPassword("db2inst1");
				splits[i].setSQL("select s_name, count(*) as numwait from (SELECT * FROM tpcd.supplier UNION SELECT * FROM TPCD.SUPPLIER_AUX), tpcd.lineitem l1, tpcd.orders, tpcd.nation where s_suppkey = l1.l_suppkey and o_orderkey = l1.l_orderkey and o_orderstatus = 'F' and l1.l_receiptdate > l1.l_commitdate and exists ( select * from tpcd.lineitem l2 where l2.l_orderkey = l1.l_orderkey and l2.l_suppkey <> l1.l_suppkey ) and not exists ( 		select 		* 		from 		tpcd.lineitem l3 		where 		l3.l_orderkey = l1.l_orderkey 		and l3.l_suppkey <> l1.l_suppkey 		and l3.l_receiptdate > l3.l_commitdate 		) 		and s_nationkey = n_nationkey 		and n_name = 'SAUDI ARABIA' 		group by 		s_name 		order by 		numwait desc, 		s_name");
				splits[i].setURL("jdbc:db2://" + locations[0] + ":50000/TPCH");
				splits[i].setUserid("db2inst1");
				i++;
			}
			DB2InputFormat.setSplits(job, splits);
			
			JobThread jobThread = new JobThread(job);
			jobThread.start();
			
			ServerSocket sock = new ServerSocket(40001);
			boolean ok = false;
			while (!ok)
			{
				ok = true;
				Socket client = sock.accept();
				InputStream stream = client.getInputStream();
				BlockingDataInput in2 = new BlockingDataInput(stream);
				
				try
				{
					Text text = new Text();
					text.readFields(in2);
					String string = text.toString();
				
					if (!string.equals("TCPIPOutputFormat"))
					{
						ok = false;
						sock.close();
						continue;
					}
				}
				catch(Exception e)
				{
					System.out.println(e);
					e.printStackTrace();
					ok = false;
					continue;
				}
				
				ResultSetWritable rs = new ResultSetWritable();
				rs.readFields(in2);
				printResultSet(rs);
			}
			
			System.out.println(runCommand("date"));
			System.out.println("Query #21 end");
			sock.close();
		}
		catch(Exception e)
		{
			System.out.println(e);
			e.printStackTrace();
			System.exit(0);
		}
	}
	
	private static void query8_n(Connection[] cons, int nodes)
	{
		System.out.println("Query #8 start");
		System.out.println(runCommand("date"));
		
		try
		{
			Configuration conf = new Configuration();
			Job job = new Job(conf, "Query #8");
			configureJob(job);
			job.getConfiguration().set("HRDBMS.nodes", "" + nodes);
			
			DB2InputSplit[] splits = new DB2InputSplit[nodes+1];
			job.setNumReduceTasks(1);
				int i = 0;
				while (i < nodes)
				{
					splits[i] = new DB2InputSplit();
					splits[i].setInstructions("Q8B");
					String[] locations = new String[1];
					locations[0] = "db2" + (i+1);
					splits[i].setLocations(locations);
					splits[i].setPassword("db2inst1");
					splits[i].setSQL("SELECT P_PARTKEY FROM TPCD.PART WHERE  p_type = 'ECONOMY ANODIZED STEEL'");
					splits[i].setURL("jdbc:db2://" + locations[0] + ":50000/TPCH");
					splits[i].setUserid("db2inst1");
					i++;
				}
				
				splits[i] = new DB2InputSplit();
				splits[i].setInstructions("Q8C");
				String[] locations = new String[1];
				locations[0] = "db21";
				splits[i].setLocations(locations);
				splits[i].setPassword("db2inst1");
				splits[i].setSQL("SELECT YEAR(O_ORDERDATE), L_EXTENDEDPRICE FROM TPCD.ORDERS, TPCD.LINEITEM WHERE 0 = 1");
				splits[i].setURL("jdbc:db2://" + locations[0] + ":50000/TPCH");
				splits[i].setUserid("db2inst1");
			
			DB2InputFormat.setSplits(job, splits);
			
			JobThread jobThread = new JobThread(job);
			jobThread.start();
			
			ServerSocket sock = new ServerSocket(40001);
			RemoteRSThread[] rsThreads = new RemoteRSThread[1];
				Socket client = sock.accept();
				InputStream stream = client.getInputStream();
				BlockingDataInput in2 = new BlockingDataInput(stream);
				
				try
				{
					Text text = new Text();
					text.readFields(in2);
					String string = text.toString();
				
					if (!string.equals("TCPIPOutputFormat"))
					{
						sock.close();
					}
				}
				catch(Exception e)
				{
					System.out.println(e);
					e.printStackTrace();
				}
				
				rsThreads[0] = new RemoteRSThread(in2);
				rsThreads[0].start();
				rsThreads[0].join();
			
				printResultSet(rsThreads[0].rs);
			
			System.out.println(runCommand("date"));
			System.out.println("Query #8 end");
			sock.close();
		}
		catch(Exception e)
		{
			System.out.println(e);
			e.printStackTrace();
			System.exit(0);
		}
	}
	
	private static void query9_n(Connection[] cons, int nodes)
	{
		System.out.println("Query #9 start");
		System.out.println(runCommand("date"));
		
		try
		{
			Configuration conf = new Configuration();
			Job job = new Job(conf, "Query #9");
			configureJob(job);
			job.getConfiguration().set("HRDBMS.nodes", "" + nodes);
			
			DB2InputSplit[] splits; 
			
			splits = new DB2InputSplit[nodes+1];
			
			int i = 0;
			while (i < nodes)
			{		
				splits[i] = new DB2InputSplit();
				splits[i].setInstructions("Q9B");
				String[] locations = new String[1];
				locations[0] = "db2" + (i+1);
				splits[i].setLocations(locations);
				splits[i].setPassword("db2inst1");
				splits[i].setSQL("select n_name as nation, ps_supplycost, p_partkey, s_suppkey from tpcd.part, (SELECT * FROM tpcd.supplier UNION SELECT * FROM tpcd.SUPPLIER_AUX), tpcd.partsupp, tpcd.nation where ps_suppkey = s_suppkey and ps_partkey = p_partkey and s_nationkey = n_nationkey and p_name like '%green%'");
				splits[i].setURL("jdbc:db2://" + locations[0] + ":50000/TPCH");
				splits[i].setUserid("db2inst1");
				i++;
			}
				
			splits[i] = new DB2InputSplit();
			splits[i].setInstructions("Q9C");
			String[] locations = new String[1];
			locations[0] = "db21";
			splits[i].setLocations(locations);
			splits[i].setPassword("db2inst1");
			splits[i].setSQL("select n_name, year(o_orderdate), l_extendedprice from tpcd.nation, tpcd.orders, tpcd.lineitem where 0 = 1");
			splits[i].setURL("jdbc:db2://" + locations[0] + ":50000/TPCH");
			splits[i].setUserid("db2inst1");
			
			DB2InputFormat.setSplits(job, splits);
			
			JobThread jobThread = new JobThread(job);
			jobThread.start();
			
			ServerSocket sock = new ServerSocket(40001);
			RemoteRSThread[] rsThreads = new RemoteRSThread[1];
				Socket client = sock.accept();
				InputStream stream = client.getInputStream();
				BlockingDataInput in2 = new BlockingDataInput(stream);
				
				try
				{
					Text text = new Text();
					text.readFields(in2);
					String string = text.toString();
				
					if (!string.equals("TCPIPOutputFormat"))
					{
						sock.close();
					}
				}
				catch(Exception e)
				{
					System.out.println(e);
					e.printStackTrace();
				}
				
				rsThreads[0] = new RemoteRSThread(in2);
				rsThreads[0].start();
			
				rsThreads[0].join();
			
			printResultSet(rsThreads[0].rs);
			
			System.out.println(runCommand("date"));
			System.out.println("Query #9 end");
			sock.close();
		}
		catch(Exception e)
		{
			System.out.println(e);
			e.printStackTrace();
			System.exit(0);
		}
	}
	
	private static void query13_n(Connection[] cons, int nodes)
	{
		System.out.println("Query #13 start");
		System.out.println(runCommand("date"));
		
		try
		{
			Configuration conf = new Configuration();
			Job job = new Job(conf, "Query #13");
			configureJob(job);
			job.getConfiguration().set("HRDBMS.nodes", "" + nodes);
			
			DB2InputSplit[] splits; 
			
			job.setNumReduceTasks(1);
			splits = new DB2InputSplit[nodes];
			
			int i = 0;
			while (i < nodes)
			{		
				splits[i] = new DB2InputSplit();
				splits[i].setInstructions("Q13B");
				String[] locations = new String[1];
				locations[0] = "db2" + (i+1);
				splits[i].setLocations(locations);
				splits[i].setPassword("db2inst1");
				splits[i].setSQL("select C_CUSTKEY FROM TPCD.CUSTOMER"); 
				splits[i].setURL("jdbc:db2://" + locations[0] + ":50000/TPCH");
				splits[i].setUserid("db2inst1");
				i++;
			}
			
			DB2InputFormat.setSplits(job, splits);
			
			JobThread jobThread = new JobThread(job);
			jobThread.start();
			
			ServerSocket sock = new ServerSocket(40001);
			RemoteRSThread[] rsThreads = new RemoteRSThread[1];
			i = 0;
			while (i < 1)
			{
				Socket client = sock.accept();
				InputStream stream = client.getInputStream();
				BlockingDataInput in2 = new BlockingDataInput(stream);
				
				try
				{
					Text text = new Text();
					text.readFields(in2);
					String string = text.toString();
				
					if (!string.equals("TCPIPOutputFormat"))
					{
						sock.close();
						continue;
					}
				}
				catch(Exception e)
				{
					System.out.println(e);
					e.printStackTrace();
					continue;
				}
				
				rsThreads[i] = new RemoteRSThread(in2);
				rsThreads[i].start();
				i++;
			}
			
			rsThreads[0].join();
			
			printResultSet(rsThreads[0].rs);
			
			System.out.println(runCommand("date"));
			System.out.println("Query #13 end");
			sock.close();
		}
		catch(Exception e)
		{
			System.out.println(e);
			e.printStackTrace();
			System.exit(0);
		}
	}
	
	private static void query17_n(Connection[] cons, int nodes)
	{
		System.out.println("Query #17 start");
		System.out.println(runCommand("date"));
		
		try
		{
			Configuration conf = new Configuration();
			Job job = new Job(conf, "Query #17");
			configureJob(job);
			job.getConfiguration().set("HRDBMS.nodes", "" + nodes);
			
			DB2InputSplit[] splits; 
			
			job.setNumReduceTasks(1);
			splits = new DB2InputSplit[nodes + 1];
			
			int i = 0;
			while (i < nodes)
			{		
				splits[i] = new DB2InputSplit();
				splits[i].setInstructions("Q17B");
				String[] locations = new String[1];
				locations[0] = "db2" + (i+1);
				splits[i].setLocations(locations);
				splits[i].setPassword("db2inst1");
				splits[i].setSQL("select p_partkey from tpcd.part where p_brand = 'Brand#23' and p_container = 'MED BOX'");
				splits[i].setURL("jdbc:db2://" + locations[0] + ":50000/TPCH");
				splits[i].setUserid("db2inst1");
				i++;
			}
			
			splits[i] = new DB2InputSplit();
			splits[i].setInstructions("Q17C");
			String[] locations = new String[1];
			locations[0] = "db21";
			splits[i].setLocations(locations);
			splits[i].setPassword("db2inst1");
			splits[i].setSQL("select sum(l_extendedprice) from tpcd.lineitem where 0 = 1");
			splits[i].setURL("jdbc:db2://" + locations[0] + ":50000/TPCH");
			splits[i].setUserid("db2inst1");
			
			DB2InputFormat.setSplits(job, splits);
			
			JobThread jobThread = new JobThread(job);
			jobThread.start();
			
			ServerSocket sock = new ServerSocket(40001);
			RemoteRSThread[] rsThreads = new RemoteRSThread[1];
			i = 0;
			while (i < 1)
			{
				Socket client = sock.accept();
				InputStream stream = client.getInputStream();
				BlockingDataInput in2 = new BlockingDataInput(stream);
				
				try
				{
					Text text = new Text();
					text.readFields(in2);
					String string = text.toString();
				
					if (!string.equals("TCPIPOutputFormat"))
					{
						sock.close();
						continue;
					}
				}
				catch(Exception e)
				{
					System.out.println(e);
					e.printStackTrace();
					continue;
				}
				
				rsThreads[i] = new RemoteRSThread(in2);
				rsThreads[i].start();
				i++;
			}
			
			rsThreads[0].join();
			
			printResultSet(rsThreads[0].rs);
			
			System.out.println(runCommand("date"));
			System.out.println("Query #17 end");
			sock.close();
		}
		catch(Exception e)
		{
			System.out.println(e);
			e.printStackTrace();
			System.exit(0);
		}
	}
	
	private static void query19_n(Connection[] cons, int nodes)
	{
		System.out.println("Query #19 start");
		System.out.println(runCommand("date"));
		
		try
		{
			Configuration conf = new Configuration();
			Job job = new Job(conf, "Query #19");
			configureJob(job);
			
			DB2InputSplit[] splits; 
			
			job.setNumReduceTasks(3);
			splits = new DB2InputSplit[nodes*6 + 1];
			
			int i = 0;
			while (i < nodes)
			{
				splits[6*i] = new DB2InputSplit();
				splits[6*i].setInstructions("Q19A1");
				String[] locations = new String[1];
				locations[0] = "db2" + (i+1);
				splits[6*i].setLocations(locations);
				splits[6*i].setPassword("db2inst1");
				splits[6*i].setSQL("select l_extendedprice* (1 - l_discount) as revenue, l_partkey from tpcd.lineitem where l_quantity >= 1 and l_quantity <= 1 + 10 and l_shipmode in ('AIR', 'AIR REG') and l_shipinstruct = 'DELIVER IN PERSON'");
				splits[6*i].setURL("jdbc:db2://" + locations[0] + ":50000/TPCH");
				splits[6*i].setUserid("db2inst1");
					
				splits[6*i+1] = new DB2InputSplit();
				splits[6*i+1].setInstructions("Q19A2");
				locations[0] = "db2" + (i+1);
				splits[6*i+1].setLocations(locations);
				splits[6*i+1].setPassword("db2inst1");
				splits[6*i+1].setSQL("select l_extendedprice* (1 - l_discount) as revenue, l_partkey from tpcd.lineitem where l_quantity >= 10 and l_quantity <= 10 + 10 and l_shipmode in ('AIR', 'AIR REG') and l_shipinstruct = 'DELIVER IN PERSON'");
				splits[6*i+1].setURL("jdbc:db2://" + locations[0] + ":50000/TPCH");
				splits[6*i+1].setUserid("db2inst1");
				
				splits[6*i+2] = new DB2InputSplit();
				splits[6*i+2].setInstructions("Q19A3");
				locations[0] = "db2" + (i+1);
				splits[6*i+2].setLocations(locations);
				splits[6*i+2].setPassword("db2inst1");
				splits[6*i+2].setSQL("select l_extendedprice* (1 - l_discount) as revenue, l_partkey from tpcd.lineitem where l_quantity >= 20 and l_quantity <= 20 + 10 and l_shipmode in ('AIR', 'AIR REG') and l_shipinstruct = 'DELIVER IN PERSON'");
				splits[6*i+2].setURL("jdbc:db2://" + locations[0] + ":50000/TPCH");
				splits[6*i+2].setUserid("db2inst1");
				
				splits[6*i+3] = new DB2InputSplit();
				splits[6*i+3].setInstructions("Q19B1");
				locations[0] = "db2" + (i+1);
				splits[6*i+3].setLocations(locations);
				splits[6*i+3].setPassword("db2inst1");
				splits[6*i+3].setSQL("select p_partkey from tpcd.part where p_brand = 'Brand#12' and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') and p_size between 1 and 5");
				splits[6*i+3].setURL("jdbc:db2://" + locations[0] + ":50000/TPCH");
				splits[6*i+3].setUserid("db2inst1");
				
				splits[6*i+4] = new DB2InputSplit();
				splits[6*i+4].setInstructions("Q19B2");
				locations[0] = "db2" + (i+1);
				splits[6*i+4].setLocations(locations);
				splits[6*i+4].setPassword("db2inst1");
				splits[6*i+4].setSQL("select p_partkey from tpcd.part where p_brand = 'Brand#23' and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') and p_size between 1 and 10");
				splits[6*i+4].setURL("jdbc:db2://" + locations[0] + ":50000/TPCH");
				splits[6*i+4].setUserid("db2inst1");
				
				splits[6*i+5] = new DB2InputSplit();
				splits[6*i+5].setInstructions("Q19B3");
				locations[0] = "db2" + (i+1);
				splits[6*i+5].setLocations(locations);
				splits[6*i+5].setPassword("db2inst1");
				splits[6*i+5].setSQL("select p_partkey from tpcd.part where p_brand = 'Brand#34' and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') and p_size between 1 and 15");
				splits[6*i+5].setURL("jdbc:db2://" + locations[0] + ":50000/TPCH");
				splits[6*i+5].setUserid("db2inst1");
				i++;
			}
			
			splits[6*i] = new DB2InputSplit();
			splits[6*i].setInstructions("Q19C");
			String[] locations = new String[1];
			locations[0] = "db21";
			splits[6*i].setLocations(locations);
			splits[6*i].setPassword("db2inst1");
			splits[6*i].setSQL("select sum(l_extendedprice) from tpcd.lineitem where 0 = 1");
			splits[6*i].setURL("jdbc:db2://" + locations[0] + ":50000/TPCH");
			splits[6*i].setUserid("db2inst1");
			
			DB2InputFormat.setSplits(job, splits);
			
			JobThread jobThread = new JobThread(job);
			jobThread.start();
			
			ServerSocket sock = new ServerSocket(40001);
			RemoteRSThread[] rsThreads = new RemoteRSThread[3];
			i = 0;
			while (i < 3)
			{
				Socket client = sock.accept();
				InputStream stream = client.getInputStream();
				BlockingDataInput in2 = new BlockingDataInput(stream);
				
				try
				{
					Text text = new Text();
					text.readFields(in2);
					String string = text.toString();
				
					if (!string.equals("TCPIPOutputFormat"))
					{
						sock.close();
						continue;
					}
				}
				catch(Exception e)
				{
					System.out.println(e);
					e.printStackTrace();
					continue;
				}
				
				rsThreads[i] = new RemoteRSThread(in2);
				rsThreads[i].start();
				i++;
			}
			
			ResultSetWritable rs = new ResultSetWritable();
			rsThreads[0].join();
			rs.setResultSetMetaData(rsThreads[0].rs.getMetaData());
			rsThreads[0].rs.next();
			double sum = rsThreads[0].rs.getDouble(1);
			i = 1;
			while (i < 3)
			{
				rsThreads[i].join();
				rsThreads[i].rs.next();
				sum += rsThreads[i].rs.getDouble(1);
				i++;
			}
			Vector<Object> row = new Vector<Object>();
			row.add(sum);
			rs.addRow(row);
			
			printResultSet(rs);
			
			System.out.println(runCommand("date"));
			System.out.println("Query #19 end");
			sock.close();
		}
		catch(Exception e)
		{
			System.out.println(e);
			e.printStackTrace();
			System.exit(0);
		}
	}
	
	private static void query20_n(Connection[] cons, int nodes)
	{
		System.out.println("Query #20 start");
		System.out.println(runCommand("date"));
		
		try
		{
			Configuration conf = new Configuration();
			Job job = new Job(conf, "Query #20");
			configureJob(job);
			job.getConfiguration().set("HRDBMS.nodes", "" + nodes);
			
			DB2InputSplit[] splits; 
			
			job.setNumReduceTasks(1);
			splits = new DB2InputSplit[nodes + 1];
			
			int i = 0;
			while (i < nodes)
			{	
				splits[i] = new DB2InputSplit();
				splits[i].setInstructions("Q20B");
				String[] locations = new String[1];
				locations[0] = "db2" + (i+1);
				splits[i].setLocations(locations);
				splits[i].setPassword("db2inst1");
				splits[i].setSQL("select ps_partkey, ps_suppkey, ps_availqty, s_name, s_address from tpcd.partsupp, tpcd.nation, tpcd.part, (SELECT * FROM TPCD.SUPPLIER UNION SELECT * FROM TPCD.SUPPLIER_AUX) WHERE s_nationkey = n_nationkey and n_name = 'CANADA' and ps_suppkey = s_suppkey and ps_partkey = p_partkey and p_name like 'forest%'");
				splits[i].setURL("jdbc:db2://" + locations[0] + ":50000/TPCH");
				splits[i].setUserid("db2inst1");
				i++;
			}
			
			splits[i] = new DB2InputSplit();
			splits[i].setInstructions("Q20C");
			String[] locations = new String[1];
			locations[0] = "db21";
			splits[i].setLocations(locations);
			splits[i].setPassword("db2inst1");
			splits[i].setSQL("select s_name, s_address from tpcd.supplier where 0 = 1");
			splits[i].setURL("jdbc:db2://" + locations[0] + ":50000/TPCH");
			splits[i].setUserid("db2inst1");
			
			DB2InputFormat.setSplits(job, splits);
			
			JobThread jobThread = new JobThread(job);
			jobThread.start();
			
			ServerSocket sock = new ServerSocket(40001);
			RemoteRSThread[] rsThreads = new RemoteRSThread[1];
			i = 0;
			while (i < 1)
			{
				Socket client = sock.accept();
				InputStream stream = client.getInputStream();
				BlockingDataInput in2 = new BlockingDataInput(stream);
				
				try
				{
					Text text = new Text();
					text.readFields(in2);
					String string = text.toString();
				
					if (!string.equals("TCPIPOutputFormat"))
					{
						sock.close();
						continue;
					}
				}
				catch(Exception e)
				{
					System.out.println(e);
					e.printStackTrace();
					continue;
				}
				
				rsThreads[i] = new RemoteRSThread(in2);
				rsThreads[i].start();
				i++;
			}
			
			rsThreads[0].join();
			
			printResultSet(rsThreads[0].rs);
			
			System.out.println(runCommand("date"));
			System.out.println("Query #20 end");
			sock.close();
		}
		catch(Exception e)
		{
			System.out.println(e);
			e.printStackTrace();
			System.exit(0);
		}
	}
}