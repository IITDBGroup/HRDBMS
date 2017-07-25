package com.exascale.testing;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

public class GeneratePBPEQueries
{
	private static String q1 = "select s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment from tpch2.part, tpch2.supplier, tpch2.partsupp, tpch2.nation, tpch2.region where p_partkey = ps_partkey and s_suppkey = ps_suppkey and p_size = SIZE and p_type like '%TYPE' and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'REGION' and ps_supplycost = (select min(ps_supplycost) from tpch2.partsupp, tpch2.supplier, tpch2.nation, tpch2.region where p_partkey = ps_partkey and s_suppkey = ps_suppkey and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'REGION') order by s_acctbal desc, n_name, s_name, p_partkey fetch first 100 rows only";
	private static String q2 = "select l_orderkey, sum(l_extendedprice * (1 - l_discount)) as revenue, o_orderdate, o_shippriority from tpch2.customer, tpch2.orders, tpch2.lineitem where c_mktsegment = 'SEGMENT' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate < date('1995-03-DAY') and l_shipdate > date('1995-03-DAY') group by l_orderkey, o_orderdate, o_shippriority order by revenue desc, o_orderdate fetch first 10 rows only";
	private static String q3 = "select o_orderpriority, count(*) as order_count from tpch2.orders where o_orderdate >= date('YEAR-MONTH-01') and o_orderdate < date('YEAR-MONTH-01') + months(3) and exists (select * from tpch2.lineitem where l_orderkey = o_orderkey and l_commitdate < l_receiptdate) group by o_orderpriority order by o_orderpriority";
	private static String q4 = "select n_name, sum(l_extendedprice * (1 - l_discount)) as revenue from tpch2.customer, tpch2.orders, tpch2.lineitem, tpch2.supplier, tpch2.nation, tpch2.region where c_custkey = o_custkey and l_orderkey = o_orderkey and l_suppkey = s_suppkey and c_nationkey = s_nationkey and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'REGION' and o_orderdate >= date('YEAR-01-01') and o_orderdate < date('YEAR-01-01') + years(1) group by n_name order by revenue desc";
	private static String q5 = "select sum(l_extendedprice * l_discount) as revenue from tpch2.lineitem where l_shipdate >= date('YEAR-01-01') and l_shipdate < date('YEAR-01-01') + years(1) and l_discount >= DISC1 and l_discount <= DISC2 and l_quantity < QUANTITY";
	private static String q6 = "select supp_nation, cust_nation, l_year, sum(volume) as revenue from (select n1.n_name as supp_nation, n2.n_name as cust_nation, year(l_shipdate) as l_year, l_extendedprice * (1 - l_discount) as volume from tpch2.supplier, tpch2.lineitem, tpch2.orders, tpch2.customer, tpch2.nation n1, tpch2.nation n2 where s_suppkey = l_suppkey and o_orderkey = l_orderkey and c_custkey = o_custkey and s_nationkey = n1.n_nationkey and c_nationkey = n2.n_nationkey and ((n1.n_name = 'NATION1' and n2.n_name = 'NATION2') or (n1.n_name = 'NATION2' and n2.n_name = 'NATION1')) and l_shipdate >= date('1995-01-01') and l_shipdate <= date('1996-12-31')) as shipping group by supp_nation, cust_nation, l_year order by supp_nation, cust_nation, l_year";
	private static String q7 = "select c_custkey, c_name, sum(l_extendedprice * (1 - l_discount)) as revenue, c_acctbal, n_name, c_address, c_phone, c_comment from tpch2.customer, tpch2.orders, tpch2.lineitem, tpch2.nation where c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate >= date('YEAR-MONTH-01') and o_orderdate < date('YEAR-MONTH-01') + months(3) and l_returnflag = 'R' and c_nationkey = n_nationkey group by c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment order by revenue desc fetch first 20 rows only";
	private static String q8 = "select sum(case when o_orderpriority ='1-URGENT' or o_orderpriority ='2-HIGH' then 1 else 0 end) as high_line_count, sum(case when o_orderpriority <> '1-URGENT' and o_orderpriority <> '2-HIGH' then 1 else 0 end) as low_line_count from tpch2.orders, tpch2.lineitem where o_orderkey = l_orderkey and l_shipmode in ('SHIP1', 'SHIP2') and l_commitdate < l_receiptdate and l_shipdate < l_commitdate and l_receiptdate >= date('YEAR-01-01') and l_receiptdate < date('YEAR-01-01') + years(1)";
	private static String q9 = "select 100.00 * sum(case when p_type like 'PROMO%' then l_extendedprice * (1 - l_discount) else 0 end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue from tpch2.lineitem, tpch2.part where l_partkey = p_partkey and l_shipdate >= date('YEAR-MONTH-01') and l_shipdate < date('YEAR-MONTH-01') + months(1)";
	private static String q10 = "select p_brand, p_type, p_size, count(distinct ps_suppkey) as supplier_cnt from tpch2.partsupp, tpch2.part where p_partkey = ps_partkey and p_brand <> 'BRAND' and p_type not like 'TYPE1 TYPE2%' and p_size in (SIZE1, SIZE2, SIZE3, SIZE4, SIZE5, SIZE6, SIZE7, SIZE8) and ps_suppkey not in (select s_suppkey from tpch2.supplier where s_comment like '%Customer%Complaints%') group by p_brand, p_type, p_size order by supplier_cnt desc, p_brand, p_type, p_size";
	private static String q11 = "select sum(l_extendedprice * (1 - l_discount)) as revenue from tpch2.lineitem, tpch2.part where (p_partkey = l_partkey and p_brand = 'BRAND1' and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') and l_quantity >= QUANTITY1 and l_quantity <= QUANTITY2 and p_size >= 1 and p_size <= 5 and l_shipmode in ('AIR', 'AIR REG') and l_shipinstruct = 'DELIVER IN PERSON') or (p_partkey = l_partkey and p_brand = 'BRAND2' and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') and l_quantity >= QUANTITY3 and l_quantity <= QUANTITY4 and p_size >= 1 and p_size <= 10 and l_shipmode in ('AIR', 'AIR REG') and l_shipinstruct = 'DELIVER IN PERSON') or (p_partkey = l_partkey and p_brand = 'BRAND3' and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') and l_quantity >= QUANTITY5 and l_quantity <= QUANTITY6 and p_size >= 1 and p_size <= 15 and l_shipmode in ('AIR', 'AIR REG') and l_shipinstruct = 'DELIVER IN PERSON')";
	private static String q12 = "select s_name, s_address from tpch2.supplier, tpch2.nation where s_suppkey in ( select ps_suppkey from tpch2.partsupp where ps_partkey in ( select p_partkey from tpch2.part where p_name like 'COLOR%') and ps_availqty > ( select 0.5 * sum(l_quantity) from tpch2.lineitem where l_partkey = ps_partkey and l_suppkey = ps_suppkey and l_shipdate >= date('YEAR-01-01') and l_shipdate < date('YEAR-01-01') + years(1))) and s_nationkey = n_nationkey and n_name = 'NATION' order by s_name";
	private static Random random = new Random();
	private static String[] types1 = { "STANDARD", "SMALL", "MEDIUM", "LARGE", "ECONOMY", "PROMO" };
	private static String[] types2 = { "ANODIZED", "BURNISHED", "PLATED", "POLISHED", "BRUSHED" };
	private static String[] types3 = { "TIN", "NICKEL", "BRASS", "STEEL", "COPPER" };
	private static String[] regions = { "AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST" };
	private static String[] segments = { "AUTOMOBILE", "BUILDING", "FURNITURE", "MACHINERY", "HOUSEHOLD" };
	private static String[] years = { "1993", "1994", "1995", "1996", "1997" };
	private static double[] discounts = { 0.02, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08, 0.09 };
	private static String[] nations = { "ALGERIA", "ARGENTINA", "BRAZIL", "CANADA", "EGYPT", "ETHIOPIA", "FRANCE", "GERMANY", "INDIA", "INDONESIA", "IRAN", "IRAQ", "JAPAN", "JORDAN", "KENYA", "MOROCCO", "MOZAMBIQUE", "PERU", "CHINA", "ROMANIA", "SAUDI ARABIA", "VIETNAM", "RUSSIA", "UNITED KINGDOM", "UNITED STATES" };
	private static String[] modes = { "REG AIR", "AIR", "MAIL", "SHIP", "RAIL", "TRUCK", "FOB" };
	private static String[] colors = { "almond", "antique", "aquamarine", "azure", "beige", "bisque", "black", "blanched", "blue", "blush", "brown", "burlywood", "burnished", "chartreuse", "chiffon", "chocolate", "coral", "cornflower", "cornsilk", "cream", "cyan", "dark", "deep", "dim", "dodger", "drab", "firebrick", "floral", "forest", "frosted", "gainsboro", "ghost", "goldenrod", "green", "grey", "honeydew", "hot", "indian", "ivory", "khaki", "lace", "lavender", "lawn", "lemon", "light", "lime", "linen", "magenta", "maroon", "medium", "metallic", "midnight", "mint", "misty", "moccasin", "navajo", "navy", "olive", "orange", "orchid", "pale", "papaya", "peach", "peru", "pink", "plum", "powder", "puff", "purple", "red", "rose", "rosy", "royal", "saddle", "salmon", "sandy", "seashell", "sienna", "sky", "slate", "smoke", "snow", "spring", "steel", "tan", "thistle", "tomato", "turquoise", "violet", "wheat", "white", "yellow" };

	public static void main(final String[] args)
	{
		try
		{
			final int streams = Integer.parseInt(args[0]);
			final PrintWriter out = new PrintWriter(new FileWriter("queries.out"));
			out.println("connect to jdbc:hrdbms://localhost:3232");
			out.println("timing on");
			int i = 0;
			while (i < streams)
			{
				final int size = random.nextInt(50) + 1;
				int t = random.nextInt(5);
				final String type = types3[t];
				t = random.nextInt(5);
				String region = regions[t];
				String temp = q1.replaceAll("SIZE", "" + size);
				temp = temp.replaceAll("TYPE", type);
				temp = temp.replaceAll("REGION", region);
				out.println(temp);

				t = random.nextInt(5);
				final String segment = segments[t];
				final int day = random.nextInt(31) + 1;
				temp = q2.replaceAll("SEGMENT", segment);
				temp = temp.replaceAll("DAY", "" + day);
				out.println(temp);

				t = random.nextInt(5);
				String year = years[t];
				final int m = random.nextInt(10) + 1;
				String month = "" + m;
				if (month.length() == 1)
				{
					month = "0" + month;
				}
				temp = q3.replaceAll("YEAR", year);
				temp = temp.replaceAll("MONTH", month);
				out.println(temp);

				t = random.nextInt(5);
				region = regions[t];
				t = random.nextInt(5);
				year = years[t];
				temp = q4.replaceAll("REGION", region);
				temp = temp.replaceAll("YEAR", year);
				out.println(temp);

				t = random.nextInt(5);
				year = years[t];
				boolean bool = random.nextBoolean();
				String quantity = "";
				if (bool)
				{
					quantity = "24";
				}
				else
				{
					quantity = "25";
				}
				t = random.nextInt(8);
				final double disc = discounts[t];
				temp = q5.replaceAll("YEAR", year);
				temp = temp.replaceAll("QUANTITY", quantity);
				temp = temp.replaceAll("DISC1", "" + (disc - 0.01));
				temp = temp.replaceAll("DISC2", "" + (disc + 0.01));
				out.println(temp);

				t = random.nextInt(25);
				int x = t;
				while (x == t)
				{
					x = random.nextInt(25);
				}
				final String nation1 = nations[t];
				final String nation2 = nations[x];
				temp = q6.replaceAll("NATION1", nation1);
				temp = temp.replaceAll("NATION2", nation2);
				out.println(temp);

				bool = random.nextBoolean();
				if (bool)
				{
					year = "1993";
				}
				else
				{
					year = "1994";
				}
				t = random.nextInt(11) + 2;
				month = "" + t;
				if (month.length() == 1)
				{
					month = "0" + month;
				}
				temp = q7.replaceAll("YEAR", year);
				temp = temp.replaceAll("MONTH", month);
				out.println(temp);

				t = random.nextInt(5);
				year = years[t];
				t = random.nextInt(7);
				x = t;
				while (x == t)
				{
					x = random.nextInt(7);
				}
				final String ship1 = modes[t];
				final String ship2 = modes[x];
				temp = q8.replaceAll("SHIP1", ship1);
				temp = temp.replaceAll("SHIP2", ship2);
				temp = temp.replaceAll("YEAR", year);
				out.println(temp);

				t = random.nextInt(5);
				year = years[t];
				t = random.nextInt(12) + 1;
				month = "" + t;
				if (month.length() == 1)
				{
					month = "0" + month;
				}
				temp = q9.replaceAll("YEAR", year);
				temp = temp.replaceAll("MONTH", month);
				out.println(temp);

				t = random.nextInt(5) + 1;
				x = random.nextInt(5) + 1;
				String brand = "Brand#" + t;
				brand += x;
				final Set<Integer> sizes = new HashSet<Integer>();
				while (sizes.size() < 8)
				{
					sizes.add(random.nextInt(50) + 1);
				}
				final int[] sizeArray = new int[8];
				int j = 0;
				for (final int s : sizes)
				{
					sizeArray[j++] = s;
				}
				t = random.nextInt(6);
				final String type1 = types1[t];
				t = random.nextInt(5);
				final String type2 = types2[t];
				temp = q10.replaceAll("BRAND", brand);
				temp = temp.replaceAll("TYPE1", type1);
				temp = temp.replaceAll("TYPE2", type2);
				j = 1;
				while (j <= 8)
				{
					temp = temp.replaceAll("SIZE" + j, "" + sizeArray[j - 1]);
					j++;
				}
				out.println(temp);

				t = random.nextInt(5) + 1;
				x = random.nextInt(5) + 1;
				String brand1 = "Brand#" + t;
				brand1 += x;
				t = random.nextInt(5) + 1;
				x = random.nextInt(5) + 1;
				String brand2 = "Brand#" + t;
				brand2 += x;
				t = random.nextInt(5) + 1;
				x = random.nextInt(5) + 1;
				String brand3 = "Brand#" + t;
				brand3 += x;
				t = random.nextInt(10) + 1;
				final String quant1 = "" + t;
				final String quant2 = "" + (t + 10);
				t = random.nextInt(11) + 10;
				final String quant3 = "" + t;
				final String quant4 = "" + (t + 10);
				t = random.nextInt(11) + 20;
				final String quant5 = "" + t;
				final String quant6 = "" + (t + 10);
				temp = q11.replaceAll("BRAND1", brand1);
				temp = temp.replaceAll("BRAND2", brand2);
				temp = temp.replaceAll("BRAND3", brand3);
				temp = temp.replaceAll("QUANTITY1", quant1);
				temp = temp.replaceAll("QUANTITY2", quant2);
				temp = temp.replaceAll("QUANTITY3", quant3);
				temp = temp.replaceAll("QUANTITY4", quant4);
				temp = temp.replaceAll("QUANTITY5", quant5);
				temp = temp.replaceAll("QUANTITY6", quant6);
				out.println(temp);

				t = random.nextInt(25);
				final String nation = nations[t];
				t = random.nextInt(5);
				year = years[t];
				t = random.nextInt(colors.length);
				final String color = colors[t];
				temp = q12.replaceAll("NATION", nation);
				temp = temp.replaceAll("YEAR", year);
				temp = temp.replaceAll("COLOR", color);
				out.println(temp);

				i++;
			}

			out.close();
		}
		catch (final Exception e)
		{
			e.printStackTrace();
		}
	}
}
