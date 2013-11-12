package com.exascale.optimizer.testing;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.Vector;

public class MetaData implements Serializable
{
	private boolean LOCAL = true;
	
	public HashMap<String, String> getCols2TypesForTable(String schema, String name) throws Exception
	{
		HashMap<String, String> retval = new HashMap<String, String>();
		if (name.equals("LINEITEM"))
		{
			retval.put("L_ORDERKEY", "INT");
			retval.put("L_PARTKEY", "INT");
			retval.put("L_SUPPKEY", "INT");
			retval.put("L_LINENUMBER", "INT");
			retval.put("L_QUANTITY", "FLOAT");
			retval.put("L_EXTENDEDPRICE", "FLOAT");
			retval.put("L_DISCOUNT", "FLOAT");
			retval.put("L_TAX", "FLOAT");
			retval.put("L_RETURNFLAG", "CHAR");
			retval.put("L_LINESTATUS", "CHAR");
			retval.put("L_SHIPDATE", "DATE");
			retval.put("L_COMMITDATE", "DATE");
			retval.put("L_RECEIPTDATE", "DATE");
			retval.put("L_SHIPINSTRUCT", "CHAR");
			retval.put("L_SHIPMODE", "CHAR");
			retval.put("L_COMMENT", "CHAR");
		}
		else if (name.equals("CUSTOMER"))
		{
			retval.put("C_CUSTKEY", "INT");
			retval.put("C_NAME", "CHAR");
			retval.put("C_ADDRESS", "CHAR");
			retval.put("C_NATIONKEY", "INT");
			retval.put("C_PHONE", "CHAR");
			retval.put("C_ACCTBAL", "FLOAT");
			retval.put("C_MKTSEGMENT", "CHAR");
			retval.put("C_COMMENT", "CHAR");
		}
		else if (name.equals("ORDERS"))
		{
			retval.put("O_ORDERKEY", "INT");
			retval.put("O_CUSTKEY", "INT");
			retval.put("O_ORDERSTATUS", "CHAR");
			retval.put("O_TOTALPRICE", "FLOAT");
			retval.put("O_ORDERDATE", "DATE");
			retval.put("O_ORDERPRIORITY", "CHAR");
			retval.put("O_CLERK", "CHAR");
			retval.put("O_SHIPPRIORITY", "INT");
			retval.put("O_COMMENT", "CHAR");
		}
		else if (name.equals("SUPPLIER"))
		{
			retval.put("S_SUPPKEY", "INT");
			retval.put("S_NAME", "CHAR");
			retval.put("S_ADDRESS", "CHAR");
			retval.put("S_NATIONKEY", "INT");
			retval.put("S_PHONE", "CHAR");
			retval.put("S_ACCTBAL", "FLOAT");
			retval.put("S_COMMENT", "CHAR");
		}
		else if (name.equals("NATION"))
		{
			retval.put("N_NATIONKEY", "INT");
			retval.put("N_NAME", "CHAR");
			retval.put("N_REGIONKEY", "INT");
			retval.put("N_COMMENT", "CHAR");
		}
		else if (name.equals("REGION"))
		{
			retval.put("R_REGIONKEY", "INT");
			retval.put("R_NAME", "CHAR");
			retval.put("R_COMMENT", "CHAR");
		}
		else if (name.equals("PART"))
		{
			retval.put("P_PARTKEY", "INT");
			retval.put("P_NAME", "CHAR");
			retval.put("P_MFGR", "CHAR");
			retval.put("P_BRAND", "CHAR");
			retval.put("P_TYPE", "CHAR");
			retval.put("P_SIZE", "INT");
			retval.put("P_CONTAINER", "CHAR");
			retval.put("P_RETAILPRICE", "FLOAT");
			retval.put("P_COMMENT","CHAR");
		}
		else if (name.equals("PARTSUPP"))
		{
			retval.put("PS_PARTKEY", "INT");
			retval.put("PS_SUPPKEY", "INT");
			retval.put("PS_AVAILQTY", "INT");
			retval.put("PS_SUPPLYCOST", "FLOAT");
			retval.put("PS_COMMENT", "CHAR");
		}
		else
		{
			throw new Exception("Unknown table.");
		}
		
		return retval;
	}
	
	public HashMap<String, Integer> getCols2PosForTable(String schema, String name) throws Exception
	{
		HashMap<String, Integer> retval = new HashMap<String, Integer>();
		if (name.equals("LINEITEM"))
		{
			retval.put("L_ORDERKEY", 0);
			retval.put("L_PARTKEY", 1);
			retval.put("L_SUPPKEY", 2);
			retval.put("L_LINENUMBER", 3);
			retval.put("L_QUANTITY", 4);
			retval.put("L_EXTENDEDPRICE", 5);
			retval.put("L_DISCOUNT", 6);
			retval.put("L_TAX", 7);
			retval.put("L_RETURNFLAG", 8);
			retval.put("L_LINESTATUS", 9);
			retval.put("L_SHIPDATE", 10);
			retval.put("L_COMMITDATE", 11);
			retval.put("L_RECEIPTDATE", 12);
			retval.put("L_SHIPINSTRUCT", 13);
			retval.put("L_SHIPMODE", 14);
			retval.put("L_COMMENT", 15);
		}
		else if (name.equals("CUSTOMER"))
		{
			retval.put("C_CUSTKEY", 0);
			retval.put("C_NAME", 1);
			retval.put("C_ADDRESS", 2);
			retval.put("C_NATIONKEY", 3);
			retval.put("C_PHONE", 4);
			retval.put("C_ACCTBAL", 5);
			retval.put("C_MKTSEGMENT", 6);
			retval.put("C_COMMENT", 7);
		}
		else if (name.equals("ORDERS"))
		{
			retval.put("O_ORDERKEY", 0);
			retval.put("O_CUSTKEY", 1);
			retval.put("O_ORDERSTATUS", 2);
			retval.put("O_TOTALPRICE", 3);
			retval.put("O_ORDERDATE", 4);
			retval.put("O_ORDERPRIORITY", 5);
			retval.put("O_CLERK", 6);
			retval.put("O_SHIPPRIORITY", 7);
			retval.put("O_COMMENT", 8);
		}
		else if (name.equals("SUPPLIER"))
		{
			retval.put("S_SUPPKEY", 0);
			retval.put("S_NAME", 1);
			retval.put("S_ADDRESS", 2);
			retval.put("S_NATIONKEY", 3);
			retval.put("S_PHONE", 4);
			retval.put("S_ACCTBAL", 5);
			retval.put("S_COMMENT", 6);
		}
		else if (name.equals("NATION"))
		{
			retval.put("N_NATIONKEY", 0);
			retval.put("N_NAME", 1);
			retval.put("N_REGIONKEY", 2);
			retval.put("N_COMMENT", 3);
		}
		else if (name.equals("REGION"))
		{
			retval.put("R_REGIONKEY", 0);
			retval.put("R_NAME", 1);
			retval.put("R_COMMENT", 2);
		}
		else if (name.equals("PART"))
		{
			retval.put("P_PARTKEY", 0);
			retval.put("P_NAME", 1);
			retval.put("P_MFGR", 2);
			retval.put("P_BRAND", 3);
			retval.put("P_TYPE", 4);
			retval.put("P_SIZE", 5);
			retval.put("P_CONTAINER", 6);
			retval.put("P_RETAILPRICE", 7);
			retval.put("P_COMMENT", 8);
		}
		else if (name.equals("PARTSUPP"))
		{
			retval.put("PS_PARTKEY", 0);
			retval.put("PS_SUPPKEY", 1);
			retval.put("PS_AVAILQTY", 2);
			retval.put("PS_SUPPLYCOST", 3);
			retval.put("PS_COMMENT", 4);
		}
		else
		{
			throw new Exception("Unknown table.");
		}
		
		return retval;
	}
	
	public TreeMap<Integer, String> getPos2ColForTable(String schema, String name) throws Exception
	{
		TreeMap<Integer, String> retval = new TreeMap<Integer, String>();
		if (name.equals("LINEITEM"))
		{
			retval.put(0, "L_ORDERKEY");
			retval.put(1, "L_PARTKEY");
			retval.put(2, "L_SUPPKEY");
			retval.put(3, "L_LINENUMBER");
			retval.put(4, "L_QUANTITY");
			retval.put(5, "L_EXTENDEDPRICE");
			retval.put(6, "L_DISCOUNT");
			retval.put(7, "L_TAX");
			retval.put(8, "L_RETURNFLAG");
			retval.put(9, "L_LINESTATUS");
			retval.put(10, "L_SHIPDATE");
			retval.put(11, "L_COMMITDATE");
			retval.put(12, "L_RECEIPTDATE");
			retval.put(13, "L_SHIPINSTRUCT");
			retval.put(14, "L_SHIPMODE");
			retval.put(15, "L_COMMENT");
		}
		else if (name.equals("CUSTOMER"))
		{
			retval.put(0, "C_CUSTKEY");
			retval.put(1, "C_NAME");
			retval.put(2, "C_ADDRESS");
			retval.put(3, "C_NATIONKEY");
			retval.put(4, "C_PHONE");
			retval.put(5, "C_ACCTBAL");
			retval.put(6, "C_MKTSEGMENT");
			retval.put(7, "C_COMMENT");
		}
		else if (name.equals("ORDERS"))
		{
			retval.put(0, "O_ORDERKEY");
			retval.put(1, "O_CUSTKEY");
			retval.put(2, "O_ORDERSTATUS");
			retval.put(3, "O_TOTALPRICE");
			retval.put(4, "O_ORDERDATE");
			retval.put(5, "O_ORDERPRIORITY");
			retval.put(6, "O_CLERK");
			retval.put(7, "O_SHIPPRIORITY");
			retval.put(8, "O_COMMENT");
		}
		else if (name.equals("SUPPLIER"))
		{
			retval.put(0, "S_SUPPKEY");
			retval.put(1, "S_NAME");
			retval.put(2, "S_ADDRESS");
			retval.put(3, "S_NATIONKEY");
			retval.put(4, "S_PHONE");
			retval.put(5, "S_ACCTBAL");
			retval.put(6, "S_COMMENT");
		}
		else if (name.equals("NATION"))
		{
			retval.put(0, "N_NATIONKEY");
			retval.put(1, "N_NAME");
			retval.put(2, "N_REGIONKEY");
			retval.put(3, "N_COMMENT");
		}
		else if (name.equals("REGION"))
		{
			retval.put(0, "R_REGIONKEY");
			retval.put(1, "R_NAME");
			retval.put(2, "R_COMMENT");
		}
		else if (name.equals("PART"))
		{
			retval.put(0, "P_PARTKEY");
			retval.put(1, "P_NAME");
			retval.put(2, "P_MFGR");
			retval.put(3, "P_BRAND");
			retval.put(4, "P_TYPE");
			retval.put(5, "P_SIZE");
			retval.put(6, "P_CONTAINER");
			retval.put(7, "P_RETAILPRICE");
			retval.put(8, "P_COMMENT");
		}
		else if (name.equals("PARTSUPP"))
		{
			retval.put(0, "PS_PARTKEY");
			retval.put(1, "PS_SUPPKEY");
			retval.put(2, "PS_AVAILQTY");
			retval.put(3, "PS_SUPPLYCOST");
			retval.put(4, "PS_COMMENT");
		}
		else
		{
			throw new Exception("Unknown table.");
		}
		
		return retval;
	}
	
	public double likelihood(ArrayList<Filter> filters, RootOperator op)
	{
		return likelihood(filters, op.getGenerated());
	}
	
	public double likelihood(ArrayList<Filter> filters)
	{
		double sum = 0;
		
		for (Filter filter : filters)
		{
			sum += likelihood(filter);
		}
		
		if (sum > 1)
		{
			sum = 1;
		}
		
		return sum;
	}
	
	public double likelihood(ArrayList<Filter> filters, HashMap<String, Double> generated)
	{
		double sum = 0;
		
		for (Filter filter : filters)
		{
			sum += likelihood(filter, generated);
		}
		
		if (sum > 1)
		{
			sum = 1;
		}
		
		return sum;
	}
	
	public double likelihood(Filter filter)
	{
		long leftCard = 1;
		long rightCard = 1;
		HashMap<String, Double> generated = new HashMap<String, Double>();
		
		if (filter.alwaysTrue())
		{
			return 1;
		}
		
		if (filter.alwaysFalse())
		{
			return 0;
		}
		
		if (filter.leftIsColumn())
		{
			leftCard = getCard(filter.leftColumn(), generated);
		}
		
		if (filter.rightIsColumn())
		{
			//figure out number of possible values for right side
			rightCard = getCard(filter.rightColumn(), generated);
		}
		
		String op = filter.op();
		
		if (op.equals("E"))
		{
			return 1.0 / bigger(leftCard, rightCard);
		}
		
		if (op.equals("NE"))
		{
			return 1.0 - 1.0 / bigger(leftCard, rightCard);
		}
		
		if (op.equals("L") || op.equals("LE"))
		{
			if (filter.leftIsColumn() && filter.rightIsColumn())
			{
				return 0.5;
			}
			
			if (filter.leftIsColumn())
			{
				if (filter.rightIsNumber())
				{
					double right = filter.getRightNumber();
					String left = filter.leftColumn();
					return percentBelow(left, right);
				}
				else if (filter.rightIsDate())
				{
					Date right = filter.getRightDate();
					String left = filter.leftColumn();
					return percentBelow(left, right);
				}
				else
				{
					//string
					String right = filter.getRightString();
					String left = filter.leftColumn();
					return percentBelow(left, right);
				}
			}
			else
			{
				if (filter.leftIsNumber())
				{
					double left = filter.getLeftNumber();
					String right = filter.rightColumn();
					return percentAbove(right, left);
				}
				else if (filter.leftIsDate())
				{
					Date left = filter.getLeftDate();
					String right = filter.rightColumn();
					return percentAbove(right, left);
				}
				else
				{
					//string
					String left = filter.getLeftString();
					String right = filter.rightColumn();
					return percentAbove(right, left);
				}
			}
		}
		
		if (op.equals("G") || op.equals("GE"))
		{
			if (filter.leftIsColumn() && filter.rightIsColumn())
			{
				return 0.5;
			}
			
			if (filter.leftIsColumn())
			{
				if (filter.rightIsNumber())
				{
					double right = filter.getRightNumber();
					String left = filter.leftColumn();
					return percentAbove(left, right);
				}
				else if (filter.rightIsDate())
				{
					Date right = filter.getRightDate();
					String left = filter.leftColumn();
					return percentAbove(left, right);
				}
				else
				{
					//string
					String right = filter.getRightString();
					String left = filter.leftColumn();
					return percentAbove(left, right);
				}
			}
			else
			{
				if (filter.leftIsNumber())
				{
					double left = filter.getLeftNumber();
					String right = filter.rightColumn();
					return percentBelow(right, left);
				}
				else if (filter.leftIsDate())
				{
					Date left = filter.getLeftDate();
					String right = filter.rightColumn();
					return percentBelow(right, left);
				}
				else
				{
					//string
					String left = filter.getLeftString();
					String right = filter.rightColumn();
					return percentBelow(right, left);
				}
			}
		}
		
		if (op.equals("LI"))
		{
			return 0.25;
		}
		
		if (op.equals("NL"))
		{
			return 0.75;
		}
		
		System.out.println("Unknown operator in likelihood()");
		System.exit(1);
		return 0;
	}
	
	public double likelihood(Filter filter, RootOperator op)
	{
		return likelihood(filter, op.getGenerated());
	}
	
	//likelihood of a row directly out of the table passing this test
	public double likelihood(Filter filter, HashMap<String, Double> generated)
	{
		long leftCard = 1;
		long rightCard = 1;
		
		if (filter.alwaysTrue())
		{
			return 1;
		}
		
		if (filter.alwaysFalse())
		{
			return 0;
		}
		
		if (filter.leftIsColumn())
		{
			leftCard = getCard(filter.leftColumn(), generated);
		}
		
		if (filter.rightIsColumn())
		{
			//figure out number of possible values for right side
			rightCard = getCard(filter.rightColumn(), generated);
		}
		
		String op = filter.op();
		
		if (op.equals("E"))
		{
			return 1.0 / bigger(leftCard, rightCard);
		}
		
		if (op.equals("NE"))
		{
			return 1.0 - 1.0 / bigger(leftCard, rightCard);
		}
		
		if (op.equals("L") || op.equals("LE"))
		{
			if (filter.leftIsColumn() && filter.rightIsColumn())
			{
				return 0.5;
			}
			
			if (filter.leftIsColumn())
			{
				if (filter.rightIsNumber())
				{
					double right = filter.getRightNumber();
					String left = filter.leftColumn();
					return percentBelow(left, right);
				}
				else if (filter.rightIsDate())
				{
					Date right = filter.getRightDate();
					String left = filter.leftColumn();
					return percentBelow(left, right);
				}
				else
				{
					//string
					String right = filter.getRightString();
					String left = filter.leftColumn();
					return percentBelow(left, right);
				}
			}
			else
			{
				if (filter.leftIsNumber())
				{
					double left = filter.getLeftNumber();
					String right = filter.rightColumn();
					return percentAbove(right, left);
				}
				else if (filter.leftIsDate())
				{
					Date left = filter.getLeftDate();
					String right = filter.rightColumn();
					return percentAbove(right, left);
				}
				else
				{
					//string
					String left = filter.getLeftString();
					String right = filter.rightColumn();
					return percentAbove(right, left);
				}
			}
		}
		
		if (op.equals("G") || op.equals("GE"))
		{
			if (filter.leftIsColumn() && filter.rightIsColumn())
			{
				return 0.5;
			}
			
			if (filter.leftIsColumn())
			{
				if (filter.rightIsNumber())
				{
					double right = filter.getRightNumber();
					String left = filter.leftColumn();
					return percentAbove(left, right);
				}
				else if (filter.rightIsDate())
				{
					Date right = filter.getRightDate();
					String left = filter.leftColumn();
					return percentAbove(left, right);
				}
				else
				{
					//string
					String right = filter.getRightString();
					String left = filter.leftColumn();
					return percentAbove(left, right);
				}
			}
			else
			{
				if (filter.leftIsNumber())
				{
					double left = filter.getLeftNumber();
					String right = filter.rightColumn();
					return percentBelow(right, left);
				}
				else if (filter.leftIsDate())
				{
					Date left = filter.getLeftDate();
					String right = filter.rightColumn();
					return percentBelow(right, left);
				}
				else
				{
					//string
					String left = filter.getLeftString();
					String right = filter.rightColumn();
					return percentBelow(right, left);
				}
			}
		}
		
		if (op.equals("LI"))
		{
			return 0.25;
		}
		
		if (op.equals("NL"))
		{
			return 0.75;
		}
		
		System.out.println("Unknown operator in likelihood()");
		System.exit(1);
		return 0;
	}
	
	private long smaller(long x, long y)
	{
		if (x <= y)
		{
			return x;
		}
		
		return y;
	}
	
	private long bigger(long x, long y)
	{
		if (x >= y)
		{
			return x;
		}
		
		return y;
	}
	
	public long getTableCard(String schema, String table)
	{
		if (table.equals("SUPPLIER"))
		{
			return 10000;
		}
		
		if (table.equals("PART"))
		{
			return 200000;
		}
		
		if (table.equals("PARTSUPP"))
		{
			return 800000;
		}
		
		if (table.equals("CUSTOMER"))
		{
			return 150000;
		}
		
		if (table.equals("ORDERS"))
		{
			return 1500000;
		}
		
		if (table.equals("LINEITEM"))
		{
			return 6001215;
		}
		
		if (table.equals("NATION"))
		{
			return 25;
		}
		
		if (table.equals("REGION"))
		{
			return 5;
		}
		
		System.out.println("Unknown table in getTableCard()");
		System.exit(1);
		return 0;
	}
	
	public long getColgroupCard(Vector<String> cols, RootOperator op)
	{
		return getColgroupCard(cols, op.getGenerated());
	}
	
	public long getColgroupCard(Vector<String> cols, HashMap<String, Double> generated)
	{
		//TODO should check gathered colgroup stats
		double card = 1;
		for (String col : cols)
		{
			card *= this.getCard(col, generated);
		}
		
		return (long)card;
	}
	
	public long getCard(String col, RootOperator op)
	{
		return getCard(col, op.getGenerated());
	}
	
	public long getCard(String col, HashMap<String, Double> generated)
	{
		try
		{
			BufferedReader in = new BufferedReader(new FileReader(new File("card.tbl")));
			String line = in.readLine();
			while (line != null)
			{
				StringTokenizer tokens = new StringTokenizer(line, "|", false);
				String column = tokens.nextToken();
				if (col.equals(column))
				{
					in.close();
					return Long.parseLong(tokens.nextToken());
				}
			
				line = in.readLine();
			}
			
			in.close();
		}
		catch(Exception e)
		{
			e.printStackTrace();
			System.exit(1);
		}
		
		if (generated.containsKey(col))
		{
			return (generated.get(col).longValue());
		}
		
		//System.out.println("Can't find cardinality for " + col);
		//Thread.dumpStack();
		return 1000000;
	}
	
	public HashMap<String, Double> generateCard(Operator op)
	{
		HashMap<Operator, ArrayList<String>> tables = new HashMap<Operator, ArrayList<String>>();
		HashMap<Operator, ArrayList<ArrayList<Filter>>> filters = new HashMap<Operator, ArrayList<ArrayList<Filter>>>();
		ArrayList<Operator> queued = new ArrayList<Operator>();
		HashMap<Operator, HashMap<String, Double>> retval = new HashMap<Operator, HashMap<String, Double>>();
		ArrayList<Operator> leaves = getLeaves(op);
		for (Operator leaf : leaves)
		{
			Operator o = doWork(leaf, tables, filters, retval);
			if (o != null)
			{
				queued.add(o);
			}
		}
		
		while (queued.size() > 1)
		{
			Operator o = queued.get(0);
			if (queued.indexOf(o) != queued.lastIndexOf(o))
			{
				queued.remove(queued.lastIndexOf(o));
				Operator o2 = doWork(o, tables, filters, retval);
				queued.add(o2);
				queued.remove(0);
			}
			else
			{
				queued.add(o);
				queued.remove(0);
			}
		}
		
		return retval.get(queued.get(0));
	}
	
	private ArrayList <Operator> getLeaves(Operator op)
	{
		if (op.children().size() == 0)
		{
			ArrayList<Operator> retval = new ArrayList<Operator>();
			retval.add(op);
			return retval;
		}
		
		ArrayList<Operator> retval = new ArrayList<Operator>();
		for (Operator o : op.children())
		{
			retval.addAll(getLeaves(o));
		}
		
		return retval;
	}
	
	private Operator doWork(Operator op, HashMap<Operator, ArrayList<String>> tables, HashMap<Operator, ArrayList<ArrayList<Filter>>> filters, HashMap<Operator, HashMap<String, Double>> retvals)
	{
		ArrayList<String> t;
		ArrayList<ArrayList<Filter>> f;
		HashMap<String, Double> r;
		
		if (op instanceof TableScanOperator)
		{
			t = new ArrayList<String>();
			f = new ArrayList<ArrayList<Filter>>();
			r = new HashMap<String, Double>();
		}
		else
		{
			t = tables.get(op);
			f = filters.get(op);
			r = retvals.get(op);
		}
		
		while (true)
		{
			if (op instanceof TableScanOperator)
			{
				t.add(((TableScanOperator)op).getSchema() + "." + ((TableScanOperator)op).getTable());
				//System.out.println("Op is TableScanOperator");
				//System.out.println("Table list is " + t);
			}
			else if (op instanceof SelectOperator)
			{
				ArrayList<Filter> filter = new ArrayList<Filter>(((SelectOperator)op).getFilter());
				f.add(filter);
				//System.out.println("Op is SelectOperator");
				//System.out.println("Filter list is " + f);
			}
			else if (op instanceof RootOperator)
			{
				return null;
			}
			else if (op instanceof MultiOperator)
			{
				//System.out.println("Op is MultiOperator");
				for (String col : ((MultiOperator)op).getOutputCols())
				{
					//System.out.println("Output col: " + col);
					double card;
					Vector<String> keys = ((MultiOperator)op).getKeys();
					if (keys.size() == 1)
					{
						card = this.getCard(keys.get(0), r);
					}
					else if (keys.size() == 0)
					{
						card = 1;
					}
					else
					{
						card = this.getColgroupCard(keys, r);
					}
			
					for (ArrayList<Filter> filter : f)
					{
						if (references(filter, new ArrayList(keys)))
						{
							card *= this.likelihood(filter, r);
						}
					}
					
					r.put(col, card);
				}
				
				//System.out.println("Generated is " + r);
			}
			else if (op instanceof YearOperator)
			{
				double card = 1;
				for (String table : t)
				{
					StringTokenizer tokens = new StringTokenizer(table, ".", false);
					String schema = tokens.nextToken();
					String table2 = tokens.nextToken();
					card *= this.getTableCard(schema, table2);
				}
			
				for (ArrayList<Filter> filter : f)
				{
					card *= this.likelihood(filter, r);
				}	
			
				r.put(((YearOperator)op).getOutputCol(), card);
			//	System.out.println("Operator is YearOperator");
				//System.out.println("Generated is " + r);
			}	
			else if (op instanceof SubstringOperator)
			{
				double card = 1;
				for (String table : t)
				{
					StringTokenizer tokens = new StringTokenizer(table, ".", false);
					String schema = tokens.nextToken();
					String table2 = tokens.nextToken();
					card *= this.getTableCard(schema, table2);
				}
			
				for (ArrayList<Filter> filter : f)
				{
					card *= this.likelihood(filter, r);
				}	
			
				r.put(((SubstringOperator)op).getOutputCol(), card);
				//System.out.println("Operator is SubstringOperator");
				//System.out.println("Generated is " + r);
			}
			else if (op instanceof RenameOperator)
			{
				for (Map.Entry entry : ((RenameOperator)op).getRenameMap().entrySet())
				{
					double card = this.getCard((String)entry.getKey(), r);
					for (ArrayList<Filter> filter : f)
					{
						ArrayList<String> keys = new ArrayList<String>();
						keys.add((String)entry.getKey());
						if (references(filter, keys))
						{
							card *= this.likelihood(filter, r);
						}
					}
				
					r.put((String)entry.getValue(), card);
				}
				
				//System.out.println("Operator is RenameOperator");
				//System.out.println("Generated is " + r);
			}
			else if (op instanceof ExtendOperator)
			{
				double card = 1;
				for (String table : t)
				{
					StringTokenizer tokens = new StringTokenizer(table, ".", false);
					String schema = tokens.nextToken();
					String table2 = tokens.nextToken();
					card *= this.getTableCard(schema, table2);
				}
			
				for (ArrayList<Filter> filter : f)
				{
					card *= this.likelihood(filter, r);
				}
			
				r.put(((ExtendOperator)op).getOutputCol(), card);
				//System.out.println("Operator is ExtendOperator");
				//System.out.println("Generated is " + r);
			}
			else if (op instanceof CaseOperator)
			{
				double card = 1;
				for (String table : t)
				{
					StringTokenizer tokens = new StringTokenizer(table, ".", false);
					String schema = tokens.nextToken();
					String table2 = tokens.nextToken();
					card *= this.getTableCard(schema, table2);
				}
			
				for (ArrayList<Filter> filter : f)
				{
					card *= this.likelihood(filter, r);
				}
			
				r.put(((CaseOperator)op).getOutputCol(), card);
				//System.out.println("Operator is CaseOperator");
				//System.out.println("Generated is " + r);
			}
			
			Operator oldOp = op;
			if (op instanceof TableScanOperator)
			{
				op = ((TableScanOperator)op).parents().get(0);
			}
			else
			{
				op = op.parent();
				if (op == null)
				{
					if (!tables.containsKey(oldOp))
					{
						tables.put(oldOp, t);
						filters.put(oldOp, f);
						retvals.put(oldOp, r);
						return oldOp;
					}
					else
					{
						tables.get(oldOp).addAll(t);
						filters.get(oldOp).addAll(f);
						retvals.get(oldOp).putAll(r);
						return oldOp;
					}
				}
			}
			
			if (op.children().size() > 1)
			{
				if (!tables.containsKey(op))
				{
					tables.put(op, t);
					filters.put(op, f);
					retvals.put(op, r);
					return op;
				}
				else
				{
					tables.get(op).addAll(t);
					filters.get(op).addAll(f);
					retvals.get(op).putAll(r);
				}
				return op;
			}
		}
	}
	
	private boolean references(ArrayList<Filter> filters, ArrayList<String> cols)
	{
		for (Filter filter : filters)
		{
			if (filter.leftIsColumn())
			{
				if (cols.contains(filter.leftColumn()))
				{
					return true;
				}
			}
			
			if (filter.rightIsColumn())
			{
				if (cols.contains(filter.rightColumn()))
				{
					return true;
				}
			}
		}
		
		return false;
	}
	
	private double percentBelow(String col, double val)
	{
		ArrayList<Double> quartiles = getDoubleQuartiles(col);
		if (quartiles == null)
		{
			return 0.5;
		}
		
		if (quartiles.get(4) <= val)
		{
			return 1;
		}
		
		if (quartiles.get(3) <= val)
		{
			return 0.75 + 0.25 * ((val - quartiles.get(3)) / (quartiles.get(4) - quartiles.get(3)));
		}
		
		if (quartiles.get(2) <= val)
		{
			return 0.5 + 0.25 * ((val - quartiles.get(2)) / (quartiles.get(3) - quartiles.get(2)));
		}
		
		if (quartiles.get(1) <= val)
		{
			return 0.25 + 0.25 * ((val - quartiles.get(1)) / (quartiles.get(2) - quartiles.get(1)));
		}
		
		if (quartiles.get(0) <= val)
		{
			return 0.25 * ((val - quartiles.get(0)) / (quartiles.get(1) - quartiles.get(0)));
		}
		
		return 0;
	}
	
	private double percentBelow(String col, String val)
	{
		ArrayList<String> quartiles = getStringQuartiles(col);
		if (quartiles == null)
		{
			return 0.5;
		}
		
		if (quartiles.get(4).compareTo(val) < 1)
		{
			return 1;
		}
		
		if (quartiles.get(3).compareTo(val) < 1)
		{
			return 0.75 + 0.25 * ((stringToLong(val) - stringToLong(quartiles.get(3))) / (stringToLong(quartiles.get(4)) - stringToLong(quartiles.get(3))));
		}
		
		if (quartiles.get(2).compareTo(val) < 1)
		{
			return 0.5 + 0.25 * ((stringToLong(val) - stringToLong(quartiles.get(2))) / (stringToLong(quartiles.get(3)) - stringToLong(quartiles.get(2))));
		}
		
		if (quartiles.get(1).compareTo(val) < 1)
		{
			return 0.25 + 0.25 * ((stringToLong(val) - stringToLong(quartiles.get(1))) / (stringToLong(quartiles.get(2)) - stringToLong(quartiles.get(1))));
		}
		
		if (quartiles.get(0).compareTo(val) < 1)
		{
			return 0.25 * ((stringToLong(val) - stringToLong(quartiles.get(0))) / (stringToLong(quartiles.get(1)) - stringToLong(quartiles.get(0))));
		}
		
		return 0;
	}
	
	private double percentBelow(String col, Date val)
	{
		ArrayList<Date> quartiles = getDateQuartiles(col);
		if (quartiles == null)
		{
			return 0.5;
		}
		
		if (quartiles.get(4).compareTo(val) < 1)
		{
			return 1;
		}
		
		if (quartiles.get(3).compareTo(val) < 1)
		{
			return 0.75 + 0.25 * ((val.getTime() - quartiles.get(3).getTime()) / (quartiles.get(4).getTime() - quartiles.get(3).getTime()));
		}
		
		if (quartiles.get(2).compareTo(val) < 1)
		{
			return 0.5 + 0.25 * ((val.getTime() - quartiles.get(2).getTime()) / (quartiles.get(3).getTime() - quartiles.get(2).getTime()));
		}
		
		if (quartiles.get(1).compareTo(val) < 1)
		{
			return 0.25 + 0.25 * ((val.getTime() - quartiles.get(1).getTime()) / (quartiles.get(2).getTime() - quartiles.get(1).getTime()));
		}
		
		if (quartiles.get(0).compareTo(val) < 1)
		{
			return 0.25 * ((val.getTime() - quartiles.get(0).getTime()) / (quartiles.get(1).getTime() - quartiles.get(0).getTime()));
		}
		
		return 0;
	}
	
	private double percentAbove(String col, double val)
	{
		ArrayList<Double> quartiles = getDoubleQuartiles(col);
		if (quartiles == null)
		{
			return 0.5;
		}
		
		if (quartiles.get(0) >= val)
		{
			return 1;
		}
		
		if (quartiles.get(1) >= val)
		{
			return 0.75 + 0.25 * ((quartiles.get(1) - val) / (quartiles.get(1) - quartiles.get(0)));
		}
		
		if (quartiles.get(2) >= val)
		{
			return 0.5 + 0.25 * ((quartiles.get(2) - val) / (quartiles.get(2) - quartiles.get(1)));
		}
		
		if (quartiles.get(3) >= val)
		{
			return 0.25 + 0.25 * ((quartiles.get(3) - val) / (quartiles.get(3) - quartiles.get(2)));
		}
		
		if (quartiles.get(4) <= val)
		{
			return 0.25 * ((quartiles.get(4) - val) / (quartiles.get(4) - quartiles.get(3)));
		}
		
		return 0;
	}
	
	private double percentAbove(String col, String val)
	{
		ArrayList<String> quartiles = getStringQuartiles(col);
		if (quartiles == null)
		{
			return 0.5;
		}
		
		if (quartiles.get(0).compareTo(val) > -1)
		{
			return 1;
		}
		
		if (quartiles.get(1).compareTo(val) > -1)
		{
			return 0.75 + 0.25 * ((stringToLong(quartiles.get(1)) - stringToLong(val)) / (stringToLong(quartiles.get(1)) - stringToLong(quartiles.get(0))));
		}
		
		if (quartiles.get(2).compareTo(val) > -1)
		{
			return 0.5 + 0.25 * ((stringToLong(quartiles.get(2)) - stringToLong(val)) / (stringToLong(quartiles.get(2)) - stringToLong(quartiles.get(1))));
		}
		
		if (quartiles.get(3).compareTo(val) > -1)
		{
			return 0.25 + 0.25 * ((stringToLong(quartiles.get(3)) - stringToLong(val)) / (stringToLong(quartiles.get(3)) - stringToLong(quartiles.get(2))));
		}
		
		if (quartiles.get(4).compareTo(val) > -1)
		{
			return 0.25 * ((stringToLong(quartiles.get(4)) - stringToLong(val)) / (stringToLong(quartiles.get(4)) - stringToLong(quartiles.get(3))));
		}
		
		return 0;
	}
	
	private double percentAbove(String col, Date val)
	{
		ArrayList<Date> quartiles = getDateQuartiles(col);
		if (quartiles == null)
		{
			return 0.5;
		}
		
		if (quartiles.get(0).compareTo(val) > -1)
		{
			return 1;
		}
		
		if (quartiles.get(1).compareTo(val) > -1)
		{
			return 0.75 + 0.25 * ((quartiles.get(1).getTime() - val.getTime()) / (quartiles.get(1).getTime() - quartiles.get(0).getTime()));
		}
		
		if (quartiles.get(2).compareTo(val) > -1)
		{
			return 0.5 + 0.25 * ((quartiles.get(2).getTime() - val.getTime()) / (quartiles.get(2).getTime() - quartiles.get(1).getTime()));
		}
		
		if (quartiles.get(3).compareTo(val) > -1)
		{
			return 0.25 + 0.25 * ((quartiles.get(3).getTime() - val.getTime()) / (quartiles.get(3).getTime() - quartiles.get(2).getTime()));
		}
		
		if (quartiles.get(4).compareTo(val) > -1)
		{
			return 0.25 * ((quartiles.get(4).getTime() - val.getTime()) / (quartiles.get(4).getTime() - quartiles.get(3).getTime()));
		}
		
		return 0;
	}
	
	private ArrayList<Double> getDoubleQuartiles(String col)
	{
		try
		{
			BufferedReader in = new BufferedReader(new FileReader(new File("dist.tbl")));
			String line = in.readLine();
			while (line != null)
			{
				StringTokenizer tokens = new StringTokenizer(line, "|", false);
				String column = tokens.nextToken();
				if (col.equals(column))
				{
					ArrayList<Double> retval = new ArrayList<Double>();
					retval.add(Double.parseDouble(tokens.nextToken()));
					retval.add(Double.parseDouble(tokens.nextToken()));
					retval.add(Double.parseDouble(tokens.nextToken()));
					retval.add(Double.parseDouble(tokens.nextToken()));
					retval.add(Double.parseDouble(tokens.nextToken()));
					in.close();
					return retval;
				}
			
				line = in.readLine();
			}
			
			in.close();
			return null;
		}
		catch(Exception e)
		{
			e.printStackTrace();
			System.exit(1);
			return null;
		}
	}
	
	private ArrayList<String> getStringQuartiles(String col)
	{
		try
		{
			BufferedReader in = new BufferedReader(new FileReader(new File("dist.tbl")));
			String line = in.readLine();
			while (line != null)
			{
				StringTokenizer tokens = new StringTokenizer(line, "|", false);
				String column = tokens.nextToken();
				if (col.equals(column))
				{
					ArrayList<String> retval = new ArrayList<String>();
					retval.add(tokens.nextToken());
					retval.add(tokens.nextToken());
					retval.add(tokens.nextToken());
					retval.add(tokens.nextToken());
					retval.add(tokens.nextToken());
					in.close();
					return retval;
				}
			
				line = in.readLine();
			}
			
			in.close();
			return null;
		}
		catch(Exception e)
		{
			e.printStackTrace();
			System.exit(1);
			return null;
		}
	}
	
	private ArrayList<Date> getDateQuartiles(String col)
	{
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		
		try
		{
			BufferedReader in = new BufferedReader(new FileReader(new File("dist.tbl")));
			String line = in.readLine();
			while (line != null)
			{
				StringTokenizer tokens = new StringTokenizer(line, "|", false);
				String column = tokens.nextToken();
				if (col.equals(column))
				{
					ArrayList<Date> retval = new ArrayList<Date>();
					retval.add(sdf.parse(tokens.nextToken()));
					retval.add(sdf.parse(tokens.nextToken()));
					retval.add(sdf.parse(tokens.nextToken()));
					retval.add(sdf.parse(tokens.nextToken()));
					retval.add(sdf.parse(tokens.nextToken()));
					in.close();
					return retval;
				}
			
				line = in.readLine();
			}
			
			in.close();
			return null;
		}
		catch(Exception e)
		{
			e.printStackTrace();
			System.exit(1);
			return null;
		}
	}
	
	private long stringToLong(String val)
	{
		int i = 0;
		long retval = 0;
		while (i < 15 && i < val.length())
		{
			int point = val.charAt(i) & 0x0000000F;
			retval += (((long)point) << (56 - (i * 4)));
			i++;
		}
		
		return retval;
	}
	
	public PartitionMetaData getPartMeta(String schema, String table)
	{
		return new PartitionMetaData(schema, table);
	}
	
	public class PartitionMetaData
	{
		private static final int NODEGROUP_NONE = -3;
		private static final int NODE_ANY = -2;
		private static final int NODE_ALL = -1;
		private static final int DEVICE_ALL = -1;
		private ArrayList<Integer> nodeGroupSet;
		private ArrayList<String> nodeGroupHash;
		private ArrayList<Object> nodeGroupRange;
		private int numNodeGroups;
		private String nodeGroupRangeCol;
		private HashMap<Integer, ArrayList<Integer>> nodeGroupHashMap;
		private ArrayList<Integer> nodeSet;
		private int numNodes;
		private ArrayList<String> nodeHash;
		private ArrayList<Object> nodeRange;
		private String nodeRangeCol;
		private int numDevices;
		private ArrayList<Integer> deviceSet;
		private ArrayList<String> deviceHash;
		private ArrayList<Object> deviceRange;
		private String deviceRangeCol;
		private String schema;
		private String table;
		
		public PartitionMetaData(String schema, String table)
		{
			this.schema = schema;
			this.table = table;
			String ngExp = getNodeGroupExpression(schema, table);
			String nExp = getNodeExpression(schema, table);
			String dExp = getDeviceExpression(schema, table);
			setNGData(ngExp);
			setNData(nExp);
			setDData(dExp);
		}
		
		public ArrayList<Object> getDeviceRanges()
		{
			return deviceRange;
		}
		
		public ArrayList<Object> getNodeRanges()
		{
			return nodeRange;
		}
		
		public ArrayList<Object> getNodeGroupRanges()
		{
			return nodeGroupRange;
		}
		
		public boolean noNodeGroupSet()
		{
			return nodeGroupSet.get(0) == NODEGROUP_NONE;
		}
		
		public boolean anyNode()
		{
			return nodeSet.get(0) == NODE_ANY;
		}
		
		public HashMap<Integer, ArrayList<Integer>> getNodeGroupHashMap()
		{
			return nodeGroupHashMap;
		}
		
		public boolean isSingleDeviceSet()
		{
			return numDevices == 1;
		}
		
		public int getSingleDevice()
		{
			if (deviceSet.get(0) == DEVICE_ALL)
			{
				return 0;
			}
			
			return deviceSet.get(0);
		}
		
		public boolean deviceIsHash()
		{
			return deviceHash != null;
		}
		
		public ArrayList<String> getDeviceHash()
		{
			return deviceHash;
		}
		
		public boolean allDevices()
		{
			return deviceSet.get(0) == DEVICE_ALL;
		}
		
		public int getNumDevices()
		{
			return numDevices;
		}
		
		public ArrayList<Integer> deviceSet()
		{
			return deviceSet;
		}
		
		public String getDeviceRangeCol()
		{
			return deviceRangeCol;
		}
		
		public ArrayList<Integer> nodeSet()
		{
			return nodeSet;
		}
		
		public boolean allNodes()
		{
			return nodeSet.get(0) == NODE_ALL;
		}
		
		public String getNodeRangeCol()
		{
			return nodeRangeCol;
		}
		
		public ArrayList<String> getNodeHash()
		{
			return nodeHash;
		}
		
		public boolean nodeIsHash()
		{
			return nodeHash != null;
		}
		
		public boolean isSingleNodeSet()
		{
			return numNodes == 1;
		}
		
		public int getSingleNode()
		{
			return nodeSet.get(0);
		}
		
		public String getNodeGroupRangeCol()
		{
			return nodeGroupRangeCol;
		}
		
		public ArrayList<Integer> nodeGroupSet()
		{
			return nodeGroupSet;
		}
		
		public ArrayList<String> getNodeGroupHash()
		{
			return nodeGroupHash;
		}
		
		public int getNumNodeGroups()
		{
			return numNodeGroups;
		}
		
		public boolean nodeGroupIsHash()
		{
			return nodeGroupHash != null;
		}
		
		public boolean isSingleNodeGroupSet()
		{
			return numNodeGroups == 1;
		}
		
		public int getSingleNodeGroup()
		{
			return nodeGroupSet.get(0);
		}
		
		public int getNumNodes()
		{
			return numNodes;
		}
		
		private void setNGData(String exp)
		{
			if (exp.equals("NONE"))
			{
				nodeGroupSet = new ArrayList<Integer>();
				nodeGroupSet.add(NODEGROUP_NONE);
				return;
			}
			
			StringTokenizer tokens = new StringTokenizer(exp, ",", false);
			String set = tokens.nextToken().substring(1);
			set = set.substring(0, set.length() - 1);
			nodeGroupSet = new ArrayList<Integer>();
			StringTokenizer tokens2 = new StringTokenizer(set, "|", false);
			numNodeGroups = 0;
			while (tokens2.hasMoreTokens())
			{
				nodeGroupSet.add(Integer.parseInt(tokens2.nextToken()));
				numNodeGroups++;
			}
			
			if (numNodeGroups == 1)
			{
				return;
			}
			
			nodeGroupHashMap = new HashMap<Integer, ArrayList<Integer>>();
			for (int groupID : nodeGroupSet)
			{
				nodeGroupHashMap.put(groupID, getNodeListForGroup(groupID));
			}
			
			String type = tokens.nextToken();
			if (type.equals("HASH"))
			{
				set = tokens.nextToken().substring(1);
				set = set.substring(0, set.length() - 1);
				nodeGroupHash = new ArrayList<String>();
				tokens2 = new StringTokenizer(set, "|", false);
				while (tokens2.hasMoreTokens())
				{
					nodeGroupHash.add(tokens2.nextToken());
				}
			}
			else
			{
				nodeGroupRangeCol = tokens.nextToken();
				set = tokens.nextToken().substring(1);
				set = set.substring(0, set.length() - 1);
				nodeGroupRange = convertRangeStringToObject(set, schema, table, nodeGroupRangeCol);
			}
		}
		
		private void setNData(String exp)
		{
			StringTokenizer tokens = new StringTokenizer(exp, ",", false);
			String first = tokens.nextToken();
			
			if (first.equals("ANY"))
			{
				nodeSet = new ArrayList<Integer>();
				nodeSet.add(NODE_ANY);
				return;
			}
			
			if (first.equals("ALL"))
			{
				nodeSet = new ArrayList<Integer>();
				nodeSet.add(NODE_ALL);
				numNodes = MetaData.this.getNumNodes();
			}
			else
			{
				String set = first.substring(1);
				set = set.substring(0, set.length() - 1);
				nodeSet = new ArrayList<Integer>();
				StringTokenizer tokens2 = new StringTokenizer(set, "|", false);
				numNodes = 0;
				while (tokens2.hasMoreTokens())
				{
					nodeSet.add(Integer.parseInt(tokens2.nextToken()));
					numNodes++;
				}
			}
			
			if (numNodes == 1)
			{
				return;
			}
			
			String type = tokens.nextToken();
			if (type.equals("HASH"))
			{
				String set = tokens.nextToken().substring(1);
				set = set.substring(0, set.length() - 1);
				nodeHash = new ArrayList<String>();
				StringTokenizer tokens2 = new StringTokenizer(set, "|", false);
				while (tokens2.hasMoreTokens())
				{
					nodeHash.add(tokens2.nextToken());
				}
			}
			else
			{
				nodeRangeCol = tokens.nextToken();
				String set = tokens.nextToken().substring(1);
				set = set.substring(0, set.length() - 1);
				nodeRange = convertRangeStringToObject(set, schema, table, nodeRangeCol);
			}
		}
		
		private void setDData(String exp)
		{
			StringTokenizer tokens = new StringTokenizer(exp, ",", false);
			String first = tokens.nextToken();
			
			if (first.equals("ALL"))
			{
				deviceSet = new ArrayList<Integer>();
				deviceSet.add(DEVICE_ALL);
				numDevices = MetaData.this.getNumDevices();
			}
			else
			{
				String set = first.substring(1);
				set = set.substring(0, set.length() - 1);
				deviceSet = new ArrayList<Integer>();
				StringTokenizer tokens2 = new StringTokenizer(set, "|", false);
				numDevices = 0;
				while (tokens2.hasMoreTokens())
				{
					deviceSet.add(Integer.parseInt(tokens2.nextToken()));
					numDevices++;
				}
			}
			
			if (numDevices == 1)
			{
				return;
			}
			
			String type = tokens.nextToken();
			if (type.equals("HASH"))
			{
				String set = tokens.nextToken().substring(1);
				set = set.substring(0, set.length() - 1);
				deviceHash = new ArrayList<String>();
				StringTokenizer tokens2 = new StringTokenizer(set, "|", false);
				while (tokens2.hasMoreTokens())
				{
					deviceHash.add(tokens2.nextToken());
				}
			}
			else
			{
				deviceRangeCol = tokens.nextToken();
				String set = tokens.nextToken().substring(1);
				set = set.substring(0, set.length() - 1);
				deviceRange = convertRangeStringToObject(set, schema, table, deviceRangeCol);
			}
		}
	}
	
	private String getNodeGroupExpression(String schema, String table)
	{
		return "NONE";
	}
	
	private String getNodeExpression(String schema, String table)
	{
		if (LOCAL)
		{
			return "{0}";
		}
		
		if (table.equals("NATION") || table.equals("REGION"))
		{
			return "ANY";
		}
		
		if (table.equals("CUSTOMER"))
		{
			return "ALL,HASH,{C_CUSTKEY}";
		}
		
		if (table.equals("ORDERS"))
		{
			return "ALL,HASH,{O_ORDERKEY}";
		}
		
		if (table.equals("LINEITEM"))
		{
			return "ALL,HASH,{L_ORDERKEY}";
		}
		
		if (table.equals("SUPPLIER"))
		{
			return "ALL,HASH,{S_SUPPKEY}";
		}
		
		if (table.equals("PART"))
		{
			return "ALL,HASH,{P_PARTKEY}";
		}
		
		if (table.equals("PARTSUPP"))
		{
			return "ALL,HASH,{PS_PARTKEY}";
		}
		
		return null;
	}
	
	private String getDeviceExpression(String schema, String table)
	{
		if (table.equals("NATION"))
		{
			return "ALL,HASH,{N_NATIONKEY}";
		}
		
		if (table.equals("REGION"))
		{
			return "ALL,HASH,{R_REGIONKEY}";
		}
		
		if (table.equals("CUSTOMER"))
		{
			return "ALL,HASH,{C_NAME}";
		}
		
		if (table.equals("ORDERS"))
		{
			return "ALL,HASH,{O_COMMENT}";
		}
		
		if (table.equals("LINEITEM"))
		{
			return "ALL,HASH,{L_COMMENT}";
		}
		
		if (table.equals("SUPPLIER"))
		{
			return "ALL,HASH,{S_NAME}";
		}
		
		if (table.equals("PART"))
		{
			return "ALL,HASH,{P_NAME}";
		}
		
		if (table.equals("PARTSUPP"))
		{
			return "ALL,HASH,{PS_COMMENT}";
		}
		
		return null;
	}
	
	private ArrayList<Integer> getNodeListForGroup(int group)
	{
		//TODO
		return null;
	}
	
	private ArrayList<Object> convertRangeStringToObject(String set, String schema, String table, String rangeCol)
	{
		//TODO
		return null;
	}
	
	public int getNumNodes()
	{
		return 2;
	}
	
	public int getNumDevices()
	{
		return 4;
	}
	
	public String getHostNameForNode(int node)
	{
		if (node == 0)
		{
			return "192.168.1.3";
		}
		
		if (node == 1)
		{
			return "192.168.1.34";
		}
		
		return null;
	}
	
	public String getDevicePath(int num)
	{
		if (num == 0)
		{
			return "/home/hrdbms/";
		}
		
		if (num == 1)
		{
			return "/temp1/";
		}
		
		if (num == 2)
		{
			return "/temp2/";
		}
		
		if (num == 3)
		{
			return "/temp3/";
		}
		
		return null;
	}
}
