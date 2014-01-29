package com.exascale.optimizer.testing;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.ArrayList;
import java.util.ArrayList;

public class BuildDistTable 
{
	protected static int x = 0;
	protected static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	
	public static void main(String[] args) throws Exception
	{
		new ResourceManager().start();
		PrintWriter out = new PrintWriter(new FileWriter("./dist.tbl"));
		MetaData meta = new MetaData();
		
		TreeMap<Integer, String> pos2Col = meta.getPos2ColForTable("TPCH",  "CUSTOMER");
		for (String col : pos2Col.values())
		{
			Operator temp = new TableScanOperator("TPCH", "CUSTOMER", meta);
			ArrayList<String> vStr = new ArrayList<String>();
			vStr.add(col);
			ArrayList<Boolean> vBool = new ArrayList<Boolean>();
			vBool.add(true);
			Operator temp2 = new SortOperator(vStr, vBool, meta);
			temp2.add(temp);
			RootOperator root = new RootOperator(meta);
			root.add(temp2);
			Driver.phase1(root);
			root.start();
			while (!((SortOperator)temp2).done())
			{
				Thread.sleep(1);
			}
			long total = ((SortOperator)temp2).size();
			long i = 0;
			
			Object val0 = ((ArrayList<Object>)root.next()).get(0);
			i++;
			
			while (i < (total / 4) - 1)
			{
				root.next();
				i++;
			}
			
			Object val1 = ((ArrayList<Object>)root.next()).get(0);
			i++;
			
			while (i < (total / 2) - 1)
			{
				root.next();
				i++;
			}
			
			Object val2 = ((ArrayList<Object>)root.next()).get(0);
			i++;
			
			while (i < (total * 3 / 4) - 1)
			{
				root.next();
				i++;
			}
			
			Object val3 = ((ArrayList<Object>)root.next()).get(0);
			i++;
			
			while (i < total - 1)
			{
				root.next();
				i++;
			}
			
			Object val4 = ((ArrayList<Object>)root.next()).get(0);
			i++;
			
			root.next();
			root.close();
			
			if (val0 instanceof Date)
			{
				out.write(col + "|" + sdf.format(val0) + "|" + sdf.format(val1) + "|" + sdf.format(val2) + "|" + sdf.format(val3) + "|" + sdf.format(val4) + "\n");
			}
			else
			{
				out.write(col + "|" + val0.toString() + "|" + val1.toString() + "|" + val2.toString() + "|" + val3.toString() + "|" + val4.toString() + "\n");
			}
			x++;
			System.out.println("Completed " + x + "/61");
		}
		
		pos2Col = meta.getPos2ColForTable("TPCH",  "ORDERS");
		for (String col : pos2Col.values())
		{
			Operator temp = new TableScanOperator("TPCH", "ORDERS", meta);
			ArrayList<String> vStr = new ArrayList<String>();
			vStr.add(col);
			ArrayList<Boolean> vBool = new ArrayList<Boolean>();
			vBool.add(true);
			Operator temp2 = new SortOperator(vStr, vBool, meta);
			temp2.add(temp);
			RootOperator root = new RootOperator(meta);
			root.add(temp2);
			Driver.phase1(root);
			root.start();
			while (!((SortOperator)temp2).done())
			{
				Thread.sleep(1);
			}
			long total = ((SortOperator)temp2).size();
			long i = 0;
			
			Object val0 = ((ArrayList<Object>)root.next()).get(0);
			i++;
			
			while (i < (total / 4) - 1)
			{
				root.next();
				i++;
			}
			
			Object val1 = ((ArrayList<Object>)root.next()).get(0);
			i++;
			
			while (i < (total / 2) - 1)
			{
				root.next();
				i++;
			}
			
			Object val2 = ((ArrayList<Object>)root.next()).get(0);
			i++;
			
			while (i < (total * 3 / 4) - 1)
			{
				root.next();
				i++;
			}
			
			Object val3 = ((ArrayList<Object>)root.next()).get(0);
			i++;
			
			while (i < total - 1)
			{
				root.next();
				i++;
			}
			
			Object val4 = ((ArrayList<Object>)root.next()).get(0);
			i++;
			
			root.next();
			root.close();
			
			if (val0 instanceof Date)
			{
				out.write(col + "|" + sdf.format(val0) + "|" + sdf.format(val1) + "|" + sdf.format(val2) + "|" + sdf.format(val3) + "|" + sdf.format(val4) + "\n");
			}
			else
			{
				out.write(col + "|" + val0.toString() + "|" + val1.toString() + "|" + val2.toString() + "|" + val3.toString() + "|" + val4.toString() + "\n");
			}
			x++;
			System.out.println("Completed " + x + "/61");
		}
		
		pos2Col = meta.getPos2ColForTable("TPCH",  "LINEITEM");
		for (String col : pos2Col.values())
		{
			Operator temp = new TableScanOperator("TPCH", "LINEITEM", meta);
			ArrayList<String> vStr = new ArrayList<String>();
			vStr.add(col);
			ArrayList<Boolean> vBool = new ArrayList<Boolean>();
			vBool.add(true);
			Operator temp2 = new SortOperator(vStr, vBool, meta);
			temp2.add(temp);
			RootOperator root = new RootOperator(meta);
			root.add(temp2);
			Driver.phase1(root);
			root.start();
			while (!((SortOperator)temp2).done())
			{
				Thread.sleep(1);
			}
			long total = ((SortOperator)temp2).size();
			long i = 0;
			
			Object val0 = ((ArrayList<Object>)root.next()).get(0);
			i++;
			
			while (i < (total / 4) - 1)
			{
				root.next();
				i++;
			}
			
			Object val1 = ((ArrayList<Object>)root.next()).get(0);
			i++;
			
			while (i < (total / 2) - 1)
			{
				root.next();
				i++;
			}
			
			Object val2 = ((ArrayList<Object>)root.next()).get(0);
			i++;
			
			while (i < (total * 3 / 4) - 1)
			{
				root.next();
				i++;
			}
			
			Object val3 = ((ArrayList<Object>)root.next()).get(0);
			i++;
			
			while (i < total - 1)
			{
				root.next();
				i++;
			}
			
			Object val4 = ((ArrayList<Object>)root.next()).get(0);
			i++;
			
			root.next();
			root.close();
			
			if (val0 instanceof Date)
			{
				out.write(col + "|" + sdf.format(val0) + "|" + sdf.format(val1) + "|" + sdf.format(val2) + "|" + sdf.format(val3) + "|" + sdf.format(val4) + "\n");
			}
			else
			{
				out.write(col + "|" + val0.toString() + "|" + val1.toString() + "|" + val2.toString() + "|" + val3.toString() + "|" + val4.toString() + "\n");
			}
			x++;
			System.out.println("Completed " + x + "/61");
		}
		
		pos2Col = meta.getPos2ColForTable("TPCH",  "SUPPLIER");
		for (String col : pos2Col.values())
		{
			Operator temp = new TableScanOperator("TPCH", "SUPPLIER", meta);
			ArrayList<String> vStr = new ArrayList<String>();
			vStr.add(col);
			ArrayList<Boolean> vBool = new ArrayList<Boolean>();
			vBool.add(true);
			Operator temp2 = new SortOperator(vStr, vBool, meta);
			temp2.add(temp);
			RootOperator root = new RootOperator(meta);
			root.add(temp2);
			Driver.phase1(root);
			root.start();
			while (!((SortOperator)temp2).done())
			{
				Thread.sleep(1);
			}
			long total = ((SortOperator)temp2).size();
			long i = 0;
			
			Object val0 = ((ArrayList<Object>)root.next()).get(0);
			i++;
			
			while (i < (total / 4) - 1)
			{
				root.next();
				i++;
			}
			
			Object val1 = ((ArrayList<Object>)root.next()).get(0);
			i++;
			
			while (i < (total / 2) - 1)
			{
				root.next();
				i++;
			}
			
			Object val2 = ((ArrayList<Object>)root.next()).get(0);
			i++;
			
			while (i < (total * 3 / 4) - 1)
			{
				root.next();
				i++;
			}
			
			Object val3 = ((ArrayList<Object>)root.next()).get(0);
			i++;
			
			while (i < total - 1)
			{
				root.next();
				i++;
			}
			
			Object val4 = ((ArrayList<Object>)root.next()).get(0);
			i++;
			
			root.next();
			root.close();
			
			if (val0 instanceof Date)
			{
				out.write(col + "|" + sdf.format(val0) + "|" + sdf.format(val1) + "|" + sdf.format(val2) + "|" + sdf.format(val3) + "|" + sdf.format(val4) + "\n");
			}
			else
			{
				out.write(col + "|" + val0.toString() + "|" + val1.toString() + "|" + val2.toString() + "|" + val3.toString() + "|" + val4.toString() + "\n");
			}
			x++;
			System.out.println("Completed " + x + "/61");
		}
		
		pos2Col = meta.getPos2ColForTable("TPCH",  "PART");
		for (String col : pos2Col.values())
		{
			Operator temp = new TableScanOperator("TPCH", "PART", meta);
			ArrayList<String> vStr = new ArrayList<String>();
			vStr.add(col);
			ArrayList<Boolean> vBool = new ArrayList<Boolean>();
			vBool.add(true);
			Operator temp2 = new SortOperator(vStr, vBool, meta);
			temp2.add(temp);
			RootOperator root = new RootOperator(meta);
			root.add(temp2);
			Driver.phase1(root);
			root.start();
			while (!((SortOperator)temp2).done())
			{
				Thread.sleep(1);
			}
			long total = ((SortOperator)temp2).size();
			long i = 0;
			
			Object val0 = ((ArrayList<Object>)root.next()).get(0);
			i++;
			
			while (i < (total / 4) - 1)
			{
				root.next();
				i++;
			}
			
			Object val1 = ((ArrayList<Object>)root.next()).get(0);
			i++;
			
			while (i < (total / 2) - 1)
			{
				root.next();
				i++;
			}
			
			Object val2 = ((ArrayList<Object>)root.next()).get(0);
			i++;
			
			while (i < (total * 3 / 4) - 1)
			{
				root.next();
				i++;
			}
			
			Object val3 = ((ArrayList<Object>)root.next()).get(0);
			i++;
			
			while (i < total - 1)
			{
				root.next();
				i++;
			}
			
			Object val4 = ((ArrayList<Object>)root.next()).get(0);
			i++;
			
			root.next();
			root.close();
			
			if (val0 instanceof Date)
			{
				out.write(col + "|" + sdf.format(val0) + "|" + sdf.format(val1) + "|" + sdf.format(val2) + "|" + sdf.format(val3) + "|" + sdf.format(val4) + "\n");
			}
			else
			{
				out.write(col + "|" + val0.toString() + "|" + val1.toString() + "|" + val2.toString() + "|" + val3.toString() + "|" + val4.toString() + "\n");
			}
			x++;
			System.out.println("Completed " + x + "/61");
		}
		
		pos2Col = meta.getPos2ColForTable("TPCH",  "PARTSUPP");
		for (String col : pos2Col.values())
		{
			Operator temp = new TableScanOperator("TPCH", "PARTSUPP", meta);
			ArrayList<String> vStr = new ArrayList<String>();
			vStr.add(col);
			ArrayList<Boolean> vBool = new ArrayList<Boolean>();
			vBool.add(true);
			Operator temp2 = new SortOperator(vStr, vBool, meta);
			temp2.add(temp);
			RootOperator root = new RootOperator(meta);
			root.add(temp2);
			Driver.phase1(root);
			root.start();
			while (!((SortOperator)temp2).done())
			{
				Thread.sleep(1);
			}
			long total = ((SortOperator)temp2).size();
			long i = 0;
			
			Object val0 = ((ArrayList<Object>)root.next()).get(0);
			i++;
			
			while (i < (total / 4) - 1)
			{
				root.next();
				i++;
			}
			
			Object val1 = ((ArrayList<Object>)root.next()).get(0);
			i++;
			
			while (i < (total / 2) - 1)
			{
				root.next();
				i++;
			}
			
			Object val2 = ((ArrayList<Object>)root.next()).get(0);
			i++;
			
			while (i < (total * 3 / 4) - 1)
			{
				root.next();
				i++;
			}
			
			Object val3 = ((ArrayList<Object>)root.next()).get(0);
			i++;
			
			while (i < total - 1)
			{
				root.next();
				i++;
			}
			
			Object val4 = ((ArrayList<Object>)root.next()).get(0);
			i++;
			
			root.next();
			root.close();
			
			if (val0 instanceof Date)
			{
				out.write(col + "|" + sdf.format(val0) + "|" + sdf.format(val1) + "|" + sdf.format(val2) + "|" + sdf.format(val3) + "|" + sdf.format(val4) + "\n");
			}
			else
			{
				out.write(col + "|" + val0.toString() + "|" + val1.toString() + "|" + val2.toString() + "|" + val3.toString() + "|" + val4.toString() + "\n");
			}
			x++;
			System.out.println("Completed " + x + "/61");
		}
		
		pos2Col = meta.getPos2ColForTable("TPCH",  "NATION");
		for (String col : pos2Col.values())
		{
			Operator temp = new TableScanOperator("TPCH", "NATION", meta);
			ArrayList<String> vStr = new ArrayList<String>();
			vStr.add(col);
			ArrayList<Boolean> vBool = new ArrayList<Boolean>();
			vBool.add(true);
			Operator temp2 = new SortOperator(vStr, vBool, meta);
			temp2.add(temp);
			RootOperator root = new RootOperator(meta);
			root.add(temp2);
			Driver.phase1(root);
			root.start();
			while (!((SortOperator)temp2).done())
			{
				Thread.sleep(1);
			}
			long total = ((SortOperator)temp2).size();
			long i = 0;
			
			Object val0 = ((ArrayList<Object>)root.next()).get(0);
			i++;
			
			while (i < (total / 4) - 1)
			{
				root.next();
				i++;
			}
			
			Object val1 = ((ArrayList<Object>)root.next()).get(0);
			i++;
			
			while (i < (total / 2) - 1)
			{
				root.next();
				i++;
			}
			
			Object val2 = ((ArrayList<Object>)root.next()).get(0);
			i++;
			
			while (i < (total * 3 / 4) - 1)
			{
				root.next();
				i++;
			}
			
			Object val3 = ((ArrayList<Object>)root.next()).get(0);
			i++;
			
			while (i < total - 1)
			{
				root.next();
				i++;
			}
			
			Object val4 = ((ArrayList<Object>)root.next()).get(0);
			i++;
			
			root.next();
			root.close();
			
			if (val0 instanceof Date)
			{
				out.write(col + "|" + sdf.format(val0) + "|" + sdf.format(val1) + "|" + sdf.format(val2) + "|" + sdf.format(val3) + "|" + sdf.format(val4) + "\n");
			}
			else
			{
				out.write(col + "|" + val0.toString() + "|" + val1.toString() + "|" + val2.toString() + "|" + val3.toString() + "|" + val4.toString() + "\n");
			}
			x++;
			System.out.println("Completed " + x + "/61");
		}
		
		pos2Col = meta.getPos2ColForTable("TPCH",  "REGION");
		for (String col : pos2Col.values())
		{
			Operator temp = new TableScanOperator("TPCH", "REGION", meta);
			ArrayList<String> vStr = new ArrayList<String>();
			vStr.add(col);
			ArrayList<Boolean> vBool = new ArrayList<Boolean>();
			vBool.add(true);
			Operator temp2 = new SortOperator(vStr, vBool, meta);
			temp2.add(temp);
			RootOperator root = new RootOperator(meta);
			root.add(temp2);
			Driver.phase1(root);
			root.start();
			while (!((SortOperator)temp2).done())
			{
				Thread.sleep(1);
			}
			long total = ((SortOperator)temp2).size();
			long i = 0;
			
			Object val0 = ((ArrayList<Object>)root.next()).get(0);
			i++;
			
			while (i < (total / 4) - 1)
			{
				root.next();
				i++;
			}
			
			Object val1 = ((ArrayList<Object>)root.next()).get(0);
			i++;
			
			while (i < (total / 2) - 1)
			{
				root.next();
				i++;
			}
			
			Object val2 = ((ArrayList<Object>)root.next()).get(0);
			i++;
			
			while (i < (total * 3 / 4) - 1)
			{
				root.next();
				i++;
			}
			
			Object val3 = ((ArrayList<Object>)root.next()).get(0);
			i++;
			
			while (i < total - 1)
			{
				root.next();
				i++;
			}
			
			Object val4 = ((ArrayList<Object>)root.next()).get(0);
			i++;
			
			root.next();
			root.close();
			
			if (val0 instanceof Date)
			{
				out.write(col + "|" + sdf.format(val0) + "|" + sdf.format(val1) + "|" + sdf.format(val2) + "|" + sdf.format(val3) + "|" + sdf.format(val4) + "\n");
			}
			else
			{
				out.write(col + "|" + val0.toString() + "|" + val1.toString() + "|" + val2.toString() + "|" + val3.toString() + "|" + val4.toString() + "\n");
			}
			x++;
			System.out.println("Completed " + x + "/61");
		}
		
		out.close();
		Thread.sleep(90000);
		System.exit(0);
	}
}
