package com.exascale.optimizer.testing;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.StringTokenizer;
import java.util.TreeMap;

public class BuildCardTable 
{
	public static void main(String[] args) throws Exception
	{
		PrintWriter out = new PrintWriter(new FileWriter("./card.tbl"));
		MetaData meta = new MetaData();
		
		BufferedReader in = new BufferedReader(new FileReader("./customer.tbl"));
		TreeMap<Integer, String> pos2Col = meta.getPos2ColForTable("TPCH",  "CUSTOMER");
		HashMap<Integer, HashSet<String>> valsForCol = new HashMap<Integer, HashSet<String>>();
		int i = 0;
		while (i < pos2Col.size())
		{
			valsForCol.put(i, new HashSet<String>());
			i++;
		}
		String line = in.readLine();
		while (line != null)
		{
			StringTokenizer tokens = new StringTokenizer(line, "|", false);
			i = 0;
			while (tokens.hasMoreTokens())
			{
				valsForCol.get(i).add(tokens.nextToken());
				i++;
			}
			line = in.readLine();
		}
		String outLine = "";
		i = 0;
		while (i < pos2Col.size())
		{
			outLine = pos2Col.get(i);
			outLine += ("|" + valsForCol.get(i).size() + "\n");
			out.write(outLine);
			outLine = "";
			i++;
		}
		in.close();
		
		in = new BufferedReader(new FileReader("./part.tbl"));
		pos2Col = meta.getPos2ColForTable("TPCH", "PART");
		valsForCol = new HashMap<Integer, HashSet<String>>();
		i = 0;
		while (i < pos2Col.size())
		{
			valsForCol.put(i, new HashSet<String>());
			i++;
		}
		line = in.readLine();
		while (line != null)
		{
			StringTokenizer tokens = new StringTokenizer(line, "|", false);
			i = 0;
			while (tokens.hasMoreTokens())
			{
				valsForCol.get(i).add(tokens.nextToken());
				i++;
			}
			line = in.readLine();
		}
		outLine = "";
		i = 0;
		while (i < pos2Col.size())
		{
			outLine = pos2Col.get(i);
			outLine += ("|" + valsForCol.get(i).size() + "\n");
			out.write(outLine);
			outLine = "";
			i++;
		}
		in.close();
		
		in = new BufferedReader(new FileReader("./supplier.tbl"));
		pos2Col = meta.getPos2ColForTable("TPCH", "SUPPLIER");
		valsForCol = new HashMap<Integer, HashSet<String>>();
		i = 0;
		while (i < pos2Col.size())
		{
			valsForCol.put(i, new HashSet<String>());
			i++;
		}
		line = in.readLine();
		while (line != null)
		{
			StringTokenizer tokens = new StringTokenizer(line, "|", false);
			i = 0;
			while (tokens.hasMoreTokens())
			{
				valsForCol.get(i).add(tokens.nextToken());
				i++;
			}
			line = in.readLine();
		}
		outLine = "";
		i = 0;
		while (i < pos2Col.size())
		{
			outLine = pos2Col.get(i);
			outLine += ("|" + valsForCol.get(i).size() + "\n");
			out.write(outLine);
			outLine = "";
			i++;
		}
		in.close();
		
		in = new BufferedReader(new FileReader("./partsupp.tbl"));
		pos2Col = meta.getPos2ColForTable("TPCH", "PARTSUPP");
		valsForCol = new HashMap<Integer, HashSet<String>>();
		i = 0;
		while (i < pos2Col.size())
		{
			valsForCol.put(i, new HashSet<String>());
			i++;
		}
		line = in.readLine();
		while (line != null)
		{
			StringTokenizer tokens = new StringTokenizer(line, "|", false);
			i = 0;
			while (tokens.hasMoreTokens())
			{
				valsForCol.get(i).add(tokens.nextToken());
				i++;
			}
			line = in.readLine();
		}
		outLine = "";
		i = 0;
		while (i < pos2Col.size())
		{
			outLine = pos2Col.get(i);
			outLine += ("|" + valsForCol.get(i).size() + "\n");
			out.write(outLine);
			outLine = "";
			i++;
		}
		in.close();
		
		in = new BufferedReader(new FileReader("./orders.tbl"));
		pos2Col = meta.getPos2ColForTable("TPCH", "ORDERS");
		valsForCol = new HashMap<Integer, HashSet<String>>();
		i = 0;
		while (i < pos2Col.size())
		{
			valsForCol.put(i, new HashSet<String>());
			i++;
		}
		line = in.readLine();
		while (line != null)
		{
			StringTokenizer tokens = new StringTokenizer(line, "|", false);
			i = 0;
			while (tokens.hasMoreTokens())
			{
				valsForCol.get(i).add(tokens.nextToken());
				i++;
			}
			line = in.readLine();
		}
		outLine = "";
		i = 0;
		while (i < pos2Col.size())
		{
			outLine = pos2Col.get(i);
			outLine += ("|" + valsForCol.get(i).size() + "\n");
			out.write(outLine);
			outLine = "";
			i++;
		}
		in.close();
		
		in = new BufferedReader(new FileReader("./lineitem.tbl"));
		pos2Col = meta.getPos2ColForTable("TPCH", "LINEITEM");
		valsForCol = new HashMap<Integer, HashSet<String>>();
		i = 0;
		while (i < pos2Col.size())
		{
			valsForCol.put(i, new HashSet<String>());
			i++;
		}
		line = in.readLine();
		while (line != null)
		{
			StringTokenizer tokens = new StringTokenizer(line, "|", false);
			i = 0;
			while (tokens.hasMoreTokens())
			{
				valsForCol.get(i).add(tokens.nextToken());
				i++;
			}
			line = in.readLine();
		}
		outLine = "";
		i = 0;
		while (i < pos2Col.size())
		{
			outLine = pos2Col.get(i);
			outLine += ("|" + valsForCol.get(i).size() + "\n");
			out.write(outLine);
			outLine = "";
			i++;
		}
		in.close();
		
		in = new BufferedReader(new FileReader("./nation.tbl"));
		pos2Col = meta.getPos2ColForTable("TPCH", "NATION");
		valsForCol = new HashMap<Integer, HashSet<String>>();
		i = 0;
		while (i < pos2Col.size())
		{
			valsForCol.put(i, new HashSet<String>());
			i++;
		}
		line = in.readLine();
		while (line != null)
		{
			StringTokenizer tokens = new StringTokenizer(line, "|", false);
			i = 0;
			while (tokens.hasMoreTokens())
			{
				valsForCol.get(i).add(tokens.nextToken());
				i++;
			}
			line = in.readLine();
		}
		outLine = "";
		i = 0;
		while (i < pos2Col.size())
		{
			outLine = pos2Col.get(i);
			outLine += ("|" + valsForCol.get(i).size() + "\n");
			out.write(outLine);
			outLine = "";
			i++;
		}
		in.close();
		
		in = new BufferedReader(new FileReader("./region.tbl"));
		pos2Col = meta.getPos2ColForTable("TPCH", "REGION");
		valsForCol = new HashMap<Integer, HashSet<String>>();
		i = 0;
		while (i < pos2Col.size())
		{
			valsForCol.put(i, new HashSet<String>());
			i++;
		}
		line = in.readLine();
		while (line != null)
		{
			StringTokenizer tokens = new StringTokenizer(line, "|", false);
			i = 0;
			while (tokens.hasMoreTokens())
			{
				valsForCol.get(i).add(tokens.nextToken());
				i++;
			}
			line = in.readLine();
		}
		outLine = "";
		i = 0;
		while (i < pos2Col.size())
		{
			outLine = pos2Col.get(i);
			outLine += ("|" + valsForCol.get(i).size() + "\n");
			out.write(outLine);
			outLine = "";
			i++;
		}
		in.close();
		out.close();
		
	}
}
