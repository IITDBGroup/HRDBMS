package com.exascale.cli;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Scanner;

public class CLI
{
	private static boolean connected = false;
	private static Connection conn;
	
	public static void main(String[] args)
	{
		Scanner in = new Scanner(System.in);
		try
		{
			Class.forName("com.exascale.client.HRDBMSDriver");
		}
		catch(Exception e)
		{
			System.out.println("Unable to load HRDBMS driver!");
			System.exit(1);
		}
		
		while (true)
		{
			System.out.print("HRDBMS> ");
			String cmd = in.nextLine();
			if (cmd.equalsIgnoreCase("QUIT"))
			{
				in.close();
				return;
			}
			
			processCommand(cmd);
		}
	}
	
	private static void processCommand(String cmd)
	{
		if (startsWithIgnoreCase(cmd, "CONNECT TO "))
		{
			connectTo(cmd);
		}
		else if (startsWithIgnoreCase(cmd, "SELECT") || startsWithIgnoreCase(cmd, "WITH"))
		{
			select(cmd);
		}
		else if (cmd.equalsIgnoreCase("COMMIT"))
		{
			commit();
		}
		else if (cmd.equalsIgnoreCase("ROLLBACK"))
		{
			rollback();
		}
		else if (startsWithIgnoreCase(cmd, "LOAD"))
		{
			load(cmd);
		}
		else
		{
			update(cmd);
		}
	}
	
	private static boolean startsWithIgnoreCase(String in, String cmp)
	{
		if (in.toUpperCase().startsWith(cmp.toUpperCase()))
		{
			return true;
		}
		else
		{
			return false;
		}
	}
	
	private static boolean endWithIgnoreCase(String in, String cmp)
	{
		if (in.toUpperCase().endsWith(cmp.toUpperCase()))
		{
			return true;
		}
		else
		{
			return false;
		}
	}
	
	private static void connectTo(String cmd)
	{
		if (connected)
		{
			try
			{
				conn.close();
				connected = false;
			}
			catch(Exception e)
			{
				System.out.println(e.getMessage());
				return;
			}
		}
		
		try
		{
			Properties prop = new Properties();
			if (endWithIgnoreCase(cmd, "FORCE"))
			{
				prop.setProperty("FORCE", "TRUE");
			}
			conn = DriverManager.getConnection(cmd.substring(11), prop);
			conn.setAutoCommit(false);
			connected = true;
		}
		catch(Exception e)
		{
			System.out.println(e.getMessage());
		}
	}
	
	private static void commit()
	{
		try
		{
			conn.commit();
		}
		catch(Exception e)
		{
			System.out.println(e.getMessage());
		}
	}
	
	private static void rollback()
	{
		try
		{
			conn.rollback();
		}
		catch(Exception e)
		{
			System.out.println(e.getMessage());
		}
	}
	
	private static void select(String cmd)
	{
		if (!connected)
		{
			System.out.println("No database connection exists");
			return;
		}
		
		try
		{
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(cmd);
			ResultSetMetaData meta = rs.getMetaData();
			ArrayList<Integer> offsets = new ArrayList<Integer>();
			int i = 1;
			String line = "";
			int colCount = meta.getColumnCount();
			while (i <= colCount)
			{
				offsets.add(line.length());
				line += (meta.getColumnName(i) + "    ");
				i++;
			}
			
			System.out.println(line);
			int len = line.length();
			line = "";
			i = 0;
			while (i < len-4)
			{
				line += '-';
				i++;
			}
		
			System.out.println(line);
			while (rs.next())
			{
				line = "";
				i = 1;
				while (i <= colCount)
				{
					line += " ";
					while (line.length() < offsets.get(i-1))
					{
						line += " ";
					}
					line += (rs.getObject(i));
					i++;
				}
			
				System.out.println(line);
			}
		
			rs.close();
		}
		catch(Exception e)
		{
			System.out.println(e.getMessage());
		}
	}
	
	private static void update(String cmd)
	{
		if (!connected)
		{
			System.out.println("No database connection exists");
			return;
		}
		
		try
		{
			Statement stmt = conn.createStatement();
			int num = stmt.executeUpdate(cmd);
			System.out.println(num + " rows were modified");
		}
		catch(Exception e)
		{
			System.out.println(e.getMessage());
		}
	}
	
	private static void load(String cmd)
	{
		if (!connected)
		{
			System.out.println("No database connection exists");
			return;
		}
		
		try
		{
			Statement stmt = conn.createStatement();
			int num = stmt.executeUpdate(cmd);
			System.out.println(num + " rows were loaded");
		}
		catch(Exception e)
		{
			System.out.println(e.getMessage());
		}
	}
}
