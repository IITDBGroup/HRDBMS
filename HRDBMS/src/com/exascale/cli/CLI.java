package com.exascale.cli;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Scanner;

public class CLI
{
	private static boolean connected = false;
	private static Connection conn;
	private static boolean timing = false;

	public static void main(String[] args)
	{
		Scanner in = new Scanner(System.in);
		try
		{
			Class.forName("com.exascale.client.HRDBMSDriver");
		}
		catch (Exception e)
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

	private static void commit()
	{
		try
		{
			conn.commit();
		}
		catch (Exception e)
		{
			System.out.println(e.getMessage());
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
			catch (Exception e)
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
				cmd = cmd.substring(0, cmd.length() - 6);
			}
			conn = DriverManager.getConnection(cmd.substring(11), prop);
			conn.setAutoCommit(false);
			connected = true;
		}
		catch (Exception e)
		{
			System.out.println(e.getMessage());
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
		catch (Exception e)
		{
			System.out.println(e.getMessage());
		}
	}

	private static void processCommand(String cmd)
	{
		if (cmd.trim().equals(""))
		{
			return;
		}
		if (startsWithIgnoreCase(cmd, "CONNECT TO "))
		{
			connectTo(cmd);
		}
		else if (startsWithIgnoreCase(cmd, "SELECT") || startsWithIgnoreCase(cmd, "WITH"))
		{
			select(cmd);
		}
		else if (cmd.equalsIgnoreCase("TIMING ON"))
		{
			timing = true;
		}
		else if (cmd.equalsIgnoreCase("TIMING OFF"))
		{
			timing = false;
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
		else if (cmd.equalsIgnoreCase("SET AUTOCOMMIT"))
		{
			try
			{
				autocommit();
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
		}
		else
		{
			update(cmd);
		}
	}

	private static void rollback()
	{
		try
		{
			conn.rollback();
		}
		catch (Exception e)
		{
			System.out.println(e.getMessage());
		}
	}
	
	private static void autocommit() throws SQLException
	{
		conn.setAutoCommit(true);
	}

	private static void select(String cmd)
	{
		long start = 0;
		long end = 0;
		if (!connected)
		{
			System.out.println("No database connection exists");
			return;
		}

		try
		{
			Statement stmt = conn.createStatement();
			start = System.currentTimeMillis();
			ResultSet rs = stmt.executeQuery(cmd);
			ResultSetMetaData meta = rs.getMetaData();
			ArrayList<Integer> offsets = new ArrayList<Integer>();
			int i = 1;
			StringBuilder line = new StringBuilder(64 * 1024);
			int colCount = meta.getColumnCount();
			while (i <= colCount)
			{
				offsets.add(line.length());
				line.append((meta.getColumnName(i) + "    "));
				i++;
			}

			System.out.println(line);
			int len = line.length();
			line.setLength(0);
			i = 0;
			while (i < len - 4)
			{
				line.append('-');
				i++;
			}
			System.out.println(line);
			line.setLength(0);
			while (rs.next())
			{
				i = 1;
				int s = line.length();
				while (i <= colCount)
				{
					line.append(" ");
					int target = s + offsets.get(i - 1);
					int x = target - line.length();
					int y = 0;
					while (y < x)
					{
						line.append(" ");
						y++;
					}
					line.append((rs.getObject(i)));
					i++;
				}

				if (line.length() >= 32 * 1024)
				{
					System.out.println(line);
					line.setLength(0);
				}
				else
				{
					line.append("\n");
				}
			}

			if (line.length() != 0)
			{
				System.out.println(line);
			}

			end = System.currentTimeMillis();
			rs.close();

			if (timing)
			{
				System.out.println("");
				System.out.println("Query took " + (end - start) / 1000.0 + " seconds");
			}
		}
		catch (Exception e)
		{
			System.out.println(e.getMessage());
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
		catch (Exception e)
		{
			System.out.println(e.getMessage());
		}
	}
}
