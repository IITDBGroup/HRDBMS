package com.exascale.cli;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;

public class CLI
{
	private static boolean connected = false;
	private static Connection conn;
	private static boolean timing = false;

	public static void main(final String[] args)
	{
		final Scanner in = new Scanner(System.in);
		try
		{
			Class.forName("com.exascale.client.HRDBMSDriver");
		}
		catch (final Exception e)
		{
			System.out.println("Unable to load HRDBMS driver!");
			System.exit(1);
		}

		while (true)
		{
			System.out.print("HRDBMS> ");
			final String cmd = in.nextLine();
			if (cmd.equalsIgnoreCase("QUIT"))
			{
				in.close();
				return;
			}

			processCommand(cmd);
		}
	}

	private static void autocommit() throws SQLException
	{
		conn.setAutoCommit(true);
	}

	private static void commit()
	{
		try
		{
			conn.commit();
		}
		catch (final Exception e)
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
			catch (final Exception e)
			{
				System.out.println(e.getMessage());
				return;
			}
		}

		try
		{
			final Properties prop = new Properties();
			if (endWithIgnoreCase(cmd, "FORCE"))
			{
				prop.setProperty("FORCE", "TRUE");
				cmd = cmd.substring(0, cmd.length() - 6);
			}
			conn = DriverManager.getConnection(cmd.substring(11), prop);
			conn.setAutoCommit(false);
			connected = true;
		}
		catch (final Exception e)
		{
			System.out.println(e.getMessage());
		}
	}

	private static boolean endWithIgnoreCase(final String in, final String cmp)
	{
		if (in.toUpperCase().endsWith(cmp.toUpperCase()))
		{
			return true;
		}

		return false;
	}

	private static void load(final String cmd)
	{
		if (!connected)
		{
			System.out.println("No database connection exists");
			return;
		}

		try
		{
			final Statement stmt = conn.createStatement();
			final int num = stmt.executeUpdate(cmd);
			System.out.println(num + " rows were loaded");
			stmt.close();
		}
		catch (final Exception e)
		{
			System.out.println(e.getMessage());
		}
	}

	private static void processCommand(final String cmd)
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
			catch (final Exception e)
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
		catch (final Exception e)
		{
			System.out.println(e.getMessage());
		}
	}

	private static void select(final String cmd)
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
			final Statement stmt = conn.createStatement();
			start = System.currentTimeMillis();
			final ResultSet rs = stmt.executeQuery(cmd);
			final ResultSetMetaData meta = rs.getMetaData();
			final List<Integer> offsets = new ArrayList<Integer>();
			int i = 1;
			final StringBuilder line = new StringBuilder(64 * 1024);
			final int colCount = meta.getColumnCount();
			while (i <= colCount)
			{
				offsets.add(line.length());
				line.append((meta.getColumnName(i) + "    "));
				i++;
			}

			System.out.println(line);
			final int len = line.length();
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
				final int s = line.length();
				while (i <= colCount)
				{
					line.append(" ");
					final int target = s + offsets.get(i - 1);
					final int x = target - line.length();
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
			stmt.close();

			if (timing)
			{
				System.out.println("");
				System.out.println("Query took " + (end - start) / 1000.0 + " seconds");
			}
		}
		catch (final Exception e)
		{
			System.out.println(e.getMessage());
		}
	}

	private static boolean startsWithIgnoreCase(final String in, final String cmp)
	{
		if (in.toUpperCase().startsWith(cmp.toUpperCase()))
		{
			return true;
		}

		return false;
	}

	private static void update(final String cmd)
	{
		if (!connected)
		{
			System.out.println("No database connection exists");
			return;
		}

		try
		{
			final Statement stmt = conn.createStatement();
			final int num = stmt.executeUpdate(cmd);
			System.out.println(num + " rows were modified");
			stmt.close();
		}
		catch (final Exception e)
		{
			System.out.println(e.getMessage());
		}
	}
}
