package com.exascale.testing;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Random;
import java.util.Scanner;
import com.exascale.client.HRDBMSConnection;

public class InsertTest
{
	private static Connection conn;

	public static void main(String[] args) throws Exception
	{
		int count = Integer.parseInt(args[0]);
		int pacingRate = Integer.parseInt(args[1]);
		int commitRate = Integer.parseInt(args[3]);
		int startNum = Integer.parseInt(args[2]);
		long start = System.currentTimeMillis();
		int batching = Integer.parseInt(args[4]);
		int updateBatching = 1;
		int updatePacing = 1;
		boolean doUpdate = true;
		try
		{
			updateBatching = Integer.parseInt(args[5]);
			updatePacing = Integer.parseInt(args[6]);
		}
		catch(Exception e)
		{
			doUpdate = false;
		}
		int deleteBatching = 1;
		int deletePacing = 1;
		boolean doDelete = true;
		try
		{
			deleteBatching = Integer.parseInt(args[7]);
			deletePacing = Integer.parseInt(args[8]);
		}
		catch(Exception e)
		{
			doDelete = false;
		}

		Class.forName("com.exascale.client.HRDBMSDriver");
		conn = DriverManager.getConnection("jdbc:hrdbms://localhost:3232");
		conn.setAutoCommit(false);
		((HRDBMSConnection)conn).saveDMLResponseTillCommit();

		Statement stmt = conn.createStatement();
		Random random = new Random();
		int i = 0;
		while (i < count)
		{
			StringBuilder sql = new StringBuilder();
			sql.append("INSERT INTO JASON.TEST1 VALUES(" + (i+startNum) + ", " + random.nextInt() + ")");
			i++;
			int j = 1;
			while (j < batching)
			{
				if (i % 100 == 0)
				{
					System.out.println(i);
				}
				
				sql.append(", (" + (i+startNum) + ", " + random.nextInt() + ")");
				j++;
				i++;
			}
			stmt.executeUpdate(sql.toString());
			
			if (i % 100 == 0)
			{
				System.out.println(i);
			}
			
			if (i % commitRate == 0)
			{
				conn.commit();
			}
			else if (i % pacingRate == 0)
			{
				((HRDBMSConnection)conn).doPacing();
			}
		}

		long end1 = System.currentTimeMillis();

		if (doUpdate)
		{
			i = 0;
			while (i < count)
			{
				StringBuilder sql = new StringBuilder();
				sql.append("UPDATE JASON.TEST1 SET COL2 = " + (((i+startNum)/3) % 5) + " WHERE COL1 = " + (i+startNum));
				i++;
				int j = 1;
				while (j < updateBatching)
				{
					if (i % 100 == 0)
					{
						System.out.println(i);
					}
					
					sql.append(" SET COL2 = " + (((i+startNum)/3) % 5) + " WHERE COL1 = " + (i+startNum));
					j++;
					i++;
				}
				stmt.executeUpdate(sql.toString());
				
				if (i % 100 == 0)
				{
					System.out.println(i);
				}
				
				if (i % commitRate == 0)
				{
					conn.commit();
				}
				else if (i % updatePacing == 0)
				{
					((HRDBMSConnection)conn).doPacing();
				}
			}
		}

		long end2 = System.currentTimeMillis();

		
		if (doDelete)
		{
			i = 0;
			while (i < count)
			{
				StringBuilder sql = new StringBuilder();
				sql.append("DELETE FROM JASON.TEST1 WHERE COL1 = " + (i+startNum));
				i++;
				int j = 1;
				while (j < deleteBatching)
				{
					if (i % 100 == 0)
					{
						System.out.println(i);
					}
					
					sql.append(" OR COL1 = " + (i+startNum));
					j++;
					i++;
				}
				stmt.executeUpdate(sql.toString());
				
				if (i % 100 == 0)
				{
					System.out.println(i);
				}
				
				if (i % commitRate == 0)
				{
					conn.commit();
				}
				else if (i % deletePacing == 0)
				{
					((HRDBMSConnection)conn).doPacing();
				}
			}
		}

		stmt.close();
		conn.close();
		long end3 = System.currentTimeMillis();
		long seconds1 = (end1 - start) / 1000;
		long minutes1 = seconds1 / 60;
		seconds1 -= (minutes1 * 60);
		System.out.println("Insert test took " + minutes1 + " minutes and " + seconds1 + " seconds.");

		long seconds2 = (end2 - end1) / 1000;
		long minutes2 = seconds2 / 60;
		seconds2 -= (minutes2 * 60);
		System.out.println("Update test took " + minutes2 + " minutes and " + seconds2 + " seconds.");

		long seconds3 = (end3 - end2) / 1000;
		long minutes3 = seconds3 / 60;
		seconds3 -= (minutes3 * 60);
		System.out.println("Delete test took " + minutes3 + " minutes and " + seconds3 + " seconds.");
		
		if (!doUpdate)
		{
			ArrayList<Integer> results = new ArrayList<Integer>(count);
			conn = DriverManager.getConnection("jdbc:hrdbms://localhost:3232");
			conn.setAutoCommit(false);
			stmt = conn.createStatement();
			start = System.currentTimeMillis();
			i = 0;
			while (i < count)
			{
				StringBuilder sql = new StringBuilder();
				sql.append("SELECT COL2 FROM JASON.TEST1 WHERE COL1 = " + (i+startNum));
				
				ResultSet rs = stmt.executeQuery(sql.toString());
				while (rs.next())
				{
					results.add(rs.getInt(1));
				}
				
				rs.close();
				i++;
				
				if (i % 100 == 0)
				{
					System.out.println(i);
				}
				
				if (i % commitRate == 0)
				{
					conn.commit();
				}
			}
			
			stmt.close();
			conn.close();
			long end = System.currentTimeMillis();
			long seconds = (end - start) / 1000;
			long minutes = seconds / 60;
			seconds -= (minutes * 60);
			System.out.println("Select test took " + minutes + " minutes and " + seconds + " seconds.");
		}
	}
}
