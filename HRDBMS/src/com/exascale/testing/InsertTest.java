package com.exascale.testing;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
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

		//i = 0;
		//while (i < count)
		//{
		//	stmt.executeUpdate("UPDATE JASON.TEST1 SET COL2 = " + random.nextInt() + " WHERE COL1 = " + i);
		//	i++;
		//	if (i % 100 == 0)
		//	{
		//		System.out.println(i);
		//	}
		
		//	if (i % commitRate == 0)
		//	{
		//		conn.commit();
		//	}
		//}

		long end2 = System.currentTimeMillis();

		//i = 0;
		//while (i < count)
		//{
		//	stmt.executeUpdate("DELETE FROM JASON.TEST1 WHERE COL1 = " + i);
		//	i++;
		//	if (i % 100 == 0)
		//	{
		//		System.out.println(i);
		//	}
		
		//	if (i % commitRate == 0)
		//	{
		//		conn.commit();
		//	}
		//}

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
	}
}
