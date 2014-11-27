package com.exascale.testing;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;
import java.util.Scanner;

public class InsertTest
{
private static Connection conn;
	
	public static void main(String[] args) throws Exception
	{
		long start = System.currentTimeMillis();
		Scanner in = new Scanner(System.in);
		Class.forName("com.exascale.client.HRDBMSDriver");
		conn = DriverManager.getConnection("jdbc:hrdbms://172.31.20.103:3232");
		conn.setAutoCommit(false);
		
		Statement stmt = conn.createStatement();
		Random random = new Random();
		int i = 0;
		while (i < 30000)
		{
			stmt.executeUpdate("INSERT INTO JASON.TEST1 VALUES(" + i + ", " + random.nextInt() + ")");
			i++;
			if (i % 100 == 0)
			{
				System.out.println(i);
				
				if (i % 10000 == 0)
				{
					conn.commit();
				}
			}
		}
		
		long end1 = System.currentTimeMillis();
		
		i = 0;
		while (i < 30000)
		{
			stmt.executeUpdate("UPDATE JASON.TEST1 SET COL2 = " + random.nextInt() + " WHERE COL1 = " + i);
			i++;
			if (i % 100 == 0)
			{
				System.out.println(i);
				
				if (i % 10000 == 0)
				{
					conn.commit();
				}
			}
		}
		
		long end2 = System.currentTimeMillis();
		
		i = 0;
		while (i < 30000)
		{
			stmt.executeUpdate("DELETE FROM JASON.TEST1 WHERE COL1 = " + i);
			i++;
			if (i % 100 == 0)
			{
				System.out.println(i);
				
				if (i % 10000 == 0)
				{
					conn.commit();
				}
			}
		}
		
		stmt.close();
		conn.close();
		long end3 = System.currentTimeMillis();
		long seconds1 = (end1 - start) / 1000;
		long minutes1 = seconds1 / 60;
		seconds1 -= (minutes1*60);
		System.out.println("Insert test took " + minutes1 + " minutes and " + seconds1 + " seconds.");
		
		long seconds2 = (end2 - end1) / 1000;
		long minutes2 = seconds2 / 60;
		seconds2 -= (minutes2*60);
		System.out.println("Update test took " + minutes2 + " minutes and " + seconds2 + " seconds.");
		
		long seconds3 = (end3 - end2) / 1000;
		long minutes3 = seconds3 / 60;
		seconds3 -= (minutes3*60);
		System.out.println("Delete test took " + minutes3 + " minutes and " + seconds3 + " seconds.");
	}
}
