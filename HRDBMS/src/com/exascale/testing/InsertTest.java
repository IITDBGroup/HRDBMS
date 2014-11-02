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
		
		stmt.close();
		conn.close();
		long end = System.currentTimeMillis();
		long seconds = (end - start) / 1000;
		long minutes = seconds / 60;
		seconds -= (minutes*60);
		System.out.println("Test took " + minutes + " minutes and " + seconds + " seconds.");
	}
}
