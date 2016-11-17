package com.exascale.testing;

import java.io.BufferedReader;
import java.io.FileReader;

public class AddUpTime
{
	public static void main(String[] args)
	{
		try
		{
			double total = 0.0;
			BufferedReader in = new BufferedReader(new FileReader(args[0]));
			String line = in.readLine();
			while (line != null)
			{
				if (line.startsWith("Query took "))
				{
					line = line.substring(11);
					int index = line.indexOf(' ');
					line = line.substring(0, index);
					double time = Double.parseDouble(line);
					total += time;
				}
				
				line = in.readLine();
			}
			
			in.close();
			System.out.println("Total time = " + total);
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
}
