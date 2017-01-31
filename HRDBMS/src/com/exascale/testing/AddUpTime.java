package com.exascale.testing;

import java.io.BufferedReader;
import java.io.FileReader;

public class AddUpTime
{
	public static void main(final String[] args)
	{
		try
		{
			double total = 0.0;
			final BufferedReader in = new BufferedReader(new FileReader(args[0]));
			String line = in.readLine();
			while (line != null)
			{
				if (line.startsWith("Query took "))
				{
					line = line.substring(11);
					final int index = line.indexOf(' ');
					line = line.substring(0, index);
					final double time = Double.parseDouble(line);
					total += time;
				}

				line = in.readLine();
			}

			in.close();
			System.out.println("Total time = " + total);
		}
		catch (final Exception e)
		{
			e.printStackTrace();
		}
	}
}
