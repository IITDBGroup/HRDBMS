package com.exascale.optimizer.externalTable;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;	

public class HTTPCsvExternal implements ExternalTableType
{

	public static String HTTP_ADDRESS_KEY = "HttpAddress";

	// fields
	private int pos;
	//This list holds a list of rows that are found in the initialize method
	private List<String[]> rows;

	@Override
	public void initialize(Properties params)
	{
		// key to find the http address in the params object
		String HTTP_ADDRESS = params.getProperty(HTTP_ADDRESS_KEY);
		// list of rows found in the csv file in the http address that should be returned to the rows list.
		List<String[]> rowsToReturn = new ArrayList<String[]>();

		try
		{
			// URL csvFile = new
			// URL("http://hrdbms.esy.es/testfiles/products.csv");
			URL csvFile = new URL(HTTP_ADDRESS);

			BufferedReader in = new BufferedReader(new InputStreamReader(csvFile.openStream()));
			String inputLine;

			String separator = ",";

			/*
			 * This is the parser spliting the data that it found into columns
			 * of a map .
			 */
			while ((inputLine = in.readLine()) != null)
			{
				String[] output = inputLine.split(separator);
				rowsToReturn.add(output);
			}
			in.close();
		}
		catch (Exception e)
		{
			System.out.println(e.getMessage());
		}

		rows = rowsToReturn;
	}

	@Override
	public String[] next()
	{
		if (rows.get(pos) != null)
		{
			return rows.get(pos);
		}
		else
		{
			return null;
		}

	}

	@Override
	public void reset()
	{
		pos = 0;

	}

	@Override
	public boolean hasNext()
	{
		if (rows.get(pos + 1) != null)
		{
			return true;
		}
		else
		{
			return false;
		}

	}

}