package com.exascale.optimizer.externalTable;

import javax.lang.model.element.Element;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.*;

public class HTTPCsvExternal implements ExternalTableType
{

	public static String HTTP_ADDRESS_KEY = "HttpAddress";
	public static String FIELD_DELIMITER  = "fieldDelimiter";

	/**
	 * List of acceptable parameters for HTTPCsvExternal class that can be passed through command line
	 */
	private static String[] paramList = {FIELD_DELIMITER, HTTP_ADDRESS_KEY};

	/**
	 * List of required parameters for HTTPCsvExternal class to be passed through command line
	 */
	private static String[] requiredParams = {HTTP_ADDRESS_KEY};


	// fields
	private int pos;
	//This list holds a list of rows that are found in the initialize method
	private List<ArrayList> rows;

	/**
	 * Method checks if parameters entered from CLI are sufficient to create metadata about external table
	 *
	 * @param params
	 */
	static public void validateProperties(Properties params)
	{
		Enumeration e = params.propertyNames();

		String[] reqParams = new String[params.size()];
		int i = 0;
		while (e.hasMoreElements()) {
			String key = (String) e.nextElement();
			reqParams[i++] = key;
			if (!Arrays.asList(paramList).contains(key)) {
				throw new RuntimeException("Parameter '" + key + "' does not exist in HTTPCsvExternal class");
			}
		}
		for (i = 0; i < requiredParams.length; i++) {
			if (!Arrays.asList(reqParams).contains(requiredParams[i])) {
				throw new RuntimeException("Parameter '" + requiredParams[i] + "' has to be identified from CLI");
			}
		}
	}

	@Override
	public void initialize(Properties params)
	{
		// key to find the http address in the params object
		String HTTP_ADDRESS = params.getProperty(HTTP_ADDRESS_KEY);
		// list of rows found in the csv file in the http address that should be returned to the rows list.
		List<ArrayList> rowsToReturn = new ArrayList<>();

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
				ArrayList<String> output = new ArrayList<>(Arrays.asList(inputLine.split(separator)));


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
	public ArrayList next()
	{
		try {
			return rows.get(pos++);
		} catch (IndexOutOfBoundsException ex )	{
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