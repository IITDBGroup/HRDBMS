package com.exascale.misc;

import java.io.IOException;
import java.util.Properties;

public class HParmsDefaults extends Properties 
{
	private static HParmsDefaults retval = null;
	
	private HParmsDefaults()
	{
		super();
	}
	
	public static HParmsDefaults getHParmsDefaults() throws IOException
	{
		if (retval != null)
		{
			return retval;
		}
		
		retval = new HParmsDefaults();
		retval.setProperty("data_directories", "/");
		return retval;
	}
}
