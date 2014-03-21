package com.exascale.misc;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class HParms extends Properties
{
	private static HParms retval = null;

	private HParms(HParmsDefaults defaults)
	{
		super(defaults);
	}

	public static HParms getHParms() throws IOException
	{
		if (retval != null)
		{
			return retval;
		}

		final HParmsDefaults defaults = HParmsDefaults.getHParmsDefaults();
		retval = new HParms(defaults);
		retval.load(new BufferedReader(new FileReader("hparms")));
		return retval;
	}
}