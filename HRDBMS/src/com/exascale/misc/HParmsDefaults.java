package com.exascale.misc;

import java.io.IOException;
import java.util.Properties;

public class HParmsDefaults extends Properties 
{
	protected static HParmsDefaults retval = null;
	
	protected HParmsDefaults()
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
		retval.setProperty("page_cleaner_dirty_pct", "50");
		retval.setProperty("page_cleaner_sleep_ms", "5000");
		retval.setProperty("deadlock_timeout_secs", "60");
		retval.setProperty("slock_block_sleep_ms", "1000");
		retval.setProperty("bp_pages", "2097152");
		retval.setProperty("checkpoint_freq_sec", "1800");
		retval.setProperty("port_number", "3232");
		retval.setProperty("data_directories", "/home/hrdbms");
		retval.setProperty("catalog_directory", "/home/hrdbms");
		retval.setProperty("log_dir", "/home/hrdbms");
		retval.setProperty("log_clean_sleep_secs", "60");
		retval.setProperty("target_log_size", "8589934592");
		retval.setProperty("catalog_sync_port", "3233");
		retval.setProperty("rm_sleep_time_ms", "10000");
		retval.setProperty("low_mem_percent", "30");
		return retval;
	}
}
