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

		HParmsDefaults retval = new HParmsDefaults();
		retval.setProperty("deadlock_timeout_secs", "60");
		retval.setProperty("slock_block_sleep_ms", "1000");
		retval.setProperty("bp_pages", "2097152");
		retval.setProperty("checkpoint_freq_sec", "1800");
		retval.setProperty("port_number", "3232");
		retval.setProperty("data_directories", "/home/hrdbms/");
		retval.setProperty("log_dir", "/home/hrdbms");
		retval.setProperty("log_clean_sleep_secs", "60");
		retval.setProperty("catalog_sync_port", "3233");
		retval.setProperty("rm_sleep_time_ms", "10000");
		retval.setProperty("low_mem_percent", "30");
		retval.setProperty("high_mem_percent", "70");
		retval.setProperty("percent_to_cut", "2");
		retval.setProperty("min_cleaner_thread_size", "20000");
		retval.setProperty("max_cleaner_threads", "16");
		retval.setProperty("profile", "false");
		retval.setProperty("detect_thread_deadlocks", "false");
		retval.setProperty("queue_size", "2500000");
		retval.setProperty("cuda_batch_size", "30720");
		retval.setProperty("gpu_offload", "false");
		retval.setProperty("temp_directories", "/home/hrdbms/");
		retval.setProperty("queue_block_size", "256");
		retval.setProperty("catalog_creation_tcp_wait_ms", "5000");
		retval.setProperty("phase1_additional_pushdowns", "25");
		retval.setProperty("max_neighbor_nodes", "100");
		retval.setProperty("max_card_before_hash", "500000");
		retval.setProperty("min_card_for_hash", "250000");
		retval.setProperty("max_local_no_hash_product", "1000000");
		retval.setProperty("max_local_left_hash", "1000000");
		retval.setProperty("min_local_left_hash", "500000");
		retval.setProperty("max_local_sort", "1000000");
		retval.setProperty("parallel_sort_min_rows", "50000");
		retval.setProperty("prefetch_request_size", "80");
		retval.setProperty("pages_in_advance", "40");
		retval.setProperty("getpage_attempts", "60000");
		retval.setProperty("getpage_fail_sleep_time_ms", "1");
		retval.setProperty("archive_dir", "/home/hrdbms/");
		retval.setProperty("hrdbms_user", "hrdbms");
		retval.setProperty("Xmx_string", "64g");
		retval.setProperty("number_of_coords", "1");
		retval.setProperty("max_batch", "100000");
		retval.setProperty("archive", "false");
		retval.setProperty("queue_flush_retry_timeout", "1000");
		retval.setProperty("statistics_refresh_target_days", "7");
		retval.setProperty("old_file_cleanup_target_days", "7");
		HParmsDefaults.retval = retval;
		return retval;
	}
}
