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
		retval.setProperty("deadlock_check_secs", "60");
		retval.setProperty("slock_block_sleep_ms", "1000");
		retval.setProperty("bp_pages", "2097152");
		retval.setProperty("checkpoint_freq_sec", "1800");
		retval.setProperty("port_number", "3232");
		retval.setProperty("data_directories", "/home/hrdbms/");
		retval.setProperty("log_dir", "/home/hrdbms");
		retval.setProperty("log_clean_sleep_secs", "60");
		retval.setProperty("catalog_sync_port", "3233");
		retval.setProperty("rm_sleep_time_ms", "5000");
		retval.setProperty("low_mem_percent", "40");
		retval.setProperty("profile", "false");
		retval.setProperty("detect_thread_deadlocks", "false");
		retval.setProperty("queue_size", "125000");
		retval.setProperty("cuda_batch_size", "30720");
		retval.setProperty("gpu_offload", "false");
		retval.setProperty("temp_directories", "/home/hrdbms/");
		retval.setProperty("queue_block_size", "256");
		retval.setProperty("catalog_creation_tcp_wait_ms", "5000");
		retval.setProperty("max_neighbor_nodes", "100");
		retval.setProperty("max_local_no_hash_product", "10000000");
		retval.setProperty("max_local_sort", "2500000");
		retval.setProperty("parallel_sort_min_rows", Integer.toString((int)(2500 * Math.pow(Math.pow(Runtime.getRuntime().availableProcessors(), 2), 1.0 / 3.0))));
		retval.setProperty("prefetch_request_size", "24");
		retval.setProperty("pages_in_advance", "1");
		retval.setProperty("getpage_attempts", "30000");
		retval.setProperty("getpage_fail_sleep_time_ms", "1");
		retval.setProperty("archive_dir", "/home/hrdbms/");
		retval.setProperty("hrdbms_user", "hrdbms");
		retval.setProperty("Xmx_string", "64g");
		retval.setProperty("number_of_coords", "1");
		retval.setProperty("max_batch", "1250000");
		retval.setProperty("archive", "false");
		retval.setProperty("queue_flush_retry_timeout", "1");
		retval.setProperty("statistics_refresh_target_days", "7");
		retval.setProperty("old_file_cleanup_target_days", "7");
		retval.setProperty("reorg_refresh_target_days", "7");
		retval.setProperty("max_load_average", Integer.toString(25 * Runtime.getRuntime().availableProcessors()));
		retval.setProperty("critical_mem_percent", "15");
		retval.setProperty("stack_size", "2M");
		retval.setProperty("jvm_args", "-XX:+UseG1GC");
		retval.setProperty("external_factor", "2.4");
		retval.setProperty("hash_external_factor", "12.5");
		retval.setProperty("max_queued_load_flush_threads", "5");
		retval.setProperty("sort_gb_factor", "2.4");
		retval.setProperty("java_path", "");
		retval.setProperty("max_cfc_cache_size", "1572864");
		retval.setProperty("batches_per_check", "4");
		retval.setProperty("create_index_batch_size", "1000000");
		retval.setProperty("num_fcs_per_cfc", "1024");
		retval.setProperty("max_open_files", "100000");
		retval.setProperty("bp_trim_sleep_ms", "300000");
		retval.setProperty("enable_cvarchar_compression", "true");
		retval.setProperty("enable_col_reordering", "true");
		retval.setProperty("hjo_bucket_size_shift", "5");
		retval.setProperty("mo_bucket_size_shift", "5");
		retval.setProperty("max_concurrent_writers_per_temp_disk", "3");
		retval.setProperty("mo_max_par", Integer.toString(Runtime.getRuntime().availableProcessors()));
		retval.setProperty("hjo_max_par", Integer.toString(Runtime.getRuntime().availableProcessors()));
		retval.setProperty("scfc", "true");
		retval.setProperty("use_direct_buffers_for_flush", "true");
		retval.setProperty("num_direct", "20000");
		retval.setProperty("num_sbms", "256");
		retval.setProperty("extend_max_par", Integer.toString(Runtime.getRuntime().availableProcessors()));
		retval.setProperty("agg_max_par", Integer.toString(Runtime.getRuntime().availableProcessors()));
		HParmsDefaults.retval = retval;
		return retval;
	}
}
