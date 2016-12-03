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

		final HParmsDefaults retval = new HParmsDefaults();
		retval.setProperty("deadlock_check_secs", "60");
		retval.setProperty("slock_block_sleep_ms", "1000");
		retval.setProperty("bp_pages", "256");
		retval.setProperty("checkpoint_freq_sec", "1800");
		retval.setProperty("port_number", "3232");
		retval.setProperty("data_directories", "/home/hrdbms/");
		retval.setProperty("log_dir", "/home/hrdbms");
		retval.setProperty("catalog_sync_port", "3233");
		retval.setProperty("rm_sleep_time_ms", "5000");
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
		retval.setProperty("pages_in_advance", "12");
		retval.setProperty("getpage_attempts", "30000");
		retval.setProperty("getpage_fail_sleep_time_ms", "1");
		retval.setProperty("archive_dir", "/home/hrdbms/");
		retval.setProperty("hrdbms_user", "hrdbms");
		retval.setProperty("Xmx_string", "32g");
		retval.setProperty("number_of_coords", "1");
		retval.setProperty("max_batch", "1250000");
		retval.setProperty("archive", "false");
		retval.setProperty("queue_flush_retry_timeout", "1");
		retval.setProperty("statistics_refresh_target_days", "7");
		retval.setProperty("old_file_cleanup_target_days", "7");
		retval.setProperty("reorg_refresh_target_days", "7");
		retval.setProperty("critical_mem_percent", "15");
		retval.setProperty("stack_size", "2M");
		retval.setProperty("jvm_args", "-XX:+UseG1GC -XX:G1HeapRegionSize=32m -XX:+ParallelRefProcEnabled -XX:MaxDirectMemorySize=327772160000 -XX:+AggressiveOpts -XX:CompileThreshold=200 -Xbatch -XX:-TieredCompilation -XX:InitiatingHeapOccupancyPercent=25 -XX:MaxGCPauseMillis=1500 -XX:ParallelGCThreads=" + Runtime.getRuntime().availableProcessors());
		retval.setProperty("external_factor", "68.0");
		retval.setProperty("hash_external_factor", "120.0");
		retval.setProperty("max_queued_load_flush_threads", "5");
		retval.setProperty("sort_gb_factor", "68.0");
		retval.setProperty("java_path", "");
		retval.setProperty("batches_per_check", "2");
		retval.setProperty("create_index_batch_size", "1000000");
		retval.setProperty("max_open_files", "100000");
		retval.setProperty("enable_cvarchar_compression", "true");
		// retval.setProperty("enable_col_reordering", "true");
		retval.setProperty("hjo_bucket_size_shift", "0");
		retval.setProperty("mo_bucket_size_shift", "0");
		retval.setProperty("max_concurrent_writers_per_temp_disk", "3");
		retval.setProperty("mo_max_par", "1");
		retval.setProperty("hjo_max_par", Integer.toString(Runtime.getRuntime().availableProcessors() / 2));
		retval.setProperty("scfc", "true");
		retval.setProperty("use_direct_buffers_for_flush", "false");
		retval.setProperty("num_direct", "20000");
		retval.setProperty("num_sbms", "256");
		retval.setProperty("extend_max_par", Integer.toString(Runtime.getRuntime().availableProcessors()));
		retval.setProperty("agg_max_par", Integer.toString(Runtime.getRuntime().availableProcessors()));
		retval.setProperty("page_size", "2093056");
		retval.setProperty("nram_spsc_queue_size", "125000");
		retval.setProperty("max_pbpe_time", "3000000");
		retval.setProperty("sort_bucket_size", "300000");
		retval.setProperty("pbpe_externalize_interval_s", "300");
		retval.setProperty("hjo_bin_size", "300000");
		retval.setProperty("mo_bin_size", "3000000");
		retval.setProperty("lock_timeout_ms", "60000");
		retval.setProperty("max_rr", "1200");
		retval.setProperty("initial_max_hops", "3");
		retval.setProperty("direct_buffer_size", "8388608");
		retval.setProperty("pbpe_version", "2");
		retval.setProperty("do_min_max", "true");
		HParmsDefaults.retval = retval;
		return retval;
	}
}
