package com.exascale.tasks;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.managers.MaintenanceManager;
import com.exascale.threads.HRDBMSThread;

public class RunstatsTask extends Task
{
	private final String table;

	public RunstatsTask(String table)
	{
		this.table = table;
	}

	@Override
	public void run()
	{
		new RunstatsThread().start();
	}

	private class RunstatsThread extends HRDBMSThread
	{
		@Override
		public void run()
		{
			try
			{
				Connection conn = DriverManager.getConnection("jdbc:hrdbms://localhost:" + HRDBMSWorker.getHParms().getProperty("port_number"));
				conn.setAutoCommit(false);
				Statement stmt = conn.createStatement();
				String sql = "RUNSTATS ON " + table;
				long start = System.currentTimeMillis();
				stmt.execute(sql);
				conn.commit();
				long end = System.currentTimeMillis();
				conn.close();

				// reschedule myself
				MaintenanceManager.schedule(RunstatsTask.this, -1, end - start, end + Long.parseLong(HRDBMSWorker.getHParms().getProperty("statistics_refresh_target_days")) * 24 * 60 * 60 * 1000);
			}
			catch (Exception e)
			{
				HRDBMSWorker.logger.warn("Error running RUNSTATS on " + table, e);
				MaintenanceManager.failed.put(table, table);
			}
		}
	}
}
