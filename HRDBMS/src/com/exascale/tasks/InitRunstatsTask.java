package com.exascale.tasks;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.exascale.managers.HRDBMSWorker;
import com.exascale.managers.MaintenanceManager;
import com.exascale.optimizer.MetaData;
import com.exascale.threads.HRDBMSThread;

public class InitRunstatsTask extends Task
{
	@Override
	public void run()
	{
		new InitRunstatsThread().start();
	}

	private class InitRunstatsThread extends HRDBMSThread
	{
		@Override
		public void run()
		{
			try
			{
				final List<String> tables = new ArrayList<String>();
				final Map<String, Long> times = new HashMap<String, Long>();
				final long target = Long.parseLong(HRDBMSWorker.getHParms().getProperty("statistics_refresh_target_days")) * 24 * 60 * 60 * 1000;
				String sql = "SELECT SCHEMA, TABNAME, TABLEID FROM SYS.TABLES";
				final int numCoords = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("number_of_coords"));
				final int myNum = MetaData.myCoordNum() * -1 - 2;
				final Connection conn = DriverManager.getConnection("jdbc:hrdbms://localhost:" + HRDBMSWorker.getHParms().getProperty("port_number"));
				conn.setAutoCommit(false);
				final Statement stmt = conn.createStatement();
				final ResultSet rs = stmt.executeQuery(sql);
				while (rs.next())
				{
					final int id = rs.getInt(3);
					if (id % numCoords == myNum)
					{
						final String table = rs.getString(1) + "." + rs.getString(2);
						tables.add(table);
					}
				}

				rs.close();
				conn.commit();

				for (final String table : tables)
				{
					try
					{
						sql = "RUNSTATS ON " + table;
						final long start = System.currentTimeMillis();
						stmt.execute(sql);
						conn.commit();
						final long end = System.currentTimeMillis();
						times.put(table, new Long(end - start));

					}
					catch (final Exception f)
					{
						HRDBMSWorker.logger.warn("Error running RUNSTATS on " + table, f);
						times.put(table, new Long(0));
					}
				}

				conn.close();

				// Initial runstats is done
				// Figure out how to schedule next round
				long totalTime = 0;
				for (final Long time : times.values())
				{
					totalTime += time;
				}

				final long extra = target - totalTime;
				final long breakTime = extra / tables.size();
				long nextTime = System.currentTimeMillis() + breakTime;
				for (final String table : tables)
				{
					MaintenanceManager.schedule(new RunstatsTask(table), nextTime, times.get(table));
					nextTime += (times.get(table) + breakTime);
				}

				nextTime -= breakTime;
				MaintenanceManager.schedule(new NewTablesRunstatsTask(tables), nextTime);
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.warn("Fatal error running RUNSTATS", e);
			}
		}
	}
}
