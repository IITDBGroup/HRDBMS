package com.exascale.tasks;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
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
		public void run()
		{
			try
			{
				ArrayList<String> tables = new ArrayList<String>();
				HashMap<String, Long> times = new HashMap<String, Long>();
				long target = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("statistics_refresh_target_days")) * 24 * 60 * 60 * 1000;
				String sql = "SELECT SCHEMA, TABNAME, TABLEID FROM SYS.TABLES";
				int numCoords = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("number_of_coords"));
				int myNum = MetaData.myCoordNum() * -1 - 2;
				Connection conn = DriverManager.getConnection("jdbc:hrdbms://localhost:" + HRDBMSWorker.getHParms().getProperty("port_number"));
				conn.setAutoCommit(false);
				Statement stmt = conn.createStatement();
				ResultSet rs = stmt.executeQuery(sql);
				while (rs.next())
				{
					int id = rs.getInt(3);
					if (id % numCoords == myNum)
					{
						String table = rs.getString(1) + "." + rs.getString(2);
						tables.add(table);
					}
				}
			
				rs.close();
				conn.commit();
			
				for (String table : tables)
				{
					try
					{
						sql = "RUNSTATS ON " + table;
						long start = System.currentTimeMillis();
						stmt.execute(sql);
						conn.commit();
						long end = System.currentTimeMillis();
						times.put(table, new Long(end - start));
						
					}
					catch(Exception f)
					{
						HRDBMSWorker.logger.warn("Error running RUNSTATS on " + table, f);
						times.put(table, new Long(0));
					}
				}
			
				conn.close();
			
				//Initial runstats is done
				//Figure out how to schedule next round
				long totalTime = 0;
				for (Long time : times.values())
				{
					totalTime += time;
				}
				
				long extra = target - totalTime;
				long breakTime = extra / tables.size();
				long nextTime = System.currentTimeMillis() + breakTime;
				for (String table : tables)
				{
					MaintenanceManager.schedule(new RunstatsTask(table), nextTime, times.get(table));
					nextTime += (times.get(table) + breakTime);
				}
				
				nextTime -= breakTime;
				MaintenanceManager.schedule(new NewTablesRunstatsTask(tables), nextTime);
			}
			catch(Exception e)
			{
				HRDBMSWorker.logger.warn("Fatal error running RUNSTATS", e);
			}
		}
	}
}
