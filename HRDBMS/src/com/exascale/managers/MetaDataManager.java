package com.exascale.managers;

import com.exascale.threads.HRDBMSThread;

public class MetaDataManager extends HRDBMSThread
{
	public MetaDataManager()
	{
		HRDBMSWorker.logger.info("Starting initialization of Metadata Manager.");
		description = "MetaData Manager";
		this.setWait(true);
	}

	@Override
	public void run()
	{
		if (!FileManager.sysTablesExists())
		{
			try
			{
				HRDBMSWorker.logger.info("About to start initial catalog creation.");
				FileManager.createCatalog();
			}
			catch (final Exception e)
			{
				HRDBMSWorker.logger.error("Exception occurred during initial catalog creation/synchronization.", e);
				System.exit(1);
			}

			HRDBMSWorker.logger.info("Metadata Manager initialization complete.");
		}

		this.terminate();
		return;
	}
}