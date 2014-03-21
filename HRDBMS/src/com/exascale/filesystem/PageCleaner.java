package com.exascale.filesystem;

import com.exascale.managers.BufferManager;
import com.exascale.managers.FileManager;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.threads.HRDBMSThread;

public class PageCleaner extends HRDBMSThread
{
	private static int sizeThresh;

	public PageCleaner()
	{
		this.setWait(false);
		this.description = "Buffer Page Cleaner";
	}

	@Override
	public void run()
	{
		final int pct = Integer.parseInt(HRDBMSWorker.getHParms().getProperty("page_cleaner_dirty_pct"));
		sizeThresh = (int)(BufferManager.bp.length - (((pct * 1.0) / 100.0) * BufferManager.bp.length));

		while (true)
		{
			if (BufferManager.numNotTouched == 0 && BufferManager.unmodLookup.size() < sizeThresh)
			{
				for (final Page p : BufferManager.bp)
				{
					if (p.isModified() && (!p.isPinned()))
					{
						p.pin(p.pinTime(), -2);
						try
						{
							FileManager.write(p.block(), p.buffer());
						}
						catch (final Exception e)
						{
							HRDBMSWorker.logger.error("PageCleaner error writing dirty page!", e);
							this.terminate();
							return;
						}

						p.unpin(-2);
					}
				}
			}
			else
			{
				final long sleep = Long.parseLong(HRDBMSWorker.getHParms().getProperty("page_cleaner_sleep_ms"));
				try
				{
					Thread.sleep(sleep);
				}
				catch (final InterruptedException e)
				{
				}
			}
		}
	}
}
