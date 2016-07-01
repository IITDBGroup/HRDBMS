package com.exascale.threads;

import com.exascale.managers.HRDBMSWorker;
import com.exascale.misc.BigDecimalReplacement;

public class WarmUpThread extends HRDBMSThread
{
	private boolean stop = true;

	public WarmUpThread()
	{
	}

	public WarmUpThread(boolean stop)
	{
		this.stop = stop;
	}

	@Override
	public void run()
	{
		HRDBMSWorker.logger.debug("Warm up thread started");
		long start = System.currentTimeMillis();
		BigDecimalReplacement[] ais = new BigDecimalReplacement[100];
		int i = 0;
		while (i < 100)
		{
			ais[i++] = new BigDecimalReplacement(Math.PI * i);
		}

		i = 0;
		while (true)
		{
			ais[i % 100].add(new BigDecimalReplacement(Math.E + i));
			i++;
			if (i % 100000 == 0)
			{
				long end = System.currentTimeMillis();
				if (end - start >= 90 * 1000)
				{
					if (stop)
					{
						HRDBMSWorker.logger.debug("Warm up thread complete");
						return;
					}
				}
			}
		}
	}
}
