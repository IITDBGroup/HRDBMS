package com.exascale.gpu;

import java.util.ArrayDeque;
import java.util.List;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.optimizer.ExtendOperator.ExtendKernel;

public class Rootbeer
{
	public void runAll(final List<Kernel> jobs) throws Exception
	{
		// HRDBMSWorker.logger.debug("Rootbeer runAll() called with " +
		// jobs.size() + " jobs");
		if (jobs.get(0) instanceof ExtendKernel)
		{
			// format things for JNI
			final ExtendKernel first = (ExtendKernel)jobs.get(0);
			final float[] rows = new float[jobs.size() * first.poses.size()];
			final float[] results = new float[jobs.size()];
			int i = 0;
			for (final Kernel k : jobs)
			{
				final ExtendKernel kernel = (ExtendKernel)k;
				for (final int pos : kernel.poses)
				{
					rows[i] = ((Number)kernel.row.get(pos)).floatValue();
					i++;
				}
			}

			i = 0;
			ArrayDeque<String> ad = first.master.clone();
			while (!ad.isEmpty())
			{
				i += (ad.pop().getBytes("US-ASCII").length + 1);
			}

			final byte[] prefixBytes = new byte[i];
			i = 0;

			ad = first.master.clone();
			while (!ad.isEmpty())
			{
				final String pre = ad.pop();
				try
				{
					final byte[] preBytes = pre.getBytes("US-ASCII");
					System.arraycopy(preBytes, 0, prefixBytes, i, preBytes.length);
					prefixBytes[i + preBytes.length] = 0;
					i += (preBytes.length + 1);
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.error("", e);
					throw e;
				}
			}
			extendKernel(rows, prefixBytes, results, jobs.size(), first.poses.size(), first.master.size(), prefixBytes.length);
			// format things for ExtendOperator
			for (final float result : results)
			{
				first.calced.add(result * 1.0);
			}
		}
		else
		{
			HRDBMSWorker.logger.error("Unknown kernel type in Rootbeer: " + jobs.get(0).getClass());
			throw new Exception("Unknown kernel type in Rootbeer: " + jobs.get(0).getClass());
		}
	}

	private native void extendKernel(float[] rows, byte[] prefix, float[] results, int numJobs, int numCols, int numPrefixes, int prefixBytesLength);
}
