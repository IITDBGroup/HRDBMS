package com.exascale.optimizer.testing;

import java.util.ArrayDeque;
import java.util.List;

import com.exascale.optimizer.testing.ExtendOperator.ExtendKernel;

public class Rootbeer 
{
	static
	{
		System.load("./extend_kernel.so");
	}
	
	public void runAll(List<Kernel> jobs)
	{
		if (jobs.get(0) instanceof ExtendKernel)
		{
			//format things for JNI
			ExtendKernel first = (ExtendKernel)jobs.get(0);
			double[] rows = new double[jobs.size() * first.poses.size()];
			double[] results = new double[jobs.size()];
			int i = 0;
			for (Kernel k : jobs)
			{
				ExtendKernel kernel = (ExtendKernel)k;
				for (int pos : kernel.poses)
				{
					rows[i] = ((Number)kernel.row.get(pos)).doubleValue();
					i++;
				}
			}
			
			i = 0;
			ArrayDeque<String> ad = first.master.clone();
			while (!ad.isEmpty())
			{
				i += (ad.pop().length() + 1);
			}
			
			byte[] prefixBytes = new byte[i];
			i = 0;

			ad = first.master.clone();
			while (!ad.isEmpty())
			{
				String pre = ad.pop();
				try
				{
					byte[] preBytes = pre.getBytes("US-ASCII");
					System.arraycopy(preBytes, 0, prefixBytes, i, preBytes.length);
					prefixBytes[i+preBytes.length] = 0;
					i += (preBytes.length + 1);
				}
				catch(Exception e)
				{
					e.printStackTrace();
					System.exit(1);
				}
			}
			
			extendKernel(rows, prefixBytes, results, jobs.size(), first.poses.size(), first.master.size(), prefixBytes.length);
			//format things for ExtendOperator
			for (double result : results)
			{
				first.calced.add(result);
			}
		}
		else
		{
			System.out.println("Unknown kernel type in Rootbeer: " + jobs.get(0).getClass());
			System.exit(1);
		}
	}
	
	private native void extendKernel(double[] rows, byte[] prefix, double[] results, int numJobs, int numCols, int numPrefixes, int prefixBytesLength);
}
