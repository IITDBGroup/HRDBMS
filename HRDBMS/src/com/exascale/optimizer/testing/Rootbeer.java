package com.exascale.optimizer.testing;

import java.util.List;

import com.exascale.optimizer.testing.ExtendOperator.ExtendKernel;

public class Rootbeer 
{
	public void runAll(List<Kernel> jobs)
	{
		if (jobs.get(0) instanceof ExtendKernel)
		{
			//format things for JNI
			extendKernel(?);
			//format things for ExtendOperator
		}
		else
		{
			System.out.println("Unknown kernel type in Rootbeer: " + jobs.get(0).getClass());
			System.exit(1);
		}
	}
	
	private native void extendKernel();
}
