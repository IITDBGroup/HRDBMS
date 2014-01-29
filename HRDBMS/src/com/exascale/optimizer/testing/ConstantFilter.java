package com.exascale.optimizer.testing;

public class ConstantFilter extends Filter
{
	protected double likelihood;

	public ConstantFilter(double likelihood)
	{
		this.likelihood = likelihood;
	}
	
	public double getLikelihood()
	{
		return likelihood;
	}
}
